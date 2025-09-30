import json
import time
import requests
from pathlib import Path
from typing import Dict, List, Optional
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('met_scraper.log'),
        logging.StreamHandler()
    ]
)

class MetScraper:
    def __init__(self, 
                 input_file: str = "object_ids.json",
                 output_file: str = "met_metadata.json",
                 images_dir: str = "met-images",
                 rate_limit_delay: float = 1.0):
        """
        Initialize the Met Museum scraper.
        
        Args:
            input_file: JSON file containing list of object IDs
            output_file: JSON file to write metadata to
            images_dir: Directory to save downloaded images
            rate_limit_delay: Seconds to wait between API calls (default 1.0)
        """
        self.input_file = input_file
        self.output_file = output_file
        self.images_dir = Path(images_dir)
        self.rate_limit_delay = rate_limit_delay
        self.base_url = "https://collectionapi.metmuseum.org/public/collection/v1/objects"
        
        # Create images directory if it doesn't exist
        self.images_dir.mkdir(exist_ok=True)
        
        # Load existing metadata to enable resume functionality
        self.metadata = self._load_existing_metadata()
        self.processed_ids = set(item['objectID'] for item in self.metadata)
        
    def _load_existing_metadata(self) -> List[Dict]:
        """Load existing metadata file if it exists."""
        try:
            with open(self.output_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            logging.info(f"No existing {self.output_file} found. Starting fresh.")
            return []
        except json.JSONDecodeError:
            logging.warning(f"Could not parse {self.output_file}. Starting fresh.")
            return []
    
    def _load_object_ids(self) -> List[int]:
        """Load object IDs from input JSON file."""
        try:
            with open(self.input_file, 'r') as f:
                data = json.load(f)
                # Handle both array format and object format
                if isinstance(data, list):
                    return data
                elif isinstance(data, dict) and 'objectIDs' in data:
                    return data['objectIDs']
                else:
                    raise ValueError("Input file must contain array of IDs or object with 'objectIDs' key")
        except Exception as e:
            logging.error(f"Error loading {self.input_file}: {e}")
            raise
    
    def _get_optimized_image_url(self, original_url: str) -> str:
        """Convert original image URL to web-large version."""
        if not original_url:
            return ""
        return original_url.replace('/original/', '/web-large/')
    
    def _fetch_artwork_data(self, object_id: int) -> Optional[Dict]:
        """
        Fetch artwork data from Met API.
        
        Returns:
            Dictionary with artwork data or None if failed
        """
        try:
            url = f"{self.base_url}/{object_id}"
            response = requests.get(url, timeout=15)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to fetch object {object_id}: {e}")
            return None
    
    def _download_image(self, image_url: str, object_id: int) -> Optional[str]:
        """
        Download image and save locally.
        
        Returns:
            Local filename if successful, None if failed
        """
        if not image_url:
            return None
            
        try:
            response = requests.get(image_url, timeout=30, stream=True)
            response.raise_for_status()
            
            # Determine file extension from URL or content-type
            content_type = response.headers.get('content-type', '')
            if 'jpeg' in content_type or 'jpg' in content_type or image_url.endswith('.jpg'):
                ext = '.jpg'
            elif 'png' in content_type or image_url.endswith('.png'):
                ext = '.png'
            else:
                ext = '.jpg'  # default
            
            filename = f"{object_id}{ext}"
            filepath = self.images_dir / filename
            
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            logging.info(f"Downloaded image for object {object_id}")
            return filename
            
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to download image for object {object_id}: {e}")
            return None
    
    def _extract_metadata(self, artwork_data: Dict, local_image_filename: Optional[str]) -> Dict:
        """
        Extract relevant metadata from artwork data.
        
        Args:
            artwork_data: Raw API response
            local_image_filename: Local filename of downloaded image
            
        Returns:
            Cleaned metadata dictionary
        """
        return {
            'objectID': artwork_data.get('objectID'),
            'title': artwork_data.get('title', 'Untitled'),
            'artistDisplayName': artwork_data.get('artistDisplayName', ''),
            'artistDisplayBio': artwork_data.get('artistDisplayBio', ''),
            'objectDate': artwork_data.get('objectDate', ''),
            'objectName': artwork_data.get('objectName', ''),
            'medium': artwork_data.get('medium', ''),
            'dimensions': artwork_data.get('dimensions', ''),
            'department': artwork_data.get('department', ''),
            'culture': artwork_data.get('culture', ''),
            'period': artwork_data.get('period', ''),
            'dynasty': artwork_data.get('dynasty', ''),
            'creditLine': artwork_data.get('creditLine', ''),
            'objectURL': artwork_data.get('objectURL', ''),
            'isPublicDomain': artwork_data.get('isPublicDomain', False),
            'primaryImage': artwork_data.get('primaryImage', ''),
            'primaryImageSmall': artwork_data.get('primaryImageSmall', ''),
            'localImage': local_image_filename,  # Our local copy
            'tags': artwork_data.get('tags', [])
        }
    
    def _save_metadata(self):
        """Save current metadata to output file."""
        with open(self.output_file, 'w', encoding='utf-8') as f:
            json.dump(self.metadata, f, indent=2, ensure_ascii=False)
        logging.info(f"Saved {len(self.metadata)} artworks to {self.output_file}")
    
    def process_artworks(self, download_images: bool = True, save_frequency: int = 10):
        """
        Main processing loop to fetch metadata and optionally download images.
        
        Args:
            download_images: Whether to download images locally
            save_frequency: Save metadata every N artworks (for crash recovery)
        """
        object_ids = self._load_object_ids()
        total = len(object_ids)
        
        logging.info(f"Loaded {total} object IDs")
        logging.info(f"Already processed: {len(self.processed_ids)}")
        logging.info(f"Remaining: {total - len(self.processed_ids)}")
        
        for idx, object_id in enumerate(object_ids, 1):
            # Skip if already processed
            if object_id in self.processed_ids:
                logging.debug(f"Skipping {object_id} (already processed)")
                continue
            
            logging.info(f"Processing {idx}/{total}: Object ID {object_id}")
            
            # Fetch artwork data
            artwork_data = self._fetch_artwork_data(object_id)
            
            if not artwork_data:
                logging.warning(f"Skipping object {object_id} (no data)")
                time.sleep(self.rate_limit_delay)
                continue
            
            # Check if it's public domain and has an image
            if not artwork_data.get('isPublicDomain'):
                logging.warning(f"Skipping object {object_id} (not public domain)")
                time.sleep(self.rate_limit_delay)
                continue
                
            if not artwork_data.get('primaryImage'):
                logging.warning(f"Skipping object {object_id} (no image)")
                time.sleep(self.rate_limit_delay)
                continue
            
            # Download image if requested
            local_image_filename = None
            if download_images:
                optimized_url = self._get_optimized_image_url(artwork_data['primaryImage'])
                local_image_filename = self._download_image(optimized_url, object_id)
            
            # Extract and save metadata
            metadata = self._extract_metadata(artwork_data, local_image_filename)
            self.metadata.append(metadata)
            self.processed_ids.add(object_id)
            
            # Periodic save for crash recovery
            if len(self.metadata) % save_frequency == 0:
                self._save_metadata()
            
            # Rate limiting
            time.sleep(self.rate_limit_delay)
        
        # Final save
        self._save_metadata()
        logging.info(f"Processing complete! Total artworks: {len(self.metadata)}")
        
        # Summary statistics
        with_images = sum(1 for item in self.metadata if item.get('localImage'))
        logging.info(f"Artworks with local images: {with_images}")


def main():
    """Example usage"""
    scraper = MetScraper(
        input_file="object_ids_json3.json",
        output_file="met_metadata.json",
        images_dir="met-images",
        rate_limit_delay=1.0  # 1 second between requests
    )
    
    # Process artworks and download images
    scraper.process_artworks(
        download_images=True,
        save_frequency=10  # Save every 10 artworks
    )


if __name__ == "__main__":
    main()