import json
import os

# Read the main JSON file
print("Reading met_metadata.json...")
with open('met_metadata.json', 'r', encoding='utf-8') as f:
    artworks = json.load(f)

print(f"Found {len(artworks)} artworks")

# Create metadata directory if it doesn't exist
os.makedirs('metadata', exist_ok=True)
print("Created 'metadata' directory")

# Split into individual files
success_count = 0
error_count = 0

for artwork in artworks:
    try:
        object_id = artwork['objectID']
        filename = f"metadata/{object_id}.json"
        
        # Write individual JSON file
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(artwork, f, ensure_ascii=False, indent=2)
        
        success_count += 1
        
        # Print progress every 100 files
        if success_count % 100 == 0:
            print(f"Processed {success_count} files...")
            
    except Exception as e:
        print(f"Error processing artwork: {e}")
        error_count += 1

print(f"\n✅ Done!")
print(f"Successfully created {success_count} JSON files")
if error_count > 0:
    print(f"❌ {error_count} files failed")
print(f"Files saved in './metadata/' directory")