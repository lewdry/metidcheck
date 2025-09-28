import csv
import requests
from time import sleep
from pathlib import Path

# Input CSV with a single column of ObjectIDs
INPUT_CSV = 'ObjectID.csv'
OUTPUT_CSV = 'met_objects_with_images.csv'

# Adjustable: pause between requests to avoid hammering the API
DELAY_SECONDS = 0.05  # 50ms between requests (20 requests/sec)

# Read ObjectIDs from input CSV
with open(INPUT_CSV, 'r', encoding='utf-8') as f:
    reader = csv.reader(f)
    next(reader, None)  # skip header if present
    object_ids = [row[0] for row in reader]

print(f"Checking {len(object_ids)} objects for images...")

# Open output CSV in append mode
file_exists = Path(OUTPUT_CSV).exists()
with open(OUTPUT_CSV, 'a', newline='', encoding='utf-8') as out_file:
    fieldnames = ['objectID', 'title', 'artist', 'date', 'medium', 'primaryImage']
    writer = csv.DictWriter(out_file, fieldnames=fieldnames)

    # Write header only if file doesn't already exist
    if not file_exists:
        writer.writeheader()

    for count, oid in enumerate(object_ids, start=1):
        try:
            url = f'https://collectionapi.metmuseum.org/public/collection/v1/objects/{oid}'
            resp = requests.get(url, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                if data.get('primaryImage'):
                    row = {
                        'objectID': data['objectID'],
                        'title': data.get('title', ''),
                        'artist': data.get('artistDisplayName', ''),
                        'date': data.get('objectDate', ''),
                        'medium': data.get('medium', ''),
                        'primaryImage': data['primaryImage']
                    }
                    writer.writerow(row)  # append immediately

        except Exception as e:
            print(f"Error fetching {oid}: {e}")

        # Progress indicator
        if count % 1000 == 0:
            print(f"Checked {count}/{len(object_ids)} objects...")

        sleep(DELAY_SECONDS)

print(f"Completed. CSV saved to {OUTPUT_CSV}")
