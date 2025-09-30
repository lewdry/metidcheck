import json

# Read the main JSON file
print("Reading met_metadata.json...")
with open('met_metadata.json', 'r', encoding='utf-8') as f:
    artworks = json.load(f)

print(f"Found {len(artworks)} artworks")

# Extract object IDs
object_ids = [artwork['objectID'] for artwork in artworks]

print(f"Extracted {len(object_ids)} object IDs")

# Sort them (optional, makes it easier to scan)
object_ids.sort()

# Save to JSON file
with open('object_ids.json', 'w') as f:
    json.dump(object_ids, f, indent=2)

print(f"\n✅ Done!")
print(f"Object IDs saved to 'object_ids.json'")
print(f"{len(object_ids)} IDs in JSON array format")