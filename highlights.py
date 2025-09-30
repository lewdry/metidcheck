import requests
import json

url = "https://collectionapi.metmuseum.org/public/collection/v1/search?isHighlight=true&isPublicDomain=true"
resp = requests.get(url)
data = resp.json()  # Will fail if the server returns HTML (502)
object_ids = data.get("objectIDs", [])
with open("met_highlight_ids.json", "w") as f:
    json.dump(object_ids, f)
