import gzip
import json

filepath = "games/380590.json.gz"

with gzip.open(filepath, "rt", encoding="utf-8") as f:
    data = json.load(f)

print(type(data))  # <class 'dict'> or list depending on your JSON
print(data)        # your JSON data as Python dict/list