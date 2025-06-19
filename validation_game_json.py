import os
import gzip
import json

games_dir = "games"
bad_files = []

for filename in os.listdir(games_dir):
    if not filename.endswith(".json.gz"):
        continue

    path = os.path.join(games_dir, filename)
    try:
        with gzip.open(path, "rt", encoding="utf-8") as f:
            json.load(f)
    except Exception as e:
        print(f"❌ Invalid JSON in {filename}: {e}")
        bad_files.append(filename)

print(f"\nChecked {len(os.listdir(games_dir))} files")
print(f"Invalid files: {len(bad_files)}")

if bad_files:
    print("List of bad files:")
    for f in bad_files:
        print(f"  - {f}")
