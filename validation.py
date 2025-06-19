import os
import sqlite3

games_dir = "games"

# Step 1: Get gamePk values from .json.gz filenames
file_gamepks = {
    f.removesuffix(".json.gz")
    for f in os.listdir(games_dir)
    if f.endswith(".json.gz") and f.removesuffix(".json.gz").isdigit()
}

print(f"Found {len(file_gamepks):,} .json.gz files in '{games_dir}/'")

# Step 2: Query finalized gamePk values from DB
conn = sqlite3.connect("mlb.db")  # Or use your actual DB connector
cursor = conn.cursor()
cursor.execute("SELECT game_pk FROM main.game WHERE status = 'Final'")
db_gamepks = {str(row[0]) for row in cursor.fetchall()}

print(f"Found {len(db_gamepks):,} games in DB with status='Final'")

# Step 3: Compare and identify missing files
missing_from_files = db_gamepks - file_gamepks

print(f"{len(missing_from_files):,} gamePk values are missing from the filesystem.")

# Step 4: Print examples
for i, pk in enumerate(sorted(missing_from_files)):
    print(pk)
    if i >= 20: break