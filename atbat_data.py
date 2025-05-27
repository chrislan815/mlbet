import sqlite3
import statsapi

conn = sqlite3.connect("mlb.db")
conn.row_factory = sqlite3.Row  # Enable name-based access
cursor = conn.cursor()

rows = cursor.execute("""
SELECT * FROM main.games
WHERE status = 'Final'
ORDER BY game_date DESC
LIMIT 10
""")

for row in rows:
    game_id = row['game_pk']
    print(game_id)

game_id_1 = "777757"
