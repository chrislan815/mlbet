# Path to the file
file_path = '/Users/chris.lan/Documents/mlb_important_games.txt'

game_pk_list = []

with open(file_path, 'r') as file:
    for line in file:
        line = line.strip()
        if line and line.isdigit():
            game_pk_list.append(int(line))

print(game_pk_list)

import sqlite3

# --- 1. Connect to SQLite ---
conn = sqlite3.connect("mlb.db")
cursor = conn.cursor()


game_pk_tuple = tuple(game_pk_list)
if len(game_pk_tuple) == 1:
    # SQLite requires a trailing comma for single-element tuples
    game_pk_tuple = (game_pk_tuple[0],)

# delete_play_event_sql = f"DELETE FROM play_event WHERE game_pk NOT IN ({','.join(['?']*len(game_pk_tuple))})"
# delete_atbat_sql = f"DELETE FROM atbat WHERE game_pk NOT IN ({','.join(['?']*len(game_pk_tuple))})"

delete_game_sql = f"DELETE FROM game WHERE game_pk NOT IN ({','.join(['?']*len(game_pk_tuple))})"

cursor.execute(delete_game_sql, game_pk_tuple)
# cursor.execute(delete_play_event_sql, game_pk_tuple)
# cursor.execute(delete_atbat_sql, game_pk_tuple)

conn.commit()
conn.close()

print(f"Deleted records for {len(game_pk_list)} game_pk values from play_event and atbat.")