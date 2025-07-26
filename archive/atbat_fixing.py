import os
import gzip
import json
import logging
import sqlite3

import requests

from atbat_data_local import save_atbat

API_ENDPOINT_TEMPLATE = "https://statsapi.mlb.com/api/v1/game/{gamePk}/winProbability"

def load_and_save(game_pk, filepath):
    url = API_ENDPOINT_TEMPLATE.format(gamePk=game_pk)
    logging.info(f"Fetching {url}")
    resp = requests.get(url)
    if resp.status_code != 200:
        print(f"Failed to fetch {game_pk}: HTTP {resp.status_code}")
        return False

    data = resp.json()
    with gzip.open(filepath, "wt", encoding="utf-8") as f:
        json.dump(data, f)
    logging.info(f"Saved compressed file {filepath}")
    return True

def process_game(connection, game_pk):
    path = os.path.join("games", f"{game_pk}.json.gz")
    data = None

    if os.path.exists(path):
        try:
            with gzip.open(path, "rt", encoding="utf-8") as f:
                data = json.load(f)
        except json.JSONDecodeError:
            print(f"Corrupt JSON in {path}, refetching...")

    if data is None:
        success = load_and_save(game_pk, path)
        if not success:
            return
        try:
            with gzip.open(path, "rt", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            print(f"Still failed to load {path} after re-fetching: {e}")
            return

    save_atbat(connection, game_pk, data)

def main(connection, game_pks):
    for game_pk in game_pks:
        print(f"Processing game {game_pk}")
        process_game(connection, game_pk)

    connection.close()

if __name__ == '__main__':
    connection = sqlite3.connect("mlb.db")
    rows = connection.cursor().execute("""
    select game_pk from main.game WHERE status = 'Final'
    """)
    # currently 6/6 2:44 2374989
    pks = [719409, 778148] or [row[0] for row in rows.fetchall()]
    print(pks)

    main(connection, pks)