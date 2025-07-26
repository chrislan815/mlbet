import gzip
import json
import io
import os
import sqlite3
from pprint import pprint


def flatten_score(team_data):
    return {
        "runs": team_data["runs"],
        "hits": team_data["hits"],
        "errors": team_data["errors"],
        "left_on_base": team_data.get("leftOnBase", 0)
    }

def get_score(_game_id):
    filepath = f"live_feeds/{_game_id}.json.gz"

    if not os.path.exists(filepath):
        return {}

    with gzip.open(filepath, "rb") as f:  # Open in binary mode
        with io.TextIOWrapper(f, encoding="utf-8") as decoder:
            data = json.load(decoder)
    score_data = data["liveData"]["boxscore"]["teams"]
    flattened_score = {
        side: flatten_score(score_data[side])
        for side in ["home", "away"]
    }
    return flattened_score


if __name__ == '__main__':
    conn = sqlite3.connect("mlb-v2.db")
    cursor = conn.cursor()
    rows = cursor.execute("""
        SELECT game_pk
        FROM game
        WHERE game_pk NOT IN (
            SELECT DISTINCT game_pk FROM lineup
        ) and status = 'Final'
        ORDER BY game_pk DESC
        limit 5;
    """).fetchall()
    data_rows = []
    for row in rows:
        game_id = row[0]
        score = get_score(game_id)
        pprint(score)