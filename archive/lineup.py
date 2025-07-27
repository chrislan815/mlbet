import gzip
import json
import io
import os
import sqlite3
import logging


def flatten(data):
    flattened = {
        "battingOrder": data["battingOrder"],
    }

    if "parentTeamId" in data:
        flattened["parentTeamId"] = data["parentTeamId"]

    for key, value in data["person"].items():
        flattened[f"person_{key}"] = value

    for key, value in data["position"].items():
        flattened[f"position_{key}"] = value
    return flattened

def insert_lineup_players(_cursor, game_id_team_type_lineup_players):
    sql = """
    INSERT OR IGNORE INTO lineup (
        game_pk,
        team_type,
        batting_order,
        player_id,
        player_name,
        player_link,
        parent_team_id,
        position_code,
        position_abbreviation,
        position_name,
        position_type
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    values = []
    for (_game_id, _team_type, _rows) in game_id_team_type_lineup_players:
        for _data in _rows:
            pareent_team_id = _data.get("parentTeamId", None)
            values.append((
                _game_id,
                _team_type,
                _data["battingOrder"],
                _data["person_id"],
                _data["person_fullName"],
                _data["person_link"],
                pareent_team_id,
                _data["position_code"],
                _data["position_abbreviation"],
                _data["position_name"],
                _data["position_type"]
            ))
    try:
        _cursor.executemany(sql, values)
        _cursor.connection.commit()
        logging.info(f"Inserted {len(values)} lineup player rows for game(s) {[gid for (gid, _, _) in game_id_team_type_lineup_players]}")
    except Exception as e:
        logging.error(f"Error inserting lineup players: {e}")

def get_lineup(_game_id):
    filepath = f"live_feeds/{_game_id}.json.gz"

    if not os.path.exists(filepath):
        return {}

    with gzip.open(filepath, "rb") as f:  # Open in binary mode
        with io.TextIOWrapper(f, encoding="utf-8") as decoder:
            data = json.load(decoder)

    _lineup = {"home": [], "away": []}
    for side in ["home", "away"]:
        players = data["liveData"]["boxscore"]["teams"][side]["players"]
        ordered = sorted(
            (p for p in players.values() if "battingOrder" in p),
            key=lambda p: int(p["battingOrder"])
        )
        _lineup[side] = [
            flatten(p)
            for p in ordered
        ]
    return _lineup

def save_lineup(cursor, game_id):
    lineup = get_lineup(game_id)
    if not lineup:
        logging.warning(f"No lineup found for game {game_id}")
        return False
    data_rows = []
    for team_type in ["home", "away"]:
        data = lineup[team_type]
        data_rows.append((game_id, team_type, data))
    try:
        insert_lineup_players(cursor, data_rows)
        logging.info(f"Saved lineup for game {game_id}")
        return True
    except Exception as e:
        logging.error(f"Error saving lineup for game {game_id}: {e}")
        return False

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    conn = sqlite3.connect("mlb-v2.db")
    cursor = conn.cursor()
    rows = cursor.execute("""
        SELECT game_pk
        FROM game
        WHERE game_pk NOT IN (
            SELECT DISTINCT game_pk FROM lineup
        ) and status = 'Final'
        ORDER BY game_pk DESC;
    """).fetchall()
    for row in rows:
        game_id = row[0]
        save_lineup(cursor, game_id)
