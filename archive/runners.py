import gzip
import json
import os
import sqlite3

# import glom
# from pprint import pprint

import statsapi
import logging


""" runners Data Schema
(game_pk and about_atBatIndex + artificial runner_index)
"""

def flatten_runner_data(game_pk, about_atBatIndex, runner_data):
    movement = runner_data.get('movement', {})
    details = runner_data.get('details', {})
    runner = details.get('runner', {})

    return {
        'game_pk': game_pk,
        'about_atBatIndex': about_atBatIndex,
        'movement_originBase': movement.get('originBase'),
        'movement_start': movement.get('start'),
        'movement_end': movement.get('end'),
        'movement_outBase': movement.get('outBase'),
        'movement_isOut': movement.get('isOut'),
        'movement_outNumber': movement.get('outNumber'),

        'details_event': details.get('event'),
        'details_eventType': details.get('eventType'),
        'details_movementReason': details.get('movementReason'),
        'details_isScoringEvent': details.get('isScoringEvent'),
        'details_rbi': details.get('rbi'),
        'details_earned': details.get('earned'),
        'details_teamUnearned': details.get('teamUnearned'),
        'details_playIndex': details.get('playIndex'),

        'runner_id': runner.get('id'),
        'runner_fullName': runner.get('fullName'),
        'runner_link': runner.get('link'),
    }


def insert_pitch_data(cursor, game_pk, about_atBatIndex, runner_datas):
    records = [flatten_runner_data(game_pk, about_atBatIndex, runner_data) for runner_data in runner_datas]
    sql = """
    INSERT OR REPLACE INTO runner (
        game_pk,
        about_atBatIndex,
        movement_originBase,
        movement_start,
        movement_end,
        movement_outBase,
        movement_isOut,
        movement_outNumber,
        details_event,
        details_eventType,
        details_movementReason,
        details_isScoringEvent,
        details_rbi,
        details_earned,
        details_teamUnearned,
        details_playIndex,
        runner_id,
        runner_fullName,
        runner_link
    ) VALUES (
        ?,?,?,?,?,?, ?,?,?,?,?,?, ?,?,?,?,?,?,?
    )
    """

    values = (
        (
            data['game_pk'],
            data['about_atBatIndex'],
            data.get('movement_originBase'),
            data.get('movement_start'),
            data.get('movement_end'),
            data.get('movement_outBase'),
            data.get('movement_isOut'),
            data.get('movement_outNumber'),
            data.get('details_event'),
            data.get('details_eventType'),
            data.get('details_movementReason'),
            data.get('details_isScoringEvent'),
            data.get('details_rbi'),
            data.get('details_earned'),
            data.get('details_teamUnearned'),
            data.get('details_playIndex'),
            data.get('runner_id'),
            data.get('runner_fullName'),
            data.get('runner_link'),
        )
        for data in records
    )
    cursor.executemany(sql, values)
    cursor.connection.commit()

def load_pbp_from_file(game_pk):
    path = os.path.join("games", f"{game_pk}.json.gz")
    if not os.path.exists(path):
        logging.warning(f"Missing file: {path}")
        return None

    try:
        with gzip.open(path, "rt", encoding="utf-8") as f:
            data = json.load(f)
        return data
    except Exception as e:
        logging.error(f"Failed to load or parse {path}: {e}")
        return None


def save_runners(cursor, game_ids):
    processed_game_pks = {
        row[0] for row in cursor.execute("SELECT DISTINCT game_pk FROM runner")
    }
    logging.info(f"Found processed_game_pks {len(processed_game_pks)} game ids")

    for game_id in game_ids:
        if game_id in processed_game_pks:
            logging.info(f"Skipping {game_id}, already processed.")
            continue
        logging.info(f"Processing game {game_id}...")
        pbp = load_pbp_from_file(game_id)
        if pbp is None:
            logging.warning(f"{game_id}.json.gz has nothing...")
            continue
        for play_event in pbp:
            atbat_index = play_event["about"]["atBatIndex"]
            runner_data = [runner for runner in play_event["runners"]]
            insert_pitch_data(cursor, game_id, atbat_index, runner_data)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    conn = sqlite3.connect("mlb.db")
    cursor = conn.cursor()
    rows = cursor.execute("SELECT distinct game_pk FROM atbat;").fetchall()
    logging.info(f"Found unproccessed game pks {len(rows)} game ids")
    game_ids = [row[0] for row in rows]
    save_runners(cursor, game_ids)
    conn.close()