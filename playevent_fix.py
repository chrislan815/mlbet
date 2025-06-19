import sqlite3

import glom
from pprint import pprint

import statsapi
import logging

from utils import print_flatten_schema


import gzip
import json
import os

def load_win_probability_from_file(game_pk):
    path = f"Games/{game_pk}.json.gz"
    if not os.path.exists(path):
        print(f"{path} not found.")
        return None

    with gzip.open(path, "rt", encoding="utf-8") as f:
        return json.load(f)

""" AtBat Data Schema
(game_pk and about_atBatIndex)
>>> runners Array of Object
>>> playEvents Array of Object
"""

def flatten_hit_data(hit_data):
    hit_coords = hit_data.get('coordinates', {})
    return {
        'hitData_launchSpeed': hit_data.get('launchSpeed'),
        'hitData_launchAngle': hit_data.get('launchAngle'),
        'hitData_totalDistance': hit_data.get('totalDistance'),
        'hitData_trajectory': hit_data.get('trajectory'),
        'hitData_hardness': hit_data.get('hardness'),
        'hitData_location': hit_data.get('location'),
        'hitData_coordinates_coordX': hit_coords.get('coordX'),
        'hitData_coordinates_coordY': hit_coords.get('coordY'),
    }


def update_hit_data(cursor, game_id, index_ab_index_pe):
    sql = """
    UPDATE play_event SET
        hitData_launchSpeed = ?,
        hitData_launchAngle = ?,
        hitData_totalDistance = ?,
        hitData_trajectory = ?,
        hitData_hardness = ?,
        hitData_location = ?,
        hitData_coordinates_coordX = ?,
        hitData_coordinates_coordY = ?
    WHERE "index" = ? AND game_pk = ? AND about_atBatIndex = ?
    """

    values = [
        (
            hit_data.get("hitData_launchSpeed"),
            hit_data.get("hitData_launchAngle"),
            hit_data.get("hitData_totalDistance"),
            hit_data.get("hitData_trajectory"),
            hit_data.get("hitData_hardness"),
            hit_data.get("hitData_location"),
            hit_data.get("hitData_coordinates_coordX"),
            hit_data.get("hitData_coordinates_coordY"),
            index,
            game_id,
            atbat_index,
        )
        for (index, atbat_index, hit_data) in index_ab_index_pe
    ]
    print(len(values))
    print(values)
    cursor.executemany(sql, values)
    cursor.connection.commit()


if __name__ == '__main__':
    # logging.basicConfig(level=logging.DEBUG)  # or INFO, WARNING, ERROR, CRITICAL
    conn = sqlite3.connect("mlb.db")
    cursor = conn.cursor()

    rows = cursor.execute("""
                          SELECT game_pk
                          FROM play_event
                          GROUP BY game_pk
                          HAVING SUM(CASE WHEN hitData_trajectory IS NOT NULL THEN 1 ELSE 0 END) = 0
                          order by game_pk;
                          """).fetchall()
    index_ab_index_pe = []
    for row in rows:
        game_id = row[0]
        print(game_id)
        pbp = load_win_probability_from_file(game_id)
        for data in pbp:
            for pe in data['playEvents']:
                if hit_data := pe.get('hitData'):
                    index = pe.get('index')
                    atbat_index = data["about"]["atBatIndex"]
                    index_ab_index_pe.append(
                        (index, atbat_index, flatten_hit_data(hit_data))
                    )
            if not index_ab_index_pe:
                continue
        if len(index_ab_index_pe) > 25000:
            update_hit_data(cursor, game_id, index_ab_index_pe)
            index_ab_index_pe = []
    conn.close()
'''
hitData_launchSpeed REAL
hitData_launchAngle INTEGER
hitData_totalDistance INTEGER
hitData_trajectory TEXT
hitData_hardness TEXT
hitData_location TEXT
hitData_coordinates_coordX REAL
hitData_coordinates_coordY REAL
'''