import sqlite3

import glom
from pprint import pprint

import statsapi
import logging

from utils import print_flatten_schema


import gzip
import json
import os

from dataclasses import dataclass

@dataclass
class RowData:
    """Class for keeping track of an item in inventory."""
    game_id: str
    atbat_index: int
    pitch_number: int
    hit_data: dict

def load_win_probability_from_file(game_pk):
    path = f"Games/{game_pk}.json.gz"
    if not os.path.exists(path):
        print(f"{path} not found.")
        return None

    with gzip.open(path, "rt", encoding="utf-8") as f:
        data = json.load(f)
        return data

""" AtBat Data Schema
(game_pk and about_atBatIndex)
>>> runners Array of Object
>>> playEvents Array of Object
"""

def flatten_hit_data(_hit_data):
    hit_coords = _hit_data.get('coordinates', {})
    return {
        'hitData_launchSpeed': _hit_data.get('launchSpeed'),
        'hitData_launchAngle': _hit_data.get('launchAngle'),
        'hitData_totalDistance': _hit_data.get('totalDistance'),
        'hitData_trajectory': _hit_data.get('trajectory'),
        'hitData_hardness': _hit_data.get('hardness'),
        'hitData_location': _hit_data.get('location'),
        'hitData_coordinates_coordX': hit_coords.get('coordX'),
        'hitData_coordinates_coordY': hit_coords.get('coordY'),
    }


def update_hit_data(_cursor, _data_rows: list[RowData]):
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
    WHERE game_pk = ? AND about_atBatIndex = ? AND pitchNumber = ?
    """

    values = [
        (
            row_data.hit_data.get("hitData_launchSpeed"),
            row_data.hit_data.get("hitData_launchAngle"),
            row_data.hit_data.get("hitData_totalDistance"),
            row_data.hit_data.get("hitData_trajectory"),
            row_data.hit_data.get("hitData_hardness"),
            row_data.hit_data.get("hitData_location"),
            row_data.hit_data.get("hitData_coordinates_coordX"),
            row_data.hit_data.get("hitData_coordinates_coordY"),
            row_data.game_id,
            row_data.atbat_index,
            row_data.pitch_number,
        )
        for row_data in _data_rows
    ]
    _cursor.executemany(sql, values)
    _cursor.connection.commit()


if __name__ == '__main__':
    conn = sqlite3.connect("mlb-v2.db")
    cursor = conn.cursor()

    rows = cursor.execute("""
                          SELECT game_pk
                          FROM play_event
                          GROUP BY game_pk
                          HAVING SUM(CASE WHEN hitData_trajectory IS NOT NULL THEN 1 ELSE 0 END) = 0
                          order by game_pk desc 
                          limit 10000;
                          """).fetchall()
    data_rows: list[RowData] = []
    for row in rows:
        game_id = row[0]
        print(game_id)
        pbp = load_win_probability_from_file(game_id)
        for atbat in pbp:
            for pe in atbat['playEvents']:
                if hit_data := pe.get('hitData'):
                    pitch_number = pe.get('pitchNumber')
                    atbat_index = atbat["about"]["atBatIndex"]
                    data_rows.append(
                        RowData(
                            game_id=game_id,
                            atbat_index=atbat_index,
                            pitch_number=pitch_number,
                            hit_data=flatten_hit_data(hit_data)
                        )
                    )
        # for data_row in data_rows:
        #     print(data_row.game_id, data_row.atbat_index, data_row.pitch_number, data_row.hit_data)
        if len(data_rows) > 50000:
            print(f"update {len(data_rows)} rows")
            update_hit_data(cursor, data_rows)
            data_rows = []
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