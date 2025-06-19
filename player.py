import gzip
import json
import os
import sqlite3
from itertools import islice

import glom
from pprint import pprint

import statsapi
import logging

from utils import print_flatten_schema


""" runners Data Schema
(game_pk and about_atBatIndex + artificial runner_index)
"""

def flatten_player(data):
    return {
        "id": data["id"],
        "fullName": data.get("fullName"),
        "link": data.get("link"),
        "firstName": data.get("firstName"),
        "lastName": data.get("lastName"),
        "birthDate": data.get("birthDate"),
        "currentAge": data.get("currentAge"),
        "birthCity": data.get("birthCity"),
        "birthStateProvince": data.get("birthStateProvince"),
        "birthCountry": data.get("birthCountry"),
        "height": data.get("height"),
        "weight": data.get("weight"),
        "active": data.get("active"),
        "useName": data.get("useName"),
        "useLastName": data.get("useLastName"),
        "middleName": data.get("middleName"),
        "boxscoreName": data.get("boxscoreName"),
        "gender": data.get("gender"),
        "isPlayer": data.get("isPlayer"),
        "isVerified": data.get("isVerified"),
        "draftYear": data.get("draftYear"),
        "batSide_code": data.get("batSide", {}).get("code"),
        "batSide_description": data.get("batSide", {}).get("description"),
        "pitchHand_code": data.get("pitchHand", {}).get("code"),
        "pitchHand_description": data.get("pitchHand", {}).get("description"),
        "nameFirstLast": data.get("nameFirstLast"),
        "nameSlug": data.get("nameSlug"),
        "firstLastName": data.get("firstLastName"),
        "lastFirstName": data.get("lastFirstName"),
        "lastInitName": data.get("lastInitName"),
        "initLastName": data.get("initLastName"),
        "fullFMLName": data.get("fullFMLName"),
        "fullLFMName": data.get("fullLFMName"),
        "strikeZoneTop": data.get("strikeZoneTop"),
        "strikeZoneBottom": data.get("strikeZoneBottom"),
        "primaryPosition_code": data.get("primaryPosition", {}).get("code"),
        "primaryPosition_name": data.get("primaryPosition", {}).get("name"),
        "primaryPosition_type": data.get("primaryPosition", {}).get("type"),
        "primaryPosition_abbreviation": data.get("primaryPosition", {}).get("abbreviation"),
    }


def insert_player(cursor, data):
    flat = flatten_player(data)
    cursor.execute("""
        INSERT OR REPLACE INTO player (
            id, fullName, link, firstName, lastName, birthDate, currentAge,
            birthCity, birthStateProvince, birthCountry, height, weight,
            active, useName, useLastName, middleName, boxscoreName, gender,
            isPlayer, isVerified, draftYear,
            batSide_code, batSide_description,
            pitchHand_code, pitchHand_description,
            nameFirstLast, nameSlug, firstLastName, lastFirstName,
            lastInitName, initLastName,
            fullFMLName, fullLFMName, strikeZoneTop, strikeZoneBottom,
            primaryPosition_code, primaryPosition_name,
            primaryPosition_type, primaryPosition_abbreviation
        ) VALUES (
            :id, :fullName, :link, :firstName, :lastName, :birthDate, :currentAge,
            :birthCity, :birthStateProvince, :birthCountry, :height, :weight,
            :active, :useName, :useLastName, :middleName, :boxscoreName, :gender,
            :isPlayer, :isVerified, :draftYear,
            :batSide_code, :batSide_description,
            :pitchHand_code, :pitchHand_description,
            :nameFirstLast, :nameSlug, :firstLastName, :lastFirstName,
            :lastInitName, :initLastName,
            :fullFMLName, :fullLFMName, :strikeZoneTop, :strikeZoneBottom,
            :primaryPosition_code, :primaryPosition_name,
            :primaryPosition_type, :primaryPosition_abbreviation
        );
    """, flat)
    cursor.connection.commit()

def batched(iterable, n):
    """Yield successive n-sized chunks from iterable."""
    it = iter(iterable)
    while True:
        batch = list(islice(it, n))
        if not batch:
            break
        yield batch

if __name__ == '__main__':
    conn = sqlite3.connect("mlb.db")
    cursor = conn.cursor()

    discovered_player_ids = {
        row[0] for row in cursor.execute("SELECT DISTINCT runner_id FROM runner")
    }
    print(f"Found {len(discovered_player_ids)} total player ids from runner object")

    rows = cursor.execute("SELECT DISTINCT id FROM player").fetchall()
    print(f"Found total processed {len(rows)} player ids")

    # Filter to only missing player IDs
    processed_ids = {row[0] for row in rows}
    missing_ids = list(discovered_player_ids - processed_ids)
    print(f"{len(missing_ids)} players still need to be fetched")

    for batch in batched(missing_ids, 20):
        ids_str = ",".join(map(str, batch))
        print(ids_str)
        response = statsapi.get("people", {"personIds": ids_str})
        for player_data in response.get("people", []):
            insert_player(cursor, player_data)  # assumes insert_player is already defined

    # conn.commit()
    # conn.close()