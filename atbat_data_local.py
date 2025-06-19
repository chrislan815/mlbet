import gzip
import json
import os
import sqlite3
import statsapi

import logging

logging.basicConfig(level=logging.DEBUG)  # or INFO, WARNING, ERROR, CRITICAL

# game_id = 777745  # example game_pk
# pbp = statsapi.get('game_winProbability', {'gamePk': game_id})



def save_atbat(conn, game_id, pbp: dict):
    cursor = conn.cursor()
    for data in pbp:
        abi = data.get('about', {}).get('atBatIndex')
        print(f'saving {game_id}: {abi}')
        row = (
            game_id,
            data.get('result', {}).get('type'),
            data.get('result', {}).get('event'),
            data.get('result', {}).get('eventType'),
            data.get('result', {}).get('description'),
            data.get('result', {}).get('rbi'),
            data.get('result', {}).get('awayScore'),
            data.get('result', {}).get('homeScore'),
            data.get('result', {}).get('isOut'),
            data.get('about', {}).get('atBatIndex'),
            data.get('about', {}).get('halfInning'),
            data.get('about', {}).get('isTopInning'),
            data.get('about', {}).get('inning'),
            data.get('about', {}).get('startTime'),
            data.get('about', {}).get('endTime'),
            data.get('about', {}).get('isComplete'),
            data.get('about', {}).get('isScoringPlay'),
            data.get('about', {}).get('hasReview'),
            data.get('about', {}).get('hasOut'),
            data.get('about', {}).get('captivatingIndex'),
            data.get('count', {}).get('balls'),
            data.get('count', {}).get('strikes'),
            data.get('count', {}).get('outs'),
            data.get('matchup', {}).get('batter', {}).get('id'),
            data.get('matchup', {}).get('batter', {}).get('fullName'),
            data.get('matchup', {}).get('batter', {}).get('link'),
            data.get('matchup', {}).get('batSide', {}).get('code'),
            data.get('matchup', {}).get('batSide', {}).get('description'),
            data.get('matchup', {}).get('pitcher', {}).get('id'),
            data.get('matchup', {}).get('pitcher', {}).get('fullName'),
            data.get('matchup', {}).get('pitcher', {}).get('link'),
            data.get('matchup', {}).get('pitchHand', {}).get('code'),
            data.get('matchup', {}).get('pitchHand', {}).get('description'),
            data.get('matchup', {}).get('splits', {}).get('batter'),
            data.get('matchup', {}).get('splits', {}).get('pitcher'),
            data.get('matchup', {}).get('splits', {}).get('menOnBase'),
            data.get('homeTeamWinProbability'),
            data.get('homeTeamWinProbabilityAdded'),
            data.get('playEndTime'),
            str(data.get('pitchIndex', [])),
            str(data.get('actionIndex', [])),
            str(data.get('runnerIndex', []))
        )

        insert_query = """
        INSERT OR IGNORE INTO atbat (
            game_pk,
            result_type, result_event, result_eventType, result_description,
            result_rbi, result_awayScore, result_homeScore, result_isOut,
            about_atBatIndex, about_halfInning, about_isTopInning, about_inning,
            about_startTime, about_endTime, about_isComplete, about_isScoringPlay,
            about_hasReview, about_hasOut, about_captivatingIndex,
            count_balls, count_strikes, count_outs,
            matchup_batter_id, matchup_batter_fullName, matchup_batter_link,
            matchup_batSide_code, matchup_batSide_description,
            matchup_pitcher_id, matchup_pitcher_fullName, matchup_pitcher_link,
            matchup_pitchHand_code, matchup_pitchHand_description,
            matchup_splits_batter, matchup_splits_pitcher, matchup_splits_menOnBase,
            homeTeamWinProbability, homeTeamWinProbabilityAdded,
            playEndTime,
            pitchIndex, actionIndex, runnerIndex
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        cursor.execute(insert_query, row)
        cursor.connection.commit()


""" AtBat Data Schema
>>> runners Array of Object => fkey atbat_id + populating the runnerIndex int
>>> playEvents Array of Object => fkey atbat_id
"""
if __name__ == '__main__':
    connection = sqlite3.connect("mlb.db")

    rows = connection.cursor().execute("""
    SELECT g.game_pk FROM game g
    """)
    # currently 6/6 2:44 2374989
    for row in rows:
        game_pk = int(row[0])
        if game_pk < 719408:
            continue
        path = os.path.join("games", f"{game_pk}.json.gz")

        if not os.path.exists(path):
            print(f"Missing file: {path}")
            continue

        print(f"Loading {path}")
        with gzip.open(path, "rt", encoding="utf-8") as f:
            data = json.load(f)  # This is your game data dictionary

        # Example: print game status

        # You can call your custom logic here
        save_atbat(connection, game_pk, data)

    connection.commit()
    connection.close()
