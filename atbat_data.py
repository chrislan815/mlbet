import sqlite3
import statsapi

import logging

logging.basicConfig(level=logging.DEBUG)  # or INFO, WARNING, ERROR, CRITICAL

# game_id = 777745  # example game_pk
# pbp = statsapi.get('game_winProbability', {'gamePk': game_id})



def save_atbat(conn, game_id):
    cursor = conn.cursor()
    pbp = statsapi.get('game_winProbability', {'gamePk': game_id})
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


""" AtBat Data Schema
>>> runners Array of Object => fkey atbat_id + populating the runnerIndex int
>>> playEvents Array of Object => fkey atbat_id
"""

connection = sqlite3.connect("mlb.db")

rows = connection.cursor().execute("""
    SELECT *
    FROM game
    WHERE status == 'Final'
    ORDER BY game_pk DESC
""")
for row in rows:
    save_atbat(connection, row[0])

connection.commit()
connection.close()
