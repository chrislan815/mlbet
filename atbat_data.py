import sqlite3
import statsapi

import logging

logging.basicConfig(level=logging.DEBUG)  # or INFO, WARNING, ERROR, CRITICAL

# conn = sqlite3.connect("mlb.db")
# conn.row_factory = sqlite3.Row  # Enable name-based access
# cursor = conn.cursor()
#
# rows = cursor.execute("""
# SELECT * FROM main.games
# WHERE status = 'Final'
# ORDER BY game_date DESC
# LIMIT 10
# """)
#
# for row in rows:
#     game_id = row['game_pk']
#     print(game_id)


game_id = 777745  # Must be an integer
pbp = statsapi.get('game_winProbability', {'gamePk': game_id})
read = set()
def print_flatten_schema(data, prefix=''):
    if isinstance(data, dict):
        for key, value in data.items():
            new_key = f"{prefix}_{key}" if prefix else key
            print_flatten_schema(value, new_key)
    elif isinstance(data, list):
        print(f">>> {prefix}")
    else:
        print(f"{prefix}")

print_flatten_schema(pbp[0])

""" AtBat Data Schema
atBatIndex
result_type
result_event
result_eventType
result_description
result_rbi
result_awayScore
result_homeScore
result_isOut
about_atBatIndex
about_halfInning
about_isTopInning
about_inning
about_startTime
about_endTime
about_isComplete
about_isScoringPlay
about_hasReview
about_hasOut
about_captivatingIndex
count_balls
count_strikes
count_outs
matchup_batter_id
matchup_batter_fullName
matchup_batter_link
matchup_batSide_code
matchup_batSide_description
matchup_pitcher_id
matchup_pitcher_fullName
matchup_pitcher_link
matchup_pitchHand_code
matchup_pitchHand_description
matchup_splits_batter
matchup_splits_pitcher
matchup_splits_menOnBase
homeTeamWinProbability float like 52.2
homeTeamWinProbability float like 52.2
homeTeamWinProbabilityAdded float like 52.2
playEndTime timestamp

>>> pitchIndex Array of Integer
>>> actionIndex Array of Integer
>>> runnerIndex Array of Integer
>>> runners Array of Object
>>> playEvents Array of Object
"""