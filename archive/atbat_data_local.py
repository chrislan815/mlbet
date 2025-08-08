import gzip
import json
import os
import sqlite3

def save_atbat(conn, game_id, pbp: dict):
    cursor = conn.cursor()
    for play in pbp:
        row_data = (
            game_id,
            play.get('result', {}).get('type'),
            play.get('result', {}).get('event'),
            play.get('result', {}).get('eventType'),
            play.get('result', {}).get('description'),
            play.get('result', {}).get('rbi'),
            play.get('result', {}).get('awayScore'),
            play.get('result', {}).get('homeScore'),
            play.get('result', {}).get('isOut'),
            play.get('about', {}).get('atBatIndex'),
            play.get('about', {}).get('halfInning'),
            play.get('about', {}).get('isTopInning'),
            play.get('about', {}).get('inning'),
            play.get('about', {}).get('startTime'),
            play.get('about', {}).get('endTime'),
            play.get('about', {}).get('isComplete'),
            play.get('about', {}).get('isScoringPlay'),
            play.get('about', {}).get('hasReview'),
            play.get('about', {}).get('hasOut'),
            play.get('about', {}).get('captivatingIndex'),
            play.get('count', {}).get('balls'),
            play.get('count', {}).get('strikes'),
            play.get('count', {}).get('outs'),
            play.get('matchup', {}).get('batter', {}).get('id'),
            play.get('matchup', {}).get('batter', {}).get('fullName'),
            play.get('matchup', {}).get('batter', {}).get('link'),
            play.get('matchup', {}).get('batSide', {}).get('code'),
            play.get('matchup', {}).get('batSide', {}).get('description'),
            play.get('matchup', {}).get('pitcher', {}).get('id'),
            play.get('matchup', {}).get('pitcher', {}).get('fullName'),
            play.get('matchup', {}).get('pitcher', {}).get('link'),
            play.get('matchup', {}).get('pitchHand', {}).get('code'),
            play.get('matchup', {}).get('pitchHand', {}).get('description'),
            play.get('matchup', {}).get('splits', {}).get('batter'),
            play.get('matchup', {}).get('splits', {}).get('pitcher'),
            play.get('matchup', {}).get('splits', {}).get('menOnBase'),
            play.get('homeTeamWinProbability'),
            play.get('homeTeamWinProbabilityAdded'),
            play.get('playEndTime'),
            str(play.get('pitchIndex', [])),
            str(play.get('actionIndex', [])),
            str(play.get('runnerIndex', []))
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

        cursor.execute(insert_query, row_data)
        cursor.connection.commit()


def save_atbat_to_db(connection, game_pk):
    path = os.path.join("games", f"{game_pk}.json.gz")
    if not os.path.exists(path):
        print(f"Missing file: {path}")
        return
    # Ensure the path exists and is a file before opening
    if not os.path.isfile(path):
        print(f"Path exists but is not a file: {path}")
        return
    print(f"Loading {path}")
    with gzip.open(path, "rt", encoding="utf-8") as f:
        data = json.load(f)
    save_atbat(connection, game_pk, data)

if __name__ == '__main__':
    connection = sqlite3.connect("mlb-v2.db")
    rows = connection.cursor().execute("""
    SELECT g.game_pk FROM game g
    """)
    for row in rows:
        game_pk = int(row[0])
        save_atbat_to_db(connection, game_pk)
    connection.close()
