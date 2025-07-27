# https://storage.googleapis.com/gcp-mlb-hackathon-2025/datasets/mlb-statsapi-docs/StatsAPI-Spec.json
# https://github.com/JavierPalomares90/pypitchfx
# https://journal.r-project.org/archive/2014-1/sievert.pdf
import logging
import sqlite3
from calendar import monthrange

import statsapi

def sanitize_sql_value(val):
    if isinstance(val, (dict, list, set, tuple)):
        return str(val)
    if isinstance(val, (int, float, str)) or val is None:
        return val
    try:
        return str(val)
    except Exception:
        return None

def save_game_to_db(conn, game):
    cursor = conn.cursor()
    try:
        values = [
            sanitize_sql_value(game.get('game_pk') or game.get('game_id')),
            sanitize_sql_value(game.get('game_date')),
            sanitize_sql_value(game.get('game_datetime')),
            sanitize_sql_value(game.get('game_num')),
            sanitize_sql_value(game.get('game_type')),
            sanitize_sql_value(game.get('status')),
            sanitize_sql_value(game.get('doubleheader')),
            sanitize_sql_value(game.get('current_inning')),
            sanitize_sql_value(game.get('inning_state')),
            sanitize_sql_value(game.get('home_team_id') or game.get('home_id')),
            sanitize_sql_value(game.get('home_team_name') or game.get('home_name')),
            sanitize_sql_value(game.get('home_score')),
            sanitize_sql_value(game.get('home_probable_pitcher')),
            sanitize_sql_value(game.get('home_pitcher_note')),
            sanitize_sql_value(game.get('away_team_id') or game.get('away_id')),
            sanitize_sql_value(game.get('away_team_name') or game.get('away_name')),
            sanitize_sql_value(game.get('away_score')),
            sanitize_sql_value(game.get('away_probable_pitcher')),
            sanitize_sql_value(game.get('away_pitcher_note')),
            sanitize_sql_value(game.get('venue_id')),
            sanitize_sql_value(game.get('venue_name')),
            sanitize_sql_value(game.get('winning_team')),
            sanitize_sql_value(game.get('winning_pitcher')),
            sanitize_sql_value(game.get('losing_team')),
            sanitize_sql_value(game.get('losing_pitcher')),
            sanitize_sql_value(game.get('save_pitcher')),
            sanitize_sql_value(game.get('series_status')),
            sanitize_sql_value(game.get('summary')),
            sanitize_sql_value(game.get('national_broadcasts'))
        ]
        cursor.execute('''
            INSERT OR REPLACE INTO game (
                game_pk, game_date, game_datetime, game_num, game_type, status, doubleheader, current_inning, inning_state,
                home_team_id, home_team_name, home_score, home_probable_pitcher, home_pitcher_note,
                away_team_id, away_team_name, away_score, away_probable_pitcher, away_pitcher_note,
                venue_id, venue_name,
                winning_team, winning_pitcher, losing_team, losing_pitcher, save_pitcher,
                series_status, summary, national_broadcasts
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', values)
        conn.commit()
        logging.info(f"Saved game {values[0]} to database.")
    except Exception as e:
        logging.error(f"Error saving game {game.get('game_pk') or game.get('game_id')}: {e}")


if __name__ == '__main__':
    conn = sqlite3.connect("mlb.db")
    cursor = conn.cursor()


    all_games = []
    for year in range(2014, 2022):
        for month in range(1, 13):
            start_date = f"{year}-{month:02d}-01"
            end_day = monthrange(year, month)[1]
            end_date = f"{year}-{month:02d}-{end_day:02d}"
            games = statsapi.schedule(start_date=start_date, end_date=end_date)
            all_games.extend(games)

    print(len(all_games))

    for game in all_games:
        save_game_to_db(conn, game)
