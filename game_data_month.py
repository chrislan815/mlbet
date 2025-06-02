# https://storage.googleapis.com/gcp-mlb-hackathon-2025/datasets/mlb-statsapi-docs/StatsAPI-Spec.json
# https://github.com/JavierPalomares90/pypitchfx
# https://journal.r-project.org/archive/2014-1/sievert.pdf
import sqlite3
from calendar import monthrange

import statsapi

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
    cursor.execute('''
        INSERT OR REPLACE INTO games (
            game_pk, game_date, game_datetime, game_num, game_type, status, doubleheader, current_inning, inning_state,
            home_team_id, home_team_name, home_score, home_probable_pitcher, home_pitcher_note,
            away_team_id, away_team_name, away_score, away_probable_pitcher, away_pitcher_note,
            venue_id, venue_name,
            winning_team, winning_pitcher, losing_team, losing_pitcher, save_pitcher,
            series_status, summary, national_broadcasts
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        game['game_id'],
        game['game_date'],
        game['game_datetime'],
        game['game_num'],
        game['game_type'],
        game['status'],
        game['doubleheader'],
        game.get('current_inning'),
        game.get('inning_state'),

        game['home_id'],
        game['home_name'],
        game['home_score'],
        game.get('home_probable_pitcher'),
        game.get('home_pitcher_note'),

        game['away_id'],
        game['away_name'],
        game['away_score'],
        game.get('away_probable_pitcher'),
        game.get('away_pitcher_note'),

        game['venue_id'],
        game['venue_name'],

        game.get('winning_team'),
        game.get('winning_pitcher'),
        game.get('losing_team'),
        game.get('losing_pitcher'),
        game.get('save_pitcher'),

        game.get('series_status'),
        game.get('summary'),
        ", ".join(game.get('national_broadcasts', []))
    ))
    conn.commit()