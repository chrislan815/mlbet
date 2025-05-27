import sqlite3

# Connect to or create the database
conn = sqlite3.connect("mlb.db")
cursor = conn.cursor()

cursor.execute(
    """
    drop table if exists games;
    """
)

# Create tables if they don't exist
cursor.execute('''
CREATE TABLE IF NOT EXISTS games (
    game_pk INTEGER PRIMARY KEY,
    game_date TEXT,
    game_datetime TEXT,
    game_num INTEGER,
    game_type TEXT,
    status TEXT,
    doubleheader TEXT,
    current_inning INTEGER,
    inning_state TEXT,

    home_team_id INTEGER,
    home_team_name TEXT,
    home_score INTEGER,
    home_probable_pitcher TEXT,
    home_pitcher_note TEXT,

    away_team_id INTEGER,
    away_team_name TEXT,
    away_score INTEGER,
    away_probable_pitcher TEXT,
    away_pitcher_note TEXT,

    venue_id INTEGER,
    venue_name TEXT,

    winning_team TEXT,
    winning_pitcher TEXT,
    losing_team TEXT,
    losing_pitcher TEXT,
    save_pitcher TEXT,

    series_status TEXT,
    summary TEXT,
    national_broadcasts TEXT  -- stored as a comma-separated string
)
''')

conn.commit()
conn.close()
