import sqlite3

# Connect to or create the database
conn = sqlite3.connect("mlb.db")
cursor = conn.cursor()

cursor.execute(
    """
    drop table if exists game;
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


cursor.execute('''
CREATE TABLE IF NOT EXISTS players (
    player_id INTEGER PRIMARY KEY,
    full_name TEXT,
    first_name TEXT,
    last_name TEXT,
    use_name TEXT,
    middle_name TEXT,
    boxscore_name TEXT,
    primary_number TEXT,
    birth_date TEXT,
    current_age INTEGER,
    birth_city TEXT,
    birth_state TEXT,
    birth_country TEXT,
    height TEXT,
    weight INTEGER,
    active BOOLEAN,
    is_verified BOOLEAN,
    primary_position TEXT,
    bat_side TEXT,
    pitch_hand TEXT,
    mlb_debut TEXT
)
''')

conn.commit()
conn.close()
