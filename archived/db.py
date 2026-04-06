import sqlite3
import sys
from pathlib import Path

DB_PATH = Path(__file__).parent / "mlb.db"

DROP_ORDER = [
    "game_weather",
    "batted_balls",
    "plate_appearances",
    "pitches",
    "runners",
    "lineups",
    "games",
    "venues",
    "players",
    "ingestion_log",
]


def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def init_schema(drop_existing=False):
    conn = get_connection()
    try:
        cur = conn.cursor()

        if drop_existing:
            for table in DROP_ORDER:
                cur.execute(f"DROP TABLE IF EXISTS {table}")

        # ── players ──────────────────────────────────────────────────
        cur.execute("""
        CREATE TABLE IF NOT EXISTS players (
            player_id INTEGER PRIMARY KEY,  -- MLB person ID
            full_name TEXT NOT NULL,
            first_name TEXT,
            last_name TEXT,
            primary_position TEXT,
            bat_side TEXT,           -- L/R/S
            pitch_hand TEXT,         -- L/R
            birth_date TEXT,
            height TEXT,
            weight INTEGER,
            active BOOLEAN
        )
        """)

        # ── games ────────────────────────────────────────────────────
        cur.execute("""
        CREATE TABLE IF NOT EXISTS games (
            game_pk INTEGER PRIMARY KEY,
            game_date TEXT NOT NULL,
            game_year INTEGER NOT NULL,
            game_type TEXT NOT NULL,      -- R, S, P, etc.
            home_team TEXT NOT NULL,
            away_team TEXT NOT NULL,
            home_score INTEGER,
            away_score INTEGER,
            venue_id INTEGER,
            venue_name TEXT
        )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_games_date ON games(game_date)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_games_year ON games(game_year)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_games_teams ON games(home_team, away_team)")

        # ── pitches (core fact table) ────────────────────────────────
        cur.execute("""
        CREATE TABLE IF NOT EXISTS pitches (
            game_pk INTEGER NOT NULL REFERENCES games(game_pk),
            at_bat_number INTEGER NOT NULL,
            pitch_number INTEGER NOT NULL,
            pitcher INTEGER NOT NULL REFERENCES players(player_id),
            batter INTEGER NOT NULL REFERENCES players(player_id),

            -- pitch classification
            pitch_type TEXT,           -- FF, SL, CU, CH, SI, FC, ST, etc.
            pitch_name TEXT,           -- Four-Seam Fastball, Slider, etc.

            -- velocity
            start_speed REAL,
            end_speed REAL,

            -- spin
            spin_rate INTEGER,
            spin_direction INTEGER,

            -- movement (inches)
            break_angle REAL,
            break_length REAL,
            break_vertical REAL,
            break_vertical_induced REAL,
            break_horizontal REAL,

            -- plate location
            plate_x REAL,
            plate_z REAL,
            zone INTEGER,
            sz_top REAL,
            sz_bot REAL,

            -- release point
            release_x REAL,
            release_y REAL,
            release_z REAL,

            -- trajectory vectors
            vx0 REAL,
            vy0 REAL,
            vz0 REAL,
            ax REAL,
            ay REAL,
            az REAL,

            -- additional pitch metrics
            pfx_x REAL,
            pfx_z REAL,
            extension REAL,
            plate_time REAL,
            type_confidence REAL,

            -- outcome
            call TEXT,                 -- Ball, Strike, Foul, In Play, etc.
            call_code TEXT,            -- B, S, F, X, etc.
            description TEXT,
            is_in_play BOOLEAN,
            is_strike BOOLEAN,
            is_ball BOOLEAN,

            -- defensive fielders
            fielder_2 INTEGER,           -- catcher
            fielder_3 INTEGER,           -- 1B
            fielder_4 INTEGER,           -- 2B
            fielder_5 INTEGER,           -- 3B
            fielder_6 INTEGER,           -- SS
            fielder_7 INTEGER,           -- LF
            fielder_8 INTEGER,           -- CF
            fielder_9 INTEGER,           -- RF

            -- game state at time of pitch
            inning INTEGER NOT NULL,
            inning_half TEXT NOT NULL,   -- top/bottom
            balls INTEGER NOT NULL,
            strikes INTEGER NOT NULL,
            outs INTEGER NOT NULL,

            PRIMARY KEY (game_pk, at_bat_number, pitch_number)
        )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_pitches_pitcher ON pitches(pitcher)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_pitches_batter ON pitches(batter)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_pitches_game ON pitches(game_pk)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_pitches_type ON pitches(pitch_type)")

        # ── plate_appearances ────────────────────────────────────────
        cur.execute("""
        CREATE TABLE IF NOT EXISTS plate_appearances (
            game_pk INTEGER NOT NULL REFERENCES games(game_pk),
            at_bat_number INTEGER NOT NULL,
            pitcher INTEGER NOT NULL REFERENCES players(player_id),
            batter INTEGER NOT NULL REFERENCES players(player_id),
            event TEXT,                -- Strikeout, Single, Home Run, Walk, etc.
            event_type TEXT,           -- strikeout, single, home_run, walk, etc.
            description TEXT,
            rbi INTEGER DEFAULT 0,
            is_out BOOLEAN,
            inning INTEGER NOT NULL,
            inning_half TEXT NOT NULL,
            bat_side TEXT,
            pitch_hand TEXT,

            PRIMARY KEY (game_pk, at_bat_number)
        )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_pa_pitcher ON plate_appearances(pitcher)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_pa_batter ON plate_appearances(batter)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_pa_event ON plate_appearances(event_type)")

        # ── batted_balls ─────────────────────────────────────────────
        cur.execute("""
        CREATE TABLE IF NOT EXISTS batted_balls (
            game_pk INTEGER NOT NULL,
            at_bat_number INTEGER NOT NULL,
            launch_speed REAL,
            launch_angle REAL,
            total_distance REAL,
            trajectory TEXT,           -- ground_ball, fly_ball, line_drive, popup
            hardness TEXT,             -- soft, medium, hard
            hit_location TEXT,
            coord_x REAL,
            coord_y REAL,

            PRIMARY KEY (game_pk, at_bat_number),
            FOREIGN KEY (game_pk, at_bat_number)
                REFERENCES plate_appearances(game_pk, at_bat_number)
        )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_bb_trajectory ON batted_balls(trajectory)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_bb_launch ON batted_balls(launch_speed, launch_angle)")

        # ── venues ──────────────────────────────────────────────────
        cur.execute("""
        CREATE TABLE IF NOT EXISTS venues (
            venue_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            city TEXT,
            state TEXT,
            country TEXT,
            latitude REAL,
            longitude REAL,
            elevation INTEGER,           -- feet
            timezone TEXT,               -- IANA timezone (e.g. America/Chicago)
            roof_type TEXT,              -- Open, Retractable, Dome
            turf_type TEXT,              -- Grass, Artificial
            capacity INTEGER,
            left_line INTEGER,
            left_center INTEGER,
            center INTEGER,
            right_center INTEGER,
            right_line INTEGER
        )
        """)

        # ── game_weather ────────────────────────────────────────────
        cur.execute("""
        CREATE TABLE IF NOT EXISTS game_weather (
            game_pk INTEGER PRIMARY KEY REFERENCES games(game_pk),
            temperature_f REAL,
            humidity INTEGER,            -- percent
            wind_speed_mph REAL,
            wind_direction INTEGER,      -- degrees (0=N, 90=E, 180=S, 270=W)
            wind_gusts_mph REAL,
            precipitation_mm REAL,
            pressure_hpa REAL,           -- sea level pressure (affects ball carry)
            cloud_cover INTEGER,         -- percent
            weather_code INTEGER         -- WMO code
        )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_gw_game ON game_weather(game_pk)")

        # ── runners ────────────────────────────────────────────────
        cur.execute("""
        CREATE TABLE IF NOT EXISTS runners (
            game_pk INTEGER NOT NULL REFERENCES games(game_pk),
            at_bat_number INTEGER NOT NULL,
            runner_id INTEGER NOT NULL REFERENCES players(player_id),
            origin_base TEXT,         -- NULL, 1B, 2B, 3B
            start_base TEXT,          -- NULL, 1B, 2B, 3B
            end_base TEXT,            -- NULL, 1B, 2B, 3B, score
            out_base TEXT,
            is_out BOOLEAN,
            out_number INTEGER,
            event TEXT,
            event_type TEXT,
            movement_reason TEXT,
            is_scoring_event BOOLEAN,
            rbi BOOLEAN,
            earned BOOLEAN,
            play_index INTEGER,
            PRIMARY KEY (game_pk, at_bat_number, runner_id, play_index)
        )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_runners_game ON runners(game_pk)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_runners_runner ON runners(runner_id)")

        # ── lineups ────────────────────────────────────────────────
        cur.execute("""
        CREATE TABLE IF NOT EXISTS lineups (
            game_pk INTEGER NOT NULL REFERENCES games(game_pk),
            team_type TEXT NOT NULL,          -- 'home' or 'away'
            batting_order INTEGER NOT NULL,   -- 100, 200, 300... 900
            player_id INTEGER NOT NULL REFERENCES players(player_id),
            position TEXT,                    -- LF, SS, C, DH, etc.
            PRIMARY KEY (game_pk, team_type, batting_order)
        )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_lineups_player ON lineups(player_id)")

        # ── ingestion_log ────────────────────────────────────────────
        cur.execute("""
        CREATE TABLE IF NOT EXISTS ingestion_log (
            game_pk INTEGER PRIMARY KEY,
            status TEXT NOT NULL,         -- 'complete' or 'failed'
            pitches_count INTEGER,
            ingested_at TEXT NOT NULL,    -- ISO timestamp
            error_msg TEXT
        )
        """)

        conn.commit()
    finally:
        conn.close()


if __name__ == "__main__":
    drop = "--drop" in sys.argv
    if drop:
        confirm = input("This will DROP all tables. Type 'yes' to confirm: ")
        if confirm != "yes":
            print("Aborted.")
            sys.exit(1)
    init_schema(drop_existing=drop)
    print(f"Schema initialized (drop_existing={drop}).")
