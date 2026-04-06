"""Create derived analytics views and the zscore lookup table."""

from scipy.stats import norm

from db import get_connection, init_schema

VIEW_NAMES = [
    "v_pitcher_profile",
    "v_batter_ppab",
    "v_batter_performance_atbat",
    "v_batter_performance_pe_pt",
    "v_batter_performance_power",
    "v_batter_profile",
]

SQL_PITCHER_PROFILE = """
CREATE VIEW IF NOT EXISTS v_pitcher_profile AS
SELECT
    p.pitcher                                                                   AS pitcher_id,
    pa.pitch_hand,
    p.pitch_type,
    p.pitch_name,
    COUNT(*)                                                                    AS total_pitches,
    ROUND(1.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY p.pitcher), 4)      AS frequency,
    ROUND(AVG(p.start_speed), 1)                                                AS avg_start_speed,
    ROUND(AVG(p.end_speed), 1)                                                  AS avg_end_speed,
    ROUND(AVG(p.spin_rate), 0)                                                  AS avg_spin_rate,
    ROUND(AVG(p.spin_direction), 0)                                             AS avg_spin_direction,
    ROUND(AVG(p.break_angle), 1)                                                AS avg_break_angle,
    ROUND(AVG(p.break_length), 1)                                               AS avg_break_length,
    ROUND(AVG(p.break_vertical), 1)                                             AS avg_break_vertical,
    ROUND(AVG(p.break_horizontal), 1)                                           AS avg_break_horizontal
FROM pitches p
JOIN plate_appearances pa
    ON p.game_pk = pa.game_pk AND p.at_bat_number = pa.at_bat_number
WHERE p.pitch_type IS NOT NULL
GROUP BY p.pitcher, pa.pitch_hand, p.pitch_type, p.pitch_name
"""

SQL_BATTER_PPAB = """
CREATE VIEW IF NOT EXISTS v_batter_ppab AS
SELECT
    pa.batter                                                                                       AS batter_id,
    pa.pitch_hand,
    COUNT(DISTINCT (pa.game_pk || '-' || pa.at_bat_number))                                         AS total_atbats,
    COUNT(p.pitch_number)                                                                           AS total_pitches,
    ROUND(1.0 * COUNT(p.pitch_number)
          / COUNT(DISTINCT (pa.game_pk || '-' || pa.at_bat_number)), 2)                             AS ppab
FROM plate_appearances pa
JOIN pitches p
    ON pa.game_pk = p.game_pk AND pa.at_bat_number = p.at_bat_number
GROUP BY pa.batter, pa.pitch_hand
"""

SQL_BATTER_PERFORMANCE_ATBAT = """
CREATE VIEW IF NOT EXISTS v_batter_performance_atbat AS
SELECT
    pa.batter                                                                       AS batter_id,
    pa.pitch_hand,
    pa.event                                                                        AS result_event,
    ROUND(1.0 * COUNT(*)
          / SUM(COUNT(*)) OVER (PARTITION BY pa.batter, pa.pitch_hand), 4)          AS percent,
    SUM(COUNT(*)) OVER (PARTITION BY pa.batter, pa.pitch_hand)                      AS total,
    COUNT(*)                                                                        AS subtotal
FROM plate_appearances pa
WHERE pa.event IS NOT NULL
GROUP BY pa.batter, pa.pitch_hand, pa.event
"""

SQL_BATTER_PERFORMANCE_PE_PT = """
CREATE VIEW IF NOT EXISTS v_batter_performance_pe_pt AS
SELECT
    p.batter                                                                                    AS batter_id,
    pa.pitch_hand,
    p.pitch_type,
    ROUND(1.0 * COUNT(*)
          / SUM(COUNT(*)) OVER (PARTITION BY p.batter, pa.pitch_hand, p.pitch_type), 4)         AS percent,
    SUM(COUNT(*)) OVER (PARTITION BY p.batter, pa.pitch_hand, p.pitch_type)                     AS total,
    COUNT(*)                                                                                    AS subtotal,
    p.call_code,
    p.call                                                                                      AS call_description
FROM pitches p
JOIN plate_appearances pa
    ON p.game_pk = pa.game_pk AND p.at_bat_number = pa.at_bat_number
WHERE p.pitch_type IS NOT NULL
GROUP BY p.batter, pa.pitch_hand, p.pitch_type, p.call_code, p.call
"""

SQL_BATTER_PERFORMANCE_POWER = """
CREATE VIEW IF NOT EXISTS v_batter_performance_power AS
SELECT
    pa.batter                       AS batter_id,
    ROUND(AVG(bb.launch_speed), 1)  AS avg_launch_speed,
    COUNT(*)                        AS total_batted_balls
FROM batted_balls bb
JOIN plate_appearances pa USING (game_pk, at_bat_number)
WHERE bb.launch_speed IS NOT NULL
GROUP BY pa.batter
"""

SQL_BATTER_PROFILE = """
CREATE VIEW IF NOT EXISTS v_batter_profile AS
SELECT
    pa.batter       AS batter_id,
    pa.pitch_hand,
    ROUND(1.0 * SUM(CASE WHEN pa.event_type = 'strikeout' THEN 1 ELSE 0 END) / COUNT(*), 4)    AS strikeout_rate,
    ROUND(1.0 * SUM(CASE WHEN pa.event_type = 'walk' THEN 1 ELSE 0 END) / COUNT(*), 4)         AS walk_rate,
    COUNT(*)                                                                                    AS total_pa
FROM plate_appearances pa
WHERE pa.event IS NOT NULL
GROUP BY pa.batter, pa.pitch_hand
"""

ALL_VIEW_SQL = [
    SQL_PITCHER_PROFILE,
    SQL_BATTER_PPAB,
    SQL_BATTER_PERFORMANCE_ATBAT,
    SQL_BATTER_PERFORMANCE_PE_PT,
    SQL_BATTER_PERFORMANCE_POWER,
    SQL_BATTER_PROFILE,
]


def create_views(conn):
    """Drop and recreate all analytics views."""
    cur = conn.cursor()
    for name in VIEW_NAMES:
        cur.execute(f"DROP VIEW IF EXISTS {name}")
    for sql in ALL_VIEW_SQL:
        cur.execute(sql)
    conn.commit()


def create_zscore_table(conn):
    """Create the zscore-to-percentile lookup table."""
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS zscore")
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS zscore (
            zscore      REAL PRIMARY KEY,
            percentile  REAL NOT NULL
        )
        """
    )

    rows = []
    z = -3.00
    while z <= 3.005:
        percentile = round(norm.cdf(z), 4)
        rows.append((round(z, 2), percentile))
        z += 0.01

    cur.executemany("INSERT INTO zscore (zscore, percentile) VALUES (?, ?)", rows)
    conn.commit()


def main():
    init_schema()
    conn = get_connection()
    try:
        create_views(conn)
        create_zscore_table(conn)
        print("Views and zscore table created.")

        all_objects = VIEW_NAMES + ["zscore"]
        for name in all_objects:
            count = conn.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
            print(f"  {name}: {count:,} rows")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
