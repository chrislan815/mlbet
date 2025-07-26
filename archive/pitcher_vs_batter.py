import sqlite3
from typing import Literal

TeamType = Literal["away", "home"]


def get_pitcher_id_and_pitchhand_code(
        connection,
        game_pk: int,
        team_type: TeamType
) -> tuple[int, Literal["R", "L"]] | None:
    halfinning = "top" if team_type == "away" else "bottom"

    cursor = connection.cursor()
    cursor.execute("""
                   SELECT DISTINCT ab.matchup_pitcher_id, pp.defense_pitcher_pitchhand_code
                   FROM atbat ab
                            JOIN pitcher_profile pp
                                 ON ab.matchup_pitcher_id = pp.defense_pitcher_id
                   WHERE ab.game_pk = ?
                     AND ab.about_inning = 2
                     AND ab.about_halfinning = ?
                   LIMIT 1
                   """, (game_pk, halfinning))

    result = cursor.fetchone()
    return (result[0], result[1]) if result else None


def get_batter_ids(connection, game_pk, team_type: TeamType) -> list[int]:
    cursor = connection.cursor()
    sql = f"""
        SELECT player_id
        FROM lineup
        WHERE game_pk = {game_pk}
          AND team_type = '{team_type}'
        ORDER BY batting_order ASC;
    """
    cursor.execute(sql)
    return [_row[0] for _row in cursor.fetchall()]


def create_table_if_not_exist(connection):
    cursor = connection.cursor()
    create_table_sql = '''
                       CREATE TABLE IF NOT EXISTS pitcher_batter_result
                       (
                           game_pk      INTEGER,
                           pitcher_id   INTEGER,
                           batter_id    INTEGER,
                           result_event TEXT,
                           num_result   INTEGER,
                           PRIMARY KEY (game_pk, pitcher_id, batter_id, result_event)
                       ); \
                       '''
    cursor.execute(create_table_sql)


def fetch_similar_pitch_results(
        conn: sqlite3.Connection,
        batter_id: int,
        pitcher_hand: Literal["R", "L"],
        percentile_window: float,
        matchup_pitcher_id: int
):
    query = f"""
    WITH batter_hand AS (
        SELECT matchup_batside_code
        FROM atbat
        WHERE matchup_batter_id = {batter_id}
        LIMIT 1
    ),
    batter_strikeout_pct AS (
        SELECT strikeout_percentile
        FROM batter_profile
        WHERE offense_batter_id = {batter_id}
          AND defense_pitcher_pitchhand_code = '{pitcher_hand}'
        LIMIT 1
    ),
    batter_launch_pct AS (
        SELECT hitdata_launchspeed_percentile
        FROM batter_profile
        WHERE offense_batter_id = {batter_id}
          AND defense_pitcher_pitchhand_code = '{pitcher_hand}'
        LIMIT 1
    ),
    similar_batters AS (
        SELECT offense_batter_id
        FROM batter_profile
        WHERE defense_pitcher_pitchhand_code = '{pitcher_hand}'
          AND strikeout_percentile BETWEEN 
              (SELECT strikeout_percentile FROM batter_strikeout_pct) - {percentile_window}
              AND (SELECT strikeout_percentile FROM batter_strikeout_pct) + {percentile_window}
          AND hitdata_launchspeed_percentile BETWEEN 
              (SELECT hitdata_launchspeed_percentile FROM batter_launch_pct) - {percentile_window}
              AND (SELECT hitdata_launchspeed_percentile FROM batter_launch_pct) + {percentile_window}
    ),
    matched_atbats AS (
        SELECT game_pk, about_atbatindex
        FROM atbat
        WHERE matchup_pitcher_id = {matchup_pitcher_id}
          AND matchup_batside_code = (SELECT matchup_batside_code FROM batter_hand)
          AND matchup_batter_id IN (SELECT offense_batter_id FROM similar_batters)
    )
    SELECT COUNT(*) AS num_result, result_event
    FROM atbat ab
    JOIN matched_atbats ma
      ON ab.game_pk = ma.game_pk AND ab.about_atbatindex = ma.about_atbatindex
    GROUP BY result_event;
    """
    cursor = conn.execute(query)
    return cursor.fetchall()


def insert_pitcher_batter_results(connection, game_pk, pitch_id, batter_id, data: list[tuple[int, str]]):
    cursor = connection.cursor()
    cursor.executemany(f"""
        INSERT OR IGNORE INTO pitcher_batter_result (
            game_pk, pitcher_id, batter_id, num_result, result_event
        ) VALUES ({game_pk}, {pitcher_id}, {batter_id}, ?, ?)
    """, data)
    connection.commit()


if __name__ == '__main__':
    connection = sqlite3.connect("/Users/chris.lan/Downloads/mlb.db")
    create_table_if_not_exist(connection)

    game_pk_sql = """
    SELECT DISTINCT game_pk
    FROM lineup
    WHERE game_pk IN (
        SELECT game_pk
        FROM game
        WHERE status = 'Final'
    )
    order by game_pk desc
    LIMIT 5005;
    """
    rows = connection.cursor().execute(game_pk_sql)
    for row in rows.fetchall():
        game_pk = row[0]
        for team_type in ["away", "home"]:
            team_type: TeamType  # Type hint within loop
            pitcher_id, pitcher_pitchhand_code = get_pitcher_id_and_pitchhand_code(connection, game_pk, team_type)

            percentile_window = 0.15
            for batter_id in get_batter_ids(connection, game_pk, team_type):
                rows = fetch_similar_pitch_results(connection, batter_id, pitcher_pitchhand_code, percentile_window,
                                                   pitcher_id)
                all_rows = [(row[0], row[1]) for row in rows]
                print("working on game", game_pk, team_type, pitcher_id, batter_id)
                insert_pitcher_batter_results(connection, game_pk, pitcher_id, batter_id, all_rows)
        connection.commit()
    connection.close()
