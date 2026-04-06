"""Daily MLB ingest pipeline for PostgreSQL.

Fetches new game data from MLB Stats API, backfills venues and weather.
Designed to run via cron on the mlb-db GCP VM.

Usage:
    python ingest_pg.py              # Ingest current + prior year (cron mode)
    python ingest_pg.py 2024 2025    # Ingest specific years
"""

import argparse
import datetime
import sys
import time
import traceback
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from zoneinfo import ZoneInfo

import psycopg2
import psycopg2.extras
import requests
import statsapi

PG_DSN = "host=127.0.0.1 port=5432 dbname=mlb user=mlb password=mlb2026"

API_DELAY_SECONDS = 0.5
MAX_WORKERS = 4
SKIP_GAME_TYPES = {"S", "E"}

WEATHER_API_URL = "https://archive-api.open-meteo.com/v1/archive"
WEATHER_FORECAST_API_URL = "https://api.open-meteo.com/v1/forecast"
HOURLY_VARS = "temperature_2m,relative_humidity_2m,wind_speed_10m,wind_direction_10m,wind_gusts_10m,precipitation,pressure_msl,cloud_cover,weather_code"
MINUTELY_15_VARS = "temperature_2m,relative_humidity_2m,wind_speed_10m,wind_gusts_10m,precipitation"
DEFAULT_GAME_HOUR = 19
MINUTES_PER_GAME = 20  # 5 hours × 4 readings per hour

POS_CODE_TO_FIELD = {
    "1": "pitcher", "2": "catcher", "3": "first", "4": "second",
    "5": "third", "6": "shortstop", "7": "left", "8": "center", "9": "right",
}


# ── DB connection ────────────────────────────────────────────────────────

def get_conn():
    return psycopg2.connect(PG_DSN)


# ── SQL statements ───────────────────────────────────────────────────────

UPSERT_GAME = """
INSERT INTO game
    ("game_pk", "game_date", "game_datetime", "game_num", "game_type", "status",
     "doubleheader", "current_inning", "inning_state",
     "home_team_id", "home_team_name", "home_score",
     "home_probable_pitcher", "home_pitcher_note",
     "away_team_id", "away_team_name", "away_score",
     "away_probable_pitcher", "away_pitcher_note",
     "venue_id", "venue_name",
     "winning_team", "winning_pitcher", "losing_team", "losing_pitcher",
     "save_pitcher", "series_status", "summary", "national_broadcasts")
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT ("game_pk") DO UPDATE SET
    "game_date"=EXCLUDED."game_date", "game_datetime"=EXCLUDED."game_datetime",
    "game_num"=EXCLUDED."game_num", "game_type"=EXCLUDED."game_type",
    "status"=EXCLUDED."status", "doubleheader"=EXCLUDED."doubleheader",
    "current_inning"=EXCLUDED."current_inning", "inning_state"=EXCLUDED."inning_state",
    "home_team_id"=EXCLUDED."home_team_id", "home_team_name"=EXCLUDED."home_team_name",
    "home_score"=EXCLUDED."home_score", "home_probable_pitcher"=EXCLUDED."home_probable_pitcher",
    "home_pitcher_note"=EXCLUDED."home_pitcher_note",
    "away_team_id"=EXCLUDED."away_team_id", "away_team_name"=EXCLUDED."away_team_name",
    "away_score"=EXCLUDED."away_score", "away_probable_pitcher"=EXCLUDED."away_probable_pitcher",
    "away_pitcher_note"=EXCLUDED."away_pitcher_note",
    "venue_id"=EXCLUDED."venue_id", "venue_name"=EXCLUDED."venue_name",
    "winning_team"=EXCLUDED."winning_team", "winning_pitcher"=EXCLUDED."winning_pitcher",
    "losing_team"=EXCLUDED."losing_team", "losing_pitcher"=EXCLUDED."losing_pitcher",
    "save_pitcher"=EXCLUDED."save_pitcher", "series_status"=EXCLUDED."series_status",
    "summary"=EXCLUDED."summary", "national_broadcasts"=EXCLUDED."national_broadcasts"
"""

UPSERT_PLAYER = """
INSERT INTO player
    ("id", "fullName", "link", "firstName", "lastName", "birthDate", "currentAge",
     "birthCity", "birthStateProvince", "birthCountry", "height", "weight", "active",
     "useName", "useLastName", "middleName", "boxscoreName", "gender",
     "isPlayer", "isVerified", "draftYear",
     "batSide_code", "batSide_description",
     "pitchHand_code", "pitchHand_description",
     "nameFirstLast", "nameSlug", "firstLastName", "lastFirstName",
     "lastInitName", "initLastName", "fullFMLName", "fullLFMName",
     "strikeZoneTop", "strikeZoneBottom",
     "primaryPosition_code", "primaryPosition_name",
     "primaryPosition_type", "primaryPosition_abbreviation")
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT ("id") DO UPDATE SET
    "fullName"=EXCLUDED."fullName", "link"=EXCLUDED."link",
    "firstName"=EXCLUDED."firstName", "lastName"=EXCLUDED."lastName",
    "birthDate"=EXCLUDED."birthDate", "currentAge"=EXCLUDED."currentAge",
    "birthCity"=EXCLUDED."birthCity", "birthStateProvince"=EXCLUDED."birthStateProvince",
    "birthCountry"=EXCLUDED."birthCountry", "height"=EXCLUDED."height",
    "weight"=EXCLUDED."weight", "active"=EXCLUDED."active",
    "useName"=EXCLUDED."useName", "useLastName"=EXCLUDED."useLastName",
    "middleName"=EXCLUDED."middleName", "boxscoreName"=EXCLUDED."boxscoreName",
    "gender"=EXCLUDED."gender", "isPlayer"=EXCLUDED."isPlayer",
    "isVerified"=EXCLUDED."isVerified", "draftYear"=EXCLUDED."draftYear",
    "batSide_code"=EXCLUDED."batSide_code", "batSide_description"=EXCLUDED."batSide_description",
    "pitchHand_code"=EXCLUDED."pitchHand_code", "pitchHand_description"=EXCLUDED."pitchHand_description",
    "nameFirstLast"=EXCLUDED."nameFirstLast", "nameSlug"=EXCLUDED."nameSlug",
    "firstLastName"=EXCLUDED."firstLastName", "lastFirstName"=EXCLUDED."lastFirstName",
    "lastInitName"=EXCLUDED."lastInitName", "initLastName"=EXCLUDED."initLastName",
    "fullFMLName"=EXCLUDED."fullFMLName", "fullLFMName"=EXCLUDED."fullLFMName",
    "strikeZoneTop"=EXCLUDED."strikeZoneTop", "strikeZoneBottom"=EXCLUDED."strikeZoneBottom",
    "primaryPosition_code"=EXCLUDED."primaryPosition_code",
    "primaryPosition_name"=EXCLUDED."primaryPosition_name",
    "primaryPosition_type"=EXCLUDED."primaryPosition_type",
    "primaryPosition_abbreviation"=EXCLUDED."primaryPosition_abbreviation"
"""

UPSERT_ATBAT = """
INSERT INTO atbat
    ("game_pk", "result_type", "result_event", "result_eventType", "result_description",
     "result_rbi", "result_awayScore", "result_homeScore", "result_isOut",
     "about_atBatIndex", "about_halfInning", "about_isTopInning", "about_inning",
     "about_startTime", "about_endTime", "about_isComplete",
     "about_isScoringPlay", "about_hasReview", "about_hasOut", "about_captivatingIndex",
     "count_balls", "count_strikes", "count_outs",
     "matchup_batter_id", "matchup_batter_fullName", "matchup_batter_link",
     "matchup_batSide_code", "matchup_batSide_description",
     "matchup_pitcher_id", "matchup_pitcher_fullName", "matchup_pitcher_link",
     "matchup_pitchHand_code", "matchup_pitchHand_description",
     "matchup_splits_batter", "matchup_splits_pitcher", "matchup_splits_menOnBase",
     "homeTeamWinProbability", "homeTeamWinProbabilityAdded",
     "playEndTime", "pitchIndex", "actionIndex", "runnerIndex")
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT ("game_pk", "about_atBatIndex") DO UPDATE SET
    "result_type"=EXCLUDED."result_type", "result_event"=EXCLUDED."result_event",
    "result_eventType"=EXCLUDED."result_eventType", "result_description"=EXCLUDED."result_description",
    "result_rbi"=EXCLUDED."result_rbi", "result_awayScore"=EXCLUDED."result_awayScore",
    "result_homeScore"=EXCLUDED."result_homeScore", "result_isOut"=EXCLUDED."result_isOut",
    "about_halfInning"=EXCLUDED."about_halfInning", "about_isTopInning"=EXCLUDED."about_isTopInning",
    "about_inning"=EXCLUDED."about_inning", "about_startTime"=EXCLUDED."about_startTime",
    "about_endTime"=EXCLUDED."about_endTime", "about_isComplete"=EXCLUDED."about_isComplete",
    "about_isScoringPlay"=EXCLUDED."about_isScoringPlay", "about_hasReview"=EXCLUDED."about_hasReview",
    "about_hasOut"=EXCLUDED."about_hasOut", "about_captivatingIndex"=EXCLUDED."about_captivatingIndex",
    "count_balls"=EXCLUDED."count_balls", "count_strikes"=EXCLUDED."count_strikes",
    "count_outs"=EXCLUDED."count_outs",
    "matchup_batter_id"=EXCLUDED."matchup_batter_id", "matchup_batter_fullName"=EXCLUDED."matchup_batter_fullName",
    "matchup_batter_link"=EXCLUDED."matchup_batter_link",
    "matchup_batSide_code"=EXCLUDED."matchup_batSide_code", "matchup_batSide_description"=EXCLUDED."matchup_batSide_description",
    "matchup_pitcher_id"=EXCLUDED."matchup_pitcher_id", "matchup_pitcher_fullName"=EXCLUDED."matchup_pitcher_fullName",
    "matchup_pitcher_link"=EXCLUDED."matchup_pitcher_link",
    "matchup_pitchHand_code"=EXCLUDED."matchup_pitchHand_code", "matchup_pitchHand_description"=EXCLUDED."matchup_pitchHand_description",
    "matchup_splits_batter"=EXCLUDED."matchup_splits_batter", "matchup_splits_pitcher"=EXCLUDED."matchup_splits_pitcher",
    "matchup_splits_menOnBase"=EXCLUDED."matchup_splits_menOnBase",
    "homeTeamWinProbability"=EXCLUDED."homeTeamWinProbability",
    "homeTeamWinProbabilityAdded"=EXCLUDED."homeTeamWinProbabilityAdded",
    "playEndTime"=EXCLUDED."playEndTime", "pitchIndex"=EXCLUDED."pitchIndex",
    "actionIndex"=EXCLUDED."actionIndex", "runnerIndex"=EXCLUDED."runnerIndex"
"""

_PE_COLS = [
    "game_pk", "about_atBatIndex",
    "details_call_code", "details_call_description", "details_description",
    "details_code", "details_ballColor", "details_trailColor",
    "details_isInPlay", "details_isStrike", "details_isBall",
    "details_type_code", "details_type_description",
    "details_isOut", "details_hasReview",
    "count_balls", "count_strikes", "count_outs",
    "preCount_balls", "preCount_strikes", "preCount_outs",
    "pitchData_startSpeed", "pitchData_endSpeed",
    "pitchData_strikeZoneTop", "pitchData_strikeZoneBottom",
    "pitchData_coordinates_aY", "pitchData_coordinates_aZ",
    "pitchData_coordinates_pfxX", "pitchData_coordinates_pfxZ",
    "pitchData_coordinates_pX", "pitchData_coordinates_pZ",
    "pitchData_coordinates_vX0", "pitchData_coordinates_vY0", "pitchData_coordinates_vZ0",
    "pitchData_coordinates_x", "pitchData_coordinates_y",
    "pitchData_coordinates_x0", "pitchData_coordinates_y0", "pitchData_coordinates_z0",
    "pitchData_coordinates_aX",
    "pitchData_breaks_breakAngle", "pitchData_breaks_breakLength",
    "pitchData_breaks_breakY",
    "pitchData_breaks_breakVertical", "pitchData_breaks_breakVerticalInduced",
    "pitchData_breaks_breakHorizontal",
    "pitchData_breaks_spinRate", "pitchData_breaks_spinDirection",
    "pitchData_zone", "pitchData_typeConfidence",
    "pitchData_plateTime", "pitchData_extension",
    "index", "playId", "pitchNumber", "startTime", "endTime", "isPitch", "type",
    "defense_pitcher_id", "defense_pitcher_link",
    "defense_pitcher_pitchHand_code", "defense_pitcher_pitchHand_description",
    "defense_catcher_id", "defense_catcher_link",
    "defense_first_id", "defense_first_link",
    "defense_second_id", "defense_second_link",
    "defense_third_id", "defense_third_link",
    "defense_shortstop_id", "defense_shortstop_link",
    "defense_left_id", "defense_left_link",
    "defense_center_id", "defense_center_link",
    "defense_right_id", "defense_right_link",
    "offense_batter_id", "offense_batter_link",
    "offense_batter_batSide_code", "offense_batter_batSide_description",
    "offense_batterPosition_code", "offense_batterPosition_name",
    "offense_batterPosition_type", "offense_batterPosition_abbreviation",
    "offense_first_id", "offense_first_link",
    "offense_second_id", "offense_second_link",
    "offense_third_id", "offense_third_link",
    "hitData_launchSpeed", "hitData_launchAngle", "hitData_totalDistance",
    "hitData_trajectory", "hitData_hardness", "hitData_location",
    "hitData_coordinates_coordX", "hitData_coordinates_coordY",
]

_PE_PK = ["game_pk", "about_atBatIndex", "pitchNumber"]
_PE_NON_PK = [c for c in _PE_COLS if c not in _PE_PK]

UPSERT_PLAY_EVENT = """
INSERT INTO play_event ({cols})
VALUES ({placeholders})
ON CONFLICT ({pk}) DO UPDATE SET
    {updates}
""".format(
    cols=", ".join(f'"{c}"' for c in _PE_COLS),
    placeholders=", ".join(["%s"] * len(_PE_COLS)),
    pk=", ".join(f'"{c}"' for c in _PE_PK),
    updates=", ".join(f'"{c}"=EXCLUDED."{c}"' for c in _PE_NON_PK),
)

UPSERT_RUNNER = """
INSERT INTO runner
    ("game_pk", "about_atBatIndex",
     "movement_originBase", "movement_start", "movement_end",
     "movement_outBase", "movement_isOut", "movement_outNumber",
     "details_event", "details_eventType", "details_movementReason",
     "details_isScoringEvent", "details_rbi", "details_earned",
     "details_teamUnearned", "details_playIndex",
     "runner_id", "runner_fullName", "runner_link")
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT ("game_pk", "about_atBatIndex", "runner_id") DO UPDATE SET
    "movement_originBase"=EXCLUDED."movement_originBase",
    "movement_start"=EXCLUDED."movement_start", "movement_end"=EXCLUDED."movement_end",
    "movement_outBase"=EXCLUDED."movement_outBase", "movement_isOut"=EXCLUDED."movement_isOut",
    "movement_outNumber"=EXCLUDED."movement_outNumber",
    "details_event"=EXCLUDED."details_event", "details_eventType"=EXCLUDED."details_eventType",
    "details_movementReason"=EXCLUDED."details_movementReason",
    "details_isScoringEvent"=EXCLUDED."details_isScoringEvent",
    "details_rbi"=EXCLUDED."details_rbi", "details_earned"=EXCLUDED."details_earned",
    "details_teamUnearned"=EXCLUDED."details_teamUnearned",
    "details_playIndex"=EXCLUDED."details_playIndex",
    "runner_fullName"=EXCLUDED."runner_fullName", "runner_link"=EXCLUDED."runner_link"
"""

UPSERT_LINEUP = """
INSERT INTO lineup
    ("game_pk", "team_type", "batting_order", "player_id", "player_name", "player_link",
     "parent_team_id", "position_code", "position_abbreviation", "position_name",
     "position_type", "parentTeamId")
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT ("game_pk", "batting_order", "player_id") DO UPDATE SET
    "team_type"=EXCLUDED."team_type",
    "player_name"=EXCLUDED."player_name", "player_link"=EXCLUDED."player_link",
    "parent_team_id"=EXCLUDED."parent_team_id",
    "position_code"=EXCLUDED."position_code", "position_abbreviation"=EXCLUDED."position_abbreviation",
    "position_name"=EXCLUDED."position_name", "position_type"=EXCLUDED."position_type",
    "parentTeamId"=EXCLUDED."parentTeamId"
"""

UPSERT_VENUE = """
INSERT INTO venues
    ("venue_id", "name", "city", "state", "country", "latitude", "longitude", "elevation",
     "timezone", "roof_type", "turf_type", "capacity",
     "left_line", "left_center", "center", "right_center", "right_line")
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT ("venue_id") DO UPDATE SET
    "name"=EXCLUDED."name", "city"=EXCLUDED."city", "state"=EXCLUDED."state",
    "country"=EXCLUDED."country", "latitude"=EXCLUDED."latitude", "longitude"=EXCLUDED."longitude",
    "elevation"=EXCLUDED."elevation", "timezone"=EXCLUDED."timezone",
    "roof_type"=EXCLUDED."roof_type", "turf_type"=EXCLUDED."turf_type",
    "capacity"=EXCLUDED."capacity", "left_line"=EXCLUDED."left_line",
    "left_center"=EXCLUDED."left_center", "center"=EXCLUDED."center",
    "right_center"=EXCLUDED."right_center", "right_line"=EXCLUDED."right_line"
"""

UPSERT_WEATHER = """
INSERT INTO game_weather
    ("game_pk", "temperature_f", "humidity", "wind_speed_mph", "wind_direction",
     "wind_gusts_mph", "precipitation_mm", "pressure_hpa", "cloud_cover", "weather_code")
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT ("game_pk") DO UPDATE SET
    "temperature_f"=EXCLUDED."temperature_f", "humidity"=EXCLUDED."humidity",
    "wind_speed_mph"=EXCLUDED."wind_speed_mph", "wind_direction"=EXCLUDED."wind_direction",
    "wind_gusts_mph"=EXCLUDED."wind_gusts_mph", "precipitation_mm"=EXCLUDED."precipitation_mm",
    "pressure_hpa"=EXCLUDED."pressure_hpa", "cloud_cover"=EXCLUDED."cloud_cover",
    "weather_code"=EXCLUDED."weather_code"
"""

UPSERT_WEATHER_NULL = """
INSERT INTO game_weather ("game_pk") VALUES (%s)
ON CONFLICT ("game_pk") DO NOTHING
"""

CREATE_WEATHER_HOURLY = """
CREATE TABLE IF NOT EXISTS game_weather_hourly (
    game_pk BIGINT NOT NULL,
    hour_offset SMALLINT NOT NULL,
    local_time TEXT,
    temperature_f DOUBLE PRECISION,
    humidity BIGINT,
    wind_speed_mph DOUBLE PRECISION,
    wind_direction BIGINT,
    wind_gusts_mph DOUBLE PRECISION,
    precipitation_mm DOUBLE PRECISION,
    pressure_hpa DOUBLE PRECISION,
    cloud_cover BIGINT,
    weather_code BIGINT,
    PRIMARY KEY (game_pk, hour_offset)
)
"""

UPSERT_WEATHER_HOURLY = """
INSERT INTO game_weather_hourly
    ("game_pk", "hour_offset", "local_time", "temperature_f", "humidity",
     "wind_speed_mph", "wind_direction", "wind_gusts_mph",
     "precipitation_mm", "pressure_hpa", "cloud_cover", "weather_code")
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT ("game_pk", "hour_offset") DO UPDATE SET
    "local_time"=EXCLUDED."local_time", "temperature_f"=EXCLUDED."temperature_f",
    "humidity"=EXCLUDED."humidity", "wind_speed_mph"=EXCLUDED."wind_speed_mph",
    "wind_direction"=EXCLUDED."wind_direction", "wind_gusts_mph"=EXCLUDED."wind_gusts_mph",
    "precipitation_mm"=EXCLUDED."precipitation_mm", "pressure_hpa"=EXCLUDED."pressure_hpa",
    "cloud_cover"=EXCLUDED."cloud_cover", "weather_code"=EXCLUDED."weather_code"
"""

CREATE_WEATHER_15MIN = """
CREATE TABLE IF NOT EXISTS game_weather_15min (
    game_pk BIGINT NOT NULL,
    minute_offset SMALLINT NOT NULL,
    local_time TEXT,
    temperature_f DOUBLE PRECISION,
    humidity BIGINT,
    wind_speed_mph DOUBLE PRECISION,
    wind_gusts_mph DOUBLE PRECISION,
    precipitation_mm DOUBLE PRECISION,
    PRIMARY KEY (game_pk, minute_offset)
)
"""

UPSERT_WEATHER_15MIN = """
INSERT INTO game_weather_15min
    ("game_pk", "minute_offset", "local_time", "temperature_f", "humidity",
     "wind_speed_mph", "wind_gusts_mph", "precipitation_mm")
VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT ("game_pk", "minute_offset") DO UPDATE SET
    "local_time"=EXCLUDED."local_time", "temperature_f"=EXCLUDED."temperature_f",
    "humidity"=EXCLUDED."humidity", "wind_speed_mph"=EXCLUDED."wind_speed_mph",
    "wind_gusts_mph"=EXCLUDED."wind_gusts_mph", "precipitation_mm"=EXCLUDED."precipitation_mm"
"""


# ── Defense tracking ─────────────────────────────────────────────────────

def _extract_starting_defense(team_box, game_data_players):
    defense = {}
    players = team_box.get("players", {})
    for key, player_data in players.items():
        pos = player_data.get("position", {})
        pos_code = pos.get("code", "")
        person = player_data.get("person", {})
        player_id = person.get("id")
        game_status = player_data.get("gameStatus", {})
        if pos_code in POS_CODE_TO_FIELD and player_id and not game_status.get("isSubstitute", False):
            field_name = POS_CODE_TO_FIELD[pos_code]
            defense[f"{field_name}_id"] = player_id
            defense[f"{field_name}_link"] = person.get("link")
            if pos_code == "1":
                p_info = game_data_players.get(f"ID{player_id}", {})
                defense["pitcher_pitchHand_code"] = p_info.get("pitchHand", {}).get("code")
                defense["pitcher_pitchHand_description"] = p_info.get("pitchHand", {}).get("description")
    return defense


def _update_defense_from_event(defense, event, game_data, is_top):
    event_type = event.get("details", {}).get("eventType", "")
    if event_type not in ("defensive_switch", "defensive_sub", "pitching_substitution"):
        return
    player = event.get("player", {})
    player_id = player.get("id")
    if not player_id:
        return
    box = game_data.get("liveData", {}).get("boxscore", {}).get("teams", {})
    team = box.get("home" if is_top else "away", {})
    players = team.get("players", {})
    player_data = players.get(f"ID{player_id}", {})
    pos_code = None
    if player_data:
        all_positions = player_data.get("allPositions", [])
        if all_positions:
            pos_code = all_positions[-1].get("code")
        else:
            pos_code = player_data.get("position", {}).get("code")
    if pos_code and pos_code in POS_CODE_TO_FIELD:
        field_name = POS_CODE_TO_FIELD[pos_code]
        person = player_data.get("person", {}) if player_data else {}
        defense[f"{field_name}_id"] = player_id
        defense[f"{field_name}_link"] = person.get("link") or player.get("link")
        if pos_code == "1":
            gd_players = game_data.get("gameData", {}).get("players", {})
            p_info = gd_players.get(f"ID{player_id}", {})
            defense["pitcher_pitchHand_code"] = p_info.get("pitchHand", {}).get("code")
            defense["pitcher_pitchHand_description"] = p_info.get("pitchHand", {}).get("description")


def build_defense_timeline(game_data):
    box = game_data.get("liveData", {}).get("boxscore", {}).get("teams", {})
    all_plays = game_data.get("liveData", {}).get("plays", {}).get("allPlays", [])
    gd_players = game_data.get("gameData", {}).get("players", {})
    home_defense = _extract_starting_defense(box.get("home", {}), gd_players)
    away_defense = _extract_starting_defense(box.get("away", {}), gd_players)
    timeline = {}
    for play in all_plays:
        about = play.get("about", {})
        at_bat_index = about.get("atBatIndex")
        is_top = about.get("isTopInning", True)
        current_defense = home_defense if is_top else away_defense
        for ev in play.get("playEvents", []):
            if ev.get("type") == "action":
                _update_defense_from_event(current_defense, ev, game_data, is_top)
        timeline[at_bat_index] = dict(current_defense)
    return timeline


# ── Baserunner tracking ──────────────────────────────────────────────────

def build_baserunner_state(play, event_index):
    bases = {"1B": None, "2B": None, "3B": None}
    for runner in play.get("runners", []):
        r_play_index = runner.get("details", {}).get("playIndex")
        if r_play_index is not None and r_play_index >= event_index:
            continue
        movement = runner.get("movement", {})
        runner_info = runner.get("details", {}).get("runner", {})
        runner_id = runner_info.get("id")
        runner_link = runner_info.get("link")
        start = movement.get("start")
        if start and start in bases:
            if bases[start] and bases[start][0] == runner_id:
                bases[start] = None
        end = movement.get("end")
        if end and end in bases and runner_id:
            bases[end] = (runner_id, runner_link)
    return {
        "first_id": bases["1B"][0] if bases["1B"] else None,
        "first_link": bases["1B"][1] if bases["1B"] else None,
        "second_id": bases["2B"][0] if bases["2B"] else None,
        "second_link": bases["2B"][1] if bases["2B"] else None,
        "third_id": bases["3B"][0] if bases["3B"] else None,
        "third_link": bases["3B"][1] if bases["3B"] else None,
    }


# ── Batter position lookup ──────────────────────────────────────────────

def get_batter_position(game_data, batter_id, is_top):
    box = game_data.get("liveData", {}).get("boxscore", {}).get("teams", {})
    team = box.get("away" if is_top else "home", {})
    player_data = team.get("players", {}).get(f"ID{batter_id}", {})
    pos = player_data.get("position", {})
    return {
        "code": pos.get("code"), "name": pos.get("name"),
        "type": pos.get("type"), "abbreviation": pos.get("abbreviation"),
    }


# ── Extraction helpers ───────────────────────────────────────────────────

def extract_game_row(g):
    return (
        g["game_id"], g["game_date"], g["game_datetime"], g["game_num"],
        g["game_type"], g["status"], g["doubleheader"],
        g.get("current_inning"), g.get("inning_state"),
        g["home_id"], g["home_name"], g["home_score"],
        g.get("home_probable_pitcher"), g.get("home_pitcher_note"),
        g["away_id"], g["away_name"], g["away_score"],
        g.get("away_probable_pitcher"), g.get("away_pitcher_note"),
        g["venue_id"], g["venue_name"],
        g.get("winning_team"), g.get("winning_pitcher"),
        g.get("losing_team"), g.get("losing_pitcher"),
        g.get("save_pitcher"), g.get("series_status"), g.get("summary"),
        ", ".join(g.get("national_broadcasts") or []),
    )


def extract_players(game_data):
    for p in game_data.get("gameData", {}).get("players", {}).values():
        yield (
            p["id"], p.get("fullName"), p.get("link"),
            p.get("firstName"), p.get("lastName"), p.get("birthDate"), p.get("currentAge"),
            p.get("birthCity"), p.get("birthStateProvince"), p.get("birthCountry"),
            p.get("height"), p.get("weight"), p.get("active"),
            p.get("useName"), p.get("useLastName"), p.get("middleName"),
            p.get("boxscoreName"), p.get("gender"),
            p.get("isPlayer"), p.get("isVerified"), p.get("draftYear"),
            p.get("batSide", {}).get("code"), p.get("batSide", {}).get("description"),
            p.get("pitchHand", {}).get("code"), p.get("pitchHand", {}).get("description"),
            p.get("nameFirstLast"), p.get("nameSlug"),
            p.get("firstLastName"), p.get("lastFirstName"),
            p.get("lastInitName"), p.get("initLastName"),
            p.get("fullFMLName"), p.get("fullLFMName"),
            p.get("strikeZoneTop"), p.get("strikeZoneBottom"),
            p.get("primaryPosition", {}).get("code"), p.get("primaryPosition", {}).get("name"),
            p.get("primaryPosition", {}).get("type"), p.get("primaryPosition", {}).get("abbreviation"),
        )


def extract_atbats(game_pk, game_data):
    all_plays = game_data.get("liveData", {}).get("plays", {}).get("allPlays", [])
    for play in all_plays:
        about = play.get("about", {})
        result = play.get("result", {})
        matchup = play.get("matchup", {})
        count = play.get("count", {})
        yield (
            game_pk, result.get("type"), result.get("event"), result.get("eventType"),
            result.get("description"), result.get("rbi"),
            result.get("awayScore"), result.get("homeScore"), result.get("isOut"),
            about.get("atBatIndex"), about.get("halfInning"), about.get("isTopInning"),
            about.get("inning"), about.get("startTime"), about.get("endTime"),
            about.get("isComplete"), about.get("isScoringPlay"),
            about.get("hasReview"), about.get("hasOut"), about.get("captivatingIndex"),
            count.get("balls"), count.get("strikes"), count.get("outs"),
            matchup.get("batter", {}).get("id"), matchup.get("batter", {}).get("fullName"),
            matchup.get("batter", {}).get("link"),
            matchup.get("batSide", {}).get("code"), matchup.get("batSide", {}).get("description"),
            matchup.get("pitcher", {}).get("id"), matchup.get("pitcher", {}).get("fullName"),
            matchup.get("pitcher", {}).get("link"),
            matchup.get("pitchHand", {}).get("code"), matchup.get("pitchHand", {}).get("description"),
            matchup.get("splits", {}).get("batter"), matchup.get("splits", {}).get("pitcher"),
            matchup.get("splits", {}).get("menOnBase"),
            play.get("homeTeamWinProbability"), play.get("homeTeamWinProbabilityAdded"),
            play.get("playEndTime"),
            str(play.get("pitchIndex", [])), str(play.get("actionIndex", [])),
            str(play.get("runnerIndex", [])),
        )


def extract_play_events(game_pk, game_data, defense_timeline):
    all_plays = game_data.get("liveData", {}).get("plays", {}).get("allPlays", [])
    for play in all_plays:
        about = play.get("about", {})
        matchup = play.get("matchup", {})
        at_bat_index = about.get("atBatIndex")
        is_top = about.get("isTopInning", True)
        batter_id = matchup.get("batter", {}).get("id")
        defense = defense_timeline.get(at_bat_index, {})
        batter_pos = get_batter_position(game_data, batter_id, is_top)
        prev_balls = 0
        prev_strikes = 0
        prev_outs = None
        events = play.get("playEvents", [])
        for ev in events:
            if not ev.get("isPitch"):
                continue
            details = ev.get("details", {})
            pd_data = ev.get("pitchData", {})
            brk = pd_data.get("breaks", {})
            coords = pd_data.get("coordinates", {})
            cnt = ev.get("count", {})
            call = details.get("call", {})
            ptype = details.get("type", {})
            hit = ev.get("hitData", {})
            hit_coords = hit.get("coordinates", {}) if hit else {}
            event_index = ev.get("index", 0)
            runners = build_baserunner_state(play, event_index)
            pre_balls = prev_balls
            pre_strikes = prev_strikes
            pre_outs = prev_outs if prev_outs is not None else cnt.get("outs")
            prev_balls = cnt.get("balls", 0)
            prev_strikes = cnt.get("strikes", 0)
            prev_outs = cnt.get("outs")
            yield (
                game_pk, at_bat_index,
                call.get("code"), call.get("description"), details.get("description"),
                details.get("code"), details.get("ballColor"), details.get("trailColor"),
                details.get("isInPlay", False), details.get("isStrike", False),
                details.get("isBall", False),
                ptype.get("code"), ptype.get("description"),
                details.get("isOut", False), details.get("hasReview", False),
                cnt.get("balls"), cnt.get("strikes"), cnt.get("outs"),
                pre_balls, pre_strikes, pre_outs,
                pd_data.get("startSpeed"), pd_data.get("endSpeed"),
                pd_data.get("strikeZoneTop"), pd_data.get("strikeZoneBottom"),
                coords.get("aY"), coords.get("aZ"),
                coords.get("pfxX"), coords.get("pfxZ"),
                coords.get("pX"), coords.get("pZ"),
                coords.get("vX0"), coords.get("vY0"), coords.get("vZ0"),
                coords.get("x"), coords.get("y"),
                coords.get("x0"), coords.get("y0"), coords.get("z0"),
                coords.get("aX"),
                brk.get("breakAngle"), brk.get("breakLength"), brk.get("breakY"),
                brk.get("breakVertical"), brk.get("breakVerticalInduced"),
                brk.get("breakHorizontal"),
                brk.get("spinRate"), brk.get("spinDirection"),
                pd_data.get("zone"), pd_data.get("typeConfidence"),
                pd_data.get("plateTime"), pd_data.get("extension"),
                ev.get("index"), ev.get("playId"), ev.get("pitchNumber"),
                ev.get("startTime"), ev.get("endTime"),
                ev.get("isPitch"), ev.get("type"),
                defense.get("pitcher_id"), defense.get("pitcher_link"),
                defense.get("pitcher_pitchHand_code"), defense.get("pitcher_pitchHand_description"),
                defense.get("catcher_id"), defense.get("catcher_link"),
                defense.get("first_id"), defense.get("first_link"),
                defense.get("second_id"), defense.get("second_link"),
                defense.get("third_id"), defense.get("third_link"),
                defense.get("shortstop_id"), defense.get("shortstop_link"),
                defense.get("left_id"), defense.get("left_link"),
                defense.get("center_id"), defense.get("center_link"),
                defense.get("right_id"), defense.get("right_link"),
                matchup.get("batter", {}).get("id"), matchup.get("batter", {}).get("link"),
                matchup.get("batSide", {}).get("code"), matchup.get("batSide", {}).get("description"),
                batter_pos.get("code"), batter_pos.get("name"),
                batter_pos.get("type"), batter_pos.get("abbreviation"),
                runners.get("first_id"), runners.get("first_link"),
                runners.get("second_id"), runners.get("second_link"),
                runners.get("third_id"), runners.get("third_link"),
                hit.get("launchSpeed"), hit.get("launchAngle"), hit.get("totalDistance"),
                hit.get("trajectory"), hit.get("hardness"), hit.get("location"),
                hit_coords.get("coordX"), hit_coords.get("coordY"),
            )


def extract_runners(game_pk, game_data):
    all_plays = game_data.get("liveData", {}).get("plays", {}).get("allPlays", [])
    for play in all_plays:
        at_bat_index = play.get("about", {}).get("atBatIndex")
        for runner in play.get("runners", []):
            movement = runner.get("movement", {})
            details = runner.get("details", {})
            runner_info = details.get("runner", {})
            yield (
                game_pk, at_bat_index,
                movement.get("originBase"), movement.get("start"), movement.get("end"),
                movement.get("outBase"), movement.get("isOut"), movement.get("outNumber"),
                details.get("event"), details.get("eventType"), details.get("movementReason"),
                details.get("isScoringEvent"), details.get("rbi"), details.get("earned"),
                details.get("teamUnearned"), details.get("playIndex"),
                runner_info.get("id"), runner_info.get("fullName"), runner_info.get("link"),
            )


def extract_lineups(game_pk, game_data):
    box = game_data.get("liveData", {}).get("boxscore", {}).get("teams", {})
    for team_type in ("home", "away"):
        team = box.get(team_type, {})
        batting_order = team.get("battingOrder", [])
        players = team.get("players", {})
        team_id = team.get("team", {}).get("id")
        for pid in batting_order:
            player_data = players.get(f"ID{pid}", {})
            person = player_data.get("person", {})
            pos = player_data.get("position", {})
            order = player_data.get("battingOrder")
            if order is not None:
                order = int(order)
            yield (
                str(game_pk), team_type, order, pid,
                person.get("fullName", ""), person.get("link"),
                team_id, pos.get("code"), pos.get("abbreviation"),
                pos.get("name"), pos.get("type"), team_id,
            )


# ── API fetching ─────────────────────────────────────────────────────────

def fetch_game_data(game_pk):
    time.sleep(API_DELAY_SECONDS)
    return game_pk, statsapi.get("game", {"gamePk": game_pk})


def fetch_schedule(year):
    time.sleep(API_DELAY_SECONDS)
    games = statsapi.schedule(start_date=f"{year}-01-01", end_date=f"{year}-12-31")
    return [g for g in games if g.get("status") == "Final" and g.get("game_type") not in SKIP_GAME_TYPES]


# ── DB writing ───────────────────────────────────────────────────────────

def write_game(conn, game_pk, schedule_entry, game_data):
    cur = conn.cursor()
    cur.execute(UPSERT_GAME, extract_game_row(schedule_entry))
    psycopg2.extras.execute_batch(cur, UPSERT_PLAYER, list(extract_players(game_data)), page_size=500)
    psycopg2.extras.execute_batch(cur, UPSERT_ATBAT, list(extract_atbats(game_pk, game_data)), page_size=500)
    defense_timeline = build_defense_timeline(game_data)
    pe_rows = list(extract_play_events(game_pk, game_data, defense_timeline))
    psycopg2.extras.execute_batch(cur, UPSERT_PLAY_EVENT, pe_rows, page_size=500)
    psycopg2.extras.execute_batch(cur, UPSERT_RUNNER, list(extract_runners(game_pk, game_data)), page_size=500)
    psycopg2.extras.execute_batch(cur, UPSERT_LINEUP, list(extract_lineups(game_pk, game_data)), page_size=500)
    cur.close()
    return len(pe_rows)


def get_existing_game_pks(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT game_pk FROM game WHERE status = 'Final'")
        return {row[0] for row in cur.fetchall()}


# ── Core ingestion ───────────────────────────────────────────────────────

def ingest_games(schedule_entries, label="", workers=MAX_WORKERS):
    conn = get_conn()
    try:
        existing = get_existing_game_pks(conn)
        to_do = [g for g in schedule_entries if g["game_id"] not in existing]
        total = len(to_do)
        skipped = len(schedule_entries) - total

        if skipped:
            print(f"Skipping {skipped} already-ingested games.")
        if total == 0:
            print("Nothing to ingest.")
            return

        schedule_by_pk = {g["game_id"]: g for g in to_do}
        total_pitches = 0
        failed = 0

        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(fetch_game_data, g["game_id"]): g["game_id"] for g in to_do}
            for idx, future in enumerate(as_completed(futures), 1):
                gpk = futures[future]
                sched = schedule_by_pk[gpk]
                date = sched["game_date"]
                away = sched.get("away_name", "???")
                home = sched.get("home_name", "???")
                try:
                    _, data = future.result()
                    pitch_count = write_game(conn, gpk, sched, data)
                    conn.commit()
                    total_pitches += pitch_count
                    print(f"[{idx}/{total}] {date} {away} @ {home}: {pitch_count} pitches")
                except Exception:
                    conn.rollback()
                    failed += 1
                    err = traceback.format_exc()
                    print(f"[{idx}/{total}] {date} {away} @ {home}: FAILED")
                    print(f"  {err.strip().splitlines()[-1]}")

        print(f"\nDone{' (' + label + ')' if label else ''}.")
        print(f"  Games ingested: {total - failed}")
        print(f"  Total pitches:  {total_pitches}")
        if failed:
            print(f"  Failed games:   {failed}")
    finally:
        conn.close()


# ── Venue backfill ───────────────────────────────────────────────────────

def backfill_venues():
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT g.venue_id, MIN(g.game_pk) AS game_pk
                FROM game g LEFT JOIN venues v ON g.venue_id = v.venue_id
                WHERE g.venue_id IS NOT NULL AND v.venue_id IS NULL
                GROUP BY g.venue_id
            """)
            missing = cur.fetchall()

        if not missing:
            print("All venues already populated.")
            return

        print(f"Backfilling {len(missing)} venues...\n")
        for idx, row in enumerate(missing, 1):
            venue_id, game_pk = row["venue_id"], row["game_pk"]
            try:
                time.sleep(API_DELAY_SECONDS)
                data = statsapi.get("game", {"gamePk": game_pk})
                venue = data["gameData"]["venue"]
                loc = venue.get("location", {})
                coords = loc.get("defaultCoordinates", {})
                fi = venue.get("fieldInfo", {})
                tz = venue.get("timeZone", {})
                with conn.cursor() as cur:
                    cur.execute(UPSERT_VENUE, (
                        venue["id"], venue["name"],
                        loc.get("city"), loc.get("state"), loc.get("country"),
                        coords.get("latitude"), coords.get("longitude"),
                        loc.get("elevation"), tz.get("id"),
                        fi.get("roofType"), fi.get("turfType"), fi.get("capacity"),
                        fi.get("leftLine"), fi.get("leftCenter"),
                        fi.get("center"), fi.get("rightCenter"), fi.get("rightLine"),
                    ))
                conn.commit()
                print(f"[{idx}/{len(missing)}] {venue['name']}: {coords.get('latitude')}, {coords.get('longitude')}")
            except Exception as e:
                conn.rollback()
                print(f"[{idx}/{len(missing)}] venue_id={venue_id}: FAILED ({e})")
    finally:
        conn.close()


# ── Weather backfill ─────────────────────────────────────────────────────

def backfill_weather():
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT g.game_pk, g.game_date, g.venue_id,
                       v.latitude, v.longitude, v.timezone, v.roof_type, v.name AS venue_name
                FROM game g
                JOIN venues v ON g.venue_id = v.venue_id
                LEFT JOIN game_weather gw ON g.game_pk = gw.game_pk
                WHERE gw.game_pk IS NULL AND g.status = 'Final'
            """)
            games = cur.fetchall()

        if not games:
            print("No games need weather data.")
            return

        dome_games = [g for g in games if g["roof_type"] == "Dome"]
        outdoor_games = [g for g in games if g["roof_type"] != "Dome"]

        dome_count = 0
        if dome_games:
            with conn.cursor() as cur:
                psycopg2.extras.execute_batch(
                    cur, UPSERT_WEATHER_NULL,
                    [(g["game_pk"],) for g in dome_games], page_size=500
                )
            conn.commit()
            dome_count = len(dome_games)
            print(f"Dome stadiums: {dome_count} games marked with NULL weather")

        groups = defaultdict(list)
        for g in outdoor_games:
            year = str(g["game_date"])[:4]
            groups[(g["venue_id"], year)].append(g)

        total_batches = len(groups)
        if total_batches == 0:
            print(f"Done. {dome_count} dome games processed.")
            return

        total_inserted = 0
        total_failed = 0

        for idx, ((venue_id, year), batch) in enumerate(sorted(groups.items()), 1):
            venue_name = batch[0]["venue_name"]
            lat, lon, tz = batch[0]["latitude"], batch[0]["longitude"], batch[0]["timezone"]

            if lat is None or lon is None:
                with conn.cursor() as cur:
                    psycopg2.extras.execute_batch(
                        cur, UPSERT_WEATHER_NULL,
                        [(g["game_pk"],) for g in batch], page_size=500
                    )
                conn.commit()
                print(f"[{idx}/{total_batches}] {venue_name} {year}: no coords, {len(batch)} games skipped")
                continue

            try:
                dates = sorted(str(g["game_date"])[:10] for g in batch)
                time.sleep(API_DELAY_SECONDS)
                resp = requests.get(WEATHER_API_URL, params={
                    "latitude": lat, "longitude": lon,
                    "start_date": dates[0], "end_date": dates[-1],
                    "hourly": HOURLY_VARS, "timezone": tz or "UTC",
                    "temperature_unit": "fahrenheit", "wind_speed_unit": "mph",
                }, timeout=30)
                resp.raise_for_status()
                data = resp.json()
                hourly = data["hourly"]
                time_index = {t: i for i, t in enumerate(hourly["time"])}

                inserted = 0
                rows = []
                null_rows = []
                for g in batch:
                    key = f"{str(g['game_date'])[:10]}T{DEFAULT_GAME_HOUR:02d}:00"
                    idx_h = time_index.get(key)
                    if idx_h is not None:
                        rows.append((
                            g["game_pk"],
                            hourly["temperature_2m"][idx_h],
                            hourly["relative_humidity_2m"][idx_h],
                            hourly["wind_speed_10m"][idx_h],
                            hourly["wind_direction_10m"][idx_h],
                            hourly["wind_gusts_10m"][idx_h],
                            hourly["precipitation"][idx_h],
                            hourly["pressure_msl"][idx_h],
                            hourly["cloud_cover"][idx_h],
                            hourly["weather_code"][idx_h],
                        ))
                        inserted += 1
                    else:
                        null_rows.append((g["game_pk"],))

                with conn.cursor() as cur:
                    if rows:
                        psycopg2.extras.execute_batch(cur, UPSERT_WEATHER, rows, page_size=500)
                    if null_rows:
                        psycopg2.extras.execute_batch(cur, UPSERT_WEATHER_NULL, null_rows, page_size=500)
                conn.commit()
                total_inserted += inserted
                print(f"[{idx}/{total_batches}] {venue_name} {year}: {len(batch)} games, {inserted} weather rows")

            except Exception as e:
                conn.rollback()
                total_failed += len(batch)
                print(f"[{idx}/{total_batches}] {venue_name} {year}: FAILED ({e})")

            time.sleep(API_DELAY_SECONDS)

        print(f"\nWeather done.")
        print(f"  Dome games (NULL): {dome_count}")
        print(f"  Weather inserted:  {total_inserted}")
        if total_failed:
            print(f"  Failed:            {total_failed}")
    finally:
        conn.close()


HOURS_PER_GAME = 5  # hours 0-4 from game start


def _utc_to_local_hour(game_datetime_str, tz_name):
    """Convert UTC game_datetime to local start hour (truncated).

    Returns local datetime at the start of the hour, or None if conversion fails.
    """
    try:
        utc_dt = datetime.datetime.fromisoformat(game_datetime_str).replace(tzinfo=ZoneInfo("UTC"))
        local_dt = utc_dt.astimezone(ZoneInfo(tz_name))
        return local_dt.replace(minute=0, second=0, microsecond=0)
    except Exception:
        return None


def backfill_weather_hourly():
    """Backfill game_weather_hourly: 5 hourly readings per game from actual start time."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(CREATE_WEATHER_HOURLY)
        conn.commit()

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT g.game_pk, g.game_date, g.game_datetime, g.venue_id,
                       v.latitude, v.longitude, v.timezone, v.roof_type, v.name AS venue_name
                FROM game g
                JOIN venues v ON g.venue_id = v.venue_id
                LEFT JOIN game_weather_hourly gwh ON g.game_pk = gwh.game_pk
                WHERE gwh.game_pk IS NULL AND g.status = 'Final'
            """)
            games = cur.fetchall()

        if not games:
            print("No games need hourly weather data.")
            return

        dome_games = [g for g in games if g["roof_type"] == "Dome"]
        outdoor_games = [g for g in games if g["roof_type"] != "Dome"]

        dome_count = 0
        if dome_games:
            rows = []
            for g in dome_games:
                for h in range(HOURS_PER_GAME):
                    rows.append((g["game_pk"], h, None, None, None, None, None, None, None, None, None, None))
            with conn.cursor() as cur:
                psycopg2.extras.execute_batch(cur, UPSERT_WEATHER_HOURLY, rows, page_size=1000)
            conn.commit()
            dome_count = len(dome_games)
            print(f"Dome stadiums: {dome_count} games ({dome_count * HOURS_PER_GAME} rows) marked NULL")

        groups = defaultdict(list)
        for g in outdoor_games:
            year = str(g["game_date"])[:4]
            groups[(g["venue_id"], year)].append(g)

        total_batches = len(groups)
        if total_batches == 0:
            print(f"Done. {dome_count} dome games processed.")
            return

        total_games = 0
        total_failed = 0

        for idx, ((venue_id, year), batch) in enumerate(sorted(groups.items()), 1):
            venue_name = batch[0]["venue_name"]
            lat, lon, tz = batch[0]["latitude"], batch[0]["longitude"], batch[0]["timezone"]

            if lat is None or lon is None:
                rows = []
                for g in batch:
                    for h in range(HOURS_PER_GAME):
                        rows.append((g["game_pk"], h, None, None, None, None, None, None, None, None, None, None))
                with conn.cursor() as cur:
                    psycopg2.extras.execute_batch(cur, UPSERT_WEATHER_HOURLY, rows, page_size=1000)
                conn.commit()
                print(f"[{idx}/{total_batches}] {venue_name} {year}: no coords, {len(batch)} games skipped")
                continue

            try:
                dates = sorted(str(g["game_date"])[:10] for g in batch)
                time.sleep(API_DELAY_SECONDS)
                resp = requests.get(WEATHER_API_URL, params={
                    "latitude": lat, "longitude": lon,
                    "start_date": dates[0], "end_date": dates[-1],
                    "hourly": HOURLY_VARS, "timezone": tz or "UTC",
                    "temperature_unit": "fahrenheit", "wind_speed_unit": "mph",
                }, timeout=30)
                resp.raise_for_status()
                data = resp.json()
                hourly = data["hourly"]
                time_index = {t: i for i, t in enumerate(hourly["time"])}

                rows = []
                games_filled = 0
                for g in batch:
                    local_start = _utc_to_local_hour(str(g["game_datetime"]), tz or "UTC")
                    if local_start is None:
                        for h in range(HOURS_PER_GAME):
                            rows.append((g["game_pk"], h, None, None, None, None, None, None, None, None, None, None))
                        continue

                    game_ok = False
                    for h in range(HOURS_PER_GAME):
                        hour_dt = local_start + datetime.timedelta(hours=h)
                        key = hour_dt.strftime("%Y-%m-%dT%H:%M")
                        idx_h = time_index.get(key)
                        if idx_h is not None:
                            rows.append((
                                g["game_pk"], h, key,
                                hourly["temperature_2m"][idx_h],
                                hourly["relative_humidity_2m"][idx_h],
                                hourly["wind_speed_10m"][idx_h],
                                hourly["wind_direction_10m"][idx_h],
                                hourly["wind_gusts_10m"][idx_h],
                                hourly["precipitation"][idx_h],
                                hourly["pressure_msl"][idx_h],
                                hourly["cloud_cover"][idx_h],
                                hourly["weather_code"][idx_h],
                            ))
                            game_ok = True
                        else:
                            rows.append((g["game_pk"], h, key, None, None, None, None, None, None, None, None, None))
                    if game_ok:
                        games_filled += 1

                with conn.cursor() as cur:
                    psycopg2.extras.execute_batch(cur, UPSERT_WEATHER_HOURLY, rows, page_size=1000)
                conn.commit()
                total_games += games_filled
                print(f"[{idx}/{total_batches}] {venue_name} {year}: {len(batch)} games, {len(rows)} hourly rows")

            except Exception as e:
                conn.rollback()
                total_failed += len(batch)
                print(f"[{idx}/{total_batches}] {venue_name} {year}: FAILED ({e})")

            time.sleep(API_DELAY_SECONDS)

        print(f"\nHourly weather done.")
        print(f"  Dome games (NULL): {dome_count}")
        print(f"  Games filled:      {total_games}")
        if total_failed:
            print(f"  Failed:            {total_failed}")
    finally:
        conn.close()


def backfill_weather_15min():
    """Backfill 15-minute weather for recent games (rolling 3-day window from forecast API)."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(CREATE_WEATHER_15MIN)
        conn.commit()

        cutoff = (datetime.date.today() - datetime.timedelta(days=3)).isoformat()

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT g.game_pk, g.game_date, g.game_datetime, g.venue_id,
                       v.latitude, v.longitude, v.timezone, v.roof_type, v.name AS venue_name
                FROM game g
                JOIN venues v ON g.venue_id = v.venue_id
                LEFT JOIN game_weather_15min gw15 ON g.game_pk = gw15.game_pk
                WHERE gw15.game_pk IS NULL
                  AND g.status = 'Final'
                  AND g.game_date::text >= %s
            """, (cutoff,))
            games = cur.fetchall()

        if not games:
            print("No recent games need 15-min weather data.")
            return

        dome_games = [g for g in games if g["roof_type"] == "Dome"]
        outdoor_games = [g for g in games if g["roof_type"] != "Dome"]

        dome_count = 0
        if dome_games:
            rows = []
            for g in dome_games:
                for m in range(MINUTES_PER_GAME):
                    rows.append((g["game_pk"], m * 15, None, None, None, None, None, None))
            with conn.cursor() as cur:
                psycopg2.extras.execute_batch(cur, UPSERT_WEATHER_15MIN, rows, page_size=1000)
            conn.commit()
            dome_count = len(dome_games)
            print(f"Dome stadiums: {dome_count} games ({dome_count * MINUTES_PER_GAME} rows) marked NULL")

        # Group by venue (all recent games at same venue use one API call)
        groups = defaultdict(list)
        for g in outdoor_games:
            groups[g["venue_id"]].append(g)

        total_venues = len(groups)
        if total_venues == 0:
            print(f"Done. {dome_count} dome games processed.")
            return

        total_games = 0
        total_failed = 0

        for idx, (venue_id, batch) in enumerate(sorted(groups.items()), 1):
            venue_name = batch[0]["venue_name"]
            lat, lon, tz = batch[0]["latitude"], batch[0]["longitude"], batch[0]["timezone"]

            if lat is None or lon is None:
                rows = []
                for g in batch:
                    for m in range(MINUTES_PER_GAME):
                        rows.append((g["game_pk"], m * 15, None, None, None, None, None, None))
                with conn.cursor() as cur:
                    psycopg2.extras.execute_batch(cur, UPSERT_WEATHER_15MIN, rows, page_size=1000)
                conn.commit()
                print(f"[{idx}/{total_venues}] {venue_name}: no coords, {len(batch)} games skipped")
                continue

            try:
                time.sleep(API_DELAY_SECONDS)
                resp = requests.get(WEATHER_FORECAST_API_URL, params={
                    "latitude": lat, "longitude": lon,
                    "minutely_15": MINUTELY_15_VARS,
                    "timezone": tz or "UTC",
                    "temperature_unit": "fahrenheit",
                    "wind_speed_unit": "mph",
                    "past_days": 3,
                    "forecast_days": 0,
                }, timeout=30)
                resp.raise_for_status()
                data = resp.json()
                m15 = data["minutely_15"]
                time_index = {t: i for i, t in enumerate(m15["time"])}

                rows = []
                games_filled = 0
                for g in batch:
                    local_start = _utc_to_local_hour(str(g["game_datetime"]), tz or "UTC")
                    if local_start is None:
                        for m in range(MINUTES_PER_GAME):
                            rows.append((g["game_pk"], m * 15, None, None, None, None, None, None))
                        continue

                    game_ok = False
                    for m in range(MINUTES_PER_GAME):
                        offset_minutes = m * 15
                        slot_dt = local_start + datetime.timedelta(minutes=offset_minutes)
                        key = slot_dt.strftime("%Y-%m-%dT%H:%M")
                        idx_m = time_index.get(key)
                        if idx_m is not None:
                            rows.append((
                                g["game_pk"], offset_minutes, key,
                                m15["temperature_2m"][idx_m],
                                m15["relative_humidity_2m"][idx_m],
                                m15["wind_speed_10m"][idx_m],
                                m15["wind_gusts_10m"][idx_m],
                                m15["precipitation"][idx_m],
                            ))
                            game_ok = True
                        else:
                            rows.append((g["game_pk"], offset_minutes, key, None, None, None, None, None))
                    if game_ok:
                        games_filled += 1

                with conn.cursor() as cur:
                    psycopg2.extras.execute_batch(cur, UPSERT_WEATHER_15MIN, rows, page_size=1000)
                conn.commit()
                total_games += games_filled
                print(f"[{idx}/{total_venues}] {venue_name}: {len(batch)} games, {len(rows)} 15-min rows")

            except Exception as e:
                conn.rollback()
                total_failed += len(batch)
                print(f"[{idx}/{total_venues}] {venue_name}: FAILED ({e})")

            time.sleep(API_DELAY_SECONDS)

        print(f"\n15-min weather done.")
        print(f"  Dome games (NULL): {dome_count}")
        print(f"  Games filled:      {total_games}")
        if total_failed:
            print(f"  Failed:            {total_failed}")
    finally:
        conn.close()


# ── CLI ──────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Daily MLB ingest to PostgreSQL.")
    parser.add_argument("years", nargs="*", type=int, help="Year(s) to ingest (default: current + prior)")
    parser.add_argument("--workers", type=int, default=MAX_WORKERS)
    args = parser.parse_args()

    today = datetime.date.today()
    years = args.years or sorted({today.year - 1, today.year})

    # Phase 1: Ingest games
    for year in years:
        print(f"\n{'=' * 60}")
        print(f"Fetching schedule for {year}...")
        schedule = fetch_schedule(year)
        print(f"Found {len(schedule)} completed games in {year}.")
        ingest_games(schedule, label=str(year), workers=args.workers)

    # Phase 2: Backfill venues
    print(f"\n{'=' * 60}")
    print("Backfilling venues...")
    backfill_venues()

    # Phase 3: Backfill weather
    print(f"\n{'=' * 60}")
    print("Backfilling weather...")
    backfill_weather()

    # Phase 4: Backfill hourly weather
    print(f"\n{'=' * 60}")
    print("Backfilling hourly weather...")
    backfill_weather_hourly()

    # Phase 5: Backfill 15-min weather (rolling 3-day window)
    print(f"\n{'=' * 60}")
    print("Backfilling 15-min weather...")
    backfill_weather_15min()

    print(f"\n{'=' * 60}")
    print("All phases complete.")


if __name__ == "__main__":
    main()
