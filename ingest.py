"""Ingest MLB game data into mlb.db.

Usage:
    python ingest.py 2025 2026    # Ingest missing games for these years
"""

import argparse
import sqlite3
import sys
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import statsapi

DB_PATH = Path(__file__).parent / "mlb.db"

API_DELAY_SECONDS = 0.5
MAX_WORKERS = 4
SKIP_GAME_TYPES = {"S", "E"}

# Position code -> field name mapping for defense tracking
POS_CODE_TO_FIELD = {
    "1": "pitcher",
    "2": "catcher",
    "3": "first",
    "4": "second",
    "5": "third",
    "6": "shortstop",
    "7": "left",
    "8": "center",
    "9": "right",
}


# ── DB connection ────────────────────────────────────────────────────────

def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


# ── SQL statements ───────────────────────────────────────────────────────

UPSERT_GAME = """
INSERT OR REPLACE INTO game
    (game_pk, game_date, game_datetime, game_num, game_type, status,
     doubleheader, current_inning, inning_state,
     home_team_id, home_team_name, home_score,
     home_probable_pitcher, home_pitcher_note,
     away_team_id, away_team_name, away_score,
     away_probable_pitcher, away_pitcher_note,
     venue_id, venue_name,
     winning_team, winning_pitcher, losing_team, losing_pitcher,
     save_pitcher, series_status, summary, national_broadcasts)
VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
"""

UPSERT_PLAYER = """
INSERT OR REPLACE INTO player
    (id, fullName, link, firstName, lastName, birthDate, currentAge,
     birthCity, birthStateProvince, birthCountry, height, weight, active,
     useName, useLastName, middleName, boxscoreName, gender,
     isPlayer, isVerified, draftYear,
     batSide_code, batSide_description,
     pitchHand_code, pitchHand_description,
     nameFirstLast, nameSlug, firstLastName, lastFirstName,
     lastInitName, initLastName, fullFMLName, fullLFMName,
     strikeZoneTop, strikeZoneBottom,
     primaryPosition_code, primaryPosition_name,
     primaryPosition_type, primaryPosition_abbreviation)
VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
"""

UPSERT_ATBAT = """
INSERT OR REPLACE INTO atbat
    (game_pk, result_type, result_event, result_eventType, result_description,
     result_rbi, result_awayScore, result_homeScore, result_isOut,
     about_atBatIndex, about_halfInning, about_isTopInning, about_inning,
     about_startTime, about_endTime, about_isComplete,
     about_isScoringPlay, about_hasReview, about_hasOut, about_captivatingIndex,
     count_balls, count_strikes, count_outs,
     matchup_batter_id, matchup_batter_fullName, matchup_batter_link,
     matchup_batSide_code, matchup_batSide_description,
     matchup_pitcher_id, matchup_pitcher_fullName, matchup_pitcher_link,
     matchup_pitchHand_code, matchup_pitchHand_description,
     matchup_splits_batter, matchup_splits_pitcher, matchup_splits_menOnBase,
     homeTeamWinProbability, homeTeamWinProbabilityAdded,
     playEndTime, pitchIndex, actionIndex, runnerIndex)
VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
"""

UPSERT_PLAY_EVENT = """
INSERT OR REPLACE INTO play_event
    (game_pk, about_atBatIndex,
     details_call_code, details_call_description, details_description,
     details_code, details_ballColor, details_trailColor,
     details_isInPlay, details_isStrike, details_isBall,
     details_type_code, details_type_description,
     details_isOut, details_hasReview,
     count_balls, count_strikes, count_outs,
     preCount_balls, preCount_strikes, preCount_outs,
     pitchData_startSpeed, pitchData_endSpeed,
     pitchData_strikeZoneTop, pitchData_strikeZoneBottom,
     pitchData_coordinates_aY, pitchData_coordinates_aZ,
     pitchData_coordinates_pfxX, pitchData_coordinates_pfxZ,
     pitchData_coordinates_pX, pitchData_coordinates_pZ,
     pitchData_coordinates_vX0, pitchData_coordinates_vY0, pitchData_coordinates_vZ0,
     pitchData_coordinates_x, pitchData_coordinates_y,
     pitchData_coordinates_x0, pitchData_coordinates_y0, pitchData_coordinates_z0,
     pitchData_coordinates_aX,
     pitchData_breaks_breakAngle, pitchData_breaks_breakLength,
     pitchData_breaks_breakY,
     pitchData_breaks_breakVertical, pitchData_breaks_breakVerticalInduced,
     pitchData_breaks_breakHorizontal,
     pitchData_breaks_spinRate, pitchData_breaks_spinDirection,
     pitchData_zone, pitchData_typeConfidence,
     pitchData_plateTime, pitchData_extension,
     [index], playId, pitchNumber, startTime, endTime, isPitch, type,
     defense_pitcher_id, defense_pitcher_link,
     defense_pitcher_pitchHand_code, defense_pitcher_pitchHand_description,
     defense_catcher_id, defense_catcher_link,
     defense_first_id, defense_first_link,
     defense_second_id, defense_second_link,
     defense_third_id, defense_third_link,
     defense_shortstop_id, defense_shortstop_link,
     defense_left_id, defense_left_link,
     defense_center_id, defense_center_link,
     defense_right_id, defense_right_link,
     offense_batter_id, offense_batter_link,
     offense_batter_batSide_code, offense_batter_batSide_description,
     offense_batterPosition_code, offense_batterPosition_name,
     offense_batterPosition_type, offense_batterPosition_abbreviation,
     offense_first_id, offense_first_link,
     offense_second_id, offense_second_link,
     offense_third_id, offense_third_link,
     hitData_launchSpeed, hitData_launchAngle, hitData_totalDistance,
     hitData_trajectory, hitData_hardness, hitData_location,
     hitData_coordinates_coordX, hitData_coordinates_coordY)
VALUES ({})
""".format(",".join(["?"] * 101))

UPSERT_RUNNER = """
INSERT OR REPLACE INTO runner
    (game_pk, about_atBatIndex,
     movement_originBase, movement_start, movement_end,
     movement_outBase, movement_isOut, movement_outNumber,
     details_event, details_eventType, details_movementReason,
     details_isScoringEvent, details_rbi, details_earned,
     details_teamUnearned, details_playIndex,
     runner_id, runner_fullName, runner_link)
VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
"""

UPSERT_LINEUP = """
INSERT OR REPLACE INTO lineup
    (game_pk, team_type, batting_order, player_id, player_name, player_link,
     parent_team_id, position_code, position_abbreviation, position_name,
     position_type, parentTeamId)
VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
"""


# ── Defense tracking ─────────────────────────────────────────────────────

def _extract_starting_defense(team_box, game_data_players):
    """Build starting defense dict from boxscore team data.

    Returns dict with keys like pitcher_id, pitcher_link, catcher_id, etc.
    """
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

            # For pitcher, also store pitchHand
            if pos_code == "1":
                p_info = game_data_players.get(f"ID{player_id}", {})
                defense["pitcher_pitchHand_code"] = p_info.get("pitchHand", {}).get("code")
                defense["pitcher_pitchHand_description"] = p_info.get("pitchHand", {}).get("description")

    return defense


def _update_defense_from_event(defense, event, game_data, is_top):
    """Update defense dict based on a substitution event."""
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
    """Build per-atBatIndex defense dicts for the fielding team."""
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
    """Determine who is on each base at the time of a given pitch event.

    Walks through runners[] entries with playIndex < event_index to build state.
    """
    bases = {"1B": None, "2B": None, "3B": None}

    for runner in play.get("runners", []):
        r_play_index = runner.get("details", {}).get("playIndex")
        if r_play_index is not None and r_play_index >= event_index:
            continue

        movement = runner.get("movement", {})
        runner_info = runner.get("details", {}).get("runner", {})
        runner_id = runner_info.get("id")
        runner_link = runner_info.get("link")

        # Remove from start base
        start = movement.get("start")
        if start and start in bases:
            if bases[start] and bases[start][0] == runner_id:
                bases[start] = None

        # Place on end base
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
    """Look up batter position from the boxscore."""
    box = game_data.get("liveData", {}).get("boxscore", {}).get("teams", {})
    # Batter is on the team that is batting: away bats in top, home bats in bottom
    team = box.get("away" if is_top else "home", {})
    player_data = team.get("players", {}).get(f"ID{batter_id}", {})
    pos = player_data.get("position", {})
    return {
        "code": pos.get("code"),
        "name": pos.get("name"),
        "type": pos.get("type"),
        "abbreviation": pos.get("abbreviation"),
    }


# ── Extraction helpers ───────────────────────────────────────────────────

def extract_game_row(g):
    """Build game row from a statsapi.schedule() entry."""
    return (
        g["game_id"],
        g["game_date"],
        g["game_datetime"],
        g["game_num"],
        g["game_type"],
        g["status"],
        g["doubleheader"],
        g.get("current_inning"),
        g.get("inning_state"),
        g["home_id"],
        g["home_name"],
        g["home_score"],
        g.get("home_probable_pitcher"),
        g.get("home_pitcher_note"),
        g["away_id"],
        g["away_name"],
        g["away_score"],
        g.get("away_probable_pitcher"),
        g.get("away_pitcher_note"),
        g["venue_id"],
        g["venue_name"],
        g.get("winning_team"),
        g.get("winning_pitcher"),
        g.get("losing_team"),
        g.get("losing_pitcher"),
        g.get("save_pitcher"),
        g.get("series_status"),
        g.get("summary"),
        ", ".join(g.get("national_broadcasts") or []),
    )


def extract_players(game_data):
    """Yield player row tuples from gameData.players."""
    for p in game_data.get("gameData", {}).get("players", {}).values():
        yield (
            p["id"],
            p.get("fullName"),
            p.get("link"),
            p.get("firstName"),
            p.get("lastName"),
            p.get("birthDate"),
            p.get("currentAge"),
            p.get("birthCity"),
            p.get("birthStateProvince"),
            p.get("birthCountry"),
            p.get("height"),
            p.get("weight"),
            p.get("active"),
            p.get("useName"),
            p.get("useLastName"),
            p.get("middleName"),
            p.get("boxscoreName"),
            p.get("gender"),
            p.get("isPlayer"),
            p.get("isVerified"),
            p.get("draftYear"),
            p.get("batSide", {}).get("code"),
            p.get("batSide", {}).get("description"),
            p.get("pitchHand", {}).get("code"),
            p.get("pitchHand", {}).get("description"),
            p.get("nameFirstLast"),
            p.get("nameSlug"),
            p.get("firstLastName"),
            p.get("lastFirstName"),
            p.get("lastInitName"),
            p.get("initLastName"),
            p.get("fullFMLName"),
            p.get("fullLFMName"),
            p.get("strikeZoneTop"),
            p.get("strikeZoneBottom"),
            p.get("primaryPosition", {}).get("code"),
            p.get("primaryPosition", {}).get("name"),
            p.get("primaryPosition", {}).get("type"),
            p.get("primaryPosition", {}).get("abbreviation"),
        )


def extract_atbats(game_pk, game_data):
    """Yield atbat row tuples from liveData.plays.allPlays."""
    all_plays = game_data.get("liveData", {}).get("plays", {}).get("allPlays", [])

    for play in all_plays:
        about = play.get("about", {})
        result = play.get("result", {})
        matchup = play.get("matchup", {})
        count = play.get("count", {})

        yield (
            game_pk,
            result.get("type"),
            result.get("event"),
            result.get("eventType"),
            result.get("description"),
            result.get("rbi"),
            result.get("awayScore"),
            result.get("homeScore"),
            result.get("isOut"),
            about.get("atBatIndex"),
            about.get("halfInning"),
            about.get("isTopInning"),
            about.get("inning"),
            about.get("startTime"),
            about.get("endTime"),
            about.get("isComplete"),
            about.get("isScoringPlay"),
            about.get("hasReview"),
            about.get("hasOut"),
            about.get("captivatingIndex"),
            count.get("balls"),
            count.get("strikes"),
            count.get("outs"),
            matchup.get("batter", {}).get("id"),
            matchup.get("batter", {}).get("fullName"),
            matchup.get("batter", {}).get("link"),
            matchup.get("batSide", {}).get("code"),
            matchup.get("batSide", {}).get("description"),
            matchup.get("pitcher", {}).get("id"),
            matchup.get("pitcher", {}).get("fullName"),
            matchup.get("pitcher", {}).get("link"),
            matchup.get("pitchHand", {}).get("code"),
            matchup.get("pitchHand", {}).get("description"),
            matchup.get("splits", {}).get("batter"),
            matchup.get("splits", {}).get("pitcher"),
            matchup.get("splits", {}).get("menOnBase"),
            play.get("homeTeamWinProbability"),
            play.get("homeTeamWinProbabilityAdded"),
            play.get("playEndTime"),
            str(play.get("pitchIndex", [])),
            str(play.get("actionIndex", [])),
            str(play.get("runnerIndex", [])),
        )


def extract_play_events(game_pk, game_data, defense_timeline):
    """Yield play_event row tuples (101 cols) from playEvents."""
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
                # Still track count for preCount
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

            # Set preCount from previous pitch state
            pre_balls = prev_balls
            pre_strikes = prev_strikes
            pre_outs = prev_outs if prev_outs is not None else cnt.get("outs")

            # Update prev for next pitch
            prev_balls = cnt.get("balls", 0)
            prev_strikes = cnt.get("strikes", 0)
            prev_outs = cnt.get("outs")

            yield (
                game_pk,
                at_bat_index,
                # details
                call.get("code"),
                call.get("description"),
                details.get("description"),
                details.get("code"),
                details.get("ballColor"),
                details.get("trailColor"),
                details.get("isInPlay", False),
                details.get("isStrike", False),
                details.get("isBall", False),
                ptype.get("code"),
                ptype.get("description"),
                details.get("isOut", False),
                details.get("hasReview", False),
                # count
                cnt.get("balls"),
                cnt.get("strikes"),
                cnt.get("outs"),
                pre_balls,
                pre_strikes,
                pre_outs,
                # pitchData
                pd_data.get("startSpeed"),
                pd_data.get("endSpeed"),
                pd_data.get("strikeZoneTop"),
                pd_data.get("strikeZoneBottom"),
                coords.get("aY"),
                coords.get("aZ"),
                coords.get("pfxX"),
                coords.get("pfxZ"),
                coords.get("pX"),
                coords.get("pZ"),
                coords.get("vX0"),
                coords.get("vY0"),
                coords.get("vZ0"),
                coords.get("x"),
                coords.get("y"),
                coords.get("x0"),
                coords.get("y0"),
                coords.get("z0"),
                coords.get("aX"),
                # breaks
                brk.get("breakAngle"),
                brk.get("breakLength"),
                brk.get("breakY"),
                brk.get("breakVertical"),
                brk.get("breakVerticalInduced"),
                brk.get("breakHorizontal"),
                brk.get("spinRate"),
                brk.get("spinDirection"),
                # more pitchData
                pd_data.get("zone"),
                pd_data.get("typeConfidence"),
                pd_data.get("plateTime"),
                pd_data.get("extension"),
                # event metadata
                ev.get("index"),
                ev.get("playId"),
                ev.get("pitchNumber"),
                ev.get("startTime"),
                ev.get("endTime"),
                ev.get("isPitch"),
                ev.get("type"),
                # defense (9 positions + pitcher details)
                defense.get("pitcher_id"),
                defense.get("pitcher_link"),
                defense.get("pitcher_pitchHand_code"),
                defense.get("pitcher_pitchHand_description"),
                defense.get("catcher_id"),
                defense.get("catcher_link"),
                defense.get("first_id"),
                defense.get("first_link"),
                defense.get("second_id"),
                defense.get("second_link"),
                defense.get("third_id"),
                defense.get("third_link"),
                defense.get("shortstop_id"),
                defense.get("shortstop_link"),
                defense.get("left_id"),
                defense.get("left_link"),
                defense.get("center_id"),
                defense.get("center_link"),
                defense.get("right_id"),
                defense.get("right_link"),
                # offense - batter
                matchup.get("batter", {}).get("id"),
                matchup.get("batter", {}).get("link"),
                matchup.get("batSide", {}).get("code"),
                matchup.get("batSide", {}).get("description"),
                # batter position
                batter_pos.get("code"),
                batter_pos.get("name"),
                batter_pos.get("type"),
                batter_pos.get("abbreviation"),
                # offense - baserunners
                runners.get("first_id"),
                runners.get("first_link"),
                runners.get("second_id"),
                runners.get("second_link"),
                runners.get("third_id"),
                runners.get("third_link"),
                # hit data
                hit.get("launchSpeed"),
                hit.get("launchAngle"),
                hit.get("totalDistance"),
                hit.get("trajectory"),
                hit.get("hardness"),
                hit.get("location"),
                hit_coords.get("coordX"),
                hit_coords.get("coordY"),
            )


def extract_runners(game_pk, game_data):
    """Yield runner row tuples from allPlays[].runners[]."""
    all_plays = game_data.get("liveData", {}).get("plays", {}).get("allPlays", [])

    for play in all_plays:
        at_bat_index = play.get("about", {}).get("atBatIndex")
        for runner in play.get("runners", []):
            movement = runner.get("movement", {})
            details = runner.get("details", {})
            runner_info = details.get("runner", {})

            yield (
                game_pk,
                at_bat_index,
                movement.get("originBase"),
                movement.get("start"),
                movement.get("end"),
                movement.get("outBase"),
                movement.get("isOut"),
                movement.get("outNumber"),
                details.get("event"),
                details.get("eventType"),
                details.get("movementReason"),
                details.get("isScoringEvent"),
                details.get("rbi"),
                details.get("earned"),
                details.get("teamUnearned"),
                details.get("playIndex"),
                runner_info.get("id"),
                runner_info.get("fullName"),
                runner_info.get("link"),
            )


def extract_lineups(game_pk, game_data):
    """Yield lineup row tuples from boxscore. game_pk is stored as TEXT."""
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
                str(game_pk),  # TEXT in old schema
                team_type,
                order,
                pid,
                person.get("fullName", ""),
                person.get("link"),
                team_id,
                pos.get("code"),
                pos.get("abbreviation"),
                pos.get("name"),
                pos.get("type"),
                team_id,  # parentTeamId duplicate
            )


# ── API fetching (thread pool) ───────────────────────────────────────────

def fetch_game_data(game_pk):
    """Fetch a single game's full feed. Thread-safe."""
    time.sleep(API_DELAY_SECONDS)
    return game_pk, statsapi.get("game", {"gamePk": game_pk})


# ── DB writing (main thread) ─────────────────────────────────────────────

def write_game(conn, game_pk, schedule_entry, game_data):
    """Write all records for one game. Returns pitch count."""
    cur = conn.cursor()

    # game row from schedule entry
    cur.execute(UPSERT_GAME, extract_game_row(schedule_entry))

    # players
    for row in extract_players(game_data):
        cur.execute(UPSERT_PLAYER, row)

    # atbats
    for row in extract_atbats(game_pk, game_data):
        cur.execute(UPSERT_ATBAT, row)

    # play_events (with defense)
    defense_timeline = build_defense_timeline(game_data)
    pitch_count = 0
    for row in extract_play_events(game_pk, game_data, defense_timeline):
        cur.execute(UPSERT_PLAY_EVENT, row)
        pitch_count += 1

    # runners
    for row in extract_runners(game_pk, game_data):
        cur.execute(UPSERT_RUNNER, row)

    # lineups
    for row in extract_lineups(game_pk, game_data):
        cur.execute(UPSERT_LINEUP, row)

    return pitch_count


# ── Schedule fetching ────────────────────────────────────────────────────

def fetch_schedule(year):
    """Return list of schedule dicts for completed, non-spring/exhibition games."""
    time.sleep(API_DELAY_SECONDS)
    games = statsapi.schedule(start_date=f"{year}-01-01", end_date=f"{year}-12-31")
    return [
        g for g in games
        if g.get("status") == "Final" and g.get("game_type") not in SKIP_GAME_TYPES
    ]


# ── Core ingestion ───────────────────────────────────────────────────────

def get_existing_game_pks(conn):
    """Return set of game_pks already in the old DB's game table."""
    rows = conn.execute("SELECT game_pk FROM game").fetchall()
    return {row["game_pk"] for row in rows}


def ingest_games(schedule_entries, label="", workers=MAX_WORKERS):
    """Ingest a list of schedule entries into the old DB.

    Parallel API fetching with serial DB writes.
    """
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

        # Index schedule entries by game_pk for quick lookup
        schedule_by_pk = {g["game_id"]: g for g in to_do}

        total_pitches = 0
        failed = 0

        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {
                pool.submit(fetch_game_data, g["game_id"]): g["game_id"]
                for g in to_do
            }

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


# ── CLI ──────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Ingest MLB game data into the old database at ~/Downloads/mlb.db."
    )
    parser.add_argument(
        "years", nargs="+", type=int,
        help="Year(s) to ingest (e.g. 2025 2026)"
    )
    parser.add_argument(
        "--workers", type=int, default=MAX_WORKERS,
        help=f"Parallel fetch threads (default: {MAX_WORKERS})"
    )
    args = parser.parse_args()

    if not DB_PATH.exists():
        print(f"Error: database not found at {DB_PATH}")
        sys.exit(1)

    for year in args.years:
        print(f"\n{'=' * 60}")
        print(f"Fetching schedule for {year}...")
        schedule = fetch_schedule(year)
        print(f"Found {len(schedule)} completed games in {year}.")
        ingest_games(schedule, label=str(year), workers=args.workers)


if __name__ == "__main__":
    main()
