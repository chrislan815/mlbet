"""ETL pipeline: fetch MLB game data from the Stats API and load into SQLite."""

import argparse
import sys
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import statsapi

from db import get_connection, init_schema

# Delay between API calls per thread to avoid rate limiting.
# With 4 workers × 0.5s delay = ~8 req/s max.
API_DELAY_SECONDS = 0.5
MAX_WORKERS = 4

# Game types to skip (spring training, exhibition)
SKIP_GAME_TYPES = {"S", "E"}


# ── SQL statements ──────────────────────────────────────────────────────────

UPSERT_PLAYER = """
INSERT OR REPLACE INTO players
    (player_id, full_name, first_name, last_name, primary_position,
     bat_side, pitch_hand, birth_date, height, weight, active)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

UPSERT_GAME = """
INSERT OR REPLACE INTO games
    (game_pk, game_date, game_year, game_type, home_team, away_team,
     home_score, away_score, venue_id, venue_name)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

UPSERT_PA = """
INSERT OR REPLACE INTO plate_appearances
    (game_pk, at_bat_number, pitcher, batter, event, event_type,
     description, rbi, is_out, inning, inning_half, bat_side, pitch_hand)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

UPSERT_PITCH = """
INSERT OR REPLACE INTO pitches
    (game_pk, at_bat_number, pitch_number, pitcher, batter,
     pitch_type, pitch_name,
     start_speed, end_speed,
     spin_rate, spin_direction,
     break_angle, break_length, break_vertical, break_vertical_induced,
     break_horizontal,
     plate_x, plate_z, zone, sz_top, sz_bot,
     release_x, release_y, release_z,
     vx0, vy0, vz0, ax, ay, az,
     pfx_x, pfx_z, extension, plate_time, type_confidence,
     call, call_code, description, is_in_play, is_strike, is_ball,
     fielder_2, fielder_3, fielder_4, fielder_5,
     fielder_6, fielder_7, fielder_8, fielder_9,
     inning, inning_half, balls, strikes, outs)
VALUES (?, ?, ?, ?, ?,
        ?, ?,
        ?, ?,
        ?, ?,
        ?, ?, ?, ?, ?,
        ?, ?, ?, ?, ?,
        ?, ?, ?,
        ?, ?, ?, ?, ?, ?,
        ?, ?, ?, ?, ?,
        ?, ?, ?, ?, ?, ?,
        ?, ?, ?, ?,
        ?, ?, ?, ?,
        ?, ?, ?, ?, ?)
"""

UPSERT_BATTED_BALL = """
INSERT OR REPLACE INTO batted_balls
    (game_pk, at_bat_number, launch_speed, launch_angle, total_distance,
     trajectory, hardness, hit_location, coord_x, coord_y)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

UPSERT_RUNNER = """
INSERT OR REPLACE INTO runners
    (game_pk, at_bat_number, runner_id, origin_base, start_base, end_base,
     out_base, is_out, out_number, event, event_type, movement_reason,
     is_scoring_event, rbi, earned, play_index)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

UPSERT_LINEUP = """
INSERT OR REPLACE INTO lineups
    (game_pk, team_type, batting_order, player_id, position)
VALUES (?, ?, ?, ?, ?)
"""

UPSERT_LOG = """
INSERT OR REPLACE INTO ingestion_log
    (game_pk, status, pitches_count, ingested_at, error_msg)
VALUES (?, ?, ?, ?, ?)
"""


# ── Extraction helpers ──────────────────────────────────────────────────────

def extract_players(game_data):
    """Yield player tuples from gameData.players."""
    for player in game_data.get("gameData", {}).get("players", {}).values():
        pos = player.get("primaryPosition", {})
        yield (
            player["id"],
            player.get("fullName"),
            player.get("firstName"),
            player.get("lastName"),
            pos.get("abbreviation"),
            player.get("batSide", {}).get("code"),
            player.get("pitchHand", {}).get("code"),
            player.get("birthDate"),
            player.get("height"),
            player.get("weight"),
            player.get("active"),
        )


def extract_game(game_pk, game_data):
    """Return a game tuple from gameData + liveData."""
    gd = game_data.get("gameData", {})
    ld = game_data.get("liveData", {})
    game_date = gd.get("datetime", {}).get("officialDate", "")
    linescore = ld.get("linescore", {}).get("teams", {})

    return (
        game_pk,
        game_date,
        int(game_date[:4]) if len(game_date) >= 4 else 0,
        gd.get("game", {}).get("type"),
        gd.get("teams", {}).get("home", {}).get("abbreviation"),
        gd.get("teams", {}).get("away", {}).get("abbreviation"),
        linescore.get("home", {}).get("runs"),
        linescore.get("away", {}).get("runs"),
        gd.get("venue", {}).get("id"),
        gd.get("venue", {}).get("name"),
    )


def extract_plays(game_pk, game_data):
    """Yield (pa_tuple, [pitch_tuples], batted_ball_tuple|None) per at-bat."""
    all_plays = game_data.get("liveData", {}).get("plays", {}).get("allPlays", [])

    for play in all_plays:
        about = play.get("about", {})
        matchup = play.get("matchup", {})
        result = play.get("result", {})
        at_bat_number = about.get("atBatIndex")
        pitcher_id = matchup.get("pitcher", {}).get("id")
        batter_id = matchup.get("batter", {}).get("id")
        inning = about.get("inning")
        inning_half = about.get("halfInning")

        pa = (
            game_pk, at_bat_number, pitcher_id, batter_id,
            result.get("event"), result.get("eventType"),
            result.get("description"), result.get("rbi"),
            result.get("isOut"), inning, inning_half,
            matchup.get("batSide", {}).get("code"),
            matchup.get("pitchHand", {}).get("code"),
        )

        pitches = []
        batted_ball = None

        for event in play.get("playEvents", []):
            if not event.get("isPitch"):
                continue

            details = event.get("details", {})
            pd = event.get("pitchData", {})
            brk = pd.get("breaks", {})
            coords = pd.get("coordinates", {})
            count = event.get("count", {})
            ptype = details.get("type", {})
            call = details.get("call", {})

            pitches.append((
                game_pk, at_bat_number, event.get("pitchNumber"),
                pitcher_id, batter_id,
                ptype.get("code"), ptype.get("description"),
                pd.get("startSpeed"), pd.get("endSpeed"),
                brk.get("spinRate"), brk.get("spinDirection"),
                brk.get("breakAngle"), brk.get("breakLength"),
                brk.get("breakVertical"), brk.get("breakVerticalInduced"),
                brk.get("breakHorizontal"),
                coords.get("pX"), coords.get("pZ"),
                pd.get("zone"), pd.get("strikeZoneTop"), pd.get("strikeZoneBottom"),
                coords.get("x0"), coords.get("y0"), coords.get("z0"),
                coords.get("vX0"), coords.get("vY0"), coords.get("vZ0"),
                coords.get("aX"), coords.get("aY"), coords.get("aZ"),
                coords.get("pfxX"), coords.get("pfxZ"),
                pd.get("extension"), pd.get("plateTime"), pd.get("typeConfidence"),
                call.get("description"), call.get("code"),
                details.get("description"),
                details.get("isInPlay", False),
                details.get("isStrike", False),
                details.get("isBall", False),
                # defensive fielders (populated by backfill script)
                None, None, None, None,  # C, 1B, 2B, 3B
                None, None, None, None,  # SS, LF, CF, RF
                inning, inning_half,
                count.get("balls"), count.get("strikes"), count.get("outs"),
            ))

            hit_data = event.get("hitData")
            if hit_data:
                bb_coords = hit_data.get("coordinates", {})
                batted_ball = (
                    game_pk, at_bat_number,
                    hit_data.get("launchSpeed"), hit_data.get("launchAngle"),
                    hit_data.get("totalDistance"), hit_data.get("trajectory"),
                    hit_data.get("hardness"), hit_data.get("location"),
                    bb_coords.get("coordX"), bb_coords.get("coordY"),
                )

        yield pa, pitches, batted_ball


def extract_runners(game_pk, game_data):
    """Yield runner tuples from allPlays[].runners[]."""
    all_plays = game_data.get("liveData", {}).get("plays", {}).get("allPlays", [])
    for play in all_plays:
        at_bat_number = play.get("about", {}).get("atBatIndex")
        for runner in play.get("runners", []):
            movement = runner.get("movement", {})
            details = runner.get("details", {})
            runner_info = details.get("runner", {})
            runner_id = runner_info.get("id")
            if runner_id is None:
                continue
            yield (
                game_pk,
                at_bat_number,
                runner_id,
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
                details.get("playIndex"),
            )


def extract_lineups(game_pk, game_data):
    """Yield lineup tuples from boxscore data."""
    box = game_data.get("liveData", {}).get("boxscore", {}).get("teams", {})
    for team_type in ("home", "away"):
        team = box.get(team_type, {})
        batting_order = team.get("battingOrder", [])
        players = team.get("players", {})
        for pid in batting_order:
            player_data = players.get(f"ID{pid}", {})
            position = player_data.get("position", {}).get("abbreviation")
            order = player_data.get("battingOrder")
            if order is not None:
                order = int(order)
            yield (game_pk, team_type, order, pid, position)


# ── API fetching (runs in thread pool) ────────────────────────────────────

def fetch_game_data(game_pk):
    """Fetch a single game's full feed from the API. Thread-safe."""
    time.sleep(API_DELAY_SECONDS)
    return game_pk, statsapi.get("game", {"gamePk": game_pk})


# ── DB writing (runs on main thread) ─────────────────────────────────────

def write_game(conn, game_pk, data):
    """Extract and write all records for one game. Returns pitch count."""
    cur = conn.cursor()

    for player_row in extract_players(data):
        cur.execute(UPSERT_PLAYER, player_row)

    cur.execute(UPSERT_GAME, extract_game(game_pk, data))

    pitch_count = 0
    for pa, pitches, batted_ball in extract_plays(game_pk, data):
        cur.execute(UPSERT_PA, pa)
        for pitch in pitches:
            cur.execute(UPSERT_PITCH, pitch)
            pitch_count += 1
        if batted_ball:
            cur.execute(UPSERT_BATTED_BALL, batted_ball)

    # Runners
    for runner_row in extract_runners(game_pk, data):
        cur.execute(UPSERT_RUNNER, runner_row)

    # Lineups
    for lineup_row in extract_lineups(game_pk, data):
        cur.execute(UPSERT_LINEUP, lineup_row)

    return pitch_count


def iso_now():
    return datetime.now(timezone.utc).isoformat()


# ── Core ingestion ────────────────────────────────────────────────────────

def get_completed_game_pks(conn):
    """Return a set of game_pks already successfully ingested."""
    rows = conn.execute(
        "SELECT game_pk FROM ingestion_log WHERE status = 'complete'"
    ).fetchall()
    return {row["game_pk"] for row in rows}


def ingest_games(schedule, label="", workers=MAX_WORKERS):
    """Ingest a list of (game_pk, game_date, away, home) tuples.

    Uses a thread pool to fetch API data in parallel, then writes
    to SQLite serially on the main thread.
    """
    conn = get_connection()
    try:
        already_done = get_completed_game_pks(conn)
        to_do = [g for g in schedule if g[0] not in already_done]
        total = len(to_do)
        skipped = len(schedule) - total

        if skipped:
            print(f"Skipping {skipped} already-ingested games.")
        if total == 0:
            print("Nothing to ingest.")
            return

        total_pitches = 0
        failed = 0
        game_info = {g[0]: (g[1], g[2], g[3]) for g in to_do}  # gpk → (date, away, home)

        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {
                pool.submit(fetch_game_data, gpk): gpk
                for gpk, _, _, _ in to_do
            }

            for idx, future in enumerate(as_completed(futures), 1):
                gpk = futures[future]
                gdate, away, home = game_info[gpk]

                try:
                    _, data = future.result()
                    pitch_count = write_game(conn, gpk, data)
                    conn.execute(UPSERT_LOG, (gpk, "complete", pitch_count, iso_now(), None))
                    conn.commit()
                    total_pitches += pitch_count
                    print(f"[{idx}/{total}] {gdate} {away} @ {home}: {pitch_count} pitches")
                except Exception:
                    conn.rollback()
                    err = traceback.format_exc()
                    try:
                        conn.execute(UPSERT_LOG, (gpk, "failed", None, iso_now(), err))
                        conn.commit()
                    except Exception:
                        pass
                    failed += 1
                    print(f"[{idx}/{total}] {gdate} {away} @ {home}: FAILED")
                    print(f"  {err.strip().splitlines()[-1]}")

        print(f"\nDone{' (' + label + ')' if label else ''}.")
        print(f"  Games ingested: {total - failed}")
        print(f"  Total pitches:  {total_pitches}")
        if failed:
            print(f"  Failed games:   {failed}")
    finally:
        conn.close()


# ── Schedule fetching ─────────────────────────────────────────────────────

def fetch_schedule(year):
    """Return list of (game_pk, game_date, away_team, home_team) for completed games."""
    time.sleep(API_DELAY_SECONDS)
    games = statsapi.schedule(start_date=f"{year}-01-01", end_date=f"{year}-12-31")
    return [
        (g["game_id"], g["game_date"], g.get("away_name", "???"), g.get("home_name", "???"))
        for g in games
        if g.get("status") == "Final" and g.get("game_type") not in SKIP_GAME_TYPES
    ]


def ingest_single_game(game_pk):
    """Ingest one game by its gamePk (for testing)."""
    conn = get_connection()
    try:
        _, data = fetch_game_data(game_pk)
        pitch_count = write_game(conn, game_pk, data)
        conn.execute(UPSERT_LOG, (game_pk, "complete", pitch_count, iso_now(), None))
        conn.commit()
        print(f"Game {game_pk}: {pitch_count} pitches ingested.")
    except Exception:
        conn.rollback()
        print(f"Game {game_pk}: FAILED")
        traceback.print_exc()
        sys.exit(1)
    finally:
        conn.close()


# ── CLI ───────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Ingest MLB pitch data from the Stats API.")
    parser.add_argument("years", nargs="*", type=int, help="Year or start/end range (e.g. 2024 or 2015 2024)")
    parser.add_argument("--game", type=int, help="Ingest a single game by gamePk (for testing)")
    parser.add_argument("--workers", type=int, default=MAX_WORKERS, help=f"Parallel fetch threads (default: {MAX_WORKERS})")
    args = parser.parse_args()

    init_schema()

    if args.game is not None:
        ingest_single_game(args.game)
        return

    if not args.years:
        parser.error("Provide at least one year, a year range, or --game GAMEPK.")

    if len(args.years) == 1:
        start_year = end_year = args.years[0]
    elif len(args.years) == 2:
        start_year, end_year = args.years
    else:
        parser.error("Provide one year or two years (start end).")

    for year in range(start_year, end_year + 1):
        print(f"\n{'=' * 60}")
        print(f"Fetching schedule for {year}...")
        schedule = fetch_schedule(year)
        print(f"Found {len(schedule)} completed games in {year}.")
        ingest_games(schedule, label=str(year), workers=args.workers)


if __name__ == "__main__":
    main()
