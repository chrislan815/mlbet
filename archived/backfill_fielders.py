"""Backfill defensive fielder IDs on the pitches table.

Derives per-pitch defensive alignment from boxscore starting positions
and substitution events within each game. Fetches game feeds from the API
for games that have NULL fielder data.
"""

import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import statsapi

from db import get_connection, init_schema

API_DELAY_SECONDS = 0.5
MAX_WORKERS = 4

# Position code → fielder column mapping
# Boxscore position codes: 1=P, 2=C, 3=1B, 4=2B, 5=3B, 6=SS, 7=LF, 8=CF, 9=RF
POS_CODE_TO_FIELDER = {
    "2": "fielder_2",  # C
    "3": "fielder_3",  # 1B
    "4": "fielder_4",  # 2B
    "5": "fielder_5",  # 3B
    "6": "fielder_6",  # SS
    "7": "fielder_7",  # LF
    "8": "fielder_8",  # CF
    "9": "fielder_9",  # RF
}


def get_games_needing_fielders(conn):
    """Return game_pks where pitches have NULL fielder_2."""
    rows = conn.execute("""
        SELECT DISTINCT game_pk FROM pitches
        WHERE fielder_2 IS NULL
        ORDER BY game_pk
    """).fetchall()
    return [row["game_pk"] for row in rows]


def build_defense_timeline(game_data):
    """Build a mapping of (inning, inning_half, at_bat_index) → {fielder_2: id, ...}.

    Strategy:
    1. Get starting lineup from boxscore (who plays what position)
    2. Walk through all plays chronologically
    3. Track substitution events that change defensive positions
    4. Return the active defense for each at-bat
    """
    box = game_data.get("liveData", {}).get("boxscore", {}).get("teams", {})
    all_plays = game_data.get("liveData", {}).get("plays", {}).get("allPlays", [])

    # Build starting defense for each team
    # Home team fields when it's the top of an inning
    # Away team fields when it's the bottom of an inning
    home_defense = _extract_starting_defense(box.get("home", {}))
    away_defense = _extract_starting_defense(box.get("away", {}))

    # Walk through plays and track substitutions
    timeline = {}
    for play in all_plays:
        about = play.get("about", {})
        at_bat_index = about.get("atBatIndex")
        is_top = about.get("isTopInning", True)

        # Which team is fielding?
        current_defense = home_defense if is_top else away_defense

        # Check for substitution events in this play's events
        for event in play.get("playEvents", []):
            if event.get("type") == "action":
                desc = event.get("details", {}).get("description", "")
                event_type = event.get("details", {}).get("eventType", "")

                # Handle defensive switches and substitutions
                if event_type in ("defensive_switch", "defensive_sub",
                                  "pitching_substitution"):
                    player = event.get("player", {})
                    player_id = player.get("id")
                    if player_id:
                        # Try to figure out what position from the description
                        pos = _parse_position_from_event(event, game_data, player_id, is_top)
                        if pos and pos in POS_CODE_TO_FIELDER:
                            field_key = POS_CODE_TO_FIELDER[pos]
                            current_defense[field_key] = player_id
                        elif event_type == "pitching_substitution":
                            # Pitcher change doesn't affect fielder columns
                            pass

        # Record the defense state for this at-bat
        timeline[at_bat_index] = dict(current_defense)

    return timeline


def _extract_starting_defense(team_box):
    """Extract starting defensive positions from boxscore team data."""
    defense = {}
    players = team_box.get("players", {})

    for key, player_data in players.items():
        pos = player_data.get("position", {})
        pos_code = pos.get("code", "")
        player_id = player_data.get("person", {}).get("id")
        game_status = player_data.get("gameStatus", {})

        # Only include starters (not bench/bullpen)
        if pos_code in POS_CODE_TO_FIELDER and player_id:
            # Check if this player is a substitute (they'll be set later)
            if not game_status.get("isSubstitute", False):
                field_key = POS_CODE_TO_FIELDER[pos_code]
                defense[field_key] = player_id

    return defense


def _parse_position_from_event(event, game_data, player_id, is_top):
    """Try to determine the new position for a substituted player."""
    # Check the boxscore for this player's position
    box = game_data.get("liveData", {}).get("boxscore", {}).get("teams", {})
    team = box.get("home" if is_top else "away", {})
    players = team.get("players", {})
    player_data = players.get(f"ID{player_id}", {})

    if player_data:
        # Use allPositions if available (lists all positions played)
        all_positions = player_data.get("allPositions", [])
        if all_positions:
            # Last position in the list is typically the current one
            return all_positions[-1].get("code")
        return player_data.get("position", {}).get("code")

    return None


def fetch_game_data(game_pk):
    """Fetch game feed from API."""
    time.sleep(API_DELAY_SECONDS)
    return game_pk, statsapi.get("game", {"gamePk": game_pk})


def backfill_game(conn, game_pk, game_data):
    """Update fielder columns for all pitches in one game."""
    timeline = build_defense_timeline(game_data)

    cur = conn.cursor()
    updated = 0
    for at_bat_index, defense in timeline.items():
        if not defense:
            continue
        cur.execute("""
            UPDATE pitches SET
                fielder_2 = ?, fielder_3 = ?, fielder_4 = ?, fielder_5 = ?,
                fielder_6 = ?, fielder_7 = ?, fielder_8 = ?, fielder_9 = ?
            WHERE game_pk = ? AND at_bat_number = ?
        """, (
            defense.get("fielder_2"), defense.get("fielder_3"),
            defense.get("fielder_4"), defense.get("fielder_5"),
            defense.get("fielder_6"), defense.get("fielder_7"),
            defense.get("fielder_8"), defense.get("fielder_9"),
            game_pk, at_bat_index,
        ))
        updated += cur.rowcount

    return updated


def main():
    init_schema()
    conn = get_connection()
    try:
        game_pks = get_games_needing_fielders(conn)
        total = len(game_pks)

        if total == 0:
            print("All games already have fielder data.")
            return

        print(f"Backfilling fielders for {total} games...\n")

        total_updated = 0
        failed = 0

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = {pool.submit(fetch_game_data, gpk): gpk for gpk in game_pks}

            for idx, future in enumerate(as_completed(futures), 1):
                gpk = futures[future]
                try:
                    _, data = future.result()
                    updated = backfill_game(conn, gpk, data)
                    conn.commit()
                    total_updated += updated
                    if idx % 100 == 0 or idx == total:
                        print(f"[{idx}/{total}] {updated} pitches updated (total: {total_updated})")
                except Exception:
                    conn.rollback()
                    failed += 1
                    if idx % 100 == 0:
                        print(f"[{idx}/{total}] FAILED: {traceback.format_exc().strip().splitlines()[-1]}")

        print(f"\nDone.")
        print(f"  Games processed: {total - failed}")
        print(f"  Pitches updated: {total_updated}")
        if failed:
            print(f"  Failed games:    {failed}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
