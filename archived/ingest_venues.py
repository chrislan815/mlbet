"""Backfill venue/stadium data from the MLB Stats API.

Finds all unique venue_ids in the games table, fetches detailed venue info
(location, field dimensions, roof type, etc.) from a game feed, and populates
the venues table.
"""

import time
import traceback

import statsapi

from db import get_connection, init_schema

API_DELAY_SECONDS = 0.5

UPSERT_VENUE = """
INSERT OR REPLACE INTO venues
    (venue_id, name, city, state, country, latitude, longitude, elevation,
     timezone, roof_type, turf_type, capacity,
     left_line, left_center, center, right_center, right_line)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""


def get_missing_venues(conn):
    """Return list of (venue_id, game_pk) for venues not yet in the venues table."""
    rows = conn.execute("""
        SELECT g.venue_id, MIN(g.game_pk) AS game_pk
        FROM games g
        LEFT JOIN venues v ON g.venue_id = v.venue_id
        WHERE g.venue_id IS NOT NULL
          AND v.venue_id IS NULL
        GROUP BY g.venue_id
    """).fetchall()
    return [(row["venue_id"], row["game_pk"]) for row in rows]


def extract_venue(data):
    """Extract a venue row tuple from game feed data."""
    venue = data["gameData"]["venue"]
    loc = venue.get("location", {})
    coords = loc.get("defaultCoordinates", {})
    fi = venue.get("fieldInfo", {})
    tz = venue.get("timeZone", {})

    return (
        venue["id"],
        venue["name"],
        loc.get("city"),
        loc.get("state"),
        loc.get("country"),
        coords.get("latitude"),
        coords.get("longitude"),
        loc.get("elevation"),
        tz.get("id"),
        fi.get("roofType"),
        fi.get("turfType"),
        fi.get("capacity"),
        fi.get("leftLine"),
        fi.get("leftCenter"),
        fi.get("center"),
        fi.get("rightCenter"),
        fi.get("rightLine"),
    )


def main():
    init_schema()

    conn = get_connection()
    try:
        missing = get_missing_venues(conn)
        total = len(missing)

        if total == 0:
            print("All venues already populated.")
            return

        print(f"Found {total} venues to backfill.\n")

        success = 0
        failed = 0

        for idx, (venue_id, game_pk) in enumerate(missing, 1):
            try:
                time.sleep(API_DELAY_SECONDS)
                data = statsapi.get("game", {"gamePk": game_pk})
                row = extract_venue(data)
                conn.execute(UPSERT_VENUE, row)
                conn.commit()
                success += 1

                name = row[1]
                lat = row[5]
                lon = row[6]
                roof = row[9]
                lc = row[13]
                ct = row[14]
                rc = row[15]

                lat_s = f"{lat}" if lat is not None else "?"
                lon_s = f"{lon}" if lon is not None else "?"
                roof_s = f"{roof} roof" if roof else "no roof info"
                dims = "/".join(
                    str(d) if d is not None else "?"
                    for d in (lc, ct, rc)
                )

                print(f"[{idx}/{total}] {name}: {lat_s}, {lon_s}, {roof_s}, {dims}")
            except Exception:
                failed += 1
                print(f"[{idx}/{total}] venue_id={venue_id} (game_pk={game_pk}): FAILED")
                print(f"  {traceback.format_exc().strip().splitlines()[-1]}")

        print(f"\nDone.")
        print(f"  Venues added: {success}")
        if failed:
            print(f"  Failed:       {failed}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
