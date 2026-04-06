"""Fetch historical weather data from Open-Meteo and store in SQLite.

Batches API calls by (venue, year) instead of per-game, reducing ~25k calls
to ~350 calls. Dome stadiums get NULL weather rows (marked as processed).
"""

import sys
import time
from collections import defaultdict

import requests

from db import get_connection, init_schema

API_URL = "https://archive-api.open-meteo.com/v1/archive"
API_DELAY_SECONDS = 0.5

HOURLY_VARIABLES = ",".join([
    "temperature_2m",
    "relative_humidity_2m",
    "wind_speed_10m",
    "wind_direction_10m",
    "wind_gusts_10m",
    "precipitation",
    "pressure_msl",
    "cloud_cover",
    "weather_code",
])

# Default game hour (local time) when exact start time is unavailable.
DEFAULT_GAME_HOUR = 19

UPSERT_WEATHER = """
INSERT OR REPLACE INTO game_weather
    (game_pk, temperature_f, humidity, wind_speed_mph, wind_direction,
     wind_gusts_mph, precipitation_mm, pressure_hpa, cloud_cover, weather_code)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""


# -- Data fetching -----------------------------------------------------------

def get_games_missing_weather(conn, year_start=None, year_end=None):
    """Return games that have no weather row yet, joined with venue info."""
    sql = """
        SELECT g.game_pk, g.game_date, g.venue_id,
               v.latitude, v.longitude, v.timezone, v.roof_type, v.name AS venue_name
        FROM games g
        JOIN venues v ON g.venue_id = v.venue_id
        LEFT JOIN game_weather gw ON g.game_pk = gw.game_pk
        WHERE gw.game_pk IS NULL
    """
    params = []
    if year_start is not None and year_end is not None:
        sql += " AND g.game_year BETWEEN ? AND ?"
        params.extend([year_start, year_end])
    elif year_start is not None:
        sql += " AND g.game_year = ?"
        params.append(year_start)

    return conn.execute(sql, params).fetchall()


def group_by_venue_year(games):
    """Group games into {(venue_id, year): [game_rows]} buckets."""
    groups = defaultdict(list)
    for game in games:
        year = game["game_date"][:4]
        groups[(game["venue_id"], year)].append(game)
    return groups


def fetch_weather(lat, lon, start_date, end_date, tz):
    """Call Open-Meteo archive API for a date range. Returns parsed JSON."""
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": HOURLY_VARIABLES,
        "timezone": tz,
        "temperature_unit": "fahrenheit",
        "wind_speed_unit": "mph",
    }
    resp = requests.get(API_URL, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def build_time_index(hourly_data):
    """Map ISO timestamp strings to their array index."""
    return {t: i for i, t in enumerate(hourly_data["time"])}


def extract_weather_at_hour(hourly_data, time_index, game_date, hour=DEFAULT_GAME_HOUR):
    """Pull weather values for a specific date+hour. Returns tuple or None."""
    key = f"{game_date}T{hour:02d}:00"
    idx = time_index.get(key)
    if idx is None:
        return None

    return (
        hourly_data["temperature_2m"][idx],
        hourly_data["relative_humidity_2m"][idx],
        hourly_data["wind_speed_10m"][idx],
        hourly_data["wind_direction_10m"][idx],
        hourly_data["wind_gusts_10m"][idx],
        hourly_data["precipitation"][idx],
        hourly_data["pressure_msl"][idx],
        hourly_data["cloud_cover"][idx],
        hourly_data["weather_code"][idx],
    )


# -- Ingestion ---------------------------------------------------------------

def process_dome_games(conn, games):
    """Insert NULL-weather rows for dome stadium games."""
    cur = conn.cursor()
    for game in games:
        cur.execute(
            "INSERT OR REPLACE INTO game_weather (game_pk) VALUES (?)",
            (game["game_pk"],),
        )
    conn.commit()
    return len(games)


def process_venue_year(conn, venue_name, games):
    """Fetch weather for one (venue, year) batch and insert rows.

    Returns (inserted_count, skipped_count).
    """
    sample = games[0]
    lat, lon, tz = sample["latitude"], sample["longitude"], sample["timezone"]

    dates = sorted(g["game_date"] for g in games)
    min_date, max_date = dates[0], dates[-1]

    data = fetch_weather(lat, lon, min_date, max_date, tz)
    hourly = data["hourly"]
    time_index = build_time_index(hourly)

    cur = conn.cursor()
    inserted = 0
    skipped = 0

    for game in games:
        weather = extract_weather_at_hour(hourly, time_index, game["game_date"])
        if weather is not None:
            cur.execute(UPSERT_WEATHER, (game["game_pk"], *weather))
            inserted += 1
        else:
            # Timestamp not found in response -- insert NULLs so game is marked processed
            cur.execute(
                "INSERT OR REPLACE INTO game_weather (game_pk) VALUES (?)",
                (game["game_pk"],),
            )
            skipped += 1

    conn.commit()
    return inserted, skipped


def ingest_weather(year_start=None, year_end=None):
    """Main entry point: fetch and store weather for games missing it."""
    init_schema()
    conn = get_connection()

    try:
        games = get_games_missing_weather(conn, year_start, year_end)
        if not games:
            print("No games need weather data.")
            return

        # Separate dome games from outdoor/retractable
        dome_games = [g for g in games if g["roof_type"] == "Dome"]
        outdoor_games = [g for g in games if g["roof_type"] != "Dome"]

        # Process dome games first (no API call needed)
        dome_count = 0
        if dome_games:
            dome_count = process_dome_games(conn, dome_games)
            print(f"Dome stadiums: {dome_count} games marked with NULL weather")

        # Group outdoor games by (venue_id, year)
        groups = group_by_venue_year(outdoor_games)
        total_batches = len(groups)

        if total_batches == 0 and dome_count > 0:
            print(f"\nDone. {dome_count} dome games processed, 0 outdoor batches.")
            return

        total_inserted = 0
        total_skipped = 0
        total_failed = 0
        failed_batches = []

        for idx, ((venue_id, year), batch_games) in enumerate(sorted(groups.items()), 1):
            venue_name = batch_games[0]["venue_name"]
            try:
                inserted, skipped = process_venue_year(conn, venue_name, batch_games)
                total_inserted += inserted
                total_skipped += skipped
                print(f"[{idx}/{total_batches}] {venue_name} {year}: "
                      f"{len(batch_games)} games, {inserted} weather rows")
            except Exception as exc:
                conn.rollback()
                total_failed += len(batch_games)
                failed_batches.append((venue_name, year))
                print(f"[{idx}/{total_batches}] {venue_name} {year}: "
                      f"FAILED ({exc})")

            time.sleep(API_DELAY_SECONDS)

        # Summary
        print(f"\nDone.")
        print(f"  Dome games (NULL weather): {dome_count}")
        print(f"  Weather rows inserted:     {total_inserted}")
        if total_skipped:
            print(f"  Timestamp mismatches:      {total_skipped}")
        if total_failed:
            print(f"  Failed games:              {total_failed}")
            for name, yr in failed_batches:
                print(f"    - {name} {yr}")

    finally:
        conn.close()


# -- CLI ---------------------------------------------------------------------

def main():
    args = sys.argv[1:]

    if len(args) == 0:
        ingest_weather()
    elif len(args) == 1:
        year = int(args[0])
        ingest_weather(year_start=year, year_end=year)
    elif len(args) == 2:
        start, end = int(args[0]), int(args[1])
        ingest_weather(year_start=start, year_end=end)
    else:
        print("Usage: python ingest_weather.py [year] [end_year]")
        print("  python ingest_weather.py              # All games missing weather")
        print("  python ingest_weather.py 2024          # One year")
        print("  python ingest_weather.py 2015 2024     # Year range")
        sys.exit(1)


if __name__ == "__main__":
    main()
