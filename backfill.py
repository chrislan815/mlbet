"""Backfill venues and weather data into mlb.db."""

import sqlite3
import time
from pathlib import Path
from collections import defaultdict

import requests
import statsapi

DB_PATH = Path(__file__).parent / "mlb.db"
API_DELAY = 0.5

WEATHER_API_URL = "https://archive-api.open-meteo.com/v1/archive"
HOURLY_VARS = "temperature_2m,relative_humidity_2m,wind_speed_10m,wind_direction_10m,wind_gusts_10m,precipitation,pressure_msl,cloud_cover,weather_code"
DEFAULT_GAME_HOUR = 19


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


# ── Venue backfill ────────────────────────────────────────────────────────

def backfill_venues():
    conn = get_conn()
    try:
        missing = conn.execute("""
            SELECT g.venue_id, MIN(g.game_pk) AS game_pk
            FROM game g
            LEFT JOIN venues v ON g.venue_id = v.venue_id
            WHERE g.venue_id IS NOT NULL AND v.venue_id IS NULL
            GROUP BY g.venue_id
        """).fetchall()

        if not missing:
            print("All venues already populated.")
            return

        print(f"Backfilling {len(missing)} venues...\n")
        for idx, row in enumerate(missing, 1):
            venue_id, game_pk = row["venue_id"], row["game_pk"]
            try:
                time.sleep(API_DELAY)
                data = statsapi.get("game", {"gamePk": game_pk})
                venue = data["gameData"]["venue"]
                loc = venue.get("location", {})
                coords = loc.get("defaultCoordinates", {})
                fi = venue.get("fieldInfo", {})
                tz = venue.get("timeZone", {})

                conn.execute("""
                    INSERT OR REPLACE INTO venues
                    (venue_id, name, city, state, country, latitude, longitude, elevation,
                     timezone, roof_type, turf_type, capacity,
                     left_line, left_center, center, right_center, right_line)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    venue["id"], venue["name"],
                    loc.get("city"), loc.get("state"), loc.get("country"),
                    coords.get("latitude"), coords.get("longitude"),
                    loc.get("elevation"), tz.get("id"),
                    fi.get("roofType"), fi.get("turfType"), fi.get("capacity"),
                    fi.get("leftLine"), fi.get("leftCenter"),
                    fi.get("center"), fi.get("rightCenter"), fi.get("rightLine"),
                ))
                conn.commit()
                lat = coords.get("latitude", "?")
                lon = coords.get("longitude", "?")
                print(f"[{idx}/{len(missing)}] {venue['name']}: {lat}, {lon}")
            except Exception as e:
                print(f"[{idx}/{len(missing)}] venue_id={venue_id}: FAILED ({e})")

        print(f"\nVenues done. Total: {conn.execute('SELECT COUNT(*) FROM venues').fetchone()[0]}")
    finally:
        conn.close()


# ── Weather backfill ──────────────────────────────────────────────────────

def backfill_weather():
    conn = get_conn()
    try:
        # Note: old DB uses 'game' not 'games', and 'game_pk' column exists
        games = conn.execute("""
            SELECT g.game_pk, g.game_date, g.venue_id,
                   v.latitude, v.longitude, v.timezone, v.roof_type, v.name AS venue_name
            FROM game g
            JOIN venues v ON g.venue_id = v.venue_id
            LEFT JOIN game_weather gw ON g.game_pk = gw.game_pk
            WHERE gw.game_pk IS NULL
              AND g.status = 'Final'
        """).fetchall()

        if not games:
            print("No games need weather data.")
            return

        # Dome games — insert NULLs
        dome_games = [g for g in games if g["roof_type"] == "Dome"]
        outdoor_games = [g for g in games if g["roof_type"] != "Dome"]

        dome_count = 0
        if dome_games:
            for g in dome_games:
                conn.execute("INSERT OR REPLACE INTO game_weather (game_pk) VALUES (?)", (g["game_pk"],))
            conn.commit()
            dome_count = len(dome_games)
            print(f"Dome stadiums: {dome_count} games marked with NULL weather")

        # Group outdoor games by (venue_id, year)
        groups = defaultdict(list)
        for g in outdoor_games:
            year = g["game_date"][:4]
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
                # No coordinates — skip
                for g in batch:
                    conn.execute("INSERT OR REPLACE INTO game_weather (game_pk) VALUES (?)", (g["game_pk"],))
                conn.commit()
                print(f"[{idx}/{total_batches}] {venue_name} {year}: no coords, {len(batch)} games skipped")
                continue

            try:
                dates = sorted(g["game_date"] for g in batch)
                time.sleep(API_DELAY)
                resp = requests.get(WEATHER_API_URL, params={
                    "latitude": lat, "longitude": lon,
                    "start_date": dates[0], "end_date": dates[-1],
                    "hourly": HOURLY_VARS,
                    "timezone": tz or "UTC",
                    "temperature_unit": "fahrenheit",
                    "wind_speed_unit": "mph",
                }, timeout=30)
                resp.raise_for_status()
                data = resp.json()
                hourly = data["hourly"]
                time_index = {t: i for i, t in enumerate(hourly["time"])}

                inserted = 0
                for g in batch:
                    key = f"{g['game_date']}T{DEFAULT_GAME_HOUR:02d}:00"
                    idx_h = time_index.get(key)
                    if idx_h is not None:
                        conn.execute("""
                            INSERT OR REPLACE INTO game_weather
                            (game_pk, temperature_f, humidity, wind_speed_mph, wind_direction,
                             wind_gusts_mph, precipitation_mm, pressure_hpa, cloud_cover, weather_code)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
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
                        conn.execute("INSERT OR REPLACE INTO game_weather (game_pk) VALUES (?)", (g["game_pk"],))

                conn.commit()
                total_inserted += inserted
                print(f"[{idx}/{total_batches}] {venue_name} {year}: {len(batch)} games, {inserted} weather rows")

            except Exception as e:
                conn.rollback()
                total_failed += len(batch)
                print(f"[{idx}/{total_batches}] {venue_name} {year}: FAILED ({e})")

            time.sleep(API_DELAY)

        print(f"\nWeather done.")
        print(f"  Dome games (NULL): {dome_count}")
        print(f"  Weather inserted:  {total_inserted}")
        if total_failed:
            print(f"  Failed:            {total_failed}")
    finally:
        conn.close()


if __name__ == "__main__":
    print("=== Backfilling venues ===")
    backfill_venues()
    print(f"\n=== Backfilling weather ===")
    backfill_weather()
