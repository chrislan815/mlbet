import requests
import sqlite3
from datetime import datetime, timedelta
from datetime import date
import time
import calendar

# Configuration

# SQLite setup
DB_PATH = "/Users/chris.lan/Downloads/mlb.db"
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()


def save_weather_for_week(cursor, year, month, day, venue_id):
    # Get latitude and longitude from venue table
    cursor.execute("SELECT latitude, longitude FROM venue WHERE venue_id = ?", (venue_id,))
    result = cursor.fetchone()
    if not result:
        print(f"Venue {venue_id} not found in database.")
        return
    latitude, longitude = result

    start_date = datetime(year, month, day)
    end_date = start_date + timedelta(days=6)
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')

    url = 'https://api.open-meteo.com/v1/forecast'
    params = {
        'latitude': latitude,
        'longitude': longitude,
        'start_date': start_date_str,
        'end_date': end_date_str,
        'hourly': ','.join([
            'temperature_2m',
            'relative_humidity_2m',
            'dew_point_2m',
            'apparent_temperature',
            'precipitation',
            'rain',
            'weathercode',
            'surface_pressure',
            'cloudcover',
            'windspeed_10m',
            'windspeed_100m',
            'winddirection_10m',
            'winddirection_100m',
            'windgusts_10m'
        ]),
        'timezone': 'UTC'
    }

    print(f"Fetching {params['start_date']} to {params['end_date']} for venue {venue_id} ({latitude}, {longitude})")

    response = requests.get(url, params=params)
    data = response.json()

    if 'hourly' not in data:
        print("No data returned for this period")
        return

    hourly = data['hourly']
    times = hourly['time']
    records = []

    for i in range(len(times)):
        records.append((
            times[i],
            venue_id,
            hourly.get('temperature_2m', [None]*len(times))[i],
            hourly.get('relative_humidity_2m', [None]*len(times))[i],
            hourly.get('dew_point_2m', [None]*len(times))[i],
            hourly.get('apparent_temperature', [None]*len(times))[i],
            hourly.get('precipitation', [None]*len(times))[i],
            hourly.get('rain', [None]*len(times))[i],
            hourly.get('weathercode', [None]*len(times))[i],
            hourly.get('surface_pressure', [None]*len(times))[i],
            hourly.get('cloudcover', [None]*len(times))[i],
            hourly.get('windspeed_10m', [None]*len(times))[i],
            hourly.get('windspeed_100m', [None]*len(times))[i],
            hourly.get('winddirection_10m', [None]*len(times))[i],
            hourly.get('winddirection_100m', [None]*len(times))[i],
            hourly.get('windgusts_10m', [None]*len(times))[i]
        ))

    cursor.executemany("""
    INSERT OR REPLACE INTO weather (
        datetime, venue_id, temperature_2m, relative_humidity_2m, dew_point_2m, apparent_temperature,
        precipitation, rain, weather_code, surface_pressure, cloud_cover,
        windspeed_10m, windspeed_100m, winddirection_10m, winddirection_100m, windgusts_10m
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, records)
    cursor.connection.commit()
    print(f"Saved {len(records)} records for venue {venue_id} from {start_date_str} to {end_date_str}")

def get_month_date_range(year, month):
    start_date = datetime(year, month, 1)
    last_day = calendar.monthrange(year, month)[1]
    end_date = datetime(year, month, last_day)
    return start_date, end_date

def create_weather_table(cursor):
    cursor.execute("""
                   CREATE TABLE IF NOT EXISTS weather
                   (
                       datetime             TIMESTAMP,
                       venue_id             INTEGER,
                       temperature_2m       REAL,
                       dew_point_2m         REAL,
                       apparent_temperature REAL,
                       precipitation        REAL,
                       rain                 REAL,
                       weather_code         INTEGER,
                       surface_pressure     REAL,
                       cloud_cover          REAL,
                       windspeed_10m        REAL,
                       windspeed_100m       REAL,
                       winddirection_10m    REAL,
                       winddirection_100m   REAL,
                       windgusts_10m        REAL,
                       PRIMARY KEY (datetime, venue_id)
                   )
                   """)

def create_venue_table(cursor):
    cursor.execute("""
                   CREATE TABLE IF NOT EXISTS venue
                   (
                       venue_id  INTEGER PRIMARY KEY,
                       name      TEXT,
                       latitude  REAL,
                       longitude REAL
                   )
                   """)

def pull_weather(cursor, start_date_str):
    today = date.today()
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
    year = start_date.year
    month = start_date.month
    end_date = datetime(year, month, calendar.monthrange(year, month)[1])
    cursor.execute("SELECT venue_id, latitude, longitude FROM venue")
    venues = cursor.fetchall()
    create_weather_table(cursor)
    create_venue_table(cursor)
    cursor.connection.commit()
    if (start_date.date() - today).days > 7:
        print(f"Skipping fetch: {start_date.date()} is more than 1 week in the future.")
        return
    for VENUE_ID, LATITUDE, LONGITUDE in venues:
        print(f"Processing venue {VENUE_ID}")
        day = start_date.day
        while day <= end_date.day:
            chunk_start = datetime(year, month, day).date()
            if (chunk_start - today).days > 7:
                print(f"Skipping fetch: {chunk_start} is more than 1 week in the future.")
                break
            save_weather_for_week(cursor, year, month, day, VENUE_ID)
            day += 7
            time.sleep(1)
    print("Done!")

if __name__ == "__main__":
    DB_PATH = "/Users/chris.lan/Downloads/mlb.db"
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    start_date = '2025-08-07'
    pull_weather(cursor, start_date)
    conn.close()
