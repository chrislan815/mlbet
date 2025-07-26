import requests
import sqlite3
from datetime import datetime, timedelta
import time

# Configuration

# SQLite setup
DB_PATH = "/Users/chris.lan/Downloads/mlb.db"
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# Load all venues except 17 and 3313
cursor.execute("SELECT venue_id, latitude, longitude FROM venue WHERE venue_id in (2395)")
venues = cursor.fetchall()

START_DATE = datetime(2015, 1, 1)
END_DATE = datetime(2020, 11, 11)
BATCH_DAYS = 90  # Fetch 30 days per batch

cursor.execute("""
CREATE TABLE IF NOT EXISTS weather (
    datetime TEXT KEY,
    venue_id INTEGER,
    temperature_2m REAL,
    relative_humidity_2m REAL,
    dew_point_2m REAL,
    apparent_temperature REAL,
    precipitation REAL,
    rain REAL,
    weather_code INTEGER,
    surface_pressure REAL,
    cloud_cover REAL,
    windspeed_10m REAL,
    windspeed_100m REAL,
    winddirection_10m REAL,
    winddirection_100m REAL,
    windgusts_10m REAL,
    PRIMARY KEY (datetime, venue_id)
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS venue (
    venue_id INTEGER PRIMARY KEY,
    name TEXT,
    latitude REAL,
    longitude REAL
)
""")
conn.commit()

def fetch_and_save(start_date, end_date):
    url = 'https://archive-api.open-meteo.com/v1/archive'
    params = {
        'latitude': LATITUDE,
        'longitude': LONGITUDE,
        'start_date': start_date.strftime('%Y-%m-%d'),
        'end_date': end_date.strftime('%Y-%m-%d'),
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

    print(f"Fetching {params['start_date']} to {params['end_date']}")

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
            VENUE_ID,  # place_id placeholder
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
    INSERT OR IGNORE INTO weather (
        datetime, venue_id, temperature_2m, relative_humidity_2m, dew_point_2m, apparent_temperature,
        precipitation, rain, weather_code, surface_pressure, cloud_cover,
        windspeed_10m, windspeed_100m, winddirection_10m, winddirection_100m, windgusts_10m
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, records)
    conn.commit()
    print(f"Saved {len(records)} records")


for VENUE_ID, LATITUDE, LONGITUDE in venues:
    print(f"Processing venue {VENUE_ID}")
    current_end = END_DATE
    while current_end >= START_DATE:
        current_start = max(current_end - timedelta(days=BATCH_DAYS - 1), START_DATE)
        try:
            fetch_and_save(current_start, current_end)
        except Exception as e:
            print("Error:", e)
        current_end = current_start - timedelta(days=1)
        time.sleep(5)  # polite delay between requests

conn.close()
print("Done!")