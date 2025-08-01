import requests
import sqlite3
from datetime import datetime, timedelta
import time
import calendar

# Configuration

# SQLite setup
DB_PATH = "/Users/chris.lan/Downloads/mlb.db"
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# Load all venues except 17 and 3313
cursor.execute("SELECT venue_id, latitude, longitude FROM venue")
venues = cursor.fetchall()

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

def save_weather(date, venue_id):
    # Get latitude and longitude from venue table
    cursor.execute("SELECT latitude, longitude FROM venue WHERE venue_id = ?", (venue_id,))
    result = cursor.fetchone()
    if not result:
        print(f"Venue {venue_id} not found in database.")
        return
    latitude, longitude = result

    url = 'https://api.open-meteo.com/v1/forecast'
    params = {
        'latitude': latitude,
        'longitude': longitude,
        'start_date': date,
        'end_date': date,
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
    INSERT OR IGNORE INTO weather (
        datetime, venue_id, temperature_2m, relative_humidity_2m, dew_point_2m, apparent_temperature,
        precipitation, rain, weather_code, surface_pressure, cloud_cover,
        windspeed_10m, windspeed_100m, winddirection_10m, winddirection_100m, windgusts_10m
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, records)
    conn.commit()
    print(f"Saved {len(records)} records")

def save_weather_for_range(start_date, end_date, venue_id):
    current_date = start_date
    while current_date <= end_date:
        try:
            save_weather(current_date.strftime('%Y-%m-%d'), venue_id)
        except Exception as e:
            print(f"Error on {current_date} for venue {venue_id}: {e}")
        current_date += timedelta(days=1)

def get_month_date_range(year, month):
    start_date = datetime(year, month, 1)
    last_day = calendar.monthrange(year, month)[1]
    end_date = datetime(year, month, last_day)
    return start_date, end_date

if __name__ == "__main__":
    year = 2025
    month = 8
    start_date, end_date = get_month_date_range(year, month)
    for VENUE_ID, LATITUDE, LONGITUDE in venues:
        print(f"Processing venue {VENUE_ID}")
        save_weather_for_range(start_date, end_date, VENUE_ID)
        time.sleep(1)  # polite delay between requests
    conn.close()
    print("Done!")
