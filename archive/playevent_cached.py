import gzip
import json
import sys
import sqlite3

# Usage: python playevent_cached.py <game_id>
game_id = sys.argv[1] if len(sys.argv) > 1 else "776998"
filepath = f"../games/{game_id}.json.gz"

with gzip.open(filepath, "rt", encoding="utf-8") as f:
    data = json.load(f)

for atbat in data:
    for pe in atbat.get('playEvents', []):
        hitdata = pe.get('hitData')
        if hitdata:
            print(f"atBatIndex: {atbat.get('about', {}).get('atBatIndex')}, pitchNumber: {pe.get('pitchNumber')}, hitData: {hitdata}")

def print_hitdata_from_db(game_id):
    conn = sqlite3.connect('/Users/chris.lan/Downloads/mlb.db')
    cursor = conn.cursor()
    query = '''
        SELECT about_atBatIndex, pitchNumber, hitData_launchSpeed, hitData_launchAngle, hitData_totalDistance, hitData_trajectory, hitData_hardness, hitData_location, hitData_coordinates_coordX, hitData_coordinates_coordY
        FROM play_event
        WHERE game_pk = ? and hitData_launchSpeed is not null
        ORDER BY about_atBatIndex, pitchNumber
    '''
    for row in cursor.execute(query, (game_id,)):
        print(f"atBatIndex: {row[0]}, pitchNumber: {row[1]}, hitData: {{'launchSpeed': {row[2]}, 'launchAngle': {row[3]}, 'totalDistance': {row[4]}, 'trajectory': {row[5]}, 'hardness': {row[6]}, 'location': {row[7]}, 'coordX': {row[8]}, 'coordY': {row[9]}}}")
    conn.close()

if __name__ == "__main__":
    print("--- DB hitData ---")
    print_hitdata_from_db(game_id)
