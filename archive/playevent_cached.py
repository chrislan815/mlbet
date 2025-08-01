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

def get_hitdata_from_db(game_id):
    conn = sqlite3.connect('/Users/chris.lan/Downloads/mlb.db')
    cursor = conn.cursor()
    query = '''
        SELECT about_atBatIndex, pitchNumber, hitData_launchSpeed, hitData_launchAngle, hitData_totalDistance, hitData_trajectory, hitData_hardness, hitData_location, hitData_coordinates_coordX, hitData_coordinates_coordY
        FROM play_event
        WHERE game_pk = ? and hitData_launchSpeed is not null
        ORDER BY about_atBatIndex, pitchNumber
    '''
    db_hitdata = {}
    for row in cursor.execute(query, (game_id,)):
        db_hitdata[(row[0], row[1])] = {
            'launchSpeed': row[2], 'launchAngle': row[3], 'totalDistance': row[4], 'trajectory': row[5], 'hardness': row[6], 'location': row[7], 'coordX': row[8], 'coordY': row[9]
        }
    conn.close()
    return db_hitdata

if __name__ == "__main__":
    db_hitdata = get_hitdata_from_db(game_id)
    mismatches = []
    for atbat in data:
        atbat_index = atbat.get('about', {}).get('atBatIndex')
        for pe in atbat.get('playEvents', []):
            pitch_number = pe.get('pitchNumber')
            file_hitdata = pe.get('hitData')
            db_data = db_hitdata.get((atbat_index, pitch_number))
            if file_hitdata and db_data:
                for key in ['launchSpeed', 'launchAngle', 'totalDistance', 'trajectory', 'hardness', 'location']:
                    file_val = file_hitdata.get(key)
                    db_val = db_data.get(key)
                    if file_val != db_val:
                        mismatches.append((atbat_index, pitch_number, key, file_val, db_val))
    if mismatches:
        print("Mismatches found:")
        for m in mismatches:
            print(f"atBatIndex: {m[0]}, pitchNumber: {m[1]}, field: {m[2]}, file: {m[3]}, db: {m[4]}")
    else:
        print("All hitData fields match between file and database.")
