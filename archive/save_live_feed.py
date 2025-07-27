import os
import json
import gzip
import requests
import logging
import ssl
import time
import statsapi

logging.basicConfig(level=logging.INFO)

os.makedirs("live_feeds", exist_ok=True)

ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE


def save_live_feed_data(game_pk):
    filepath = os.path.join(os.path.dirname(__file__), '..', 'live_feeds', f"{game_pk}.json.gz")
    if os.path.exists(filepath):
        logging.info(f"Skipping {filepath}, file already exists.")
        return
    API_ENDPOINT_TEMPLATE = "https://statsapi.mlb.com/api/v1.1/game/{gamePk}/feed/live"
    url = API_ENDPOINT_TEMPLATE.format(gamePk=game_pk)
    logging.info(f"Fetching {url}")
    try:
        resp = requests.get(url, verify=False)
        if resp.status_code != 200:
            logging.error(f"Failed to fetch {game_pk}: HTTP {resp.status_code}")
            return
        data = resp.json()
        with gzip.open(filepath, "wt", encoding="utf-8") as f:
            json.dump(data, f)
        logging.info(f"Saved compressed file {filepath}")
    except Exception as e:
        logging.error(f"Exception fetching {game_pk}: {e}")


def main():
    schedule = statsapi.schedule()
    final_game_pks = [g["game_id"] for g in schedule if g.get("status") == "Final"]
    for game_pk in final_game_pks:
        save_live_feed_data(game_pk)


if __name__ == "__main__":
    main()