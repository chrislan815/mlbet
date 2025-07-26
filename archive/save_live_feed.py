import os
import json
import gzip
import asyncio
import aiohttp
import aiosqlite
import logging
import ssl
from asyncio_throttle import Throttler  # lightweight rate limiter

logging.basicConfig(level=logging.INFO)


os.makedirs("games", exist_ok=True)

# Throttle to 5 requests per second
throttler = Throttler(rate_limit=20, period=1.0)

# Create SSL context that disables verification (for debugging ONLY)
ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE


async def fetch_and_save(session, game_pk):
    filepath = f"live_feeds/{game_pk}.json.gz"
    if os.path.exists(filepath):
        logging.info(f"Skipping {filepath}, file already exists.")
        return

    async with throttler:
        API_ENDPOINT_TEMPLATE = "https://statsapi.mlb.com/api/v1.1/game/{gamePk}/feed/live"
        url = API_ENDPOINT_TEMPLATE.format(gamePk=game_pk)
        logging.info(f"Fetching {url}")
        async with session.get(url) as resp:
            if resp.status != 200:
                logging.error(f"Failed to fetch {game_pk}: HTTP {resp.status}")
                return
            data = await resp.json()

        with gzip.open(filepath, "wt", encoding="utf-8") as f:
            json.dump(data, f)
        logging.info(f"Saved compressed file {filepath}")


async def main():
    async with aiosqlite.connect("mlb-v2.db") as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("""
            SELECT game_pk
            FROM main.game
            WHERE status = 'Final'
            ORDER BY game_date DESC
        """)
        rows = await cursor.fetchall()
        await cursor.close()

        game_pks = [row["game_pk"] for row in rows]

    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=ssl_context)) as session:
        tasks = [fetch_and_save(session, game_pk) for game_pk in game_pks]
        await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())