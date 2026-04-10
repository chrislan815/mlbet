"""Live MLB game ingest daemon for PostgreSQL.

Polls for in-progress games and ingests pitch-by-pitch data in near-real-time.
Designed to run alongside ingest_pg.py (daily batch pipeline).

Usage:
    python live_ingest.py                # Run daemon (default: poll every 1s)
    python live_ingest.py --poll-interval 5   # Poll every 5 seconds
    python live_ingest.py --once         # One poll cycle and exit (testing)
"""

import argparse
import datetime
import signal
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed

import statsapi

from ingest_pg import get_conn, write_game, SKIP_GAME_TYPES

LIVE_STATUSES = {
    "In Progress", "Manager Challenge", "Umpire Review",
    "Delayed", "Warmup", "Delayed Start",
}
FINAL_STATUSES = {"Final", "Game Over"}

DEFAULT_POLL_INTERVAL = 1       # seconds between game feed polls
SCHEDULE_INTERVAL = 60          # seconds between schedule checks
MAX_WORKERS = 8

shutdown_event = threading.Event()


def handle_shutdown(signum, frame):
    print(f"\nShutdown requested (signal {signum}). Finishing current cycle...")
    shutdown_event.set()


def discover_games(date_str):
    """Fetch today's schedule and return dict of game_pk -> schedule_entry."""
    games = statsapi.schedule(date=date_str)
    return {
        g["game_id"]: g
        for g in games
        if g.get("game_type") not in SKIP_GAME_TYPES
    }


def fetch_live_feed(game_pk):
    """Fetch a game's live feed without the batch delay."""
    return game_pk, statsapi.get("game", {"gamePk": game_pk})


def main():
    parser = argparse.ArgumentParser(description="Live MLB game ingest daemon.")
    parser.add_argument("--poll-interval", type=float, default=DEFAULT_POLL_INTERVAL,
                        help="Seconds between game feed polls (default: 1)")
    parser.add_argument("--schedule-interval", type=float, default=SCHEDULE_INTERVAL,
                        help="Seconds between schedule checks (default: 60)")
    parser.add_argument("--workers", type=int, default=MAX_WORKERS,
                        help="Max parallel API workers (default: 8)")
    parser.add_argument("--once", action="store_true",
                        help="Run one poll cycle and exit")
    args = parser.parse_args()

    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    conn = get_conn()
    # tracked_games: game_pk -> {"status": "ACTIVE"|"FINALIZING"|"DONE", "schedule": dict, "last_poll": float}
    tracked_games = {}
    last_schedule_check = 0

    print(f"Live ingest daemon started (poll={args.poll_interval}s, schedule={args.schedule_interval}s, workers={args.workers})")

    while not shutdown_event.is_set():
        now = time.time()

        # ── Schedule check ──────────────────────────────────────────
        if now - last_schedule_check >= args.schedule_interval:
            today = datetime.date.today()
            yesterday = today - datetime.timedelta(days=1)
            all_games = {}
            for d in (yesterday, today):
                all_games.update(discover_games(d.isoformat()))

            for gpk, entry in all_games.items():
                status = entry.get("status", "")
                if status in LIVE_STATUSES:
                    if gpk not in tracked_games or tracked_games[gpk]["status"] == "DONE":
                        tracked_games[gpk] = {
                            "status": "ACTIVE",
                            "schedule": entry,
                            "last_poll": 0,
                        }
                    else:
                        tracked_games[gpk]["schedule"] = entry
                elif status in FINAL_STATUSES and gpk in tracked_games:
                    if tracked_games[gpk]["status"] == "ACTIVE":
                        tracked_games[gpk]["status"] = "FINALIZING"
                        tracked_games[gpk]["schedule"] = entry

            # Clean up done games
            for gpk in list(tracked_games):
                if tracked_games[gpk]["status"] == "DONE":
                    del tracked_games[gpk]

            active = sum(1 for g in tracked_games.values() if g["status"] == "ACTIVE")
            finalizing = sum(1 for g in tracked_games.values() if g["status"] == "FINALIZING")
            last_schedule_check = now
            if active or finalizing:
                print(f"[SCHEDULE] {active} active, {finalizing} finalizing")
            else:
                print(f"[SCHEDULE] No active games")

        # ── Poll active games ───────────────────────────────────────
        games_to_poll = []
        for gpk, info in tracked_games.items():
            if info["status"] in ("ACTIVE", "FINALIZING"):
                if now - info["last_poll"] >= args.poll_interval:
                    games_to_poll.append(gpk)

        if games_to_poll:
            # Verify DB connection is alive
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
            except Exception:
                try:
                    conn.close()
                except Exception:
                    pass
                conn = get_conn()
                print("[RECONNECT] DB connection re-established")

            with ThreadPoolExecutor(max_workers=min(args.workers, len(games_to_poll))) as pool:
                futures = {pool.submit(fetch_live_feed, gpk): gpk for gpk in games_to_poll}
                for future in as_completed(futures):
                    gpk = futures[future]
                    info = tracked_games[gpk]
                    sched = info["schedule"]
                    away = sched.get("away_name", "???")
                    home = sched.get("home_name", "???")
                    try:
                        _, data = future.result()
                        pitch_count = write_game(conn, gpk, sched, data)
                        conn.commit()
                        info["last_poll"] = time.time()

                        feed_state = data.get("gameData", {}).get("status", {}).get("abstractGameState", "")
                        linescore = data.get("liveData", {}).get("linescore", {})
                        away_runs = linescore.get("teams", {}).get("away", {}).get("runs", 0)
                        home_runs = linescore.get("teams", {}).get("home", {}).get("runs", 0)

                        if info["status"] == "FINALIZING" and feed_state == "Final":
                            info["status"] = "DONE"
                            print(f"[FINAL] {away} {away_runs} @ {home} {home_runs} "
                                  f"(gp{gpk}): {pitch_count} pitches — game complete")
                        elif feed_state == "Final" and info["status"] == "ACTIVE":
                            # Feed says final before schedule caught up
                            info["status"] = "DONE"
                            info["schedule"] = sched
                            print(f"[FINAL] {away} {away_runs} @ {home} {home_runs} "
                                  f"(gp{gpk}): {pitch_count} pitches — game complete")
                        else:
                            current_inning = linescore.get("currentInningOrdinal", "")
                            inning_half = linescore.get("inningHalf", "")
                            print(f"[LIVE] {away} {away_runs} @ {home} {home_runs} "
                                  f"({inning_half} {current_inning}) — {pitch_count} pitches")
                    except Exception:
                        try:
                            conn.rollback()
                        except Exception:
                            # Connection is dead — replace it
                            try:
                                conn.close()
                            except Exception:
                                pass
                            conn = get_conn()
                            print("[RECONNECT] DB connection re-established")
                        tb_lines = traceback.format_exc().strip().splitlines()
                        err = tb_lines[-1] if tb_lines else "unknown"
                        # Show last 3 lines for more context
                        for line in tb_lines[-3:]:
                            print(f"[ERROR] {away} @ {home} (gp{gpk}): {line}")

        if args.once:
            break

        # Sleep until next poll is due
        if tracked_games:
            time.sleep(max(0.1, args.poll_interval / 2))
        else:
            time.sleep(min(5.0, args.schedule_interval / 2))

    conn.close()
    print("Shutdown complete.")


if __name__ == "__main__":
    main()
