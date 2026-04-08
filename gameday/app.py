"""
FastAPI backend for MLB Gameday live UI.

Fully async with asyncpg connection pool. Parallel queries via asyncio.gather.
Only polls live games; stops polling finals.
"""

import asyncio
import hashlib
import json
import re
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from pathlib import Path

import asyncpg
import httpx
import websockets
from fastapi import FastAPI, Query, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from starlette.responses import StreamingResponse

PG_DSN = "postgresql://mlb:mlb2026@127.0.0.1:5432/mlb"
STATIC_DIR = Path(__file__).resolve().parent / "static"

LIVE_STATUSES = frozenset({"In Progress", "Manager Challenge", "Umpire Review",
                           "Delayed", "Warmup", "Delayed Start"})

POLL_LIVE = 0.15       # seconds between polls for live games
POLL_FINAL = 5.0       # seconds between polls for final games (just to catch late updates)
SSE_INTERVAL = 0.1     # seconds between SSE push checks

GAMMA_API = "https://gamma-api.polymarket.com"
CLOB_API = "https://clob.polymarket.com"
CLOB_WSS = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
POLYMARKET_POLL = 30.0   # metadata only (market list, volume, questions) — order books come via WS
POLYMARKET_WS_PING = 10.0
MLB_TAG_ID = 100381
ODDS_IDLE_TIMEOUT = 30.0

TEAM_ABBREV: dict[str, str] = {
    "Arizona Diamondbacks": "ari", "Atlanta Braves": "atl", "Baltimore Orioles": "bal",
    "Boston Red Sox": "bos", "Chicago Cubs": "chc", "Chicago White Sox": "cws",
    "Cincinnati Reds": "cin", "Cleveland Guardians": "cle", "Colorado Rockies": "col",
    "Detroit Tigers": "det", "Houston Astros": "hou", "Kansas City Royals": "kc",
    "Los Angeles Angels": "laa", "Los Angeles Dodgers": "lad", "Miami Marlins": "mia",
    "Milwaukee Brewers": "mil", "Minnesota Twins": "min", "New York Mets": "nym",
    "New York Yankees": "nyy", "Oakland Athletics": "oak", "Philadelphia Phillies": "phi",
    "Pittsburgh Pirates": "pit", "San Diego Padres": "sd", "San Francisco Giants": "sf",
    "Seattle Mariners": "sea", "St. Louis Cardinals": "stl", "Tampa Bay Rays": "tb",
    "Texas Rangers": "tex", "Toronto Blue Jays": "tor", "Washington Nationals": "wsh",
}


def _game_slug(away_team: str, home_team: str, game_date: str) -> str | None:
    away_abbr = TEAM_ABBREV.get(away_team)
    home_abbr = TEAM_ABBREV.get(home_team)
    if away_abbr and home_abbr:
        return f"{away_abbr}-{home_abbr}-{game_date}"
    return None

pool: asyncpg.Pool | None = None
http_client: httpx.AsyncClient | None = None

# ── Game state cache ─────────────────────────────────────────────────────

_game_cache: dict[int, dict] = {}
_cache_subscribers: dict[int, int] = {}
_cache_tasks: dict[int, asyncio.Task] = {}

# ── Polymarket odds cache ───────────────────────────────────────────────

_odds_cache: dict[int, dict] = {}
_odds_last_request: dict[int, float] = {}
_odds_tasks: dict[int, asyncio.Task] = {}
_poly_event_cache: dict[int, dict | None] = {}
_poly_event_cache_time: dict[int, float] = {}
_poly_slug_cache: dict[int, str] = {}

# Live order book state keyed by token_id. Stored as dicts {price: size}
# internally for O(1) delta application; serialized to sorted level arrays
# only when building outgoing payloads.
_live_books: dict[str, dict] = {}
_ws_tasks: dict[int, asyncio.Task] = {}              # game_pk -> websocket pump task
_ws_tokens: dict[int, list[str]] = {}                # game_pk -> token_ids subscribed
_odds_update_events: dict[int, asyncio.Event] = {}   # game_pk -> SSE wake signal


# ── Lifecycle ────────────────────────────────────────────────────────────

async def _sync_game_statuses():
    """Periodically sync game statuses from MLB API to catch Final transitions."""
    while True:
        try:
            today = datetime.now().strftime("%m/%d/%Y")
            resp = await asyncio.to_thread(
                lambda: __import__("urllib.request", fromlist=["urlopen"]).urlopen(
                    f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={today}"
                ).read()
            )
            data = json.loads(resp)
            for date_entry in data.get("dates", []):
                for g in date_entry.get("games", []):
                    pk = g.get("gamePk")
                    status = g.get("status", {}).get("detailedState", "")
                    if status in ("Final", "Game Over") and pk:
                        async with pool.acquire() as conn:
                            await conn.execute(
                                'UPDATE game SET status = $1 WHERE game_pk = $2 AND status != $1',
                                status, pk)
        except Exception:
            pass
        await asyncio.sleep(60)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    global pool, http_client
    pool = await asyncpg.create_pool(PG_DSN, min_size=2, max_size=10)
    http_client = httpx.AsyncClient(timeout=10.0)
    sync_task = asyncio.create_task(_sync_game_statuses())
    yield
    sync_task.cancel()
    await http_client.aclose()
    await pool.close()


app = FastAPI(title="MLB Gameday", lifespan=lifespan)

if (STATIC_DIR / "assets").exists():
    app.mount("/assets", StaticFiles(directory=str(STATIC_DIR / "assets")), name="assets")


# ── Query helpers ────────────────────────────────────────────────────────

def _tobool(val) -> bool:
    if isinstance(val, bool):
        return val
    if isinstance(val, str):
        return val.lower() in ("true", "1")
    return val == 1 if isinstance(val, int) else False


def _tofloat(val) -> float | None:
    return float(val) if val is not None else None


def _toint(val) -> int:
    return int(val) if val is not None else 0


# ── Build game state (all queries in parallel) ──────────────────────────

async def _fetch(sql: str, *args):
    async with pool.acquire() as conn:
        return await conn.fetch(sql, *args)

async def _fetchrow(sql: str, *args):
    async with pool.acquire() as conn:
        return await conn.fetchrow(sql, *args)

async def build_game_state(game_pk: int) -> dict | None:
    # Fire all 4 queries in parallel on separate connections
    game_row, linescore_rows, ab_row, play_rows = await asyncio.gather(
        _fetchrow(
            'SELECT game_pk, status, home_team_name, away_team_name, '
            'home_score, away_score, current_inning, inning_state, venue_name '
            'FROM game WHERE game_pk = $1', game_pk),
        _fetch(
            'WITH inning_scores AS ('
            '  SELECT about_inning,'
            '    MAX(CASE WHEN about_is_top_inning IN (\'true\',\'1\') THEN result_away_score END) AS away_cum,'
            '    MAX(CASE WHEN about_is_top_inning IN (\'false\',\'0\') THEN result_home_score END) AS home_cum'
            '  FROM atbat WHERE game_pk = $1 AND about_is_complete IN (\'true\',\'1\')'
            '  GROUP BY about_inning'
            ') SELECT about_inning AS inning,'
            '  COALESCE(away_cum - LAG(away_cum, 1, 0) OVER (ORDER BY about_inning), away_cum) AS away,'
            '  COALESCE(home_cum - LAG(home_cum, 1, 0) OVER (ORDER BY about_inning), home_cum) AS home'
            ' FROM inning_scores ORDER BY about_inning', game_pk),
        _fetchrow(
            'SELECT about_at_bat_index, matchup_batter_id, matchup_batter_full_name,'
            '  matchup_pitcher_id, matchup_pitcher_full_name,'
            '  matchup_bat_side_code, matchup_pitch_hand_code,'
            '  about_is_complete, result_event, result_description'
            ' FROM atbat WHERE game_pk = $1'
            ' ORDER BY about_at_bat_index DESC LIMIT 1', game_pk),
        _fetch(
            'SELECT about_at_bat_index, about_inning, about_is_top_inning,'
            '  result_event, result_description,'
            '  matchup_batter_full_name, matchup_pitcher_full_name,'
            '  result_away_score, result_home_score, about_is_scoring_play'
            ' FROM atbat WHERE game_pk = $1 AND about_is_complete IN (\'true\',\'1\')'
            ' ORDER BY about_at_bat_index DESC LIMIT 25', game_pk),
    )

    if not game_row:
        return None

    game_info = {
        "game_pk": game_row["game_pk"],
        "status": game_row["status"],
        "home_team_name": game_row["home_team_name"],
        "away_team_name": game_row["away_team_name"],
        "home_score": _toint(game_row["home_score"]),
        "away_score": _toint(game_row["away_score"]),
        "current_inning": game_row["current_inning"],
        "inning_state": game_row["inning_state"],
        "venue_name": game_row["venue_name"],
    }

    linescore = [{"inning": int(r["inning"]), "away": _toint(r["away"]), "home": _toint(r["home"])}
                 for r in linescore_rows]

    current_ab = None
    pitches: list[dict] = []
    runners = {"first": None, "second": None, "third": None}
    count = {"balls": 0, "strikes": 0, "outs": 0}

    if ab_row:
        ab_index = ab_row["about_at_bat_index"]
        current_ab = {
            "atBatIndex": ab_index,
            "batter_name": ab_row["matchup_batter_full_name"],
            "batter_id": ab_row["matchup_batter_id"],
            "pitcher_name": ab_row["matchup_pitcher_full_name"],
            "pitcher_id": ab_row["matchup_pitcher_id"],
            "bat_side": ab_row["matchup_bat_side_code"],
            "pitch_hand": ab_row["matchup_pitch_hand_code"],
            "is_complete": _tobool(ab_row["about_is_complete"]),
            "result": ab_row["result_event"],
            "result_description": ab_row["result_description"],
        }

        # Second wave: pitch-dependent query (needs ab_index)
        pitch_rows = await _fetch(
            'SELECT pitch_number, details_type_code, details_type_description,'
            '  pitch_data_start_speed, details_call_description, details_call_code,'
            '  pitch_data_coordinates_p_x, pitch_data_coordinates_p_z,'
            '  pitch_data_strike_zone_top, pitch_data_strike_zone_bottom,'
            '  details_is_strike, details_is_ball, details_is_in_play,'
            '  pitch_data_breaks_spin_rate,'
            '  pitch_data_breaks_break_vertical_induced, pitch_data_breaks_break_horizontal,'
            '  hit_data_launch_speed, hit_data_launch_angle, hit_data_total_distance,'
            '  count_balls, count_strikes, count_outs,'
            '  offense_first_id, offense_second_id, offense_third_id'
            ' FROM play_event'
            ' WHERE game_pk = $1 AND about_at_bat_index = $2'
            ' ORDER BY pitch_number DESC', game_pk, ab_index)

        # Extract count + runners from latest event, pitches from all isPitch rows
        if pitch_rows:
            latest = pitch_rows[0]  # DESC order, so first is latest
            count = {
                "balls": min(_toint(latest["count_balls"]), 3),
                "strikes": min(_toint(latest["count_strikes"]), 2),
                "outs": min(_toint(latest["count_outs"]), 2),
            }

            # Runner names - batch lookup
            runner_ids = [latest["offense_first_id"], latest["offense_second_id"], latest["offense_third_id"]]
            non_null = [rid for rid in runner_ids if rid is not None]
            name_map: dict[int, str] = {}
            if non_null:
                rows = await _fetch(
                    'SELECT id, full_name FROM player WHERE id = ANY($1)', non_null)
                name_map = {r["id"]: r["full_name"] for r in rows}

            for base, rid in [("first", runner_ids[0]), ("second", runner_ids[1]), ("third", runner_ids[2])]:
                runners[base] = {"id": rid, "name": name_map.get(rid, "?")} if rid else None

        # Build pitch list (filter to actual pitches, reverse to ASC)
        for p in reversed(pitch_rows):
            if not _tobool(p.get("is_pitch", p.get("details_call_code"))):
                # If is_pitch not in this query, use presence of pitch_number > 0
                pass
            pitches.append({
                "num": p["pitch_number"],
                "type_code": p["details_type_code"],
                "type_desc": p["details_type_description"],
                "speed": _tofloat(p["pitch_data_start_speed"]),
                "call": p["details_call_description"],
                "call_code": p["details_call_code"],
                "pX": _tofloat(p["pitch_data_coordinates_p_x"]),
                "pZ": _tofloat(p["pitch_data_coordinates_p_z"]),
                "szTop": _tofloat(p["pitch_data_strike_zone_top"]),
                "szBottom": _tofloat(p["pitch_data_strike_zone_bottom"]),
                "is_strike": _tobool(p["details_is_strike"]),
                "is_ball": _tobool(p["details_is_ball"]),
                "is_in_play": _tobool(p["details_is_in_play"]),
                "spin_rate": int(p["pitch_data_breaks_spin_rate"]) if p["pitch_data_breaks_spin_rate"] is not None else None,
                "break_vert": _tofloat(p["pitch_data_breaks_break_vertical_induced"]),
                "break_horiz": _tofloat(p["pitch_data_breaks_break_horizontal"]),
                "hit_speed": _tofloat(p["hit_data_launch_speed"]),
                "hit_angle": _tofloat(p["hit_data_launch_angle"]),
                "hit_distance": _tofloat(p["hit_data_total_distance"]),
            })
        # Filter to only rows with a pitch number > 0 and a call code
        pitches = [p for p in pitches if p["num"] and p["num"] > 0 and p["call_code"]]

    plays = [
        {
            "atBatIndex": r["about_at_bat_index"],
            "inning": int(r["about_inning"]),
            "is_top": _tobool(r["about_is_top_inning"]),
            "event": r["result_event"],
            "description": r["result_description"],
            "batter": r["matchup_batter_full_name"],
            "pitcher": r["matchup_pitcher_full_name"],
            "away_score": _toint(r["result_away_score"]),
            "home_score": _toint(r["result_home_score"]),
            "is_scoring": _tobool(r["about_is_scoring_play"]),
        }
        for r in play_rows
    ]

    return {
        "game": game_info, "linescore": linescore, "current_ab": current_ab,
        "pitches": pitches, "runners": runners, "count": count, "plays": plays,
    }


# ── SSE background poller (adaptive rate) ────────────────────────────────

async def _poll_game_loop(game_pk: int):
    """Poll DB for game state. Fast for live, slow for final, stop when no subscribers."""
    consecutive_finals = 0
    while _cache_subscribers.get(game_pk, 0) > 0:
        try:
            state = await build_game_state(game_pk)
            if state:
                _game_cache[game_pk] = state
                status = state["game"]["status"]
                if status in LIVE_STATUSES:
                    consecutive_finals = 0
                    await asyncio.sleep(POLL_LIVE)
                else:
                    consecutive_finals += 1
                    if consecutive_finals > 20:
                        # Game has been final for a while, stop polling
                        break
                    await asyncio.sleep(POLL_FINAL)
            else:
                await asyncio.sleep(POLL_FINAL)
        except Exception:
            await asyncio.sleep(1.0)
    _cache_tasks.pop(game_pk, None)


# ── Polymarket odds helpers ─────────────────────────────────────────────

async def _resolve_polymarket_event(game_pk: int, home_team: str, away_team: str,
                                     game_date: str | None = None) -> dict | None:
    """Find the Polymarket event matching this game via slug lookup."""
    now = asyncio.get_event_loop().time()
    cached_time = _poly_event_cache_time.get(game_pk, 0)

    slug = _poly_slug_cache.get(game_pk)

    # Return cached if fresh
    if slug and game_pk in _poly_event_cache and (now - cached_time) < 300:
        return _poly_event_cache[game_pk]

    # Construct slug from team abbreviations + date
    if not slug and game_date:
        away_abbr = TEAM_ABBREV.get(away_team)
        home_abbr = TEAM_ABBREV.get(home_team)
        if away_abbr and home_abbr:
            slug = f"mlb-{away_abbr}-{home_abbr}-{game_date}"
            _poly_slug_cache[game_pk] = slug

    if not slug:
        _poly_event_cache[game_pk] = None
        _poly_event_cache_time[game_pk] = now
        return None

    try:
        resp = await http_client.get(f"{GAMMA_API}/events", params={"slug": slug})
        resp.raise_for_status()
        events = resp.json()
        if events:
            event = events[0]
            _poly_event_cache[game_pk] = event
            _poly_event_cache_time[game_pk] = now
            return event
    except Exception:
        return _poly_event_cache.get(game_pk)

    # Slug didn't match — clear it so next attempt tries full search
    _poly_slug_cache.pop(game_pk, None)
    _poly_event_cache[game_pk] = None
    _poly_event_cache_time[game_pk] = now
    return None


def _extract_line(question: str, market_type: str) -> float | None:
    """Extract the numeric line from a market question (e.g., 'O/U 8.5' → 8.5, '(-1.5)' → 1.5)."""
    if market_type == "spread":
        m = re.search(r'\(([+-]?\d+\.?\d*)\)', question)
        return abs(float(m.group(1))) if m else None
    elif market_type == "total":
        m = re.search(r'O/U\s*(\d+\.?\d*)', question)
        return float(m.group(1)) if m else None
    return None


async def _fetch_market_data(event: dict) -> dict:
    """Build structured market data from a Polymarket event."""
    markets = event.get("markets", [])
    result = {"event_slug": event.get("slug", ""), "markets": []}

    for market in markets:
        market_type = market.get("sportsMarketType", "")
        # Normalize plural types from Gamma API
        if market_type in ("spreads",):
            market_type = "spread"
        elif market_type in ("totals",):
            market_type = "total"
        if market_type not in ("moneyline", "spread", "total", "nrfi"):
            continue

        outcomes = market.get("outcomes", [])
        if isinstance(outcomes, str):
            outcomes = json.loads(outcomes)
        prices_raw = market.get("outcomePrices", [])
        if isinstance(prices_raw, str):
            prices_raw = json.loads(prices_raw)
        token_ids = market.get("clobTokenIds", [])
        if isinstance(token_ids, str):
            token_ids = json.loads(token_ids)

        question = market.get("question", "")
        line = _extract_line(question, market_type)

        outcome_list = []
        for i, name in enumerate(outcomes):
            price = float(prices_raw[i]) if i < len(prices_raw) else 0.0
            token_id = token_ids[i] if i < len(token_ids) else None
            outcome_list.append({"name": name, "price": price, "token_id": token_id})

        result["markets"].append({
            "type": market_type,
            "question": question,
            "volume": market.get("volume", "0"),
            "line": line,
            "outcomes": outcome_list,
        })

    return result


_clob_sem = asyncio.Semaphore(10)  # max concurrent CLOB REST requests (priming only)


def _tracked_token_ids(markets: list[dict]) -> list[str]:
    """Return token_ids we want live books for: moneyline + first spread + first total."""
    seen_types: set[str] = set()
    token_ids: list[str] = []
    for market in markets:
        mtype = market.get("type", "")
        if mtype in ("spread", "total") and mtype in seen_types:
            continue
        seen_types.add(mtype)
        for outcome in market.get("outcomes", []):
            tid = outcome.get("token_id")
            if tid:
                token_ids.append(tid)
    return token_ids


async def _prime_live_books(token_ids: list[str]) -> None:
    """Seed _live_books via REST /book snapshot so the first SSE push has depth.
    The WS stream will take over and keep it current."""
    async def prime(tid: str) -> None:
        async with _clob_sem:
            try:
                resp = await http_client.get(f"{CLOB_API}/book", params={"token_id": tid})
                resp.raise_for_status()
                data = resp.json()
                _live_books[tid] = {
                    "bids": {float(b["price"]): float(b["size"]) for b in data.get("bids", [])},
                    "asks": {float(a["price"]): float(a["size"]) for a in data.get("asks", [])},
                    "last_trade_price": float(data.get("last_trade_price", 0) or 0),
                }
            except Exception:
                pass

    await asyncio.gather(*(prime(tid) for tid in token_ids))


def _serialize_book(token_id: str | None) -> dict | None:
    """Render the internal live-book dict into the shape the frontend consumes.
    Sorted bids (descending) + asks (ascending), trimmed to 15 levels per side."""
    if not token_id:
        return None
    lb = _live_books.get(token_id)
    if lb is None:
        return None
    bids_sorted = sorted(
        ((p, s) for p, s in lb["bids"].items() if s > 0),
        key=lambda x: x[0], reverse=True,
    )[:15]
    asks_sorted = sorted(
        ((p, s) for p, s in lb["asks"].items() if s > 0),
        key=lambda x: x[0],
    )[:15]
    bids = [{"price": p, "size": s, "total": round(p * s, 2)} for p, s in bids_sorted]
    asks = [{"price": p, "size": s, "total": round(p * s, 2)} for p, s in asks_sorted]
    best_bid = bids[0]["price"] if bids else None
    best_ask = asks[0]["price"] if asks else None
    spread = round(best_ask - best_bid, 4) if best_bid is not None and best_ask is not None else 0
    return {
        "bid": best_bid, "ask": best_ask, "spread": spread,
        "last_trade_price": lb.get("last_trade_price", 0),
        "bids": bids, "asks": asks,
    }


def _populate_order_books(market_data: dict) -> None:
    """Attach fresh order_books to each market outcome from _live_books."""
    for market in market_data.get("markets", []):
        market["order_books"] = [
            _serialize_book(outcome.get("token_id"))
            for outcome in market.get("outcomes", [])
        ]


def _apply_ws_event(ev: dict) -> set[str]:
    """Apply one WS event to _live_books. Returns set of token_ids touched."""
    et = ev.get("event_type")
    touched: set[str] = set()
    if et == "book":
        tid = ev.get("asset_id")
        if not tid:
            return touched
        prior_ltp = _live_books.get(tid, {}).get("last_trade_price", 0)
        _live_books[tid] = {
            "bids": {float(b["price"]): float(b["size"]) for b in ev.get("bids", [])},
            "asks": {float(a["price"]): float(a["size"]) for a in ev.get("asks", [])},
            "last_trade_price": prior_ltp,
        }
        touched.add(tid)
    elif et == "price_change":
        for change in ev.get("price_changes", []):
            tid = change.get("asset_id")
            if not tid:
                continue
            lb = _live_books.setdefault(tid, {"bids": {}, "asks": {}, "last_trade_price": 0})
            side_key = "bids" if change.get("side") == "BUY" else "asks"
            try:
                price = float(change["price"])
                size = float(change["size"])
            except (KeyError, TypeError, ValueError):
                continue
            book_side = lb[side_key]
            if size == 0:
                book_side.pop(price, None)
            else:
                book_side[price] = size
            touched.add(tid)
    elif et == "last_trade_price":
        tid = ev.get("asset_id")
        if tid and tid in _live_books:
            try:
                _live_books[tid]["last_trade_price"] = float(ev.get("price", 0))
                touched.add(tid)
            except (TypeError, ValueError):
                pass
    return touched


async def _ws_market_loop(game_pk: int, token_ids: list[str]) -> None:
    """Maintain a live Polymarket WS subscription for this game's tokens.

    Applies book snapshots and price_change deltas into _live_books and sets the
    per-game update Event so any SSE streams wake up and push a new payload.
    """
    update_event = _odds_update_events.setdefault(game_pk, asyncio.Event())

    async for conn in websockets.connect(CLOB_WSS, ping_interval=None, open_timeout=15, close_timeout=5):
        try:
            # Subscribe (note the plural `assets_ids` — this is the correct field name).
            await conn.send(json.dumps({"assets_ids": token_ids, "type": "market"}))

            async def _heartbeat() -> None:
                # Polymarket requires an application-level "PING" text frame every ~10s.
                # The RFC6455 WS ping from the library is not sufficient (see py-clob-client#292).
                while True:
                    await asyncio.sleep(POLYMARKET_WS_PING)
                    try:
                        await conn.send("PING")
                    except Exception:
                        return

            hb = asyncio.create_task(_heartbeat())
            try:
                async for raw in conn:
                    if raw == "PONG":
                        continue
                    try:
                        msg = json.loads(raw)
                    except Exception:
                        continue
                    events = msg if isinstance(msg, list) else [msg]
                    any_touched = False
                    for ev in events:
                        if _apply_ws_event(ev):
                            any_touched = True
                    if any_touched:
                        update_event.set()
            finally:
                hb.cancel()
        except asyncio.CancelledError:
            raise
        except Exception:
            await asyncio.sleep(1.0)
            continue


async def _ensure_ws_stream(game_pk: int, markets: list[dict]) -> None:
    """Prime books via REST and start the WS pump if not already running."""
    desired = _tracked_token_ids(markets)
    if not desired:
        return

    existing = _ws_tokens.get(game_pk, [])
    task = _ws_tasks.get(game_pk)
    if task and not task.done() and existing == desired:
        return  # already streaming the right set

    # Token set changed (or first run) — cancel old task, reseed, start fresh.
    if task and not task.done():
        task.cancel()
        try:
            await task
        except (asyncio.CancelledError, Exception):
            pass

    await _prime_live_books(desired)
    _ws_tokens[game_pk] = desired
    _ws_tasks[game_pk] = asyncio.create_task(_ws_market_loop(game_pk, desired))


async def _poll_odds_loop(game_pk: int):
    """Periodically refresh Polymarket market metadata (list, volumes, questions)
    and keep the live WS order-book stream alive. Stops when idle."""
    while True:
        now = asyncio.get_event_loop().time()
        last_req = _odds_last_request.get(game_pk, 0)
        if now - last_req > ODDS_IDLE_TIMEOUT:
            break

        try:
            game_state = _game_cache.get(game_pk)
            if not game_state:
                row = await _fetchrow(
                    'SELECT home_team_name, away_team_name, status, game_date FROM game WHERE game_pk = $1', game_pk)
                if not row:
                    await asyncio.sleep(POLYMARKET_POLL)
                    continue
                home, away = row["home_team_name"], row["away_team_name"]
                game_date = str(row["game_date"])
            else:
                home = game_state["game"]["home_team_name"]
                away = game_state["game"]["away_team_name"]
                if game_pk not in _poly_slug_cache:
                    row = await _fetchrow('SELECT game_date FROM game WHERE game_pk = $1', game_pk)
                    game_date = str(row["game_date"]) if row else None
                else:
                    game_date = None  # slug already cached

            event = await _resolve_polymarket_event(game_pk, home, away, game_date)
            if not event:
                _odds_cache[game_pk] = {"available": False, "markets": []}
                await asyncio.sleep(POLYMARKET_POLL)
                continue

            market_data = await _fetch_market_data(event)
            market_data["available"] = True
            market_data["updated_at"] = datetime.utcnow().isoformat()
            _odds_cache[game_pk] = market_data

            # Ensure the live WS stream is running for this game's tokens.
            # (_ensure_ws_stream is a no-op if the same tokens are already streaming.)
            await _ensure_ws_stream(game_pk, market_data["markets"])

            # Wake any connected SSE streams so metadata changes (volume, new lines) propagate.
            ev = _odds_update_events.get(game_pk)
            if ev:
                ev.set()

        except Exception:
            if game_pk not in _odds_cache:
                _odds_cache[game_pk] = {"available": False, "markets": []}

        await asyncio.sleep(POLYMARKET_POLL)

    # Idle — tear down WS for this game.
    ws_task = _ws_tasks.pop(game_pk, None)
    if ws_task and not ws_task.done():
        ws_task.cancel()
    _ws_tokens.pop(game_pk, None)
    _odds_update_events.pop(game_pk, None)
    _odds_tasks.pop(game_pk, None)


# ── Routes ───────────────────────────────────────────────────────────────

@app.get("/")
async def index():
    return FileResponse(str(STATIC_DIR / "index.html"))


@app.get("/api/games")
async def games(date: str | None = Query(default=None)):
    if date:
        try:
            target = datetime.strptime(date, "%Y-%m-%d").date()
        except ValueError:
            target = datetime.now().date()
    else:
        target = datetime.now().date()

    async with pool.acquire() as conn:
        rows = await conn.fetch(
            'SELECT game_pk, status, home_team_name, away_team_name,'
            '  home_score, away_score, current_inning, inning_state,'
            '  venue_name, game_datetime, game_date'
            ' FROM game WHERE game_date = $1'
            ' ORDER BY game_datetime',
            str(target))

    result = []
    for r in rows:
        d = dict(r)
        d["slug"] = _game_slug(d.get("away_team_name", ""), d.get("home_team_name", ""), str(d.get("game_date", "")))
        result.append(d)
    return result


@app.get("/api/game/{game_pk}")
async def game_snapshot(game_pk: int):
    state = await build_game_state(game_pk)
    if state is None:
        return JSONResponse({"error": "Game not found"}, status_code=404)
    return state


@app.get("/api/game/{game_pk}/stream")
async def game_stream(game_pk: int, request: Request):
    _cache_subscribers[game_pk] = _cache_subscribers.get(game_pk, 0) + 1
    if game_pk not in _cache_tasks or _cache_tasks[game_pk].done():
        _cache_tasks[game_pk] = asyncio.create_task(_poll_game_loop(game_pk))

    async def generate():
        last_hash = None
        try:
            while True:
                if await request.is_disconnected():
                    break
                state = _game_cache.get(game_pk)
                if state:
                    data = json.dumps(state, default=str)
                    h = hashlib.md5(data.encode()).hexdigest()
                    if h != last_hash:
                        last_hash = h
                        yield f"data: {data}\n\n"
                await asyncio.sleep(SSE_INTERVAL)
        finally:
            _cache_subscribers[game_pk] = max(0, _cache_subscribers.get(game_pk, 0) - 1)

    return StreamingResponse(
        generate(), media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


def _materialize_odds(game_pk: int) -> dict:
    """Build the odds response for a game: cached metadata joined with live books."""
    cached = _odds_cache.get(game_pk)
    if not cached:
        return {"available": False, "markets": [], "loading": True}
    # Shallow-copy each market so _populate_order_books doesn't mutate the cache.
    payload = {
        "available": cached.get("available", False),
        "event_slug": cached.get("event_slug", ""),
        "updated_at": cached.get("updated_at"),
        "markets": [dict(m) for m in cached.get("markets", [])],
    }
    _populate_order_books(payload)
    return payload


@app.get("/api/game/{game_pk}/odds")
async def game_odds(game_pk: int):
    _odds_last_request[game_pk] = asyncio.get_event_loop().time()
    if game_pk not in _odds_tasks or _odds_tasks[game_pk].done():
        _odds_tasks[game_pk] = asyncio.create_task(_poll_odds_loop(game_pk))
    return _materialize_odds(game_pk)


@app.get("/api/game/{game_pk}/odds/stream")
async def game_odds_stream(game_pk: int, request: Request):
    """SSE stream of odds. Pushes on every Polymarket WS event affecting this
    game's tracked tokens (plus periodic metadata refreshes from the odds loop)."""
    _odds_last_request[game_pk] = asyncio.get_event_loop().time()
    if game_pk not in _odds_tasks or _odds_tasks[game_pk].done():
        _odds_tasks[game_pk] = asyncio.create_task(_poll_odds_loop(game_pk))
    update_event = _odds_update_events.setdefault(game_pk, asyncio.Event())

    async def generate():
        last_hash = None
        # First emit whatever we already have so the client paints immediately.
        try:
            while True:
                if await request.is_disconnected():
                    break
                _odds_last_request[game_pk] = asyncio.get_event_loop().time()
                payload = _materialize_odds(game_pk)
                data = json.dumps(payload, default=str)
                h = hashlib.md5(data.encode()).hexdigest()
                if h != last_hash:
                    last_hash = h
                    yield f"data: {data}\n\n"
                # Wait for the next WS update, or poll metadata at 1s worst case.
                try:
                    await asyncio.wait_for(update_event.wait(), timeout=1.0)
                    update_event.clear()
                except asyncio.TimeoutError:
                    pass
        except asyncio.CancelledError:
            raise

    return StreamingResponse(
        generate(), media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.get("/api/game/by-slug/{slug}")
async def game_by_slug(slug: str):
    """Resolve a slug like 'chc-tb-2026-04-06' to a game_pk."""
    # rsplit on last 3 dashes gives: ['away-home', 'YYYY', 'MM', 'DD']
    parts = slug.rsplit("-", 3)
    if len(parts) != 4:
        return {"error": "Invalid slug"}
    date_str = f"{parts[1]}-{parts[2]}-{parts[3]}"
    team_part = parts[0]  # e.g. 'chc-tb' or 'hou-col'
    team_parts = team_part.split("-")
    if len(team_parts) < 2:
        return JSONResponse({"error": "Invalid slug"}, status_code=400)
    home_abbr = team_parts[-1]
    away_abbr = "-".join(team_parts[:-1])

    # Reverse lookup: abbreviation -> full team name
    abbrev_to_team = {v: k for k, v in TEAM_ABBREV.items()}
    away_team = abbrev_to_team.get(away_abbr)
    home_team = abbrev_to_team.get(home_abbr)

    if not away_team or not home_team:
        return JSONResponse({"error": "Unknown team abbreviation"}, status_code=404)

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            'SELECT game_pk FROM game WHERE away_team_name = $1 AND home_team_name = $2 AND game_date = $3',
            away_team, home_team, date_str)

    if not row:
        return JSONResponse({"error": "Game not found"}, status_code=404)
    return {"game_pk": row["game_pk"], "slug": slug}


@app.get("/{path:path}")
async def spa_catchall(path: str):
    """Serve index.html for all non-API routes (SPA client-side routing)."""
    if path.startswith("api/") or path.startswith("assets/"):
        return {"error": "Not found"}
    return FileResponse(str(STATIC_DIR / "index.html"))
