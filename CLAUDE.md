# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

MLB pitch-level analytics project. Fetches full game data from MLB's official Stats API (`statsapi` / MLB-StatsAPI), stores it in a SQLite database (`~/Downloads/mlb.db`) for SQL analytics. Covers 2014–2025 with ~28k games and ~7.8M pitches.

## Setup

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Running Scripts

```bash
python ingest.py 2025 2026       # Ingest missing games for given years
python backfill.py               # Backfill venues + weather data
```

## Architecture

- **Data source**: MLB Stats API via `statsapi.get('game', {gamePk})` — provides pitch-by-pitch tracking data (velocity, spin, movement, exit velo, launch angle)
- **Database**: `mlb.db` (project root) — SQLite with WAL mode
- **`ingest.py`**: ETL pipeline — fetches game schedule via `statsapi.schedule()`, fetches each game's play-by-play feed, writes to all tables. Parallel API fetching (4 threads) with serial DB writes. Skips already-ingested games. Commits per game.
- **`backfill.py`**: Backfills `venues` (from game feeds) and `game_weather` (from Open-Meteo archive API, batched by venue+year). Dome stadiums get NULL weather.

## Database Schema

- **`game`**: Game dimension (date, teams, score, venue, pitchers, broadcasts). PK: `game_pk`
- **`player`**: Player dimension (full bio, position, handedness). PK: `id`
- **`atbat`**: At-bat level data (result, matchup, count, win probability). PK: `(game_pk, about_atBatIndex)`
- **`play_event`**: Core pitch fact table (~7.8M rows). Pitch tracking, defense, baserunners, hit data. PK: `(game_pk, about_atBatIndex, [index])`
- **`runner`**: Baserunner movements per play. PK: `(game_pk, about_atBatIndex, runner_id, movement_start, movement_end)`
- **`lineup`**: Starting batting orders per game. PK: `(game_pk, team_type, player_id)`
- **`venues`**: Stadium info (location, dimensions, roof type). PK: `venue_id`
- **`game_weather`**: Hourly weather at game time. PK: `game_pk`

## Archived

The `archived/` directory contains a previous normalized schema design (`db.py`, `ingest.py`, `create_views.py`, etc.) and its database. Not actively used.
