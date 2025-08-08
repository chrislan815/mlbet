import statsapi
import sqlite3

from archive.atbat_data_local import save_atbat_to_db
from archive.game_data_month import save_game_to_db
from archive.gamewin_data import save_win_probability_data
from archive.lineup import save_lineup
from archive.playevent_fix import save_hit_data
from archive.playevent_local import save_play_events_to_db
from archive.runners import save_runners
from archive.save_live_feed import save_live_feed_data
from archive.weather_data import pull_weather


# Get today's MLB schedule
if __name__ == '__main__':
    conn = sqlite3.connect('/Users/chris.lan/Downloads/mlb.db')
    cursor = conn.cursor()

    start_date = '2025-08-07'
    schedule = statsapi.schedule(start_date=start_date)

    final_games = [g for g in schedule if g.get('status') == 'Final']
    final_game_pks = [g['game_id'] for g in final_games]

    [save_game_to_db(conn, game) for game in final_games]
    [save_win_probability_data(game_pk) for game_pk in final_game_pks]
    [save_live_feed_data(game_pk) for game_pk in final_game_pks]

    [save_lineup(cursor, game_pk) for game_pk in final_game_pks]
    save_runners(cursor, final_game_pks)
    [save_atbat_to_db(conn, game_pk) for game_pk in final_game_pks]
    [save_play_events_to_db(cursor, game_pk) for game_pk in final_game_pks]
    save_hit_data(cursor, final_game_pks)
    pull_weather(cursor, start_date)

