# day range
# pull all games from the day range
# save to a csv file
# save live_feeds to folder
# game, atbat, runner, lineup, weather and play_event


import statsapi
import sqlite3

from archive.gamewin_data import fetch_and_save_win_probability_data
from archive.save_live_feed import fetch_and_save_live_feed_data



# Get today's MLB schedule
if __name__ == '__main__':
    conn = sqlite3.connect('/Users/chris.lan/Downloads/mlb.db')
    cursor = conn.cursor()

    schedule = statsapi.schedule()
    final_game_pks = [g['game_id'] for g in schedule if g.get('status') == 'Final']
    [fetch_and_save_win_probability_data(game_pk) for game_pk in final_game_pks]
    [fetch_and_save_live_feed_data(game_pk) for game_pk in final_game_pks]

