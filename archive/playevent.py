import sqlite3


import statsapi

""" AtBat Data Schema
(game_pk and about_atBatIndex)
>>> runners Array of Object
>>> playEvents Array of Object
"""

def flatten_pitch_data(game_pk, about_atBatIndex, pitch):
    details = pitch.get('details', {})
    details_call = details.get('call', {})
    details_type = details.get('type', {})
    count = pitch.get('count', {})
    preCount = pitch.get('preCount', {})
    pitchData = pitch.get('pitchData', {})
    pitchData_coordinates = pitchData.get('coordinates', {})
    pitchData_breaks = pitchData.get('breaks', {})
    defense = pitch.get('defense', {})
    defense_pitcher = defense.get('pitcher', {})
    defense_catcher = defense.get('catcher', {})
    defense_first = defense.get('first', {})
    defense_second = defense.get('second', {})
    defense_third = defense.get('third', {})
    defense_shortstop = defense.get('shortstop', {})
    defense_left = defense.get('left', {})
    defense_center = defense.get('center', {})
    defense_right = defense.get('right', {})
    offense = pitch.get('offense', {})
    offense_batter = offense.get('batter', {})
    offense_batterPosition = offense.get('batterPosition', {})
    offense_first = offense.get('first', {})
    offense_second = offense.get('second', {})
    offense_third = offense.get('third', {})

    return {
        'game_pk': game_pk,
        'about_atBatIndex': about_atBatIndex,
        'details_call_code': details_call.get('code'),
        'details_call_description': details_call.get('description'),
        'details_description': details.get('description'),
        'details_code': details.get('code'),
        'details_ballColor': details.get('ballColor'),
        'details_trailColor': details.get('trailColor'),
        'details_isInPlay': details.get('isInPlay'),
        'details_isStrike': details.get('isStrike'),
        'details_isBall': details.get('isBall'),
        'details_type_code': details_type.get('code'),
        'details_type_description': details_type.get('description'),
        'details_isOut': details.get('isOut'),
        'details_hasReview': details.get('hasReview'),
        'count_balls': count.get('balls'),
        'count_strikes': count.get('strikes'),
        'count_outs': count.get('outs'),
        'preCount_balls': preCount.get('balls'),
        'preCount_strikes': preCount.get('strikes'),
        'preCount_outs': preCount.get('outs'),
        'pitchData_startSpeed': pitchData.get('startSpeed'),
        'pitchData_endSpeed': pitchData.get('endSpeed'),
        'pitchData_strikeZoneTop': pitchData.get('strikeZoneTop'),
        'pitchData_strikeZoneBottom': pitchData.get('strikeZoneBottom'),
        'pitchData_coordinates_aY': pitchData_coordinates.get('aY'),
        'pitchData_coordinates_aZ': pitchData_coordinates.get('aZ'),
        'pitchData_coordinates_pfxX': pitchData_coordinates.get('pfxX'),
        'pitchData_coordinates_pfxZ': pitchData_coordinates.get('pfxZ'),
        'pitchData_coordinates_pX': pitchData_coordinates.get('pX'),
        'pitchData_coordinates_pZ': pitchData_coordinates.get('pZ'),
        'pitchData_coordinates_vX0': pitchData_coordinates.get('vX0'),
        'pitchData_coordinates_vY0': pitchData_coordinates.get('vY0'),
        'pitchData_coordinates_vZ0': pitchData_coordinates.get('vZ0'),
        'pitchData_coordinates_x': pitchData_coordinates.get('x'),
        'pitchData_coordinates_y': pitchData_coordinates.get('y'),
        'pitchData_coordinates_x0': pitchData_coordinates.get('x0'),
        'pitchData_coordinates_y0': pitchData_coordinates.get('y0'),
        'pitchData_coordinates_z0': pitchData_coordinates.get('z0'),
        'pitchData_coordinates_aX': pitchData_coordinates.get('aX'),
        'pitchData_breaks_breakAngle': pitchData_breaks.get('breakAngle'),
        'pitchData_breaks_breakLength': pitchData_breaks.get('breakLength'),
        'pitchData_breaks_breakY': pitchData_breaks.get('breakY'),
        'pitchData_breaks_breakVertical': pitchData_breaks.get('breakVertical'),
        'pitchData_breaks_breakVerticalInduced': pitchData_breaks.get('breakVerticalInduced'),
        'pitchData_breaks_breakHorizontal': pitchData_breaks.get('breakHorizontal'),
        'pitchData_breaks_spinRate': pitchData_breaks.get('spinRate'),
        'pitchData_breaks_spinDirection': pitchData_breaks.get('spinDirection'),
        'pitchData_zone': pitchData.get('zone'),
        'pitchData_typeConfidence': pitchData.get('typeConfidence'),
        'pitchData_plateTime': pitchData.get('plateTime'),
        'pitchData_extension': pitchData.get('extension'),
        'index': pitch.get('index'),
        'playId': pitch.get('playId'),
        'pitchNumber': pitch.get('pitchNumber'),
        'startTime': pitch.get('startTime'),
        'endTime': pitch.get('endTime'),
        'isPitch': pitch.get('isPitch'),
        'type': pitch.get('type'),

        'defense_pitcher_id': defense_pitcher.get('id'),
        'defense_pitcher_link': defense_pitcher.get('link'),
        'defense_pitcher_pitchHand_code': defense_pitcher.get('pitchHand', {}).get('code'),
        'defense_pitcher_pitchHand_description': defense_pitcher.get('pitchHand', {}).get('description'),

        'defense_catcher_id': defense_catcher.get('id'),
        'defense_catcher_link': defense_catcher.get('link'),

        'defense_first_id': defense_first.get('id'),
        'defense_first_link': defense_first.get('link'),

        'defense_second_id': defense_second.get('id'),
        'defense_second_link': defense_second.get('link'),

        'defense_third_id': defense_third.get('id'),
        'defense_third_link': defense_third.get('link'),

        'defense_shortstop_id': defense_shortstop.get('id'),
        'defense_shortstop_link': defense_shortstop.get('link'),

        'defense_left_id': defense_left.get('id'),
        'defense_left_link': defense_left.get('link'),

        'defense_center_id': defense_center.get('id'),
        'defense_center_link': defense_center.get('link'),

        'defense_right_id': defense_right.get('id'),
        'defense_right_link': defense_right.get('link'),

        'offense_batter_id': offense_batter.get('id'),
        'offense_batter_link': offense_batter.get('link'),
        'offense_batter_batSide_code': offense_batter.get('batSide', {}).get('code'),
        'offense_batter_batSide_description': offense_batter.get('batSide', {}).get('description'),

        'offense_batterPosition_code': offense_batterPosition.get('code'),
        'offense_batterPosition_name': offense_batterPosition.get('name'),
        'offense_batterPosition_type': offense_batterPosition.get('type'),
        'offense_batterPosition_abbreviation': offense_batterPosition.get('abbreviation'),

        'offense_first_id': offense_first.get('id'),
        'offense_first_link': offense_first.get('link'),

        'offense_second_id': offense_second.get('id'),
        'offense_second_link': offense_second.get('link'),

        'offense_third_id': offense_third.get('id'),
        'offense_third_link': offense_third.get('link'),
    }


def insert_pitch_data(cursor, game_pk, about_atBatIndex, pitch):
    data = flatten_pitch_data(game_pk, about_atBatIndex, pitch)
    sql = """
    INSERT OR REPLACE INTO play_event (
        game_pk,
        about_atBatIndex,
        details_call_code,
        details_call_description,
        details_description,
        details_code,
        details_ballColor,
        details_trailColor,
        details_isInPlay,
        details_isStrike,
        details_isBall,
        details_type_code,
        details_type_description,
        details_isOut,
        details_hasReview,
        count_balls,
        count_strikes,
        count_outs,
        preCount_balls,
        preCount_strikes,
        preCount_outs,
        pitchData_startSpeed,
        pitchData_endSpeed,
        pitchData_strikeZoneTop,
        pitchData_strikeZoneBottom,
        pitchData_coordinates_aY,
        pitchData_coordinates_aZ,
        pitchData_coordinates_pfxX,
        pitchData_coordinates_pfxZ,
        pitchData_coordinates_pX,
        pitchData_coordinates_pZ,
        pitchData_coordinates_vX0,
        pitchData_coordinates_vY0,
        pitchData_coordinates_vZ0,
        pitchData_coordinates_x,
        pitchData_coordinates_y,
        pitchData_coordinates_x0,
        pitchData_coordinates_y0,
        pitchData_coordinates_z0,
        pitchData_coordinates_aX,
        pitchData_breaks_breakAngle,
        pitchData_breaks_breakLength,
        pitchData_breaks_breakY,
        pitchData_breaks_breakVertical,
        pitchData_breaks_breakVerticalInduced,
        pitchData_breaks_breakHorizontal,
        pitchData_breaks_spinRate,
        pitchData_breaks_spinDirection,
        pitchData_zone,
        pitchData_typeConfidence,
        pitchData_plateTime,
        pitchData_extension,
        'index',
        playId,
        pitchNumber,
        startTime,
        endTime,
        isPitch,
        type,
        defense_pitcher_id,
        defense_pitcher_link,
        defense_pitcher_pitchHand_code,
        defense_pitcher_pitchHand_description,
        defense_catcher_id,
        defense_catcher_link,
        defense_first_id,
        defense_first_link,
        defense_second_id,
        defense_second_link,
        defense_third_id,
        defense_third_link,
        defense_shortstop_id,
        defense_shortstop_link,
        defense_left_id,
        defense_left_link,
        defense_center_id,
        defense_center_link,
        defense_right_id,
        defense_right_link,
        offense_batter_id,
        offense_batter_link,
        offense_batter_batSide_code,
        offense_batter_batSide_description,
        offense_batterPosition_code,
        offense_batterPosition_name,
        offense_batterPosition_type,
        offense_batterPosition_abbreviation,
        offense_first_id,
        offense_first_link,
        offense_second_id,
        offense_second_link,
        offense_third_id,
        offense_third_link
    ) VALUES (
        ?,?,?,?,?,?, ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
    )
    """

    values = (
        game_pk,
        about_atBatIndex,
        data.get('details_call_code'),
        data.get('details_call_description'),
        data.get('details_description'),
        data.get('details_code'),
        data.get('details_ballColor'),
        data.get('details_trailColor'),
        data.get('details_isInPlay'),
        data.get('details_isStrike'),
        data.get('details_isBall'),
        data.get('details_type_code'),
        data.get('details_type_description'),
        data.get('details_isOut'),
        data.get('details_hasReview'),
        data.get('count_balls'),
        data.get('count_strikes'),
        data.get('count_outs'),
        data.get('preCount_balls'),
        data.get('preCount_strikes'),
        data.get('preCount_outs'),
        data.get('pitchData_startSpeed'),
        data.get('pitchData_endSpeed'),
        data.get('pitchData_strikeZoneTop'),
        data.get('pitchData_strikeZoneBottom'),
        data.get('pitchData_coordinates_aY'),
        data.get('pitchData_coordinates_aZ'),
        data.get('pitchData_coordinates_pfxX'),
        data.get('pitchData_coordinates_pfxZ'),
        data.get('pitchData_coordinates_pX'),
        data.get('pitchData_coordinates_pZ'),
        data.get('pitchData_coordinates_vX0'),
        data.get('pitchData_coordinates_vY0'),
        data.get('pitchData_coordinates_vZ0'),
        data.get('pitchData_coordinates_x'),
        data.get('pitchData_coordinates_y'),
        data.get('pitchData_coordinates_x0'),
        data.get('pitchData_coordinates_y0'),
        data.get('pitchData_coordinates_z0'),
        data.get('pitchData_coordinates_aX'),
        data.get('pitchData_breaks_breakAngle'),
        data.get('pitchData_breaks_breakLength'),
        data.get('pitchData_breaks_breakY'),
        data.get('pitchData_breaks_breakVertical'),
        data.get('pitchData_breaks_breakVerticalInduced'),
        data.get('pitchData_breaks_breakHorizontal'),
        data.get('pitchData_breaks_spinRate'),
        data.get('pitchData_breaks_spinDirection'),
        data.get('pitchData_zone'),
        data.get('pitchData_typeConfidence'),
        data.get('pitchData_plateTime'),
        data.get('pitchData_extension'),
        data.get('index'),
        data.get('playId'),
        data.get('pitchNumber'),
        data.get('startTime'),
        data.get('endTime'),
        data.get('isPitch'),
        data.get('type'),
        data.get('defense_pitcher_id'),
        data.get('defense_pitcher_link'),
        data.get('defense_pitcher_pitchHand_code'),
        data.get('defense_pitcher_pitchHand_description'),
        data.get('defense_catcher_id'),
        data.get('defense_catcher_link'),
        data.get('defense_first_id'),
        data.get('defense_first_link'),
        data.get('defense_second_id'),
        data.get('defense_second_link'),
        data.get('defense_third_id'),
        data.get('defense_third_link'),
        data.get('defense_shortstop_id'),
        data.get('defense_shortstop_link'),
        data.get('defense_left_id'),
        data.get('defense_left_link'),
        data.get('defense_center_id'),
        data.get('defense_center_link'),
        data.get('defense_right_id'),
        data.get('defense_right_link'),
        data.get('offense_batter_id'),
        data.get('offense_batter_link'),
        data.get('offense_batter_batSide_code'),
        data.get('offense_batter_batSide_description'),
        data.get('offense_batterPosition_code'),
        data.get('offense_batterPosition_name'),
        data.get('offense_batterPosition_type'),
        data.get('offense_batterPosition_abbreviation'),
        data.get('offense_first_id'),
        data.get('offense_first_link'),
        data.get('offense_second_id'),
        data.get('offense_second_link'),
        data.get('offense_third_id'),
        data.get('offense_third_link'),
    )

    cursor.execute(sql, values)


if __name__ == '__main__':
    # logging.basicConfig(level=logging.DEBUG)  # or INFO, WARNING, ERROR, CRITICAL
    conn = sqlite3.connect("mlb.db")
    cursor = conn.cursor()

    rows = cursor.execute("""
        SELECT *
        FROM game
        WHERE status = 'Final'
        ORDER BY game_pk DESC
        LIMIT 100
    """).fetchall()
    for row in rows:
        game_id = row[0]
        print(game_id)
        pbp = statsapi.get('game_winProbability', {'gamePk': game_id})
        for data in pbp:
            for pe in data['playEvents']:
                if pe.get('details', {}).get('call', {}).get('code'):
                    insert_pitch_data(cursor, game_id, data["about"]["atBatIndex"], pe)

    conn.commit()
    conn.close()