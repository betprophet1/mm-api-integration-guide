import json

with open('user_info_staging_mmi.json') as fp:
    user_info_dict = json.load(fp)

MM_KEYS = {
    'access_key': user_info_dict['access_key'],
    'secret_key': user_info_dict['secret_key'],
}

IS_INDIVIDUAL = True
MM_APP_KEY = '1d9316f2cf4c0c40efe1'
APP_CLUSTER = 'mt1'
TOURNAMENTS_INTERESTED = user_info_dict['tournaments']

BASE_URL = 'https://api-staging.betprophet.co'
URL = {
    'mm_login': 'partner/auth/login',
    'mm_refresh': 'partner/auth/refresh',
    'mm_ping': 'partner/mm/pusher/ping',
    'mm_auth': 'partner/mm/pusher',
    'mm_tournaments': 'partner/mm/get_tournaments',
    'mm_events': 'partner/mm/get_sport_events',
    'mm_markets': 'partner/mm/get_markets',
    'mm_balance': 'partner/mm/get_balance',
    'mm_place_wager': 'partner/mm/place_wager',
    'mm_cancel_wager': 'partner/mm/cancel_wager',
    'mm_odds_ladder': 'partner/mm/get_odds_ladder',
    'mm_batch_cancel': 'partner/mm/cancel_multiple_wagers',
    'mm_batch_place': 'partner/mm/place_multiple_wagers',
}
