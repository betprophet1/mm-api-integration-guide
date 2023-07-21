import json

with open('user_info.json') as fp:
    user_info_dict = json.load(fp)

MM_KEYS = {
    'access_key': user_info_dict['access_key'],
    'secret_key': user_info_dict['secret_key'],
}

MM_APP_KEY = '30f867796adefdeecb75'
APP_CLUSTER = 'us2'
TOURNAMENTS_INTERESTED = user_info_dict['tournaments']
LOAD_ALL_TOURNAMENTS = user_info_dict['load_all_tournaments']

BASE_URL = 'https://api-sandbox.betprophet.co'
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
