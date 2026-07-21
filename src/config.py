import json
import os

_here = os.path.dirname(os.path.abspath(__file__))
with open(os.path.join(_here, 'user_info.json')) as fp:
    user_info_dict = json.load(fp)

MM_KEYS = {
    'access_key': user_info_dict['access_key'],
    'secret_key': user_info_dict['secret_key'],
}

TOURNAMENTS_INTERESTED = user_info_dict['tournaments']

BASE_URL = 'https://api-ss-qa.betprophet.co'
URL = {
    'mm_login': 'partner/auth/login',
    'mm_refresh': 'partner/auth/refresh',
    'mm_ping': 'partner/mm/pusher/ping',
    'mm_auth': 'partner/mm/pusher',
    'mm_tournaments': 'partner/mm/get_tournaments',
    'mm_events': 'partner/mm/get_sport_events',
    'mm_markets': 'partner/v4/mm/get_markets',
    'mm_multiple_markets': 'partner/v4/mm/get_multiple_markets',
    'mm_balance': 'partner/v4/mm/get_balance',
    'mm_place_wager': 'partner/v4/mm/submit_order',
    'mm_cancel_wager': 'partner/v4/mm/cancel_order',
    'mm_odds_ladder': 'partner/v4/mm/get_odds_ladder',
    'mm_batch_cancel': 'partner/v4/mm/cancel_multiple_orders',
    'mm_batch_place': 'partner/v4/mm/submit_multiple_orders',
    'mm_cancel_all_wagers': 'partner/v4/mm/cancel_all_orders',
    'websocket_config': 'partner/websocket/connection-config',
}
