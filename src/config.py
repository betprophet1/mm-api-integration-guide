import json
import os

# Get the directory where config.py is located
script_dir = os.path.dirname(os.path.abspath(__file__))

def load_user_config(config_file='user_info.json'):
    """Load user configuration from specified file"""
    user_info_path = os.path.join(script_dir, config_file)
    with open(user_info_path) as fp:
        return json.load(fp)

# Load default account (Account 1)
user_info_dict = load_user_config('user_info.json')

MM_KEYS = {
    'access_key': user_info_dict['access_key'],
    'secret_key': user_info_dict['secret_key'],
}

TOURNAMENTS_INTERESTED = user_info_dict['tournaments']

# Function to get credentials for specific account
def get_account_credentials(account_num=1):
    """Get credentials for specified account number"""
    if account_num == 1:
        config = load_user_config('user_info.json')
    elif account_num == 2:
        config = load_user_config('user_info_account2.json')
    else:
        raise ValueError(f"Account {account_num} not supported")
    
    return {
        'access_key': config['access_key'],
        'secret_key': config['secret_key'],
        'tournaments': config['tournaments']
    }

BASE_URL = 'https://api-ss-sandbox.betprophet.co'
URL = {
    'mm_login': 'partner/auth/login',
    'mm_refresh': 'partner/auth/refresh',
    'mm_ping': 'partner/mm/pusher/ping',
    'mm_auth': 'partner/mm/pusher',
    'mm_tournaments': 'partner/mm/get_tournaments',
    'mm_events': 'partner/mm/get_sport_events',
    'mm_markets': 'partner/mm/get_markets',
    'mm_multiple_markets': 'partner/mm/get_multiple_markets',
    'mm_balance': 'partner/mm/get_balance',
    'mm_place_wager': 'partner/mm/place_wager',
    'mm_cancel_wager': 'partner/mm/cancel_wager',
    'mm_odds_ladder': 'partner/mm/get_odds_ladder',
    'mm_batch_cancel': 'partner/mm/cancel_multiple_wagers',
    'mm_batch_place': 'partner/mm/place_multiple_wagers',
    'mm_cancel_all_wagers': 'partner/mm/cancel_all_wagers',
    'websocket_config': 'partner/websocket/connection-config',
    # Exposure testing endpoints
    'exposure_balance': 'partner/exposure/get_balance',
    'exposure_credits': 'partner/exposure/get_credits',
    'exposure_sync': 'partner/exposure/sync',
}
