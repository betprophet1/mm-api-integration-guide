import json
import os

# Get the directory where config.py is located
script_dir = os.path.dirname(os.path.abspath(__file__))

# Environment configuration
ENVIRONMENT = os.getenv('MM_ENVIRONMENT', 'sandbox')  # Default to sandbox

# Environment-specific URLs
ENVIRONMENT_URLS = {
    'sandbox': 'https://api-ss-sandbox.betprophet.co',
    'staging': 'https://api-ss-staging.betprophet.co'
}

def load_user_config(config_file='user_info.json'):
    """Load user configuration from specified file"""
    user_info_path = os.path.join(script_dir, config_file)
    with open(user_info_path) as fp:
        return json.load(fp)

# Load default account (Account 1) for current environment
default_config_file = f'user_info_{ENVIRONMENT}.json' if ENVIRONMENT != 'sandbox' else 'user_info.json'
user_info_dict = load_user_config(default_config_file)

MM_KEYS = {
    'access_key': user_info_dict['access_key'],
    'secret_key': user_info_dict['secret_key'],
}

TOURNAMENTS_INTERESTED = user_info_dict['tournaments']

# Function to get credentials for specific account and environment
def get_account_credentials(account_num=1, environment=None):
    """Get credentials for specified account number and environment"""
    env = environment or ENVIRONMENT
    
    if account_num == 1:
        if env == 'sandbox':
            config = load_user_config('user_info.json')
        else:
            config = load_user_config(f'user_info_{env}.json')
    elif account_num == 2:
        if env == 'sandbox':
            config = load_user_config('user_info_account2.json')
        else:
            config = load_user_config(f'user_info_account2_{env}.json')
    elif account_num == 3:
        if env == 'sandbox':
            config = load_user_config('user_info_account3.json')
        else:
            config = load_user_config(f'user_info_account3_{env}.json')
    elif account_num == 4:
        if env == 'sandbox':
            config = load_user_config('user_info_account4.json')
        else:
            config = load_user_config(f'user_info_account4_{env}.json')
    elif account_num == 5:
        if env == 'sandbox':
            config = load_user_config('user_info_account5.json')
        else:
            config = load_user_config(f'user_info_account5_{env}.json')
    elif account_num == 6:
        if env == 'sandbox':
            config = load_user_config('user_info_account6.json')
        else:
            config = load_user_config(f'user_info_account6_{env}.json')
    elif account_num == 7:
        if env == 'sandbox':
            config = load_user_config('user_info_account7.json')
        else:
            config = load_user_config(f'user_info_account7_{env}.json')
    elif account_num == 8:
        if env == 'sandbox':
            config = load_user_config('user_info_account8.json')
        else:
            config = load_user_config(f'user_info_account8_{env}.json')
    elif account_num == 9:
        if env == 'sandbox':
            config = load_user_config('user_info_account9.json')
        else:
            config = load_user_config(f'user_info_account9_{env}.json')
    elif account_num == 10:
        if env == 'sandbox':
            config = load_user_config('user_info_account10.json')
        else:
            config = load_user_config(f'user_info_account10_{env}.json')
    elif account_num == 'patron':
        config = load_user_config(f'user_info_patron_{env}.json')
    elif isinstance(account_num, str) and account_num.startswith('exposure_mm'):
        # Exposure MM accounts from user_info_exposure.json
        exposure_config = load_user_config('user_info_exposure.json')
        config = exposure_config[account_num]
    elif isinstance(account_num, str) and account_num.startswith('patron'):
        # Support patron3, patron4, etc.
        config = load_user_config(f'user_info_{account_num}_{env}.json')
    else:
        raise ValueError(f"Account {account_num} not supported")
    
    return {
        'access_key': config['access_key'],
        'secret_key': config['secret_key'],
        'tournaments': config.get('tournaments', ['MLB'])
    }

# Set BASE_URL based on environment
BASE_URL = ENVIRONMENT_URLS.get(ENVIRONMENT, ENVIRONMENT_URLS['sandbox'])
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
    'mm_get_matched_bets': 'partner/mm/get_matched_bets',
    'websocket_config': 'partner/websocket/connection-config',
    # Exposure testing endpoints
    'exposure_balance': 'partner/exposure/get_balance',
    'exposure_credits': 'partner/exposure/get_credits',
    'exposure_sync': 'partner/exposure/sync',
}
