import json
import os

# Get the directory where config.py is located
script_dir = os.path.dirname(os.path.abspath(__file__))

# Environment configuration
ENVIRONMENT = os.getenv('MM_ENVIRONMENT', 'sandbox')  # Default to sandbox

# Environment-specific URLs
ENVIRONMENT_URLS = {
    'sandbox': 'https://api.sandbox.prophetx.dev',
    'staging': 'https://api.staging.prophetx.dev',
    'qa': 'https://api.qa.prophetx.dev'
}

def load_user_config(config_file='user_info.json'):
    """Load user configuration from specified file"""
    user_info_path = os.path.join(script_dir, config_file)
    with open(user_info_path) as fp:
        return json.load(fp)

def load_env_account_config(env, folder_filename, legacy_flat_filename):
    """Load one account's credentials for `env`.

    New environments (e.g. qa) group their account files under
    src/accounts/{env}/{folder_filename}. Environments not yet migrated
    (sandbox, staging) keep their original flat src/{legacy_flat_filename}
    layout -- that path is only used when no folder file exists, so this
    is a no-op for them.
    """
    folder_path = os.path.join(script_dir, 'accounts', env, folder_filename)
    if os.path.exists(folder_path):
        with open(folder_path) as fp:
            return json.load(fp)
    return load_user_config(legacy_flat_filename)

# Load default account (Account 1) for current environment
user_info_dict = load_env_account_config(
    ENVIRONMENT, 'account1.json',
    'user_info.json' if ENVIRONMENT == 'sandbox' else f'user_info_{ENVIRONMENT}.json')

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
        legacy = 'user_info.json' if env == 'sandbox' else f'user_info_{env}.json'
        config = load_env_account_config(env, 'account1.json', legacy)
    elif isinstance(account_num, int) and 2 <= account_num <= 10:
        legacy = (f'user_info_account{account_num}.json' if env == 'sandbox'
                  else f'user_info_account{account_num}_{env}.json')
        config = load_env_account_config(env, f'account{account_num}.json', legacy)
    elif account_num == 'patron':
        config = load_env_account_config(env, 'patron.json', f'user_info_patron_{env}.json')
    elif isinstance(account_num, str) and account_num.startswith('exposure_mm'):
        # Exposure MM accounts from user_info_exposure.json
        exposure_config = load_user_config('user_info_exposure.json')
        config = exposure_config[account_num]
    elif isinstance(account_num, str) and account_num.startswith('patron'):
        # Support patron3, patron4, etc.
        config = load_env_account_config(env, f'{account_num}.json', f'user_info_{account_num}_{env}.json')
    else:
        raise ValueError(f"Account {account_num} not supported")

    return {
        'access_key': config['access_key'],
        'secret_key': config['secret_key'],
        'tournaments': config.get('tournaments', ['MLB']),
        # Present only for accounts that also have a web login (needed for GEC/LEC,
        # which requires a web token -- partner/auth/login gives an MM-only token).
        'email': config.get('email'),
        'password': config.get('password'),
    }

# Set BASE_URL based on environment
BASE_URL = ENVIRONMENT_URLS.get(ENVIRONMENT, ENVIRONMENT_URLS['sandbox'])
URL = {
    'mm_login': 'partner/auth/login',
    'mm_refresh': 'partner/auth/refresh',
    'mm_ping': 'partner/mm/pusher/ping',
    'mm_auth': 'partner/v4/mm/pusher',
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
