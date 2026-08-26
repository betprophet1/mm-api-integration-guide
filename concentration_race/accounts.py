"""Account setup for the concentration-race scenario.

Maker and taker are deliberately different account *types*: MM accounts are
always makers, patron accounts are always takers -- this mirrors the real
trading model (market makers post liquidity, patrons take it) and isn't
something the script chooses, so adding accounts widens one side, it never
turns a patron into a maker or vice versa.

Multiple maker/taker accounts are supported, but MM identity is not the same
as MM credential: mm1/mm2 resolve to the same underlying partner on staging
(confirmed via identical /partner/mm/get_balance responses), so a second MM
credential is not automatically a second maker. login_makers() checks the
partnerID claim in each account's JWT and fails fast on a collision, instead
of letting it surface later as a confusing pile of self_match wager errors.
"""
import base64
import json

from src import config


def _extract_partner_id(access_token):
    """Decode the JWT payload's partnerID claim, without verifying the signature."""
    try:
        payload = access_token.split('.')[1]
        padding = 4 - (len(payload) % 4)
        if padding != 4:
            payload += '=' * padding
        return json.loads(base64.urlsafe_b64decode(payload)).get('partnerID', 'unknown')
    except Exception:
        return 'unknown'


def login_makers(framework, account_nums):
    """Log in one or more MM accounts as makers.

    account_nums: e.g. [1, 2] or ['exposure_mm1', 'exposure_mm2'] -- passed
    straight to config.get_account_credentials.

    Raises if two accounts resolve to the same partnerID (see module
    docstring) -- self_match would silently drop those wagers instead of
    matching them, which is worse than failing fast here.

    Returns list of account names used, in the same order as account_nums.
    """
    names = []
    partner_of = {}  # partnerID -> account name that first claimed it
    for num in account_nums:
        name = f'maker{num}'
        creds = config.get_account_credentials(num, framework.environment)
        session = framework.login_account(name, creds, account_type='mm')
        partner_id = _extract_partner_id(session.get('access_token', ''))
        if partner_id in partner_of:
            raise Exception(
                f"Maker account {num} ({name}) resolves to the same partner as "
                f"{partner_of[partner_id]} (partnerID={partner_id}) -- wagers between "
                f"them would be rejected as self_match. Use a genuinely distinct MM credential."
            )
        partner_of[partner_id] = name
        names.append(name)
    return names


def login_takers(framework, account_ids):
    """Log in one or more patron accounts as takers.

    account_ids: e.g. ['patron', 'patron3'] -- 'patron' loads
    user_info_patron_{env}.json (the default staging taker), anything else
    loads user_info_{account_id}_{env}.json (e.g. patron3, patron8).

    Returns list of account names used, in the same order as account_ids.
    """
    names = []
    for account_id in account_ids:
        name = f'taker_{account_id}'
        folder_filename = 'patron.json' if account_id == 'patron' else f'{account_id}.json'
        legacy_flat_filename = (f'user_info_patron_{framework.environment}.json' if account_id == 'patron'
                                 else f'user_info_{account_id}_{framework.environment}.json')
        patron_config = config.load_env_account_config(framework.environment, folder_filename, legacy_flat_filename)
        creds = {'username': patron_config['email'], 'password': patron_config['password']}
        framework.login_account(name, creds, account_type='patron')
        names.append(name)
    return names
