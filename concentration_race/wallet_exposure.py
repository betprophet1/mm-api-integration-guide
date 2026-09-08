"""GEC/LEC wallet-exposure helpers, shared by concentration_race.rapid_fire's
--check-exposure flag and the exposure_credit/ generate-then-spend scripts.

Background (confirmed against ss-exposure-app / ss-wallet-app source, and live
on sandbox + QA):

- GEC (Global Exposure Credit): `wallet.exposureCredit` on `GET /api/v1/wallet`.
  Granted once a user has matched wagers on BOTH outcomes of a market (any
  market/event -- GEC is spendable anywhere). Formula (moneyline):
    GEC = max(0, min(0, p1-s2, p2-s1) + cash)
  where p/s are matched profit/stake per outcome and `cash` is the CASH-funded
  (not credit-funded) portion of those matches. It scales with stake size and
  odds skew, not with wager count -- confirmed live: 1500 tiny ($2-10) wagers
  produced a $2 spend total, while a single ~$5 both-sides match produced
  GEC in the ~$1-2 range consistent with this formula.
- LEC (Local Exposure Credit): per (event, market, outcome, line) via
  `GET /api/v2/wallet/exposures`. Granted on the OPPOSITE outcome+line of a
  matched wager -- confirmed live, and NOT simply "this outcome's own stake":
  a match on outcome 5 moved outcome 4's balance, not outcome 5's.
- Both types are computed asynchronously, not at match time: ss-exposure-app's
  processor ticks every 5s (batch 100 wagers/tick), and ss-wallet-app polls it
  back every 5s (`run_sync_exposure_data_to_wallet_detail.sh 5`, confirmed in
  ss-cloud-platform-cd). A burst of wagers can take well over the 6-10s this
  repo's other scripts wait -- poll, don't just sleep once.
- Spending is automatic, not requested: `ss-exposure-app`'s designateMatchedStake
  funds a NEW wager's stake from available LEC (if same outcome+line) or GEC
  (any market) before cash. Place a wager within the available credit and it
  spends on its own.
"""
import logging
from urllib.parse import urljoin

import requests

from src import config

# Matches ss-exposure-app's processor tick / ss-wallet-app's sync poll (both 5s).
EXPOSURE_SYNC_POLL_INTERVAL_SEC = 5.0
EXPOSURE_SYNC_TIMEOUT_SEC = 90.0


def get_gec(framework, account_name):
    """GEC snapshot: wallet.exposureCredit, from the same /api/v1/wallet payload
    framework.get_balance() already fetches for patron-type accounts. Returns
    None if unavailable (e.g. the account has no web token)."""
    try:
        return framework.get_balance(account_name).get('exposureCredit')
    except Exception as e:
        logging.warning(f"GEC check failed for {account_name}: {e}")
        return None


def get_lec_snapshot(framework, account_name, event_id):
    """LEC snapshot: GET /api/v2/wallet/exposures, scoped to event_id (a required
    param -- confirmed live, 422 without it). Returns {(marketId, outcomeId):
    balance} for event_id's entries, or None on failure.

    Confirmed live field names: each entry has eventId/marketId/outcomeId/balance/
    line. An account can carry entries for OTHER events/markets from prior activity
    -- always compare the same (marketId, outcomeId) key before/after, never just
    entry count: a populated entry's balance can also move *down* as exposure
    nets out, not just appear from empty."""
    url = urljoin(framework.base_url, 'api/v2/wallet/exposures')
    try:
        response = requests.get(url, headers=framework.get_auth_header(account_name),
                                 params={'eventIds': str(event_id)}, timeout=10)
        if response.status_code != 200:
            logging.warning(f"LEC check failed for {account_name}: {response.status_code} "
                             f"{response.text[:200]}")
            return None
        entries = response.json().get('data', [])
        return {(e.get('marketId'), e.get('outcomeId')): e.get('balance', 0)
                for e in entries if e.get('eventId') == event_id}
    except Exception as e:
        logging.warning(f"LEC check exception for {account_name}: {e}")
        return None


def lec_changed(before, after):
    """True if any (marketId, outcomeId) entry's balance differs, or a key was
    added/removed, between two get_lec_snapshot() results. False (not None) if
    either snapshot failed, so a fetch failure doesn't masquerade as 'changed'."""
    if before is None or after is None:
        return False
    return before != after


def login_maker_web_session(framework, maker_account_num, maker_name):
    """Log in a second, web-token session for a maker account, needed for GEC/LEC
    (partner/auth/login gives only an MM token). Session name: f'{maker_name}_web'.
    Returns that name, or None if the account has no email/password on file (not
    every MM account does)."""
    creds = config.get_account_credentials(maker_account_num, framework.environment)
    if not creds.get('email'):
        return None
    web_name = f'{maker_name}_web'
    framework.login_account(web_name, {'email': creds['email'], 'password': creds['password']},
                             account_type='patron')
    return web_name


def wait_for_gec_change(framework, account_name, baseline_gec,
                         poll_interval=EXPOSURE_SYNC_POLL_INTERVAL_SEC,
                         timeout=EXPOSURE_SYNC_TIMEOUT_SEC, on_poll=None):
    """Poll GEC every `poll_interval`s (default matches the pipeline's own 5s
    cadence) until it differs from `baseline_gec`, or `timeout`s elapse.

    Returns (changed: bool, final_gec). Prefer this over a fixed sleep(30-60) --
    the exposure processor + wallet sync round-trip is two independent 5s-tick
    loops plus a variable-size batch backlog, so a fixed wait either
    under-waits (checks too early) or over-waits (wastes time every run)."""
    import time
    elapsed = 0.0
    while elapsed <= timeout:
        current = get_gec(framework, account_name)
        if on_poll:
            on_poll(elapsed, current)
        if current is not None and baseline_gec is not None and current != baseline_gec:
            return True, current
        time.sleep(poll_interval)
        elapsed += poll_interval
    return False, get_gec(framework, account_name)


def wait_for_lec_change(framework, account_name, event_id, baseline_lec,
                         poll_interval=EXPOSURE_SYNC_POLL_INTERVAL_SEC,
                         timeout=EXPOSURE_SYNC_TIMEOUT_SEC, on_poll=None):
    """Same as wait_for_gec_change, for LEC (scoped to event_id). Returns
    (changed: bool, final_lec_snapshot)."""
    import time
    elapsed = 0.0
    while elapsed <= timeout:
        current = get_lec_snapshot(framework, account_name, event_id)
        if on_poll:
            on_poll(elapsed, current)
        if lec_changed(baseline_lec, current):
            return True, current
        time.sleep(poll_interval)
        elapsed += poll_interval
    return False, get_lec_snapshot(framework, account_name, event_id)
