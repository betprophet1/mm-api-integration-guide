#!/usr/bin/env python3
"""
Race Condition Test: Bet Batch vs Cancel Batch

Simulates the scenario where:
- MM1 (deduce) and MM2 (non-deduce) rapidly place & cancel bets
- Patron4 (deduce) and Patron8 (non-deduce) try to place against MM bets

Two cancel strategies (both used in production):
  1. cancel_multiple_wagers — batch cancel with SAME wager IDs from place response (primary)
  2. cancel_all_wagers — periodic sweep cancel without wager IDs (every N cycles)

Modes:
  --deducemode    : MM1(deduce) + MM2(non-deduce) vs Patron4(deduce) + Patron8(non-deduce)
  (default)       : MM2(non-deduce) vs Patron8(non-deduce) only

Race condition target:
  MM sends place_batch → gets wager IDs → immediately sends cancel_batch with same IDs
  Backend receives cancel while open wager_jobs are still processing
  Meanwhile patrons race to match before cancel goes through

Usage:
    python test_backend_fairness.py --event "20023350" --cycles 50
    python test_backend_fairness.py --event "20023350" --cycles 100 --batch-size 10 --deducemode
    python test_backend_fairness.py --event "20023350" --cycles 20 --verbose --workers 50
"""

import argparse
import time
import threading
import random
import json
import os
import sys
import uuid
import base64
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
from urllib.parse import urljoin
from datetime import datetime

from src import mm_calls
from src.log import logging
from src import config

# ============================================================================
# Global shared state
# ============================================================================
# Patron bet queue: MM places bets → adds line_id/odds here → Patron picks up
patron_bet_queue = []
patron_queue_lock = threading.Lock()

# Race condition metrics
race_metrics = {
    'total_cycles': 0,
    'total_bets_placed': 0,
    'total_bets_place_failed': 0,
    # batch cancel (cancel_multiple_wagers) metrics
    'batch_cancel_sent': 0,
    'batch_cancel_succeeded': 0,
    'batch_cancel_failed': 0,
    'batch_cancel_per_wager_ok': 0,
    'batch_cancel_per_wager_fail': 0,
    # cancel_all metrics
    'cancel_all_sent': 0,
    'cancel_all_succeeded': 0,
    'cancel_all_failed': 0,
    # combined cancel failure breakdown
    'cancel_fail_still_processing': 0,  # Race condition: bet batch still processing
    'cancel_fail_already_matched': 0,   # Patron matched before cancel arrived
    'cancel_fail_already_cancelled': 0, # Already cancelled
    'cancel_fail_not_found': 0,         # 404 - wager not found
    'cancel_fail_placing': 0,           # 409 - wager is still placing (bet delay)
    'cancel_fail_other': 0,             # Other failures
    'patron_bets_attempted': 0,
    'patron_bets_succeeded': 0,
    'patron_bets_failed': 0,
    'race_condition_gaps': [],           # Actual time gaps between place fire and cancel fire
    'cancel_error_details': [],          # Detailed error messages for analysis
}
race_lock = threading.Lock()

# Per-user metrics
user_metrics = defaultdict(lambda: {
    'placed': 0,
    'cancelled': 0,
    'cancel_failed': 0,
    'matched_by_patron': 0,
    'response_times_place': [],
    'response_times_cancel': [],
})


# ============================================================================
# Account loading
# ============================================================================
def load_mm_account(account_num, environment='sandbox'):
    """Load a single MM account"""
    credentials = config.get_account_credentials(account_num, environment)

    mm_instance = mm_calls.MMInteractions()
    mm_instance.mm_keys = {
        'access_key': credentials['access_key'],
        'secret_key': credentials['secret_key']
    }

    mm_instance.mm_login()
    mm_instance.get_balance()
    mm_instance.seeding()

    # Extract partner UUID from JWT
    user_id = _extract_user_id(mm_instance)
    mm_instance.user_id = user_id
    mm_instance.account_num = account_num

    return mm_instance, user_id


def load_patron_account(patron_num, environment='sandbox'):
    """Load a patron account by number

    Patron accounts use email/password web auth, not API keys.
    - patron_num=4 → user_info_patron_{env}.json  (deduce, usr004)
    - patron_num=8 → user_info_patron8_{env}.json  (non-deduce, usr008)
    """
    if patron_num == 4:
        # Patron4 uses the existing patron config
        folder_filename, legacy_filename = 'patron.json', f'user_info_patron_{environment}.json'
    else:
        folder_filename = f'patron{patron_num}.json'
        legacy_filename = f'user_info_patron{patron_num}_{environment}.json'

    patron_config = config.load_env_account_config(environment, folder_filename, legacy_filename)

    base_url = config.ENVIRONMENT_URLS.get(environment, config.ENVIRONMENT_URLS['sandbox'])

    # Login
    login_url = urljoin(base_url, 'api/v1/auth/login')
    device_id = str(uuid.uuid1())
    headers = {
        '__source': 'web',
        'accept': 'application/json, text/plain, */*',
        'content-type': 'application/json',
        'origin': base_url.replace('api-', ''),
        'x-currency': 'cash'
    }
    request_body = {
        'email': patron_config['email'],
        'password': patron_config['password'],
        'device_id': device_id
    }

    response = requests.post(login_url, headers=headers, json=request_body)
    if response.status_code != 200:
        raise Exception(f"Patron{patron_num} login failed: {response.status_code} - {response.text}")

    jwt_token = response.json().get('accessToken')
    if not jwt_token:
        raise Exception(f"Patron{patron_num} no access token received")

    # Get balance
    balance_url = urljoin(base_url, 'api/v1/wallet')
    auth_headers = {
        'Authorization': f'Bearer {jwt_token}',
        'Content-Type': 'application/json',
        'x-currency': 'cash',
        'accept': 'application/json'
    }
    balance_resp = requests.get(balance_url, headers=auth_headers)
    balance = 0
    if balance_resp.status_code == 200:
        balance = balance_resp.json().get('data', {}).get('balance', 0)

    patron_info = {
        'patron_num': patron_num,
        'email': patron_config['email'],
        'jwt': jwt_token,
        'base_url': base_url,
        'balance': balance,
        'environment': environment,
        'device_id': device_id,
        'password': patron_config['password'],
    }
    logging.info(f"  Patron{patron_num}: {patron_config['email']} (Balance: ${balance:,.2f})")
    return patron_info


def _extract_user_id(mm_instance):
    """Extract partner UUID from JWT token"""
    access_token = mm_instance.mm_session.get('access_token', '')
    try:
        parts = access_token.split('.')
        if len(parts) >= 2:
            payload = parts[1]
            padding = 4 - (len(payload) % 4)
            if padding != 4:
                payload += '=' * padding
            decoded = json.loads(base64.urlsafe_b64decode(payload))
            return decoded.get('partnerID', 'unknown')
    except Exception:
        pass
    return mm_instance.mm_keys.get('access_key', 'unknown')[:8]


def _refresh_patron_token(patron_info):
    """Refresh patron JWT token"""
    try:
        login_url = urljoin(patron_info['base_url'], 'api/v1/auth/login')
        headers = {
            '__source': 'web',
            'accept': 'application/json, text/plain, */*',
            'content-type': 'application/json',
            'origin': patron_info['base_url'].replace('api-', ''),
            'x-currency': 'cash'
        }
        request_body = {
            'email': patron_info['email'],
            'password': patron_info['password'],
            'device_id': patron_info['device_id']
        }
        response = requests.post(login_url, headers=headers, json=request_body)
        if response.status_code == 200:
            patron_info['jwt'] = response.json().get('accessToken')
            logging.info(f"  Patron{patron_info['patron_num']} token refreshed")
            return True
    except Exception as e:
        logging.error(f"  Patron{patron_info['patron_num']} token refresh failed: {e}")
    return False


# ============================================================================
# Rate limiter
# ============================================================================
class RateLimiter:
    """Thread-safe token-bucket rate limiter for wager RPS control"""
    def __init__(self, max_wagers_per_sec):
        self.min_interval = 1.0 / max_wagers_per_sec if max_wagers_per_sec > 0 else 0
        self.lock = threading.Lock()
        self.tokens = 0
        self.last_refill = time.time()
        self.max_wagers_per_sec = max_wagers_per_sec

    def acquire(self, num_wagers=1):
        """Block until we have capacity for num_wagers"""
        if self.max_wagers_per_sec <= 0:
            return
        sleep_time = num_wagers * self.min_interval
        with self.lock:
            now = time.time()
            elapsed = now - self.last_refill
            # Simple: ensure minimum spacing between bursts
            if elapsed < sleep_time:
                wait = sleep_time - elapsed
                time.sleep(wait)
            self.last_refill = time.time()


# ============================================================================
# MM Place & Cancel cycle (the race condition producer)
# ============================================================================
def _build_batch_wagers(mm_instance, markets, batch_size, random_stake_range):
    """Build a batch of wagers with random odds & stakes"""
    batch = []
    for i in range(batch_size):
        market = markets[i % len(markets)]
        line_id = market['line_id'] if isinstance(market, dict) else market
        odds = mm_instance._MMInteractions__get_random_odds()
        stake = round(random.uniform(*random_stake_range), 2)
        batch.append({
            'external_id': str(uuid.uuid1()),
            'line_id': line_id,
            'odds': odds,
            'stake': stake,
        })
    return batch


def _send_batch_place(mm_instance, user_id, batch_wagers, verbose=False):
    """Send batch place and return placed wagers list"""
    batch_play_url = urljoin(mm_instance.base_url, config.URL['mm_batch_place'])
    place_start = time.time()
    try:
        place_resp = requests.post(batch_play_url, json={"data": batch_wagers},
                                   headers=mm_instance._MMInteractions__get_auth_header())
    except Exception as e:
        logging.error(f"  MM {user_id[:8]} place exception: {e}")
        with race_lock:
            race_metrics['total_bets_place_failed'] += len(batch_wagers)
        return [], time.time() - place_start

    place_elapsed = time.time() - place_start
    placed = []
    if place_resp.status_code == 200:
        try:
            for w in place_resp.json().get('data', {}).get('succeed_wagers', []):
                placed.append({'external_id': w['external_id'], 'wager_id': w['id']})
                mm_instance.wagers[w['external_id']] = w['id']
        except Exception as e:
            logging.error(f"  MM {user_id[:8]} place parse error: {e}")
    elif verbose:
        logging.warning(f"  MM {user_id[:8]} place failed: {place_resp.status_code} - {place_resp.text[:200]}")

    num_placed = len(placed)
    with race_lock:
        race_metrics['total_bets_placed'] += num_placed
        race_metrics['total_bets_place_failed'] += (len(batch_wagers) - num_placed)
        user_metrics[user_id]['placed'] += num_placed
        user_metrics[user_id]['response_times_place'].append(place_elapsed)

    return placed, place_elapsed


def _classify_cancel_error(error_code, error_text):
    """Classify a cancel error into a known category and return the metric key."""
    error_lower = error_text.lower()
    if error_code == 404:
        return 'cancel_fail_not_found'
    elif 'already cancelled' in error_lower or 'already canceled' in error_lower:
        return 'cancel_fail_already_cancelled'
    elif 'already_matched' in error_lower or 'already matched' in error_lower or 'matched' in error_lower:
        return 'cancel_fail_already_matched'
    elif error_code == 409 or 'placing' in error_lower or 'placing play' in error_lower:
        return 'cancel_fail_placing'
    elif ('processing' in error_lower or 'pending' in error_lower
          or error_code == 422 or error_code == 500):
        return 'cancel_fail_still_processing'
    else:
        return 'cancel_fail_other'


def _send_batch_cancel(mm_instance, user_id, placed_wagers, place_end_time, verbose=False):
    """
    Send cancel_multiple_wagers with the SAME wager IDs returned from place response.
    This replicates the production pattern (SSE-1767/SSE-1858).

    :param placed_wagers: list of {'external_id': ..., 'wager_id': ...} from place response
    :param place_end_time: timestamp when place response was received
    """
    if not placed_wagers:
        return

    batch_cancel_url = urljoin(mm_instance.base_url, config.URL['mm_batch_cancel'])
    batch_cancel_body = [{
        'wager_id': w['wager_id'],
        'external_id': w['external_id']
    } for w in placed_wagers]

    cancel_start = time.time()
    actual_gap = cancel_start - place_end_time  # Gap from place RESPONSE to cancel REQUEST

    try:
        cancel_resp = requests.post(batch_cancel_url, json={'data': batch_cancel_body},
                                    headers=mm_instance._MMInteractions__get_auth_header())
    except Exception as e:
        logging.error(f"  MM {user_id[:8]} batch_cancel exception: {e}")
        with race_lock:
            race_metrics['batch_cancel_failed'] += 1
            race_metrics['cancel_fail_other'] += len(placed_wagers)
        return

    cancel_elapsed = time.time() - cancel_start

    with race_lock:
        race_metrics['batch_cancel_sent'] += 1
        race_metrics['race_condition_gaps'].append(actual_gap)
        user_metrics[user_id]['response_times_cancel'].append(cancel_elapsed)

    if cancel_resp.status_code == 200:
        # Parse per-wager results from batch cancel response
        try:
            resp_data = cancel_resp.json().get('data', [])
            per_ok = 0
            per_fail = 0
            for item in resp_data:
                if item.get('success'):
                    per_ok += 1
                else:
                    per_fail += 1
                    err = item.get('error', {})
                    err_msg = err.get('message', '') if isinstance(err, dict) else str(err)
                    err_code = err.get('error_code', 0) if isinstance(err, dict) else 0
                    status_code = item.get('statusCode', 0)
                    category = _classify_cancel_error(status_code, err_msg)
                    with race_lock:
                        race_metrics[category] += 1
                        race_metrics['cancel_error_details'].append({
                            'timestamp': datetime.now().isoformat(),
                            'cancel_type': 'batch',
                            'mm_user': user_id[:8],
                            'status_code': status_code,
                            'error': err_msg[:200],
                            'gap_ms': actual_gap * 1000,
                        })
            with race_lock:
                race_metrics['batch_cancel_succeeded'] += 1
                race_metrics['batch_cancel_per_wager_ok'] += per_ok
                race_metrics['batch_cancel_per_wager_fail'] += per_fail
                user_metrics[user_id]['cancelled'] += per_ok
                user_metrics[user_id]['cancel_failed'] += per_fail

            # Clean up wagers dict
            for w in placed_wagers:
                mm_instance.wagers.pop(w['external_id'], None)

            if verbose:
                logging.info(f"  MM {user_id[:8]} batch_cancel OK {per_ok}/{len(placed_wagers)} "
                             f"({cancel_elapsed*1000:.0f}ms, gap={actual_gap*1000:.1f}ms)"
                             f"{f' [{per_fail} failed]' if per_fail else ''}")
        except Exception as e:
            logging.error(f"  MM {user_id[:8]} batch_cancel parse error: {e}")
            with race_lock:
                race_metrics['batch_cancel_succeeded'] += 1
                race_metrics['batch_cancel_per_wager_ok'] += len(placed_wagers)
                user_metrics[user_id]['cancelled'] += len(placed_wagers)
            mm_instance.wagers.clear()
    else:
        error_text = ''
        try:
            error_text = json.dumps(cancel_resp.json())
        except Exception:
            error_text = cancel_resp.text[:300]

        category = _classify_cancel_error(cancel_resp.status_code, error_text)
        with race_lock:
            race_metrics['batch_cancel_failed'] += 1
            race_metrics[category] += len(placed_wagers)
            user_metrics[user_id]['cancel_failed'] += len(placed_wagers)
            race_metrics['cancel_error_details'].append({
                'timestamp': datetime.now().isoformat(),
                'cancel_type': 'batch',
                'mm_user': user_id[:8],
                'status_code': cancel_resp.status_code,
                'error': error_text[:200],
                'gap_ms': actual_gap * 1000,
                'num_wagers': len(placed_wagers),
            })

        if verbose:
            logging.warning(f"  MM {user_id[:8]} BATCH_CANCEL FAILED [{cancel_resp.status_code}] "
                            f"({cancel_elapsed*1000:.0f}ms, gap={actual_gap*1000:.1f}ms): {error_text[:120]}")


def _send_cancel_all(mm_instance, user_id, place_fire_time, verbose=False):
    """
    Fire cancel_all_wagers as a periodic sweep.
    This doesn't need wager_ids — it cancels everything open for this user.
    """
    cancel_all_url = urljoin(mm_instance.base_url, config.URL['mm_cancel_all_wagers'])

    cancel_start = time.time()
    try:
        cancel_resp = requests.post(cancel_all_url, json={},
                                    headers=mm_instance._MMInteractions__get_auth_header())
    except Exception as e:
        logging.error(f"  MM {user_id[:8]} cancel_all exception: {e}")
        with race_lock:
            race_metrics['cancel_all_failed'] += 1
            race_metrics['cancel_fail_other'] += 1
        return

    cancel_elapsed = time.time() - cancel_start
    actual_gap = cancel_start - place_fire_time

    with race_lock:
        race_metrics['cancel_all_sent'] += 1
        race_metrics['race_condition_gaps'].append(actual_gap)
        user_metrics[user_id]['response_times_cancel'].append(cancel_elapsed)

    if cancel_resp.status_code == 200:
        with race_lock:
            race_metrics['cancel_all_succeeded'] += 1
            user_metrics[user_id]['cancelled'] += len(mm_instance.wagers)
        mm_instance.wagers.clear()
        if verbose:
            logging.info(f"  MM {user_id[:8]} cancel_all OK ({cancel_elapsed*1000:.0f}ms, gap={actual_gap*1000:.1f}ms)")
    else:
        error_text = ''
        try:
            error_text = json.dumps(cancel_resp.json())
        except Exception:
            error_text = cancel_resp.text[:300]

        category = _classify_cancel_error(cancel_resp.status_code, error_text)
        with race_lock:
            race_metrics['cancel_all_failed'] += 1
            race_metrics[category] += 1
            user_metrics[user_id]['cancel_failed'] += 1
            race_metrics['cancel_error_details'].append({
                'timestamp': datetime.now().isoformat(),
                'cancel_type': 'cancel_all',
                'mm_user': user_id[:8],
                'status_code': cancel_resp.status_code,
                'error': error_text[:200],
                'gap_ms': actual_gap * 1000,
            })

        if verbose:
            logging.warning(f"  MM {user_id[:8]} CANCEL_ALL FAILED [{cancel_resp.status_code}] "
                            f"({cancel_elapsed*1000:.0f}ms, gap={actual_gap*1000:.1f}ms): {error_text[:120]}")


def _run_race_cycle(mm_instance, user_id, markets, batch_size, cancel_gap,
                    random_stake_range, rate_limiter, cycle_num=0,
                    cancel_all_interval=0, verbose=False, overlap_mode=False):
    """
    Single race condition cycle.

    Primary strategy (cancel_multiple_wagers):
      t=0ms       → fire batch place request
      t=Xms       → place response arrives with wager IDs
      t=X+gap ms  → fire cancel_multiple_wagers with SAME wager IDs

    Periodic sweep (cancel_all_wagers, every N cycles):
      t=0ms       → fire batch place in background
      t=gap ms    → fire cancel_all_wagers (no IDs needed, while place is in-flight)

    Overlap mode (--overlap): Designed to trigger database deadlocks.
      t=0ms       → fire batch place + cancel_all SIMULTANEOUSLY
      t=Xms       → place response arrives → immediately fire batch_cancel (double-tap)
      Every cycle uses cancel_all, not just every Nth.
    """
    # Rate limit
    if rate_limiter:
        rate_limiter.acquire(batch_size)

    batch_wagers = _build_batch_wagers(mm_instance, markets, batch_size, random_stake_range)

    if overlap_mode:
        # === OVERLAP MODE: fire place + cancel_all simultaneously, then batch_cancel ===
        place_result = [None, 0]
        place_fire_time = time.time()

        def _do_place():
            placed, elapsed = _send_batch_place(mm_instance, user_id, batch_wagers, verbose)
            place_result[0] = placed
            place_result[1] = elapsed

        def _do_cancel_all():
            _send_cancel_all(mm_instance, user_id, place_fire_time, verbose)

        # Fire BOTH at the exact same instant
        place_thread = threading.Thread(target=_do_place, daemon=True)
        cancel_thread = threading.Thread(target=_do_cancel_all, daemon=True)
        place_thread.start()
        cancel_thread.start()

        # Wait for both to complete
        place_thread.join()
        cancel_thread.join()
        new_placed = place_result[0] or []

        # Double-tap: also batch_cancel the specific wager IDs (server may deadlock on this)
        if new_placed:
            place_end_time = time.time()
            _send_batch_cancel(mm_instance, user_id, new_placed, place_end_time, verbose)

    else:
        use_cancel_all = cancel_all_interval > 0 and cycle_num > 0 and cycle_num % cancel_all_interval == 0

        if use_cancel_all:
            # === cancel_all strategy: fire place in background, cancel_all after gap ===
            place_result = [None, 0]
            place_fire_time = time.time()

            def _do_place():
                placed, elapsed = _send_batch_place(mm_instance, user_id, batch_wagers, verbose)
                place_result[0] = placed
                place_result[1] = elapsed

            place_thread = threading.Thread(target=_do_place, daemon=True)
            place_thread.start()

            time.sleep(cancel_gap)
            _send_cancel_all(mm_instance, user_id, place_fire_time, verbose)

            place_thread.join()
            new_placed = place_result[0] or []
        else:
            # === batch cancel strategy: place → get IDs → immediately cancel same IDs ===
            place_fire_time = time.time()
            new_placed, place_elapsed = _send_batch_place(mm_instance, user_id, batch_wagers, verbose)
            place_end_time = time.time()

            if new_placed:
                # Optional tiny gap before cancel (simulates network/processing delay)
                if cancel_gap > 0:
                    time.sleep(cancel_gap)
                _send_batch_cancel(mm_instance, user_id, new_placed, place_end_time, verbose)

    # Push newly placed wagers to patron queue (some may have been cancelled already)
    if new_placed:
        with patron_queue_lock:
            for w_req, w_resp in zip(batch_wagers, new_placed):
                patron_bet_queue.append({
                    'line_id': w_req['line_id'],
                    'odds': w_req['odds'],
                    'stake': w_req['stake'],
                    'wager_id': w_resp['wager_id'],
                    'external_id': w_resp['external_id'],
                    'mm_user_id': user_id,
                    'timestamp': time.time(),
                })
        if verbose:
            if overlap_mode:
                cancel_type = "overlap"
                elapsed_ms = place_result[1] * 1000
            elif use_cancel_all:
                cancel_type = "cancel_all"
                elapsed_ms = place_result[1] * 1000
            else:
                cancel_type = "batch_cancel"
                elapsed_ms = place_elapsed * 1000
            logging.info(f"  MM {user_id[:8]} [{cancel_type}] placed {len(new_placed)}/{batch_size} ({elapsed_ms:.0f}ms)")

    with race_lock:
        race_metrics['total_cycles'] += 1

    return new_placed


def mm_worker(mm_instance, user_id, markets, cycles, batch_size, cancel_gap,
              random_stake_range, max_workers=1, rate_limiter=None,
              cancel_all_interval=0, verbose=False, overlap_mode=False):
    """
    MM worker: runs race condition cycles with dual cancel strategy.

    Primary: place → get wager IDs → immediately cancel_multiple_wagers (same IDs)
    Every cancel_all_interval cycles: place in background → cancel_all_wagers after gap
    Overlap: place + cancel_all simultaneously every cycle (deadlock hunter)

    When max_workers > 1, multiple independent pipelines run concurrently.
    """
    def _run_pipeline(pipeline_cycles, start_cycle=0):
        for i in range(pipeline_cycles):
            cycle_num = start_cycle + i + 1
            try:
                _run_race_cycle(
                    mm_instance, user_id, markets, batch_size, cancel_gap,
                    random_stake_range, rate_limiter, cycle_num,
                    cancel_all_interval, verbose, overlap_mode
                )
            except Exception as e:
                logging.error(f"  MM {user_id[:8]} cycle {cycle_num} error: {e}")
            if not overlap_mode:
                time.sleep(random.uniform(0.005, 0.02))

    if max_workers <= 1:
        _run_pipeline(cycles)
    else:
        cycles_per_worker = max(1, cycles // max_workers)
        remainder = cycles - (cycles_per_worker * max_workers)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            offset = 0
            for i in range(max_workers):
                c = cycles_per_worker + (1 if i < remainder else 0)
                if c > 0:
                    futures.append(executor.submit(_run_pipeline, c, offset))
                    offset += c
            for f in as_completed(futures):
                try:
                    f.result()
                except Exception as e:
                    logging.error(f"  MM {user_id[:8]} worker exception: {e}")


# ============================================================================
# Patron matching worker (the race condition consumer)
# ============================================================================
def patron_place_bet(patron_info, line_id, odds, stake):
    """Place a single patron bet against an MM wager"""
    bet_url = urljoin(patron_info['base_url'], 'trade/private/api/v2/wagers')

    # Negate odds for the opposite side
    opposite_odds = -odds

    bet_body = {
        'lineID': line_id,
        'odds': opposite_odds,
        'stake': stake,
    }

    headers = {
        'Authorization': f'Bearer {patron_info["jwt"]}',
        'Content-Type': 'application/json',
        'x-currency': 'cash',
        'accept': 'application/json',
        '__source': 'web',
        'origin': patron_info['base_url'].replace('api-', ''),
    }

    try:
        resp = requests.post(bet_url, json=bet_body, headers=headers)
        if resp.status_code == 401:
            # Token expired, refresh and retry once
            if _refresh_patron_token(patron_info):
                headers['Authorization'] = f'Bearer {patron_info["jwt"]}'
                resp = requests.post(bet_url, json=bet_body, headers=headers)
            else:
                return False, resp.status_code, "Token refresh failed"

        if resp.status_code in (200, 201):
            return True, resp.status_code, "OK"
        else:
            error_msg = resp.text[:200]
            return False, resp.status_code, error_msg
    except Exception as e:
        return False, 0, str(e)


def patron_worker(patron_info, stop_event, verbose=False):
    """
    Patron worker thread: continuously drains the bet queue and places counter-bets.
    Races against MM cancel to try to get matched.
    """
    patron_label = f"Patron{patron_info['patron_num']}"

    while not stop_event.is_set():
        bet = None
        with patron_queue_lock:
            if patron_bet_queue:
                bet = patron_bet_queue.pop(0)

        if bet is None:
            time.sleep(0.005)  # Tight poll - patrons need to be fast
            continue

        with race_lock:
            race_metrics['patron_bets_attempted'] += 1

        success, status_code, msg = patron_place_bet(
            patron_info, bet['line_id'], bet['odds'], bet['stake']
        )

        if success:
            with race_lock:
                race_metrics['patron_bets_succeeded'] += 1
                user_metrics[bet['mm_user_id']]['matched_by_patron'] += 1
            if verbose:
                logging.info(f"  {patron_label} MATCHED line={bet['line_id'][:12]}... odds={bet['odds']} stake=${bet['stake']}")
        else:
            with race_lock:
                race_metrics['patron_bets_failed'] += 1
            if verbose:
                logging.info(f"  {patron_label} FAILED [{status_code}]: {msg[:80]}")


# ============================================================================
# Main test orchestrator
# ============================================================================
def collect_markets(mm_instance, event_name):
    """Collect available markets for the target event"""
    matching_events = mm_instance.find_event_by_id_or_name(event_name)
    if not matching_events:
        logging.error(f"No events found matching '{event_name}'")
        logging.info("Available events:")
        for evt_id, evt_data in list(mm_instance.sport_events.items())[:10]:
            logging.info(f"  ID: {evt_id} | Name: {evt_data.get('name', 'Unknown')}")
        return []

    markets = []
    for event in matching_events[:5]:
        event_name_str = event.get('name', 'Unknown')
        for market in event.get('markets', []):
            if market.get('selections'):
                for selection_group in market['selections']:
                    if isinstance(selection_group, list):
                        for selection in selection_group:
                            if selection.get('line_id'):
                                markets.append({
                                    'event_name': event_name_str,
                                    'market_type': market.get('type'),
                                    'line_id': selection['line_id'],
                                    'selection_name': selection.get('name', 'Unknown'),
                                })
    return markets


def print_race_condition_report(elapsed, mm_instances, patron_infos):
    """Print the final race condition analysis report"""
    logging.info("")
    logging.info("=" * 80)
    logging.info("  RACE CONDITION TEST REPORT")
    logging.info("=" * 80)

    # Overview
    logging.info(f"\n  Duration: {elapsed:.1f}s")
    logging.info(f"  Total place-cancel cycles: {race_metrics['total_cycles']}")

    # Bet placement
    logging.info(f"\n  --- BET PLACEMENT ---")
    logging.info(f"  Bets placed successfully:   {race_metrics['total_bets_placed']}")
    logging.info(f"  Bets failed to place:       {race_metrics['total_bets_place_failed']}")

    # Cancel results by type
    logging.info(f"\n  --- BATCH CANCEL (cancel_multiple_wagers — same wager IDs) ---")
    logging.info(f"  Requests sent:              {race_metrics['batch_cancel_sent']}")
    logging.info(f"  Requests succeeded:         {race_metrics['batch_cancel_succeeded']}")
    logging.info(f"  Requests failed:            {race_metrics['batch_cancel_failed']}")
    logging.info(f"  Per-wager OK:               {race_metrics['batch_cancel_per_wager_ok']}")
    logging.info(f"  Per-wager FAIL:             {race_metrics['batch_cancel_per_wager_fail']}")

    logging.info(f"\n  --- CANCEL ALL (cancel_all_wagers — sweep) ---")
    logging.info(f"  Requests sent:              {race_metrics['cancel_all_sent']}")
    logging.info(f"  Requests succeeded:         {race_metrics['cancel_all_succeeded']}")
    logging.info(f"  Requests failed:            {race_metrics['cancel_all_failed']}")

    # Combined failure breakdown
    total_wager_cancel_fails = (race_metrics['batch_cancel_per_wager_fail']
                                + race_metrics['cancel_fail_still_processing']
                                + race_metrics['cancel_fail_already_matched']
                                + race_metrics['cancel_fail_already_cancelled']
                                + race_metrics['cancel_fail_not_found']
                                + race_metrics['cancel_fail_placing']
                                + race_metrics['cancel_fail_other'])
    logging.info(f"\n  --- CANCEL FAILURE BREAKDOWN (all types combined) ---")
    logging.info(f"  Still processing (race):    {race_metrics['cancel_fail_still_processing']}  <-- RACE CONDITION")
    logging.info(f"  Placing (409, bet delay):   {race_metrics['cancel_fail_placing']}  <-- RACE CONDITION")
    logging.info(f"  Already matched:            {race_metrics['cancel_fail_already_matched']}")
    logging.info(f"  Already cancelled:          {race_metrics['cancel_fail_already_cancelled']}")
    logging.info(f"  Not found (404):            {race_metrics['cancel_fail_not_found']}")
    logging.info(f"  Other errors:               {race_metrics['cancel_fail_other']}")

    # Race condition rate
    total_cancel_wagers = race_metrics['batch_cancel_per_wager_ok'] + race_metrics['batch_cancel_per_wager_fail']
    race_hits = race_metrics['cancel_fail_still_processing'] + race_metrics['cancel_fail_placing']
    if total_cancel_wagers > 0:
        race_rate = race_hits / total_cancel_wagers * 100
        fail_rate = race_metrics['batch_cancel_per_wager_fail'] / total_cancel_wagers * 100
        logging.info(f"\n  RACE CONDITION HIT RATE:    {race_rate:.1f}% ({race_hits}/{total_cancel_wagers})")
        logging.info(f"  BATCH CANCEL FAIL RATE:     {fail_rate:.1f}%")

    # Gap analysis
    gaps = race_metrics['race_condition_gaps']
    if gaps:
        avg_gap = sum(gaps) / len(gaps)
        min_gap = min(gaps)
        max_gap = max(gaps)
        logging.info(f"\n  --- TIMING ANALYSIS ---")
        logging.info(f"  Place-to-cancel gap (avg):  {avg_gap*1000:.2f}ms")
        logging.info(f"  Place-to-cancel gap (min):  {min_gap*1000:.2f}ms")
        logging.info(f"  Place-to-cancel gap (max):  {max_gap*1000:.2f}ms")

    # Patron results
    logging.info(f"\n  --- PATRON MATCHING ---")
    logging.info(f"  Patron bets attempted:      {race_metrics['patron_bets_attempted']}")
    logging.info(f"  Patron bets succeeded:      {race_metrics['patron_bets_succeeded']}")
    logging.info(f"  Patron bets failed:         {race_metrics['patron_bets_failed']}")

    # Per-user breakdown
    logging.info(f"\n  --- PER-USER BREAKDOWN ---")
    all_user_ids = list(user_metrics.keys())
    for uid in all_user_ids:
        m = user_metrics[uid]
        avg_place = (sum(m['response_times_place']) / len(m['response_times_place']) * 1000
                     if m['response_times_place'] else 0)
        avg_cancel = (sum(m['response_times_cancel']) / len(m['response_times_cancel']) * 1000
                      if m['response_times_cancel'] else 0)
        logging.info(f"\n  MM {uid[:8]}...:")
        logging.info(f"    Placed: {m['placed']}  |  Cancelled: {m['cancelled']}  |  Cancel failed: {m['cancel_failed']}")
        logging.info(f"    Matched by patron: {m['matched_by_patron']}")
        logging.info(f"    Avg place time: {avg_place:.0f}ms  |  Avg cancel time: {avg_cancel:.0f}ms")

    # Balance check
    logging.info(f"\n  --- BALANCE VERIFICATION ---")
    for uid, mm_inst in mm_instances.items():
        try:
            mm_inst.get_balance()
            logging.info(f"  MM {uid[:8]}...: ${mm_inst.balance:,.2f}")
        except Exception as e:
            logging.info(f"  MM {uid[:8]}...: Error getting balance - {e}")

    for pinfo in patron_infos:
        try:
            auth_headers = {
                'Authorization': f'Bearer {pinfo["jwt"]}',
                'Content-Type': 'application/json',
                'x-currency': 'cash',
                'accept': 'application/json'
            }
            resp = requests.get(urljoin(pinfo['base_url'], 'api/v1/wallet'), headers=auth_headers)
            if resp.status_code == 200:
                bal = resp.json().get('data', {}).get('balance', 0)
                logging.info(f"  Patron{pinfo['patron_num']}: ${bal:,.2f}")
        except Exception as e:
            logging.info(f"  Patron{pinfo['patron_num']}: Error - {e}")

    # Error details (first 10)
    if race_metrics['cancel_error_details']:
        logging.info(f"\n  --- CANCEL ERROR SAMPLES (first 10) ---")
        for err in race_metrics['cancel_error_details'][:10]:
            logging.info(f"  [{err['timestamp']}] MM {err['mm_user']} | "
                         f"HTTP {err['status_code']} | gap={err['gap_ms']:.1f}ms | "
                         f"{err['error'][:100]}")

    logging.info("\n" + "=" * 80)


def run_race_condition_test(event_name, cycles=50, batch_size=10, cancel_gap=0.02,
                             environment='sandbox', verbose=False,
                             stake_min=1.0, stake_max=5.0,
                             deduce_mode=False, max_workers=1, max_rps=2500,
                             cancel_all_interval=0, mm_accounts=None, overlap_mode=False):
    """
    Main test runner.

    Args:
        event_name:          Target event name/ID
        cycles:              Number of place-cancel cycles per MM account
        batch_size:          Wagers per batch (max 20 for API)
        cancel_gap:          Seconds between place response and cancel request (0.02 = 20ms)
        environment:         sandbox or staging
        verbose:             Detailed per-wager logging
        stake_min:           Minimum random stake
        stake_max:           Maximum random stake
        deduce_mode:         True = MM1+MM2 vs Patron4+Patron8, False = MM2 vs Patron8 only
        max_workers:         Concurrent workers per MM account (controls RPS)
        max_rps:             Max wagers per second across all workers (0 = unlimited)
        cancel_all_interval: Use cancel_all_wagers every N cycles (0 = never, batch cancel only)
    """
    test_start = time.time()

    # Determine which MM accounts to load
    if mm_accounts:
        mm_account_list = mm_accounts
        mode_label = f"CUSTOM MM ({'+'.join(str(a) for a in mm_account_list)})"
    elif deduce_mode:
        mm_account_list = [1, 2]
        mode_label = "DEDUCE (MM1+MM2 vs Patron4+Patron8)"
    else:
        mm_account_list = [2]
        mode_label = "NON-DEDUCE (MM2 vs Patron8)"

    logging.info("=" * 80)
    logging.info("  RACE CONDITION TEST: Bet Batch vs Cancel Batch")
    logging.info("=" * 80)
    logging.info(f"  Environment:      {environment}")
    logging.info(f"  Mode:             {mode_label}")
    logging.info(f"  Target event:     {event_name}")
    logging.info(f"  Cycles per MM:    {cycles}")
    logging.info(f"  Batch size:       {batch_size}")
    logging.info(f"  Cancel gap:       {cancel_gap*1000:.1f}ms")
    logging.info(f"  Workers per MM:   {max_workers}")
    logging.info(f"  Max RPS:          {max_rps} wagers/sec")
    logging.info(f"  Stake range:      ${stake_min:.2f} - ${stake_max:.2f}")
    if overlap_mode:
        cancel_strat_label = "OVERLAP (place + cancel_all simultaneous + batch_cancel double-tap)"
    else:
        cancel_all_label = f"every {cancel_all_interval} cycles" if cancel_all_interval > 0 else "disabled (batch cancel only)"
        cancel_strat_label = f"batch_cancel (primary) + cancel_all ({cancel_all_label})"
    logging.info(f"  Cancel strategy:  {cancel_strat_label}")
    logging.info(f"  Overlap mode:     {overlap_mode}")
    logging.info(f"  Verbose:          {verbose}")
    logging.info("=" * 80)

    # --- Load MM accounts ---
    logging.info("\n  Loading MM accounts...")
    os.environ['MM_ENVIRONMENT'] = environment
    import importlib
    importlib.reload(config)

    mm_instances = {}

    for acct in mm_account_list:
        try:
            mm_inst, uid = load_mm_account(acct, environment)
            mm_instances[uid] = mm_inst
            logging.info(f"  MM {acct}:  {uid} (Balance: ${mm_inst.balance:,.2f})")
        except Exception as e:
            logging.error(f"  Failed to load MM {acct}: {e}")
            return

    if not mm_instances:
        logging.error("  No MM accounts loaded, cannot run test")
        return

    # --- Load Patron accounts ---
    logging.info("\n  Loading Patron accounts...")
    patron_infos = []

    if deduce_mode:
        # Deduce mode: include Patron4 (deduce, usr004)
        try:
            p4 = load_patron_account(4, environment)
            patron_infos.append(p4)
            logging.info(f"  Patron4 (deduce):     loaded")
        except Exception as e:
            logging.warning(f"  Patron4 skipped: {e}")

    # Patron8 always loaded (non-deduce, usr008)
    try:
        p8 = load_patron_account(8, environment)
        patron_infos.append(p8)
        logging.info(f"  Patron8 (non-deduce): loaded")
    except Exception as e:
        logging.warning(f"  Patron8 skipped: {e}")

    if not patron_infos:
        logging.error(f"  No patron accounts loaded, cannot run test")
        return

    # --- Collect markets ---
    logging.info(f"\n  Collecting markets for '{event_name}'...")
    markets = collect_markets(list(mm_instances.values())[0], event_name)
    if not markets:
        return
    logging.info(f"  Found {len(markets)} markets across matching events")

    # --- Start patron workers ---
    logging.info(f"\n  Starting patron workers...")
    stop_event = threading.Event()
    patron_threads = []
    for pinfo in patron_infos:
        t = threading.Thread(
            target=patron_worker,
            args=(pinfo, stop_event, verbose),
            daemon=True,
            name=f"Patron{pinfo['patron_num']}Worker"
        )
        t.start()
        patron_threads.append(t)
    logging.info(f"  {len(patron_threads)} patron workers running")

    # --- Start MM workers ---
    if overlap_mode:
        logging.info(f"\n  Starting MM OVERLAP cycles (place+cancel_all simultaneous, workers={max_workers})...")
        logging.info(f"  Strategy: place + cancel_all fire at t=0 → batch_cancel double-tap on response")
        logging.info(f"  No inter-cycle sleep — maximum server pressure")
    else:
        logging.info(f"\n  Starting MM place-cancel cycles (gap={cancel_gap*1000:.1f}ms, workers={max_workers})...")
        logging.info(f"  Primary: place → get IDs → {cancel_gap*1000:.0f}ms → cancel_multiple_wagers (same IDs)")
        if cancel_all_interval > 0:
            logging.info(f"  Sweep:   every {cancel_all_interval} cycles → cancel_all_wagers (in-flight overlap)")
    logging.info(f"  Each MM will run {cycles} cycles of {batch_size} wagers\n")

    random_stake_range = (stake_min, stake_max)
    rate_limiter = RateLimiter(max_rps) if max_rps > 0 else None
    mm_threads = []
    for uid, mm_inst in mm_instances.items():
        t = threading.Thread(
            target=mm_worker,
            args=(mm_inst, uid, markets, cycles, batch_size, cancel_gap,
                  random_stake_range, max_workers, rate_limiter,
                  cancel_all_interval, verbose, overlap_mode),
            daemon=True,
            name=f"MM-{uid[:8]}"
        )
        t.start()
        mm_threads.append(t)

    # --- Wait for MM workers to finish with progress ---
    last_progress = time.time()
    while any(t.is_alive() for t in mm_threads):
        time.sleep(1)
        now = time.time()
        if now - last_progress >= 5:
            with race_lock:
                c = race_metrics['total_cycles']
                total_expected = cycles * len(mm_instances)
                pct = c / total_expected * 100 if total_expected > 0 else 0
                placed = race_metrics['total_bets_placed']
                batch_ok = race_metrics['batch_cancel_per_wager_ok']
                batch_fail = race_metrics['batch_cancel_per_wager_fail']
                all_ok = race_metrics['cancel_all_succeeded']
                all_fail = race_metrics['cancel_all_failed']
                race_hits = race_metrics['cancel_fail_still_processing'] + race_metrics['cancel_fail_placing']
                patron_ok = race_metrics['patron_bets_succeeded']
            logging.info(f"  Progress: {c}/{total_expected} cycles ({pct:.0f}%) | "
                         f"Placed: {placed} | BatchCancel OK/FAIL: {batch_ok}/{batch_fail} | "
                         f"CancelAll OK/FAIL: {all_ok}/{all_fail} | "
                         f"Race hits: {race_hits} | Patron matched: {patron_ok}")
            last_progress = now

    # Give patrons a moment to drain remaining queue
    time.sleep(0.5)
    stop_event.set()
    for t in patron_threads:
        t.join(timeout=2)

    elapsed = time.time() - test_start

    # --- Print report ---
    print_race_condition_report(elapsed, mm_instances, patron_infos)

    # --- Save results to JSON ---
    results = {
        'test_config': {
            'environment': environment,
            'event': event_name,
            'cycles_per_mm': cycles,
            'batch_size': batch_size,
            'cancel_gap_ms': cancel_gap * 1000,
            'cancel_all_interval': cancel_all_interval,
            'stake_range': [stake_min, stake_max],
            'num_mm_accounts': len(mm_instances),
            'num_patron_accounts': len(patron_infos),
            'duration_seconds': elapsed,
        },
        'race_metrics': {k: v for k, v in race_metrics.items()
                         if k not in ('race_condition_gaps', 'cancel_error_details')},
        'timing': {
            'avg_gap_ms': (sum(race_metrics['race_condition_gaps']) / len(race_metrics['race_condition_gaps']) * 1000
                           if race_metrics['race_condition_gaps'] else 0),
            'min_gap_ms': (min(race_metrics['race_condition_gaps']) * 1000
                           if race_metrics['race_condition_gaps'] else 0),
            'max_gap_ms': (max(race_metrics['race_condition_gaps']) * 1000
                           if race_metrics['race_condition_gaps'] else 0),
        },
        'cancel_error_samples': race_metrics['cancel_error_details'][:20],
        'per_user_metrics': {uid: dict(m) for uid, m in user_metrics.items()},
    }

    output_file = f"race_condition_test_{int(time.time())}.json"
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    logging.info(f"\n  Results saved to: {output_file}")


# ============================================================================
# CLI
# ============================================================================
if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Race Condition Test: Bet Batch vs Cancel Batch',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Non-deduce only (MM2 vs Patron8), batch cancel + cancel_all every 10 cycles
  python test_backend_fairness.py --event "20023350" --cycles 50

  # Batch cancel only (no cancel_all sweep)
  python test_backend_fairness.py --event "20023350" --cycles 100 --cancel-all-interval 0

  # Deduce mode (MM1+MM2 vs Patron4+Patron8)
  python test_backend_fairness.py --event "20023350" --cycles 100 --deducemode

  # High RPS with 50 concurrent workers per MM
  python test_backend_fairness.py --event "20023350" --cycles 1000 --workers 50

  # Zero gap — cancel immediately after getting place response
  python test_backend_fairness.py --event "20023350" --cycles 100 --gap 0 --batch-size 15

  # Full combo
  python test_backend_fairness.py --event "20023350" --cycles 500 --deducemode --workers 20 --verbose
        """
    )
    parser.add_argument('--event', type=str, required=True,
                        help='Target event name or ID')
    parser.add_argument('--cycles', type=int, default=50,
                        help='Place-cancel cycles per MM account (default: 50)')
    parser.add_argument('--batch-size', type=int, default=10,
                        help='Wagers per batch, max 20 (default: 10)')
    parser.add_argument('--gap', type=float, default=0.02,
                        help='Seconds between place and cancel (default: 0.02 = 20ms)')
    parser.add_argument('--env', type=str, default='sandbox',
                        choices=['sandbox', 'staging'],
                        help='Environment (default: sandbox)')
    parser.add_argument('--verbose', action='store_true',
                        help='Enable detailed per-wager logging')
    parser.add_argument('--stake-min', type=float, default=1.0,
                        help='Minimum random stake (default: 1.0)')
    parser.add_argument('--stake-max', type=float, default=5.0,
                        help='Maximum random stake (default: 5.0)')
    parser.add_argument('--deducemode', action='store_true',
                        help='Enable deduce mode: MM1(deduce)+MM2(non-deduce) vs Patron4(deduce)+Patron8(non-deduce). '
                             'Default: MM2(non-deduce) vs Patron8(non-deduce) only')
    parser.add_argument('--workers', type=int, default=1,
                        help='Concurrent workers per MM account for higher RPS (default: 1 = sequential)')
    parser.add_argument('--rps', type=int, default=2500,
                        help='Max wagers per second across all workers (default: 2500, 0 = unlimited)')
    parser.add_argument('--cancel-all-interval', type=int, default=10,
                        help='Use cancel_all_wagers every N cycles as sweep (default: 10, 0 = batch cancel only)')
    parser.add_argument('--mm', type=str, nargs='+', default=None,
                        help='MM accounts to use, e.g. --mm 1 2 exposure_mm1 exposure_mm2')
    parser.add_argument('--overlap', action='store_true',
                        help='Overlap mode: fire place + cancel_all simultaneously every cycle '
                             '(designed to trigger database deadlocks). No inter-cycle sleep.')

    args = parser.parse_args()

    # Validate batch size
    if args.batch_size > 20:
        logging.warning("Batch size capped at 20 (API limit)")
        args.batch_size = 20

    # Parse --mm accounts: convert pure digits to int, keep strings as-is
    mm_accounts = None
    if args.mm:
        mm_accounts = [int(x) if x.isdigit() else x for x in args.mm]

    run_race_condition_test(
        event_name=args.event,
        cycles=args.cycles,
        batch_size=args.batch_size,
        cancel_gap=args.gap,
        environment=args.env,
        verbose=args.verbose,
        stake_min=args.stake_min,
        stake_max=args.stake_max,
        deduce_mode=args.deducemode,
        max_workers=args.workers,
        max_rps=args.rps,
        cancel_all_interval=args.cancel_all_interval,
        mm_accounts=mm_accounts,
        overlap_mode=args.overlap,
    )
