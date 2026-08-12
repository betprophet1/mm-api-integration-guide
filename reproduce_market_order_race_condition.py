#!/usr/bin/env python3
"""
Market Order Race Condition Reproducer

This script reproduces a race condition bug where:
- Market orders have a 5-second timeout
- If a market order gets matched right at the timeout moment, the cancel operation runs
- The refund amount doesn't subtract the matched amount, returning the full stake to user
- This causes double payment: matched wager + full refund

Example from production wager 437927388:
- open: $501.35
- matched: $83.33 
- cancel: $501.35 (should be $418.02 = 501.35 - 83.33)

Strategy:
1. Fill liquidity on event 20022482 using multiple MM accounts (excluding deduce user)
2. Place Market Orders using Patron account with maxStakeSize to maximize matching
3. Run continuously to hit the 5-second timeout window where race condition occurs
"""

import argparse
import sys
import time
import threading
import requests
import json
import uuid
import base64
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin
from collections import defaultdict

from src import mm_calls
from src.log import logging
from src import config

# Global statistics
stats = {
    'liquidity_wagers_placed': 0,
    'market_orders_placed': 0,
    'market_orders_matched': 0,
    'market_orders_timeout': 0,
    'market_orders_failed': 0,
    'potential_race_conditions': 0,
    'cancel_before_timeout_success': 0,
    'cancel_before_timeout_failed': 0,
    'cancel_after_timeout_already_cancelled': 0,
    'cancel_after_timeout_unexpected': 0,
}
stats_lock = threading.Lock()

DEDUCE_USER_UUID = '279cc6a1-d926-4273-a18a-782eccfbce7b'  # Exclude from liquidity
TARGET_EVENT_ID = 20023066  # Default event ID, can be overridden by --event-id


def load_patron_account(environment='qa'):
    """Load patron account for Market Order placement using web authentication"""
    import os
    
    # Load patron credentials (email/password)
    try:
        patron_config_file = f'user_info_patron_{environment}.json'
        patron_config_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            'src',
            patron_config_file
        )
        
        with open(patron_config_path) as f:
            patron_creds = json.load(f)
            email = patron_creds.get('email')
            password = patron_creds.get('password')
        
        logging.info(f"✅ Loaded patron credentials from {patron_config_file}")
    except Exception as e:
        logging.error(f"❌ Failed to load patron credentials: {e}")
        return None
    
    # Login using web authentication
    base_url = config.ENVIRONMENT_URLS.get(environment, config.ENVIRONMENT_URLS['qa'])
    login_url = urljoin(base_url, 'api/v1/auth/login')
    device_id = str(uuid.uuid1())
    
    headers = {
        '__source': 'web',
        'accept': 'application/json, text/plain, */*',
        'content-type': 'application/json',
        'origin': base_url.replace('api-', ''),
        'x-currency': 'cash'
    }
    
    # Try without code first (may not need 2FA in sandbox)
    request_body = {
        'email': email,
        'password': password,
        'device_id': device_id
    }
    
    try:
        response = requests.post(login_url, headers=headers, json=request_body)
        
        if response.status_code != 200:
            logging.error(f"❌ Patron login failed: {response.status_code} - {response.text}")
            return None
        
        response_data = response.json()
        patron_jwt = response_data.get('accessToken')
        
        if not patron_jwt:
            logging.error("❌ No access token received from login")
            return None
        
        logging.info("✅ Patron account login successful")
        
        # Create a simple patron object to hold session info
        class PatronAccount:
            def __init__(self, jwt_token, base_url):
                self.jwt_token = jwt_token
                self.base_url = base_url
                self.balance = 0
                self.user_id = 'patron'
                self.sport_events = {}
            
            def get_auth_header(self):
                return {
                    'Authorization': f'Bearer {self.jwt_token}',
                    'Content-Type': 'application/json',
                    'x-currency': 'cash',
                    'accept': 'application/json',
                    '__source': 'web'
                }
            
            def get_balance(self):
                try:
                    balance_url = urljoin(self.base_url, 'api/v1/wallet')
                    response = requests.get(balance_url, headers=self.get_auth_header())
                    if response.status_code == 200:
                        wallet_data = response.json().get('data', {})
                        self.balance = wallet_data.get('balance', 0)
                        logging.info(f"💰 Patron balance: ${self.balance:.2f}")
                        return self.balance
                except Exception as e:
                    logging.error(f"❌ Error getting patron balance: {e}")
                return 0
        
        patron = PatronAccount(patron_jwt, base_url)
        patron.get_balance()
        
        logging.info(f"✅ Loaded Patron account (Balance: ${patron.balance:.2f})")
        return patron
        
    except Exception as e:
        logging.error(f"❌ Error during patron login: {e}")
        return None


def load_liquidity_providers(environment='qa', num_accounts=5):
    """Load MM accounts for liquidity provision, excluding deduce user"""
    import os
    import importlib
    
    os.environ['MM_ENVIRONMENT'] = environment
    importlib.reload(config)
    
    mm_instances = {}
    
    # Start from account 2 since account 1 is used as patron
    for account_num in range(2, num_accounts + 2):
        try:
            credentials = config.get_account_credentials(account_num, environment)
            
            mm_instance = mm_calls.MMInteractions()
            mm_instance.mm_keys = {
                'access_key': credentials['access_key'],
                'secret_key': credentials['secret_key']
            }
            
            mm_instance.mm_login()
            mm_instance.get_balance()
            mm_instance.seeding()
            
            # Extract UUID
            access_token = mm_instance.mm_session.get('access_token', '')
            try:
                parts = access_token.split('.')
                if len(parts) >= 2:
                    payload = parts[1]
                    padding = 4 - (len(payload) % 4)
                    if padding != 4:
                        payload += '=' * padding
                    
                    decoded_bytes = base64.urlsafe_b64decode(payload)
                    decoded_json = json.loads(decoded_bytes)
                    user_id = decoded_json.get('partnerID', 'unknown')
                else:
                    raise Exception("Invalid token format")
            except Exception as e:
                access_key = credentials['access_key']
                user_id = access_key.split('_')[0] if '_' in access_key else access_key[:8]
                logging.warning(f"⚠️  Could not extract UUID ({str(e)}), using: {user_id}")
            
            # Skip deduce user
            if user_id == DEDUCE_USER_UUID:
                logging.warning(f"⚠️  Skipping deduce user: {user_id}")
                continue
            
            mm_instance.user_id = user_id
            mm_instances[user_id] = mm_instance
            logging.info(f"✅ Loaded liquidity provider {account_num}: {user_id} (Balance: ${mm_instance.balance:.2f})")
            
        except Exception as e:
            logging.warning(f"⚠️  Could not load account {account_num}: {str(e)}")
            continue
    
    return mm_instances


def provide_liquidity_worker(user_id, mm_inst, line_ids, duration_seconds):
    """
    Worker function for a single MM account to provide liquidity
    Runs concurrently with other workers
    """
    start_time = time.time()
    wager_count = 0
    worker_name = f"MM-{user_id[:8]}"
    
    logging.info(f"🔄 {worker_name}: Starting liquidity worker")
    
    while time.time() - start_time < duration_seconds:
        # Randomly select a line_id
        line_data = random.choice(line_ids)
        
        # Place a wager
        external_id = str(uuid.uuid1())
        odds = mm_inst._MMInteractions__get_random_odds()
        
        body = {
            'external_id': external_id,
            'line_id': line_data['line_id'],
            'odds': odds,
            'stake': 1.0
        }
        
        try:
            play_url = urljoin(mm_inst.base_url, config.URL['mm_place_wager'])
            response = requests.post(play_url, json=body, headers=mm_inst._MMInteractions__get_auth_header())
            
            if response.status_code == 200:
                with stats_lock:
                    stats['liquidity_wagers_placed'] += 1
                wager_count += 1
                
                if wager_count % 100 == 0:
                    logging.info(f"📊 {worker_name}: {wager_count} wagers placed")
        except Exception as e:
            logging.error(f"❌ {worker_name}: Error - {e}")
        
        # Delay to avoid filling liquidity too quickly (0.1s per wager)
        time.sleep(0.1)
    
    logging.info(f"✅ {worker_name}: Completed {wager_count} wagers")


def provide_liquidity_continuously(mm_instances, target_event_id, duration_seconds=300, workers_per_account=1):
    """
    Continuously provide liquidity on target event using MM accounts
    This runs in the background with concurrent workers (multiple per account)
    """
    total_workers = len(mm_instances) * workers_per_account
    logging.info(f"\n🔄 Starting liquidity provision for event {target_event_id}")
    logging.info(f"   Duration: {duration_seconds}s")
    logging.info(f"   Accounts: {len(mm_instances)} MM accounts")
    logging.info(f"   Workers: {workers_per_account} per account = {total_workers} total workers\n")
    
    # Find target event in one of the MM instances
    mm_instance = list(mm_instances.values())[0]
    target_event = None
    for event_id, event_data in mm_instance.sport_events.items():
        if event_id == target_event_id:
            target_event = event_data
            break
    
    if not target_event:
        logging.error(f"❌ Event {target_event_id} not found!")
        return
    
    logging.info(f"✅ Found event: {target_event.get('name', 'Unknown')}")
    
    # Collect line_ids by fetching markets FRESH via get_multiple_markets. The
    # cached mm_instance.sport_events structure omits line_ids for some events
    # (e.g. those exposing lines under `market_lines` rather than `selections`),
    # which made this no-op (0 line_ids → 0 liquidity → 0 market orders). This
    # mirrors exposure-stress's working extraction and handles both shapes.
    line_ids = []
    def _add_sel(sel, mtype):
        if isinstance(sel, dict) and sel.get('line_id'):
            line_ids.append({
                'line_id': sel['line_id'],
                'market_type': mtype,
                'selection_name': sel.get('name', 'Unknown'),
            })
    try:
        _tok = list(mm_instances.values())[0].mm_session.get('access_token', '')
        _resp = requests.get(
            f"{config.BASE_URL}/partner/mm/get_multiple_markets",
            params={"event_ids": str(target_event_id)},
            headers={"Authorization": f"Bearer {_tok}"}, timeout=15)
        _data = _resp.json().get('data', {}) if _resp.status_code == 200 else {}
    except Exception as _e:
        logging.error(f"get_multiple_markets fetch failed: {_e}")
        _data = {}
    for _eid, _markets in _data.items():
        for market in _markets:
            mtype = market.get('type', 'unknown')
            for group in (market.get('selections') or []):
                if isinstance(group, list):
                    for sel in group:
                        _add_sel(sel, mtype)
                else:
                    _add_sel(group, mtype)
            for ml in (market.get('market_lines') or []):
                for group in (ml.get('selections') or []):
                    if isinstance(group, list):
                        for sel in group:
                            _add_sel(sel, mtype)
                    else:
                        _add_sel(group, mtype)
    
    logging.info(f"✅ Collected {len(line_ids)} line_ids from event")
    logging.info(f"   Markets: {len(set(l['market_type'] for l in line_ids))} unique market types\n")
    
    if not line_ids:
        logging.error("❌ No line_ids found in event!")
        return
    
    # Start concurrent workers - multiple per MM account
    with ThreadPoolExecutor(max_workers=total_workers) as executor:
        futures = []
        worker_counter = 0
        for user_id, mm_inst in mm_instances.items():
            # Spawn multiple workers for this account
            for worker_num in range(workers_per_account):
                worker_counter += 1
                # Add worker number to differentiate multiple workers from same account
                worker_id = f"{user_id}-W{worker_num+1}" if workers_per_account > 1 else user_id
                future = executor.submit(
                    provide_liquidity_worker,
                    worker_id,
                    mm_inst,
                    line_ids,
                    duration_seconds
                )
                futures.append(future)
        
        # Wait for all workers to complete
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logging.error(f"❌ Liquidity worker error: {e}")
    
    logging.info(f"\n✅ All liquidity workers completed")


def get_estimate_odds(patron, line_id, stake):
    """
    Call GET Estimate Odds endpoint
    Returns: (maxStakeSize, expectedAverageOdds, oddsList)
    """
    estimate_url = urljoin(patron.base_url, 'trade/private/api/v1/market-orders/estimate-odds')
    
    headers = patron.get_auth_header()
    
    body = {
        'lineId': line_id,
        'stake': stake
    }
    
    try:
        response = requests.post(estimate_url, json=body, headers=headers)
        if response.status_code == 200:
            data = response.json().get('data', {})
            return (
                data.get('maxStakeSize', 0),
                data.get('expectedAverageOdds', 0),
                data.get('oddsList', [])
            )
        else:
            logging.error(f"❌ Estimate odds failed: {response.status_code} - {response.text}")
            return None, None, None
    except Exception as e:
        logging.error(f"❌ Estimate odds error: {e}")
        return None, None, None


def cancel_market_order(patron, wager_id):
    """
    Cancel a Market Order
    Returns: (success, response_text)
    """
    cancel_url = urljoin(patron.base_url, f'trade/private/api/v1/market-orders/{wager_id}')
    headers = patron.get_auth_header()
    
    try:
        response = requests.delete(cancel_url, headers=headers)
        if response.status_code == 200:
            return True, response.text
        else:
            return False, response.text
    except Exception as e:
        return False, str(e)


def place_market_order(patron, line_id, stake, expected_avg_odds, odds_list, expected_timeout=5.0):
    """
    Place a Market Order
    Returns: (success, wager_id, status)
    :param expected_timeout: Expected timeout in seconds (5s for normal, 10s for live)
    """
    mo_url = urljoin(patron.base_url, 'trade/private/api/v1/market-orders')
    
    headers = patron.get_auth_header()
    
    body = {
        'lineID': line_id,
        'expectedAverageOdds': expected_avg_odds,
        'oddsList': odds_list,
        'stake': stake
    }
    
    try:
        start_time = time.time()
        response = requests.post(mo_url, json=body, headers=headers)
        elapsed = time.time() - start_time
        
        if response.status_code == 200:
            data = response.json().get('data', {})
            # The response carries refId (uuid) and id (int) — there is NO
            # 'wagerId' field. Reading a key that does not exist made wager_id
            # always None, so the caller's `if success and wager_id:` never fired:
            # market_orders_placed stayed 0 and every run was reported as a
            # "0 market orders — no-op" INCONCLUSIVE, for weeks, while orders were
            # in fact being placed successfully (verified 2026-08-12: the patron
            # balance moved and a POTENTIAL RACE CONDITION warning fired during a
            # run that still reported 0). It also meant cancel_market_order() was
            # called with None, so the cancel-race half never ran either.
            #   {"success":true,"data":{"id":38892,"refId":"5c41dbf7-…",
            #    "lineID":"…","status":"pending",…}}
            wager_id = data.get('refId') or data.get('id') or data.get('wagerId')
            status = data.get('status', 'unknown')
            
            logging.info(f"✅ Market Order placed: ID={wager_id}, Status={status}, Elapsed={elapsed:.2f}s")
            
            # Check if it took close to expected timeout (potential timeout scenario)
            timeout_threshold = expected_timeout - 0.5  # Check if within 0.5s of timeout
            if elapsed >= timeout_threshold:
                logging.warning(f"⚠️  POTENTIAL RACE CONDITION: Order took {elapsed:.2f}s (near {expected_timeout}s timeout)")
                with stats_lock:
                    stats['potential_race_conditions'] += 1
            
            return True, wager_id, status
        else:
            logging.error(f"❌ Market Order failed: {response.status_code} - {response.text}")
            return False, None, None
    except Exception as e:
        logging.error(f"❌ Market Order error: {e}")
        return False, None, None


def market_order_worker(patron, line_ids, duration_seconds, expected_timeout, worker_name="Patron", verify_timeout=False):
    """
    Worker function for placing market orders
    Runs concurrently to maximize stress on the system
    :param expected_timeout: Expected timeout in seconds (5s for normal, 10s for live)
    :param verify_timeout: If True, periodically test cancel before/after timeout
    """
    start_time = time.time()
    order_count = 0
    verification_count = 0
    
    logging.info(f"🚀 {worker_name}: Starting market order worker (Timeout verification: {verify_timeout})")
    
    while time.time() - start_time < duration_seconds:
        # Randomly select a line_id
        line_data = random.choice(line_ids)
        line_id = line_data['line_id']
        
        # Get current balance
        try:
            patron.get_balance()
            available_balance = patron.balance
        except:
            available_balance = 10.0  # Fallback to small amount
        
        # Cap initial estimate to reasonable amount (max 10% of balance or $100, whichever is lower)
        max_initial_stake = min(100.0, available_balance * 0.1)
        
        # Step 1: Get estimate with capped stake
        max_stake, avg_odds, odds_list = get_estimate_odds(patron, line_id, max_initial_stake)
        
        if max_stake is None:
            time.sleep(0.5)
            continue
        
        # Step 2: Cap max_stake to affordable amount (leave 20% buffer for safety)
        affordable_stake = available_balance * 0.8
        max_stake = min(max_stake, affordable_stake)
        
        # If still reasonable, re-estimate with the capped max stake
        if max_stake > max_initial_stake and max_stake <= affordable_stake:
            max_stake2, avg_odds, odds_list = get_estimate_odds(patron, line_id, max_stake)
            if max_stake2 is not None:
                # Cap again in case estimate returned higher
                max_stake = min(max_stake2, affordable_stake)
        
        # Ensure we never exceed balance
        if max_stake > affordable_stake:
            max_stake = affordable_stake
        
        # Step 3: Place Market Order with safe stake amount
        if max_stake > 0 and avg_odds and odds_list and max_stake <= available_balance:
            success, wager_id, status = place_market_order(patron, line_id, max_stake, avg_odds, odds_list, expected_timeout)
            
            if success and wager_id:
                with stats_lock:
                    stats['market_orders_placed'] += 1
                    if status == 'matched':
                        stats['market_orders_matched'] += 1
                    elif status == 'timeout' or status == 'cancelled':
                        stats['market_orders_timeout'] += 1
                
                order_count += 1
                
                # Perform timeout verification test every 5 orders
                if verify_timeout and order_count % 5 == 0 and status == 'pending':
                    verification_count += 1
                    logging.info(f"\n🔍 {worker_name}: TIMEOUT VERIFICATION TEST #{verification_count}")
                    logging.info(f"   Wager ID: {wager_id}")
                    logging.info(f"   Expected timeout: {expected_timeout}s")
                    
                    # Test 1: Cancel BEFORE timeout (at 50% of timeout)
                    cancel_before_delay = expected_timeout * 0.5
                    logging.info(f"   Test 1: Cancelling at {cancel_before_delay:.1f}s (BEFORE timeout)")
                    time.sleep(cancel_before_delay)
                    
                    cancel_success, cancel_response = cancel_market_order(patron, wager_id)
                    if cancel_success:
                        logging.info(f"   ✅ Cancel succeeded (expected) - wager cancelled before timeout")
                        with stats_lock:
                            stats['cancel_before_timeout_success'] += 1
                    else:
                        if 'already matched' in cancel_response.lower() or 'already cancelled' in cancel_response.lower():
                            logging.info(f"   ⚠️  Wager already processed - matched/cancelled before cancel request")
                        else:
                            logging.warning(f"   ❌ Cancel failed (unexpected): {cancel_response[:100]}")
                            with stats_lock:
                                stats['cancel_before_timeout_failed'] += 1
                    
                    # Test 2: Wait until AFTER timeout and try cancel again
                    remaining_time = expected_timeout - cancel_before_delay + 2.0  # Wait 2s past timeout
                    logging.info(f"   Test 2: Waiting {remaining_time:.1f}s more (AFTER {expected_timeout}s timeout)")
                    time.sleep(remaining_time)
                    
                    cancel_success2, cancel_response2 = cancel_market_order(patron, wager_id)
                    if not cancel_success2:
                        if 'already cancelled' in cancel_response2.lower() or 'not found' in cancel_response2.lower():
                            logging.info(f"   ✅ Wager already timed out/cancelled (expected behavior)")
                            with stats_lock:
                                stats['cancel_after_timeout_already_cancelled'] += 1
                        else:
                            logging.warning(f"   ⚠️  Unexpected response: {cancel_response2[:100]}")
                            with stats_lock:
                                stats['cancel_after_timeout_unexpected'] += 1
                    else:
                        logging.warning(f"   ❌ Cancel succeeded AFTER timeout (unexpected!)")
                        with stats_lock:
                            stats['cancel_after_timeout_unexpected'] += 1
                    
                    logging.info(f"   Verification complete\n")
                
                if order_count % 20 == 0:
                    logging.info(f"📊 {worker_name}: {order_count} orders placed")
            else:
                with stats_lock:
                    stats['market_orders_failed'] += 1
        
        # Small delay between orders (skip if we just did verification)
        if not (verify_timeout and order_count % 5 == 0):
            time.sleep(1)
    
    logging.info(f"✅ {worker_name}: Completed {order_count} market orders")


def run_market_order_stress_test(patron, mm_instances, target_event_id, duration_seconds=300, patron_workers=1):
    """
    Continuously place Market Orders on target event using concurrent workers
    This tries to hit the race condition where orders match at timeout
    """
    # Determine expected timeout based on event type
    # Live events (event_id starts with 1500) have 10s timeout
    # Normal events have 5s timeout
    is_live_event = str(target_event_id).startswith('1500')
    expected_timeout = 10.0 if is_live_event else 5.0
    event_type = "LIVE" if is_live_event else "NORMAL"
    
    logging.info(f"\n🎯 Event Type: {event_type} (Expected timeout: {expected_timeout}s)")
    logging.info(f"\n🚀 Starting Market Order stress test for event {target_event_id}")
    logging.info(f"   Duration: {duration_seconds}s")
    logging.info(f"   Workers: {patron_workers} patron workers")
    logging.info(f"   Strategy: Concurrent workers with random line selection\n")
    
    # Find target event from MM instances (patron doesn't have seeded events)
    target_event = None
    if mm_instances:
        mm_instance = list(mm_instances.values())[0]
        for event_id, event_data in mm_instance.sport_events.items():
            if event_id == target_event_id:
                target_event = event_data
                break
    
    if not target_event:
        logging.error(f"❌ Event {target_event_id} not found!")
        return
    
    logging.info(f"✅ Found event: {target_event.get('name', 'Unknown')}")
    
    # Collect line_ids by fetching markets FRESH via get_multiple_markets. The
    # cached mm_instance.sport_events structure omits line_ids for some events
    # (e.g. those exposing lines under `market_lines` rather than `selections`),
    # which made this no-op (0 line_ids → 0 liquidity → 0 market orders). This
    # mirrors exposure-stress's working extraction and handles both shapes.
    line_ids = []
    def _add_sel(sel, mtype):
        if isinstance(sel, dict) and sel.get('line_id'):
            line_ids.append({
                'line_id': sel['line_id'],
                'market_type': mtype,
                'selection_name': sel.get('name', 'Unknown'),
            })
    try:
        _tok = list(mm_instances.values())[0].mm_session.get('access_token', '')
        _resp = requests.get(
            f"{config.BASE_URL}/partner/mm/get_multiple_markets",
            params={"event_ids": str(target_event_id)},
            headers={"Authorization": f"Bearer {_tok}"}, timeout=15)
        _data = _resp.json().get('data', {}) if _resp.status_code == 200 else {}
    except Exception as _e:
        logging.error(f"get_multiple_markets fetch failed: {_e}")
        _data = {}
    for _eid, _markets in _data.items():
        for market in _markets:
            mtype = market.get('type', 'unknown')
            for group in (market.get('selections') or []):
                if isinstance(group, list):
                    for sel in group:
                        _add_sel(sel, mtype)
                else:
                    _add_sel(group, mtype)
            for ml in (market.get('market_lines') or []):
                for group in (ml.get('selections') or []):
                    if isinstance(group, list):
                        for sel in group:
                            _add_sel(sel, mtype)
                    else:
                        _add_sel(group, mtype)
    
    logging.info(f"✅ Collected {len(line_ids)} line_ids from event")
    logging.info(f"   Markets: {len(set(l['market_type'] for l in line_ids))} unique market types\n")
    
    if not line_ids:
        logging.error("❌ No line_ids found in event!")
        return

    # Keep only lines that can actually FILL a market order.
    #
    # market_order_worker picks a line at random, calls estimate-odds, and on a
    # None/zero result just sleeps and retries. If most lines have no fillable
    # book that loop spins for the whole duration and places nothing — which is
    # exactly the "0 market orders placed — test was a no-op" this phase has
    # produced for weeks. A line being LIVE is not the same as a line having
    # LIQUIDITY: a market order consumes resting size on the book, so a perfectly
    # live line can have nothing to fill against.
    #
    # Measured on sandbox 2026-08-12: 8 of 8 lines sampled from a discovered event
    # had availableStake = 0. Filtering up front turns a silent 20-minute no-op
    # into either a real run or an immediate, explicit precondition failure.
    # Same gate as prophet-api-automation globalSetup (QA-236, 2026-08-12).
    MO_PROBE_STAKE = 10.0
    logging.info(f"🔍 Probing {len(line_ids)} line(s) for market-order liquidity "
                 f"(need >= ${MO_PROBE_STAKE:.0f} fillable)...")
    liquid_lines = []
    for ld in line_ids:
        try:
            max_stake, avg_odds, odds_list = get_estimate_odds(patron, ld['line_id'], MO_PROBE_STAKE)
        except Exception:
            max_stake, avg_odds, odds_list = None, None, None
        if max_stake and max_stake >= MO_PROBE_STAKE and avg_odds and odds_list:
            liquid_lines.append(ld)

    logging.info(f"✅ {len(liquid_lines)}/{len(line_ids)} line(s) have fillable liquidity")

    if not liquid_lines:
        logging.error(f"❌ PRECONDITION FAILED: no line on this event can fill a "
                      f"${MO_PROBE_STAKE:.0f} market order.")
        logging.error("   Every order would be skipped and this would report a 0-order "
                      "no-op after burning the full duration.")
        logging.error("   Need an event with resting size on the book (an MM actively "
                      "quoting), or run this alongside the stability traffic.")
        return

    line_ids = liquid_lines

    # Run market order workers (patron account)
    # Spawn multiple workers for increased stress
    with ThreadPoolExecutor(max_workers=patron_workers) as executor:
        futures = []
        for worker_num in range(patron_workers):
            worker_name = f"Patron-W{worker_num+1}" if patron_workers > 1 else "Patron"
            # Enable verification for first worker only to avoid conflicts
            verify_timeout = (worker_num == 0)
            future = executor.submit(
                market_order_worker,
                patron,
                line_ids,
                duration_seconds,
                expected_timeout,
                worker_name,
                verify_timeout
            )
            futures.append(future)
        
        # Wait for all workers to complete
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logging.error(f"❌ Market order worker error: {e}")
    
    logging.info(f"\n✅ Market Order stress test completed")


def print_final_report():
    """Print final statistics report"""
    logging.info("\n" + "="*70)
    logging.info("MARKET ORDER RACE CONDITION TEST - FINAL REPORT")
    logging.info("="*70)
    logging.info(f"Liquidity Wagers Placed:       {stats['liquidity_wagers_placed']}")
    logging.info(f"Market Orders Placed:          {stats['market_orders_placed']}")
    logging.info(f"  - Matched:                   {stats['market_orders_matched']}")
    logging.info(f"  - Timeout/Cancelled:         {stats['market_orders_timeout']}")
    logging.info(f"  - Failed:                    {stats['market_orders_failed']}")
    logging.info(f"Potential Race Conditions:     {stats['potential_race_conditions']}")
    logging.info("")
    logging.info("TIMEOUT VERIFICATION RESULTS:")
    logging.info(f"  Cancel Before Timeout:")
    logging.info(f"    - Success:                 {stats['cancel_before_timeout_success']}")
    logging.info(f"    - Failed:                  {stats['cancel_before_timeout_failed']}")
    logging.info(f"  Cancel After Timeout:")
    logging.info(f"    - Already Cancelled:       {stats['cancel_after_timeout_already_cancelled']}")
    logging.info(f"    - Unexpected:              {stats['cancel_after_timeout_unexpected']}")
    logging.info("="*70)
    logging.info("")
    logging.info("⚠️  CHECK DATABASE FOR RACE CONDITION:")
    logging.info("   Look for wagers where:")
    logging.info("   - 'cancel' value == 'open' value (full refund)")
    logging.info("   - BUT 'match' transaction exists (partial match)")
    logging.info("   - Timestamp of 'match' and 'cancel' are within milliseconds")
    logging.info("")
    logging.info("   Example SQL query:")
    logging.info("   SELECT * FROM transactions")
    logging.info("   WHERE wager_id IN (")
    logging.info("     SELECT wager_id FROM transactions")
    logging.info("     WHERE action_type = 'cancel'")
    logging.info("     AND timestamp > NOW() - INTERVAL '1 hour'")
    logging.info("   )")
    logging.info("   ORDER BY wager_id, timestamp;")
    logging.info("="*70 + "\n")


def main(environment='qa', duration=300, workers_per_account=5, target_event_id=None):
    """Main function to run the race condition reproducer"""
    # Use provided event_id or default
    event_id = target_event_id if target_event_id else TARGET_EVENT_ID
    
    # Determine event type
    is_live = str(event_id).startswith('1500')
    event_type = "LIVE" if is_live else "NORMAL"
    expected_timeout = 10.0 if is_live else 5.0
    
    logging.info("\n" + "="*70)
    logging.info("MARKET ORDER RACE CONDITION REPRODUCER")
    logging.info("="*70)
    logging.info(f"Environment: {environment}")
    logging.info(f"Target Event: {event_id} ({event_type})")
    logging.info(f"Expected Timeout: {expected_timeout}s")
    logging.info(f"Duration: {duration}s ({duration/60:.1f} minutes)")
    logging.info(f"Workers per Account: {workers_per_account}x")
    logging.info(f"Excluding Deduce User: {DEDUCE_USER_UUID}")
    logging.info("="*70 + "\n")
    
    # Load accounts
    logging.info("📦 Loading accounts...")
    patron = load_patron_account(environment)
    mm_instances = load_liquidity_providers(environment, num_accounts=5)
    
    if not mm_instances:
        logging.error("❌ No liquidity providers loaded!")
        return
    
    logging.info(f"\n✅ Loaded {len(mm_instances)} liquidity providers + 1 patron account\n")
    
    # Start liquidity provision in background thread
    liquidity_thread = threading.Thread(
        target=provide_liquidity_continuously,
        args=(mm_instances, event_id, duration, workers_per_account),
        daemon=True
    )
    liquidity_thread.start()
    
    # Give liquidity providers a head start
    logging.info("⏳ Waiting 10 seconds for liquidity to build up...\n")
    time.sleep(10)
    
    # Start Market Order stress test in main thread
    run_market_order_stress_test(patron, mm_instances, event_id, duration - 10, workers_per_account)
    
    # Wait for liquidity thread to finish
    liquidity_thread.join(timeout=30)

    # Print final report
    print_final_report()

    # A run that placed zero market orders exercised nothing (e.g. target event
    # not in the seeded tournaments) — fail loudly instead of a silent green.
    if stats['market_orders_placed'] == 0:
        logging.error("❌ NO-OP RUN: 0 market orders placed — treat as FAIL")
        sys.exit(2)
    if stats['potential_race_conditions'] > 0:
        sys.exit(1)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Reproduce Market Order race condition bug')
    parser.add_argument('--env', type=str, default='qa', choices=['qa', 'sandbox', 'staging'],
                       help='Environment (default: qa)')
    parser.add_argument('--duration', type=int, default=300,
                       help='Test duration in seconds (default: 300 = 5 minutes)')
    parser.add_argument('--workers', type=int, default=5,
                       help='Workers per account for scalability (default: 5, range: 1-10)')
    parser.add_argument('--event-id', type=int, default=None,
                       help='Target event ID (optional, overrides default)')
    
    args = parser.parse_args()
    
    # Validate workers range
    if args.workers < 1 or args.workers > 10:
        logging.error("❌ Workers must be between 1 and 10")
        exit(1)
    
    main(environment=args.env, duration=args.duration, workers_per_account=args.workers, target_event_id=args.event_id)
