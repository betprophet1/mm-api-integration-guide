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
}
stats_lock = threading.Lock()

DEDUCE_USER_UUID = '279cc6a1-d926-4273-a18a-782eccfbce7b'  # Exclude from liquidity
TARGET_EVENT_ID = 20023066


def load_patron_account(environment='sandbox'):
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
    base_url = config.ENVIRONMENT_URLS.get(environment, config.ENVIRONMENT_URLS['sandbox'])
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


def load_liquidity_providers(environment='sandbox', num_accounts=5):
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
    
    # Collect all line_ids from the event
    line_ids = []
    for market in target_event.get('markets', []):
        for selection_group in market.get('selections', []):
            if isinstance(selection_group, list):
                for selection in selection_group:
                    if selection.get('line_id'):
                        line_ids.append({
                            'line_id': selection['line_id'],
                            'market_type': market.get('type'),
                            'selection_name': selection.get('name', 'Unknown')
                        })
    
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


def place_market_order(patron, line_id, stake, expected_avg_odds, odds_list):
    """
    Place a Market Order
    Returns: (success, wager_id, status)
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
            wager_id = data.get('wagerId')
            status = data.get('status', 'unknown')
            
            logging.info(f"✅ Market Order placed: ID={wager_id}, Status={status}, Elapsed={elapsed:.2f}s")
            
            # Check if it took close to 5 seconds (potential timeout scenario)
            if elapsed >= 4.5:
                logging.warning(f"⚠️  POTENTIAL RACE CONDITION: Order took {elapsed:.2f}s (near 5s timeout)")
                with stats_lock:
                    stats['potential_race_conditions'] += 1
            
            return True, wager_id, status
        else:
            logging.error(f"❌ Market Order failed: {response.status_code} - {response.text}")
            return False, None, None
    except Exception as e:
        logging.error(f"❌ Market Order error: {e}")
        return False, None, None


def market_order_worker(patron, line_ids, duration_seconds, worker_name="Patron"):
    """
    Worker function for placing market orders
    Runs concurrently to maximize stress on the system
    """
    start_time = time.time()
    order_count = 0
    
    logging.info(f"🚀 {worker_name}: Starting market order worker")
    
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
            success, wager_id, status = place_market_order(patron, line_id, max_stake, avg_odds, odds_list)
            
            if success:
                with stats_lock:
                    stats['market_orders_placed'] += 1
                    if status == 'matched':
                        stats['market_orders_matched'] += 1
                    elif status == 'timeout' or status == 'cancelled':
                        stats['market_orders_timeout'] += 1
                
                order_count += 1
                
                if order_count % 20 == 0:
                    logging.info(f"📊 {worker_name}: {order_count} orders placed")
            else:
                with stats_lock:
                    stats['market_orders_failed'] += 1
        
        # Small delay between orders
        time.sleep(1)
    
    logging.info(f"✅ {worker_name}: Completed {order_count} market orders")


def run_market_order_stress_test(patron, mm_instances, target_event_id, duration_seconds=300, patron_workers=1):
    """
    Continuously place Market Orders on target event using concurrent workers
    This tries to hit the race condition where orders match at timeout
    """
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
    
    # Collect all line_ids from the event
    line_ids = []
    for market in target_event.get('markets', []):
        for selection_group in market.get('selections', []):
            if isinstance(selection_group, list):
                for selection in selection_group:
                    if selection.get('line_id'):
                        line_ids.append({
                            'line_id': selection['line_id'],
                            'market_type': market.get('type'),
                            'selection_name': selection.get('name', 'Unknown')
                        })
    
    logging.info(f"✅ Collected {len(line_ids)} line_ids from event")
    logging.info(f"   Markets: {len(set(l['market_type'] for l in line_ids))} unique market types\n")
    
    if not line_ids:
        logging.error("❌ No line_ids found in event!")
        return
    
    # Run market order workers (patron account)
    # Spawn multiple workers for increased stress
    with ThreadPoolExecutor(max_workers=patron_workers) as executor:
        futures = []
        for worker_num in range(patron_workers):
            worker_name = f"Patron-W{worker_num+1}" if patron_workers > 1 else "Patron"
            future = executor.submit(
                market_order_worker,
                patron,
                line_ids,
                duration_seconds,
                worker_name
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
    logging.info(f"Liquidity Wagers Placed:      {stats['liquidity_wagers_placed']}")
    logging.info(f"Market Orders Placed:          {stats['market_orders_placed']}")
    logging.info(f"  - Matched:                   {stats['market_orders_matched']}")
    logging.info(f"  - Timeout/Cancelled:         {stats['market_orders_timeout']}")
    logging.info(f"  - Failed:                    {stats['market_orders_failed']}")
    logging.info(f"Potential Race Conditions:     {stats['potential_race_conditions']}")
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


def main(environment='sandbox', duration=300, workers_per_account=5):
    """Main function to run the race condition reproducer"""
    logging.info("\n" + "="*70)
    logging.info("MARKET ORDER RACE CONDITION REPRODUCER")
    logging.info("="*70)
    logging.info(f"Environment: {environment}")
    logging.info(f"Target Event: {TARGET_EVENT_ID}")
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
        args=(mm_instances, TARGET_EVENT_ID, duration, workers_per_account),
        daemon=True
    )
    liquidity_thread.start()
    
    # Give liquidity providers a head start
    logging.info("⏳ Waiting 10 seconds for liquidity to build up...\n")
    time.sleep(10)
    
    # Start Market Order stress test in main thread
    run_market_order_stress_test(patron, mm_instances, TARGET_EVENT_ID, duration - 10, workers_per_account)
    
    # Wait for liquidity thread to finish
    liquidity_thread.join(timeout=30)
    
    # Print final report
    print_final_report()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Reproduce Market Order race condition bug')
    parser.add_argument('--env', type=str, default='sandbox', choices=['sandbox', 'staging'],
                       help='Environment (default: sandbox)')
    parser.add_argument('--duration', type=int, default=300,
                       help='Test duration in seconds (default: 300 = 5 minutes)')
    parser.add_argument('--workers', type=int, default=5,
                       help='Workers per account for scalability (default: 5, range: 1-10)')
    
    args = parser.parse_args()
    
    # Validate workers range
    if args.workers < 1 or args.workers > 10:
        logging.error("❌ Workers must be between 1 and 10")
        exit(1)
    
    main(environment=args.env, duration=args.duration, workers_per_account=args.workers)
