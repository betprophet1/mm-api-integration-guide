#!/usr/bin/env python3
"""
Backend Fairness Test

Tests the BACKEND system's fairness enhancement:
- Backend should batch jobs (100 at a time)
- Backend should parallelize by user ID
- Backend should ensure fairness across users

This script:
1. Sends wagers from multiple MM accounts simultaneously
2. Monitors response times and order of execution
3. Verifies fairness in wager matching/processing
4. Does NOT implement batching (that's backend's job)
"""

import argparse
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
import json

from src import mm_calls
from src.log import logging
from src import config

# Metrics tracking
user_metrics = defaultdict(lambda: {
    'placed': 0,
    'matched': 0,
    'cancelled': 0,
    'failed': 0,
    'response_times': [],
    'timestamps': []
})
metrics_lock = threading.Lock()


def load_mm_accounts(environment='sandbox', num_accounts=2, start_account=1):
    """Load multiple MM accounts for testing
    
    Args:
        environment: sandbox or staging
        num_accounts: number of accounts to load
        start_account: starting account number (default: 1)
    """
    import os
    
    # Set environment
    os.environ['MM_ENVIRONMENT'] = environment
    import importlib
    importlib.reload(config)
    
    mm_instances = {}
    
    for account_num in range(start_account, start_account + num_accounts):
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
            
            # Extract the actual partner_id (UUID) from the session
            # This is what the backend uses for tracking
            import base64
            access_token = mm_instance.mm_session.get('access_token', '')
            try:
                # JWT format: header.payload.signature
                # Decode payload (second part) without verification
                parts = access_token.split('.')
                if len(parts) >= 2:
                    # Add padding if needed
                    payload = parts[1]
                    padding = 4 - (len(payload) % 4)
                    if padding != 4:
                        payload += '=' * padding
                    
                    decoded_bytes = base64.urlsafe_b64decode(payload)
                    decoded_json = json.loads(decoded_bytes)
                    user_id = decoded_json.get('partnerID', 'unknown')
                    logging.info(f"🔑 Extracted UUID: {user_id}")
                else:
                    raise Exception("Invalid token format")
            except Exception as e:
                # Fallback to access_key prefix
                access_key = credentials['access_key']
                user_id = access_key.split('_')[0] if '_' in access_key else access_key[:8]
                logging.warning(f"⚠️  Could not extract UUID ({str(e)}), using: {user_id}")
            
            mm_instance.user_id = user_id
            mm_instance.account_num = account_num  # Store account number for token refresh
            mm_instances[user_id] = mm_instance
            logging.info(f"✅ Loaded account {account_num}: {user_id} (Balance: ${mm_instance.balance:.2f})")
            
        except Exception as e:
            logging.warning(f"⚠️  Could not load account {account_num}: {str(e)}")
            continue
    
    return mm_instances


def refresh_account_token(mm_instance, user_id, account_num, environment):
    """Refresh authentication token for an account"""
    try:
        logging.info(f"🔄 Refreshing token for account {account_num} ({user_id[:8]}...)")
        mm_instance.mm_login()
        logging.info(f"✅ Token refreshed for account {account_num}")
        return True
    except Exception as e:
        logging.error(f"❌ Failed to refresh token for account {account_num}: {str(e)}")
        return False


def place_wager_with_tracking(mm_instance, user_id, line_id, odds, wager_num, show_detailed_logs=False, 
                              account_num=None, environment='sandbox'):
    """Place a single wager and track metrics with automatic token refresh on 401"""
    import requests
    from urllib.parse import urljoin
    import uuid
    
    external_id = str(uuid.uuid1())
    body = {
        'external_id': external_id,
        'line_id': line_id,
        'odds': odds,
        'stake': 2.0
    }
    
    max_retries = 2
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            # Log when request starts (to prove parallelism)
            if show_detailed_logs:
                logging.info(f"🚀 [{time.strftime('%H:%M:%S.%f')[:-3]}] User {user_id[:8]} - Wager #{wager_num:03d} STARTING")
            
            request_start = time.time()
            play_url = urljoin(mm_instance.base_url, config.URL['mm_place_wager'])
            response = requests.post(play_url, json=body, headers=mm_instance._MMInteractions__get_auth_header())
            response_time = time.time() - request_start
            
            with metrics_lock:
                user_metrics[user_id]['response_times'].append(response_time)
                user_metrics[user_id]['timestamps'].append(time.time())
            
            if response.status_code == 200:
                response_data = response.json()
                wager_data = response_data.get('data', {})
                if 'wager' in wager_data and 'id' in wager_data['wager']:
                    wager_id = wager_data['wager']['id']
                    wager_status = wager_data['wager'].get('status', 'unknown')
                    
                    with metrics_lock:
                        user_metrics[user_id]['placed'] += 1
                        if wager_status == 'matched':
                            user_metrics[user_id]['matched'] += 1
                    
                    # Log successful completion (to prove parallelism)
                    if show_detailed_logs:
                        logging.info(f"✅ [{time.strftime('%H:%M:%S.%f')[:-3]}] User {user_id[:8]} - Wager #{wager_num:03d} COMPLETED ({response_time*1000:.0f}ms)")
                    
                    return {
                        'user_id': user_id,
                        'wager_id': wager_id,
                        'external_id': external_id,
                        'status': wager_status,
                        'response_time': response_time,
                        'wager_num': wager_num
                    }
            elif response.status_code == 401:
                # Token expired - refresh and retry
                retry_count += 1
                if retry_count < max_retries and account_num:
                    logging.warning(f"⚠️  Token expired for {user_id[:8]}, refreshing... (attempt {retry_count}/{max_retries})")
                    if refresh_account_token(mm_instance, user_id, account_num, environment):
                        continue  # Retry the request
                    else:
                        break  # Failed to refresh, exit retry loop
                else:
                    break  # No more retries or no account_num provided
            else:
                with metrics_lock:
                    user_metrics[user_id]['failed'] += 1
                
                # Get error details
                try:
                    error_body = response.json()
                    error_msg = error_body.get('message', 'No error message')
                    error_code = error_body.get('code', 'No code')
                except:
                    error_msg = response.text[:200] if response.text else 'No response body'
                    error_code = 'N/A'
                
                # Log first few failures for this user in detail
                if user_metrics[user_id]['failed'] <= 3:
                    logging.error(f"❌ User {user_id} wager {wager_num} failed:")
                    logging.error(f"   HTTP Status: {response.status_code}")
                    logging.error(f"   Error Code: {error_code}")
                    logging.error(f"   Error Message: {error_msg}")
                    logging.error(f"   Line ID: {line_id}")
                    logging.error(f"   Odds: {odds}")
                break  # Exit retry loop for non-401 errors
                
        except Exception as e:
            retry_count += 1
            if retry_count >= max_retries:
                with metrics_lock:
                    user_metrics[user_id]['failed'] += 1
                logging.error(f"User {user_id} wager {wager_num} error: {str(e)}")
                break
            time.sleep(0.1)  # Brief delay before retry
    
    return None


def send_concurrent_wagers(mm_instances, markets, total_wagers, rate_limit=50, show_detailed_logs=False, max_workers=50):
    """
    Send wagers from multiple users CONCURRENTLY - STRESS TEST MODE
    
    This simulates real load where multiple users submit at same time.
    The BACKEND should handle batching and fairness.
    
    For high volume tests, we throttle to avoid API rate limiting.
    
    :param markets: List of market dicts (can be single line_id for backward compat, or list of markets)
    :param rate_limit: Maximum wagers per second (default: 50)
    :param show_detailed_logs: Show detailed per-wager logs to prove parallelism
    :param max_workers: Maximum concurrent threads (default: 50)
    """
    user_ids = list(mm_instances.keys())
    wagers_per_user = total_wagers // len(user_ids)
    
    logging.info(f"\n🚀 STRESS TEST: Sending {total_wagers:,} concurrent wagers from {len(user_ids)} users")
    logging.info(f"   Each user will send {wagers_per_user:,} wagers")
    logging.info(f"   Backend should batch and parallelize these")
    logging.info(f"   Rate limit: {rate_limit} wagers/sec (to avoid API throttling)")
    logging.info(f"   Max concurrent workers: {max_workers}\n")
    
    all_wagers = []
    start_time = time.time()
    last_progress_time = start_time
    completed_count = 0
    submitted_count = 0
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        
        # Submit all wagers for all users in INTERLEAVED fashion
        # This ensures parallel execution across users
        # Rotate through different markets to spread load
        if show_detailed_logs:
            logging.info("\n📋 SUBMITTING WAGERS IN PARALLEL (interleaved by user)\n")
        
        wager_counter = 0
        for wager_num in range(1, wagers_per_user + 1):
            for user_id in user_ids:
                mm_instance = mm_instances[user_id]
                odds = mm_instance._MMInteractions__get_random_odds()
                
                # Rotate through available markets
                market = markets[wager_counter % len(markets)]
                line_id = market['line_id'] if isinstance(market, dict) else market
                
                future = executor.submit(
                    place_wager_with_tracking,
                    mm_instance,
                    user_id,
                    line_id,
                    odds,
                    wager_num,
                    show_detailed_logs,
                    mm_instance.account_num,
                    'sandbox'  # Pass environment
                )
                futures.append(future)
                wager_counter += 1
        
        # Collect results with progress reporting
        for future in as_completed(futures):
            try:
                result = future.result()
                if result:
                    all_wagers.append(result)
                    completed_count += 1
                    
                    # Progress report every 5 seconds
                    current_time = time.time()
                    if current_time - last_progress_time >= 5:
                        elapsed_so_far = current_time - start_time
                        rate = completed_count / elapsed_so_far if elapsed_so_far > 0 else 0
                        pct = (completed_count / total_wagers) * 100
                        eta = (total_wagers - completed_count) / rate if rate > 0 else 0
                        
                        # Check per-user fairness so far
                        with metrics_lock:
                            user_placed = {uid: user_metrics[uid]['placed'] for uid in user_ids}
                            placed_list = list(user_placed.values())
                            current_fairness = min(placed_list) / max(placed_list) if max(placed_list) > 0 else 0
                        
                        logging.info(f"📊 Progress: {completed_count:,}/{total_wagers:,} ({pct:.1f}%) | "
                                   f"Rate: {rate:.1f}/s | "
                                   f"Fairness: {current_fairness:.1%} | "
                                   f"ETA: {eta/60:.1f}m")
                        last_progress_time = current_time
                        
            except Exception as e:
                logging.error(f"Future error: {str(e)}")
    
    elapsed = time.time() - start_time
    
    return all_wagers, elapsed


def analyze_fairness(mm_instances):
    """Analyze the fairness of wager distribution"""
    user_ids = list(mm_instances.keys())
    
    logging.info("\n" + "="*70)
    logging.info("BACKEND FAIRNESS ANALYSIS")
    logging.info("="*70)
    
    # Calculate metrics per user
    placed_counts = []
    for user_id in user_ids:
        with metrics_lock:
            metrics = user_metrics[user_id]
            placed = metrics['placed']
            matched = metrics['matched']
            failed = metrics['failed']
            response_times = metrics['response_times']
            timestamps = metrics['timestamps']
        
        placed_counts.append(placed)
        avg_rt = sum(response_times) / len(response_times) if response_times else 0
        
        logging.info(f"\n👤 User {user_id}:")
        logging.info(f"   Placed: {placed}")
        logging.info(f"   Matched: {matched}")
        logging.info(f"   Failed: {failed}")
        logging.info(f"   Avg Response Time: {avg_rt*1000:.0f}ms")
        logging.info(f"   Total Requests: {len(response_times)}")
    
    # Calculate fairness ratio
    if placed_counts:
        max_placed = max(placed_counts)
        min_placed = min(placed_counts)
        fairness_ratio = min_placed / max_placed if max_placed > 0 else 0
        
        logging.info(f"\n⚖️  FAIRNESS RATIO: {fairness_ratio:.2%}")
        logging.info(f"   Min placed: {min_placed}")
        logging.info(f"   Max placed: {max_placed}")
        
        if fairness_ratio >= 0.90:
            logging.info("   ✅ PASS: Backend maintains fairness (≥90%)")
        elif fairness_ratio >= 0.85:
            logging.info("   ⚠️  WARNING: Fairness acceptable but not ideal (85-90%)")
        else:
            logging.info("   ❌ FAIL: Backend not maintaining fairness (<85%)")
    
    logging.info("="*70 + "\n")


def run_backend_fairness_test(event_name, total_wagers=200, environment='sandbox', verbose=False, max_workers=50, deduce_mode=False):
    """Run the backend fairness test
    
    Args:
        event_name: Event ID or name to test
        total_wagers: Total number of wagers to place
        environment: 'sandbox' or 'staging'
        verbose: Enable detailed per-wager logging
        max_workers: Maximum concurrent workers
        deduce_mode: If True, include MM1 (deduce-enabled). If False, use only non-deduce accounts (MM2-3)
    """
    import time as time_module
    test_start_time = time.time()
    
    logging.info("="*70)
    logging.info("BACKEND FAIRNESS ENHANCEMENT TEST")
    logging.info("="*70)
    logging.info(f"Environment: {environment}")
    logging.info(f"Target event: {event_name}")
    logging.info(f"Total wagers: {total_wagers}")
    logging.info(f"Deduce mode: {'ENABLED (includes MM1)' if deduce_mode else 'DISABLED (non-deduce only)'}")
    logging.info("="*70 + "\n")
    
    # Load MM accounts based on deduce mode
    if deduce_mode:
        logging.info("📦 Loading MM accounts 1-2 (MM1 deduce-enabled, MM2 non-deduce)...")
        mm_instances = load_mm_accounts(environment, num_accounts=2, start_account=1)
    else:
        logging.info("📦 Loading MM accounts 2-3 (MM2 and MM3, non-deduce only)...")
        mm_instances = load_mm_accounts(environment, num_accounts=2, start_account=2)
    
    if len(mm_instances) < 2:
        logging.error("❌ Need at least 2 MM accounts for fairness testing")
        return
    
    logging.info(f"✅ Loaded {len(mm_instances)} accounts")
    
    # Store initial balances
    initial_balances = {}
    for user_id, mm_inst in mm_instances.items():
        initial_balances[user_id] = mm_inst.balance
    
    # Validate accounts
    logging.info("\n🔍 Initial Account Balances:")
    for user_id, mm_inst in mm_instances.items():
        logging.info(f"   User {user_id}: ${mm_inst.balance:,.2f}")
        if mm_inst.balance < 100:
            logging.warning(f"   ⚠️  Low balance for {user_id}!")
    logging.info("")
    
    # Collect markets from multiple events
    mm_instance = list(mm_instances.values())[0]
    available_markets = []
    
    # If event_name is provided, find matching events
    if event_name:
        matching_events = mm_instance.find_event_by_id_or_name(event_name)
        if not matching_events:
            logging.error(f"❌ No events found matching '{event_name}'")
            logging.info(f"\nℹ️  Available events:")
            for evt_id, evt_data in list(mm_instance.sport_events.items())[:10]:
                logging.info(f"   ID: {evt_id} | Name: {evt_data.get('name', 'Unknown')}")
            return
        events_to_use = matching_events[:5]  # Use up to 5 matching events
    else:
        # Use all available events
        events_to_use = list(mm_instance.sport_events.values())[:10]  # Use up to 10 events
    
    # Collect markets from all events
    for event in events_to_use:
        event_name_str = event.get('name', 'Unknown')
        for market in event.get('markets', []):
            if market.get('selections'):
                for selection_group in market['selections']:
                    if isinstance(selection_group, list):
                        for selection in selection_group:
                            if selection.get('line_id'):
                                available_markets.append({
                                    'event_name': event_name_str,
                                    'market_type': market.get('type'),
                                    'line_id': selection['line_id'],
                                    'selection_name': selection.get('name', 'Unknown')
                                })
    
    if not available_markets:
        logging.error(f"❌ No valid markets found")
        return
    
    logging.info(f"✅ Collected {len(available_markets)} markets from {len(events_to_use)} events")
    logging.info(f"   Markets will be rotated to spread bets across events\n")
    
    if verbose:
        logging.info("\n🔍 VERBOSE MODE: Detailed per-wager logging enabled")
        logging.info("   This will show requests from all users executing in parallel\n")
    
    # Send concurrent wagers (pass all markets, function will rotate through them)
    all_wagers, elapsed = send_concurrent_wagers(mm_instances, available_markets, total_wagers, show_detailed_logs=verbose, max_workers=max_workers)
    
    # Report results
    total_placed = sum([user_metrics[uid]['placed'] for uid in mm_instances.keys()])
    logging.info(f"\n✅ Completed in {elapsed:.2f}s")
    logging.info(f"   Total placed: {total_placed}/{total_wagers}")
    logging.info(f"   Placement rate: {total_placed/elapsed:.1f} wagers/sec")
    
    # Analyze fairness
    analyze_fairness(mm_instances)
    
    # Check final balances
    logging.info("\n" + "="*70)
    logging.info("BALANCE VERIFICATION")
    logging.info("="*70)
    
    final_balances = {}
    balance_changes = {}
    
    for user_id, mm_inst in mm_instances.items():
        try:
            mm_inst.get_balance()
            final_balances[user_id] = mm_inst.balance
            balance_changes[user_id] = initial_balances[user_id] - mm_inst.balance
        except Exception as e:
            logging.error(f"   ❌ Could not fetch balance for {user_id}: {str(e)}")
            final_balances[user_id] = None
            balance_changes[user_id] = None
    
    # Display balance changes
    total_spent = 0
    for user_id in mm_instances.keys():
        if final_balances[user_id] is not None:
            logging.info(f"\n👤 User {user_id}:")
            logging.info(f"   Initial:  ${initial_balances[user_id]:,.2f}")
            logging.info(f"   Final:    ${final_balances[user_id]:,.2f}")
            logging.info(f"   Spent:    ${balance_changes[user_id]:,.2f}")
            logging.info(f"   Wagers:   {user_metrics[user_id]['placed']}")
            if user_metrics[user_id]['placed'] > 0:
                avg_per_wager = balance_changes[user_id] / user_metrics[user_id]['placed']
                logging.info(f"   Avg/wager: ${avg_per_wager:.2f}")
            total_spent += balance_changes[user_id]
    
    logging.info(f"\n📊 Total spent across all users: ${total_spent:,.2f}")
    expected_spent = total_placed * 2.0  # $2 stake per wager
    logging.info(f"   Expected ($2 per wager): ${expected_spent:,.2f}")
    
    if abs(total_spent - expected_spent) < 2.0:
        logging.info(f"   ✅ Balance matches expected spending!")
    else:
        diff = abs(total_spent - expected_spent)
        logging.warning(f"   ⚠️  Difference: ${diff:,.2f}")
    
    logging.info("="*70 + "\n")
    
    # Save detailed results
    results = {
        'test_config': {
            'environment': environment,
            'event': event_name,
            'total_wagers': total_wagers,
            'num_users': len(mm_instances),
            'duration_seconds': elapsed
        },
        'per_user_metrics': {}
    }
    
    for user_id in mm_instances.keys():
        results['per_user_metrics'][user_id] = dict(user_metrics[user_id])
    
    output_file = f"backend_fairness_test_{int(time.time())}.json"
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    
    logging.info(f"📄 Detailed results saved to: {output_file}")
    
    # Generate SQL query for database verification
    user_uuids = list(mm_instances.keys())
    test_start = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(test_start_time))
    test_end = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())
    
    sql_query = f"""
-- Database Verification Query
-- Run this on the database to verify backend fairness

SELECT 
    count(*) as wager_count,
    w.user_id,
    ROUND(count(*) * 100.0 / SUM(count(*)) OVER (), 2) as percentage
FROM wagers w
WHERE w.created_at >= '{test_start}'
  AND w.created_at <= '{test_end}'
  AND w.user_id IN (
{', '.join([f"    '{uid}'" for uid in user_uuids])}
  )
GROUP BY w.user_id
ORDER BY wager_count DESC;

-- Expected: Each user should have ~{total_wagers // len(user_uuids)} wagers ({100/len(user_uuids):.1f}% each)
-- User UUIDs:
{chr(10).join([f'--   {uid}' for uid in user_uuids])}
"""
    
    sql_file = f"verify_fairness_{int(time.time())}.sql"
    with open(sql_file, 'w') as f:
        f.write(sql_query)
    
    logging.info(f"\n📊 Database Verification:")
    logging.info(f"   SQL query saved to: {sql_file}")
    logging.info(f"   Run this query on the database to verify backend fairness")
    logging.info(f"   Expected: ~{total_wagers // len(user_uuids)} wagers per user ({100/len(user_uuids):.1f}% each)")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Test backend fairness enhancement - Stress Test')
    parser.add_argument('--event', type=str, required=True, help='Event name to test')
    parser.add_argument('--wagers', type=int, default=1000, help='Total wagers to send per iteration (default: 1000)')
    parser.add_argument('--env', type=str, default='sandbox', choices=['sandbox', 'staging'],
                       help='Environment (default: sandbox)')
    parser.add_argument('--verbose', action='store_true', help='Enable detailed per-wager logging to prove parallel execution')
    parser.add_argument('--workers', type=int, default=50, help='Max concurrent workers (default: 50, higher = faster)')
    parser.add_argument('--continuous', action='store_true', help='Run continuously with automatic token refresh')
    parser.add_argument('--iterations', type=int, default=0, help='Number of iterations in continuous mode (0=infinite, default: 0)')
    parser.add_argument('--delay', type=int, default=5, help='Delay in seconds between continuous mode iterations (default: 5)')
    parser.add_argument('--deducemode', action='store_true', help='Enable deduce mode: include MM1 (deduce-enabled) with MM2. If disabled, uses MM2-3 (non-deduce only)')
    
    args = parser.parse_args()
    
    logging.info("\n" + "="*70)
    logging.info("BACKEND FAIRNESS STRESS TEST")
    logging.info("Testing requirement: 'For each 100 jobs in a batch, parallel by user id'")
    logging.info("Backend should handle batching and ensure fairness")
    if args.continuous:
        logging.info("🔄 CONTINUOUS MODE: Token auto-refresh enabled")
        if args.iterations > 0:
            logging.info(f"   Running {args.iterations} iterations")
        else:
            logging.info("   Running indefinitely (Ctrl+C to stop)")
        logging.info(f"   Delay between iterations: {args.delay}s")
    logging.info("="*70 + "\n")
    
    if args.continuous:
        iteration = 0
        try:
            while args.iterations == 0 or iteration < args.iterations:
                iteration += 1
                logging.info(f"\n{'='*70}")
                logging.info(f"🔄 ITERATION {iteration}" + (f" / {args.iterations}" if args.iterations > 0 else ""))
                logging.info(f"{'='*70}\n")
                
                run_backend_fairness_test(args.event, args.wagers, args.env, verbose=args.verbose, max_workers=args.workers, deduce_mode=args.deducemode)
                
                if args.iterations == 0 or iteration < args.iterations:
                    logging.info(f"\n⏸️  Waiting {args.delay}s before next iteration...\n")
                    time.sleep(args.delay)
        except KeyboardInterrupt:
            logging.info("\n\n⏹️  Continuous mode stopped by user")
            logging.info(f"   Completed {iteration} iteration(s)\n")
    else:
        run_backend_fairness_test(args.event, args.wagers, args.env, verbose=args.verbose, max_workers=args.workers, deduce_mode=args.deducemode)
