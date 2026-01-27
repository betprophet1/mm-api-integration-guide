#!/usr/bin/env python3
"""
Stress Test: 30K Bet & Cancel Wagers on Single Event
Target: Spawn 30,000 wagers (bet + cancel) on 1 event
Strategy: Aggressive batch placement + immediate cancellation

ENHANCEMENT: Fairness validation
- For each 100 jobs in a batch, parallelize requests by user ID
- Ensures fair distribution across multiple users/SPs
- Tracks per-user metrics to validate fairness
"""

import argparse
import signal
import sys
import time
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
from datetime import datetime

from src import mm_calls
from src.log import logging

# Global control
should_stop = False
wagers_placed_count = 0
wagers_cancelled_count = 0
target_wagers = 30000
wagers_lock = threading.Lock()

# Fairness tracking per user
user_metrics = defaultdict(lambda: {
    'placed': 0,
    'cancelled': 0,
    'failed': 0,
    'total_response_time': 0,
    'request_count': 0,
    'last_request_time': None
})
metrics_lock = threading.Lock()

def signal_handler(sig, frame):
    """Handle Ctrl+C to gracefully stop"""
    global should_stop
    logging.info("\n🛑 Received stop signal. Stopping stress test...")
    should_stop = True
    sys.exit(0)

def progress_monitor():
    """Monitor and report progress with fairness metrics"""
    global should_stop, wagers_placed_count, wagers_cancelled_count, target_wagers, user_metrics
    
    start_time = time.time()
    while not should_stop:
        time.sleep(5)  # Report every 5 seconds
        if not should_stop:
            elapsed = time.time() - start_time
            with wagers_lock:
                placed = wagers_placed_count
                cancelled = wagers_cancelled_count
            
            placed_rate = placed / elapsed if elapsed > 0 else 0
            cancelled_rate = cancelled / elapsed if elapsed > 0 else 0
            remaining = target_wagers - placed
            eta = remaining / placed_rate if placed_rate > 0 else 0
            
            # Get fairness metrics
            with metrics_lock:
                user_counts = {uid: metrics['placed'] for uid, metrics in user_metrics.items()}
                if user_counts:
                    avg_per_user = sum(user_counts.values()) / len(user_counts)
                    max_user = max(user_counts.values()) if user_counts else 0
                    min_user = min(user_counts.values()) if user_counts else 0
                    fairness_ratio = min_user / max_user if max_user > 0 else 0
                    fairness_str = f"Fairness: {fairness_ratio:.2%} (min:{min_user} max:{max_user} avg:{avg_per_user:.0f})"
                else:
                    fairness_str = "Fairness: N/A"
            
            logging.info(f"📊 PROGRESS: {placed}/{target_wagers} placed ({placed/target_wagers*100:.1f}%) | "
                        f"{cancelled} cancelled | "
                        f"Rate: {placed_rate:.1f} bets/s, {cancelled_rate:.1f} cancels/s | "
                        f"{fairness_str} | "
                        f"ETA: {eta/60:.1f}m")

def place_single_wager(mm_instance, line_id, odds, user_id=None):
    """Place a single wager with user tracking"""
    global should_stop, wagers_placed_count, user_metrics
    
    if should_stop:
        return None
    
    if user_id is None:
        user_id = getattr(mm_instance, 'user_id', 'unknown')
        
    external_id = str(uuid.uuid1())
    body = {
        'external_id': external_id,
        'line_id': line_id,
        'odds': odds,
        'stake': 1.0
    }
    
    try:
        import requests
        from urllib.parse import urljoin
        from src import config
        
        request_start = time.time()
        play_url = urljoin(mm_instance.base_url, config.URL['mm_place_wager'])
        response = requests.post(play_url, json=body, headers=mm_instance._MMInteractions__get_auth_header())
        response_time = time.time() - request_start
        
        with metrics_lock:
            user_metrics[user_id]['request_count'] += 1
            user_metrics[user_id]['total_response_time'] += response_time
            user_metrics[user_id]['last_request_time'] = time.time()
        
        if response.status_code == 200:
            response_data = response.json()
            wager_data = response_data.get('data', {})
            if 'wager' in wager_data and 'id' in wager_data['wager']:
                wager_id = wager_data['wager']['id']
                with wagers_lock:
                    wagers_placed_count += 1
                with metrics_lock:
                    user_metrics[user_id]['placed'] += 1
                return {'external_id': external_id, 'wager_id': wager_id, 'user_id': user_id}
        else:
            with metrics_lock:
                user_metrics[user_id]['failed'] += 1
    except Exception as e:
        logging.error(f"Error placing single wager (user {user_id}): {str(e)}")
        with metrics_lock:
            user_metrics[user_id]['failed'] += 1
    
    return None

def place_batch_wagers(mm_instance, line_id, odds, batch_size=20, user_id=None):
    """Place batch wagers (max 20 per API call) with user tracking"""
    global should_stop, wagers_placed_count, user_metrics
    
    if should_stop:
        return []
    
    if user_id is None:
        user_id = getattr(mm_instance, 'user_id', 'unknown')
    
    external_ids = [str(uuid.uuid1()) for _ in range(batch_size)]
    batch_body = [{
        'external_id': external_ids[i],
        'line_id': line_id,
        'odds': odds,
        'stake': 1.0
    } for i in range(batch_size)]
    
    try:
        import requests
        from urllib.parse import urljoin
        from src import config
        
        request_start = time.time()
        batch_url = urljoin(mm_instance.base_url, config.URL['mm_batch_place'])
        response = requests.post(batch_url, json={"data": batch_body}, 
                                headers=mm_instance._MMInteractions__get_auth_header())
        response_time = time.time() - request_start
        
        with metrics_lock:
            user_metrics[user_id]['request_count'] += 1
            user_metrics[user_id]['total_response_time'] += response_time
            user_metrics[user_id]['last_request_time'] = time.time()
        
        if response.status_code == 200:
            batch_response = response.json()
            succeed_wagers = batch_response.get('data', {}).get('succeed_wagers', [])
            with wagers_lock:
                wagers_placed_count += len(succeed_wagers)
            with metrics_lock:
                user_metrics[user_id]['placed'] += len(succeed_wagers)
            return [{'external_id': w['external_id'], 'wager_id': w['id'], 'user_id': user_id} for w in succeed_wagers]
        else:
            with metrics_lock:
                user_metrics[user_id]['failed'] += batch_size
    except Exception as e:
        logging.error(f"Error placing batch wagers (user {user_id}): {str(e)}")
        with metrics_lock:
            user_metrics[user_id]['failed'] += batch_size
    
    return []

def cancel_wagers(mm_instance, wagers_to_cancel, user_id=None):
    """Cancel a batch of wagers with user tracking"""
    global should_stop, wagers_cancelled_count, user_metrics
    
    if should_stop or not wagers_to_cancel:
        return
    
    if user_id is None:
        user_id = getattr(mm_instance, 'user_id', 'unknown')
    
    # Batch cancel (max 20 per API call)
    batch_size = min(20, len(wagers_to_cancel))
    batch = wagers_to_cancel[:batch_size]
    
    try:
        import requests
        from urllib.parse import urljoin
        from src import config
        
        batch_cancel_body = [{'wager_id': w['wager_id'], 'external_id': w['external_id']} for w in batch]
        cancel_url = urljoin(mm_instance.base_url, config.URL['mm_batch_cancel'])
        response = requests.post(cancel_url, json={'data': batch_cancel_body}, 
                                headers=mm_instance._MMInteractions__get_auth_header())
        
        if response.status_code == 200:
            with wagers_lock:
                wagers_cancelled_count += len(batch)
            with metrics_lock:
                user_metrics[user_id]['cancelled'] += len(batch)
        elif response.status_code == 404:
            # Already cancelled is ok
            with wagers_lock:
                wagers_cancelled_count += len(batch)
            with metrics_lock:
                user_metrics[user_id]['cancelled'] += len(batch)
    except Exception as e:
        logging.error(f"Error cancelling wagers (user {user_id}): {str(e)}")

def stress_test_worker_parallel(worker_id, mm_instances, event, market, selection, num_wagers):
    """
    Worker thread with FAIRNESS enhancement:
    - For each 100 jobs, parallelize requests by user ID
    - Ensures fair distribution across all MM accounts
    """
    global should_stop, wagers_placed_count, target_wagers
    
    user_ids = list(mm_instances.keys())
    logging.info(f"🔥 Worker {worker_id}: Starting with target {num_wagers} wagers across {len(user_ids)} users")
    
    line_id = selection[0]['line_id']
    wagers_buffer_by_user = {uid: [] for uid in user_ids}
    placed_by_worker = 0
    batch_count = 0
    
    while not should_stop and placed_by_worker < num_wagers:
        # Check global target
        with wagers_lock:
            if wagers_placed_count >= target_wagers:
                break
        
        # FAIRNESS: Every 100 jobs, parallelize by user ID
        jobs_in_batch = min(100, num_wagers - placed_by_worker)
        jobs_per_user = jobs_in_batch // len(user_ids)
        
        batch_count += 1
        logging.info(f"\n🔄 Worker {worker_id} | Batch {batch_count}:")
        logging.info(f"   📦 Processing {jobs_in_batch} jobs → Split: {jobs_per_user} per user")
        logging.info(f"   👥 Users: {', '.join([uid[:8] for uid in user_ids])}")
        
        # Parallelize placement across users
        logging.info(f"   ⚡ PARALLELIZING: Submitting requests for all users simultaneously...")
        parallel_start = time.time()
        
        with ThreadPoolExecutor(max_workers=len(user_ids)) as executor:
            future_to_user = {}
            user_job_count = {uid: 0 for uid in user_ids}
            
            for user_id in user_ids:
                mm_instance = mm_instances[user_id]
                odds = mm_instance._MMInteractions__get_random_odds()
                
                # Submit parallel requests for this user (batches of 20)
                num_batches = (jobs_per_user + 19) // 20  # Round up
                user_jobs = jobs_per_user
                
                for _ in range(num_batches):
                    batch_size = min(20, user_jobs)
                    if batch_size > 0:
                        future = executor.submit(
                            place_batch_wagers,
                            mm_instance,
                            line_id,
                            odds,
                            batch_size,
                            user_id
                        )
                        future_to_user[future] = user_id
                        user_job_count[user_id] += batch_size
                        user_jobs -= batch_size
            
            logging.info(f"   📤 Submitted: {', '.join([f'{uid[:8]}:{cnt}' for uid, cnt in user_job_count.items()])}") 
            
            # Collect results
            user_placed_count = {uid: 0 for uid in user_ids}
            for future in as_completed(future_to_user):
                user_id = future_to_user[future]
                try:
                    batch_wagers = future.result()
                    if batch_wagers:
                        wagers_buffer_by_user[user_id].extend(batch_wagers)
                        placed_by_worker += len(batch_wagers)
                        user_placed_count[user_id] += len(batch_wagers)
                except Exception as e:
                    logging.error(f"Worker {worker_id} error for user {user_id}: {str(e)}")
        
        parallel_elapsed = time.time() - parallel_start
        logging.info(f"   ✅ Completed in {parallel_elapsed:.2f}s: {', '.join([f'{uid[:8]}:{cnt}' for uid, cnt in user_placed_count.items()])}")
        
        # Show running totals per user
        with metrics_lock:
            running_totals = {uid: user_metrics[uid]['placed'] for uid in user_ids}
            logging.info(f"   📊 Running totals: {', '.join([f'{uid[:8]}:{cnt}' for uid, cnt in running_totals.items()])}") 
        
        # Cancel wagers for each user when buffer is large enough
        for user_id in user_ids:
            if len(wagers_buffer_by_user[user_id]) >= 40:
                mm_instance = mm_instances[user_id]
                cancel_wagers(mm_instance, wagers_buffer_by_user[user_id][:20], user_id)
                wagers_buffer_by_user[user_id] = wagers_buffer_by_user[user_id][20:]
        
        # Small delay between batches
        time.sleep(0.1)
    
    # Cancel remaining wagers for all users
    for user_id in user_ids:
        mm_instance = mm_instances[user_id]
        while wagers_buffer_by_user[user_id] and not should_stop:
            cancel_wagers(mm_instance, wagers_buffer_by_user[user_id][:20], user_id)
            wagers_buffer_by_user[user_id] = wagers_buffer_by_user[user_id][20:]
            time.sleep(0.1)
    
    logging.info(f"✅ Worker {worker_id}: Completed {placed_by_worker} wagers (distributed across {len(user_ids)} users)")

def load_multiple_mm_accounts(environment='sandbox'):
    """
    Load multiple MM accounts for fairness testing
    Returns dict of {user_id: mm_instance}
    """
    from src import config
    
    mm_instances = {}
    
    # Load multiple accounts (Account 1 and Account 2)
    account_numbers = [1, 2]
    
    for account_num in account_numbers:
        try:
            # Get credentials for this account
            credentials = config.get_account_credentials(account_num, environment)
            
            # Create MM instance for this account
            mm_instance = mm_calls.MMInteractions()
            
            # Override mm_keys with this account's credentials
            mm_instance.mm_keys = {
                'access_key': credentials['access_key'],
                'secret_key': credentials['secret_key']
            }
            
            # Login using credentials
            mm_instance.mm_login()
            mm_instance.get_balance()
            mm_instance.seeding()
            
            # Extract user_id from access_key
            access_key = credentials['access_key']
            user_id = access_key.split('_')[0] if '_' in access_key else access_key[:8]
            mm_instance.user_id = user_id
            
            mm_instances[user_id] = mm_instance
            logging.info(f"✅ Loaded MM account {account_num}: {user_id} (Balance: ${mm_instance.balance:.2f})")
            
        except Exception as e:
            logging.warning(f"⚠️  Could not load account {account_num}: {str(e)}")
            continue
    
    return mm_instances

def run_stress_test(event_name, num_workers=10, environment='sandbox'):
    """
    Run the stress test with FAIRNESS enhancement
    :param event_name: Name of the event to target
    :param num_workers: Number of concurrent workers (default 10)
    :param environment: Environment to run on (sandbox/staging, default sandbox)
    
    FAIRNESS ENHANCEMENT:
    - Loads multiple MM accounts to simulate multiple users/SPs
    - For each 100 jobs, parallelizes requests by user ID
    - Tracks per-user metrics to validate fairness
    """
    global should_stop, wagers_placed_count, wagers_cancelled_count, target_wagers, user_metrics
    
    # Set environment
    import os
    os.environ['MM_ENVIRONMENT'] = environment
    # Reload config to pick up new environment
    import importlib
    from src import config
    importlib.reload(config)
    
    logging.info("🚀 STRESS TEST: 30K BET & CANCEL WAGERS (with FAIRNESS validation)")
    logging.info(f"🌍 Environment: {environment}")
    logging.info(f"🎯 Target Event: {event_name}")
    logging.info(f"🔥 Target Wagers: {target_wagers:,}")
    logging.info(f"⚡ Concurrent Workers: {num_workers}")
    
    # Load multiple MM accounts for fairness testing
    logging.info("📦 Loading multiple MM accounts for fairness testing...")
    mm_instances = load_multiple_mm_accounts(environment)
    
    if not mm_instances:
        logging.error("❌ No MM accounts could be loaded. Exiting.")
        return
    
    logging.info(f"✅ Loaded {len(mm_instances)} MM accounts for fairness testing")
    
    # Use first MM instance to find the event
    mm_instance = list(mm_instances.values())[0]
    
    # Find target event
    matching_events = mm_instance.find_event_by_name(event_name)
    if not matching_events:
        logging.error(f"❌ No events found matching '{event_name}'")
        return
    
    target_event = matching_events[0]
    logging.info(f"✅ Found event: {target_event['name']}")
    
    # Find a market with selections
    target_market = None
    target_selection = None
    for market in target_event.get('markets', []):
        if market.get('selections'):
            target_market = market
            target_selection = market['selections'][0]
            break
    
    if not target_market or not target_selection:
        logging.error(f"❌ No valid markets found for event")
        return
    
    logging.info(f"✅ Using market: {target_market['type']}, selection: {target_selection[0]['name']}")
    
    # Start progress monitor
    progress_thread = threading.Thread(target=progress_monitor, daemon=True)
    progress_thread.start()
    
    # Calculate wagers per worker
    wagers_per_worker = target_wagers // num_workers
    
    # Start workers with PARALLEL USER PLACEMENT
    start_time = time.time()
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = []
        for i in range(num_workers):
            future = executor.submit(
                stress_test_worker_parallel,  # Use parallel worker
                i+1, 
                mm_instances,  # Pass all MM instances
                target_event, 
                target_market, 
                target_selection, 
                wagers_per_worker
            )
            futures.append(future)
        
        # Wait for all workers to complete
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logging.error(f"Worker error: {str(e)}")
    
    elapsed = time.time() - start_time
    
    # Calculate fairness metrics
    with metrics_lock:
        fairness_report = []
        for user_id, metrics in user_metrics.items():
            avg_response_time = metrics['total_response_time'] / metrics['request_count'] if metrics['request_count'] > 0 else 0
            fairness_report.append({
                'user_id': user_id,
                'placed': metrics['placed'],
                'cancelled': metrics['cancelled'],
                'failed': metrics['failed'],
                'avg_response_time': avg_response_time,
                'request_count': metrics['request_count']
            })
        
        # Sort by user_id
        fairness_report.sort(key=lambda x: x['user_id'])
        
        # Calculate fairness ratio
        placed_counts = [r['placed'] for r in fairness_report]
        max_placed = max(placed_counts) if placed_counts else 0
        min_placed = min(placed_counts) if placed_counts else 0
        fairness_ratio = min_placed / max_placed if max_placed > 0 else 0
    
    # Final report with FAIRNESS METRICS
    logging.info("")
    logging.info("╔════════════════════════════════════════════════════════════╗")
    logging.info("║               🎉 STRESS TEST COMPLETED 🎉                  ║")
    logging.info("╠════════════════════════════════════════════════════════════╣")
    logging.info(f"║ ⏰ Duration:        {elapsed/60:.2f} minutes                    ║")
    logging.info(f"║ 🎯 Wagers Placed:   {wagers_placed_count:,}                          ║")
    logging.info(f"║ ✅ Wagers Cancelled: {wagers_cancelled_count:,}                          ║")
    logging.info(f"║ 📊 Placement Rate:  {wagers_placed_count/elapsed:.1f} wagers/sec           ║")
    logging.info(f"║ 🚀 Cancel Rate:     {wagers_cancelled_count/elapsed:.1f} cancels/sec          ║")
    logging.info("╠════════════════════════════════════════════════════════════╣")
    logging.info(f"║ ⚖️  FAIRNESS RATIO:  {fairness_ratio:.2%} (min/max placed)           ║")
    logging.info("╠════════════════════════════════════════════════════════════╣")
    
    # Per-user statistics
    for report in fairness_report:
        user_id_display = report['user_id'][:8]
        logging.info(f"║ 👤 {user_id_display}: {report['placed']:>6} placed | {report['cancelled']:>6} cancelled ║")
        logging.info(f"║      Avg RT: {report['avg_response_time']*1000:.0f}ms | Requests: {report['request_count']:>4}        ║")
    
    logging.info("╚════════════════════════════════════════════════════════════╝")
    
    # Print final balances
    logging.info("\n💰 Final Balances:")
    for user_id, mm_inst in mm_instances.items():
        mm_inst.get_balance()
        logging.info(f"   {user_id[:8]}: ${mm_inst.balance:.2f}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Stress test: 30K bet & cancel wagers on single event')
    parser.add_argument('--event', type=str, required=True,
                       help='Target event name (partial or exact match)')
    parser.add_argument('--workers', type=int, default=10,
                       help='Number of concurrent workers (default: 10)')
    parser.add_argument('--target', type=int, default=30000,
                       help='Target number of wagers (default: 30000)')
    parser.add_argument('--env', type=str, default='sandbox', choices=['sandbox', 'staging'],
                       help='Environment to run on (default: sandbox)')
    args = parser.parse_args()
    
    # Update target if specified
    target_wagers = args.target
    
    # Set up signal handler
    signal.signal(signal.SIGINT, signal_handler)
    
    try:
        run_stress_test(args.event, num_workers=args.workers, environment=args.env)
    except Exception as e:
        logging.error(f"❌ Stress test failed: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        should_stop = True
        logging.info("✅ Stress test stopped")
