#!/usr/bin/env python3
"""
Backend Fairness Test - Continuous Mode

This script runs the backend fairness test in continuous mode with:
- Automatic token refresh when tokens expire
- Re-authentication if refresh fails
- Error recovery and retry logic
- Progress tracking and metrics
"""

import argparse
import time
import threading
import signal
import sys
from collections import defaultdict
import json

from src import mm_calls
from src.log import logging
from src import config

# Import functions from the original test
from test_backend_fairness import (
    user_metrics,
    metrics_lock,
    place_wager_with_tracking,
    analyze_fairness
)

# Global flag for graceful shutdown
shutdown_requested = False

def signal_handler(sig, frame):
    """Handle Ctrl+C gracefully"""
    global shutdown_requested
    logging.info("\n\n🛑 Shutdown requested... Finishing current batch and cleaning up...")
    shutdown_requested = True

signal.signal(signal.SIGINT, signal_handler)


def load_mm_accounts_with_retry(environment='sandbox', num_accounts=2, start_account=1, max_retries=3):
    """Load MM accounts with retry logic and token refresh support"""
    import os
    
    os.environ['MM_ENVIRONMENT'] = environment
    import importlib
    importlib.reload(config)
    
    mm_instances = {}
    
    for account_num in range(start_account, start_account + num_accounts):
        retries = 0
        while retries < max_retries:
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
                import base64
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
                        logging.info(f"🔑 Extracted UUID: {user_id}")
                    else:
                        raise Exception("Invalid token format")
                except Exception as e:
                    access_key = credentials['access_key']
                    user_id = access_key.split('_')[0] if '_' in access_key else access_key[:8]
                    logging.warning(f"⚠️  Could not extract UUID ({str(e)}), using: {user_id}")
                
                mm_instance.user_id = user_id
                mm_instances[user_id] = mm_instance
                logging.info(f"✅ Loaded account {account_num}: {user_id} (Balance: ${mm_instance.balance:.2f})")
                break
                
            except Exception as e:
                retries += 1
                if retries < max_retries:
                    logging.warning(f"⚠️  Failed to load account {account_num} (attempt {retries}/{max_retries}): {str(e)}")
                    time.sleep(2 ** retries)  # Exponential backoff
                else:
                    logging.error(f"❌ Could not load account {account_num} after {max_retries} attempts: {str(e)}")
                    continue
    
    return mm_instances


def place_wager_with_retry(mm_instance, user_id, line_id, odds, wager_num, max_retries=3):
    """Place wager with automatic token refresh and retry"""
    import requests
    from urllib.parse import urljoin
    import uuid
    
    for attempt in range(max_retries):
        try:
            # Ensure token is valid before placing wager
            if not mm_instance.ensure_valid_token():
                logging.warning(f"⚠️  Token validation failed for user {user_id[:8]}")
                time.sleep(1)
                continue
            
            external_id = str(uuid.uuid1())
            body = {
                'external_id': external_id,
                'line_id': line_id,
                'odds': odds,
                'stake': 2.0
            }
            
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
                    
                    return {
                        'user_id': user_id,
                        'wager_id': wager_id,
                        'external_id': external_id,
                        'status': wager_status,
                        'response_time': response_time,
                        'wager_num': wager_num
                    }
            elif response.status_code == 401:
                # Token expired, retry after refresh
                logging.warning(f"🔄 401 Unauthorized - forcing token refresh for user {user_id[:8]}")
                mm_instance.mm_login()
                time.sleep(0.5)
                continue
            else:
                with metrics_lock:
                    user_metrics[user_id]['failed'] += 1
                
                if user_metrics[user_id]['failed'] <= 3:
                    try:
                        error_body = response.json()
                        error_msg = error_body.get('message', 'No error message')
                        logging.error(f"❌ User {user_id[:8]} wager failed: {error_msg}")
                    except:
                        pass
                return None
                
        except Exception as e:
            if attempt < max_retries - 1:
                logging.warning(f"⚠️  Wager attempt {attempt + 1} failed, retrying: {str(e)}")
                time.sleep(0.5)
            else:
                with metrics_lock:
                    user_metrics[user_id]['failed'] += 1
    
    return None


def run_continuous_batch(mm_instances, markets, batch_size=1000, max_workers=50):
    """Run a single batch of wagers"""
    from concurrent.futures import ThreadPoolExecutor, as_completed
    
    user_ids = list(mm_instances.keys())
    wagers_per_user = batch_size // len(user_ids)
    
    batch_results = []
    batch_start = time.time()
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        wager_counter = 0
        
        for wager_num in range(1, wagers_per_user + 1):
            if shutdown_requested:
                break
            
            for user_id in user_ids:
                if shutdown_requested:
                    break
                    
                mm_instance = mm_instances[user_id]
                odds = mm_instance._MMInteractions__get_random_odds()
                market = markets[wager_counter % len(markets)]
                line_id = market['line_id'] if isinstance(market, dict) else market
                
                future = executor.submit(
                    place_wager_with_retry,
                    mm_instance,
                    user_id,
                    line_id,
                    odds,
                    wager_num
                )
                futures.append(future)
                wager_counter += 1
        
        for future in as_completed(futures):
            if shutdown_requested:
                break
            try:
                result = future.result()
                if result:
                    batch_results.append(result)
            except Exception as e:
                logging.error(f"Future error: {str(e)}")
    
    batch_elapsed = time.time() - batch_start
    return batch_results, batch_elapsed


def run_continuous_fairness_test(event_name, batch_size=1000, total_batches=None, 
                                 environment='sandbox', max_workers=50, 
                                 num_accounts=10, start_account=1):
    """Run continuous fairness test with automatic token refresh"""
    test_start_time = time.time()
    
    logging.info("=" * 70)
    logging.info("BACKEND FAIRNESS TEST - CONTINUOUS MODE")
    logging.info("=" * 70)
    logging.info(f"Environment: {environment}")
    logging.info(f"Target event: {event_name}")
    logging.info(f"Batch size: {batch_size}")
    logging.info(f"Total batches: {'Unlimited' if total_batches is None else total_batches}")
    logging.info(f"Max workers: {max_workers}")
    logging.info("=" * 70 + "\n")
    
    # Load accounts
    logging.info(f"📦 Loading MM accounts {start_account}-{start_account + num_accounts - 1}...")
    mm_instances = load_mm_accounts_with_retry(environment, num_accounts, start_account)
    
    if len(mm_instances) < 2:
        logging.error("❌ Need at least 2 MM accounts for fairness testing")
        return
    
    logging.info(f"✅ Loaded {len(mm_instances)} accounts\n")
    
    # Collect markets
    mm_instance = list(mm_instances.values())[0]
    matching_events = mm_instance.find_event_by_id_or_name(event_name)
    
    if not matching_events:
        logging.error(f"❌ No events found matching '{event_name}'")
        return
    
    events_to_use = matching_events[:5]
    available_markets = []
    
    for event in events_to_use:
        for market in event.get('markets', []):
            if market.get('selections'):
                for selection_group in market['selections']:
                    if isinstance(selection_group, list):
                        for selection in selection_group:
                            if selection.get('line_id'):
                                available_markets.append({
                                    'event_name': event.get('name', 'Unknown'),
                                    'market_type': market.get('type'),
                                    'line_id': selection['line_id'],
                                    'selection_name': selection.get('name', 'Unknown')
                                })
    
    if not available_markets:
        logging.error("❌ No valid markets found")
        return
    
    logging.info(f"✅ Collected {len(available_markets)} markets from {len(events_to_use)} events\n")
    
    # Run continuous batches
    batch_count = 0
    total_wagers_placed = 0
    last_report_time = time.time()
    report_interval = 30  # Report every 30 seconds
    
    try:
        while not shutdown_requested:
            if total_batches is not None and batch_count >= total_batches:
                break
            
            batch_count += 1
            logging.info(f"\n{'='*70}")
            logging.info(f"📊 BATCH #{batch_count}")
            logging.info(f"{'='*70}")
            
            batch_results, batch_elapsed = run_continuous_batch(
                mm_instances, available_markets, batch_size, max_workers
            )
            
            batch_placed = len(batch_results)
            total_wagers_placed += batch_placed
            
            logging.info(f"✅ Batch #{batch_count} completed:")
            logging.info(f"   Placed: {batch_placed}/{batch_size}")
            logging.info(f"   Duration: {batch_elapsed:.2f}s")
            logging.info(f"   Rate: {batch_placed/batch_elapsed:.1f} wagers/sec")
            logging.info(f"   Total wagers so far: {total_wagers_placed:,}")
            
            # Periodic report
            current_time = time.time()
            if current_time - last_report_time >= report_interval:
                logging.info(f"\n{'='*70}")
                logging.info("PROGRESS REPORT")
                logging.info(f"{'='*70}")
                
                total_placed = sum([user_metrics[uid]['placed'] for uid in mm_instances.keys()])
                total_failed = sum([user_metrics[uid]['failed'] for uid in mm_instances.keys()])
                elapsed = current_time - test_start_time
                
                logging.info(f"Total runtime: {elapsed/60:.1f} minutes")
                logging.info(f"Total batches: {batch_count}")
                logging.info(f"Total placed: {total_placed:,}")
                logging.info(f"Total failed: {total_failed:,}")
                logging.info(f"Overall rate: {total_placed/elapsed:.1f} wagers/sec")
                
                # Token refresh stats
                token_refreshes = sum([mm.session_stats['token_refreshes'] for mm in mm_instances.values()])
                re_auths = sum([mm.session_stats['re_authentications'] for mm in mm_instances.values()])
                logging.info(f"Token refreshes: {token_refreshes}")
                logging.info(f"Re-authentications: {re_auths}")
                
                # Quick fairness check
                placed_counts = [user_metrics[uid]['placed'] for uid in mm_instances.keys()]
                if placed_counts and max(placed_counts) > 0:
                    fairness = min(placed_counts) / max(placed_counts)
                    logging.info(f"Current fairness: {fairness:.2%}")
                
                last_report_time = current_time
            
            # Small delay between batches to avoid overwhelming the system
            if not shutdown_requested:
                time.sleep(1)
    
    except KeyboardInterrupt:
        logging.info("\n\n🛑 Interrupted by user")
    
    # Final report
    logging.info(f"\n\n{'='*70}")
    logging.info("FINAL RESULTS")
    logging.info(f"{'='*70}")
    
    elapsed = time.time() - test_start_time
    total_placed = sum([user_metrics[uid]['placed'] for uid in mm_instances.keys()])
    
    logging.info(f"Total runtime: {elapsed/60:.1f} minutes")
    logging.info(f"Total batches: {batch_count}")
    logging.info(f"Total placed: {total_placed:,}")
    logging.info(f"Overall rate: {total_placed/elapsed:.1f} wagers/sec")
    
    # Analyze fairness
    analyze_fairness(mm_instances)
    
    # Save results
    results = {
        'test_config': {
            'mode': 'continuous',
            'environment': environment,
            'event': event_name,
            'batch_size': batch_size,
            'total_batches': batch_count,
            'num_users': len(mm_instances),
            'duration_seconds': elapsed
        },
        'per_user_metrics': {}
    }
    
    for user_id in mm_instances.keys():
        results['per_user_metrics'][user_id] = dict(user_metrics[user_id])
    
    output_file = f"backend_fairness_continuous_{int(time.time())}.json"
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    
    logging.info(f"\n📄 Results saved to: {output_file}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Backend Fairness Test - Continuous Mode')
    parser.add_argument('--event', type=str, required=True, help='Event ID or name')
    parser.add_argument('--batch-size', type=int, default=1000, help='Wagers per batch (default: 1000)')
    parser.add_argument('--total-batches', type=int, default=None, help='Total batches to run (default: unlimited)')
    parser.add_argument('--env', type=str, default='sandbox', choices=['sandbox', 'staging'],
                       help='Environment (default: sandbox)')
    parser.add_argument('--workers', type=int, default=50, help='Max concurrent workers (default: 50)')
    parser.add_argument('--accounts', type=int, default=10, help='Number of accounts to use (default: 10)')
    parser.add_argument('--start-account', type=int, default=1, help='Starting account number (default: 1)')
    
    args = parser.parse_args()
    
    logging.info("\n" + "="*70)
    logging.info("BACKEND FAIRNESS TEST - CONTINUOUS MODE")
    logging.info("With automatic token refresh and re-authentication")
    logging.info("Press Ctrl+C to stop gracefully")
    logging.info("="*70 + "\n")
    
    run_continuous_fairness_test(
        args.event,
        batch_size=args.batch_size,
        total_batches=args.total_batches,
        environment=args.env,
        max_workers=args.workers,
        num_accounts=args.accounts,
        start_account=args.start_account
    )
