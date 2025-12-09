#!/usr/bin/env python3
"""
Stress Test: 30K Bet & Cancel Wagers on Single Event
Target: Spawn 30,000 wagers (bet + cancel) on 1 event
Strategy: Aggressive batch placement + immediate cancellation
"""

import argparse
import signal
import sys
import time
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed

from src import mm_calls
from src.log import logging

# Global control
should_stop = False
wagers_placed_count = 0
wagers_cancelled_count = 0
target_wagers = 30000
wagers_lock = threading.Lock()

def signal_handler(sig, frame):
    """Handle Ctrl+C to gracefully stop"""
    global should_stop
    logging.info("\n🛑 Received stop signal. Stopping stress test...")
    should_stop = True
    sys.exit(0)

def progress_monitor():
    """Monitor and report progress"""
    global should_stop, wagers_placed_count, wagers_cancelled_count, target_wagers
    
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
            
            logging.info(f"📊 PROGRESS: {placed}/{target_wagers} placed ({placed/target_wagers*100:.1f}%) | "
                        f"{cancelled} cancelled | "
                        f"Rate: {placed_rate:.1f} bets/s, {cancelled_rate:.1f} cancels/s | "
                        f"ETA: {eta/60:.1f}m")

def place_single_wager(mm_instance, line_id, odds):
    """Place a single wager"""
    global should_stop, wagers_placed_count
    
    if should_stop:
        return None
        
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
        
        play_url = urljoin(mm_instance.base_url, config.URL['mm_place_wager'])
        response = requests.post(play_url, json=body, headers=mm_instance._MMInteractions__get_auth_header())
        
        if response.status_code == 200:
            response_data = response.json()
            wager_data = response_data.get('data', {})
            if 'wager' in wager_data and 'id' in wager_data['wager']:
                wager_id = wager_data['wager']['id']
                with wagers_lock:
                    wagers_placed_count += 1
                return {'external_id': external_id, 'wager_id': wager_id}
    except Exception as e:
        logging.error(f"Error placing single wager: {str(e)}")
    
    return None

def place_batch_wagers(mm_instance, line_id, odds, batch_size=20):
    """Place batch wagers (max 20 per API call)"""
    global should_stop, wagers_placed_count
    
    if should_stop:
        return []
    
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
        
        batch_url = urljoin(mm_instance.base_url, config.URL['mm_batch_place'])
        response = requests.post(batch_url, json={"data": batch_body}, 
                                headers=mm_instance._MMInteractions__get_auth_header())
        
        if response.status_code == 200:
            batch_response = response.json()
            succeed_wagers = batch_response.get('data', {}).get('succeed_wagers', [])
            with wagers_lock:
                wagers_placed_count += len(succeed_wagers)
            return [{'external_id': w['external_id'], 'wager_id': w['id']} for w in succeed_wagers]
    except Exception as e:
        logging.error(f"Error placing batch wagers: {str(e)}")
    
    return []

def cancel_wagers(mm_instance, wagers_to_cancel):
    """Cancel a batch of wagers"""
    global should_stop, wagers_cancelled_count
    
    if should_stop or not wagers_to_cancel:
        return
    
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
        elif response.status_code == 404:
            # Already cancelled is ok
            with wagers_lock:
                wagers_cancelled_count += len(batch)
    except Exception as e:
        logging.error(f"Error cancelling wagers: {str(e)}")

def stress_test_worker(worker_id, mm_instance, event, market, selection, num_wagers):
    """Worker thread to place and cancel wagers"""
    global should_stop, wagers_placed_count, target_wagers
    
    logging.info(f"🔥 Worker {worker_id}: Starting with target {num_wagers} wagers")
    
    line_id = selection[0]['line_id']
    odds = mm_instance._MMInteractions__get_random_odds()
    
    wagers_buffer = []
    placed_by_worker = 0
    
    while not should_stop and placed_by_worker < num_wagers:
        # Check global target
        with wagers_lock:
            if wagers_placed_count >= target_wagers:
                break
        
        # Batch placement (20 wagers per call)
        batch_wagers = place_batch_wagers(mm_instance, line_id, odds, batch_size=20)
        if batch_wagers:
            wagers_buffer.extend(batch_wagers)
            placed_by_worker += len(batch_wagers)
        
        # Cancel accumulated wagers when buffer reaches 40
        if len(wagers_buffer) >= 40:
            cancel_wagers(mm_instance, wagers_buffer[:20])
            wagers_buffer = wagers_buffer[20:]
        
        # Small delay to avoid overwhelming the API
        time.sleep(0.05)
    
    # Cancel remaining wagers
    while wagers_buffer and not should_stop:
        cancel_wagers(mm_instance, wagers_buffer[:20])
        wagers_buffer = wagers_buffer[20:]
        time.sleep(0.1)
    
    logging.info(f"✅ Worker {worker_id}: Completed {placed_by_worker} wagers")

def run_stress_test(event_name, num_workers=10):
    """
    Run the stress test
    :param event_name: Name of the event to target
    :param num_workers: Number of concurrent workers (default 10)
    """
    global should_stop, wagers_placed_count, wagers_cancelled_count, target_wagers
    
    logging.info("🚀 STRESS TEST: 30K BET & CANCEL WAGERS")
    logging.info(f"🎯 Target Event: {event_name}")
    logging.info(f"🔥 Target Wagers: {target_wagers:,}")
    logging.info(f"⚡ Concurrent Workers: {num_workers}")
    
    # Initialize MM instance
    mm_instance = mm_calls.MMInteractions()
    mm_instance.mm_login()
    mm_instance.get_balance()
    mm_instance.seeding()
    
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
    
    # Start workers
    start_time = time.time()
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = []
        for i in range(num_workers):
            future = executor.submit(
                stress_test_worker, 
                i+1, 
                mm_instance, 
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
    
    # Final report
    logging.info("")
    logging.info("╔════════════════════════════════════════════════════════════╗")
    logging.info("║               🎉 STRESS TEST COMPLETED 🎉                  ║")
    logging.info("╠════════════════════════════════════════════════════════════╣")
    logging.info(f"║ ⏰ Duration:        {elapsed/60:.2f} minutes                    ║")
    logging.info(f"║ 🎯 Wagers Placed:   {wagers_placed_count:,}                          ║")
    logging.info(f"║ ✅ Wagers Cancelled: {wagers_cancelled_count:,}                          ║")
    logging.info(f"║ 📊 Placement Rate:  {wagers_placed_count/elapsed:.1f} wagers/sec           ║")
    logging.info(f"║ 🚀 Cancel Rate:     {wagers_cancelled_count/elapsed:.1f} cancels/sec          ║")
    logging.info(f"║ 💰 Final Balance:   ${mm_instance.balance:.2f}                    ║")
    logging.info("╚════════════════════════════════════════════════════════════╝")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Stress test: 30K bet & cancel wagers on single event')
    parser.add_argument('--event', type=str, required=True,
                       help='Target event name (partial or exact match)')
    parser.add_argument('--workers', type=int, default=10,
                       help='Number of concurrent workers (default: 10)')
    parser.add_argument('--target', type=int, default=30000,
                       help='Target number of wagers (default: 30000)')
    args = parser.parse_args()
    
    # Update target if specified
    target_wagers = args.target
    
    # Set up signal handler
    signal.signal(signal.SIGINT, signal_handler)
    
    try:
        run_stress_test(args.event, num_workers=args.workers)
    except Exception as e:
        logging.error(f"❌ Stress test failed: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        should_stop = True
        logging.info("✅ Stress test stopped")
