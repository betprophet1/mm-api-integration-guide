#!/usr/bin/env python3
"""
Non-Deduce Only Test Script
Run tests with only non-deduce MM and Patron accounts for bug investigation
Event: 30024812
"""

import time
import json
import sys
import os
import threading
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from test_deduce_race_conditions import DeduceTestFramework, RaceConditionTest, Colors, get_odds_ladder, get_opposite_line_id
from src import config
from src.log import logging


def test_nondeduce_cancel_race(duration=30, cancel_rate=0.5, event_id=30024812):
    """
    Test Case: Cancel Race - NON-DEDUCE ONLY
    
    Scenario:
    - MM2 (non-deduce) and 2 Patron (non-deduce) accounts
    - MMs rapidly place and cancel bets
    - Patrons try to match before cancellation
    - Verify cancellation behavior without deduce accounts
    """
    print(f"\n{Colors.BOLD}{Colors.MAGENTA}{'='*70}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.MAGENTA}Test: Cancel Race - NON-DEDUCE ACCOUNTS ONLY{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.MAGENTA}Event ID: {event_id}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.MAGENTA}{'='*70}{Colors.RESET}\n")
    
    framework = DeduceTestFramework(environment=config.ENVIRONMENT)
    test = RaceConditionTest(framework)
    
    # Setup accounts - NON-DEDUCE ONLY
    print(f"{Colors.CYAN}📦 Setting up NON-DEDUCE accounts only...{Colors.RESET}\n")
    
    # MM2 - NON-deduce account
    mm2_creds = config.get_account_credentials(2, config.ENVIRONMENT)
    framework.login_account('mm2', mm2_creds, account_type='mm')
    print(f"{Colors.GREEN}✅ MM2 (NON-DEDUCE) logged in{Colors.RESET}")
    
    # Get initial balance for MM2
    mm2_initial_balance_data = framework.get_balance('mm2')
    mm2_initial_balance = mm2_initial_balance_data.get('balance', 0)
    
    print(f"{Colors.CYAN}MM2 Initial balance: ${mm2_initial_balance:,.2f}{Colors.RESET}\n")
    
    # Patron account - NON-DEDUCE matcher
    patron_config = config.load_user_config(f'user_info_patron_{config.ENVIRONMENT}.json')
    patron_nondeduce_creds = {'username': patron_config['email'], 'password': patron_config['password']}
    framework.login_account('patron_nondeduce', patron_nondeduce_creds, account_type='patron')
    print(f"{Colors.GREEN}✅ Patron non-deduce (matcher) logged in{Colors.RESET}\n")
    
    # Get market for the event
    print(f"{Colors.CYAN}🔍 Getting markets for event {event_id}...{Colors.RESET}")
    
    import requests
    from urllib.parse import urljoin
    
    multiple_markets_url = urljoin(framework.base_url, config.URL['mm_multiple_markets'])
    markets_response = requests.get(
        multiple_markets_url,
        params={'event_ids': str(event_id)},
        headers=framework.get_auth_header('mm2')
    )
    
    if markets_response.status_code != 200:
        print(f"{Colors.RED}❌ Failed to get markets for event {event_id}{Colors.RESET}")
        return
    
    markets_data = json.loads(markets_response.content).get('data', {})
    event_markets = markets_data.get(str(event_id), [])
    
    if not event_markets:
        print(f"{Colors.RED}❌ No markets available for event {event_id}{Colors.RESET}")
        return
    
    # Find first market with selections
    line_id = None
    event_name = "Unknown"
    for market in event_markets:
        if market.get('selections'):
            selections = market.get('selections', [])
            if selections and len(selections) > 0:
                try:
                    if isinstance(selections[0], list) and len(selections[0]) > 0:
                        line_id = selections[0][0].get('line_id')
                    elif isinstance(selections[0], dict):
                        line_id = selections[0].get('line_id')
                    
                    if line_id:
                        event_name = market.get('event_name', 'Unknown')
                        break
                except:
                    continue
    
    if not line_id:
        print(f"{Colors.RED}❌ Could not find valid line_id in event {event_id}{Colors.RESET}")
        return
    
    print(f"{Colors.GREEN}✅ Event: {event_name} (ID: {event_id}){Colors.RESET}")
    print(f"{Colors.CYAN}Market: {event_name} (line: {line_id[:16]}...){Colors.RESET}\n")
    
    # Fetch odds ladder
    print(f"{Colors.YELLOW}🎯 Fetching odds ladder...{Colors.RESET}")
    odds_ladder = get_odds_ladder(framework)
    print(f"{Colors.GREEN}✅ Loaded {len(odds_ladder)} odds values{Colors.RESET}")
    
    # Get opposite line_id for patrons
    print(f"{Colors.YELLOW}🔍 Finding opposite line_id for patrons...{Colors.RESET}")
    opposite_line_id = get_opposite_line_id(framework, 'mm2', event_id, line_id)
    
    if not opposite_line_id:
        print(f"{Colors.RED}❌ Could not find opposite line_id{Colors.RESET}")
        print(f"{Colors.YELLOW}   Continuing anyway - patrons will bet on same line{Colors.RESET}\n")
        opposite_line_id = line_id
    else:
        print(f"{Colors.GREEN}✅ Opposite line found (line: {opposite_line_id[:16]}...){Colors.RESET}\n")
    
    # Test execution
    print(f"{Colors.BOLD}🚀 Starting {duration}s place & cancel test (NON-DEDUCE ONLY)...{Colors.RESET}")
    print(f"{Colors.CYAN}   Cancel rate: {cancel_rate*100:.0f}% of wagers will be cancelled{Colors.RESET}")
    print(f"{Colors.CYAN}   MM2 bets on line: {line_id[:16]}...{Colors.RESET}")
    print(f"{Colors.CYAN}   Patrons match on: {opposite_line_id[:16]}...{Colors.RESET}\n")
    
    # Shared data structures
    mm_wagers = []
    cancelled_wagers = []
    matched_by_patrons = []
    errors = []
    lock = threading.Lock()
    
    # Shared queue for MM wagers
    mm_wager_queue = []
    queue_lock = threading.Lock()
    
    # Tracking for bet delay verification
    bet_placement_times = []
    bet_delays_lock = threading.Lock()
    
    def mm2_place_and_cancel_worker():
        """MM2 (non-deduce) worker: Place wagers and randomly cancel them"""
        count = 0
        cancelled = 0
        start_time = time.time()
        
        while time.time() - start_time < duration:
            try:
                # Pick random odds from ladder
                mm_odds = random.choice([o for o in odds_ladder if o > 0])
                
                # Track placement time
                placement_time = time.time()
                
                # Place wager
                result = framework.place_wager('mm2', line_id, mm_odds, 2.0)
                response_time = time.time()
                
                if result.get('success'):
                    count += 1
                    wager_data = result.get('data', {})
                    wager = wager_data.get('wager', {})
                    wager_id = wager.get('id') or wager.get('wager_id')
                    external_id = wager.get('external_id')
                    placed_odds = wager.get('odds', result.get('odds'))
                    
                    # Calculate processing delay
                    processing_delay = response_time - placement_time
                    
                    # Track for delay verification
                    with bet_delays_lock:
                        bet_placement_times.append({
                            'account': 'mm2',
                            'wager_id': wager_id,
                            'placement_time': placement_time,
                            'response_time': response_time,
                            'processing_delay': processing_delay
                        })
                    
                    wager_info = {
                        'account': 'mm2',
                        'wager_id': wager_id,
                        'external_id': external_id,
                        'placed_at': placement_time,
                        'odds': placed_odds,
                        'line_id': line_id,
                        'data': wager_data
                    }
                    
                    with lock:
                        mm_wagers.append(wager_info)
                    
                    # Add to queue for patrons
                    with queue_lock:
                        mm_wager_queue.append(wager_info)
                    
                    # Randomly cancel
                    if random.random() < cancel_rate and wager_id and external_id:
                        cancel_success = framework.cancel_wager('mm2', external_id, wager_id)
                        
                        if cancel_success:
                            cancelled += 1
                            with lock:
                                cancelled_wagers.append({
                                    'wager_id': wager_id,
                                    'external_id': external_id,
                                    'cancelled_at': time.time()
                                })
                        
                        if count % 20 == 0:
                            print(f"{Colors.MAGENTA}📊 MM2: {count} placed, {cancelled} cancelled{Colors.RESET}")
                    elif count % 20 == 0:
                        print(f"{Colors.YELLOW}📊 MM2: {count} placed{Colors.RESET}")
                else:
                    with lock:
                        errors.append({'account': 'mm2', 'error': result.get('error'), 'type': 'PLACE'})
            
            except Exception as e:
                with lock:
                    errors.append({'account': 'mm2', 'error': str(e), 'type': 'EXCEPTION'})
            
            time.sleep(random.uniform(0.01, 0.03))
        
        return count, cancelled
    
    def patron_matcher_worker(account_name: str):
        """Patron: Try to match MM wagers"""
        count = 0
        start_time = time.time()
        
        while time.time() - start_time < duration:
            try:
                mm_wager = None
                
                # Try to get an MM wager
                with queue_lock:
                    if mm_wager_queue:
                        mm_wager = mm_wager_queue.pop(0)
                
                if mm_wager:
                    # Match with opposite odds
                    opposite_odds = -mm_wager['odds']
                    
                    result = framework.place_wager(
                        account_name,
                        opposite_line_id,
                        opposite_odds,
                        2.0
                    )
                    
                    if result.get('success'):
                        count += 1
                        with lock:
                            matched_by_patrons.append({
                                'patron': account_name,
                                'mm_wager_id': mm_wager['wager_id'],
                                'matched_at': time.time()
                            })
                        
                        if count % 10 == 0:
                            print(f"{Colors.BLUE}📊 {account_name}: {count} matches{Colors.RESET}")
                    else:
                        with lock:
                            errors.append({'account': account_name, 'error': result.get('error'), 'type': 'MATCH'})
                else:
                    time.sleep(0.01)
                    
            except Exception as e:
                with lock:
                    errors.append({'account': account_name, 'error': str(e), 'type': 'EXCEPTION'})
        
        return count
    
    # Run workers
    with ThreadPoolExecutor(max_workers=2) as executor:
        mm2_future = executor.submit(mm2_place_and_cancel_worker)
        patron_future = executor.submit(patron_matcher_worker, 'patron_nondeduce')
        
        mm2_placed, mm2_cancelled = mm2_future.result()
        patron_matched = patron_future.result()
    
    print(f"\n{Colors.GREEN}✅ Test completed!{Colors.RESET}")
    print(f"   MM2: {mm2_placed} placed, {mm2_cancelled} cancelled")
    print(f"   Patron matches: {patron_matched}")
    print(f"   Errors: {len(errors)}\n")
    
    # Wait for system to process
    print(f"{Colors.CYAN}⏳ Waiting 5s for system to process...{Colors.RESET}\n")
    time.sleep(5)
    
    # Get final balance
    mm2_final_balance_data = framework.get_balance('mm2')
    mm2_final_balance = mm2_final_balance_data.get('balance', 0)
    
    # Analysis
    print(f"{Colors.BOLD}{'='*70}{Colors.RESET}")
    print(f"{Colors.BOLD}RESULTS - NON-DEDUCE ONLY{Colors.RESET}")
    print(f"{Colors.BOLD}{'='*70}{Colors.RESET}\n")
    
    print(f"{Colors.CYAN}MM2 (NON-DEDUCE):{Colors.RESET}")
    print(f"  Initial balance: ${mm2_initial_balance:,.2f}")
    print(f"  Final balance:   ${mm2_final_balance:,.2f}")
    print(f"  Change:          ${mm2_initial_balance - mm2_final_balance:,.2f}\n")
    
    # Bet delay verification
    if bet_placement_times:
        print(f"{Colors.BOLD}BET DELAY STATS:{Colors.RESET}")
        delays = [bt['processing_delay'] for bt in bet_placement_times]
        print(f"  Avg delay: {sum(delays)/len(delays):.3f}s")
        print(f"  Min delay: {min(delays):.3f}s")
        print(f"  Max delay: {max(delays):.3f}s\n")
    
    # Error summary
    if errors:
        print(f"{Colors.BOLD}ERRORS:{Colors.RESET}")
        error_types = {}
        for err in errors:
            key = f"{err['account']}-{err['type']}"
            error_types[key] = error_types.get(key, 0) + 1
        
        for key, count in error_types.items():
            print(f"  {key}: {count}")
        print()
    
    # Save report
    report = {
        'test': 'nondeduce_only_cancel_race',
        'event_id': event_id,
        'duration': duration,
        'cancel_rate': cancel_rate,
        'mm2_placed': mm2_placed,
        'mm2_cancelled': mm2_cancelled,
        'patron_matches': patron_matched,
        'mm_wagers': mm_wagers[:50],
        'cancelled_wagers': cancelled_wagers[:50],
        'errors': errors,
        'bet_delays': bet_placement_times[:20] if bet_placement_times else None,
        'balance': {
            'initial': mm2_initial_balance,
            'final': mm2_final_balance,
            'change': mm2_initial_balance - mm2_final_balance
        }
    }
    
    report_file = f"nondeduce_only_test_{event_id}_{int(time.time())}.json"
    with open(report_file, 'w') as f:
        json.dump(report, f, indent=2)
    
    print(f"{Colors.GREEN}📄 Report saved: {report_file}{Colors.RESET}\n")


if __name__ == '__main__':
    print(f"\n{Colors.BOLD}{Colors.CYAN}{'='*70}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.CYAN}NON-DEDUCE ONLY TEST SUITE{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.CYAN}Event: 30024812{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.CYAN}{'='*70}{Colors.RESET}\n")
    
    test_nondeduce_cancel_race(duration=30, cancel_rate=0.5, event_id=30024812)
    
    print(f"\n{Colors.BOLD}{Colors.GREEN}{'='*70}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.GREEN}TEST COMPLETED{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.GREEN}{'='*70}{Colors.RESET}\n")
