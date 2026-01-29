#!/usr/bin/env python3
"""
DEDUCE Race Condition Test Suite

Comprehensive tests for race conditions and edge cases in the DEDUCE feature:
- Account type matching combinations (deduce vs non-deduce)
- Simultaneous matching scenarios
- Timing-critical race conditions
- Balance consistency verification
- Multi-account concurrent operations

Test Categories:
1. Two-Account Races: Direct deduce vs non-deduce matching
2. Four-Account Races: Complex multi-party matching scenarios
3. Timing Tests: Varying delays between placement and matching
4. Partial Match Tests: Partial fills and cancellations
5. High-Volume Stress: Rapid concurrent operations
"""

import time
import json
import sys
import os
import threading
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Dict, List, Tuple, Optional

sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from deduce_tests import DeduceTestFramework, Colors
from src import config
from src.log import logging


# Global cache for odds ladder
_ODDS_LADDER_CACHE = {'odds': [], 'timestamp': 0}

def get_odds_ladder(framework):
    """Fetch odds ladder from API (cached for 5 minutes)"""
    import requests
    from urllib.parse import urljoin
    
    # Use cache if fresh (< 5 minutes old)
    if _ODDS_LADDER_CACHE['odds'] and (time.time() - _ODDS_LADDER_CACHE['timestamp']) < 300:
        return _ODDS_LADDER_CACHE['odds']
    
    try:
        url = urljoin(framework.base_url, 'trade/public/api/v1/markets/moneyline/odds-ladder')
        headers = {
            '__source': 'web',
            'accept': 'application/json'
        }
        
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            
            # Handle both response formats: {'data': {'odds': [...]}} or {'data': [...]}
            if isinstance(data, dict):
                data_content = data.get('data', [])
                if isinstance(data_content, dict):
                    odds_list = data_content.get('odds', [])
                else:
                    odds_list = data_content
            else:
                odds_list = data
            
            # Filter to reasonable odds range for testing (between -500 and +500)
            odds_list = [o for o in odds_list if -500 <= o <= 500 and o != 0]
            
            # Update cache
            _ODDS_LADDER_CACHE['odds'] = odds_list
            _ODDS_LADDER_CACHE['timestamp'] = time.time()
            
            logging.info(f"Fetched odds ladder: {len(odds_list)} odds values")
            return odds_list
        else:
            logging.warning(f"Failed to fetch odds ladder: {response.status_code}")
            # Fallback to common odds
            return [-500, -400, -300, -200, -150, -130, 110, 130, 150, 200, 300, 400, 500]
    except Exception as e:
        logging.error(f"Error fetching odds ladder: {e}")
        return [-500, -400, -300, -200, -150, -130, 110, 130, 150, 200, 300, 400, 500]

def get_market_for_event(framework, account_name, event_id):
    """Get market info for a specific event ID"""
    import requests
    from urllib.parse import urljoin
    
    try:
        multiple_markets_url = urljoin(framework.base_url, config.URL['mm_multiple_markets'])
        markets_response = requests.get(
            multiple_markets_url,
            params={'event_ids': str(event_id)},
            headers=framework.get_auth_header(account_name)
        )
        
        if markets_response.status_code != 200:
            logging.error(f"Failed to get markets for event {event_id}: {markets_response.status_code}")
            return None
        
        markets_data = json.loads(markets_response.content).get('data', {})
        event_markets = markets_data.get(str(event_id), [])
        
        if not event_markets:
            logging.error(f"No markets available for event {event_id}")
            return None
        
        # Find first market with selections
        for market in event_markets:
            if market.get('selections'):
                selections = market.get('selections', [])
                if selections and len(selections) > 0:
                    try:
                        line_id = None
                        if isinstance(selections[0], list) and len(selections[0]) > 0:
                            line_id = selections[0][0].get('line_id')
                        elif isinstance(selections[0], dict):
                            line_id = selections[0].get('line_id')
                        
                        if line_id:
                            event_name = market.get('event_name', 'Unknown')
                            return {'line_id': line_id, 'event': {'name': event_name, 'event_id': event_id}}
                    except:
                        continue
        
        logging.error(f"Could not find valid line_id in event {event_id}")
        return None
    except Exception as e:
        logging.error(f"Error getting market for event {event_id}: {e}")
        return None

def get_opposite_line_id(framework, account_name, event_id, current_line_id):
    """Get the opposite line_id for matching (e.g., if MM bets on Team A, get Team B's line_id)"""
    import requests
    from urllib.parse import urljoin
    
    try:
        # Get markets for the event
        multiple_markets_url = urljoin(framework.base_url, config.URL['mm_multiple_markets'])
        markets_response = requests.get(
            multiple_markets_url,
            params={'event_ids': str(event_id)},
            headers=framework.get_auth_header(account_name)
        )
        
        if markets_response.status_code != 200:
            logging.error(f"Failed to get markets: {markets_response.status_code}")
            return None
        
        markets_data = json.loads(markets_response.content).get('data', {})
        event_markets = markets_data.get(str(event_id), [])
        
        logging.debug(f"Found {len(event_markets)} markets for event {event_id}")
        
        # Find the market containing our line_id and get the opposite
        for market in event_markets:
            selections = market.get('selections', [])
            market_name = market.get('name', 'Unknown')
            
            # Flatten selections (handle nested structure)
            flat_selections = []
            for sel in selections:
                if isinstance(sel, list):
                    flat_selections.extend(sel)
                elif isinstance(sel, dict):
                    flat_selections.append(sel)
            
            # Check if our line_id is in this market
            line_ids = [s.get('line_id') for s in flat_selections if s.get('line_id')]
            
            logging.debug(f"Market '{market_name}': {len(line_ids)} line_ids")
            
            if current_line_id in line_ids:
                logging.info(f"Found current line_id in market '{market_name}' with {len(line_ids)} total lines")
                if len(line_ids) >= 2:
                    # Return the other line_id (the opposite side)
                    opposite_lines = [lid for lid in line_ids if lid != current_line_id]
                    if opposite_lines:
                        logging.info(f"Found opposite line_id: {opposite_lines[0][:16]}...")
                        return opposite_lines[0]
                else:
                    logging.warning(f"Market has only {len(line_ids)} line(s) - cannot find opposite")
        
        logging.warning(f"Line_id {current_line_id[:16]}... not found in any market for event {event_id}")
        return None
    except Exception as e:
        logging.error(f"Error getting opposite line_id: {e}")
        import traceback
        logging.error(traceback.format_exc())
        return None

class RaceConditionTest:
    """Framework for testing race conditions in deduce feature"""
    
    def __init__(self, framework: DeduceTestFramework):
        self.framework = framework
        self.test_results = []
        self.matched_bets = []
        self.api_errors = []
        self.lock = threading.Lock()
        self.balance_snapshots = {}
        
    def snapshot_balances(self, accounts: List[str], label: str):
        """Take balance snapshots for all accounts"""
        snapshot = {
            'timestamp': datetime.now().isoformat(),
            'label': label,
            'balances': {}
        }
        
        for account_name in accounts:
            try:
                balance_data = self.framework.get_balance(account_name)
                snapshot['balances'][account_name] = balance_data.get('balance', 0)
            except Exception as e:
                logging.error(f"Failed to get balance for {account_name}: {e}")
                snapshot['balances'][account_name] = None
        
        with self.lock:
            self.balance_snapshots[label] = snapshot
        
        return snapshot
    
    def verify_balance_consistency(self, account_name: str, initial: float, 
                                   expected_change: float, behavior: str) -> dict:
        """Verify balance changed as expected"""
        try:
            balance_data = self.framework.get_balance(account_name)
            current = balance_data.get('balance', 0)
            actual_change = initial - current
            
            result = {
                'account': account_name,
                'behavior': behavior,
                'initial': initial,
                'current': current,
                'expected_change': expected_change,
                'actual_change': actual_change,
                'valid': abs(actual_change - expected_change) < 0.01
            }
            
            return result
        except Exception as e:
            return {
                'account': account_name,
                'error': str(e),
                'valid': False
            }


def test_deduce_sp_vs_nondeduce_sp(duration=30, event_id=None):
    """
    Test Case 1A: Deduce SP vs Non-Deduce SP
    
    Scenario:
    - MM1 (deduce) and MM2 (non-deduce) place opposite bets
    - Both provide liquidity simultaneously
    - Verify MM1 balance unchanged until match
    - Verify MM2 balance deducted immediately
    """
    print(f"\n{Colors.BOLD}{Colors.MAGENTA}{'='*70}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.MAGENTA}Test 1A: Deduce SP vs Non-Deduce SP Race{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.MAGENTA}{'='*70}{Colors.RESET}\n")
    
    framework = DeduceTestFramework(environment=config.ENVIRONMENT)
    test = RaceConditionTest(framework)
    
    # Setup accounts
    print(f"{Colors.CYAN}📦 Setting up accounts...{Colors.RESET}\n")
    
    mm1_creds = config.get_account_credentials(1, config.ENVIRONMENT)
    framework.login_account('mm1', mm1_creds, account_type='mm')
    
    mm2_creds = config.get_account_credentials(2, config.ENVIRONMENT)
    framework.login_account('mm2', mm2_creds, account_type='mm')
    
    # Get initial balances
    initial_snapshot = test.snapshot_balances(['mm1', 'mm2'], 'initial')
    print(f"{Colors.GREEN}✅ MM1 (deduce): ${initial_snapshot['balances']['mm1']:,.2f}{Colors.RESET}")
    print(f"{Colors.GREEN}✅ MM2 (normal): ${initial_snapshot['balances']['mm2']:,.2f}{Colors.RESET}\n")
    
    # Get market
    print(f"{Colors.CYAN}🔍 Finding market...{Colors.RESET}")
    if event_id:
        market_info = get_market_for_event(framework, 'mm1', event_id)
    else:
        market_info = framework.get_available_market('mm1')
    
    if not market_info:
        print(f"{Colors.RED}❌ No markets available{Colors.RESET}")
        return
    
    event_name = market_info.get('event', {}).get('name', 'Unknown')
    print(f"{Colors.GREEN}✅ Market: {event_name}{Colors.RESET}")
    
    # Fetch odds ladder
    print(f"{Colors.YELLOW}🎯 Fetching odds ladder...{Colors.RESET}")
    odds_ladder = get_odds_ladder(framework)
    positive_odds = [o for o in odds_ladder if o > 0]
    negative_odds = [o for o in odds_ladder if o < 0]
    print(f"{Colors.GREEN}✅ Loaded {len(odds_ladder)} odds values{Colors.RESET}\n")
    
    # Test execution
    print(f"{Colors.BOLD}🚀 Starting {duration}s race test...{Colors.RESET}\n")
    
    matched_bets = []
    errors = []
    lock = threading.Lock()
    
    def mm1_worker():
        """MM1 (deduce) worker - places bets continuously"""
        count = 0
        start_time = time.time()
        
        while time.time() - start_time < duration:
            try:
                odds = random.choice(positive_odds)
                result = framework.place_wager('mm1', market_info['line_id'], odds, 1.0)
                if result.get('success'):
                    count += 1
                    if count % 10 == 0:
                        print(f"{Colors.CYAN}📊 MM1: {count} bets placed{Colors.RESET}")
                else:
                    with lock:
                        errors.append({'account': 'mm1', 'error': result.get('error')})
            except Exception as e:
                with lock:
                    errors.append({'account': 'mm1', 'error': str(e)})
            
            time.sleep(0.075)  # 4x more aggressive
        
        return count
    
    def mm2_worker():
        """MM2 (non-deduce) worker - places opposite bets"""
        count = 0
        start_time = time.time()
        
        while time.time() - start_time < duration:
            try:
                odds = random.choice(negative_odds)
                result = framework.place_wager('mm2', market_info['line_id'], odds, 1.0)
                if result.get('success'):
                    count += 1
                    if count % 10 == 0:
                        print(f"{Colors.BLUE}📊 MM2: {count} bets placed{Colors.RESET}")
                else:
                    with lock:
                        errors.append({'account': 'mm2', 'error': result.get('error')})
            except Exception as e:
                with lock:
                    errors.append({'account': 'mm2', 'error': str(e)})
            
            time.sleep(0.075)  # 4x more aggressive
        
        return count
    
    # Run concurrent workers
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(mm1_worker),
            executor.submit(mm2_worker)
        ]
        
        results = [f.result() for f in as_completed(futures)]
    
    # Wait for matches to settle
    print(f"\n{Colors.CYAN}⏳ Waiting for matches to settle...{Colors.RESET}")
    time.sleep(5)
    
    # Get final balances
    final_snapshot = test.snapshot_balances(['mm1', 'mm2'], 'final')
    
    # Analysis
    print(f"\n{Colors.BOLD}{'='*70}{Colors.RESET}")
    print(f"{Colors.BOLD}RESULTS{Colors.RESET}")
    print(f"{Colors.BOLD}{'='*70}{Colors.RESET}\n")
    
    mm1_change = initial_snapshot['balances']['mm1'] - final_snapshot['balances']['mm1']
    mm2_change = initial_snapshot['balances']['mm2'] - final_snapshot['balances']['mm2']
    
    print(f"{Colors.BOLD}MM1 (DEDUCE):{Colors.RESET}")
    print(f"  Initial: ${initial_snapshot['balances']['mm1']:,.2f}")
    print(f"  Final:   ${final_snapshot['balances']['mm1']:,.2f}")
    print(f"  Change:  ${mm1_change:,.2f}\n")
    
    print(f"{Colors.BOLD}MM2 (NORMAL):{Colors.RESET}")
    print(f"  Initial: ${initial_snapshot['balances']['mm2']:,.2f}")
    print(f"  Final:   ${final_snapshot['balances']['mm2']:,.2f}")
    print(f"  Change:  ${mm2_change:,.2f}\n")
    
    print(f"{Colors.BOLD}Errors: {len(errors)}{Colors.RESET}\n")
    
    # Save report
    report = {
        'test': 'deduce_sp_vs_nondeduce_sp',
        'duration': duration,
        'snapshots': test.balance_snapshots,
        'errors': errors
    }
    
    report_file = f"race_test_1a_{int(time.time())}.json"
    with open(report_file, 'w') as f:
        json.dump(report, f, indent=2)
    
    print(f"{Colors.GREEN}📄 Report saved: {report_file}{Colors.RESET}\n")


def test_four_way_mexican_standoff(duration=30, event_id=None):
    """
    Test Case: 4-Way Mexican Standoff
    
    Scenario:
    - 4 accounts (2 deduce + 2 non-deduce) all placing bets simultaneously
    - Mixed SP and Patron accounts
    - Both back and lay sides
    - Verify correct matching priority and balance deductions
    
    Accounts:
    - MM1 (deduce SP) - back side
    - MM2 (non-deduce SP) - lay side
    - Patron deduce - back side
    - Patron non-deduce - lay side
    """
    print(f"\n{Colors.BOLD}{Colors.MAGENTA}{'='*70}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.MAGENTA}Test: 4-Way Mexican Standoff{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.MAGENTA}Complex Multi-Party Race Condition{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.MAGENTA}{'='*70}{Colors.RESET}\n")
    
    framework = DeduceTestFramework(environment=config.ENVIRONMENT)
    test = RaceConditionTest(framework)
    
    # Setup accounts
    print(f"{Colors.CYAN}📦 Setting up 4 accounts...{Colors.RESET}\n")
    
    accounts = {}
    
    # MM1 (deduce)
    mm1_creds = config.get_account_credentials(1, config.ENVIRONMENT)
    framework.login_account('mm1', mm1_creds, account_type='mm')
    accounts['mm1'] = {'type': 'sp', 'behavior': 'deduce', 'side': 'back', 'odds': 150}
    
    # MM2 (non-deduce)
    mm2_creds = config.get_account_credentials(2, config.ENVIRONMENT)
    framework.login_account('mm2', mm2_creds, account_type='mm')
    accounts['mm2'] = {'type': 'sp', 'behavior': 'normal', 'side': 'lay', 'odds': -150}
    
    # Patron deduce
    patron_deduce_creds = {'email': 'deduct.sanbox.test1@yopmail.com', 'password': 'Matkhau1$'}
    framework.login_account('patron_deduce', patron_deduce_creds, account_type='patron')
    accounts['patron_deduce'] = {'type': 'patron', 'behavior': 'deduce', 'side': 'back', 'odds': 150}
    
    # Patron non-deduce
    patron_config = config.load_user_config(f'user_info_patron_{config.ENVIRONMENT}.json')
    patron_nondeduce_creds = {'username': patron_config['email'], 'password': patron_config['password']}
    framework.login_account('patron_nondeduce', patron_nondeduce_creds, account_type='patron')
    accounts['patron_nondeduce'] = {'type': 'patron', 'behavior': 'normal', 'side': 'lay', 'odds': -150}
    
    # Get initial balances
    account_names = list(accounts.keys())
    initial_snapshot = test.snapshot_balances(account_names, 'initial')
    
    # Check for balance retrieval failures
    failed_accounts = [acc for acc, bal in initial_snapshot['balances'].items() if bal is None]
    if failed_accounts:
        print(f"{Colors.RED}❌ Failed to retrieve balance for: {', '.join(failed_accounts)}{Colors.RESET}")
        print(f"{Colors.YELLOW}⚠️  Retrying balance retrieval...{Colors.RESET}\n")
        time.sleep(2)
        
        # Retry failed accounts
        for acc in failed_accounts:
            try:
                balance_data = framework.get_balance(acc)
                initial_snapshot['balances'][acc] = balance_data.get('balance', 0)
            except Exception as e:
                print(f"{Colors.RED}❌ Still failed for {acc}: {e}{Colors.RESET}")
                print(f"{Colors.RED}   Aborting test due to connection issue{Colors.RESET}\n")
                return
    
    for acc_name in account_names:
        acc_info = accounts[acc_name]
        balance = initial_snapshot['balances'][acc_name]
        if balance is not None:
            behavior_str = f"{acc_info['behavior']} {acc_info['type']}"
            print(f"{Colors.GREEN}✅ {acc_name} ({behavior_str}): ${balance:,.2f}{Colors.RESET}")
    print()
    
    # Get market
    print(f"{Colors.CYAN}🔍 Finding market...{Colors.RESET}")
    if event_id:
        market_info = get_market_for_event(framework, 'mm1', event_id)
    else:
        market_info = framework.get_available_market('mm1')
    
    if not market_info:
        print(f"{Colors.RED}❌ No markets available{Colors.RESET}")
        return
    
    event_name = market_info.get('event', {}).get('name', 'Unknown')
    line_id = market_info['line_id']
    print(f"{Colors.GREEN}✅ Market: {event_name}{Colors.RESET}")
    
    # Fetch odds ladder
    print(f"{Colors.YELLOW}🎯 Fetching odds ladder...{Colors.RESET}")
    odds_ladder = get_odds_ladder(framework)
    print(f"{Colors.GREEN}✅ Loaded {len(odds_ladder)} odds values{Colors.RESET}\n")
    
    # Test execution
    print(f"{Colors.BOLD}🚀 Starting {duration}s 4-way race...{Colors.RESET}\n")
    
    errors = []
    wager_counts = {acc: 0 for acc in account_names}
    lock = threading.Lock()
    
    def account_worker(account_name: str, account_info: dict):
        """Generic worker for any account"""
        count = 0
        start_time = time.time()
        
        # Get odds pool for this side
        if account_info['side'] == 'back':
            odds_pool = [o for o in odds_ladder if o > 0]
        else:  # lay side
            odds_pool = [o for o in odds_ladder if o < 0]
        
        while time.time() - start_time < duration:
            try:
                # Pick random odds from pool
                odds = random.choice(odds_pool)
                result = framework.place_wager(account_name, line_id, odds, 1.0)
                if result.get('success'):
                    count += 1
                    with lock:
                        wager_counts[account_name] = count
                    if count % 10 == 0:
                        color = Colors.CYAN if account_info['behavior'] == 'deduce' else Colors.BLUE
                        print(f"{color}📊 {account_name}: {count} bets{Colors.RESET}")
                else:
                    with lock:
                        errors.append({'account': account_name, 'error': result.get('error')})
            except Exception as e:
                with lock:
                    errors.append({'account': account_name, 'error': str(e)})
            
            # Add small random jitter to create race conditions (4x more aggressive)
            time.sleep(0.05 + random.uniform(0, 0.025))
        
        return count
    
    # Run all 4 workers concurrently
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = []
        for acc_name, acc_info in accounts.items():
            future = executor.submit(account_worker, acc_name, acc_info)
            futures.append((acc_name, future))
        
        # Wait for all to complete
        for acc_name, future in futures:
            try:
                result = future.result()
                print(f"{Colors.GREEN}✅ {acc_name}: Completed {result} wagers{Colors.RESET}")
            except Exception as e:
                print(f"{Colors.RED}❌ {acc_name}: Error - {e}{Colors.RESET}")
    
    # Wait for matches to settle
    print(f"\n{Colors.CYAN}⏳ Waiting for matches to settle...{Colors.RESET}")
    time.sleep(5)
    
    # Get final balances
    final_snapshot = test.snapshot_balances(account_names, 'final')
    
    # Analysis
    print(f"\n{Colors.BOLD}{'='*70}{Colors.RESET}")
    print(f"{Colors.BOLD}FINAL RESULTS{Colors.RESET}")
    print(f"{Colors.BOLD}{'='*70}{Colors.RESET}\n")
    
    for acc_name in account_names:
        acc_info = accounts[acc_name]
        initial = initial_snapshot['balances'][acc_name]
        final = final_snapshot['balances'][acc_name]
        change = initial - final
        
        behavior_str = f"{acc_info['behavior']} {acc_info['type']} ({acc_info['side']})"
        color = Colors.CYAN if acc_info['behavior'] == 'deduce' else Colors.BLUE
        
        print(f"{color}{Colors.BOLD}{acc_name.upper()} - {behavior_str}:{Colors.RESET}")
        print(f"  Wagers Placed: {wager_counts[acc_name]}")
        print(f"  Initial:       ${initial:,.2f}")
        print(f"  Final:         ${final:,.2f}")
        print(f"  Change:        ${change:,.2f}\n")
    
    print(f"{Colors.BOLD}Errors: {len(errors)}{Colors.RESET}")
    if errors:
        error_summary = {}
        for err in errors:
            key = err['account']
            error_summary[key] = error_summary.get(key, 0) + 1
        
        for acc, count in error_summary.items():
            print(f"  {acc}: {count}")
    print()
    
    # Verify deduce accounts had no immediate deduction
    print(f"{Colors.BOLD}DEDUCE VERIFICATION:{Colors.RESET}")
    deduce_accounts = [acc for acc, info in accounts.items() if info['behavior'] == 'deduce']
    
    for acc in deduce_accounts:
        initial = initial_snapshot['balances'][acc]
        final = final_snapshot['balances'][acc]
        change = initial - final
        
        # Get matched bets to verify deduction = matched amount
        matched_bets = framework.get_matched_bets(acc, limit=100)
        matched_total = sum(bet.get('stake', 0) for bet in matched_bets)
        
        matches = abs(change - matched_total) < 0.01
        status = f"{Colors.GREEN}✅ PASS{Colors.RESET}" if matches else f"{Colors.RED}❌ FAIL{Colors.RESET}"
        
        print(f"{status} - {acc}:")
        print(f"  Balance change: ${change:.2f}")
        print(f"  Matched total:  ${matched_total:.2f}")
        print(f"  Matched bets:   {len(matched_bets)}\n")
    
    # Save report
    report = {
        'test': 'four_way_mexican_standoff',
        'duration': duration,
        'accounts': accounts,
        'wager_counts': wager_counts,
        'snapshots': test.balance_snapshots,
        'errors': errors
    }
    
    report_file = f"race_test_4way_{int(time.time())}.json"
    with open(report_file, 'w') as f:
        json.dump(report, f, indent=2)
    
    print(f"{Colors.GREEN}📄 Report saved: {report_file}{Colors.RESET}\n")


def test_rapid_fire_race(duration=20, bets_per_second=40, event_id=None):
    """
    Test Case: Rapid Fire Race Condition
    
    Scenario:
    - High-frequency betting from deduce account
    - Matching account tries to take all bets
    - Test system's ability to handle rapid concurrent operations
    - Verify no double-deductions or missed deductions
    """
    print(f"\n{Colors.BOLD}{Colors.MAGENTA}{'='*70}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.MAGENTA}Test: Rapid Fire Race Condition{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.MAGENTA}High-Frequency Concurrent Operations{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.MAGENTA}{'='*70}{Colors.RESET}\n")
    
    framework = DeduceTestFramework(environment=config.ENVIRONMENT)
    test = RaceConditionTest(framework)
    
    # Setup accounts
    print(f"{Colors.CYAN}📦 Setting up accounts...{Colors.RESET}\n")
    
    mm1_creds = config.get_account_credentials(1, config.ENVIRONMENT)
    framework.login_account('mm1', mm1_creds, account_type='mm')
    
    mm2_creds = config.get_account_credentials(2, config.ENVIRONMENT)
    framework.login_account('mm2', mm2_creds, account_type='mm')
    
    # Get initial balances
    initial_snapshot = test.snapshot_balances(['mm1', 'mm2'], 'initial')
    print(f"{Colors.GREEN}✅ MM1 (deduce): ${initial_snapshot['balances']['mm1']:,.2f}{Colors.RESET}")
    print(f"{Colors.GREEN}✅ MM2 (taker): ${initial_snapshot['balances']['mm2']:,.2f}{Colors.RESET}\n")
    
    # Get market
    print(f"{Colors.CYAN}🔍 Finding market...{Colors.RESET}")
    if event_id:
        market_info = get_market_for_event(framework, 'mm1', event_id)
    else:
        market_info = framework.get_available_market('mm1')
    
    if not market_info:
        print(f"{Colors.RED}❌ No markets available{Colors.RESET}")
        return
    
    line_id = market_info['line_id']
    print(f"{Colors.GREEN}✅ Market found{Colors.RESET}")
    
    # Fetch odds ladder
    print(f"{Colors.YELLOW}🎯 Fetching odds ladder...{Colors.RESET}")
    odds_ladder = get_odds_ladder(framework)
    positive_odds = [o for o in odds_ladder if o > 0]
    negative_odds = [o for o in odds_ladder if o < 0]
    print(f"{Colors.GREEN}✅ Loaded {len(odds_ladder)} odds values{Colors.RESET}\n")
    
    # Test execution
    delay = 1.0 / bets_per_second
    print(f"{Colors.BOLD}🚀 Starting rapid fire: {bets_per_second} bets/sec for {duration}s{Colors.RESET}\n")
    
    errors = []
    lock = threading.Lock()
    mm1_count = 0
    mm2_count = 0
    
    def rapid_fire_maker():
        """MM1 places bets as fast as possible"""
        nonlocal mm1_count
        start_time = time.time()
        
        while time.time() - start_time < duration:
            try:
                odds = random.choice(positive_odds)
                result = framework.place_wager('mm1', line_id, odds, 2.0)  # $2 to meet minimum
                if result.get('success'):
                    mm1_count += 1
                else:
                    with lock:
                        errors.append({'account': 'mm1', 'error': result.get('error')})
            except Exception as e:
                with lock:
                    errors.append({'account': 'mm1', 'error': str(e)})
            
            time.sleep(delay)
        
        return mm1_count
    
    def rapid_fire_taker():
        """MM2 tries to take all MM1's bets"""
        nonlocal mm2_count
        start_time = time.time()
        
        while time.time() - start_time < duration:
            try:
                odds = random.choice(negative_odds)
                result = framework.place_wager('mm2', line_id, odds, 2.0)  # $2 to meet minimum
                if result.get('success'):
                    mm2_count += 1
                else:
                    with lock:
                        errors.append({'account': 'mm2', 'error': result.get('error')})
            except Exception as e:
                with lock:
                    errors.append({'account': 'mm2', 'error': str(e)})
            
            time.sleep(delay)
        
        return mm2_count
    
    # Run concurrent workers
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(rapid_fire_maker),
            executor.submit(rapid_fire_taker)
        ]
        
        results = [f.result() for f in as_completed(futures)]
    
    print(f"\n{Colors.GREEN}✅ MM1 placed: {mm1_count} bets{Colors.RESET}")
    print(f"{Colors.GREEN}✅ MM2 placed: {mm2_count} bets{Colors.RESET}\n")
    
    # Wait for settling
    print(f"{Colors.CYAN}⏳ Waiting for matches to settle...{Colors.RESET}")
    time.sleep(5)
    
    # Get final balances and matched bets
    final_snapshot = test.snapshot_balances(['mm1', 'mm2'], 'final')
    
    mm1_matched = framework.get_matched_bets('mm1', limit=500)
    mm1_matched_total = sum(bet.get('stake', 0) for bet in mm1_matched)
    
    # Analysis
    print(f"\n{Colors.BOLD}{'='*70}{Colors.RESET}")
    print(f"{Colors.BOLD}RESULTS{Colors.RESET}")
    print(f"{Colors.BOLD}{'='*70}{Colors.RESET}\n")
    
    mm1_initial = initial_snapshot['balances']['mm1']
    mm1_final = final_snapshot['balances']['mm1']
    mm1_change = mm1_initial - mm1_final
    
    mm2_initial = initial_snapshot['balances']['mm2']
    mm2_final = final_snapshot['balances']['mm2']
    mm2_change = mm2_initial - mm2_final
    
    print(f"{Colors.BOLD}MM1 (DEDUCE MAKER):{Colors.RESET}")
    print(f"  Bets Placed:    {mm1_count}")
    print(f"  Bets Matched:   {len(mm1_matched)}")
    print(f"  Matched Total:  ${mm1_matched_total:.2f}")
    print(f"  Balance Change: ${mm1_change:.2f}")
    
    # Verify balance change = matched total
    consistency_check = abs(mm1_change - mm1_matched_total) < 0.01
    status = f"{Colors.GREEN}✅ CONSISTENT{Colors.RESET}" if consistency_check else f"{Colors.RED}❌ INCONSISTENT{Colors.RESET}"
    print(f"  Consistency:    {status}\n")
    
    print(f"{Colors.BOLD}MM2 (TAKER):{Colors.RESET}")
    print(f"  Bets Placed:    {mm2_count}")
    print(f"  Balance Change: ${mm2_change:.2f}\n")
    
    print(f"{Colors.BOLD}Errors: {len(errors)}{Colors.RESET}\n")
    
    # Calculate metrics
    actual_rate = (mm1_count + mm2_count) / duration / 2 if duration > 0 else 0
    print(f"{Colors.BOLD}PERFORMANCE:{Colors.RESET}")
    print(f"  Target Rate:    {bets_per_second} bets/sec")
    print(f"  Actual Rate:    {actual_rate:.2f} bets/sec")
    
    match_rate = (len(mm1_matched) / mm1_count * 100) if mm1_count > 0 else 0
    print(f"  Match Rate:     {match_rate:.1f}%\n")
    
    # Save report
    report = {
        'test': 'rapid_fire_race',
        'duration': duration,
        'target_rate': bets_per_second,
        'actual_rate': actual_rate,
        'mm1_placed': mm1_count,
        'mm1_matched': len(mm1_matched),
        'mm2_placed': mm2_count,
        'snapshots': test.balance_snapshots,
        'consistency_check': consistency_check,
        'errors': errors
    }
    
    report_file = f"race_test_rapid_{int(time.time())}.json"
    with open(report_file, 'w') as f:
        json.dump(report, f, indent=2)
    
    print(f"{Colors.GREEN}📄 Report saved: {report_file}{Colors.RESET}\n")


def test_simultaneous_burst(event_id=None):
    """
    Test Case: Simultaneous Burst
    
    Scenario:
    - All 4 accounts place bets at the EXACT same time (within 10ms)
    - Multiple rounds of synchronized bursts
    - Tests matching engine's handling of true simultaneous requests
    """
    print(f"\n{Colors.BOLD}{Colors.MAGENTA}{'='*70}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.MAGENTA}Test: Simultaneous Burst{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.MAGENTA}True Simultaneous Operations{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.MAGENTA}{'='*70}{Colors.RESET}\n")
    
    framework = DeduceTestFramework(environment=config.ENVIRONMENT)
    test = RaceConditionTest(framework)
    
    # Setup accounts
    print(f"{Colors.CYAN}📦 Setting up 4 accounts...{Colors.RESET}\n")
    
    accounts = {}
    
    mm1_creds = config.get_account_credentials(1, config.ENVIRONMENT)
    framework.login_account('mm1', mm1_creds, account_type='mm')
    accounts['mm1'] = {'odds': 150, 'behavior': 'deduce'}
    
    mm2_creds = config.get_account_credentials(2, config.ENVIRONMENT)
    framework.login_account('mm2', mm2_creds, account_type='mm')
    accounts['mm2'] = {'odds': -150, 'behavior': 'normal'}
    
    patron_deduce_creds = {'email': 'deduct.sanbox.test1@yopmail.com', 'password': 'Matkhau1$'}
    framework.login_account('patron_deduce', patron_deduce_creds, account_type='patron')
    accounts['patron_deduce'] = {'odds': 150, 'behavior': 'deduce'}
    
    patron_config = config.load_user_config(f'user_info_patron_{config.ENVIRONMENT}.json')
    patron_nondeduce_creds = {'username': patron_config['email'], 'password': patron_config['password']}
    framework.login_account('patron_nondeduce', patron_nondeduce_creds, account_type='patron')
    accounts['patron_nondeduce'] = {'odds': -150, 'behavior': 'normal'}
    
    # Get initial balances
    account_names = list(accounts.keys())
    initial_snapshot = test.snapshot_balances(account_names, 'initial')
    
    for acc_name in account_names:
        print(f"{Colors.GREEN}✅ {acc_name}: ${initial_snapshot['balances'][acc_name]:,.2f}{Colors.RESET}")
    print()
    
    # Get market
    print(f"{Colors.CYAN}🔍 Finding market...{Colors.RESET}")
    if event_id:
        market_info = get_market_for_event(framework, 'mm1', event_id)
    else:
        market_info = framework.get_available_market('mm1')
    
    if not market_info:
        print(f"{Colors.RED}❌ No markets available{Colors.RESET}")
        return
    
    line_id = market_info['line_id']
    print(f"{Colors.GREEN}✅ Market found{Colors.RESET}\n")
    
    # Test execution - 10 synchronized bursts
    num_bursts = 10
    print(f"{Colors.BOLD}🚀 Running {num_bursts} synchronized bursts...{Colors.RESET}\n")
    
    errors = []
    burst_results = []
    
    for burst_num in range(num_bursts):
        print(f"{Colors.CYAN}💥 Burst {burst_num + 1}/{num_bursts}{Colors.RESET}")
        
        # Synchronization barrier
        barrier = threading.Barrier(4)
        burst_start = None
        
        def synchronized_bet(account_name: str, odds: int):
            """Place bet synchronized with barrier"""
            nonlocal burst_start
            
            # Wait for all threads to be ready
            barrier.wait()
            
            # Record exact start time
            if burst_start is None:
                burst_start = time.time()
            
            # Place bet
            try:
                start = time.time()
                result = framework.place_wager(account_name, line_id, odds, 1.0)
                duration = time.time() - start
                
                return {
                    'account': account_name,
                    'success': result.get('success'),
                    'duration': duration,
                    'error': result.get('error') if not result.get('success') else None
                }
            except Exception as e:
                return {
                    'account': account_name,
                    'success': False,
                    'error': str(e)
                }
        
        # Execute burst
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = []
            for acc_name, acc_info in accounts.items():
                future = executor.submit(synchronized_bet, acc_name, acc_info['odds'])
                futures.append(future)
            
            results = [f.result() for f in as_completed(futures)]
        
        burst_results.append(results)
        
        # Show results
        for res in results:
            status = "✅" if res['success'] else "❌"
            print(f"  {status} {res['account']}: {res['duration']*1000:.1f}ms")
        
        # Wait between bursts
        time.sleep(2)
    
    # Wait for final settling
    print(f"\n{Colors.CYAN}⏳ Waiting for all matches to settle...{Colors.RESET}")
    time.sleep(5)
    
    # Get final balances
    final_snapshot = test.snapshot_balances(account_names, 'final')
    
    # Analysis
    print(f"\n{Colors.BOLD}{'='*70}{Colors.RESET}")
    print(f"{Colors.BOLD}RESULTS{Colors.RESET}")
    print(f"{Colors.BOLD}{'='*70}{Colors.RESET}\n")
    
    for acc_name in account_names:
        acc_info = accounts[acc_name]
        initial = initial_snapshot['balances'][acc_name]
        final = final_snapshot['balances'][acc_name]
        change = initial - final
        
        # Count successful bets
        successful = sum(1 for burst in burst_results for res in burst if res['account'] == acc_name and res['success'])
        
        color = Colors.CYAN if acc_info['behavior'] == 'deduce' else Colors.BLUE
        
        print(f"{color}{Colors.BOLD}{acc_name.upper()} ({acc_info['behavior']}):{Colors.RESET}")
        print(f"  Successful Bets: {successful}/{num_bursts}")
        print(f"  Initial:         ${initial:,.2f}")
        print(f"  Final:           ${final:,.2f}")
        print(f"  Change:          ${change:,.2f}\n")
    
    # Save report
    report = {
        'test': 'simultaneous_burst',
        'num_bursts': num_bursts,
        'burst_results': burst_results,
        'snapshots': test.balance_snapshots
    }
    
    report_file = f"race_test_burst_{int(time.time())}.json"
    with open(report_file, 'w') as f:
        json.dump(report, f, indent=2)
    
    print(f"{Colors.GREEN}📄 Report saved: {report_file}{Colors.RESET}\n")


def test_deduce_accounts_get_matched(duration=30, event_id=None):
    """
    Test Case: Deduce Accounts Get Matched
    
    Scenario:
    - Deduce accounts (MM1 + Patron deduce) place bets
    - Non-deduce accounts (MM2 + Patron non-deduce) MATCH those bets
    - Critical: We verify deduce accounts' balance ONLY changes when matched
    
    Flow:
    - MM1 (deduce) places bet at odds 150 → MM2 (normal) matches at -150
    - Patron deduce places bet at odds 150 → Patron non-deduce matches at -150
    - Verify MM1 and Patron deduce balance stays 0 until match occurs
    - Verify balance changes AFTER match completes
    """
    print(f"\n{Colors.BOLD}{Colors.MAGENTA}{'='*70}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.MAGENTA}Test: Deduce Accounts Get Matched{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.MAGENTA}Verify deduce balance deduction happens AT MATCH TIME{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.MAGENTA}{'='*70}{Colors.RESET}\n")
    
    framework = DeduceTestFramework(environment=config.ENVIRONMENT)
    test = RaceConditionTest(framework)
    
    # Setup accounts
    print(f"{Colors.CYAN}📦 Setting up accounts...{Colors.RESET}\n")
    
    accounts = {}
    
    # Deduce accounts (makers - will get matched)
    mm1_creds = config.get_account_credentials(1, config.ENVIRONMENT)
    framework.login_account('mm1', mm1_creds, account_type='mm')
    accounts['mm1'] = {'type': 'mm', 'behavior': 'deduce', 'role': 'maker', 'odds': 150}
    
    patron_deduce_creds = {'email': 'deduct.sanbox.test1@yopmail.com', 'password': 'Matkhau1$'}
    framework.login_account('patron_deduce', patron_deduce_creds, account_type='patron')
    accounts['patron_deduce'] = {'type': 'patron', 'behavior': 'deduce', 'role': 'maker', 'odds': 150}
    
    # Non-deduce accounts (takers - will match deduce bets)
    mm2_creds = config.get_account_credentials(2, config.ENVIRONMENT)
    framework.login_account('mm2', mm2_creds, account_type='mm')
    accounts['mm2'] = {'type': 'mm', 'behavior': 'normal', 'role': 'taker', 'odds': -150}
    
    patron_config = config.load_user_config(f'user_info_patron_{config.ENVIRONMENT}.json')
    patron_nondeduce_creds = {'username': patron_config['email'], 'password': patron_config['password']}
    framework.login_account('patron_nondeduce', patron_nondeduce_creds, account_type='patron')
    accounts['patron_nondeduce'] = {'type': 'patron', 'behavior': 'normal', 'role': 'taker', 'odds': -150}
    
    # Get initial balances
    account_names = list(accounts.keys())
    initial_snapshot = test.snapshot_balances(account_names, 'initial')
    
    # Check for balance retrieval failures
    failed_accounts = [acc for acc, bal in initial_snapshot['balances'].items() if bal is None]
    if failed_accounts:
        print(f"{Colors.RED}❌ Failed to retrieve balance for: {', '.join(failed_accounts)}{Colors.RESET}")
        print(f"{Colors.YELLOW}⚠️  Retrying balance retrieval...{Colors.RESET}\n")
        time.sleep(2)
        
        # Retry failed accounts
        for acc in failed_accounts:
            try:
                balance_data = framework.get_balance(acc)
                initial_snapshot['balances'][acc] = balance_data.get('balance', 0)
            except Exception as e:
                print(f"{Colors.RED}❌ Still failed for {acc}: {e}{Colors.RESET}")
                print(f"{Colors.RED}   Aborting test due to connection issue{Colors.RESET}\n")
                return
    
    for acc_name in account_names:
        acc_info = accounts[acc_name]
        balance = initial_snapshot['balances'][acc_name]
        if balance is not None:
            role_str = f"{acc_info['behavior']} {acc_info['type']} ({acc_info['role']})"
            print(f"{Colors.GREEN}✅ {acc_name} ({role_str}): ${balance:,.2f}{Colors.RESET}")
    print()
    
    # Get market
    print(f"{Colors.CYAN}🔍 Finding market...{Colors.RESET}")
    if event_id:
        market_info = get_market_for_event(framework, 'mm1', event_id)
    else:
        market_info = framework.get_available_market('mm1')
    
    if not market_info:
        print(f"{Colors.RED}❌ No markets available{Colors.RESET}")
        return
    
    event_name = market_info.get('event', {}).get('name', 'Unknown')
    line_id = market_info['line_id']
    print(f"{Colors.GREEN}✅ Market: {event_name}{Colors.RESET}\n")

    # Determine opposite line for matchers
    print(f"{Colors.YELLOW}🔍 Resolving opposite line for matchers...{Colors.RESET}")
    match_line_id = get_opposite_line_id(framework, 'mm1', event_id or market_info.get('event', {}).get('event_id'), line_id)
    if not match_line_id:
        print(f"{Colors.YELLOW}⚠️  Opposite line not found. Falling back to same line (may not match depending on market model).{Colors.RESET}")
        match_line_id = line_id
    else:
        print(f"{Colors.GREEN}✅ Opposite line: {match_line_id[:16]}...{Colors.RESET}")
    
    # Test execution
    print(f"{Colors.BOLD}🚀 Starting {duration}s test...{Colors.RESET}")
    print(f"{Colors.CYAN}   Phase 1: Deduce accounts place bets (first 5s solo){Colors.RESET}")
    print(f"{Colors.CYAN}   Phase 2: Non-deduce accounts start matching{Colors.RESET}")
    print(f"{Colors.CYAN}   Phase 3: Verify deduce balance changes at match time{Colors.RESET}\n")
    
    errors = []
    wager_counts = {acc: 0 for acc in account_names}
    match_tracking = []
    lock = threading.Lock()
    
    # Shared queue for deduce bets that need matching
    deduce_bet_queue = []
    queue_lock = threading.Lock()
    
    # Balance tracking at each phase
    phase_snapshots = {}
    
    # Control flags
    stop_deduce_makers = threading.Event()
    start_matchers = threading.Event()
    
    def deduce_maker_worker(account_name: str):
        """Deduce accounts place bets continuously"""
        count = 0
        
        while not stop_deduce_makers.is_set():
            try:
                result = framework.place_wager(account_name, line_id, 150, 1.0)
                if result.get('success'):
                    count += 1
                    with lock:
                        wager_counts[account_name] = count
                    
                    # Add to queue for matching
                    with queue_lock:
                        deduce_bet_queue.append({
                            'deduce_account': account_name,
                            'line_id': line_id,
                            'odds': 150,
                            'stake': 1.0,
                            'timestamp': time.time()
                        })
                    
                    if count % 5 == 0:
                        color = Colors.CYAN
                        print(f"{color}📊 {account_name} (deduce): {count} bets placed{Colors.RESET}")
                else:
                    with lock:
                        errors.append({'account': account_name, 'error': result.get('error'), 'type': 'PLACE'})
            except Exception as e:
                with lock:
                    errors.append({'account': account_name, 'error': str(e), 'type': 'EXCEPTION'})
            
            time.sleep(0.3)
        
        return count
    
    def nondeduce_matcher_worker(account_name: str):
        """Non-deduce accounts match deduce bets"""
        count = 0
        
        # Wait for signal to start
        start_matchers.wait()
        start_time = time.time()
        
        while time.time() - start_time < duration:
            try:
                deduce_bet = None
                
                # Get a deduce bet to match
                with queue_lock:
                    if deduce_bet_queue:
                        deduce_bet = deduce_bet_queue.pop(0)
                
                if deduce_bet:
                    # Place opposite bet to match (use opposite selection line)
                    result = framework.place_wager(
                        account_name,
                        match_line_id,
                        -150,  # Opposite odds
                        1.0
                    )
                    
                    if result.get('success'):
                        count += 1
                        with lock:
                            wager_counts[account_name] = count
                            match_tracking.append({
                                'deduce_account': deduce_bet['deduce_account'],
                                'matcher_account': account_name,
                                'timestamp': time.time()
                            })
                        
                        if count % 5 == 0:
                            print(f"{Colors.BLUE}📊 {account_name} (matcher): {count} matches{Colors.RESET}")
                    else:
                        with lock:
                            errors.append({'account': account_name, 'error': result.get('error'), 'type': 'MATCH'})
                else:
                    time.sleep(0.05)
                    
            except Exception as e:
                with lock:
                    errors.append({'account': account_name, 'error': str(e), 'type': 'EXCEPTION'})
        
        return count
    
    # Start all workers
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = []
        
        # Start deduce makers
        for acc_name, acc_info in accounts.items():
            if acc_info['role'] == 'maker':
                future = executor.submit(deduce_maker_worker, acc_name)
                futures.append((acc_name, future))
        
        # Start matchers (they'll wait for signal)
        for acc_name, acc_info in accounts.items():
            if acc_info['role'] == 'taker':
                future = executor.submit(nondeduce_matcher_worker, acc_name)
                futures.append((acc_name, future))
        
        # Phase 1: Let deduce accounts build up bets
        print(f"{Colors.YELLOW}⏱️  Phase 1 (5s): Deduce accounts placing bets...{Colors.RESET}")
        time.sleep(5)
        
        phase_snapshots['after_placement'] = test.snapshot_balances(account_names, 'after_placement')
        print(f"{Colors.GREEN}✅ Snapshot taken - deduce bets placed, not yet matched{Colors.RESET}")
        print(f"{Colors.CYAN}   Queue size: {len(deduce_bet_queue)} bets waiting to be matched{Colors.RESET}\n")
        
        # Phase 2: Start matching
        print(f"{Colors.YELLOW}⏱️  Phase 2: Starting matchers...{Colors.RESET}")
        start_matchers.set()  # Signal matchers to start
        
        # Let matching run
        time.sleep(duration)
        
        # Stop deduce makers
        stop_deduce_makers.set()
        
        # Wait for completion
        print(f"\n{Colors.CYAN}Waiting for all workers to complete...{Colors.RESET}\n")
        for acc_name, future in futures:
            try:
                result = future.result()
                print(f"{Colors.GREEN}✅ {acc_name}: Completed {result} wagers{Colors.RESET}")
            except Exception as e:
                print(f"{Colors.RED}❌ {acc_name}: Error - {e}{Colors.RESET}")
    
    # Phase 2: Take snapshot immediately after matching
    print(f"\n{Colors.YELLOW}⏱️  Taking snapshot immediately after matching...{Colors.RESET}")
    phase_snapshots['after_matching'] = test.snapshot_balances(account_names, 'after_matching')
    
    # Phase 3: Wait for settlements with multiple checks
    print(f"{Colors.CYAN}⏳ Waiting for settlements and balance updates...{Colors.RESET}")
    
    # Check balance multiple times to see when it updates
    for wait_time in [3, 5, 8, 12]:
        print(f"{Colors.YELLOW}  Checking after {wait_time}s...{Colors.RESET}")
        time.sleep(wait_time if wait_time == 3 else wait_time - phase_snapshots.get('last_wait', 0))
        phase_snapshots['last_wait'] = wait_time
        
        temp_snapshot = test.snapshot_balances(['mm1', 'patron_deduce'], f'check_{wait_time}s')
        
        # Show deduce account balances
        for acc in ['mm1', 'patron_deduce']:
            current_bal = temp_snapshot['balances'][acc]
            initial_bal = initial_snapshot['balances'][acc]
            change = initial_bal - current_bal
            
            if abs(change) > 0.01:
                print(f"{Colors.GREEN}    ✅ {acc}: ${current_bal:,.2f} (Δ ${change:,.2f}) - BALANCE UPDATED!{Colors.RESET}")
            else:
                print(f"{Colors.YELLOW}    ⏳ {acc}: ${current_bal:,.2f} (Δ ${change:,.2f}) - No change yet{Colors.RESET}")
    
    print()
    
    # Final snapshot
    print(f"{Colors.CYAN}📸 Taking final balance snapshot (after 15s total)...{Colors.RESET}")
    time.sleep(3)  # Total 15s
    final_snapshot = test.snapshot_balances(account_names, 'final')
    
    # Analysis
    print(f"\n{Colors.BOLD}{'='*70}{Colors.RESET}")
    print(f"{Colors.BOLD}CRITICAL: DEDUCE BALANCE DEDUCTION TIMING ANALYSIS{Colors.RESET}")
    print(f"{Colors.BOLD}{'='*70}{Colors.RESET}\n")
    
    print(f"{Colors.CYAN}Tracking balance changes across phases:{Colors.RESET}")
    print(f"  Initial → After Placement → After Matching → Final\n")
    
    for acc_name in ['mm1', 'patron_deduce']:  # Focus on deduce accounts
        acc_info = accounts[acc_name]
        
        initial = initial_snapshot['balances'][acc_name]
        after_place = phase_snapshots['after_placement']['balances'][acc_name]
        after_match = phase_snapshots['after_matching']['balances'][acc_name]
        final = final_snapshot['balances'][acc_name]
        
        change_at_placement = initial - after_place
        change_at_matching = after_place - after_match
        change_after_settlement = after_match - final
        total_change = initial - final
        
        print(f"{Colors.BOLD}{Colors.CYAN}{acc_name.upper()} (DEDUCE):{Colors.RESET}")
        print(f"  1. Initial:         ${initial:,.2f}")
        print(f"  2. After Placement: ${after_place:,.2f} (Δ ${change_at_placement:,.2f})")
        
        if abs(change_at_placement) > 0.01:
            print(f"     {Colors.RED}❌ FAIL: Balance changed immediately on placement!{Colors.RESET}")
            print(f"     {Colors.RED}   Deduce NOT working - should be $0.00 change{Colors.RESET}")
        else:
            print(f"     {Colors.GREEN}✅ PASS: No change on placement (deduce working){Colors.RESET}")
        
        print(f"  3. After Matching:  ${after_match:,.2f} (Δ ${change_at_matching:,.2f})")
        
        if abs(change_at_matching) > 0.01:
            print(f"     {Colors.GREEN}✅ EXPECTED: Balance deducted at match time{Colors.RESET}")
        else:
            print(f"     {Colors.YELLOW}⚠️  No change yet - checking after settlement...{Colors.RESET}")
        
        print(f"  4. Final:           ${final:,.2f} (Δ ${change_after_settlement:,.2f})")
        print(f"     Total change:    ${total_change:,.2f}")
        print(f"     Wagers placed:   {wager_counts[acc_name]}")
        print(f"     Expected:        ~${wager_counts[acc_name]:.2f}\n")
    
    # Also show non-deduce for comparison
    print(f"{Colors.BOLD}NON-DEDUCE ACCOUNTS (for comparison):{Colors.RESET}\n")
    
    for acc_name in ['mm2', 'patron_nondeduce']:
        acc_info = accounts[acc_name]
        
        initial = initial_snapshot['balances'][acc_name]
        after_place = phase_snapshots['after_placement']['balances'][acc_name]
        final = final_snapshot['balances'][acc_name]
        
        change_at_placement = initial - after_place
        total_change = initial - final
        
        print(f"{Colors.BLUE}{acc_name.upper()} (NORMAL):{Colors.RESET}")
        print(f"  Initial:          ${initial:,.2f}")
        print(f"  After Placement:  ${after_place:,.2f} (Δ ${change_at_placement:,.2f})")
        
        if abs(change_at_placement) > 0.01:
            print(f"     {Colors.GREEN}✅ EXPECTED: Immediate deduction{Colors.RESET}")
        
        print(f"  Final:            ${final:,.2f}")
        print(f"  Total change:     ${total_change:,.2f}")
        print(f"  Matches made:     {wager_counts[acc_name]}\n")
    
    # Matching stats
    print(f"{Colors.BOLD}MATCHING STATS:{Colors.RESET}")
    print(f"  Total matches:        {len(match_tracking)}")
    print(f"  Unmatched bets:       {len(deduce_bet_queue)}")
    print(f"  Errors:               {len(errors)}\n")
    
    # Get matched bets for verification
    print(f"{Colors.BOLD}MATCHED BETS VERIFICATION:{Colors.RESET}")
    from datetime import datetime
    date_from = datetime.now().strftime('%Y-%m-%d')
    
    for acc in ['mm1', 'patron_deduce']:
        try:
            matched_bets = framework.get_matched_bets(acc, limit=200, date_from=date_from)
            acc_type = accounts[acc]['type']
            
            if acc_type == 'mm':
                matched_total = sum(bet.get('stake', 0) for bet in matched_bets)
            else:
                matched_total = sum(bet.get('amount', bet.get('stake', 0)) for bet in matched_bets)
            
            print(f"{Colors.CYAN}{acc}:{Colors.RESET}")
            print(f"  Matched bets:   {len(matched_bets)}")
            print(f"  Matched total:  ${matched_total:.2f}")
            print(f"  Balance change: ${initial_snapshot['balances'][acc] - final_snapshot['balances'][acc]:.2f}")
            
            if abs(matched_total - (initial_snapshot['balances'][acc] - final_snapshot['balances'][acc])) < 0.01:
                print(f"  {Colors.GREEN}✅ PASS: Balance change = Matched total{Colors.RESET}\n")
            else:
                print(f"  {Colors.YELLOW}⚠️  Check: Balance change ≠ Matched total{Colors.RESET}\n")
        except Exception as e:
            print(f"{Colors.YELLOW}{acc}: Could not verify - {e}{Colors.RESET}\n")
    
    # Manual verification prompt
    print(f"\n{Colors.BOLD}{Colors.YELLOW}{'='*70}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.YELLOW}MANUAL VERIFICATION{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.YELLOW}{'='*70}{Colors.RESET}\n")
    
    print(f"{Colors.CYAN}If you see different balances in the UI, press Enter to refresh and check again...{Colors.RESET}")
    print(f"{Colors.CYAN}Or type 'skip' to continue without checking.{Colors.RESET}")
    
    user_input = input().strip().lower()
    
    if user_input != 'skip':
        print(f"\n{Colors.YELLOW}Refreshing balances from API...{Colors.RESET}\n")
        
        manual_snapshot = test.snapshot_balances(['mm1', 'patron_deduce'], 'manual_check')
        
        for acc in ['mm1', 'patron_deduce']:
            manual_bal = manual_snapshot['balances'][acc]
            initial_bal = initial_snapshot['balances'][acc]
            final_bal = final_snapshot['balances'][acc]
            
            manual_change = initial_bal - manual_bal
            
            print(f"{Colors.BOLD}{Colors.CYAN}{acc.upper()}:{Colors.RESET}")
            print(f"  Initial balance:      ${initial_bal:,.2f}")
            print(f"  Auto final (15s):     ${final_bal:,.2f}")
            print(f"  Manual check (now):   ${manual_bal:,.2f}")
            print(f"  Total change:         ${manual_change:,.2f}")
            print(f"  Expected (~matches):  ~${wager_counts[acc]:.2f}\n")
        
        # Update final snapshot with manual check
        final_snapshot = manual_snapshot
    
    # Save report
    report = {
        'test': 'deduce_accounts_get_matched',
        'duration': duration,
        'accounts': accounts,
        'wager_counts': wager_counts,
        'match_tracking': match_tracking,
        'snapshots': {
            'initial': initial_snapshot,
            'after_placement': phase_snapshots['after_placement'],
            'after_matching': phase_snapshots['after_matching'],
            'final': final_snapshot,
            'periodic_checks': {k: v for k, v in phase_snapshots.items() if k.startswith('check_')}
        },
        'errors': errors
    }
    
    report_file = f"race_test_deduce_matched_{int(time.time())}.json"
    with open(report_file, 'w') as f:
        json.dump(report, f, indent=2)
    
    print(f"{Colors.GREEN}📄 Report saved: {report_file}{Colors.RESET}\n")


def test_patron_matches_mm_wagers(duration=30, event_id=None, aggression=1.0):
    """
    Test Case: Patron Matches MM Wagers
    
    Scenario:
    - MM accounts (deduce + non-deduce) place bets continuously
    - Patron accounts actively match those bets (opposite side)
    - Similar to patron_match_mm_bets.py but with both deduce & non-deduce
    - Verify correct balance deduction timing for all account types
    
    Flow:
    - MM1 (deduce) places bet at odds 150
    - MM2 (normal) places bet at odds 150  
    - Patron deduce matches at odds -150 (opposite side)
    - Patron non-deduce matches at odds -150 (opposite side)
    """
    print(f"\n{Colors.BOLD}{Colors.MAGENTA}{'='*70}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.MAGENTA}Test: Patron Matches MM Wagers{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.MAGENTA}Patrons actively matching MM bets{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.MAGENTA}{'='*70}{Colors.RESET}\n")
    
    framework = DeduceTestFramework(environment=config.ENVIRONMENT)
    test = RaceConditionTest(framework)
    
    # Setup accounts
    print(f"{Colors.CYAN}📦 Setting up accounts...{Colors.RESET}\n")
    
    accounts = {}
    
    # MM accounts (makers)
    mm1_creds = config.get_account_credentials(1, config.ENVIRONMENT)
    framework.login_account('mm1', mm1_creds, account_type='mm')
    accounts['mm1'] = {'type': 'mm', 'behavior': 'deduce', 'role': 'maker', 'odds': 150}
    
    mm2_creds = config.get_account_credentials(2, config.ENVIRONMENT)
    framework.login_account('mm2', mm2_creds, account_type='mm')
    accounts['mm2'] = {'type': 'mm', 'behavior': 'normal', 'role': 'maker', 'odds': 150}
    
    # Patron accounts (takers)
    patron_deduce_creds = {'email': 'deduct.sanbox.test1@yopmail.com', 'password': 'Matkhau1$'}
    framework.login_account('patron_deduce', patron_deduce_creds, account_type='patron')
    accounts['patron_deduce'] = {'type': 'patron', 'behavior': 'deduce', 'role': 'taker', 'odds': -150}
    
    patron_config = config.load_user_config(f'user_info_patron_{config.ENVIRONMENT}.json')
    patron_nondeduce_creds = {'username': patron_config['email'], 'password': patron_config['password']}
    framework.login_account('patron_nondeduce', patron_nondeduce_creds, account_type='patron')
    accounts['patron_nondeduce'] = {'type': 'patron', 'behavior': 'normal', 'role': 'taker', 'odds': -150}
    
    # Get initial balances
    account_names = list(accounts.keys())
    initial_snapshot = test.snapshot_balances(account_names, 'initial')
    
    for acc_name in account_names:
        acc_info = accounts[acc_name]
        role_str = f"{acc_info['behavior']} {acc_info['type']} ({acc_info['role']})"
        print(f"{Colors.GREEN}✅ {acc_name} ({role_str}): ${initial_snapshot['balances'][acc_name]:,.2f}{Colors.RESET}")
    print()
    
    # Get market
    print(f"{Colors.CYAN}🔍 Finding market...{Colors.RESET}")
    if event_id:
        market_info = get_market_for_event(framework, 'mm1', event_id)
    else:
        market_info = framework.get_available_market('mm1')
    
    if not market_info:
        print(f"{Colors.RED}❌ No markets available{Colors.RESET}")
        return
    
    event_name = market_info.get('event', {}).get('name', 'Unknown')
    line_id = market_info['line_id']
    print(f"{Colors.GREEN}✅ Market: {event_name}{Colors.RESET}")

    # Determine opposite line for patrons to match
    print(f"{Colors.YELLOW}🔍 Resolving opposite line for patrons...{Colors.RESET}")
    opposite_line_id = get_opposite_line_id(framework, 'mm1', event_id or market_info.get('event', {}).get('event_id'), line_id)
    if not opposite_line_id:
        print(f"{Colors.YELLOW}⚠️  Opposite line not found. Falling back to same line (may not match depending on market model).{Colors.RESET}")
        opposite_line_id = line_id
    else:
        print(f"{Colors.GREEN}✅ Opposite line: {opposite_line_id[:16]}...{Colors.RESET}")
    
    # Fetch odds ladder
    print(f"{Colors.YELLOW}🎯 Fetching odds ladder...{Colors.RESET}")
    odds_ladder = get_odds_ladder(framework)
    positive_odds = [o for o in odds_ladder if o > 0]
    negative_odds = [o for o in odds_ladder if o < 0]
    print(f"{Colors.GREEN}✅ Loaded {len(odds_ladder)} odds values{Colors.RESET}\n")
    
    # Test execution
    print(f"{Colors.BOLD}🚀 Starting {duration}s patron matching test...{Colors.RESET}")
    print(f"{Colors.CYAN}   MMs place bets → Patrons match them{Colors.RESET}\n")
    
    errors = []
    wager_counts = {acc: 0 for acc in account_names}
    match_tracking = []  # Track MM->Patron matches
    lock = threading.Lock()
    
    # Shared queue for MM bets that need matching
    mm_bet_queue = []
    queue_lock = threading.Lock()
    
    def mm_maker_worker(account_name: str):
        """MM accounts place bets continuously"""
        count = 0
        start_time = time.time()
        
        while time.time() - start_time < duration:
            try:
                odds = random.choice(positive_odds)
                result = framework.place_wager(account_name, line_id, odds, 1.0)
                if result.get('success'):
                    count += 1
                    with lock:
                        wager_counts[account_name] = count
                    
                    # Add to queue for patrons to match (with actual odds used)
                    with queue_lock:
                        mm_bet_queue.append({
                            'mm_account': account_name,
                            'line_id': line_id,
                            'odds': odds,  # Use actual odds, not hardcoded 150
                            'stake': 1.0,
                            'timestamp': time.time()
                        })
                    
                    if count % 10 == 0:
                        print(f"{Colors.CYAN}📊 {account_name}: {count} bets placed{Colors.RESET}")
                else:
                    with lock:
                        errors.append({'account': account_name, 'error': result.get('error'), 'type': 'PLACE'})
            except Exception as e:
                with lock:
                    errors.append({'account': account_name, 'error': str(e), 'type': 'EXCEPTION'})
            
            # Scale pacing by aggression factor (higher = faster)
            time.sleep(max(0.005, 0.1 / max(aggression, 0.1)))  # base 0.1s -> halve when aggression=2
        
        return count
    
    def patron_matcher_worker(account_name: str):
        """Patron accounts match MM bets from the queue"""
        count = 0
        start_time = time.time()
        
        while time.time() - start_time < duration:
            try:
                mm_bet = None
                
                # Try to get an MM bet to match
                with queue_lock:
                    if mm_bet_queue:
                        mm_bet = mm_bet_queue.pop(0)
                
                if mm_bet:
                    # Small delay to simulate patron reaction time, scaled by aggression
                    time.sleep(max(0.003, 0.025 / max(aggression, 0.1)))
                    
                    # Place opposite bet (negative odds)
                    odds = random.choice(negative_odds)
                    result = framework.place_wager(
                        account_name, 
                        opposite_line_id, 
                        odds,
                        1.0
                    )
                    
                    if result.get('success'):
                        count += 1
                        with lock:
                            wager_counts[account_name] = count
                            match_tracking.append({
                                'mm': mm_bet['mm_account'],
                                'patron': account_name,
                                'timestamp': time.time()
                            })
                        
                        if count % 10 == 0:
                            print(f"{Colors.BLUE}📊 {account_name}: {count} matches{Colors.RESET}")
                    else:
                        with lock:
                            errors.append({'account': account_name, 'error': result.get('error'), 'type': 'MATCH'})
                else:
                    # No bets to match, wait a bit (scaled by aggression)
                    time.sleep(max(0.003, 0.025 / max(aggression, 0.1)))
                    
            except Exception as e:
                with lock:
                    errors.append({'account': account_name, 'error': str(e), 'type': 'EXCEPTION'})
        
        return count
    
    # Run workers concurrently
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = []
        
        # Start MM makers
        for acc_name, acc_info in accounts.items():
            if acc_info['role'] == 'maker':
                future = executor.submit(mm_maker_worker, acc_name)
                futures.append((acc_name, future))
        
        # Start patron matchers
        for acc_name, acc_info in accounts.items():
            if acc_info['role'] == 'taker':
                future = executor.submit(patron_matcher_worker, acc_name)
                futures.append((acc_name, future))
        
        # Wait for completion
        for acc_name, future in futures:
            try:
                result = future.result()
                print(f"{Colors.GREEN}✅ {acc_name}: Completed {result} wagers{Colors.RESET}")
            except Exception as e:
                print(f"{Colors.RED}❌ {acc_name}: Error - {e}{Colors.RESET}")
    
    # Wait for matches to settle
    print(f"\n{Colors.CYAN}⏳ Waiting for matches to settle...{Colors.RESET}")
    time.sleep(5)
    
    # Take intermediate snapshot to track settlement
    print(f"{Colors.CYAN}📸 Taking intermediate balance snapshot...{Colors.RESET}")
    intermediate_snapshot = test.snapshot_balances(account_names, 'intermediate')
    
    # Wait a bit more for any delayed settlements
    time.sleep(3)
    
    # Get final balances
    print(f"{Colors.CYAN}📸 Taking final balance snapshot...{Colors.RESET}")
    final_snapshot = test.snapshot_balances(account_names, 'final')
    
    # Analysis
    print(f"\n{Colors.BOLD}{'='*70}{Colors.RESET}")
    print(f"{Colors.BOLD}FINAL RESULTS{Colors.RESET}")
    print(f"{Colors.BOLD}{'='*70}{Colors.RESET}\n")
    
    print(f"{Colors.BOLD}MM MAKERS (providing liquidity):{Colors.RESET}\n")
    for acc_name in ['mm1', 'mm2']:
        acc_info = accounts[acc_name]
        initial = initial_snapshot['balances'][acc_name]
        final = final_snapshot['balances'][acc_name]
        change = initial - final
        
        print(f"{Colors.CYAN}{acc_name.upper()} ({acc_info['behavior']}):{Colors.RESET}")
        print(f"  Wagers Placed: {wager_counts[acc_name]}")
        print(f"  Initial:       ${initial:,.2f}")
        print(f"  Final:         ${final:,.2f}")
        print(f"  Change:        ${change:,.2f}\n")
    
    print(f"{Colors.BOLD}PATRON MATCHERS (taking liquidity):{Colors.RESET}\n")
    for acc_name in ['patron_deduce', 'patron_nondeduce']:
        acc_info = accounts[acc_name]
        initial = initial_snapshot['balances'][acc_name]
        final = final_snapshot['balances'][acc_name]
        change = initial - final
        
        print(f"{Colors.BLUE}{acc_name.upper()} ({acc_info['behavior']}):{Colors.RESET}")
        print(f"  Matches Made:  {wager_counts[acc_name]}")
        print(f"  Initial:       ${initial:,.2f}")
        print(f"  Final:         ${final:,.2f}")
        print(f"  Change:        ${change:,.2f}\n")
    
    print(f"{Colors.BOLD}MATCHING STATS:{Colors.RESET}")
    print(f"  Total matches: {len(match_tracking)}")
    print(f"  Unmatched MM bets: {len(mm_bet_queue)}\n")
    
    print(f"{Colors.BOLD}Errors: {len(errors)}{Colors.RESET}")
    if errors:
        error_summary = {}
        for err in errors:
            key = f"{err['account']}-{err['type']}"
            error_summary[key] = error_summary.get(key, 0) + 1
        
        for error_key, count in error_summary.items():
            print(f"  {error_key}: {count}")
    print()
    
    # Detailed balance change analysis
    print(f"{Colors.BOLD}BALANCE CHANGE ANALYSIS:{Colors.RESET}")
    print(f"{Colors.CYAN}Initial → Intermediate → Final{Colors.RESET}\n")
    
    for acc_name in account_names:
        initial = initial_snapshot['balances'][acc_name]
        intermediate = intermediate_snapshot['balances'][acc_name]
        final = final_snapshot['balances'][acc_name]
        
        change_during = initial - intermediate
        change_after = intermediate - final
        total_change = initial - final
        
        behavior = accounts[acc_name]['behavior']
        color = Colors.CYAN if behavior == 'deduce' else Colors.BLUE
        
        print(f"{color}{acc_name}:{Colors.RESET}")
        print(f"  Initial:       ${initial:,.2f}")
        print(f"  Intermediate:  ${intermediate:,.2f} (change: ${change_during:,.2f})")
        print(f"  Final:         ${final:,.2f} (change: ${change_after:,.2f})")
        print(f"  Total change:  ${total_change:,.2f}\n")
    
    # Get exposure information for all accounts
    print(f"{Colors.BOLD}EXPOSURE & PENDING SETTLEMENTS:{Colors.RESET}")
    for acc_name in account_names:
        acc_type = accounts[acc_name]['type']
        if acc_type == 'mm':  # Only MM accounts have exposure endpoint
            try:
                exposure_data = framework.get_exposure(acc_name)
                if exposure_data:
                    print(f"{Colors.CYAN}{acc_name}:{Colors.RESET}")
                    print(f"  Exposure: {json.dumps(exposure_data, indent=4)}\n")
            except Exception as e:
                print(f"{Colors.YELLOW}{acc_name}: Could not get exposure - {e}{Colors.RESET}\n")
    
    # Verify deduce accounts
    print(f"{Colors.BOLD}DEDUCE VERIFICATION:{Colors.RESET}")
    deduce_accounts = [acc for acc, info in accounts.items() if info['behavior'] == 'deduce']
    
    for acc in deduce_accounts:
        initial = initial_snapshot['balances'][acc]
        final = final_snapshot['balances'][acc]
        change = initial - final
        
        acc_type = accounts[acc]['type']
        
        # Get matched bets for all account types (both MM and patron)
        matched_bets = []
        matched_total = 0.0
        
        try:
            # Pass today's date for patron accounts
            from datetime import datetime
            date_from = datetime.now().strftime('%Y-%m-%d')
            matched_bets = framework.get_matched_bets(acc, limit=200, date_from=date_from)
            
            # Calculate total stake from matched bets
            if acc_type == 'mm':
                # MM API format
                matched_total = sum(bet.get('stake', 0) for bet in matched_bets)
            else:
                # Patron API format - check for 'amount' or 'stake' field
                matched_total = sum(bet.get('amount', bet.get('stake', 0)) for bet in matched_bets)
        except Exception as e:
            print(f"{Colors.YELLOW}Note: Could not retrieve matched bets for {acc}: {e}{Colors.RESET}")
        
        # For deduce accounts, we expect:
        # - Balance change should equal matched amount (or be very close)
        # - OR if we can't get matched bets, we use wager count as proxy
        
        wagers_placed = wager_counts.get(acc, 0)
        expected_matched = wagers_placed  # Assuming $1 stakes
        
        if matched_bets:
            # We have matched bet data
            matches = abs(change - matched_total) < 0.01
            status = f"{Colors.GREEN}✅ PASS{Colors.RESET}" if matches else f"{Colors.RED}❌ FAIL{Colors.RESET}"
            
            print(f"{status} - {acc}:")
            print(f"  Wagers placed:  {wagers_placed}")
            print(f"  Balance change: ${change:.2f}")
            print(f"  Matched total:  ${matched_total:.2f}")
            print(f"  Matched bets:   {len(matched_bets)}")
        else:
            # No matched bet data, use wager count
            # For deduce: change should be 0 if nothing matched, or equal to match count
            # We'll check if change is reasonable given the wagers
            
            if abs(change) < 0.01:
                # No balance change - either no matches or deduce working correctly
                status = f"{Colors.GREEN}✅ PASS{Colors.RESET}"
                note = "No balance change (no matches or matches not yet settled)"
            elif abs(change - expected_matched) < expected_matched * 0.1:  # Within 10%
                status = f"{Colors.GREEN}✅ PASS{Colors.RESET}"
                note = "Balance change consistent with wagers placed"
            else:
                status = f"{Colors.YELLOW}⚠️  CHECK{Colors.RESET}"
                note = "Balance change differs from expected - verify manually"
            
            print(f"{status} - {acc}:")
            print(f"  Wagers placed:  {wagers_placed}")
            print(f"  Balance change: ${change:.2f}")
            print(f"  Expected:       ~${expected_matched:.2f}")
            print(f"  Note: {note}")
        
        print()
    
    # Balance reconciliation check
    print(f"{Colors.BOLD}BALANCE RECONCILIATION:{Colors.RESET}")
    total_mm_change = sum(
        initial_snapshot['balances'][acc] - final_snapshot['balances'][acc]
        for acc in ['mm1', 'mm2']
    )
    total_patron_change = sum(
        initial_snapshot['balances'][acc] - final_snapshot['balances'][acc]
        for acc in ['patron_deduce', 'patron_nondeduce']
    )
    
    print(f"  Total MM balance change:     ${total_mm_change:.2f}")
    print(f"  Total Patron balance change: ${total_patron_change:.2f}")
    print(f"  Net change (should be ~0):   ${(total_mm_change - total_patron_change):.2f}")
    
    if abs(total_mm_change - total_patron_change) < 1.0:
        print(f"  {Colors.GREEN}✅ Balanced - MMs and Patrons offset each other{Colors.RESET}\n")
    else:
        print(f"  {Colors.YELLOW}⚠️  Imbalanced - Check for pending settlements or errors{Colors.RESET}\n")
    
    # Save report
    report = {
        'test': 'patron_matches_mm_wagers',
        'duration': duration,
        'accounts': accounts,
        'wager_counts': wager_counts,
        'match_tracking': match_tracking,
        'snapshots': test.balance_snapshots,
        'errors': errors,
        'balance_reconciliation': {
            'mm_total_change': total_mm_change,
            'patron_total_change': total_patron_change,
            'net_difference': total_mm_change - total_patron_change,
            'balanced': abs(total_mm_change - total_patron_change) < 1.0
        }
    }
    
    report_file = f"race_test_patron_mm_{int(time.time())}.json"
    with open(report_file, 'w') as f:
        json.dump(report, f, indent=2)
    
    print(f"{Colors.GREEN}📄 Report saved: {report_file}{Colors.RESET}\n")


def test_cancel_race_wager_job_bug_with_retry(duration=30, cancel_rate=0.5, event_id=None, rps=None):
    """
    Wrapper that handles token expiration and retries for long-running tests
    """
    max_session_duration = 900  # 15 minutes (tokens expire after ~15-18 mins)
    remaining_duration = duration
    cumulative_stats = {
        'mm1_placed': 0, 'mm1_cancelled': 0,
        'mm2_placed': 0, 'mm2_cancelled': 0,
        'patron_matches': 0, 'errors': 0
    }
    
    print(f"{Colors.BOLD}{Colors.CYAN}\n{'='*70}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.CYAN}LONG-RUNNING TEST WITH AUTO-RETRY{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.CYAN}Total duration: {duration//60} minutes ({duration}s){Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.CYAN}{'='*70}{Colors.RESET}\n")
    
    session_num = 0
    while remaining_duration > 0:
        session_num += 1
        session_duration = min(remaining_duration, max_session_duration)
        
        print(f"{Colors.YELLOW}\n▶️  SESSION {session_num}: Running {session_duration}s ({session_duration//60}min)...{Colors.RESET}\n")
        
        try:
            # Run the test for this session
            stats = test_cancel_race_wager_job_bug(
                duration=session_duration,
                cancel_rate=cancel_rate,
                event_id=event_id,
                rps=rps
            )
            
            # Accumulate stats if returned
            if stats:
                cumulative_stats['mm1_placed'] += stats.get('mm1_placed', 0)
                cumulative_stats['mm1_cancelled'] += stats.get('mm1_cancelled', 0)
                cumulative_stats['mm2_placed'] += stats.get('mm2_placed', 0)
                cumulative_stats['mm2_cancelled'] += stats.get('mm2_cancelled', 0)
                cumulative_stats['patron_matches'] += stats.get('patron_matches', 0)
                cumulative_stats['errors'] += stats.get('errors', 0)
        
        except Exception as e:
            print(f"{Colors.RED}❌ Session {session_num} failed: {e}{Colors.RESET}")
            print(f"{Colors.YELLOW}Retrying with fresh login...{Colors.RESET}\n")
        
        remaining_duration -= session_duration
        
        if remaining_duration > 0:
            print(f"{Colors.CYAN}\n⏳ Remaining: {remaining_duration}s ({remaining_duration//60}min)...{Colors.RESET}")
            print(f"{Colors.YELLOW}Waiting 5s before next session...{Colors.RESET}\n")
            time.sleep(5)
    
    # Final summary
    print(f"{Colors.BOLD}{Colors.GREEN}\n{'='*70}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.GREEN}ALL SESSIONS COMPLETED{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.GREEN}{'='*70}{Colors.RESET}\n")
    
    print(f"{Colors.CYAN}Cumulative Stats:{Colors.RESET}")
    print(f"  MM1: {cumulative_stats['mm1_placed']} placed, {cumulative_stats['mm1_cancelled']} cancelled")
    print(f"  MM2: {cumulative_stats['mm2_placed']} placed, {cumulative_stats['mm2_cancelled']} cancelled")
    print(f"  Total: {cumulative_stats['mm1_placed'] + cumulative_stats['mm2_placed']} placed, "
          f"{cumulative_stats['mm1_cancelled'] + cumulative_stats['mm2_cancelled']} cancelled")
    print(f"  Patron matches: {cumulative_stats['patron_matches']}")
    print(f"  Errors: {cumulative_stats['errors']}\n")

def test_cancel_race_wager_job_bug(duration=30, cancel_rate=0.5, event_id=None, rps=None):
    """
    Test Case: Cancel Race Condition - Wager Job Bug
    
    BUG BEING TESTED:
    When a wager is cancelled, wager_jobs with type="open" can remain stuck in "pending" status
    even though the wager itself shows status="cancelled".
    
    Scenario:
    - MM1 rapidly places and cancels wagers (deduce account)
    - 2 Patron accounts try to match MM1's wagers before they cancel
    - cancel_rate: probability of cancelling each wager (0.5 = 50% chance)
    - After test, check wager_jobs table for stuck "pending" jobs
    - Verify through balance if wagers matched or cancelled
    
    Balance Verification:
    - If MATCHED: MM1's matched_wager_balance increases by matched amount
    - If CANCELLED: MM1's balance unchanged, no exposure increase
    
    Expected Bug:
    - Wagers with status='cancelled' should NOT have wager_jobs with status='pending'
    - But the bug causes wager_jobs to get stuck in 'pending' state
    """
    print(f"\n{Colors.BOLD}{Colors.MAGENTA}{'='*70}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.MAGENTA}Test: Cancel Race Condition - Wager Job Bug{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.MAGENTA}Testing wager_job stuck in pending after cancel{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.MAGENTA}{'='*70}{Colors.RESET}\n")
    
    print(f"{Colors.RED}BUG: wager_jobs with type='open' stay 'pending' even after wager cancelled{Colors.RESET}")
    print(f"{Colors.YELLOW}This test will rapidly place & cancel to reproduce the issue{Colors.RESET}\n")
    
    framework = DeduceTestFramework(environment=config.ENVIRONMENT)
    test = RaceConditionTest(framework)
    
    # Setup accounts
    print(f"{Colors.CYAN}📦 Setting up accounts...{Colors.RESET}\n")
    
    # MM1 - deduce account (placer/canceller)
    mm1_creds = config.get_account_credentials(1, config.ENVIRONMENT)
    framework.login_account('mm1', mm1_creds, account_type='mm')
    print(f"{Colors.GREEN}✅ MM1 (deduce maker) logged in{Colors.RESET}")
    
    # MM2 - non-deduce account (placer/canceller)
    mm2_creds = config.get_account_credentials(2, config.ENVIRONMENT)
    framework.login_account('mm2', mm2_creds, account_type='mm')
    print(f"{Colors.GREEN}✅ MM2 (normal maker) logged in{Colors.RESET}")
    
    # Get initial balances for both MMs
    mm1_initial_balance_data = framework.get_balance('mm1')
    mm1_initial_balance = mm1_initial_balance_data.get('balance', 0)
    mm1_initial_matched = mm1_initial_balance_data.get('matched_wager_balance', 0)
    mm1_initial_unmatched = mm1_initial_balance_data.get('unmatched_wager_balance', 0)
    
    mm2_initial_balance_data = framework.get_balance('mm2')
    mm2_initial_balance = mm2_initial_balance_data.get('balance', 0)
    
    print(f"{Colors.CYAN}MM1 Initial state:{Colors.RESET}")
    print(f"  Balance: ${mm1_initial_balance:,.2f}")
    print(f"  Matched: ${mm1_initial_matched:,.2f} ← Track this!")
    print(f"  Unmatched: ${mm1_initial_unmatched:,.2f}")
    print(f"{Colors.CYAN}MM2 Initial balance: ${mm2_initial_balance:,.2f}{Colors.RESET}\n")
    
    # Patron accounts - matchers
    patron_deduce_creds = {'email': 'deduct.sanbox.test1@yopmail.com', 'password': 'Matkhau1$'}
    framework.login_account('patron_deduce', patron_deduce_creds, account_type='patron')
    print(f"{Colors.GREEN}✅ Patron deduce (matcher 1) logged in{Colors.RESET}")
    
    patron_config = config.load_user_config(f'user_info_patron_{config.ENVIRONMENT}.json')
    patron_nondeduce_creds = {'username': patron_config['email'], 'password': patron_config['password']}
    framework.login_account('patron_nondeduce', patron_nondeduce_creds, account_type='patron')
    print(f"{Colors.GREEN}✅ Patron non-deduce (matcher 2) logged in{Colors.RESET}\n")
    
    # Get market
    if event_id:
        print(f"{Colors.CYAN}🔍 Getting markets for event {event_id}...{Colors.RESET}")
        
        # Get markets for specific event using MM API
        import requests
        from urllib.parse import urljoin
        
        multiple_markets_url = urljoin(framework.base_url, config.URL['mm_multiple_markets'])
        markets_response = requests.get(
            multiple_markets_url,
            params={'event_ids': str(event_id)},
            headers=framework.get_auth_header('mm1')
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
    else:
        print(f"{Colors.CYAN}🔍 Finding active market...{Colors.RESET}")
        market_info = framework.get_available_market('mm1')
        
        if not market_info:
            print(f"{Colors.RED}❌ No markets available{Colors.RESET}")
            return
        
        line_id = market_info['line_id']
        event_id = market_info.get('event', {}).get('event_id')
        event_name = market_info.get('event', {}).get('name', 'Unknown')
        print(f"{Colors.GREEN}✅ Event: {event_name} (ID: {event_id}){Colors.RESET}")
    
    # Verify market is active by placing a small test bet
    print(f"{Colors.YELLOW}🔍 Verifying market is active...{Colors.RESET}")
    test_result = framework.place_wager('mm1', line_id, 150, 1.0)
    
    if test_result.get('success'):
        test_wager = test_result.get('data', {}).get('wager', {})
        wager_status = test_wager.get('status', 'unknown')
        
        if wager_status == 'inactive':
            print(f"{Colors.RED}❌ Market line is INACTIVE - wagers won't match{Colors.RESET}")
            
            # Only retry if event_id was NOT explicitly provided
            if not event_id:
                print(f"{Colors.YELLOW}   Trying to find an active market...{Colors.RESET}")
                
                # Try to find another market
                for attempt in range(5):
                    market_info = framework.get_available_market('mm1')
                    if market_info:
                        line_id = market_info['line_id']
                        event_id = market_info.get('event', {}).get('event_id')
                        test_result = framework.place_wager('mm1', line_id, 150, 1.0)
                        if test_result.get('success'):
                            test_wager = test_result.get('data', {}).get('wager', {})
                            if test_wager.get('status') != 'inactive':
                                print(f"{Colors.GREEN}✅ Found active market!{Colors.RESET}")
                                break
                else:
                    print(f"{Colors.RED}❌ Could not find active market after 5 attempts{Colors.RESET}")
                    print(f"{Colors.YELLOW}   Test will continue but wagers may not match{Colors.RESET}")
            else:
                print(f"{Colors.YELLOW}   Specific event_id was provided - not retrying with different event{Colors.RESET}")
                print(f"{Colors.YELLOW}   Test will continue but wagers may not match{Colors.RESET}")
        else:
            print(f"{Colors.GREEN}✅ Market is ACTIVE (status: {wager_status}){Colors.RESET}")
    else:
        print(f"{Colors.YELLOW}⚠️  Could not verify market status{Colors.RESET}")
    
    print(f"{Colors.CYAN}Market: {event_name} (line: {line_id[:16]}...){Colors.RESET}\n")
    
    # Fetch odds ladder
    print(f"{Colors.YELLOW}🎯 Fetching odds ladder...{Colors.RESET}")
    odds_ladder = get_odds_ladder(framework)
    print(f"{Colors.GREEN}✅ Loaded {len(odds_ladder)} odds values{Colors.RESET}")
    
    # Get opposite line_id for patrons to match
    print(f"{Colors.YELLOW}🔍 Finding opposite line_id for patrons (event {event_id})...{Colors.RESET}")
    opposite_line_id = get_opposite_line_id(framework, 'mm1', event_id, line_id)
    
    if not opposite_line_id:
        print(f"{Colors.RED}❌ Could not find opposite line_id - patrons won't be able to match{Colors.RESET}")
        print(f"{Colors.YELLOW}   Test will continue but matching may not work{Colors.RESET}\n")
    else:
        print(f"{Colors.GREEN}✅ Opposite line found (line: {opposite_line_id[:16]}...){Colors.RESET}\n")
    
    # Test execution
    print(f"{Colors.BOLD}🚀 Starting {duration}s place & cancel stress test...{Colors.RESET}")
    print(f"{Colors.CYAN}   Cancel rate: {cancel_rate*100:.0f}% of wagers will be cancelled{Colors.RESET}")
    print(f"{Colors.CYAN}   MM1 & MM2 bet on line: {line_id[:16]}...{Colors.RESET}")
    print(f"{Colors.CYAN}   Patrons match on: {opposite_line_id[:16] if opposite_line_id else 'N/A'}...{Colors.RESET}\n")
    
    # Shared data structures
    mm_wagers = {'mm1': [], 'mm2': []}  # Track wagers by MM
    cancelled_wagers = {'mm1': [], 'mm2': []}  # Track cancellations by MM
    matched_by_patrons = []
    errors = []
    lock = threading.Lock()
    
    # Shared queue for both MM's wagers that patrons can match
    mm_wager_queue = []
    queue_lock = threading.Lock()
    
    # Tracking for bet delay verification
    bet_placement_times = []
    bet_delays_lock = threading.Lock()
    
    def mm_place_and_cancel_worker(account_name: str):
        """MM worker: Place wagers and randomly cancel them"""
        count = 0
        cancelled = 0
        start_time = time.time()
        
        per_mm_rps = (rps / 2.0) if rps else None
        while time.time() - start_time < duration:
            iter_start = time.time()
            try:
                # Pick random odds from ladder (only positive odds for MMs)
                mm_odds = random.choice([o for o in odds_ladder if o > 0])
                
                # Track placement time for delay verification
                placement_time = time.time()
                
                # Place wager
                result = framework.place_wager(account_name, line_id, mm_odds, 2.0)
                
                reqs_this_iter = 1  # one place call
                response_time = time.time()
                
                if result.get('success'):
                    count += 1
                    wager_data = result.get('data', {})
                    wager = wager_data.get('wager', {})
                    wager_id = wager.get('id') or wager.get('wager_id')
                    external_id = wager.get('external_id')
                    placed_odds = wager.get('odds', result.get('odds'))
                    wager_created_at = wager.get('created_at')
                    
                    # Calculate actual processing delay (from request to creation)
                    processing_delay = response_time - placement_time
                    
                    # Track this for delay verification
                    with bet_delays_lock:
                        bet_placement_times.append({
                            'account': account_name,
                            'wager_id': wager_id,
                            'placement_time': placement_time,
                            'response_time': response_time,
                            'processing_delay': processing_delay,
                            'created_at': wager_created_at
                        })
                    
                    wager_info = {
                        'account': account_name,
                        'wager_id': wager_id,
                        'external_id': external_id,
                        'placed_at': placement_time,
                        'response_at': response_time,
                        'processing_delay': processing_delay,
                        'odds': placed_odds,
                        'line_id': line_id,
                        'data': wager_data
                    }
                    
                    with lock:
                        mm_wagers[account_name].append(wager_info)
                    
                    # Add to queue for patrons to match
                    with queue_lock:
                        mm_wager_queue.append(wager_info)
                    
                    # Randomly decide to cancel
                    if random.random() < cancel_rate and wager_id and external_id:
                        # Cancel IMMEDIATELY to create race condition with wager_job processing
                        # (no delay - this is when the bug occurs)
                        
                        # Cancel the wager
                        cancel_success = framework.cancel_wager(account_name, external_id, wager_id)
                        reqs_this_iter += 1  # one cancel call
                        
                        if cancel_success:
                            cancelled += 1
                            with lock:
                                cancelled_wagers[account_name].append({
                                    'wager_id': wager_id,
                                    'external_id': external_id,
                                    'cancelled_at': time.time()
                                })
                        
                        if count % 50 == 0:
                            color = Colors.CYAN if account_name == 'mm1' else Colors.MAGENTA
                            print(f"{color}📊 {account_name.upper()}: {count} placed, {cancelled} cancelled{Colors.RESET}")
                    elif count % 50 == 0:
                        color = Colors.BLUE if account_name == 'mm1' else Colors.YELLOW
                        print(f"{color}📊 {account_name.upper()}: {count} placed{Colors.RESET}")
                else:
                    with lock:
                        errors.append({'account': account_name, 'error': result.get('error'), 'type': 'PLACE'})
            
            except Exception as e:
                with lock:
                    errors.append({'account': account_name, 'error': str(e), 'type': 'EXCEPTION'})
            
            # Throttle to target RPS if provided
            if per_mm_rps:
                target_interval = reqs_this_iter / per_mm_rps
                elapsed = time.time() - iter_start
                sleep_for = max(0.0, target_interval - elapsed)
                if sleep_for > 0:
                    time.sleep(sleep_for)
            else:
                # Default aggressive pacing (4x more aggressive)
                time.sleep(random.uniform(0.0025, 0.0075))
        
        return count, cancelled
    
    def patron_matcher_worker(account_name: str):
        """Patron: Try to match MM wagers before they get cancelled"""
        count = 0
        start_time = time.time()
        
        # Skip if no opposite line found
        if not opposite_line_id:
            return 0
        
        while time.time() - start_time < duration:
            try:
                mm_wager = None
                
                # Try to get an MM wager from queue (from either MM1 or MM2)
                with queue_lock:
                    if mm_wager_queue:
                        mm_wager = mm_wager_queue.pop(0)
                
                if mm_wager:
                    # Match with opposite odds on OPPOSITE line_id
                    # Example: MM bets +130 on Team A -> Patron bets -130 on Team B
                    opposite_odds = -mm_wager['odds']
                    
                    result = framework.place_wager(
                        account_name,
                        opposite_line_id,  # Use opposite line, not same line!
                        opposite_odds,
                        2.0
                    )
                    
                    if result.get('success'):
                        count += 1
                        with lock:
                            matched_by_patrons.append({
                                'patron': account_name,
                                'mm_account': mm_wager.get('account', 'unknown'),
                                'mm_wager_id': mm_wager['wager_id'],
                                'matched_at': time.time()
                            })
                    else:
                        with lock:
                            errors.append({'account': account_name, 'error': result.get('error'), 'type': 'MATCH'})
                else:
                    time.sleep(0.005)  # Wait for wagers (10x faster)
                    
            except Exception as e:
                with lock:
                    errors.append({'account': account_name, 'error': str(e), 'type': 'EXCEPTION'})
        
        return count
    
    # Run workers (2 MMs + 2 Patrons)
    with ThreadPoolExecutor(max_workers=4) as executor:
        mm1_future = executor.submit(mm_place_and_cancel_worker, 'mm1')
        mm2_future = executor.submit(mm_place_and_cancel_worker, 'mm2')
        patron1_future = executor.submit(patron_matcher_worker, 'patron_deduce')
        patron2_future = executor.submit(patron_matcher_worker, 'patron_nondeduce')
        
        mm1_placed, mm1_cancelled = mm1_future.result()
        mm2_placed, mm2_cancelled = mm2_future.result()
        patron1_matched = patron1_future.result()
        patron2_matched = patron2_future.result()
    
    total_placed = mm1_placed + mm2_placed
    total_cancelled = mm1_cancelled + mm2_cancelled
    
    print(f"\n{Colors.GREEN}✅ Test completed!{Colors.RESET}")
    print(f"   MM1: {mm1_placed} placed, {mm1_cancelled} cancelled ({mm1_cancelled/mm1_placed*100 if mm1_placed > 0 else 0:.1f}%)")
    print(f"   MM2: {mm2_placed} placed, {mm2_cancelled} cancelled ({mm2_cancelled/mm2_placed*100 if mm2_placed > 0 else 0:.1f}%)")
    print(f"   Total MMs: {total_placed} placed, {total_cancelled} cancelled")
    print(f"   Patron matches: {patron1_matched + patron2_matched} ({patron1_matched} + {patron2_matched})")
    print(f"   Errors: {len(errors)}\n")
    
    # Wait for system to process cancellations
    print(f"{Colors.CYAN}⏳ Waiting 5s for system to process cancellations...{Colors.RESET}\n")
    time.sleep(5)
    
    # Get final balances
    mm1_final_balance_data = framework.get_balance('mm1')
    mm1_final_balance = mm1_final_balance_data.get('balance', 0)
    mm1_final_matched = mm1_final_balance_data.get('matched_wager_balance', 0)
    mm1_final_unmatched = mm1_final_balance_data.get('unmatched_wager_balance', 0)
    
    mm2_final_balance_data = framework.get_balance('mm2')
    mm2_final_balance = mm2_final_balance_data.get('balance', 0)
    
    # Analysis
    print(f"{Colors.BOLD}{'='*70}{Colors.RESET}")
    print(f"{Colors.BOLD}BALANCE VERIFICATION{Colors.RESET}")
    print(f"{Colors.BOLD}{'='*70}{Colors.RESET}\n")
    
    print(f"{Colors.CYAN}MM1 (DEDUCE) Balance:{Colors.RESET}")
    print(f"  Initial balance:   ${mm1_initial_balance:,.2f}")
    print(f"  Final balance:     ${mm1_final_balance:,.2f}")
    print(f"  Change:            ${mm1_initial_balance - mm1_final_balance:,.2f}")
    print()
    
    print(f"{Colors.CYAN}MM1 Matched wager balance:{Colors.RESET}")
    print(f"  Initial matched:   ${mm1_initial_matched:,.2f}")
    print(f"  Final matched:     ${mm1_final_matched:,.2f}")
    mm1_matched_increase = mm1_final_matched - mm1_initial_matched
    print(f"  Increase:          ${mm1_matched_increase:,.2f}")
    print()
    
    print(f"{Colors.CYAN}MM1 Unmatched wager balance:{Colors.RESET}")
    print(f"  Initial unmatched: ${mm1_initial_unmatched:,.2f}")
    print(f"  Final unmatched:   ${mm1_final_unmatched:,.2f}")
    mm1_unmatched_change = mm1_final_unmatched - mm1_initial_unmatched
    print(f"  Change:            ${mm1_unmatched_change:,.2f}")
    print()
    
    print(f"{Colors.CYAN}MM2 (NORMAL) Balance:{Colors.RESET}")
    print(f"  Initial balance:   ${mm2_initial_balance:,.2f}")
    print(f"  Final balance:     ${mm2_final_balance:,.2f}")
    print(f"  Change:            ${mm2_initial_balance - mm2_final_balance:,.2f}")
    print()
    
    # Analyze what happened
    print(f"\n{Colors.BOLD}ANALYSIS:{Colors.RESET}")
    
    if mm1_matched_increase > 0:
        print(f"{Colors.GREEN}✅ MM1 Wagers MATCHED:{Colors.RESET}")
        print(f"   - ${mm1_matched_increase:.2f} worth of wagers got matched")
        print(f"   - matched_wager_balance increased (deduce behavior)\n")
    
    if abs(mm1_unmatched_change) < 0.01 and total_cancelled == 0:
        if total_placed > 0 and mm1_matched_increase > 0:
            print(f"{Colors.YELLOW}⚠️  All wagers matched immediately:{Colors.RESET}")
            print(f"   - Placed {total_placed} wagers")
            print(f"   - All matched before cancellation could occur")
            print(f"   - This is a LIQUID market - wagers match instantly\n")
        else:
            print(f"{Colors.GREEN}✅ All wagers cancelled successfully:{Colors.RESET}")
            print(f"   - Money returned (unmatched balance unchanged)\n")
    elif total_cancelled > 0:
        print(f"{Colors.GREEN}✅ Cancellations worked:{Colors.RESET}")
        print(f"   - {total_cancelled} wagers cancelled (MM1: {mm1_cancelled}, MM2: {mm2_cancelled})")
        print(f"   - Money should be returned\n")
    
    # Expected behavior
    expected_exposure = total_placed * 2.0  # $2 per wager
    actual_exposure = mm1_matched_increase + mm1_unmatched_change
    
    print(f"{Colors.BOLD}EXPECTED vs ACTUAL (MM1):{Colors.RESET}")
    print(f"  MM1 wagers placed: {mm1_placed} x $2.00 = ${mm1_placed * 2.0:.2f}")
    print(f"  MM1 exposure:      ${actual_exposure:.2f}")
    print(f"  Difference:        ${(mm1_placed * 2.0) - actual_exposure:.2f}")
    
    if abs(expected_exposure - actual_exposure) < total_placed * 0.5:  # Within 50% tolerance
        print(f"  {Colors.GREEN}✅ Balance consistent with placed wagers{Colors.RESET}\n")
    else:
        print(f"  {Colors.YELLOW}⚠️  Large difference - some wagers cancelled/returned{Colors.RESET}\n")
    
    # Bet delay verification (5s delay for live events)
    print(f"{Colors.BOLD}{'='*70}{Colors.RESET}")
    print(f"{Colors.BOLD}BET DELAY VERIFICATION (Live Event 5s Delay){Colors.RESET}")
    print(f"{Colors.BOLD}{'='*70}{Colors.RESET}\n")
    
    if bet_placement_times:
        delays = [bt['processing_delay'] for bt in bet_placement_times]
        avg_delay = sum(delays) / len(delays)
        min_delay = min(delays)
        max_delay = max(delays)
        
        # Count how many bets had >= 5s delay
        delays_5s_or_more = [d for d in delays if d >= 5.0]
        delays_less_than_5s = [d for d in delays if d < 5.0]
        
        print(f"{Colors.CYAN}Bet Processing Delay Statistics:{Colors.RESET}")
        print(f"  Total bets tracked: {len(delays)}")
        print(f"  Average delay:      {avg_delay:.3f}s")
        print(f"  Min delay:          {min_delay:.3f}s")
        print(f"  Max delay:          {max_delay:.3f}s")
        print(f"  Median delay:       {sorted(delays)[len(delays)//2]:.3f}s")
        print()
        
        print(f"{Colors.CYAN}5-Second Delay Distribution:{Colors.RESET}")
        print(f"  Bets with ≥5s delay:  {len(delays_5s_or_more)} ({len(delays_5s_or_more)/len(delays)*100:.1f}%)")
        print(f"  Bets with <5s delay:  {len(delays_less_than_5s)} ({len(delays_less_than_5s)/len(delays)*100:.1f}%)")
        print()
        
        # Verification status
        if len(delays_5s_or_more) > len(delays) * 0.9:  # 90% or more have 5s+ delay
            print(f"{Colors.GREEN}✅ PASS: 5-second bet delay is in place for live events{Colors.RESET}")
            print(f"   {len(delays_5s_or_more)}/{len(delays)} bets had ≥5s delay")
        elif len(delays_5s_or_more) > len(delays) * 0.5:  # 50-90% have delay
            print(f"{Colors.YELLOW}⚠️  PARTIAL: Some bets have 5s delay, but not all{Colors.RESET}")
            print(f"   {len(delays_5s_or_more)}/{len(delays)} bets had ≥5s delay")
            print(f"   Expected: >90% of bets should have ≥5s delay for live events")
        else:
            print(f"{Colors.RED}❌ FAIL: 5-second delay NOT detected{Colors.RESET}")
            print(f"   Only {len(delays_5s_or_more)}/{len(delays)} bets had ≥5s delay")
            print(f"   Expected: >90% of bets should have ≥5s delay for live events")
        
        # Show sample of delays
        print(f"\n{Colors.CYAN}Sample delays (first 10 bets):{Colors.RESET}")
        for i, bt in enumerate(bet_placement_times[:10]):
            delay = bt['processing_delay']
            status_icon = "✅" if delay >= 5.0 else "⚠️"
            print(f"  {status_icon} Bet {i+1}: {delay:.3f}s delay ({bt['account']})")
        print()
    else:
        print(f"{Colors.YELLOW}No bet placement times tracked{Colors.RESET}\n")
    
    # Bug verification
    print(f"{Colors.BOLD}{'='*70}{Colors.RESET}")
    print(f"{Colors.BOLD}DATABASE BUG VERIFICATION{Colors.RESET}")
    print(f"{Colors.BOLD}{'='*70}{Colors.RESET}\n")
    
    print(f"{Colors.YELLOW}To verify this bug, check the database:{Colors.RESET}\n")
    
    all_cancelled = cancelled_wagers['mm1'] + cancelled_wagers['mm2']
    if all_cancelled:
        print(f"{Colors.CYAN}Example cancelled wagers (check these for stuck wager_jobs):{Colors.RESET}")
        for i, wager in enumerate(all_cancelled[:5]):  # Show first 5
            wager_id = wager['wager_id']
            print(f"  {i+1}. Wager ID: {wager_id}")
        
        # Generate SQL queries for verification
        sample_wager_ids = [w['wager_id'] for w in all_cancelled[:10]]
        wager_ids_str = ','.join(map(str, filter(None, sample_wager_ids)))
        
        if wager_ids_str:
            print(f"\n{Colors.YELLOW}SQL to check for the bug:{Colors.RESET}\n")
            
            print(f"{Colors.CYAN}1. Check wager status (should be 'cancelled'):{Colors.RESET}")
            print(f"   SELECT id, status, created_at FROM wagers")
            print(f"   WHERE id IN ({wager_ids_str});\n")
            
            print(f"{Colors.CYAN}2. Check wager_jobs status (BUG: some may be 'pending'):{Colors.RESET}")
            print(f"   SELECT wager_id, job_type, status, created_at FROM wager_jobs")
            print(f"   WHERE wager_id IN ({wager_ids_str})")
            print(f"   AND job_type = 'open';\n")
            
            print(f"{Colors.CYAN}3. Find stuck jobs (wager cancelled but job pending):{Colors.RESET}")
            print(f"   SELECT w.id, w.status as wager_status,")
            print(f"          wj.job_type, wj.status as job_status")
            print(f"   FROM wagers w")
            print(f"   JOIN wager_jobs wj ON w.id = wj.wager_id")
            print(f"   WHERE w.id IN ({wager_ids_str})")
            print(f"   AND w.status = 'cancelled'")
            print(f"   AND wj.status = 'pending'")
            print(f"   AND wj.job_type = 'open';\n")
        
        print(f"{Colors.RED}{Colors.BOLD}EXPECTED BUG BEHAVIOR:{Colors.RESET}")
        print(f"  • Wagers will show status='cancelled'")
        print(f"  • But wager_jobs with type='open' will be stuck at status='pending'")
        print(f"  • This is the race condition bug!\n")
    else:
        print(f"{Colors.YELLOW}No wagers were cancelled (increase cancel_rate or duration){Colors.RESET}\n")
    
    # Save report
    bet_delay_stats = None
    if bet_placement_times:
        delays = [bt['processing_delay'] for bt in bet_placement_times]
        bet_delay_stats = {
            'total_bets': len(delays),
            'avg_delay': sum(delays) / len(delays),
            'min_delay': min(delays),
            'max_delay': max(delays),
            'median_delay': sorted(delays)[len(delays)//2],
            'delays_5s_or_more': len([d for d in delays if d >= 5.0]),
            'delays_less_than_5s': len([d for d in delays if d < 5.0]),
            'all_delays': delays,
            'sample_bets': bet_placement_times[:20]  # First 20 for inspection
        }
    
    report = {
        'test': 'cancel_race_wager_job_bug',
        'duration': duration,
        'cancel_rate': cancel_rate,
        'event_id': event_id,
        'mm1_line_id': line_id,
        'opposite_line_id': opposite_line_id,
        'mm1_placed_count': mm1_placed,
        'mm2_placed_count': mm2_placed,
        'total_placed_count': total_placed,
        'mm1_cancelled_count': mm1_cancelled,
        'mm2_cancelled_count': mm2_cancelled,
        'total_cancelled_count': total_cancelled,
        'patron_matches': patron1_matched + patron2_matched,
        'patron_match_details': matched_by_patrons,
        'mm_wagers': mm_wagers,
        'cancelled_wagers': cancelled_wagers,
        'errors': errors,
        'bet_delay_verification': bet_delay_stats,
        'sql_verification_wager_ids': [w['wager_id'] for w in all_cancelled[:20]],
        'balance_verification': {
            'mm1': {
                'initial': {
                    'balance': mm1_initial_balance,
                    'matched': mm1_initial_matched,
                    'unmatched': mm1_initial_unmatched
                },
                'final': {
                    'balance': mm1_final_balance,
                    'matched': mm1_final_matched,
                    'unmatched': mm1_final_unmatched
                },
                'changes': {
                    'balance_change': mm1_initial_balance - mm1_final_balance,
                    'matched_increase': mm1_matched_increase,
                    'unmatched_change': mm1_unmatched_change
                }
            },
            'mm2': {
                'initial': {'balance': mm2_initial_balance},
                'final': {'balance': mm2_final_balance},
                'changes': {'balance_change': mm2_initial_balance - mm2_final_balance}
            }
        }
    }
    
    report_file = f"race_test_cancel_bug_{int(time.time())}.json"
    with open(report_file, 'w') as f:
        json.dump(report, f, indent=2)
    
    print(f"{Colors.GREEN}📄 Report saved: {report_file}{Colors.RESET}")
    print(f"{Colors.CYAN}   Contains wager IDs for DB verification{Colors.RESET}\n")
    
    # Return stats for retry wrapper
    return {
        'mm1_placed': mm1_placed,
        'mm1_cancelled': mm1_cancelled,
        'mm2_placed': mm2_placed,
        'mm2_cancelled': mm2_cancelled,
        'patron_matches': patron1_matched + patron2_matched,
        'errors': len(errors)
    }


def test_aggressive_all_lines(duration=60, event_id=None):
    """
    Test Case: Aggressive All-Lines Betting
    
    Scenario:
    - All accounts (deduce + non-deduce, MM + patron) fetch ALL line IDs from an event
    - Each account aggressively places bets on EVERY available line
    - Line IDs are refreshed periodically (every 5s) as they change
    - All accounts compete naturally in the market
    - Verify deduce accounts' matched_wager_balance increases correctly
    
    This creates realistic race conditions without coordinated matching.
    """
    print(f"\n{Colors.BOLD}{Colors.MAGENTA}{'='*70}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.MAGENTA}Test: Aggressive All-Lines Betting{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.MAGENTA}All accounts spam ALL lines in event simultaneously{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.MAGENTA}{'='*70}{Colors.RESET}\n")
    
    framework = DeduceTestFramework(environment=config.ENVIRONMENT)
    test = RaceConditionTest(framework)
    
    # Setup accounts
    print(f"{Colors.CYAN}📦 Setting up accounts...{Colors.RESET}\n")
    
    accounts = {}
    
    # MM accounts
    mm1_creds = config.get_account_credentials(1, config.ENVIRONMENT)
    framework.login_account('mm1', mm1_creds, account_type='mm')
    accounts['mm1'] = {'type': 'mm', 'behavior': 'deduce'}
    
    mm2_creds = config.get_account_credentials(2, config.ENVIRONMENT)
    framework.login_account('mm2', mm2_creds, account_type='mm')
    accounts['mm2'] = {'type': 'mm', 'behavior': 'normal'}
    
    # Patron accounts
    patron_deduce_creds = {'email': 'deduct.sanbox.test1@yopmail.com', 'password': 'Matkhau1$'}
    framework.login_account('patron_deduce', patron_deduce_creds, account_type='patron')
    accounts['patron_deduce'] = {'type': 'patron', 'behavior': 'deduce'}
    
    patron_config = config.load_user_config(f'user_info_patron_{config.ENVIRONMENT}.json')
    patron_nondeduce_creds = {'username': patron_config['email'], 'password': patron_config['password']}
    framework.login_account('patron_nondeduce', patron_nondeduce_creds, account_type='patron')
    accounts['patron_nondeduce'] = {'type': 'patron', 'behavior': 'normal'}
    
    account_names = list(accounts.keys())
    
    # Get initial balances
    print(f"{Colors.CYAN}💰 Initial balances:{Colors.RESET}")
    initial_snapshot = test.snapshot_balances(account_names, 'initial')
    
    for acc_name in account_names:
        balance = initial_snapshot['balances'][acc_name]
        if balance is not None:
            behavior = accounts[acc_name]['behavior']
            acc_type = accounts[acc_name]['type']
            print(f"  {acc_name} ({behavior} {acc_type}): ${balance:,.2f}")
    print()
    
    # Get event - if not provided, find one
    if not event_id:
        print(f"{Colors.CYAN}🔍 Finding event...{Colors.RESET}")
        market_info = framework.get_available_market('mm1')
        if not market_info:
            print(f"{Colors.RED}❌ No markets available{Colors.RESET}")
            return
        event_id = market_info.get('event', {}).get('event_id')
        event_name = market_info.get('event', {}).get('name', 'Unknown')
        print(f"{Colors.GREEN}✅ Event: {event_name} (ID: {event_id}){Colors.RESET}\n")
    else:
        print(f"{Colors.CYAN}Using provided event ID: {event_id}{Colors.RESET}\n")
    
    # Shared data structures
    errors = []
    wager_counts = {acc: 0 for acc in account_names}
    line_refresh_count = 0
    lock = threading.Lock()
    stop_flag = threading.Event()
    
    # Shared line ID cache with timestamp
    line_cache = {'line_ids': [], 'last_updated': 0, 'lock': threading.Lock()}
    
    def fetch_all_line_ids():
        """Fetch all line IDs from the event using public API"""
        try:
            import requests
            from urllib.parse import urljoin
            
            # Public endpoint - no auth needed
            url = urljoin(framework.base_url, f'trade/public/api/v2/events/{event_id}/markets')
            headers = {
                '__source': 'web',
                'accept': 'application/json'
            }
            
            response = requests.get(url, headers=headers)
            
            if response.status_code == 200:
                data = response.json()
                markets = data.get('data', {}).get('markets', [])
                
                line_ids = []
                for market in markets:
                    # Extract lineID from outcomes
                    for outcome in market.get('outcomes', []):
                        if 'lineID' in outcome:
                            line_ids.append(outcome['lineID'])
                    
                    # Also check market lines (for multi-line markets like totals/spreads)
                    for market_line in market.get('marketLines', []):
                        for outcome in market_line.get('outcomes', []):
                            if 'lineID' in outcome:
                                line_ids.append(outcome['lineID'])
                
                # Remove duplicates
                line_ids = list(set(line_ids))
                return line_ids
            else:
                logging.warning(f"Failed to fetch line IDs: {response.status_code}")
                return []
        except Exception as e:
            logging.error(f"Error fetching line IDs: {e}")
            return []
    
    def line_refresher():
        """Background thread to refresh line IDs periodically"""
        nonlocal line_refresh_count
        
        while not stop_flag.is_set():
            new_line_ids = fetch_all_line_ids()
            
            if new_line_ids:
                with line_cache['lock']:
                    old_count = len(line_cache['line_ids'])
                    line_cache['line_ids'] = new_line_ids
                    line_cache['last_updated'] = time.time()
                    line_refresh_count += 1
                    
                    if line_refresh_count % 5 == 0:  # Print every 5 refreshes
                        print(f"{Colors.YELLOW}🔄 Line refresh #{line_refresh_count}: " +
                              f"{len(new_line_ids)} lines (was {old_count}){Colors.RESET}")
            
            # Wait 5 seconds before next refresh
            for _ in range(50):  # Check stop flag every 0.1s
                if stop_flag.is_set():
                    break
                time.sleep(0.1)
    
    def aggressive_betting_worker(account_name: str):
        """Worker that aggressively bets on all available lines"""
        count = 0
        start_time = time.time()
        
        # Randomize odds for variety
        odds_pool = [150, -150, 200, -200, 180, -180]
        
        while time.time() - start_time < duration:
            try:
                # Get current line IDs
                with line_cache['lock']:
                    current_lines = line_cache['line_ids'].copy()
                
                if not current_lines:
                    time.sleep(0.5)
                    continue
                
                # Pick a random line to bet on
                line_id = random.choice(current_lines)
                odds = random.choice(odds_pool)
                
                result = framework.place_wager(account_name, line_id, odds, 1.0)
                
                if result.get('success'):
                    count += 1
                    with lock:
                        wager_counts[account_name] = count
                    
                    if count % 20 == 0:
                        behavior = accounts[account_name]['behavior']
                        color = Colors.CYAN if behavior == 'deduce' else Colors.BLUE
                        print(f"{color}📊 {account_name}: {count} bets{Colors.RESET}")
                else:
                    error_msg = result.get('error', 'Unknown error')
                    # Only log non-routine errors
                    if 'minimum' not in str(error_msg).lower() and 'suspended' not in str(error_msg).lower():
                        with lock:
                            errors.append({
                                'account': account_name,
                                'error': error_msg,
                                'line_id': line_id,
                                'odds': odds
                            })
                
                # Small random delay to create race conditions
                time.sleep(random.uniform(0.05, 0.15))
                
            except Exception as e:
                with lock:
                    errors.append({'account': account_name, 'error': str(e), 'type': 'EXCEPTION'})
                time.sleep(0.1)
        
        return count
    
    # Start test
    print(f"{Colors.BOLD}🚀 Starting {duration}s aggressive betting test...{Colors.RESET}")
    print(f"{Colors.CYAN}   All accounts will spam ALL lines in the event{Colors.RESET}")
    print(f"{Colors.CYAN}   Lines will be refreshed every 5 seconds{Colors.RESET}\n")
    
    # Initial line fetch
    print(f"{Colors.YELLOW}Fetching initial line IDs...{Colors.RESET}")
    initial_lines = fetch_all_line_ids()
    if not initial_lines:
        print(f"{Colors.RED}❌ Could not fetch line IDs from event{Colors.RESET}")
        return
    
    with line_cache['lock']:
        line_cache['line_ids'] = initial_lines
        line_cache['last_updated'] = time.time()
    
    print(f"{Colors.GREEN}✅ Found {len(initial_lines)} lines to bet on{Colors.RESET}\n")
    
    # Start line refresher thread
    refresher_thread = threading.Thread(target=line_refresher, daemon=True)
    refresher_thread.start()
    
    # Run all workers concurrently
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = []
        
        for acc_name in account_names:
            future = executor.submit(aggressive_betting_worker, acc_name)
            futures.append((acc_name, future))
        
        # Wait for completion
        for acc_name, future in futures:
            try:
                result = future.result()
                print(f"{Colors.GREEN}✅ {acc_name}: Completed {result} wagers{Colors.RESET}")
            except Exception as e:
                print(f"{Colors.RED}❌ {acc_name}: Error - {e}{Colors.RESET}")
    
    # Stop refresher
    stop_flag.set()
    refresher_thread.join(timeout=1)
    
    # Wait for settlements
    print(f"\n{Colors.CYAN}⏳ Waiting 10s for final settlements...{Colors.RESET}")
    time.sleep(10)
    
    # Get final balances with detailed breakdown
    print(f"\n{Colors.CYAN}📸 Taking final balance snapshots...{Colors.RESET}\n")
    
    print(f"{Colors.BOLD}{'='*70}{Colors.RESET}")
    print(f"{Colors.BOLD}FINAL RESULTS{Colors.RESET}")
    print(f"{Colors.BOLD}{'='*70}{Colors.RESET}\n")
    
    for acc_name in account_names:
        behavior = accounts[acc_name]['behavior']
        acc_type = accounts[acc_name]['type']
        initial_bal = initial_snapshot['balances'][acc_name]
        
        # Get detailed balance for MM accounts
        if acc_type == 'mm':
            balance_data = framework.get_balance(acc_name)
            main_bal = balance_data.get('balance', 0)
            matched_wager_bal = balance_data.get('matched_wager_balance', 0)
            unmatched_wager_bal = balance_data.get('unmatched_wager_balance', 0)
            available_bal = main_bal - matched_wager_bal - unmatched_wager_bal
            
            color = Colors.CYAN if behavior == 'deduce' else Colors.BLUE
            
            print(f"{color}{Colors.BOLD}{acc_name.upper()} ({behavior} {acc_type}):{Colors.RESET}")
            print(f"  Wagers placed:        {wager_counts[acc_name]}")
            print(f"  Main balance:         ${main_bal:,.2f} (initial: ${initial_bal:,.2f})")
            print(f"  Matched exposure:     ${matched_wager_bal:,.2f}")
            print(f"  Unmatched exposure:   ${unmatched_wager_bal:,.2f}")
            print(f"  Available balance:    ${available_bal:,.2f}")
            
            if behavior == 'deduce':
                # For deduce accounts, check if matched_wager_balance increased
                if matched_wager_bal > 0:
                    print(f"  {Colors.GREEN}✅ DEDUCE WORKING: Matched exposure tracked separately{Colors.RESET}")
                else:
                    print(f"  {Colors.YELLOW}⚠️  No matched exposure yet{Colors.RESET}")
            else:
                # For normal accounts, main balance should decrease
                balance_change = initial_bal - main_bal
                if balance_change > 0:
                    print(f"  Balance decreased:    ${balance_change:,.2f}")
                    print(f"  {Colors.GREEN}✅ NORMAL BEHAVIOR: Balance deducted immediately{Colors.RESET}")
            
            print()
        else:
            # Patron accounts - simpler balance
            balance_data = framework.get_balance(acc_name)
            final_bal = balance_data.get('balance', 0)
            change = initial_bal - final_bal
            
            color = Colors.CYAN if behavior == 'deduce' else Colors.BLUE
            
            print(f"{color}{Colors.BOLD}{acc_name.upper()} ({behavior} {acc_type}):{Colors.RESET}")
            print(f"  Wagers placed:   {wager_counts[acc_name]}")
            print(f"  Initial balance: ${initial_bal:,.2f}")
            print(f"  Final balance:   ${final_bal:,.2f}")
            print(f"  Change:          ${change:,.2f}")
            print()
    
    # Error summary
    print(f"{Colors.BOLD}ERROR SUMMARY:{Colors.RESET}")
    print(f"  Total errors:      {len(errors)}")
    print(f"  Line refreshes:    {line_refresh_count}")
    
    if errors:
        # Group errors by account
        error_by_account = {}
        for err in errors:
            acc = err['account']
            error_by_account[acc] = error_by_account.get(acc, 0) + 1
        
        print(f"\n  Errors by account:")
        for acc, count in error_by_account.items():
            print(f"    {acc}: {count}")
    
    print()
    
    # Save report
    report = {
        'test': 'aggressive_all_lines',
        'duration': duration,
        'event_id': event_id,
        'accounts': accounts,
        'wager_counts': wager_counts,
        'line_refresh_count': line_refresh_count,
        'total_lines_found': len(line_cache['line_ids']),
        'errors': errors[:100]  # Limit error list size
    }
    
    report_file = f"race_test_aggressive_lines_{int(time.time())}.json"
    with open(report_file, 'w') as f:
        json.dump(report, f, indent=2)
    
    print(f"{Colors.GREEN}📄 Report saved: {report_file}{Colors.RESET}\n")


def test_live_event_5s_delay(event_id):
    """
    Test Case: Live Event 5-Second Bet Delay
    
    This test verifies that the 5-second delay is in place for live events by:
    1. Placing a bet and trying to cancel it before 5s (should fail)
    2. Placing a bet and trying to cancel it after 5s (should succeed)
    3. Placing back-to-back bets - the 2nd bet should be delayed until 1st completes
    
    Args:
        event_id: The live event ID to test against
    """
    print(f"\n{Colors.BOLD}{Colors.MAGENTA}{'='*70}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.MAGENTA}Test: Live Event 5-Second Bet Delay Verification{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.MAGENTA}Event ID: {event_id}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.MAGENTA}{'='*70}{Colors.RESET}\n")
    
    framework = DeduceTestFramework(environment=config.ENVIRONMENT)
    
    # Setup MM1 account
    print(f"{Colors.CYAN}📦 Setting up account...{Colors.RESET}\n")
    mm1_creds = config.get_account_credentials(1, config.ENVIRONMENT)
    framework.login_account('mm1', mm1_creds, account_type='mm')
    print(f"{Colors.GREEN}✅ MM1 logged in{Colors.RESET}\n")
    
    # Get market for the event
    print(f"{Colors.CYAN}🔍 Getting market for event {event_id}...{Colors.RESET}")
    import requests
    from urllib.parse import urljoin
    
    multiple_markets_url = urljoin(framework.base_url, config.URL['mm_multiple_markets'])
    markets_response = requests.get(
        multiple_markets_url,
        params={'event_ids': str(event_id)},
        headers=framework.get_auth_header('mm1')
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
    
    print(f"{Colors.GREEN}✅ Event: {event_name}{Colors.RESET}")
    print(f"{Colors.GREEN}✅ Line ID: {line_id[:16]}...{Colors.RESET}\n")
    
    # Fetch odds ladder
    odds_ladder = get_odds_ladder(framework)
    positive_odds = [o for o in odds_ladder if o > 0]
    
    test_results = []
    
    # TEST 1: Place bet and cancel before 5s (should fail or be delayed)
    print(f"{Colors.BOLD}{'='*70}{Colors.RESET}")
    print(f"{Colors.BOLD}TEST 1: Cancel Before 5 Seconds{Colors.RESET}")
    print(f"{Colors.BOLD}{'='*70}{Colors.RESET}\n")
    
    print(f"{Colors.CYAN}Placing bet...{Colors.RESET}")
    bet1_place_time = time.time()
    result1 = framework.place_wager('mm1', line_id, random.choice(positive_odds), 2.0)
    
    if result1.get('success'):
        wager1_data = result1.get('data', {})
        wager1 = wager1_data.get('wager', {})
        wager1_id = wager1.get('id') or wager1.get('wager_id')
        external_id1 = wager1.get('external_id')
        
        print(f"{Colors.GREEN}✅ Bet placed (ID: {wager1_id}){Colors.RESET}")
        print(f"{Colors.YELLOW}⏱️  Waiting 2 seconds before canceling...{Colors.RESET}")
        time.sleep(2)
        
        print(f"{Colors.CYAN}Attempting to cancel (at ~2s after placement)...{Colors.RESET}")
        cancel1_time = time.time()
        time_elapsed = cancel1_time - bet1_place_time
        
        cancel1_success = framework.cancel_wager('mm1', external_id1, wager1_id)
        
        print(f"\n{Colors.BOLD}Result:{Colors.RESET}")
        print(f"  Time elapsed: {time_elapsed:.2f}s")
        
        if cancel1_success:
            print(f"  {Colors.YELLOW}⚠️  Cancel SUCCEEDED at {time_elapsed:.2f}s{Colors.RESET}")
            print(f"  Expected: Cancel should fail/delay for live events with 5s delay")
            test_results.append({'test': 'cancel_before_5s', 'passed': False, 'time': time_elapsed})
        else:
            print(f"  {Colors.GREEN}✅ Cancel FAILED/DELAYED at {time_elapsed:.2f}s{Colors.RESET}")
            print(f"  This is expected for live events with 5s delay")
            test_results.append({'test': 'cancel_before_5s', 'passed': True, 'time': time_elapsed})
    else:
        print(f"{Colors.RED}❌ Failed to place bet: {result1.get('error')}{Colors.RESET}")
        test_results.append({'test': 'cancel_before_5s', 'passed': False, 'error': 'placement_failed'})
    
    print()
    time.sleep(5)  # Wait between tests
    
    # TEST 2: Place bet and cancel after 5s (should succeed)
    print(f"{Colors.BOLD}{'='*70}{Colors.RESET}")
    print(f"{Colors.BOLD}TEST 2: Cancel After 5 Seconds{Colors.RESET}")
    print(f"{Colors.BOLD}{'='*70}{Colors.RESET}\n")
    
    print(f"{Colors.CYAN}Placing bet...{Colors.RESET}")
    bet2_place_time = time.time()
    result2 = framework.place_wager('mm1', line_id, random.choice(positive_odds), 2.0)
    
    if result2.get('success'):
        wager2_data = result2.get('data', {})
        wager2 = wager2_data.get('wager', {})
        wager2_id = wager2.get('id') or wager2.get('wager_id')
        external_id2 = wager2.get('external_id')
        
        print(f"{Colors.GREEN}✅ Bet placed (ID: {wager2_id}){Colors.RESET}")
        print(f"{Colors.YELLOW}⏱️  Waiting 6 seconds before canceling...{Colors.RESET}")
        time.sleep(6)
        
        print(f"{Colors.CYAN}Attempting to cancel (at ~6s after placement)...{Colors.RESET}")
        cancel2_time = time.time()
        time_elapsed = cancel2_time - bet2_place_time
        
        cancel2_success = framework.cancel_wager('mm1', external_id2, wager2_id)
        
        print(f"\n{Colors.BOLD}Result:{Colors.RESET}")
        print(f"  Time elapsed: {time_elapsed:.2f}s")
        
        if cancel2_success:
            print(f"  {Colors.GREEN}✅ Cancel SUCCEEDED at {time_elapsed:.2f}s{Colors.RESET}")
            print(f"  This is expected after the 5s delay period")
            test_results.append({'test': 'cancel_after_5s', 'passed': True, 'time': time_elapsed})
        else:
            print(f"  {Colors.YELLOW}⚠️  Cancel FAILED at {time_elapsed:.2f}s{Colors.RESET}")
            print(f"  Unexpected: Cancel should succeed after 5s delay")
            test_results.append({'test': 'cancel_after_5s', 'passed': False, 'time': time_elapsed})
    else:
        print(f"{Colors.RED}❌ Failed to place bet: {result2.get('error')}{Colors.RESET}")
        test_results.append({'test': 'cancel_after_5s', 'passed': False, 'error': 'placement_failed'})
    
    print()
    time.sleep(3)
    
    # TEST 3: Back-to-back bets (2nd should be delayed)
    print(f"{Colors.BOLD}{'='*70}{Colors.RESET}")
    print(f"{Colors.BOLD}TEST 3: Back-to-Back Bet Delay{Colors.RESET}")
    print(f"{Colors.BOLD}{'='*70}{Colors.RESET}\n")
    
    print(f"{Colors.CYAN}Placing 1st bet...{Colors.RESET}")
    bet3a_start = time.time()
    result3a = framework.place_wager('mm1', line_id, random.choice(positive_odds), 2.0)
    bet3a_end = time.time()
    bet3a_duration = bet3a_end - bet3a_start
    
    if result3a.get('success'):
        print(f"{Colors.GREEN}✅ 1st bet placed in {bet3a_duration:.3f}s{Colors.RESET}")
        
        print(f"{Colors.CYAN}Immediately placing 2nd bet...{Colors.RESET}")
        bet3b_start = time.time()
        result3b = framework.place_wager('mm1', line_id, random.choice(positive_odds), 2.0)
        bet3b_end = time.time()
        bet3b_duration = bet3b_end - bet3b_start
        total_time_between = bet3b_end - bet3a_start
        
        if result3b.get('success'):
            print(f"{Colors.GREEN}✅ 2nd bet placed in {bet3b_duration:.3f}s{Colors.RESET}")
            print(f"\n{Colors.BOLD}Result:{Colors.RESET}")
            print(f"  1st bet duration: {bet3a_duration:.3f}s")
            print(f"  2nd bet duration: {bet3b_duration:.3f}s")
            print(f"  Total time:       {total_time_between:.3f}s")
            
            if bet3b_duration >= 4.5:  # Allow some tolerance (4.5s instead of strict 5s)
                print(f"  {Colors.GREEN}✅ 2nd bet was DELAYED (~5s as expected){Colors.RESET}")
                print(f"  The 5-second delay prevented immediate submission")
                test_results.append({'test': 'back_to_back_delay', 'passed': True, 'delay': bet3b_duration})
            else:
                print(f"  {Colors.YELLOW}⚠️  2nd bet was NOT significantly delayed{Colors.RESET}")
                print(f"  Expected: ~5s delay for live events")
                test_results.append({'test': 'back_to_back_delay', 'passed': False, 'delay': bet3b_duration})
        else:
            print(f"{Colors.RED}❌ 2nd bet failed: {result3b.get('error')}{Colors.RESET}")
            test_results.append({'test': 'back_to_back_delay', 'passed': False, 'error': '2nd_bet_failed'})
    else:
        print(f"{Colors.RED}❌ 1st bet failed: {result3a.get('error')}{Colors.RESET}")
        test_results.append({'test': 'back_to_back_delay', 'passed': False, 'error': '1st_bet_failed'})
    
    # Final summary
    print(f"\n{Colors.BOLD}{'='*70}{Colors.RESET}")
    print(f"{Colors.BOLD}SUMMARY: 5-Second Delay Verification{Colors.RESET}")
    print(f"{Colors.BOLD}{'='*70}{Colors.RESET}\n")
    
    passed_tests = sum(1 for r in test_results if r.get('passed', False))
    total_tests = len(test_results)
    
    for result in test_results:
        test_name = result['test']
        passed = result.get('passed', False)
        status = f"{Colors.GREEN}✅ PASS" if passed else f"{Colors.RED}❌ FAIL"
        
        if test_name == 'cancel_before_5s':
            time_str = f" (at {result.get('time', 0):.2f}s)" if 'time' in result else ""
            print(f"{status}{Colors.RESET} - Cancel before 5s should fail{time_str}")
        elif test_name == 'cancel_after_5s':
            time_str = f" (at {result.get('time', 0):.2f}s)" if 'time' in result else ""
            print(f"{status}{Colors.RESET} - Cancel after 5s should succeed{time_str}")
        elif test_name == 'back_to_back_delay':
            delay_str = f" (2nd bet took {result.get('delay', 0):.3f}s)" if 'delay' in result else ""
            print(f"{status}{Colors.RESET} - Back-to-back bets should delay{delay_str}")
    
    print(f"\n{Colors.BOLD}Overall: {passed_tests}/{total_tests} tests passed{Colors.RESET}")
    
    if passed_tests == total_tests:
        print(f"{Colors.GREEN}\n✅ 5-SECOND DELAY IS IN PLACE FOR THIS LIVE EVENT{Colors.RESET}\n")
    elif passed_tests >= total_tests / 2:
        print(f"{Colors.YELLOW}\n⚠️  PARTIAL: Some delay behavior detected but not all tests passed{Colors.RESET}\n")
    else:
        print(f"{Colors.RED}\n❌ 5-SECOND DELAY NOT DETECTED FOR THIS EVENT{Colors.RESET}\n")
    
    # Save report
    report = {
        'test': 'live_event_5s_delay',
        'event_id': event_id,
        'event_name': event_name,
        'line_id': line_id,
        'results': test_results,
        'passed': passed_tests,
        'total': total_tests
    }
    
    report_file = f"live_event_5s_delay_{event_id}_{int(time.time())}.json"
    with open(report_file, 'w') as f:
        json.dump(report, f, indent=2)
    
    print(f"{Colors.GREEN}📄 Report saved: {report_file}{Colors.RESET}\n")


def test_flexible_cancel_race(duration=30, cancel_rate=0.5, event_id=None, use_deduce=True, use_mm1=True, use_mm2=True, use_patron=True):
    """
    Flexible Cancel Race Test - Can toggle deduce/non-deduce accounts
    
    Args:
        duration: Test duration in seconds
        cancel_rate: Probability of canceling each wager (0.0-1.0)
        event_id: Specific event ID to test (optional)
        use_deduce: If True, use deduce accounts; if False, use only non-deduce
        use_mm1: Include MM1 account (deduce)
        use_mm2: Include MM2 account (non-deduce)
        use_patron: Include Patron account(s)
    """
    print(f"\n{Colors.BOLD}{Colors.MAGENTA}{'='*70}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.MAGENTA}Test: Flexible Cancel Race{Colors.RESET}")
    if event_id:
        print(f"{Colors.BOLD}{Colors.MAGENTA}Event ID: {event_id}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.MAGENTA}Mode: {'DEDUCE + NON-DEDUCE' if use_deduce else 'NON-DEDUCE ONLY'}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.MAGENTA}{'='*70}{Colors.RESET}\n")
    
    framework = DeduceTestFramework(environment=config.ENVIRONMENT)
    test = RaceConditionTest(framework)
    
    # Setup accounts based on parameters
    print(f"{Colors.CYAN}📦 Setting up accounts...{Colors.RESET}\n")
    
    accounts = {}
    
    # MM1 (deduce) - only if use_deduce and use_mm1
    if use_deduce and use_mm1:
        mm1_creds = config.get_account_credentials(1, config.ENVIRONMENT)
        framework.login_account('mm1', mm1_creds, account_type='mm')
        accounts['mm1'] = {'type': 'mm', 'behavior': 'deduce'}
        print(f"{Colors.GREEN}✅ MM1 (DEDUCE) logged in{Colors.RESET}")
    
    # MM2 (non-deduce) - only if use_mm2
    if use_mm2:
        mm2_creds = config.get_account_credentials(2, config.ENVIRONMENT)
        framework.login_account('mm2', mm2_creds, account_type='mm')
        accounts['mm2'] = {'type': 'mm', 'behavior': 'normal'}
        print(f"{Colors.GREEN}✅ MM2 (NON-DEDUCE) logged in{Colors.RESET}")
    
    # Patron deduce - only if use_deduce and use_patron
    if use_deduce and use_patron:
        patron_deduce_creds = {'email': 'deduct.sanbox.test1@yopmail.com', 'password': 'Matkhau1$'}
        framework.login_account('patron_deduce', patron_deduce_creds, account_type='patron')
        accounts['patron_deduce'] = {'type': 'patron', 'behavior': 'deduce'}
        print(f"{Colors.GREEN}✅ Patron deduce logged in{Colors.RESET}")
    
    # Patron non-deduce - only if use_patron
    if use_patron:
        patron_config = config.load_user_config(f'user_info_patron_{config.ENVIRONMENT}.json')
        patron_nondeduce_creds = {'username': patron_config['email'], 'password': patron_config['password']}
        framework.login_account('patron_nondeduce', patron_nondeduce_creds, account_type='patron')
        accounts['patron_nondeduce'] = {'type': 'patron', 'behavior': 'normal'}
        print(f"{Colors.GREEN}✅ Patron non-deduce logged in{Colors.RESET}")
    
    if not accounts:
        print(f"{Colors.RED}❌ No accounts configured!{Colors.RESET}")
        return
    
    print()
    
    # Get initial balances
    account_names = list(accounts.keys())
    initial_snapshot = test.snapshot_balances(account_names, 'initial')
    
    for acc_name in account_names:
        acc_info = accounts[acc_name]
        balance = initial_snapshot['balances'][acc_name]
        print(f"{Colors.CYAN}{acc_name} ({acc_info['behavior']} {acc_info['type']}): ${balance:,.2f}{Colors.RESET}")
    print()
    
    # Get market
    if event_id:
        print(f"{Colors.CYAN}🔍 Getting markets for event {event_id}...{Colors.RESET}")
        import requests
        from urllib.parse import urljoin
        
        # Use first MM account for API calls
        mm_account = 'mm1' if 'mm1' in accounts else 'mm2'
        
        multiple_markets_url = urljoin(framework.base_url, config.URL['mm_multiple_markets'])
        markets_response = requests.get(
            multiple_markets_url,
            params={'event_ids': str(event_id)},
            headers=framework.get_auth_header(mm_account)
        )
        
        if markets_response.status_code != 200:
            print(f"{Colors.RED}❌ Failed to get markets{Colors.RESET}")
            return
        
        markets_data = json.loads(markets_response.content).get('data', {})
        event_markets = markets_data.get(str(event_id), [])
        
        if not event_markets:
            print(f"{Colors.RED}❌ No markets available{Colors.RESET}")
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
            print(f"{Colors.RED}❌ Could not find valid line_id{Colors.RESET}")
            return
        
        print(f"{Colors.GREEN}✅ Event: {event_name} (ID: {event_id}){Colors.RESET}")
    else:
        print(f"{Colors.CYAN}🔍 Finding active market...{Colors.RESET}")
        mm_account = 'mm1' if 'mm1' in accounts else 'mm2'
        market_info = framework.get_available_market(mm_account)
        
        if not market_info:
            print(f"{Colors.RED}❌ No markets available{Colors.RESET}")
            return
        
        line_id = market_info['line_id']
        event_id = market_info.get('event', {}).get('event_id')
        event_name = market_info.get('event', {}).get('name', 'Unknown')
        print(f"{Colors.GREEN}✅ Market: {event_name}{Colors.RESET}")
    
    print(f"{Colors.CYAN}Line ID: {line_id[:16]}...{Colors.RESET}\n")
    
    # Fetch odds ladder
    print(f"{Colors.YELLOW}🎯 Fetching odds ladder...{Colors.RESET}")
    odds_ladder = get_odds_ladder(framework)
    print(f"{Colors.GREEN}✅ Loaded {len(odds_ladder)} odds values{Colors.RESET}")
    
    # Get opposite line_id for patrons
    mm_account = 'mm1' if 'mm1' in accounts else 'mm2'
    print(f"{Colors.YELLOW}🔍 Finding opposite line_id...{Colors.RESET}")
    opposite_line_id = get_opposite_line_id(framework, mm_account, event_id, line_id)
    
    if not opposite_line_id:
        print(f"{Colors.YELLOW}⚠️  Using same line_id for patrons{Colors.RESET}\n")
        opposite_line_id = line_id
    else:
        print(f"{Colors.GREEN}✅ Opposite line: {opposite_line_id[:16]}...{Colors.RESET}\n")
    
    # Test execution
    print(f"{Colors.BOLD}🚀 Starting {duration}s place & cancel test...{Colors.RESET}")
    print(f"{Colors.CYAN}   Cancel rate: {cancel_rate*100:.0f}%{Colors.RESET}")
    print(f"{Colors.CYAN}   MM accounts: {', '.join([a for a in accounts if accounts[a]['type'] == 'mm'])}{Colors.RESET}")
    print(f"{Colors.CYAN}   Patron accounts: {', '.join([a for a in accounts if accounts[a]['type'] == 'patron'])}{Colors.RESET}\n")
    
    # Shared data structures
    mm_wagers = {acc: [] for acc in accounts if accounts[acc]['type'] == 'mm'}
    cancelled_wagers = {acc: [] for acc in accounts if accounts[acc]['type'] == 'mm'}
    matched_by_patrons = []
    errors = []
    lock = threading.Lock()
    
    mm_wager_queue = []
    queue_lock = threading.Lock()
    
    bet_placement_times = []
    bet_delays_lock = threading.Lock()
    
    def mm_place_and_cancel_worker(account_name: str):
        """MM worker: Place and randomly cancel wagers"""
        count = 0
        cancelled = 0
        start_time = time.time()
        
        while time.time() - start_time < duration:
            try:
                mm_odds = random.choice([o for o in odds_ladder if o > 0])
                placement_time = time.time()
                
                result = framework.place_wager(account_name, line_id, mm_odds, 2.0)
                response_time = time.time()
                
                if result.get('success'):
                    count += 1
                    wager_data = result.get('data', {})
                    wager = wager_data.get('wager', {})
                    wager_id = wager.get('id') or wager.get('wager_id')
                    external_id = wager.get('external_id')
                    
                    processing_delay = response_time - placement_time
                    
                    with bet_delays_lock:
                        bet_placement_times.append({
                            'account': account_name,
                            'wager_id': wager_id,
                            'processing_delay': processing_delay
                        })
                    
                    wager_info = {
                        'account': account_name,
                        'wager_id': wager_id,
                        'external_id': external_id,
                        'odds': mm_odds,
                        'placed_at': placement_time
                    }
                    
                    with lock:
                        mm_wagers[account_name].append(wager_info)
                    
                    with queue_lock:
                        mm_wager_queue.append(wager_info)
                    
                    # Randomly cancel
                    if random.random() < cancel_rate and wager_id and external_id:
                        cancel_success = framework.cancel_wager(account_name, external_id, wager_id)
                        
                        if cancel_success:
                            cancelled += 1
                            with lock:
                                cancelled_wagers[account_name].append(wager_id)
                        
                        if count % 20 == 0:
                            color = Colors.CYAN if accounts[account_name]['behavior'] == 'deduce' else Colors.MAGENTA
                            print(f"{color}📊 {account_name}: {count} placed, {cancelled} cancelled{Colors.RESET}")
                    elif count % 20 == 0:
                        color = Colors.CYAN if accounts[account_name]['behavior'] == 'deduce' else Colors.YELLOW
                        print(f"{color}📊 {account_name}: {count} placed{Colors.RESET}")
                else:
                    with lock:
                        errors.append({'account': account_name, 'error': result.get('error'), 'type': 'PLACE'})
            
            except Exception as e:
                with lock:
                    errors.append({'account': account_name, 'error': str(e), 'type': 'EXCEPTION'})
            
            time.sleep(random.uniform(0.01, 0.03))
        
        return count, cancelled
    
    def patron_matcher_worker(account_name: str):
        """Patron: Match MM wagers"""
        count = 0
        start_time = time.time()
        
        while time.time() - start_time < duration:
            try:
                mm_wager = None
                
                with queue_lock:
                    if mm_wager_queue:
                        mm_wager = mm_wager_queue.pop(0)
                
                if mm_wager:
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
                            color = Colors.CYAN if accounts[account_name]['behavior'] == 'deduce' else Colors.BLUE
                            print(f"{color}📊 {account_name}: {count} matches{Colors.RESET}")
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
    mm_accounts = [a for a in accounts if accounts[a]['type'] == 'mm']
    patron_accounts = [a for a in accounts if accounts[a]['type'] == 'patron']
    
    with ThreadPoolExecutor(max_workers=len(accounts)) as executor:
        futures = {}
        
        # Start MM workers
        for acc in mm_accounts:
            futures[acc] = executor.submit(mm_place_and_cancel_worker, acc)
        
        # Start patron workers
        for acc in patron_accounts:
            futures[acc] = executor.submit(patron_matcher_worker, acc)
        
        # Collect results
        results = {}
        for acc, future in futures.items():
            try:
                results[acc] = future.result()
                if accounts[acc]['type'] == 'mm':
                    placed, cancelled = results[acc]
                    print(f"{Colors.GREEN}✅ {acc}: {placed} placed, {cancelled} cancelled{Colors.RESET}")
                else:
                    matched = results[acc]
                    print(f"{Colors.GREEN}✅ {acc}: {matched} matches{Colors.RESET}")
            except Exception as e:
                print(f"{Colors.RED}❌ {acc}: Error - {e}{Colors.RESET}")
    
    # Wait for system processing
    print(f"\n{Colors.CYAN}⏳ Waiting 5s for system processing...{Colors.RESET}\n")
    time.sleep(5)
    
    # Get final balances
    final_snapshot = test.snapshot_balances(account_names, 'final')
    
    # Analysis
    print(f"{Colors.BOLD}{'='*70}{Colors.RESET}")
    print(f"{Colors.BOLD}RESULTS{Colors.RESET}")
    print(f"{Colors.BOLD}{'='*70}{Colors.RESET}\n")
    
    for acc_name in account_names:
        acc_info = accounts[acc_name]
        initial = initial_snapshot['balances'][acc_name]
        final = final_snapshot['balances'][acc_name]
        change = initial - final
        
        color = Colors.CYAN if acc_info['behavior'] == 'deduce' else Colors.BLUE
        print(f"{color}{acc_name.upper()} ({acc_info['behavior']} {acc_info['type']}){Colors.RESET}")
        print(f"  Initial:  ${initial:,.2f}")
        print(f"  Final:    ${final:,.2f}")
        print(f"  Change:   ${change:,.2f}\n")
    
    # Delay stats
    if bet_placement_times:
        delays = [bt['processing_delay'] for bt in bet_placement_times]
        print(f"{Colors.BOLD}BET DELAY STATS:{Colors.RESET}")
        print(f"  Avg: {sum(delays)/len(delays):.3f}s")
        print(f"  Min: {min(delays):.3f}s")
        print(f"  Max: {max(delays):.3f}s\n")
    
    # Error summary
    if errors:
        print(f"{Colors.BOLD}ERRORS: {len(errors)}{Colors.RESET}")
        error_types = {}
        for err in errors:
            key = f"{err['account']}-{err['type']}"
            error_types[key] = error_types.get(key, 0) + 1
        for key, count in error_types.items():
            print(f"  {key}: {count}")
        print()
    
    # Save report
    report = {
        'test': 'flexible_cancel_race',
        'event_id': event_id,
        'duration': duration,
        'cancel_rate': cancel_rate,
        'use_deduce': use_deduce,
        'accounts': accounts,
        'results': results,
        'errors': errors,
        'bet_delays': bet_placement_times[:20] if bet_placement_times else None,
        'balance_changes': {
            acc: {
                'initial': initial_snapshot['balances'][acc],
                'final': final_snapshot['balances'][acc],
                'change': initial_snapshot['balances'][acc] - final_snapshot['balances'][acc]
            } for acc in account_names
        }
    }
    
    mode_str = 'deduce' if use_deduce else 'nondeduce'
    report_file = f"flexible_cancel_{mode_str}_{event_id or 'auto'}_{int(time.time())}.json"
    with open(report_file, 'w') as f:
        json.dump(report, f, indent=2)
    
    print(f"{Colors.GREEN}📄 Report saved: {report_file}{Colors.RESET}\n")


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Run deduce race condition tests')
    parser.add_argument('--test', choices=['1a', '4way', 'rapid', 'burst', 'patron_mm', 'deduce_matched', 'aggressive', 'cancel_bug', 'live_delay', 'flexible', 'all'],
                       default='all', help='Which test to run')
    parser.add_argument('--duration', type=int, default=30, 
                       help='Test duration in seconds (for applicable tests)')
    parser.add_argument('--rps', type=float, default=None,
                       help='Target total requests per second across MMs (place+cancel). Example: 40')
    parser.add_argument('--event-id', type=int, default=None,
                       help='Specific event ID to use for testing (applies to all tests)')
    parser.add_argument('--aggression', type=float, default=1.0,
                       help='Speed multiplier for certain tests (e.g., patron_mm). 2.0 = ~2x faster pacing')
    parser.add_argument('--no-deduce', action='store_true',
                       help='Exclude deduce accounts (for flexible test)')
    parser.add_argument('--no-mm1', action='store_true',
                       help='Exclude MM1 account (for flexible test)')
    parser.add_argument('--no-mm2', action='store_true',
                       help='Exclude MM2 account (for flexible test)')
    parser.add_argument('--no-patron', action='store_true',
                       help='Exclude patron accounts (for flexible test)')
    
    args = parser.parse_args()
    
    print(f"\n{Colors.BOLD}{Colors.CYAN}{'='*70}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.CYAN}DEDUCE RACE CONDITION TEST SUITE{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.CYAN}{'='*70}{Colors.RESET}\n")
    
    if args.test in ['1a', 'all']:
        test_deduce_sp_vs_nondeduce_sp(duration=args.duration, event_id=args.event_id)
    
    if args.test in ['4way', 'all']:
        test_four_way_mexican_standoff(duration=args.duration, event_id=args.event_id)
    
    if args.test in ['rapid', 'all']:
        test_rapid_fire_race(duration=args.duration, bets_per_second=10, event_id=args.event_id)
    
    if args.test in ['burst', 'all']:
        test_simultaneous_burst(event_id=args.event_id)
    
    if args.test in ['patron_mm', 'all']:
        test_patron_matches_mm_wagers(duration=args.duration, event_id=args.event_id, aggression=args.aggression)
    
    if args.test in ['deduce_matched']:
        test_deduce_accounts_get_matched(duration=args.duration, event_id=args.event_id)
    
    if args.test in ['aggressive']:
        test_aggressive_all_lines(duration=args.duration, event_id=args.event_id)
    
    if args.test in ['cancel_bug']:
        # Use retry wrapper for long runs (> 15 mins), otherwise run directly
        if args.duration > 900:
            test_cancel_race_wager_job_bug_with_retry(duration=args.duration, cancel_rate=0.8, event_id=args.event_id, rps=args.rps)
        else:
            test_cancel_race_wager_job_bug(duration=args.duration, cancel_rate=0.8, event_id=args.event_id, rps=args.rps)
    
    if args.test in ['live_delay']:
        if not args.event_id:
            print(f"{Colors.RED}Error: --event-id is required for live_delay test{Colors.RESET}")
        else:
            test_live_event_5s_delay(event_id=args.event_id)
    
    if args.test in ['flexible']:
        test_flexible_cancel_race(
            duration=args.duration,
            cancel_rate=0.5,
            event_id=args.event_id,
            use_deduce=not args.no_deduce,
            use_mm1=not args.no_mm1,
            use_mm2=not args.no_mm2,
            use_patron=not args.no_patron
        )
    
    print(f"\n{Colors.BOLD}{Colors.GREEN}{'='*70}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.GREEN}ALL TESTS COMPLETED{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.GREEN}{'='*70}{Colors.RESET}\n")
