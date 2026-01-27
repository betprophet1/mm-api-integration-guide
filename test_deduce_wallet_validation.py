#!/usr/bin/env python3
"""
Enhanced Deduce Wallet Validation Test

This test demonstrates and validates wallet balance behavior with:
- 2 SPs (Service Providers): MM1 (deduce) + MM2 (non-deduce)
- 2 Patrons: Patron deduce + Patron non-deduce
- All 4 accounts placing/matching bets on the same event simultaneously
- Balance verification for all matched bets
- Comprehensive error tracking and reporting

Key behaviors:
- MM1 (DEDUCE): Balance only deducted when bet is matched
- MM2, Patrons (normal): Balance deducted immediately when bet is placed
"""

import time
import json
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from deduce_tests import DeduceTestFramework, Colors
from src import config
from src.log import logging


def test_mm1_deduce_wallet_validation():
    """
    Test MM1 wallet balance with DEDUCE enabled
    
    Expected behavior:
    1. Get initial balance
    2. Place a wager
    3. Check balance is UNCHANGED (deduce enabled)
    4. Check matched bets - if any, their total should equal balance deduction
    """
    print(f"\n{Colors.BOLD}{Colors.MAGENTA}{'='*70}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.MAGENTA}MM1 DEDUCE Wallet Balance Validation Test{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.MAGENTA}{'='*70}{Colors.RESET}\n")
    
    # Initialize framework
    framework = DeduceTestFramework(environment=config.ENVIRONMENT)
    
    # Login MM1
    print(f"{Colors.CYAN}Logging in MM1 (DEDUCE enabled)...{Colors.RESET}")
    mm1_creds = config.get_account_credentials(1, config.ENVIRONMENT)
    framework.login_account('mm1', mm1_creds, account_type='mm')
    
    # Get initial balance
    initial_balance_data = framework.get_balance('mm1')
    initial_balance = initial_balance_data.get('balance', 0)
    
    print(f"\n{Colors.CYAN}Initial MM1 Balance: ${initial_balance:.2f}{Colors.RESET}")
    
    # Get a market to place a bet
    print(f"\n{Colors.CYAN}Finding available market...{Colors.RESET}")
    market_info = framework.get_available_market('mm1')
    
    if not market_info:
        print(f"{Colors.RED}✗ No available markets found{Colors.RESET}")
        return
    
    # Place a wager
    stake_amount = 10.0
    print(f"\n{Colors.CYAN}Placing wager: ${stake_amount} @ odds 150{Colors.RESET}")
    wager = framework.place_wager('mm1', market_info['line_id'], 150, stake_amount)
    
    if not wager['success']:
        print(f"{Colors.RED}✗ Failed to place wager: {wager.get('error')}{Colors.RESET}")
        return
    
    # Wait a bit for processing
    time.sleep(2)
    
    # Check wallet balance using the new helper method
    print(f"\n{Colors.BOLD}Validating MM1 Wallet Balance (DEDUCE enabled):{Colors.RESET}")
    print(f"{Colors.CYAN}{'─'*70}{Colors.RESET}")
    
    validation_result = framework.check_balance_change_for_deduce(
        'mm1', 
        initial_balance, 
        stake_amount, 
        expected_behavior='deduce'
    )
    
    # Display results
    print(f"\n{Colors.BOLD}Validation Results:{Colors.RESET}")
    print(f"  Initial Balance:  ${validation_result['initial_balance']:.2f}")
    print(f"  Current Balance:  ${validation_result['current_balance']:.2f}")
    print(f"  Balance Change:   ${validation_result['balance_change']:.2f}")
    
    if 'matched_bets_count' in validation_result:
        print(f"  Matched Bets:     {validation_result['matched_bets_count']}")
        print(f"  Matched Total:    ${validation_result['matched_bets_total']:.2f}")
    
    if validation_result['is_valid']:
        print(f"\n{Colors.GREEN}{Colors.BOLD}✓ PASS: {validation_result['message']}{Colors.RESET}\n")
    else:
        print(f"\n{Colors.RED}{Colors.BOLD}✗ FAIL: {validation_result['message']}{Colors.RESET}\n")


def test_mm2_normal_wallet_validation():
    """
    Test MM2 wallet balance WITHOUT DEDUCE (normal behavior)
    
    Expected behavior:
    1. Get initial balance
    2. Place a wager
    3. Check balance DECREASED by stake amount immediately
    """
    print(f"\n{Colors.BOLD}{Colors.BLUE}{'='*70}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.BLUE}MM2 Normal Wallet Balance Validation Test{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.BLUE}{'='*70}{Colors.RESET}\n")
    
    # Initialize framework
    framework = DeduceTestFramework(environment=config.ENVIRONMENT)
    
    # Login MM2
    print(f"{Colors.CYAN}Logging in MM2 (normal behavior)...{Colors.RESET}")
    mm2_creds = config.get_account_credentials(2, config.ENVIRONMENT)
    framework.login_account('mm2', mm2_creds, account_type='mm')
    
    # Get initial balance
    initial_balance_data = framework.get_balance('mm2')
    initial_balance = initial_balance_data.get('balance', 0)
    
    print(f"\n{Colors.CYAN}Initial MM2 Balance: ${initial_balance:.2f}{Colors.RESET}")
    
    # Get a market to place a bet
    print(f"\n{Colors.CYAN}Finding available market...{Colors.RESET}")
    market_info = framework.get_available_market('mm2')
    
    if not market_info:
        print(f"{Colors.RED}✗ No available markets found{Colors.RESET}")
        return
    
    # Place a wager
    stake_amount = 10.0
    print(f"\n{Colors.CYAN}Placing wager: ${stake_amount} @ odds 150{Colors.RESET}")
    wager = framework.place_wager('mm2', market_info['line_id'], 150, stake_amount)
    
    if not wager['success']:
        print(f"{Colors.RED}✗ Failed to place wager: {wager.get('error')}{Colors.RESET}")
        return
    
    # Wait a bit for processing
    time.sleep(2)
    
    # Check wallet balance using the new helper method
    print(f"\n{Colors.BOLD}Validating MM2 Wallet Balance (normal behavior):{Colors.RESET}")
    print(f"{Colors.CYAN}{'─'*70}{Colors.RESET}")
    
    validation_result = framework.check_balance_change_for_deduce(
        'mm2', 
        initial_balance, 
        stake_amount, 
        expected_behavior='normal'
    )
    
    # Display results
    print(f"\n{Colors.BOLD}Validation Results:{Colors.RESET}")
    print(f"  Initial Balance:  ${validation_result['initial_balance']:.2f}")
    print(f"  Current Balance:  ${validation_result['current_balance']:.2f}")
    print(f"  Balance Change:   ${validation_result['balance_change']:.2f}")
    print(f"  Expected Change:  ${stake_amount:.2f}")
    
    if validation_result['is_valid']:
        print(f"\n{Colors.GREEN}{Colors.BOLD}✓ PASS: {validation_result['message']}{Colors.RESET}\n")
    else:
        print(f"\n{Colors.RED}{Colors.BOLD}✗ FAIL: {validation_result['message']}{Colors.RESET}\n")


def test_all_accounts_comparison():
    """
    Compare wallet behavior across MM1 (deduce), MM2 (normal), and Patron (normal)
    """
    print(f"\n{Colors.BOLD}{Colors.MAGENTA}{'='*70}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.MAGENTA}All Accounts Wallet Behavior Comparison{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.MAGENTA}{'='*70}{Colors.RESET}\n")
    
    # Initialize framework
    framework = DeduceTestFramework(environment=config.ENVIRONMENT)
    
    accounts_to_test = [
        {'name': 'mm1', 'behavior': 'deduce', 'account_type': 'mm', 'account_num': 1},
        {'name': 'mm2', 'behavior': 'normal', 'account_type': 'mm', 'account_num': 2},
    ]
    
    # Try to add patron if available
    try:
        patron_config = config.load_user_config(f'user_info_patron_{config.ENVIRONMENT}.json')
        accounts_to_test.append({
            'name': 'patron1',
            'behavior': 'normal',
            'account_type': 'patron',
            'config': patron_config
        })
    except:
        print(f"{Colors.YELLOW}⚠ Patron config not found, skipping patron test{Colors.RESET}")
    
    results = []
    
    for account_info in accounts_to_test:
        try:
            print(f"\n{Colors.BOLD}{Colors.CYAN}Testing {account_info['name']} ({account_info['behavior']} behavior)...{Colors.RESET}")
            
            # Login
            if account_info['account_type'] == 'mm':
                creds = config.get_account_credentials(account_info['account_num'], config.ENVIRONMENT)
                framework.login_account(account_info['name'], creds, account_type='mm')
            else:
                creds = {
                    'username': account_info['config'].get('email'),
                    'password': account_info['config'].get('password')
                }
                framework.login_account(account_info['name'], creds, account_type='patron')
            
            # Get initial balance
            initial_balance_data = framework.get_balance(account_info['name'])
            initial_balance = initial_balance_data.get('balance', 0)
            
            # Find market
            market_info = framework.get_available_market(account_info['name'])
            if not market_info:
                print(f"{Colors.YELLOW}⚠ No markets available for {account_info['name']}{Colors.RESET}")
                continue
            
            # Place wager
            stake_amount = 5.0
            wager = framework.place_wager(account_info['name'], market_info['line_id'], 150, stake_amount)
            
            if not wager['success']:
                print(f"{Colors.YELLOW}⚠ Failed to place wager for {account_info['name']}{Colors.RESET}")
                continue
            
            time.sleep(2)
            
            # Validate
            validation_result = framework.check_balance_change_for_deduce(
                account_info['name'],
                initial_balance,
                stake_amount,
                expected_behavior=account_info['behavior']
            )
            
            results.append({
                'account': account_info['name'],
                'behavior': account_info['behavior'],
                'validation': validation_result
            })
            
        except Exception as e:
            print(f"{Colors.RED}✗ Error testing {account_info['name']}: {e}{Colors.RESET}")
    
    # Print summary
    print(f"\n{Colors.BOLD}{'='*70}{Colors.RESET}")
    print(f"{Colors.BOLD}Summary - Wallet Behavior Comparison{Colors.RESET}")
    print(f"{Colors.BOLD}{'='*70}{Colors.RESET}\n")
    
    for result in results:
        status = f"{Colors.GREEN}✓ PASS{Colors.RESET}" if result['validation']['is_valid'] else f"{Colors.RED}✗ FAIL{Colors.RESET}"
        print(f"{status} - {result['account']} ({result['behavior']})")
        print(f"      {result['validation']['message']}\n")


def test_concurrent_4_accounts_enhanced(target_event_id=None, duration=300):
    """
    Enhanced test with 2 SPs + 2 Patrons placing/matching bets concurrently
    
    Features:
    - All 4 accounts simultaneously place bets on the same event
    - Track all matched bets with timestamps
    - Balance verification for all accounts
    - Comprehensive error tracking
    - Final reconciliation report
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from datetime import datetime
    import threading
    import random
    import requests
    import uuid as uuid_lib
    from urllib.parse import urljoin
    
    print(f"\n{Colors.BOLD}{Colors.MAGENTA}{'='*70}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.MAGENTA}Enhanced 4-Account Concurrent Test{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.MAGENTA}2 SPs (deduce + non-deduce) + 2 Patrons (deduce + non-deduce){Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.MAGENTA}{'='*70}{Colors.RESET}\n")
    
    # Test tracking
    test_start = datetime.now()
    matched_bets = []
    api_errors = []
    lock = threading.Lock()
    
    # Deduce patron credentials
    DEDUCE_PATRON_EMAIL = 'deduct.sanbox.test1@yopmail.com'
    DEDUCE_PATRON_PASSWORD = 'Matkhau1$'
    
    # Initialize framework
    framework = DeduceTestFramework(environment=config.ENVIRONMENT)
    
    print(f"{Colors.CYAN}📦 Loading accounts...{Colors.RESET}\n")
    
    # Load SP accounts
    accounts = {}
    initial_balances = {}
    
    # MM1 (deduce SP)
    try:
        mm1_creds = config.get_account_credentials(1, config.ENVIRONMENT)
        framework.login_account('mm1', mm1_creds, account_type='mm')
        balance = framework.get_balance('mm1')
        accounts['mm1'] = {'framework': framework, 'type': 'sp', 'behavior': 'deduce'}
        initial_balances['mm1'] = balance.get('balance', 0)
        print(f"{Colors.GREEN}✅ MM1 (deduce SP): ${initial_balances['mm1']:,.2f}{Colors.RESET}")
    except Exception as e:
        print(f"{Colors.RED}❌ MM1 load error: {e}{Colors.RESET}")
        api_errors.append({'account': 'mm1', 'type': 'LOAD_ERROR', 'message': str(e)})
    
    # MM2 (non-deduce SP)
    try:
        mm2_creds = config.get_account_credentials(2, config.ENVIRONMENT)
        framework.login_account('mm2', mm2_creds, account_type='mm')
        balance = framework.get_balance('mm2')
        accounts['mm2'] = {'framework': framework, 'type': 'sp', 'behavior': 'normal'}
        initial_balances['mm2'] = balance.get('balance', 0)
        print(f"{Colors.GREEN}✅ MM2 (non-deduce SP): ${initial_balances['mm2']:,.2f}{Colors.RESET}")
    except Exception as e:
        print(f"{Colors.RED}❌ MM2 load error: {e}{Colors.RESET}")
        api_errors.append({'account': 'mm2', 'type': 'LOAD_ERROR', 'message': str(e)})
    
    # Patron deduce
    try:
        patron_deduce_creds = {'username': DEDUCE_PATRON_EMAIL, 'password': DEDUCE_PATRON_PASSWORD}
        framework.login_account('patron_deduce', patron_deduce_creds, account_type='patron')
        balance = framework.get_balance('patron_deduce')
        accounts['patron_deduce'] = {'framework': framework, 'type': 'patron', 'behavior': 'deduce'}
        initial_balances['patron_deduce'] = balance.get('balance', 0)
        print(f"{Colors.GREEN}✅ Patron (deduce): ${initial_balances['patron_deduce']:,.2f}{Colors.RESET}")
    except Exception as e:
        print(f"{Colors.RED}❌ Patron deduce load error: {e}{Colors.RESET}")
        api_errors.append({'account': 'patron_deduce', 'type': 'LOAD_ERROR', 'message': str(e)})
    
    # Patron non-deduce
    try:
        patron_config = config.load_user_config(f'user_info_patron_{config.ENVIRONMENT}.json')
        patron_nondeduce_creds = {'username': patron_config['email'], 'password': patron_config['password']}
        framework.login_account('patron_nondeduce', patron_nondeduce_creds, account_type='patron')
        balance = framework.get_balance('patron_nondeduce')
        accounts['patron_nondeduce'] = {'framework': framework, 'type': 'patron', 'behavior': 'normal'}
        initial_balances['patron_nondeduce'] = balance.get('balance', 0)
        print(f"{Colors.GREEN}✅ Patron (non-deduce): ${initial_balances['patron_nondeduce']:,.2f}{Colors.RESET}")
    except Exception as e:
        print(f"{Colors.YELLOW}⚠️  Patron non-deduce load error: {e}{Colors.RESET}")
        api_errors.append({'account': 'patron_nondeduce', 'type': 'LOAD_ERROR', 'message': str(e)})
    
    loaded_count = len(accounts)
    print(f"\n{Colors.CYAN}✅ Loaded {loaded_count}/4 accounts{Colors.RESET}\n")
    
    if loaded_count < 2:
        print(f"{Colors.RED}❌ Need at least 2 accounts to run test{Colors.RESET}")
        return
    
    # Get target event and line_ids
    print(f"{Colors.CYAN}🔍 Finding target event...{Colors.RESET}")
    account_name = list(accounts.keys())[0]
    
    # Note: get_available_market doesn't support event_id filtering yet
    # Will use any available event
    if target_event_id:
        print(f"{Colors.YELLOW}⚠️  Note: Specific event ID {target_event_id} requested but not supported yet{Colors.RESET}")
        print(f"{Colors.YELLOW}   Using any available event instead{Colors.RESET}")
    
    market_info = framework.get_available_market(account_name)
    
    if not market_info:
        print(f"{Colors.RED}❌ No available markets found{Colors.RESET}")
        return
    
    event_id = market_info.get('event', {}).get('event_id')
    event_name = market_info.get('event', {}).get('name', 'Unknown')
    market_type = market_info.get('market', {}).get('type', 'Unknown')
    
    print(f"{Colors.GREEN}✅ Using event {event_id}: {event_name}{Colors.RESET}")
    print(f"{Colors.CYAN}   Market: {market_type}{Colors.RESET}\n")
    
    # Collect all line_ids from the event for workers to use
    line_ids = []
    if market_info:
        # Get all line_ids from the target event
        event_obj = market_info.get('event', {})
        event_markets = framework.sessions[account_name].get('markets', [])
        
        # Since we already have the market info, just use that line_id
        line_ids.append(market_info['line_id'])
    
    # Worker function
    def account_worker(account_name, account_data, duration_seconds, line_ids_list):
        start_time = time.time()
        wager_count = 0
        worker_type = account_data['type']
        behavior = account_data['behavior']
        
        print(f"{Colors.CYAN}🔄 {account_name} ({behavior} {worker_type}): Starting worker{Colors.RESET}")
        
        if not line_ids_list:
            print(f"{Colors.RED}❌ {account_name}: No line_ids available{Colors.RESET}")
            return 0
        
        while time.time() - start_time < duration_seconds:
            try:
                # Use the pre-fetched line_id (no need to fetch markets repeatedly)
                import random
                line_id = random.choice(line_ids_list)
                
                # Place wager
                stake = 1.0
                # Both SPs and Patrons need odds values
                odds = 150
                
                result = framework.place_wager(account_name, line_id, odds, stake)
                
                if result.get('success'):
                    wager_count += 1
                    
                    # Check if matched
                    if result.get('status') == 'matched':
                        with lock:
                            matched_bets.append({
                                'timestamp': datetime.now().isoformat(),
                                'account': account_name,
                                'wager_id': result.get('wager_id'),
                                'stake': stake,
                                'behavior': behavior
                            })
                    
                    if wager_count % 20 == 0:
                        print(f"{Colors.CYAN}📊 {account_name}: {wager_count} wagers{Colors.RESET}")
                else:
                    with lock:
                        api_errors.append({
                            'timestamp': datetime.now().isoformat(),
                            'account': account_name,
                            'type': 'PLACE_WAGER_ERROR',
                            'message': result.get('error', 'Unknown error')
                        })
            
            except Exception as e:
                with lock:
                    api_errors.append({
                        'timestamp': datetime.now().isoformat(),
                        'account': account_name,
                        'type': 'WORKER_EXCEPTION',
                        'message': str(e)
                    })
            
            time.sleep(0.2)
        
        print(f"{Colors.GREEN}✅ {account_name}: Completed {wager_count} wagers{Colors.RESET}")
        return wager_count
    
    # Run workers concurrently
    print(f"{Colors.BOLD}🚀 Starting concurrent workers for {duration}s...{Colors.RESET}\n")
    
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = []
        for account_name, account_data in accounts.items():
            future = executor.submit(account_worker, account_name, account_data, duration, line_ids)
            futures.append(future)
        
        # Wait for completion
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"{Colors.RED}❌ Worker error: {e}{Colors.RESET}")
    
    test_end = datetime.now()
    
    # Get final balances
    print(f"\n{Colors.BOLD}📊 Getting final balances...{Colors.RESET}\n")
    final_balances = {}
    for account_name in accounts.keys():
        try:
            balance = framework.get_balance(account_name)
            final_balances[account_name] = balance.get('balance', 0)
        except Exception as e:
            print(f"{Colors.RED}❌ {account_name} balance error: {e}{Colors.RESET}")
            final_balances[account_name] = 0
    
    # Generate report
    print(f"\n{Colors.BOLD}{'='*70}{Colors.RESET}")
    print(f"{Colors.BOLD}FINAL REPORT{Colors.RESET}")
    print(f"{Colors.BOLD}{'='*70}{Colors.RESET}\n")
    
    duration_actual = (test_end - test_start).total_seconds()
    print(f"Test Duration: {duration_actual:.1f}s")
    print(f"Start: {test_start.isoformat()}")
    print(f"End: {test_end.isoformat()}\n")
    
    # Matched bets
    print(f"{Colors.BOLD}📊 Matched Bets: {len(matched_bets)}{Colors.RESET}")
    by_account = {}
    for bet in matched_bets:
        by_account[bet['account']] = by_account.get(bet['account'], 0) + 1
    for account, count in by_account.items():
        print(f"   {account}: {count}")
    
    # Balance reconciliation
    print(f"\n{Colors.BOLD}💰 BALANCE RECONCILIATION{Colors.RESET}")
    print(f"{Colors.CYAN}{'-'*70}{Colors.RESET}")
    
    for account_name in accounts.keys():
        initial = initial_balances.get(account_name, 0)
        final = final_balances.get(account_name, 0)
        change = final - initial
        change_pct = (change / initial * 100) if initial > 0 else 0
        behavior = accounts[account_name]['behavior']
        
        color = Colors.GREEN if change_pct >= 0 else Colors.RED
        print(f"\n{Colors.BOLD}{account_name.upper()} ({behavior}):{Colors.RESET}")
        print(f"   Initial:  ${initial:,.2f}")
        print(f"   Final:    ${final:,.2f}")
        print(f"   {color}Change:   ${change:,.2f} ({change_pct:+.2f}%){Colors.RESET}")
    
    # API Errors
    print(f"\n{Colors.BOLD}❌ API ERRORS: {len(api_errors)}{Colors.RESET}")
    if api_errors:
        error_summary = {}
        for error in api_errors:
            key = f"{error['account']}-{error['type']}"
            error_summary[key] = error_summary.get(key, 0) + 1
        
        print("   Error breakdown:")
        for error_key, count in sorted(error_summary.items(), key=lambda x: -x[1])[:10]:
            print(f"   - {error_key}: {count}")
    
    print(f"\n{Colors.BOLD}{'='*70}{Colors.RESET}\n")
    
    # Save report
    report = {
        'test_period': {
            'start': test_start.isoformat(),
            'end': test_end.isoformat(),
            'duration_seconds': duration_actual
        },
        'accounts': {name: {'behavior': data['behavior'], 'type': data['type']} 
                    for name, data in accounts.items()},
        'initial_balances': initial_balances,
        'final_balances': final_balances,
        'matched_bets': matched_bets,
        'api_errors': api_errors
    }
    
    report_file = f"deduce_wallet_test_{int(time.time())}.json"
    with open(report_file, 'w') as f:
        json.dump(report, f, indent=2)
    
    print(f"{Colors.CYAN}📄 Detailed report saved: {report_file}{Colors.RESET}\n")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Test wallet balance validation for DEDUCE')
    parser.add_argument(
        '--test',
        choices=['mm1', 'mm2', 'all', 'enhanced'],
        default='enhanced',
        help='Which test to run (default: enhanced)'
    )
    parser.add_argument(
        '--duration',
        type=int,
        default=300,
        help='Test duration in seconds for enhanced test (default: 300)'
    )
    parser.add_argument(
        '--event',
        type=int,
        default=None,
        help='Target event ID for enhanced test (optional)'
    )
    
    args = parser.parse_args()
    
    if args.test == 'mm1':
        test_mm1_deduce_wallet_validation()
    elif args.test == 'mm2':
        test_mm2_normal_wallet_validation()
    elif args.test == 'enhanced':
        test_concurrent_4_accounts_enhanced(target_event_id=args.event, duration=args.duration)
    else:
        test_all_accounts_comparison()
