#!/usr/bin/env python3
"""
Test to demonstrate wallet balance validation for DEDUCE-enabled accounts

This test shows the difference between:
- MM1 (DEDUCE enabled): Balance only deducted when bet is matched
- MM2 & Patron (normal): Balance deducted immediately when bet is placed
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


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Test wallet balance validation for DEDUCE')
    parser.add_argument(
        '--test',
        choices=['mm1', 'mm2', 'all'],
        default='all',
        help='Which test to run (default: all)'
    )
    
    args = parser.parse_args()
    
    if args.test == 'mm1':
        test_mm1_deduce_wallet_validation()
    elif args.test == 'mm2':
        test_mm2_normal_wallet_validation()
    else:
        test_all_accounts_comparison()
