#!/usr/bin/env python3
"""
Duplicate Bet Submission Test

Test Scenario:
Based on discussion: When wallet hasn't finished processing, instead of checking the result 
via an API call, the system submits a duplicate bet to check the result. This causes 
duplicate bet transactions submitted within ~600ms of each other.

This test simulates:
1. Submit a bet (first submission)
2. Wait 600ms
3. Submit the EXACT same bet payload again (duplicate submission)
4. Verify system behavior:
   - Should the second submission be rejected as duplicate?
   - Does the wallet process both submissions?
   - Are there any race conditions in bet deduplication logic?

Expected Behavior:
- Second submission should be rejected with duplicate error
- Wallet balance should only be deducted once
- No duplicate bet transactions should exist in the system

Potential Bug:
If wallet processing is slow and retry logic submits duplicate bets instead of 
checking bet status via API, this can cause:
- Double deductions
- Duplicate bet records
- Race conditions in wallet processing
"""

import time
import json
import sys
import os
from datetime import datetime

sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from deduce_tests import DeduceTestFramework, Colors
from src import config
from src.log import logging


def test_duplicate_bet_submission_600ms(event_id=None, num_tests=10):
    """
    Test Case: Duplicate Bet Submission with 600ms Interval
    
    Scenario:
    - Submit a bet with specific payload
    - Wait exactly 600ms (as mentioned in discussion)
    - Submit EXACT same payload again
    - Verify second submission handling
    
    Args:
        event_id: Specific event to test on (optional)
        num_tests: Number of duplicate submission tests to run
    """
    print(f"\n{Colors.BOLD}{Colors.MAGENTA}{'='*70}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.MAGENTA}Test: Duplicate Bet Submission (600ms interval){Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.MAGENTA}Simulating wallet processing retry logic bug{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.MAGENTA}{'='*70}{Colors.RESET}\n")
    
    framework = DeduceTestFramework(environment=config.ENVIRONMENT)
    
    # Setup account
    print(f"{Colors.CYAN}📦 Setting up MM account...{Colors.RESET}\n")
    
    mm1_creds = config.get_account_credentials(1, config.ENVIRONMENT)
    framework.login_account('mm1', mm1_creds, account_type='mm')
    
    # Get initial balance
    initial_balance_data = framework.get_balance('mm1')
    initial_balance = initial_balance_data.get('balance', 0)
    print(f"{Colors.GREEN}✅ MM1 initial balance: ${initial_balance:,.2f}{Colors.RESET}\n")
    
    # Get market
    print(f"{Colors.CYAN}🔍 Finding market...{Colors.RESET}")
    if event_id:
        from test_deduce_race_conditions import get_market_for_event
        market_info = get_market_for_event(framework, 'mm1', event_id)
    else:
        market_info = framework.get_available_market('mm1')
    
    if not market_info:
        print(f"{Colors.RED}❌ No markets available{Colors.RESET}")
        return
    
    event_name = market_info.get('event', {}).get('name', 'Unknown')
    line_id = market_info['line_id']
    print(f"{Colors.GREEN}✅ Market: {event_name}{Colors.RESET}")
    print(f"{Colors.YELLOW}   Line ID: {line_id[:16]}...{Colors.RESET}\n")
    
    # Test execution
    print(f"{Colors.BOLD}🚀 Starting {num_tests} duplicate submission tests...{Colors.RESET}\n")
    
    test_results = []
    duplicate_interval_ms = 600  # Based on discussion
    
    for test_num in range(1, num_tests + 1):
        print(f"{Colors.CYAN}{'─'*70}{Colors.RESET}")
        print(f"{Colors.BOLD}Test #{test_num}/{num_tests}{Colors.RESET}\n")
        
        # Use consistent odds and stake for duplicate test
        odds = 150
        stake = 1.0
        
        print(f"{Colors.YELLOW}📤 Submission 1: odds={odds}, stake=${stake:.2f}{Colors.RESET}")
        
        # First submission
        start_time = time.time()
        result1 = framework.place_wager('mm1', line_id, odds, stake)
        submission1_time = time.time()
        
        if result1.get('success'):
            wager_id = result1.get('wager_id', 'N/A')
            print(f"{Colors.GREEN}   ✅ Success - Wager ID: {wager_id}{Colors.RESET}")
        else:
            error_msg = result1.get('error', 'Unknown error')
            print(f"{Colors.RED}   ❌ Failed: {error_msg}{Colors.RESET}")
            print(f"{Colors.YELLOW}   Skipping duplicate submission test for this iteration{Colors.RESET}\n")
            continue
        
        # Wait exactly 600ms
        elapsed_ms = (time.time() - submission1_time) * 1000
        wait_ms = max(0, duplicate_interval_ms - elapsed_ms)
        
        if wait_ms > 0:
            print(f"{Colors.YELLOW}⏳ Waiting {wait_ms:.0f}ms before duplicate submission...{Colors.RESET}")
            time.sleep(wait_ms / 1000.0)
        
        # Second submission (DUPLICATE)
        print(f"{Colors.YELLOW}📤 Submission 2: DUPLICATE (same odds={odds}, stake=${stake:.2f}){Colors.RESET}")
        result2 = framework.place_wager('mm1', line_id, odds, stake)
        submission2_time = time.time()
        
        actual_interval_ms = (submission2_time - submission1_time) * 1000
        
        # Analyze results
        test_result = {
            'test_num': test_num,
            'interval_ms': actual_interval_ms,
            'submission1': {
                'success': result1.get('success'),
                'wager_id': result1.get('wager_id'),
                'error': result1.get('error')
            },
            'submission2': {
                'success': result2.get('success'),
                'wager_id': result2.get('wager_id'),
                'error': result2.get('error')
            },
            'timestamp': datetime.now().isoformat()
        }
        
        if result2.get('success'):
            duplicate_wager_id = result2.get('wager_id', 'N/A')
            print(f"{Colors.RED}   ⚠️  SUCCESS - Wager ID: {duplicate_wager_id}{Colors.RESET}")
            print(f"{Colors.RED}   ⚠️  BUG: Duplicate submission was ACCEPTED!{Colors.RESET}")
            
            # Check if same wager ID or different
            if duplicate_wager_id == wager_id:
                print(f"{Colors.YELLOW}   → Same wager ID returned (may be idempotent){Colors.RESET}")
                test_result['duplicate_accepted'] = True
                test_result['same_wager_id'] = True
            else:
                print(f"{Colors.RED}   → DIFFERENT wager ID! Two separate bets created!{Colors.RESET}")
                test_result['duplicate_accepted'] = True
                test_result['same_wager_id'] = False
        else:
            error_msg = result2.get('error', 'Unknown error')
            print(f"{Colors.GREEN}   ✅ REJECTED: {error_msg}{Colors.RESET}")
            print(f"{Colors.GREEN}   ✅ EXPECTED: Duplicate was properly rejected{Colors.RESET}")
            test_result['duplicate_accepted'] = False
        
        print(f"{Colors.CYAN}   Actual interval: {actual_interval_ms:.1f}ms{Colors.RESET}\n")
        
        test_results.append(test_result)
        
        # Small delay between tests
        time.sleep(0.5)
    
    # Wait for any pending settlements
    print(f"\n{Colors.CYAN}⏳ Waiting for settlements...{Colors.RESET}")
    time.sleep(3)
    
    # Get final balance
    final_balance_data = framework.get_balance('mm1')
    final_balance = final_balance_data.get('balance', 0)
    balance_change = initial_balance - final_balance
    
    # Analysis
    print(f"\n{Colors.BOLD}{'='*70}{Colors.RESET}")
    print(f"{Colors.BOLD}TEST RESULTS{Colors.RESET}")
    print(f"{Colors.BOLD}{'='*70}{Colors.RESET}\n")
    
    successful_duplicates = sum(1 for r in test_results if r.get('duplicate_accepted', False))
    rejected_duplicates = len(test_results) - successful_duplicates
    different_ids = sum(1 for r in test_results if r.get('duplicate_accepted', False) and not r.get('same_wager_id', True))
    
    print(f"{Colors.BOLD}Summary:{Colors.RESET}")
    print(f"  Total tests:              {len(test_results)}")
    print(f"  Duplicates ACCEPTED:      {successful_duplicates} ", end="")
    if successful_duplicates > 0:
        print(f"{Colors.RED}← BUG!{Colors.RESET}")
    else:
        print(f"{Colors.GREEN}← Good{Colors.RESET}")
    
    print(f"  Duplicates REJECTED:      {rejected_duplicates} ", end="")
    if rejected_duplicates == len(test_results):
        print(f"{Colors.GREEN}← Good{Colors.RESET}")
    else:
        print(f"{Colors.YELLOW}← Some duplicates accepted{Colors.RESET}")
    
    if different_ids > 0:
        print(f"  Different Wager IDs:      {different_ids} {Colors.RED}← CRITICAL BUG!{Colors.RESET}")
    print()
    
    print(f"{Colors.BOLD}Balance Analysis:{Colors.RESET}")
    print(f"  Initial balance:          ${initial_balance:,.2f}")
    print(f"  Final balance:            ${final_balance:,.2f}")
    print(f"  Total change:             ${balance_change:,.2f}")
    
    # Expected change = number of unique successful bets * stake
    # If duplicates were accepted, we'd see 2x the change
    expected_change = len(test_results) * 1.0  # $1 stake per test
    
    if abs(balance_change - expected_change) < 0.01:
        print(f"  Expected change:          ${expected_change:.2f} {Colors.GREEN}← Matches!{Colors.RESET}")
    elif balance_change > expected_change + 0.01:
        print(f"  Expected change:          ${expected_change:.2f}")
        print(f"  {Colors.RED}⚠️  Balance change > expected! Possible duplicate processing!{Colors.RESET}")
    else:
        print(f"  Expected change:          ${expected_change:.2f}")
        print(f"  {Colors.YELLOW}⚠️  Balance change < expected. Some bets may not have settled yet.{Colors.RESET}")
    print()
    
    # Detailed results
    if successful_duplicates > 0:
        print(f"{Colors.BOLD}{Colors.RED}DUPLICATE ACCEPTANCE DETAILS:{Colors.RESET}\n")
        for r in test_results:
            if r.get('duplicate_accepted', False):
                print(f"  Test #{r['test_num']}:")
                print(f"    Interval:     {r['interval_ms']:.1f}ms")
                print(f"    Wager ID 1:   {r['submission1']['wager_id']}")
                print(f"    Wager ID 2:   {r['submission2']['wager_id']}")
                if r.get('same_wager_id', True):
                    print(f"    Status:       {Colors.YELLOW}Same ID (possibly idempotent){Colors.RESET}")
                else:
                    print(f"    Status:       {Colors.RED}DIFFERENT IDs - DUPLICATE BET CREATED!{Colors.RESET}")
                print()
    
    # Recommendations
    print(f"{Colors.BOLD}RECOMMENDATIONS:{Colors.RESET}\n")
    
    if successful_duplicates > 0:
        print(f"{Colors.RED}❌ BUG CONFIRMED:{Colors.RESET}")
        print(f"   System accepted duplicate bet submissions within 600ms interval")
        print()
        print(f"{Colors.YELLOW}Root Cause:{Colors.RESET}")
        print(f"   Instead of calling an API to check bet status after wallet processing,")
        print(f"   the retry logic submits the same bet payload again to 'check' the result.")
        print()
        print(f"{Colors.GREEN}Fix Required:{Colors.RESET}")
        print(f"   1. Implement proper bet deduplication based on payload hash/signature")
        print(f"   2. Add idempotency key to bet submissions")
        print(f"   3. Create dedicated API endpoint to check bet status")
        print(f"   4. Don't retry with duplicate submission - check status instead")
        print()
    else:
        print(f"{Colors.GREEN}✅ NO BUG DETECTED:{Colors.RESET}")
        print(f"   All duplicate submissions were properly rejected")
        print(f"   System has proper deduplication logic in place")
        print()
    
    # Save report
    report = {
        'test': 'duplicate_bet_submission_600ms',
        'timestamp': datetime.now().isoformat(),
        'num_tests': len(test_results),
        'duplicate_interval_ms': duplicate_interval_ms,
        'results': test_results,
        'summary': {
            'duplicates_accepted': successful_duplicates,
            'duplicates_rejected': rejected_duplicates,
            'different_wager_ids': different_ids
        },
        'balance': {
            'initial': initial_balance,
            'final': final_balance,
            'change': balance_change,
            'expected_change': expected_change
        }
    }
    
    report_file = f"test_duplicate_submission_{int(time.time())}.json"
    with open(report_file, 'w') as f:
        json.dump(report, f, indent=2)
    
    print(f"{Colors.GREEN}📄 Report saved: {report_file}{Colors.RESET}\n")


def test_rapid_duplicate_submissions(event_id=None, num_duplicates=5, interval_ms=100):
    """
    Test Case: Rapid Duplicate Submissions
    
    More aggressive test:
    - Submit same bet multiple times in rapid succession
    - Test with different intervals (100ms, 200ms, 600ms)
    - Verify deduplication at various speeds
    """
    print(f"\n{Colors.BOLD}{Colors.MAGENTA}{'='*70}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.MAGENTA}Test: Rapid Duplicate Submissions{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.MAGENTA}Testing deduplication under rapid-fire scenario{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.MAGENTA}{'='*70}{Colors.RESET}\n")
    
    framework = DeduceTestFramework(environment=config.ENVIRONMENT)
    
    # Setup
    print(f"{Colors.CYAN}📦 Setting up account...{Colors.RESET}\n")
    mm1_creds = config.get_account_credentials(1, config.ENVIRONMENT)
    framework.login_account('mm1', mm1_creds, account_type='mm')
    
    initial_balance_data = framework.get_balance('mm1')
    initial_balance = initial_balance_data.get('balance', 0)
    print(f"{Colors.GREEN}✅ MM1 initial balance: ${initial_balance:,.2f}{Colors.RESET}\n")
    
    # Get market
    print(f"{Colors.CYAN}🔍 Finding market...{Colors.RESET}")
    if event_id:
        from test_deduce_race_conditions import get_market_for_event
        market_info = get_market_for_event(framework, 'mm1', event_id)
    else:
        market_info = framework.get_available_market('mm1')
    
    if not market_info:
        print(f"{Colors.RED}❌ No markets available{Colors.RESET}")
        return
    
    line_id = market_info['line_id']
    print(f"{Colors.GREEN}✅ Market found{Colors.RESET}\n")
    
    # Test execution
    print(f"{Colors.BOLD}🚀 Submitting {num_duplicates} duplicates with {interval_ms}ms intervals...{Colors.RESET}\n")
    
    odds = 150
    stake = 1.0
    
    results = []
    wager_ids = set()
    
    for i in range(num_duplicates):
        print(f"{Colors.CYAN}Submission #{i+1}... {Colors.RESET}", end="")
        
        start = time.time()
        result = framework.place_wager('mm1', line_id, odds, stake)
        duration = (time.time() - start) * 1000
        
        results.append(result)
        
        if result.get('success'):
            wager_id = result.get('wager_id')
            wager_ids.add(wager_id)
            print(f"{Colors.GREEN}✅ Success ({duration:.0f}ms) - ID: {wager_id}{Colors.RESET}")
        else:
            error = result.get('error', 'Unknown')
            print(f"{Colors.YELLOW}❌ Rejected ({duration:.0f}ms) - {error}{Colors.RESET}")
        
        if i < num_duplicates - 1:
            time.sleep(interval_ms / 1000.0)
    
    # Analysis
    print(f"\n{Colors.BOLD}{'─'*70}{Colors.RESET}")
    print(f"{Colors.BOLD}RESULTS{Colors.RESET}\n")
    
    successful = sum(1 for r in results if r.get('success'))
    rejected = len(results) - successful
    unique_ids = len(wager_ids)
    
    print(f"Total submissions:    {len(results)}")
    print(f"Accepted:             {successful} ", end="")
    if successful > 1:
        print(f"{Colors.RED}← Multiple accepted!{Colors.RESET}")
    else:
        print(f"{Colors.GREEN}← Good{Colors.RESET}")
    
    print(f"Rejected:             {rejected}")
    print(f"Unique wager IDs:     {unique_ids} ", end="")
    if unique_ids > 1:
        print(f"{Colors.RED}← BUG: Multiple bets created!{Colors.RESET}")
    else:
        print(f"{Colors.GREEN}← Good{Colors.RESET}")
    
    print()
    
    # Wait and check balance
    print(f"{Colors.CYAN}⏳ Waiting for settlement...{Colors.RESET}")
    time.sleep(3)
    
    final_balance_data = framework.get_balance('mm1')
    final_balance = final_balance_data.get('balance', 0)
    balance_change = initial_balance - final_balance
    
    print(f"\n{Colors.BOLD}Balance Check:{Colors.RESET}")
    print(f"  Initial:  ${initial_balance:,.2f}")
    print(f"  Final:    ${final_balance:,.2f}")
    print(f"  Change:   ${balance_change:.2f}")
    print(f"  Expected: ${stake:.2f} (only 1 bet should be deducted)")
    
    if abs(balance_change - stake) < 0.01:
        print(f"  {Colors.GREEN}✅ Balance correct - only 1 bet processed{Colors.RESET}")
    elif balance_change > stake + 0.01:
        print(f"  {Colors.RED}❌ BUG: Balance indicates multiple bets were processed!{Colors.RESET}")
    print()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Test duplicate bet submission')
    parser.add_argument('--test', choices=['duplicate_600ms', 'rapid', 'both'], 
                        default='both', help='Which test to run')
    parser.add_argument('--event-id', type=int, help='Specific event ID to test')
    parser.add_argument('--num-tests', type=int, default=10, 
                        help='Number of duplicate submission tests (for duplicate_600ms)')
    parser.add_argument('--num-duplicates', type=int, default=5,
                        help='Number of rapid duplicates (for rapid test)')
    parser.add_argument('--interval', type=int, default=100,
                        help='Interval in ms between rapid submissions')
    
    args = parser.parse_args()
    
    if args.test in ['duplicate_600ms', 'both']:
        test_duplicate_bet_submission_600ms(
            event_id=args.event_id,
            num_tests=args.num_tests
        )
    
    if args.test in ['rapid', 'both']:
        if args.test == 'both':
            print(f"\n{Colors.CYAN}{'='*70}{Colors.RESET}\n")
        
        test_rapid_duplicate_submissions(
            event_id=args.event_id,
            num_duplicates=args.num_duplicates,
            interval_ms=args.interval
        )
