#!/usr/bin/env python3
"""
Wallet Retry Logic Test

Context từ Trade Side:
- Wallet complain là Trade retry quá nhanh cho: submit wager, cancel wager
- Khi Wallet trả về lỗi 5xx hoặc không trả về response, Trade retry lại
- Hot fix: thêm delay 5s trước khi retry để tránh deadlock ở Wallet
- Mục tiêu: Wallet không bị lỗi, balance trả về đúng

Test Scenarios:
1. Test retry behavior khi Wallet timeout/5xx
2. Test với different retry delays (no delay, 1s, 5s, 10s)
3. Verify balance consistency sau khi retry
4. Test deadlock scenarios với rapid retries
5. Test concurrent operations (submit + cancel) với retries

Expected Behavior:
- Retry với delay >= 5s: Wallet process OK, balance đúng
- Retry quá nhanh (< 5s): Có thể gây deadlock, balance sai
- Cancel retry quá nhanh: Wager có thể vẫn execute, balance không đúng
"""

import time
import json
import sys
import os
import threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from deduce_tests import DeduceTestFramework, Colors
from src import config
from src.log import logging


def test_submit_wager_with_retry_delays(event_id=None, num_tests=10):
    """
    Test Case: Submit Wager với Different Retry Delays
    
    Simulate tình huống:
    - Submit wager có thể timeout hoặc nhận 5xx error từ Wallet
    - Trade retry với different delays
    - Verify balance consistency
    
    Test delays:
    - 0s (immediate retry - OLD behavior, causes deadlock)
    - 1s (too fast - may still cause issues)
    - 5s (current hot fix)
    - 10s (very safe)
    
    Uses 10 MM accounts to test concurrent retry behavior
    """
    print(f"\n{Colors.BOLD}{Colors.MAGENTA}{'='*70}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.MAGENTA}Test: Submit Wager Retry Delays (50 WORKERS - AGGRESSIVE){Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.MAGENTA}Testing wallet retry behavior với different delays{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.MAGENTA}{'='*70}{Colors.RESET}\n")
    
    framework = DeduceTestFramework(environment=config.ENVIRONMENT)
    
    # Setup 50 MM accounts (reuse accounts with different virtual workers)
    print(f"{Colors.CYAN}📦 Setting up 50 MM workers (using 10 accounts x 5 workers each)...{Colors.RESET}\n")
    
    # First, login to 10 actual MM accounts
    base_mm_accounts = []
    for i in range(1, 11):
        mm_name = f'mm{i}'
        try:
            mm_creds = config.get_account_credentials(i, config.ENVIRONMENT)
            framework.login_account(mm_name, mm_creds, account_type='mm')
            balance_data = framework.get_balance(mm_name)
            balance = balance_data.get('balance', 0)
            base_mm_accounts.append(mm_name)
            print(f"{Colors.GREEN}✅ {mm_name}: ${balance:,.2f}{Colors.RESET}")
        except Exception as e:
            print(f"{Colors.RED}❌ Failed to setup {mm_name}: {e}{Colors.RESET}")
    
    # Create 50 virtual workers by reusing the 10 accounts (5 workers per account)
    mm_accounts = []
    for worker_id in range(50):
        account_index = worker_id % len(base_mm_accounts)
        mm_accounts.append(base_mm_accounts[account_index])
    
    print(f"\n{Colors.GREEN}✅ Created {len(mm_accounts)} workers from {len(base_mm_accounts)} accounts{Colors.RESET}")
    print(f"{Colors.YELLOW}⚠️  WARNING: This will stress test with {len(mm_accounts)} concurrent requests!{Colors.RESET}\n")
    
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
    print(f"{Colors.GREEN}✅ Market: {event_name}{Colors.RESET}\n")
    
    # Test different retry delays
    retry_delays = [
        {'delay': 0, 'description': 'Immediate retry (OLD - causes deadlock)', 'expected': 'DEADLOCK'},
        {'delay': 0.6, 'description': '600ms retry (fast retry)', 'expected': 'MAY FAIL'},
        {'delay': 1, 'description': '1s retry (still too fast)', 'expected': 'MAY FAIL'},
        {'delay': 5, 'description': '5s retry (current hot fix)', 'expected': 'OK'},
        {'delay': 10, 'description': '10s retry (very safe)', 'expected': 'OK'},
    ]
    
    print(f"{Colors.BOLD}🚀 Testing retry logic với {len(retry_delays)} different delays using {len(mm_accounts)} MMs...{Colors.RESET}\n")
    
    test_results = []
    lock = threading.Lock()
    
    def test_mm_retry(mm_name, delay, line_id, odds, stake):
        """Test retry logic for a single MM account"""
        # Get balance before
        balance_before = framework.get_balance(mm_name).get('balance', 0)
        
        # First submission
        result1 = framework.place_wager(mm_name, line_id, odds, stake)
        
        # Wait for the specified delay
        if delay > 0:
            time.sleep(delay)
        
        # Retry (simulate Trade retry behavior)
        result2 = framework.place_wager(mm_name, line_id, odds, stake)
        
        # Wait for settlement
        time.sleep(2)
        
        # Check balance after
        balance_after = framework.get_balance(mm_name).get('balance', 0)
        balance_change = balance_before - balance_after
        
        # Analyze result
        result_status = 'OK'
        if balance_change > stake + 0.5:  # More than 1 bet processed
            result_status = 'DUPLICATE_PROCESSED'
        elif abs(balance_change - stake) < 0.01:
            result_status = 'OK'
        elif balance_change < 0.01:
            result_status = 'NO_DEDUCTION'
        
        # Check if duplicate wager was created
        duplicate_created = False
        same_wager = False
        if result1.get('success') and result2.get('success'):
            wager_id1 = result1.get('wager_id')
            wager_id2 = result2.get('wager_id')
            if wager_id1 == wager_id2:
                same_wager = True
            else:
                duplicate_created = True
        
        return {
            'mm_name': mm_name,
            'balance_before': balance_before,
            'balance_after': balance_after,
            'balance_change': balance_change,
            'result1': result1,
            'result2': result2,
            'status': result_status,
            'duplicate_created': duplicate_created,
            'same_wager': same_wager
        }
    
    for retry_config in retry_delays:
        delay = retry_config['delay']
        description = retry_config['description']
        expected = retry_config['expected']
        
        print(f"{Colors.CYAN}{'─'*70}{Colors.RESET}")
        print(f"{Colors.BOLD}Testing: {description}{Colors.RESET}")
        print(f"{Colors.YELLOW}Expected: {expected}{Colors.RESET}")
        print(f"{Colors.CYAN}Running {len(mm_accounts)} MMs in parallel...{Colors.RESET}\n")
        
        odds = 150
        stake = 1.0
        
        # Run all MMs in parallel for this retry delay
        with ThreadPoolExecutor(max_workers=len(mm_accounts)) as executor:
            futures = [
                executor.submit(test_mm_retry, mm_name, delay, line_id, odds, stake)
                for mm_name in mm_accounts
            ]
            
            mm_results = []
            for future in as_completed(futures):
                try:
                    result = future.result()
                    mm_results.append(result)
                    
                    # Print progress
                    mm_name = result['mm_name']
                    status = result['status']
                    if result['duplicate_created']:
                        print(f"{Colors.RED}  {mm_name}: DUPLICATE wager created! ({status}){Colors.RESET}")
                    elif result['same_wager']:
                        print(f"{Colors.GREEN}  {mm_name}: Same wager (idempotent) ({status}){Colors.RESET}")
                    else:
                        print(f"{Colors.YELLOW}  {mm_name}: {status}{Colors.RESET}")
                except Exception as e:
                    print(f"{Colors.RED}  Error: {e}{Colors.RESET}")
        
        # Aggregate results
        duplicates_count = sum(1 for r in mm_results if r['status'] == 'DUPLICATE_PROCESSED')
        ok_count = sum(1 for r in mm_results if r['status'] == 'OK')
        no_deduction_count = sum(1 for r in mm_results if r['status'] == 'NO_DEDUCTION')
        duplicate_created_count = sum(1 for r in mm_results if r['duplicate_created'])
        
        print(f"\n{Colors.BOLD}Summary for {delay}s delay:{Colors.RESET}")
        print(f"  OK: {ok_count}/{len(mm_results)}")
        print(f"  Duplicate Processed: {duplicates_count}/{len(mm_results)}")
        print(f"  No Deduction: {no_deduction_count}/{len(mm_results)}")
        print(f"  Duplicate Wagers Created: {duplicate_created_count}/{len(mm_results)}")
        
        # Overall status for this delay
        if duplicates_count > 0:
            overall_status = 'DUPLICATE_PROCESSED'
        elif ok_count == len(mm_results):
            overall_status = 'OK'
        else:
            overall_status = 'NO_DEDUCTION'
        
        test_results.append({
            'delay': delay,
            'description': description,
            'expected': expected,
            'mm_results': mm_results,
            'status': overall_status,
            'duplicates_count': duplicates_count,
            'duplicate_created_count': duplicate_created_count,
            'ok_count': ok_count,
            'no_deduction_count': no_deduction_count
        })
        
        print()
        time.sleep(2)
    
    # Final analysis
    print(f"\n{Colors.BOLD}{'='*70}{Colors.RESET}")
    print(f"{Colors.BOLD}SUMMARY{Colors.RESET}")
    print(f"{Colors.BOLD}{'='*70}{Colors.RESET}\n")
    
    print(f"{Colors.BOLD}Retry Delay Analysis:{Colors.RESET}\n")
    
    for result in test_results:
        delay = result['delay']
        status = result['status']
        expected = result['expected']
        
        status_color = Colors.GREEN if status == 'OK' else Colors.RED
        
        print(f"  {delay:>4}s delay: {status:20} (expected: {expected})")
        if status == 'DUPLICATE_PROCESSED':
            print(f"        {Colors.RED}⚠️  Wallet processed duplicate - retry too fast!{Colors.RESET}")
        elif status == 'OK' and expected == 'OK':
            print(f"        {Colors.GREEN}✅ Working as expected{Colors.RESET}")
    
    print()
    
    # Recommendations
    print(f"{Colors.BOLD}RECOMMENDATIONS:{Colors.RESET}\n")
    
    duplicates = sum(1 for r in test_results if r['status'] == 'DUPLICATE_PROCESSED')
    hotfix_result = next((r for r in test_results if r['delay'] == 5), None)
    
    if duplicates > 0:
        print(f"{Colors.RED}⚠️  Found {duplicates} cases where duplicates were processed{Colors.RESET}\n")
        print(f"{Colors.YELLOW}Current hot fix (5s delay) analysis:{Colors.RESET}")
        
        if hotfix_result and hotfix_result['status'] == 'OK':
            print(f"  {Colors.GREEN}✅ 5s delay is working correctly{Colors.RESET}")
        else:
            print(f"  {Colors.RED}❌ 5s delay may not be sufficient{Colors.RESET}")
        
        print()
        print(f"{Colors.GREEN}Better solutions:{Colors.RESET}")
        print(f"  1. Implement idempotency key cho mỗi request")
        print(f"  2. Add request ID để Wallet track duplicate requests")
        print(f"  3. Implement exponential backoff (1s, 2s, 4s, 8s...)")
        print(f"  4. Add status check API thay vì retry blind")
        print(f"  5. Use distributed lock để prevent concurrent retries")
    else:
        print(f"{Colors.GREEN}✅ All retry delays handled correctly{Colors.RESET}")
        print(f"   Current 5s delay hot fix is working well")
    
    print()
    
    # Save report
    report = {
        'test': 'submit_wager_retry_delays',
        'timestamp': datetime.now().isoformat(),
        'results': test_results,
        'summary': {
            'total_tests': len(test_results),
            'duplicates_processed': duplicates,
            'hotfix_working': hotfix_result and hotfix_result['status'] == 'OK' if hotfix_result else False
        }
    }
    
    report_file = f"test_wallet_retry_{int(time.time())}.json"
    with open(report_file, 'w') as f:
        json.dump(report, f, indent=2)
    
    print(f"{Colors.GREEN}📄 Report saved: {report_file}{Colors.RESET}\n")


def test_cancel_wager_retry_race_condition(event_id=None, num_tests=10):
    """
    Test Case: Cancel Wager Retry Race Condition
    
    Scenario:
    - Submit wager
    - Immediately cancel (may timeout/5xx)
    - Trade retry cancel too fast
    - Race condition: wager may still execute while cancel retrying
    
    Expected behavior:
    - Fast retry: wager executes despite cancel, balance deducted
    - Proper delay: cancel succeeds, no balance deduction
    """
    print(f"\n{Colors.BOLD}{Colors.MAGENTA}{'='*70}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.MAGENTA}Test: Cancel Wager Retry Race Condition{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.MAGENTA}Testing cancel retry với rapid retries{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.MAGENTA}{'='*70}{Colors.RESET}\n")
    
    framework = DeduceTestFramework(environment=config.ENVIRONMENT)
    
    # Setup
    print(f"{Colors.CYAN}📦 Setting up account...{Colors.RESET}\n")
    mm1_creds = config.get_account_credentials(1, config.ENVIRONMENT)
    framework.login_account('mm1', mm1_creds, account_type='mm')
    
    initial_balance = framework.get_balance('mm1').get('balance', 0)
    print(f"{Colors.GREEN}✅ Initial balance: ${initial_balance:,.2f}{Colors.RESET}\n")
    
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
    
    # Test different cancel retry delays
    cancel_delays = [0, 0.5, 1, 5]
    
    print(f"{Colors.BOLD}🚀 Testing cancel retry với {len(cancel_delays)} different delays...{Colors.RESET}\n")
    
    test_results = []
    
    for delay in cancel_delays:
        print(f"{Colors.CYAN}{'─'*70}{Colors.RESET}")
        print(f"{Colors.BOLD}Test: Cancel retry delay = {delay}s{Colors.RESET}\n")
        
        balance_before = framework.get_balance('mm1').get('balance', 0)
        
        # Submit wager
        odds = 150
        stake = 2.0
        
        print(f"{Colors.YELLOW}📤 Submitting wager...{Colors.RESET}")
        submit_result = framework.place_wager('mm1', line_id, odds, stake)
        
        if not submit_result.get('success'):
            print(f"{Colors.RED}   ❌ Failed to submit wager{Colors.RESET}\n")
            continue
        
        wager_id = submit_result.get('wager_id')
        external_id = submit_result.get('external_id')
        print(f"{Colors.GREEN}   ✅ Wager submitted: {wager_id}{Colors.RESET}")
        
        # Immediate cancel attempt
        print(f"{Colors.YELLOW}🚫 Attempt 1: Canceling wager...{Colors.RESET}")
        cancel_success1 = framework.cancel_wager('mm1', external_id, wager_id)
        cancel_result1 = {'success': cancel_success1}
        
        if cancel_result1.get('success'):
            print(f"{Colors.GREEN}   ✅ Cancel succeeded{Colors.RESET}")
        else:
            print(f"{Colors.YELLOW}   ⚠️  Cancel failed (may timeout/5xx){Colors.RESET}")
        
        # Wait for retry delay
        if delay > 0:
            print(f"{Colors.YELLOW}⏳ Waiting {delay}s before cancel retry...{Colors.RESET}")
            time.sleep(delay)
        else:
            print(f"{Colors.RED}🔥 IMMEDIATE cancel retry (no delay){Colors.RESET}")
        
        # Retry cancel
        print(f"{Colors.YELLOW}🚫 Attempt 2: RETRY cancel with {delay}s delay...{Colors.RESET}")
        cancel_success2 = framework.cancel_wager('mm1', external_id, wager_id)
        cancel_result2 = {'success': cancel_success2}
        
        if cancel_result2.get('success'):
            print(f"{Colors.GREEN}   ✅ Retry cancel succeeded{Colors.RESET}")
        else:
            error = cancel_result2.get('error', 'Unknown')
            print(f"{Colors.YELLOW}   ⚠️  Retry cancel failed: {error}{Colors.RESET}")
        
        # Wait for processing
        time.sleep(3)
        
        # Check balance
        balance_after = framework.get_balance('mm1').get('balance', 0)
        balance_change = balance_before - balance_after
        
        print(f"{Colors.CYAN}💰 Balance: ${balance_before:,.2f} → ${balance_after:,.2f} (Δ ${balance_change:.2f}){Colors.RESET}")
        
        # Analyze
        if abs(balance_change) < 0.01:
            status = 'CANCEL_SUCCESS'
            print(f"{Colors.GREEN}   ✅ Cancel successful - no balance deduction{Colors.RESET}")
        else:
            status = 'WAGER_EXECUTED'
            print(f"{Colors.RED}   ❌ Wager was EXECUTED despite cancel - balance deducted!{Colors.RESET}")
            print(f"{Colors.RED}      Race condition: Cancel retry too slow, wager already matched{Colors.RESET}")
        
        test_results.append({
            'cancel_delay': delay,
            'wager_id': wager_id,
            'cancel_attempt1': cancel_result1,
            'cancel_attempt2': cancel_result2,
            'balance_change': balance_change,
            'status': status
        })
        
        print()
        time.sleep(2)
    
    # Summary
    print(f"\n{Colors.BOLD}{'='*70}{Colors.RESET}")
    print(f"{Colors.BOLD}CANCEL RETRY SUMMARY{Colors.RESET}")
    print(f"{Colors.BOLD}{'='*70}{Colors.RESET}\n")
    
    for result in test_results:
        delay = result['cancel_delay']
        status = result['status']
        
        status_icon = '✅' if status == 'CANCEL_SUCCESS' else '❌'
        status_color = Colors.GREEN if status == 'CANCEL_SUCCESS' else Colors.RED
        
        print(f"  {delay:>4}s delay: {status_color}{status_icon} {status}{Colors.RESET}")
    
    print()
    
    # Recommendations
    executed_count = sum(1 for r in test_results if r['status'] == 'WAGER_EXECUTED')
    
    if executed_count > 0:
        print(f"{Colors.RED}⚠️  {executed_count} wagers were executed despite cancel attempts{Colors.RESET}\n")
        print(f"{Colors.YELLOW}Issue:{Colors.RESET}")
        print(f"  - Cancel retry có thể quá chậm")
        print(f"  - Wager đã matched trước khi cancel execute")
        print(f"  - Trade retry logic needs improvement")
        print()
        print(f"{Colors.GREEN}Solutions:{Colors.RESET}")
        print(f"  1. Implement immediate cancel với higher priority")
        print(f"  2. Add 'pending_cancel' state để prevent matching")
        print(f"  3. Wallet should reject matching for wagers với pending cancel")
        print(f"  4. Use optimistic locking cho cancel operations")
    else:
        print(f"{Colors.GREEN}✅ All cancels succeeded - no race conditions detected{Colors.RESET}")
    
    print()


def test_concurrent_submit_cancel_with_retries(event_id=None, duration=30):
    """
    Test Case: Concurrent Submit + Cancel với Retries
    
    High-stress test:
    - Multiple threads submitting wagers
    - Multiple threads canceling wagers
    - Both with retry logic
    - Simulate real production load
    
    Verify:
    - No deadlocks
    - Balance consistency
    - Proper retry handling
    """
    print(f"\n{Colors.BOLD}{Colors.MAGENTA}{'='*70}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.MAGENTA}Test: Concurrent Submit + Cancel với Retries{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.MAGENTA}High-stress deadlock test{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.MAGENTA}{'='*70}{Colors.RESET}\n")
    
    framework = DeduceTestFramework(environment=config.ENVIRONMENT)
    
    # Setup
    print(f"{Colors.CYAN}📦 Setting up account...{Colors.RESET}\n")
    mm1_creds = config.get_account_credentials(1, config.ENVIRONMENT)
    framework.login_account('mm1', mm1_creds, account_type='mm')
    
    initial_balance = framework.get_balance('mm1').get('balance', 0)
    print(f"{Colors.GREEN}✅ Initial balance: ${initial_balance:,.2f}{Colors.RESET}\n")
    
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
    print(f"{Colors.BOLD}🚀 Running {duration}s concurrent stress test...{Colors.RESET}\n")
    
    stats = {
        'submitted': 0,
        'cancelled': 0,
        'submit_errors': 0,
        'cancel_errors': 0,
        'retries': 0
    }
    lock = threading.Lock()
    wager_queue = []
    
    def submit_worker():
        """Worker that submits wagers with retry"""
        start_time = time.time()
        while time.time() - start_time < duration:
            try:
                odds = 150
                stake = 1.0
                
                # First attempt
                result = framework.place_wager('mm1', line_id, odds, stake)
                
                if result.get('success'):
                    wager_info = {
                        'wager_id': result.get('wager_id'),
                        'external_id': result.get('external_id')
                    }
                    with lock:
                        stats['submitted'] += 1
                        wager_queue.append(wager_info)
                    
                    if stats['submitted'] % 10 == 0:
                        print(f"{Colors.CYAN}📊 Submitted: {stats['submitted']}{Colors.RESET}")
                else:
                    # Simulate retry with 5s delay (hot fix)
                    with lock:
                        stats['retries'] += 1
                    
                    time.sleep(5)
                    
                    # Retry
                    retry_result = framework.place_wager('mm1', line_id, odds, stake)
                    if retry_result.get('success'):
                        wager_info = {
                            'wager_id': retry_result.get('wager_id'),
                            'external_id': retry_result.get('external_id')
                        }
                        with lock:
                            stats['submitted'] += 1
                            wager_queue.append(wager_info)
                    else:
                        with lock:
                            stats['submit_errors'] += 1
                
            except Exception as e:
                with lock:
                    stats['submit_errors'] += 1
            
            time.sleep(0.2)
    
    def cancel_worker():
        """Worker that cancels wagers with retry"""
        start_time = time.time()
        while time.time() - start_time < duration:
            wager_info = None
            
            with lock:
                if wager_queue:
                    wager_info = wager_queue.pop(0)
            
            if wager_info:
                try:
                    external_id = wager_info['external_id']
                    wager_id = wager_info['wager_id']
                    
                    cancel_success = framework.cancel_wager('mm1', external_id, wager_id)
                    
                    if cancel_success:
                        with lock:
                            stats['cancelled'] += 1
                        
                        if stats['cancelled'] % 10 == 0:
                            print(f"{Colors.YELLOW}📊 Cancelled: {stats['cancelled']}{Colors.RESET}")
                    else:
                        # Simulate retry with 5s delay
                        with lock:
                            stats['retries'] += 1
                        
                        time.sleep(5)
                        
                        # Retry
                        retry_success = framework.cancel_wager('mm1', external_id, wager_id)
                        if retry_success:
                            with lock:
                                stats['cancelled'] += 1
                        else:
                            with lock:
                                stats['cancel_errors'] += 1
                
                except Exception as e:
                    with lock:
                        stats['cancel_errors'] += 1
            else:
                time.sleep(0.1)
    
    # Run concurrent workers
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [
            executor.submit(submit_worker),
            executor.submit(submit_worker),
            executor.submit(cancel_worker),
            executor.submit(cancel_worker)
        ]
        
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"{Colors.RED}Worker error: {e}{Colors.RESET}")
    
    # Wait for settling
    print(f"\n{Colors.CYAN}⏳ Waiting for operations to settle...{Colors.RESET}")
    time.sleep(5)
    
    # Final balance
    final_balance = framework.get_balance('mm1').get('balance', 0)
    balance_change = initial_balance - final_balance
    
    # Results
    print(f"\n{Colors.BOLD}{'='*70}{Colors.RESET}")
    print(f"{Colors.BOLD}CONCURRENT STRESS TEST RESULTS{Colors.RESET}")
    print(f"{Colors.BOLD}{'='*70}{Colors.RESET}\n")
    
    print(f"{Colors.BOLD}Operations:{Colors.RESET}")
    print(f"  Wagers submitted:     {stats['submitted']}")
    print(f"  Wagers cancelled:     {stats['cancelled']}")
    print(f"  Total retries:        {stats['retries']}")
    print(f"  Submit errors:        {stats['submit_errors']}")
    print(f"  Cancel errors:        {stats['cancel_errors']}")
    print()
    
    print(f"{Colors.BOLD}Balance:{Colors.RESET}")
    print(f"  Initial:              ${initial_balance:,.2f}")
    print(f"  Final:                ${final_balance:,.2f}")
    print(f"  Change:               ${balance_change:.2f}")
    
    # Expected: submitted - cancelled
    expected_executed = stats['submitted'] - stats['cancelled']
    expected_change = expected_executed * 1.0
    
    print(f"  Expected change:      ~${expected_change:.2f}")
    
    if abs(balance_change - expected_change) < expected_executed * 0.1:  # Within 10%
        print(f"  {Colors.GREEN}✅ Balance consistent with operations{Colors.RESET}")
    else:
        print(f"  {Colors.RED}❌ Balance inconsistent - possible deadlock/race condition{Colors.RESET}")
    
    print()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Test wallet retry logic and deadlock scenarios')
    parser.add_argument('--test', choices=['submit_retry', 'cancel_retry', 'concurrent', 'all'],
                        default='all', help='Which test to run')
    parser.add_argument('--event-id', type=int, help='Specific event ID')
    parser.add_argument('--num-tests', type=int, default=10, help='Number of tests')
    parser.add_argument('--duration', type=int, default=30, help='Duration for concurrent test (seconds)')
    
    args = parser.parse_args()
    
    if args.test in ['submit_retry', 'all']:
        test_submit_wager_with_retry_delays(
            event_id=args.event_id,
            num_tests=args.num_tests
        )
    
    if args.test in ['cancel_retry', 'all']:
        if args.test == 'all':
            print(f"\n{Colors.CYAN}{'='*70}{Colors.RESET}\n")
        
        test_cancel_wager_retry_race_condition(
            event_id=args.event_id,
            num_tests=args.num_tests
        )
    
    if args.test in ['concurrent', 'all']:
        if args.test == 'all':
            print(f"\n{Colors.CYAN}{'='*70}{Colors.RESET}\n")
        
        test_concurrent_submit_cancel_with_retries(
            event_id=args.event_id,
            duration=args.duration
        )
