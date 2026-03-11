#!/usr/bin/env python3
"""
DAILY STRESS TEST - Multi-Account Coordinated Betting
=====================================================

Purpose: Replace autoplay cancel script for daily testing
- Aggressive coordinated betting between multiple accounts
- 5 iterations with configurable duration
- Robust error handling and recovery
- Comprehensive logging and reporting
- Production-ready for daily automation

Usage:
    python daily_stress_test.py --env sandbox --accounts 2 --duration 300 --iterations 5
    python daily_stress_test.py --env production --accounts 1,2,3 --duration 600 --iterations 3
"""

import threading
import time
import random
import logging
import argparse
import sys
import signal
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import json
import os
import uuid
import requests
from urllib.parse import urljoin

# Add the src directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

# Import your existing modules
from src.mm_calls import MMInteractions
from src import config


# Configure logging for production
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)-8s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f'daily_stress_test_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    ]
)
logger = logging.getLogger(__name__)


class StressTestCoordinator:
    """Coordinates multi-account stress testing with enhanced robustness"""
    
    def __init__(self, accounts: List[str], environment: str, test_duration: int, iterations: int):
        self.accounts = accounts
        self.environment = environment
        self.test_duration = test_duration  # seconds per iteration
        self.iterations = iterations
        self.current_target_event = None
        self.current_target_market = None
        self.target_lock = threading.Lock()
        self.shutdown_flag = threading.Event()
        self.iteration_results = []
        
        # Enhanced settings for aggressive testing
        self.config = {
            'bet_amount_range': (1, 5),  # $1-$5 per bet
            'batch_size_range': (10, 25),  # 10-25 bets per batch
            'bet_frequency_range': (2, 6),  # 2-6 seconds between batches
            'event_rotation_range': (15, 30),  # 15-30 seconds per event
            'max_concurrent_bets': 500,  # Maximum open bets per account
            'rate_limit_retry_delay': (5, 15),  # 5-15 second delay on rate limits
            'error_recovery_delay': (3, 10),  # 3-10 second delay on errors
        }
        
        # Available market types for rotation
        self.market_types = ['moneyline', 'spread', 'total']
        
        # Stats tracking
        self.stats = {
            account: {
                'total_bets': 0,
                'successful_batches': 0,
                'failed_batches': 0,
                'rate_limits': 0,
                'errors': 0,
                'balance_start': 0,
                'balance_end': 0,
                'session_start': None,
                'session_end': None
            } for account in accounts
        }

    def setup_signal_handlers(self):
        """Setup graceful shutdown handlers"""
        def signal_handler(signum, frame):
            logger.warning(f"🛑 Received signal {signum}, initiating graceful shutdown...")
            self.shutdown_flag.set()
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    def get_target_event_and_market(self) -> Tuple[Optional[str], Optional[str]]:
        """Get current target event and market with thread safety"""
        with self.target_lock:
            return self.current_target_event, self.current_target_market

    def set_target_event_and_market(self, event: str, market: str):
        """Set new target event and market with thread safety"""
        with self.target_lock:
            self.current_target_event = event
            self.current_target_market = market
            logger.info(f"🎯 COORDINATOR: New target -> '{event}' on {market} market")

    def rotate_target_events(self, mm_instance):
        """Background thread to rotate target events"""
        logger.info("🔄 Starting event rotation coordinator...")
        
        while not self.shutdown_flag.is_set():
            try:
                # Get available events (using names from sport_events values)
                available_events = [event for event in mm_instance.sport_events.values() if event.get('markets', [])]
                if not available_events:
                    logger.warning("⚠️ No events available, waiting...")
                    time.sleep(10)
                    continue
                
                # Select random event and market
                target_event_data = random.choice(available_events)
                target_event_name = target_event_data.get('name')
                target_market = random.choice(self.market_types)
                
                self.set_target_event_and_market(target_event_name, target_market)
                
                # Wait before next rotation
                rotation_delay = random.randint(*self.config['event_rotation_range'])
                self.shutdown_flag.wait(rotation_delay)
                
            except Exception as e:
                logger.error(f"❌ Event rotation error: {e}")
                self.shutdown_flag.wait(5)

    def get_market_selections(self, mm_instance, event_name: str, market_type: str) -> List[Dict]:
        """Get available selections for a market using the coordinated betting approach"""
        try:
            # Find the event in sport_events values (not keys)
            target_event = None
            for event_data in mm_instance.sport_events.values():
                if event_data.get('name') == event_name:
                    target_event = event_data
                    break
            
            if not target_event:
                return []
            
            # Find the market in the event's markets list
            target_market = None
            for market in target_event.get('markets', []):
                if market.get('type') == market_type:
                    target_market = market
                    break
            
            if not target_market:
                return []
            
            return target_market.get('selections', [])
            
        except Exception as e:
            logger.warning(f"⚠️ Error getting market selections: {e}")
            return []

    def create_batch_wagers(self, mm_instance, account: str, target_event_name: str, target_market: str) -> List[Dict]:
        """Create a batch of coordinated wagers using HTTP approach"""
        selections = self.get_market_selections(mm_instance, target_event_name, target_market)
        if not selections:
            return []
        
        # Pick a random selection from the market
        selection = random.choice(selections)
        if not selection or len(selection) == 0:
            return []
            
        selection_data = selection[0]  # Get the selection data
        odds_to_play = mm_instance._MMInteractions__get_random_odds()
        
        batch_size = random.randint(*self.config['batch_size_range'])
        external_ids = [str(uuid.uuid1()) for _ in range(batch_size)]
        
        batch_body = [{
            'external_id': external_ids[i],
            'line_id': selection_data['line_id'],
            'odds': odds_to_play,
            'stake': float(random.randint(*self.config['bet_amount_range']))
        } for i in range(batch_size)]
        
        return batch_body

    def account_betting_loop(self, account: str, mm_instance):
        """Main betting loop for a single account"""
        account_name = f"Account {account}"
        logger.info(f"🎯 {account_name} ({self.environment}) - Starting stress test loop...")
        
        self.stats[account]['session_start'] = datetime.now()
        
        # Get initial balance
        try:
            mm_instance.get_balance()
            balance = mm_instance.balance or 0
            self.stats[account]['balance_start'] = balance
            logger.info(f"💰 {account_name} ({self.environment}) - Starting balance: ${balance:.2f}")
        except Exception as e:
            logger.error(f"❌ {account_name}: Failed to get initial balance: {e}")
        
        consecutive_errors = 0
        max_consecutive_errors = 10
        
        while not self.shutdown_flag.is_set():
            try:
                # Check if we have too many open bets
                current_open = len(getattr(mm_instance, 'wagers', {}))
                if current_open >= self.config['max_concurrent_bets']:
                    logger.warning(f"⚠️ {account_name}: Max concurrent bets reached ({current_open}), waiting...")
                    self.shutdown_flag.wait(10)
                    continue
                
                # Get current target
                target_event, target_market = self.get_target_event_and_market()
                if not target_event or not target_market:
                    logger.warning(f"⚠️ {account_name}: No target event set, waiting...")
                    self.shutdown_flag.wait(5)
                    continue
                
                # Create batch wagers
                batch_body = self.create_batch_wagers(mm_instance, account, target_event, target_market)
                if not batch_body:
                    logger.warning(f"⚠️ {account_name}: No wagers created for {target_event}, trying next cycle...")
                    self.shutdown_flag.wait(3)
                    continue
                
                # Place the batch using HTTP request
                batch_play_url = urljoin(mm_instance.base_url, config.URL['mm_batch_place'])
                batch_response = requests.post(
                    batch_play_url, 
                    json={"data": batch_body},
                    headers=mm_instance._MMInteractions__get_auth_header()
                )
                
                if batch_response.status_code == 200:
                    try:
                        batch_result = batch_response.json().get('data', {}).get('succeed_wagers', [])
                        if batch_result:
                            successful_count = len(batch_result)
                            self.stats[account]['total_bets'] += successful_count
                            self.stats[account]['successful_batches'] += 1
                            
                            # Determine selection name
                            selection_name = "mixed selections"
                            if batch_body:
                                # Find the selection name from the batch
                                selections = self.get_market_selections(mm_instance, target_event, target_market)
                                for selection_group in selections:
                                    if selection_group and len(selection_group) > 0:
                                        selection_name = selection_group[0].get('name', 'unknown')
                                        break
                            
                            logger.info(f"🚀 {account_name} ({self.environment}): Placed {successful_count} bets on '{target_event}' {target_market} ({selection_name}) [Total: {self.stats[account]['total_bets']}]")
                            consecutive_errors = 0
                            
                            # Store wagers for tracking (if wagers dict exists)
                            if not hasattr(mm_instance, 'wagers'):
                                mm_instance.wagers = {}
                            for wager in batch_result:
                                mm_instance.wagers[wager['external_id']] = wager['id']
                            
                        else:
                            logger.warning(f"⚠️ {account_name} ({self.environment}): Batch response 200 but no wagers succeeded")
                            self.stats[account]['failed_batches'] += 1
                            
                    except Exception as e:
                        logger.error(f"❌ {account_name}: Error parsing batch response: {e}")
                        self.stats[account]['errors'] += 1
                        consecutive_errors += 1
                        
                elif batch_response.status_code == 429:
                    # Rate limited
                    self.stats[account]['rate_limits'] += 1
                    delay = random.randint(*self.config['rate_limit_retry_delay'])
                    logger.warning(f"⚠️ {account_name} ({self.environment}): Rate limited, waiting {delay}s...")
                    self.shutdown_flag.wait(delay)
                    continue
                
                else:
                    # Other error
                    self.stats[account]['failed_batches'] += 1
                    self.stats[account]['errors'] += 1
                    consecutive_errors += 1
                    
                    logger.warning(f"❌ {account_name} ({self.environment}): Batch bet failed - {batch_response.status_code}")
                    
                    if consecutive_errors >= max_consecutive_errors:
                        logger.error(f"❌ {account_name}: Too many consecutive errors ({consecutive_errors}), stopping this account")
                        break
                    
                    # Error recovery delay
                    delay = random.randint(*self.config['error_recovery_delay'])
                    self.shutdown_flag.wait(delay)
                    continue
                
                # Wait before next batch
                bet_delay = random.randint(*self.config['bet_frequency_range'])
                self.shutdown_flag.wait(bet_delay)
                
            except Exception as e:
                self.stats[account]['errors'] += 1
                consecutive_errors += 1
                logger.error(f"❌ {account_name}: Unexpected error in betting loop: {e}")
                
                if consecutive_errors >= max_consecutive_errors:
                    logger.error(f"❌ {account_name}: Too many consecutive errors, stopping this account")
                    break
                
                self.shutdown_flag.wait(random.randint(5, 15))
        
        # Finalize stats
        self.stats[account]['session_end'] = datetime.now()
        
        # Get final balance
        try:
            mm_instance.get_balance()
            self.stats[account]['balance_end'] = mm_instance.balance or 0
        except Exception as e:
            logger.error(f"❌ {account_name}: Failed to get final balance: {e}")
        
        logger.info(f"🏁 {account_name} ({self.environment}) - Betting loop finished")

    def setup_account(self, account: str) -> Optional[MMInteractions]:
        """Setup and initialize an account for testing"""
        account_name = f"Account {account}"
        logger.info(f"🚀 Starting stress test for {account_name} ({self.environment})")
        
        try:
            # Initialize MM instance
            mm_instance = MMInteractions()
            
            # Get credentials and setup
            logger.info(f"🔐 {account_name} ({self.environment}) - Logging in...")
            credentials = config.get_account_credentials(int(account), self.environment)
            mm_instance.mm_keys = {
                'access_key': credentials['access_key'],
                'secret_key': credentials['secret_key']
            }
            
            # Login
            login_result = mm_instance.mm_login()
            if not login_result:
                logger.error(f"❌ {account_name}: Login failed")
                return None
            
            # Seed events
            logger.info(f"🌱 {account_name} ({self.environment}) - Seeding...")
            mm_instance.seeding()
            
            # Check if seeding was successful
            if not mm_instance.sport_events:
                logger.error(f"❌ {account_name}: Event seeding failed - no events available")
                return None
            
            available_events = len(mm_instance.sport_events)
            logger.info(f"📋 {account_name}: {available_events} events available for stress testing")
            
            return mm_instance
            
        except Exception as e:
            logger.error(f"❌ {account_name}: Setup failed: {e}")
            return None

    def print_iteration_report(self, iteration: int, duration: int):
        """Print detailed report for an iteration"""
        logger.info("=" * 80)
        logger.info(f"📊 ITERATION {iteration} STRESS TEST REPORT - {duration}s Duration")
        logger.info("=" * 80)
        
        total_bets = sum(stats['total_bets'] for stats in self.stats.values())
        total_successful = sum(stats['successful_batches'] for stats in self.stats.values())
        total_failed = sum(stats['failed_batches'] for stats in self.stats.values())
        total_rate_limits = sum(stats['rate_limits'] for stats in self.stats.values())
        total_errors = sum(stats['errors'] for stats in self.stats.values())
        
        logger.info(f"🎯 OVERALL PERFORMANCE:")
        logger.info(f"  • Total Accounts: {len(self.accounts)}")
        logger.info(f"  • Total Bets Placed: {total_bets}")
        logger.info(f"  • Successful Batches: {total_successful}")
        logger.info(f"  • Failed Batches: {total_failed}")
        logger.info(f"  • Rate Limits Hit: {total_rate_limits}")
        logger.info(f"  • Errors Encountered: {total_errors}")
        logger.info(f"  • Bets per Minute: {(total_bets / (duration / 60)):.1f}")
        logger.info(f"  • Success Rate: {(total_successful / max(total_successful + total_failed, 1) * 100):.1f}%")
        
        logger.info(f"")
        logger.info(f"📋 PER-ACCOUNT BREAKDOWN:")
        
        for account in self.accounts:
            stats = self.stats[account]
            account_name = f"Account {account}"
            
            # Calculate session duration
            session_duration = 0
            if stats['session_start'] and stats['session_end']:
                session_duration = (stats['session_end'] - stats['session_start']).total_seconds()
            
            balance_change = stats['balance_end'] - stats['balance_start'] if stats['balance_start'] > 0 else 0
            
            logger.info(f"  🏦 {account_name}:")
            logger.info(f"    • Bets Placed: {stats['total_bets']}")
            logger.info(f"    • Successful Batches: {stats['successful_batches']}")
            logger.info(f"    • Failed Batches: {stats['failed_batches']}")
            logger.info(f"    • Rate Limits: {stats['rate_limits']}")
            logger.info(f"    • Errors: {stats['errors']}")
            logger.info(f"    • Starting Balance: ${stats['balance_start']:.2f}")
            logger.info(f"    • Ending Balance: ${stats['balance_end']:.2f}")
            logger.info(f"    • Balance Change: ${balance_change:+.2f}")
            if session_duration > 0:
                logger.info(f"    • Bets per Minute: {(stats['total_bets'] / (session_duration / 60)):.1f}")
        
        # Store iteration results
        self.iteration_results.append({
            'iteration': iteration,
            'duration': duration,
            'total_bets': total_bets,
            'successful_batches': total_successful,
            'failed_batches': total_failed,
            'rate_limits': total_rate_limits,
            'errors': total_errors,
            'accounts': dict(self.stats)
        })
        
        logger.info("=" * 80)

    def print_final_report(self):
        """Print comprehensive final report across all iterations"""
        logger.info("🏆" * 80)
        logger.info("🏆 FINAL DAILY STRESS TEST REPORT")
        logger.info("🏆" * 80)
        
        if not self.iteration_results:
            logger.info("❌ No iterations completed")
            return
        
        total_duration = sum(r['duration'] for r in self.iteration_results)
        total_bets_all = sum(r['total_bets'] for r in self.iteration_results)
        total_successful_all = sum(r['successful_batches'] for r in self.iteration_results)
        total_failed_all = sum(r['failed_batches'] for r in self.iteration_results)
        total_rate_limits_all = sum(r['rate_limits'] for r in self.iteration_results)
        total_errors_all = sum(r['errors'] for r in self.iteration_results)
        
        logger.info(f"📈 AGGREGATE STATISTICS:")
        logger.info(f"  • Total Iterations: {len(self.iteration_results)}")
        logger.info(f"  • Total Test Time: {total_duration}s ({total_duration/60:.1f} minutes)")
        logger.info(f"  • Total Bets Placed: {total_bets_all}")
        logger.info(f"  • Total Successful Batches: {total_successful_all}")
        logger.info(f"  • Total Failed Batches: {total_failed_all}")
        logger.info(f"  • Total Rate Limits: {total_rate_limits_all}")
        logger.info(f"  • Total Errors: {total_errors_all}")
        logger.info(f"  • Overall Bets per Minute: {(total_bets_all / (total_duration / 60)):.1f}")
        logger.info(f"  • Overall Success Rate: {(total_successful_all / max(total_successful_all + total_failed_all, 1) * 100):.1f}%")
        
        logger.info(f"")
        logger.info(f"📊 ITERATION BREAKDOWN:")
        for result in self.iteration_results:
            logger.info(f"  Iteration {result['iteration']}: {result['total_bets']} bets, {result['successful_batches']} successful, {result['rate_limits']} rate limits, {result['errors']} errors")
        
        logger.info("🏆" * 80)
        
        # Save results to file
        results_file = f"stress_test_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        try:
            with open(results_file, 'w') as f:
                json.dump({
                    'test_config': {
                        'accounts': self.accounts,
                        'environment': self.environment,
                        'iterations': self.iterations,
                        'duration_per_iteration': self.test_duration,
                        'config': self.config
                    },
                    'results': self.iteration_results,
                    'summary': {
                        'total_duration': total_duration,
                        'total_bets': total_bets_all,
                        'total_successful_batches': total_successful_all,
                        'total_failed_batches': total_failed_all,
                        'total_rate_limits': total_rate_limits_all,
                        'total_errors': total_errors_all,
                        'overall_bets_per_minute': total_bets_all / (total_duration / 60) if total_duration > 0 else 0,
                        'overall_success_rate': (total_successful_all / max(total_successful_all + total_failed_all, 1) * 100)
                    }
                }, f, indent=2, default=str)
            logger.info(f"📁 Results saved to: {results_file}")
        except Exception as e:
            logger.error(f"❌ Failed to save results: {e}")

    def run_iteration(self, iteration: int) -> bool:
        """Run a single stress test iteration"""
        logger.info(f"🎬 STARTING ITERATION {iteration}/{self.iterations}")
        logger.info(f"🌐 Environment: {self.environment.upper()}")
        logger.info(f"👥 Accounts: {', '.join([f'Account {a}' for a in self.accounts])}")
        logger.info(f"⏱️ Duration: {self.test_duration}s ({self.test_duration/60:.1f} minutes)")
        logger.info(f"🎲 Strategy: Aggressive coordinated batch betting")
        logger.info(f"💥 NO CANCELLATION: All bets remain open")
        
        # Reset stats for this iteration
        for account in self.accounts:
            self.stats[account] = {
                'total_bets': 0,
                'successful_batches': 0,
                'failed_batches': 0,
                'rate_limits': 0,
                'errors': 0,
                'balance_start': 0,
                'balance_end': 0,
                'session_start': None,
                'session_end': None
            }
        
        # Setup accounts
        mm_instances = {}
        for account in self.accounts:
            mm_instance = self.setup_account(account)
            if mm_instance:
                mm_instances[account] = mm_instance
                logger.info(f"✅ Account {account} setup complete")
            else:
                logger.error(f"❌ Account {account} setup failed, skipping this iteration")
                return False
        
        if not mm_instances:
            logger.error("❌ No accounts successfully initialized")
            return False
        
        self.shutdown_flag.clear()
        threads = []
        
        try:
            # Start event rotation coordinator (using first available instance)
            coordinator_instance = list(mm_instances.values())[0]
            coordinator_thread = threading.Thread(
                target=self.rotate_target_events,
                args=(coordinator_instance,),
                daemon=True
            )
            coordinator_thread.start()
            threads.append(coordinator_thread)
            
            # Start betting threads for each account
            for account, mm_instance in mm_instances.items():
                betting_thread = threading.Thread(
                    target=self.account_betting_loop,
                    args=(account, mm_instance),
                    daemon=True
                )
                betting_thread.start()
                threads.append(betting_thread)
                logger.info(f"✅ Started stress testing for Account {account}")
            
            logger.info(f"🏃 All {len(mm_instances)} accounts running aggressive stress test...")
            
            # Run for specified duration
            start_time = time.time()
            while time.time() - start_time < self.test_duration and not self.shutdown_flag.is_set():
                time.sleep(1)
            
            # Signal shutdown
            logger.info(f"⏱️ Iteration {iteration} duration complete, stopping...")
            self.shutdown_flag.set()
            
            # Wait for threads to finish
            for thread in threads:
                thread.join(timeout=30)
            
            # Print iteration report
            self.print_iteration_report(iteration, self.test_duration)
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Iteration {iteration} failed: {e}")
            self.shutdown_flag.set()
            return False

    def run_daily_stress_test(self) -> bool:
        """Run the complete daily stress test with multiple iterations"""
        logger.info("🚀" * 50)
        logger.info("🚀 DAILY STRESS TEST STARTING")
        logger.info("🚀" * 50)
        
        self.setup_signal_handlers()
        
        successful_iterations = 0
        
        for iteration in range(1, self.iterations + 1):
            try:
                if self.run_iteration(iteration):
                    successful_iterations += 1
                    logger.info(f"✅ Iteration {iteration} completed successfully")
                else:
                    logger.error(f"❌ Iteration {iteration} failed")
                
                # Wait between iterations (except for the last one)
                if iteration < self.iterations and not self.shutdown_flag.is_set():
                    logger.info(f"⏸️ Waiting 30 seconds before iteration {iteration + 1}...")
                    time.sleep(30)
                    
            except KeyboardInterrupt:
                logger.warning("🛑 Received interrupt, stopping stress test...")
                break
            except Exception as e:
                logger.error(f"❌ Unexpected error in iteration {iteration}: {e}")
        
        # Print final comprehensive report
        self.print_final_report()
        
        success_rate = (successful_iterations / self.iterations * 100)
        logger.info(f"🎯 FINAL RESULT: {successful_iterations}/{self.iterations} iterations successful ({success_rate:.1f}%)")
        
        if successful_iterations == self.iterations:
            logger.info("🏆 DAILY STRESS TEST COMPLETED SUCCESSFULLY!")
            return True
        else:
            logger.warning(f"⚠️ DAILY STRESS TEST COMPLETED WITH {self.iterations - successful_iterations} FAILED ITERATIONS")
            return False


def main():
    parser = argparse.ArgumentParser(description='Daily Stress Test - Multi-Account Coordinated Betting')
    parser.add_argument('--env', '--environment', default='sandbox', 
                       choices=['sandbox', 'production'],
                       help='Environment to test (default: sandbox)')
    parser.add_argument('--accounts', default='1,2',
                       help='Comma-separated list of account numbers (default: 1,2)')
    parser.add_argument('--duration', type=int, default=300,
                       help='Duration per iteration in seconds (default: 300)')
    parser.add_argument('--iterations', type=int, default=5,
                       help='Number of iterations to run (default: 5)')
    
    args = parser.parse_args()
    
    # Parse accounts
    try:
        accounts = [acc.strip() for acc in args.accounts.split(',')]
        if not accounts:
            raise ValueError("No accounts specified")
    except Exception as e:
        logger.error(f"❌ Invalid accounts format: {e}")
        sys.exit(1)
    
    # Validate environment
    if args.env not in ['sandbox', 'production']:
        logger.error(f"❌ Invalid environment: {args.env}")
        sys.exit(1)
    
    # Safety check for production
    if args.env == 'production':
        confirm = input("⚠️  You are about to run stress tests on PRODUCTION. Are you sure? (yes/no): ")
        if confirm.lower() != 'yes':
            logger.info("Stress test cancelled.")
            sys.exit(0)
    
    logger.info(f"🎯 Daily Stress Test Configuration:")
    logger.info(f"  • Environment: {args.env}")
    logger.info(f"  • Accounts: {', '.join(accounts)}")
    logger.info(f"  • Duration per iteration: {args.duration}s ({args.duration/60:.1f} minutes)")
    logger.info(f"  • Total iterations: {args.iterations}")
    logger.info(f"  • Total estimated time: {args.iterations * args.duration / 60:.1f} minutes")
    
    # Initialize and run stress test
    coordinator = StressTestCoordinator(
        accounts=accounts,
        environment=args.env,
        test_duration=args.duration,
        iterations=args.iterations
    )
    
    success = coordinator.run_daily_stress_test()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()