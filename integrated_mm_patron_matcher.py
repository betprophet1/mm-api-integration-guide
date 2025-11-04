#!/usr/bin/env python3
"""
Integrated MM and Patron Matcher

Runs MM accounts and patron matcher in the same process with shared bet queue.
MM bets are automatically forwarded to the patron matcher for matching.
"""

import argparse
import signal
import sys
import time
import threading
import os
import random

sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.mm_calls import MMInteractions
from src.log import logging
from src import config
from patron_match_mm_bets import PatronAccountMatcher

# Global variables
should_stop = False
patron_matcher = None
mm_instances = []
token_expired = False
restart_lock = threading.Lock()


def signal_handler(sig, frame):
    """Handle Ctrl+C to gracefully stop"""
    global should_stop, patron_matcher
    logging.info("\n🛑 Received stop signal. Stopping all processes...")
    should_stop = True
    if patron_matcher:
        patron_matcher.stop()
    sys.exit(0)


class IntegratedMMInteractions(MMInteractions):
    """Extended MM interactions that forwards bets to patron matcher"""
    
    def __init__(self, account_name="MM Account"):
        super().__init__()
        self.account_name = account_name
    
    def place_wager(self, line_id, odds, stake):
        """Place a single wager"""
        from urllib.parse import urljoin
        import requests
        import json
        import uuid
        from src import config
        global token_expired
        
        play_url = urljoin(self.base_url, config.URL['mm_place_wager'])
        external_id = str(uuid.uuid1())
        
        body_to_send = {
            'external_id': external_id,
            'line_id': line_id,
            'odds': odds,
            'stake': stake
        }
        
        play_response = requests.post(play_url, json=body_to_send, headers=self._MMInteractions__get_auth_header())
        
        if play_response.status_code != 200:
            # Check if token expired
            try:
                error_data = json.loads(play_response.content)
                if error_data.get('error') == 'unauthorized' or 'token expired' in str(error_data.get('message', '')).lower():
                    logging.warning(f"🔄 {self.account_name} - Token expired, triggering restart...")
                    token_expired = True
                    return None
            except:
                pass
            logging.error(f"{self.account_name} - Failed to place wager: {play_response.content}")
            return None
        
        try:
            response_data = json.loads(play_response.content)
            wager_data = response_data.get('data', {})
            if 'wager' in wager_data and 'id' in wager_data['wager']:
                wager_id = wager_data['wager']['id']
                self.wagers[external_id] = wager_id
                logging.info(f"✅ {self.account_name} - Wager placed successfully")
                return {'external_id': external_id, 'wager_id': wager_id}
            else:
                logging.warning(f"⚠️ {self.account_name} - Response 200 but no wager data: {response_data}")
                return None
        except Exception as e:
            logging.error(f"❌ {self.account_name} - Error parsing response: {e}")
            return None
        
    def place_wager_with_notification(self, line_id, odds, stake, event_id=None, market_id=None, outcome_id=None):
        """Place a wager and notify patron matcher"""
        global patron_matcher
        
        # Place the wager using parent method
        result = self.place_wager(line_id, odds, stake)
        
        # If successful and we have the required info, notify patron matcher
        if result and patron_matcher and event_id and market_id and outcome_id:
            try:
                # Add bet to patron matcher queue with line_id
                patron_matcher.add_mm_bet(
                    event_id=event_id,
                    market_id=market_id,
                    outcome_id=outcome_id,
                    odds=odds,
                    stake=stake,
                    line_id=line_id
                )
                logging.info(f"📤 {self.account_name} - Forwarded bet to patron matcher: Event {event_id}, Market {market_id}, Outcome {outcome_id}")
            except Exception as e:
                logging.error(f"❌ {self.account_name} - Failed to forward bet to patron: {e}")
        
        return result
    
    def auto_playing_with_patron(self, target_event='ALL'):
        """Auto play with patron matcher integration"""
        global should_stop, token_expired
        
        while not should_stop and not token_expired:
            try:
                # Get events and filter
                if target_event.upper() == 'ALL':
                    target_events = list(self.sport_events.values())
                else:
                    target_events = [
                        event for event in self.sport_events.values()
                        if target_event.lower() in event['name'].lower()
                    ]
                
                if not target_events:
                    logging.warning(f"⚠️ {self.account_name} - No events found. Waiting...")
                    time.sleep(5)
                    continue
                
                # Pick a random event
                event = random.choice(target_events)
                event_id = event['event_id']
                
                # Get markets for this event
                markets = event.get('markets', [])
                if not markets:
                    # AGGRESSIVE: Skip immediately, no delay
                    continue
                
                # Pick a random market
                market = random.choice(markets)
                market_id = market.get('market_id') or market.get('id')
                
                # Get selections
                selections = market.get('selections', [])
                if not selections:
                    # AGGRESSIVE: Skip immediately, no delay
                    continue
                
                # Pick a random selection
                flat_selections = []
                for sel_group in selections:
                    if isinstance(sel_group, list):
                        flat_selections.extend(sel_group)
                    else:
                        flat_selections.append(sel_group)
                
                if not flat_selections:
                    continue
                
                selection = random.choice(flat_selections)
                line_id = selection.get('line_id')
                outcome_id = selection.get('outcome_id') or selection.get('id')
                
                if not line_id:
                    continue
                
                # Get random odds
                if self.valid_odds:
                    odds = random.choice(self.valid_odds)
                else:
                    odds = 2.0
                
                stake = 1.0
                
                # Place wager with notification
                logging.info(f"🎲 {self.account_name} - Placing bet: {event['name']} (Event {event_id}, Market {market_id}, Outcome {outcome_id}) @ {odds}")
                self.place_wager_with_notification(
                    line_id=line_id,
                    odds=odds,
                    stake=stake,
                    event_id=event_id,
                    market_id=market_id,
                    outcome_id=outcome_id
                )
                
                # AGGRESSIVE: Minimal delay between bets
                time.sleep(random.uniform(0.1, 0.5))
                
            except Exception as e:
                logging.error(f"❌ {self.account_name} - Error in auto play: {e}")
                time.sleep(5)


def run_mm_account(account_num, target_event, environment):
    """Run MM account with patron integration"""
    global should_stop, mm_instances, token_expired
    
    account_name = f"MM Account {account_num}"
    logging.info(f"🚀 Starting {account_name}")
    
    try:
        # Get credentials
        credentials = config.get_account_credentials(account_num, environment)
        
        # Create MM instance
        mm_instance = IntegratedMMInteractions(account_name)
        mm_instance.mm_keys = {
            'access_key': credentials['access_key'],
            'secret_key': credentials['secret_key']
        }
        mm_instances.append(mm_instance)
        
        # Login
        logging.info(f"🔐 {account_name} - Logging in...")
        mm_instance.mm_login()
        
        # Get balance
        mm_instance.get_balance()
        logging.info(f"💰 {account_name} - Balance: ${mm_instance.balance:.2f}")
        
        # Seed
        logging.info(f"🌱 {account_name} - Seeding...")
        mm_instance.seeding()
        
        # Start auto play with patron integration
        logging.info(f"🎰 {account_name} - Starting auto play with patron integration...")
        mm_instance.auto_playing_with_patron(target_event)
        
        # Check if token expired
        if token_expired:
            logging.info(f"🔄 {account_name} - Detected token expiration")
        
    except Exception as e:
        logging.error(f"❌ {account_name} - Error: {e}")
    finally:
        logging.info(f"✅ {account_name} - Stopped")


def run_patron_matcher(environment, match_rate, match_delay):
    """Run patron matcher"""
    global should_stop, patron_matcher, token_expired
    
    logging.info("💰 Starting Patron Matcher...")
    
    try:
        patron_matcher = PatronAccountMatcher(
            environment=environment,
            match_rate=match_rate,
            match_delay=match_delay
        )
        
        if not patron_matcher.start():
            logging.error("❌ Failed to start patron matcher")
            return
        
        logging.info("✅ Patron matcher started and waiting for MM bets")
        
        # Keep patron matcher alive
        while not should_stop and not token_expired:
            time.sleep(1)
            
    except Exception as e:
        logging.error(f"❌ Patron matcher error: {e}")
    finally:
        if patron_matcher:
            patron_matcher.stop()
        logging.info("✅ Patron matcher stopped")


def run_session(args, account_numbers):
    """Run a single session with MM and patron matcher"""
    global should_stop, token_expired, mm_instances, patron_matcher
    
    # Reset global state for new session
    should_stop = False
    token_expired = False
    mm_instances = []
    patron_matcher = None
    
    logging.info("🚀 INTEGRATED MM + PATRON MATCHER")
    logging.info("=" * 70)
    logging.info(f"Environment: {args.env.upper()}")
    logging.info(f"MM Accounts: {', '.join(map(str, account_numbers))}")
    logging.info(f"Target Event: {args.event}")
    logging.info(f"Patron Match Rate: {args.match_rate:.0%}")
    logging.info(f"Patron Match Delay: {args.match_delay}s")
    logging.info("=" * 70)
    logging.info("ℹ️  MM bets will be automatically forwarded to patron matcher")
    logging.info("ℹ️  Auto-restart enabled on token expiration")
    logging.info("ℹ️  Press Ctrl+C to stop all processes")
    logging.info("=" * 70)
    
    # Start patron matcher in a separate thread
    patron_thread = threading.Thread(
        target=run_patron_matcher,
        args=(args.env, args.match_rate, args.match_delay),
        daemon=False,
        name="PatronMatcher"
    )
    patron_thread.start()
    
    # Give patron matcher time to initialize
    time.sleep(3)
    
    # Start MM account threads
    mm_threads = []
    for account_num in account_numbers:
        thread = threading.Thread(
            target=run_mm_account,
            args=(account_num, args.event, args.env),
            daemon=False,
            name=f"MMAccount{account_num}"
        )
        mm_threads.append(thread)
        thread.start()
        time.sleep(1)
    
    logging.info(f"✅ Started {len(mm_threads)} MM accounts and 1 patron matcher")
    logging.info("📊 Monitoring activity...")
    
    # Keep main thread alive
    try:
        while not should_stop and not token_expired:
            time.sleep(1)
            
            # Check if token expired
            if token_expired:
                logging.info("\n🔄 TOKEN EXPIRED - Initiating restart...")
                should_stop = True
                break
                
    except KeyboardInterrupt:
        logging.info("\n🛑 Stopping all processes...")
        should_stop = True
    
    # Wait for threads to finish
    logging.info("⏳ Waiting for threads to stop...")
    for thread in mm_threads + [patron_thread]:
        thread.join(timeout=5)
    
    logging.info("✅ All processes stopped")
    
    # Return True if token expired (should restart), False otherwise
    return token_expired


def main():
    """Main entry point with auto-restart on token expiration"""
    global should_stop
    
    parser = argparse.ArgumentParser(
        description='Integrated MM and Patron Matcher with Auto-Restart'
    )
    parser.add_argument(
        '--env',
        type=str,
        default='sandbox',
        choices=['sandbox', 'staging'],
        help='Environment (default: sandbox)'
    )
    parser.add_argument(
        '--accounts',
        type=str,
        default='1,2',
        help='MM accounts to run (default: 1,2)'
    )
    parser.add_argument(
        '--event',
        type=str,
        default='ALL',
        help='Target event name or "ALL" (default: ALL)'
    )
    parser.add_argument(
        '--match-rate',
        type=float,
        default=0.7,
        help='Patron match rate (0.0-1.0, default: 0.7)'
    )
    parser.add_argument(
        '--match-delay',
        type=float,
        default=0.1,
        help='Patron match delay in seconds (default: 0.1)'
    )
    
    args = parser.parse_args()
    
    # Parse account numbers
    try:
        account_numbers = [int(x.strip()) for x in args.accounts.split(',')]
    except ValueError:
        logging.error(f"❌ Invalid account numbers: {args.accounts}")
        sys.exit(1)
    
    # Validate match rate
    if not 0.0 <= args.match_rate <= 1.0:
        logging.error("❌ Match rate must be between 0.0 and 1.0")
        sys.exit(1)
    
    # Set up signal handler
    signal.signal(signal.SIGINT, signal_handler)
    
    # Main restart loop
    restart_count = 0
    while True:
        if restart_count > 0:
            logging.info("")
            logging.info("="*70)
            logging.info(f"🔄 RESTARTING SESSION #{restart_count + 1} (Token refresh)")
            logging.info("="*70)
            time.sleep(2)  # Brief pause before restart
        
        # Run session
        should_restart = run_session(args, account_numbers)
        
        # Check if we should restart or exit
        if not should_restart:
            # User stopped with Ctrl+C or other reason
            logging.info("👋 Session ended by user")
            break
        
        # Token expired, restart with fresh login
        restart_count += 1
        logging.info(f"🔄 Token expired. Restarting with fresh login... (Restart #{restart_count})")
        time.sleep(3)  # Wait before restarting


if __name__ == '__main__':
    main()
