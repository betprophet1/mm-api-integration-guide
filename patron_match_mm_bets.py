#!/usr/bin/env python3
"""
Patron Account Bet Matching Script

This script runs a patron account (lam.tran+usr004@betprophet.co) that:
1. Lists available events from the patron account
2. Monitors MM account bets via WebSocket
3. Matches random bets from MM accounts
4. Tracks all activity in real-time

Usage:
    python patron_match_mm_bets.py --env sandbox --match-rate 0.7
    
Options:
    --env: Environment (sandbox or staging)
    --match-rate: Probability of matching a bet when one is detected (0.0-1.0)
    --match-delay: Delay in seconds before matching (0.1-5.0), default 0.5
"""

import argparse
import signal
import sys
import time
import threading
import os
import random
import json
import requests
import pysher
import base64
import uuid
from urllib.parse import urljoin
from datetime import datetime
from typing import Dict, List, Any

sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src import config
from src.log import logging


class PatronAccountMatcher:
    """Patron account that fetches events and matches MM bets"""
    
    def __init__(self, environment='sandbox', match_rate=0.7, match_delay=0.5):
        self.environment = environment
        self.base_url = config.ENVIRONMENT_URLS.get(environment, config.ENVIRONMENT_URLS['sandbox'])
        self.match_rate = match_rate
        self.match_delay = match_delay
        
        # Credentials for patron account
        self.patron_credentials = None
        self.patron_session = None
        self.patron_jwt = None
        
        # Events and markets
        self.available_events = {}
        self.all_tournaments = {}
        self.valid_odds = []
        
        # Statistics
        self.session_stats = {
            'start_time': time.time(),
            'events_fetched': 0,
            'bets_matched': 0,
            'match_attempts': 0,
            'successful_matches': 0,
            'failed_matches': 0,
            'balance': 0
        }
        
        # Event tracking for matching
        self.mm_bets_to_match = []
        self.matched_bets = {}
        self.lock = threading.Lock()
        self.is_running = True
        
    def load_patron_credentials(self):
        """Load patron account credentials from config"""
        try:
            # Try to load patron-specific credentials based on environment
            if self.environment == 'sandbox':
                patron_config_file = 'user_info_patron.json'
            else:
                patron_config_file = f'user_info_patron_{self.environment}.json'
            
            patron_config_path = os.path.join(
                os.path.dirname(os.path.abspath(__file__)), 
                'src', 
                patron_config_file
            )
            
            if os.path.exists(patron_config_path):
                with open(patron_config_path) as f:
                    config_data = json.load(f)
                    self.patron_credentials = {
                        'email': config_data.get('email'),
                        'password': config_data.get('password'),
                        'tournaments': config_data.get('tournaments', ['MLB'])
                    }
                    logging.info(f"✅ Loaded patron credentials from {patron_config_file}")
                    return True
            else:
                logging.warning(f"⚠️  Patron config file not found at {patron_config_path}")
                logging.info("📝 Please create src/user_info_patron.json with patron account credentials")
                return False
                
        except Exception as e:
            logging.error(f"❌ Error loading patron credentials: {e}")
            return False
    
    def patron_login(self) -> bool:
        """Login to patron account using web authentication"""
        try:
            # Use web authentication endpoint
            login_url = urljoin(self.base_url, 'api/v1/auth/login')
            device_id = str(uuid.uuid1())
            
            headers = {
                '__source': 'web',
                'accept': 'application/json, text/plain, */*',
                'content-type': 'application/json',
                'origin': self.base_url.replace('api-', ''),
                'x-currency': 'cash'
            }
            
            request_body = {
                'email': self.patron_credentials['email'],
                'password': self.patron_credentials['password'],
                'code': '123456',
                'device_id': device_id
            }
            
            response = requests.post(login_url, headers=headers, json=request_body)
            
            if response.status_code != 200:
                logging.error(f"❌ Patron login failed: {response.status_code}")
                logging.error(f"Response: {response.text}")
                return False
            
            response_data = response.json()
            self.patron_jwt = response_data.get('accessToken')
            
            if not self.patron_jwt:
                logging.error("❌ No access token received from login")
                return False
            
            logging.info("✅ Patron account login successful")
            return True
            
        except Exception as e:
            logging.error(f"❌ Error during patron login: {e}")
            return False
    
    def __get_patron_auth_header(self) -> Dict[str, str]:
        """Get authentication header for patron account"""
        return {
            'Authorization': f'Bearer {self.patron_jwt}',
            'Content-Type': 'application/json',
            'x-currency': 'cash',
            'accept': 'application/json'
        }
    
    def seed_patron_events(self):
        """Fetch tournaments, events and markets for patron account
        
        Note: Patron accounts don't have MM API access, so we skip seeding.
        Events will be added dynamically as MM bets come in.
        """
        logging.info("ℹ️  Patron account - skipping event seeding (will use MM bet data)")
        logging.info("ℹ️  Events will be populated dynamically from incoming MM bets")
        return True
    
    def get_patron_balance(self) -> float:
        """Get patron account balance using web API"""
        try:
            balance_url = urljoin(self.base_url, 'api/v1/wallet')
            response = requests.get(balance_url, headers=self.__get_patron_auth_header())
            
            if response.status_code == 200:
                wallet_data = response.json().get('data', {})
                balance = wallet_data.get('balance', 0)
                self.session_stats['balance'] = balance
                logging.info(f"💰 Patron balance: ${balance:.2f}")
                return balance
            else:
                logging.error(f"❌ Failed to get patron balance: {response.status_code}")
                return 0
                
        except Exception as e:
            logging.error(f"❌ Error getting patron balance: {e}")
            return 0
    
    def match_bet(self, event_id: int, market_id: int, outcome_id: int, odds: float, stake: float = 1.0, line_id: str = None):
        """Place a matching bet using web API
        
        For proper matching:
        - Use negative odds (if MM placed -13000, patron uses 13000)
        - Use opposite outcome (if MM placed outcome 4, patron uses outcome 5)
        
        Args:
            event_id: Event ID from MM bet
            market_id: Market ID from MM bet  
            outcome_id: Outcome/selection ID from MM bet
            odds: Odds from MM bet (will be negated)
            stake: Bet stake amount (default 1.0)
            line_id: Line ID from MM bet (required for patron API)
        """
        try:
            # Calculate opposite odds (negate)
            opposite_odds = -odds
            
            # Calculate opposite outcome (toggle between 4 and 5, or 1 and 2)
            # Common pattern: if outcome is 4, opposite is 5, if 5 then 4
            # For outcomes 1/2, if 1 then 2, if 2 then 1
            if outcome_id == 4:
                opposite_outcome = 5
            elif outcome_id == 5:
                opposite_outcome = 4
            elif outcome_id == 1:
                opposite_outcome = 2
            elif outcome_id == 2:
                opposite_outcome = 1
            else:
                # Default fallback - try toggling between adjacent outcomes
                opposite_outcome = outcome_id + 1 if outcome_id % 2 == 1 else outcome_id - 1
            
            if not line_id:
                logging.warning(f"⚠️  No line_id provided for matching, cannot place bet")
                self.session_stats['failed_matches'] += 1
                return False
            
            # Place bet using correct patron API endpoint
            bet_url = urljoin(self.base_url, 'trade/private/api/v2/wagers')
            
            # Use simple body format matching the curl example
            bet_body = {
                'lineID': line_id,
                'odds': opposite_odds,
                'stake': stake
            }
            
            # Add required headers
            headers = self.__get_patron_auth_header()
            headers['__source'] = 'web'
            headers['origin'] = self.base_url.replace('api-', '')
            
            response = requests.post(
                bet_url,
                json=bet_body,
                headers=headers
            )
            
            if response.status_code == 200 or response.status_code == 201:
                response_data = response.json()
                
                bet_id = str(uuid.uuid4())
                self.matched_bets[bet_id] = {
                    'bet_id': bet_id,
                    'event_id': event_id,
                    'market_id': market_id,
                    'original_outcome': outcome_id,
                    'matched_outcome': opposite_outcome,
                    'original_odds': odds,
                    'matched_odds': opposite_odds,
                    'stake': stake,
                    'line_id': line_id,
                    'timestamp': datetime.now().isoformat()
                }
                self.session_stats['successful_matches'] += 1
                logging.info(f"✅ MATCHED: Event {event_id} Market {market_id} - MM Outcome {outcome_id} @ {odds} -> Patron Outcome {opposite_outcome} @ {opposite_odds} (Stake: ${stake})")
                return True
            else:
                self.session_stats['failed_matches'] += 1
                logging.warning(f"⚠️  Failed to match bet: {response.status_code} - {response.text}")
                return False
            
        except Exception as e:
            logging.error(f"❌ Error matching bet: {e}")
            self.session_stats['failed_matches'] += 1
            return False
    
    def add_mm_bet(self, event_id: int, market_id: int, outcome_id: int, odds: float, stake: float = 1.0, line_id: str = None):
        """Add an MM bet to the matching queue
        
        Args:
            event_id: Event ID
            market_id: Market ID
            outcome_id: Outcome/selection ID
            odds: Odds of the bet
            stake: Bet stake amount (default 1.0)
            line_id: Line ID from MM bet (required for patron matching)
        """
        with self.lock:
            self.mm_bets_to_match.append({
                'event_id': event_id,
                'market_id': market_id,
                'outcome_id': outcome_id,
                'odds': odds,
                'stake': stake,
                'line_id': line_id,
                'timestamp': time.time()
            })
    
    def matching_worker(self):
        """Worker thread that processes MM bets and matches them"""
        logging.info("🔄 Started bet matching worker thread")
        
        while self.is_running:
            try:
                with self.lock:
                    if self.mm_bets_to_match:
                        bet = self.mm_bets_to_match.pop(0)
                
                        # Simulate matching delay
                        time.sleep(self.match_delay)
                        
                        # Decide whether to match
                        if random.random() < self.match_rate:
                            self.session_stats['match_attempts'] += 1
                            self.match_bet(
                                bet['event_id'],
                                bet['market_id'],
                                bet['outcome_id'],
                                bet['odds'],
                                bet.get('stake', 1.0),
                                bet.get('line_id')
                            )
                        else:
                            logging.info(f"⏭️  Skipped matching (random skip: {1 - self.match_rate:.0%} chance)")
                
                time.sleep(0.1)
                
            except Exception as e:
                logging.error(f"❌ Error in matching worker: {e}")
                time.sleep(1)
    
    def balance_monitor(self):
        """Monitor patron balance periodically"""
        while self.is_running:
            try:
                self.get_patron_balance()
                time.sleep(30)  # Check every 30 seconds
            except Exception as e:
                logging.error(f"❌ Error in balance monitor: {e}")
                time.sleep(30)
    
    def session_reporter(self):
        """Report session statistics periodically"""
        while self.is_running:
            try:
                time.sleep(60)  # Report every 60 seconds
                
                elapsed = int(time.time() - self.session_stats['start_time'])
                logging.info("=" * 70)
                logging.info(f"📊 PATRON SESSION REPORT (Running for {elapsed}s)")
                logging.info("=" * 70)
                logging.info(f"  Events fetched: {self.session_stats['events_fetched']}")
                logging.info(f"  Bets received: {self.session_stats['match_attempts']}")
                logging.info(f"  Successful matches: {self.session_stats['successful_matches']}")
                logging.info(f"  Failed matches: {self.session_stats['failed_matches']}")
                logging.info(f"  Current balance: ${self.session_stats['balance']:.2f}")
                logging.info("=" * 70)
                
            except Exception as e:
                logging.error(f"❌ Error in session reporter: {e}")
    
    def start(self) -> bool:
        """Initialize and start the patron account matcher"""
        logging.info("🚀 Starting Patron Account Bet Matcher")
        
        # Load credentials
        if not self.load_patron_credentials():
            return False
        
        # Login
        if not self.patron_login():
            return False
        
        # Get balance
        self.get_patron_balance()
        
        # Seed events
        if not self.seed_patron_events():
            logging.warning("⚠️  Could not seed events, but continuing anyway")
        
        # Start worker threads
        self.balance_thread = threading.Thread(
            target=self.balance_monitor,
            daemon=True,
            name="PatronBalanceMonitor"
        )
        self.balance_thread.start()
        
        self.reporter_thread = threading.Thread(
            target=self.session_reporter,
            daemon=True,
            name="PatronSessionReporter"
        )
        self.reporter_thread.start()
        
        self.matcher_thread = threading.Thread(
            target=self.matching_worker,
            daemon=True,
            name="PatronBetMatcher"
        )
        self.matcher_thread.start()
        
        logging.info("✅ Patron account matcher initialized and running")
        return True
    
    def stop(self):
        """Stop the patron account matcher"""
        logging.info("🛑 Stopping patron account matcher")
        self.is_running = False


# Global patron matcher instance
patron_matcher = None


def signal_handler(sig, frame):
    """Handle Ctrl+C to gracefully stop"""
    global patron_matcher
    logging.info("\n🛑 Received stop signal")
    if patron_matcher:
        patron_matcher.stop()
    sys.exit(0)


def main():
    """Main entry point"""
    global patron_matcher
    
    parser = argparse.ArgumentParser(
        description='Patron account bet matcher for MM API testing'
    )
    parser.add_argument(
        '--env',
        type=str,
        default='sandbox',
        choices=['sandbox', 'staging'],
        help='Environment (default: sandbox)'
    )
    parser.add_argument(
        '--match-rate',
        type=float,
        default=0.7,
        help='Probability of matching a detected bet (0.0-1.0, default: 0.7)'
    )
    parser.add_argument(
        '--match-delay',
        type=float,
        default=0.5,
        help='Delay in seconds before matching a bet (default: 0.5)'
    )
    
    args = parser.parse_args()
    
    # Validate match rate
    if not 0.0 <= args.match_rate <= 1.0:
        logging.error("❌ Match rate must be between 0.0 and 1.0")
        sys.exit(1)
    
    # Set up signal handler
    signal.signal(signal.SIGINT, signal_handler)
    
    # Create patron matcher
    patron_matcher = PatronAccountMatcher(
        environment=args.env,
        match_rate=args.match_rate,
        match_delay=args.match_delay
    )
    
    logging.info("🎯 PATRON ACCOUNT BET MATCHER")
    logging.info("=" * 70)
    logging.info(f"Environment: {args.env.upper()}")
    logging.info(f"Match rate: {args.match_rate:.0%}")
    logging.info(f"Match delay: {args.match_delay}s")
    logging.info("=" * 70)
    
    # Start the matcher
    if not patron_matcher.start():
        logging.error("❌ Failed to start patron matcher")
        sys.exit(1)
    
    # Keep main thread alive
    logging.info("📡 Waiting for MM bets to match (Press Ctrl+C to stop)...")
    
    try:
        while patron_matcher.is_running:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        logging.info("✅ Patron account matcher stopped")


if __name__ == '__main__':
    main()
