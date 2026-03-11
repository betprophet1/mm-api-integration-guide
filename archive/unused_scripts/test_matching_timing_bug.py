#!/usr/bin/env python3
"""
Bug Reproduction Test: Best-Execution Matching With Simultaneous Wagers

Scenario under test:
1. MM account 1 places an open wager at -117 odds (which creates +117 on the opposite side).
2. Wait 5 seconds.
3. MM account 1 places -114 odds on Team A AND Patron places +114 odds on Team B at nearly the same time
   (MM -114 a few milliseconds earlier).
4. Observe matching results.

Expected behavior (Best Execution):
- Patron's +114 wager should be filled against the existing +117 liquidity first (price improvement to +117).
- Only any remainder (if patron stake > available at +117) should then match against the new MM -114 order.

Bug to reproduce:
- When -114 and +114 are placed at the same time, they match directly with each other at +114/-114
  without consuming the existing +117, thus failing to trigger best-execution price improvement.

Usage:
    python test_matching_timing_bug.py --env sandbox --event-id 20022672
    python test_matching_timing_bug.py --env sandbox --event-id 20022672 --market-type moneyline
"""

import argparse
import signal
import sys
import time
import threading
import os
import json
import requests
import uuid
from urllib.parse import urljoin
from datetime import datetime

sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.mm_calls import MMInteractions
from src.log import logging
from src import config

# Global stop flag
should_stop = False


def signal_handler(sig, frame):
    """Handle Ctrl+C gracefully"""
    global should_stop
    logging.info("\n🛑 Stopping test...")
    should_stop = True
    sys.exit(0)


class TestMMAccount(MMInteractions):
    """Extended MM class for testing"""
    
    def __init__(self, account_name="MM Account"):
        super().__init__()
        self.account_name = account_name
    
    def place_single_wager(self, line_id, odds, stake, description=""):
        """Place a single wager with detailed logging"""
        play_url = urljoin(self.base_url, config.URL['mm_place_wager'])
        external_id = str(uuid.uuid1())
        
        body = {
            'external_id': external_id,
            'line_id': line_id,
            'odds': odds,
            'stake': stake
        }
        
        # Record timestamp before request
        timestamp_before = datetime.now().isoformat()
        
        logging.info(f"📤 {self.account_name} - Placing wager: {description}")
        logging.info(f"   Line ID: {line_id}, Odds: {odds}, Stake: ${stake}")
        logging.info(f"   Timestamp: {timestamp_before}")
        
        try:
            response = requests.post(play_url, json=body, headers=self._MMInteractions__get_auth_header())
            timestamp_after = datetime.now().isoformat()
            
            if response.status_code == 200:
                result = response.json().get('data', {})
                wager = result.get('wager', {})
                wager_id = wager.get('id')
                wager_status = wager.get('status')
                
                logging.info(f"✅ {self.account_name} - Wager placed successfully")
                logging.info(f"   Wager ID: {wager_id}")
                logging.info(f"   Status: {wager_status}")
                logging.info(f"   Response time: {timestamp_after}")
                logging.info(f"   Full response: {json.dumps(result, indent=2)}")
                
                return {
                    'success': True,
                    'external_id': external_id,
                    'wager_id': wager_id,
                    'status': wager_status,
                    'timestamp_before': timestamp_before,
                    'timestamp_after': timestamp_after,
                    'response': result
                }
            else:
                logging.error(f"❌ {self.account_name} - Failed to place wager")
                logging.error(f"   Status: {response.status_code}")
                logging.error(f"   Response: {response.text}")
                return {
                    'success': False,
                    'error': response.text,
                    'status_code': response.status_code
                }
        except Exception as e:
            logging.error(f"❌ {self.account_name} - Exception: {e}")
            return {
                'success': False,
                'error': str(e)
            }


class PatronAccount:
    """Patron account for placing matching bets"""
    
    def __init__(self, environment='sandbox'):
        self.environment = environment
        self.base_url = config.ENVIRONMENT_URLS.get(environment, config.ENVIRONMENT_URLS['sandbox'])
        self.jwt_token = None
        self.account_name = "Patron Account"
    
    def login(self):
        """Login to patron account"""
        try:
            # Load patron credentials based on environment
            config_filename = f'user_info_patron_{self.environment}.json'
            patron_config_path = os.path.join(
                os.path.dirname(os.path.abspath(__file__)), 
                'src', 
                config_filename
            )
            
            if not os.path.exists(patron_config_path):
                logging.error(f"❌ Patron config not found at {patron_config_path}")
                return False
            
            with open(patron_config_path) as f:
                patron_creds = json.load(f)
            
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
                'email': patron_creds['email'],
                'password': patron_creds['password'],
                'code': '123456',
                'device_id': device_id
            }
            
            response = requests.post(login_url, headers=headers, json=request_body)
            
            if response.status_code == 200:
                response_data = response.json()
                self.jwt_token = response_data.get('accessToken')
                logging.info(f"✅ {self.account_name} - Login successful")
                return True
            else:
                logging.error(f"❌ {self.account_name} - Login failed: {response.text}")
                return False
                
        except Exception as e:
            logging.error(f"❌ {self.account_name} - Login exception: {e}")
            return False
    
    def __get_auth_header(self):
        """Get patron auth header"""
        return {
            'Authorization': f'Bearer {self.jwt_token}',
            'Content-Type': 'application/json',
            'x-currency': 'cash',
            'accept': 'application/json',
            '__source': 'web'
        }
    
    def place_bet(self, line_id, odds, stake, description=""):
        """Place a patron bet using web API and return raw response for fill analysis"""
        bet_url = urljoin(self.base_url, 'trade/private/api/v2/wagers')
        
        body = {
            'lineId': line_id,
            'odds': odds,
            'stake': stake
        }
        
        timestamp_before = datetime.now().isoformat()
        
        logging.info(f"📤 {self.account_name} - Placing bet: {description}")
        logging.info(f"   Line ID: {line_id}, Odds: {odds}, Stake: ${stake}")
        logging.info(f"   Timestamp: {timestamp_before}")
        
        try:
            response = requests.post(bet_url, json=body, headers=self.__get_auth_header())
            timestamp_after = datetime.now().isoformat()
            
            if response.status_code == 200:
                resp_json = response.json()
                result = resp_json.get('data', resp_json)
                
                logging.info(f"✅ {self.account_name} - Bet placed successfully")
                logging.info(f"   Response time: {timestamp_after}")
                # Keep logs concise; full object kept in return
                try:
                    preview = json.dumps(result)[:400]
                except Exception:
                    preview = str(result)[:400]
                logging.info(f"   Response preview: {preview}...")
                
                return {
                    'success': True,
                    'timestamp_before': timestamp_before,
                    'timestamp_after': timestamp_after,
                    'response': result
                }
            else:
                logging.error(f"❌ {self.account_name} - Failed to place bet")
                logging.error(f"   Status: {response.status_code}")
                logging.error(f"   Response: {response.text}")
                return {
                    'success': False,
                    'error': response.text,
                    'status_code': response.status_code
                }
        except Exception as e:
            logging.error(f"❌ {self.account_name} - Exception: {e}")
            return {
                'success': False,
                'error': str(e)
            }


def fetch_event_markets(event_id, market_type='moneyline', environment='sandbox'):
    """Fetch event markets from public API"""
    try:
        base_url = config.ENVIRONMENT_URLS.get(environment, config.ENVIRONMENT_URLS['sandbox'])
        url = f'{base_url}/partner/v2/public/get_multiple_markets?market_types={market_type}&event_ids={event_id}'
        
        headers = {
            '__source': 'web',
            'accept': 'application/json, text/plain, */*',
            'authorization': 'testtoken',
            'x-currency': 'cash'
        }
        
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            data = response.json().get('data', [])
            # Response is a list of events
            if isinstance(data, list) and len(data) > 0:
                # Find the matching event
                for event in data:
                    if event.get('eventId') == event_id:
                        return event
                # If not found, return first event
                return data[0]
            return None
        else:
            logging.error(f"Failed to fetch markets: {response.status_code}")
            return None
    except Exception as e:
        logging.error(f"Exception fetching markets: {e}")
        import traceback
        traceback.print_exc()
        return None


def run_test(event_id, market_type, mm1, patron, environment='sandbox'):
    """
    Run the bug reproduction test
    
    Steps:
    1. MM account 1 places open wager at -117 odds on Team A (creates +117 on Team B)
    2. Wait 5 seconds
    3. MM account 1 places -114 odds on Team A AND Patron places +114 odds on Team B simultaneously
    4. Check matching results
    
    Expected (Best Execution):
    - Patron's +114 order should match against existing +117 liquidity first (price improvement)
    - Only remainder (if any) matches against the new -114 order
    
    Bug:
    - When -114 and +114 arrive simultaneously, they match directly at +114/-114
    - This bypasses the existing +117 liquidity, violating best execution
    """
    logging.info("\n" + "="*80)
    logging.info("🧪 BUG REPRODUCTION TEST - BEST EXECUTION WITH SIMULTANEOUS ORDERS")
    logging.info("="*80)
    
    # Fetch event markets
    logging.info(f"\n📊 Fetching event {event_id} markets...")
    event_data = fetch_event_markets(event_id, market_type, environment)
    
    if not event_data or 'markets' not in event_data:
        logging.error("❌ Failed to fetch event data or no markets available")
        return False
    
    markets = event_data.get('markets', [])
    
    # Find the target market
    target_market = None
    for market in markets:
        if market.get('type') == market_type:
            target_market = market
            break
    
    if not target_market:
        logging.error(f"❌ No {market_type} market found")
        return False
    
    logging.info(f"✅ Found {market_type} market")
    
    selections = target_market.get('selections', [])
    if len(selections) < 2:
        logging.error("❌ Not enough selections in market")
        return False
    
    # Get Team A and Team B selections
    team_a_selection = selections[0][0] if isinstance(selections[0], list) else selections[0]
    team_b_selection = selections[1][0] if isinstance(selections[1], list) else selections[1]
    
    team_a_name = team_a_selection.get('name', 'Team A')
    team_b_name = team_b_selection.get('name', 'Team B')
    # API uses lineID (camelCase) not line_id
    team_a_line_id = team_a_selection.get('lineID') or team_a_selection.get('line_id')
    team_b_line_id = team_b_selection.get('lineID') or team_b_selection.get('line_id')
    
    if not team_a_line_id or not team_b_line_id:
        logging.error("❌ Missing line IDs for selections")
        return False
    
    logging.info(f"\n🏟️  Teams:")
    logging.info(f"   Team A: {team_a_name} (Line ID: {team_a_line_id})")
    logging.info(f"   Team B: {team_b_name} (Line ID: {team_b_line_id})")
    
    # STEP 1: MM Account 1 places -117 odds on Team A
    logging.info("\n" + "="*80)
    logging.info("STEP 1: MM Account 1 places wager at -117 odds on Team A")
    logging.info("="*80)
    
    result1 = mm1.place_single_wager(
        line_id=team_a_line_id,
        odds=-117,
        stake=10.0,
        description=f"{team_a_name} at -117 odds (creates +117 on {team_b_name})"
    )
    
    if not result1.get('success'):
        logging.error("❌ Step 1 failed - Cannot continue test")
        return False
    
    wager1_id = result1.get('wager_id')
    wager1_status = result1.get('status')
    
    logging.info(f"\n✅ Step 1 complete - Wager placed (Status: {wager1_status})")
    logging.info(f"   This creates an open +117 odds opportunity on {team_b_name}")
    logging.info(f"   Any Patron bet on {team_b_name} should get price improvement to +117 (best execution)")
    
    # STEP 2: Wait 5 seconds
    logging.info("\n" + "="*80)
    logging.info("STEP 2: Wait 5 seconds")
    logging.info("="*80)
    
    for i in range(5, 0, -1):
        logging.info(f"⏳ Waiting... {i} seconds remaining")
        time.sleep(1)
    
    # STEP 3: Place simultaneous bets
    logging.info("\n" + "="*80)
    logging.info("STEP 3: Place simultaneous bets")
    logging.info("  - MM Account 1: -114 odds on Team A (a few milliseconds first)")
    logging.info("  - Patron Account: +114 odds on Team B (immediately after)")
    logging.info("")
    logging.info("🎯 Expected: Patron +114 should fill against existing +117 first (best execution)")
    logging.info("🐛 Bug: If +114 and -114 match directly, best execution is bypassed")
    logging.info("="*80)
    
    # Threading to ensure near-simultaneous execution
    mm_result = {}
    patron_result = {}
    
    def place_mm_bet():
        mm_result.update(mm1.place_single_wager(
            line_id=team_a_line_id,
            odds=-114,
            stake=10.0,
            description=f"{team_a_name} at -114 odds"
        ))
    
    def place_patron_bet():
        # Small delay to ensure MM is a few milliseconds ahead
        time.sleep(0.005)  # 5 milliseconds
        patron_result.update(patron.place_bet(
            line_id=team_b_line_id,
            odds=114,  # Positive odds for patron
            stake=10.0,
            description=f"{team_b_name} at +114 odds"
        ))
    
    # Start both threads
    mm_thread = threading.Thread(target=place_mm_bet)
    patron_thread = threading.Thread(target=place_patron_bet)
    
    logging.info("\n🚀 Starting simultaneous bet placement...")
    mm_thread.start()
    patron_thread.start()
    
    # Wait for both to complete
    mm_thread.join()
    patron_thread.join()
    
    logging.info("\n✅ Step 3 complete - Both bets placed")
    
    # STEP 4: Analyze results
    logging.info("\n" + "="*80)
    logging.info("STEP 4: Analyzing matching results")
    logging.info("="*80)
    
    mm_success = mm_result.get('success')
    patron_success = patron_result.get('success')
    
    logging.info(f"\n📊 Bet Placement Results:")
    logging.info(f"   MM Account 1 (-114): {'✅ Success' if mm_success else '❌ Failed'}")
    logging.info(f"   Patron Account (+114): {'✅ Success' if patron_success else '❌ Failed'}")
    
    def _extract_patron_fill_odds(obj):
        """Best-effort extraction of actual filled odds from patron response"""
        try:
            # Common locations/keys in bet APIs
            # Look for any key containing 'odds' that is numeric and not nested 'request' odds
            found = []
            def walk(x):
                if isinstance(x, dict):
                    for k, v in x.items():
                        lk = str(k).lower()
                        if isinstance(v, (int, float)) and 'odds' in lk:
                            found.append(float(v))
                        elif isinstance(v, (dict, list)):
                            walk(v)
                elif isinstance(x, list):
                    for it in x:
                        walk(it)
            walk(obj)
            # Heuristic: prefer values > 100 (American positive odds)
            positives = [v for v in found if v >= 100]
            if positives:
                # Return the smallest positive as a conservative fill (e.g., 114 vs 117)
                return min(positives)
            return positives[0] if positives else (found[0] if found else None)
        except Exception:
            return None
    
    if mm_success and patron_success:
        mm_wager_status = mm_result.get('status')
        patron_fill_odds = _extract_patron_fill_odds(patron_result.get('response', {}))
        
        logging.info(f"\n🔍 Matching Analysis:")
        logging.info(f"   MM -114 wager status: {mm_wager_status}")
        logging.info(f"   Patron reported filled odds (best-effort): {patron_fill_odds}")
        
        # Wait a moment for matching to occur
        logging.info("\n⏳ Waiting 2 seconds for matching engine to process...")
        time.sleep(2)
        
        # Final status snapshot
        logging.info("\n📋 Final Status Snapshot:")
        logging.info(f"   Wager 1 (MM @ -117): {wager1_id} - initial status: {wager1_status}")
        logging.info(f"   Wager 2 (MM @ -114): {mm_result.get('wager_id')} - status: {mm_wager_status}")
        logging.info(f"   Wager 3 (Patron @ +114): see filled odds above")
        
        # Determine outcome against expected best execution
        bug_reproduced = False
        best_exec_ok = False
        
        if patron_fill_odds is not None:
            # Best execution means patron gets improved to +117 when +117 liquidity exists
            best_exec_ok = patron_fill_odds >= 117 - 1e-6  # allow float tolerance
            # Bug pattern: patron shows +114 fill and MM -114 shows matched
            bug_reproduced = (patron_fill_odds <= 114 + 1e-6) and (str(mm_wager_status).lower() == 'matched')
        
        logging.info("\n" + "="*80)
        logging.info("📊 MATCHING BEHAVIOR VERDICT")
        logging.info("="*80)
        if best_exec_ok:
            logging.info("✅ Best execution observed: Patron received price improvement to ≥ +117 before any -114 crossing")
        elif bug_reproduced:
            logging.info("🐛 BUG REPRODUCED: Patron filled at +114 and MM -114 matched, bypassing existing +117 liquidity")
        else:
            logging.info("ℹ️ Unable to conclusively determine from responses. Please inspect logs and backend match records.")
        
        logging.info("\n💡 To further verify, check:")
        logging.info("   1. Patron ticket fill breakdown (did any fill occur at +117?)")
        logging.info("   2. MM -114 wager counterparty and fill time")
        logging.info("   3. Whether Wager 1 (-117) remained open immediately after these fills")
        
    else:
        logging.error("\n❌ Test incomplete - One or both bets failed to place")
    
    logging.info("\n" + "="*80)
    logging.info("✅ TEST COMPLETE")
    logging.info("="*80)
    
    return True


def main():
    global should_stop
    
    parser = argparse.ArgumentParser(
        description='Bug Reproduction Test - Matching Timing Behavior'
    )
    parser.add_argument(
        '--env',
        type=str,
        default='sandbox',
        choices=['sandbox', 'staging'],
        help='Environment (default: sandbox)'
    )
    parser.add_argument(
        '--event-id',
        type=int,
        required=True,
        help='Event ID to test with'
    )
    parser.add_argument(
        '--market-type',
        type=str,
        default='moneyline',
        choices=['moneyline', 'spread', 'total'],
        help='Market type to test (default: moneyline)'
    )
    
    args = parser.parse_args()
    
    # Set up signal handler
    signal.signal(signal.SIGINT, signal_handler)
    
    logging.info("🚀 BUG REPRODUCTION TEST")
    logging.info("="*80)
    logging.info(f"Environment: {args.env}")
    logging.info(f"Event ID: {args.event_id}")
    logging.info(f"Market Type: {args.market_type}")
    logging.info("="*80)
    
    try:
        # Set up MM Account 1
        logging.info("\n🔐 Setting up MM Account 1...")
        credentials1 = config.get_account_credentials(1, args.env)
        mm1 = TestMMAccount("MM Account 1")
        mm1.base_url = config.ENVIRONMENT_URLS.get(args.env, config.ENVIRONMENT_URLS['sandbox'])
        mm1.mm_keys = {
            'access_key': credentials1['access_key'],
            'secret_key': credentials1['secret_key']
        }
        mm1.mm_login()
        mm1.get_balance()
        mm1.seeding()
        logging.info(f"✅ MM Account 1 ready (Balance: ${mm1.balance:.2f})")
        
        # Set up Patron Account
        logging.info("\n🔐 Setting up Patron Account...")
        patron = PatronAccount(args.env)
        if not patron.login():
            logging.error("❌ Failed to login patron account")
            return
        logging.info("✅ Patron Account ready")
        
        # Run the test
        run_test(args.event_id, args.market_type, mm1, patron, args.env)
        
    except Exception as e:
        logging.error(f"❌ Test failed with exception: {e}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    main()
