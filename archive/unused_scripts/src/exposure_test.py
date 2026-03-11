import time
import requests
import json
import uuid
import random
from urllib.parse import urljoin
from src import config
from src.log import logging


class ExposureTest:
    """Test class for exposure functionality including edge cases"""
    
    def __init__(self, mm_interactions):
        self.mm = mm_interactions
        self.base_url = mm_interactions.base_url
        self.exposure_balances = {}
        self.last_sync_time = 0
        
    def get_exposure_balance(self):
        """Retrieve current exposure balance"""
        try:
            exposure_balance_url = urljoin(self.base_url, config.URL.get('exposure_balance', 'partner/exposure/get_balance'))
            response = requests.get(exposure_balance_url, headers=self.mm._MMInteractions__get_auth_header())
            
            if response.status_code == 200:
                balance_data = response.json().get('data', {})
                logging.info(f"💰 Exposure balance retrieved: {balance_data}")
                return balance_data
            else:
                logging.error(f"❌ Failed to retrieve exposure balance: {response.status_code}")
                return None
        except Exception as e:
            logging.error(f"❌ Exception getting exposure balance: {str(e)}")
            return None

    def get_exposure_credits(self, event_id=None, market_id=None):
        """Retrieve LEC/GEC credits"""
        try:
            exposure_credits_url = urljoin(self.base_url, config.URL.get('exposure_credits', 'partner/exposure/get_credits'))
            params = {}
            if event_id:
                params['event_id'] = event_id
            if market_id:
                params['market_id'] = market_id
                
            response = requests.get(exposure_credits_url, params=params, headers=self.mm._MMInteractions__get_auth_header())
            
            if response.status_code == 200:
                credits_data = response.json().get('data', {})
                logging.info(f"🎯 Exposure credits retrieved: {json.dumps(credits_data, indent=2)}")
                return credits_data
            else:
                logging.error(f"❌ Failed to retrieve exposure credits: {response.status_code}")
                return None
        except Exception as e:
            logging.error(f"❌ Exception getting exposure credits: {str(e)}")
            return None

    def sync_exposure_updates(self):
        """Sync exposure updates - simulates 10s wallet sync"""
        try:
            exposure_sync_url = urljoin(self.base_url, config.URL.get('exposure_sync', 'partner/exposure/sync'))
            response = requests.post(exposure_sync_url, headers=self.mm._MMInteractions__get_auth_header())
            
            if response.status_code == 200:
                sync_data = response.json().get('data', {})
                self.last_sync_time = time.time()
                logging.info(f"🔄 Exposure sync successful at {time.strftime('%H:%M:%S')}: {json.dumps(sync_data, indent=2)}")
                return sync_data
            else:
                logging.error(f"❌ Failed to sync exposure updates: {response.status_code}")
                return None
        except Exception as e:
            logging.error(f"❌ Exception syncing exposure: {str(e)}")
            return None

    def test_negative_lec_edge_case(self):
        """Test the negative LEC edge case scenario"""
        logging.info("🧪 Starting Negative LEC Edge Case Test")
        
        # Step 1: Place initial bet to trigger LEC creation
        logging.info("📍 Step 1: Place $30 bet on outcome1 +100 matched")
        bet1_result = self.place_test_bet(30, "+100", "outcome1")
        if not bet1_result:
            logging.error("❌ Failed to place initial bet")
            return False
            
        time.sleep(1)
        
        # Step 2: Get LEC for outcome2 (should be $60)  
        logging.info("📍 Step 2: Check LEC for outcome2 (expecting $60)")
        credits = self.get_exposure_credits()
        if not credits:
            logging.error("❌ No credits received")
            return False
            
        time.sleep(1)
        
        # Step 3: Place $20 bet using LEC on outcome2
        logging.info("📍 Step 3: Place $20 LEC bet on outcome2 -200 matched")
        bet2_result = self.place_test_bet(20, "-200", "outcome2", use_lec=True)
        if not bet2_result:
            logging.error("❌ Failed to place LEC bet")
            return False
            
        time.sleep(1)
        
        # Step 4: Exposure service recalculates (LEC=$50, GEC=$10)
        logging.info("📍 Step 4: Exposure service recalculates (LEC=$50, GEC=$10)")
        logging.info("⏳ Waiting for exposure recalculation...")
        time.sleep(2)
        
        # Step 5: Place $40 LEC bet BEFORE wallet syncs
        logging.info("📍 Step 5: Place $40 LEC bet on outcome2 BEFORE sync")
        bet3_result = self.place_test_bet(40, "-200", "outcome2", use_lec=True)
        
        # Step 6: Force wallet sync and check for negative LEC
        logging.info("📍 Step 6: Force wallet sync and check for negative LEC")
        sync_result = self.sync_exposure_updates()
        
        if sync_result:
            logging.info("🔍 Analyzing sync result for negative LEC...")
            # Check if we have negative LEC situation
            self.analyze_negative_lec_situation(sync_result)
        
        return True

    def test_double_spending_gec(self):
        """Test double spending of GEC across markets"""
        logging.info("🧪 Starting Double Spending GEC Test")
        
        # Create scenario where user has GEC and tries to spend it on multiple markets
        logging.info("📍 Creating GEC through matched bets on Market A")
        
        # Place bets to generate GEC
        bet1 = self.place_test_bet(100, "+120", "team1", market="A")
        bet2 = self.place_test_bet(80, "-150", "team2", market="A")
        
        time.sleep(2)
        
        # Check GEC balance
        credits = self.get_exposure_credits()
        if credits and 'gec' in credits:
            gec_amount = credits['gec']
            logging.info(f"💰 Available GEC: ${gec_amount}")
            
            # Try to spend full GEC on Market B
            logging.info("📍 Attempting to spend full GEC on Market B")
            bet3 = self.place_test_bet(gec_amount, "+110", "team1", market="B", use_gec=True)
            
            # Before sync, try to spend GEC again on Market C
            logging.info("📍 Before sync: Attempting to spend GEC again on Market C")
            bet4 = self.place_test_bet(gec_amount/2, "-120", "team2", market="C", use_gec=True)
            
            # Now sync and check for overspend
            sync_result = self.sync_exposure_updates()
            self.analyze_gec_overspend(sync_result)
            
        return True

    def test_phantom_balance_after_settlement(self):
        """Test phantom balance after settlement"""
        logging.info("🧪 Starting Phantom Balance After Settlement Test")
        
        # Create LEC scenario
        bet1 = self.place_test_bet(200, "+110", "team1")
        time.sleep(1)
        
        credits = self.get_exposure_credits()
        if credits and 'lec' in credits:
            # Use part of LEC
            lec_amount = credits['lec']
            used_amount = lec_amount * 0.75  # Use 75% of LEC
            
            logging.info(f"📍 Using ${used_amount} out of ${lec_amount} LEC")
            bet2 = self.place_test_bet(used_amount, "-120", "team2", use_lec=True)
            
            # Simulate game settlement (team1 loses)
            logging.info("📍 Simulating game settlement - Team1 loses")
            
            # Before wallet syncs settlement, try to use remaining LEC
            remaining_lec = lec_amount - used_amount
            logging.info(f"📍 Attempting to use remaining ${remaining_lec} LEC before settlement sync")
            bet3 = self.place_test_bet(remaining_lec, "-110", "team2", use_lec=True)
            
            # Now sync settlement
            sync_result = self.sync_exposure_updates()
            self.analyze_phantom_balance(sync_result)
            
        return True

    def place_test_bet(self, amount, odds, outcome, market="default", use_lec=False, use_gec=False):
        """Place a test bet with specified parameters"""
        try:
            play_url = urljoin(self.base_url, config.URL['mm_place_wager'])
            external_id = str(uuid.uuid1())
            
            # Find a suitable line_id from available events
            line_id = self.get_test_line_id(market, outcome)
            if not line_id:
                logging.error(f"❌ No suitable line_id found for {market}/{outcome}")
                return False
            
            body = {
                'external_id': external_id,
                'line_id': line_id,
                'odds': self.parse_odds(odds),
                'stake': amount
            }
            
            if use_lec:
                body['use_lec'] = True
            if use_gec:
                body['use_gec'] = True
            
            logging.info(f"🎲 Placing ${amount} bet on {outcome} at {odds} odds")
            if use_lec:
                logging.info("   💳 Using LEC")
            if use_gec:
                logging.info("   💳 Using GEC")
                
            response = requests.post(play_url, json=body, headers=self.mm._MMInteractions__get_auth_header())
            
            if response.status_code == 200:
                result = response.json().get('data', {})
                wager_id = result.get('wager', {}).get('id')
                logging.info(f"✅ Bet placed successfully: {wager_id}")
                
                # Store wager for tracking
                self.mm.wagers[external_id] = wager_id
                return True
            else:
                logging.error(f"❌ Failed to place bet: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            logging.error(f"❌ Exception placing bet: {str(e)}")
            return False

    def get_test_line_id(self, market, outcome):
        """Get a line_id for testing purposes"""
        # Use the first available line_id from sport_events
        for event_id, event in self.mm.sport_events.items():
            for market_data in event.get('markets', []):
                if market_data.get('type') == 'moneyline':
                    selections = market_data.get('selections', [])
                    if selections and len(selections) > 0:
                        return selections[0][0].get('line_id')
        return None

    def parse_odds(self, odds_str):
        """Parse odds string to numeric format"""
        if odds_str.startswith('+'):
            return int(odds_str[1:])
        elif odds_str.startswith('-'):
            return int(odds_str)
        else:
            return int(odds_str)

    def analyze_negative_lec_situation(self, sync_data):
        """Analyze sync data for negative LEC situation"""
        logging.info("🔍 Analyzing for negative LEC situation...")
        
        if 'lec' in sync_data:
            lec_data = sync_data['lec']
            for lec_entry in lec_data:
                available = lec_entry.get('amount', 0)
                spent = lec_entry.get('spent_amount', 0)
                remaining = available - spent
                
                if remaining < 0:
                    logging.error(f"🚨 NEGATIVE LEC DETECTED!")
                    logging.error(f"   Available: ${available}")
                    logging.error(f"   Spent: ${spent}")
                    logging.error(f"   Deficit: ${remaining}")
                    
                    # This is the bug we're testing for!
                    return True
                    
        logging.info("✅ No negative LEC detected")
        return False

    def analyze_gec_overspend(self, sync_data):
        """Analyze sync data for GEC overspending"""
        logging.info("🔍 Analyzing for GEC overspending...")
        
        if 'gec' in sync_data:
            gec_data = sync_data['gec']
            total_spent = sum(entry.get('spent_amount', 0) for entry in gec_data)
            total_available = sum(entry.get('amount', 0) for entry in gec_data)
            
            if total_spent > total_available:
                logging.error(f"🚨 GEC OVERSPEND DETECTED!")
                logging.error(f"   Total Available: ${total_available}")
                logging.error(f"   Total Spent: ${total_spent}")
                logging.error(f"   Overspend: ${total_spent - total_available}")
                return True
                
        logging.info("✅ No GEC overspend detected")
        return False

    def analyze_phantom_balance(self, sync_data):
        """Analyze for phantom balance after settlement"""
        logging.info("🔍 Analyzing for phantom balance after settlement...")
        
        if 'lec' in sync_data:
            for lec_entry in sync_data['lec']:
                status = lec_entry.get('status', 'open')
                spent = lec_entry.get('spent_amount', 0)
                
                if status == 'closed' and spent > 0:
                    logging.error(f"🚨 PHANTOM BALANCE DETECTED!")
                    logging.error(f"   Status: {status}")
                    logging.error(f"   Amount used after closure: ${spent}")
                    return True
                    
        logging.info("✅ No phantom balance detected")
        return False

    def run_comprehensive_test_suite(self):
        """Run all exposure edge case tests"""
        logging.info("🚀 Starting Comprehensive Exposure Test Suite")
        logging.info("=" * 60)
        
        test_results = {}
        
        # Test 1: Negative LEC Edge Case
        try:
            logging.info("🧪 Test 1: Negative LEC Edge Case")
            test_results['negative_lec'] = self.test_negative_lec_edge_case()
            time.sleep(5)
        except Exception as e:
            logging.error(f"❌ Test 1 failed: {str(e)}")
            test_results['negative_lec'] = False
        
        # Test 2: Double Spending GEC
        try:
            logging.info("🧪 Test 2: Double Spending GEC")
            test_results['double_spending_gec'] = self.test_double_spending_gec()
            time.sleep(5)
        except Exception as e:
            logging.error(f"❌ Test 2 failed: {str(e)}")
            test_results['double_spending_gec'] = False
        
        # Test 3: Phantom Balance After Settlement
        try:
            logging.info("🧪 Test 3: Phantom Balance After Settlement")
            test_results['phantom_balance'] = self.test_phantom_balance_after_settlement()
            time.sleep(5)
        except Exception as e:
            logging.error(f"❌ Test 3 failed: {str(e)}")
            test_results['phantom_balance'] = False
        
        # Report results
        logging.info("=" * 60)
        logging.info("🏁 TEST SUITE RESULTS:")
        for test_name, result in test_results.items():
            status = "✅ PASS" if result else "❌ FAIL"
            logging.info(f"   {test_name}: {status}")
        
        return test_results

    def simulate_10s_sync_delay(self):
        """Simulate the 10-second sync delay mentioned in docs"""
        current_time = time.time()
        time_since_last_sync = current_time - self.last_sync_time
        
        if time_since_last_sync < 10:
            wait_time = 10 - time_since_last_sync
            logging.info(f"⏳ Simulating sync delay: waiting {wait_time:.1f}s")
            time.sleep(wait_time)
            
        return self.sync_exposure_updates()
