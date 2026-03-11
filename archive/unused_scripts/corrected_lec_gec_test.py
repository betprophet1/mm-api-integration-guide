#!/usr/bin/env python3
"""
Corrected LEC to GEC Test - Proper understanding of exposure credit system
1. Place bet on Team A → Get MATCHED → Generates LEC for this market
2. Use LEC to bet on Team B (opposite side of SAME market) → Get MATCHED → Generates GEC
3. GEC can then be used for any market in any event
"""

import requests
import json
import uuid
import time
from datetime import datetime
from urllib.parse import urljoin

# Base URLs
BASE_URL = "https://api-ss-sandbox.betprophet.co"

# Use Account 1 for the test (has higher balance and existing LEC activity)
ACCOUNT = {
    "name": "Account 1 (lam.tran+usr002)",
    "email": "lam.tran+usr002@betprophet.co", 
    "password": "Kh0ngbiet1",
    "access_key": "3324857df2d66566dfe6b660faa2923f",
    "secret_key": "8c970658226e64c7346e753ed7377c48"
}

class CorrectLecGecTester:
    def __init__(self, account_info):
        self.account = account_info
        self.mm_token = None
        self.web_token = None
        self.balance = 0
        
    def mm_login(self):
        """Login using MM API"""
        login_url = f"{BASE_URL}/partner/auth/login"
        request_body = {
            'access_key': self.account['access_key'],
            'secret_key': self.account['secret_key'],
        }
        
        try:
            response = requests.post(login_url, data=json.dumps(request_body))
            if response.status_code == 200:
                mm_session = json.loads(response.content)['data']
                self.mm_token = mm_session['access_token']
                print(f"✅ MM Login successful!")
                return True
            else:
                print(f"❌ MM Login failed: {response.status_code}")
                return False
        except Exception as e:
            print(f"❌ MM Login error: {e}")
            return False
    
    def web_login(self):
        """Login using web authentication"""
        device_id = str(uuid.uuid4())
        headers = {
            '__source': 'web',
            'accept': 'application/json, text/plain, */*',
            'content-type': 'application/json',
            'origin': 'https://ss-sandbox.betprophet.co',
            'x-currency': 'cash'
        }
        
        payload = {
            "email": self.account['email'],
            "password": self.account['password'],
            "code": "123456",
            "device_id": device_id
        }
        
        try:
            response = requests.post(f"{BASE_URL}/api/v1/auth/login", headers=headers, json=payload, timeout=10)
            if response.status_code == 200:
                data = response.json()
                self.web_token = data.get('accessToken')
                print(f"✅ Web login successful!")
                return True
            else:
                print(f"❌ Web login failed: {response.status_code}")
                return False
        except Exception as e:
            print(f"❌ Web login error: {e}")
            return False
    
    def get_mm_auth_header(self):
        """Get MM API authorization header"""
        return {'Authorization': f'Bearer {self.mm_token}'}
    
    def get_web_auth_header(self):
        """Get web API authorization header"""
        return {
            'Authorization': f'Bearer {self.web_token}',
            'x-currency': 'cash',
            'accept': 'application/json'
        }
    
    def get_balance(self):
        """Get account balance using MM API"""
        if not self.mm_token:
            return 0
            
        balance_url = urljoin(BASE_URL, "partner/mm/get_balance")
        try:
            response = requests.get(balance_url, headers=self.get_mm_auth_header(), timeout=10)
            if response.status_code == 200:
                self.balance = response.json().get('data', {}).get('balance', 0)
                print(f"💰 Current Balance: ${self.balance}")
                return self.balance
            else:
                print(f"❌ Balance check failed: {response.status_code}")
                return 0
        except Exception as e:
            print(f"❌ Balance error: {e}")
            return 0
    
    def get_wallet_info(self):
        """Get wallet information including GEC"""
        if not self.web_token:
            return None
            
        try:
            response = requests.get(f"{BASE_URL}/api/v1/wallet", headers=self.get_web_auth_header(), timeout=10)
            if response.status_code == 200:
                wallet_data = response.json()['data']
                print(f"🏦 Wallet Status:")
                print(f"   Cash Balance: ${wallet_data.get('balance', 'N/A')}")
                print(f"   GEC (Global Exposure Credit): ${wallet_data.get('exposureCredit', 'N/A')}")
                print(f"   Total Balance: ${wallet_data.get('totalBalance', 'N/A')}")
                return wallet_data
            else:
                print(f"❌ Wallet check failed: {response.status_code}")
                return None
        except Exception as e:
            print(f"❌ Wallet error: {e}")
            return None
    
    def check_lec_for_market(self, event_id, market_id):
        """Check LEC (market-specific exposure credits) for a specific market"""
        if not self.web_token:
            return None
            
        params = {
            'eventIds': str(event_id),
            'marketIds': str(market_id)
        }
        
        try:
            response = requests.get(f"{BASE_URL}/api/v2/wallet/exposures", 
                                  headers=self.get_web_auth_header(), 
                                  params=params, timeout=10)
            
            if response.status_code == 200:
                exposure_data = response.json()
                lec_positions = exposure_data.get('data', [])
                
                print(f"🎯 LEC Check for Market {market_id} in Event {event_id}:")
                if lec_positions:
                    active_lec = [pos for pos in lec_positions if pos.get('balance', 0) > 0]
                    if active_lec:
                        print(f"   ✅ Active LEC Positions Found: {len(active_lec)}")
                        for pos in active_lec:
                            print(f"     Outcome {pos.get('outcomeId')}: ${pos.get('balance')} LEC")
                        return active_lec
                    else:
                        print(f"   ℹ️  No active LEC (all balances are $0)")
                        return []
                else:
                    print(f"   ℹ️  No LEC positions found")
                    return []
            else:
                print(f"❌ LEC check failed: {response.status_code}")
                return None
        except Exception as e:
            print(f"❌ LEC check error: {e}")
            return None
    
    def find_active_moneyline_market(self):
        """Find an active moneyline market for testing"""
        print("🔍 Finding active moneyline market...")
        
        tournaments_url = urljoin(BASE_URL, "partner/mm/get_tournaments")
        events_url = urljoin(BASE_URL, "partner/mm/get_sport_events")
        multiple_markets_url = urljoin(BASE_URL, "partner/mm/get_multiple_markets")
        
        try:
            # Get tournaments
            response = requests.get(tournaments_url, headers=self.get_mm_auth_header(), timeout=10)
            if response.status_code != 200:
                print(f"❌ Tournament fetch failed: {response.status_code}")
                return None
                
            tournaments = response.json().get('data', {}).get('tournaments', [])
            
            # Focus on MLB for moneyline markets
            mlb_tournaments = [t for t in tournaments if t.get('name') == 'MLB']
            tournaments_to_check = mlb_tournaments if mlb_tournaments else tournaments[:3]
            
            for tournament in tournaments_to_check:
                print(f"   Checking tournament: {tournament['name']} (ID: {tournament['id']})")
                
                # Get events
                events_response = requests.get(events_url, 
                                             params={'tournament_id': tournament['id']}, 
                                             headers=self.get_mm_auth_header(), timeout=10)
                
                if events_response.status_code == 200:
                    events = events_response.json().get('data', {}).get('sport_events', [])
                    if not events:
                        continue
                        
                    # Check first few events for moneyline markets
                    for event in events[:3]:
                        event_id = event['event_id']
                        event_name = event.get('name', f'Event {event_id}')
                        
                        print(f"      Checking event: {event_name} (ID: {event_id})")
                        
                        # Get markets
                        markets_response = requests.get(multiple_markets_url,
                                                      params={'event_ids': str(event_id)},
                                                      headers=self.get_mm_auth_header(), timeout=10)
                        
                        if markets_response.status_code == 200:
                            markets_by_event = markets_response.json().get('data', {})
                            event_markets = markets_by_event.get(str(event_id), [])
                            
                            # Find moneyline market (ID: 251)
                            for market in event_markets:
                                market_id = market.get('market_id') or market.get('id')
                                market_type = market.get('type', 'unknown')
                                
                                if market_type == 'moneyline' and market_id == 251:
                                    # Extract line IDs for both teams
                                    selections = market.get('selections', [])
                                    team_lines = {}
                                    
                                    for selection_group in selections:
                                        if isinstance(selection_group, list):
                                            for selection in selection_group:
                                                line_id = selection.get('line_id')
                                                name = selection.get('name', 'Unknown')
                                                if line_id:
                                                    team_lines[name] = line_id
                                    
                                    if len(team_lines) >= 2:
                                        team_names = list(team_lines.keys())
                                        print(f"✅ Found suitable moneyline market!")
                                        print(f"   Event: {event_name}")
                                        print(f"   Market ID: {market_id}")
                                        print(f"   Teams: {team_names[0]} vs {team_names[1]}")
                                        
                                        return {
                                            'event_id': event_id,
                                            'event_name': event_name,
                                            'market_id': market_id,
                                            'team_lines': team_lines,
                                            'team_names': team_names,
                                            'tournament_name': tournament['name']
                                        }
            
            print("❌ No suitable moneyline markets found")
            return None
            
        except Exception as e:
            print(f"❌ Error finding moneyline market: {e}")
            return None
    
    def place_bet(self, line_id, odds, stake, side_name="", use_lec=False):
        """Place a bet using MM API, optionally using LEC"""
        if not self.mm_token:
            return None
            
        wager_url = urljoin(BASE_URL, "partner/mm/place_wager")
        external_id = str(uuid.uuid1())
        
        body = {
            'external_id': external_id,
            'line_id': line_id,
            'odds': odds,
            'stake': stake
        }
        
        # Add LEC usage flag if specified
        if use_lec:
            body['use_lec'] = True
        
        try:
            response = requests.post(wager_url, json=body, headers=self.get_mm_auth_header(), timeout=10)
            if response.status_code == 200:
                result = response.json().get('data', {})
                wager = result.get('wager', {})
                wager_id = wager.get('id')
                
                credit_source = "LEC" if use_lec else "Cash"
                print(f"🎲 Bet placed successfully!")
                print(f"   Wager ID: {wager_id}")
                print(f"   Team: {side_name}")
                print(f"   Stake: ${stake} (using {credit_source})")
                print(f"   Odds: {odds}")
                print(f"   Line ID: {line_id}")
                
                return {
                    'wager_id': wager_id,
                    'external_id': external_id,
                    'line_id': line_id,
                    'odds': odds,
                    'stake': stake,
                    'used_lec': use_lec
                }
            else:
                print(f"❌ Bet placement failed: {response.status_code}")
                print(f"   Response: {response.text}")
                return None
        except Exception as e:
            print(f"❌ Bet placement error: {e}")
            return None
    
    def run_lec_gec_flow_test(self):
        """Run the complete LEC → GEC flow test"""
        print("="*80)
        print("🧪 CORRECT LEC → GEC FLOW TEST")
        print("="*80)
        print("Flow: Cash Bet → Matched → LEC → Opposite Bet using LEC → Matched → GEC")
        print()
        
        # Step 1: Find suitable market
        print("📍 STEP 1: Finding active moneyline market")
        market_data = self.find_active_moneyline_market()
        if not market_data:
            print("❌ Could not find suitable market")
            return False
        
        event_id = market_data['event_id']
        market_id = market_data['market_id']
        team_lines = market_data['team_lines']
        team_names = market_data['team_names']
        
        # Step 2: Record initial state
        print(f"\n📍 STEP 2: Recording initial state")
        initial_wallet = self.get_wallet_info()
        initial_gec = initial_wallet.get('exposureCredit', 0) if initial_wallet else 0
        
        initial_lec = self.check_lec_for_market(event_id, market_id)
        initial_active_lec = len([pos for pos in (initial_lec or []) if pos.get('balance', 0) > 0])
        
        print(f"📊 Initial State Summary:")
        print(f"   Initial GEC: ${initial_gec}")
        print(f"   Initial active LEC positions: {initial_active_lec}")
        
        # Step 3: Place first bet on Team A (should generate LEC when matched)
        print(f"\n📍 STEP 3: Placing bet on {team_names[0]} (Team A)")
        print(f"Expected: Bet gets matched → LEC generated for this market")
        
        team_a_line = team_lines[team_names[0]]
        first_bet = self.place_bet(
            line_id=team_a_line,
            odds=120,  # +120 odds
            stake=30.0,  # $30 bet
            side_name=team_names[0],
            use_lec=False  # Using cash
        )
        
        if not first_bet:
            print("❌ First bet failed")
            return False
        
        # Step 4: Wait and check for LEC generation
        print(f"\n📍 STEP 4: Waiting for bet to be matched and LEC to generate")
        print("⏳ Waiting 8 seconds for matching and LEC generation...")
        time.sleep(8)
        
        post_first_wallet = self.get_wallet_info()
        post_first_lec = self.check_lec_for_market(event_id, market_id)
        
        # Analyze LEC generation
        new_lec_positions = []
        if post_first_lec:
            new_lec_positions = [pos for pos in post_first_lec if pos.get('balance', 0) > 0]
        
        if new_lec_positions:
            print(f"✅ SUCCESS: LEC generated after first bet!")
            for pos in new_lec_positions:
                print(f"   Generated ${pos.get('balance')} LEC for Outcome {pos.get('outcomeId')}")
        else:
            print(f"⚠️  No new LEC detected. Possible reasons:")
            print(f"   - Bet not matched yet (need matching bet from another user)")
            print(f"   - LEC generation delay")
            print(f"   - Different LEC generation logic")
        
        # Step 5: Place second bet on Team B using LEC (should generate GEC when matched)
        print(f"\n📍 STEP 5: Placing opposite bet on {team_names[1]} (Team B) using LEC")
        print(f"Expected: LEC used → Bet gets matched → GEC generated")
        
        team_b_line = team_lines[team_names[1]]
        second_bet = self.place_bet(
            line_id=team_b_line,
            odds=-120,  # -120 odds (opposite side)
            stake=25.0,  # $25 bet
            side_name=team_names[1],
            use_lec=True  # Using LEC!
        )
        
        if not second_bet:
            print("❌ Second bet failed (may indicate no LEC available)")
            print("💡 This is expected if the first bet wasn't matched yet")
            return False
        
        # Step 6: Monitor for GEC generation
        print(f"\n📍 STEP 6: Monitoring for GEC generation")
        print("👀 Monitoring for 45 seconds...")
        
        best_gec_change = 0
        best_lec_info = None
        
        for i in range(15):  # 15 iterations, 3 seconds each = 45 seconds
            time.sleep(3)
            print(f"   🔍 Check {i+1}/15...")
            
            current_wallet = self.get_wallet_info()
            current_lec = self.check_lec_for_market(event_id, market_id)
            
            if current_wallet:
                current_gec = current_wallet.get('exposureCredit', 0)
                gec_change = current_gec - initial_gec
                
                if abs(gec_change) > abs(best_gec_change):
                    best_gec_change = gec_change
                
                if gec_change != 0:
                    print(f"   🚨 GEC CHANGE: ${initial_gec} → ${current_gec} (Change: ${gec_change})")
                
                # Track LEC changes
                if current_lec:
                    active_lec = [pos for pos in current_lec if pos.get('balance', 0) > 0]
                    if active_lec:
                        print(f"   📊 Active LEC: {len(active_lec)} positions")
                        best_lec_info = active_lec
                    else:
                        print(f"   ℹ️  No active LEC (may have been consumed)")
        
        # Step 7: Final analysis
        print(f"\n📍 STEP 7: Final Analysis")
        final_wallet = self.get_wallet_info()
        final_lec = self.check_lec_for_market(event_id, market_id)
        
        if final_wallet:
            final_gec = final_wallet.get('exposureCredit', 0)
            total_gec_change = final_gec - initial_gec
            
            print(f"🏁 FINAL RESULTS:")
            print(f"   Market: {market_data['event_name']} (Event {event_id}, Market {market_id})")
            print(f"   Bets Placed: ${first_bet['stake']} on {team_names[0]}, ${second_bet['stake']} on {team_names[1]}")
            print(f"   Initial GEC: ${initial_gec}")
            print(f"   Final GEC: ${final_gec}")
            print(f"   Total GEC Change: ${total_gec_change}")
            print(f"   Peak GEC Change: ${best_gec_change}")
            
            if total_gec_change > 0:
                print(f"✅ SUCCESS: GEC increased by ${total_gec_change}!")
                print(f"🎉 LEC → GEC conversion working correctly!")
            elif total_gec_change < 0:
                print(f"📊 INFO: GEC decreased by ${abs(total_gec_change)}")
                print(f"💡 This suggests exposure was consumed rather than generated")
            else:
                print(f"ℹ️  No net GEC change detected")
                if best_gec_change != 0:
                    print(f"   Note: Peak change of ${best_gec_change} was observed during monitoring")
                
                print(f"💭 Possible reasons for no change:")
                print(f"   - Bets not fully matched yet")
                print(f"   - LEC → GEC conversion requires different conditions")
                print(f"   - GEC was generated but consumed by other system processes")
        
        return True

def main():
    print("="*80)
    print("🧪 CORRECTED LEC → GEC CONVERSION TEST")
    print("="*80)
    print(f"Timestamp: {datetime.now()}")
    print("Understanding:")
    print("• LEC = Market-specific credit from matched bet on one side")
    print("• GEC = Global credit from matched bet using LEC on opposite side")
    print("• Flow: Cash Bet → Match → LEC → LEC Bet → Match → GEC")
    print()
    
    # Initialize tester
    tester = CorrectLecGecTester(ACCOUNT)
    
    # Authenticate
    print("🔐 Authenticating...")
    if not (tester.mm_login() and tester.web_login()):
        print("❌ Authentication failed")
        return
    
    # Check balance
    balance = tester.get_balance()
    if balance < 60:  # Need at least $60 for the test
        print(f"❌ Insufficient balance: ${balance} (need at least $60)")
        return
    
    # Run the test
    print(f"✅ Ready to test with ${balance} balance")
    success = tester.run_lec_gec_flow_test()
    
    print("\n" + "="*80)
    print("🏁 TEST COMPLETED")
    print("="*80)
    
    if success:
        print("✅ Test executed successfully!")
        print("📊 Review the results above to understand LEC → GEC conversion")
        print("💡 Remember: Both bets need to be MATCHED for the full flow to work")
    else:
        print("❌ Test encountered issues")
        print("💡 This might indicate missing market liquidity or different system behavior")
    
    print(f"\nCompleted at: {datetime.now()}")

if __name__ == "__main__":
    main()
