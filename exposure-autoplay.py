#!/usr/bin/env python3
"""
Exposure Autoplay Script
========================
Automated parallel exposure testing that coordinates two accounts to:
1. Login with both MM and Web authentication
2. Seed tournaments/events/markets
3. Place opposing bets to match each other
4. Monitor exposure generation (LEC/GEC) in real-time
"""

import json
import time
import uuid
import requests
import threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin

# Configuration
BASE_URL = "https://api-ss-sandbox.betprophet.co"
TARGET_EVENTS = 1  # Number of events to generate GEC from
TARGET_EVENT_ID = 60073014  # Specific event to target
MAX_BET_ROUNDS = 20  # Maximum betting rounds per event

# Account credentials
ACCOUNTS = [
    {
        "id": 1,
        "name": "Account 1 (lam.tran+usr002)",
        "email": "lam.tran+usr002@betprophet.co", 
        "password": "Kh0ngbiet1",
        "access_key": "3324857df2d66566dfe6b660faa2923f",
        "secret_key": "8c970658226e64c7346e753ed7377c48"
    },
    {
        "id": 2,
        "name": "Account 2 (lam.tran+usr001)", 
        "email": "lam.tran+usr001@betprophet.co",
        "password": "Kh0ngbiet1", 
        "access_key": "cef986b533dfd3a0b1a732e34e5c1d60",
        "secret_key": "9d2f9f93158526a2fb9aeb24a6c5c082"
    }
]

# Shared data between accounts
shared_data = {
    "tournament_ids": [],  # Changed to support multiple tournaments
    "event_ids": [],
    "market_data": [],
    "lock": threading.Lock()
}

class ExposureAutoplay:
    """Main class for handling exposure testing automation"""
    
    def __init__(self, account_info):
        self.account = account_info
        self.mm_token = None
        self.web_token = None
        self.balance = 0
    
    # Authentication Methods
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
                print(f"✅ {self.account['name']} - MM Login successful!")
                return True
            else:
                print(f"❌ {self.account['name']} - MM Login failed: {response.status_code}")
                return False
        except Exception as e:
            print(f"❌ {self.account['name']} - MM Login error: {e}")
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
                print(f"✅ {self.account['name']} - Web login successful!")
                return True
            else:
                print(f"❌ {self.account['name']} - Web login failed: {response.status_code}")
                return False
        except Exception as e:
            print(f"❌ {self.account['name']} - Web login error: {e}")
            return False
    
    # Helper Methods
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
    
    # Balance and Wallet Methods
    def get_balance(self):
        """Get account balance using MM API"""
        if not self.mm_token:
            return 0
            
        balance_url = urljoin(BASE_URL, "partner/mm/get_balance")
        try:
            response = requests.get(balance_url, headers=self.get_mm_auth_header(), timeout=10)
            if response.status_code == 200:
                self.balance = response.json().get('data', {}).get('balance', 0)
                print(f"💰 {self.account['name']} - Balance: ${self.balance}")
                return self.balance
            else:
                print(f"❌ {self.account['name']} - Balance check failed: {response.status_code}")
                return 0
        except Exception as e:
            print(f"❌ {self.account['name']} - Balance error: {e}")
            return 0
    
    def get_wallet_info(self):
        """Get wallet information using web token"""
        if not self.web_token:
            return None
            
        try:
            response = requests.get(f"{BASE_URL}/api/v1/wallet", headers=self.get_web_auth_header(), timeout=10)
            if response.status_code == 200:
                wallet_data = response.json()['data']
                print(f"🏦 {self.account['name']} - Wallet Info:")
                print(f"   Balance: ${wallet_data.get('balance', 'N/A')}")
                print(f"   Exposure Credit (GEC): ${wallet_data.get('exposureCredit', 'N/A')}")
                print(f"   Total Balance: ${wallet_data.get('totalBalance', 'N/A')}")
                return wallet_data
            else:
                print(f"❌ {self.account['name']} - Wallet check failed: {response.status_code}")
                return None
        except Exception as e:
            print(f"❌ {self.account['name']} - Wallet error: {e}")
            return None
    
    def check_exposures(self, event_ids=None, market_ids=None):
        """Check wallet exposures (LEC) using web token"""
        if not self.web_token:
            return None
            
        params = {}
        if event_ids:
            params['eventIds'] = ','.join(map(str, event_ids))
        if market_ids:
            params['marketIds'] = ','.join(map(str, market_ids))
        
        try:
            response = requests.get(f"{BASE_URL}/api/v2/wallet/exposures", 
                                  headers=self.get_web_auth_header(), 
                                  params=params, timeout=10)
            
            print(f"🎯 {self.account['name']} - Exposure Check Status: {response.status_code}")
            if response.status_code == 200:
                exposure_data = response.json()
                print(f"📊 {self.account['name']} - LEC Data: {json.dumps(exposure_data, indent=2)}")
                return exposure_data
            else:
                print(f"❌ {self.account['name']} - Exposure check failed: {response.text}")
                return None
        except Exception as e:
            print(f"❌ {self.account['name']} - Exposure error: {e}")
            return None
    
    # Market Seeding Methods (Account 1 only)
    def seed_tournament(self):
        """Find a tournament with active events (only done by Account 1)"""
        if self.account['id'] != 1 or not self.mm_token:
            return False
            
        tournaments_url = urljoin(BASE_URL, "partner/mm/get_tournaments")
        events_url = urljoin(BASE_URL, "partner/mm/get_sport_events")
        
        try:
            response = requests.get(tournaments_url, headers=self.get_mm_auth_header(), timeout=10)
            if response.status_code == 200:
                tournaments = response.json().get('data', {}).get('tournaments', [])
                
                if not tournaments:
                    print("❌ No tournaments available")
                    return False
                
                print(f"🔍 Found {len(tournaments)} tournaments, scanning for comprehensive market coverage...")
                
                # Required market types
                REQUIRED_MARKET_TYPES = {'moneyline', 'spread', 'total'}
                qualifying_tournaments = []
                
                # First pass: identify tournaments with ALL market types
                for tournament in tournaments:
                    t_name = tournament['name']
                    t_id = tournament['id']
                    
                    print(f"   🏆 {t_name}: Checking...")
                    
                    # Get events
                    events_response = requests.get(events_url,
                                                 params={'tournament_id': t_id},
                                                 headers=self.get_mm_auth_header(), timeout=10)
                    
                    if events_response.status_code != 200:
                        print(f"      ⚠️ Failed to get events")
                        continue
                    
                    events = events_response.json().get('data', {}).get('sport_events', [])
                    if not events:
                        print(f"      ⚠️ No events available")
                        continue
                    
                    # Get markets for first 5 events to check market types
                    event_ids_str = ','.join(str(e['event_id']) for e in events[:5])
                    markets_url = urljoin(BASE_URL, "partner/mm/get_multiple_markets")
                    markets_response = requests.get(markets_url,
                                                   params={'event_ids': event_ids_str},
                                                   headers=self.get_mm_auth_header(), timeout=10)
                    
                    if markets_response.status_code != 200:
                        print(f"      ⚠️ Failed to get markets")
                        continue
                    
                    markets_by_event = markets_response.json().get('data', {})
                    found_market_types = set()
                    
                    # Check which market types are available
                    for event_id, markets in markets_by_event.items():
                        for market in markets:
                            market_type = market.get('type')
                            if market_type in REQUIRED_MARKET_TYPES:
                                found_market_types.add(market_type)
                    
                    # Check if this tournament has ALL required market types
                    if found_market_types == REQUIRED_MARKET_TYPES:
                        qualifying_tournaments.append(t_id)
                        print(f"      ✅ HAS ALL TYPES: {len(events)} events, Types: {', '.join(sorted(found_market_types))}")
                    else:
                        missing = REQUIRED_MARKET_TYPES - found_market_types
                        print(f"      ❌ Missing: {', '.join(sorted(missing))}")
                
                # Store all qualifying tournament IDs
                if qualifying_tournaments:
                    with shared_data["lock"]:
                        shared_data["tournament_ids"] = qualifying_tournaments
                    print(f"\n✅ Selected {len(qualifying_tournaments)} tournaments with ALL market types")
                    return True
                
                # Fallback: use any available tournament
                print("🔍 Target tournaments not available, using any available tournament...")
                for tournament in tournaments:
                    events_response = requests.get(events_url, 
                                                 params={'tournament_id': tournament['id']}, 
                                                 headers=self.get_mm_auth_header(), timeout=10)
                    
                    if events_response.status_code == 200:
                        events = events_response.json().get('data', {}).get('sport_events', [])
                        if events:
                            with shared_data["lock"]:
                                shared_data["tournament_ids"] = [tournament['id']]
                            print(f"✅ Using {tournament['name']} with {len(events)} events")
                            return True
                
                print("❌ No tournaments with events found")
                return False
            else:
                print(f"❌ Tournament fetch failed: {response.status_code}")
                return False
        except Exception as e:
            print(f"❌ Tournament seeding error: {e}")
            return False
    
    def get_events_and_markets(self):
        """Get events and markets from multiple tournaments (only done by Account 1)"""
        if self.account['id'] != 1 or not self.mm_token or not shared_data.get("tournament_ids"):
            return False
            
        events_url = urljoin(BASE_URL, "partner/mm/get_sport_events")
        multiple_markets_url = urljoin(BASE_URL, "partner/mm/get_multiple_markets")
        
        all_event_ids = []

        try:
            # Use target event directly if set, otherwise discover from tournaments
            if TARGET_EVENT_ID:
                all_event_ids = [TARGET_EVENT_ID]
                print(f"🎯 Using target event: {TARGET_EVENT_ID}")
            else:
                for tournament_id in shared_data["tournament_ids"]:
                    response = requests.get(events_url,
                                          params={'tournament_id': tournament_id},
                                          headers=self.get_mm_auth_header(), timeout=10)

                    if response.status_code == 200:
                        events = response.json().get('data', {}).get('sport_events', [])
                        if events:
                            tournament_event_ids = [event['event_id'] for event in events[:3]]
                            all_event_ids.extend(tournament_event_ids)
            
            if not all_event_ids:
                print("❌ No events found in any tournament")
                return False
            
            with shared_data["lock"]:
                shared_data["event_ids"] = all_event_ids
            
            print(f"🎮 Found {len(all_event_ids)} events across {len(shared_data['tournament_ids'])} tournaments")
            
            # Get markets for these events
            event_ids_str = ','.join(map(str, all_event_ids))
            print(f"   Fetching markets for events: {event_ids_str}")
            response = requests.get(multiple_markets_url,
                                  params={'event_ids': event_ids_str},
                                  headers=self.get_mm_auth_header(), timeout=10)
            
            print(f"   Markets response status: {response.status_code}")
            if response.status_code == 200:
                markets_by_event = response.json().get('data', {})
                
                # Extract market and line data
                # Market types to test
                MARKET_TYPES = ['moneyline', 'spread', 'total']
                market_data = []
                market_type_counts = {'moneyline': 0, 'spread': 0, 'total': 0}
                
                for event_id, markets in markets_by_event.items():
                    print(f"   Processing event {event_id} with {len(markets)} markets")
                    for market in markets:
                        try:
                            market_id = market.get('market_id') or market.get('id')
                            market_type = market.get('type', 'unknown')
                            
                            # Include moneyline, spread, and total markets
                            if market_type not in MARKET_TYPES:
                                continue
                            
                            print(f"   📈 Found {market_type} market (ID: {market_id})")
                            
                            # Handle markets with market_lines (spread, total)
                            if 'market_lines' in market:
                                for market_line in market.get('market_lines', []):
                                    selection_details = []
                                    for selection_group in market_line.get('selections', []):
                                        if isinstance(selection_group, list):
                                            for selection in selection_group:
                                                if selection.get('line_id'):
                                                    selection_details.append({
                                                        'line_id': selection['line_id'],
                                                        'name': selection.get('name', 'Unknown'),
                                                        'odds': selection.get('odds') if selection.get('odds') is not None else -110,
                                                        'outcome_id': selection.get('outcome_id', 'Unknown')
                                                    })
                                    
                                    if selection_details and len(selection_details) >= 2:
                                        line_ids = [s['line_id'] for s in selection_details]
                                        market_data.append({
                                            'event_id': int(event_id),
                                            'market_id': market_id,
                                            'market_type': market_type,
                                            'line_ids': line_ids,
                                            'selections': selection_details
                                        })
                                        market_type_counts[market_type] += 1
                            else:
                                # Handle markets with direct selections (moneyline)
                                selection_details = []
                                selections = market.get('selections', [])
                                
                                if selections:
                                    for selection_group in selections:
                                        if isinstance(selection_group, list):
                                            for selection in selection_group:
                                                if selection.get('line_id'):
                                                    selection_details.append({
                                                        'line_id': selection['line_id'],
                                                        'name': selection.get('name', 'Unknown'),
                                                        'odds': selection.get('odds') if selection.get('odds') is not None else -110,
                                                        'outcome_id': selection.get('outcome_id', 'Unknown')
                                                    })
                                        elif isinstance(selection_group, dict) and selection_group.get('line_id'):
                                            selection_details.append({
                                                'line_id': selection_group['line_id'],
                                                'name': selection_group.get('name', 'Unknown'),
                                                'odds': selection_group.get('odds') if selection_group.get('odds') is not None else -110,
                                                'outcome_id': selection_group.get('outcome_id', 'Unknown')
                                            })
                                
                                if selection_details and len(selection_details) >= 2:
                                    line_ids = [s['line_id'] for s in selection_details]
                                    market_data.append({
                                        'event_id': int(event_id),
                                        'market_id': market_id,
                                        'market_type': market_type,
                                        'line_ids': line_ids,
                                        'selections': selection_details
                                    })
                                    market_type_counts[market_type] += 1
                                    
                                    print(f"   ✅ Market added - Event: {event_id}, Type: {market_type}, Market ID: {market_id}, Lines: {len(line_ids)}")
                        except Exception as e:
                            print(f"   ❌ Error processing market: {e}")
                
                with shared_data["lock"]:
                    shared_data["market_data"] = market_data
                
                print(f"✅ Seeded {len(market_data)} total markets with betting lines:")
                print(f"   - Moneyline: {market_type_counts['moneyline']}")
                print(f"   - Spread: {market_type_counts['spread']}")
                print(f"   - Total: {market_type_counts['total']}")
                return len(market_data) > 0
            else:
                print(f"❌ Markets fetch failed: {response.status_code}")
                return False
        except Exception as e:
            print(f"❌ Events/Markets error: {e}")
            return False
    
    # Betting Methods
    def place_bet(self, line_id, odds, stake, side_name=""):
        """Place a bet using MM API"""
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
        
        try:
            response = requests.post(wager_url, json=body, headers=self.get_mm_auth_header(), timeout=10)
            if response.status_code == 200:
                result = response.json().get('data', {})
                wager = result.get('wager', {})
                wager_id = wager.get('id')
                print(f"🎲 {self.account['name']} - Bet placed successfully!")
                print(f"   Wager ID: {wager_id}")
                print(f"   Side: {side_name}")
                print(f"   Stake: ${stake}, Odds: {odds}")
                return {
                    'wager_id': wager_id,
                    'external_id': external_id,
                    'line_id': line_id,
                    'odds': odds,
                    'stake': stake
                }
            else:
                print(f"❌ {self.account['name']} - Bet placement failed: {response.status_code}")
                print(f"   Response: {response.text}")
                return None
        except Exception as e:
            print(f"❌ {self.account['name']} - Bet placement error: {e}")
            return None

# Utility Functions
def has_gec_for_event(tester, event_id):
    """Check if tester has GEC from a specific event"""
    try:
        lec_data = tester.check_exposures([event_id], [219])  # Check moneyline market
        if lec_data and lec_data.get('data'):
            for exposure in lec_data['data']:
                if exposure.get('eventId') == event_id and exposure.get('balance', 0) > 0:
                    return True
    except:
        pass
    return False

def coordinate_betting_for_gec_generation(authenticated_testers):
    """Coordinate betting between both accounts to generate GEC from multiple events"""
    print("\n" + "="*80)
    print("🎯 COORDINATED BETTING FOR GEC GENERATION")
    print("="*80)
    print(f"🎯 Target: Generate GEC from {TARGET_EVENTS} events")
    
    if not shared_data["market_data"]:
        print("❌ No market data available for betting")
        return False
    
    testers = authenticated_testers
    
    # Ensure both are logged in
    for tester in testers:
        if not tester.mm_token:
            print(f"❌ {tester.account['name']} not properly authenticated")
            return False
    
    # Track GEC generation progress
    events_with_gec = set()
    
    # Group markets by event
    events_markets = {}
    for market in shared_data["market_data"]:
        event_id = market['event_id']
        if event_id not in events_markets:
            events_markets[event_id] = []
        events_markets[event_id].append(market)
    
    print(f"📊 Available events for betting: {list(events_markets.keys())}")
    
    # Process each event until we have GEC from target number of events
    for event_id in events_markets.keys():
        if len(events_with_gec) >= TARGET_EVENTS:
            print(f"🎉 Target achieved! Generated GEC from {len(events_with_gec)} events")
            break
        
        print(f"\n" + "="*60)
        print(f"🎯 PROCESSING EVENT {event_id}")
        print(f"🎯 Events with GEC so far: {len(events_with_gec)}/{TARGET_EVENTS}")
        print("="*60)
        
        markets = events_markets[event_id]
        
        # Get initial balances
        print("💰 Getting initial balances...")
        initial_balances = []
        for tester in testers:
            balance = tester.get_balance()
            wallet = tester.get_wallet_info()
            initial_balances.append({
                'balance': balance,
                'gec': wallet.get('exposureCredit', 0) if wallet else 0
            })
        
        # Keep betting on this event until GEC is generated
        bet_round = 1
        gec_generated = False
        
        while not gec_generated and bet_round <= MAX_BET_ROUNDS:
            print(f"\n🎲 BET ROUND {bet_round} for Event {event_id}")
            
            # Select market for this round
            target_market = markets[(bet_round - 1) % len(markets)]
            print(f"   Using Market: {target_market['market_id']}, Lines: {len(target_market['line_ids'])}")
            
            if len(target_market['selections']) < 2:
                print(f"   ⚠️ Need at least 2 teams for opposing bets")
                bet_round += 1
                continue
            
            # Get teams based on outcome_id to ensure true opposition
            outcome_4_teams = [s for s in target_market['selections'] if s.get('outcome_id') == 4]
            outcome_5_teams = [s for s in target_market['selections'] if s.get('outcome_id') == 5]
            
            if not outcome_4_teams or not outcome_5_teams:
                print(f"   ⚠️ Missing opposing teams - Outcome 4: {len(outcome_4_teams)}, Outcome 5: {len(outcome_5_teams)}")
                bet_round += 1
                continue
            
            # Use first available team from each outcome
            team_outcome_4 = outcome_4_teams[0]  # Team A (outcome 4)
            team_outcome_5 = outcome_5_teams[0]  # Team B (outcome 5)
            
            # Calculate stake (increases each round)
            stake = 5.0 + (bet_round * 2.0)
            
            # Alternate who goes first each round
            if bet_round % 2 == 1:  # Odd rounds: Account 1 leads, Account 2 follows
                maker_account = testers[0]
                taker_account = testers[1]
                maker_team = team_outcome_4
                taker_team = team_outcome_5
                maker_odds = 160
                taker_odds = -160
                print(f"   🏆 Round {bet_round}: Account 1 leads on {maker_team['name']}, Account 2 follows on {taker_team['name']}")
            else:  # Even rounds: Account 2 leads, Account 1 follows
                maker_account = testers[1]
                taker_account = testers[0]
                maker_team = team_outcome_5
                taker_team = team_outcome_4
                maker_odds = 160
                taker_odds = -160
                print(f"   🏆 Round {bet_round}: Account 2 leads on {maker_team['name']}, Account 1 follows on {taker_team['name']}")
            
            # Step 1: Maker places initial bet
            print(f"   💼 STEP 1: {maker_account.account['name']} (MAKER) betting on {maker_team['name']}")
            maker_bet = maker_account.place_bet(
                line_id=maker_team['line_id'],
                odds=maker_odds,
                stake=stake,
                side_name=f"BACK {maker_team['name']}"
            )
            
            if not maker_bet:
                print(f"   ❌ Maker bet failed, skipping round {bet_round}")
                bet_round += 1
                continue
            
            # Wait for the position to be available
            print(f"   ⏱️ Waiting 3 seconds for maker bet to be available...")
            time.sleep(3)
            
            # Step 2: Taker matches the bet
            print(f"   🎯 STEP 2: {taker_account.account['name']} (TAKER) matching with {taker_team['name']}")
            taker_bet = taker_account.place_bet(
                line_id=taker_team['line_id'], 
                odds=taker_odds,
                stake=stake,
                side_name=f"BACK {taker_team['name']}"
            )
            
            # Count successful bets
            successful_bets = sum(1 for bet in [maker_bet, taker_bet] if bet is not None)
            print(f"   📊 Round {bet_round}: {successful_bets}/2 bets successful")
            
            if successful_bets >= 1:
                # Wait for exposure processing
                print(f"   ⏱️ Waiting 5 seconds for exposure processing...")
                time.sleep(5)
                
                # Check if GEC was generated for either account
                for i, tester in enumerate(testers):
                    if has_gec_for_event(tester, event_id):
                        print(f"   🎉 {tester.account['name']} - GEC GENERATED for Event {event_id}!")
                        gec_generated = True
                        events_with_gec.add(event_id)
                        
                        # Show current exposure details
                        lec_data = tester.check_exposures([event_id], [251])
                        if lec_data and lec_data.get('data'):
                            for exposure in lec_data['data']:
                                if exposure.get('eventId') == event_id:
                                    print(f"   💰 LEC Balance: ${exposure.get('balance', 0)}")
                        break
                
                # Also check wallet GEC changes
                for i, tester in enumerate(testers):
                    current_wallet = tester.get_wallet_info()
                    if current_wallet:
                        current_gec = current_wallet.get('exposureCredit', 0)
                        gec_change = current_gec - initial_balances[i]['gec']
                        if gec_change != 0:
                            print(f"   💰 {tester.account['name']} - GEC changed by ${gec_change}")
                
                if gec_generated:
                    print(f"   ✅ Event {event_id} successfully generated GEC!")
                    break
                else:
                    print(f"   ⏳ No GEC detected yet for Event {event_id}, continuing...")
            else:
                print(f"   ❌ Round {bet_round}: No successful bets, retrying...")
            
            bet_round += 1
            time.sleep(2)  # Brief pause between rounds
        
        if not gec_generated:
            print(f"   ⚠️ Could not generate GEC for Event {event_id} after {MAX_BET_ROUNDS} rounds")
        
        # Brief pause before moving to next event
        time.sleep(3)
    
    # Final summary
    print(f"\n" + "="*80)
    print(f"📋 GEC GENERATION SUMMARY")
    print(f"="*80)
    print(f"🎯 Target: Generate GEC from {TARGET_EVENTS} events")
    print(f"✅ Achieved: Generated GEC from {len(events_with_gec)} events")
    print(f"📊 Events with GEC: {list(events_with_gec)}")
    
    if len(events_with_gec) >= TARGET_EVENTS:
        print(f"🎉 SUCCESS! Generated GEC from {len(events_with_gec)} events!")
        return True
    else:
        print(f"⚠️ Partial success: Generated GEC from {len(events_with_gec)}/{TARGET_EVENTS} events")
        return len(events_with_gec) > 0

def main():
    """Main function to run exposure autoplay testing"""
    print("="*80)
    print("🚀 EXPOSURE AUTOPLAY - Automated Parallel Testing")
    print("="*80)
    print(f"Timestamp: {datetime.now()}")
    print(f"Testing coordinated betting between accounts to generate LEC/GEC\n")
    
    # Step 1: Parallel Authentication
    print("="*80)
    print("🔐 AUTHENTICATION PHASE")
    print("="*80)
    
    testers = [ExposureAutoplay(account) for account in ACCOUNTS]
    
    # Authenticate both accounts in parallel
    def authenticate_account(tester):
        mm_success = tester.mm_login()
        web_success = tester.web_login()
        if not web_success:
            print(f"⚠️  {tester.account['name']} - Web login failed (exposure checks will be skipped)")
        return tester, mm_success  # Only MM login is required
    
    with ThreadPoolExecutor(max_workers=2) as executor:
        auth_futures = [executor.submit(authenticate_account, tester) for tester in testers]
        auth_results = [future.result() for future in as_completed(auth_futures)]
    
    successful_auths = sum(1 for _, success in auth_results if success)
    print(f"\n📊 Authentication Results: {successful_auths}/2 accounts authenticated")
    
    if successful_auths < 2:
        print("❌ Need both accounts authenticated to proceed")
        return
    
    # Update testers list with authenticated instances
    testers = [tester for tester, success in auth_results if success]
    
    # Step 2: Tournament and Market Seeding (done by Account 1)
    print("\n" + "="*80)
    print("🌱 SEEDING PHASE")
    print("="*80)
    
    account1_tester = next(t for t in testers if t.account['id'] == 1)
    
    # Skip tournament discovery - target specific event directly
    shared_data["tournament_ids"] = ["direct"]
    print(f"🎯 Targeting specific event: {TARGET_EVENT_ID}")

    if not account1_tester.get_events_and_markets():
        print("❌ Events/Markets seeding failed")
        return
    
    # Step 3: Coordinated Betting for GEC Generation
    success = coordinate_betting_for_gec_generation(testers)
    
    # Step 4: Final Summary
    print("\n" + "="*80)
    print("📋 FINAL SUMMARY")
    print("="*80)
    
    if success:
        print("✅ Exposure autoplay completed successfully!")
        print("🎯 Check the output above for exposure changes (GEC/LEC)")
        print("💡 If no exposure changes detected, it may be due to:")
        print("   - Bets not matching/settling yet")
        print("   - Exposure thresholds not met")
        print("   - System processing delays")
    else:
        print("❌ Exposure autoplay encountered issues")
    
    print(f"\nTest completed at: {datetime.now()}")

if __name__ == "__main__":
    main()
