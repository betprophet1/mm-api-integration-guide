#!/usr/bin/env python3

import sys
import os
import requests
import json
import time
from urllib.parse import urljoin

sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.mm_calls import MMInteractions
from src.log import logging
from src import config

def test_wallet_endpoints(mm, account_name):
    """Test wallet endpoints for a specific account"""
    
    logging.info(f"🧪 Testing wallet endpoints for {account_name}")
    
    # Get a few event IDs from seeded data
    event_ids = []
    if mm.sport_events:
        event_ids = list(mm.sport_events.keys())[:2]
    
    # Test 1: GEC from wallet endpoint
    logging.info("💰 Testing GEC retrieval...")
    try:
        gec_url = 'https://api-ss-sandbox.betprophet.co/api/v1/wallet'
        response = requests.get(gec_url, headers=mm._MMInteractions__get_auth_header())
        
        if response.status_code == 200:
            wallet_data = response.json().get('data', {})
            exposure_credit = wallet_data.get('exposureCredit', 'N/A')
            logging.info(f"✅ GEC retrieved: {exposure_credit}")
            logging.info(f"📋 Full wallet data: {json.dumps(wallet_data, indent=2)}")
        else:
            logging.info(f"❌ GEC endpoint returned: {response.status_code}")
            logging.info(f"   Response: {response.text}")
    except Exception as e:
        logging.error(f"❌ Error getting GEC: {str(e)}")
    
    # Test 2: LEC from exposures endpoint
    logging.info("🎯 Testing LEC retrieval...")
    try:
        if event_ids:
            event_ids_str = ','.join(str(eid) for eid in event_ids)
            lec_url = f'https://api-ss-sandbox.betprophet.co/api/v2/wallet/exposures?eventIds={event_ids_str}&marketIds=251'
            logging.info(f"🔍 Querying LEC for events: {event_ids_str}")
        else:
            lec_url = 'https://api-ss-sandbox.betprophet.co/api/v2/wallet/exposures?eventIds=10075921&marketIds=251'
            
        response = requests.get(lec_url, headers=mm._MMInteractions__get_auth_header())
        
        if response.status_code == 200:
            lec_data = response.json().get('data', {})
            if lec_data:
                logging.info(f"✅ LEC retrieved: {json.dumps(lec_data, indent=2)}")
            else:
                logging.info("ℹ️  No LEC data found")
        else:
            logging.info(f"❌ LEC endpoint returned: {response.status_code}")
            logging.info(f"   Response: {response.text}")
    except Exception as e:
        logging.error(f"❌ Error getting LEC: {str(e)}")

def place_test_bet_and_check_exposure(mm, account_name):
    """Place a test bet and check for generated exposure"""
    
    if not mm.sport_events:
        logging.info("ℹ️  No sport events available")
        return False
    
    # Place a test bet
    event_id = list(mm.sport_events.keys())[0]
    event = mm.sport_events[event_id]
    
    logging.info(f"🏟️  Using event: {event.get('name', 'Unknown')}")
    
    # Find moneyline market
    for market in event.get('markets', []):
        if market.get('type') == 'moneyline':
            selections = market.get('selections', [])
            if selections and len(selections) > 0:
                line_id = selections[0][0].get('line_id')
                team_name = selections[0][0].get('name', 'Unknown')
                
                if line_id:
                    # Place bet
                    import uuid
                    play_url = urljoin(mm.base_url, config.URL['mm_place_wager'])
                    external_id = str(uuid.uuid1())
                    
                    body = {
                        'external_id': external_id,
                        'line_id': line_id,
                        'odds': 110,
                        'stake': 10.0  # $10 bet
                    }
                    
                    logging.info(f"🎲 Placing $10 test bet on {team_name} at +110 odds")
                    
                    try:
                        response = requests.post(play_url, json=body, headers=mm._MMInteractions__get_auth_header())
                        
                        if response.status_code == 200:
                            result = response.json().get('data', {})
                            wager_id = result.get('wager', {}).get('id')
                            logging.info(f"✅ Bet placed successfully! Wager ID: {wager_id}")
                            
                            # Wait and check for exposure
                            logging.info("⏳ Waiting 5 seconds for exposure generation...")
                            time.sleep(5)
                            
                            # Check wallet endpoints again
                            test_wallet_endpoints(mm, account_name)
                            return True
                        else:
                            logging.error(f"❌ Failed to place bet: {response.status_code}")
                            return False
                    except Exception as e:
                        logging.error(f"❌ Exception placing bet: {str(e)}")
                        return False
    return False

def test_account(account_num, account_name):
    """Test a specific account"""
    
    logging.info("=" * 70)
    logging.info(f"🔑 Testing {account_name}")
    logging.info("=" * 70)
    
    try:
        # Get credentials for specific account
        credentials = config.get_account_credentials(account_num)
        
        # Create MM instance with specific credentials
        mm = MMInteractions()
        mm.mm_keys = {
            'access_key': credentials['access_key'],
            'secret_key': credentials['secret_key']
        }
        
        logging.info(f"🔐 Logging in with access key: {credentials['access_key']}")
        mm.mm_login()
        
        # Quick seed to get some events
        logging.info("🌱 Seeding tournaments and events...")
        mm.seeding()
        
        # Get balance
        mm.get_balance()
        logging.info(f"💰 Current balance: ${mm.balance}")
        
        # Test wallet endpoints initially
        test_wallet_endpoints(mm, account_name)
        
        # Place test bet and check exposure if balance available
        if mm.balance > 10:
            logging.info("🎲 Placing test bet to generate exposure...")
            place_test_bet_and_check_exposure(mm, account_name)
        else:
            logging.info("ℹ️  Insufficient balance for test bet")
            
        return True
        
    except Exception as e:
        logging.error(f"❌ Error testing {account_name}: {str(e)}")
        return False

def main():
    logging.info("🚀 Testing Exposure Access with Both Accounts")
    
    # Test Account 1 (user002)
    success1 = test_account(1, "Account 1 (user002@betprophet.co)")
    
    # Test Account 2 (user001) 
    success2 = test_account(2, "Account 2 (user001@betprophet.co)")
    
    # Summary
    logging.info("=" * 70)
    logging.info("📊 TEST SUMMARY")
    logging.info("=" * 70)
    logging.info(f"Account 1 (user002): {'✅ SUCCESS' if success1 else '❌ FAILED'}")
    logging.info(f"Account 2 (user001): {'✅ SUCCESS' if success2 else '❌ FAILED'}")
    
    if not success1 and not success2:
        logging.info("💡 Both accounts failed - Exposure feature may not be enabled yet")
    elif success1 or success2:
        logging.info("🎉 At least one account has exposure access!")

if __name__ == "__main__":
    main()
