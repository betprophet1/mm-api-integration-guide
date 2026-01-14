#!/usr/bin/env python3
"""
Market Type Discovery Script
Checks what market types (moneyline, spread, total) are available in sandbox
"""

import requests
import json
from collections import defaultdict

BASE_URL = "https://api-ss-sandbox.betprophet.co"

# Account 1 credentials
ACCESS_KEY = "3324857df2d66566dfe6b660faa2923f"
SECRET_KEY = "8c970658226e64c7346e753ed7377c48"

def mm_login():
    """Login to MM API"""
    login_url = f"{BASE_URL}/partner/auth/login"
    request_body = {
        'access_key': ACCESS_KEY,
        'secret_key': SECRET_KEY,
    }
    
    try:
        response = requests.post(login_url, data=json.dumps(request_body))
        if response.status_code == 200:
            mm_session = response.json()['data']
            token = mm_session['access_token']
            print("✅ Login successful")
            return token
        else:
            print(f"❌ Login failed: {response.status_code}")
            return None
    except Exception as e:
        print(f"❌ Login error: {e}")
        return None

def get_auth_header(token):
    """Get authorization header"""
    return {'Authorization': f'Bearer {token}'}

def discover_markets(token):
    """Discover all available market types"""
    print("\n🔍 DISCOVERING MARKET TYPES...")
    print("=" * 80)
    
    # Get tournaments
    print("\n📋 Fetching tournaments...")
    response = requests.get(f"{BASE_URL}/partner/mm/get_tournaments", 
                          headers=get_auth_header(token))
    
    if response.status_code != 200:
        print("❌ Failed to get tournaments")
        return
    
    tournaments = response.json().get('data', {}).get('tournaments', [])
    print(f"✅ Found {len(tournaments)} tournaments")
    
    market_type_stats = defaultdict(lambda: {'count': 0, 'events': set(), 'tournaments': set()})
    total_events = 0
    total_markets = 0
    
    # Check each tournament
    for tournament in tournaments:
        t_name = tournament['name']
        t_id = tournament['id']
        
        print(f"\n🏆 Checking tournament: {t_name} (ID: {t_id})")
        
        # Get events
        events_response = requests.get(f"{BASE_URL}/partner/mm/get_sport_events",
                                      params={'tournament_id': t_id},
                                      headers=get_auth_header(token))
        
        if events_response.status_code != 200:
            print(f"   ⚠️ Failed to get events")
            continue
        
        events = events_response.json().get('data', {}).get('sport_events', [])
        if not events:
            print(f"   ℹ️ No events available")
            continue
        
        print(f"   ✅ Found {len(events)} events")
        total_events += len(events)
        
        # Get markets for these events
        event_ids = ','.join(str(e['event_id']) for e in events[:10])  # Check first 10
        markets_response = requests.get(f"{BASE_URL}/partner/mm/get_multiple_markets",
                                       params={'event_ids': event_ids},
                                       headers=get_auth_header(token))
        
        if markets_response.status_code != 200:
            print(f"   ⚠️ Failed to get markets")
            continue
        
        markets_by_event = markets_response.json().get('data', {})
        
        # Analyze markets
        for event_id, markets in markets_by_event.items():
            for market in markets:
                market_type = market.get('type', 'unknown')
                total_markets += 1
                
                if market_type in ['moneyline', 'spread', 'total']:
                    market_type_stats[market_type]['count'] += 1
                    market_type_stats[market_type]['events'].add(event_id)
                    market_type_stats[market_type]['tournaments'].add(t_name)
        
        # Show tournament summary
        t_market_types = set()
        for event_id, markets in markets_by_event.items():
            for market in markets:
                t_market_types.add(market.get('type', 'unknown'))
        
        print(f"   📊 Market types in tournament: {', '.join(sorted(t_market_types))}")
    
    # Final summary
    print("\n" + "=" * 80)
    print("📊 MARKET TYPE SUMMARY")
    print("=" * 80)
    print(f"Total Events Checked: {total_events}")
    print(f"Total Markets Found: {total_markets}")
    print()
    
    for market_type in ['moneyline', 'spread', 'total']:
        stats = market_type_stats[market_type]
        if stats['count'] > 0:
            print(f"✅ {market_type.upper()}")
            print(f"   Markets: {stats['count']}")
            print(f"   Events: {len(stats['events'])}")
            print(f"   Tournaments: {', '.join(stats['tournaments'])}")
        else:
            print(f"❌ {market_type.upper()}: Not found")
        print()
    
    # Show recommendation
    print("=" * 80)
    print("💡 RECOMMENDATIONS")
    print("=" * 80)
    
    if market_type_stats['spread']['count'] > 0 and market_type_stats['total']['count'] > 0:
        print("✅ All market types available! Ready for comprehensive testing.")
    else:
        print("⚠️ Limited market types available:")
        if market_type_stats['spread']['count'] == 0:
            print("   - No SPREAD markets found")
        if market_type_stats['total']['count'] == 0:
            print("   - No TOTAL markets found")
        print("\nSuggestions:")
        print("   1. Test during peak betting hours (closer to event start)")
        print("   2. Try different sports (NFL/NBA typically have all types)")
        print("   3. Test in staging environment")
    
    return market_type_stats

if __name__ == "__main__":
    print("🚀 MARKET TYPE DISCOVERY TOOL")
    print("=" * 80)
    
    token = mm_login()
    if token:
        discover_markets(token)
    else:
        print("❌ Cannot proceed without authentication")
