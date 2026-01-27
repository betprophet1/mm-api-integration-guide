#!/usr/bin/env python3
"""
Exposure Stress Test: Aggressive Multi-Account Betting
Target: Generate maximum exposure (GEC/LEC) through coordinated betting
Strategy: High-frequency parallel bet placement across multiple events
"""

import argparse
import signal
import sys
import time
import threading
import uuid
import requests
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin
from datetime import datetime

# Global control
should_stop = False
bets_placed_count = 0
gec_events_count = 0
target_bets = 1000
target_gec_events = 5
stats_lock = threading.Lock()

# Performance tracking
performance_data = {
    'start_time': None,
    'end_time': None,
    'worker_stats': {},
    'account_stats': {},
    'error_log': [],
    'bet_timeline': []
}

# Environment URLs
ENVIRONMENT_URLS = {
    'sandbox': 'https://api-ss-sandbox.betprophet.co',
    'staging': 'https://api-ss-staging.betprophet.co'
}

# Account credentials
ACCOUNTS = [
    {
        "id": 1,
        "name": "Account 1",
        "email": "lam.tran+usr002@betprophet.co", 
        "password": "Kh0ngbiet1",
        "access_key": "3324857df2d66566dfe6b660faa2923f",
        "secret_key": "8c970658226e64c7346e753ed7377c48"
    },
    {
        "id": 2,
        "name": "Account 2", 
        "email": "lam.tran+usr001@betprophet.co",
        "password": "Kh0ngbiet1", 
        "access_key": "cef986b533dfd3a0b1a732e34e5c1d60",
        "secret_key": "9d2f9f93158526a2fb9aeb24a6c5c082"
    }
]

def signal_handler(sig, frame):
    """Handle Ctrl+C to gracefully stop"""
    global should_stop
    print("\n🛑 Received stop signal. Stopping stress test...")
    should_stop = True
    sys.exit(0)

def progress_monitor():
    """Monitor and report progress"""
    global should_stop, bets_placed_count, gec_events_count, target_bets, target_gec_events
    
    start_time = time.time()
    while not should_stop:
        time.sleep(10)  # Report every 10 seconds
        if not should_stop:
            elapsed = time.time() - start_time
            with stats_lock:
                placed = bets_placed_count
                gec_events = gec_events_count
            
            bet_rate = placed / elapsed if elapsed > 0 else 0
            remaining = target_bets - placed
            eta = remaining / bet_rate if bet_rate > 0 else 0
            
            print(f"📊 PROGRESS: {placed}/{target_bets} bets ({placed/target_bets*100:.1f}%) | "
                  f"{gec_events}/{target_gec_events} GEC events | "
                  f"Rate: {bet_rate:.1f} bets/s | "
                  f"ETA: {eta/60:.1f}m")

class ExposureTester:
    """Handles authentication and betting for a single account"""
    
    def __init__(self, account_info, base_url):
        self.account = account_info
        self.base_url = base_url
        self.mm_token = None
        self.web_token = None
        self.balance = 0
    
    def mm_login(self):
        """Login using MM API"""
        login_url = f"{self.base_url}/partner/auth/login"
        request_body = {
            'access_key': self.account['access_key'],
            'secret_key': self.account['secret_key'],
        }
        
        try:
            response = requests.post(login_url, data=json.dumps(request_body), timeout=10)
            if response.status_code == 200:
                mm_session = response.json()['data']
                self.mm_token = mm_session['access_token']
                print(f"✅ {self.account['name']} - MM Login successful")
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
            'origin': self.base_url.replace('api-ss', 'ss'),
            'x-currency': 'cash'
        }
        
        payload = {
            "email": self.account['email'],
            "password": self.account['password'],
            "code": "123456",
            "device_id": device_id
        }
        
        try:
            response = requests.post(f"{self.base_url}/api/v1/auth/login", 
                                    headers=headers, json=payload, timeout=10)
            if response.status_code == 200:
                self.web_token = response.json().get('accessToken')
                print(f"✅ {self.account['name']} - Web login successful")
                return True
            else:
                print(f"❌ {self.account['name']} - Web login failed: {response.status_code}")
                return False
        except Exception as e:
            print(f"❌ {self.account['name']} - Web login error: {e}")
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
        """Get account balance"""
        if not self.mm_token:
            return 0
        
        try:
            response = requests.get(f"{self.base_url}/partner/mm/get_balance", 
                                   headers=self.get_mm_auth_header(), timeout=10)
            if response.status_code == 200:
                self.balance = response.json().get('data', {}).get('balance', 0)
                return self.balance
        except Exception as e:
            print(f"❌ Balance error: {e}")
        return 0
    
    def place_bet(self, line_id, odds, stake):
        """Place a single bet"""
        global should_stop, bets_placed_count
        
        if should_stop or not self.mm_token:
            return None
        
        external_id = str(uuid.uuid1())
        body = {
            'external_id': external_id,
            'line_id': line_id,
            'odds': odds,
            'stake': stake
        }
        
        try:
            response = requests.post(f"{self.base_url}/partner/mm/place_wager", 
                                    json=body, headers=self.get_mm_auth_header(), timeout=10)
            if response.status_code == 200:
                result = response.json().get('data', {})
                wager = result.get('wager', {})
                with stats_lock:
                    bets_placed_count += 1
                return {
                    'wager_id': wager.get('id'),
                    'external_id': external_id,
                    'line_id': line_id
                }
            else:
                print(f"❌ {self.account['name']} bet failed (HTTP {response.status_code}): {response.text[:200]}")
        except Exception as e:
            print(f"❌ {self.account['name']} bet error: {e}")
        return None
    
    def check_exposures(self, event_ids=None, market_ids=None):
        """Check wallet exposures (LEC) - uses MM API for SP accounts"""
        if not self.mm_token:
            return None
        
        # For MM/SP accounts, exposures can be checked via balance endpoint
        # This is a simplified version - full exposure tracking would need web API
        return None  # Disabled for MM-only accounts

def generate_performance_report(environment, num_workers, elapsed_time):
    """Generate and save performance test report"""
    global performance_data, bets_placed_count, gec_events_count, target_bets, target_gec_events
    
    report_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_filename = f"exposure_stress_test_report_{report_timestamp}.txt"
    
    # Calculate aggregate stats
    total_errors = len(performance_data['error_log'])
    success_rate = (bets_placed_count / target_bets * 100) if target_bets > 0 else 0
    avg_bet_rate = bets_placed_count / elapsed_time if elapsed_time > 0 else 0
    
    # Worker performance summary
    worker_summary = []
    for worker_name, stats in performance_data['worker_stats'].items():
        worker_summary.append(f"  {worker_name}:")
        worker_summary.append(f"    Bets Placed: {stats['bets_placed']}")
        worker_summary.append(f"    Successful Pairs: {stats['successful_pairs']}")
        worker_summary.append(f"    Errors: {stats['errors']}")
        worker_summary.append(f"    Duration: {stats['duration_seconds']:.2f}s")
        worker_summary.append(f"    Bet Rate: {stats['bet_rate']:.2f} bets/s")
        worker_summary.append("")
    
    # Generate report content
    report_lines = [
        "=" * 80,
        "EXPOSURE STRESS TEST - PERFORMANCE REPORT",
        "=" * 80,
        "",
        f"Test Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"Environment: {environment}",
        f"Duration: {elapsed_time:.2f} seconds ({elapsed_time/60:.2f} minutes)",
        "",
        "=" * 80,
        "TEST CONFIGURATION",
        "=" * 80,
        f"Target Bets: {target_bets}",
        f"Target GEC Events: {target_gec_events}",
        f"Number of Workers: {num_workers}",
        f"Number of Accounts: 2",
        "",
        "=" * 80,
        "RESULTS SUMMARY",
        "=" * 80,
        f"Total Bets Placed: {bets_placed_count} / {target_bets} ({success_rate:.1f}%)",
        f"GEC Events Generated: {gec_events_count} / {target_gec_events}",
        f"Total Errors: {total_errors}",
        f"Average Bet Rate: {avg_bet_rate:.2f} bets/second",
        f"Peak Bet Rate: {max((s['bet_rate'] for s in performance_data['worker_stats'].values()), default=0):.2f} bets/second",
        "",
        "=" * 80,
        "WORKER PERFORMANCE",
        "=" * 80,
        *worker_summary,
        "=" * 80,
        "ACCOUNT STATISTICS",
        "=" * 80,
    ]
    
    # Add account stats
    for account_name, stats in performance_data['account_stats'].items():
        report_lines.append(f"\n{account_name}:")
        report_lines.append(f"  Starting Balance: ${stats.get('balance_start', 0):.2f}")
        report_lines.append(f"  Ending Balance: ${stats.get('balance_end', 0):.2f}")
        report_lines.append(f"  Balance Change: ${stats.get('balance_end', 0) - stats.get('balance_start', 0):.2f}")
    
    report_lines.extend([
        "",
        "=" * 80,
        "ERROR SUMMARY",
        "=" * 80,
        f"Total Errors: {total_errors}",
        ""
    ])
    
    # Add error breakdown
    if total_errors > 0:
        error_types = {}
        for error in performance_data['error_log']:
            error_msg = error.get('error', 'Unknown')
            error_types[error_msg] = error_types.get(error_msg, 0) + 1
        
        report_lines.append("Error Breakdown:")
        for error_type, count in sorted(error_types.items(), key=lambda x: x[1], reverse=True):
            report_lines.append(f"  {error_type}: {count} occurrences")
        
        # Show last 10 errors
        report_lines.append("\nLast 10 Errors:")
        for error in performance_data['error_log'][-10:]:
            timestamp = datetime.fromtimestamp(error['timestamp']).strftime('%H:%M:%S')
            worker = error.get('worker_id', 'N/A')
            account = error.get('account', 'N/A')
            msg = error.get('error', 'Unknown')
            report_lines.append(f"  [{timestamp}] Worker {worker}, {account}: {msg}")
    else:
        report_lines.append("No errors recorded ✅")
    
    report_lines.extend([
        "",
        "=" * 80,
        "PERFORMANCE METRICS",
        "=" * 80,
        f"Bets per Second (Average): {avg_bet_rate:.2f}",
        f"Bets per Minute (Average): {avg_bet_rate * 60:.2f}",
        f"Bets per Worker (Average): {bets_placed_count / num_workers:.2f}",
        f"Success Rate: {success_rate:.2f}%",
        f"Error Rate: {(total_errors / max(bets_placed_count, 1)) * 100:.2f}%",
        "",
        "=" * 80,
        "END OF REPORT",
        "=" * 80
    ])
    
    # Write report to file
    report_content = "\n".join(report_lines)
    with open(report_filename, 'w') as f:
        f.write(report_content)
    
    # Also print summary to console
    print("\n" + "=" * 80)
    print("📊 PERFORMANCE REPORT GENERATED")
    print("=" * 80)
    print(f"Report saved to: {report_filename}")
    print(f"\nKey Metrics:")
    print(f"  • Total Bets: {bets_placed_count}/{target_bets} ({success_rate:.1f}%)")
    print(f"  • Avg Rate: {avg_bet_rate:.2f} bets/s")
    print(f"  • Total Errors: {total_errors}")
    print(f"  • Duration: {elapsed_time/60:.2f} minutes")
    print("=" * 80)
    
    return report_filename

def get_market_data(tester, event_id=None):
    """Get tournaments, events and markets - auto-selects tournaments with ALL market types or uses specific event"""
    if not tester.mm_token:
        return []
    
    # Market types to test
    REQUIRED_MARKET_TYPES = {'moneyline', 'spread', 'total'}
    
    try:
        # If specific event ID provided, use it directly
        if event_id:
            print(f"🎯 Using specific event ID: {event_id}")
            response = requests.get(f"{tester.base_url}/partner/mm/get_multiple_markets",
                                  params={'event_ids': str(event_id)},
                                  headers=tester.get_mm_auth_header(), timeout=10)
            
            if response.status_code != 200:
                print(f"❌ Failed to get markets for event {event_id}")
                return []
            
            markets_by_event = response.json().get('data', {})
            all_market_data = []
            market_type_counts = {'moneyline': 0, 'spread': 0, 'total': 0}
            
            for evt_id, markets in markets_by_event.items():
                for market in markets:
                    market_type = market.get('type')
                    
                    if market_type in REQUIRED_MARKET_TYPES:
                        # Handle markets with market_lines (spread, total)
                        if 'market_lines' in market:
                            for market_line in market.get('market_lines', []):
                                selections = []
                                for sel_group in market_line.get('selections', []):
                                    if isinstance(sel_group, list):
                                        for sel in sel_group:
                                            if sel.get('line_id'):
                                                selections.append({
                                                    'line_id': sel['line_id'],
                                                    'outcome_id': sel.get('outcome_id'),
                                                    'odds': sel.get('odds') if sel.get('odds') is not None else -110,
                                                    'name': sel.get('name', 'Unknown')
                                                })
                                
                                if len(selections) >= 2:
                                    all_market_data.append({
                                        'event_id': int(evt_id),
                                        'market_id': market.get('id'),
                                        'market_type': market_type,
                                        'tournament': f'Event {evt_id}',
                                        'selections': selections
                                    })
                                    market_type_counts[market_type] += 1
                        else:
                            # Handle markets with direct selections (moneyline)
                            selections = []
                            for sel_group in market.get('selections', []):
                                if isinstance(sel_group, list):
                                    for sel in sel_group:
                                        if sel.get('line_id'):
                                            selections.append({
                                                'line_id': sel['line_id'],
                                                'outcome_id': sel.get('outcome_id'),
                                                'odds': sel.get('odds') if sel.get('odds') is not None else -110,
                                                'name': sel.get('name', 'Unknown')
                                            })
                            
                            if len(selections) >= 2:
                                all_market_data.append({
                                    'event_id': int(evt_id),
                                    'market_id': market.get('market_id') or market.get('id'),
                                    'market_type': market_type,
                                    'tournament': f'Event {evt_id}',
                                    'selections': selections
                                })
                                market_type_counts[market_type] += 1
            
            print(f"✅ Found {len(all_market_data)} bettable markets:")
            print(f"   - Moneyline: {market_type_counts['moneyline']}")
            print(f"   - Spread: {market_type_counts['spread']}")
            print(f"   - Total: {market_type_counts['total']}")
            return all_market_data
        
        # Otherwise, scan tournaments to find those with ALL market types
        # Get tournaments
        response = requests.get(f"{tester.base_url}/partner/mm/get_tournaments",
                              headers=tester.get_mm_auth_header(), timeout=10)
        if response.status_code != 200:
            return []
        
        tournaments = response.json().get('data', {}).get('tournaments', [])
        if not tournaments:
            return []
        
        # Filter to only NBA, MLB, and NFL tournaments
        target_tournament_names = ['NBA', 'MLB', 'NFL']
        tournaments = [t for t in tournaments if t['name'] in target_tournament_names]
        
        print(f"🔍 Scanning {len(tournaments)} tournaments (NBA, MLB, NFL) to find those with ALL market types...")
        
        # First pass: identify tournaments with all market types
        qualifying_tournaments = []
        
        for tournament in tournaments:
            t_name = tournament['name']
            t_id = tournament['id']
            
            print(f"   🏆 {t_name}: Checking...")
            
            # Get events
            response = requests.get(f"{tester.base_url}/partner/mm/get_sport_events",
                                  params={'tournament_id': t_id},
                                  headers=tester.get_mm_auth_header(), timeout=10)
            
            if response.status_code != 200:
                print(f"      ⚠️ Failed to get events")
                continue
            
            events = response.json().get('data', {}).get('sport_events', [])
            if not events:
                print(f"      ⚠️ No events available")
                continue
            
            # Get markets for first 5 events to check market types
            event_ids = ','.join(str(e['event_id']) for e in events[:5])
            response = requests.get(f"{tester.base_url}/partner/mm/get_multiple_markets",
                                  params={'event_ids': event_ids},
                                  headers=tester.get_mm_auth_header(), timeout=10)
            
            if response.status_code != 200:
                print(f"      ⚠️ Failed to get markets")
                continue
            
            markets_by_event = response.json().get('data', {})
            
            # Check which market types are available
            found_market_types = set()
            for event_id, markets in markets_by_event.items():
                for market in markets:
                    market_type = market.get('type')
                    if market_type in REQUIRED_MARKET_TYPES:
                        found_market_types.add(market_type)
            
            # Check if tournament has ALL required market types
            if found_market_types == REQUIRED_MARKET_TYPES:
                qualifying_tournaments.append({
                    'name': t_name,
                    'id': t_id,
                    'events': events,
                    'market_types': found_market_types
                })
                print(f"      ✅ HAS ALL TYPES: {len(events)} events, Types: {', '.join(sorted(found_market_types))}")
            else:
                missing = REQUIRED_MARKET_TYPES - found_market_types
                print(f"      ❌ Missing: {', '.join(sorted(missing))}")
        
        if not qualifying_tournaments:
            print("⚠️ No tournaments found with ALL market types. Falling back to any available markets...")
            # Fallback: use any tournament with events
            for tournament in tournaments:
                response = requests.get(f"{tester.base_url}/partner/mm/get_sport_events",
                                      params={'tournament_id': tournament['id']},
                                      headers=tester.get_mm_auth_header(), timeout=10)
                if response.status_code == 200:
                    events = response.json().get('data', {}).get('sport_events', [])
                    if events:
                        qualifying_tournaments = [{'name': tournament['name'], 'id': tournament['id'], 'events': events}]
                        print(f"✅ Using {tournament['name']} (fallback)")
                        break
        
        # Second pass: collect markets from qualifying tournaments
        all_market_data = []
        overall_market_type_counts = {'moneyline': 0, 'spread': 0, 'total': 0}
        tournaments_used = []
        
        print(f"\n🎯 Selected {len(qualifying_tournaments)} qualifying tournaments")
        
        for tournament_info in qualifying_tournaments:
            t_name = tournament_info['name']
            t_id = tournament_info['id']
            events = tournament_info['events']
            
            print(f"   📈 {t_name}: Collecting markets...")
            
            # Get markets for first 5 events
            event_ids = ','.join(str(e['event_id']) for e in events[:5])
            response = requests.get(f"{tester.base_url}/partner/mm/get_multiple_markets",
                                  params={'event_ids': event_ids},
                                  headers=tester.get_mm_auth_header(), timeout=10)
            
            if response.status_code != 200:
                print(f"      ⚠️ Failed to get markets")
                continue
            
            markets_by_event = response.json().get('data', {})
            tournament_market_type_counts = {'moneyline': 0, 'spread': 0, 'total': 0}
            
            # Extract market data for this tournament
            for event_id, markets in markets_by_event.items():
                for market in markets:
                    market_type = market.get('type')
                    
                    # Include moneyline, spread, and total markets
                    if market_type in REQUIRED_MARKET_TYPES:
                        selections = []
                        for sel_group in market.get('selections', []):
                            if isinstance(sel_group, list):
                                for sel in sel_group:
                                    # Include all selections with line_id (use default odds if null)
                                    if sel.get('line_id'):
                                        selections.append({
                                            'line_id': sel['line_id'],
                                            'outcome_id': sel.get('outcome_id'),
                                            'odds': sel.get('odds') if sel.get('odds') is not None else -110,
                                            'name': sel.get('name', 'Unknown')
                                        })
                        
                        if len(selections) >= 2:
                            all_market_data.append({
                                'event_id': int(event_id),
                                'market_id': market.get('market_id') or market.get('id'),
                                'market_type': market_type,
                                'tournament': t_name,
                                'selections': selections
                            })
                            tournament_market_type_counts[market_type] += 1
                            overall_market_type_counts[market_type] += 1
            
            if sum(tournament_market_type_counts.values()) > 0:
                print(f"      ✅ Collected: Moneyline={tournament_market_type_counts['moneyline']}, "
                      f"Spread={tournament_market_type_counts['spread']}, "
                      f"Total={tournament_market_type_counts['total']}")
                tournaments_used.append(t_name)
            else:
                print(f"      ⚠️ No usable markets found")
        
        # Fallback: if no markets from target tournaments, use any available
        if not all_market_data:
            print("🔍 Target tournaments had no markets, searching all tournaments...")
            for tournament in tournaments:
                response = requests.get(f"{tester.base_url}/partner/mm/get_sport_events",
                                      params={'tournament_id': tournament['id']},
                                      headers=tester.get_mm_auth_header(), timeout=10)
                
                if response.status_code == 200:
                    events = response.json().get('data', {}).get('sport_events', [])
                    if events:
                        event_ids = ','.join(str(e['event_id']) for e in events[:5])
                        response = requests.get(f"{tester.base_url}/partner/mm/get_multiple_markets",
                                              params={'event_ids': event_ids},
                                              headers=tester.get_mm_auth_header(), timeout=10)
                        
                        if response.status_code == 200:
                            markets_by_event = response.json().get('data', {})
                            
                            for event_id, markets in markets_by_event.items():
                                for market in markets:
                                    market_type = market.get('type')
                                    
                                    if market_type in MARKET_TYPES:
                                        selections = []
                                        for sel_group in market.get('selections', []):
                                            if isinstance(sel_group, list):
                                                for sel in sel_group:
                                                    if sel.get('line_id'):
                                                        selections.append({
                                                            'line_id': sel['line_id'],
                                                            'outcome_id': sel.get('outcome_id'),
                                                            'odds': sel.get('odds', 160),
                                                            'name': sel.get('name', 'Unknown')
                                                        })
                                        
                                        if len(selections) >= 2:
                                            all_market_data.append({
                                                'event_id': int(event_id),
                                                'market_id': market.get('market_id') or market.get('id'),
                                                'market_type': market_type,
                                                'tournament': tournament['name'],
                                                'selections': selections
                                            })
                                            overall_market_type_counts[market_type] += 1
                            
                            if all_market_data:
                                print(f"✅ Found {len(all_market_data)} markets in {tournament['name']}")
                                break
        
        # Final summary
        if all_market_data:
            print(f"\n✅ Total: {len(all_market_data)} markets from {len(tournaments_used)} tournaments")
            print(f"   - Moneyline: {overall_market_type_counts['moneyline']}")
            print(f"   - Spread: {overall_market_type_counts['spread']}")
            print(f"   - Total: {overall_market_type_counts['total']}")
            return all_market_data
        else:
            print("❌ No markets found")
            return []
            
    except Exception as e:
        print(f"❌ Market seeding error: {e}")
        import traceback
        traceback.print_exc()
    
    return []

def stress_test_worker(worker_id, testers, market_data, num_bets):
    """Worker thread to place coordinated bets"""
    global should_stop, bets_placed_count, gec_events_count, target_bets, performance_data
    
    print(f"🔥 Worker {worker_id}: Starting with target {num_bets} bets")
    
    worker_start_time = time.time()
    placed_by_worker = 0
    failed_attempts = 0
    max_failed_attempts = 10
    worker_errors = 0
    successful_pairs = 0
    
    while not should_stop and placed_by_worker < num_bets:
        # Check global target
        with stats_lock:
            if bets_placed_count >= target_bets:
                break
        
        # Select random market
        if not market_data:
            print(f"❌ Worker {worker_id}: No market data available")
            break
        
        try:
            market = market_data[placed_by_worker % len(market_data)]
            selections = market['selections']
            
            if len(selections) < 2:
                print(f"⚠️ Worker {worker_id}: Market has less than 2 selections, skipping")
                failed_attempts += 1
                if failed_attempts >= max_failed_attempts:
                    print(f"❌ Worker {worker_id}: Too many failed attempts, stopping")
                    break
                continue
            
            # Get opposing selections
            outcome_4 = [s for s in selections if s.get('outcome_id') == 4]
            outcome_5 = [s for s in selections if s.get('outcome_id') == 5]
            
            if not outcome_4 or not outcome_5:
                # Try any two different selections
                if len(selections) >= 2:
                    outcome_4 = [selections[0]]
                    outcome_5 = [selections[1]]
                else:
                    print(f"⚠️ Worker {worker_id}: Cannot find opposing outcomes, skipping market")
                    failed_attempts += 1
                    if failed_attempts >= max_failed_attempts:
                        print(f"❌ Worker {worker_id}: Too many failed attempts, stopping")
                        break
                    continue
            
            # Place opposing bets
            stake = 5.0
            
            # Account 1 bets on outcome 4
            bet1 = testers[0].place_bet(outcome_4[0]['line_id'], 160, stake)
            if not bet1:
                print(f"⚠️ Worker {worker_id}: Account 1 bet failed")
                failed_attempts += 1
                worker_errors += 1
                with stats_lock:
                    performance_data['error_log'].append({
                        'timestamp': time.time(),
                        'worker_id': worker_id,
                        'account': 'Account 1',
                        'error': 'Bet placement failed'
                    })
                if failed_attempts >= max_failed_attempts:
                    print(f"❌ Worker {worker_id}: Too many failed attempts, stopping")
                    break
                time.sleep(1)
                continue
                
            time.sleep(0.5)
            
            # Account 2 bets on outcome 5
            bet2 = testers[1].place_bet(outcome_5[0]['line_id'], -160, stake)
            if not bet2:
                print(f"⚠️ Worker {worker_id}: Account 2 bet failed")
                failed_attempts += 1
                worker_errors += 1
                with stats_lock:
                    performance_data['error_log'].append({
                        'timestamp': time.time(),
                        'worker_id': worker_id,
                        'account': 'Account 2',
                        'error': 'Bet placement failed'
                    })
                if failed_attempts >= max_failed_attempts:
                    print(f"❌ Worker {worker_id}: Too many failed attempts, stopping")
                    break
                time.sleep(1)
                continue
            
            if bet1 and bet2:
                placed_by_worker += 2
                successful_pairs += 1
                failed_attempts = 0  # Reset on success
                with stats_lock:
                    performance_data['bet_timeline'].append({
                        'timestamp': time.time(),
                        'worker_id': worker_id,
                        'bets_placed': 2
                    })
            
            # Check for GEC generation periodically
            if placed_by_worker % 20 == 0 and placed_by_worker > 0:
                try:
                    for tester in testers:
                        exposure = tester.check_exposures([market['event_id']], [market['market_id']])
                        if exposure and exposure.get('data'):
                            for exp in exposure['data']:
                                if exp.get('balance', 0) > 0:
                                    with stats_lock:
                                        gec_events_count += 1
                except Exception as e:
                    print(f"⚠️ Worker {worker_id}: Exposure check error: {e}")
            
            time.sleep(0.2)  # Brief delay between bet pairs
            
        except Exception as e:
            print(f"❌ Worker {worker_id}: Error in betting loop: {e}")
            failed_attempts += 1
            worker_errors += 1
            with stats_lock:
                performance_data['error_log'].append({
                    'timestamp': time.time(),
                    'worker_id': worker_id,
                    'error': str(e)
                })
            if failed_attempts >= max_failed_attempts:
                print(f"❌ Worker {worker_id}: Too many errors, stopping")
                break
            time.sleep(1)
    
    worker_end_time = time.time()
    worker_duration = worker_end_time - worker_start_time
    
    # Save worker stats
    with stats_lock:
        performance_data['worker_stats'][f'worker_{worker_id}'] = {
            'bets_placed': placed_by_worker,
            'successful_pairs': successful_pairs,
            'errors': worker_errors,
            'duration_seconds': worker_duration,
            'bet_rate': placed_by_worker / worker_duration if worker_duration > 0 else 0
        }
    
    print(f"✅ Worker {worker_id}: Completed {placed_by_worker} bets in {worker_duration:.1f}s ({placed_by_worker/worker_duration:.1f} bets/s)")

def run_stress_test(num_workers=5, environment='sandbox', event_id=None):
    """
    Run the exposure stress test
    :param num_workers: Number of concurrent workers (default 5)
    :param environment: Environment to run on (sandbox/staging, default sandbox)
    :param event_id: Specific event ID to test (optional, bypasses tournament scanning)
    """
    global should_stop, bets_placed_count, gec_events_count, target_bets, target_gec_events, performance_data
    
    base_url = ENVIRONMENT_URLS.get(environment, ENVIRONMENT_URLS['sandbox'])
    
    print("🚀 EXPOSURE STRESS TEST: COORDINATED BETTING")
    print(f"🌍 Environment: {environment}")
    print(f"🎯 Target Bets: {target_bets:,}")
    print(f"🎯 Target GEC Events: {target_gec_events}")
    print(f"⚡ Concurrent Workers: {num_workers}")
    if event_id:
        print(f"🎯 Using specific event ID: {event_id}")
    
    # Initialize testers
    print("\n🔐 Authenticating accounts...")
    testers = [ExposureTester(account, base_url) for account in ACCOUNTS]
    
    for tester in testers:
        # MM/SP accounts only need MM login
        if not tester.mm_login():
            print(f"❌ Authentication failed for {tester.account['name']}")
            return
        balance = tester.get_balance()
        print(f"💰 {tester.account['name']} balance: ${balance}")
        
        # Store initial balance
        performance_data['account_stats'][tester.account['name']] = {
            'balance_start': balance,
            'balance_end': 0
        }
    
    # Seed market data
    print("\n🌱 Seeding market data...")
    market_data = get_market_data(testers[0], event_id=event_id)
    
    if not market_data:
        print("❌ No market data available")
        return
    
    print(f"✅ Loaded {len(market_data)} markets for testing")
    
    # Start progress monitor
    progress_thread = threading.Thread(target=progress_monitor, daemon=True)
    progress_thread.start()
    
    # Calculate bets per worker
    bets_per_worker = target_bets // num_workers
    
    # Start workers
    start_time = time.time()
    performance_data['start_time'] = start_time
    
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = []
        for i in range(num_workers):
            future = executor.submit(
                stress_test_worker,
                i+1,
                testers,
                market_data,
                bets_per_worker
            )
            futures.append(future)
        
        # Wait for all workers to complete
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Worker error: {str(e)}")
    
    elapsed = time.time() - start_time
    performance_data['end_time'] = time.time()
    
    # Get final balances
    for tester in testers:
        balance = tester.get_balance()
        if tester.account['name'] in performance_data['account_stats']:
            performance_data['account_stats'][tester.account['name']]['balance_end'] = balance
    
    # Final report
    print("")
    print("╔══════════════════════════════════════════════════════════════╗")
    print("║           🎉 EXPOSURE STRESS TEST COMPLETED 🎉             ║")
    print("╠══════════════════════════════════════════════════════════════╣")
    print(f"║ ⏰ Duration:        {elapsed/60:.2f} minutes                    ║")
    print(f"║ 🎯 Bets Placed:     {bets_placed_count:,}                          ║")
    print(f"║ 📊 GEC Events:      {gec_events_count}/{target_gec_events}                            ║")
    print(f"║ 🚀 Bet Rate:        {bets_placed_count/elapsed:.1f} bets/sec              ║")
    for tester in testers:
        balance = performance_data['account_stats'][tester.account['name']]['balance_end']
        print(f"║ 💰 {tester.account['name']} balance: ${balance:.2f}                 ║")
    print("╚══════════════════════════════════════════════════════════════╝")
    
    # Generate and save performance report
    generate_performance_report(environment, num_workers, elapsed)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Exposure stress test: coordinated betting for GEC generation')
    parser.add_argument('--workers', type=int, default=5,
                       help='Number of concurrent workers (default: 5)')
    parser.add_argument('--target', type=int, default=1000,
                       help='Target number of bets (default: 1000)')
    parser.add_argument('--gec-events', type=int, default=5,
                       help='Target number of GEC events (default: 5)')
    parser.add_argument('--env', type=str, default='sandbox', choices=['sandbox', 'staging'],
                       help='Environment to run against (default: sandbox)')
    parser.add_argument('--event-id', type=str, default=None,
                       help='Specific event ID to use for testing (bypasses tournament scanning)')
    args = parser.parse_args()
    
    # Update targets
    target_bets = args.target
    target_gec_events = args.gec_events
    
    # Set up signal handler
    signal.signal(signal.SIGINT, signal_handler)
    
    try:
        run_stress_test(num_workers=args.workers, environment=args.env, event_id=args.event_id)
    except Exception as e:
        print(f"❌ Stress test failed: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        should_stop = True
        print("✅ Stress test stopped")
