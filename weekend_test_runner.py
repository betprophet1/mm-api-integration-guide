#!/usr/bin/env python3
"""
Weekend Test Runner - Continuous Execution with Market Discovery
Runs all race condition tests against NBA & NHL markets for 24+ hours
"""

import sys
import os
import time
import json
import subprocess
import requests
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import logging
from pathlib import Path

sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))
from src import config

# Configure logging
log_file = f"weekend_runner_{int(time.time())}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

class WeekendTestRunner:
    def __init__(self, duration_hours=24, test_duration_minutes=10):
        self.duration_hours = duration_hours
        self.test_duration_minutes = test_duration_minutes
        # Get base URL based on environment
        if hasattr(config, 'BASE_URL') and isinstance(config.BASE_URL, dict):
            self.base_url = config.BASE_URL.get(config.ENVIRONMENT, config.BASE_URL)
        else:
            self.base_url = config.BASE_URL
        self.start_time = time.time()
        self.end_time = self.start_time + (duration_hours * 3600)
        self.test_results = []
        self.markets_tested = set()
        
        # Available tests
        self.tests = ['1a', '4way', 'rapid', 'patron_mm', 'cancel_bug', 'aggressive']
        
        logging.info(f"Weekend Test Runner initialized")
        logging.info(f"Target duration: {duration_hours} hours")
        logging.info(f"Test duration per market: {test_duration_minutes} minutes")
        logging.info(f"Will run until: {datetime.fromtimestamp(self.end_time)}")
    
    def get_nba_nhl_events(self) -> List[Dict]:
        """Discover available NBA & NHL events"""
        logging.info("Discovering NBA & NHL markets...")
        
        try:
            # Get MM account credentials - these are API keys
            mm1_creds = config.get_account_credentials(1, config.ENVIRONMENT)
            
            if not isinstance(mm1_creds, dict):
                logging.error("Invalid credentials format")
                return []
            
            access_key = mm1_creds.get('access_key')
            secret_key = mm1_creds.get('secret_key')
            
            if not access_key or not secret_key:
                logging.error("Missing API keys in credentials")
                return []
            
            # Use MM Partner API headers with API keys
            headers = {
                '__source': 'web',
                'X-Access-Key': access_key,
                'X-Secret-Key': secret_key
            }
            
            # Search for NBA and NHL tournaments
            tournaments_url = f"{self.base_url}/trade/public/api/v1/tournaments"
            tournaments_response = requests.get(
                tournaments_url,
                headers={'__source': 'web'}
            )
            
            if tournaments_response.status_code != 200:
                logging.error(f"Failed to get tournaments: {tournaments_response.status_code}")
                return []
            
            tournaments = tournaments_response.json().get('data', [])
            
            # Filter NBA & NHL tournaments
            target_sports = ['Basketball', 'Ice Hockey']
            target_names = ['NBA', 'NHL']
            
            nba_nhl_tournaments = [
                t for t in tournaments 
                if (t.get('sport_name') in target_sports or 
                    any(name in t.get('name', '') for name in target_names))
            ]
            
            logging.info(f"Found {len(nba_nhl_tournaments)} NBA/NHL tournaments")
            
            # Get events from these tournaments
            events = []
            for tournament in nba_nhl_tournaments[:20]:  # Limit to 20 tournaments
                tournament_id = tournament.get('id')
                events_url = f"{self.base_url}/trade/public/api/v1/events"
                
                events_response = requests.get(
                    events_url,
                    params={'tournament_id': tournament_id},
                    headers={'__source': 'web'}
                )
                
                if events_response.status_code == 200:
                    tournament_events = events_response.json().get('data', [])
                    
                    # Filter for upcoming/live events
                    now = datetime.now()
                    for event in tournament_events:
                        event_start = event.get('starts_at')
                        if event_start:
                            try:
                                start_dt = datetime.fromisoformat(event_start.replace('Z', '+00:00'))
                                # Events within next 7 days
                                if start_dt > now and start_dt < now + timedelta(days=7):
                                    event['tournament_name'] = tournament.get('name')
                                    events.append(event)
                            except:
                                pass
            
            logging.info(f"Found {len(events)} upcoming NBA/NHL events")
            return events[:50]  # Limit to 50 events
            
        except Exception as e:
            logging.error(f"Error discovering events: {e}")
            import traceback
            logging.error(traceback.format_exc())
            return []
    
    def run_test(self, test_name: str, event_id: int, event_name: str) -> Dict:
        """Run a single test against an event"""
        logging.info(f"Running test '{test_name}' on event {event_id} ({event_name})")
        
        cmd = [
            'python3',
            'test_deduce_race_conditions.py',
            '--test', test_name,
            '--duration', str(self.test_duration_minutes * 60)
        ]
        
        # Add event-id for tests that support it
        if test_name in ['cancel_bug', 'aggressive']:
            cmd.extend(['--event-id', str(event_id)])
        
        # Add RPS for cancel_bug
        if test_name == 'cancel_bug':
            cmd.extend(['--rps', '40'])
        
        result = {
            'test': test_name,
            'event_id': event_id,
            'event_name': event_name,
            'start_time': datetime.now().isoformat(),
            'status': 'running'
        }
        
        try:
            start = time.time()
            process = subprocess.run(
                cmd,
                cwd='/Users/tranlam/Documents/GitHub/mm-api-integration-guide',
                capture_output=True,
                text=True,
                timeout=(self.test_duration_minutes * 60) + 120  # Add 2min buffer
            )
            
            duration = time.time() - start
            
            result['status'] = 'completed' if process.returncode == 0 else 'failed'
            result['return_code'] = process.returncode
            result['duration_seconds'] = duration
            result['end_time'] = datetime.now().isoformat()
            
            # Extract key metrics from output
            if 'Report saved:' in process.stdout:
                report_line = [line for line in process.stdout.split('\n') if 'Report saved:' in line]
                if report_line:
                    result['report_file'] = report_line[0].split('Report saved:')[1].strip()
            
            if process.returncode != 0:
                logging.warning(f"Test '{test_name}' failed with return code {process.returncode}")
                result['error'] = process.stderr[-500:] if process.stderr else "Unknown error"
            else:
                logging.info(f"Test '{test_name}' completed successfully in {duration:.1f}s")
            
        except subprocess.TimeoutExpired:
            result['status'] = 'timeout'
            result['end_time'] = datetime.now().isoformat()
            logging.error(f"Test '{test_name}' timed out")
        except Exception as e:
            result['status'] = 'error'
            result['error'] = str(e)
            result['end_time'] = datetime.now().isoformat()
            logging.error(f"Test '{test_name}' error: {e}")
        
        return result
    
    def save_progress(self):
        """Save current progress to file"""
        progress_file = f"weekend_runner_progress_{int(self.start_time)}.json"
        
        progress = {
            'start_time': datetime.fromtimestamp(self.start_time).isoformat(),
            'current_time': datetime.now().isoformat(),
            'target_end_time': datetime.fromtimestamp(self.end_time).isoformat(),
            'duration_hours': self.duration_hours,
            'markets_tested': list(self.markets_tested),
            'total_tests_run': len(self.test_results),
            'test_results': self.test_results[-100:]  # Last 100 results
        }
        
        with open(progress_file, 'w') as f:
            json.dump(progress, f, indent=2)
        
        logging.info(f"Progress saved to {progress_file}")
    
    def run(self):
        """Main execution loop"""
        logging.info("Starting weekend test runner...")
        
        cycle = 0
        
        while time.time() < self.end_time:
            cycle += 1
            remaining_hours = (self.end_time - time.time()) / 3600
            
            logging.info(f"\n{'='*70}")
            logging.info(f"CYCLE {cycle} - {remaining_hours:.1f} hours remaining")
            logging.info(f"{'='*70}\n")
            
            # Discover markets
            events = self.get_nba_nhl_events()
            
            if not events:
                logging.warning("No events found, waiting 10 minutes before retry...")
                time.sleep(600)
                continue
            
            # Filter out already tested markets (in last 2 hours)
            recent_cutoff = time.time() - 7200  # 2 hours
            recent_markets = {
                r['event_id'] for r in self.test_results 
                if 'start_time' in r and 
                datetime.fromisoformat(r['start_time']).timestamp() > recent_cutoff
            }
            
            fresh_events = [e for e in events if e.get('id') not in recent_markets]
            
            if not fresh_events:
                logging.info("All discovered events recently tested, discovering new markets...")
                time.sleep(600)  # Wait 10 minutes
                continue
            
            # Run tests on each event
            for event in fresh_events[:5]:  # Test up to 5 events per cycle
                event_id = event.get('id')
                event_name = event.get('name', 'Unknown')
                tournament = event.get('tournament_name', 'Unknown')
                
                logging.info(f"\n--- Testing Event {event_id}: {event_name} ({tournament}) ---")
                
                # Run all tests
                for test_name in self.tests:
                    # Check time limit
                    if time.time() >= self.end_time:
                        logging.info("Time limit reached, stopping...")
                        break
                    
                    result = self.run_test(test_name, event_id, event_name)
                    self.test_results.append(result)
                    self.markets_tested.add(event_id)
                    
                    # Save progress after each test
                    self.save_progress()
                    
                    # Brief pause between tests
                    time.sleep(30)
                
                # Check time limit
                if time.time() >= self.end_time:
                    break
            
            # Save progress after each cycle
            self.save_progress()
            
            # Summary
            completed = sum(1 for r in self.test_results if r.get('status') == 'completed')
            failed = sum(1 for r in self.test_results if r.get('status') == 'failed')
            
            logging.info(f"\nCycle {cycle} Summary:")
            logging.info(f"  Total tests run: {len(self.test_results)}")
            logging.info(f"  Completed: {completed}")
            logging.info(f"  Failed: {failed}")
            logging.info(f"  Unique markets tested: {len(self.markets_tested)}")
            
            # Wait before next cycle
            if time.time() < self.end_time:
                logging.info("\nWaiting 5 minutes before next cycle...")
                time.sleep(300)
        
        # Final summary
        logging.info(f"\n{'='*70}")
        logging.info("WEEKEND TEST RUNNER COMPLETED")
        logging.info(f"{'='*70}\n")
        logging.info(f"Total runtime: {(time.time() - self.start_time) / 3600:.2f} hours")
        logging.info(f"Total tests executed: {len(self.test_results)}")
        logging.info(f"Unique markets tested: {len(self.markets_tested)}")
        
        self.save_progress()


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Weekend Test Runner')
    parser.add_argument('--hours', type=int, default=24, 
                       help='Duration in hours (default: 24)')
    parser.add_argument('--test-duration', type=int, default=10,
                       help='Test duration per market in minutes (default: 10)')
    
    args = parser.parse_args()
    
    runner = WeekendTestRunner(
        duration_hours=args.hours,
        test_duration_minutes=args.test_duration
    )
    
    try:
        runner.run()
    except KeyboardInterrupt:
        logging.info("\n\nRunner interrupted by user")
        runner.save_progress()
    except Exception as e:
        logging.error(f"\n\nRunner crashed: {e}")
        import traceback
        logging.error(traceback.format_exc())
        runner.save_progress()
