#!/usr/bin/env python3
import os
import sys
import time
import json
import subprocess
import requests
from urllib.parse import urljoin
from datetime import datetime

# Local imports
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))
from src import config

BASE_URL = config.BASE_URL
URL = config.URL

LOG_FILE = f"run_all_events_{int(time.time())}.log"

def log(msg):
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    line = f"[{ts}] {msg}"
    print(line)
    with open(LOG_FILE, 'a') as f:
        f.write(line + "\n")


def mm_login():
    creds = config.get_account_credentials(1, config.ENVIRONMENT)
    login_url = urljoin(BASE_URL, URL['mm_login'])
    resp = requests.post(login_url, json={'access_key': creds['access_key'], 'secret_key': creds['secret_key']})
    if resp.status_code != 200:
        raise RuntimeError(f"MM login failed: {resp.status_code} {resp.text}")
    data = resp.json().get('data', {})
    return data.get('access_token')


def retry_request(url, max_retries=3, **kwargs):
    for attempt in range(max_retries):
        try:
            r = requests.get(url, timeout=30, **kwargs)
            return r
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            if attempt == max_retries - 1:
                raise
            wait = 2 ** attempt
            log(f"Request failed (attempt {attempt+1}/{max_retries}), retrying in {wait}s")
            time.sleep(wait)


def list_all_events(access_token, limit_per_tournament=50, target_tournament='NHL'):
    headers = {'Authorization': f'Bearer {access_token}'}
    t_url = urljoin(BASE_URL, URL['mm_tournaments'])
    r = retry_request(t_url, headers=headers)
    r.raise_for_status()
    tournaments = r.json().get('data', {}).get('tournaments', [])
    log(f"Found {len(tournaments)} tournaments")

    # Filter to target tournament
    if target_tournament:
        tournaments = [t for t in tournaments if target_tournament.lower() in t.get('name', '').lower()]
        log(f"Filtered to {len(tournaments)} tournaments matching '{target_tournament}'")

    event_url = urljoin(BASE_URL, URL['mm_events'])
    events = []
    for idx, t in enumerate(tournaments, 1):
        log(f"Fetching events for tournament {idx}/{len(tournaments)}: {t.get('name', t['id'])}")
        try:
            tr = retry_request(event_url, params={'tournament_id': t['id']}, headers=headers)
            if tr.status_code != 200:
                log(f"  Skipping: HTTP {tr.status_code}")
                continue
            sport_events = tr.json().get('data', {}).get('sport_events', [])
            for e in sport_events[:limit_per_tournament]:
                events.append({'event_id': e['event_id'], 'name': e.get('name', 'Unknown'), 'tournament': t['name']})
            log(f"  Found {len(sport_events[:limit_per_tournament])} events")
        except Exception as ex:
            log(f"  Error: {ex}")
            continue
        time.sleep(0.5)
    log(f"Collected {len(events)} events total")
    return events


def run_tests_for_event(event_id, duration_sec=600, rps=40):
    # For tests that accept --event-id directly
    per_event_tests = [
        ['cancel_bug', '--rps', str(rps)],
        ['aggressive']
    ]

    # Tests that rely on get_available_market; use env var TARGET_EVENT_ID
    generic_tests = ['1a', '4way', 'rapid', 'patron_mm']

    env = os.environ.copy()
    env['TARGET_EVENT_ID'] = str(event_id)

    for test_spec in per_event_tests:
        test_name = test_spec[0]
        extra_args = test_spec[1:] if len(test_spec) > 1 else []
        cmd = ['python3', 'test_deduce_race_conditions.py', '--test', test_name, '--duration', str(duration_sec), '--event-id', str(event_id), *extra_args]
        log(f"Running {test_name} for event {event_id}...")
        p = subprocess.run(cmd, cwd=os.path.dirname(__file__), capture_output=True, text=True, env=env)
        with open(LOG_FILE, 'a') as f:
            f.write(p.stdout)
            f.write(p.stderr)
        log(f"{test_name} exit code: {p.returncode}")
        time.sleep(5)

    for test_name in generic_tests:
        cmd = ['python3', 'test_deduce_race_conditions.py', '--test', test_name, '--duration', str(duration_sec)]
        log(f"Running {test_name} for event {event_id} (via TARGET_EVENT_ID)...")
        p = subprocess.run(cmd, cwd=os.path.dirname(__file__), capture_output=True, text=True, env=env)
        with open(LOG_FILE, 'a') as f:
            f.write(p.stdout)
            f.write(p.stderr)
        log(f"{test_name} exit code: {p.returncode}")
        time.sleep(5)


def generate_summary_report(results, total_duration, args):
    """Generate comprehensive summary report"""
    log("\n" + "="*80)
    log("FINAL SUMMARY REPORT")
    log("="*80)
    
    # Overall stats
    total_events = len(results)
    completed = sum(1 for r in results if r['status'] == 'completed')
    errors = sum(1 for r in results if r['status'] == 'error')
    
    hours = int(total_duration // 3600)
    minutes = int((total_duration % 3600) // 60)
    seconds = int(total_duration % 60)
    
    log(f"\nExecution Summary:")
    log(f"  Total Runtime:     {hours:02d}h {minutes:02d}m {seconds:02d}s")
    log(f"  Events Processed:  {total_events}")
    log(f"  Completed:         {completed}")
    log(f"  Errors:            {errors}")
    log(f"  Test Duration:     {args.duration}s per test")
    log(f"  RPS (cancel_bug):  {args.rps}")
    
    # Per-event breakdown
    log(f"\nPer-Event Results:")
    for idx, r in enumerate(results, 1):
        status_icon = "✅" if r['status'] == 'completed' else "❌"
        duration_str = f"{r.get('duration', 0):.0f}s" if 'duration' in r else "N/A"
        log(f"  [{idx}] {status_icon} {r['event_name']} ({r['tournament']}) - {duration_str}")
        if r['status'] == 'error':
            log(f"      Error: {r.get('error', 'Unknown')}")
    
    # Count test reports generated
    import glob
    report_pattern = "race_test_*.json"
    report_files = glob.glob(report_pattern)
    log(f"\nGenerated Reports:")
    log(f"  Test reports found: {len(report_files)}")
    log(f"  Log file: {LOG_FILE}")
    
    # Save JSON summary
    summary_file = f"run_summary_{int(time.time())}.json"
    summary_data = {
        'execution_time': total_duration,
        'events_processed': total_events,
        'completed': completed,
        'errors': errors,
        'config': {
            'duration_per_test': args.duration,
            'rps': args.rps,
            'limit_events': args.limit_events
        },
        'results': results,
        'timestamp': datetime.now().isoformat()
    }
    
    with open(summary_file, 'w') as f:
        json.dump(summary_data, f, indent=2)
    
    log(f"\nSummary saved to: {summary_file}")
    log("="*80 + "\n")


def main():
    import argparse
    parser = argparse.ArgumentParser(description='Run all tests against every event in all tournaments')
    parser.add_argument('--duration', type=int, default=600, help='Per-test duration in seconds (default 600)')
    parser.add_argument('--limit-events', type=int, default=0, help='Limit total events (0 = no limit)')
    parser.add_argument('--rps', type=int, default=40, help='RPS for cancel_bug test (default 40)')
    args = parser.parse_args()

    start_time = time.time()
    token = mm_login()
    events = list_all_events(token)
    if args.limit_events and args.limit_events > 0:
        events = events[:args.limit_events]

    # Track results
    results = []
    
    for idx, e in enumerate(events, 1):
        log(f"=== [{idx}/{len(events)}] Event {e['event_id']}: {e['name']} ({e['tournament']}) ===")
        event_result = {
            'event_id': e['event_id'],
            'event_name': e['name'],
            'tournament': e['tournament'],
            'tests': {}
        }
        
        try:
            test_start = time.time()
            run_tests_for_event(e['event_id'], duration_sec=args.duration, rps=args.rps)
            event_result['duration'] = time.time() - test_start
            event_result['status'] = 'completed'
        except Exception as ex:
            log(f"Error running tests for event {e['event_id']}: {ex}")
            event_result['status'] = 'error'
            event_result['error'] = str(ex)
        
        results.append(event_result)
        time.sleep(10)

    total_duration = time.time() - start_time
    log("All events completed")
    
    # Generate summary report
    generate_summary_report(results, total_duration, args)

if __name__ == '__main__':
    main()
