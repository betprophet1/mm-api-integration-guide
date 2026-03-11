#!/usr/bin/env python3
"""
Integrated MM and Patron Account Testing Script

Runs both MM account betting and patron account matching simultaneously:
- MM accounts: Place random bets on all markets
- Patron account: Monitors MM activity and matches bets randomly

Usage:
    python run_mm_and_patron.py --env sandbox --accounts 1,2 --match-rate 0.7
    
This requires:
    - MM accounts configured in user_info.json and user_info_account2.json
    - Patron account configured in src/user_info_patron.json
"""

import argparse
import subprocess
import signal
import sys
import time
import threading
import os


class IntegratedTestRunner:
    """Runs MM and Patron scripts together"""
    
    def __init__(self, env='sandbox', accounts='1,2', match_rate=0.7, match_delay=0.5):
        self.env = env
        self.accounts = accounts
        self.match_rate = match_rate
        self.match_delay = match_delay
        self.processes = []
        self.is_running = True
    
    def start_mm_autoplay(self):
        """Start MM autoplay cancel script"""
        print("\n" + "="*70)
        print("🎰 STARTING MM AUTOPLAY CANCEL")
        print("="*70)
        
        cmd = [
            'python3',
            'multi_account_autoplay_cancel.py',
            '--env', self.env,
            '--accounts', self.accounts,
            '--event', 'ALL'
        ]
        
        try:
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
            self.processes.append(('MM Autoplay', process))
            print(f"✅ Started MM autoplay with command: {' '.join(cmd)}")
        except Exception as e:
            print(f"❌ Failed to start MM autoplay: {e}")
    
    def start_patron_matcher(self):
        """Start Patron matcher script"""
        print("\n" + "="*70)
        print("💰 STARTING PATRON ACCOUNT BET MATCHER")
        print("="*70)
        
        cmd = [
            'python3',
            'patron_match_mm_bets.py',
            '--env', self.env,
            '--match-rate', str(self.match_rate),
            '--match-delay', str(self.match_delay)
        ]
        
        try:
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
            self.processes.append(('Patron Matcher', process))
            print(f"✅ Started patron matcher with command: {' '.join(cmd)}")
        except Exception as e:
            print(f"❌ Failed to start patron matcher: {e}")
    
    def monitor_processes(self):
        """Monitor running processes and print output"""
        print("\n" + "="*70)
        print("📊 INTEGRATED TEST RUNNING")
        print("="*70)
        print("Monitoring MM autoplay and patron matcher...")
        print("Press Ctrl+C to stop all processes\n")
        
        while self.is_running:
            for name, process in self.processes:
                if process.poll() is not None:
                    print(f"\n⚠️  {name} process has terminated (exit code: {process.returncode})")
            
            time.sleep(1)
    
    def run(self):
        """Run the integrated test"""
        print("\n🚀 INTEGRATED MM + PATRON TESTING")
        print("="*70)
        print(f"Environment: {self.env.upper()}")
        print(f"MM Accounts: {self.accounts}")
        print(f"Match Rate: {self.match_rate:.0%}")
        print(f"Match Delay: {self.match_delay}s")
        print("="*70)
        
        # Start both scripts
        self.start_mm_autoplay()
        time.sleep(2)  # Give MM a head start
        self.start_patron_matcher()
        
        # Monitor processes
        try:
            self.monitor_processes()
        except KeyboardInterrupt:
            print("\n\n🛑 Stopping all processes...")
            self.stop()
    
    def stop(self):
        """Stop all processes"""
        self.is_running = False
        
        for name, process in self.processes:
            try:
                print(f"🔄 Stopping {name}...")
                process.terminate()
                
                # Wait for graceful shutdown
                try:
                    process.wait(timeout=5)
                    print(f"✅ {name} stopped gracefully")
                except subprocess.TimeoutExpired:
                    print(f"⚠️  {name} didn't stop gracefully, killing...")
                    process.kill()
                    process.wait()
                    print(f"✅ {name} killed")
            except Exception as e:
                print(f"❌ Error stopping {name}: {e}")


def main():
    parser = argparse.ArgumentParser(
        description='Run integrated MM and patron account testing'
    )
    parser.add_argument(
        '--env',
        type=str,
        default='sandbox',
        choices=['sandbox', 'staging'],
        help='Environment (default: sandbox)'
    )
    parser.add_argument(
        '--accounts',
        type=str,
        default='1,2',
        help='MM accounts to run (default: 1,2)'
    )
    parser.add_argument(
        '--match-rate',
        type=float,
        default=0.7,
        help='Patron match rate (0.0-1.0, default: 0.7)'
    )
    parser.add_argument(
        '--match-delay',
        type=float,
        default=0.5,
        help='Patron match delay in seconds (default: 0.5)'
    )
    
    args = parser.parse_args()
    
    # Validate arguments
    if not 0.0 <= args.match_rate <= 1.0:
        print("❌ Match rate must be between 0.0 and 1.0")
        sys.exit(1)
    
    # Create and run integrated tester
    runner = IntegratedTestRunner(
        env=args.env,
        accounts=args.accounts,
        match_rate=args.match_rate,
        match_delay=args.match_delay
    )
    
    try:
        runner.run()
    except KeyboardInterrupt:
        print("\n🛑 Interrupted")
        runner.stop()
    except Exception as e:
        print(f"❌ Error: {e}")
        runner.stop()
        sys.exit(1)


if __name__ == '__main__':
    main()
