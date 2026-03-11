#!/usr/bin/env python3
"""
Debug script to check MM1 balance, exposure, and matched bets
"""

import time
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from deduce_tests import DeduceTestFramework, Colors
from src import config

def main():
    print(f"\n{Colors.BOLD}{Colors.CYAN}{'='*70}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.CYAN}MM1 Balance & Exposure Debug Tool{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.CYAN}{'='*70}{Colors.RESET}\n")
    
    # Initialize framework
    framework = DeduceTestFramework(environment=config.ENVIRONMENT)
    
    # Login MM1
    print(f"{Colors.CYAN}Logging in MM1 (deduce account)...{Colors.RESET}")
    mm1_creds = config.get_account_credentials(1, config.ENVIRONMENT)
    framework.login_account('mm1', mm1_creds, account_type='mm')
    print(f"{Colors.GREEN}✅ MM1 logged in{Colors.RESET}\n")
    
    # Check balance multiple times
    print(f"{Colors.BOLD}Checking balance (will check 5 times with 3s intervals):{Colors.RESET}\n")
    
    for i in range(5):
        print(f"{Colors.YELLOW}Check #{i+1}:{Colors.RESET}")
        
        # Get balance
        try:
            balance_data = framework.get_balance('mm1')
            print(f"  Balance: ${balance_data.get('balance', 0):,.2f}")
            print(f"  Full response: {balance_data}")
        except Exception as e:
            print(f"  {Colors.RED}Error getting balance: {e}{Colors.RESET}")
        
        # Get exposure
        try:
            exposure_data = framework.get_exposure('mm1')
            if exposure_data:
                print(f"  Exposure: {exposure_data}")
            else:
                print(f"  {Colors.YELLOW}No exposure data available{Colors.RESET}")
        except Exception as e:
            print(f"  {Colors.YELLOW}Error getting exposure: {e}{Colors.RESET}")
        
        # Try to get matched bets
        try:
            from datetime import datetime
            date_from = datetime.now().strftime('%Y-%m-%d')
            matched_bets = framework.get_matched_bets('mm1', limit=50, date_from=date_from)
            print(f"  Matched bets: {len(matched_bets)} found")
            if matched_bets:
                total_stake = sum(bet.get('stake', 0) for bet in matched_bets)
                print(f"  Total matched stake: ${total_stake:.2f}")
                print(f"  Recent bets: {matched_bets[:3]}")  # Show first 3
        except Exception as e:
            print(f"  {Colors.YELLOW}Error getting matched bets: {e}{Colors.RESET}")
        
        print()
        
        if i < 4:
            time.sleep(3)
    
    print(f"{Colors.GREEN}Debug check complete!{Colors.RESET}\n")

if __name__ == '__main__':
    main()
