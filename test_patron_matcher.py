#!/usr/bin/env python3
"""
Test script for patron matcher

Demonstrates how to use the patron matcher by manually adding bets to match.
In production, you would integrate this with WebSocket or other bet monitoring.
"""

import time
import sys
from patron_match_mm_bets import PatronAccountMatcher

def main():
    # Create patron matcher instance
    print("🚀 Initializing Patron Matcher Test...")
    matcher = PatronAccountMatcher(environment='sandbox', match_rate=1.0, match_delay=0.5)
    
    # Start the matcher
    if not matcher.start():
        print("❌ Failed to start patron matcher")
        sys.exit(1)
    
    print("\n✅ Patron matcher started successfully!")
    print("💡 In production, integrate with WebSocket to detect MM bets automatically")
    print("💡 For now, we'll simulate adding bets manually\n")
    
    # Simulate adding some MM bets to match
    # These would normally come from WebSocket bet events
    print("📝 Simulating MM bet additions...")
    
    # Example bet 1
    matcher.add_mm_bet(
        event_id=12345,
        market_id=1,
        outcome_id=1,
        odds=2.0,
        stake=10.0
    )
    print("  Added bet: Event 12345, Market 1, Outcome 1, Odds 2.0, Stake $10")
    
    # Example bet 2
    time.sleep(1)
    matcher.add_mm_bet(
        event_id=12345,
        market_id=1,
        outcome_id=2,
        odds=1.8,
        stake=15.0
    )
    print("  Added bet: Event 12345, Market 1, Outcome 2, Odds 1.8, Stake $15")
    
    # Example bet 3
    time.sleep(1)
    matcher.add_mm_bet(
        event_id=67890,
        market_id=2,
        outcome_id=3,
        odds=2.5,
        stake=20.0
    )
    print("  Added bet: Event 67890, Market 2, Outcome 3, Odds 2.5, Stake $20")
    
    # Let the matcher process the bets
    print("\n⏳ Waiting for patron matcher to process bets (10 seconds)...")
    time.sleep(10)
    
    # Print statistics
    print("\n" + "="*70)
    print("📊 FINAL STATISTICS")
    print("="*70)
    print(f"  Match attempts: {matcher.session_stats['match_attempts']}")
    print(f"  Successful matches: {matcher.session_stats['successful_matches']}")
    print(f"  Failed matches: {matcher.session_stats['failed_matches']}")
    print(f"  Current balance: ${matcher.session_stats['balance']:.2f}")
    print("="*70)
    
    # Stop the matcher
    print("\n🛑 Stopping patron matcher...")
    matcher.stop()
    print("✅ Test complete!")

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n🛑 Test interrupted by user")
        sys.exit(0)
