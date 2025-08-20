#!/usr/bin/env python3

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.mm_calls import MMInteractions
from src.exposure_test import ExposureTest
from src.log import logging

def main():
    logging.info("🚀 Starting Exposure Credit Testing")
    
    # Initialize MM interactions
    mm = MMInteractions()
    
    try:
        # Login
        logging.info("🔐 Logging in...")
        mm.mm_login()
        
        # Seed tournaments and events
        logging.info("🌱 Seeding tournaments and events...")
        mm.seeding()
        
        # Get current balance
        logging.info("💰 Getting current balance...")
        mm.get_balance()
        
        # Initialize exposure testing
        exposure_test = ExposureTest(mm)
        
        # Test exposure balance retrieval
        logging.info("🎯 Testing exposure balance retrieval...")
        balance = exposure_test.get_exposure_balance()
        
        # Test exposure credits retrieval
        logging.info("🎯 Testing exposure credits retrieval...")
        credits = exposure_test.get_exposure_credits()
        
        # Test sync functionality
        logging.info("🔄 Testing exposure sync...")
        sync_result = exposure_test.sync_exposure_updates()
        
        # If we have events available, run a simple bet to generate exposure
        if mm.sport_events:
            logging.info("🎲 Attempting to place a test bet to generate exposure credits...")
            
            # Place a small test bet
            result = exposure_test.place_test_bet(10, "+110", "outcome1")
            
            if result:
                logging.info("✅ Test bet placed successfully!")
                
                # Wait a moment and check for exposure credits
                import time
                time.sleep(2)
                
                logging.info("🔍 Checking for generated exposure credits...")
                new_credits = exposure_test.get_exposure_credits()
                
                if new_credits:
                    logging.info("🎉 Exposure credits detected!")
                    logging.info(f"Credits: {new_credits}")
                else:
                    logging.info("ℹ️  No exposure credits generated yet (may need more time)")
            else:
                logging.error("❌ Failed to place test bet")
        
        logging.info("✅ Exposure testing completed successfully!")
        
    except Exception as e:
        logging.error(f"❌ Error during exposure testing: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
