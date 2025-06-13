import argparse
import signal
import sys
import time
import threading

from . import mm_calls
from .log import logging

# Global variable to track if we should stop
should_stop = False

def signal_handler(sig, frame):
    """Handle Ctrl+C to gracefully stop the auto play loop"""
    global should_stop
    logging.info("\n🛑 Received stop signal. Stopping auto play...")
    should_stop = True
    sys.exit(0)

def balance_monitor(mm_instance):
    """Monitor balance and stop playing if it reaches 0"""
    global should_stop
    while not should_stop:
        try:
            mm_instance.get_balance()
            if mm_instance.balance <= 0:
                logging.warning("💸 Balance reached 0! Stopping auto play now Louis senpai")
                should_stop = True
                break
            time.sleep(5)  # Check balance every 5 seconds
        except Exception as e:
            logging.error(f"Error checking balance: {str(e)}")
            time.sleep(10)  # Wait longer if there's an error

if __name__ == '__main__':
    # Set up signal handler for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    
    logging.info("🎰 Starting MM API auto play and cancel loop")
    logging.info("📊 Balance will be monitored after each play")
    logging.info("⏹️  Auto play will stop when balance reaches 0")
    logging.info("🔴 Press Ctrl+C to stop the loop manually")

    try:
        mm_instance = mm_calls.MMInteractions()
        mm_instance.mm_login()
        mm_instance.get_balance()
        
        if mm_instance.balance <= 0:
            logging.error("💸 Starting balance is 0 or negative. Cannot start auto play.")
            sys.exit(1)
            
        logging.info(f"💰 Starting balance: ${mm_instance.balance:.2f}")
        mm_instance.seeding()
        # Skip channel subscription for simpler auto play
        
        # Start balance monitoring in a separate thread
        balance_thread = threading.Thread(target=balance_monitor, args=(mm_instance,), daemon=True)
        balance_thread.start()
        
        logging.info("🚀 Starting auto play and cancel loop...")
        mm_instance.auto_playing()
        
        # Keep the main thread alive
        while not should_stop:
            try:
                time.sleep(1)
            except KeyboardInterrupt:
                logging.info("\n🛑 Stopping auto play and cancel loop...")
                break
                
    except Exception as e:
        logging.error(f"❌ Error: {str(e)}")
    finally:
        should_stop = True
        logging.info("✅ Auto play and cancel loop stopped")

