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
            time.sleep(2)  # AGGRESSIVE: Check balance every 2 seconds
        except Exception as e:
            logging.error(f"Error checking balance: {str(e)}")
            time.sleep(10)  # Wait longer if there's an error

def session_reporter(mm_instance):
    """Print beautiful session reports every 30 seconds"""
    global should_stop
    while not should_stop:
        try:
            time.sleep(30)  # Report every 30 seconds
            if not should_stop:
                mm_instance.print_session_report()
        except Exception as e:
            logging.error(f"Error generating session report: {str(e)}")
            time.sleep(30)

if __name__ == '__main__':
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description='MM API auto play and cancel loop with event targeting')
    parser.add_argument('--event', type=str, default='ALL',
                        help='Target MLB event name (partial or exact match) or "ALL" for all MLB events. Default: "ALL"')
    args = parser.parse_args()

    # Set up signal handler for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    
    logging.info("🔥🎰 SUPER AGGRESSIVE MM API auto play and cancel loop")
    if args.event.upper() == 'ALL':
        logging.info("🔥🌎 TARGETING: ALL MLB EVENTS (Maximum Coverage!)")
    else:
        logging.info(f"🔥🎯 TARGETING EVENT: {args.event}")
    logging.info("🔥🎲 ALL MARKETS: Now betting on ALL market types (moneyline, spread, totals, etc.)")
    logging.info("🔥📊 AGGRESSIVE: Balance monitored every 2 seconds")
    logging.info("🔥⚡ IMMEDIATE CANCEL MODE: Each single bet followed by single cancel, each batch bet followed by batch cancel!")
    logging.info("🔥🔴 Press Ctrl+C to stop the super aggressive loop")

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
        
        # Start session reporting in a separate thread
        reporter_thread = threading.Thread(target=session_reporter, args=(mm_instance,), daemon=True)
        reporter_thread.start()
        
        logging.info("🚀 Starting auto play and cancel loop...")
        logging.info("📊 Session reports will be generated every 30 seconds")
        mm_instance.auto_playing(target_event=args.event)
        
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

