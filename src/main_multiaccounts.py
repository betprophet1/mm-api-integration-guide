import threading
import time
from mm_calls import MMInteractions
import config

def run_account_autoplay(account_num):
    """Run autoplay for a specific account"""
    print(f"Starting autoplay for Account {account_num}")
    
    try:
        # Get credentials for this account
        credentials = config.get_account_credentials(account_num)
        
        # Temporarily override the config for this account
        original_keys = config.MM_KEYS
        original_tournaments = config.TOURNAMENTS_INTERESTED
        
        config.MM_KEYS = {
            'access_key': credentials['access_key'],
            'secret_key': credentials['secret_key']
        }
        config.TOURNAMENTS_INTERESTED = credentials['tournaments']
        
        # Create MMInteractions instance 
        mm = MMInteractions()
        
        # Login
        print(f"Account {account_num}: Logging in...")
        mm.mm_login()
        
        # Get balance
        print(f"Account {account_num}: Getting balance...")
        mm.get_balance()
        
        # Seed random number generator
        print(f"Account {account_num}: Seeding...")
        mm.seeding()
        
        # Subscribe to tournaments
        print(f"Account {account_num}: Subscribing to tournaments...")
        mm.subscribe()
        
        # Start autoplay
        print(f"Account {account_num}: Starting autoplay...")
        mm.auto_playing()
        
        # Keep alive
        print(f"Account {account_num}: Keeping connection alive...")
        mm.keep_alive()
        
    except Exception as e:
        print(f"Error in Account {account_num}: {e}")

def main():
    """Main function to run both accounts simultaneously"""
    print("Starting multi-account autoplay for MLB tournament...")
    
    # Create threads for each account
    account1_thread = threading.Thread(target=run_account_autoplay, args=(1,), name="Account1")
    account2_thread = threading.Thread(target=run_account_autoplay, args=(2,), name="Account2")
    
    # Start both threads
    account1_thread.start()
    account2_thread.start()
    
    try:
        # Wait for both threads to complete (they should run indefinitely)
        account1_thread.join()
        account2_thread.join()
    except KeyboardInterrupt:
        print("\nReceived interrupt signal. Shutting down...")
        
if __name__ == "__main__":
    main()

