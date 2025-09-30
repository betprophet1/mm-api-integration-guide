#!/usr/bin/env python3
"""
Multi-Account Direct WebSocket Listener

Simply configure the JWT tokens in this script and run.
No interactive input required.
"""

import requests
import json
import logging
import time
import pysher
import threading
from datetime import datetime
from typing import List, Dict, Any, Callable
import base64

# Configure logging to show everything
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

class AccountWebSocketListener:
    """WebSocket listener for a single account"""
    
    def __init__(self, account_name: str, user_jwt_token: str):
        self.account_name = account_name
        self.base_url = 'https://api-ss-sandbox.betprophet.co'
        self.user_jwt_token = user_jwt_token
        self.pusher = None
        self.pusher_connected = False
        self.socket_id = None
        self.events_received = []
        self.is_listening = False
        self.user_id = None
        self.sp_channel = None
        
        # Extract user info from JWT
        self._extract_user_info()
        
        # User's Pusher configuration
        self.user_pusher_config = {
            "key": "c20fa36fbc3c3c308ffa",
            "cluster": "mt1"
        }
        
    def _extract_user_info(self):
        """Extract user information from JWT token"""
        try:
            # Split the token
            header, payload, signature = self.user_jwt_token.split('.')
            
            # Add padding if needed
            payload += '=' * (4 - len(payload) % 4)
            
            # Decode payload
            decoded = base64.urlsafe_b64decode(payload)
            payload_data = json.loads(decoded)
            
            # Extract user ID and create SP channel
            self.user_id = payload_data.get('userId')
            if self.user_id:
                # Remove dashes for SP channel
                user_id_no_dashes = self.user_id.replace('-', '')
                self.sp_channel = f"private-service=4-device_type=5-user={user_id_no_dashes}"
            
            # Check expiration
            exp = payload_data.get('exp')
            if exp:
                exp_date = datetime.fromtimestamp(exp)
                current_time = datetime.now()
                print(f"[{self.account_name}] Token expires: {exp_date}")
                print(f"[{self.account_name}] Token expired: {current_time > exp_date}")
                
        except Exception as e:
            print(f"[{self.account_name}] Error extracting user info: {e}")
            self.user_id = "unknown"
            self.sp_channel = "unknown"
        
    def setup_websocket(self) -> bool:
        """Setup WebSocket connection"""
        print(f"[{self.account_name}] STARTING WEBSOCKET LISTENER")
        print(f"[{self.account_name}] User ID: {self.user_id}")
        print(f"[{self.account_name}] SP Channel: {self.sp_channel}")
        
        try:
            # Create Pusher client
            auth_endpoint_url = f"{self.base_url}/parlay/pusher/auth"
            auth_headers = {
                "Authorization": f"Bearer {self.user_jwt_token}",
                "Content-Type": "application/x-www-form-urlencoded"
            }
            
            self.pusher = pysher.Pusher(
                key=self.user_pusher_config['key'],
                cluster=self.user_pusher_config['cluster'],
                auth_endpoint=auth_endpoint_url,
                auth_endpoint_headers=auth_headers
            )
            
            # Setup handlers
            self.pusher.connection.bind('pusher:connection_established', self._on_connect)
            self.pusher.connection.bind('pusher:connection_failed', self._on_failed)
            self.pusher.connection.bind('pusher:error', self._on_error)
            
            # Connect
            print(f"[{self.account_name}] Connecting to Pusher...")
            self.pusher.connect()
            
            # Wait for connection
            max_wait = 15
            wait_time = 0
            while not self.pusher_connected and wait_time < max_wait:
                time.sleep(0.5)
                wait_time += 0.5
                
            return self.pusher_connected
            
        except Exception as e:
            print(f"[{self.account_name}] ERROR: {e}")
            return False
    
    def _on_connect(self, data):
        """Handle connection establishment"""
        print(f"[{self.account_name}] PUSHER CONNECTION ESTABLISHED")
        
        connection_data = json.loads(data)
        self.socket_id = connection_data['socket_id']
        print(f"[{self.account_name}] Socket ID: {self.socket_id}")
        
        # Subscribe to SP channel
        if self._subscribe_to_sp_channel():
            self.pusher_connected = True
            print(f"[{self.account_name}] CONNECTION READY")
        else:
            print(f"[{self.account_name}] CONNECTION FAILED - subscription error")
            self.pusher_connected = False
    
    def _subscribe_to_sp_channel(self) -> bool:
        """Subscribe to SP channel"""
        print(f"[{self.account_name}] SUBSCRIBING TO SP CHANNEL")
        
        try:
            # Subscribe to the SP channel
            channel = self.pusher.subscribe(self.sp_channel)
            print(f"[{self.account_name}] Subscription request sent")
            
            # Bind to all events
            events_to_bind = [
                'parlay.settled',
                'order.settled', 
                'order.finalized',
                'price.confirm.new',
                'refund.processed',
                'health_check'
            ]
            
            for event_name in events_to_bind:
                channel.bind(event_name, self._handle_raw_event)
                print(f"[{self.account_name}] Bound to event: {event_name}")
            
            print(f"[{self.account_name}] SP CHANNEL SUBSCRIPTION COMPLETE")
            return True
            
        except Exception as e:
            print(f"[{self.account_name}] SUBSCRIPTION ERROR: {e}")
            return False
    
    def _on_failed(self, data):
        """Handle connection failure"""
        print(f"[{self.account_name}] CONNECTION FAILED")
        print(f"[{self.account_name}] Failure data: {data}")
        self.pusher_connected = False
    
    def _on_error(self, data):
        """Handle connection errors"""
        print(f"[{self.account_name}] PUSHER ERROR")
        print(f"[{self.account_name}] Error data: {data}")
        
        if isinstance(data, dict):
            error_code = data.get('code')
            error_message = data.get('message')
            print(f"[{self.account_name}] Error code: {error_code}")
            print(f"[{self.account_name}] Error message: {error_message}")
    
    def _handle_raw_event(self, data: Any, *args, **kwargs):
        """Handle all events - show raw data"""
        timestamp = datetime.now()
        
        print("=" * 80)
        print(f"🎯 [{self.account_name}] EVENT RECEIVED! {timestamp}")
        print(f"[{self.account_name}] Raw data: {data}")
        
        # Try to parse if it's JSON
        try:
            if isinstance(data, str):
                parsed_data = json.loads(data)
                print(f"[{self.account_name}] Parsed JSON:")
                print(json.dumps(parsed_data, indent=2))
        except:
            print(f"[{self.account_name}] Data is not JSON")
        
        print("=" * 80)
        
        # Store the event
        event_info = {
            "timestamp": str(timestamp),
            "account": self.account_name,
            "data": data,
            "args": args,
            "kwargs": kwargs
        }
        self.events_received.append(event_info)
    
    def start_listening_thread(self):
        """Start listening in a separate thread"""
        self.is_listening = True
        
        if not self.setup_websocket():
            print(f"[{self.account_name}] SETUP FAILED")
            return False
        
        print(f"[{self.account_name}] LISTENING FOR RAW MESSAGES...")
        
        try:
            while self.pusher_connected and self.is_listening:
                time.sleep(1)
                
        except Exception as e:
            print(f"[{self.account_name}] ERROR: {e}")
        finally:
            self.stop_listening()
        
        return True
    
    def stop_listening(self):
        """Stop listening"""
        self.is_listening = False
        if self.pusher:
            print(f"[{self.account_name}] DISCONNECTING...")
            try:
                self.pusher.disconnect()
            except Exception as e:
                print(f"[{self.account_name}] DISCONNECT ERROR: {e}")
    
    def get_events_count(self):
        """Get number of events received"""
        return len(self.events_received)


def main():
    """Main entry point"""
    
    # ==========================================
    # CONFIGURE YOUR JWT TOKENS HERE
    # ==========================================
    
    # Account 1 Configuration
    ACCOUNT1_NAME = "LongPartner"
    ACCOUNT1_JWT = "eyJhbGciOiJIUzI1NiIsImtpZCI6InNpbTIifQ.eyJhY2NvdW50VHlwZSI6MCwiZXhwIjoxNzU5MjE1MTQwLCJleHRyYU9ubGluZVRpbWUiOjAsImlzT3RwRXhwaXJlZCI6ZmFsc2UsImlzU3VzcGVuZGVkIjpmYWxzZSwiaXNzIjoiaHR0cDovL21vdGhlcnNoaXAubW90aGVyc2hpcC1zYW5kYm94IiwianRpIjoiNGEzMGQ2ZjgtNTNkYi00ZDI0LThmYzItMDc1NmE3YzZlNmQ2Iiwia2JhQW5zd2Vyc0luZm8iOiJOL0EiLCJreWNJbmZvIjoiU3VjY2VzcyIsInBlbmRpbmdCeUFkbWluIjpmYWxzZSwicHVzaGVySW5mbyI6eyJhdXRoQ2hhbm5lbCI6InVzZXIuYXV0aGVudGljYXRpb24uNGEzMGQ2ZjgtNTNkYi00ZDI0LThmYzItMDc1NmE3YzZlNmQ2IiwiYmFsYW5jZVVwZGF0ZWRFdmVudCI6IndhbGxldC5iYWxhbmNlLnVwZGF0ZWQiLCJjbHVzdGVyIjoibXQxIiwiaWQiOiJjMjBmYTM2ZmJjM2MzYzMwOGZmYSIsImluZm9DaGFubmVsIjoidXNlci5pbmZvcm1hdGlvbi45ZWMwNzFiNy1lMTA2LTQ4MzgtODlmMC02ZjY2ZmE4Mjc2ODEiLCJpbmZvcm1hdGlvblVwZGF0ZWRFdmVudCI6InVzZXIuaW5mb3JtYXRpb24udXBkYXRlZCIsImtiYUNoYWxsZW5nZVF1ZXN0aW9uc0V2ZW50IjoidXNlci5rYmEuY2hhbGxlbmdlIiwic2Vzc2lvblRlcm1pbmF0ZWRFdmVudCI6InNlc3Npb24udGVybWluYXRlZCJ9LCJyZWdpb24iOiJOWSIsInN1YiI6ImxvbmcucGFydG5lckB5b3BtYWlsLmNvbSIsInVzZXJJZCI6IjllYzA3MWI3LWUxMDYtNDgzOC04OWYwLTZmNjZmYTgyNzY4MSJ9.w_qtb_M4h3_1242_REc0CR_ivOj60bp4zBlKxZQrdro"
    
    # Account 2 Configuration  
    ACCOUNT2_NAME = "HoangThai"
    ACCOUNT2_JWT = "eyJhbGciOiJIUzI1NiIsImtpZCI6InNpbTIifQ.eyJhY2NvdW50VHlwZSI6MCwiZXhwIjoxNzU5MjE1MjEwLCJleHRyYU9ubGluZVRpbWUiOjAsImlzT3RwRXhwaXJlZCI6ZmFsc2UsImlzU3VzcGVuZGVkIjpmYWxzZSwiaXNzIjoiaHR0cDovL21vdGhlcnNoaXAubW90aGVyc2hpcC1zYW5kYm94IiwianRpIjoiYTczYzNkM2YtOWQ2OS00ZTY0LTllZGUtZDJhYzU5MjYyNzA4Iiwia2JhQW5zd2Vyc0luZm8iOiJOL0EiLCJreWNJbmZvIjoiU3VjY2VzcyIsInBlbmRpbmdCeUFkbWluIjpmYWxzZSwicHVzaGVySW5mbyI6eyJhdXRoQ2hhbm5lbCI6InVzZXIuYXV0aGVudGljYXRpb24uYTczYzNkM2YtOWQ2OS00ZTY0LTllZGUtZDJhYzU5MjYyNzA4IiwiYmFsYW5jZVVwZGF0ZWRFdmVudCI6IndhbGxldC5iYWxhbmNlLnVwZGF0ZWQiLCJjbHVzdGVyIjoibXQxIiwiaWQiOiJjMjBmYTM2ZmJjM2MzYzMwOGZmYSIsImluZm9DaGFubmVsIjoidXNlci5pbmZvcm1hdGlvbi5hZjVhODQ5OS03Y2I4LTQ1ZDItODU2Ni0yOWY5M2QyMzRhZTgiLCJpbmZvcm1hdGlvblVwZGF0ZWRFdmVudCI6InVzZXIuaW5mb3JtYXRpb24udXBkYXRlZCIsImtiYUNoYWxsZW5nZVF1ZXN0aW9uc0V2ZW50IjoidXNlci5rYmEuY2hhbGxlbmdlIiwic2Vzc2lvblRlcm1pbmF0ZWRFdmVudCI6InNlc3Npb24udGVybWluYXRlZCJ9LCJyZWdpb24iOiJOWSIsInN1YiI6ImhvYW5nLnRoYWkrcGFydG5lcnNiMDFAYmV0cHJvcGhldC5jbyIsInVzZXJJZCI6ImFmNWE4NDk5LTdjYjgtNDVkMi04NTY2LTI5ZjkzZDIzNGFlOCJ9.jEgg183J-gOj_BEi2jwUInhdmG1Kz_AkG_MVW7Vi4-4"
    
    # ==========================================
    
    print("🚀 MULTI-ACCOUNT WEBSOCKET LISTENER")
    print("=" * 50)
    
    # Create listeners
    listeners = []
    threads = []
    
    # Add Account 1
    if ACCOUNT1_JWT and ACCOUNT1_JWT != "PASTE_YOUR_FIRST_JWT_TOKEN_HERE":
        print(f"✅ Configuring {ACCOUNT1_NAME}...")
        listener1 = AccountWebSocketListener(ACCOUNT1_NAME, ACCOUNT1_JWT)
        listeners.append(listener1)
    else:
        print(f"⚠️  Skipping {ACCOUNT1_NAME} - No JWT token configured")
    
    # Add Account 2
    if ACCOUNT2_JWT and ACCOUNT2_JWT != "PASTE_YOUR_SECOND_JWT_TOKEN_HERE":
        print(f"✅ Configuring {ACCOUNT2_NAME}...")
        listener2 = AccountWebSocketListener(ACCOUNT2_NAME, ACCOUNT2_JWT)
        listeners.append(listener2)
    else:
        print(f"⚠️  Skipping {ACCOUNT2_NAME} - No JWT token configured")
    
    if not listeners:
        print("❌ No accounts configured! Please update the JWT tokens in the script.")
        return
    
    print(f"\n🎯 Starting monitoring for {len(listeners)} account(s):")
    for listener in listeners:
        print(f"   - {listener.account_name} (User: {listener.user_id})")
    
    print("=" * 50)
    
    # Start each listener in its own thread
    for listener in listeners:
        thread = threading.Thread(
            target=listener.start_listening_thread,
            daemon=True,
            name=f"Listener-{listener.account_name}"
        )
        thread.start()
        threads.append(thread)
        print(f"🔄 Started thread for {listener.account_name}")
    
    # Wait for connections to establish
    time.sleep(3)
    
    # Show connection status
    print("\n📊 CONNECTION STATUS:")
    for listener in listeners:
        status = "✅ CONNECTED" if listener.pusher_connected else "❌ FAILED"
        print(f"   {listener.account_name}: {status}")
        if listener.pusher_connected:
            print(f"      Socket ID: {listener.socket_id}")
            print(f"      Channel: {listener.sp_channel}")
    
    print("=" * 50)
    print("🎧 LISTENING FOR EVENTS... (Press Ctrl+C to stop)")
    
    # Main monitoring loop
    start_time = time.time()
    
    try:
        while True:
            time.sleep(30)  # Check every 30 seconds
            
            current_time = time.time()
            elapsed = int(current_time - start_time)
            
            # Show periodic status
            print(f"\n⏰ STATUS UPDATE - {elapsed}s running")
            
            total_events = 0
            for listener in listeners:
                events_count = listener.get_events_count()
                total_events += events_count
                status = "🟢" if listener.pusher_connected else "🔴"
                print(f"   {status} {listener.account_name}: {events_count} events")
            
            print(f"📈 Total events across all accounts: {total_events}")
            
    except KeyboardInterrupt:
        print("\n🛑 STOPPING...")
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
    finally:
        # Stop all listeners
        print("🔄 Stopping all listeners...")
        for listener in listeners:
            listener.stop_listening()
        
        # Wait for threads to finish
        for thread in threads:
            thread.join(timeout=5)
    
    # Show final summary
    print("\n" + "=" * 50)
    print("📋 FINAL SUMMARY")
    print("=" * 50)
    
    total_events = 0
    for listener in listeners:
        events_count = listener.get_events_count()
        total_events += events_count
        
        print(f"\n{listener.account_name}:")
        print(f"   User ID: {listener.user_id}")
        print(f"   Events received: {events_count}")
        
        if events_count > 0:
            print("   Event details:")
            for i, event in enumerate(listener.events_received, 1):
                print(f"     {i}. {event['timestamp']} - {event.get('data', 'No data')}")
    
    print(f"\n🎯 TOTAL EVENTS: {total_events}")
    print("=" * 50)


if __name__ == "__main__":
    main()