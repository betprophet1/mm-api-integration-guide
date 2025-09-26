#!/usr/bin/env python3
"""
Raw WebSocket Listener

Shows exact WebSocket behavior including ping/pong and all raw messages.
No beautification - just raw data.
"""

import requests
import json
import logging
import time
import pysher
from datetime import datetime
from typing import List, Dict, Any, Callable

# Configure logging to show everything
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

class RawWebSocketListener:
    """Raw WebSocket listener showing exact behavior"""
    
    def __init__(self, user_jwt_token: str):
        self.base_url = 'https://api-ss-sandbox.betprophet.co'
        self.user_jwt_token = user_jwt_token
        self.pusher = None
        self.pusher_connected = False
        self.socket_id = None
        self.events_received = []
        self.is_listening = False
        
        # User's Pusher configuration
        self.user_pusher_config = {
            "key": "c20fa36fbc3c3c308ffa",
            "cluster": "mt1"
        }
        
        # Target information
        self.user_id = "9ec071b7-e106-4838-89f0-6f66fa827681"
        self.sp_channel = "private-service=4-device_type=5-user=9ec071b7e106483889f06f66fa827681"
        
    def setup_websocket(self) -> bool:
        """Setup WebSocket connection"""
        print(f"[{datetime.now()}] RAW WEBSOCKET LISTENER")
        print(f"[{datetime.now()}] Pusher Key: {self.user_pusher_config['key']}")
        print(f"[{datetime.now()}] Pusher Cluster: {self.user_pusher_config['cluster']}")
        print(f"[{datetime.now()}] User ID: {self.user_id}")
        print(f"[{datetime.now()}] SP Channel: {self.sp_channel}")
        
        try:
            # Create Pusher client
            auth_endpoint_url = f"{self.base_url}/parlay/pusher/auth"
            auth_headers = {
                "Authorization": f"Bearer {self.user_jwt_token}",
                "Content-Type": "application/x-www-form-urlencoded"
            }
            
            print(f"[{datetime.now()}] Auth endpoint: {auth_endpoint_url}")
            print(f"[{datetime.now()}] Auth headers: {auth_headers}")
            
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
            print(f"[{datetime.now()}] Connecting to Pusher...")
            self.pusher.connect()
            
            # Wait for connection
            max_wait = 15
            wait_time = 0
            while not self.pusher_connected and wait_time < max_wait:
                time.sleep(0.5)
                wait_time += 0.5
                
            return self.pusher_connected
            
        except Exception as e:
            print(f"[{datetime.now()}] ERROR: {e}")
            return False
    
    def _on_connect(self, data):
        """Handle connection establishment"""
        print(f"[{datetime.now()}] PUSHER CONNECTION ESTABLISHED")
        print(f"[{datetime.now()}] Raw connection data: {data}")
        
        connection_data = json.loads(data)
        self.socket_id = connection_data['socket_id']
        print(f"[{datetime.now()}] Socket ID: {self.socket_id}")
        
        # Subscribe to SP channel
        if self._subscribe_to_sp_channel():
            self.pusher_connected = True
            print(f"[{datetime.now()}] CONNECTION READY")
        else:
            print(f"[{datetime.now()}] CONNECTION FAILED - subscription error")
            self.pusher_connected = False
    
    def _subscribe_to_sp_channel(self) -> bool:
        """Subscribe to SP channel"""
        print(f"[{datetime.now()}] SUBSCRIBING TO SP CHANNEL")
        print(f"[{datetime.now()}] Channel: {self.sp_channel}")
        
        try:
            # Subscribe to the SP channel
            channel = self.pusher.subscribe(self.sp_channel)
            print(f"[{datetime.now()}] Subscription request sent")
            
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
                print(f"[{datetime.now()}] Bound to event: {event_name}")
            
            print(f"[{datetime.now()}] SP CHANNEL SUBSCRIPTION COMPLETE")
            return True
            
        except Exception as e:
            print(f"[{datetime.now()}] SUBSCRIPTION ERROR: {e}")
            return False
    
    def _on_failed(self, data):
        """Handle connection failure"""
        print(f"[{datetime.now()}] CONNECTION FAILED")
        print(f"[{datetime.now()}] Failure data: {data}")
        self.pusher_connected = False
    
    def _on_error(self, data):
        """Handle connection errors"""
        print(f"[{datetime.now()}] PUSHER ERROR")
        print(f"[{datetime.now()}] Error data: {data}")
        
        if isinstance(data, dict):
            error_code = data.get('code')
            error_message = data.get('message')
            print(f"[{datetime.now()}] Error code: {error_code}")
            print(f"[{datetime.now()}] Error message: {error_message}")
    
    def _handle_raw_event(self, data: Any, *args, **kwargs):
        """Handle all events - show raw data"""
        timestamp = datetime.now()
        
        print("=" * 80)
        print(f"[{timestamp}] EVENT RECEIVED!")
        print(f"[{timestamp}] Raw data: {data}")
        print(f"[{timestamp}] Args: {args}")
        print(f"[{timestamp}] Kwargs: {kwargs}")
        
        # Try to parse if it's JSON
        try:
            if isinstance(data, str):
                parsed_data = json.loads(data)
                print(f"[{timestamp}] Parsed JSON: {json.dumps(parsed_data, indent=2)}")
        except:
            print(f"[{timestamp}] Data is not JSON")
        
        print("=" * 80)
        
        # Store the event
        event_info = {
            "timestamp": str(timestamp),
            "data": data,
            "args": args,
            "kwargs": kwargs
        }
        self.events_received.append(event_info)
    
    def start_listening(self):
        """Start listening continuously"""
        print(f"[{datetime.now()}] STARTING RAW WEBSOCKET LISTENER")
        print(f"[{datetime.now()}] Will show ALL raw WebSocket messages")
        print(f"[{datetime.now()}] Including ping/pong and connection messages")
        print("=" * 80)
        
        if not self.setup_websocket():
            print(f"[{datetime.now()}] SETUP FAILED")
            return False
        
        self.is_listening = True
        
        print(f"[{datetime.now()}] LISTENING FOR RAW MESSAGES...")
        print("=" * 80)
        
        start_time = time.time()
        
        try:
            while self.pusher_connected and self.is_listening:
                time.sleep(1)
                
                current_time = time.time()
                elapsed = int(current_time - start_time)
                
                # Show periodic status
                if elapsed % 30 == 0 and elapsed > 0:
                    events_count = len(self.events_received)
                    print(f"[{datetime.now()}] STATUS: {elapsed}s running | {events_count} events received")
                
        except KeyboardInterrupt:
            print(f"[{datetime.now()}] STOPPING...")
        except Exception as e:
            print(f"[{datetime.now()}] ERROR: {e}")
        finally:
            self.stop_listening()
        
        self._show_summary()
        return True
    
    def stop_listening(self):
        """Stop listening"""
        self.is_listening = False
        if self.pusher:
            print(f"[{datetime.now()}] DISCONNECTING...")
            try:
                self.pusher.disconnect()
            except Exception as e:
                print(f"[{datetime.now()}] DISCONNECT ERROR: {e}")
    
    def _show_summary(self):
        """Show summary"""
        print("=" * 80)
        print(f"[{datetime.now()}] RAW WEBSOCKET LISTENER SUMMARY")
        print("=" * 80)
        
        events_count = len(self.events_received)
        print(f"[{datetime.now()}] Total events received: {events_count}")
        
        if events_count > 0:
            print(f"[{datetime.now()}] Event details:")
            for i, event in enumerate(self.events_received, 1):
                print(f"  {i}. {event['timestamp']} - {event.get('data', 'No data')}")
        else:
            print(f"[{datetime.now()}] No events received")
        
        print("=" * 80)

def main():
    """Main entry point"""
    
    # Fresh JWT token
    USER_JWT_TOKEN = "eyJhbGciOiJIUzI1NiIsImtpZCI6InNpbTIifQ.eyJhY2NvdW50VHlwZSI6MCwiZXhwIjoxNzU4ODg4NTc5LCJleHRyYU9ubGluZVRpbWUiOjAsImlzT3RwRXhwaXJlZCI6ZmFsc2UsImlzU3VzcGVuZGVkIjpmYWxzZSwiaXNzIjoiaHR0cDovL21vdGhlcnNoaXAubW90aGVyc2hpcC1zYW5kYm94IiwianRpIjoiNGU5MDA5NmItNDNmMC00ODdmLWIxMDMtMzY1Mjk3ODkyY2E2Iiwia2JhQW5zd2Vyc0luZm8iOiJOL0EiLCJreWNJbmZvIjoiU3VjY2VzcyIsInBlbmRpbmdCeUFkbWluIjpmYWxzZSwicHVzaGVySW5mbyI6eyJhdXRoQ2hhbm5lbCI6InVzZXIuYXV0aGVudGljYXRpb24uNGU5MDA5NmItNDNmMC00ODdmLWIxMDMtMzY1Mjk3ODkyY2E2IiwiYmFsYW5jZVVwZGF0ZWRFdmVudCI6IndhbGxldC5iYWxhbmNlLnVwZGF0ZWQiLCJjbHVzdGVyIjoibXQxIiwiaWQiOiJjMjBmYTM2ZmJjM2MzYzMwOGZmYSIsImluZm9DaGFubmVsIjoidXNlci5pbmZvcm1hdGlvbi45ZWMwNzFiNy1lMTA2LTQ4MzgtODlmMC02ZjY2ZmE4Mjc2ODEiLCJpbmZvcm1hdGlvblVwZGF0ZWRFdmVudCI6InVzZXIuaW5mb3JtYXRpb24udXBkYXRlZCIsImtiYUNoYWxsZW5nZVF1ZXN0aW9uc0V2ZW50IjoidXNlci5rYmEuY2hhbGxlbmdlIiwic2Vzc2lvblRlcm1pbmF0ZWRFdmVudCI6InNlc3Npb24udGVybWluYXRlZCJ9LCJyZWdpb24iOiJOWSIsInN1YiI6ImxvbmcucGFydG5lckB5b3BtYWlsLmNvbSIsInVzZXJJZCI6IjllYzA3MWI3LWUxMDYtNDgzOC04OWYwLTZmNjZmYTgyNzY4MSJ9.utMBchrjp_Xap4cE3L_vA4ZlsbqpdsYZLANI8sSvRj4"
    
    # Create raw listener
    listener = RawWebSocketListener(user_jwt_token=USER_JWT_TOKEN)
    
    # Start listening
    listener.start_listening()

if __name__ == "__main__":
    main()