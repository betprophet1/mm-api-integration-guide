#!/usr/bin/env python3
"""
Hybrid SP All Events WebSocket Listener

Uses the correct SP Pusher configuration (us2 cluster, SP key) 
but combines with user JWT authentication for channel access.
This should provide access to both broadcast and user-specific channels.
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

class HybridSPAccountWebSocketListener:
    """Hybrid SP WebSocket listener using correct SP config + user auth"""
    
    def __init__(self, account_name: str, user_jwt_token: str, sp_jwt_token: str):
        self.account_name = account_name
        self.base_url = 'https://api-ss-sandbox.betprophet.co'
        self.user_jwt_token = user_jwt_token
        self.sp_jwt_token = sp_jwt_token
        self.pusher = None
        self.pusher_connected = False
        self.socket_id = None
        self.events_received = []
        self.is_listening = False
        self.user_id = None
        self.channels_subscribed = []
        self.sp_pusher_config = None
        
        # Extract user info from user JWT
        self._extract_user_info()
        
        # Get SP Pusher configuration
        self._get_sp_pusher_config()
        
        # All channels and events to monitor
        self.all_channels_events = [
            {
                "channel_name": "private-broadcast-service=4-device_type=5",
                "events": ["price.ask.new", "order.matched"]
            },
            {
                "channel_name": f"private-service=4-device_type=5-user={self.user_id.replace('-', '') if self.user_id else 'unknown'}",
                "events": ["price.confirm.new", "refund.processed", "order.finalized", "order.settled", "health_check", "parlay.settled"]
            }
        ]
        
    def _extract_user_info(self):
        """Extract user information from user JWT token"""
        try:
            # Split the token
            header, payload, signature = self.user_jwt_token.split('.')
            
            # Add padding if needed
            payload += '=' * (4 - len(payload) % 4)
            
            # Decode payload
            decoded = base64.urlsafe_b64decode(payload)
            payload_data = json.loads(decoded)
            
            # Extract user ID
            self.user_id = payload_data.get('userId')
            
            # Check expiration
            exp = payload_data.get('exp')
            if exp:
                exp_date = datetime.fromtimestamp(exp)
                current_time = datetime.now()
                print(f"[{self.account_name}] User token expires: {exp_date}")
                print(f"[{self.account_name}] User token expired: {current_time > exp_date}")
                
        except Exception as e:
            print(f"[{self.account_name}] Error extracting user info: {e}")
            self.user_id = "unknown"
    
    def _get_sp_pusher_config(self):
        """Get SP Pusher configuration using SP JWT"""
        try:
            config_url = f'{self.base_url}/parlay/sp/websocket/connection-config'
            headers = {
                'Authorization': f'Bearer {self.sp_jwt_token}',
                'Content-Type': 'application/json'
            }
            
            print(f"[{self.account_name}] 🔄 Getting SP Pusher config...")
            response = requests.get(config_url, headers=headers)
            
            if response.status_code == 200:
                self.sp_pusher_config = response.json()
                print(f"[{self.account_name}] ✅ SP Pusher Config:")
                print(f"[{self.account_name}]   App ID: {self.sp_pusher_config.get('app_id')}")
                print(f"[{self.account_name}]   Cluster: {self.sp_pusher_config.get('cluster')}")
                print(f"[{self.account_name}]   Key: {self.sp_pusher_config.get('key')}")
            else:
                print(f"[{self.account_name}] ❌ Failed to get SP config: {response.text}")
                # Fallback to known SP config
                self.sp_pusher_config = {
                    "app_id": "1971558",
                    "cluster": "us2", 
                    "key": "8f413ec26ae0915cd52f"
                }
                print(f"[{self.account_name}] 🔄 Using fallback SP config")
                
        except Exception as e:
            print(f"[{self.account_name}] ⚠️ Error getting SP config: {e}")
            # Fallback config
            self.sp_pusher_config = {
                "app_id": "1971558",
                "cluster": "us2", 
                "key": "8f413ec26ae0915cd52f"
            }
        
    def setup_websocket(self) -> bool:
        """Setup WebSocket connection using SP config + user auth"""
        print(f"[{self.account_name}] 🚀 STARTING HYBRID SP WEBSOCKET LISTENER")
        print(f"[{self.account_name}] User ID: {self.user_id}")
        print(f"[{self.account_name}] Using SP Pusher Config:")
        print(f"[{self.account_name}]   Cluster: {self.sp_pusher_config.get('cluster')}")
        print(f"[{self.account_name}]   Key: {self.sp_pusher_config.get('key')}")
        print(f"[{self.account_name}] Channels to monitor:")
        for channel_info in self.all_channels_events:
            print(f"  📡 {channel_info['channel_name']}")
            print(f"     Events: {', '.join(channel_info['events'])}")
        
        try:
            # Create Pusher client with SP config but user auth
            auth_endpoint_url = f"{self.base_url}/parlay/pusher/auth"
            auth_headers = {
                "Authorization": f"Bearer {self.user_jwt_token}",
                "Content-Type": "application/x-www-form-urlencoded"
            }
            
            self.pusher = pysher.Pusher(
                key=self.sp_pusher_config['key'],
                cluster=self.sp_pusher_config['cluster'],
                auth_endpoint=auth_endpoint_url,
                auth_endpoint_headers=auth_headers
            )
            
            # Setup handlers
            self.pusher.connection.bind('pusher:connection_established', self._on_connect)
            self.pusher.connection.bind('pusher:connection_failed', self._on_failed)
            self.pusher.connection.bind('pusher:error', self._on_error)
            
            # Connect
            print(f"[{self.account_name}] 🔄 Connecting to SP Pusher...")
            self.pusher.connect()
            
            # Wait for connection
            max_wait = 15
            wait_time = 0
            while not self.pusher_connected and wait_time < max_wait:
                time.sleep(0.5)
                wait_time += 0.5
                
            return self.pusher_connected
            
        except Exception as e:
            print(f"[{self.account_name}] ❌ ERROR: {e}")
            return False
    
    def _on_connect(self, data):
        """Handle connection establishment"""
        print(f"[{self.account_name}] ✅ SP PUSHER CONNECTION ESTABLISHED")
        
        connection_data = json.loads(data)
        self.socket_id = connection_data['socket_id']
        print(f"[{self.account_name}] Socket ID: {self.socket_id}")
        
        # Subscribe to ALL channels
        if self._subscribe_to_all_channels():
            self.pusher_connected = True
            print(f"[{self.account_name}] 🎯 ALL SP CHANNELS READY")
        else:
            print(f"[{self.account_name}] ⚠️ PARTIAL CONNECTION - some channels failed")
            # Still consider connected if at least one channel works
            self.pusher_connected = len(self.channels_subscribed) > 0
    
    def _subscribe_to_all_channels(self) -> bool:
        """Subscribe to ALL authorized channels"""
        print(f"[{self.account_name}] 📡 SUBSCRIBING TO ALL SP CHANNELS")
        
        try:
            success_count = 0
            
            for channel_info in self.all_channels_events:
                channel_name = channel_info["channel_name"]
                events = channel_info["events"]
                
                print(f"[{self.account_name}] 🔄 Subscribing to: {channel_name}")
                
                try:
                    # Subscribe to the channel
                    channel = self.pusher.subscribe(channel_name)
                    print(f"[{self.account_name}]   ✅ Subscribed to: {channel_name}")
                    
                    # Bind to all events for this channel
                    for event_name in events:
                        channel.bind(event_name, lambda data, event=event_name, chan=channel_name: self._handle_raw_event(data, event, chan))
                        print(f"[{self.account_name}]   🎧 Bound to event: {event_name}")
                    
                    self.channels_subscribed.append(channel_name)
                    success_count += 1
                    
                except Exception as e:
                    print(f"[{self.account_name}]   ❌ Failed to subscribe to {channel_name}: {e}")
            
            print(f"[{self.account_name}] 📊 SP SUBSCRIPTION SUMMARY:")
            print(f"[{self.account_name}]   ✅ Successfully subscribed to {success_count}/{len(self.all_channels_events)} channels")
            print(f"[{self.account_name}]   📡 Active channels: {self.channels_subscribed}")
            
            return success_count > 0
            
        except Exception as e:
            print(f"[{self.account_name}] ❌ SUBSCRIPTION ERROR: {e}")
            return False
    
    def _on_failed(self, data):
        """Handle connection failure"""
        print(f"[{self.account_name}] ❌ CONNECTION FAILED")
        print(f"[{self.account_name}] Failure data: {data}")
        self.pusher_connected = False
    
    def _on_error(self, data):
        """Handle connection errors"""
        print(f"[{self.account_name}] ⚠️ PUSHER ERROR")
        print(f"[{self.account_name}] Error data: {data}")
        
        if isinstance(data, dict):
            error_code = data.get('code')
            error_message = data.get('message')
            print(f"[{self.account_name}] Error code: {error_code}")
            print(f"[{self.account_name}] Error message: {error_message}")
    
    def _handle_raw_event(self, data: Any, event_name: str, channel_name: str):
        """Handle all events - show raw data with enhanced info"""
        timestamp = datetime.now()
        
        print("=" * 100)
        print(f"🎯 [{self.account_name}] SP EVENT RECEIVED! {timestamp}")
        print(f"📡 Channel: {channel_name}")
        print(f"🎧 Event: {event_name}")
        print(f"📦 Raw data: {data}")
        
        # Try to parse if it's JSON
        parsed_data = None
        try:
            if isinstance(data, str):
                parsed_data = json.loads(data)
                print(f"📋 Parsed JSON:")
                print(json.dumps(parsed_data, indent=2))
                
                # Extract key information if available
                if isinstance(parsed_data, dict):
                    payload = parsed_data.get('payload', {})
                    if payload:
                        print(f"🔍 Key Details:")
                        if 'order_uuid' in payload:
                            print(f"   📝 Order UUID: {payload['order_uuid']}")
                        if 'parlay_id' in payload:
                            print(f"   🎲 Parlay ID: {payload['parlay_id']}")
                        if 'profit' in payload:
                            print(f"   💰 Profit: ${payload['profit']}")
                        if 'settlement_status' in payload:
                            print(f"   📊 Status: {payload['settlement_status']}")
                        if 'price' in payload:
                            print(f"   💲 Price: {payload['price']}")
                        if 'market_id' in payload:
                            print(f"   🎯 Market ID: {payload['market_id']}")
                        if 'match_id' in payload:
                            print(f"   ⚽ Match ID: {payload['match_id']}")
                            
        except Exception as e:
            print(f"⚠️ JSON parsing error: {e}")
        
        print("=" * 100)
        
        # Store the event with enhanced metadata
        event_info = {
            "timestamp": str(timestamp),
            "account": self.account_name,
            "channel": channel_name,
            "event": event_name,
            "data": data,
            "parsed_data": parsed_data
        }
        self.events_received.append(event_info)
    
    def start_listening_thread(self):
        """Start listening in a separate thread"""
        self.is_listening = True
        
        if not self.setup_websocket():
            print(f"[{self.account_name}] ❌ SETUP FAILED")
            return False
        
        print(f"[{self.account_name}] 🎧 LISTENING FOR ALL SP EVENTS...")
        
        try:
            while self.pusher_connected and self.is_listening:
                time.sleep(1)
                
        except Exception as e:
            print(f"[{self.account_name}] ❌ ERROR: {e}")
        finally:
            self.stop_listening()
        
        return True
    
    def stop_listening(self):
        """Stop listening"""
        self.is_listening = False
        if self.pusher:
            print(f"[{self.account_name}] 🔄 DISCONNECTING...")
            try:
                self.pusher.disconnect()
            except Exception as e:
                print(f"[{self.account_name}] ⚠️ DISCONNECT ERROR: {e}")
    
    def get_events_count(self):
        """Get number of events received"""
        return len(self.events_received)
    
    def get_events_by_type(self):
        """Get events grouped by type"""
        events_by_type = {}
        for event in self.events_received:
            event_type = event['event']
            if event_type not in events_by_type:
                events_by_type[event_type] = []
            events_by_type[event_type].append(event)
        return events_by_type


def main():
    """Main entry point"""
    
    # ==========================================
    # CONFIGURE YOUR JWT TOKENS HERE
    # ==========================================
    
    # SP JWT Token (for SP config)
    SP_JWT_TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJqdGkiOiI1MmFkNzE2Ni0xMGZmLTQwNGYtYTBiOC0xMjJhYjk2ZTI3NjAiLCJleHAiOjE3NTkyMTQyNjUsImlhdCI6MTc1OTIxMzA2NSwicGFydG5lclR5cGUiOiJtbSIsInBhcnRuZXJJRCI6IjllYzA3MWI3LWUxMDYtNDgzOC04OWYwLTZmNjZmYTgyNzY4MSIsInR5cGUiOiJhY2Nlc3MiLCJhY2Nlc3NLZXkiOiI2MWExODMwYmQ1NWE4ODU1NzQyNDNhZDZjZjAxNmZlZiJ9.L_eM68cuxEwKco7F5mPqm9ct0R4hIV92dOIcKkwM7Kk"
    
    # Account 1 Configuration (User JWT for auth)
    ACCOUNT1_NAME = "LongPartner"
    ACCOUNT1_JWT = "eyJhbGciOiJIUzI1NiIsImtpZCI6InNpbTIifQ.eyJhY2NvdW50VHlwZSI6MCwiZXhwIjoxNzU5MjE1MTQwLCJleHRyYU9ubGluZVRpbWUiOjAsImlzT3RwRXhwaXJlZCI6ZmFsc2UsImlzU3VzcGVuZGVkIjpmYWxzZSwiaXNzIjoiaHR0cDovL21vdGhlcnNoaXAubW90aGVyc2hpcC1zYW5kYm94IiwianRpIjoiNGEzMGQ2ZjgtNTNkYi00ZDI0LThmYzItMDc1NmE3YzZlNmQ2Iiwia2JhQW5zd2Vyc0luZm8iOiJOL0EiLCJreWNJbmZvIjoiU3VjY2VzcyIsInBlbmRpbmdCeUFkbWluIjpmYWxzZSwicHVzaGVySW5mbyI6eyJhdXRoQ2hhbm5lbCI6InVzZXIuYXV0aGVudGljYXRpb24uNGEzMGQ2ZjgtNTNkYi00ZDI0LThmYzItMDc1NmE3YzZlNmQ2IiwiYmFsYW5jZVVwZGF0ZWRFdmVudCI6IndhbGxldC5iYWxhbmNlLnVwZGF0ZWQiLCJjbHVzdGVyIjoibXQxIiwiaWQiOiJjMjBmYTM2ZmJjM2MzYzMwOGZmYSIsImluZm9DaGFubmVsIjoidXNlci5pbmZvcm1hdGlvbi45ZWMwNzFiNy1lMTA2LTQ4MzgtODlmMC02ZjY2ZmE4Mjc2ODEiLCJpbmZvcm1hdGlvblVwZGF0ZWRFdmVudCI6InVzZXIuaW5mb3JtYXRpb24udXBkYXRlZCIsImtiYUNoYWxsZW5nZVF1ZXN0aW9uc0V2ZW50IjoidXNlci5rYmEuY2hhbGxlbmdlIiwic2Vzc2lvblRlcm1pbmF0ZWRFdmVudCI6InNlc3Npb24udGVybWluYXRlZCJ9LCJyZWdpb24iOiJOWSIsInN1YiI6ImxvbmcucGFydG5lckB5b3BtYWlsLmNvbSIsInVzZXJJZCI6IjllYzA3MWI3LWUxMDYtNDgzOC04OWYwLTZmNjZmYTgyNzY4MSJ9.w_qtb_M4h3_1242_REc0CR_ivOj60bp4zBlKxZQrdro"
    
    # Account 2 Configuration (User JWT for auth) 
    ACCOUNT2_NAME = "HoangThai"
    ACCOUNT2_JWT = "eyJhbGciOiJIUzI1NiIsImtpZCI6InNpbTIifQ.eyJhY2NvdW50VHlwZSI6MCwiZXhwIjoxNzU5MjE1MjEwLCJleHRyYU9ubGluZVRpbWUiOjAsImlzT3RwRXhwaXJlZCI6ZmFsc2UsImlzU3VzcGVuZGVkIjpmYWxzZSwiaXNzIjoiaHR0cDovL21vdGhlcnNoaXAubW90aGVyc2hpcC1zYW5kYm94IiwianRpIjoiYTczYzNkM2YtOWQ2OS00ZTY0LTllZGUtZDJhYzU5MjYyNzA4Iiwia2JhQW5zd2Vyc0luZm8iOiJOL0EiLCJreWNJbmZvIjoiU3VjY2VzcyIsInBlbmRpbmdCeUFkbWluIjpmYWxzZSwicHVzaGVySW5mbyI6eyJhdXRoQ2hhbm5lbCI6InVzZXIuYXV0aGVudGljYXRpb24uYTczYzNkM2YtOWQ2OS00ZTY0LTllZGUtZDJhYzU5MjYyNzA4IiwiYmFsYW5jZVVwZGF0ZWRFdmVudCI6IndhbGxldC5iYWxhbmNlLnVwZGF0ZWQiLCJjbHVzdGVyIjoibXQxIiwiaWQiOiJjMjBmYTM2ZmJjM2MzYzMwOGZmYSIsImluZm9DaGFubmVsIjoidXNlci5pbmZvcm1hdGlvbi5hZjVhODQ5OS03Y2I4LTQ1ZDItODU2Ni0yOWY5M2QyMzRhZTgiLCJpbmZvcm1hdGlvblVwZGF0ZWRFdmVudCI6InVzZXIuaW5mb3JtYXRpb24udXBkYXRlZCIsImtiYUNoYWxsZW5nZVF1ZXN0aW9uc0V2ZW50IjoidXNlci5rYmEuY2hhbGxlbmdlIiwic2Vzc2lvblRlcm1pbmF0ZWRFdmVudCI6InNlc3Npb24udGVybWluYXRlZCJ9LCJyZWdpb24iOiJOWSIsInN1YiI6ImhvYW5nLnRoYWkrcGFydG5lcnNiMDFAYmV0cHJvcGhldC5jbyIsInVzZXJJZCI6ImFmNWE4NDk5LTdjYjgtNDVkMi04NTY2LTI5ZjkzZDIzNGFlOCJ9.jEgg183J-gOj_BEi2jwUInhdmG1Kz_AkG_MVW7Vi4-4"
    
    # ==========================================
    
    print("🚀 HYBRID SP ALL EVENTS WEBSOCKET LISTENER")
    print("=" * 70)
    
    # Create listeners
    listeners = []
    threads = []
    
    # Add Account 1
    if ACCOUNT1_JWT:
        print(f"✅ Configuring {ACCOUNT1_NAME} with SP hybrid approach...")
        listener1 = HybridSPAccountWebSocketListener(ACCOUNT1_NAME, ACCOUNT1_JWT, SP_JWT_TOKEN)
        listeners.append(listener1)
    else:
        print(f"⚠️  Skipping {ACCOUNT1_NAME} - No JWT token configured")
    
    # Add Account 2
    if ACCOUNT2_JWT:
        print(f"✅ Configuring {ACCOUNT2_NAME} with SP hybrid approach...")
        listener2 = HybridSPAccountWebSocketListener(ACCOUNT2_NAME, ACCOUNT2_JWT, SP_JWT_TOKEN)
        listeners.append(listener2)
    else:
        print(f"⚠️  Skipping {ACCOUNT2_NAME} - No JWT token configured")
    
    if not listeners:
        print("❌ No accounts configured!")
        return
    
    print(f"\n🎯 Starting SP hybrid monitoring for {len(listeners)} account(s):")
    for listener in listeners:
        print(f"   - {listener.account_name} (User: {listener.user_id})")
    
    print("=" * 70)
    
    # Start each listener in its own thread
    for listener in listeners:
        thread = threading.Thread(
            target=listener.start_listening_thread,
            daemon=True,
            name=f"SP-Listener-{listener.account_name}"
        )
        thread.start()
        threads.append(thread)
        print(f"🔄 Started SP thread for {listener.account_name}")
    
    # Wait for connections to establish
    time.sleep(5)
    
    # Show connection status
    print("\n📊 SP CONNECTION STATUS:")
    for listener in listeners:
        status = "✅ CONNECTED" if listener.pusher_connected else "❌ FAILED"
        print(f"   {listener.account_name}: {status}")
        if listener.pusher_connected:
            print(f"      Socket ID: {listener.socket_id}")
            print(f"      Subscribed channels: {len(listener.channels_subscribed)}")
            for channel in listener.channels_subscribed:
                print(f"        📡 {channel}")
    
    print("=" * 70)
    print("🎧 LISTENING FOR ALL SP EVENTS... (Press Ctrl+C to stop)")
    
    # Main monitoring loop
    start_time = time.time()
    
    try:
        while True:
            time.sleep(30)  # Check every 30 seconds
            
            current_time = time.time()
            elapsed = int(current_time - start_time)
            
            # Show periodic status
            print(f"\n⏰ SP STATUS UPDATE - {elapsed}s running")
            
            total_events = 0
            for listener in listeners:
                events_count = listener.get_events_count()
                total_events += events_count
                status = "🟢" if listener.pusher_connected else "🔴"
                print(f"   {status} {listener.account_name}: {events_count} events")
                
                # Show event breakdown by type
                events_by_type = listener.get_events_by_type()
                if events_by_type:
                    print(f"      📊 Event breakdown:")
                    for event_type, events in events_by_type.items():
                        print(f"        🎧 {event_type}: {len(events)}")
            
            print(f"📈 Total SP events: {total_events}")
            
    except KeyboardInterrupt:
        print("\n🛑 STOPPING SP LISTENERS...")
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
    finally:
        # Stop all listeners
        print("🔄 Stopping all SP listeners...")
        for listener in listeners:
            listener.stop_listening()
        
        # Wait for threads to finish
        for thread in threads:
            thread.join(timeout=5)
    
    # Show final summary
    print("\n" + "=" * 70)
    print("📋 FINAL SP HYBRID SUMMARY")
    print("=" * 70)
    
    total_events = 0
    for listener in listeners:
        events_count = listener.get_events_count()
        total_events += events_count
        
        print(f"\n{listener.account_name}:")
        print(f"   User ID: {listener.user_id}")
        print(f"   Channels monitored: {len(listener.channels_subscribed)}")
        print(f"   Total events received: {events_count}")
        
        # Show events by type
        events_by_type = listener.get_events_by_type()
        if events_by_type:
            print("   📊 Events by type:")
            for event_type, events in events_by_type.items():
                print(f"     🎧 {event_type}: {len(events)}")
                
        # Show recent events
        if events_count > 0:
            print("   📝 Recent events:")
            recent_events = listener.events_received[-3:]  # Last 3 events
            for i, event in enumerate(recent_events, 1):
                print(f"     {i}. {event['timestamp']} - {event['channel']} - {event['event']}")
    
    print(f"\n🎯 TOTAL SP EVENTS: {total_events}")
    print("=" * 70)


if __name__ == "__main__":
    main()