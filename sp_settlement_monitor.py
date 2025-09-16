#!/usr/bin/env python3
"""
This script provides comprehensive monitoring capabilities for Service Provider orders including:
- Dynamic order list fetching from API
- Real-time WebSocket monitoring via Pusher
- Individual order detail retrieval 
- Live settlement status tracking
"""

import requests
import json
import logging
import time
import base64
import pysher
from datetime import datetime
from collections import defaultdict
from typing import Dict, List, Optional, Any
import concurrent.futures


class ServiceProviderMonitor:
    
    # API Configuration
    BASE_URL = 'https://api-ss-sandbox.betprophet.co'
    
    # API Endpoints
    ENDPOINTS = {
        'auth_login': 'partner/auth/login',
        'orders_list': 'parlay/sp/orders',
        'order_detail': 'parlay/sp/orders',
        'websocket_config': 'partner/websocket/connection-config',
        'pusher_auth': 'partner/pusher'
    }
    
    # WebSocket Channels for monitoring
    MONITORING_CHANNELS = [
        'sp-orders',
        'sp-settlements', 
        'parlay-updates',
        'order-status'
    ]
    
    # Events to monitor on each channel
    MONITORED_EVENTS = [
        'order_settlement',
        'order_status_update', 
        'parlay_settlement',
        'settlement_update',
        'order_update',
        'status_change'
    ]

    def __init__(self, access_key: str, secret_key: str, log_level: str = 'INFO'):
        """
        Initialize the Service Provider Monitor
        
        Args:
            access_key (str): API access key
            secret_key (str): API secret key  
            log_level (str): Logging level (DEBUG, INFO, WARNING, ERROR)
        """
        # Authentication credentials
        self.access_key = access_key
        self.secret_key = secret_key
        self.access_token: Optional[str] = None
        
        # WebSocket connection
        self.pusher: Optional[pysher.Pusher] = None
        self.pusher_connected = False
        
        # Order tracking
        self.orders_to_monitor: List[str] = []
        self.order_statuses: Dict[str, Any] = {}
        self.order_updates_received: Dict[str, int] = defaultdict(int)
        
        # Setup logging
        self._setup_logging(log_level)
        
    def _setup_logging(self, log_level: str) -> None:
        """Configure logging with professional formatting"""
        logging.basicConfig(
            level=getattr(logging, log_level.upper()),
            format='%(asctime)s | %(levelname)-8s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        self.logger = logging.getLogger(__name__)
        
        # Suppress pysher debug logs for cleaner output
        logging.getLogger('pysher').setLevel(logging.WARNING)
        
    def authenticate(self) -> bool:
        """
        Authenticate and obtain access token
        
        Returns:
            bool: True if authentication successful, False otherwise
        """
        self.logger.info("🔐 Authenticating...")
        
        url = f"{self.BASE_URL}/{self.ENDPOINTS['auth_login']}"
        payload = {
            "access_key": self.access_key,
            "secret_key": self.secret_key
        }
        headers = {'Content-Type': 'application/json'}
        
        try:
            response = requests.post(url, json=payload, headers=headers, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                self.access_token = data['data']['access_token']
                self.logger.info("✅ Authentication successful!")
                self.logger.debug(f"Access token: {self.access_token[:50]}...")
                return True
            else:
                self.logger.error(f"❌ Authentication failed: {response.status_code} - {response.text}")
                return False
                
        except requests.RequestException as e:
            self.logger.error(f"❌ Authentication error: {str(e)}")
            return False
    
    def _get_auth_headers(self) -> Dict[str, str]:
        """Get authorization headers for authenticated API calls"""
        return {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json'
        }
    
    def fetch_orders_from_api(self, limit: int = 10) -> bool:
        """
        Fetch orders list dynamically from API
        
        Args:
            limit (int): Maximum number of orders to fetch
            
        Returns:
            bool: True if orders fetched successfully, False otherwise
        """
        self.logger.info(f"📡 Fetching orders list from API (limit: {limit})...")
        
        url = f"{self.BASE_URL}/{self.ENDPOINTS['orders_list']}?limit={limit}"
        
        try:
            response = requests.get(url, headers=self._get_auth_headers(), timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                orders = data.get('data', {}).get('orders', [])
                
                self.logger.info(f"✅ Successfully fetched {len(orders)} orders from API")
                
                # Log the full API response for transparency
                self.logger.info("📦 Full Orders List Response:")
                self.logger.info("-" * 80)
                self.logger.info(json.dumps(data, indent=2))
                self.logger.info("-" * 80)
                
                # Extract order UUIDs
                self.orders_to_monitor = [order['order_uuid'] for order in orders]
                
                # Display order summary
                self._display_orders_summary(orders)
                
                return True
            else:
                self.logger.error(f"❌ Failed to fetch orders: {response.status_code} - {response.text}")
                return False
                
        except requests.RequestException as e:
            self.logger.error(f"❌ Error fetching orders: {str(e)}")
            return False
    
    def _display_orders_summary(self, orders: List[Dict[str, Any]]) -> None:
        """Display a formatted summary of fetched orders"""
        self.logger.info(f"📋 Orders Summary ({len(orders)} orders found):")
        self.logger.info("-" * 100)
        
        for i, order in enumerate(orders, 1):
            order_uuid = order['order_uuid']
            status = order.get('status', 'unknown')
            settlement_status = order.get('settlement_status', 'unknown')
            stake = order.get('confirmed_stake', 'N/A')
            
            # Format stake display
            stake_display = f"${stake}" if stake else "N/A"
            
            self.logger.info(
                f"{i:2d}. {order_uuid} | "
                f"Status: {status:10} | "
                f"Settlement: {settlement_status:6} | "
                f"Stake: {stake_display:>8}"
            )
        
        self.logger.info("-" * 100)
    
    def setup_websocket_connection(self) -> bool:
        """
        Setup Pusher WebSocket connection for real-time monitoring
        
        Returns:
            bool: True if connection established successfully, False otherwise
        """
        self.logger.info("🚀 Setting up Pusher WebSocket connection...")
        
        # Get Pusher configuration
        config_url = f"{self.BASE_URL}/{self.ENDPOINTS['websocket_config']}"
        
        try:
            response = requests.get(config_url, headers=self._get_auth_headers(), timeout=30)
            
            if response.status_code != 200:
                self.logger.error(f"❌ Failed to get Pusher config: {response.status_code}")
                return False
                
            config = response.json()
            pusher_key = config['key']
            pusher_cluster = config['cluster']
            
            self.logger.info(f"🔑 Pusher Key: {pusher_key}")
            self.logger.info(f"🌐 Pusher Cluster: {pusher_cluster}")
            
        except requests.RequestException as e:
            self.logger.error(f"❌ Error getting Pusher config: {str(e)}")
            return False
        
        # Setup Pusher client
        auth_endpoint_url = f"{self.BASE_URL}/{self.ENDPOINTS['pusher_auth']}"
        auth_headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/x-www-form-urlencoded",
            "header-subscriptions": json.dumps([{
                "type": "service_provider",
                "events": self.MONITORED_EVENTS
            }])
        }
        
        self.pusher = pysher.Pusher(
            key=pusher_key, 
            cluster=pusher_cluster,
            auth_endpoint=auth_endpoint_url,
            auth_endpoint_headers=auth_headers
        )
        
        # Setup event handlers
        self._setup_websocket_handlers()
        
        # Connect to Pusher
        self.logger.info("🔌 Connecting to Pusher WebSocket...")
        self.pusher.connect()
        
        # Wait for connection with timeout
        max_wait_time = 15
        wait_time = 0
        
        while not self.pusher_connected and wait_time < max_wait_time:
            time.sleep(0.5)
            wait_time += 0.5
            
        if self.pusher_connected:
            self.logger.info("✅ WebSocket connection established successfully!")
            return True
        else:
            self.logger.error("❌ WebSocket connection timeout")
            return False
    
    def _setup_websocket_handlers(self) -> None:
        """Setup WebSocket event handlers"""
        
        def on_connection_established(data):
            """Handle WebSocket connection establishment"""
            self.logger.info("🔌 Pusher WebSocket connected!")
            
            try:
                connection_data = json.loads(data)
                socket_id = connection_data['socket_id']
                self.logger.debug(f"Socket ID: {socket_id}")
                
                # Subscribe to monitoring channels
                self._subscribe_to_channels()
                
                self.pusher_connected = True
                
            except Exception as e:
                self.logger.error(f"❌ Error in connection handler: {str(e)}")
        
        def on_connection_failed(data):
            """Handle WebSocket connection failure"""
            self.logger.error(f"❌ Pusher connection failed: {data}")
            self.pusher_connected = False
            
        def on_disconnection(data):
            """Handle WebSocket disconnection"""
            self.logger.warning("🔌 Pusher disconnected!")
            self.pusher_connected = False
            
        def on_error(data):
            """Handle WebSocket errors"""
            self.logger.error(f"❌ Pusher error: {data}")
        
        # Bind connection events
        self.pusher.connection.bind('pusher:connection_established', on_connection_established)
        self.pusher.connection.bind('pusher:connection_failed', on_connection_failed)
        self.pusher.connection.bind('pusher:disconnected', on_disconnection)
        self.pusher.connection.bind('pusher:error', on_error)
    
    def _subscribe_to_channels(self) -> None:
        """Subscribe to order monitoring channels"""
        self.logger.info("📺 Subscribing to monitoring channels...")
        
        for channel_name in self.MONITORING_CHANNELS:
            try:
                self.logger.info(f"📺 Subscribing to: {channel_name}")
                channel = self.pusher.subscribe(channel_name)
                
                # Bind to monitored events
                for event_name in self.MONITORED_EVENTS:
                    channel.bind(event_name, self._handle_order_event)
                    self.logger.debug(f"🎯 Bound to event '{event_name}' on '{channel_name}'")
                
                self.logger.info(f"✅ Subscribed to channel: {channel_name}")
                
            except Exception as e:
                self.logger.warning(f"⚠️ Could not subscribe to {channel_name}: {str(e)}")
    
    def _handle_order_event(self, data: Any, event_name: str = None, *args, **kwargs) -> None:
        """
        Handle incoming order-related WebSocket events
        
        Args:
            data: Event data from WebSocket
            event_name: Name of the triggered event
        """
        try:
            timestamp = datetime.now().strftime("%H:%M:%S")
            self.logger.info(f"📨 [{timestamp}] REAL-TIME ORDER EVENT RECEIVED!")
            
            if event_name:
                self.logger.info(f"🎯 Event: {event_name}")
            
            self.logger.info(f"📄 Raw Data: {data}")
            
            # Process event data
            if isinstance(data, str):
                try:
                    event_data = json.loads(data)
                    self._process_order_event_data(event_data, event_name)
                except json.JSONDecodeError:
                    self.logger.info(f"📄 Non-JSON data received: {data}")
            else:
                self._process_order_event_data(data, event_name)
                
        except Exception as e:
            self.logger.error(f"❌ Error handling order event: {str(e)}")
    
    def _process_order_event_data(self, event_data: Dict[str, Any], event_name: str = None) -> None:
        """Process and analyze order event data"""
        
        # Extract order information
        order_fields = ['order_id', 'order_uuid', 'p_id']
        status_fields = ['settlement_status', 'status']
        
        extracted_data = {}
        for field in order_fields + status_fields:
            if field in event_data:
                extracted_data[field] = event_data[field]
        
        if extracted_data:
            self.logger.info("📦 Order Data Extracted:")
            for key, value in extracted_data.items():
                self.logger.info(f"   {key}: {value}")
            
            # Check if this is a monitored order
            order_uuid = extracted_data.get('order_uuid') or extracted_data.get('order_id')
            if order_uuid and order_uuid in self.orders_to_monitor:
                self.order_updates_received[order_uuid] += 1
                self.logger.info(f"🎯 MONITORED ORDER UPDATE! {order_uuid}")
                self.logger.info(f"📈 Update #{self.order_updates_received[order_uuid]} for this order")
                
                # Update stored status
                if order_uuid in self.order_statuses:
                    for key, value in extracted_data.items():
                        if key in status_fields:
                            self.order_statuses[order_uuid][key] = value
        
        # Handle base64 encoded payloads
        if 'payload' in event_data:
            try:
                decoded_payload = base64.b64decode(event_data['payload']).decode('utf-8')
                self.logger.info(f"🔓 Decoded Payload: {decoded_payload}")
                
                try:
                    payload_json = json.loads(decoded_payload)
                    self._process_order_event_data(payload_json, f"{event_name}_payload")
                except json.JSONDecodeError:
                    pass
                    
            except Exception:
                self.logger.info(f"🔍 Payload (not base64): {event_data['payload']}")
    
    def fetch_individual_order_details(self) -> bool:
        """
        Fetch detailed information for each order individually
        
        Returns:
            bool: True if at least one order was fetched successfully
        """
        if not self.orders_to_monitor:
            self.logger.warning("⚠️ No orders to fetch details for")
            return False
        
        self.logger.info(f"🚀 Fetching individual details for {len(self.orders_to_monitor)} orders...")
        self.logger.info("=" * 100)
        
        successful_fetches = 0
        failed_fetches = 0
        
        for i, order_uuid in enumerate(self.orders_to_monitor, 1):
            self.logger.info(f"🔍 [{i}/{len(self.orders_to_monitor)}] Fetching: {order_uuid}")
            
            if self._fetch_single_order(order_uuid):
                successful_fetches += 1
            else:
                failed_fetches += 1
            
            # Rate limiting - small delay between requests
            if i < len(self.orders_to_monitor):
                time.sleep(1)
        
        self.logger.info("=" * 100)
        self.logger.info(
            f"📊 Individual fetch completed - "
            f"Success: {successful_fetches}, Failed: {failed_fetches}"
        )
        
        return successful_fetches > 0
    
    def _fetch_single_order(self, order_uuid: str) -> bool:
        """
        Fetch details for a single order
        
        Args:
            order_uuid (str): UUID of the order to fetch
            
        Returns:
            bool: True if successful, False otherwise
        """
        url = f"{self.BASE_URL}/{self.ENDPOINTS['order_detail']}/{order_uuid}"
        
        try:
            response = requests.get(url, headers=self._get_auth_headers(), timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                order_data = data['data']
                
                # Store order data
                self.order_statuses[order_uuid] = order_data
                
                # Log summary
                status = order_data.get('status', 'unknown')
                settlement_status = order_data.get('settlement_status', 'unknown')
                
                self.logger.info(f"✅ {order_uuid[:8]}... | Status: {status} | Settlement: {settlement_status}")
                
                # Log full response
                self.logger.info(f"📦 Full Response for {order_uuid}:")
                self.logger.info(json.dumps(data, indent=2))
                self.logger.info("-" * 80)
                
                return True
            else:
                self.logger.error(f"❌ Failed to fetch {order_uuid[:8]}... - {response.status_code}: {response.text}")
                return False
                
        except requests.RequestException as e:
            self.logger.error(f"❌ Error fetching {order_uuid[:8]}...: {str(e)}")
            return False
    
    def display_monitoring_dashboard(self) -> None:
        """Display current monitoring status dashboard"""
        self.logger.info("📊 === ORDER MONITORING DASHBOARD ===")
        self.logger.info("-" * 100)
        
        # Count orders by status
        status_counts = defaultdict(int)
        
        for i, order_uuid in enumerate(self.orders_to_monitor, 1):
            if order_uuid in self.order_statuses:
                order_data = self.order_statuses[order_uuid]
                status = order_data.get('status', 'unknown')
                settlement_status = order_data.get('settlement_status', 'unknown')
                updates_count = self.order_updates_received.get(order_uuid, 0)
                
                status_counts[status] += 1
                
                self.logger.info(
                    f"{i:2d}. {order_uuid[:8]}... | "
                    f"Status: {status:10} | "
                    f"Settlement: {settlement_status:6} | "
                    f"Updates: {updates_count}"
                )
            else:
                status_counts['unknown'] += 1
                self.logger.info(
                    f"{i:2d}. {order_uuid[:8]}... | "
                    f"Status: {'UNKNOWN':10} | "
                    f"Settlement: {'?':6} | "
                    f"Updates: 0"
                )
        
        # Display summary
        self.logger.info("-" * 100)
        summary_parts = [f"{status.title()}: {count}" for status, count in status_counts.items()]
        self.logger.info(f"📈 Summary: {', '.join(summary_parts)}")
        
        # Display real-time updates summary
        total_updates = sum(self.order_updates_received.values())
        if total_updates > 0:
            self.logger.info(f"🔥 Total real-time updates received: {total_updates}")
        else:
            self.logger.info("📡 No real-time updates received yet")
    
    def start_monitoring_loop(self) -> None:
        """Start the continuous monitoring loop"""
        self.logger.info("🔄 Starting continuous order monitoring...")
        self.logger.info("   Press Ctrl+C to stop monitoring")
        
        try:
            loop_count = 0
            while self.pusher_connected:
                time.sleep(60)  # Check every minute
                loop_count += 1
                
                self.logger.info(f"⏰ Monitoring Loop #{loop_count} (every 60s)")
                self.display_monitoring_dashboard()
                
        except KeyboardInterrupt:
            self.logger.info("⏹️ Monitoring stopped by user")
        except Exception as e:
            self.logger.error(f"❌ Error in monitoring loop: {str(e)}")
    
    def cleanup(self) -> None:
        """Clean up resources"""
        if self.pusher:
            self.logger.info("🧹 Disconnecting from Pusher...")
            try:
                self.pusher.disconnect()
            except Exception as e:
                self.logger.warning(f"Warning during cleanup: {str(e)}")
    
    def run_full_monitoring_session(self, orders_limit: int = 10) -> bool:
        """
        Run complete monitoring session with all steps
        
        Args:
            orders_limit (int): Maximum number of orders to monitor
            
        Returns:
            bool: True if session completed successfully
        """
        session_start = datetime.now()
        self.logger.info("🚀 Starting SP Order Settlement Monitor")
        self.logger.info("=" * 100)
        self.logger.info(f"Session started at: {session_start.strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info("=" * 100)
        
        try:
            # Step 1: Authentication
            if not self.authenticate():
                self.logger.error("❌ Authentication failed. Exiting.")
                return False
            
            self.logger.info("=" * 100)
            
            # Step 2: Fetch orders from API
            if not self.fetch_orders_from_api(limit=orders_limit):
                self.logger.error("❌ Failed to fetch orders from API. Exiting.")
                return False
            
            self.logger.info("=" * 100)
            
            # Step 3: Setup WebSocket connection
            if not self.setup_websocket_connection():
                self.logger.error("❌ Failed to setup WebSocket connection. Exiting.")
                return False
            
            self.logger.info("=" * 100)
            
            # Step 4: Fetch individual order details
            if not self.fetch_individual_order_details():
                self.logger.warning("⚠️ Failed to fetch some order details. Continuing with monitoring...")
            
            self.logger.info("=" * 100)
            
            # Step 5: Display initial dashboard
            self.display_monitoring_dashboard()
            
            self.logger.info("=" * 100)
            
            # Step 6: Start real-time monitoring
            self.start_monitoring_loop()
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Unexpected error in monitoring session: {str(e)}")
            return False
            
        finally:
            # Cleanup
            self.cleanup()
            
            # Session summary
            session_end = datetime.now()
            duration = session_end - session_start
            
            self.logger.info("=" * 100)
            self.logger.info("📋 FINAL SESSION SUMMARY:")
            self.display_monitoring_dashboard()
            
            total_updates = sum(self.order_updates_received.values())
            self.logger.info(f"🔥 Total real-time updates received: {total_updates}")
            self.logger.info(f"⏱️  Session duration: {duration}")
            self.logger.info(f"✅ Session completed at: {session_end.strftime('%Y-%m-%d %H:%M:%S')}")
            self.logger.info("=" * 100)


def main():
    """
    Main entry point for the Service Provider Order Settlement Monitor
    
    Configure your credentials and run the monitoring session.
    """
    
    # Configuration
    ACCESS_KEY = "61a1830bd55a885574243ad6cf016fef"
    SECRET_KEY = "40d6eea4b5874090105bd7126bf15510"
    ORDERS_LIMIT = 10
    LOG_LEVEL = "INFO"  # Options: DEBUG, INFO, WARNING, ERROR
    
    # Create and run monitor
    monitor = ServiceProviderMonitor(
        access_key=ACCESS_KEY,
        secret_key=SECRET_KEY,
        log_level=LOG_LEVEL
    )
    
    # Run complete monitoring session
    success = monitor.run_full_monitoring_session(orders_limit=ORDERS_LIMIT)
    
    if success:
        print("\n✅ Monitoring session completed successfully!")
    else:
        print("\n❌ Monitoring session encountered errors!")
        exit(1)


if __name__ == "__main__":
    main()