
A comprehensive Python tool for monitoring Service Provider orders in real-time using the BetProphet API and WebSocket integration.

## 🚀 Features

- **Dynamic Order Fetching**: Automatically retrieves the latest orders from BetProphet API
- **Real-time WebSocket Monitoring**: Live settlement status updates via Pusher WebSocket
- **Individual Order Details**: Fetches complete order information with full API responses
- **Professional Logging**: Clean, formatted output with multiple log levels
- **Error Handling**: Robust error handling with graceful degradation
- **Session Management**: Complete session tracking with start/end timestamps

## 📋 Prerequisites

### Required Python Packages
```bash
pip install requests pysher
```

### API Credentials
You'll need valid BetProphet API credentials:
- `access_key`: Your Service Provider API access key
- `secret_key`: Your Service Provider API secret key

## 🛠️ Installation

1. **Clone or download the script:**
   ```bash
   wget https://your-repo/sp_settlement_monitor.py
   ```

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure credentials** in the script or via environment variables

## 🎯 Usage

### Basic Usage
```python
python sp_settlement_monitor.py
```

### Programmatic Usage
```python
from sp_settlement_monitor import ServiceProviderMonitor

# Initialize monitor
monitor = ServiceProviderMonitor(
    access_key="your_access_key",
    secret_key="your_secret_key",
    log_level="INFO"
)

# Run complete monitoring session
success = monitor.run_full_monitoring_session(orders_limit=10)
```

### Configuration Options

| Parameter | Default | Description |
|-----------|---------|-------------|
| `access_key` | Required | BetProphet API access key |
| `secret_key` | Required | BetProphet API secret key |
| `log_level` | "INFO" | Logging level (DEBUG, INFO, WARNING, ERROR) |
| `orders_limit` | 10 | Maximum number of orders to fetch |

## 📊 What the Script Does

### Step-by-Step Process

1. **🔐 Authentication**
   - Authenticates with BetProphet API
   - Obtains access token for subsequent requests

2. **📡 Orders List Fetching**
   - Calls: `GET /parlay/sp/orders?limit=10`
   - Displays full API response
   - Extracts order UUIDs for monitoring

3. **🚀 WebSocket Connection**
   - Establishes Pusher WebSocket connection
   - Subscribes to monitoring channels:
     - `sp-orders`
     - `sp-settlements`
     - `parlay-updates`
     - `order-status`

4. **🔍 Individual Order Details**
   - Fetches complete details for each order
   - Calls: `GET /parlay/sp/orders/{order_uuid}`
   - Displays full response body for each order

5. **👂 Real-time Monitoring**
   - Listens for live settlement updates
   - Processes and logs WebSocket events
   - Updates order status in real-time

## 📈 Output Examples

### Orders List Response
```json
{
  "data": {
    "limit": 10,
    "orders": [
      {
        "confirmed_odds": -1100,
        "confirmed_stake": 1100,
        "order_uuid": "01995248-2f6d-7313-bbb5-990e50707fff",
        "p_id": "01995248-1d87-7b30-8a80-572bd11e6db5",
        "settlement_status": "tbd",
        "status": "finalized",
        "updated_at": 1758022090
      }
    ]
  }
}
```

### Individual Order Detail Response
```json
{
  "data": {
    "confirmed_odds": -1100,
    "confirmed_stake": 55.11,
    "legs": null,
    "order_uuid": "01994c7e-4a0c-79b2-aee4-2bbde6608f0b",
    "p_id": "01994c7e-38e4-7ea2-895f-a05a6caa87b1",
    "settlement_status": "won",
    "status": "settled",
    "updated_at": 1758010502
  }
}
```

### Real-time Event Example
```
2025-09-16 18:45:32 | INFO     | 📨 [18:45:32] REAL-TIME ORDER EVENT RECEIVED!
2025-09-16 18:45:32 | INFO     | 🎯 Event: order_settlement
2025-09-16 18:45:32 | INFO     | 📦 Order Data Extracted:
2025-09-16 18:45:32 | INFO     |    order_uuid: 01994c7e-4a0c-79b2-aee4-2bbde6608f0b
2025-09-16 18:45:32 | INFO     |    settlement_status: won
2025-09-16 18:45:32 | INFO     | 🎯 MONITORED ORDER UPDATE! 01994c7e-4a0c-79b2-aee4-2bbde6608f0b
```

## 🏗️ Architecture

### Key Components

1. **ServiceProviderMonitor Class**
   - Main monitoring orchestrator
   - Handles authentication and session management

2. **API Integration**
   - RESTful API calls to BetProphet endpoints
   - Proper error handling and retries

3. **WebSocket Integration** 
   - Pusher WebSocket client
   - Real-time event processing

4. **Logging System**
   - Professional formatted logs
   - Multiple log levels for different use cases

### API Endpoints Used

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/partner/auth/login` | POST | Authentication |
| `/parlay/sp/orders` | GET | List orders |
| `/parlay/sp/orders/{uuid}` | GET | Individual order details |
| `/partner/websocket/connection-config` | GET | WebSocket configuration |
| `/partner/pusher` | POST | WebSocket authentication |

## 🔧 Advanced Configuration

### Environment Variables
```bash
export BETPROPHET_ACCESS_KEY="your_access_key"
export BETPROPHET_SECRET_KEY="your_secret_key"
export BETPROPHET_LOG_LEVEL="DEBUG"
```

### Custom Logging
```python
import logging

# Custom logger configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(name)s | %(levelname)s | %(message)s',
    handlers=[
        logging.FileHandler('sp_monitor.log'),
        logging.StreamHandler()
    ]
)
```

## 🛡️ Error Handling

The script includes comprehensive error handling for:
- Network connectivity issues
- API authentication failures
- WebSocket connection problems
- JSON parsing errors
- Rate limiting

## 📝 Logging Levels

- **DEBUG**: Detailed technical information
- **INFO**: General operational messages (default)
- **WARNING**: Important notices
- **ERROR**: Error conditions

## 🤝 Support

For questions or issues:
1. Check the logs for detailed error information
2. Verify API credentials are correct
3. Ensure network connectivity to BetProphet API
4. Contact BetProphet support for API-specific issues
