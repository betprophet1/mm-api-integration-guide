# Backend Fairness Test - Continuous Mode

## Overview

The continuous mode script (`test_backend_fairness_continuous.py`) extends the backend fairness testing capabilities with automatic token management and continuous operation support.

## Key Features

### 1. **Automatic Token Refresh**
- Monitors token expiration automatically
- Refreshes access tokens before they expire (60-second buffer)
- Falls back to re-authentication if refresh fails
- Thread-safe token management with locks

### 2. **Resilient Error Handling**
- Automatic retry logic for failed wagers (up to 3 attempts)
- Exponential backoff for account loading
- Handles 401 Unauthorized errors gracefully
- Connection error recovery

### 3. **Batch Processing**
- Processes wagers in configurable batches
- Supports unlimited or limited batch runs
- 1-second pause between batches to avoid overwhelming the system

### 4. **Progress Monitoring**
- Real-time progress reports every 30 seconds
- Tracks token refreshes and re-authentications
- Monitors fairness metrics during execution
- Per-batch and overall statistics

### 5. **Graceful Shutdown**
- Ctrl+C handling for clean shutdown
- Completes current batch before stopping
- Generates final reports and saves results

## Usage

### Basic Command
```bash
python3 test_backend_fairness_continuous.py --event <EVENT_ID>
```

### All Options
```bash
python3 test_backend_fairness_continuous.py \
  --event <EVENT_ID>              # Event ID or name (required)
  --batch-size 1000               # Wagers per batch (default: 1000)
  --total-batches 10              # Number of batches (default: unlimited)
  --env sandbox                   # Environment: sandbox or staging
  --workers 50                    # Max concurrent workers (default: 50)
  --accounts 10                   # Number of accounts (default: 10)
  --start-account 1               # Starting account number (default: 1)
```

### Examples

#### Run 5 batches with accounts 6-10
```bash
python3 test_backend_fairness_continuous.py \
  --event 20023191 \
  --batch-size 1000 \
  --total-batches 5 \
  --accounts 5 \
  --start-account 6
```

#### Continuous mode until stopped (Ctrl+C)
```bash
python3 test_backend_fairness_continuous.py \
  --event 20023191 \
  --batch-size 500 \
  --accounts 10
```

#### High-throughput test
```bash
python3 test_backend_fairness_continuous.py \
  --event 20023191 \
  --batch-size 2000 \
  --workers 100 \
  --accounts 10
```

## Output

### Console Output

The script provides real-time feedback:

```
======================================================================
BACKEND FAIRNESS TEST - CONTINUOUS MODE
======================================================================
Environment: sandbox
Target event: 20023191
Batch size: 1000
Total batches: Unlimited
Max workers: 50
======================================================================

📦 Loading MM accounts 1-10...
✅ Loaded 10 accounts

======================================================================
📊 BATCH #1
======================================================================
✅ Batch #1 completed:
   Placed: 998/1000
   Duration: 15.23s
   Rate: 65.5 wagers/sec
   Total wagers so far: 998

======================================================================
PROGRESS REPORT
======================================================================
Total runtime: 0.5 minutes
Total batches: 2
Total placed: 1,996
Total failed: 4
Overall rate: 66.5 wagers/sec
Token refreshes: 0
Re-authentications: 0
Current fairness: 99.80%
```

### Saved Results

Results are saved to `backend_fairness_continuous_<timestamp>.json`:

```json
{
  "test_config": {
    "mode": "continuous",
    "environment": "sandbox",
    "event": "20023191",
    "batch_size": 1000,
    "total_batches": 10,
    "num_users": 10,
    "duration_seconds": 152.45
  },
  "per_user_metrics": {
    "user_uuid_1": {
      "placed": 998,
      "matched": 0,
      "failed": 2,
      "response_times": [0.85, 0.92, ...],
      "timestamps": [1706876543.21, ...]
    },
    ...
  }
}
```

## Token Management

### How It Works

1. **Proactive Refresh**: Tokens are checked before each API call
2. **60-Second Buffer**: Tokens are refreshed 60 seconds before expiration
3. **Fallback Authentication**: If refresh fails, re-authenticates from scratch
4. **Thread-Safe**: Uses locks to prevent race conditions in concurrent operations

### Token Lifecycle

```
┌─────────────────────────────────────────────────────────────┐
│ 1. Initial Login                                            │
│    - Get access_token (expires in 20 min)                   │
│    - Get refresh_token (expires in 72 hours)                │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│ 2. Continuous Operation                                     │
│    - Before each API call: check if token expires in <60s   │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
              ┌────────┴────────┐
              │ Token OK?       │
              └────┬─────────┬──┘
                   │ Yes     │ No
                   │         │
                   │         ▼
                   │   ┌──────────────────────┐
                   │   │ 3. Refresh Token     │
                   │   │    - Use refresh_token│
                   │   │    - Get new access   │
                   │   └──────┬────────────────┘
                   │          │
                   │          ▼
                   │   ┌──────────────┐
                   │   │ Refresh OK?  │
                   │   └──┬────────┬──┘
                   │      │ Yes    │ No
                   │      │        │
                   │      │        ▼
                   │      │   ┌─────────────────────┐
                   │      │   │ 4. Re-authenticate  │
                   │      │   │    - Full login     │
                   │      │   │    - New tokens     │
                   │      │   └─────────────────────┘
                   │      │
                   ▼      ▼
              ┌────────────────┐
              │ 5. API Call    │
              └────────────────┘
```

## Monitoring

### Real-Time Metrics

- **Placement Rate**: Wagers per second
- **Fairness**: Distribution across users
- **Token Stats**: Refreshes and re-authentications
- **Error Rate**: Failed wagers

### Final Report

After completion (or Ctrl+C), the script generates:
- Per-user fairness analysis
- Overall statistics
- Token management summary
- JSON results file

## Best Practices

### 1. **Batch Sizing**
- **Small batches (500-1000)**: Better for monitoring, more frequent progress updates
- **Large batches (2000-5000)**: Higher throughput, less overhead

### 2. **Worker Count**
- **Default (50)**: Balanced for most scenarios
- **High (100-200)**: Maximum throughput, may hit rate limits
- **Low (20-30)**: Conservative, good for testing

### 3. **Account Selection**
- **All accounts (1-10)**: Maximum fairness testing
- **Subset (6-10)**: Test specific accounts
- **Single account**: Baseline performance testing

### 4. **Duration**
- **Limited batches**: Controlled test runs
- **Unlimited**: Long-running stress tests (use Ctrl+C to stop)

## Troubleshooting

### Token Refresh Failures

If you see multiple refresh failures:
```
⚠️  Token refresh failed: 401
🔐 Refresh failed, re-authenticating...
```

**Solutions:**
- Check network connectivity
- Verify account credentials in `user_info_account*.json`
- Ensure refresh tokens haven't expired (72-hour limit)

### High Failure Rate

If wagers are failing:
```
❌ User abc12345 wager failed: profit must be greater than 1 cent
```

**Solutions:**
- Increase stake amount (currently $2.00)
- Check if markets are still active
- Verify event hasn't ended

### Connection Errors

```
⚠️  Wager attempt 1 failed, retrying: Max retries exceeded
```

**Solutions:**
- Check network stability
- Reduce worker count (`--workers 30`)
- Reduce batch size (`--batch-size 500`)

## Comparison: Regular vs Continuous Mode

| Feature | Regular Mode | Continuous Mode |
|---------|-------------|-----------------|
| Token Management | Manual | Automatic |
| Long Running | No (tokens expire) | Yes (refreshes) |
| Error Recovery | Basic | Advanced retry logic |
| Batch Processing | Single run | Multiple batches |
| Progress Tracking | End only | Real-time updates |
| Graceful Shutdown | No | Yes (Ctrl+C) |

## Technical Details

### Token Refresh Implementation

```python
def ensure_valid_token(self) -> bool:
    """Ensure we have a valid token, refresh or re-authenticate if needed"""
    if self.is_token_expired():
        logging.info("🔄 Token expired or expiring soon, attempting refresh...")
        if not self.refresh_access_token():
            logging.info("🔐 Refresh failed, re-authenticating...")
            try:
                self.mm_login()
                self.session_stats['re_authentications'] += 1
                logging.info("✅ Re-authentication successful")
                return True
            except Exception as e:
                logging.error(f"❌ Re-authentication failed: {str(e)}")
                return False
    return True
```

### Wager Retry Logic

```python
def place_wager_with_retry(mm_instance, user_id, line_id, odds, wager_num, max_retries=3):
    """Place wager with automatic token refresh and retry"""
    for attempt in range(max_retries):
        try:
            # Ensure token is valid before placing wager
            if not mm_instance.ensure_valid_token():
                logging.warning(f"⚠️  Token validation failed for user {user_id[:8]}")
                time.sleep(1)
                continue
            
            # Place wager...
            
        except Exception as e:
            if attempt < max_retries - 1:
                logging.warning(f"⚠️  Wager attempt {attempt + 1} failed, retrying: {str(e)}")
                time.sleep(0.5)
            else:
                with metrics_lock:
                    user_metrics[user_id]['failed'] += 1
```

## See Also

- `test_backend_fairness.py` - Original single-run fairness test
- `src/mm_calls.py` - Core MM API interaction class
- `verify_fairness_*.sql` - Database verification queries
