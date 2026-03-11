# Stress Test: 30K Bet & Cancel Wagers

## Overview
This stress test is designed to spawn **30,000 bet & cancel wagers** targeting a single event. It uses concurrent workers to rapidly place and cancel bets using the MM API.

## Features
- ✅ Targets a single event by name
- ✅ Places 30,000 wagers using batch API (20 wagers per call)
- ✅ Immediately cancels wagers after placement
- ✅ Multi-threaded execution with configurable workers (default: 10)
- ✅ Real-time progress monitoring (every 5 seconds)
- ✅ Comprehensive final report with rates and statistics
- ✅ Graceful shutdown with Ctrl+C

## Requirements
- Python 3.7+
- Valid MM API credentials configured in `src/user_info.json`
- Available MLB events in the system

## Usage

### Basic Usage
```bash
python stress_test_30k.py --event "Yankees"
```

### With Custom Workers
```bash
python stress_test_30k.py --event "Yankees" --workers 20
```

### With Custom Target
```bash
python stress_test_30k.py --event "Yankees" --target 50000 --workers 15
```

## Command Line Arguments

| Argument | Description | Default | Required |
|----------|-------------|---------|----------|
| `--event` | Target event name (partial or exact match) | - | Yes |
| `--workers` | Number of concurrent workers | 10 | No |
| `--target` | Target number of wagers to place | 30000 | No |

## How It Works

### Architecture
```
Main Thread
├── Progress Monitor (reports every 5s)
└── Thread Pool (N workers)
    ├── Worker 1: Place batch → Cancel batch
    ├── Worker 2: Place batch → Cancel batch
    ├── Worker 3: Place batch → Cancel batch
    └── ...
```

### Strategy
1. **Login & Seed**: Authenticate and load available events
2. **Find Event**: Match the target event by name
3. **Spawn Workers**: Create N concurrent workers (default: 10)
4. **Each Worker**:
   - Places 20 wagers per batch call
   - Buffers wagers until 40 accumulated
   - Cancels 20 at a time when buffer reaches 40
   - Repeats until quota reached
5. **Progress Monitor**: Reports stats every 5 seconds
6. **Final Cleanup**: Cancels any remaining wagers

### API Usage
- **Batch Place**: `place_multiple_wagers` (max 20 wagers/call)
- **Batch Cancel**: `cancel_multiple_wagers` (max 20 wagers/call)
- **Rate Limiting**: 50ms delay between batch calls per worker

## Performance Expectations

### With 10 Workers
- **Expected Rate**: ~200-400 wagers/second
- **Estimated Time**: 1.5-2.5 minutes for 30k wagers
- **API Calls**: ~1,500 place calls + ~1,500 cancel calls

### With 20 Workers
- **Expected Rate**: ~400-800 wagers/second
- **Estimated Time**: 0.75-1.25 minutes for 30k wagers
- **API Calls**: ~1,500 place calls + ~1,500 cancel calls

*Note: Actual performance depends on API response time and network latency*

## Example Output

```
🚀 STRESS TEST: 30K BET & CANCEL WAGERS
🎯 Target Event: Yankees vs Red Sox
🔥 Target Wagers: 30,000
⚡ Concurrent Workers: 10
✅ Found event: Yankees vs Red Sox
✅ Using market: moneyline, selection: Yankees

🔥 Worker 1: Starting with target 3000 wagers
🔥 Worker 2: Starting with target 3000 wagers
...

📊 PROGRESS: 5420/30000 placed (18.1%) | 2180 cancelled | 
           Rate: 271.0 bets/s, 109.0 cancels/s | ETA: 1.5m

📊 PROGRESS: 12840/30000 placed (42.8%) | 7420 cancelled | 
           Rate: 321.0 bets/s, 185.5 cancels/s | ETA: 0.9m

...

✅ Worker 1: Completed 3000 wagers
✅ Worker 2: Completed 3000 wagers
...

╔════════════════════════════════════════════════════════════╗
║               🎉 STRESS TEST COMPLETED 🎉                  ║
╠════════════════════════════════════════════════════════════╣
║ ⏰ Duration:        1.67 minutes                           ║
║ 🎯 Wagers Placed:   30,000                                 ║
║ ✅ Wagers Cancelled: 29,987                                ║
║ 📊 Placement Rate:  299.4 wagers/sec                       ║
║ 🚀 Cancel Rate:     299.3 cancels/sec                      ║
║ 💰 Final Balance:   $10000.00                              ║
╚════════════════════════════════════════════════════════════╝
```

## Configuration

### Environment
Set your environment in the shell:
```bash
export MM_ENVIRONMENT=sandbox  # or 'staging'
```

### Credentials
Ensure your `src/user_info.json` (for sandbox) or `src/user_info_staging.json` (for staging) contains valid credentials:
```json
{
  "access_key": "your-access-key",
  "secret_key": "your-secret-key",
  "tournaments": ["MLB"]
}
```

## Troubleshooting

### No events found
- Make sure there are active MLB events
- Try a broader search term (e.g., use "Yankees" instead of full team name)
- Run `python -m src.main` to see available events

### API Rate Limiting
- Reduce the number of workers with `--workers 5`
- Increase the delay in `stress_test_worker()` (line 188)

### Balance Issues
- The test uses $1 stake per wager
- For 30k wagers, you need sufficient balance (though wagers are cancelled quickly)
- Check balance with: `python -c "from src import mm_calls; m=mm_calls.MMInteractions(); m.mm_login(); m.get_balance()"`

### Worker Errors
- Check logs for specific error messages
- Verify network connectivity
- Ensure API credentials are valid and not expired

## Advanced Usage

### Dry Run (Test Configuration)
To test with fewer wagers first:
```bash
python stress_test_30k.py --event "Yankees" --target 100 --workers 2
```

### Maximum Stress
For higher load, increase workers and target:
```bash
python stress_test_30k.py --event "Yankees" --target 100000 --workers 30
```

### Monitor System Resources
Run with system monitoring:
```bash
# Terminal 1
python stress_test_30k.py --event "Yankees"

# Terminal 2
watch -n 1 'ps aux | grep stress_test_30k'
```

## Safety Notes
⚠️ **WARNING**: This script places real wagers (though it cancels them immediately)

- Only run in **sandbox** or **staging** environments
- The script checks for production URLs and will error if detected
- Cancelled wagers may still briefly appear in your account
- Monitor your balance during execution

## Comparison with auto_play_cancel.py

| Feature | stress_test_30k.py | auto_play_cancel.py |
|---------|-------------------|---------------------|
| Target | Single event | All MLB events or specific event |
| Concurrency | Multi-threaded (10+ workers) | Single-threaded with scheduler |
| Goal | Reach specific wager count | Continuous operation |
| Cancellation | Immediate (buffered) | Scheduled intervals |
| Best for | Load testing, specific targets | Continuous stress, long-running |

## Support
For issues or questions:
1. Check the logs for error messages
2. Verify your configuration and credentials
3. Review the API documentation
4. Reduce load (fewer workers/target) to isolate issues
