# Stress Test 30K - Fairness Enhancement Documentation

## Overview
This document describes the **Fairness Enhancement** implemented in `stress_test_30k.py` to validate the new requirement:

> **"Wager placement needs to continue ensuring fairness to all users and SPs. Inside trade, for each 100 jobs in a batch, parallel the request by user id."**

## What Was Changed

### 1. Multi-Account Support
- **Before**: Single MM account used for all wager placements
- **After**: Loads multiple MM accounts (Account 1 & Account 2) to simulate multiple users/SPs
- Uses `config.get_account_credentials()` to load credentials for each account

### 2. Parallelization by User ID
- **Key Implementation**: `stress_test_worker_parallel()` function
- For every **100 jobs** in a batch:
  - Jobs are distributed evenly across all available user accounts
  - Requests are parallelized by user ID using `ThreadPoolExecutor`
  - Each user processes their share of the batch simultaneously
  
**Example**: With 2 users and 100 jobs:
- User A gets 50 wagers
- User B gets 50 wagers  
- Both users submit requests in parallel

### 3. Fairness Tracking & Metrics
New metrics tracked per user:
- **Placed**: Number of wagers successfully placed
- **Cancelled**: Number of wagers cancelled
- **Failed**: Number of failed placement attempts
- **Response Time**: Average API response time
- **Request Count**: Total API requests made

**Fairness Ratio** = min(placed) / max(placed)
- **100%**: Perfect fairness (all users placed equal wagers)
- **80-99%**: Good fairness  
- **<80%**: May indicate unfairness

### 4. Enhanced Progress Monitoring
Real-time fairness metrics displayed during execution:
```
📊 PROGRESS: 5000/30000 placed (16.7%) | 4500 cancelled | 
Rate: 45.2 bets/s, 40.8 cancels/s | 
Fairness: 98.5% (min:2450 max:2550 avg:2500) | 
ETA: 9.2m
```

### 5. Detailed Final Report
```
╔════════════════════════════════════════════════════════════╗
║               🎉 STRESS TEST COMPLETED 🎉                  ║
╠════════════════════════════════════════════════════════════╣
║ ⏰ Duration:        10.50 minutes                          ║
║ 🎯 Wagers Placed:   30,000                                 ║
║ ✅ Wagers Cancelled: 29,800                                ║
║ 📊 Placement Rate:  47.6 wagers/sec                        ║
║ 🚀 Cancel Rate:     47.1 cancels/sec                       ║
╠════════════════════════════════════════════════════════════╣
║ ⚖️  FAIRNESS RATIO:  98.67% (min/max placed)              ║
╠════════════════════════════════════════════════════════════╣
║ 👤 12345678: 14,800 placed | 14,700 cancelled             ║
║      Avg RT: 115ms | Requests: 740                         ║
║ 👤 87654321: 15,200 placed | 15,100 cancelled             ║
║      Avg RT: 118ms | Requests: 760                         ║
╚════════════════════════════════════════════════════════════╝

💰 Final Balances:
   12345678: $1,000,000.00
   87654321: $1,000,000.00
```

## How to Run

### Prerequisites
Ensure you have valid credentials in:
- **Sandbox**: `src/user_info.json` and `src/user_info_account2.json`
- **Staging**: `src/user_info_staging.json` and `src/user_info_account2_staging.json`

### Basic Usage
```bash
# Run on staging with default settings (30K wagers, 10 workers)
python stress_test_30k.py --event "Pelicans" --env staging

# Run with custom target and workers
python stress_test_30k.py --event "Lakers" --env staging --target 10000 --workers 5

# Run on sandbox
python stress_test_30k.py --event "Cubs" --env sandbox --target 5000
```

### Command Line Arguments
- `--event`: Event name to target (required, partial match supported)
- `--env`: Environment (`sandbox` or `staging`, default: `sandbox`)
- `--target`: Target number of wagers (default: `30000`)
- `--workers`: Number of concurrent workers (default: `10`)

## Technical Details

### Architecture

```
Main Thread
    └── load_multiple_mm_accounts()
            ├── Account 1 (MM Instance 1)
            └── Account 2 (MM Instance 2)
    
    └── ThreadPoolExecutor (num_workers)
            ├── Worker 1 → stress_test_worker_parallel()
            ├── Worker 2 → stress_test_worker_parallel()
            └── Worker N → stress_test_worker_parallel()

Each Worker:
    └── For each 100 jobs batch:
            └── ThreadPoolExecutor (per user)
                    ├── User A → place_batch_wagers() [batches of 20]
                    └── User B → place_batch_wagers() [batches of 20]
```

### Batch Processing Flow
1. **Job Distribution**: 100 jobs per batch
2. **User Split**: Jobs divided evenly among all users
3. **Parallel Execution**: Each user places wagers simultaneously
4. **API Batching**: Max 20 wagers per API call
5. **Cancellation**: Accumulated wagers cancelled when buffer ≥40
6. **Fairness Tracking**: All metrics updated in thread-safe manner

### Thread Safety
- `wagers_lock`: Protects global wager counts
- `metrics_lock`: Protects per-user metrics dictionary
- Thread-safe operations ensure accurate reporting

## Validation Checklist

When reviewing test results, verify:

✅ **Multiple users loaded**: Check log for "Loaded X MM accounts"  
✅ **Fair distribution**: Fairness ratio should be >85%  
✅ **Similar wager counts**: Each user should place similar number of wagers  
✅ **Similar response times**: Average response times should be comparable  
✅ **No failures**: Failed count should be 0 or very low  
✅ **Balance unchanged**: Final balances should match initial (DEDUCE feature)  
✅ **Performance maintained**: Placement rate should be >30 wagers/sec  

## Troubleshooting

### Issue: Only 1 account loaded
**Solution**: Check that both user JSON files exist and have valid credentials

### Issue: Low fairness ratio (<80%)
**Possible causes**:
- One account hitting rate limits
- Network issues affecting one account
- API throttling one user more than others

### Issue: High failure rate
**Possible causes**:
- Invalid credentials
- Insufficient balance
- Market/event no longer available
- API rate limiting

### Issue: Script crashes during execution
**Solution**: Check logs for specific error, ensure valid event name

## Code Modifications Summary

### New Functions
- `load_multiple_mm_accounts()`: Loads multiple MM accounts
- `stress_test_worker_parallel()`: Worker with user ID parallelization

### Modified Functions
- `place_single_wager()`: Added user_id tracking
- `place_batch_wagers()`: Added user_id tracking and metrics
- `cancel_wagers()`: Added user_id tracking
- `progress_monitor()`: Added fairness metrics display
- `run_stress_test()`: Uses multiple accounts and parallel workers

### New Global Variables
- `user_metrics`: Per-user statistics dictionary
- `metrics_lock`: Thread lock for metrics access

## Performance Impact

### Expected Behavior
- **Throughput**: Should maintain or slightly improve (parallel requests)
- **Fairness**: Should achieve >90% fairness ratio
- **Resource Usage**: Increased due to multiple accounts and parallelization
- **API Load**: More concurrent connections, but total request volume same

### Benchmarks (Expected)
- **Placement Rate**: 40-60 wagers/sec (2 accounts)
- **Fairness Ratio**: 95-100%
- **Failure Rate**: <1%
- **Response Time**: 100-150ms per request

## Future Enhancements

Potential improvements:
1. **Dynamic User Scaling**: Support 3+ accounts
2. **Adaptive Batch Size**: Adjust batch size based on performance
3. **Real-time Fairness Adjustment**: Throttle faster users to maintain fairness
4. **Historical Tracking**: Save fairness metrics to database
5. **Alerting**: Alert if fairness ratio drops below threshold

## Related Documents
- Original test plan: Test Cases PDF
- DEDUCE testing: `DEDUCE_TEST_REPORT.md`
- API documentation: BetProphet MM API docs

---

**Last Updated**: January 5, 2025  
**Author**: Warp AI Agent  
**Version**: 1.0
