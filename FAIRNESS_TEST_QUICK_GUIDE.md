# Fairness Enhancement - Quick Test Guide

## Requirement Being Validated
> **"Wager placement needs to continue ensuring fairness to all users and SPs. Inside trade, for each 100 jobs in a batch, parallel the request by user id."**

## Quick Start

### 1. Run the Test
```bash
# On staging (recommended)
python stress_test_30k.py --event "Pelicans" --env staging --target 10000

# On sandbox (for testing)
python stress_test_30k.py --event "Cubs" --env sandbox --target 5000
```

### 2. What to Look For

#### ✅ SUCCESS INDICATORS
- **Multiple accounts loaded**: See "Loaded 2 MM accounts"
- **Fairness ratio > 90%**: e.g., "Fairness: 95.5%"
- **Even distribution**: Both users place similar number of wagers
- **Low failure rate**: < 1% failures

#### ❌ FAILURE INDICATORS  
- Only 1 account loaded
- Fairness ratio < 85%
- High failure rate (> 5%)
- One user dominating (placing 80%+ of wagers)

### 3. Sample Output (What Success Looks Like)

```
🚀 STRESS TEST: 30K BET & CANCEL WAGERS (with FAIRNESS validation)
🌍 Environment: staging
🎯 Target Event: Pelicans vs Lakers
🔥 Target Wagers: 10,000
⚡ Concurrent Workers: 10

📦 Loading multiple MM accounts for fairness testing...
✅ Loaded MM account 1: 12345678 (Balance: $1,000,000.00)
✅ Loaded MM account 2: 87654321 (Balance: $1,000,000.00)
✅ Loaded 2 MM accounts for fairness testing

📊 PROGRESS: 2500/10000 placed (25.0%) | 2300 cancelled | 
Rate: 50.0 bets/s, 46.0 cancels/s | 
Fairness: 98.0% (min:1225 max:1275 avg:1250) | 
ETA: 2.5m

╔════════════════════════════════════════════════════════════╗
║               🎉 STRESS TEST COMPLETED 🎉                  ║
╠════════════════════════════════════════════════════════════╣
║ ⏰ Duration:        3.33 minutes                           ║
║ 🎯 Wagers Placed:   10,000                                 ║
║ ✅ Wagers Cancelled: 9,950                                 ║
║ 📊 Placement Rate:  50.0 wagers/sec                        ║
║ 🚀 Cancel Rate:     49.8 cancels/sec                       ║
╠════════════════════════════════════════════════════════════╣
║ ⚖️  FAIRNESS RATIO:  98.00% (min/max placed)              ║  ← KEY METRIC
╠════════════════════════════════════════════════════════════╣
║ 👤 12345678:   4,900 placed |   4,875 cancelled           ║  ← USER 1
║      Avg RT: 120ms | Requests: 245                         ║
║ 👤 87654321:   5,100 placed |   5,075 cancelled           ║  ← USER 2
║      Avg RT: 122ms | Requests: 255                         ║
╚════════════════════════════════════════════════════════════╝

💰 Final Balances:
   12345678: $1,000,000.00
   87654321: $1,000,000.00
```

## Key Validation Points

### 1. Parallelization by User ID ✓
- Each worker processes **100 jobs per batch**
- Jobs are **split evenly** across users
- Users submit requests **in parallel**

### 2. Fairness Metrics ✓
- **Fairness Ratio** = min_placed / max_placed
- **Target**: > 90% fairness
- **Acceptable**: 85-100%
- **Unacceptable**: < 85%

### 3. Performance ✓
- Should maintain **40-60 wagers/sec**
- Similar to single-account performance
- No significant degradation from parallelization

## Quick Troubleshooting

| Issue | Solution |
|-------|----------|
| Only 1 account loads | Check both user JSON files exist |
| Fairness < 85% | Re-run test, may be transient API issue |
| High failures | Check credentials and balance |
| Crash on start | Verify event name exists |

## Pass/Fail Criteria

### ✅ PASS
- 2 accounts loaded
- Fairness ratio ≥ 85%
- Performance ≥ 30 wagers/sec
- Failure rate < 5%

### ❌ FAIL  
- Only 1 account loaded
- Fairness ratio < 85%
- Performance < 30 wagers/sec
- Failure rate > 5%

## Next Steps After Testing

1. **If PASS**: Document results, share with team
2. **If FAIL**: Review logs, check troubleshooting section
3. **For deeper analysis**: See `STRESS_TEST_FAIRNESS_ENHANCEMENT.md`

## Quick Commands Cheat Sheet

```bash
# Small test (5K wagers, 5 workers)
python stress_test_30k.py --event "Lakers" --env staging --target 5000 --workers 5

# Medium test (10K wagers, 10 workers)  
python stress_test_30k.py --event "Lakers" --env staging --target 10000

# Full test (30K wagers, 10 workers)
python stress_test_30k.py --event "Lakers" --env staging

# Sandbox test
python stress_test_30k.py --event "Cubs" --env sandbox --target 3000
```

---

**For detailed documentation, see**: `STRESS_TEST_FAIRNESS_ENHANCEMENT.md`
