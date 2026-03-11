# Investigating DEDUCE Test Failures - Quick Guide

## Test to Run for Investigation

The **Patron Matches MM Wagers** test is ideal for investigating deduce behavior issues:

```bash
# Run this test to investigate deduce behavior
python3 test_deduce_race_conditions.py --test patron_mm --duration 30
```

## Why This Test?

This test simulates a realistic market scenario:
- **MM accounts** (deduce + non-deduce) provide liquidity
- **Patron accounts** (deduce + non-deduce) consume liquidity
- Clear maker-taker relationship
- Easy to track matching behavior

## What to Look For

### 1. Balance Change Analysis

The test shows three balance snapshots:
```
Initial → Intermediate → Final
```

**For DEDUCE accounts (mm1, patron_deduce):**
- ✅ PASS: Balance change = $0.00 throughout
- ✅ PASS: Balance only changes when matched (check intermediate snapshot)
- ❌ FAIL: Balance changes immediately at Initial → Intermediate

**For NON-DEDUCE accounts (mm2, patron_nondeduce):**
- ✅ PASS: Balance changes immediately (Initial → Intermediate shows change)
- ✅ PASS: No further change (Intermediate → Final = $0.00)
- ❌ FAIL: Balance doesn't change or changes late

### 2. Balance Reconciliation

Check the final section:
```
BALANCE RECONCILIATION:
  Total MM balance change:     $XX.XX
  Total Patron balance change: $YY.YY
  Net change (should be ~0):   $Z.ZZ
```

**Expected:**
- Net change should be close to $0 (within $1-2)
- MM losses = Patron losses (they offset each other)

**If imbalanced:**
- Check for unmatched bets in queue
- Check for pending settlements
- Verify all matches were processed

### 3. Matched Bets Verification

The test attempts to get matched bets for all accounts:

**For MM accounts:**
- Uses `partner/mm/get_matched_bets` endpoint
- Should return matched bet history

**For Patron accounts:**
- Uses `api/v2/transaction/wagers/cursor` endpoint
- Filters: `matchingStatus=partially_matched,fully_matched`
- Should return matched wagers

**If matched bets data is available:**
- Verify balance change = sum of matched stakes
- Count should match wager count

**If matched bets data is NOT available:**
- Test uses wager count as proxy
- Checks if balance change is reasonable

### 4. Exposure Check

For MM accounts, check the exposure data:
```
EXPOSURE & PENDING SETTLEMENTS:
  mm1:
    Exposure: {...}
```

**Look for:**
- Pending settlements
- Unmatched wagers
- Liability/exposure amounts

## Common Failure Patterns

### Pattern 1: Deduce Account Shows Immediate Deduction
```
mm1:
  Initial:       $1,000.00
  Intermediate:  $971.00  ← ❌ Changed immediately!
  Final:         $971.00
```

**Issue**: Deduce not working - balance deducted immediately

**Check:**
- Is deduce feature enabled for account?
- Database logs for wallet transactions
- Wallet service configuration

### Pattern 2: Deduce Account Never Deducts
```
mm1:
  Initial:       $1,000.00
  Intermediate:  $1,000.00
  Final:         $1,000.00  ← ❌ Never changed!
  
Matched bets: 29 (total stake: $29.00)
```

**Issue**: Bets matched but balance never deducted

**Check:**
- Settlement service logs
- Match notification events
- Wallet deduction callbacks

### Pattern 3: Imbalanced Reconciliation
```
BALANCE RECONCILIATION:
  Total MM balance change:     $50.00
  Total Patron balance change: $30.00
  Net change (should be ~0):   $20.00  ← ❌ Too large!
```

**Issue**: Mismatched deductions between makers and takers

**Check:**
- Unmatched bets in queue
- Failed settlements
- Double deductions
- Missed deductions

### Pattern 4: Delayed Deduction
```
mm1:
  Initial:       $1,000.00
  Intermediate:  $1,000.00  ← ✅ No change yet
  Final:         $971.00    ← ⚠️  Changed after settlement wait
```

**Status**: Could be normal if settlement is slow

**Verify:**
- Check matched bets timeline
- Compare intermediate vs final timestamps
- Acceptable if < 5 seconds delay

## Detailed Investigation Steps

### Step 1: Run the Test
```bash
python3 test_deduce_race_conditions.py --test patron_mm --duration 30
```

### Step 2: Review Console Output

Look at the sections in order:
1. Account setup - verify all 4 accounts loaded
2. Wager counts - check if bets were placed
3. Balance change analysis - identify anomalies
4. Deduce verification - check PASS/FAIL status
5. Balance reconciliation - verify overall consistency

### Step 3: Check JSON Report

Open the generated report:
```bash
cat race_test_patron_mm_*.json | jq .
```

Key sections:
```json
{
  "wager_counts": {...},  // Bets placed per account
  "match_tracking": [...], // MM->Patron match pairs
  "snapshots": {
    "initial": {...},
    "intermediate": {...},
    "final": {...}
  },
  "balance_reconciliation": {
    "mm_total_change": XX,
    "patron_total_change": YY,
    "balanced": true/false
  },
  "errors": [...]  // Any API errors
}
```

### Step 4: Check External Systems

**Database queries:**
```sql
-- Check wallet transactions for deduce accounts
SELECT * FROM wallet_transactions 
WHERE user_id = '<mm1_user_id>' 
  AND timestamp > NOW() - INTERVAL '5 minutes'
ORDER BY timestamp DESC;

-- Check matched bets
SELECT * FROM matched_bets 
WHERE user_id IN ('<mm1_user_id>', '<patron_deduce_user_id>')
  AND created_at > NOW() - INTERVAL '5 minutes'
ORDER BY created_at DESC;
```

**Service logs:**
```bash
# Wallet service
grep "deduce" /var/log/wallet-service/*.log

# Matching engine
grep "settlement" /var/log/matching-engine/*.log

# Check for errors
grep -i "error\|exception" /var/log/*/*.log | grep -i deduce
```

### Step 5: Compare Behavior

Run the test multiple times to check consistency:
```bash
# Run 3 times
for i in {1..3}; do
  echo "=== Run $i ==="
  python3 test_deduce_race_conditions.py --test patron_mm --duration 20
  sleep 5
done
```

Compare results:
- Is the failure consistent?
- Does it only happen under load?
- Are specific accounts affected?

## Quick Diagnosis Checklist

- [ ] All 4 accounts loaded successfully
- [ ] All accounts placed wagers (count > 0)
- [ ] Zero API errors (or very few)
- [ ] Deduce accounts show $0 change at intermediate snapshot
- [ ] Non-deduce accounts show immediate change at intermediate
- [ ] Final balance changes are reasonable
- [ ] Balance reconciliation shows balanced (< $2 difference)
- [ ] Matched bets data retrieved successfully
- [ ] Matched bets count ≈ wager count
- [ ] No pending settlements stuck

## When to Escalate

Escalate to engineering if:
1. ❌ Deduce accounts show immediate deduction consistently
2. ❌ Deduce accounts never deduct even after settlements
3. ❌ Large imbalance (> $5) in reconciliation consistently
4. ❌ High error rates (> 5%) during test
5. ❌ Database shows mismatched transaction states
6. ❌ Logs show repeated errors or exceptions

## Additional Test Options

### Longer Duration Test
```bash
# 2-minute test for more data
python3 test_deduce_race_conditions.py --test patron_mm --duration 120
```

### Run All Tests
```bash
# Comprehensive test suite
python3 test_deduce_race_conditions.py --test all --duration 30
```

### Compare with 4-Way Test
```bash
# Different matching pattern
python3 test_deduce_race_conditions.py --test 4way --duration 30
```

## Report Format

When reporting issues, include:
1. Test command used
2. Console output (full or last 100 lines)
3. JSON report file
4. Database query results (if accessible)
5. Relevant log snippets
6. Grafana/metrics screenshots (if available)

## Summary

The **patron_mm** test provides the clearest view of deduce behavior by:
- ✅ Separating makers (MM) from takers (Patron)
- ✅ Multiple balance snapshots (initial, intermediate, final)
- ✅ Balance reconciliation check
- ✅ Matched bets verification for all account types
- ✅ Exposure and settlement tracking
- ✅ Detailed error tracking

Use this test as your primary investigation tool for deduce-related issues.
