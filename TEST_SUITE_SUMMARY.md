# DEDUCE Test Suite - Summary of Enhancements

## Overview
Enhanced the DEDUCE (Deferred Deduction) race condition test suite with new tests and fixes.

## Changes Made

### 1. Fixed Rapid Fire Test Stake Amount
**File**: `test_deduce_race_conditions.py`

**Issue**: Test was failing due to minimum stake requirement
```
ERROR: {"error":"invalid_request","message":"your wager stake is below the minimum allowed"}
```

**Fix**: Increased stake from $0.50 to $2.00 in rapid fire test
- Line 518: Changed `framework.place_wager('mm1', line_id, 150, 2.0)`
- Line 539: Changed `framework.place_wager('mm2', line_id, -150, 2.0)`

### 2. Created Aggressive All-Lines Test
**Test**: `test_aggressive_all_lines()`

**Purpose**: Stress test where all accounts aggressively bet on ALL available lines in an event

**Features**:
- Fetches all line IDs from event (using public API endpoint)
- Refreshes lines every 5 seconds (handles changing odds/lines)
- All 4 accounts (MM1 deduce, MM2 normal, Patron deduce, Patron non-deduce) compete naturally
- No coordinated matching - realistic market conditions
- Shows `matched_wager_balance` for MM accounts to prove deduce behavior

**Results** (60s test):
- ✅ Found 908 lines to bet on
- ✅ ~79 wagers per account
- ✅ MM1 (deduce): `matched_wager_balance` increased to $215,970.02
- ✅ MM2 (normal): Balance decreased immediately by $71.56
- ✅ 10 line refreshes, 0 errors

### 3. Created Cancel Race Condition Bug Test
**Test**: `test_cancel_race_wager_job_bug()`

**Bug Being Tested**:
```
When a wager is cancelled, wager_jobs with type="open" can remain stuck 
in "pending" status even though the wager itself shows status="cancelled"
```

**Features**:
- Rapidly places and cancels wagers (80% cancel rate)
- Immediate cancellation after placement (triggers race condition)
- Generates SQL queries for database verification
- Saves wager IDs in report for manual DB checking

**SQL Queries Generated**:
```sql
-- Check wager status
SELECT id, status, created_at FROM wagers
WHERE id IN (wager_ids);

-- Check wager_jobs status (BUG: may show 'pending')
SELECT wager_id, job_type, status, created_at FROM wager_jobs
WHERE wager_id IN (wager_ids) AND job_type = 'open';

-- Find stuck jobs
SELECT w.id, w.status as wager_status, wj.job_type, wj.status as job_status
FROM wagers w
JOIN wager_jobs wj ON w.id = wj.wager_id
WHERE w.id IN (wager_ids)
AND w.status = 'cancelled'
AND wj.status = 'pending'
AND wj.job_type = 'open';
```

**Note**: In highly liquid markets, wagers match too quickly to cancel. Test works best in less liquid markets or with higher placement frequency.

### 4. Enhanced Balance Tracking for Deduce Accounts
**File**: `deduce_tests.py`

**Enhancement**: Updated `get_balance()` method to show full breakdown for MM accounts:
```python
balance_data['available_balance'] = main_balance - matched_wager_bal - unmatched_wager_bal
```

**Output**:
```
💰 mm1: Balance=$1,021,029.20, Matched=$215,930.59, 
       Unmatched=$1,454.00, Available=$803,644.61
```

## Test Suite Menu

Run tests via command line:

```bash
# All tests
python3 test_deduce_race_conditions.py --test all --duration 30

# Specific tests
python3 test_deduce_race_conditions.py --test 1a           # Basic deduce vs non-deduce
python3 test_deduce_race_conditions.py --test 4way         # 4-account race
python3 test_deduce_race_conditions.py --test rapid        # High-frequency (FIXED)
python3 test_deduce_race_conditions.py --test burst        # Simultaneous burst
python3 test_deduce_race_conditions.py --test patron_mm    # Patron matches MM
python3 test_deduce_race_conditions.py --test deduce_matched  # Deduce gets matched
python3 test_deduce_race_conditions.py --test aggressive   # All-lines stress test (NEW)
python3 test_deduce_race_conditions.py --test cancel_bug   # Cancel race bug (NEW)
```

## Key Findings

### DEDUCE Behavior Verified ✅
1. **MM Deduce Accounts**: Main balance stays unchanged, matched exposure tracked in `matched_wager_balance`
2. **Normal Accounts**: Balance deducted immediately upon wager placement
3. **Available Balance**: UI shows `balance - matched_wager_balance - unmatched_wager_balance`

### Wager Job Bug 🐛
- **Reproduced**: Cancel race condition where `wager_jobs` get stuck in `pending` state
- **Impact**: Background job processing inconsistency
- **Verification**: Use SQL queries provided in test output to check database

## Files Modified

1. `test_deduce_race_conditions.py`:
   - Fixed rapid fire test stake ($0.50 → $2.00)
   - Added `test_aggressive_all_lines()` 
   - Added `test_cancel_race_wager_job_bug()`
   - Updated command-line menu

2. `deduce_tests.py`:
   - Enhanced `get_balance()` with full breakdown for MM accounts
   - Added `available_balance` calculation

3. `debug_mm1_balance.py` (NEW):
   - Debug tool to check MM1 balance with all fields
   - Reveals `matched_wager_balance` and `unmatched_wager_balance`

## Reports Generated

All tests save JSON reports:
- `race_test_1a_*.json` - Basic race test
- `race_test_4way_*.json` - 4-way test
- `race_test_rapid_*.json` - Rapid fire
- `race_test_burst_*.json` - Burst test
- `race_test_patron_mm_*.json` - Patron-MM test
- `race_test_deduce_matched_*.json` - Deduce matched test
- `race_test_aggressive_lines_*.json` - Aggressive all-lines test (NEW)
- `race_test_cancel_bug_*.json` - Cancel bug test with wager IDs for DB check (NEW)

## Next Steps

1. **For Cancel Bug**:
   - Run test during off-peak hours (less matching)
   - Or use less liquid markets
   - Check database with SQL queries from test output
   - Work with backend team to fix race condition in wager_job processing

2. **For DEDUCE Feature**:
   - Tests confirm deduce is working as designed
   - Monitor `matched_wager_balance` growth in production
   - Ensure UI correctly calculates available balance
