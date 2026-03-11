# Test Suite Summary: Deduce Race Conditions

**Script**: `test_deduce_race_conditions.py`  
**Location**: `/Users/tranlam/Documents/GitHub/mm-api-integration-guide/`  
**Language**: Python 3  
**Last Updated**: January 27, 2026

---

## Overview
This test suite validates the **DEDUCE feature** under high-stress, concurrent, and edge-case scenarios. DEDUCE is a payment model where money is only deducted from accounts when bets are matched, rather than when they're placed.

### What is DEDUCE?
- **Traditional Model**: Money deducted immediately when bet is placed
- **DEDUCE Model**: Money deducted only when bet is matched with another user
- **Business Value**: Better liquidity management and user experience

### What This Suite Tests:
- ✅ Correct balance deductions (deduce vs non-deduce accounts)
- ✅ No double-deductions or missed deductions
- ✅ System stability under concurrent operations
- ✅ Proper handling of rapid bet placement and cancellations
- ✅ Cross-account-type matching (MM ↔ Patron)
- ✅ Live event betting delays and restrictions

---

## Quick Start

### Prerequisites
```bash
# Ensure you have Python 3 installed
python3 --version

# Navigate to the project directory
cd /Users/tranlam/Documents/GitHub/mm-api-integration-guide

# Install required dependencies (if needed)
pip3 install requests pysher
```

### Run All Tests (Recommended for CI/CD)
```bash
python3 test_deduce_race_conditions.py --test all --duration 30
```

### Run Single Test (Quick Validation)
```bash
# Basic race condition test (fastest)
python3 test_deduce_race_conditions.py --test 1a --duration 30

# Patron matching test (2 MMs + 2 Patrons)
python3 test_deduce_race_conditions.py --test patron_mm --duration 30

# Live event 5s delay verification
python3 test_deduce_race_conditions.py --test live_delay --event-id 30024797
```

### Common Options
```bash
--test <name>        # Which test to run (see test list below)
--duration <seconds> # How long to run the test (default: 30)
--event-id <id>      # Specific event ID (required for some tests)
--rps <number>       # Target requests per second (for cancel_bug test)
```

---

## Test Inventory

### 1. **Test 1A: Deduce SP vs Non-Deduce SP Race**
**Test Name**: `1a`  
**Duration**: 30s (configurable)  
**Accounts**: 2 MMs

**Purpose**: Basic two-account race condition testing  

**What it does**:
- Two market maker (MM) accounts place opposite bets simultaneously
- **MM1** uses DEDUCE (deduct on match), **MM2** uses normal (deduct immediately)
- Uses **random odds from odds ladder API** (not fixed values)
- Runs for configurable duration with aggressive betting (every 0.075s)

**What we verify**:
- ✅ MM1 balance stays unchanged until bets match
- ✅ MM2 balance decreases immediately on bet placement
- ✅ No balance inconsistencies under concurrent operations
- ✅ Works with varying odds from odds ladder

**Run command**:
```bash
python3 test_deduce_race_conditions.py --test 1a --duration 30
```

**Business value**: Ensures basic DEDUCE behavior works correctly when competing with traditional accounts

---

### 2. **Test: 4-Way Mexican Standoff**
**Purpose**: Complex multi-party race condition testing  
**What it does**:
- 4 accounts bet simultaneously (2 MM + 2 Patron accounts)
- Mix of DEDUCE and non-DEDUCE accounts on both sides
- All accounts compete for matching in real-time

**What we verify**:
- Correct matching priority
- Balance changes match actual matched bets
- DEDUCE accounts only deducted when matched (not on placement)

**Business value**: Tests real-world scenarios with multiple account types competing simultaneously

---

### 3. **Test: Rapid Fire Race Condition**
**Purpose**: High-frequency stress testing  
**What it does**:
- MM1 (DEDUCE) places bets as fast as possible
- MM2 tries to match all bets immediately
- Default: 40 bets/second for 20 seconds

**What we verify**:
- No double-deductions under high load
- Balance consistency (deducted amount = matched amount)
- System handles rapid concurrent operations

**Business value**: Ensures system stability under high trading volumes

---

### 4. **Test: Simultaneous Burst**
**Purpose**: True simultaneous operation testing  
**What it does**:
- 4 accounts place bets at the EXACT same time (within 10ms)
- Runs 10 synchronized bursts
- Tests matching engine's handling of truly simultaneous requests

**What we verify**:
- Matching engine handles concurrent requests correctly
- No race conditions in bet processing
- Balance updates are atomic and correct

**Business value**: Tests absolute worst-case scenario for race conditions

---

### 5. **Test: Deduce Accounts Get Matched**
**Purpose**: Timing verification for DEDUCE balance deductions  
**What it does**:
- DEDUCE accounts place bets first (5 seconds solo)
- Non-DEDUCE accounts then match those bets
- Tracks balance changes at each phase

**What we verify**:
- DEDUCE balance unchanged during placement phase
- DEDUCE balance deducted ONLY when matched
- Non-DEDUCE balance deducted immediately

**Business value**: Critical verification that DEDUCE timing works as designed

---

### 6. **Test: Patron Matches MM Wagers** (2 MMs + 2 Patrons)
**Purpose**: Cross-account-type matching verification with realistic trading  
**What it does**:
- **2 MM accounts** (MM1 deduce + MM2 non-deduce) place bets continuously using **random odds from odds ladder**
- **2 Patron accounts** (Patron deduce + Patron non-deduce) actively match those MM bets
- MMs place bets on one side (Team A), Patrons match on opposite side (Team B)
- Uses real odds ladder API for realistic odds selection
- Tests both DEDUCE and non-DEDUCE for each account type

**What we verify**:
- Patron-MM matching works correctly across different odds
- DEDUCE behavior consistent across account types (MM and Patron)
- Balance reconciliation (money moved correctly between accounts)
- Matching works with dynamic odds (not just fixed +/-150)

**Business value**: Ensures DEDUCE works between different account types (MM ↔ Patron) with realistic market conditions

---

### 7. **Test: Cancel Race - Wager Job Bug**
**Purpose**: Reproduce and verify cancellation race condition bug  
**What it does**:
- MM accounts rapidly place and immediately cancel bets (50% cancel rate)
- Patron accounts try to match before cancellation
- Simulates real trading conditions

**What we verify**:
- Cancellations process correctly
- Database consistency (wager_jobs don't get stuck in "pending" state)
- Balance verification after cancellations

**Known Bug**: When wagers are cancelled, `wager_jobs` can remain stuck in "pending" status even though wager shows "cancelled"

**Business value**: Validates fix for critical bug that could cause database inconsistencies

---

### 8. **Test: Aggressive All-Lines Betting**
**Purpose**: Realistic market stress testing  
**What it does**:
- All accounts fetch ALL available betting lines from an event
- Each account aggressively bets on EVERY line simultaneously
- Line IDs refreshed every 5 seconds
- All accounts compete naturally without coordination

**What we verify**:
- DEDUCE works across multiple markets
- System handles organic market competition
- matched_wager_balance increases correctly

**Business value**: Most realistic test - simulates active market with multiple aggressive traders

---

### 9. **Test: Live Event 5-Second Bet Delay** ⭐ NEW
**Purpose**: Verify 5-second delay implementation for live events  
**What it does**:
- Places bet and tries to cancel before 5s (should fail)
- Places bet and tries to cancel after 5s (should succeed)  
- Places back-to-back bets (2nd should be delayed ~5s)

**What we verify**:
- Bets remain in "placing" state for ~5 seconds
- Cannot cancel during placement window
- Delay prevents bet manipulation during live events

**Business value**: Ensures fraud prevention mechanism is working for live sporting events

---

## How to Run Tests

### Run all tests:
```bash
python3 test_deduce_race_conditions.py --test all --duration 30
```

### Run specific test:
```bash
# Basic race test
python3 test_deduce_race_conditions.py --test 1a --duration 30

# 4-way test
python3 test_deduce_race_conditions.py --test 4way --duration 30

# Rapid fire test
python3 test_deduce_race_conditions.py --test rapid --duration 20

# Cancel bug test
python3 test_deduce_race_conditions.py --test cancel_bug --duration 30 --event-id 30024797

# Live event 5s delay test
python3 test_deduce_race_conditions.py --test live_delay --event-id 30024797
```

### Options:
- `--test`: Which test to run (1a, 4way, rapid, burst, patron_mm, deduce_matched, aggressive, cancel_bug, live_delay, all)
- `--duration`: Test duration in seconds (default: 30)
- `--event-id`: Specific event ID to test (required for cancel_bug and live_delay)
- `--rps`: Target requests per second (for cancel_bug test)

---

## Test Outputs

Each test generates:
1. **Console output**: Real-time test progress and results
2. **JSON report**: Detailed test data saved to file
   - `race_test_1a_<timestamp>.json`
   - `race_test_4way_<timestamp>.json`
   - `race_test_cancel_bug_<timestamp>.json`
   - `live_event_5s_delay_<event_id>_<timestamp>.json`
   - etc.

3. **Balance verification**: Before/after snapshots
4. **Error logs**: Any failures or inconsistencies
5. **SQL queries**: For database verification (cancel_bug test)

---

## What Managers Should Know

### Why These Tests Matter:
1. **Financial Integrity**: Ensure no money is lost or double-deducted
2. **System Stability**: Validate system handles high-load trading
3. **Feature Correctness**: DEDUCE is a core differentiator - must work perfectly
4. **Bug Prevention**: Catch race conditions before they reach production
5. **Regulatory Compliance**: Proper money handling is legally required

### Test Frequency:
- Run **before every DEDUCE-related deployment**
- Run **weekly** as regression tests
- Run **after database schema changes**
- Run **when investigating balance discrepancies**

### Success Criteria:
- ✅ All balance changes match expected amounts
- ✅ No balance inconsistencies
- ✅ DEDUCE accounts only deducted on match
- ✅ Non-DEDUCE accounts deducted immediately
- ✅ No stuck wager_jobs in database
- ✅ System stable under high load

### Red Flags:
- ❌ Balance discrepancies > $0.01
- ❌ DEDUCE balance deducted before match
- ❌ Wager_jobs stuck in "pending" state
- ❌ API errors during normal operation
- ❌ 5-second delay not working for live events

---

## Recent Additions & Fixes

### Latest Fix: Patron Matches MM Test (January 2026)
**What was fixed**: 
- ✅ Fixed hardcoded odds in bet queue (was using fixed 150, now uses actual random odds)
- ✅ Test now uses full odds ladder API for realistic market conditions
- ✅ Both MMs and Patrons use random odds from ladder (not just +/-150)

**How it works**:
- 2 MMs place bets with random positive odds (+100 to +500)
- 2 Patrons match with random negative odds (-100 to -500)
- Uses real odds ladder API: `/trade/public/api/v1/markets/moneyline/odds-ladder`

**See**: `PATRON_MM_TEST_IMPROVEMENTS.md` for detailed explanation

### Latest Test: Live Event 5-Second Delay (January 2026)
**Background**: A 5-second delay was implemented for live events to prevent bet manipulation

**Test Results on Event 30024797**:
- ✅ **PASS**: Cannot cancel bets for ~5 seconds (stays in "placing" state)
- ✅ **PASS**: Can cancel after 5 seconds
- ❌ **FAIL**: Back-to-back bets not delayed (can submit immediately)

**Conclusion**: Delay works as a processing window, but does NOT rate-limit consecutive bet submissions

---

## Contact
For questions about these tests, contact the QA/Trading Platform team.

**Test Suite Location**: `/Users/tranlam/Documents/GitHub/mm-api-integration-guide/test_deduce_race_conditions.py`
