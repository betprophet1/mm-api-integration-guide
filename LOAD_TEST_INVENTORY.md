# Load & Stress Test Inventory

**Repository:** mm-api-integration-guide  
**Generated:** 2026-02-06  
**Purpose:** Comprehensive list of all load/stress testing scripts for management review

---

## 1. Backend Fairness Test (`test_backend_fairness.py`)

### Purpose
Tests the backend's fairness enhancement to ensure equal distribution of wagers across multiple user accounts.

### Key Features
- **Multi-account testing:** Supports 2-10 MM accounts
- **Fairness validation:** Verifies backend batching (100 jobs at a time) and parallelization by user ID
- **Continuous mode:** Automatic token refresh when tokens expire (401 errors)
- **Configurable workers:** Default 40-50 concurrent workers
- **Real-time metrics:** Progress tracking, fairness ratio, placement rate, ETA

### Test Requirements
- Backend must batch jobs (100 at a time)
- Backend must parallelize by user ID
- Fairness ratio should be ≥90% (min/max wagers per user)

### Usage
```bash
# Basic test
python3 test_backend_fairness.py --event <EVENT_ID> --wagers 1000

# Continuous mode with auto token refresh
python3 test_backend_fairness.py --event <EVENT_ID> --wagers 1000000 \
  --continuous --iterations 2 --workers 40

# Exclude deduce-enabled account (MM1)
# Currently configured: MM2-3 only
```

### Metrics Tracked
- Placed wagers per user
- Matched wagers
- Failed requests
- Response times
- Fairness ratio (min/max distribution)
- Balance changes

### Output
- JSON results file: `backend_fairness_test_<timestamp>.json`
- SQL verification query: `verify_fairness_<timestamp>.sql`

---

## 2. Backend Fairness Test - Continuous (`test_backend_fairness_continuous.py`)

### Purpose
Extended version with enhanced continuous operation and automatic recovery.

### Key Features
- Automatic token refresh and re-authentication
- Error recovery and retry logic
- Exponential backoff on failures
- Enhanced progress tracking
- Graceful shutdown handling (Ctrl+C)

### Enhancements Over Standard Test
- Token validation before each wager
- Retry logic (max 3 attempts)
- Better error handling and logging
- Continuous batch processing

---

## 3. 30K Stress Test (`stress_test_30k.py`)

### Purpose
High-volume bet & cancel stress test on a single event with fairness validation.

### Key Features
- **Target:** 30,000 wagers (placement + cancellation)
- **Strategy:** Aggressive batch placement + immediate cancellation
- **Fairness tracking:** Per-user metrics to validate fair distribution
- **Batch operations:** Up to 20 wagers per API call

### Test Flow
1. Place wagers in batches (max 20 per call)
2. Immediately cancel placed wagers
3. Track fairness across users
4. Report min/max/avg wagers per user

### Metrics Tracked
- Wagers placed per user
- Wagers cancelled per user
- Failed requests per user
- Average response time per user
- Fairness ratio

### Usage
```bash
python3 stress_test_30k.py --event <EVENT_ID> --accounts 2
```

---

## 4. Exposure Stress Test (`exposure-stress-test.py`)

### Purpose
Generate maximum exposure (GEC/LEC) through coordinated aggressive betting across multiple accounts and events.

### Key Features
- **Multi-account:** Typically 2 accounts with separate credentials
- **Multi-event:** Tests across multiple events simultaneously
- **High-frequency:** Parallel bet placement to maximize load
- **Dual authentication:** Supports both MM API and Web API login
- **Real-time monitoring:** Progress tracking every 10 seconds

### Test Strategy
- Coordinate betting across accounts
- Target multiple events to maximize exposure
- Generate GEC (Gross Event Credit) scenarios
- Track bet placement rate

### Metrics Tracked
- Total bets placed
- GEC events generated
- Bet placement rate (bets/sec)
- Account balances
- Worker statistics
- Error log

### Usage
```bash
python3 exposure-stress-test.py --env sandbox --target-bets 1000 --target-gec 5
```

### Account Configuration
Supports 2 hardcoded accounts with email/password authentication

---

## 5. Deduce Tests (`deduce_tests.py`)

### Purpose
Framework for testing DEDUCE feature behavior and race conditions.

### Key Features
- **Account type testing:** Deduce vs non-deduce account matching
- **Balance validation:** Verifies deduce accounts are not debited until match
- **Race condition testing:** Tests timing-critical scenarios
- **Multi-account support:** Tests 2-4 accounts simultaneously

### Test Scenarios
1. Deduce SP vs Non-Deduce SP
2. Deduce SP vs Patron
3. Four-way matching scenarios
4. Timing tests with varying delays
5. Partial match and cancellation tests

---

## 6. Deduce Race Condition Tests (`test_deduce_race_conditions.py`)

### Purpose
Comprehensive race condition testing for the DEDUCE feature.

### Test Categories
1. **Two-Account Races:** Direct deduce vs non-deduce matching
2. **Four-Account Races:** Complex multi-party matching scenarios
3. **Timing Tests:** Varying delays between placement and matching
4. **Partial Match Tests:** Partial fills and cancellations
5. **High-Volume Stress:** Rapid concurrent operations

### Key Features
- Balance consistency verification
- Snapshot-based tracking
- Opposite line_id matching
- Odds ladder fetching
- Concurrent worker execution

---

## 7. Deduce Wallet Validation Test (`test_deduce_wallet_validation.py`)

### Purpose
Validates wallet balance behavior for deduce-enabled accounts.

### Test Focus
- Balance not debited until match for deduce accounts
- Immediate debit for non-deduce accounts
- Multi-wager validation
- Balance reconciliation

---

## 8. Duplicate Bet Submission Test (`test_duplicate_bet_submission.py`)

### Purpose
Tests system behavior when duplicate bets are submitted.

### Test Scenarios
- Duplicate external_id handling
- Concurrent duplicate submissions
- Error code validation

---

## 9. Non-Deduce Only Test (`test_nondeduce_only.py`)

### Purpose
Isolates testing to non-deduce accounts only (excludes MM1).

### Key Features
- Uses accounts 2-10 (excludes deduce-enabled MM1)
- Validates non-deduce behavior independently
- Useful for baseline performance testing

---

## 10. Wallet Retry Logic Tests

### Files
- `test_wallet_retry_logic.py`
- `test_wallet_retry_logic_exclude_mm1.py`

### Purpose
Tests automatic retry logic for wallet operations.

### Key Features
- Retry on transient failures
- Token expiration handling
- Exponential backoff
- Option to exclude deduce account

---

## 11. 30-Minute Autoplay Test (`run_30min_autoplay_test.py`)

### Purpose
Long-running automated test for continuous operation validation.

### Key Features
- 30-minute sustained operation
- Automated bet placement
- Continuous monitoring
- Performance baseline establishment

---

## 12. Weekend Test Runner (`weekend_test_runner.py`)

### Purpose
Orchestrates multiple tests for weekend regression testing.

### Key Features
- Sequential test execution
- Multiple test scenarios
- Comprehensive reporting
- Automated scheduling support

---

## Summary Statistics

| Test Type | Concurrent Accounts | Max Workers | Token Refresh | Fairness Tracking |
|-----------|-------------------|-------------|---------------|-------------------|
| Backend Fairness | 2-10 | 40-50 | ✅ | ✅ |
| 30K Stress | 2+ | Configurable | ❌ | ✅ |
| Exposure Stress | 2 | Configurable | ❌ | ❌ |
| Deduce Tests | 2-4 | Varies | ❌ | ✅ |
| Race Conditions | 2-4 | Varies | ❌ | ✅ |

---

## Current Test Configuration

### Active Test: `test_backend_fairness.py`
- **Accounts:** MM2 and MM3 only (non-deduce)
- **Event:** 20023208
- **Volume:** 1,000,000 wagers per iteration
- **Iterations:** 2
- **Workers:** 40
- **Mode:** Continuous with auto token refresh

### Key Capabilities
1. **Token Management:** Automatic refresh on 401 errors
2. **Fairness Validation:** Real-time fairness ratio tracking
3. **Multi-iteration:** Continuous mode with configurable iterations
4. **Error Recovery:** Retry logic with exponential backoff
5. **Metrics Export:** JSON and SQL output for database verification

---

## Recommended Usage by Scenario

### Scenario 1: Backend Fairness Validation
**Use:** `test_backend_fairness.py`  
**Configuration:** 2-10 accounts, 40-50 workers, continuous mode

### Scenario 2: High-Volume Stress Testing
**Use:** `stress_test_30k.py`  
**Configuration:** 30K wagers, batch placement + cancellation

### Scenario 3: Exposure/GEC Testing
**Use:** `exposure-stress-test.py`  
**Configuration:** 2 accounts, multiple events, high frequency

### Scenario 4: DEDUCE Feature Validation
**Use:** `test_deduce_race_conditions.py`  
**Configuration:** Mixed deduce/non-deduce accounts, timing tests

### Scenario 5: Long-Running Stability
**Use:** `run_30min_autoplay_test.py`  
**Configuration:** 30-minute sustained operation

---

## Notes for Management

1. **Current Production Test:** `test_backend_fairness.py` is the most actively maintained and feature-rich test
2. **Token Management:** Only backend fairness tests have automatic token refresh
3. **Fairness Tracking:** Most tests now include per-user fairness metrics
4. **Scalability:** Tests support 2-10 concurrent accounts and 40-50+ workers
5. **Continuous Operation:** Continuous mode enables long-running tests without manual intervention
6. **Database Verification:** SQL queries generated for backend validation

---

## Test Output Locations

- **JSON Results:** `backend_fairness_test_<timestamp>.json`
- **SQL Queries:** `verify_fairness_<timestamp>.sql`
- **Logs:** Console output with structured logging
- **Metrics:** Real-time progress reporting every 5 seconds

---

## PARLAY API TESTS

### Repository: `python-parlay-api-integration-guide`

The Parlay repository contains extensive load and validation testing for the Parlay betting API.

---

## 13. Comprehensive Parlay Load Test (`comprehensive_load_test.py`)

### Purpose
Advanced load testing that runs multiple successful parlay scenarios with configurable iterations.

### Key Features
- **Multi-scenario:** Basic Single SP Success + Complete Multi-Tier Success
- **Configurable iterations:** Default 10 iterations per scenario
- **Execution modes:** Concurrent and sequential
- **Performance analytics:** Detailed bottleneck identification
- **System stability:** Reliability testing under load

### Test Scenarios
1. **Single SP Success:** Most reliable scenario with one SP
2. **Multi-Tier Success:** Complex scenario with multiple SPs accepting

### Metrics Tracked
- Authentication time
- Create parlay time
- SP offer time
- User confirmation time
- SP acceptance time
- User view time
- Success/failure rates per scenario
- Statistical analysis (mean, median, p95, p99)

### Market Lines
- Uses 2 predefined market lines
- Tests across different outcomes and events

### Output
- Log file: `comprehensive_load_test_<timestamp>.log`
- Detailed performance analytics
- Test result summaries

---

## 14. Parlay Load Test (`parlay_load_test.py`)

### Purpose
Runs successful parlay scenarios 10 times to test system performance under load.

### Key Features
- **Target:** 10 iterations of successful parlay flows
- **Focus:** Basic Single SP Success scenario
- **Multi-account:** Tests both SP1 and SP2
- **Performance tracking:** Response times for each API call
- **Thread-safe:** Concurrent execution with proper locking

### Test Flow
1. User authentication
2. Create parlay request
3. SP provides offer
4. User confirms parlay
5. SP accepts/rejects
6. User views final result

### Metrics Tracked
- Auth response times
- Create parlay response times
- SP offer response times
- User confirmation times
- SP acceptance times
- User view times
- Overall success rate

---

## 15. Parlay Chaos Test (`parlay_chaos_test.py`)

### Purpose
Validation rule verification through intentional creation of both valid and invalid parlays.

### Test Distribution
- **50% Valid parlays:** Should be accepted by API
- **50% Invalid parlays:** Should be properly rejected
  - 25% Rule 1 violations: Both sides moneyline
  - 25% Rule 2 violations: Moneyline + negative spread
  - 25% Rule 3 violations: Opposing spreads sum ≤ 0
  - 25% Rule 4 violations: Over/under diff ≥ 0

### Key Features
- **Validation testing:** Verifies API enforces all betting rules
- **Rule coverage:** Tests all 4 major parlay rules
- **Market categorization:** Separates moneylines, spreads, totals
- **Graceful shutdown:** Signal handling for Ctrl+C
- **Fresh data:** Loads market lines from JSON file

### Test Validation
- Verifies expected rejections occur
- Validates acceptance of valid parlays
- Tracks validation pass rate

### Metrics Tracked
- Parlay type (valid/invalid)
- Expected vs actual result
- Leg count
- Validation passed/failed
- Error messages
- Parlay IDs

---

## 16. Additional Archived Parlay Tests

### Located in: `archived_tests/`

The repository contains 60+ archived test files covering:

1. **Aggressive Load Tests**
   - `aggressive_load_test_scenario_1.py`
   - `multi_scenario_load_test.py`
   - `randomized_parlay_load_test.py`

2. **Scenario-Based Tests**
   - `test_scenario_01.py` through `test_scenario_10.py`
   - Various test flows for different betting scenarios

3. **Validation Tests**
   - `test_odds_validation.py`
   - `test_e2e_odds_validation.py`
   - `test_expired_odds_focus.py`
   - `test_price_probability_*.py` (multiple versions)

4. **Workflow Tests**
   - `test_parlay_matching_flow*.py` (multiple versions)
   - `test_workflow_validation.py`
   - `diagnostic_sp_workflow.py`

5. **Comprehensive Test Suites**
   - `comprehensive_parlay_test_suite.py`
   - `full_comprehensive_test_suite.py`
   - `complete_test_with_logging.py`

6. **Analysis & Debug**
   - `bug_analysis.py`
   - `analyze_stake_calculation.py`
   - `debug_stake_calculation.py`
   - `rejection_vs_timeout_analysis.py`

7. **Special Tests**
   - `test_duplicate_bet_submission.py`
   - `test_ack_without_pp_quick.py`
   - `test_fresh_flow_ack.py`

---

## Summary: All Repositories

### MM API Integration Guide (12 tests)
- Backend fairness testing
- Multi-account stress testing
- DEDUCE feature testing
- Exposure/GEC testing
- Race condition testing
- Wallet validation

### Python Parlay API Integration Guide (60+ tests)
- Comprehensive load testing
- Validation rule testing
- Chaos/fuzzy testing
- Multi-scenario testing
- Performance benchmarking
- Workflow validation

### Total Test Inventory
- **Active tests:** 15+ production-ready tests
- **Archived tests:** 60+ historical/specialized tests
- **Total coverage:** 75+ test scripts across both repositories

---

## Comparison Matrix: Parlay vs MM Tests

| Feature | Parlay Tests | MM Tests |
|---------|-------------|----------|
| Load Testing | ✅ 10+ iterations | ✅ Up to 1M wagers |
| Validation | ✅ Rule enforcement | ✅ Fairness tracking |
| Multi-account | ✅ 2 accounts | ✅ Up to 10 accounts |
| Token Refresh | ❌ | ✅ |
| Chaos Testing | ✅ | ❌ |
| Concurrent Workers | Limited | 40-50+ |
| Continuous Mode | ❌ | ✅ |

---

## Recommended Test Selection

### For Parlay API Testing
**Use:** `comprehensive_load_test.py` or `parlay_chaos_test.py`  
**Purpose:** Load testing + validation rule verification

### For MM API Testing
**Use:** `test_backend_fairness.py`  
**Purpose:** Fairness validation + high-volume stress testing

### For Combined System Testing
**Strategy:** Run both suites in parallel to test full betting ecosystem

---

**End of Inventory**
