# DEDUCE Feature Test Suite - Comprehensive Guide

## Overview

This test suite provides comprehensive validation of the DEDUCE (Deferred Deduction) feature through various race condition scenarios, edge cases, and stress tests.

## Test Files

### 1. `test_deduce_wallet_validation.py`
Basic validation tests for deduce wallet behavior.

**Tests:**
- `test_mm1_deduce_wallet_validation()` - Verify MM1 deduce behavior
- `test_mm2_normal_wallet_validation()` - Verify MM2 normal behavior  
- `test_all_accounts_comparison()` - Compare all account types
- `test_concurrent_4_accounts_enhanced()` - 4-account concurrent test

**Usage:**
```bash
# Run enhanced 4-account test (recommended)
python3 test_deduce_wallet_validation.py --test enhanced --duration 60

# Run specific test
python3 test_deduce_wallet_validation.py --test mm1  # or mm2, all
```

### 2. `test_deduce_race_conditions.py`
Advanced race condition and edge case tests.

**Tests:**
- `test_deduce_sp_vs_nondeduce_sp()` - Test 1A
- `test_four_way_mexican_standoff()` - 4-way race
- `test_rapid_fire_race()` - High-frequency stress test
- `test_simultaneous_burst()` - Synchronized burst test
- `test_patron_matches_mm_wagers()` - Patron matching MM bets

**Usage:**
```bash
# Run all race condition tests
python3 test_deduce_race_conditions.py --test all --duration 30

# Run specific test
python3 test_deduce_race_conditions.py --test 1a --duration 60
python3 test_deduce_race_conditions.py --test 4way --duration 30
python3 test_deduce_race_conditions.py --test rapid --duration 20
python3 test_deduce_race_conditions.py --test burst
python3 test_deduce_race_conditions.py --test patron_mm --duration 30
```

## Test Case Categories

### Category 1: Account Type Matching Combinations

#### **Test 1A: Deduce SP vs Non-Deduce SP**
- **Scenario**: MM1 (deduce) and MM2 (non-deduce) place opposite bets continuously
- **Purpose**: Verify MM1 balance unchanged until match, MM2 deducted immediately
- **Duration**: 30s default
- **Expected Results**:
  - MM1 balance change = sum of matched bets only
  - MM2 balance deducted immediately for all placed bets
- **Command**: `python3 test_deduce_race_conditions.py --test 1a`

#### **Test 4-Way: Mexican Standoff**
- **Scenario**: 4 accounts (2 deduce + 2 non-deduce, mixed SP/Patron) all place bets simultaneously
- **Accounts**:
  - MM1 (deduce SP) - back side
  - MM2 (non-deduce SP) - lay side
  - Patron deduce - back side
  - Patron non-deduce - lay side
- **Purpose**: Test complex multi-party matching with different account types
- **Duration**: 30s default
- **Expected Results**:
  - Both deduce accounts: balance change = matched bets only
  - Both non-deduce accounts: balance deducted immediately
- **Command**: `python3 test_deduce_race_conditions.py --test 4way`

### Category 2: Timing & High-Frequency Tests

#### **Test: Rapid Fire Race**
- **Scenario**: High-frequency betting (10 bets/second default) from both sides
- **Purpose**: Test system under rapid concurrent operations
- **Key Metrics**:
  - Actual throughput vs target
  - Match rate percentage
  - Balance consistency check
- **Duration**: 20s default
- **Expected Results**:
  - No double-deductions
  - No missed deductions
  - Balance change = matched total
- **Command**: `python3 test_deduce_race_conditions.py --test rapid --duration 20`

#### **Test: Simultaneous Burst**
- **Scenario**: 10 rounds of perfectly synchronized bets (within 10ms) from all 4 accounts
- **Purpose**: Test matching engine's handling of true simultaneous requests
- **Expected Results**:
  - Correct matching priority
  - Proper balance deduction timing
  - No race condition errors
- **Command**: `python3 test_deduce_race_conditions.py --test burst`

#### **Test: Patron Matches MM Wagers**
- **Scenario**: MM accounts place bets, patron accounts actively match them (maker-taker model)
- **Roles**:
  - MM1 (deduce) & MM2 (non-deduce) - Makers (provide liquidity)
  - Patron deduce & Patron non-deduce - Takers (consume liquidity)
- **Purpose**: Test realistic market scenario where patrons match MM bets
- **Key Behavior**:
  - MMs place bets at odds 150 (back side)
  - Patrons match with odds -150 (lay side)
  - Queue-based matching system
- **Duration**: 30s default
- **Expected Results**:
  - MM deduce: balance unchanged until matched
  - MM non-deduce: balance deducted immediately
  - Patron deduce: balance unchanged until matched
  - Patron non-deduce: balance deducted immediately
  - All matches tracked and balanced
- **Command**: `python3 test_deduce_race_conditions.py --test patron_mm --duration 30`

### Category 3: Balance & Exposure Edge Cases

#### **Insufficient Balance Scenarios**
- Deduce account with $0 balance
- Exact balance = stake (boundary condition)
- Insufficient funds at match time

**To Test:**
```bash
# Manually modify stake amounts in test to exceed balance
# Or reduce patron_deduce balance to $0.99 (already low)
python3 test_deduce_race_conditions.py --test 4way
```

#### **High-Volume Stress**
```bash
# Increase duration for sustained load
python3 test_deduce_race_conditions.py --test rapid --duration 120

# Or run all tests back-to-back
python3 test_deduce_race_conditions.py --test all --duration 60
```

### Category 4: Verification & Consistency Checks

All tests include automatic verification:
- **Balance reconciliation**: Initial - Final = Expected change
- **Matched bet verification**: Balance change = Sum of matched bet stakes (for deduce)
- **Error tracking**: All API errors captured and categorized
- **Timing analysis**: Request durations and throughput metrics

## Test Accounts

### Market Maker (MM) Accounts
- **MM1**: Deduce-enabled SP account
- **MM2**: Normal (non-deduce) SP account

### Patron Accounts
- **Patron Deduce**: `deduct.sanbox.test1@yopmail.com` / `Matkhau1$`
- **Patron Non-Deduce**: From `user_info_patron_{environment}.json`

## Expected Behaviors

### Deduce Accounts (MM1, Patron Deduce)
1. **Bet Placement**: Balance unchanged
2. **Bet Matched**: Balance deducted by matched amount only
3. **Partial Match**: Balance deducted by partial matched amount
4. **Cancel**: If cancelled before match, no deduction
5. **Win**: Payout added to balance
6. **Void**: Refund added to balance

### Non-Deduce Accounts (MM2, Patron Non-Deduce)
1. **Bet Placement**: Balance immediately deducted by stake amount
2. **Bet Matched**: No additional change
3. **Partial Match**: Full stake already deducted
4. **Cancel**: Refund of unmatched amount
5. **Win**: Payout added to balance
6. **Void**: Refund added to balance

## Report Files

Each test generates a detailed JSON report:
- `race_test_1a_{timestamp}.json` - Test 1A results
- `race_test_4way_{timestamp}.json` - 4-way test results
- `race_test_rapid_{timestamp}.json` - Rapid fire test results
- `race_test_burst_{timestamp}.json` - Burst test results
- `deduce_wallet_test_{timestamp}.json` - Enhanced validation test results

### Report Contents
```json
{
  "test": "test_name",
  "duration": 30,
  "accounts": {...},
  "wager_counts": {...},
  "snapshots": {
    "initial": {"timestamp": "...", "balances": {...}},
    "final": {"timestamp": "...", "balances": {...}}
  },
  "errors": [...],
  "consistency_check": true
}
```

## Critical Test Scenarios

### Priority 1: Race Conditions
✅ **Must Test:**
1. Deduce SP vs Non-Deduce SP (Test 1A)
2. 4-Way Mexican Standoff (all account types)
3. Rapid Fire (high-frequency stress)

### Priority 2: Edge Cases
📋 **Should Test:**
1. Simultaneous Burst (true concurrency)
2. Insufficient balance scenarios
3. Partial matches
4. Cancel operations

### Priority 3: Extended Tests
🔄 **Optional:**
1. Long-duration runs (2+ hours)
2. Multiple events/markets simultaneously
3. Extreme stake amounts
4. Network failure simulations

## Interpreting Results

### Success Criteria

#### For Deduce Accounts:
- ✅ Balance unchanged after bet placement
- ✅ Balance change equals sum of matched bet stakes
- ✅ No double-deductions
- ✅ All matched bets appear in history

#### For Non-Deduce Accounts:
- ✅ Balance deducted immediately after placement
- ✅ Deduction equals stake amount
- ✅ No additional deduction on match

### Common Issues

❌ **Balance inconsistency**: 
- Check matched bet history
- Verify time window for matched bets
- Look for pending settlements

❌ **API errors**:
- Check authentication tokens
- Verify account has sufficient balance
- Review rate limiting

❌ **No matches**:
- Verify opposite odds are being used
- Check market liquidity
- Ensure line_ids are valid

## Advanced Testing Scenarios

### Custom Test Development

To create custom test scenarios:

```python
from deduce_tests import DeduceTestFramework, Colors
from test_deduce_race_conditions import RaceConditionTest

# Initialize
framework = DeduceTestFramework(environment='staging')
test = RaceConditionTest(framework)

# Login accounts
framework.login_account('mm1', mm1_creds, account_type='mm')
framework.login_account('patron', patron_creds, account_type='patron')

# Take initial snapshot
initial = test.snapshot_balances(['mm1', 'patron'], 'initial')

# Your test logic here...

# Take final snapshot
final = test.snapshot_balances(['mm1', 'patron'], 'final')

# Verify consistency
for acc in ['mm1', 'patron']:
    matched_bets = framework.get_matched_bets(acc)
    matched_total = sum(b.get('stake', 0) for b in matched_bets)
    actual_change = initial['balances'][acc] - final['balances'][acc]
    print(f"{acc}: Expected ${matched_total}, Actual ${actual_change}")
```

## Monitoring & Validation

### During Test Execution
Monitor for:
- Wager placement success rates
- API error rates
- Balance changes in real-time
- Match rates and latency

### After Test Completion
Verify:
1. Zero API errors (or acceptable error rate)
2. Balance consistency across all accounts
3. All matched bets accounted for
4. No orphaned or missing transactions

### Database & Logs
External monitoring should check:
- Database transaction logs
- Matching engine logs
- Wallet service logs
- Any error or exception logs

## Troubleshooting

### Test Won't Start
```bash
# Check account credentials
cat user_config_staging.json
cat user_info_patron_staging.json

# Verify environment
echo $ENVIRONMENT  # Should be 'staging' or 'production'

# Test login manually
python3 -c "from deduce_tests import *; f=DeduceTestFramework(); f.login_account('test', creds, 'mm')"
```

### No Matches Occurring
- Check if markets are active
- Verify opposite odds are correct
- Ensure sufficient balance on both sides
- Try different event/market

### Balance Inconsistencies
```python
# Manual verification script
framework = DeduceTestFramework()
framework.login_account('mm1', mm1_creds, 'mm')

# Get detailed info
balance = framework.get_balance('mm1')
matched_bets = framework.get_matched_bets('mm1', limit=100)
exposure = framework.get_exposure('mm1')

print(f"Balance: {balance}")
print(f"Matched: {len(matched_bets)} bets")
print(f"Total matched stake: {sum(b['stake'] for b in matched_bets)}")
```

## Best Practices

1. **Start with short durations** (30s) to verify setup
2. **Monitor first test closely** to catch configuration issues early
3. **Run tests during low-traffic periods** for cleaner results
4. **Save all report files** for historical comparison
5. **Document any anomalies** immediately
6. **Run tests multiple times** to identify flaky behaviors
7. **Compare results across runs** to spot trends

## Future Test Scenarios

### To Be Implemented:
- [ ] Partial match scenarios
- [ ] Cancel operations during various stages
- [ ] Market closure edge cases
- [ ] Settlement and payout verification
- [ ] Void/refund scenarios
- [ ] Multi-event concurrent testing
- [ ] Long-running stability tests (24h+)
- [ ] Network failure simulation
- [ ] Database rollback recovery
- [ ] Exposure limit testing

## Contact & Support

For issues with the test framework:
1. Check error logs in reports
2. Verify account credentials and balances
3. Review this guide for troubleshooting steps
4. Check database logs for transaction details
