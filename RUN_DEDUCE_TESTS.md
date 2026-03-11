# Quick Start: DEDUCE Testing

## 🚀 Run Tests Now

```bash
# 1. Set environment to staging
export MM_ENVIRONMENT=staging

# 2. Run all tests
python deduce_tests.py
```

## 📋 What Gets Tested

### Patron Tests (Tests 1-4)
✅ Basic DEDUCE flow (no deduction on placement)  
✅ Multiple wagers with same balance  
✅ Cancel before match  
✅ Exposure calculation (10x rule)

### MM Stress Tests (Tests 5-7)
✅ **Test 5**: Place 100+ wagers massively  
✅ **Test 6**: Cancel all wagers simultaneously  
✅ **Test 7**: Rapid place & cancel cycles (stress test)

## 🔑 Authentication

### MM Accounts (from JSON files)
- Uses `access_key` and `secret_key` from:
  - `src/user_info_staging.json` (MM1)
  - `src/user_info_account2_staging.json` (MM2)

### Patron Accounts (username/password)
Hardcoded in script:
- `parlay_cash_patron@betprophet.co` / `Testing@123`
- `thinh.tran@betprophet.co` / `Matkhau1$`
- `lam.tran+usr001@betprophet.co` / `Kh0ngbiet1`

## 📊 Expected Output

```
======================================================================
DEDUCE Feature Automated Tests
======================================================================

Setting up test accounts...

Logging in MM accounts...
✓ Logged in mm1 (mm)
✓ Logged in mm2 (mm)

Logging in Patron accounts...
✓ Logged in patron1 (patron)
✓ Logged in patron2 (patron)
✓ Logged in patron3 (patron)

Running tests...

=== PATRON TESTS ===
✓ PASS - Test 1: Basic DEDUCE Flow
✓ PASS - Test 2: Multiple Wagers Same Balance
✓ PASS - Test 3: Cancel Before Match
✓ PASS - Test 4: Exposure Calculation (10x)

=== MM STRESS TESTS ===
✓ PASS - Test 5: MM Massive Placement (100 wagers)
  Placement rate: 45.2 wagers/sec
  Balance unchanged: $1000.00 -> $1000.00

✓ PASS - Test 6: MM Massive Simultaneous Cancellation
  Cancellation rate: 52.3 cancellations/sec
  Balance unchanged: $1000.00 -> $1000.00

✓ PASS - Test 7: MM Stress Test (3 cycles)
  Total: 90 wagers placed/cancelled
  Balance unchanged: $1000.00 -> $1000.00

======================================================================
DEDUCE Test Summary
======================================================================
Total Tests: 7
Passed: 7
Failed: 0
Time: 45.67s
======================================================================
```

## 🎯 Key Validations

### DEDUCE Core Behavior
1. ✅ Balance **NOT** deducted when wager placed
2. ✅ Balance **ONLY** deducted when wager matched
3. ✅ Multiple wagers can use same balance (different markets)
4. ✅ Auto-cancellation when balance depleted

### MM Stress Tests
1. ✅ Handle 100+ simultaneous wagers
2. ✅ No performance degradation
3. ✅ Balance accuracy maintained under load
4. ✅ Massive cancellations processed correctly

## 🔧 Troubleshooting

### Issue: "Login failed for mm1"
**Fix**: Check `src/user_info_staging.json` has correct access_key/secret_key

### Issue: "Login failed for patron1"
**Fix**: Patron credentials are hardcoded - check if passwords changed

### Issue: "No available markets"
**Fix**: Run tests during active sports hours (games available)

### Issue: Balance too low
**Fix**: Add funds to test accounts via admin panel

## 📈 Performance Benchmarks

| Metric | Target | Typical |
|--------|--------|---------|
| Wager placement rate | >30/sec | 40-50/sec |
| Cancellation rate | >40/sec | 50-60/sec |
| Balance check latency | <500ms | 200-300ms |
| Wallet write reduction | 98% | 98%+ ✅ |

## 🐛 Report Issues

If tests fail:
1. Check `deduce_test_results_*.json` for details
2. Review console output for error messages
3. Verify staging environment is healthy
4. Check test account balances

## 📁 Files Created

- `deduce_tests.py` - Main test script
- `DEDUCE_PAIR_TESTING_CHECKLIST.md` - Manual test checklist
- `DEDUCE_TEST_README.md` - Detailed documentation
- `deduce_test_results_YYYYMMDD_HHMMSS.json` - Test results (auto-generated)

---

**Ready to test?** Just run: `python deduce_tests.py`
