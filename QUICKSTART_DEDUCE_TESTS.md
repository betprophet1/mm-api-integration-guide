# DEDUCE Tests - Quick Start Guide

## TL;DR - Run Tests Now

```bash
# Quick validation test (60 seconds, 4 accounts)
python3 test_deduce_wallet_validation.py --test enhanced --duration 60

# Run all race condition tests (recommended for comprehensive testing)
python3 test_deduce_race_conditions.py --test all --duration 30
```

## What Gets Tested

### ✅ Account Combinations
- **Deduce SP** (MM1) ↔ **Non-Deduce SP** (MM2)
- **Deduce Patron** ↔ **Non-Deduce Patron**
- All 4 accounts simultaneously placing & matching bets

### ✅ Race Conditions
- Simultaneous bet placement (within milliseconds)
- High-frequency operations (10+ bets/second)
- Multi-party matching scenarios
- Balance deduction timing verification

### ✅ Edge Cases
- Insufficient balance scenarios
- Rapid concurrent operations
- True simultaneous bursts
- Balance consistency checks

## Test Commands

### Basic Tests
```bash
# Test deduce SP only (30s)
python3 test_deduce_wallet_validation.py --test mm1

# Test normal SP only (30s)
python3 test_deduce_wallet_validation.py --test mm2

# Test all basic scenarios
python3 test_deduce_wallet_validation.py --test all
```

### Race Condition Tests
```bash
# Two-account race (deduce vs non-deduce SPs)
python3 test_deduce_race_conditions.py --test 1a --duration 30

# Four-way race (all account types)
python3 test_deduce_race_conditions.py --test 4way --duration 30

# High-frequency stress test
python3 test_deduce_race_conditions.py --test rapid --duration 20

# Synchronized burst test
python3 test_deduce_race_conditions.py --test burst

# Patron matching MM bets (maker-taker model)
python3 test_deduce_race_conditions.py --test patron_mm --duration 30

# Run everything
python3 test_deduce_race_conditions.py --test all --duration 30
```

## What To Look For

### ✅ PASS Indicators
- **Deduce accounts**: Balance change = Matched bets total
- **Non-deduce accounts**: Balance deducted immediately
- **Zero API errors** (or very low error rate)
- **Consistency checks pass**: All balance reconciliations match

### ❌ FAIL Indicators
- Balance changes don't match expectations
- High API error rates (>5%)
- Double-deductions detected
- Missed deductions

## Quick Results Interpretation

After test completes, check the output:

```
FINAL RESULTS
======================================================================

MM1 (DEDUCE):
  Wagers Placed: 95
  Initial:  $1,021,039.20
  Final:    $1,020,944.20
  Change:   $95.00

MM2 (NORMAL):
  Wagers Placed: 95
  Initial:  $10,019,666.65
  Final:    $10,019,571.65
  Change:   $95.00

DEDUCE VERIFICATION:
✅ PASS - mm1:
  Balance change: $95.00
  Matched total:  $95.00
  Matched bets:   95

Errors: 0
```

**This is a PASS** ✅

## Report Files

Each test creates a JSON report:
- `deduce_wallet_test_{timestamp}.json`
- `race_test_1a_{timestamp}.json`
- `race_test_4way_{timestamp}.json`
- `race_test_rapid_{timestamp}.json`
- `race_test_burst_{timestamp}.json`
- `race_test_patron_mm_{timestamp}.json`

## Recommended Test Sequence

### First Run (Setup Verification)
```bash
# 1. Quick 30-second validation
python3 test_deduce_wallet_validation.py --test enhanced --duration 30

# If PASS, continue...
```

### Standard Run (Daily Testing)
```bash
# 2. Run all race condition tests
python3 test_deduce_race_conditions.py --test all --duration 30
```

### Extended Run (Weekly/Release Testing)
```bash
# 3. Longer duration tests
python3 test_deduce_race_conditions.py --test 1a --duration 120
python3 test_deduce_race_conditions.py --test 4way --duration 120
python3 test_deduce_race_conditions.py --test rapid --duration 60
```

## Troubleshooting

### Test fails to start
```bash
# Check credentials
cat user_config_staging.json

# Verify environment
python3 -c "from src import config; print(config.ENVIRONMENT)"
```

### No matches occurring
- Markets might be closed/inactive
- Try running during peak hours
- Check balance is sufficient

### API errors
- Token might be expired - restart test
- Rate limiting - reduce frequency
- Check network connectivity

## Next Steps

For detailed information:
- See `DEDUCE_TEST_GUIDE.md` for comprehensive documentation
- Review test reports in JSON files
- Monitor database logs during tests
- Check metrics/grafana for system behavior

## Support

If you encounter issues:
1. Check the error messages in output
2. Review the JSON report file
3. Verify account balances are sufficient
4. Consult `DEDUCE_TEST_GUIDE.md` for detailed troubleshooting
