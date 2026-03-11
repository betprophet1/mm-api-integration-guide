# DEDUCE Automated Testing Guide

## Overview
Automated test suite for DEDUCE feature testing on staging environment.

## Prerequisites

### 1. Python Environment
```bash
# Activate your virtual environment
source .venv/bin/activate  # or your venv path
```

### 2. Configuration Files
You need to create configuration files for each test account in `src/` directory:

#### `src/user_info_staging.json` (Patron 1)
```json
{
  "access_key": "your_patron1_access_key",
  "secret_key": "your_patron1_secret_key",
  "tournaments": ["MLB"]
}
```

#### `src/user_info_account2_staging.json` (Patron 2)
```json
{
  "access_key": "your_patron2_access_key",
  "secret_key": "your_patron2_secret_key",
  "tournaments": ["MLB"]
}
```

#### `src/user_info_patron_staging.json` (Patron 3)
```json
{
  "access_key": "your_patron3_access_key",
  "secret_key": "your_patron3_secret_key",
  "tournaments": ["MLB"]
}
```

### 3. Test Accounts
Based on the test plan, these are the available test accounts:
- **parlay_cash_patron@betprophet.co** (Password: Testing@123, OTP: 123456)
- **thinh.tran@betprophet.co** (Password: Matkhau1$)
- **lam.tran+usr001@betprophet.co** (Password: Kh0ngbiet1)

## Running the Tests

### Run All Tests
```bash
# Set environment to staging
export MM_ENVIRONMENT=staging

# Run the test suite
python deduce_tests.py
```

### Run with Verbose Logging
```bash
python deduce_tests.py 2>&1 | tee deduce_test_output.log
```

## Test Cases Included

### ✅ Test 1: Basic DEDUCE Flow
- **Purpose**: Verify money is NOT deducted on placement, only on match
- **Steps**:
  1. Check initial balance
  2. Place wager on market
  3. Verify balance unchanged after placement
  4. Check wager status is "open/unmatched"

### ✅ Test 2: Multiple Wagers Same Balance
- **Purpose**: Users can place multiple wagers with same $100 on different unique markets
- **Steps**:
  1. Place 3 wagers on different markets
  2. Verify balance unchanged after each placement
  3. Check total exposure vs balance

### ✅ Test 3: Cancel Before Match
- **Purpose**: User can cancel unmatched wagers, balance remains unchanged
- **Steps**:
  1. Place wager
  2. Verify balance unchanged
  3. Cancel wager
  4. Verify balance still unchanged

### ✅ Test 4: Exposure Calculation (10x Rule)
- **Purpose**: System enforces max exposure = Balance × 10
- **Steps**:
  1. Get current balance
  2. Check exposure limits
  3. Verify 10x rule applied

## Test Output

### Console Output
The tests will output colorized results:
- 🟢 **Green**: Successful operations
- 🔴 **Red**: Failures
- 🟡 **Yellow**: Warnings
- 🔵 **Blue**: Test headers
- 🟣 **Cyan**: Balance information

### Example Output
```
======================================================================
DEDUCE Feature Automated Tests
======================================================================

Setting up test accounts...

✓ Logged in patron1
✓ Logged in patron2
✓ Logged in patron3

Running tests...

======================================================================
Test 1: Basic DEDUCE Flow
======================================================================

💰 patron1 balance: $100.00
💰 patron2 balance: $100.00
Using market: Lakers vs Celtics
✓ patron1 placed wager: $10.0 at odds 150
💰 patron1 balance: $100.00
✓ Balance unchanged after placement (DEDUCE working!)

✓ PASS - Test 1: Basic DEDUCE Flow
  Balance correctly unchanged on placement: $100.00 -> $100.00

...

======================================================================
DEDUCE Test Summary
======================================================================
Total Tests: 4
Passed: 4
Failed: 0
Time: 12.34s
======================================================================

Results saved to: deduce_test_results_20251219_065524.json
```

### JSON Results File
Each test run creates a JSON file with detailed results:

```json
[
  {
    "test": "Test 1: Basic DEDUCE Flow",
    "passed": true,
    "message": "Balance correctly unchanged on placement: $100.00 -> $100.00",
    "details": {
      "patron1_initial": 100.0,
      "patron1_after_place": 100.0,
      "wager_amount": 10.0
    },
    "timestamp": "2025-12-19T06:55:24.123456"
  }
]
```

## Extending the Tests

### Adding New Test Cases

Add new test methods to the `DeduceTestFramework` class:

```python
def test_05_your_new_test(self, patron: str):
    """
    Test 5: Your Test Description
    """
    test_name = "Test 5: Your Test Name"
    logging.info(f"\n{Colors.BOLD}{Colors.BLUE}{'='*70}{Colors.RESET}")
    logging.info(f"{Colors.BOLD}{Colors.BLUE}{test_name}{Colors.RESET}")
    logging.info(f"{Colors.BOLD}{Colors.BLUE}{'='*70}{Colors.RESET}\n")
    
    try:
        # Your test logic here
        
        self.log_test_result(
            test_name,
            True,  # or False
            "Your success message",
            {'detail1': 'value1'}
        )
        
    except Exception as e:
        self.log_test_result(test_name, False, f"Exception: {str(e)}")
```

Then add to `main()`:
```python
framework.test_05_your_new_test('patron1')
```

## Troubleshooting

### Issue: Login Failed
**Solution**: Check your configuration files have correct access_key and secret_key

### Issue: No Available Markets
**Solution**: 
- Check that tournaments have active events
- Try different time of day when more markets are available
- Verify staging environment has test markets

### Issue: Balance Too Low
**Solution**: Contact admin to add funds to test accounts

### Issue: Import Errors
**Solution**: 
```bash
# Make sure you're in the project root
cd /Users/tranlam/Documents/GitHub/mm-api-integration-guide

# Install dependencies
pip install requests pysher
```

## CI/CD Integration

### GitHub Actions Example
```yaml
name: DEDUCE Tests

on:
  push:
    branches: [ qa-mm-autoplay ]
  schedule:
    - cron: '0 */6 * * *'  # Run every 6 hours

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Set up Python
        uses: actions/setup-python@v2
        with:
          python-version: '3.9'
      - name: Install dependencies
        run: pip install -r requirements.txt
      - name: Run DEDUCE tests
        env:
          MM_ENVIRONMENT: staging
        run: python deduce_tests.py
      - name: Upload results
        uses: actions/upload-artifact@v2
        with:
          name: test-results
          path: deduce_test_results_*.json
```

## Best Practices

1. **Run before meetings**: Execute tests 30 minutes before sync-up
2. **Check balances**: Ensure test accounts have sufficient funds
3. **Review failures**: Investigate any failures immediately
4. **Save results**: Keep JSON results for comparison
5. **Clean state**: Cancel all open wagers between test runs if needed

## Quick Reference

| Command | Purpose |
|---------|---------|
| `python deduce_tests.py` | Run all tests |
| `export MM_ENVIRONMENT=staging` | Set environment |
| `cat deduce_test_results_*.json` | View latest results |
| `grep "FAIL" deduce_test_output.log` | Find failures |

## Support

For issues or questions:
1. Check this README
2. Review `DEDUCE_PAIR_TESTING_CHECKLIST.md` for manual test steps
3. Contact QA team

---

**Last Updated**: December 19, 2025  
**Environment**: Staging  
**Python Version**: 3.8+
