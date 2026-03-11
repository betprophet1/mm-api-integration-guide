# Wallet Balance Validation for DEDUCE Feature

## Overview

This guide explains how to validate wallet balance behavior for accounts with different deduction models:

- **MM1**: Has DEDUCE enabled - balance deducted only when bet is matched
- **MM2 & Patron**: Normal behavior - balance deducted immediately when bet is placed

## Key Concepts

### DEDUCE Model (MM1)
When DEDUCE is enabled:
1. **Placing a bet**: Balance remains UNCHANGED
2. **Bet gets matched**: Balance is deducted by the matched bet stake
3. **Validation**: Check if balance deduction equals total matched bets stakes

### Normal Model (MM2 & Patron)
Without DEDUCE:
1. **Placing a bet**: Balance is immediately reduced by stake amount
2. **Bet gets matched**: No additional balance change (already deducted)
3. **Validation**: Check if balance decreased by exactly the stake amount

## Implementation

### 1. New API Endpoint

Added to `src/config.py`:
```python
'mm_get_matched_bets': 'partner/mm/get_matched_bets'
```

This endpoint retrieves recent matched bets for an account.

### 2. Helper Methods

#### `get_matched_bets(account_name, limit=100, offset=0)`
Retrieves matched bets from the API.

**Usage:**
```python
framework = DeduceTestFramework(environment='sandbox')
framework.login_account('mm1', mm1_creds, account_type='mm')

matched_bets = framework.get_matched_bets('mm1', limit=50)
print(f"Found {len(matched_bets)} matched bets")
```

#### `check_balance_change_for_deduce(account_name, initial_balance, wager_stake, expected_behavior)`
Validates wallet balance changes based on the account's deduction model.

**Parameters:**
- `account_name`: Name of the account to check
- `initial_balance`: Balance before placing bets
- `wager_stake`: The stake amount that was placed
- `expected_behavior`: Either `'deduce'` or `'normal'`

**Returns:**
```python
{
    'account': 'mm1',
    'initial_balance': 1000.0,
    'current_balance': 1000.0,
    'balance_change': 0.0,
    'expected_behavior': 'deduce',
    'is_valid': True,
    'message': '✓ No matched bets, balance unchanged (DEDUCE working correctly)',
    'matched_bets_count': 0,  # Only if matched bets exist
    'matched_bets_total': 0.0  # Only if matched bets exist
}
```

## Usage Examples

### Example 1: Validate MM1 (DEDUCE enabled)

```python
from deduce_tests import DeduceTestFramework
from src import config

# Initialize
framework = DeduceTestFramework(environment='sandbox')

# Login MM1
mm1_creds = config.get_account_credentials(1, 'sandbox')
framework.login_account('mm1', mm1_creds, account_type='mm')

# Get initial balance
initial_balance = framework.get_balance('mm1')['balance']

# Place a bet (assume you have market_info)
wager = framework.place_wager('mm1', line_id, 150, 10.0)

# Validate wallet balance
result = framework.check_balance_change_for_deduce(
    'mm1',
    initial_balance,
    10.0,
    expected_behavior='deduce'
)

if result['is_valid']:
    print(f"✓ PASS: {result['message']}")
else:
    print(f"✗ FAIL: {result['message']}")
```

### Example 2: Validate MM2 (Normal behavior)

```python
# Login MM2
mm2_creds = config.get_account_credentials(2, 'sandbox')
framework.login_account('mm2', mm2_creds, account_type='mm')

# Get initial balance
initial_balance = framework.get_balance('mm2')['balance']

# Place a bet
wager = framework.place_wager('mm2', line_id, 150, 10.0)

# Validate wallet balance
result = framework.check_balance_change_for_deduce(
    'mm2',
    initial_balance,
    10.0,
    expected_behavior='normal'
)

if result['is_valid']:
    print(f"✓ PASS: {result['message']}")
else:
    print(f"✗ FAIL: {result['message']}")
```

### Example 3: Run the Complete Test Suite

```bash
# Run all tests (MM1, MM2, Patron comparison)
python test_deduce_wallet_validation.py --test all

# Run only MM1 test
python test_deduce_wallet_validation.py --test mm1

# Run only MM2 test
python test_deduce_wallet_validation.py --test mm2
```

## Test Script: `test_deduce_wallet_validation.py`

This script provides three test modes:

### 1. MM1 DEDUCE Test
Tests MM1 account with DEDUCE enabled:
- Places a bet
- Verifies balance unchanged initially
- Checks matched bets to validate any balance deductions

### 2. MM2 Normal Test
Tests MM2 account with normal behavior:
- Places a bet
- Verifies balance decreased immediately by stake amount

### 3. All Accounts Comparison
Tests all available accounts side-by-side:
- MM1 (deduce)
- MM2 (normal)
- Patron (normal, if configured)

Provides a summary comparison of behaviors.

## Validation Logic

### For DEDUCE accounts (MM1):
```python
if no matched bets and balance unchanged:
    ✓ PASS - DEDUCE working correctly
elif matched bets exist and balance_change == sum(matched_stakes):
    ✓ PASS - Balance matches matched bets total
else:
    ✗ FAIL - Balance change doesn't match expected behavior
```

### For Normal accounts (MM2, Patron):
```python
if balance_change == wager_stake:
    ✓ PASS - Balance decreased as expected
else:
    ✗ FAIL - Balance change doesn't match wager stake
```

## Integration with Existing Tests

You can integrate this validation into your existing test suites:

```python
# In your test method
def test_my_scenario(self):
    # ... place bets ...
    
    # Validate MM1 (DEDUCE)
    mm1_result = framework.check_balance_change_for_deduce(
        'mm1', mm1_initial_balance, stake, 'deduce'
    )
    
    # Validate MM2 (Normal)
    mm2_result = framework.check_balance_change_for_deduce(
        'mm2', mm2_initial_balance, stake, 'normal'
    )
    
    # Assert results
    assert mm1_result['is_valid'], mm1_result['message']
    assert mm2_result['is_valid'], mm2_result['message']
```

## Expected Outputs

### MM1 (DEDUCE) - No Matched Bets
```
✓ No matched bets, balance unchanged (DEDUCE working correctly)
Initial Balance:  $1000.00
Current Balance:  $1000.00
Balance Change:   $0.00
```

### MM1 (DEDUCE) - With Matched Bets
```
✓ Balance change ($30.00) matches matched bets total ($30.00)
Initial Balance:  $1000.00
Current Balance:  $970.00
Balance Change:   $30.00
Matched Bets:     3
Matched Total:    $30.00
```

### MM2 (Normal)
```
✓ Balance decreased by wager stake ($10.00) as expected (normal behavior)
Initial Balance:  $1000.00
Current Balance:  $990.00
Balance Change:   $10.00
Expected Change:  $10.00
```

## API Response Format

### Get Matched Bets Response
```json
{
  "data": {
    "matched_bets": [
      {
        "id": "bet_123",
        "stake": 10.0,
        "odds": 150,
        "matched_at": "2026-01-12T13:45:00Z",
        "event_id": 12345,
        "market_id": 67890
      }
    ]
  }
}
```

## Troubleshooting

### Issue: Balance changed but no matched bets found
This could indicate:
- Matched bets API not returning all records (check limit/offset)
- System deduction for other reasons (fees, settlements)
- DEDUCE not properly enabled on the account

### Issue: Balance didn't change for normal account
This could indicate:
- Insufficient balance
- Bet placement failed
- Bet is pending/processing

### Issue: Matched bets total doesn't match balance change
This could indicate:
- Other bets were matched between checks
- Partial matches or cancellations occurred
- Need to query more matched bet records (increase limit)

## Notes

- Always wait a few seconds after placing bets before checking balance to allow for processing
- For DEDUCE accounts, balance changes are asynchronous (happen when matched)
- For normal accounts, balance changes are immediate
- The `get_matched_bets` endpoint may have pagination - adjust `limit` if needed
- Account behavior (deduce vs normal) is configured at the account level, not per-bet
