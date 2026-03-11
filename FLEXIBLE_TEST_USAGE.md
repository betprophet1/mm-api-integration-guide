# Flexible Cancel Race Test - Usage Guide

## Overview
The **`flexible`** test in `test_deduce_race_conditions.py` allows you to toggle deduce/non-deduce accounts on or off for targeted bug investigation.

**Added**: January 28, 2026  
**Test Name**: `test_flexible_cancel_race`

---

## Features

### ✅ Toggle Deduce/Non-Deduce
- `--no-deduce`: Exclude ALL deduce accounts (MM1 + Patron deduce)
- Default: Include both deduce and non-deduce accounts

### ✅ Select Specific Accounts
- `--no-mm1`: Exclude MM1 (deduce account)
- `--no-mm2`: Exclude MM2 (non-deduce account)
- `--no-patron`: Exclude all patron accounts

### ✅ Configurable Parameters
- `--duration`: Test duration in seconds
- `--event-id`: Specific event to test
- Cancel rate: 50% (hardcoded, can be modified in code)

---

## Usage Examples

### 1. **Non-Deduce Only** (Your Use Case)
Test with only non-deduce accounts for bug investigation:

```bash
python3 test_deduce_race_conditions.py \
  --test flexible \
  --event-id 30024812 \
  --duration 30 \
  --no-deduce
```

**Accounts Used**:
- ✅ MM2 (non-deduce)
- ✅ Patron non-deduce
- ❌ MM1 (deduce) - excluded
- ❌ Patron deduce - excluded

---

### 2. **Deduce Only**
Test with only deduce accounts:

```bash
python3 test_deduce_race_conditions.py \
  --test flexible \
  --event-id 30024812 \
  --duration 30 \
  --no-mm2 \
  --no-patron
```

Then manually add deduce patron (or modify code).

**Accounts Used**:
- ✅ MM1 (deduce)
- ✅ Patron deduce
- ❌ MM2 (non-deduce) - excluded
- ❌ Patron non-deduce - excluded

---

### 3. **MM Only** (No Patrons)
Test with only MM accounts:

```bash
python3 test_deduce_race_conditions.py \
  --test flexible \
  --event-id 30024812 \
  --duration 60 \
  --no-patron
```

**Accounts Used**:
- ✅ MM1 (deduce)
- ✅ MM2 (non-deduce)
- ❌ Patrons - excluded

---

### 4. **MM2 Only** (Single Account Test)
Test with just one account:

```bash
python3 test_deduce_race_conditions.py \
  --test flexible \
  --event-id 30024812 \
  --duration 30 \
  --no-deduce \
  --no-patron
```

**Accounts Used**:
- ✅ MM2 (non-deduce) only
- ❌ All others excluded

---

### 5. **All Accounts** (Default)
Test with all accounts (deduce + non-deduce):

```bash
python3 test_deduce_race_conditions.py \
  --test flexible \
  --event-id 30024812 \
  --duration 30
```

**Accounts Used**:
- ✅ MM1 (deduce)
- ✅ MM2 (non-deduce)  
- ✅ Patron deduce
- ✅ Patron non-deduce

---

## Test Output

### Console Output
- Real-time bet placement logs
- Color-coded by account type:
  - 🔵 Cyan: Deduce accounts
  - 🟣 Magenta/Blue: Non-deduce accounts
- Balance changes
- Error summary
- Bet delay statistics

### Report File
Generated automatically with naming pattern:
```
flexible_cancel_{deduce|nondeduce}_{event_id}_{timestamp}.json
```

**Example**: `flexible_cancel_nondeduce_30024812_1769564153.json`

**Contains**:
- Full test configuration
- Account details
- Wager statistics
- Error logs
- Balance changes
- Bet processing delays

---

## Sample Test Run (Event 30024812)

### Command:
```bash
python3 test_deduce_race_conditions.py \
  --test flexible \
  --event-id 30024812 \
  --duration 30 \
  --no-deduce
```

### Results:
```
Mode: NON-DEDUCE ONLY
Accounts: MM2 + Patron non-deduce

MM2 Performance:
├─ Placed: 28
├─ Cancelled: 15 (53.6%)
└─ Balance Change: -$26.15

Patron Performance:
├─ Matches: 27
└─ Balance Change: -$54.00

Delay Stats:
├─ Avg: 0.670s
├─ Min: 0.639s
└─ Max: 0.784s

Errors: 0
Status: ✅ SUCCESS
```

---

## Comparison: Dedicated vs Flexible Test

### Old Way (Separate Scripts)
```bash
# Had to create separate script
python3 test_nondeduce_only.py
```

**Issues**:
- ❌ Separate file to maintain
- ❌ Code duplication
- ❌ Hard to switch between modes

### New Way (Flexible Test)
```bash
# Just toggle flags
python3 test_deduce_race_conditions.py --test flexible --no-deduce
```

**Benefits**:
- ✅ Single unified test
- ✅ No code duplication
- ✅ Easy to switch modes
- ✅ Integrated with main test suite

---

## Advanced Usage

### Custom Cancel Rate
To change cancel rate from default 50%, modify the code:

```python
# In test_deduce_race_conditions.py, line ~3561
test_flexible_cancel_race(
    duration=args.duration,
    cancel_rate=0.8,  # Change to 80%
    event_id=args.event_id,
    use_deduce=not args.no_deduce,
    use_mm1=not args.no_mm1,
    use_mm2=not args.no_mm2,
    use_patron=not args.no_patron
)
```

### Programmatic Usage
```python
from test_deduce_race_conditions import test_flexible_cancel_race

# Non-deduce only
test_flexible_cancel_race(
    duration=30,
    cancel_rate=0.5,
    event_id=30024812,
    use_deduce=False,  # Exclude deduce
    use_mm1=True,
    use_mm2=True,
    use_patron=True
)

# MM2 only
test_flexible_cancel_race(
    duration=30,
    cancel_rate=0.5,
    event_id=30024812,
    use_deduce=False,
    use_mm1=False,  # Exclude MM1
    use_mm2=True,   # Only MM2
    use_patron=False  # No patrons
)
```

---

## Available Tests

### Full Test List
```bash
# View all available tests
python3 test_deduce_race_conditions.py --help
```

**Test Options**:
- `1a` - Basic deduce vs non-deduce
- `4way` - 4-account mexican standoff
- `rapid` - High-frequency stress test
- `burst` - Simultaneous burst test
- `patron_mm` - Patron matching test
- `deduce_matched` - Deduce timing verification
- `aggressive` - All-lines betting
- `cancel_bug` - Cancel race bug test
- `live_delay` - 5-second delay verification
- **`flexible`** ⭐ NEW - Configurable cancel race test
- `all` - Run all tests

---

## Troubleshooting

### No Accounts Configured
```
❌ No accounts configured!
```

**Cause**: All accounts excluded by flags  
**Fix**: Remove some exclusion flags

### Event ID Required
For specific event testing, always provide `--event-id`:
```bash
python3 test_deduce_race_conditions.py --test flexible --event-id 30024812 --no-deduce
```

### Patron Login Failed
If patron accounts fail to login, check credentials in:
- `src/user_info_patron_sandbox.json`

---

## Files Generated

### Log File
If using `tee` to capture output:
```bash
flexible_test_30024812.log
```

### Report File
JSON report with full test data:
```bash
flexible_cancel_nondeduce_30024812_{timestamp}.json
```

### Documentation
- `FLEXIBLE_TEST_USAGE.md` (this file)
- `TEST_SUITE_SUMMARY_RACE_CONDITIONS.md` (full suite docs)
- `NONDEDUCE_TEST_RESULTS_30024812.md` (previous test results)

---

## Next Steps

### For Bug Investigation
1. **Run non-deduce test** (done ✅)
   ```bash
   python3 test_deduce_race_conditions.py --test flexible --event-id EVENT_ID --no-deduce
   ```

2. **Run with deduce accounts**
   ```bash
   python3 test_deduce_race_conditions.py --test flexible --event-id EVENT_ID
   ```

3. **Compare results** to identify deduce-specific issues

### For Longer Tests
```bash
# 5 minute stress test
python3 test_deduce_race_conditions.py --test flexible --event-id EVENT_ID --duration 300 --no-deduce
```

### For Higher Cancel Rate
Modify code to set `cancel_rate=0.8` or `0.9` for more aggressive testing

---

## Summary

✅ **Added flexible test to main test suite**  
✅ **Can toggle deduce/non-deduce with command flags**  
✅ **Tested successfully on event 30024812**  
✅ **Generates detailed logs and reports**  
✅ **Integrated with existing test framework**

**Your Request**: ✅ COMPLETED
- Non-deduce only mode available via `--no-deduce` flag
- Integrated into `test_deduce_race_conditions.py`
- No separate script needed
- Full logging and reporting included
