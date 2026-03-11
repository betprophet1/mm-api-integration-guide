# Market Types Enhancement - Exposure Scripts Update

## Overview
All exposure testing scripts have been enhanced to support **moneyline, spread, and total** market types for comprehensive exposure feature validation.

---

## Changes Made

### 1. **exposure-stress-test.py**

#### Updated Function: `get_market_data()`

**Before:**
```python
# Only filtered for moneyline markets
if market.get('type') == 'moneyline':
```

**After:**
```python
# Market types to test
MARKET_TYPES = ['moneyline', 'spread', 'total']

# Include all three market types
if market_type in MARKET_TYPES:
```

#### New Features:
- ✅ Collects moneyline, spread, and total markets
- ✅ Tracks count of each market type
- ✅ Displays breakdown in console output
- ✅ Stores `market_type` in market data for reference

#### Output Example:
```
✅ Found 15 usable markets:
   - Moneyline: 5
   - Spread: 5
   - Total: 5
```

---

### 2. **exposure-autoplay.py**

#### Updated Function: `get_events_and_markets()`

**Before:**
```python
# Filter for moneyline markets with marketId=219
if market_type != 'moneyline' or market_id != 219:
    continue
```

**After:**
```python
# Market types to test
MARKET_TYPES = ['moneyline', 'spread', 'total']

# Include all three market types
if market_type not in MARKET_TYPES:
    continue
```

#### New Features:
- ✅ Processes all three market types
- ✅ Tracks market type counts
- ✅ Improved logging with market type information
- ✅ Displays summary of markets collected

#### Output Example:
```
✅ Seeded 12 total markets with betting lines:
   - Moneyline: 4
   - Spread: 4
   - Total: 4
```

---

## Benefits

### 1. **Comprehensive Testing**
- Tests exposure generation across all major market types
- Ensures GEC/LEC works correctly for moneyline, spread, and total bets

### 2. **Better Coverage**
- Previously only tested moneyline markets
- Now covers 3x more market scenarios
- Identifies market-type-specific issues

### 3. **Real-World Scenarios**
- Matches actual user betting patterns
- Tests all market types users would encounter
- Validates exposure calculations across different bet types

### 4. **Improved Visibility**
- Console output shows breakdown by market type
- Easy to identify if specific market types are missing
- Better debugging when markets aren't available

---

## Market Type Characteristics

### Moneyline
- **Description:** Bet on which team will win
- **Selections:** Typically 2 (Team A, Team B)
- **Outcomes:** Usually outcome_id 4 and 5
- **Example:** Lakers to win at +150

### Spread
- **Description:** Bet on point spread/handicap
- **Selections:** 2 sides of the spread
- **Outcomes:** Varies
- **Example:** Lakers -5.5 at -110

### Total
- **Description:** Bet on over/under total score
- **Selections:** Over and Under
- **Outcomes:** Varies
- **Example:** Over 215.5 at -110

---

## Testing Recommendations

### Test Scenarios

#### 1. **Mixed Market Test**
```bash
# Run stress test to ensure all market types work
python exposure-stress-test.py --target 300 --workers 5
```
- Should show markets from all 3 types
- Bets should be placed across different market types
- Exposure should generate correctly for all types

#### 2. **Market Type Balance**
- Verify approximately equal distribution of market types
- Check that workers rotate through different market types
- Ensure no market type is excluded

#### 3. **Exposure Verification**
- Place bets on moneyline → verify GEC/LEC
- Place bets on spread → verify GEC/LEC  
- Place bets on total → verify GEC/LEC
- Compare exposure calculations across types

---

## Backward Compatibility

### What's Preserved:
- ✅ All existing functionality maintained
- ✅ Same API endpoints used
- ✅ Same authentication flow
- ✅ Same bet placement logic
- ✅ Same error handling

### What's Enhanced:
- ✅ Market discovery now includes 3 types instead of 1
- ✅ More markets available for testing
- ✅ Better logging and visibility
- ✅ Market type tracking in data structures

---

## Expected Results

### Console Output Changes

**Before:**
```
✅ Found 5 usable markets
```

**After:**
```
✅ Found 15 usable markets:
   - Moneyline: 5
   - Spread: 5
   - Total: 5
```

### Market Data Structure

**Before:**
```python
{
    'event_id': 123,
    'market_id': 219,
    'selections': [...]
}
```

**After:**
```python
{
    'event_id': 123,
    'market_id': 219,
    'market_type': 'moneyline',  # NEW: market type included
    'selections': [...]
}
```

---

## Troubleshooting

### Issue: No spread or total markets found

**Possible Causes:**
- Tournament doesn't offer these market types
- Event timing (markets added closer to event start)
- Specific sport limitations

**Solution:**
```bash
# Try different tournament/sport
# Check during peak betting hours
# Verify market availability in UI first
```

### Issue: Bets failing on spread/total markets

**Possible Causes:**
- Different outcome_id structure
- Different odds format
- Line movement (spread/total values change)

**Solution:**
- Check worker logic for outcome selection
- Review error logs for specific failure reasons
- May need to adjust odds selection for spread/total

---

## Future Enhancements

### Potential Additions:
1. **Market Type Filtering** - CLI argument to test specific market types
2. **Market Type Quotas** - Ensure minimum bets per market type
3. **Outcome Mapping** - Better handling of outcome_ids by market type
4. **Report Breakdown** - Market type statistics in performance report

### Example Future CLI:
```bash
# Test only spread markets
python exposure-stress-test.py --market-types spread --target 200

# Test specific combination
python exposure-stress-test.py --market-types moneyline,total --target 500
```

---

## Validation Checklist

After deploying these changes:

- [ ] Run exposure-stress-test.py and verify 3 market types appear
- [ ] Confirm bets place successfully on all market types
- [ ] Verify GEC/LEC generates for moneyline bets
- [ ] Verify GEC/LEC generates for spread bets
- [ ] Verify GEC/LEC generates for total bets
- [ ] Check performance report includes all market types
- [ ] Validate no regression in existing functionality
- [ ] Test with multiple tournaments/sports

---

## Summary

### Scripts Updated:
1. ✅ `exposure-stress-test.py` - Multi-threaded stress testing
2. ✅ `exposure-autoplay.py` - Functional exposure testing

### Key Changes:
- Market type filtering expanded from 1 to 3 types
- Market type tracking and reporting added
- Console output enhanced with breakdown
- Data structures include market_type field

### Testing Impact:
- **3x more markets** available for testing
- **Complete coverage** of major bet types
- **Better validation** of exposure feature
- **More realistic** test scenarios

---

**Date:** 2026-01-05  
**Version:** 1.1.0  
**Status:** ✅ Complete
