# Best-Execution Matching Bug Test

## Quick Summary

This test reproduces a bug where simultaneous wagers bypass best-execution price improvement.

### The Bug

**Scenario:**
1. MM places -117 odds on Team A → creates +117 liquidity on Team B
2. Wait 5 seconds
3. MM places -114 on Team A AND Patron places +114 on Team B simultaneously

**Expected (Best Execution):**
- Patron's +114 bet should match against existing +117 first (price improvement)
- Patron gets filled at better odds (+117 > +114)

**Actual Bug:**
- When -114 and +114 arrive at the same time, they match directly with each other
- Patron gets filled at +114 instead of getting price improvement to +117
- The existing +117 liquidity is bypassed

## Quick Start

### Run the Test

```bash
python test_matching_timing_bug.py --env sandbox --event-id 20022672
```

### Find Active Events

```bash
curl -s 'https://api-ss-sandbox.betprophet.co/partner/v2/public/get_multiple_markets?market_types=moneyline,spread,total&event_ids=20022673,20022672,20022674,20022675,20022676,20022677,20022685' \
  -H '__source: web' \
  -H 'accept: application/json, text/plain, */*' \
  -H 'authorization: testtoken' \
  -H 'x-currency: cash' | python3 -m json.tool | grep -E '(event_id|totalStake|name)'
```

Use events with `totalStake > 0`.

## Understanding the Output

### Success Case (Best Execution Working)

```
📊 MATCHING BEHAVIOR VERDICT
================================================================================
✅ Best execution observed: Patron received price improvement to ≥ +117 before any -114 crossing
```

**Meaning:** The system correctly matched patron's +114 order against the existing +117 liquidity first.

### Bug Case (Best Execution Bypassed)

```
📊 MATCHING BEHAVIOR VERDICT
================================================================================
🐛 BUG REPRODUCED: Patron filled at +114 and MM -114 matched, bypassing existing +117 liquidity
```

**Meaning:** The -114 and +114 orders matched directly, ignoring better pricing available at +117.

## What to Check

After running the test, verify:

1. **Patron Fill Price**: Did patron get +117 (good) or +114 (bug)?
2. **MM -114 Status**: Is it matched immediately (suggests bug)?
3. **MM -117 Status**: Did it remain open (suggests bug) or get filled first (correct)?

## Prerequisites

### Required Files

1. `src/user_info.json` or `src/user_info_sandbox.json` - MM account credentials
2. `src/user_info_patron.json` - Patron account credentials

Example `src/user_info_patron.json`:
```json
{
  "email": "patron@example.com",
  "password": "your_password",
  "tournaments": ["MLB"]
}
```

### Account Requirements

- **MM Account**: $30+ balance (for 2 wagers at $10 each)
- **Patron Account**: $10+ balance

## Command Options

```bash
python test_matching_timing_bug.py \
  --env sandbox \              # Environment: sandbox or staging
  --event-id 20022672 \        # Event ID (required)
  --market-type moneyline      # Market: moneyline, spread, or total
```

## Test Flow

### Step-by-Step

1. **Setup**
   - Login MM Account 1
   - Login Patron Account
   - Fetch event markets

2. **Step 1: Create +117 Liquidity**
   - MM places -117 on Team A
   - This creates +117 available on Team B
   - Status logged

3. **Step 2: Wait**
   - 5-second wait to ensure order is in the book

4. **Step 3: Simultaneous Orders**
   - MM places -114 on Team A (thread 1)
   - Patron places +114 on Team B (thread 2, 5ms delay)
   - Both execute nearly simultaneously

5. **Step 4: Analysis**
   - Check patron fill price
   - Check wager statuses
   - Determine if bug occurred

## Technical Details

### Timing Mechanism

The test uses threading to simulate simultaneous orders:
```python
mm_thread = threading.Thread(target=place_mm_bet)
patron_thread = threading.Thread(target=place_patron_bet)  # 5ms delay
```

The 5ms delay ensures MM order arrives slightly first, simulating real-world order arrival.

### Fill Price Detection

The test attempts to extract the actual fill price from the patron API response:
```python
patron_fill_odds = _extract_patron_fill_odds(patron_result.get('response', {}))
```

It walks the response JSON looking for odds values and determines:
- **+117**: Best execution worked ✅
- **+114**: Bug reproduced 🐛

### Bug Detection Logic

```python
best_exec_ok = patron_fill_odds >= 117    # Price improvement happened
bug_reproduced = (patron_fill_odds <= 114) and (mm_wager_status == 'matched')
```

## Customization

### Adjust Simultaneous Timing

Edit line 388 in `test_matching_timing_bug.py`:
```python
time.sleep(0.005)  # Change to 0.001 for 1ms, 0.010 for 10ms, etc.
```

### Change Wait Period

Edit line 368:
```python
for i in range(5, 0, -1):  # Change 5 to desired seconds
```

### Use Different Odds

Edit the odds in the test:
```python
# Step 1: Initial MM wager
odds=-117  # Line 347

# Step 3: Simultaneous orders  
odds=-114  # Line 383 (MM)
odds=114   # Line 393 (Patron, should be positive)
```

### Adjust Stakes

```python
stake=10.0  # Lines 349, 385, 395
```

## Troubleshooting

### "No markets available"
- Use the curl command to find active events
- Check that the event has markets with `totalStake > 0`
- Try a different event ID

### "Failed to place wager"
- Check account balances (need $30 MM, $10 Patron)
- Verify credentials in config files
- Check if event/market is still active

### "Unable to conclusively determine"
- The API response structure might have changed
- Check full logs for raw responses
- Manually inspect wager match details in the backend

### Authentication errors
- Verify `src/user_info.json` has correct MM credentials
- Verify `src/user_info_patron.json` has correct patron credentials
- Check that passwords haven't been changed

## Example Output

```
🚀 BUG REPRODUCTION TEST
================================================================================
Environment: sandbox
Event ID: 20022672
Market Type: moneyline
================================================================================

🔐 Setting up MM Account 1...
✅ MM Account 1 ready (Balance: $1000.00)

🔐 Setting up Patron Account...
✅ Patron Account ready

🧪 BUG REPRODUCTION TEST - BEST EXECUTION WITH SIMULTANEOUS ORDERS
================================================================================

📊 Fetching event 20022672 markets...
✅ Found moneyline market

🏟️  Teams:
   Team A: Brooklyn Nets (Line ID: xxx)
   Team B: Toronto Raptors (Line ID: yyy)

================================================================================
STEP 1: MM Account 1 places wager at -117 odds on Team A
================================================================================
📤 MM Account 1 - Placing wager: Brooklyn Nets at -117 odds
✅ MM Account 1 - Wager placed successfully
   Wager ID: partner_xxxx
   Status: open

✅ Step 1 complete - Wager placed (Status: open)
   This creates an open +117 odds opportunity on Toronto Raptors
   Any Patron bet on Toronto Raptors should get price improvement to +117 (best execution)

================================================================================
STEP 2: Wait 5 seconds
================================================================================
⏳ Waiting... 5 seconds remaining
...

================================================================================
STEP 3: Place simultaneous bets
  - MM Account 1: -114 odds on Team A (a few milliseconds first)
  - Patron Account: +114 odds on Team B (immediately after)

🎯 Expected: Patron +114 should fill against existing +117 first (best execution)
🐛 Bug: If +114 and -114 match directly, best execution is bypassed
================================================================================

🚀 Starting simultaneous bet placement...
✅ Step 3 complete - Both bets placed

================================================================================
STEP 4: Analyzing matching results
================================================================================

📊 Bet Placement Results:
   MM Account 1 (-114): ✅ Success
   Patron Account (+114): ✅ Success

🔍 Matching Analysis:
   MM -114 wager status: matched
   Patron reported filled odds (best-effort): 114.0

⏳ Waiting 2 seconds for matching engine to process...

📋 Final Status Snapshot:
   Wager 1 (MM @ -117): partner_xxxx - initial status: open
   Wager 2 (MM @ -114): partner_yyyy - status: matched
   Wager 3 (Patron @ +114): see filled odds above

================================================================================
📊 MATCHING BEHAVIOR VERDICT
================================================================================
🐛 BUG REPRODUCED: Patron filled at +114 and MM -114 matched, bypassing existing +117 liquidity

💡 To further verify, check:
   1. Patron ticket fill breakdown (did any fill occur at +117?)
   2. MM -114 wager counterparty and fill time
   3. Whether Wager 1 (-117) remained open immediately after these fills

================================================================================
✅ TEST COMPLETE
================================================================================
```

## Why This Matters

### For Market Makers
- Need to understand if their liquidity is truly visible and matchable
- Affects pricing strategies and risk management
- Impacts expected fill rates at posted odds

### For Patrons
- Entitled to best execution (price improvement)
- Missing out on better odds when available
- Affects expected returns over many bets

### For the Platform
- Best execution is a regulatory and competitive requirement
- Must ensure fair and optimal matching
- Impacts user trust and satisfaction

## Next Steps

After reproducing the bug:

1. **Document Results**: Save logs and screenshots
2. **Backend Analysis**: Check match records, timestamps, and counterparties
3. **Root Cause**: Investigate matching engine order processing
4. **Fix**: Implement proper best-execution logic for simultaneous orders
5. **Regression Test**: Add automated tests to prevent recurrence

## Related Files

- `test_matching_timing_bug.py` - Main test script
- `src/mm_calls.py` - MM API wrapper
- `patron_match_mm_bets.py` - Patron matching utilities
- `integrated_mm_patron_matcher.py` - Integrated testing framework
