# Patron Account Bet Matching Setup

This guide explains how to set up and run the patron account betting system that monitors MM accounts and matches their bets.

## Overview

The patron account system has three components:

1. **MM Autoplay** (`multi_account_autoplay_cancel.py`) - Places random bets on all markets
2. **Patron Matcher** (`patron_match_mm_bets.py`) - Fetches events and matches MM bets randomly
3. **Integrated Runner** (`run_mm_and_patron.py`) - Runs both scripts together

## Setup

### Step 1: Create Patron Account Credentials File

Create `src/user_info_patron.json` with patron account credentials (lam.tran+usr004@betprophet.co):

```json
{
    "access_key": "YOUR_PATRON_ACCESS_KEY",
    "secret_key": "YOUR_PATRON_SECRET_KEY",
    "tournaments": ["MLB"]
}
```

### Step 2: Verify MM Account Credentials

Ensure you have MM account credentials configured:
- `src/user_info.json` - Account 1 credentials
- `src/user_info_account2.json` - Account 2 credentials

For staging environment:
- `src/user_info_staging.json`
- `src/user_info_account2_staging.json`
- `src/user_info_patron_staging.json`

## Usage

### Option 1: Run Patron Matcher Only

Just run the patron account to listen for MM activity and match bets:

```bash
# Sandbox environment (default)
python patron_match_mm_bets.py

# Staging environment
python patron_match_mm_bets.py --env staging

# Custom match rate (70% chance to match detected bets)
python patron_match_mm_bets.py --match-rate 0.7

# Custom match delay (500ms before matching)
python patron_match_mm_bets.py --match-delay 0.5
```

### Option 2: Run MM + Patron Together (Recommended)

This runs both MM autoplay and patron matcher simultaneously:

```bash
# Run with defaults (Account 1+2, 70% match rate)
python run_mm_and_patron.py

# Staging environment
python run_mm_and_patron.py --env staging

# Run with specific accounts
python run_mm_and_patron.py --accounts 1

# Custom match rate
python run_mm_and_patron.py --match-rate 0.9

# Full example
python run_mm_and_patron.py --env sandbox --accounts 1,2 --match-rate 0.8 --match-delay 0.3
```

### Option 3: Run MM Autoplay Only

If you only want MM accounts placing bets (no patron matching):

```bash
# Run both MM accounts
python multi_account_autoplay_cancel.py

# Run specific account(s)
python multi_account_autoplay_cancel.py --accounts 1

# Target specific event
python multi_account_autoplay_cancel.py --event "Boston Red Sox"
```

## Configuration Options

### Patron Matcher Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `--env` | string | sandbox | Environment (sandbox or staging) |
| `--match-rate` | float | 0.7 | Probability of matching a detected bet (0.0-1.0) |
| `--match-delay` | float | 0.5 | Delay in seconds before matching a bet |

### MM Autoplay Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `--env` | string | sandbox | Environment (sandbox or staging) |
| `--accounts` | string | 1,2 | Comma-separated account numbers |
| `--event` | string | ALL | Target event name (or "ALL" for all MLB events) |

## How It Works

### Patron Matcher Workflow

1. **Initialization**
   - Loads patron credentials from `src/user_info_patron.json`
   - Logs in to patron account
   - Gets patron balance
   - Fetches all available tournaments and events
   - Seeds market data for all events

2. **Event Monitoring**
   - Maintains internal queue of MM bets to match
   - Monitors patron balance every 30 seconds
   - Reports session statistics every 60 seconds

3. **Bet Matching**
   - Receives MM bet notifications (via `add_mm_bet()` method)
   - Applies match delay (configurable, default 0.5s)
   - Randomly decides whether to match based on match rate
   - Places opposite-side bet in patron account
   - Tracks matched bets in session statistics

### Session Statistics

The patron matcher tracks:
- `events_fetched` - Number of events seeded
- `match_attempts` - Number of bet matching attempts
- `successful_matches` - Successful matched bets
- `failed_matches` - Failed matching attempts
- `balance` - Current patron account balance

## Example Scenarios

### Scenario 1: 100% Match Rate, No Delay

```bash
python run_mm_and_patron.py --match-rate 1.0 --match-delay 0.0
```
- Matches every MM bet immediately
- Good for testing API response under load

### Scenario 2: Low Match Rate with Delay

```bash
python run_mm_and_patron.py --match-rate 0.3 --match-delay 2.0
```
- Matches only 30% of MM bets
- Introduces 2-second delay before matching
- Good for testing intermittent matching

### Scenario 3: Production-like Rate

```bash
python run_mm_and_patron.py --match-rate 0.5 --match-delay 1.0
```
- Matches 50% of MM bets (realistic market making scenario)
- 1-second delay between bets
- Balanced load testing

## Monitoring

### View Patron Balance

Check patron account balance periodically:
```bash
# In a separate terminal while patron matcher is running
python -c "
import sys, os, json
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))
from src import config
from patron_match_mm_bets import PatronAccountMatcher
m = PatronAccountMatcher()
m.load_patron_credentials()
m.patron_login()
m.get_patron_balance()
"
```

### Log Files

Output includes:
- Event fetching progress
- Bet matching attempts and results
- Balance updates
- Session reports every 60 seconds

## Integration with MM API

The patron matcher integrates with existing MM API infrastructure:

- Uses same credential format as MM accounts
- Compatible with existing tournament/event seeding
- Uses same market data structure
- Places wagers using same API endpoints
- Tracks wagers in same session stats format

## Troubleshooting

### Patron credentials not loading

```
❌ Patron config file not found at src/user_info_patron.json
📝 Please create src/user_info_patron.json with patron account credentials
```

**Fix**: Create the JSON file with valid credentials

### Failed to match bet

```
⚠️  Failed to match bet: 400
```

**Possible causes**:
- Event/market data stale (refresh by restarting)
- Selection no longer available
- Patron balance too low
- Invalid odds

### No events fetched

```
✅ Fetched 0 events for patron account
```

**Fix**: Check that:
- Tournaments are correctly configured in `user_info_patron.json`
- Patron account has access to those tournaments
- API is responding correctly

## Advanced Usage

### Custom Match Logic

To modify matching behavior, edit `match_bet()` method in `patron_match_mm_bets.py`:

```python
def match_bet(self, event_id: int, market_type: str, selection_name: str, odds: float):
    # Add custom logic here
    pass
```

### WebSocket Integration

Future enhancement: Replace polling with WebSocket to monitor MM bets in real-time instead of manual queue insertion.

## Performance Notes

- Default configuration uses ~2-5% CPU
- Memory footprint: 50-100 MB
- Network: Minimal bandwidth (API calls only)
- Best run on dedicated test environment
- Test for 30+ minutes to validate stability

## Support

For issues or questions:
1. Check logs for error messages
2. Verify all credentials are correct
3. Ensure sufficient balance in patron account
4. Try with lower match rate to isolate issues
