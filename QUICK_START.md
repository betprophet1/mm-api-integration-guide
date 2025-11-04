# Quick Start: Running MM + Patron Test

## Current Status

✅ **Scripts created and ready:**
- `patron_match_mm_bets.py` - Patron account bet matcher
- `run_mm_and_patron.py` - Integrated runner (runs both MM and patron)
- `multi_account_autoplay_cancel.py` - MM account betting (already exists)

✅ **Dependencies installed:**
- All required Python packages installed via requirements.txt

## Next Step: Add Patron Credentials

The patron account credentials file has been created at:
```
src/user_info_patron.json
```

**Currently contains placeholder values:**
```json
{
    "access_key": "REPLACE_WITH_PATRON_ACCESS_KEY",
    "secret_key": "REPLACE_WITH_PATRON_SECRET_KEY",
    "tournaments": ["MLB"]
}
```

### To Run Tests

1. **Update patron credentials** in `src/user_info_patron.json`:
   ```bash
   # Edit the file with actual credentials for lam.tran+usr004@betprophet.co
   nano src/user_info_patron.json
   ```

2. **Run the integrated test**:
   ```bash
   # Run MM accounts + patron matcher together
   python3 run_mm_and_patron.py
   
   # Or with custom settings
   python3 run_mm_and_patron.py --match-rate 0.9 --match-delay 0.3
   ```

3. **Or run just the patron matcher**:
   ```bash
   python3 patron_match_mm_bets.py
   ```

4. **Or run just MM accounts**:
   ```bash
   python3 multi_account_autoplay_cancel.py
   ```

## Command Options

### Integrated Runner
```bash
python3 run_mm_and_patron.py [OPTIONS]

Options:
  --env {sandbox,staging}      Environment (default: sandbox)
  --accounts ACCOUNTS          MM accounts to run (default: 1,2)
  --match-rate RATE            Patron match rate 0.0-1.0 (default: 0.7)
  --match-delay DELAY          Delay before matching in seconds (default: 0.5)

Examples:
  python3 run_mm_and_patron.py                              # Defaults
  python3 run_mm_and_patron.py --env staging                # Staging env
  python3 run_mm_and_patron.py --match-rate 1.0             # Match all
  python3 run_mm_and_patron.py --accounts 1 --match-rate 0.5 # Account 1 only, 50% match
```

### Patron Matcher Only
```bash
python3 patron_match_mm_bets.py [OPTIONS]

Options:
  --env {sandbox,staging}      Environment (default: sandbox)
  --match-rate RATE            Match rate 0.0-1.0 (default: 0.7)
  --match-delay DELAY          Delay before matching in seconds (default: 0.5)
```

### MM Autoplay Only
```bash
python3 multi_account_autoplay_cancel.py [OPTIONS]

Options:
  --env {sandbox,staging}      Environment (default: sandbox)
  --accounts ACCOUNTS          Accounts to run (default: 1,2)
  --event EVENT                Target event name or "ALL" (default: ALL)
```

## What Happens When You Run

### MM Autoplay Script
- Logs in to MM accounts (1 and 2)
- Gets starting balance
- Seeds tournaments and events
- Places random bets on all markets in an infinite loop
- Monitors balance every 2 seconds
- Reports session stats every 30 seconds
- Continues betting even with $0 balance

### Patron Matcher Script
- Logs in to patron account
- Fetches all available events and markets
- Monitors balance every 30 seconds
- Ready to accept and match MM bets
- Reports session stats every 60 seconds
- Currently waiting for MM bets (manual integration needed)

### Integration Notes

**Current limitation:** The patron matcher has a method `add_mm_bet()` to accept bets, but the MM script doesn't currently call it. To fully integrate:

1. Option A: **Manual bet queue** - Update MM script to notify patron matcher of new bets
2. Option B: **WebSocket monitoring** - Implement real-time monitoring via WebSocket
3. Option C: **Shared API monitoring** - Both scripts query API separately

## Troubleshooting

### "Patron login failed: 401"
- ❌ Invalid credentials in `src/user_info_patron.json`
- ✅ Replace with actual patron account credentials

### "No events fetched"
- ❌ Check if patron account has access to tournaments
- ✅ Verify tournaments list in config matches available tournaments

### MM script exits immediately
- ❌ Check if MM account credentials are valid
- ✅ Verify `src/user_info.json` and `src/user_info_account2.json`

### Import errors
- ❌ Missing dependencies
- ✅ Run: `pip3 install --break-system-packages -r requirements.txt`

## Files

### Core Scripts
- `patron_match_mm_bets.py` - Main patron matcher (521 lines)
- `run_mm_and_patron.py` - Integrated runner (195 lines)
- `multi_account_autoplay_cancel.py` - MM betting (existing)

### Config Files
- `src/user_info.json` - Account 1 credentials (MM)
- `src/user_info_account2.json` - Account 2 credentials (MM)
- `src/user_info_patron.json` - Patron credentials (needs update)

### Documentation
- `PATRON_SETUP.md` - Detailed setup guide
- `QUICK_START.md` - This file

## Next Steps

1. Add real patron credentials to `src/user_info_patron.json`
2. Run: `python3 run_mm_and_patron.py`
3. Monitor the output for bet placement and matching activity
4. Enhance integration to pass MM bets to patron matcher

## Support

See `PATRON_SETUP.md` for detailed documentation and troubleshooting.
