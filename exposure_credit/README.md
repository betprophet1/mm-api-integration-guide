# exposure_credit

Generates a real, verifiable GEC/LEC balance, then spends it, in two
separate steps. Built after `concentration_race`'s `--check-exposure` flag
showed that firing many small wagers in a burst (1500 wagers, one $2 spend)
produces almost no usable signal -- see the findings below for why, sourced
from `ss-exposure-app` / `ss-wallet-app`.

## Why two separate scripts, not one

Generating credit and spending it are separated by an async pipeline delay
(confirmed in source, not guessed): `ss-exposure-app`'s processor ticks every
5s (batch 100 wagers/tick), and `ss-wallet-app` polls it back every 5s
(`run_sync_exposure_data_to_wallet_detail.sh 5`). A burst of wagers can sit
in that pipeline for well over the 6-10s other scripts in this repo wait.
Rather than a fixed `sleep(30-60)` guess, `spend.py` **polls** every 5s (its
`--poll-interval`, matching the pipeline's own cadence) up to a 90s timeout
(`--timeout`) until the balance actually changes.

## Files

- `generate.py` — places ONE deliberately larger (`--stake`, default $100)
  both-sides match, confirms both sides actually MATCHED (not just placed),
  and saves state to `state/<timestamp>.json`.
- `spend.py` — loads that state, polls until GEC and/or LEC land, then
  places one more matched wager that's eligible to draw down that credit,
  and reports whether it actually decreased.
- `state/` — generated run state (gitignored, like `logs/`).

## Usage

```bash
MM_ENVIRONMENT=qa python3 -m exposure_credit.generate --event-id 10079295 --stake 100
# generate.py defaults to --rounds 50 (see below) -- prints the matching spend.py command, e.g.:
MM_ENVIRONMENT=qa python3 -m exposure_credit.spend --state-file exposure_credit/state/1787800000.json --rounds 50
```

`generate.py` defaults to `--rounds 50` -- a single match produces too little
signal for load-test purposes (one earlier run: 1500 tiny wagers, $2 spent).
Each round is its own market lookup + fresh maker/taker match, so both wager
count and cumulative GEC/LEC scale with `--rounds`. Pass `--rounds 1` for the
old single-match behavior:

```bash
MM_ENVIRONMENT=qa python3 -m exposure_credit.generate --rounds 1 --stake 200
```

`generate.py --rounds N` fires N independent both-sides matches (2N wagers),
skipping any round that rests instead of crossing rather than aborting the
whole run, and saves only the rounds that actually matched. `spend.py
--rounds N` processes up to N of those: for LEC each round targets its own
generate.py round's event (since LEC is scoped per event/market/outcome); for
GEC (one global, cumulative balance) it's just N follow-up spend attempts on
N different events. `spend.py --rounds` defaults to however many rounds
`generate.py` actually saved (read from the state file, not re-typed).

`spend.py` handles the wait itself — just run it right after `generate.py`,
there's no separate sleep step. It'll print each poll:

```
⏳ Polling for GEC to change from baseline ($0)...
  [0s] GEC: 0
  [5s] GEC: 0
  [10s] GEC: 1.72
✅ GEC landed: $1.72
```

### Flags

| Flag | Default | Meaning |
|---|---|---|
| `--maker-account` | `1` | MM account (config.get_account_credentials key). Must have `email`/`password` on file for its web token — see `src/accounts/qa/account1.json` for the pattern. |
| `--taker-account` | `patron` | Patron account id. |
| `--event-id` | auto-discover | Event to bet on. |
| `--stake` (generate) | `100.0` | Stake per side of the generating match — this is a cash amount, not a wager count. See "Why stake size, not wager count" below. |
| `--rounds` (both) | `50` (generate) / all saved rounds (spend) | Number of independent both-sides matches / follow-up spend attempts. See "Usage" above. |
| `--credit-type` (spend) | `both` | `gec`, `lec`, or `both`. |
| `--spend-stake` (spend) | `20.0` | Stake for the follow-up wager. Keep it at or below the polled balance for an unambiguous read — see the GEC caveat below. |
| `--poll-interval` / `--timeout` (spend) | `5.0` / `90.0` | Matches the pipeline's own tick; raise `--timeout` if a run is under heavy load. |

## What GEC/LEC actually are (confirmed against source)

From `ss-exposure-app`'s own README:

- **LEC** (Local Exposure Credit): scoped to one event+market+outcome+line.
  Backing Home grants LEC on Away.
- **GEC** (Global Exposure Credit): usable on *any* market/event. Granted
  when a user has matched wagers on **both outcomes of a market**.

The actual formula (`internal/calculator/moneylineV1.go`), for a moneyline
market:

```
p1, p2 = matched profit on each outcome
s1, s2 = matched stake on each outcome
cash   = matched CASH-funded (not credit-funded) portion of both legs

GEC = max(0, min(0, p1 - s2, p2 - s1) + cash)
```

This is a **guaranteed-winnings** calculation, not a stake-volume one. It
scales with stake size and how unbalanced the odds are — not with how many
wagers you place. That's why `generate.py` places ONE larger match instead
of many small ones.

LEC's own crediting (`internal/calculator/local.go`) lands on the **opposite**
outcome+line of what you matched — confirmed live, not assumed: matching
outcome 5 moved outcome 4's balance, not outcome 5's. `spend.py` accounts for
this by targeting whichever `(marketId, outcomeId)` entry has the largest
balance, not the one you originally bet on.

**Spending is automatic.** `ss-wallet-app`'s `designateMatchedStake` funds a
new wager's stake from available LEC/GEC before cash, as long as the new
wager is eligible (same outcome+line for LEC, any market for GEC). There's
no "use my credit" flag to pass — `spend.py` just places an eligible wager
and checks whether the balance dropped.

### GEC follow-up caveat

Matching a new wager on a different event is *also*, independently, a fresh
"both sides matched" event. If the follow-up stake is funded entirely by
existing GEC, `cash=0` in the formula above, so it contributes zero *new*
GEC — a clean decrease confirms spend. If GEC only partially covers the
stake, the cash-funded remainder generates a bit of fresh GEC on top,
muddying the delta. Keep `--spend-stake` at or below the polled GEC to avoid
this.

## Known limitations

- No DB access from this tool — like `concentration_race`, it only reads the
  wallet's own API (`/api/v1/wallet`, `/api/v2/wallet/exposures`). If a run
  looks wrong, cross-check `wallet_details`/`wallet_detail_logs`
  (`total`/`spent`/`paid_back`/`status`) directly.
- `generate.py`'s match can rest instead of crossing (odds/liquidity vary
  run to run) — it checks `get_matched_bets` before declaring success and
  exits non-zero if it didn't actually match, rather than saving unusable
  state.
