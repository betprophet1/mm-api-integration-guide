# concentration_race

SSE-2441 staging load/verification tool. Fires concurrent wagers from two
genuinely distinct staging accounts onto opposing sides of one line, to
generate the race-condition traffic pattern needed to verify matching
behavior (including the "one order, multiple fills" over-fill check).
Covers both limit orders and market orders (see below) as taker wager types.

## Files

- `accounts.py` — logs in the maker (mm1) and taker (staging patron).
- `market.py` — finds a market for an event and resolves the opposite line
  (a match requires opposite lines with crossing odds, not the same line —
  see `../TEST_MATCHING_BUG_README.md`).
- `logutil.py` — routes the framework's verbose per-request logging to a
  file so the console only shows the run summary.
- `rapid_fire.py` — the scenario + CLI. This is the only entry point.

## Why maker=mm1, taker=patron

mm1 and mm2's staging credentials resolve to the same underlying partner
account (confirmed via identical `/partner/mm/get_balance` responses), so
MM-vs-MM wagers are rejected as `self_match`. There's also only one distinct
staging patron identity in this repo (`src/user_info_patron_staging.json`;
other patron files are sandbox-only). So the maker is always mm1 and the
taker is always the staging patron — there's no account combination available
for a true 3+-party race.

## Usage

Run from the repo root. `MM_ENVIRONMENT` must be `staging` (do not use
`export`, just prefix the command):

```bash
MM_ENVIRONMENT=staging python3 -m concentration_race.rapid_fire
```

Pin a pre-match event instead of auto-discovering (recommended — auto-discovery
can land on a live/in-play event that goes `event_not_available` mid-run):

```bash
MM_ENVIRONMENT=staging python3 -m concentration_race.rapid_fire --event-id 10079186
```

Quick smoke test before a long run:

```bash
MM_ENVIRONMENT=staging python3 -m concentration_race.rapid_fire --duration 8 --rps 1 --event-id 10079186
```

Full 5-minute load:

```bash
MM_ENVIRONMENT=staging python3 -m concentration_race.rapid_fire --duration 300 --rps 5 --event-id 10079186
```

### Flags

| Flag | Default | Meaning |
|---|---|---|
| `--duration` | `300` | Seconds to fire wagers, per side |
| `--rps` | `5` | Target/max bets per second per side, aggregate across `--workers` — a ceiling, not a guarantee, see below |
| `--workers` | `1` | Concurrent placing threads per side |
| `--event-id` | auto-discover | Staging event ID to bet on |
| `--min-stake` | `2.0` | Minimum stake per wager |
| `--max-stake` | `10.0` | Maximum stake per wager |
| `--cancel-interval` | `0` (disabled) | Seconds between maker `cancel_all_wagers` calls, see below |
| `--max-fills-per-min` | `300` | Caps taker-side match-producing placements per rolling 60s window, see below |
| `--market-order-ratio` | `0.2` | Fraction of taker placements that sweep via a market order instead of a limit wager, see below |
| `--check-exposure` | off | Assert the maker's GEC/LEC changed after the run, see below |

Maker picks a random positive odds from the live ladder each wager; taker
mirrors it (`-maker_odds`) — same convention as every other working matching
script in this repo (`patron_match_mm_bets.py`, `test_backend_fairness.py`:
`opposite_odds = -odds`) — so every wager placed in the current "epoch" is
guaranteed to cross. The shared odds value is re-randomized every 3s by a
background thread, so odds still vary over the run without needing two
independent random picks to happen to land on a matchable pair (tried
earlier — with a full ladder they almost never do). Stake is randomized per
wager so some maker orders end up large enough to receive fills from more
than one taker wager, which is what exercises the "one order, multiple
fills" check without any dedicated flag.

### Market orders (`--market-order-ratio`)

By default, 20% of taker placements sweep via a market order
(`trade/private/api/v1/market-orders`) instead of a mirrored limit wager,
against maker's resting liquidity on `match_line_id`. Before firing, the
script calls `estimate-odds` to get the book's current `maxStakeSize` and
clamps the order's stake to it, so it stays matchable; if the book is
momentarily empty (`maxStakeSize <= 0`) it falls back to a normal limit
wager for that iteration instead of firing an order guaranteed not to fill.
Set `--market-order-ratio 0` to disable and use limit orders only.

**This is a genuinely different code path, not just another wager shape.**
`calculateCancelWagerRefundAmount` (ss-trade-app) returns `0` for
market-order-tagged wagers specifically — their cancel/refund goes through
`CreateMarketOrderRefundJob` instead of the generic wager-cancel path this
script otherwise exercises via `cancel_all_wagers`. So `--cancel-interval`'s
race doesn't touch market orders at all; their own known race is different:
a market order matching right at its ~5s auto-cancel timeout, previously
found to cause a real double-payment bug (full refund + the matched amount
both credited) — see `../reproduce_market_order_race_condition.py`, which
is the dedicated reproducer for that specific timeout race. This script's
market-order coverage is about exercising the placement/match code path
alongside everything else here (over-fill check, balance consistency), not
about reproducing that specific timeout bug — use the dedicated reproducer
for that.

Market order results appear as a `Market Orders: N placed, M matched` line
under `TAKER:` (only when `--market-order-ratio > 0`), and as
`market_order_placed`/`market_order_matched` in the JSON report.

### Checking GEC/LEC (`--check-exposure`)

Asserts that the maker's exposure credit actually moves as a result of this
run: **GEC** (`wallet.exposureCredit`, `GET /api/v1/wallet`) and **LEC**
(per-market/outcome entries, `GET /api/v2/wallet/exposures`). Off by default.

```bash
MM_ENVIRONMENT=qa python3 -m concentration_race.rapid_fire --duration 60 --rps 5 --check-exposure --event-id 10079186
```

**Requires the maker account to have a web login on file** (`email`/
`password` in its `src/accounts/{env}/accountN.json`, alongside the usual
`access_key`/`secret_key`) — `partner/auth/login` (the MM login every wager
placement uses) gives an MM-only token, not the web token GEC/LEC need. A
maker with no `email`/`password` on file is skipped (prints a warning, no
error) — checked per-run in `login_maker_web_session()`.

**GEC/LEC only move once the maker has been matched on BOTH sides of the
market — confirmed live, not assumed.** A maker or taker matched on only one
side (which is all the main race loop above ever produces — makers only ever
bet `line_id`) never moves either value, on any account tried, including a
plain patron. So after the race settles, this flag fires one more matched
wager pair with each checked maker on `match_line_id` (mirrored by
`takers[0]` on `line_id`) before taking the "after" snapshot — this is why
the run takes ~10s longer with the flag on. This extra wager is placed
*after* the over-fill/consistency check above is computed, specifically so
it isn't counted into that check's own numbers (which are scoped to the main
race loop's wagers only).

The check asserts **changed**, not "increased" — confirmed live, a LEC
entry's `balance` can move down as exposure nets out, not just up from zero.
Console output:

```
GEC/LEC EXPOSURE CHECK (maker, event <id>, after swap round):
  maker1: GEC $74.57 -> $0  ✅ changed
  maker1: LEC {(219, 4): 20.87, ...} -> {(219, 4): 20.87, (1700044760, 2): 207.54, ...}  ✅ changed
```

LEC is keyed by `(marketId, outcomeId)` — an account can carry entries from
unrelated prior activity on the same event, so the check compares each key's
`balance` before/after rather than entry count.

### Capping the fill rate (`--max-fills-per-min`)

The backend gets high-latency once fills exceed roughly 500/min. Since taker
mirrors maker's odds, virtually every taker placement should cross, so taker
placement rate is used as a proxy for fill rate: `fire_taker` blocks whenever
the trailing-60s count of its own successful placements is at
`--max-fills-per-min`, defaulting to 300 (well under the ~500/min danger
zone). This is a client-side, best-effort proxy — not a DB-verified fill
count — the console prints the observed average (`FILL RATE` section) and
the JSON report has `avg_fills_per_min`. Set `--max-fills-per-min 0` to
disable the cap entirely (e.g. if you're deliberately trying to reproduce the
high-latency condition).

### `--rps` is a target, not a measured rate

Each `place_wager` call blocks on one real HTTP round-trip to staging. With
the default `--workers 1`, `fire_maker`/`fire_taker` are single sequential
loops, so the achieved rate is capped at `1 / request_latency` regardless of
`--rps` — e.g. `--rps 10` with ~0.9s/request round-trips still only places
~1 bet/sec. `--rps` is the aggregate target across `--workers` concurrent
placing threads per side (same convention as `test_backend_fairness.py`'s
`--rps`, "max ... across all workers") — raise `--workers` to actually
approach it:

```bash
MM_ENVIRONMENT=staging python3 -m concentration_race.rapid_fire --duration 60 --rps 10 --workers 5 --event-id 10079186
```

If "Bets Placed" is still far below `rps × duration` after raising
`--workers`, check the `.log` file for `Failed to place wager` lines first —
a low placed count is often rejected requests (e.g. insufficient balance),
not a throughput ceiling; those failures land in the `Errors` count, not
`Bets Placed`.

## Simulating cancel-all vs. an in-flight deduct job (SSE-2441 bulk-cancel gap)

`ss-trade-app`'s single-wager cancel path (`CancelWager` / `internal/repository/postgres/cancel_wager_by_id.go`)
checks `matching_stake_ongoing` and refuses to cancel a wager while its deduct
job is in flight (`ErrWagerHasOngoingDeductJob`). The **bulk** cancel paths —
`CancelWagersByUserId` (MM "cancel all") and `CancelWagersByEventId` /
`CancelUserWagers` (cancel by event) — have no such guard in their SQL; they
only check `status IN ('open','inactive') AND unmatched_stake > 0`. So an MM
calling cancel-all (or ops cancelling by event) while one of that MM's wagers
is mid-match can race the deduct job, and the backend needs to revert/refund
that fill correctly rather than leaving it inconsistent.

Pass `--cancel-interval` to add a third thread that repeatedly calls the
maker's `cancel_all_wagers` panic-button endpoint while maker/taker keep
firing, so cancel-all keeps landing while fresh fills are still mid-deduct:

```bash
MM_ENVIRONMENT=staging python3 -m concentration_race.rapid_fire --duration 60 --rps 5 --cancel-interval 1 --event-id 10079186
```

Only the public MM `cancel_all_wagers` endpoint is exercised this way — the
"cancel by event id" path (`internalCancelUserWagersByEvent`) is an ops-only
internal API with no MM-facing wrapper in this repo, so it isn't reachable
from here.

**Not a duplicate of `test_backend_fairness.py`.** That script already races
`cancel_all_wagers`/`cancel_multiple_wagers` against `place`, with an
`--overlap` mode that fires them simultaneously — but its race target is
cancel-vs-*placement/matching* (did the cancel land before or after any match
at all), and it has no concept of `matching_stake_ongoing` or deduct jobs. It
can't hit the window this section targets: cancel-all landing *after* a match
has already started and a deduct job is actively confirming with the wallet.
`rapid_fire.py`'s maker/taker already organically cross into real matches
(randomized crossing odds, opposite-line resolution), which is what makes it
possible to land cancel-all inside that narrower post-match window. Failed
cancel-all calls are classified with `test_backend_fairness.py`'s
`_classify_cancel_error` (imported, not copied) so the report distinguishes
*why* a cancel failed (`cancel_fail_already_matched`,
`cancel_fail_still_processing`, etc.) instead of just counting pass/fail.

**This tool has no DB access — it can't read `deduct_jobs` or
`fee_transactions` directly.** It only reports its own `cancel_all_wagers`
attempt/success/failure-category counts and the usual balance/matched-bet
numbers. To confirm whether the race was actually hit and handled correctly,
check the DB yourself for wagers cancelled during the run's time window:

- `deduct_jobs` — expect some `failed` (not 100% `succeed`) for jobs whose
  wager got cancelled mid-flight.
- `fee_transactions` — expect some `refunding`/`failed` rows tied to those
  jobs, not every fee settling `succeed` with no refund case.

If every deduct job still comes back `succeed` and every fee transaction
`succeed` with zero refunds, either the race window wasn't hit (try a smaller
`--cancel-interval`, higher `--rps`, or a longer `--duration`) or the bulk
cancel path isn't checking `matching_stake_ongoing` and needs a fix mirroring
`CancelWager`'s guard.

## Reading the output

The console prints a summary; full per-request logs (wager placements,
balance breakdowns, raw API error bodies) go to
`logs/<timestamp>/concentration_race_<timestamp>.log`. A JSON report is also
saved to `logs/<timestamp>/concentration_race_<timestamp>.json` (same
timestamp, same folder, one per run).

```
MAKER:
  Bets Placed:    ...
  Fills Matched:  ...   <- scoped to wager_ids THIS run placed, not the account's history
  Matched Total:  $...
  Balance Change: $...
  Consistency:    CONSISTENT | INCONSISTENT

TAKER:
  ...

MULTI-FILL / OVER-FILL CHECK:
  Orders with >1 fill: N of M filled orders
  Over-filled orders:  none over-filled | 🐛 N OVER-FILLED (bug)
```

**"Fills Matched" and "Consistency" are scoped to this run's own wagers.**
The maker (mm1) is a shared, long-lived staging account with $49k+ of
unrelated matched history — a raw `get_matched_bets` call returns that
history, not this run's results. The script filters by `wager_id` before
computing totals; don't reason about matching from an unfiltered API call
against this account.

**The multi-fill/over-fill check is the actual SSE-2441 assertion.** It
groups this run's maker fills by `wager_id` and verifies no single order's
cumulative fills exceed the stake it was placed for. `Orders with >1 fill`
being 0 just means none of this run's orders happened to draw multiple
fills (random stake/odds, not guaranteed every run) — it is not a failure by
itself. `OVER-FILLED` is the failure signal.

## Known environment quirks (not bugs in this tool)

- `partner/exposure/get_balance` returns a plain `404` on this staging
  deployment — the route doesn't exist here. `get_balance`'s
  `matched_wager_balance`/`unmatched_wager_balance` fields already cover the
  same information, so this isn't fetched. Not to be confused with GEC/LEC
  (`--check-exposure` above), which use different, confirmed-working
  endpoints (`api/v1/wallet`, `api/v2/wallet/exposures`) — this 404'ing route
  is unrelated and unused by this tool.
- `get_matched_bets` on the MM endpoint rejects `limit` values above 100
  (`"Invalid param: invalid limit param"`) — this is why the script caps at
  100 rather than pulling "everything."
- mm1's `Available` balance (`balance - matched_wager_balance -
  unmatched_wager_balance`) is already deeply negative from accumulated
  cross-session test history. This is pre-existing account state, not
  something this run caused.
