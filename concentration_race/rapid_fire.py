"""Concentration race: fire randomized-odds, randomized-stake wagers from
genuinely distinct staging accounts (makers=MM accounts, takers=staging
patron accounts) onto opposing sides of one line. Supports one or many
accounts per side (see --maker-accounts/--taker-accounts).

Maker picks a random positive odds from the live ladder; taker mirrors it
(-maker_odds) so every wager placed in the same "epoch" is guaranteed to
cross -- same convention as every other working matching script in this repo
(patron_match_mm_bets.py, test_backend_fairness.py: opposite_odds = -odds).
The shared odds value is re-randomized every ODDS_REFRESH_INTERVAL_SEC
seconds by a dedicated thread, so odds still vary over the run without
needing per-wager independent randomness (which, tried earlier, almost never
crosses -- two independent random picks off the same ladder rarely land on
a matchable pair). See TEST_MATCHING_BUG_README.md for why the opposite line
(not the same line) is required for a match.

Stake is randomized per wager (--min-stake/--max-stake) so some maker orders
end up large enough to receive fills from more than one taker wager over the
run -- this naturally covers the SSE-2441 "one order, multiple fills"
scenario without needing a dedicated script or flag for it. After the run,
every maker wager's fills are grouped by wager_id and checked: total filled
must never exceed that order's placed stake.

Optional (--cancel-interval > 0): a third thread repeatedly calls every
maker's cancel_all_wagers panic-button endpoint while maker/taker keep
firing, to race a cancel-all against wagers that currently have a deduct job
in flight (mid-match, not yet wallet-confirmed). The single-wager cancel path
checks matching_stake_ongoing and refuses to cancel a wager mid-deduct
(ErrWagerHasOngoingDeductJob), but the bulk cancel-all/cancel-by-event paths
(CancelWagersByUserId / CancelWagersByEventId in ss-trade-app) do not -- so
this is the scenario that needs the deduct job to revert/refund correctly on
cancellation. This tool has no DB access: it can only generate the race and
report its own attempt counts / balance deltas. Confirm the actual outcome by
checking deduct_jobs and fee_transactions status (expect some 'failed'/
'refunding' rows, not 100% 'succeed') in the DB for wagers cancelled during
this run.

--rps is a target/max rate aggregate across every worker AND every account on
a side, enforced by one shared RateLimiter per side (same convention as
test_backend_fairness.py's --rps: "max wagers per second across all
workers"). --workers means concurrent workers per account on that side, so
total maker threads = --maker-workers * len(--maker-accounts), and likewise
for takers.

--max-fills-per-min (default 300) caps taker-side match-producing placements
to a rolling 60s window, throttling fire_taker whenever the recent rate is at
the cap. This is a best-effort client-side proxy for real fills, not a DB-
verified count: since taker mirrors maker's odds every wager should cross,
so taker placement rate approximates match rate. Needed because the backend
gets high-latency above ~500 fills/min; keep this run's rate well under
whatever threshold you're protecting. It's a single cap shared across every
taker account, since it approximates total match rate on the line, not
per-account throughput.

--maker-accounts/--taker-accounts take one or more account identifiers (see
concentration_race/accounts.py). Default is the original single-account
behavior: maker=mm1, taker=the staging patron. login_makers() verifies every
maker resolves to a genuinely distinct partner (mm1/mm2 collide on staging,
see accounts.py) and fails fast on a collision rather than letting it surface
as a pile of self_match wager errors mid-run.

--market-order-ratio (default 0.2) makes that fraction of taker placements
sweep via a market order (trade/private/api/v1/market-orders) instead of a
mirrored limit wager -- sized against current maker liquidity via
estimate-odds first so it stays matchable, falling back to a limit wager if
the book is momentarily empty. Market orders are a genuinely different code
path from everything else this script exercises: their cancel/refund goes
through CreateMarketOrderRefundJob (ss-trade-app), not the generic
wager-cancel path -- calculateCancelWagerRefundAmount returns 0 for
market-order-tagged wagers, so cancel_all_wagers's refund accounting never
applies to these. Their own known race is matching right at their ~5s
auto-cancel timeout (a real, previously-found double-payment bug -- see
reproduce_market_order_race_condition.py), separate from the cancel-all-vs-
deduct-job race above.

Run: python3 -m concentration_race.rapid_fire --duration 300 --rps 5 --workers 5 --event-id <id>
Multi-account: python3 -m concentration_race.rapid_fire --maker-accounts 1 2 --taker-accounts patron patron3
"""
import argparse
import json
import logging
import os
import random
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed

from deduce_tests import DeduceTestFramework, Colors
from test_deduce_race_conditions import RaceConditionTest, get_odds_ladder
from test_backend_fairness import _classify_cancel_error, RateLimiter
from src import config
from concentration_race.accounts import login_makers, login_takers
from concentration_race.market import find_market, resolve_opposite_line
from concentration_race.logutil import quiet_console_log_to_file
from concentration_race.wallet_exposure import (
    get_gec, get_lec_snapshot, lec_changed, login_maker_web_session,
)


ODDS_REFRESH_INTERVAL_SEC = 3.0
FILL_RATE_WINDOW_SEC = 60.0


def run(duration=300, bets_per_second=5, event_id=None, min_stake=2.0, max_stake=10.0, cancel_interval=0.0,
        workers=1, max_fills_per_min=300, market_order_ratio=0.2, maker_workers=None, taker_workers=None,
        maker_accounts=None, taker_accounts=None, check_exposure=False):
    run_id = int(time.time())
    log_dir = os.path.join("logs", str(run_id))
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f"concentration_race_{run_id}.log")
    quiet_console_log_to_file(log_file)
    print(f"{Colors.CYAN}📝 Full logs: {log_file}{Colors.RESET}\n")

    framework = DeduceTestFramework(environment=config.ENVIRONMENT)
    test = RaceConditionTest(framework)

    maker_accounts = maker_accounts or [1]
    taker_accounts = taker_accounts or ['patron']
    makers = login_makers(framework, maker_accounts)
    takers = login_takers(framework, taker_accounts)

    initial_snapshot = test.snapshot_balances(makers + takers, 'initial')
    for name in makers:
        print(f"{Colors.GREEN}✅ Maker {name}: ${initial_snapshot['balances'][name]:,.2f}{Colors.RESET}")
    for name in takers:
        print(f"{Colors.GREEN}✅ Taker {name}: ${initial_snapshot['balances'][name]:,.2f}{Colors.RESET}")
    print()

    print(f"{Colors.CYAN}🔍 Finding market...{Colors.RESET}")
    market_info = find_market(framework, makers[0], event_id)
    if not market_info:
        print(f"{Colors.RED}❌ No markets available{Colors.RESET}")
        return None

    line_id = market_info['line_id']
    resolved_event_id = event_id or market_info.get('event', {}).get('event_id')
    print(f"{Colors.GREEN}✅ Market found (event {resolved_event_id}){Colors.RESET}")

    print(f"{Colors.YELLOW}🔍 Resolving opposite line for taker...{Colors.RESET}")
    match_line_id = resolve_opposite_line(framework, makers[0], resolved_event_id, line_id)
    if not match_line_id:
        print(f"{Colors.YELLOW}⚠️  Opposite line not found. Falling back to same line "
              f"(may not match depending on market model).{Colors.RESET}")
        match_line_id = line_id
    else:
        print(f"{Colors.GREEN}✅ Opposite line: {match_line_id[:16]}...{Colors.RESET}")

    # GEC/LEC only ever moves for the account that ends up MATCHED ON BOTH SIDES
    # of a market (confirmed live -- a taker/patron never carries it, and a maker
    # one-sided match doesn't move it either). Makers here only ever bet line_id,
    # so satisfying that needs one extra maker-bets-match_line_id round after the
    # main race (see the swap-round block below) -- checked on the maker(s), not
    # the taker(s).
    maker_web_sessions = {}
    exposure_before = {}
    if check_exposure:
        print(f"{Colors.CYAN}🔍 Logging in maker web session(s) for GEC/LEC...{Colors.RESET}")
        for num, name in zip(maker_accounts, makers):
            web_name = login_maker_web_session(framework, num, name)
            if web_name:
                maker_web_sessions[name] = web_name
                print(f"  {name}: web session ready ({web_name})")
            else:
                print(f"  {Colors.YELLOW}⚠️  {name}: no email/password on file -- "
                      f"skipping GEC/LEC check for this maker{Colors.RESET}")
        print(f"{Colors.CYAN}🔍 Snapshotting maker GEC/LEC before firing (event {resolved_event_id})...{Colors.RESET}")
        for name, web_name in maker_web_sessions.items():
            exposure_before[name] = {
                'gec': get_gec(framework, web_name),
                'lec': get_lec_snapshot(framework, web_name, resolved_event_id),
            }
            print(f"  {name}: GEC=${exposure_before[name]['gec']}  "
                  f"LEC entries={len(exposure_before[name]['lec']) if exposure_before[name]['lec'] is not None else 'N/A'}")
        print()

    odds_ladder = get_odds_ladder(framework)
    # American odds are only valid at |odds| > 100 -- get_odds_ladder's own filter
    # (-500 <= o <= 500, o != 0) lets 1..100 through, which the API rejects
    # (error_code 30010: "odds must be greater than or equal to 100 or less than
    # -100" -- that error text is misleading, exactly +-100 is rejected too;
    # confirmed live 2026-08-26, three straight odds=-100 placements all 400'd).
    # Since taker mirrors with -value, a bad small maker pick becomes an equally
    # invalid negative for taker, so filter here regardless of which side would
    # have hit it first.
    positive_odds = [o for o in odds_ladder if o > 100] or [150]

    errors = []
    lock = threading.Lock()
    maker_count = 0
    taker_count = 0
    maker_wagers = {}  # wager_id -> placed stake
    taker_wager_ids = set()
    market_order_wager_ids = set()  # subset of taker_wager_ids placed as market orders

    maker_workers = maker_workers if maker_workers is not None else workers
    taker_workers = taker_workers if taker_workers is not None else workers

    # One shared rate limiter per side, aggregate across every worker AND every
    # account on that side -- same convention as test_backend_fairness.py's
    # --rps ("max wagers per second across all workers"), extended here across
    # multiple maker/taker accounts. RateLimiter.acquire() blocks under its own
    # lock, so (unlike the old workers/rps sleep-based pacing) it can't be raced
    # by concurrent threads into a burst.
    maker_rate_limiter = RateLimiter(bets_per_second) if bets_per_second > 0 else None
    taker_rate_limiter = RateLimiter(bets_per_second) if bets_per_second > 0 else None

    logged_first = {'maker': False, 'taker': False}
    cancel_all_attempts = 0
    cancel_all_successes = 0
    cancel_fail_categories = {}  # category (from _classify_cancel_error) -> count

    # Shared odds epoch: makers read this directly, takers read its negation, so
    # every wager placed within the same epoch is guaranteed to cross. Refreshed
    # periodically by refresh_odds() so odds still vary over the run.
    epoch_odds = {'value': random.choice(positive_odds)}

    # Rolling-window fill-rate proxy: taker placements approximate fills (see
    # module docstring). fire_taker blocks whenever the trailing-window count
    # is at max_fills_per_min, throttling how fast matches can be produced.
    fill_timestamps = deque()

    def try_reserve_fill_slot():
        """Atomically check-and-reserve a slot in the rolling fill-rate window.

        Reserving here (not checking, then having the caller record success later)
        fixes a thundering-herd bug: with a plain check, every taker worker thread
        could see "not at cap" in the same instant and all fire concurrently,
        overshooting max_fills_per_min in a burst large enough to trip the
        backend's real rate limiter (429s). Now only as many threads as there are
        open slots can proceed per instant.
        """
        if max_fills_per_min <= 0:
            return True
        now = time.time()
        with lock:
            while fill_timestamps and now - fill_timestamps[0] > FILL_RATE_WINDOW_SEC:
                fill_timestamps.popleft()
            if len(fill_timestamps) >= max_fills_per_min:
                return False
            fill_timestamps.append(now)
            return True

    def fire_maker(account_name):
        nonlocal maker_count
        start = time.time()
        while time.time() - start < duration:
            try:
                if maker_rate_limiter:
                    maker_rate_limiter.acquire()
                odds = epoch_odds['value']
                stake = round(random.uniform(min_stake, max_stake), 2)
                result = framework.place_wager(account_name, line_id, odds, stake)
                if result.get('success'):
                    with lock:
                        maker_count += 1
                        maker_wagers[result.get('wager_id')] = stake
                    if not logged_first['maker']:
                        logged_first['maker'] = True
                        logging.debug(f"maker wager response: {json.dumps(result.get('data', {}), default=str)}")
                else:
                    with lock:
                        errors.append({'account': account_name, 'error': result.get('error')})
            except Exception as e:
                with lock:
                    errors.append({'account': account_name, 'error': str(e)})
        return maker_count

    def place_market_order_sweep(account_name, stake):
        """Place a market order sized against current maker liquidity on match_line_id.

        Falls back to a normal mirrored limit wager if the book is currently
        empty (max_stake_size <= 0) so this iteration still contributes
        traffic instead of firing an order guaranteed not to match.
        """
        estimate = framework.get_market_order_estimate(account_name, match_line_id, stake)
        max_stake_size = estimate['max_stake_size']
        if max_stake_size <= 0:
            odds = -epoch_odds['value']
            result = framework.place_wager(account_name, match_line_id, odds, stake)
            result['is_market_order'] = False
            return result
        clamped_stake = min(stake, max_stake_size)
        result = framework.place_market_order(
            account_name, match_line_id, clamped_stake,
            estimate['expected_average_odds'], estimate['odds_list'])
        result['is_market_order'] = True
        return result

    def fire_taker(account_name):
        nonlocal taker_count
        start = time.time()
        while time.time() - start < duration:
            while not try_reserve_fill_slot():
                if time.time() - start >= duration:
                    return taker_count
                time.sleep(0.1)
            try:
                if taker_rate_limiter:
                    taker_rate_limiter.acquire()
                stake = round(random.uniform(min_stake, max_stake), 2)
                if market_order_ratio > 0 and random.random() < market_order_ratio:
                    result = place_market_order_sweep(account_name, stake)
                else:
                    odds = -epoch_odds['value']
                    result = framework.place_wager(account_name, match_line_id, odds, stake)
                    result['is_market_order'] = False
                if result.get('success'):
                    with lock:
                        taker_count += 1
                        taker_wager_ids.add(result.get('wager_id'))
                        if result['is_market_order']:
                            market_order_wager_ids.add(result.get('wager_id'))
                    if not logged_first['taker']:
                        logged_first['taker'] = True
                        logging.debug(f"taker wager response: {json.dumps(result.get('data', {}), default=str)}")
                else:
                    with lock:
                        errors.append({'account': account_name, 'error': result.get('error')})
            except Exception as e:
                with lock:
                    errors.append({'account': account_name, 'error': str(e)})
        return taker_count

    def refresh_odds():
        start = time.time()
        while time.time() - start < duration:
            time.sleep(ODDS_REFRESH_INTERVAL_SEC)
            epoch_odds['value'] = random.choice(positive_odds)

    def fire_cancel_all():
        nonlocal cancel_all_attempts, cancel_all_successes
        start = time.time()
        while True:
            # Only cancel if it lands within `duration` -- if the next interval
            # would push past it, skip rather than sleeping the full interval
            # and running a cancel late. This thread is one of the futures the
            # main ThreadPoolExecutor waits on to shut down, so running past
            # duration here would delay the whole run's results indefinitely.
            if duration - (time.time() - start) <= cancel_interval:
                break
            time.sleep(cancel_interval)
            for account_name in makers:
                try:
                    result = framework.cancel_all_wagers(account_name)
                    with lock:
                        cancel_all_attempts += 1
                        if result['success']:
                            cancel_all_successes += 1
                        else:
                            category = _classify_cancel_error(result['status_code'], result['error_text'])
                            cancel_fail_categories[category] = cancel_fail_categories.get(category, 0) + 1
                except Exception as e:
                    with lock:
                        errors.append({'account': account_name, 'error': f"cancel_all: {e}"})
        return cancel_all_attempts

    worker_fns = []
    for name in makers:
        worker_fns += [(lambda n=name: fire_maker(n))] * maker_workers
    for name in takers:
        worker_fns += [(lambda n=name: fire_taker(n))] * taker_workers
    worker_fns.append(refresh_odds)
    if cancel_interval > 0:
        worker_fns.append(fire_cancel_all)
        print(f"{Colors.YELLOW}⚠️  Cancel-all race enabled: calling cancel_all_wagers on "
              f"{', '.join(makers)} every {cancel_interval}s while firing{Colors.RESET}\n")
    if max_fills_per_min > 0:
        print(f"{Colors.YELLOW}⚠️  Fill-rate cap enabled: takers throttled to ~{max_fills_per_min} "
              f"match-producing placements/min combined (rolling {FILL_RATE_WINDOW_SEC:.0f}s window){Colors.RESET}\n")

    print(f"\n{Colors.BOLD}🚀 Starting rapid fire: target {bets_per_second} bets/sec/side aggregate "
          f"(makers: {len(makers)} account{'s' if len(makers) != 1 else ''} x {maker_workers} worker"
          f"{'s' if maker_workers != 1 else ''}, takers: {len(takers)} account{'s' if len(takers) != 1 else ''} "
          f"x {taker_workers} worker{'s' if taker_workers != 1 else ''}) for {duration}s{Colors.RESET}\n")
    with ThreadPoolExecutor(max_workers=len(worker_fns)) as executor:
        futures = [executor.submit(w) for w in worker_fns]
        [f.result() for f in as_completed(futures)]

    print(f"\n{Colors.GREEN}✅ Maker placed: {maker_count} bets{Colors.RESET}")
    print(f"{Colors.GREEN}✅ Taker placed: {taker_count} bets{Colors.RESET}\n")

    print(f"{Colors.CYAN}⏳ Waiting for matches to settle...{Colors.RESET}")
    time.sleep(5)

    final_snapshot = test.snapshot_balances(makers + takers, 'final')
    # limit=100 only pages the most recent matched bets on this shared, long-lived
    # staging account (it carries $49k+ of unrelated matched history) -- so scope
    # down to wager_ids this run actually placed before drawing any conclusion.
    maker_matched = []
    for name in makers:
        matched_all = framework.get_matched_bets(name, limit=100)
        for bet in matched_all:
            if bet.get('wager_id') in maker_wagers:
                bet['_account'] = name
                maker_matched.append(bet)

    taker_matched = []
    for name in takers:
        matched_all = framework.get_matched_bets(name, limit=100)
        for bet in matched_all:
            if bet.get('id') in taker_wager_ids:
                bet['_account'] = name
                taker_matched.append(bet)

    market_order_matched = [b for b in taker_matched if b.get('id') in market_order_wager_ids]
    maker_matched_total = sum(bet.get('stake', 0) for bet in maker_matched)
    maker_change = sum(initial_snapshot['balances'][n] - final_snapshot['balances'][n] for n in makers)
    taker_change = sum(initial_snapshot['balances'][n] - final_snapshot['balances'][n] for n in takers)

    if maker_matched:
        logging.debug(f"sample maker matched bet (this run): {json.dumps(maker_matched[0], default=str)}")
    if taker_matched:
        logging.debug(f"sample taker matched bet (this run): {json.dumps(taker_matched[0], default=str)}")

    # SSE-2441 over-fill check: group this run's maker fills by wager_id and
    # verify no single order's cumulative fills exceed what it was placed for.
    fills_by_wager = {}
    for bet in maker_matched:
        fills_by_wager.setdefault(bet.get('wager_id'), []).append(bet)

    multi_fill_orders = {wid: fills for wid, fills in fills_by_wager.items() if len(fills) > 1}
    overfilled_orders = []
    for wid, fills in fills_by_wager.items():
        filled_total = sum(f.get('stake', 0) for f in fills)
        placed_stake = maker_wagers.get(wid, 0)
        if filled_total > placed_stake + 0.01:
            overfilled_orders.append({
                'wager_id': wid, 'placed_stake': placed_stake, 'filled_total': filled_total,
            })

    consistency_check = abs(maker_change - maker_matched_total) < 0.01

    if check_exposure and maker_web_sessions:
        # Placed after final_snapshot/consistency_check above -- this round's own
        # matched wager must NOT be counted in this run's over-fill/consistency
        # math (that check is documented as scoped to the main race loop's own
        # wagers only). The main race loop only ever has makers bet line_id --
        # confirmed live, GEC/LEC needs a maker MATCHED ON BOTH SIDES of the
        # market, so a maker that only ever bet one side won't show anything no
        # matter how many times that loop ran. This extra round has each checked
        # maker also bet match_line_id (mirrored by takers[0] on line_id),
        # crossing with a fresh odds pick so it's a genuinely new match.
        print(f"{Colors.CYAN}🔄 Swap round: matching checked maker(s) on the opposite side too "
              f"(needed for GEC/LEC){Colors.RESET}")
        swap_odds = random.choice(positive_odds)
        for name in maker_web_sessions:
            r_maker = framework.place_wager(name, match_line_id, swap_odds, min_stake)
            time.sleep(2)
            r_taker = framework.place_wager(takers[0], line_id, -swap_odds, min_stake)
            print(f"  {name}@{swap_odds} -> {r_maker.get('success')}   "
                  f"{takers[0]}@{-swap_odds} -> {r_taker.get('success')}")
        print(f"{Colors.CYAN}⏳ Waiting for swap round to settle...{Colors.RESET}")
        time.sleep(6)

    exposure_results = {}
    if check_exposure:
        for name, web_name in maker_web_sessions.items():
            gec_after = get_gec(framework, web_name)
            lec_after = get_lec_snapshot(framework, web_name, resolved_event_id)
            before = exposure_before[name]
            exposure_results[name] = {
                'gec_before': before['gec'], 'gec_after': gec_after,
                'lec_before': before['lec'], 'lec_after': lec_after,
                'gec_changed': (before['gec'] is not None and gec_after is not None
                                and gec_after != before['gec']),
                'lec_changed': lec_changed(before['lec'], lec_after),
            }

    print(f"\n{Colors.BOLD}{'='*70}{Colors.RESET}")
    print(f"{Colors.BOLD}RESULTS{Colors.RESET}")
    print(f"{Colors.BOLD}{'='*70}{Colors.RESET}\n")
    print(f"{Colors.BOLD}MAKER{'S' if len(makers) > 1 else ''} (aggregate):{Colors.RESET}")
    print(f"  Bets Placed:    {maker_count}")
    print(f"  Fills Matched:  {len(maker_matched)} (this run's wagers only)")
    print(f"  Matched Total:  ${maker_matched_total:.2f}")
    print(f"  Balance Change: ${maker_change:.2f}")
    status = f"{Colors.GREEN}✅ CONSISTENT{Colors.RESET}" if consistency_check else f"{Colors.RED}❌ INCONSISTENT{Colors.RESET}"
    print(f"  Consistency:    {status}")
    if len(makers) > 1:
        for name in makers:
            name_change = initial_snapshot['balances'][name] - final_snapshot['balances'][name]
            name_matched = len([b for b in maker_matched if b['_account'] == name])
            print(f"    {name}: balance change ${name_change:.2f}, {name_matched} fills")
    print()
    print(f"{Colors.BOLD}TAKER{'S' if len(takers) > 1 else ''} (aggregate):{Colors.RESET}")
    print(f"  Bets Placed:    {taker_count}")
    print(f"  Bets Matched:   {len(taker_matched)} (this run's wagers only)")
    print(f"  Balance Change: ${taker_change:.2f}")
    if market_order_ratio > 0:
        print(f"  Market Orders:  {len(market_order_wager_ids)} placed, "
              f"{len(market_order_matched)} matched")
    if len(takers) > 1:
        for name in takers:
            name_change = initial_snapshot['balances'][name] - final_snapshot['balances'][name]
            name_matched = len([b for b in taker_matched if b['_account'] == name])
            print(f"    {name}: balance change ${name_change:.2f}, {name_matched} matched")
    print()
    avg_fills_per_min = (taker_count / duration) * 60 if duration else 0
    cap_note = f" (capped at {max_fills_per_min})" if max_fills_per_min > 0 else " (uncapped)"
    print(f"{Colors.BOLD}FILL RATE (taker placements as proxy, not DB-verified):{Colors.RESET}")
    print(f"  Avg over run: {avg_fills_per_min:.1f}/min{cap_note}\n")
    print(f"{Colors.BOLD}MULTI-FILL / OVER-FILL CHECK:{Colors.RESET}")
    print(f"  Orders with >1 fill: {len(multi_fill_orders)} of {len(fills_by_wager)} filled orders")
    over_status = (f"{Colors.RED}🐛 {len(overfilled_orders)} OVER-FILLED (bug){Colors.RESET}"
                   if overfilled_orders else f"{Colors.GREEN}✅ none over-filled{Colors.RESET}")
    print(f"  Over-filled orders:  {over_status}\n")
    if check_exposure:
        print(f"{Colors.BOLD}GEC/LEC EXPOSURE CHECK (maker, event {resolved_event_id}, "
              f"after swap round):{Colors.RESET}")
        if not maker_web_sessions:
            print(f"  {Colors.YELLOW}N/A -- no maker had an email/password on file{Colors.RESET}")
        for name, r in exposure_results.items():
            gec_status = (f"{Colors.GREEN}✅ changed{Colors.RESET}" if r['gec_changed']
                           else f"{Colors.RED}❌ did not change{Colors.RESET}")
            lec_status = (f"{Colors.GREEN}✅ changed{Colors.RESET}" if r['lec_changed']
                           else f"{Colors.RED}❌ did not change{Colors.RESET}")
            print(f"  {name}: GEC ${r['gec_before']} -> ${r['gec_after']}  {gec_status}")
            print(f"  {name}: LEC {r['lec_before']} -> {r['lec_after']}  {lec_status}")
        print()
    if cancel_interval > 0:
        print(f"{Colors.BOLD}CANCEL-ALL RACE ({', '.join(makers)}):{Colors.RESET}")
        print(f"  Attempts:    {cancel_all_attempts}")
        print(f"  Successes:   {cancel_all_successes}")
        if cancel_fail_categories:
            print(f"  Failure breakdown (see test_backend_fairness.py's _classify_cancel_error):")
            for category, count in sorted(cancel_fail_categories.items(), key=lambda kv: -kv[1]):
                print(f"    {category}: {count}")
        print(f"  {Colors.YELLOW}This tool can't see deduct_jobs/fee_transactions -- check the DB for "
              f"wagers cancelled during this run: expect some deduct_jobs/fee_transactions rows "
              f"'failed' or 'refunding', not 100% 'succeed' with no refund.{Colors.RESET}\n")
    print(f"{Colors.BOLD}Errors: {len(errors)}{Colors.RESET}\n")

    # json.dump can't serialize the (marketId, outcomeId) tuple keys inside
    # exposure_results' lec_before/lec_after -- stringify them for the report only.
    exposure_results_json_safe = {
        name: {**r, 'lec_before': {str(k): v for k, v in (r['lec_before'] or {}).items()},
               'lec_after': {str(k): v for k, v in (r['lec_after'] or {}).items()}}
        for name, r in exposure_results.items()
    }

    report = {
        'test': 'concentration_race',
        'duration': duration,
        'target_rate': bets_per_second,
        'event_id': resolved_event_id,
        'line_id': line_id,
        'match_line_id': match_line_id,
        'maker_accounts': makers,
        'taker_accounts': takers,
        'maker_placed': maker_count,
        'maker_matched': len(maker_matched),
        'taker_placed': taker_count,
        'taker_matched': len(taker_matched),
        'market_order_ratio': market_order_ratio,
        'market_order_placed': len(market_order_wager_ids),
        'market_order_matched': len(market_order_matched),
        'multi_fill_orders': len(multi_fill_orders),
        'overfilled_orders': overfilled_orders,
        'consistency_check': consistency_check,
        'check_exposure': check_exposure,
        'exposure_results': exposure_results_json_safe,
        'max_fills_per_min': max_fills_per_min,
        'avg_fills_per_min': avg_fills_per_min,
        'cancel_interval': cancel_interval,
        'cancel_all_attempts': cancel_all_attempts,
        'cancel_all_successes': cancel_all_successes,
        'cancel_fail_categories': cancel_fail_categories,
        'snapshots': test.balance_snapshots,
        'errors': errors,
    }
    report_file = os.path.join(log_dir, f"concentration_race_{run_id}.json")
    with open(report_file, 'w') as f:
        json.dump(report, f, indent=2)
    print(f"{Colors.GREEN}📄 Report saved: {report_file}{Colors.RESET}\n")
    return report


def main():
    parser = argparse.ArgumentParser(
        description='Concentration race: fire randomized-odds wagers, checking for over-filled orders'
    )
    parser.add_argument('--duration', type=int, default=300, help='Duration in seconds (default 300)')
    parser.add_argument('--rps', type=float, default=5,
                         help='Target/max bets per second per side, aggregate across every worker and every '
                              'account on that side (default 5), enforced by one shared rate limiter per side.')
    parser.add_argument('--workers', type=int, default=1,
                         help='Concurrent placing threads per account per side (default 1 = sequential). '
                              'Total threads on a side = workers * number of accounts on that side. '
                              'Overridden per side by --maker-workers/--taker-workers when given.')
    parser.add_argument('--maker-workers', type=int, default=None,
                         help='Concurrent placing threads per maker account (default: --workers).')
    parser.add_argument('--taker-workers', type=int, default=None,
                         help='Concurrent placing threads per taker account (default: --workers).')
    parser.add_argument('--maker-accounts', type=str, nargs='+', default=None,
                         help='MM accounts to use as makers, e.g. --maker-accounts 1 2 or '
                              '--maker-accounts exposure_mm1 exposure_mm2. Default: [1] (mm1). Verified to '
                              'resolve to distinct partners before use -- see concentration_race/accounts.py.')
    parser.add_argument('--taker-accounts', type=str, nargs='+', default=None,
                         help='Patron accounts to use as takers, e.g. --taker-accounts patron patron3. '
                              'Default: [patron] (the staging patron account).')
    parser.add_argument('--event-id', type=int, default=None, help='Staging event ID (auto-discover if omitted)')
    parser.add_argument('--min-stake', type=float, default=2.0, help='Minimum stake per wager (default 2.0)')
    parser.add_argument('--max-stake', type=float, default=10.0, help='Maximum stake per wager (default 10.0)')
    parser.add_argument('--cancel-interval', type=float, default=0.0,
                         help='Seconds between maker cancel_all_wagers calls (called for every maker account), '
                              'racing them against in-flight deduct jobs (SSE-2441 bulk-cancel gap). '
                              '0 = disabled (default)')
    parser.add_argument('--max-fills-per-min', type=float, default=300,
                         help='Cap on taker-side match-producing placements per rolling 60s window, shared '
                              'across every taker account, used as a fill-rate proxy (the backend gets '
                              'high-latency above ~500 fills/min). Default 300. 0 = unlimited.')
    parser.add_argument('--market-order-ratio', type=float, default=0.2,
                         help='Fraction of taker placements (0-1) that sweep via a market order '
                              '(trade/private/api/v1/market-orders) instead of a mirrored limit wager -- '
                              'a genuinely different code path (own cancel/refund job, own ~5s auto-cancel '
                              'race, see reproduce_market_order_race_condition.py). Sized against current '
                              'maker liquidity via estimate-odds so it stays matchable. Default 0.2. '
                              '0 = disabled (limit orders only).')
    parser.add_argument('--check-exposure', action='store_true',
                         help='Snapshot each maker\'s GEC (wallet.exposureCredit) and LEC '
                              '(/api/v2/wallet/exposures) before firing and after an extra swap round, '
                              'asserting both changed. Confirmed live: GEC/LEC only moves once a maker '
                              'is matched on BOTH sides of the market -- the main race loop only ever '
                              'bets one side, so this adds one more matched wager on the opposite side '
                              'for each checked maker after the race settles. Skipped for any maker with '
                              'no email/password on file (needed for a web token -- MM login alone '
                              'doesn\'t give one). Default: disabled.')
    args = parser.parse_args()

    # Parse --maker-accounts: convert pure digits to int (config.get_account_credentials
    # expects an int for numbered MM accounts), keep other identifiers (e.g. exposure_mm1) as-is.
    maker_accounts = None
    if args.maker_accounts:
        maker_accounts = [int(x) if x.isdigit() else x for x in args.maker_accounts]

    run(
        duration=args.duration,
        bets_per_second=args.rps,
        event_id=args.event_id,
        min_stake=args.min_stake,
        max_stake=args.max_stake,
        cancel_interval=args.cancel_interval,
        workers=args.workers,
        max_fills_per_min=args.max_fills_per_min,
        market_order_ratio=args.market_order_ratio,
        maker_workers=args.maker_workers,
        taker_workers=args.taker_workers,
        maker_accounts=maker_accounts,
        taker_accounts=args.taker_accounts,
        check_exposure=args.check_exposure,
    )


if __name__ == '__main__':
    main()
