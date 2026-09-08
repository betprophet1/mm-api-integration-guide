"""Phase 2: wait for generate.py's GEC/LEC to actually land, then spend it.

This is the answer to "how do I run the follow-up after waiting 30-60s" --
don't wait externally. Run this right after generate.py; it POLLS the wallet
every 5s (matching ss-exposure-app's processor tick / ss-wallet-app's sync
poll, both confirmed 5s in source) until the balance changes or 90s elapses,
instead of guessing a fixed sleep.

Spending is automatic, not requested: ss-exposure-app's designateMatchedStake
funds a new wager's stake from available LEC (same event+market+outcome+line)
or GEC (any market) before cash. So "spending" here means: place one more
matched wager that's eligible, then confirm the balance dropped.

CAVEAT on the GEC follow-up specifically: matching a NEW wager on a different
event is ALSO, independently, a fresh "both sides matched" event -- if the
follow-up stake is funded entirely by existing GEC (cash=0 in the formula,
see concentration_race/wallet_exposure.py), it contributes zero *new* GEC, so
a clean decrease confirms spend. If GEC only partially covers the stake, the
cash-funded remainder DOES generate a bit of fresh GEC on top, muddying the
before/after delta slightly -- keep --spend-stake at or below the polled GEC
if you want an unambiguous read.

--rounds controls how many follow-up spend attempts to make: for LEC, each
round targets one of generate.py's rounds (its own event+market+outcome); for
GEC (a single global balance) it's just N follow-up attempts on N different
events. Defaults to however many rounds generate.py saved (or 1 for an older,
single-round state file).

Usage:
  MM_ENVIRONMENT=qa python3 -m exposure_credit.spend --state-file exposure_credit/state/<ts>.json
  MM_ENVIRONMENT=qa python3 -m exposure_credit.spend --state-file ... --credit-type lec
  MM_ENVIRONMENT=qa python3 -m exposure_credit.spend --state-file ... --spend-stake 20 --timeout 120
  MM_ENVIRONMENT=qa python3 -m exposure_credit.spend --state-file ... --rounds 50 --spend-stake 100
"""
import argparse
import json
import random
import time
from urllib.parse import urljoin

import requests

from deduce_tests import DeduceTestFramework, Colors
from test_deduce_race_conditions import get_odds_ladder
from concentration_race.accounts import login_makers, login_takers
from concentration_race.market import find_market, resolve_opposite_line
from concentration_race.wallet_exposure import (
    get_gec, get_lec_snapshot, wait_for_gec_change, wait_for_lec_change,
    login_maker_web_session, EXPOSURE_SYNC_POLL_INTERVAL_SEC, EXPOSURE_SYNC_TIMEOUT_SEC,
)
from src import config


def _lec_snapshot_from_json(raw):
    if raw is None:
        return None
    return {tuple(int(x) for x in k.split(',')): v for k, v in raw.items()}


def _rounds_from_state(state):
    """Return state['rounds'] if present, else synthesize one round from an
    older, single-round state file (event_id/line_id/match_line_id/baseline_lec
    used to live at the top level before --rounds was added to generate.py)."""
    if 'rounds' in state:
        return state['rounds']
    return [{
        'event_id': state['event_id'],
        'line_id': state['line_id'],
        'match_line_id': state['match_line_id'],
        'odds': state.get('odds'),
        'stake': state.get('stake'),
        'baseline_lec': state.get('baseline_lec'),
    }]


def _line_id_for_outcome(framework, account_name, event_id, market_id, outcome_id):
    """Look up the line_id for a specific (market_id, outcome_id) on event_id --
    needed to place a wager that's eligible to draw down a specific LEC entry."""
    url = urljoin(framework.base_url, config.URL['mm_multiple_markets'])
    response = requests.get(url, params={'event_ids': str(event_id)}, headers=framework.get_auth_header(account_name),
                             timeout=30)
    if response.status_code != 200:
        return None
    markets = response.json().get('data', {}).get(str(event_id), [])
    for market in markets:
        if market.get('id') != market_id:
            continue
        for group in market.get('selections', []):
            for sel in group:
                if sel.get('outcome_id') == outcome_id:
                    return sel.get('line_id')
    return None


def on_poll_factory(label):
    def on_poll(elapsed, value):
        print(f"  [{elapsed:.0f}s] {label}: {value}")
    return on_poll


def spend_gec(framework, state, rounds, spend_stake, poll_interval, timeout):
    print(f"\n{Colors.BOLD}=== GEC ==={Colors.RESET}")
    web_name = state['maker_web_name']
    maker, taker = state['maker_name'], state['taker_name']
    baseline = state['baseline_gec']
    print(f"{Colors.CYAN}⏳ Polling for GEC to change from baseline (${baseline})...{Colors.RESET}")
    changed, current_gec = wait_for_gec_change(
        framework, web_name, baseline, poll_interval, timeout, on_poll=on_poll_factory('GEC'))
    if not changed:
        print(f"{Colors.RED}✗ GEC never changed from ${baseline} within {timeout:.0f}s. "
              f"Either the match didn't generate GEC (too small/balanced a hedge -- see the formula "
              f"in wallet_exposure.py) or the pipeline hasn't caught up yet. Not attempting to spend.{Colors.RESET}")
        return False, 0.0
    if baseline is None or current_gec is None or current_gec <= baseline:
        # Confirmed live: a match can DECREASE existing GEC instead of growing it
        # (the wallet's exposure_credit is a running ledger, not a per-market
        # recompute -- generate.py's match interacted with pre-existing state
        # rather than adding independent new credit). "Changed" alone isn't
        # success here; only report generated credit as spendable if it grew.
        print(f"{Colors.RED}✗ GEC changed (${baseline} -> ${current_gec}) but did NOT increase -- "
              f"generate.py's match reduced or zeroed existing GEC rather than adding new credit. "
              f"Nothing to spend. Try a different account/event with no pre-existing GEC, or a larger "
              f"--stake in generate.py.{Colors.RESET}")
        return False, 0.0
    print(f"{Colors.GREEN}✅ GEC increased: ${baseline} -> ${current_gec}{Colors.RESET}")

    exclude_event_ids = {r['event_id'] for r in rounds}
    remaining_gec = current_gec
    total_spent = 0.0
    successes = 0

    for i in range(len(rounds)):
        print(f"\n{Colors.BOLD}-- GEC spend attempt {i + 1}/{len(rounds)} (current ~${remaining_gec}) --{Colors.RESET}")
        print(f"{Colors.CYAN}🔍 Finding a DIFFERENT event to spend GEC on (GEC is global, per the README -- "
              f"any market/event is eligible)...{Colors.RESET}")
        new_event_id, line_id, match_line_id = None, None, None
        for _ in range(8):
            market_info = find_market(framework, maker, None)
            if not market_info:
                continue
            candidate = market_info.get('event', {}).get('event_id')
            if candidate and candidate not in exclude_event_ids:
                new_event_id = candidate
                line_id = market_info['line_id']
                match_line_id = resolve_opposite_line(framework, maker, new_event_id, line_id)
                if match_line_id:
                    break
        if not new_event_id or not match_line_id:
            print(f"{Colors.RED}✗ Could not find a different, usable event to spend GEC on, skipping attempt.{Colors.RESET}")
            continue
        exclude_event_ids.add(new_event_id)
        print(f"{Colors.GREEN}✅ Spending on event {new_event_id}{Colors.RESET}")

        ladder = get_odds_ladder(framework)
        positive = [o for o in ladder if o > 100] or [150]
        odds = random.choice(positive)
        framework.place_wager(maker, line_id, odds, spend_stake)
        time.sleep(2)
        framework.place_wager(taker, match_line_id, -odds, spend_stake)
        print(f"{Colors.CYAN}⏳ Placed ${spend_stake} follow-up match, polling for GEC to drop...{Colors.RESET}")

        time.sleep(poll_interval)
        after_gec = get_gec(framework, web_name)
        spent = remaining_gec - after_gec if (remaining_gec is not None and after_gec is not None) else None
        print(f"{Colors.BOLD}GEC: ${remaining_gec} -> ${after_gec}{Colors.RESET}")
        if spent is not None and spent > 0.001:
            print(f"{Colors.GREEN}✅ Spent ~${spent:.2f} of GEC on a different market.{Colors.RESET}")
            total_spent += spent
            successes += 1
        else:
            print(f"{Colors.RED}✗ GEC did not decrease this attempt -- see this file's module docstring caveat: "
                  f"if the follow-up stake was cash-funded (not credit-funded), or the pipeline hasn't synced "
                  f"this match yet, no decrease will show.{Colors.RESET}")
        remaining_gec = after_gec if after_gec is not None else remaining_gec

    print(f"\n{Colors.BOLD}GEC total: spent ~${total_spent:.2f} across {successes}/{len(rounds)} attempts.{Colors.RESET}")
    return successes > 0, total_spent


def _spend_lec_round(framework, state, round_state, spend_stake, poll_interval, timeout):
    web_name = state['maker_web_name']
    maker, taker = state['maker_name'], state['taker_name']
    event_id = round_state['event_id']
    baseline = _lec_snapshot_from_json(round_state.get('baseline_lec'))
    print(f"{Colors.CYAN}⏳ Polling for LEC to change on event {event_id}...{Colors.RESET}")
    changed, current_lec = wait_for_lec_change(
        framework, web_name, event_id, baseline, poll_interval, timeout, on_poll=on_poll_factory('LEC'))
    if not changed or not current_lec:
        print(f"{Colors.RED}✗ LEC never changed on event {event_id} within {timeout:.0f}s.{Colors.RESET}")
        return 0.0
    print(f"{Colors.GREEN}✅ LEC landed: {current_lec}{Colors.RESET}")

    # Confirmed live: LEC lands on the OPPOSITE outcome+line of what you matched,
    # not the one you bet. Target the entry THIS round's match actually grew (by
    # delta vs baseline), not just whichever has the largest absolute balance --
    # an account can carry a bigger, unrelated LEC entry from earlier activity
    # that this round never touched (confirmed live: baseline already had a
    # $207.54 entry before this run's match; the new entry was a separate $64).
    baseline = baseline or {}
    (market_id, outcome_id), balance = max(
        current_lec.items(), key=lambda kv: kv[1] - baseline.get(kv[0], 0))
    delta = balance - baseline.get((market_id, outcome_id), 0)
    if delta <= 0:
        print(f"{Colors.RED}✗ No LEC entry grew from this round's match (best delta ${delta} on "
              f"market={market_id} outcome={outcome_id}). Nothing new to spend.{Colors.RESET}")
        return 0.0
    print(f"{Colors.CYAN}🎯 Targeting market={market_id} outcome={outcome_id} "
          f"(${balance}, grew by ${delta} this round){Colors.RESET}")

    line_id = _line_id_for_outcome(framework, maker, event_id, market_id, outcome_id)
    if not line_id:
        print(f"{Colors.RED}✗ Could not resolve line_id for outcome {outcome_id}.{Colors.RESET}")
        return 0.0
    match_line_id = resolve_opposite_line(framework, maker, event_id, line_id)
    if not match_line_id:
        print(f"{Colors.RED}✗ Could not resolve opposite line for outcome {outcome_id}.{Colors.RESET}")
        return 0.0

    ladder = get_odds_ladder(framework)
    positive = [o for o in ladder if o > 100] or [150]
    odds = random.choice(positive)
    framework.place_wager(maker, line_id, odds, spend_stake)
    time.sleep(2)
    framework.place_wager(taker, match_line_id, -odds, spend_stake)
    print(f"{Colors.CYAN}⏳ Placed ${spend_stake} follow-up match on the LEC-holding outcome, "
          f"polling for it to drop...{Colors.RESET}")

    time.sleep(poll_interval)
    after_lec = get_lec_snapshot(framework, web_name, event_id) or {}
    after_balance = after_lec.get((market_id, outcome_id), 0)
    spent = balance - after_balance
    print(f"{Colors.BOLD}LEC (market={market_id} outcome={outcome_id}): ${balance} -> ${after_balance}{Colors.RESET}")
    if spent > 0.001:
        print(f"{Colors.GREEN}✅ Spent ~${spent:.2f} of LEC.{Colors.RESET}")
        return spent
    print(f"{Colors.RED}✗ That LEC entry did not decrease. The pipeline may not have synced this match yet.{Colors.RESET}")
    return 0.0


def spend_lec(framework, state, rounds, spend_stake, poll_interval, timeout):
    print(f"\n{Colors.BOLD}=== LEC ==={Colors.RESET}")
    total_spent = 0.0
    successes = 0
    for i, round_state in enumerate(rounds):
        print(f"\n{Colors.BOLD}-- LEC round {i + 1}/{len(rounds)} (event {round_state['event_id']}) --{Colors.RESET}")
        spent = _spend_lec_round(framework, state, round_state, spend_stake, poll_interval, timeout)
        if spent > 0.001:
            total_spent += spent
            successes += 1

    print(f"\n{Colors.BOLD}LEC total: spent ~${total_spent:.2f} across {successes}/{len(rounds)} rounds.{Colors.RESET}")
    return successes > 0, total_spent


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--state-file', required=True, help='State JSON saved by generate.py.')
    ap.add_argument('--credit-type', choices=['gec', 'lec', 'both'], default='both')
    ap.add_argument('--rounds', type=int, default=None,
                     help='How many rounds to process (default: however many generate.py saved, or 1 for an '
                          'older single-round state file). For LEC, each round targets one generate.py round\'s '
                          'own event; for GEC (one global balance) it\'s just N follow-up spend attempts.')
    ap.add_argument('--spend-stake', type=float, default=20.0,
                     help='Stake for the follow-up (spend) wager (default 20.0). Keep it at or below the '
                          'polled balance for an unambiguous read -- see module docstring caveat.')
    ap.add_argument('--poll-interval', type=float, default=EXPOSURE_SYNC_POLL_INTERVAL_SEC,
                     help=f'Seconds between sync polls (default {EXPOSURE_SYNC_POLL_INTERVAL_SEC}, matching '
                          f'the pipeline\'s own tick).')
    ap.add_argument('--timeout', type=float, default=EXPOSURE_SYNC_TIMEOUT_SEC,
                     help=f'Max seconds to poll before giving up (default {EXPOSURE_SYNC_TIMEOUT_SEC}).')
    args = ap.parse_args()

    with open(args.state_file) as f:
        state = json.load(f)

    all_rounds = _rounds_from_state(state)
    n_rounds = args.rounds or len(all_rounds)
    rounds = all_rounds[:n_rounds]
    print(f"{Colors.CYAN}Processing {len(rounds)}/{len(all_rounds)} round(s) from {args.state_file}.{Colors.RESET}")

    framework = DeduceTestFramework(environment=state['env'])
    login_makers(framework, [state['maker_account']])
    login_takers(framework, [state['taker_account']])
    login_maker_web_session(framework, state['maker_account'], state['maker_name'])

    results = {}
    spent = {}
    if args.credit_type in ('gec', 'both'):
        results['gec'], spent['gec'] = spend_gec(framework, state, rounds, args.spend_stake, args.poll_interval, args.timeout)
    if args.credit_type in ('lec', 'both'):
        results['lec'], spent['lec'] = spend_lec(framework, state, rounds, args.spend_stake, args.poll_interval, args.timeout)

    print(f"\n{Colors.BOLD}{'='*50}\nSUMMARY: succeeded={results} spent={spent}\n{'='*50}{Colors.RESET}")
    return 0 if all(results.values()) else 1


if __name__ == '__main__':
    exit(main())
