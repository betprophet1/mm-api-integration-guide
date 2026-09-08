"""Phase 1: generate a real, meaningfully-sized GEC/LEC balance.

Places ONE deliberately larger (--stake, default $100) both-sides match on a
single market -- one maker wager, one taker wager, mirrored crossing odds --
and confirms both sides actually MATCHED (not just placed) before declaring
success. Confirmed live: GEC/LEC scale with stake size and how unbalanced the
match's odds are (see concentration_race/wallet_exposure.py's module
docstring for the formula), NOT with wager count -- 1500 tiny ($2-10) wagers
fired in a burst produced far less signal than one clean, larger match.

--rounds (default 50) repeats this match N times (each its own market lookup
+ maker/taker pair + confirm), so both wager count and total GEC/LEC scale
with N -- this is the default because a single match produces too little
signal for load-test purposes (see the docstring above). All rounds are
saved into ONE state file for spend.py to work through with its own --rounds
flag. Pass --rounds 1 for the old single-match behavior.

Saves state to a JSON file so `spend.py` can run as a separate step (the
exposure pipeline needs ~5-90s to sync -- see spend.py, which polls for this
rather than requiring a fixed sleep in between).

Usage:
  MM_ENVIRONMENT=qa python3 -m exposure_credit.generate --event-id 10079295
  MM_ENVIRONMENT=qa python3 -m exposure_credit.generate --rounds 1 --stake 200 --maker-account 1
  MM_ENVIRONMENT=qa python3 -m exposure_credit.generate --rounds 50 --stake 500

Then, in a separate command (see printed instructions):
  MM_ENVIRONMENT=qa python3 -m exposure_credit.spend --state-file <path> --rounds 50
"""
import argparse
import json
import os
import random
import time

from deduce_tests import DeduceTestFramework, Colors
from test_deduce_race_conditions import get_odds_ladder
from concentration_race.accounts import login_makers, login_takers
from concentration_race.market import find_market, resolve_opposite_line
from concentration_race.wallet_exposure import get_gec, get_lec_snapshot, login_maker_web_session
from src import config

STATE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'state')


def _lec_snapshot_to_json_safe(snapshot):
    if snapshot is None:
        return None
    return {f'{k[0]},{k[1]}': v for k, v in snapshot.items()}


def confirm_matched(framework, account_name, wager_id, tries=6, delay=2.0):
    """Poll get_matched_bets until wager_id shows up as matched, or give up.

    Placement success ("wager placed") does NOT mean it matched -- confirmed
    live, a placed wager can simply rest unmatched. Only a hit here counts."""
    for _ in range(tries):
        matched = framework.get_matched_bets(account_name, limit=20)
        for bet in matched:
            if bet.get('wager_id') == wager_id or bet.get('id') == wager_id:
                return True
        time.sleep(delay)
    return False


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--env', default=config.ENVIRONMENT, choices=['sandbox', 'staging', 'qa'])
    ap.add_argument('--maker-account', default=1,
                     help='MM account for config.get_account_credentials (e.g. 1, or exposure_mm1). '
                          'Must have email/password on file -- see concentration_race/wallet_exposure.py.')
    ap.add_argument('--taker-account', default='patron', help="Patron account id, e.g. 'patron' or 'patron3'.")
    ap.add_argument('--event-id', type=int, default=None, help='Event to bet on (auto-discover if omitted).')
    ap.add_argument('--stake', type=float, default=100.0,
                     help='Stake per side (default 100.0). Larger stake -> larger GEC/LEC -- see the formula '
                          'in concentration_race/wallet_exposure.py; this is a cash amount, not a wager count.')
    ap.add_argument('--rounds', type=int, default=50,
                     help='How many separate both-sides matches to fire (default 50). Each round is its own '
                          'market lookup + maker/taker pair + match confirmation -- use this for load-test '
                          'scale wager counts and cumulative GEC/LEC, instead of one giant match. Pass 1 for '
                          'the old single-match behavior.')
    ap.add_argument('--state-file', default=None, help='Where to save state for spend.py (default: auto-named).')
    args = ap.parse_args()

    maker_account = int(args.maker_account) if str(args.maker_account).isdigit() else args.maker_account

    framework = DeduceTestFramework(environment=args.env)
    makers = login_makers(framework, [maker_account])
    takers = login_takers(framework, [args.taker_account])
    maker, taker = makers[0], takers[0]

    web_name = login_maker_web_session(framework, maker_account, maker)
    if not web_name:
        print(f"{Colors.RED}✗ Maker account {maker_account} has no email/password on file -- "
              f"can't read GEC/LEC (needs a web token). Add them to its src/accounts/{args.env}/*.json "
              f"first (see src/accounts/qa/account1.json for the pattern).{Colors.RESET}")
        return 2

    baseline_gec = get_gec(framework, web_name)
    print(f"{Colors.CYAN}Baseline GEC: ${baseline_gec}{Colors.RESET}")

    ladder = get_odds_ladder(framework)
    positive = [o for o in ladder if o > 100] or [150]

    rounds = []
    for i in range(args.rounds):
        print(f"\n{Colors.BOLD}--- Round {i + 1}/{args.rounds} ---{Colors.RESET}")
        market_info = find_market(framework, maker, args.event_id)
        if not market_info:
            print(f"{Colors.RED}✗ No markets available, skipping round{Colors.RESET}")
            continue
        event_id = args.event_id or market_info.get('event', {}).get('event_id')
        line_id = market_info['line_id']
        match_line_id = resolve_opposite_line(framework, maker, event_id, line_id)
        if not match_line_id:
            print(f"{Colors.RED}✗ Could not resolve opposite line for event {event_id}, skipping round{Colors.RESET}")
            continue
        print(f"{Colors.GREEN}✅ Event {event_id}, line {line_id[:10]}... vs {match_line_id[:10]}...{Colors.RESET}")

        baseline_lec = get_lec_snapshot(framework, web_name, event_id)

        odds = random.choice(positive)
        print(f"{Colors.CYAN}🎲 Matching ${args.stake} at odds {odds} / -{odds}...{Colors.RESET}")

        r_maker = framework.place_wager(maker, line_id, odds, args.stake)
        if not r_maker.get('success'):
            print(f"{Colors.RED}✗ Maker placement failed: {r_maker.get('error')}, skipping round{Colors.RESET}")
            continue
        time.sleep(2)
        r_taker = framework.place_wager(taker, match_line_id, -odds, args.stake)
        if not r_taker.get('success'):
            print(f"{Colors.RED}✗ Taker placement failed: {r_taker.get('error')}, skipping round{Colors.RESET}")
            continue

        print(f"{Colors.CYAN}⏳ Confirming both sides matched (this is NOT the GEC/LEC sync wait -- "
              f"that's spend.py's job)...{Colors.RESET}")
        maker_matched = confirm_matched(framework, maker, r_maker.get('wager_id'))
        taker_matched = confirm_matched(framework, taker, r_taker.get('wager_id'))
        if not (maker_matched and taker_matched):
            print(f"{Colors.RED}✗ Not both sides matched (maker={maker_matched}, taker={taker_matched}). "
                  f"This wager rested instead of crossing -- GEC/LEC will NOT be generated for this round. "
                  f"Odds/liquidity vary run to run.{Colors.RESET}")
            continue
        print(f"{Colors.GREEN}✅ Both sides matched.{Colors.RESET}")

        rounds.append({
            'event_id': event_id,
            'line_id': line_id,
            'match_line_id': match_line_id,
            'odds': odds,
            'stake': args.stake,
            'baseline_lec': _lec_snapshot_to_json_safe(baseline_lec),
        })

    print(f"\n{Colors.BOLD}{len(rounds)}/{args.rounds} rounds matched.{Colors.RESET}")
    if not rounds:
        print(f"{Colors.RED}✗ No round matched -- nothing to save.{Colors.RESET}")
        return 1

    os.makedirs(STATE_DIR, exist_ok=True)
    state_file = args.state_file or os.path.join(STATE_DIR, f'{int(time.time())}.json')
    state = {
        'env': args.env,
        'maker_account': maker_account,
        'maker_name': maker,
        'maker_web_name': web_name,
        'taker_account': args.taker_account,
        'taker_name': taker,
        'stake': args.stake,
        'baseline_gec': baseline_gec,
        'rounds': rounds,
        'generated_at': time.time(),
    }
    with open(state_file, 'w') as f:
        json.dump(state, f, indent=2)

    print(f"\n{Colors.GREEN}📄 State saved: {state_file} ({len(rounds)} round(s)){Colors.RESET}")
    print(f"\n{Colors.BOLD}Next: run spend.py (it polls internally -- no need to sleep yourself):{Colors.RESET}")
    print(f"  MM_ENVIRONMENT={args.env} python3 -m exposure_credit.spend "
          f"--state-file {state_file} --rounds {len(rounds)}")
    return 0


if __name__ == '__main__':
    exit(main())
