#!/usr/bin/env python3
"""
Calibrate the balance_check fixture (usr002) so the ×20 cap is actually reachable
=================================================================================
WHY THIS EXISTS

`replicate_yesterday_balance_check.py` proves two balance-validator paths:

  insufficient_funds  — stake > available balance
  max_stake_exceeded  — stake <= balance BUT total resting exposure is at the ×20 cap

The second only fires if the run can first build resting exposure up to
`balance × 20`. That is the whole precondition. And it is a moving target:

  cap        = balance × 20                      (product constant, fixed)
  achievable = per-line resting capacity × lines (varies with the event)

The event is auto-discovered each week, so the line count swings (10–42 observed).
A hard-coded balance therefore cannot work: at $87 the cap is $1,740, but a
28-line event only holds roughly $616. The prime stalls, the fire phase then rests
wagers instead of tripping the cap, concurrent stability traffic matches them, and
the reserved account drains to ~zero — which is exactly what happened on
2026-08-04 ($87 → $0.45) and 2026-08-11 ($87 → $0.03), both times reported as
"max_stake_exceeded=0 — cap regression" when the cap was never exercised.

So: size the fixture to the event, every run, instead of hoping a fixed number fits.

WHAT IT DOES
  1. Discovers the lines the balance_check will actually use (same code path).
  2. Estimates achievable resting exposure = PER_LINE_CAPACITY × lines.
  3. Targets cap = SAFETY × achievable, so the prime reaches it with headroom.
  4. Sets usr002's balance to cap / 20 via Nova, and RE-READS it from the trade
     API to confirm — nova_wallet's adjust reports success even when it failed.

USAGE
  python3 calibrate_balance_fixture.py --event-id 13002474
  python3 calibrate_balance_fixture.py --event-id 13002474 --dry-run

Exit 0 = fixture ready. Exit 2 = could not calibrate (too few lines / Nova down).
"""
from __future__ import annotations

import argparse
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, "..", "qa-mm-autoplay", "src"))

# Empirically measured on sandbox: 42 lines held ~$934 of resting exposure before
# every line hit its own per-line cap (2026-08-04). ~$22/line. Deliberately
# conservative — underestimating just means a smaller, still-valid cap.
PER_LINE_CAPACITY = 22.0

# Aim the cap below what is achievable so the prime clears it with room to spare.
SAFETY = 0.70

# Floor: below this the auto-calibrated SMALL/LARGE stakes stop being meaningful
# (LARGE must exceed the balance to trip insufficient_funds).
MIN_BALANCE = 5.0


def main():
    ap = argparse.ArgumentParser(description="Size usr002 so the ×20 cap is reachable")
    ap.add_argument("--event-id", type=int, required=True,
                    help="event the balance_check will run against")
    ap.add_argument("--env", default="sandbox", choices=["sandbox", "qa"])
    ap.add_argument("--dry-run", action="store_true", help="compute only, change nothing")
    args = ap.parse_args()

    os.environ["REPL_EVENT_IDS"] = str(args.event_id)
    import replicate_yesterday_balance_check as R  # noqa: E402  (needs the env var)

    print("=" * 72)
    print("  CALIBRATE balance_check fixture (usr002)")
    print(f"  event={args.event_id}  env={args.env}")
    print("=" * 72)

    token = R.login()
    balance_before = R.get_balance(token)
    exposure = R.get_open_exposure(token)
    lines = R.find_line_ids(token)

    print(f"  balance now:      ${balance_before:,.2f}")
    print(f"  open exposure:    ${exposure:,.2f}")
    print(f"  lines discovered: {len(lines)}")

    if len(lines) < 4:
        print(f"\n  ABORT: only {len(lines)} line(s) — too few to build meaningful exposure.")
        print("  Pick a richer event; do not run balance_check against this one.")
        return 2

    achievable = PER_LINE_CAPACITY * len(lines)
    target_cap = SAFETY * achievable
    required_balance = round(target_cap / 20.0, 2)

    print(f"\n  achievable exposure ≈ ${achievable:,.0f}   ({PER_LINE_CAPACITY:.0f}/line × {len(lines)})")
    print(f"  target ×20 cap      ≈ ${target_cap:,.0f}   ({SAFETY:.0%} of achievable)")
    print(f"  => required balance = ${required_balance:,.2f}")
    print(f"     SMALL ≈ ${max(round(required_balance*0.5),50):,.0f} (≤ balance → max_stake)")
    print(f"     LARGE ≈ ${max(round(required_balance*1.5),200):,.0f} (> balance → insufficient)")

    if required_balance < MIN_BALANCE:
        print(f"\n  ABORT: required balance ${required_balance:.2f} below the ${MIN_BALANCE:.0f} floor.")
        return 2

    if args.dry_run:
        print("\n  [dry-run] nothing changed.")
        return 0

    # Set it via Nova, then verify against the trade API — nova_wallet's adjust
    # returns success even when the change did not land.
    try:
        import nova_wallet as N
    except Exception as ex:
        print(f"\n  ABORT: cannot import nova_wallet ({ex}). Is qa-mm-autoplay present?")
        return 2

    NOVA_HOSTS = {"sandbox": "https://nova.sandbox.prophetx.dev",
                  "qa": "https://nova.qa.prophetx.dev"}
    uuid = os.getenv("REPL_USER_UUID", "a9702e96-7a59-43b9-8bae-74d38b5a4566")

    nv = N.NovaWallet(env=args.env)
    nv.base_url = NOVA_HOSTS[args.env]
    if not nv.login():
        print("\n  ABORT: Nova login failed (Tailscale up?).")
        return 2

    nv.set_balance_by_uuid(uuid, required_balance,
                           notes="calibrate balance_check fixture: make ×20 cap reachable")

    balance_after = R.get_balance(R.login())
    print(f"\n  verified live balance: ${balance_after:,.2f}  (wanted ${required_balance:,.2f})")

    if abs(balance_after - required_balance) > 1.0:
        print("  ABORT: balance did not land. Nova reported success but the API disagrees.")
        return 2

    print(f"  new ×20 cap = ${balance_after*20:,.2f}  (prime should reach ~${achievable:,.0f})")
    print("\n  Fixture ready.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
