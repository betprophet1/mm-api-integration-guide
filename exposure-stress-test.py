#!/usr/bin/env python3
"""
Exposure Stress Test: GEC Deadlock Simulator
Strategy: Each account bets BOTH sides of the same market to generate GEC.
  - Bet $100 on outcome 1 at +100
  - Bet $100 on outcome 2 at -100
  - This triggers Global Exposure Check (GEC)
  - 4 accounts doing this concurrently across many markets = deadlock potential

Accounts (all have exposure enabled):
  1. lam.tran+usr001@betprophet.co (patron)
  2. lam.tran+usr002@betprophet.co (patron)
  3. Exposure-MM1-19270 (thanos_exposure_19270@gmail.com)
  4. Exposure-MM2-19271 (thanos_exposure_19271@gmail.com)

Usage:
  python3 exposure-stress-test.py --event-id 30024944 --workers 20 --target 5000
  python3 exposure-stress-test.py --event-id 30024944 --workers 50 --target 10000
"""

import argparse
import signal
import sys
import time
import threading
import uuid
import requests
import json
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# ─── Global state ────────────────────────────────────────────────────────────

should_stop = False
stats_lock = threading.Lock()

stats = {
    'bets_placed': 0,
    'bets_failed': 0,
    'gec_pairs': 0,         # successful both-sides pair = 1 GEC trigger
    'by_account': {},        # per-account stats
    'errors': [],
    'start_time': None,
}

# ─── Environment ─────────────────────────────────────────────────────────────

ENVIRONMENT_URLS = {
    'sandbox': 'https://api-ss-sandbox.betprophet.co',
    'staging': 'https://api-ss-staging.betprophet.co',
}

# ─── 4 Exposure-enabled accounts ─────────────────────────────────────────────

ACCOUNTS = [
    {
        "name": "usr001",
        "access_key": "cef986b533dfd3a0b1a732e34e5c1d60",
        "secret_key": "9d2f9f93158526a2fb9aeb24a6c5c082",
    },
    {
        "name": "usr002",
        "access_key": "3324857df2d66566dfe6b660faa2923f",
        "secret_key": "8c970658226e64c7346e753ed7377c48",
    },
    {
        "name": "Exposure-MM1-19270",
        "access_key": "a58fb825b4a165e202eb9be999f52e39",
        "secret_key": "b45edd218e76320ae57cd2011556e432",
    },
    {
        "name": "Exposure-MM2-19271",
        "access_key": "d126b05dea547ff34cd7bff2796b22ea",
        "secret_key": "ea609c8ae92a81cd6ae51d2c93bb5aa2",
    },
]

# ─── Signal handler ──────────────────────────────────────────────────────────

def signal_handler(sig, frame):
    global should_stop
    print("\n  Stopping...")
    should_stop = True

signal.signal(signal.SIGINT, signal_handler)

# ─── Account helper ──────────────────────────────────────────────────────────

class MMAccount:
    """Thin wrapper around MM API for a single account."""

    def __init__(self, creds: dict, base_url: str):
        self.name = creds["name"]
        self.base_url = base_url
        self.creds = creds
        self.token = None
        self.balance = 0

    def login(self) -> bool:
        try:
            resp = requests.post(
                f"{self.base_url}/partner/auth/login",
                json={"access_key": self.creds["access_key"], "secret_key": self.creds["secret_key"]},
                timeout=10,
            )
            if resp.status_code == 200:
                self.token = resp.json()["data"]["access_token"]
                return True
            print(f"  {self.name} login failed: {resp.status_code}")
        except Exception as e:
            print(f"  {self.name} login error: {e}")
        return False

    def refresh_token(self) -> bool:
        """Re-login to refresh expired token."""
        return self.login()

    def get_balance(self) -> float:
        try:
            resp = requests.get(
                f"{self.base_url}/partner/mm/get_balance",
                headers=self._h(),
                timeout=10,
            )
            if resp.status_code == 200:
                self.balance = resp.json().get("data", {}).get("balance", 0)
        except:
            pass
        return self.balance

    def place_wager(self, line_id: str, odds: int, stake: float) -> dict | None:
        """Place a single wager. Returns {wager_id, external_id} or None."""
        ext_id = str(uuid.uuid1())
        try:
            resp = requests.post(
                f"{self.base_url}/partner/mm/place_wager",
                json={"external_id": ext_id, "line_id": line_id, "odds": odds, "stake": stake},
                headers=self._h(),
                timeout=10,
            )
            if resp.status_code == 200:
                wager = resp.json().get("data", {}).get("wager", {})
                return {"wager_id": wager.get("id"), "external_id": ext_id}
            # Log non-200 errors for debugging
            with stats_lock:
                err_msg = f"{resp.status_code}: {resp.text[:200]}"
                stats['errors'].append({"ts": time.time(), "account": self.name, "err": err_msg})
            # Token expired? Try refresh once
            if resp.status_code == 401:
                if self.refresh_token():
                    resp2 = requests.post(
                        f"{self.base_url}/partner/mm/place_wager",
                        json={"external_id": ext_id, "line_id": line_id, "odds": odds, "stake": stake},
                        headers=self._h(),
                        timeout=10,
                    )
                    if resp2.status_code == 200:
                        wager = resp2.json().get("data", {}).get("wager", {})
                        return {"wager_id": wager.get("id"), "external_id": ext_id}
        except Exception as e:
            with stats_lock:
                stats['errors'].append({"ts": time.time(), "account": self.name, "err": str(e)})
        return None

    def _h(self):
        return {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"}


# ─── Market data ─────────────────────────────────────────────────────────────

def fetch_market_pairs(account: MMAccount, event_id: int) -> list[dict]:
    """
    Fetch markets for an event and return list of betting pairs:
    [{ event_id, market_id, market_type, sel_a: {line_id, outcome_id}, sel_b: {line_id, outcome_id} }]
    Each pair = two opposing selections in the same market.
    """
    resp = requests.get(
        f"{account.base_url}/partner/mm/get_multiple_markets",
        params={"event_ids": str(event_id)},
        headers=account._h(),
        timeout=10,
    )
    if resp.status_code != 200:
        print(f"  Failed to fetch markets for event {event_id}: {resp.status_code}")
        return []

    data = resp.json().get("data", {})
    pairs = []

    for evt_id, markets in data.items():
        for market in markets:
            market_id = market.get("id")
            market_type = market.get("type", "unknown")

            # Collect all selections
            all_sels = []

            if "selections" in market:
                for group in market.get("selections", []):
                    if isinstance(group, list):
                        for sel in group:
                            if sel.get("line_id"):
                                all_sels.append(sel)
                    elif isinstance(group, dict) and group.get("line_id"):
                        all_sels.append(group)

            if "market_lines" in market:
                for ml in market.get("market_lines", []):
                    for group in ml.get("selections", []):
                        if isinstance(group, list):
                            for sel in group:
                                if sel.get("line_id"):
                                    all_sels.append(sel)
                        elif isinstance(group, dict) and group.get("line_id"):
                            all_sels.append(group)

            # Group by outcome_id to find opposing sides
            by_outcome = {}
            for sel in all_sels:
                oid = sel.get("outcome_id")
                if oid not in by_outcome:
                    by_outcome[oid] = []
                by_outcome[oid].append(sel)

            outcome_ids = sorted(by_outcome.keys())

            # Create pairs from every two distinct outcomes
            for i in range(len(outcome_ids)):
                for j in range(i + 1, len(outcome_ids)):
                    oid_a, oid_b = outcome_ids[i], outcome_ids[j]
                    sel_a = by_outcome[oid_a][0]
                    sel_b = by_outcome[oid_b][0]
                    pairs.append({
                        "event_id": int(evt_id),
                        "market_id": market_id,
                        "market_type": market_type,
                        "sel_a": {"line_id": sel_a["line_id"], "outcome_id": oid_a},
                        "sel_b": {"line_id": sel_b["line_id"], "outcome_id": oid_b},
                    })

    return pairs


# ─── Worker ──────────────────────────────────────────────────────────────────

def gec_worker(worker_id: int, account: MMAccount, pairs: list[dict],
               target_pairs: int, stake: float):
    """
    Single worker: picks a market pair, bets both sides with the SAME account.
    Each successful both-sides bet = 1 GEC trigger.
    """
    global should_stop

    placed = 0
    failed = 0
    gecs = 0

    while not should_stop and gecs < target_pairs:
        # Check global stop
        with stats_lock:
            if stats['bets_placed'] >= target_pairs * len(ACCOUNTS) * 2:
                break

        pair = pairs[gecs % len(pairs)]

        # Bet side A: outcome 1, odds +100
        res_a = account.place_wager(pair["sel_a"]["line_id"], odds=100, stake=stake)
        if res_a:
            with stats_lock:
                stats['bets_placed'] += 1
                stats['by_account'].setdefault(account.name, {"placed": 0, "failed": 0, "gecs": 0})
                stats['by_account'][account.name]["placed"] += 1
            placed += 1
        else:
            with stats_lock:
                stats['bets_failed'] += 1
                stats['by_account'].setdefault(account.name, {"placed": 0, "failed": 0, "gecs": 0})
                stats['by_account'][account.name]["failed"] += 1
            failed += 1
            continue  # skip side B if side A failed

        # Bet side B: outcome 2, odds -110 (immediately after A to maximize GEC concurrency)
        # Note: API rejects odds=-100 exactly; must be < -100 or >= 100
        res_b = account.place_wager(pair["sel_b"]["line_id"], odds=-110, stake=stake)
        if res_b:
            with stats_lock:
                stats['bets_placed'] += 1
                stats['gec_pairs'] += 1
                stats['by_account'][account.name]["placed"] += 1
                stats['by_account'][account.name]["gecs"] += 1
            placed += 1
            gecs += 1
        else:
            with stats_lock:
                stats['bets_failed'] += 1
                stats['by_account'][account.name]["failed"] += 1
            failed += 1

    return placed, failed, gecs


# ─── Progress monitor ────────────────────────────────────────────────────────

def progress_monitor(target_total: int):
    global should_stop
    while not should_stop:
        time.sleep(5)
        if should_stop:
            break
        with stats_lock:
            elapsed = time.time() - stats['start_time'] if stats['start_time'] else 0
            placed = stats['bets_placed']
            failed = stats['bets_failed']
            gecs = stats['gec_pairs']
        if elapsed <= 0:
            continue
        rate = placed / elapsed
        print(f"  Progress: {placed} bets | {gecs} GEC pairs | {failed} failed | "
              f"{rate:.1f} bets/s | {elapsed:.0f}s elapsed")


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="GEC Deadlock Stress Test")
    parser.add_argument("--event-id", type=int, required=True, help="Target event ID")
    parser.add_argument("--workers", type=int, default=20, help="Workers PER account (default: 20)")
    parser.add_argument("--target", type=int, default=1000,
                        help="Target GEC pairs PER account (default: 1000)")
    parser.add_argument("--stake", type=float, default=100.0, help="Stake per bet in $ (default: 100)")
    parser.add_argument("--env", default="sandbox", choices=["sandbox", "staging"])
    args = parser.parse_args()

    base_url = ENVIRONMENT_URLS[args.env]
    total_workers = args.workers * len(ACCOUNTS)

    print("=" * 70)
    print("  GEC DEADLOCK STRESS TEST")
    print("=" * 70)
    print(f"  Event:            {args.event_id}")
    print(f"  Accounts:         {len(ACCOUNTS)}")
    print(f"  Workers/account:  {args.workers}")
    print(f"  Total workers:    {total_workers}")
    print(f"  Target GEC/acct:  {args.target}")
    print(f"  Stake:            ${args.stake:.0f}")
    print(f"  Environment:      {args.env}")
    print("=" * 70)

    # ── Auth ──
    print("\n--- Authenticating ---")
    accounts: list[MMAccount] = []
    for creds in ACCOUNTS:
        acct = MMAccount(creds, base_url)
        if not acct.login():
            print(f"  FATAL: {acct.name} auth failed")
            return
        bal = acct.get_balance()
        print(f"  {acct.name:20s}  balance: ${bal:,.2f}")
        accounts.append(acct)

    # ── Fetch market pairs ──
    print(f"\n--- Fetching market pairs for event {args.event_id} ---")
    pairs = fetch_market_pairs(accounts[0], args.event_id)
    print(f"  Found {len(pairs)} opposing-side pairs across markets")
    if not pairs:
        print("  FATAL: No pairs found")
        return

    # Show sample
    for p in pairs[:5]:
        print(f"    market={p['market_id']} type={p['market_type']} "
              f"outcome_a={p['sel_a']['outcome_id']} outcome_b={p['sel_b']['outcome_id']}")
    if len(pairs) > 5:
        print(f"    ... and {len(pairs) - 5} more")

    # ── Launch ──
    stats['start_time'] = time.time()
    target_per_worker = max(1, args.target // args.workers)

    # Start progress monitor
    mon = threading.Thread(target=progress_monitor, args=(args.target * len(ACCOUNTS),), daemon=True)
    mon.start()

    print(f"\n--- Launching {total_workers} workers ({args.workers} x {len(ACCOUNTS)} accounts) ---")
    print(f"  Each account: {args.workers} workers x {target_per_worker} GEC pairs/worker")
    print(f"  Strategy: bet ${args.stake:.0f} outcome1 +100 → bet ${args.stake:.0f} outcome2 -100 → GEC\n")

    futures = []
    with ThreadPoolExecutor(max_workers=total_workers) as pool:
        wid = 0
        for acct in accounts:
            for _ in range(args.workers):
                wid += 1
                f = pool.submit(gec_worker, wid, acct, pairs, target_per_worker, args.stake)
                futures.append((f, acct.name))

        for f, acct_name in futures:
            try:
                f.result()
            except Exception as e:
                print(f"  Worker error ({acct_name}): {e}")

    elapsed = time.time() - stats['start_time']

    # ── Final balances ──
    print("\n--- Final balances ---")
    for acct in accounts:
        bal = acct.get_balance()
        print(f"  {acct.name:20s}  ${bal:,.2f}")

    # ── Report ──
    print("\n" + "=" * 70)
    print("  GEC DEADLOCK STRESS TEST - REPORT")
    print("=" * 70)
    print(f"  Duration:       {elapsed:.1f}s ({elapsed/60:.1f}m)")
    print(f"  Bets placed:    {stats['bets_placed']}")
    print(f"  Bets failed:    {stats['bets_failed']}")
    print(f"  GEC pairs:      {stats['gec_pairs']}")
    if elapsed > 0:
        print(f"  Bet rate:       {stats['bets_placed']/elapsed:.1f} bets/s")
        print(f"  GEC rate:       {stats['gec_pairs']/elapsed:.1f} GECs/s")

    print(f"\n  Per-account breakdown:")
    for name, s in sorted(stats['by_account'].items()):
        print(f"    {name:20s}  placed={s['placed']}  failed={s['failed']}  gecs={s['gecs']}")

    if stats['errors']:
        print(f"\n  Last 10 errors:")
        for e in stats['errors'][-10:]:
            ts = datetime.fromtimestamp(e['ts']).strftime('%H:%M:%S')
            print(f"    [{ts}] {e['account']}: {e['err'][:100]}")

    # Save report
    report_file = f"gec_stress_test_{int(time.time())}.json"
    with open(report_file, "w") as f:
        json.dump({
            "duration_s": elapsed,
            "event_id": args.event_id,
            "accounts": len(ACCOUNTS),
            "workers_per_account": args.workers,
            "stake": args.stake,
            "stats": {
                "bets_placed": stats['bets_placed'],
                "bets_failed": stats['bets_failed'],
                "gec_pairs": stats['gec_pairs'],
                "by_account": stats['by_account'],
            },
            "errors_count": len(stats['errors']),
        }, f, indent=2)
    print(f"\n  Report saved: {report_file}")
    print("=" * 70)


if __name__ == "__main__":
    main()
