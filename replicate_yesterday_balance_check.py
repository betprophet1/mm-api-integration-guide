#!/usr/bin/env python3
"""
replicate_yesterday_balance_check.py
====================================

Drives BOTH balance-check failure counters during the stability load test:

  insufficient_funds   — stake > available balance (wallet check fails)
  max_stake_exceeded   — stake <= available, but cumulative resting exposure
                         + stake > balance × 20 (the position-limit cap)

WHICH COUNTER FIRES is decided purely by `stake` vs `available_balance`, checked
*before* the cap. So to fire max_stake we need an account whose `available ≈ full
balance` (i.e. LOW matched holds) AND whose resting exposure is already at the
×20 cap — then a small stake (≤ balance) passes the wallet check and trips the
cap. That's why this script PRIMES first.

THREE PHASES
  1. PRECONDITION  set `auto-cancel-unmatched-bet` OFF (else resting wagers get
     auto-cancelled and exposure never reaches the cap).
  2. PRIME         place deep-odds (uncrossable, +900) wagers — they REST
     unmatched (matched holds stay ~0, available stays ≈ balance) — until one is
     rejected, i.e. exposure has reached the cap. Empirical: no need to know the
     exact cap formula. Idempotent — if the account is already over the cap the
     first probe rejects and nothing is added.
  3. FIRE          worker threads alternate small (≤ balance → max_stake) and
     large (> balance → insufficient) wagers for the configured window. Over-cap/
     over-balance wagers are rejected (go invalid), so they don't accumulate.

Stakes auto-calibrate from the live balance (small ≈ 0.9×, large ≈ 1.5×) unless
REPL_SMALL_STAKE / REPL_LARGE_STAKE are set. The old $50/$200 were for usr002 at
balance ~$87; usr002 is ~100× larger now, so fixed small stakes just rest.

NOTE ON COUNTS: placement is async (returns `inactive`, the clear-pending-wager
validator flips it to `invalid` + increments the counter ~seconds later), so this
script CANNOT split max_stake vs insufficient client-side — it counts by *intent*
(small→max_stake, large→insufficient). The authoritative split is the Prometheus
counter; the weekly runner's verify_balance_check_counters.py reads that.

Default account: lam.tran+usr002@betprophet.co (RESERVED for this). Primed state
is left in place for reuse (set REPL_TEARDOWN=1 to cancel what this run built).
"""
from __future__ import annotations

import json
import os
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid
from concurrent.futures import ThreadPoolExecutor

BASE_URL = os.getenv("REPL_BASE_URL", "https://sandbox.prophetx.dev")
EMAIL = os.getenv("REPL_EMAIL", "lam.tran+usr002@betprophet.co")
PASSWORD = os.getenv("REPL_PASSWORD", "Kh0ngbiet1")
# Live event(s) to harvest market lines from (comma-sep). One event yields
# several lineIDs; priming to the ×20 cap needs multiple lines (per-line cap is
# below the cap). 80051288 = the live WNBA event confirmed on sandbox.
EVENT_IDS = [int(x) for x in os.getenv("REPL_EVENT_IDS", "80051288").split(",") if x.strip()]
# Known-live fallback lines (same WNBA event, both rest); always merged in.
LINE_FALLBACKS = [s for s in os.getenv(
    "REPL_LINES",
    "1f511e37c9cdbc463d61762e69219859,7ef015e9993f40b784c616a1af14ee7b").split(",") if s.strip()]

WORKERS = int(os.getenv("REPL_WORKERS", "8"))
DURATION_MIN = int(os.getenv("REPL_DURATION_MIN", "30"))
SMALL_STAKE = float(os.getenv("REPL_SMALL_STAKE", "0"))       # 0 = auto-calibrate
LARGE_STAKE = float(os.getenv("REPL_LARGE_STAKE", "0"))       # 0 = auto-calibrate
ODDS = int(os.getenv("REPL_ODDS", "900"))                     # deep/uncrossable → rests
GAP_S = float(os.getenv("REPL_GAP_S", "0.6"))
MAX_PRIME = int(os.getenv("REPL_MAX_PRIME", "200"))           # safety cap on prime wagers
TEARDOWN = os.getenv("REPL_TEARDOWN", "0") == "1"

counters = {"max_stake_exceeded": 0, "insufficient_funds": 0,
            "rested": 0, "http_error": 0, "exceptions": 0}
counter_lock = threading.Lock()
# NOTE: this is a placeholder. It is RESET at the start of the fire phase — see
# main(). Computing the deadline at import time is a bug: priming runs first and
# can take minutes (436 lines on a rich event), so the whole DURATION_MIN budget
# can be spent before a single fire wager is placed. The workers then see
# time.time() >= stop_at and exit instantly, producing an all-zero report that
# looks exactly like "the validator never rejected anything". Observed 2026-08-12:
# prime succeeded ($8,729 exposure vs $6,714 cap) yet every counter read 0 and no
# [report] line was ever emitted.
stop_at = time.time() + DURATION_MIN * 60
primed_refs: list[str] = []
primed_lock = threading.Lock()


def _http(method, path, token=None, body=None, timeout=15):
    url = BASE_URL + path
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    req.add_header("x-currency", "cash")
    req.add_header("__source", "web")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read() or b"{}")
    except urllib.error.HTTPError as ex:
        try:
            return ex.code, json.loads(ex.read())
        except Exception:
            return ex.code, {"error": str(ex)}


def login() -> str:
    s, b = _http("POST", "/api/v1/auth/login", body={
        "email": EMAIL, "password": PASSWORD, "device_id": str(uuid.uuid4()),
    })
    if s != 200 or not b.get("accessToken"):
        raise RuntimeError(f"login failed {s}: {b}")
    return b["accessToken"]


def find_line_ids(token: str) -> list:
    """Return MANY distinct lineIDs (one event's markets give several). Multiple
    lines are required: each line caps resting at a per-line limit BELOW the ×20
    cap, so priming to the cap must spread across lines."""
    lines = []
    for ev in EVENT_IDS:
        s, b = _http("GET", f"/trade/public/api/v2/events/{ev}/markets", token=token)
        if s != 200:
            continue
        def walk(o):
            if isinstance(o, dict):
                if isinstance(o.get("lineID"), str):
                    yield o["lineID"]
                for v in o.values():
                    yield from walk(v)
            elif isinstance(o, list):
                for v in o:
                    yield from walk(v)
        lines.extend(walk(b))
    # dedupe, keep order; always include the known-live fallback lines
    seen, out = set(), []
    for lid in lines + LINE_FALLBACKS:
        if lid and lid not in seen:
            seen.add(lid)
            out.append(lid)
    return out


def set_auto_cancel(token, enabled):
    return _http("PUT", "/api/v1/user/auto-cancel-unmatched-bet", token=token,
                 body={"isAutoCancelUnmatchedBet": enabled})


def get_balance(token):
    s, b = _http("GET", "/api/v1/wallet", token=token)
    return float((b.get("data") or {}).get("balance") or 0)


def get_open_exposure(token):
    s, b = _http("GET", "/trade/private/api/v1/wagers?status=open&limit=400", token=token)
    return sum(float(w.get("unmatchedStake") or 0)
               for w in ((b.get("data") or {}).get("wagers") or []))


def place(token, line_id, stake):
    return _http("POST", "/trade/private/api/v2/wagers", token=token,
                 body={"lineID": line_id, "odds": ODDS, "stake": stake})


def poll_final_status(token, ref, tries=12):
    """Placement is async (inactive→open|invalid). Poll until terminal."""
    for _ in range(tries):
        s, b = _http("GET", f"/trade/private/api/v1/wagers/{ref}", token=token)
        st = ((b.get("data") or {}).get("wager") or {}).get("status")
        if st and st != "inactive":
            return st
        time.sleep(0.5)
    return "inactive"


def cancel(token, ref):
    _http("PUT", f"/trade/private/api/v1/wagers/{ref}/cancel", token=token)


def prime(token, lines, build_stake, cap) -> bool:
    """Build TOTAL resting exposure across MANY lines until it reaches the ×20
    cap. Each line caps resting BELOW the ×20 cap, so we round-robin: place a
    build_stake on each line per round, watch total exposure grow, and move past
    lines that are full. Primed when total open exposure >= cap*0.97."""
    target = cap * 0.97
    print(f"[prime] building resting across {len(lines)} line(s) to the ×20 cap "
          f"(target ${target:.0f}) with ${build_stake} deep-odds wagers…")
    stall = 0
    for rnd in range(MAX_PRIME):
        exp = get_open_exposure(token)
        if exp >= target:
            print(f"[prime] PRIMED — total open exposure ${exp:.0f} >= target ${target:.0f} (cap ${cap:.0f}).")
            return True
        before = exp
        for line in lines:
            s, b = place(token, line, build_stake)
            ref = ((b.get("data") or {}).get("wager") or {}).get("refId")
            if ref:
                with primed_lock:
                    primed_refs.append(ref)
        time.sleep(2.5)  # let async validation settle before re-measuring
        after = get_open_exposure(token)
        print(f"[prime] round {rnd+1}: exposure ${before:.0f} → ${after:.0f}")
        stall = stall + 1 if after <= before + 1 else 0
        if stall >= 3:
            print(f"[prime] WARNING exposure stalled at ${after:.0f} < cap ${cap:.0f} — "
                  f"all {len(lines)} lines full; need more live markets. Proceeding (max_stake may not fire).")
            return after >= target
    return get_open_exposure(token) >= target


def worker(wid, lines):
    token = login()
    placed = 0
    while time.time() < stop_at:
        is_small = placed % 2 == 0
        stake = SMALL_STAKE if is_small else LARGE_STAKE
        intent = "max_stake_exceeded" if is_small else "insufficient_funds"
        # round-robin across lines so no single line's per-line cap matters —
        # once total exposure is at the ×20 cap, any new wager trips it.
        line_id = lines[(wid + placed) % len(lines)]
        try:
            s, b = place(token, line_id, stake)
            with counter_lock:
                if s != 200:
                    counters["http_error"] += 1
                else:
                    w = (b.get("data") or {}).get("wager") or {}
                    st = w.get("status")
                    if st in ("inactive", "invalid"):
                        # rejected over-cap/over-balance wagers land here (async).
                        # Count by intent — Prometheus is authoritative for the split.
                        counters[intent] += 1
                    elif st == "open":
                        # a SMALL wager rested → exposure was under cap; it just
                        # re-primed. Cancel so it doesn't balloon, count as rested.
                        counters["rested"] += 1
                        ref = w.get("refId")
                        if ref and is_small:
                            threading.Thread(target=lambda r=ref: (poll_final_status(token, r),
                                                                   cancel(token, r)),
                                             daemon=True).start()
        except urllib.error.URLError:
            with counter_lock:
                counters["exceptions"] += 1
            time.sleep(2)
            try:
                token = login()
            except Exception:
                time.sleep(5)
        except Exception:
            with counter_lock:
                counters["exceptions"] += 1
        placed += 1
        time.sleep(GAP_S)


def guardian(token, lines, cap):
    """Every 30s ensure total exposure stays at/over the cap; re-prime across
    lines if it dropped (so SMALL fire-wagers keep tripping max_stake)."""
    while time.time() < stop_at:
        time.sleep(30)
        try:
            exp = get_open_exposure(token)
            if exp < cap * 0.95:
                print(f"[guardian] exposure ${exp:.0f} dropped below cap ${cap:.0f} — re-priming")
                prime(token, lines, SMALL_STAKE, cap)
        except Exception:
            pass


def reporter():
    last = dict(counters)
    last_t = time.time()
    while time.time() < stop_at:
        time.sleep(15)
        now = time.time()
        el = now - last_t
        with counter_lock:
            cur = dict(counters)
        d = {k: cur[k] - last[k] for k in cur}
        print(f"[report] +{int(el)}s  max_stake(intent)={d['max_stake_exceeded']} "
              f"(~{d['max_stake_exceeded']/el*60:.0f}/min)  "
              f"insufficient(intent)={d['insufficient_funds']} (~{d['insufficient_funds']/el*60:.0f}/min)  "
              f"rested={d['rested']} http_err={d['http_error']} exc={d['exceptions']}  "
              f"remaining={max(0,int(stop_at-now))}s")
        last, last_t = cur, now


def main() -> int:
    global SMALL_STAKE, LARGE_STAKE
    print(f"[init] login as {EMAIL} @ {BASE_URL}")
    token = login()

    sc, _ = set_auto_cancel(token, False)
    print(f"[init] auto-cancel-unmatched-bet → OFF (http {sc})")

    bal = get_balance(token)
    exp = get_open_exposure(token)
    # SMALL must stay <= AVAILABLE balance so it passes the wallet check and the
    # validator reaches the cap check (→ max_stake). 0.5×balance is a safe margin
    # even if some matched holds reduce available; lower REPL_SMALL_STAKE further
    # if Grafana shows SMALL landing on insufficient instead of max_stake.
    # 2026-08-12: the old `max(..., 50)` / `max(..., 200)` floors are wrong for a
    # small balance. The fixture is now sized to the event so the ×20 cap is
    # reachable (see calibrate_balance_fixture.py), which can put the balance
    # around $20 — and a $50 floor would make SMALL *exceed* the balance, so it
    # would trip insufficient_funds instead of max_stake and the cap path would
    # never be proven. Scale strictly from the balance; only guard against 0.
    if SMALL_STAKE <= 0:
        SMALL_STAKE = max(round(bal * 0.5, 2), 1.0)
    if LARGE_STAKE <= 0:
        LARGE_STAKE = max(round(bal * 1.5, 2), SMALL_STAKE + 1.0)
    if SMALL_STAKE > bal:
        print(f"[init] WARNING SMALL=${SMALL_STAKE} exceeds balance ${bal:.2f} — it would trip "
              "insufficient_funds, not max_stake. Clamping.")
        SMALL_STAKE = round(bal * 0.5, 2)
    if LARGE_STAKE <= bal:
        print(f"[init] WARNING LARGE=${LARGE_STAKE} does not exceed balance ${bal:.2f} — it would "
              "not trip insufficient_funds. Raising.")
        LARGE_STAKE = round(bal * 1.5, 2)
    cap = bal * 20
    print(f"[init] balance=${bal:.2f} open_exposure=${exp:.2f} ×20 cap=${cap:.2f}")
    print(f"[init] SMALL=${SMALL_STAKE} (≤balance→max_stake)  LARGE=${LARGE_STAKE} (>balance→insufficient)")

    lines = find_line_ids(token)
    print(f"[init] discovered {len(lines)} line(s) across events {EVENT_IDS}: {[l[:10] for l in lines]}")
    if not lines:
        print("[init] FATAL: no lines discovered — cannot prime.")
        return 2

    # PHASE 2 — prime TOTAL resting across lines to the ×20 cap.
    #
    # The return value is NOT optional. If we cannot get total resting exposure to
    # the cap, the fire phase below is actively harmful: SMALL wagers are then
    # under the cap, so instead of being rejected they REST — and any concurrent
    # traffic on the same event (the stability phase runs in parallel) MATCHES
    # them, draining the reserved account to ~zero. That is what emptied usr002 on
    # 2026-08-04 ($87 -> $0.45) and 2026-08-11 ($87 -> $0.03), and it made the
    # phase report "max_stake_exceeded=0 — cap regression" when the cap was never
    # exercised at all. Abort instead: a missing precondition is not a product
    # verdict. Exit 2 = precondition not met (runner classifies as INCONCLUSIVE).
    if not prime(token, lines, SMALL_STAKE, cap):
        exp_now = get_open_exposure(token)
        print(f"[init] ABORT: prime reached only ${exp_now:.0f} of the ${cap:.0f} ×20 cap "
              f"across {len(lines)} line(s).")
        print("[init] Firing now would rest wagers instead of tripping the cap, and "
              "concurrent traffic would match them and drain the account.")
        print("[init] Need more live lines (richer event) or a lower balance so the "
              "cap is reachable. Not firing.")
        print("BALANCE_CHECK_REPL_RESULT: precondition_failed "
              f"exposure={exp_now:.0f} cap={cap:.0f} lines={len(lines)}")
        return 2

    # PHASE 3 — fire across lines (total at cap → SMALL trips max_stake, LARGE insufficient).
    # Start the clock HERE, not at import: priming has just consumed an unknown
    # amount of wall time and the fire phase needs its full DURATION_MIN.
    global stop_at
    stop_at = time.time() + DURATION_MIN * 60
    print(f"[fire] {WORKERS} workers × {DURATION_MIN}min, alternating SMALL/LARGE across {len(lines)} lines")
    threading.Thread(target=reporter, daemon=True).start()
    threading.Thread(target=guardian, args=(token, lines, cap), daemon=True).start()
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futs = [pool.submit(worker, i, lines) for i in range(WORKERS)]
        for f in futs:
            try:
                f.result()
            except Exception as ex:
                print(f"worker failed: {ex}")

    if TEARDOWN and primed_refs:
        print(f"[teardown] cancelling {len(primed_refs)} resting wagers this run created")
        for ref in primed_refs:
            try:
                cancel(token, ref)
            except Exception:
                pass

    print("\n" + "=" * 60 + "\nFINAL TOTALS (intent-based; Grafana counter is authoritative)\n" + "=" * 60)
    for k, v in counters.items():
        print(f"  {k:<22} {v}")
    print(f"BALANCE_CHECK_REPL_RESULT: "
          f"max_stake_exceeded={counters['max_stake_exceeded']} "
          f"insufficient_funds={counters['insufficient_funds']} "
          f"duration_min={DURATION_MIN} workers={WORKERS}")
    # Non-zero exit only if we produced essentially no load (real failure).
    fired = counters["max_stake_exceeded"] + counters["insufficient_funds"]
    return 0 if fired > 0 else 1


if __name__ == "__main__":
    sys.exit(main())
