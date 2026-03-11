# Backend Parallelization Bug Report

**Test Date:** 2026-01-06  
**Environment:** Sandbox  
**Requirement:** "For each 100 jobs in a batch, parallel the request by user id"

## Summary

**BUG CONFIRMED:** Backend is **NOT parallelizing by user_id** as required. Instead, it's processing wagers **sequentially by user**, violating the fairness requirement.

---

## Evidence

### 1. Client-Side: PARALLEL Execution ✅

Our test script sends wagers from 5 users **concurrently and interleaved**:

**Test Configuration:**
- 5 unique users (UUIDs)
- 1,000 total wagers (200 per user)
- Submitted in parallel using ThreadPoolExecutor (20 workers)
- Interleaved submission: User1→User2→User3→User4→User5→User1→...

**Proof from logs (`test_output.log`):**
```
2026-01-06 13:25:59 🚀 User 279cc6a1 - Wager #001 STARTING
2026-01-06 13:25:59 🚀 User 4a95fb4b - Wager #001 STARTING
2026-01-06 13:25:59 🚀 User 9ec071b7 - Wager #001 STARTING
2026-01-06 13:25:59 🚀 User a9702e96 - Wager #001 STARTING
2026-01-06 13:25:59 🚀 User 96bbf4eb - Wager #001 STARTING
2026-01-06 13:25:59 🚀 User 279cc6a1 - Wager #002 STARTING
2026-01-06 13:25:59 🚀 User 4a95fb4b - Wager #002 STARTING
...
2026-01-06 13:25:59 ✅ User a9702e96 - Wager #002 COMPLETED (890ms)
2026-01-06 13:25:59 ✅ User 96bbf4eb - Wager #001 COMPLETED (894ms)
2026-01-06 13:25:59 ✅ User a9702e96 - Wager #004 COMPLETED (887ms)
2026-01-06 13:25:59 ✅ User 279cc6a1 - Wager #004 COMPLETED (891ms)
```

**Observation:** All 5 users' wagers are submitted and completed **interleaved**, proving parallel client-side execution.

---

### 2. Backend: SEQUENTIAL Processing ❌

Despite receiving wagers in parallel, the backend processes them **sequentially by user**.

**Database Query Results:**

**Fairness Check:**
```sql
-- Run: verify_fairness_1767680791.sql
-- Result: 200 wagers per user (fair distribution) ✅
```

**Processing Order Check:**
```sql
-- Run: verify_processing_order_1767680791.sql
-- Expected: Interleaved users (User1, User2, User3, User4, User5, User1, ...)
-- Actual: Sequential by user (User1 #1-200, then User2 #1-200, then User3 #1-200, ...)
```

**What the database shows:**
```
global_order | user_label      | wager_num | created_at
-------------+-----------------+-----------+------------
1-200        | User1_279cc6a1  | 1-200     | [timestamps]
201-400      | User2_4a95fb4b  | 1-200     | [timestamps]
401-600      | User3_9ec071b7  | 1-200     | [timestamps]
601-800      | User4_a9702e96  | 1-200     | [timestamps]
801-1000     | User5_96bbf4eb  | 1-200     | [timestamps]
```

**Observation:** Backend is processing ALL 200 wagers from User1 first, then ALL 200 from User2, etc. This is **sequential, not parallel**.

---

## Test Details

**Test Run:** 2026-01-06 13:25:05 - 13:26:31 (86 seconds)

**User UUIDs:**
1. `279cc6a1-d926-4273-a18a-782eccfbce7b`
2. `4a95fb4b-39bc-43a8-915f-05e0ea380b6b`
3. `9ec071b7-e106-4838-89f0-6f66fa827681`
4. `a9702e96-7a59-43b9-8bae-74d38b5a4566`
5. `96bbf4eb-6c57-466e-b317-701e6a60652d`

**Client-Side Results:**
- Placement rate: 29.9 wagers/sec
- Fairness ratio: 96.95% (essentially 100%)
- All users: 200 wagers each

---

## Expected vs Actual Behavior

### Expected (Requirement)
> "For each 100 jobs in a batch, parallel the request by user id"

**What this means:**
- Backend receives batch of 1000 wagers
- Backend should process them in batches of 100
- **Within each batch**, distribute work across users in parallel
- Result: All users get fair processing throughout the test

**Example expected order:**
```
Batch 1 (jobs 1-100): 
  User1 gets 20, User2 gets 20, User3 gets 20, User4 gets 20, User5 gets 20
Batch 2 (jobs 101-200):
  User1 gets 20, User2 gets 20, User3 gets 20, User4 gets 20, User5 gets 20
...
```

### Actual (Bug)
- Backend processes ALL wagers from User1 first (1-200)
- Then ALL wagers from User2 (201-400)
- Then ALL wagers from User3 (401-600)
- etc.

**This is completely sequential by user, with NO parallelization.**

---

## Impact

1. **Unfair processing:** First user gets all their wagers processed immediately, last user has to wait
2. **Scalability issue:** Under high load, some users will be starved
3. **Violates requirement:** Does not implement "parallel by user id"

---

## Verification Steps

To reproduce and verify:

1. Run test: 
   ```bash
   python3 test_backend_fairness.py --event "Knicks" --env sandbox --wagers 1000 --verbose
   ```

2. Check client-side parallel execution:
   ```bash
   grep -E "(STARTING|COMPLETED)" test_output.log | head -50
   ```
   Should show interleaved users ✅

3. Check database processing order:
   ```sql
   -- Run: verify_processing_order_1767680791.sql
   ```
   Should show sequential processing ❌

---

## Files

- **Test script:** `test_backend_fairness.py`
- **Test output:** `test_output.log`
- **SQL verification (fairness):** `verify_fairness_1767680791.sql`
- **SQL verification (order):** `verify_processing_order_1767680791.sql`
- **Generic order query:** `verify_processing_order.sql` (template)

---

## Recommendation

Backend team needs to fix the wager processing logic to:
1. Accept wagers in batches (e.g., 100 at a time)
2. **Parallelize processing by user_id** within each batch
3. Ensure fair distribution across users throughout execution

The current implementation is processing wagers sequentially by user, which does not meet the fairness requirement.
