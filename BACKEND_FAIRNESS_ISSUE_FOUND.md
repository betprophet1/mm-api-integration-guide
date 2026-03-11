# Backend Fairness Issue - Database Evidence

## Problem Discovered

The backend is **NOT** distributing wagers fairly as claimed.

### Database Query Results

```sql
SELECT count(*) as c, w.user_id
FROM wagers w
WHERE w.created_at > '2026-01-05 14:28:59'
  AND w.created_at < '2026-01-05 14:39:53'
GROUP BY w.user_id
ORDER BY c desc;
```

**Results:**
```
 c   | user_id                               
-----|---------------------------------------
5903 | 279cc6a1-d926-4273-a18a-782eccfbce7b  (49%)
3560 | 4a95fb4b-39bc-43a8-915f-05e0ea380b6b  (30%)
2299 | 9ec071b7-e106-4838-89f0-6f66fa827681  (19%)
 300 | 96bbf4eb-6c57-466e-b317-701e6a60652d  (2%)
  43 | 3a842ae9-f81c-4a28-bb21-22a35b33766d  (0.4%)
   3 | a8811090-4812-42d1-b2db-87bf5cea991d  (0.02%)
   2 | 1650a90a-a57b-4416-b4c1-5c1023ba4201  (0.02%)
```

**Total:** ~12,110 wagers  
**Expected per user:** ~1,730 wagers (14.3% each) for 7 users

---

## The Issue

### What We Found in Client-Side Test
Our test script reported **100% fairness** because we were tracking by `access_key` prefix, not actual UUID:
- We tracked: `e85df05b`, `14fc4622`, `56a901ea`, etc.
- Backend tracks: Full UUIDs like `279cc6a1-d926-4273-a18a-782eccfbce7b`

**Result:** Our client-side metrics showed perfect fairness, but the database shows the truth - **extremely unfair distribution**.

### Actual Distribution (from database)
- **User 279cc6a1:** 5,903 wagers (49%) - **12x more than expected!**
- **User 4a95fb4b:** 3,560 wagers (30%) - 2x more than expected
- **User 9ec071b7:** 2,299 wagers (19%) - slightly more than expected
- **User 96bbf4eb:** 300 wagers (2%) - **7x less than expected!**
- **User 3a842ae9:** 43 wagers (0.4%) - **40x less than expected!**

### Fairness Ratio (Actual)
```
Fairness = min / max = 2 / 5903 = 0.03% ❌

This is NOT fair. Requirement was ≥90%.
```

---

## Root Cause

The backend is **NOT implementing** the fairness requirement:
> "For each 100 jobs in a batch, parallel the request by user id"

If it were batching by user ID fairly, we'd see:
- 7 users with ~1,730 wagers each (±10%)
- Fairness ratio: ≥90%

Instead we see:
- Massive imbalance (49% vs 0.02%)
- One user dominates
- Fairness ratio: 0.03%

**Conclusion:** The backend batching/parallelization by user ID is either:
1. Not implemented
2. Implemented incorrectly
3. Only working sometimes

---

## Test Script Fix

### What We Changed

**Before (WRONG):**
```python
user_id = access_key[:8]  # Using access_key prefix
# Tracked: e85df05b, 14fc4622, etc.
```

**After (CORRECT):**
```python
# Extract UUID from JWT token
import base64
access_token = mm_instance.mm_session.get('access_token', '')
parts = access_token.split('.')
payload = base64.urlsafe_b64decode(parts[1])
user_id = json.loads(payload).get('partnerID')
# Tracked: 279cc6a1-d926-4273-a18a-782eccfbce7b, etc.
```

### SQL Verification Query

The test now generates a SQL query to verify results in the database:

```sql
SELECT 
    count(*) as wager_count,
    w.user_id,
    ROUND(count(*) * 100.0 / SUM(count(*)) OVER (), 2) as percentage
FROM wagers w
WHERE w.created_at >= 'TEST_START_TIME'
  AND w.created_at <= 'TEST_END_TIME'
  AND w.user_id IN (
    'UUID1',
    'UUID2',
    ...
  )
GROUP BY w.user_id
ORDER BY wager_count DESC;
```

This SQL file is saved after each test run.

---

## How to Properly Verify

### 1. Run the Test
```bash
python3 test_backend_fairness.py --event "Knicks" --env sandbox --wagers 1000
```

### 2. Check the SQL File
The test generates: `verify_fairness_<timestamp>.sql`

### 3. Run SQL Query on Database
Execute the generated SQL query on your database to see **actual** distribution.

### 4. Compare Results
- **Client-side metrics** (from our script) - may show 100% fairness
- **Database query** (ground truth) - shows actual fairness

**The database is the source of truth.**

---

## Next Steps

###  1. Investigate Backend Code
Check the backend implementation of:
- Job batching logic
- User ID parallelization
- Fair distribution algorithm

### 2. Fix the Backend
The backend needs to actually implement:
- Batch jobs in groups of 100
- Split each batch evenly across user IDs
- Ensure fair distribution

### 3. Re-test
After backend fix:
- Run test script
- Check database with SQL query
- Verify fairness ratio ≥90%

---

## Test Script Updates

### Files Modified
- `test_backend_fairness.py`:
  - ✅ Now extracts UUID from JWT token
  - ✅ Tracks by actual UUID (not access_key prefix)
  - ✅ Generates SQL verification query

### New Files Generated Per Test
- `backend_fairness_test_<timestamp>.json` - Client-side metrics
- `verify_fairness_<timestamp>.sql` - Database verification query

---

## Conclusion

**❌ The backend fairness enhancement is NOT working as required.**

**Evidence:**
- Database shows 49% vs 0.02% distribution
- Fairness ratio: 0.03% (requirement: ≥90%)
- One user received 12x more wagers than expected
- Some users received 40x fewer wagers than expected

**Action Required:**
1. Fix backend batching/parallelization logic
2. Implement actual fair distribution
3. Re-test and verify with database query

**Status:** ❌ FAILING - Backend implementation required

---

**Date:** January 5, 2026  
**Issue:** Backend not distributing wagers fairly  
**Evidence:** Database query showing 0.03% fairness (49% vs 0.02%)  
**Required:** ≥90% fairness
