
-- Database Verification Query
-- Run this on the database to verify backend fairness

SELECT 
    count(*) as wager_count,
    w.user_id,
    ROUND(count(*) * 100.0 / SUM(count(*)) OVER (), 2) as percentage
FROM wagers w
WHERE w.created_at >= '2026-03-02 09:32:03'
  AND w.created_at <= '2026-03-02 09:51:17'
  AND w.user_id IN (
    '279cc6a1-d926-4273-a18a-782eccfbce7b',     '4a95fb4b-39bc-43a8-915f-05e0ea380b6b',     'af5a8499-7cb8-45d2-8566-29f93d234ae8'
  )
GROUP BY w.user_id
ORDER BY wager_count DESC;

-- Expected: Each user should have ~13333 wagers (33.3% each)
-- User UUIDs:
--   279cc6a1-d926-4273-a18a-782eccfbce7b
--   4a95fb4b-39bc-43a8-915f-05e0ea380b6b
--   af5a8499-7cb8-45d2-8566-29f93d234ae8
