-- ============================================================================
-- ANALYTICS QUERIES - Auto CDC Demo
-- ============================================================================
--
-- PURPOSE:
-- Demonstrates how to query both current state (SCD Type 1) and
-- historical data (SCD Type 2) from Auto CDC processed tables.
--
-- TABLES USED:
-- - silver_users_current: SCD Type 1 (current state only)
-- - silver_users_history: SCD Type 2 (full version history)
-- - gold_user_demographics: Aggregated analytics
--
-- INSTRUCTIONS:
-- Run these queries in Databricks SQL Editor or notebook.
-- Replace catalog/schema with your actual target location.

-- ============================================================================
-- QUERY 1: Current User Profile (SCD Type 1)
-- ============================================================================
--
-- USE CASE: Get the latest profile for a specific user
-- WHY TYPE 1: Fast lookup, single row per user, operational queries
--

SELECT
  cpf,
  user_id,
  email,
  delivery_address,
  city,
  first_name,
  last_name,
  job
FROM silver_users_current
WHERE cpf = '000.481.949-11';

-- Expected result: 1 row with current user state


-- ============================================================================
-- QUERY 2: User Change History (SCD Type 2)
-- ============================================================================
--
-- USE CASE: Track all changes to a user's profile over time (LGPD/GDPR audit)
-- WHY TYPE 2: Full history, compliance, temporal analysis
--

SELECT
  cpf,
  email,
  city,
  first_name,
  last_name,
  __START_AT AS version_start,
  __END_AT AS version_end,
  CASE
    WHEN __END_AT IS NULL THEN 'CURRENT'
    ELSE 'HISTORICAL'
  END AS version_status,
  DATEDIFF(
    DAY,
    CAST(__START_AT AS TIMESTAMP),
    COALESCE(
      CAST(__END_AT AS TIMESTAMP),
      CURRENT_TIMESTAMP()
    )
  ) AS days_active
FROM silver_users_history
WHERE cpf = '000.481.949-11'
ORDER BY __START_AT DESC;

-- Expected result: Multiple rows showing all versions of the user profile
-- Most recent row has __END_AT = NULL (current version)


-- ============================================================================
-- QUERY 3: City-Level Analytics with Trends
-- ============================================================================
--
-- USE CASE: Business analytics - Compare current state vs historical patterns
-- COMBINES: Type 1 (current totals) + Type 2 (change activity)
--

WITH current_state AS (
  -- Current user counts by city (from Gold table)
  SELECT
    city,
    total_users,
    users_with_email,
    users_with_address,
    users_with_profile,
    ROUND(100.0 * users_with_email / NULLIF(total_users, 0), 1) AS email_completion_pct,
    ROUND(100.0 * users_with_profile / NULLIF(total_users, 0), 1) AS profile_completion_pct
  FROM main.uber_eats_analytics.gold_user_demographics
),
recent_changes AS (
  -- Count profile changes in last 7 days (from SCD Type 2)
  SELECT
    city,
    COUNT(DISTINCT cpf) AS users_changed_last_7d,
    COUNT(*) AS total_changes_last_7d
  FROM main.uber_eats_analytics.silver_users_history
  WHERE __START_AT >= CURRENT_DATE() - INTERVAL 7 DAYS
  GROUP BY city
)
SELECT
  cs.city,
  cs.total_users,
  cs.email_completion_pct,
  cs.profile_completion_pct,
  COALESCE(rc.users_changed_last_7d, 0) AS users_changed_last_7d,
  COALESCE(rc.total_changes_last_7d, 0) AS total_changes_last_7d,
  ROUND(100.0 * COALESCE(rc.users_changed_last_7d, 0) / NULLIF(cs.total_users, 0), 1) AS change_rate_pct
FROM current_state cs
LEFT JOIN recent_changes rc ON cs.city = rc.city
ORDER BY cs.total_users DESC;

-- Expected result: City-level metrics combining current state + change activity
-- Shows which cities have high user counts and high change rates


-- ============================================================================
-- ADVANCED QUERY: Point-in-Time Analysis (SCD Type 2)
-- ============================================================================
--
-- USE CASE: "What did this user's profile look like on January 1, 2025?"
-- WHY: Regulatory compliance, historical reconstruction, time-travel queries
--

SELECT
  cpf,
  email,
  city,
  first_name,
  last_name,
  __START_AT,
  __END_AT
FROM main.uber_eats_analytics.silver_users_history
WHERE cpf = '12345678900'
  AND __START_AT <= '2025-01-01'
  AND (__END_AT > '2025-01-01' OR __END_AT IS NULL)
LIMIT 1;

-- Expected result: The version of the user that was active on 2025-01-01


-- ============================================================================
-- QUERY PATTERNS SUMMARY
-- ============================================================================

/*
SCD TYPE 1 (silver_users_current):
✓ Use for: Operational queries, current state lookups
✓ Pattern: WHERE cpf = 'xxx'
✓ Result: Single row per user
✓ Performance: Fast (1 row per key)

SCD TYPE 2 (silver_users_history):
✓ Use for: Audit trails, compliance, trend analysis
✓ Pattern: WHERE cpf = 'xxx' ORDER BY __START_AT DESC
✓ Result: Multiple rows (all versions)
✓ Performance: Slower (multiple rows per key)

Current version from Type 2:
WHERE __END_AT IS NULL

Historical versions from Type 2:
WHERE __END_AT IS NOT NULL

Point-in-time from Type 2:
WHERE __START_AT <= 'date' AND (__END_AT > 'date' OR __END_AT IS NULL)
*/
