-- ============================================================================
-- GOLD LAYER - User Analytics (Simplified)
-- ============================================================================
--
-- PURPOSE:
-- Single Gold table demonstrating Auto CDC consumption.
-- Focuses on current user demographics for operational analytics.
--
-- WHAT IT DOES:
-- - Aggregates current user state by city
-- - Calculates profile completeness metrics
-- - Simple business insights for dashboards

-- ============================================================================
-- User Demographics by City
-- ============================================================================

CREATE OR REFRESH LIVE TABLE gold_user_demographics
COMMENT 'User demographics aggregated by city - operational analytics'
TBLPROPERTIES (
  'quality' = 'gold',
  'use_case' = 'operational_analytics'
)
AS
SELECT
  city,
  COUNT(DISTINCT cpf) AS total_users,
  COUNT(DISTINCT CASE WHEN email IS NOT NULL THEN cpf END) AS users_with_email,
  COUNT(DISTINCT CASE WHEN delivery_address IS NOT NULL THEN cpf END) AS users_with_address,
  COUNT(DISTINCT CASE WHEN first_name IS NOT NULL THEN cpf END) AS users_with_profile
FROM LIVE.silver_users_current
GROUP BY city;
