-- ============================================================================
-- GOLD LAYER - Delivery Time Distribution Analysis
-- ============================================================================
--
-- PURPOSE:
-- Analyzes delivery speed by categorizing orders into time buckets to identify
-- performance patterns and distribution trends.
--
-- WHAT IT DOES:
-- - Groups orders into delivery time categories (Very Fast, Fast, Normal, Slow)
-- - Counts orders and calculates metrics within each time bucket
-- - Provides distribution analysis for performance monitoring
-- - Demonstrates bucketing continuous values for better insights
--
-- TIME BUCKETS:
-- - Very Fast: < 15 minutes
-- - Fast: 15-30 minutes
-- - Normal: 30-45 minutes
-- - Slow: > 45 minutes
--
-- KEY METRICS:
-- - order_count: Number of orders in each bucket
-- - avg_order_value, total_revenue: Revenue analysis by speed
-- - avg_delivery_time: Average within each bucket
-- - min_delivery_time, max_delivery_time: Range within bucket
--
-- LEARNING OBJECTIVES:
-- - Use CASE expressions in GROUP BY for dynamic categorization
-- - Bucket continuous values into meaningful categories
-- - Understand streaming aggregation limitations
-- - Note: Percentage calculations removed (see *-with-pct.sql for batch alternative)
--
-- IMPORTANT:
-- Window functions (OVER clause) are not supported in streaming.
-- For percentage calculations, use the companion batch view file.
--
-- Detailed learning notes available at the end of this file.
-- ============================================================================

CREATE OR REFRESH STREAMING LIVE TABLE gold_delivery_time_distribution
COMMENT "Orders grouped by delivery speed buckets - helps identify performance patterns"
AS
SELECT
  CASE
    WHEN delivery_time_minutes < 15 THEN 'Very Fast (<15min)'
    WHEN delivery_time_minutes < 30 THEN 'Fast (15-30min)'
    WHEN delivery_time_minutes < 45 THEN 'Normal (30-45min)'
    ELSE 'Slow (>45min)'
  END as time_bucket,

  COUNT(*) as order_count, 

  ROUND(AVG(total_amount), 2) as avg_order_value, 
  ROUND(SUM(total_amount), 2) as total_revenue,  
  ROUND(MIN(total_amount), 2) as min_order_value,
  ROUND(MAX(total_amount), 2) as max_order_value,

  ROUND(AVG(delivery_time_minutes), 2) as avg_delivery_time, 
  ROUND(MIN(delivery_time_minutes), 2) as min_delivery_time,
  ROUND(MAX(delivery_time_minutes), 2) as max_delivery_time,

  CURRENT_TIMESTAMP() as last_updated

FROM STREAM(LIVE.silver_order_status)
WHERE delivery_time_minutes IS NOT NULL
GROUP BY
  CASE
    WHEN delivery_time_minutes < 15 THEN 'Very Fast (<15min)'
    WHEN delivery_time_minutes < 30 THEN 'Fast (15-30min)'
    WHEN delivery_time_minutes < 45 THEN 'Normal (30-45min)'
    ELSE 'Slow (>45min)'
  END;
