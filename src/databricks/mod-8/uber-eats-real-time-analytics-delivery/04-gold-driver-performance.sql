-- ============================================================================
-- GOLD LAYER - Driver Performance Metrics
-- ============================================================================
--
-- PURPOSE:
-- Aggregates delivery data by driver to provide performance metrics for
-- driver management, leaderboards, and operational monitoring.
--
-- WHAT IT DOES:
-- - Groups all orders by driver to track individual performance
-- - Calculates delivery volume, speed, and quality metrics per driver
-- - Uses approximate distinct counts for streaming compatibility
-- - Enables driver leaderboards and performance reviews
--
-- KEY METRICS:
-- - Volume: total_deliveries, restaurants_served, customers_served
-- - Revenue: total_order_value, avg_order_value
-- - Speed: avg_delivery_time, fastest_delivery, slowest_delivery
-- - Quality: delayed_deliveries, completed_deliveries, on_time_rate_pct
--
-- LEARNING OBJECTIVES:
-- - Apply GROUP BY pattern to different dimensions (driver vs restaurant)
-- - Use approx_count_distinct() for streaming cardinality estimation
-- - Calculate performance percentages (on-time rate)
-- - Design metrics for operational leaderboards
--
-- IMPORTANT:
-- Uses approx_count_distinct() instead of COUNT(DISTINCT) for streaming compatibility.
-- Approximate counts have ~2% error rate, which is acceptable for analytics.
--
-- Detailed learning notes available at the end of this file.
-- ============================================================================

CREATE OR REFRESH STREAMING LIVE TABLE gold_driver_performance
COMMENT "Driver performance metrics - leaderboard and monitoring for driver management"
AS
SELECT
  driver_key,

  COUNT(*) as total_deliveries, 
  approx_count_distinct(restaurant_key) as restaurants_served, 
  approx_count_distinct(customer_key) as customers_served,  

  ROUND(SUM(total_amount), 2) as total_order_value,  
  ROUND(AVG(total_amount), 2) as avg_order_value,  

  ROUND(AVG(delivery_time_minutes), 2) as avg_delivery_time,
  ROUND(MIN(delivery_time_minutes), 2) as fastest_delivery, 
  ROUND(MAX(delivery_time_minutes), 2) as slowest_delivery, 

  SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END) as delayed_deliveries, 
  SUM(CASE WHEN is_delivered THEN 1 ELSE 0 END) as completed_deliveries, 

  ROUND(
    (COUNT(*) - SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END)) * 100.0 / COUNT(*),
    2
  ) as on_time_rate_pct,

  CURRENT_TIMESTAMP() as last_updated

FROM STREAM(LIVE.silver_order_status)
GROUP BY driver_key;
