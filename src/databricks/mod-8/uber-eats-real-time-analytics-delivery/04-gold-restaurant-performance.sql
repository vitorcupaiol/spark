-- ============================================================================
-- GOLD LAYER - Restaurant Performance Metrics
-- ============================================================================
--
-- PURPOSE:
-- Aggregates order data by restaurant to provide real-time performance metrics
-- for operational dashboards and SLA monitoring.
--
-- WHAT IT DOES:
-- - Groups orders by restaurant within 10-minute time windows
-- - Calculates order volume, revenue, and customer metrics
-- - Measures delivery performance (speed, on-time rate, completion rate)
-- - Uses approximate distinct counts for streaming compatibility
-- - Provides windowed aggregations with watermarking for append mode
--
-- KEY METRICS:
-- - Volume: order_count, unique_customers, unique_drivers
-- - Revenue: total_revenue, avg_order_value
-- - Speed: avg_delivery_time, min_delivery_time, max_delivery_time
-- - Quality: delayed_orders, on_time_rate_pct, completion_rate_pct
--
-- LEARNING OBJECTIVES:
-- - Implement GROUP BY with multiple aggregation functions
-- - Use time windows for streaming aggregations
-- - Apply approx_count_distinct() for cardinality estimation
-- - Calculate percentage metrics in streaming context
-- - Configure watermarks for append mode streaming
--
-- Detailed learning notes available at the end of this file.
-- ============================================================================

CREATE OR REFRESH STREAMING LIVE TABLE gold_restaurant_performance
COMMENT "Restaurant performance metrics - per restaurant aggregations for operational dashboards"
TBLPROPERTIES (
  "quality" = "gold",
  "pipelines.autoOptimize.zOrderCols" = "restaurant_key"
)
AS
SELECT
  restaurant_key, 
  window(processed_timestamp, '10 minutes').start as window_start,
  window(processed_timestamp, '10 minutes').end as window_end,

  COUNT(*) as order_count, 
  APPROX_COUNT_DISTINCT(customer_key) as unique_customers, 
  APPROX_COUNT_DISTINCT(driver_key) as unique_drivers, 
  
  ROUND(SUM(total_amount), 2) as total_revenue,
  ROUND(AVG(total_amount), 2) as avg_order_value, 

  ROUND(AVG(delivery_time_minutes), 2) as avg_delivery_time,
  ROUND(MAX(delivery_time_minutes), 2) as max_delivery_time,
  ROUND(MIN(delivery_time_minutes), 2) as min_delivery_time, 

  SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END) as delayed_orders, 
  SUM(CASE WHEN is_delivered THEN 1 ELSE 0 END) as completed_orders,
  SUM(CASE WHEN is_delivered = false THEN 1 ELSE 0 END) as pending_orders,

  ROUND(
    (COUNT(*) - SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END)) * 100.0 / COUNT(*),
    2
  ) as on_time_rate_pct,

  ROUND(
    SUM(CASE WHEN is_delivered THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
    2
  ) as completion_rate_pct,

  CURRENT_TIMESTAMP() as last_updated

FROM STREAM(live.silver_order_status)
GROUP BY restaurant_key, window(processed_timestamp, '10 minutes');
