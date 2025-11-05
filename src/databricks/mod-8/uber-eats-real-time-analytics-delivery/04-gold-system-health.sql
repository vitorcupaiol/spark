-- ============================================================================
-- GOLD LAYER - Platform-Wide System Health Metrics
-- ============================================================================
--
-- PURPOSE:
-- Provides platform-wide aggregated metrics for executive dashboards and
-- overall system health monitoring across all restaurants, drivers, and customers.
--
-- WHAT IT DOES:
-- - Aggregates all orders within 10-minute time windows
-- - Calculates platform-wide KPIs (no grouping by entity)
-- - Tracks active participants (restaurants, drivers, customers)
-- - Monitors overall system performance and health indicators
--
-- KEY METRICS:
-- - Volume: total_orders, active_restaurants, active_drivers, active_customers
-- - Revenue: total_platform_revenue, avg_order_value
-- - Performance: avg_delivery_time, delayed_orders, on_time_rate_pct
-- - Status: completed_orders, pending_orders, completion_rate_pct
-- - Alerts: critical_count, normal_count, info_count (by severity)
--
-- LEARNING OBJECTIVES:
-- - Implement global aggregations (no GROUP BY dimension)
-- - Use time windows for system-wide metrics
-- - Apply approx_count_distinct() for platform cardinality
-- - Calculate system health indicators and SLA metrics
--
-- USE CASE:
-- Executive dashboard showing platform health at a glance.
-- No drill-down by restaurant/driver - just overall system status.
--
-- Detailed learning notes available at the end of this file.
-- ============================================================================

CREATE OR REFRESH STREAMING LIVE TABLE gold_system_health
COMMENT "Platform-wide health metrics - executive dashboard showing overall system performance"
TBLPROPERTIES (
  "quality" = "gold",
  "pipelines.autoOptimize.managed" = "true"
)
AS
SELECT
  window(processed_timestamp, '10 minutes').start as window_start,
  window(processed_timestamp, '10 minutes').end as window_end,

  COUNT(*) as total_orders,
  APPROX_COUNT_DISTINCT(restaurant_key) as active_restaurants,
  APPROX_COUNT_DISTINCT(driver_key) as active_drivers,
  APPROX_COUNT_DISTINCT(customer_key) as active_customers,

  ROUND(SUM(total_amount), 2) as total_platform_revenue,
  ROUND(AVG(total_amount), 2) as avg_order_value,
  ROUND(MIN(total_amount), 2) as min_order_value,
  ROUND(MAX(total_amount), 2) as max_order_value,

  ROUND(AVG(delivery_time_minutes), 2) as avg_delivery_time,
  ROUND(MAX(delivery_time_minutes), 2) as max_delivery_time,
  ROUND(MIN(delivery_time_minutes), 2) as min_delivery_time,

  SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END) as delayed_orders,
  SUM(CASE WHEN is_delivered THEN 1 ELSE 0 END) as completed_orders,
  SUM(CASE WHEN is_delivered = false THEN 1 ELSE 0 END) as pending_orders,

  ROUND(
    (COUNT(*) - SUM(CASE WHEN is_delayed THEN 1 ELSE 0 END)) * 100.0 / COUNT(*),
    2
  ) as platform_health_score,

  CURRENT_TIMESTAMP() as last_updated

FROM STREAM(live.silver_order_status)
GROUP BY window(processed_timestamp, '10 minutes');
