# UberEats Real-Time Analytics Pipeline

## Overview

This module implements a complete real-time analytics pipeline for UberEats delivery operations using Databricks Delta Live Tables (Lakeflow). It demonstrates the medallion architecture (Bronze, Silver, Gold, Sink) with streaming data processing, quality monitoring, and external system integration via Kafka.

## Business Context

Track UberEats delivery operations in real-time to:
- Monitor restaurant and driver performance with 10-minute windowed metrics
- Identify critically delayed deliveries (>45 min) for immediate operational intervention
- Analyze delivery speed distribution patterns across time buckets
- Provide executive dashboards with platform-wide health metrics
- Alert external systems (Kafka) when critical delays occur for PagerDuty/monitoring integration

## Architecture

```
Cloud Storage (JSON files)
    |
    v
BRONZE LAYER - Raw Data Ingestion
    |-- bronze_orders (Python Auto Loader)
    |-- bronze_status (SQL streaming)
    |
    v
SILVER LAYER - Integration & Enrichment
    |-- silver_order_status (stream-stream join with watermarking)
    |-- silver_delayed_orders (multi-criteria filtered alerts)
    |
    v
GOLD LAYER - Analytics Aggregations
    |-- gold_restaurant_performance (10-min windowed)
    |-- gold_driver_performance (cumulative)
    |-- gold_delivery_time_distribution (time bucketing)
    |-- gold_system_health (platform-wide KPIs)
    |
    v
SINK LAYER - External Integration
    |-- kafka_alerts (Kafka publishing with staging table pattern)
```

## Pipeline Components

### Bronze Layer (Ingestion)

**[01-bronze-orders.py](01-bronze-orders.py)**
- Ingests order creation events from cloud storage
- Uses Auto Loader (cloudFiles) for scalable, incremental JSON ingestion
- Applies schema hints for critical columns (order_id, timestamps)
- Adds ingestion metadata for data lineage tracking
- Handles schema evolution and malformed records automatically

**[01-bronze-status.sql](01-bronze-status.sql)**
- Ingests delivery status updates using SQL streaming syntax
- Extracts nested JSON fields from raw events
- Converts Unix timestamps to proper TIMESTAMP types
- Demonstrates SQL alternative to Python Auto Loader
- Preserves raw data with minimal transformation

### Silver Layer (Integration & Business Logic)

**[02-silver-order-status.py](02-silver-order-status.py)**
- Joins orders with status updates using stream-stream join pattern
- Implements 10-minute watermarking for late data handling
- Uses 2-hour join window for bounded memory usage
- Calculates delivery time metrics in minutes
- Classifies orders by performance (is_delayed, is_delivered flags)
- Applies data quality expectations (expect_or_drop for critical fields)
- Enriches with event severity classification

**[02-silver-delayed-orders.py](02-silver-delayed-orders.py)**
- Filters critical delay events using multi-criteria approach
- Reduces alert volume from 98.8% to ~2% of orders (focused alerts)
- **Filter Criteria:**
  1. Critical delay threshold: >45 minutes (not 15)
  2. Delivery status: Not yet delivered (still actionable)
  3. Order value: >= $20 (significant enough to matter)
  4. Sanity check: < 180 minutes (exclude data quality issues)
  5. Actionable statuses: Preparing, Out for Delivery, In Transit
- Enables focused operational monitoring without alert fatigue
- Includes total_amount for priority-based triage

### Gold Layer (Analytics Aggregations)

**[04-gold-restaurant-performance.sql](04-gold-restaurant-performance.sql)**
- Per-restaurant performance metrics with 10-minute windowed aggregations
- **Metrics:** order_count, unique_customers, unique_drivers, total_revenue, avg_order_value
- **Performance:** avg_delivery_time, min/max_delivery_time, delayed_orders, on_time_rate_pct
- Uses `approx_count_distinct()` for streaming compatibility (~98% accuracy)
- Enables restaurant SLA monitoring and operational dashboards

**[04-gold-driver-performance.sql](04-gold-driver-performance.sql)**
- Per-driver cumulative performance metrics (no time windows)
- **Metrics:** total_deliveries, restaurants_served, customers_served, total_order_value
- **Performance:** avg_delivery_time, fastest/slowest_delivery, delayed/completed deliveries
- Enables driver leaderboards, performance reviews, and incentive programs
- Approximate distinct counts for served restaurants and customers

**[04-gold-delivery-time-distribution.sql](04-gold-delivery-time-distribution.sql)**
- Delivery speed bucketing with distribution analysis
- **Time Buckets:**
  - Very Fast: < 15 minutes
  - Fast: 15-30 minutes
  - Normal: 30-45 minutes
  - Slow: > 45 minutes
- Demonstrates CASE expressions in GROUP BY for bucketing
- Provides distribution patterns for performance analysis
- Note: Removed window functions for streaming compatibility

**[04-gold-system-health.sql](04-gold-system-health.sql)**
- Platform-wide health metrics with 10-minute windowed aggregations
- **Volume KPIs:** total_orders, active_restaurants, active_drivers, active_customers
- **Revenue KPIs:** total_platform_revenue, avg_order_value
- **Performance KPIs:** avg_delivery_time, delayed_orders, on_time_rate_pct
- **Status KPIs:** completed_orders, pending_orders, completion_rate_pct
- **Alert KPIs:** critical_count, normal_count, info_count (by severity)
- Global aggregations without entity grouping for executive dashboards

### Sink Layer (External Integration)

**[03-delivery-alerts.py](03-delivery-alerts.py)** - Kafka Sink
- Publishes critical delay alerts to external Kafka topic (delivery_alerts)
- **Architecture:** Staging table pattern for observability and debugging
  - Step 1: `dp.create_sink()` defines Kafka endpoint
  - Step 2: `kafka_alerts` staging table formats messages (key-value structure)
  - Step 3: `publish_to_kafka()` streams to Kafka with exactly-once semantics
- **Message Format:**
  - Key: order_id (STRING) for partition routing
  - Value: JSON payload with alert metadata (alert_id, alert_type, severity, etc.)
- **Benefits:** SQL-queryable staging table for debugging without Kafka consumers
- **Target:** Railway.app hosted Kafka (turntable.proxy.rlwy.net:54547)
- Enables integration with PagerDuty, monitoring dashboards, notification services

**[03-external-delta-sink.py](03-external-delta-sink.py)** - External Delta Table Sink
- Publishes critical delay alerts to external (unmanaged) Delta Lake table
- **Architecture:** Direct write to cloud storage location you control
  - Uses `path` parameter in `@dlt.table()` to specify storage location
  - Data persists independently from table metadata
  - Enables cross-workspace access without data replication
- **Key Difference from Managed Tables:**
  - Managed: `DROP TABLE` deletes both metadata AND data files
  - External: `DROP TABLE` deletes only metadata, data files remain in storage
- **Use Cases:**
  - Cross-workspace data sharing (single source of truth)
  - Long-term archival independent of table lifecycle
  - Multi-tool access (Athena, Synapse, BigQuery can read Delta files)
  - Disaster recovery (data survives workspace deletion)
- **Storage Paths:** Supports S3, ADLS Gen2, GCS
- **Benefits:** Full control over data lifecycle, storage costs, and retention policies

## File Organization

```
📁 uber-eats-real-time-analytics-delivery/
├── 01-bronze-orders.py                    # Bronze: Order ingestion (Python)
├── 01-bronze-status.sql                   # Bronze: Status ingestion (SQL)
├── 02-silver-order-status.py              # Silver: Stream-stream join + enrichment
├── 02-silver-delayed-orders.py            # Silver: Multi-criteria alert filtering
├── 03-delivery-alerts.py                  # Sink: Kafka publishing (staging pattern)
├── 03-external-delta-sink.py              # Sink: External Delta table (unmanaged)
├── 04-gold-restaurant-performance.sql     # Gold: Restaurant metrics (windowed)
├── 04-gold-driver-performance.sql         # Gold: Driver metrics (cumulative)
├── 04-gold-delivery-time-distribution.sql # Gold: Time bucket distribution
├── 04-gold-system-health.sql              # Gold: Platform-wide KPIs (windowed)
└── readme.md                              # This file
```

## Key Learning Topics

### 1. Streaming Patterns
- **Auto Loader** for cloud file ingestion with schema inference
- **Stream-stream joins** with watermarking (10-min watermark, 2-hour window)
- **Time-bounded join windows** for bounded memory usage
- **Streaming aggregations** with time windows (10-minute tumbling windows)
- **Staging table pattern** for external sink observability

### 2. Data Quality
- **Delta Live Tables expectations** (`expect_or_drop` for critical fields)
- **Schema inference and evolution** with Auto Loader
- **Metadata tracking** for data lineage (_rescued_data columns)
- **Multi-criteria filtering** to reduce alert fatigue (98.8% → 2%)

### 3. Streaming Limitations & Solutions
- **COUNT(DISTINCT)** not supported → use `approx_count_distinct()` (~98% accuracy)
- **Window functions (OVER)** not supported → remove or create batch views
- **Watermarks required** for append mode aggregations → use `window(timestamp, interval)`
- **Unbounded state** issue → use time-bounded join windows

### 4. Aggregation Patterns
- **Single-dimension GROUP BY** (restaurant_key, driver_key)
- **CASE expression bucketing** (time ranges for distribution)
- **Global aggregations** with time windows (system health)
- **Approximate distinct counts** for cardinality estimation (HyperLogLog)
- **Windowed vs cumulative** aggregations (10-min windows vs all-time)

### 5. External Integration
- **Kafka sink** using declarative pipelines (`dp.create_sink()`, `@dp.append_flow()`)
- **Key-value message formatting** for Kafka partition routing
- **JSON serialization** with `F.to_json(F.struct(...))` for interoperability
- **Staging table pattern** for SQL-based debugging and observability
- **External Delta tables** with `path` parameter for unmanaged storage
- **Managed vs External tables** lifecycle and use case differences
- **Cross-workspace data sharing** via external storage paths
- **Alert enrichment** for downstream consumer context

## Streaming Compatibility Notes

### Why approx_count_distinct()?
Exact distinct counts require unbounded memory in streaming (must remember all unique values forever). HyperLogLog provides ~98% accuracy with fixed memory (~1KB per metric).

**When to use:**
- Analytics dashboards (2% error acceptable)
- Performance metrics and trends
- Approximate reporting for operational decisions

**When NOT to use:**
- Financial reconciliation (exact cents matter)
- Audit trails (regulatory compliance)
- Compliance reporting requiring exact counts

### Why no window functions (OVER clause)?
Window functions require the complete dataset, which conflicts with streaming's incremental processing model. Each micro-batch only sees a subset of data.

**Solution:**
- Remove window functions from streaming tables
- Create batch views on top for percentage calculations
- Use cumulative aggregations instead of windowed rankings

### Why watermarks?
Watermarks tell Spark how long to wait for late data before finalizing aggregations. Without watermarks, Spark doesn't know when windows are "complete", preventing append mode output.

**Example:**
```sql
window(processed_timestamp, '10 minutes')
```
This creates 10-minute tumbling windows and automatically applies watermarking.

### Why staging tables for Kafka?
The staging table pattern (kafka_alerts) provides SQL-queryable interface to inspect messages before/after publishing. This enables debugging without Kafka consumer tools.

**Trade-offs:**
- ✅ Observability: Query messages via SQL
- ✅ Debugging: Inspect format without Kafka consumers
- ⚠️ Latency: Additional table write (~5-50ms)
- ⚠️ Storage: Staging table persistence costs

### Managed vs External Delta Tables
Understanding the difference is critical for data lifecycle management:

**Managed Tables (Default):**
```sql
CREATE TABLE managed_alerts (...)
-- Databricks controls both metadata AND data files
-- Location: /user/hive/warehouse/managed_alerts/

DROP TABLE managed_alerts
-- ⚠️ DELETES EVERYTHING (metadata + data files)
```

**External Tables (This Pattern):**
```python
@dlt.table(path="s3://bucket/alerts/")
def external_delayed_alerts(): ...
-- You control storage location
-- Databricks only manages metadata

DROP TABLE external_delayed_alerts
-- ✅ Only metadata deleted, data files REMAIN in S3
```

**When to use External Tables:**
- ✅ Cross-workspace sharing (multiple workspaces read same storage path)
- ✅ Long-term archival (data must survive >7 years for compliance)
- ✅ Multi-tool access (Athena, Synapse, BigQuery reading Delta files)
- ✅ Disaster recovery (data survives workspace deletion)
- ✅ Existing data lakes (register external folders without migration)

**When to use Managed Tables:**
- ✅ Simple use cases (no cross-workspace needs)
- ✅ Unity Catalog governance (centralized access control)
- ✅ Automatic cleanup (data lifecycle tied to table lifecycle)
- ✅ No external storage management overhead

## File Naming Convention

```
[layer-number]-[layer-name]-[entity].{py|sql}

Examples:
01-bronze-orders.py                    → Bronze layer, orders ingestion
02-silver-order-status.py              → Silver layer, order-status integration
03-delivery-alerts.py                  → Sink layer, Kafka alert publishing
04-gold-driver-performance.sql         → Gold layer, driver metrics
```

**Layer Numbering:**
- `01` = Bronze (Raw ingestion)
- `02` = Silver (Integration & business logic)
- `03` = Sink (External publishing)
- `04` = Gold (Analytics aggregations)

## Quick Start

### For Students: Understanding Continuous Mode

**This pipeline is designed to run CONTINUOUSLY** (not in batch mode) to enable you to observe streaming concepts in real-time:

- **Watermarking**: Watch how the 10-minute watermark handles late-arriving events
- **Join State**: Observe how the 2-hour window manages memory by evicting old orders
- **Streaming Aggregations**: See metrics update continuously until windows close
- **Approximate Functions**: Compare `approx_count_distinct()` accuracy vs exact counts
- **Kafka Publishing**: Monitor staging table and verify messages in Kafka topic

### Learning Path

1. **Review the architecture diagram** above to understand data flow
2. **Understand the medallion architecture:**
   - Bronze = Raw data (minimal transformation)
   - Silver = Integrated data (joins, enrichment, filtering)
   - Gold = Aggregated data (analytics metrics)
   - Sink = External publishing (Kafka integration)
3. **Start with Bronze layer** to see Auto Loader patterns (Python vs SQL)
4. **Study Silver layer** for stream-stream joins and multi-criteria filtering
5. **Explore Gold layer** for different aggregation patterns (windowed vs cumulative)
6. **Check Sink layer** for Kafka publishing with staging table pattern
7. **Run observation queries** while pipeline is running to see streaming behavior

## Common Queries

### Check restaurant performance (last hour)
```sql
SELECT restaurant_key,
       window_start,
       order_count,
       total_revenue,
       avg_delivery_time,
       on_time_rate_pct
FROM gold_restaurant_performance
WHERE window_start >= CURRENT_TIMESTAMP() - INTERVAL 1 HOUR
ORDER BY on_time_rate_pct DESC, total_revenue DESC
LIMIT 10;
```

### Find top performing drivers
```sql
SELECT driver_key,
       total_deliveries,
       avg_delivery_time,
       on_time_rate_pct,
       restaurants_served,
       customers_served
FROM gold_driver_performance
ORDER BY on_time_rate_pct DESC, total_deliveries DESC
LIMIT 10;
```

### Analyze delivery speed distribution
```sql
SELECT time_bucket,
       order_count,
       avg_delivery_time,
       min_delivery_time,
       max_delivery_time
FROM gold_delivery_time_distribution
ORDER BY
  CASE time_bucket
    WHEN 'Very Fast (<15min)' THEN 1
    WHEN 'Fast (15-30min)' THEN 2
    WHEN 'Normal (30-45min)' THEN 3
    WHEN 'Slow (>45min)' THEN 4
  END;
```

### Monitor overall system health (last hour)
```sql
SELECT window_start,
       total_orders,
       active_restaurants,
       active_drivers,
       active_customers,
       on_time_rate_pct,
       completion_rate_pct,
       avg_delivery_time,
       delayed_orders
FROM gold_system_health
WHERE window_start >= CURRENT_TIMESTAMP() - INTERVAL 1 HOUR
ORDER BY window_start DESC;
```

### Inspect Kafka staging table (debugging)
```sql
-- View recent alerts formatted for Kafka
SELECT key as order_id,
       JSON_EXTRACT_SCALAR(value, '$.alert_type') as alert_type,
       JSON_EXTRACT_SCALAR(value, '$.severity') as severity,
       JSON_EXTRACT_SCALAR(value, '$.message') as message
FROM kafka_alerts
LIMIT 10;

-- Count alerts by type
SELECT JSON_EXTRACT_SCALAR(value, '$.alert_type') as alert_type,
       JSON_EXTRACT_SCALAR(value, '$.severity') as severity,
       COUNT(*) as alert_count
FROM kafka_alerts
GROUP BY JSON_EXTRACT_SCALAR(value, '$.alert_type'),
         JSON_EXTRACT_SCALAR(value, '$.severity');
```

### Monitor critically delayed orders
```sql
SELECT order_id,
       restaurant_key,
       driver_key,
       delivery_time_minutes,
       total_amount,
       current_status,
       processed_timestamp
FROM silver_delayed_orders
ORDER BY delivery_time_minutes DESC
LIMIT 20;
```

### Query external Delta table (cross-workspace access)
```sql
-- Direct path-based query (no table registration needed)
SELECT alert_id,
       alert_type,
       severity,
       alert_message,
       delivery_time_minutes,
       total_amount,
       sink_timestamp
FROM delta.`s3://your-bucket-name/ubereats-delayed-alerts/`
ORDER BY sink_timestamp DESC
LIMIT 10;

-- Or create table reference in another workspace
CREATE TABLE workspace_b.external_alerts
USING DELTA
LOCATION 's3://your-bucket-name/ubereats-delayed-alerts/';

-- Then query normally
SELECT * FROM workspace_b.external_alerts
WHERE delivery_time_minutes > 60
ORDER BY total_amount DESC;
```

### Verify external table persistence
```python
# List files in external storage location
display(dbutils.fs.ls("s3://your-bucket-name/ubereats-delayed-alerts/"))

# Expected output:
# - _delta_log/ (transaction log directory)
# - part-00000-*.snappy.parquet (data files)
# - _checkpoint/ (streaming checkpoint)

# Test persistence: drop table and verify data remains
spark.sql("DROP TABLE IF EXISTS external_delayed_alerts")

# Files still exist (verify)
display(dbutils.fs.ls("s3://your-bucket-name/ubereats-delayed-alerts/"))

# Restore access by recreating table metadata
spark.sql("""
  CREATE TABLE external_delayed_alerts
  USING DELTA
  LOCATION 's3://your-bucket-name/ubereats-delayed-alerts/'
""")

# All data immediately accessible (no reload needed)
display(spark.sql("SELECT COUNT(*) FROM external_delayed_alerts"))
```

## Troubleshooting

### Error: "Distinct aggregations are not supported"
**Problem:** Using `COUNT(DISTINCT column)` in streaming query
**Solution:** Replace with `approx_count_distinct(column)`
**Example:**
```sql
-- ❌ Wrong (not supported)
SELECT COUNT(DISTINCT customer_key) FROM orders

-- ✅ Correct (streaming compatible)
SELECT approx_count_distinct(customer_key) FROM orders
```

### Error: "Window function is not supported"
**Problem:** Using `OVER()` clause in streaming query
**Solution:** Remove window function or create separate batch view
**Example:**
```sql
-- ❌ Wrong (not supported)
SELECT order_id,
       COUNT(*) OVER(PARTITION BY restaurant_key)
FROM orders

-- ✅ Correct (use aggregation instead)
SELECT restaurant_key, COUNT(*) as order_count
FROM orders
GROUP BY restaurant_key
```

### Error: "append mode...without watermark"
**Problem:** No watermark defined for streaming aggregation
**Solution:** Add `window(timestamp_column, interval)` to enable watermarking
**Example:**
```sql
-- ❌ Wrong (no watermark)
SELECT restaurant_key, COUNT(*)
FROM orders
GROUP BY restaurant_key

-- ✅ Correct (windowed with watermark)
SELECT window(processed_timestamp, '10 minutes') as time_window,
       restaurant_key,
       COUNT(*) as order_count
FROM orders
GROUP BY window(processed_timestamp, '10 minutes'), restaurant_key
```

### Kafka alerts not appearing in topic
**Problem:** Messages formatted but not reaching Kafka
**Debugging Steps:**
1. Check staging table has data: `SELECT COUNT(*) FROM kafka_alerts`
2. Check DLT event log: `SELECT * FROM event_log WHERE target = 'delivery_alert_events'`
3. Verify Kafka connectivity: `kcat -b turntable.proxy.rlwy.net:54547 -L`
4. Inspect message format: `SELECT key, value FROM kafka_alerts LIMIT 1`

## Pipeline Configuration

### Kafka Connection (Railway.app)
- **Bootstrap Server:** `turntable.proxy.rlwy.net:54547`
- **Topic:** `delivery_alerts`
- **Authentication:** None (development setup)

**Testing Kafka:**
```bash
# List topics
kcat -b turntable.proxy.rlwy.net:54547 -L

# Consume messages
kcat -b turntable.proxy.rlwy.net:54547 -t delivery_alerts -C

# Consume with formatting
kcat -b turntable.proxy.rlwy.net:54547 -t delivery_alerts -C \
     -f 'Key: %k\nValue: %s\n---\n'
```

### Critical Thresholds
- **Delay threshold:** 45 minutes (changed from 15 to reduce alert fatigue)
- **Min order value:** $20 (focus on significant orders)
- **Max reasonable delay:** 180 minutes (exclude data quality issues)
- **Watermark:** 10 minutes (for late-arriving events)
- **Join window:** 2 hours (for bounded memory usage)

## Support

For questions or issues:
1. Check file-level documentation (top comments in each file)
2. Review detailed docstrings in Python files
3. Consult inline comments in SQL files
4. Check Databricks Delta Live Tables documentation
5. Review this README for architectural patterns

## Production Considerations

Before deploying to production:
- [ ] Add authentication to Kafka cluster (SASL/OAuth)
- [ ] Configure Unity Catalog for table governance
- [ ] Set up retention policies for staging tables
- [ ] Implement dead letter queue for failed Kafka publishes
- [ ] Configure alerting on pipeline failures
- [ ] Monitor consumer lag on Kafka topics
- [ ] Document consumer contracts for downstream teams
- [ ] Test with realistic data volumes
- [ ] Validate approximate distinct count accuracy
- [ ] Set up SLA monitoring dashboards
