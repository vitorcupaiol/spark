"""
SILVER LAYER - Order Status Integration and Enrichment

PURPOSE:
This module joins order creation events with delivery status updates to create a unified view
of the order lifecycle. It implements stream-stream joins with watermarking for reliable
real-time data integration.

WHAT IT DOES:
- Joins order events with status update events using watermarked streaming
- Calculates delivery time metrics from event timestamps
- Classifies orders by delivery performance (on-time vs delayed)
- Applies data quality expectations to ensure clean downstream data
- Enriches orders with severity levels for alerting systems

DATA FLOW:
  bronze_orders (streaming) + bronze_status (streaming)
    -> Watermarked Stream-Stream Join
    -> Business Logic Enrichment
    -> Data Quality Validation
    -> silver_order_status (curated table)

KEY FEATURES:
- Stream-stream join: Correlates orders with their status updates in real-time
- Watermarking (10 minutes): Waits up to 10 minutes for late events before closing windows
- Time-bounded join (2 hours): Matches status updates arriving within 2 hours of order
- Metric calculation: Computes delivery time in minutes
- Quality gates: Validates critical business rules before persisting data

WHY THREE TIME MECHANISMS?
1. Watermark (10 min): Controls HOW LONG to wait for late data
2. Join Window (2 hours): Controls HOW LONG orders stay in memory
3. Window Function (10 min): Controls TIME BUCKETS for aggregations (used in Gold layer)

These work together to enable scalable, fault-tolerant streaming joins.

BUSINESS RULES:
- Delayed threshold: Orders taking more than 15 minutes are flagged
- Join window: Status updates must arrive within 2 hours of order
- Watermark: Tolerate up to 10 minutes of late data arrival

LEARNING OBJECTIVES:
- Implement stream-stream joins with watermarking
- Apply time-bounded join conditions for event correlation
- Calculate derived metrics from temporal data
- Use Delta Live Tables expectations for data quality
- Design severity classification for operational alerting

CONFIGURATION:
- WATERMARK_DURATION: Maximum lateness tolerance (10 minutes)
- DELAY_THRESHOLD_MINUTES: Threshold for delayed order classification (15 minutes)

OUTPUT SCHEMA:
- order_id: Unique order identifier
- order_date: When order was created (business time)
- restaurant_key, driver_key, customer_key: Entity identifiers
- total_amount: Order monetary value
- order_event_time: When order event arrived in Databricks (event time)
- current_status: Latest delivery status (Pending, In Transit, Delivered)
- status_update_time: When status was last updated (business time)
- status_event_time: When status event arrived in Databricks (event time)
- delivery_time_minutes: Event-time lag (status_event_time - order_event_time)
- is_delayed: Boolean flag for delayed orders
- is_delivered: Boolean flag for completed orders
- event_severity: Classification (CRITICAL, INFO, NORMAL)
- processed_timestamp: When enrichment occurred
"""

import dlt
from pyspark.sql import functions as F

WATERMARK_DURATION = "10 minutes"
DELAY_THRESHOLD_MINUTES = 15

@dlt.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
@dlt.expect_or_drop("valid_order_date", "order_date IS NOT NULL")
@dlt.expect_or_drop("valid_total_amount", "total_amount > 0")
@dlt.table(
    name="silver_order_status",
    comment="Orders enriched with delivery status - tracks order lifecycle",
    table_properties={
        "quality": "silver",
        "layer": "integration"
    }
)
def silver_order_status():
    """
    Join order creation events with status updates to track order lifecycle.

    This function implements a time-bounded stream-stream join between orders
    and their status updates. It uses watermarking to handle late-arriving data
    and applies business logic to classify order performance.

    Join Strategy:
        - Join Type: LEFT JOIN (preserves all orders, even without status)
        - Join Key: order_id from orders matches order_identifier from status
        - Time Condition: Status must arrive within 2 hours of order creation
        - Watermark: Both streams tolerate 10 minutes of lateness

    Watermarking Explained:
        Watermark defines how long Spark waits for late data before closing a window.
        Example: Order at 10:00 AM with 10-minute watermark
                 - Accepts status updates until 10:10 AM
                 - After 10:10 AM, late updates are dropped
                 - Prevents unbounded state growth in streaming joins

    Time-Bounded Join Window:
        Status updates must arrive within 2 hours of order creation.
        This prevents matching unrelated events with same order_id and
        keeps join state manageable by allowing Spark to evict old records.

    Derived Metrics:
        - delivery_time_minutes: Event-time lag between order and status arrival
          IMPORTANT: Uses dt_current_timestamp (event time) not order_date (business time)
          Rationale: Measures observable system lag, consistent with watermark semantics
          Example: Order event arrives at 12:00, status arrives at 12:15 = 15 minutes

          Implementation Note:
          Uses direct timestamp arithmetic (.cast("long")) instead of unix_timestamp()
          because dt_current_timestamp is already TIMESTAMP type:
          - cast("long") converts TIMESTAMP to seconds since epoch
          - Direct arithmetic is more efficient than function calls
          - abs() handles any out-of-order events (rare with watermark)
        - is_delayed: True if delivery_time > 15 minutes
        - is_delivered: True if status equals "Delivered"
        - event_severity: CRITICAL (delayed), INFO (delivered), NORMAL (other)

    Event-Time vs Business-Time:
        WHY use dt_current_timestamp instead of order_date?

        Business Time (order_date, status_timestamp):
        - Represents when events happened in the real world
        - Subject to clock skew between systems
        - May have historical data with old timestamps
        - Not aligned with streaming watermark logic

        Event Time (dt_current_timestamp):
        - Represents when events arrived in Databricks
        - Consistent across all sources (system clock)
        - Matches watermark semantics (both streams use dt_current_timestamp)
        - Measures actual observable processing lag

        Result: Realistic delivery times (minutes), not astronomical values (days)

    Data Quality Expectations:
        Three critical validations ensure data integrity:
        1. order_id must not be null (business key requirement)
        2. order_date must not be null (needed for time calculations)
        3. total_amount must be positive (prevents invalid transactions)

        Violations are DROPPED (expect_or_drop), not failed, to prevent
        pipeline stoppage while maintaining data quality.

    Returns:
        DataFrame: Streaming DataFrame with enriched order status information

    Streaming Characteristics:
        - Input: Two streaming sources (orders and status)
        - Output: Streaming table with append mode
        - State: Bounded by watermark and join window
        - Latency: Near real-time (watermark + processing time)
    """

    orders_df = (
        dlt.read_stream("live.bronze_orders")
        .withColumn("dt_current_timestamp", F.col("dt_current_timestamp").cast("timestamp"))
        .withWatermark("dt_current_timestamp", WATERMARK_DURATION)
    )

    status_df = (
        dlt.read_stream("live.bronze_status")
        .withColumn("dt_current_timestamp", F.col("dt_current_timestamp").cast("timestamp"))
        .withWatermark("dt_current_timestamp", WATERMARK_DURATION)
    )

    return (
        orders_df
        .join(
            status_df,
            (orders_df.order_id == status_df.order_identifier) &
            (status_df.dt_current_timestamp >= orders_df.dt_current_timestamp) &
            (status_df.dt_current_timestamp <= orders_df.dt_current_timestamp + F.expr("INTERVAL 2 HOURS")),
            "left"
        )
        .select(
            orders_df.order_id,
            orders_df.order_date,
            orders_df.restaurant_key,
            orders_df.driver_key,
            orders_df.user_key.alias("customer_key"),
            orders_df.total_amount,
            orders_df.dt_current_timestamp.alias("order_event_time"),
            status_df.status_name.alias("current_status"),
            status_df.status_timestamp.alias("status_update_time"),
            status_df.dt_current_timestamp.alias("status_event_time")
        )
        .withColumn("delivery_time_minutes",
            F.when(
                F.col("status_event_time").isNotNull(),
                F.abs((F.col("status_event_time").cast("long") - F.col("order_event_time").cast("long"))) / 60
            )
        )
        .withColumn("is_delayed",
            F.when(F.col("delivery_time_minutes") > DELAY_THRESHOLD_MINUTES, True).otherwise(False)
        )
        .withColumn("is_delivered",
            F.col("current_status") == "Delivered"
        )
        .withColumn("event_severity",
            F.when(F.col("is_delayed"), "CRITICAL")
            .when(F.col("is_delivered"), "INFO")
            .otherwise("NORMAL")
        )
        .withColumn("processed_timestamp", F.current_timestamp())
    )
