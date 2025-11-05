"""
SINK LAYER - Kafka Alert Publishing

PURPOSE:
This module publishes critical delivery delay alerts to external Kafka topics for consumption
by downstream systems. It implements the Databricks Lakeflow declarative pipeline pattern
for streaming sink integration.

WHAT IT DOES:
- Creates a Kafka sink endpoint for external system integration
- Formats delayed order data into Kafka-compatible key-value structure
- Enriches alerts with metadata for downstream processing
- Publishes to external Kafka topic using declarative pipeline pattern

DATA FLOW:
  silver_delayed_orders (critical events)
    -> Format as Kafka messages (key + JSON value)
    -> Add alert metadata (ID, type, severity, message)
    -> Publish to Kafka topic (delivery_alerts)
    -> Consumed by external systems

KEY FEATURES:
- Native Databricks Kafka integration using dp.create_sink()
- Automatic checkpointing and fault tolerance
- Key-based partitioning for ordered message delivery
- JSON payload serialization for interoperability
- Real-time streaming publish (no batch support)

ARCHITECTURE PATTERN:
  Step 1: CREATE SINK - Define Kafka endpoint configuration
  Step 2: FORMAT DATA - Transform to Kafka key-value schema
  Step 3: PUBLISH - Stream data to sink using append_flow

LEARNING OBJECTIVES:
- Implement Kafka sink pattern with declarative pipelines
- Design key-value message structures for Kafka
- Apply alert enrichment for downstream consumers
- Use dp.create_sink() and dp.append_flow() decorators
- Understand streaming-only sink limitations

CONFIGURATION:
- KAFKA_BOOTSTRAP_SERVERS: Kafka cluster connection string
- KAFKA_TOPIC: Target topic name (delivery_alerts)
- Optional: Unity Catalog credentials for authentication

OUTPUT MESSAGE STRUCTURE:
  Kafka Key: order_id (ensures same order goes to same partition)
  Kafka Value: JSON payload containing:
    - alert_id: Unique UUID for tracking/deduplication
    - alert_type: Classification (DELIVERY_DELAY)
    - severity: Priority level (HIGH)
    - order_id, restaurant_key, driver_key, customer_key
    - delivery_time_minutes: Quantified delay
    - current_status: Latest delivery state
    - message: Human-readable alert description
    - alert_timestamp: When delay was detected

TO-DO:
- create kafka topic "delivery_alerts"

ACCESS:
- list topics = kcat -b turntable.proxy.rlwy.net:54547 -L
- create topic = kcat -b turntable.proxy.rlwy.net:54547 -t delivery_alerts -P
- read topic = kcat -b turntable.proxy.rlwy.net:54547 -t delivery_alerts -C
"""

import dlt
from pyspark import pipelines as dp
from pyspark.sql import functions as F

KAFKA_BOOTSTRAP_SERVERS = spark.conf.get("kafka_bootstrap_servers", "turntable.proxy.rlwy.net:54547")
KAFKA_TOPIC = "delivery_alerts"

dp.create_sink(
    name="delivery_alert_events",
    format="kafka",
    options={
        "kafka.bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "topic": KAFKA_TOPIC
    }
)

@dlt.table(
    comment="Formatted alerts ready for Kafka publishing",
    table_properties={
        "quality": "sink",
        "destination": "kafka"
    }
)
def kafka_alerts():
    """
    Transform delayed delivery alerts into Kafka message format.

    This function prepares delayed orders for external Kafka publication by:
    1. Adding alert metadata (ID, type, severity, human-readable message)
    2. Creating Kafka-required key-value structure
    3. Serializing payload as JSON for interoperability

    Alert Metadata Enrichment:
        - alert_id: UUID for unique identification and deduplication
        - alert_type: Fixed value "DELIVERY_DELAY" for classification
        - severity: Fixed value "HIGH" for priority routing
        - message: Concatenated human-readable description

    Kafka Message Structure:
        Kafka requires exactly two columns: key and value (both strings)
        - key: order_id (enables partition-based ordering)
        - value: JSON-serialized struct with all alert fields

    Key Selection Strategy:
        Using order_id as Kafka key ensures:
        - Same order always routes to same partition
        - Ordered delivery for updates to same order
        - Stateful consumers can track order lifecycle
        - Load balancing across partitions by order distribution

    Value Serialization:
        F.to_json(F.struct(...)) converts multiple columns into single JSON string
        Downstream consumers parse JSON to access individual fields
        Self-describing payload requires no external schema

    Returns:
        DataFrame: Two-column DataFrame with key and value strings

    Note:
        This is a staging table. Actual Kafka publishing happens in
        publish_to_kafka() function using dp.append_flow() decorator.
    """
    return (
        dlt.read_stream("LIVE.silver_delayed_orders")
        .withColumn("alert_id", F.expr("uuid()"))
        .withColumn("alert_type", F.lit("DELIVERY_DELAY"))
        .withColumn("severity", F.lit("HIGH"))
        .withColumn("message",
            F.concat(
                F.lit("Order "),
                F.col("order_id"),
                F.lit(" is delayed ("),
                F.round(F.col("delivery_time_minutes"), 2),
                F.lit(" minutes). Driver: "),
                F.col("driver_key")
            )
        )
        .withColumn("key", F.col("order_id").cast("string"))
        .withColumn("value",
            F.to_json(F.struct(
                "alert_id",
                "order_id",
                "alert_type",
                "severity",
                "restaurant_key",
                "driver_key",
                "customer_key",
                "delivery_time_minutes",
                "current_status",
                "message",
                F.col("processed_timestamp").alias("alert_timestamp")
            ))
        )
        .select("key", "value")
    )

@dp.append_flow(
    name="publish_to_kafka",
    target="delivery_alert_events"
)
def publish_to_kafka():
    """
    Publish formatted alerts to external Kafka topic.

    This function implements the streaming sink pattern using Databricks
    declarative pipelines. The dp.append_flow() decorator connects the
    sink_kafka_alerts staging table to the delivery_alerts_sink endpoint.

    Publishing Behavior:
        - Streaming-only: No batch support (append_flow limitation)
        - Continuous: Publishes new alerts as they arrive
        - Fault-tolerant: Automatic checkpointing ensures exactly-once delivery
        - Ordered: Kafka partition ordering preserved per key

    Monitoring:
        - Check DLT event log for sink publishing metrics
        - Monitor Kafka consumer lag for downstream health
        - Track partition distribution for load balancing

    Error Handling:
        - Transient failures: Automatic retry with backpressure
        - Persistent failures: Pipeline stops to prevent data loss
        - Dead letter: Configure at Kafka consumer level

    Returns:
        DataFrame: Streaming read from sink_kafka_alerts staging table

    Note:
        This function only defines the data source for publishing.
        Actual Kafka write is handled by the dp.append_flow() decorator
        targeting the delivery_alerts_sink created earlier.
    """
    return spark.readStream.table("kafka_alerts")
