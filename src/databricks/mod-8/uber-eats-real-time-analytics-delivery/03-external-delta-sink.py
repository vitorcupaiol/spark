"""
SINK LAYER - External Delta Lake Table (Unmanaged)

═══════════════════════════════════════════════════════════════════════════════
PURPOSE
═══════════════════════════════════════════════════════════════════════════════
This module demonstrates publishing critical delivery alerts to an EXTERNAL Delta
Lake table in cloud storage. Unlike managed tables, external tables persist data
independently of the table metadata, enabling cross-workspace access and long-term
archival with full lifecycle control.

Key Capabilities:
  • Publishes alerts to external cloud storage location (S3, ADLS, GCS)
  • Creates unmanaged Delta table (data survives table DROP)
  • Enables cross-workspace data sharing without replication
  • Provides full control over data lifecycle and retention
  • Demonstrates streaming writes to external storage with checkpointing

═══════════════════════════════════════════════════════════════════════════════
DATA FLOW
═══════════════════════════════════════════════════════════════════════════════
  silver_delayed_orders (critical events)
    ↓
  external_delayed_alerts (external Delta table)
    • Write to cloud storage path (e.g., s3://bucket/alerts/)
    • Maintain Delta transaction log independently
    • Enable cross-workspace queries via path-based access
    ↓
  Cloud Storage (S3/ADLS/GCS)
    • Persisted Delta files in your storage account
    • Accessible from any Databricks workspace
    • Can be read by external tools (Athena, Synapse, BigQuery)

═══════════════════════════════════════════════════════════════════════════════
MANAGED vs EXTERNAL TABLES - Key Differences
═══════════════════════════════════════════════════════════════════════════════

MANAGED TABLE (Default Databricks Behavior):
  ✅ Simple to use (no path management)
  ✅ Automatic cleanup when table dropped
  ✅ Unity Catalog integration
  ⚠️  Data files deleted with DROP TABLE
  ⚠️  Limited cross-workspace sharing
  ⚠️  Databricks controls storage location

  Example:
    CREATE TABLE managed_alerts (...)
    -- Data stored in: /user/hive/warehouse/managed_alerts/
    DROP TABLE managed_alerts  -- ⚠️ DELETES ALL DATA FILES

EXTERNAL TABLE (This Pattern):
  ✅ Data persists after DROP TABLE
  ✅ Cross-workspace sharing via storage path
  ✅ External tool access (Athena, Synapse, etc.)
  ✅ Full lifecycle control (retention, archival)
  ⚠️  Manual path management required
  ⚠️  Need storage permissions configuration

  Example:
    CREATE TABLE external_alerts (...)
    LOCATION 's3://my-bucket/alerts/'
    -- Data stored in: s3://my-bucket/alerts/
    DROP TABLE external_alerts  -- ✅ Data files still exist in S3

═══════════════════════════════════════════════════════════════════════════════
USE CASES FOR EXTERNAL TABLES
═══════════════════════════════════════════════════════════════════════════════

1. Cross-Workspace Data Sharing:
   Workspace A writes to s3://shared-bucket/alerts/
   Workspace B reads from same path without replication
   → Cost-effective multi-workspace analytics

2. Long-Term Archival & Compliance:
   Retain data beyond table lifecycle for regulatory requirements
   Example: Financial transactions must persist 7+ years
   → External table ensures data survival independent of metadata

3. Multi-Tool Access:
   Databricks writes Delta files to s3://data-lake/events/
   AWS Athena queries same path with Delta Lake connector
   → Single source of truth across platforms

4. Data Lake Integration:
   Existing data lake at s3://enterprise-lake/
   Create external tables pointing to existing folders
   → Register existing data without migration

5. Disaster Recovery:
   External data survives workspace deletion/corruption
   Restore by recreating table metadata pointing to same path
   → Business continuity through storage independence

═══════════════════════════════════════════════════════════════════════════════
ARCHITECTURE PATTERN
═══════════════════════════════════════════════════════════════════════════════
  Step 1: DEFINE STORAGE PATH
          Configure cloud storage location (S3, ADLS, GCS)
          EXTERNAL_TABLE_PATH = "s3://my-bucket/delayed-alerts/"

  Step 2: CREATE EXTERNAL TABLE
          Use @dlt.table() with path parameter
          @dlt.table(path=EXTERNAL_TABLE_PATH)

  Step 3: WRITE DATA
          DLT handles streaming writes with checkpointing
          Data written to specified external location

  Step 4: VERIFY PERSISTENCE
          DROP TABLE external_delayed_alerts
          Data files remain in cloud storage
          Recreate table pointing to same path to restore access

═══════════════════════════════════════════════════════════════════════════════
CONFIGURATION
═══════════════════════════════════════════════════════════════════════════════
Required:
  • EXTERNAL_TABLE_PATH: Cloud storage location
    S3:   "s3://bucket-name/path/to/alerts/"
    ADLS: "abfss://container@account.dfs.core.windows.net/path/"
    GCS:  "gs://bucket-name/path/to/alerts/"

  • Cloud Storage Permissions:
    - Write access for Databricks cluster/workspace
    - Read access for consuming workspaces/tools
    - Proper IAM roles, service principals, or service accounts

Optional:
  • Partition columns for efficient querying
  • Table properties for optimization settings
  • Lifecycle policies on storage bucket

═══════════════════════════════════════════════════════════════════════════════
OUTPUT SCHEMA
═══════════════════════════════════════════════════════════════════════════════
All fields from silver_delayed_orders PLUS alert metadata:

  alert_id                  STRING        Unique UUID for alert tracking
  alert_type                STRING        Fixed: "DELIVERY_DELAY"
  severity                  STRING        Fixed: "HIGH"
  alert_message             STRING        Human-readable description
  order_id                  STRING        Order identifier
  order_date                TIMESTAMP     When order was created
  restaurant_key            STRING        Restaurant identifier
  driver_key                STRING        Driver identifier
  customer_key              STRING        Customer identifier
  current_status            STRING        Latest delivery status
  delivery_time_minutes     DOUBLE        Delay duration
  total_amount              DOUBLE        Order value (for prioritization)
  event_severity            STRING        From upstream (CRITICAL)
  processed_timestamp       TIMESTAMP     When delay was detected
  sink_timestamp            TIMESTAMP     When written to external table

═══════════════════════════════════════════════════════════════════════════════
STORAGE PATH CONFIGURATION EXAMPLES
═══════════════════════════════════════════════════════════════════════════════

AWS S3:
  # Using bucket path
  EXTERNAL_TABLE_PATH = "s3://ubereats-analytics/delayed-alerts/"

  # Using instance profile (recommended for production)
  spark.conf.set("spark.hadoop.fs.s3a.aws.credentials.provider",
                 "com.amazonaws.auth.InstanceProfileCredentialsProvider")

Azure Data Lake Storage (ADLS Gen2):
  # Using service principal
  EXTERNAL_TABLE_PATH = "abfss://analytics@ubereatsdata.dfs.core.windows.net/delayed-alerts/"

  # Configure service principal authentication
  spark.conf.set("fs.azure.account.auth.type", "OAuth")
  spark.conf.set("fs.azure.account.oauth.provider.type",
                 "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")

Google Cloud Storage (GCS):
  # Using service account
  EXTERNAL_TABLE_PATH = "gs://ubereats-analytics/delayed-alerts/"

  # Configure service account authentication
  spark.conf.set("google.cloud.auth.service.account.enable", "true")

═══════════════════════════════════════════════════════════════════════════════
CROSS-WORKSPACE ACCESS PATTERN
═══════════════════════════════════════════════════════════════════════════════

Workspace A (Producer):
  # Create and write external table
  @dlt.table(path="s3://shared/alerts/")
  def external_delayed_alerts():
      return transform_alerts()

  # Data written to: s3://shared/alerts/

Workspace B (Consumer):
  # Read from same external location
  CREATE TABLE workspace_b.alerts
  USING DELTA
  LOCATION 's3://shared/alerts/'

  # Queries read from same files, no data duplication

Benefits:
  ✅ Single source of truth (no ETL between workspaces)
  ✅ Real-time access (no replication lag)
  ✅ Storage efficiency (data stored once)
  ✅ Consistent schema (Delta enforces structure)

═══════════════════════════════════════════════════════════════════════════════
LEARNING OBJECTIVES
═══════════════════════════════════════════════════════════════════════════════
By studying this module, you will learn:
  • Create external Delta tables with DLT
  • Configure cloud storage paths for different providers (S3, ADLS, GCS)
  • Understand managed vs external table lifecycle differences
  • Implement cross-workspace data sharing patterns
  • Design data lake integration architectures
  • Apply storage-based disaster recovery strategies
  • Optimize external table performance with partitioning

═══════════════════════════════════════════════════════════════════════════════
VERIFICATION & TESTING
═══════════════════════════════════════════════════════════════════════════════

1. Verify Data Written to External Path:
   # List files in cloud storage
   dbutils.fs.ls("s3://your-bucket/delayed-alerts/")

   # Should see Delta files:
   # - _delta_log/ (transaction log)
   # - part-00000-*.parquet (data files)
   # - _checkpoint/ (streaming checkpoint)

2. Test External Table Persistence:
   # Drop table metadata
   DROP TABLE IF EXISTS external_delayed_alerts;

   # Verify files still exist
   dbutils.fs.ls("s3://your-bucket/delayed-alerts/")  # ✅ Files remain

   # Restore access by recreating table
   CREATE TABLE external_delayed_alerts
   USING DELTA
   LOCATION 's3://your-bucket/delayed-alerts/';

   # Query works immediately (no data reload needed)
   SELECT COUNT(*) FROM external_delayed_alerts;

3. Cross-Workspace Access Test:
   # In different workspace, create table pointing to same path
   CREATE TABLE workspace_b.external_alerts
   USING DELTA
   LOCATION 's3://your-bucket/delayed-alerts/';

   # Verify same data visible
   SELECT * FROM workspace_b.external_alerts LIMIT 10;

═══════════════════════════════════════════════════════════════════════════════
PRODUCTION CHECKLIST
═══════════════════════════════════════════════════════════════════════════════
Before Production Deployment:
  ☐ Configure storage bucket with proper IAM policies
  ☐ Set up lifecycle rules for old data archival/deletion
  ☐ Enable server-side encryption (SSE-S3, SSE-KMS, etc.)
  ☐ Configure cross-workspace access permissions
  ☐ Test disaster recovery procedure (DROP + RECREATE)
  ☐ Document external table locations for team
  ☐ Set up monitoring for storage costs
  ☐ Configure partition pruning for query performance
  ☐ Implement table versioning strategy (Delta time travel)
  ☐ Create backup/retention policies

Production Considerations:
  • Storage Costs: External tables incur cloud storage costs
  • Performance: Partition by frequently filtered columns (e.g., date)
  • Governance: Document external paths in data catalog
  • Security: Encrypt data at rest and in transit
  • Monitoring: Track storage growth and query patterns
"""

import dlt
from pyspark.sql import functions as F

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════════

# External table storage location
# IMPORTANT: Replace with your actual cloud storage path
EXTERNAL_TABLE_PATH = spark.conf.get(
    "external_table_path",
    "s3://your-bucket-name/ubereats-delayed-alerts/"  # ⚠️ CHANGE THIS
)

# Alternative paths for different cloud providers:
# AWS S3:   "s3://bucket-name/path/"
# ADLS:     "abfss://container@account.dfs.core.windows.net/path/"
# GCS:      "gs://bucket-name/path/"
# DBFS:     "/mnt/external-mount/path/" (if using mount points)

@dlt.table(
    name="external_delayed_alerts",
    comment="Critical delay alerts persisted to external Delta table for cross-workspace access and long-term retention",
    path=EXTERNAL_TABLE_PATH,  # ⚡ KEY: This makes it an EXTERNAL table
    table_properties={
        "quality": "sink",
        "destination": "external_delta",
        "delta.autoOptimize.optimizeWrite": "true",      # Auto-optimize writes
        "delta.autoOptimize.autoCompact": "true"         # Auto-compact small files
    }
)
def external_delayed_alerts():
    """
    Publish critical delay alerts to external Delta Lake table.

    This function demonstrates the external table pattern by writing streaming
    data to a user-specified cloud storage location. The resulting Delta table
    is UNMANAGED, meaning the data files persist independently of the table
    metadata.

    EXTERNAL TABLE BEHAVIOR:
        When you specify path=EXTERNAL_TABLE_PATH in @dlt.table():

        1. Table Creation:
           - DLT creates Delta table at specified cloud path
           - Transaction log written to: {path}/_delta_log/
           - Data files written to: {path}/*.parquet
           - Metadata registered in catalog

        2. Data Lifecycle:
           - DROP TABLE: Deletes metadata, DATA FILES REMAIN
           - TRUNCATE TABLE: Empties table, DATA FILES DELETED
           - ALTER TABLE: Metadata changes only

        3. Recovery:
           - Recreate table: CREATE TABLE name USING DELTA LOCATION 'path'
           - All historical data immediately accessible
           - Delta time travel available (if not manually deleted)

    STREAMING WRITE CHARACTERISTICS:
        - Append-only: New alerts continuously added
        - Checkpointing: Automatic exactly-once semantics
        - Fault tolerance: Restarts resume from checkpoint
        - File size: Auto-optimized for query performance

    TRANSFORMATION STEPS:
        1. Read from silver_delayed_orders (critical events)
        2. Enrich with alert metadata (UUID, type, severity, message)
        3. Add sink_timestamp for traceability
        4. Write to external Delta location with streaming

    ALERT METADATA ENRICHMENT:
        alert_id:       UUID for unique identification and deduplication
        alert_type:     Fixed value "DELIVERY_DELAY" for classification
        severity:       Fixed value "HIGH" for priority routing
        alert_message:  Human-readable description for display
        sink_timestamp: When record was written to external table

    SCHEMA PRESERVATION:
        All columns from silver_delayed_orders are preserved:
        - order_id, order_date
        - restaurant_key, driver_key, customer_key
        - current_status, delivery_time_minutes, total_amount
        - event_severity, processed_timestamp

    CROSS-WORKSPACE USAGE:
        Once this table is created, other Databricks workspaces can access
        the same data by creating a table pointing to EXTERNAL_TABLE_PATH:

        CREATE TABLE workspace_b.delayed_alerts
        USING DELTA
        LOCATION 's3://your-bucket-name/ubereats-delayed-alerts/';

    PERFORMANCE OPTIMIZATION:
        - Auto-optimize write: Reduces small file problem
        - Auto-compact: Periodically consolidates files
        - Consider partitioning by date for time-based queries:
          .withColumn("alert_date", F.to_date("processed_timestamp"))
          Then add: partition_cols=["alert_date"] to @dlt.table()

    DISASTER RECOVERY SCENARIO:
        1. Pipeline deleted or workspace corrupted
        2. External data files remain in cloud storage
        3. Recreate pipeline and table pointing to same path
        4. Historical data immediately accessible
        5. Streaming resumes from checkpoint

    Returns:
        DataFrame: Enriched alert records written to external Delta location

    Note:
        Ensure your Databricks cluster/workspace has proper permissions
        to write to the specified cloud storage path (IAM role, service
        principal, or service account with write access).
    """
    return (
        dlt.read_stream("live.silver_delayed_orders")

        # ENRICHMENT: Add alert metadata for downstream consumers
        .withColumn("alert_id", F.expr("uuid()"))
        .withColumn("alert_type", F.lit("DELIVERY_DELAY"))
        .withColumn("severity", F.lit("HIGH"))
        .withColumn("alert_message",
            F.concat(
                F.lit("CRITICAL DELAY: Order "),
                F.col("order_id"),
                F.lit(" delayed by "),
                F.round(F.col("delivery_time_minutes"), 1),
                F.lit(" minutes. Restaurant: "),
                F.col("restaurant_key"),
                F.lit(", Driver: "),
                F.col("driver_key"),
                F.lit(", Order Value: $"),
                F.round(F.col("total_amount"), 2)
            )
        )

        # TRACEABILITY: Add timestamp when written to external sink
        .withColumn("sink_timestamp", F.current_timestamp())

        # SELECT: All original columns + new alert fields
        .select(
            "alert_id",
            "alert_type",
            "severity",
            "alert_message",
            "order_id",
            "order_date",
            "restaurant_key",
            "driver_key",
            "customer_key",
            "current_status",
            "delivery_time_minutes",
            "total_amount",
            "event_severity",
            "processed_timestamp",
            "sink_timestamp"
        )
    )
