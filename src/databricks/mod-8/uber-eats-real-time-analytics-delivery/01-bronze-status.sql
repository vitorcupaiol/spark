-- ============================================================================
-- BRONZE LAYER - Delivery Status Events Ingestion (SQL Implementation)
-- ============================================================================
--
-- PURPOSE:
-- Ingests raw delivery status update events from cloud storage into Delta Lake.
-- Demonstrates SQL-based streaming ingestion as alternative to Python Auto Loader.
--
-- WHAT IT DOES:
-- - Reads JSON status update files from cloud storage using read_files()
-- - Automatically infers schema from JSON structure
-- - Extracts nested status fields (status_name, timestamp)
-- - Converts Unix epoch timestamp to proper TIMESTAMP type
-- - Adds metadata columns for lineage and audit tracking
--
-- DATA FLOW:
--   Cloud Storage (JSON files)
--     -> read_files() streaming function
--     -> Schema inference and field extraction
--     -> Delta Lake (bronze_status table)
--
-- KEY FEATURES:
-- - SQL native streaming: Uses read_files() instead of Python cloudFiles
-- - Nested field extraction: Accesses status.status_name and status.timestamp
-- - Type conversion: Transforms Unix milliseconds to TIMESTAMP
-- - Metadata tracking: Captures source file and ingestion time
-- - Change Data Feed: Enables incremental downstream processing
--
-- LEARNING OBJECTIVES:
-- - Compare SQL vs Python streaming ingestion approaches
-- - Extract and flatten nested JSON structures
-- - Apply timestamp conversions (Unix epoch to TIMESTAMP)
-- - Implement metadata enrichment in SQL
-- - Configure Delta table properties for streaming workloads
--
-- OUTPUT SCHEMA:
-- - status_id: Unique status update identifier
-- - order_identifier: Links to corresponding order
-- - status: Full nested status structure (kept for reference)
-- - dt_current_timestamp: Event time from source system
-- - status_name: Extracted status value (Pending, In Transit, Delivered)
-- - status_timestamp: Converted timestamp of status change
-- - ingestion_timestamp: When Spark ingested this record
-- - source_file: Origin file path for lineage
--
-- ============================================================================

CREATE OR REFRESH STREAMING TABLE bronze_status
COMMENT "Delivery status update events from Kafka - tracks order lifecycle (SQL version)"
TBLPROPERTIES (
  "quality" = "bronze",
  "layer" = "ingestion",
  "source" = "kafka",
  "topic" = "status",
  "delta.enableChangeDataFeed" = "true",
  "language" = "SQL"
)
AS
SELECT
  status_id,
  order_identifier,
  status,
  dt_current_timestamp,
  status.status_name as status_name,
  CAST(FROM_UNIXTIME(status.timestamp / 1000) AS TIMESTAMP) as status_timestamp,
  CURRENT_TIMESTAMP() as ingestion_timestamp,
  _metadata.file_path as source_file
FROM STREAM read_files(
  'abfss://owshq-shadow-traffic@owshqblobstg.dfs.core.windows.net/kafka/status/*',
  format => 'json',
  inferSchema => true
);
