-- ============================================================================
-- BRONZE LAYER - MongoDB User CDC (Simplified Demo)
-- ============================================================================
--
-- PURPOSE:
-- Demonstrates Databricks Auto CDC API with minimal configuration.
-- This is a simplified version focusing on the core CDC capabilities.
--
-- WHAT IT DOES:
-- - Ingests CDC events from MongoDB using Auto Loader
-- - Creates a STREAMING TABLE for continuous processing
-- - Preserves CDC metadata (operation, sequenceNum)
-- - No transformations - raw data as-is
--
-- CDC EVENT STRUCTURE:
-- - operation: INSERT, UPDATE, DELETE
-- - sequenceNum: Event ordering number
-- - user data: cpf, email, delivery_address, city, etc.
-- - dt_current_timestamp: Event timestamp
--
-- AUTO CDC API DEMO:
-- This Bronze table feeds into Silver layer where apply_changes()
-- automatically handles INSERT/UPDATE/DELETE operations.

CREATE OR REFRESH STREAMING TABLE bronze_mongodb_users
COMMENT 'MongoDB CDC events - Auto Loader ingestion'
TBLPROPERTIES (
  'quality' = 'bronze',
  'delta.enableChangeDataFeed' = 'true'
)
AS
SELECT
  *,
  current_timestamp() AS ingestion_timestamp,
  _metadata.file_path AS source_file
FROM cloud_files(
  'abfss://owshq-shadow-traffic@owshqblobstg.dfs.core.windows.net/cdc/mongodb/users',
  'json',
  map(
    'cloudFiles.inferColumnTypes', 'true',
    'cloudFiles.schemaEvolutionMode', 'rescue'
  )
);
