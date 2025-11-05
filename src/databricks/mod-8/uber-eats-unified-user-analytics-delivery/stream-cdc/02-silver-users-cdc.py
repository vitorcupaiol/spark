"""
============================================================================
SILVER LAYER - Auto CDC API Demo (Simplified)
============================================================================

PURPOSE:
Demonstrates Databricks Auto CDC API with minimal code.
Shows how apply_changes() automatically handles INSERT/UPDATE/DELETE operations.

WHAT IT DOES:
- Unions CDC events from MongoDB and MSSQL
- Uses apply_changes() to automatically process CDC operations
- Creates SCD Type 1 (current state) table
- Creates SCD Type 2 (full history) table

AUTO CDC API FEATURES DEMONSTRATED:
1. Automatic CDC operation handling (INSERT/UPDATE/DELETE)
2. Out-of-order event handling with sequence_by
3. SCD Type 1 and Type 2 pattern
4. Streaming source processing

OFFICIAL DOCS:
https://docs.databricks.com/aws/en/ldp/cdc?language=Python
"""

import dlt
from pyspark.sql.functions import col, lit

# ============================================================================
# STEP 1: Union CDC Streams
# ============================================================================

@dlt.table(
    name="silver_users_staging",
    comment="Unified CDC events from MongoDB and MSSQL",
    table_properties={"quality": "silver"}
)
def silver_users_staging():
    """
    Combines CDC events from both sources.
    Simple UNION - apply_changes() handles the merging.
    """

    # Read MongoDB CDC stream
    mongodb = dlt.read_stream("bronze_mongodb_users").select(
        col("cpf"),
        col("user_id").cast("bigint").alias("user_id"),
        col("email"),
        col("delivery_address"),
        col("city"),
        lit(None).cast("string").alias("first_name"),
        lit(None).cast("string").alias("last_name"),
        lit(None).cast("string").alias("job"),
        col("operation"),
        col("sequenceNum").cast("bigint").alias("sequenceNum"),
        col("dt_current_timestamp").cast("timestamp").alias("dt_current_timestamp")
    ).filter(col("cpf").isNotNull())

    # Read MSSQL CDC stream
    mssql = dlt.read_stream("bronze_mssql_users").select(
        col("cpf"),
        col("user_id").cast("bigint").alias("user_id"),
        lit(None).cast("string").alias("email"),
        lit(None).cast("string").alias("delivery_address"),
        lit(None).cast("string").alias("city"),
        col("first_name"),
        col("last_name"),
        col("job"),
        col("operation"),
        col("sequenceNum").cast("bigint").alias("sequenceNum"),
        col("dt_current_timestamp").cast("timestamp").alias("dt_current_timestamp")
    ).filter(col("cpf").isNotNull())

    # Union both streams
    return mongodb.union(mssql)


# ============================================================================
# STEP 2: Auto CDC - SCD Type 1 (Current State Only)
# ============================================================================

dlt.create_streaming_table(
    name="silver_users_current",
    comment="Current user state - Auto CDC SCD Type 1",
    table_properties={"quality": "silver", "scd_type": "1"}
)

dlt.apply_changes(
    target="silver_users_current",
    source="silver_users_staging",
    keys=["cpf"],
    sequence_by=col("sequenceNum"),
    stored_as_scd_type=1,
    apply_as_deletes="operation = 'DELETE'",
    except_column_list=["operation", "sequenceNum", "dt_current_timestamp"]
)


# ============================================================================
# STEP 3: Auto CDC - SCD Type 2 (Full History)
# ============================================================================

dlt.create_streaming_table(
    name="silver_users_history",
    comment="Complete user history - Auto CDC SCD Type 2",
    table_properties={"quality": "silver", "scd_type": "2"}
)

dlt.apply_changes(
    target="silver_users_history",
    source="silver_users_staging",
    keys=["cpf"],
    sequence_by=col("sequenceNum"),
    stored_as_scd_type=2,
    track_history_column_list=["email", "delivery_address", "city", "first_name", "last_name", "job"],
    apply_as_deletes="operation = 'DELETE'",
    except_column_list=["operation", "sequenceNum", "dt_current_timestamp"]
)


# ============================================================================
# AUTO CDC API KEY CONCEPTS
# ============================================================================

"""
1. AUTOMATIC OPERATION HANDLING:
   - apply_changes() reads 'operation' column automatically
   - INSERT: Creates new record
   - UPDATE: Modifies existing record
   - DELETE: Removes (Type 1) or soft-deletes (Type 2)

2. OUT-OF-ORDER EVENTS:
   - sequence_by=col("sequenceNum") ensures correct ordering
   - Events processed in sequenceNum order, not arrival order
   - Prevents data inconsistency from network delays

3. SCD TYPE 1 vs TYPE 2:
   Type 1 (Current State):
   - One row per key
   - UPDATE overwrites
   - No history tracking

   Type 2 (Full History):
   - Multiple rows per key
   - UPDATE creates new version
   - Adds: __START_AT, __END_AT, __CURRENT columns

4. STREAMING SOURCE:
   - Uses create_streaming_table() for continuous processing
   - Processes events as they arrive
   - Real-time CDC capability

5. FIELD-LEVEL TRACKING (Type 2 only):
   - track_history_column_list: Only these fields trigger new versions
   - Changes to other fields update in-place
   - Reduces storage for high-cardinality changes

QUERY EXAMPLES:

-- Current state (Type 1):
SELECT * FROM silver_users_current WHERE cpf = '12345678900';

-- Current version (Type 2):
SELECT * FROM silver_users_history
WHERE cpf = '12345678900' AND __END_AT IS NULL;

-- All versions (Type 2):
SELECT * FROM silver_users_history
WHERE cpf = '12345678900'
ORDER BY __START_AT;

-- Version at specific time (Type 2):
SELECT * FROM silver_users_history
WHERE cpf = '12345678900'
  AND __START_AT <= '2025-01-01'
  AND (__END_AT > '2025-01-01' OR __END_AT IS NULL);
"""
