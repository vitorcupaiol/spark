"""
Apache Iceberg Demo 6: DML Operations & Data Management
=======================================================

This demo covers:
- Apache Iceberg: DML Operations {Update, Delete, Merge}
- Apache Iceberg: Copy-on-Write vs. Merge-on-Read
- Apache Iceberg: CDC {Change Data Capture}
- Apache Iceberg: SCD {Slowly Changing Dimensions}
- Apache Iceberg: Delete Files: Handling Position and Equality Deletes
- Apache Iceberg: CALL set_current_snapshot

Run with:
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/bitnami/spark/jobs/spark/mod-4/demo-12.py
"""

import base64
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType, BooleanType
from pyspark.sql.functions import col, current_timestamp, lit, when, row_number, max as spark_max
from pyspark.sql.window import Window


def spark_session():
    """Create Spark Session with Apache Iceberg and MinIO support"""

    encoded_access_key = "bWluaW9sYWtl"
    encoded_secret_key = "TGFrRTE0MjUzNkBA"
    access_key = base64.b64decode(encoded_access_key).decode("utf-8")
    secret_key = base64.b64decode(encoded_secret_key).decode("utf-8")

    spark = SparkSession.builder \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.hadoop_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.hadoop_catalog.type", "hadoop") \
        .config("spark.sql.catalog.hadoop_catalog.warehouse", "s3a://owshq-catalog/warehouse") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://24.144.65.249:80") \
        .config("spark.hadoop.fs.s3a.access.key", access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    print("✅ Apache Iceberg Spark Session Created Successfully!")
    print(f"🚀 Spark Version: {spark.version}")

    return spark


def setup_namespace(spark):
    """Setup namespace for demo"""

    print("\n=== Setting Up Demo Namespace ===")

    # TODO create namespace
    print("📁 creating namespace...")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS hadoop_catalog.ubereats")

    # TODO set catalog context
    spark.catalog.setCurrentCatalog("hadoop_catalog")
    spark.catalog.setCurrentDatabase("ubereats")

    print("✅ namespace ready!")


def dml_operations(spark):
    """Demonstrate DML Operations: Update, Delete, Merge"""

    print("\n=== Apache Iceberg: DML Operations ===")

    table_fq = "hadoop_catalog.ubereats.orders_dml"

    # TODO create table for DML operations
    print("🏗️ creating orders table for DML operations...")
    spark.sql(f"""
              CREATE TABLE IF NOT EXISTS {table_fq}
              (
                  order_id STRING,
                  user_id INT,
                  restaurant_id INT,
                  total_amount DOUBLE,
                  status STRING,
                  created_at TIMESTAMP,
                  updated_at TIMESTAMP
              ) USING iceberg
              PARTITIONED BY (status)
              """)

    # TODO insert initial data
    print("💾 inserting initial data...")
    spark.sql(f"""
              INSERT INTO {table_fq} VALUES
              ('ORD-001', 1001, 1, 25.50, 'pending', current_timestamp(), current_timestamp()),
              ('ORD-002', 1002, 2, 18.75, 'pending', current_timestamp(), current_timestamp()),
              ('ORD-003', 1003, 3, 32.00, 'confirmed', current_timestamp(), current_timestamp()),
              ('ORD-004', 1004, 4, 45.90, 'confirmed', current_timestamp(), current_timestamp()),
              ('ORD-005', 1005, 5, 22.30, 'pending', current_timestamp(), current_timestamp())
              """)

    print("🔍 initial data state...")
    spark.sql(f"SELECT * FROM {table_fq} ORDER BY order_id").show()

    # TODO demonstrate UPDATE operation
    print("🔄 UPDATE: updating pending orders to processing...")
    spark.sql(f"""
              UPDATE {table_fq} 
              SET status = 'processing', 
                  updated_at = current_timestamp()
              WHERE status = 'pending'
              """)

    print("🔍 after UPDATE...")
    spark.sql(f"SELECT order_id, status, updated_at FROM {table_fq} ORDER BY order_id").show()

    # TODO demonstrate DELETE operation
    print("🗑️ DELETE: removing orders with amount < 20...")
    spark.sql(f"""
              DELETE FROM {table_fq} 
              WHERE total_amount < 20.00
              """)

    print("🔍 after DELETE...")
    spark.sql(f"SELECT order_id, total_amount FROM {table_fq} ORDER BY order_id").show()

    # TODO demonstrate MERGE operation
    print("🔀 MERGE: merging new order data...")

    # Create source data
    spark.sql(f"""
              CREATE OR REPLACE TEMPORARY VIEW orders_updates AS
              SELECT * FROM VALUES
              ('ORD-003', 1003, 3, 35.00, 'delivered', current_timestamp(), current_timestamp()),
              ('ORD-006', 1006, 6, 28.75, 'pending', current_timestamp(), current_timestamp()),
              ('ORD-007', 1007, 7, 41.50, 'confirmed', current_timestamp(), current_timestamp())
              AS t(order_id, user_id, restaurant_id, total_amount, status, created_at, updated_at)
              """)

    spark.sql(f"""
              MERGE INTO {table_fq} AS target
              USING orders_updates AS source
              ON target.order_id = source.order_id
              WHEN MATCHED THEN 
                  UPDATE SET 
                      total_amount = source.total_amount,
                      status = source.status,
                      updated_at = source.updated_at
              WHEN NOT MATCHED THEN
                  INSERT (order_id, user_id, restaurant_id, total_amount, status, created_at, updated_at)
                  VALUES (source.order_id, source.user_id, source.restaurant_id, 
                         source.total_amount, source.status, source.created_at, source.updated_at)
              """)

    print("🔍 after MERGE...")
    spark.sql(f"SELECT * FROM {table_fq} ORDER BY order_id").show()

    print("✅ DML operations demonstrated!")


def copy_on_write_vs_merge_on_read(spark):
    """Demonstrate Copy-on-Write vs. Merge-on-Read

    COW = Read-heavy workloads
    MOR = Write-heavy workloads

    Copy-on-Write Mechanics:

    Before UPDATE:
    File Structure:
    data/
    └── data-file-1.parquet
        ├── ORD-001, 1001, 25.50, 'pending'
        ├── ORD-002, 1002, 18.75, 'pending'
        └── ORD-003, 1003, 32.00, 'confirmed'

    After UPDATE (COW):
    File Structure:
    data/
    ├── data-file-1.parquet  # Logically deleted, not physically removed
    │   ├── ORD-001, 1001, 25.50, 'pending'      # ← Old version
    │   ├── ORD-002, 1002, 18.75, 'pending'      # ← Old version
    │   └── ORD-003, 1003, 32.00, 'confirmed'
    └── data-file-2.parquet  # New file with updated records
        ├── ORD-001, 1001, 25.50, 'processing'   # ← Updated
        ├── ORD-002, 1002, 18.75, 'processing'   # ← Updated
        └── ORD-003, 1003, 32.00, 'confirmed'    # ← Unchanged but rewritten

    SELECT * FROM orders_cow WHERE status = 'processing'

    -- Execution plan:
    -- 1. Read only data-file-2.parquet
    -- 2. Apply filter: status = 'processing'
    -- 3. Return results (simple file scan)

    Merge-on-Read Mechanics:
    After UPDATE (MOR):
    File Structure:
    data/
    ├── data-file-1.parquet  # Original data remains
    │   ├── ORD-001, 1001, 25.50, 'pending'
    │   ├── ORD-002, 1002, 18.75, 'pending'
    │   └── ORD-003, 1003, 32.00, 'confirmed'
    └── delete-file-1.parquet  # Delete markers for updated records
        ├── Delete: ORD-001 (position-based or equality-based)
        └── Delete: ORD-002 (position-based or equality-based)
    ├── insert-file-1.parquet  # New versions of updated records
        ├── ORD-001, 1001, 25.50, 'processing'   # ← New version
        └── ORD-002, 1002, 18.75, 'processing'   # ← New version

    SELECT * FROM orders_mor WHERE status = 'processing'

    -- Execution plan:
    -- 1. Read data-file-1.parquet
    -- 2. Read delete-file-1.parquet
    -- 3. Read insert-file-1.parquet
    -- 4. Apply deletes to original data
    -- 5. Merge with inserted data
    -- 6. Apply filter: status = 'processing'
    -- 7. Return results (complex merge operation)
    """

    print("\n=== Apache Iceberg: Copy-on-Write vs. Merge-on-Read ===")

    # TODO create Copy-on-Write table
    table_cow = "hadoop_catalog.ubereats.orders_cow"
    print("🏗️ creating Copy-on-Write table...")
    spark.sql(f"""
              CREATE TABLE IF NOT EXISTS {table_cow}
              (
                  order_id STRING,
                  user_id INT,
                  total_amount DOUBLE,
                  status STRING
              ) USING iceberg
              TBLPROPERTIES (
                  'write.delete.mode' = 'copy-on-write',
                  'write.update.mode' = 'copy-on-write',
                  'write.merge.mode' = 'copy-on-write'
              )
              """)

    # TODO create Merge-on-Read table
    table_mor = "hadoop_catalog.ubereats.orders_mor"
    print("🏗️ creating Merge-on-Read table...")
    spark.sql(f"""
              CREATE TABLE IF NOT EXISTS {table_mor}
              (
                  order_id STRING,
                  user_id INT,
                  total_amount DOUBLE,
                  status STRING
              ) USING iceberg
              TBLPROPERTIES (
                  'write.delete.mode' = 'merge-on-read',
                  'write.update.mode' = 'merge-on-read',
                  'write.merge.mode' = 'merge-on-read'
              )
              """)

    # TODO insert same data to both tables
    print("💾 inserting data to both tables...")
    data_sql = """
        ('ORD-001', 1001, 25.50, 'pending'),
        ('ORD-002', 1002, 18.75, 'pending'),
        ('ORD-003', 1003, 32.00, 'confirmed')
    """

    spark.sql(f"INSERT INTO {table_cow} VALUES {data_sql}")
    spark.sql(f"INSERT INTO {table_mor} VALUES {data_sql}")

    # TODO perform UPDATE on both tables
    print("🔄 performing UPDATE on both tables...")
    spark.sql(f"UPDATE {table_cow} SET status = 'processing' WHERE status = 'pending'")
    spark.sql(f"UPDATE {table_mor} SET status = 'processing' WHERE status = 'pending'")

    # TODO show file structure differences
    print("🔍 Copy-on-Write files...")
    spark.sql(f"SELECT file_path, record_count FROM {table_cow}.files").show(truncate=False)

    print("🔍 Merge-on-Read files...")
    spark.sql(f"SELECT file_path, record_count FROM {table_mor}.files").show(truncate=False)

    # TODO show table properties
    print("🔍 Copy-on-Write properties...")
    spark.sql(f"SHOW TBLPROPERTIES {table_cow}").filter(col("key").contains("write")).show()

    print("🔍 Merge-on-Read properties...")
    spark.sql(f"SHOW TBLPROPERTIES {table_mor}").filter(col("key").contains("write")).show()

    print("✅ Copy-on-Write vs. Merge-on-Read demonstrated!")


def change_data_capture(spark):
    """
    INSERT INTO users_cdc VALUES
    (1001, 'Alice', 'alice@email.com', 'active'),
    (1002, 'Bob', 'bob@email.com', 'active')

    -- CDC Records Generated:
    user_id | name  | email           | status | _change_type | _commit_snapshot_id | _change_ordinal
    1001    | Alice | alice@email.com | active | INSERT       | 123456789          | 0
    1002    | Bob   | bob@email.com   | active | INSERT       | 123456789          | 1

    DELETE FROM users_cdc WHERE user_id = 1002

    -- CDC Records Generated:
    user_id | name | email         | status | _change_type | _commit_snapshot_id | _change_ordinal
    1002    | Bob  | bob@email.com | active | DELETE       | 123456791          | 0

    -- Track who changed what and when
    SELECT
        user_id,
        _change_type,
        name,
        email,
        status,
        s.committed_at as change_time
    FROM users_cdc.changes c
    JOIN users_cdc.snapshots s ON c._commit_snapshot_id = s.snapshot_id
    WHERE _change_type IN ('UPDATE_BEFORE', 'UPDATE_AFTER', 'DELETE')
    ORDER BY change_time DESC
    """

    table_fq = "hadoop_catalog.ubereats.users_cdc"
    spark.sql(f"DROP TABLE IF EXISTS {table_fq}")
    spark.sql(f'''
        CREATE TABLE {table_fq} (
            user_id INT,
            name STRING,
            email STRING,
            status STRING
        ) USING iceberg
        TBLPROPERTIES (
            'format-version' = '2',
            'write.delete.mode' = 'merge-on-read',
            'write.update.mode' = 'merge-on-read',
            'read.delete.mode' = 'merge-on-read',
            'read.update.mode' = 'merge-on-read',
            'cdc.enabled' = 'true'
        )
    ''')

    # Insert and update data
    spark.sql(f"INSERT INTO {table_fq} VALUES (1001, 'Alice', 'alice@email.com', 'active')")
    spark.sql(f"INSERT INTO {table_fq} VALUES (1002, 'Bob', 'bob@email.com', 'active')")
    spark.sql(f"UPDATE {table_fq} SET status = 'inactive' WHERE user_id = 1001")

    # Check CDC metadata columns
    changes_schema = spark.sql(f"DESCRIBE {table_fq}.changes").toPandas()
    print(changes_schema)
    cdc_metadata_cols = {'_change_type', '_commit_snapshot_id', '_change_ordinal'}
    schema_cols = set(changes_schema['col_name'].tolist())
    if not cdc_metadata_cols.intersection(schema_cols):
        print("❌ CDC metadata columns not found! CDC is NOT enabled or not supported in your environment.")
        print("Columns found:", schema_cols)
        return

    # Show all CDC changes
    try:
        cdc_df = spark.sql(f"SELECT * FROM {table_fq}.changes")
        cdc_df.show(truncate=False)
    except Exception as ex:
        print(f"⚠️ CDC query failed: {ex}")

    print("✅ CDC demo complete.")


def slowly_changing_dimensions(spark):
    """Demonstrate Slowly Changing Dimensions (SCD)"""

    import time

    print("\n=== Apache Iceberg: SCD (Slowly Changing Dimensions) ===")

    table_fq = "hadoop_catalog.ubereats.restaurant_scd"

    # Create SCD table
    print("🏗️ creating restaurant SCD table...")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_fq}
        (
            restaurant_id INT,
            name STRING,
            cuisine_type STRING,
            rating DOUBLE,
            phone STRING,
            is_current BOOLEAN,
            valid_from TIMESTAMP,
            valid_to TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (is_current)
    """)

    # Insert initial SCD data
    print("💾 inserting initial SCD data...")
    spark.sql(f"""
        INSERT INTO {table_fq} VALUES
        (1, 'Pizza Palace', 'Italian', 4.2, '555-0001', true, current_timestamp(), null),
        (2, 'Burger King', 'American', 3.8, '555-0002', true, current_timestamp(), null),
        (3, 'Sushi Zen', 'Japanese', 4.7, '555-0003', true, current_timestamp(), null)
    """)

    print("🔍 initial SCD state...")
    spark.sql(f"SELECT * FROM {table_fq} WHERE is_current = true ORDER BY restaurant_id").show()

    time.sleep(1)

    # Simulate SCD Type 2 update (rating change)
    print("🔄 simulating SCD Type 2 update (rating change for Pizza Palace)...")
    current_ts = spark.sql("SELECT current_timestamp() as ts").collect()[0]['ts']

    spark.sql(f"""
        UPDATE {table_fq} 
        SET is_current = false, valid_to = TIMESTAMP '{current_ts}'
        WHERE restaurant_id = 1 AND is_current = true
    """)

    spark.sql(f"""
        INSERT INTO {table_fq} VALUES
        (1, 'Pizza Palace', 'Italian', 4.5, '555-0001', true, TIMESTAMP '{current_ts}', null)
    """)

    time.sleep(1)

    # Simulate phone number change
    print("🔄 simulating phone number change for Burger King...")
    current_ts2 = spark.sql("SELECT current_timestamp() as ts").collect()[0]['ts']

    spark.sql(f"""
        UPDATE {table_fq} 
        SET is_current = false, valid_to = TIMESTAMP '{current_ts2}'
        WHERE restaurant_id = 2 AND is_current = true
    """)

    spark.sql(f"""
        INSERT INTO {table_fq} VALUES
        (2, 'Burger King', 'American', 3.8, '555-0999', true, TIMESTAMP '{current_ts2}', null)
    """)

    # Show current SCD state
    print("🔍 current SCD state...")
    spark.sql(f"SELECT * FROM {table_fq} WHERE is_current = true ORDER BY restaurant_id").show()

    # Show full history
    print("🔍 full SCD history...")
    spark.sql(f"""
        SELECT restaurant_id, name, rating, phone, is_current, valid_from, valid_to
        FROM {table_fq} 
        ORDER BY restaurant_id, valid_from
    """).show(truncate=False)

    print("✅ SCD demonstrated!")


def delete_files_operations(spark):
    """Demonstrate Delete Files: Position and Equality Deletes in Iceberg"""

    print("\n=== Apache Iceberg: Delete Files Operations ===")

    table_fq = "hadoop_catalog.ubereats.orders_deletes"

    # 1. Create table for delete operations
    print("🏗️ Creating table for delete operations...")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_fq}
        (
            order_id STRING,
            user_id INT,
            total_amount DOUBLE,
            status STRING
        ) USING iceberg
        TBLPROPERTIES (
            'write.delete.mode' = 'merge-on-read'
        )
    """)

    # 2. Insert data
    print("💾 Inserting data...")
    spark.sql(f"""
        INSERT INTO {table_fq} VALUES
        ('ORD-001', 1001, 25.50, 'pending'),
        ('ORD-002', 1002, 18.75, 'confirmed'),
        ('ORD-003', 1003, 32.00, 'pending'),
        ('ORD-004', 1004, 45.90, 'confirmed'),
        ('ORD-005', 1005, 22.30, 'pending')
    """)

    # 3. Show initial files
    print("🔍 Initial files (before deletes)...")
    spark.sql(f"SELECT file_path, record_count, content FROM {table_fq}.files").show(truncate=False)

    # 4. Perform delete operation (creates delete files)
    print("🗑️ Performing delete operation (delete all 'pending' orders)...")
    spark.sql(f"DELETE FROM {table_fq} WHERE status = 'pending'")

    # 5. Show files after delete (data + delete files)
    print("🔍 Files after delete (data + delete files)...")
    spark.sql(f"SELECT file_path, record_count, content FROM {table_fq}.files").show(truncate=False)

    # 6. Show data after delete
    print("🔍 Data after delete...")
    spark.sql(f"SELECT * FROM {table_fq} ORDER BY order_id").show()

    # 7. Show manifest entries (using correct columns)
    print("🔍 Manifest entries...")
    spark.sql(f"""
        SELECT path, content, added_snapshot_id, added_delete_files_count, deleted_data_files_count
        FROM {table_fq}.manifests
        LIMIT 5
    """).show(truncate=False)

    # 8. Optionally, show delete file details
    print("🔍 Delete file details (content=1 means delete file)...")
    spark.sql(f"""
        SELECT file_path, record_count, content
        FROM {table_fq}.files
        WHERE content = 1
    """).show(truncate=False)

    print("✅ Delete files operations demonstrated!")


def set_current_snapshot(spark):
    """Demonstrate CALL set_current_snapshot in Apache Iceberg"""

    import time

    print("\n=== Apache Iceberg: CALL set_current_snapshot ===")

    table_fq = "hadoop_catalog.ubereats.orders_snapshot_ops"

    # 1. Create table and generate snapshots
    print("🏗️ Creating table and generating snapshots...")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_fq}
        (
            order_id STRING,
            status STRING,
            amount DOUBLE
        ) USING iceberg
    """)

    # Generate multiple snapshots
    spark.sql(f"INSERT INTO {table_fq} VALUES ('ORD-001', 'pending', 25.50)")
    time.sleep(1)
    spark.sql(f"INSERT INTO {table_fq} VALUES ('ORD-002', 'confirmed', 18.75)")
    time.sleep(1)
    spark.sql(f"UPDATE {table_fq} SET status = 'processing' WHERE order_id = 'ORD-001'")

    # 2. Show available snapshots
    print("🔍 Available snapshots...")
    snapshots_df = spark.sql(f"""
        SELECT snapshot_id, committed_at, operation 
        FROM {table_fq}.snapshots 
        ORDER BY committed_at
    """)
    snapshots_df.show(truncate=False)
    snapshots = snapshots_df.collect()

    if len(snapshots) < 2:
        print("⚠️ Not enough snapshots to demonstrate set_current_snapshot. Try making more changes first.")
        return

    # 3. Show current data
    print("🔍 Current data...")
    spark.sql(f"SELECT * FROM {table_fq}").show()

    # 4. Set current snapshot to an earlier version (e.g., the second snapshot)
    target_snapshot = snapshots[1]['snapshot_id']
    print(f"🔄 Setting current snapshot to {target_snapshot}...")

    spark.sql(f"""
        CALL hadoop_catalog.system.set_current_snapshot(
            table => '{table_fq}',
            snapshot_id => {target_snapshot}
        )
    """)

    # 5. Verify snapshot change
    print("🔍 Data after set_current_snapshot...")
    spark.sql(f"SELECT * FROM {table_fq}").show()

    # 6. Show current snapshot in history
    print("🔍 Current snapshot in history...")
    spark.sql(f"""
        SELECT snapshot_id, made_current_at, is_current_ancestor
        FROM {table_fq}.history 
        ORDER BY made_current_at DESC
        LIMIT 3
    """).show(truncate=False)

    print("✅ set_current_snapshot demonstrated!")


def cleanup_resources(spark):
    """Clean up demo resources"""

    print("\n=== Cleanup ===")

    try:
        # TODO drop tables with fully qualified names
        tables = [
            'hadoop_catalog.ubereats.orders_dml',
            'hadoop_catalog.ubereats.orders_cow',
            'hadoop_catalog.ubereats.orders_mor',
            'hadoop_catalog.ubereats.users_cdc',
            'hadoop_catalog.ubereats.restaurant_scd',
            'hadoop_catalog.ubereats.orders_deletes',
            'hadoop_catalog.ubereats.orders_snapshot_ops',
            'hadoop_catalog.ubereats.orders_source',
            'hadoop_catalog.ubereats.orders_target',
            'hadoop_catalog.ubereats.orders_api'
        ]

        for table in tables:
            spark.sql(f"DROP TABLE IF EXISTS {table}")

        # TODO drop namespace
        spark.sql("DROP NAMESPACE IF EXISTS hadoop_catalog.ubereats CASCADE")

        print("✅ demo resources cleaned up successfully!")

    except Exception as e:
        print(f"⚠️ cleanup warning: {e}")


def main():
    """Main demo execution"""

    print("🚀 Starting Apache Iceberg Demo 6: DML Operations & Data Management")
    print("=" * 80)

    # TODO create Spark session
    spark = spark_session()

    try:
        # TODO run demo sections
        setup_namespace(spark)
        dml_operations(spark)
        copy_on_write_vs_merge_on_read(spark)
        change_data_capture(spark)
        slowly_changing_dimensions(spark)
        delete_files_operations(spark)
        set_current_snapshot(spark)

        print("\n" + "=" * 80)
        print("🎉 Demo 6 completed successfully!")
        print("📚 Key concepts covered:")
        print("   ✓ DML Operations (Update, Delete, Merge)")
        print("   ✓ Copy-on-Write vs. Merge-on-Read modes")
        print("   ✓ Change Data Capture (CDC) patterns")
        print("   ✓ Slowly Changing Dimensions (SCD)")
        print("   ✓ Delete files and operations")
        print("   ✓ Snapshot management (set_current, cherrypick)")
        print("   ✓ Iceberg Table API exploration")

        print("\n🔗 What's Next:")
        print("   → Demo 7: Performance Optimization & Maintenance")
        print("   → Demo 8: Integration & Advanced Features")

    except Exception as e:
        print(f"❌ Demo failed with error: {e}")
        import traceback
        traceback.print_exc()

    finally:
        # TODO cleanup
        # cleanup_resources(spark)
        spark.stop()
        print("🔒 Spark session stopped")


if __name__ == "__main__":
    main()
