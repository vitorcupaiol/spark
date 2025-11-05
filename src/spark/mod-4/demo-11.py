"""
Apache Iceberg Demo 5: Time Travel & Version Control
====================================================

This demo covers:
- Apache Iceberg: Time Travel with Spark SQL: VERSION AS OF e TIMESTAMP AS OF
- Apache Iceberg: Time Travel with DataFrame API: option("snapshot-id") Magic
- Apache Iceberg: Snapshots Metadata Table
- Apache Iceberg: History Metadata Table
- Apache Iceberg: CALL rollback_to_snapshot

Run with:
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/bitnami/spark/jobs/spark/mod-4/demo-11.py
"""

import base64
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.functions import col, current_timestamp, lit


def spark_session():
    """Create Spark Session with Apache Iceberg and MinIO support"""

    encoded_access_key = "bWluaW9sYWtl"
    encoded_secret_key = "TGFrRTE0MjUzNkBA"
    access_key = base64.b64decode(encoded_access_key).decode("utf-8")
    secret_key = base64.b64decode(encoded_secret_key).decode("utf-8")

    spark = SparkSession.builder \
        .appName("IcebergDemo5-TimeTravel") \
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


def setup_data_for_time_travel(spark):
    """Setup initial data with multiple versions for time travel demonstration"""

    print("\n=== Setting Up Data for Time Travel ===")

    table_fq = "hadoop_catalog.ubereats.orders_time_travel"

    # TODO create table
    print("🏗️ creating orders table...")
    spark.sql(f"""
              CREATE TABLE IF NOT EXISTS {table_fq}
              (
                  order_id STRING,
                  user_id INT,
                  restaurant_id INT,
                  total_amount DOUBLE,
                  status STRING,
                  created_at TIMESTAMP
              ) USING iceberg
              PARTITIONED BY (status)
              TBLPROPERTIES (
                  'write.format.default' = 'parquet'
              )
              """)

    # TODO insert initial data (version 1)
    print("💾 inserting initial data (version 1)...")
    spark.sql(f"""
              INSERT INTO {table_fq} VALUES
              ('ORD-001', 1001, 1, 25.50, 'pending', current_timestamp()),
              ('ORD-002', 1002, 2, 18.75, 'pending', current_timestamp()),
              ('ORD-003', 1003, 3, 32.00, 'pending', current_timestamp())
              """)

    # TODO wait and update data (version 2)
    print("⏱️ waiting 2 seconds...")
    time.sleep(2)

    print("🔄 updating orders to processing (version 2)...")
    spark.sql(f"""
              UPDATE {table_fq} 
              SET status = 'processing', total_amount = total_amount + 2.50
              WHERE status = 'pending'
              """)

    # TODO wait and add more data (version 3)
    print("⏱️ waiting 2 seconds...")
    time.sleep(2)

    print("➕ adding new orders (version 3)...")
    spark.sql(f"""
              INSERT INTO {table_fq} VALUES
              ('ORD-004', 1004, 4, 45.90, 'processing', current_timestamp()),
              ('ORD-005', 1005, 5, 22.30, 'processing', current_timestamp())
              """)

    # TODO wait and complete some orders (version 4)
    print("⏱️ waiting 2 seconds...")
    time.sleep(2)

    print("✅ completing some orders (version 4)...")
    spark.sql(f"""
              UPDATE {table_fq} 
              SET status = 'completed'
              WHERE order_id IN ('ORD-001', 'ORD-003', 'ORD-005')
              """)

    print("✅ data setup complete with 4 versions!")


def time_travel_spark_sql(spark):
    """Demonstrate Time Travel with Spark SQL"""

    print("\n=== Apache Iceberg: Time Travel with Spark SQL ===")

    table_fq = "hadoop_catalog.ubereats.orders_time_travel"

    # TODO show current data
    print("🔍 current data state...")
    spark.sql(f"SELECT * FROM {table_fq} ORDER BY order_id").show()

    # TODO get snapshots for VERSION AS OF
    print("🔍 getting available snapshots...")
    snapshots_df = spark.sql(f"SELECT snapshot_id, committed_at FROM {table_fq}.snapshots ORDER BY committed_at")
    snapshots_df.show(truncate=False)

    # TODO collect snapshot info
    snapshots = snapshots_df.collect()

    if len(snapshots) >= 2:
        # TODO time travel using VERSION AS OF (snapshot_id)
        first_snapshot_id = snapshots[0]['snapshot_id']
        print(f"🕐 time travel using VERSION AS OF (snapshot: {first_snapshot_id})...")
        spark.sql(f"""
                  SELECT * FROM {table_fq} VERSION AS OF {first_snapshot_id}
                  ORDER BY order_id
                  """).show()

        # TODO time travel using TIMESTAMP AS OF
        first_timestamp = snapshots[0]['committed_at']
        print(f"🕐 time travel using TIMESTAMP AS OF ({first_timestamp})...")
        spark.sql(f"""
                  SELECT * FROM {table_fq} TIMESTAMP AS OF '{first_timestamp}'
                  ORDER BY order_id
                  """).show()

        # TODO compare versions
        print("🔍 comparing first vs current version counts...")
        first_count = \
        spark.sql(f"SELECT COUNT(*) as count FROM {table_fq} VERSION AS OF {first_snapshot_id}").collect()[0]['count']
        current_count = spark.sql(f"SELECT COUNT(*) as count FROM {table_fq}").collect()[0]['count']

        print(f"   📊 First version: {first_count} orders")
        print(f"   📊 Current version: {current_count} orders")

    print("✅ Spark SQL time travel demonstrated!")


def time_travel_dataframe_api(spark):
    """Demonstrate Time Travel with DataFrame API"""

    print("\n=== Apache Iceberg: Time Travel with DataFrame API ===")

    table_fq = "hadoop_catalog.ubereats.orders_time_travel"

    # TODO get snapshot information
    snapshots_df = spark.sql(f"SELECT snapshot_id, committed_at FROM {table_fq}.snapshots ORDER BY committed_at")
    snapshots = snapshots_df.collect()

    if len(snapshots) >= 2:
        # TODO time travel using DataFrame API with snapshot-id
        first_snapshot_id = snapshots[0]['snapshot_id']
        print(f"🔍 DataFrame API time travel using snapshot-id ({first_snapshot_id})...")

        df_snapshot = spark.read \
            .format("iceberg") \
            .option("snapshot-id", str(first_snapshot_id)) \
            .load(table_fq)

        print(f"   📊 Records in snapshot: {df_snapshot.count()}")
        df_snapshot.orderBy("order_id").show()

        # TODO time travel using DataFrame API with as-of-timestamp
        first_timestamp = snapshots[0]['committed_at']
        print(f"🔍 DataFrame API time travel using as-of-timestamp ({first_timestamp})...")

        df_timestamp = spark.read \
            .format("iceberg") \
            .option("as-of-timestamp", str(int(first_timestamp.timestamp() * 1000))) \
            .load(table_fq)

        print(f"   📊 Records at timestamp: {df_timestamp.count()}")
        df_timestamp.orderBy("order_id").show()

        # TODO demonstrate filtering on historical data
        print("🔍 filtering historical data...")
        historical_pending = df_snapshot.filter(col("status") == "pending")
        print(f"   📊 Pending orders in first snapshot: {historical_pending.count()}")
        historical_pending.show()

    print("✅ DataFrame API time travel demonstrated!")


def snapshots_metadata_table(spark):
    """Demonstrate Snapshots Metadata Table"""

    print("\n=== Apache Iceberg: Snapshots Metadata Table ===")

    table_fq = "hadoop_catalog.ubereats.orders_time_travel"

    # TODO show all snapshots
    print("🔍 showing all snapshots...")
    spark.sql(f"""
              SELECT snapshot_id, 
                     committed_at,
                     operation,
                     summary
              FROM {table_fq}.snapshots 
              ORDER BY committed_at
              """).show(truncate=False)

    # TODO show snapshot details with summary
    print("🔍 showing snapshot summary details...")
    spark.sql(f"""
              SELECT snapshot_id,
                     committed_at,
                     operation,
                     summary['added-data-files'] as added_files,
                     summary['added-records'] as added_records,
                     summary['total-data-files'] as total_files,
                     summary['total-records'] as total_records
              FROM {table_fq}.snapshots 
              ORDER BY committed_at
              """).show(truncate=False)

    # TODO show manifest information
    print("🔍 showing manifest information...")
    spark.sql(f"""
              SELECT snapshot_id,
                     manifest_list,
                     operation
              FROM {table_fq}.snapshots 
              ORDER BY committed_at DESC
              LIMIT 3
              """).show(truncate=False)

    print("✅ snapshots metadata table demonstrated!")


def history_metadata_table(spark):
    """Demonstrate History Metadata Table"""

    print("\n=== Apache Iceberg: History Metadata Table ===")

    table_fq = "hadoop_catalog.ubereats.orders_time_travel"

    # TODO show table history
    print("🔍 showing complete table history...")
    spark.sql(f"""
              SELECT made_current_at,
                     snapshot_id,
                     parent_id,
                     is_current_ancestor
              FROM {table_fq}.history 
              ORDER BY made_current_at
              """).show(truncate=False)

    # TODO show detailed history with operations
    print("🔍 showing detailed history with operations...")
    spark.sql(f"""
              SELECT h.made_current_at,
                     h.snapshot_id,
                     s.operation,
                     s.summary['added-records'] as added_records,
                     s.summary['deleted-records'] as deleted_records
              FROM {table_fq}.history h
              JOIN {table_fq}.snapshots s ON h.snapshot_id = s.snapshot_id
              ORDER BY h.made_current_at
              """).show(truncate=False)

    # TODO show parent-child relationship
    print("🔍 showing parent-child snapshot relationships...")
    spark.sql(f"""
              SELECT snapshot_id,
                     parent_id,
                     CASE 
                        WHEN parent_id IS NULL THEN 'ROOT'
                        ELSE 'CHILD'
                     END as relationship
              FROM {table_fq}.history 
              ORDER BY made_current_at
              """).show()

    print("✅ history metadata table demonstrated!")


def rollback_to_snapshot(spark):
    """Demonstrate CALL rollback_to_snapshot"""

    print("\n=== Apache Iceberg: CALL rollback_to_snapshot ===")

    table_fq = "hadoop_catalog.ubereats.orders_time_travel"

    # TODO show current state
    print("🔍 current state before rollback...")
    current_count = spark.sql(f"SELECT COUNT(*) as count FROM {table_fq}").collect()[0]['count']
    print(f"   📊 Current record count: {current_count}")
    spark.sql(f"SELECT status, COUNT(*) as count FROM {table_fq} GROUP BY status").show()

    # TODO get snapshot to rollback to (second snapshot)
    snapshots_df = spark.sql(f"""
                             SELECT snapshot_id, committed_at, operation 
                             FROM {table_fq}.snapshots 
                             ORDER BY committed_at
                             """)
    snapshots = snapshots_df.collect()

    if len(snapshots) >= 3:
        # TODO rollback to second snapshot
        target_snapshot_id = snapshots[1]['snapshot_id']
        target_timestamp = snapshots[1]['committed_at']

        print(f"🔄 rolling back to snapshot {target_snapshot_id} ({target_timestamp})...")

        spark.sql(f"""
                  CALL hadoop_catalog.system.rollback_to_snapshot(
                      table => '{table_fq}',
                      snapshot_id => {target_snapshot_id}
                  )
                  """)

        # TODO verify rollback
        print("🔍 state after rollback...")
        rollback_count = spark.sql(f"SELECT COUNT(*) as count FROM {table_fq}").collect()[0]['count']
        print(f"   📊 Record count after rollback: {rollback_count}")
        spark.sql(f"SELECT status, COUNT(*) as count FROM {table_fq} GROUP BY status").show()

        # TODO show current snapshot
        print("🔍 current snapshot after rollback...")
        spark.sql(f"""
                  SELECT snapshot_id, committed_at, operation 
                  FROM {table_fq}.snapshots 
                  WHERE snapshot_id = (
                      SELECT snapshot_id FROM {table_fq}.history 
                      WHERE is_current_ancestor = true 
                      ORDER BY made_current_at DESC 
                      LIMIT 1
                  )
                  """).show(truncate=False)

    print("✅ rollback_to_snapshot demonstrated!")


def cleanup_resources(spark):
    """Clean up demo resources"""

    print("\n=== Cleanup ===")

    try:
        # TODO drop tables with fully qualified names
        tables = [
            'hadoop_catalog.ubereats.orders_time_travel'
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

    print("🚀 Starting Apache Iceberg Demo 5: Time Travel & Version Control")
    print("=" * 80)

    # TODO create Spark session
    spark = spark_session()

    try:
        # TODO run demo sections
        setup_namespace(spark)
        setup_data_for_time_travel(spark)
        time_travel_spark_sql(spark)
        time_travel_dataframe_api(spark)
        snapshots_metadata_table(spark)
        history_metadata_table(spark)
        rollback_to_snapshot(spark)

        print("\n" + "=" * 80)
        print("🎉 Demo 5 completed successfully!")
        print("📚 Key concepts covered:")
        print("   ✓ Time Travel with Spark SQL (VERSION AS OF, TIMESTAMP AS OF)")
        print("   ✓ Time Travel with DataFrame API (snapshot-id, as-of-timestamp)")
        print("   ✓ Snapshots metadata table exploration")
        print("   ✓ History metadata table analysis")
        print("   ✓ Rollback to specific snapshot")
        print("   ✓ Rollback to specific timestamp")

        print("\n🔗 What's Next:")
        print("   → Demo 6: Schema Evolution & Data Types")
        print("   → Demo 7: Performance Optimization & Maintenance")

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
