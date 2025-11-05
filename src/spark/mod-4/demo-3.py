"""
Demo 3: Time Travel & History Management
=======================================

This demo covers:
- Time Travel
- DESCRIBE HISTORY with Spark
- Spark Delta History API
- RESTORE TABLE with Spark
- VACUUM

Run command:
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/bitnami/spark/jobs/spark/mod-4/demo-3.py
"""

import base64
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pyspark.sql.functions import col


def spark_session():
    """Create Spark Session with Delta Lake and MinIO S3 configurations"""

    encoded_access_key = "bWluaW9sYWtl"
    encoded_secret_key = "TGFrRTE0MjUzNkBA"
    access_key = base64.b64decode(encoded_access_key).decode('utf-8')
    secret_key = base64.b64decode(encoded_secret_key).decode('utf-8')

    spark = SparkSession.builder \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://24.144.65.249:80") \
        .config("spark.hadoop.fs.s3a.access.key", access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.connection.maximum", "100") \
        .config("spark.hadoop.fs.s3a.threads.max", "20") \
        .config("spark.hadoop.fs.s3a.connection.timeout", "300000") \
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000") \
        .config("spark.hadoop.fs.s3a.readahead.range", "256K") \
        .config("spark.hadoop.fs.s3a.fast.upload", "true") \
        .config("spark.hadoop.fs.s3a.fast.upload.buffer", "disk") \
        .config("spark.hadoop.fs.s3a.multipart.size", "64M") \
        .config("spark.hadoop.fs.s3a.multipart.threshold", "64M") \
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
        .config("spark.hadoop.fs.s3a.committer.name", "directory") \
        .config("spark.sql.parquet.filterPushdown", "true") \
        .config("spark.sql.parquet.mergeSchema", "false") \
        .config("spark.sql.parquet.columnarReaderBatchSize", "4096") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark


def setup_versioned_table(spark):
    """Setup a table with multiple versions for time travel demo"""

    orders_path = "s3a://owshq-shadow-traffic-uber-eats/kafka/orders/01JTKHGQ46BST7RAY6Q47YH7EH.jsonl"
    orders_df = spark.read.json(orders_path)

    lk_orders_path = "s3a://owshq-uber-eats-lakehouse/bronze/orders_v"

    print("Version 0: Initial data load")
    orders_df.limit(100).write \
        .format("delta") \
        .mode("overwrite") \
        .save(lk_orders_path)

    initial_count = spark.read.format("delta").load(lk_orders_path).count()
    print(f"✅ Version 0 created with {initial_count} orders")

    # TODO Version 1: Add more orders (simulate daily batch)
    print("\n📊 Version 1: Adding more orders")
    orders_df.limit(200).write \
        .format("delta") \
        .mode("append") \
        .save(lk_orders_path)

    v1_count = spark.read.format("delta").load(lk_orders_path).count()
    print(f"✅ Version 1 created with {v1_count} total orders")

    # TODO Version 2: Update order amounts (simulate price adjustment)
    print("\n📊 Version 2: Updating order amounts")
    delta_table = DeltaTable.forPath(spark, lk_orders_path)
    delta_table.update(
        condition=col("total_amount") < 50,
        set={"total_amount": col("total_amount") * 1.05}
    )

    print("✅ Version 2 created with price updates")

    # TODO Version 3: Add new column and data
    print("\n📊 Version 3: Schema evolution with new column")
    new_orders_with_category = spark.createDataFrame([
        ("ORDER-NEW-TT-001", "2024-01-01", 89.99, "premium"),
        ("ORDER-NEW-TT-002", "2024-01-01", 45.50, "standard"),
        ("ORDER-NEW-TT-003", "2024-01-01", 125.00, "premium")
    ], ["order_id", "order_date", "total_amount", "category"])

    new_orders_with_category.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .save(lk_orders_path)

    print("✅ Version 3 created with schema evolution")

    # TODO Version 4: Delete some orders (simulate cancellations)
    print("\n📊 Version 4: Deleting cancelled orders")
    delta_table.delete(col("total_amount") > 200)

    final_count = spark.read.format("delta").load(lk_orders_path).count()
    print(f"✅ Version 4 created with {final_count} orders after deletions")

    return lk_orders_path


def time_travel(spark, table_path):
    """Demo: Time travel to read different versions"""

    # TODO Read current version (latest)
    print("📖 Reading current version (latest):")
    current_df = spark.read.format("delta").load(table_path)
    current_count = current_df.count()
    print(f"📊 Current version has {current_count} records")

    # TODO Time travel by version number
    print("\n📖 Time travel by version number:")

    for version in [0, 1, 2, 3]:
        try:
            version_df = spark.read.format("delta") \
                .option("versionAsOf", version) \
                .load(table_path)

            version_count = version_df.count()
            print(f"📊 Version {version}: {version_count} records")

            if version == 0:
                print("🔍 Sample data from Version 0:")
                version_df.select("order_id", "total_amount").show(3)

        except Exception as e:
            print(f"❌ Version {version} not accessible: {e}")

    # TODO Time travel by timestamp
    print("\n📖 Time travel by timestamp:")

    # TODO Get table history to find timestamps
    delta_table = DeltaTable.forPath(spark, table_path)
    history_df = delta_table.history()

    # TODO Get timestamp from version 1
    try:
        timestamps = history_df.select("timestamp").collect()
        if len(timestamps) >= 2:
            v1_timestamp = timestamps[-2][0]
            print(f"📅 Version 1 timestamp: {v1_timestamp}")

            timestamp_df = spark.read.format("delta") \
                .option("timestampAsOf", v1_timestamp) \
                .load(table_path)

            timestamp_count = timestamp_df.count()
            print(f"📊 Data at timestamp {v1_timestamp}: {timestamp_count} records")

    except Exception as e:
        print(f"❌ Timestamp-based time travel failed: {e}")

    # TODO Compare versions side by side
    print("\n🔄 Comparing different versions:")
    try:
        v0_df = spark.read.format("delta").option("versionAsOf", 0).load(table_path)
        v_current_df = spark.read.format("delta").load(table_path)

        v0_count = v0_df.count()
        v_current_count = v_current_df.count()

        print(f"📊 Version 0: {v0_count} records")
        print(f"📊 Current version: {v_current_count} records")
        print(f"📈 Growth: {v_current_count - v0_count} records added")

    except Exception as e:
        print(f"❌ Version comparison failed: {e}")

    return current_df


def history_analysis(spark, table_path):
    """Demo: Analyzing table history with DESCRIBE HISTORY and History API"""

    print("\n📜 History Analysis Demo")
    print("=" * 50)

    # TODO Method 1: Using SQL DESCRIBE HISTORY
    print("📋 Using SQL DESCRIBE HISTORY:")
    try:
        spark.sql(f"DESCRIBE HISTORY delta.`{table_path}`") \
            .select("version", "timestamp", "operation", "operationParameters") \
            .show(truncate=False)
    except Exception as e:
        print(f"❌ SQL DESCRIBE HISTORY failed: {e}")

    # TODO Method 2: Using Delta Table History API
    print("\n🔍 Using Delta Table History API:")
    delta_table = DeltaTable.forPath(spark, table_path)
    history_df = delta_table.history()

    print("📊 Complete history overview:")
    history_df.select(
        "version",
        "timestamp",
        "operation",
        "operationParameters",
        "operationMetrics"
    ).show(truncate=False)

    # TODO Analyze operation types
    print("\n📈 Operations summary:")
    operations_summary = history_df.groupBy("operation").count() \
        .orderBy(col("count").desc())
    operations_summary.show()

    # TODO Show detailed metrics for each operation
    print("\n📊 Detailed operation metrics:")
    for row in history_df.select("version", "operation", "operationMetrics").collect():
        version = row["version"]
        operation = row["operation"]
        metrics = row["operationMetrics"]

        print(f"\n🔹 Version {version} - {operation}:")
        if metrics:
            for key, value in metrics.items():
                print(f"   {key}: {value}")
        else:
            print("   No metrics available")

    # TODO Get specific version details
    print("\n🔍 Latest version details:")
    latest_version = history_df.select("version").first()[0]
    latest_details = history_df.filter(col("version") == latest_version).first()

    print(f"📊 Version: {latest_details['version']}")
    print(f"📅 Timestamp: {latest_details['timestamp']}")
    print(f"🔧 Operation: {latest_details['operation']}")

    try:
        user_name = latest_details['userName'] if 'userName' in latest_details.__fields__ else 'Unknown'
        print(f"👤 User: {user_name}")
    except:
        print(f"👤 User: Unknown")

    try:
        engine_info = latest_details['engineInfo'] if 'engineInfo' in latest_details.__fields__ else 'Unknown'
        print(f"💻 Engine: {engine_info}")
    except:
        print(f"💻 Engine: Spark")

    return history_df


def restore_table(spark, table_path):
    """Demo: RESTORE TABLE functionality"""

    print("\n🔄 RESTORE TABLE Demo")
    print("=" * 50)

    delta_table = DeltaTable.forPath(spark, table_path)

    current_df = spark.read.format("delta").load(table_path)
    current_count = current_df.count()
    print(f"📊 Current table has {current_count} records")

    # TODO Get available versions for restore
    history_df = delta_table.history()
    available_versions = [row["version"] for row in history_df.select("version").collect()]
    print(f"📋 Available versions: {available_versions}")

    # TODO Choose a version to restore (e.g., version 1)
    restore_version = 1 if 1 in available_versions else available_versions[-2] if len(available_versions) > 1 else 0

    print(f"\n🔄 Restoring table to version {restore_version}...")

    try:
        # TODO Method 1: Using SQL RESTORE
        spark.sql(f"""
            RESTORE TABLE delta.`{table_path}` 
            TO VERSION AS OF {restore_version}
        """)

        print(f"✅ Table restored to version {restore_version}")

        # TODO Verify restore
        restored_df = spark.read.format("delta").load(table_path)
        restored_count = restored_df.count()
        print(f"📊 Restored table has {restored_count} records")

        # TODO Show new version created by restore
        new_history = delta_table.history()
        latest_operation = new_history.select("version", "operation").first()
        print(f"📋 New version {latest_operation['version']} created with operation: {latest_operation['operation']}")

    except Exception as e:
        print(f"❌ RESTORE failed: {e}")
        print("ℹ️  This might be due to version compatibility or table state")

    # TODO Alternative: Time-based restore
    print(f"\n🔄 Alternative: Demonstrating time-based restore concept...")
    try:
        # TODO Get timestamp for version 0
        version_0_timestamp = history_df.filter(col("version") == 0).select("timestamp").first()
        if version_0_timestamp:
            timestamp = version_0_timestamp[0]
            print(f"📅 Version 0 timestamp: {timestamp}")

            # TODO Read data as of that timestamp (simulating restore)
            historical_df = spark.read.format("delta") \
                .option("timestampAsOf", timestamp) \
                .load(table_path)

            historical_count = historical_df.count()
            print(f"📊 Data at timestamp would have {historical_count} records")
            print("ℹ️  In practice, you would write this back to restore the table")

    except Exception as e:
        print(f"❌ Time-based restore demo failed: {e}")

    return delta_table


def vacuum(spark, table_path):
    """Demo: VACUUM operation to clean up old files"""

    print("\n🧹 VACUUM Demo")
    print("=" * 50)

    delta_table = DeltaTable.forPath(spark, table_path)

    print("📊 Current table details:")
    try:
        spark.sql(f"DESCRIBE DETAIL delta.`{table_path}`") \
            .select("location", "numFiles", "sizeInBytes") \
            .show(truncate=False)
    except Exception as e:
        print(f"❌ Could not describe table: {e}")

    print("\n📜 Table history before VACUUM:")
    history_before = delta_table.history()
    version_count_before = history_before.count()
    print(f"📋 Available versions: {version_count_before}")
    history_before.select("version", "timestamp", "operation").show(5)

    print("\n🔍 VACUUM dry run (checking what would be deleted):")
    try:
        # TODO Note: In production, use a longer retention period (default is 7 days)
        dry_run_result = spark.sql(f"""
            VACUUM delta.`{table_path}` 
            RETAIN 0 HOURS 
            DRY RUN
        """)

        files_to_delete = dry_run_result.count()
        print(f"📁 Files that would be deleted: {files_to_delete}")

        if files_to_delete > 0:
            print("🔍 Sample files to be deleted:")
            dry_run_result.show(3, truncate=False)

    except Exception as e:
        print(f"❌ VACUUM dry run failed: {e}")
        print("ℹ️  This might be due to Delta Lake version or configuration")

    print(f"\n🧹 Performing VACUUM operation...")
    try:
        # TODO For demo purposes, disable retention check and use short retention
        # TODO WARNING: This is ONLY for demo - never do this in production!
        spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

        print("⚠️  Demo: Disabling retention duration check for educational purposes")
        print("⚠️  Production: Always use default 168 hours (7 days) or longer!")

        vacuum_result = spark.sql(f"""
            VACUUM delta.`{table_path}` 
            RETAIN 0 HOURS
        """)

        print("✅ VACUUM completed successfully")

        spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "true")

        print("\n📊 Table details after VACUUM:")
        spark.sql(f"DESCRIBE DETAIL delta.`{table_path}`") \
            .select("location", "numFiles", "sizeInBytes") \
            .show(truncate=False)

        print("\n⏰ Time travel capabilities after VACUUM:")
        post_vacuum_history = delta_table.history()
        version_count_after = post_vacuum_history.count()
        print(f"📋 Available versions after VACUUM: {version_count_after}")

        try:
            old_version_df = spark.read.format("delta") \
                .option("versionAsOf", 0) \
                .load(table_path)

            old_count = old_version_df.count()
            print(f"✅ Can still read version 0: {old_count} records")

        except Exception as e:
            print(f"❌ Cannot read version 0 after VACUUM: {e}")
            print("ℹ️  This is expected if files were vacuumed")

    except Exception as e:
        print(f"❌ VACUUM operation failed: {e}")

        print(f"\n🏭 Demonstrating production-safe VACUUM (168 hours retention):")
        try:
            production_vacuum = spark.sql(f"""
                VACUUM delta.`{table_path}` 
                RETAIN 168 HOURS
            """)
            print("✅ Production-safe VACUUM completed successfully")

        except Exception as prod_e:
            print(f"ℹ️  Production VACUUM also failed: {prod_e}")
            print("ℹ️  This table might be too new for any cleanup")

    print(f"\n📅 Realistic VACUUM Schedule Example:")
    print("   • Daily cleanup: VACUUM RETAIN 168 HOURS (keep 7 days)")
    print("   • Weekly cleanup: VACUUM RETAIN 720 HOURS (keep 30 days)")
    print("   • Monthly cleanup: VACUUM RETAIN 2160 HOURS (keep 90 days)")
    print("   • Compliance: VACUUM RETAIN 8760 HOURS (keep 1 year)")

    print(f"\n⚙️  VACUUM Safety Configuration:")
    retention_check = spark.conf.get("spark.databricks.delta.retentionDurationCheck.enabled", "true")
    print(f"   spark.databricks.delta.retentionDurationCheck.enabled = {retention_check}")
    print(f"   Minimum safe retention: 168 hours (7 days)")
    print(f"   Recommended retention: 720+ hours (30+ days)")

    print("\n💡 VACUUM Best Practices:")
    print("   • Default retention: 7 days (168 hours)")
    print("   • Consider time travel needs before vacuuming")
    print("   • Run VACUUM during low-activity periods")
    print("   • Monitor storage costs vs. time travel requirements")
    print("   • Use DRY RUN first to estimate impact")

    return delta_table


def main():
    """Main execution function"""

    spark = spark_session()

    # TODO Demo 1: Setup versioned table
    # setup_versioned_table(spark)

    table_path = "s3a://owshq-uber-eats-lakehouse/bronze/orders_v"

    # TODO Demo 2: Time travel operations
    # part_2_time_travel = time_travel(spark, table_path)

    # TODO Demo 3: History analysis
    # part_3_history_analysis = history_analysis(spark, table_path)

    # TODO Demo 4: Restore operations
    # part_4_restore_table = restore_table(spark, table_path)

    # TODO Demo 5: Vacuum operations
    part_5_vacuum = vacuum(spark, table_path)

    spark.stop()


if __name__ == "__main__":
    main()
