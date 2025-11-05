"""
Demo 2: ACID Transactions & Concurrency
=======================================

This demo covers:
- ACID Transactions
- Concurrent Writes
- Transaction Log {_delta_log}
- Spark Isolation Levels
- DDL Operations

Run command:
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/bitnami/spark/jobs/spark/mod-4/demo-2.py
"""

import base64
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pyspark.sql.functions import col, current_timestamp, lit, when, expr
import time
import threading


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


def acid_transaction(spark):
    """Demo: ACID Transaction properties in Delta Lake"""

    payments_path = "s3a://owshq-shadow-traffic-uber-eats/kafka/payments/01JTKHGFDV54E72T6G97ESQ7ET.jsonl"
    payments_df = spark.read.json(payments_path)

    lk_payments_path = "s3a://owshq-uber-eats-lakehouse/bronze/payments"

    payments_df.write \
        .format("delta") \
        .mode("overwrite") \
        .save(lk_payments_path)

    initial_count = spark.read.format("delta").load(lk_payments_path).count()
    print(f"initial payments count: {initial_count}")

    try:
        delta_table = DeltaTable.forPath(spark, lk_payments_path)

        # TODO Simulate a complex financial transaction
        # TODO 1. Update payment status
        # TODO 2. Insert new transaction record
        # TODO 3. Delete failed payments

        delta_table.update(
            condition=col("status") == "pending",
            set={"status": lit("processing"), "timestamp": current_timestamp()}
        )

        audit_record = spark.createDataFrame([
            ("AUDIT-001", "BATCH-UPDATE", current_timestamp(), "pending->processing")
        ], ["audit_id", "operation", "timestamp", "description"])

        audit_record.write \
            .format("delta") \
            .mode("append") \
            .save(lk_payments_path)

        print("✅ Atomic transaction completed successfully")

    except Exception as e:
        print(f"❌ Transaction failed atomically: {e}")


    final_df = spark.read.format("delta").load(lk_payments_path)
    final_count = final_df.count()

    print(f"final payments count: {final_count}")
    print("payment status distribution:")
    final_df.groupBy("status").count().show()

    return final_df


def concurrent_writes(spark):
    """Demo: Concurrent write operations and conflict resolution"""

    orders_path = "s3a://owshq-shadow-traffic-uber-eats/kafka/orders/01JTKHGWX6MQAZKVVP6TA7XQNP.jsonl"
    orders_df = spark.read.json(orders_path)

    lk_orders_path = "s3a://owshq-uber-eats-lakehouse/bronze/orders"

    orders_df.write \
        .format("delta") \
        .mode("overwrite") \
        .save(lk_orders_path)

    print("Initial setup completed")

    # TODO Simulate concurrent writes using sequential operations with delays
    # TODO This demonstrates the concept without threading complications

    print("🔄 Simulating Writer 1: Updating order amounts...")
    try:
        delta_table = DeltaTable.forPath(spark, lk_orders_path)

        version_before = delta_table.history(1).select("version").collect()[0][0]
        print(f"📋 Table version before update: {version_before}")

        delta_table.update(
            condition=col("total_amount") < 50,
            set={"total_amount": col("total_amount") * 1.1}
        )

        version_after_update = delta_table.history(1).select("version").collect()[0][0]
        print(f"✅ Writer 1 completed. New version: {version_after_update}")

    except Exception as e:
        print(f"❌ Writer 1 failed: {e}")

    print("\n🔄 Simulating Writer 2: Adding new orders...")
    try:
        new_orders = spark.createDataFrame([
            ("ORDER-NEW-001", "2024-01-01", 75.50, "CONCURRENT-TEST"),
            ("ORDER-NEW-002", "2024-01-01", 125.25, "CONCURRENT-TEST")
        ], ["order_id", "order_date", "total_amount", "source"])

        new_orders.write \
            .format("delta") \
            .mode("append") \
            .save(lk_orders_path)

        version_after_append = delta_table.history(1).select("version").collect()[0][0]
        print(f"✅ Writer 2 completed. New version: {version_after_append}")

    except Exception as e:
        print(f"❌ Writer 2 failed: {e}")

    print("\n🔄 Simulating Writer 3: Concurrent update attempt...")
    try:
        delta_table.update(
            condition=col("total_amount") > 100,
            set={"total_amount": col("total_amount") * 0.95}
        )

        version_after_discount = delta_table.history(1).select("version").collect()[0][0]
        print(f"✅ Writer 3 completed. New version: {version_after_discount}")

    except Exception as e:
        print(f"❌ Writer 3 failed: {e}")

    print("\n📜 Transaction History (showing ACID compliance):")
    delta_table.history().select("version", "timestamp", "operation", "operationParameters") \
        .orderBy("version") \
        .show(truncate=False)

    print("\n🔍 Checking final state after all operations...")
    final_orders = spark.read.format("delta").load(lk_orders_path)
    final_count = final_orders.count()

    print(f"📊 Final order count: {final_count}")

    try:
        concurrent_records = final_orders.filter(col("source") == "CONCURRENT-TEST")
        print(f"🎯 Concurrent test records: {concurrent_records.count()}")
    except:
        print("ℹ️  No concurrent test records found (expected if column doesn't exist)")

    print("\n Order Amount Statistics:")
    final_orders.select("total_amount").describe().show()

    return final_orders


def transaction_log(spark):
    """Demo: Exploring Delta Lake transaction log"""

    lk_orders_path = "s3a://owshq-uber-eats-lakehouse/bronze/orders"
    delta_table = DeltaTable.forPath(spark, lk_orders_path)

    print("📋 Transaction History:")
    history_df = delta_table.history()
    history_df.select(
        "version",
        "timestamp",
        "operation",
        "operationParameters",
        "operationMetrics"
    ).show(10, truncate=False)

    print("\n🔍 Latest Transaction Details:")
    latest_version = history_df.select("version").first()[0]

    print(f"Latest version: {latest_version}")
    print("\n📁 Table Details:")
    spark.sql(f"DESCRIBE DETAIL delta.`{lk_orders_path}`") \
        .select("location", "numFiles", "sizeInBytes", "minReaderVersion", "minWriterVersion") \
        .show(truncate=False)

    return delta_table


def isolation_levels(spark):
    """Demo: Spark isolation levels with Delta Lake"""

    users_path = "s3a://owshq-shadow-traffic-uber-eats/mongodb/users/01JTKHGN85QX2TKEP5MYTV32PH.jsonl"
    users_df = spark.read.json(users_path)

    lk_users_path = "s3a://owshq-uber-eats-lakehouse/bronze/users"

    users_df.write \
        .format("delta") \
        .mode("overwrite") \
        .save(lk_users_path)

    print("📊 Users table created for isolation testing")
    print("\n👀 READ ISOLATION: Snapshot consistency")

    reader_snapshot = spark.read.format("delta").load(lk_users_path)
    initial_count = reader_snapshot.count()
    print(f"📈 Reader sees {initial_count} users")

    delta_table = DeltaTable.forPath(spark, lk_users_path)

    new_users = spark.createDataFrame([
        (9999, "isolation@test.com", "Test City", "BR"),
        (9998, "concurrent@test.com", "Test City 2", "BR")
    ], ["user_id", "email", "city", "country"])

    new_users.write \
        .format("delta") \
        .mode("append") \
        .save(lk_users_path)

    print("✍️  Writer added new users")

    reader_count_after = reader_snapshot.count()
    new_reader_snapshot = spark.read.format("delta").load(lk_users_path)
    new_reader_count = new_reader_snapshot.count()

    print(f"👁️  Original reader still sees: {reader_count_after} users")
    print(f"👁️  New reader sees: {new_reader_count} users")
    print("✅ Isolation maintained: readers get consistent snapshots")

    return delta_table


def ddl_operations(spark):
    """Demo: DDL operations with Delta Lake"""

    lk_ddl_path = "s3a://owshq-uber-eats-lakehouse/ddl"

    print("📋 Creating table with CREATE TABLE DDL...")
    spark.sql(f"""
        CREATE OR REPLACE TABLE test (
            id BIGINT,
            name STRING,
            email STRING,
            created_at TIMESTAMP,
            status STRING,
            metadata MAP<STRING, STRING>
        )
        USING DELTA
        LOCATION '{lk_ddl_path}'
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact' = 'true'
        )
    """)

    print("✅ Table created with DDL")

    # TODO Insert some data
    print("Inserting sample data...")
    spark.sql("""
              INSERT INTO test (id, name, email, created_at, status, metadata)
              VALUES (1, 'John Doe', 'john@example.com', current_timestamp(), 'active', map('role', 'admin')),
                     (2, 'Jane Smith', 'jane@example.com', current_timestamp(), 'active', map('role', 'user')),
                     (3, 'Bob Wilson', 'bob@example.com', current_timestamp(), 'pending', map('role', 'user'))
              """)

    print("✅ Sample data inserted")

    # TODO Add column
    print("➕ Adding new column...")
    spark.sql("ALTER TABLE test ADD COLUMN (last_login TIMESTAMP)")
    print("✅ Added column: last_login")

    # TODO Change column comment
    print("💬 Adding column comment...")
    spark.sql("ALTER TABLE test ALTER COLUMN email COMMENT 'User email address'")
    print("✅ Added column comment")

    # TODO Set table properties
    print("⚙️  Setting table properties...")
    spark.sql("""
              ALTER TABLE test
                  SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = 'interval 7 days')
              """)
    print("✅ Updated table properties")

    # TODO Enable column defaults feature and add default column
    print("\n🔧 Enabling column defaults feature...")
    try:
        spark.sql("""
                  ALTER TABLE test
                      SET TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
                  """)
        print("✅ Column defaults feature enabled")

        # TODO Now add a column with default value
        spark.sql("""
                  ALTER TABLE test
                      ADD COLUMN (subscription_type STRING DEFAULT 'basic')
                  """)
        print("✅ Added column with default value: subscription_type")

    except Exception as e:
        print(f"ℹ️  Column defaults not supported in this Delta version: {e}")
        spark.sql("ALTER TABLE test ADD COLUMN (subscription_type STRING)")
        print("✅ Added column without default: subscription_type")

    print("\n📋 Final table schema:")
    spark.sql("DESCRIBE EXTENDED test").show(truncate=False)

    # TODO Show table properties
    print("\n⚙️  Table properties:")
    try:
        spark.sql("SHOW TBLPROPERTIES test").show(truncate=False)
    except Exception as e:
        print(f"ℹ️  Could not show table properties: {e}")

    # TODO Show data with new columns
    print("\n Table data after schema changes:")
    result_df = spark.sql("SELECT * FROM test")
    result_df.show(truncate=False)

    print("\n Testing schema evolution with new data...")
    try:
        spark.sql("""
                  INSERT INTO test
                  (id, name, email, created_at, status, metadata, last_login, subscription_type)
                  VALUES (4, 'Alice Cooper', 'alice@example.com', current_timestamp(), 'active',
                          map('role', 'premium'), current_timestamp(), 'premium')
                  """)
        print("✅ Successfully inserted data with new schema")
    except Exception as e:
        print(f"ℹ️  Schema evolution test: {e}")

    print("\n Final table contents:")
    final_df = spark.sql("SELECT id, name, email, status, subscription_type FROM test")
    final_df.show(truncate=False)

    return spark.sql("SELECT * FROM test")


def main():
    """Main execution function"""

    spark = spark_session()

    # TODO demos

    # TODO Demo 1: ACID Transactions
    # part_1_acid_transactions = acid_transaction(spark)

    # TODO Demo 2: Concurrent Writes
    # part_2_concurrent_writes = concurrent_writes(spark)

    # TODO Demo 3: Transaction Log Exploration
    # part_3_transaction_log = transaction_log(spark)

    # TODO Demo 4: Isolation Levels
    # part_4_isolation_levels = isolation_levels(spark)

    # TODO Demo 5: DDL Operations
    part_5_ddl_operations = ddl_operations(spark)

    spark.stop()


if __name__ == "__main__":
    main()
