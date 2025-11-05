"""
Apache Iceberg Demo 8: Maintenance & Optimization
==================================================

This demo covers:
- Apache Iceberg: CALL rewrite_data_files
- Apache Iceberg: File Compaction Strategies
- Apache Iceberg: Fanout Writers: Parallel Writing Strategies
- Apache Iceberg: Manifest Caching for Metadata Performance Boost
- Apache Iceberg: Table Metrics Collection

Run with:
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/bitnami/spark/jobs/spark/mod-4/demo-14.py
"""

import base64
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


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


def rewrite_data_files(spark):
    """Demonstrate CALL rewrite_data_files

    -- Basic compaction (consolidates all small files)
    CALL hadoop_catalog.system.rewrite_data_files(
        table => 'orders'
    )

    -- Compaction with specific strategy
    CALL hadoop_catalog.system.rewrite_data_files(
        table => 'orders',
        strategy => 'binpack',                    -- Bin-packing algorithm
        options => map(
            'target-file-size-bytes', '134217728', -- 128MB target size
            'max-file-group-size-bytes', '1073741824', -- 1GB max group
            'min-file-size-bytes', '67108864'     -- 64MB minimum threshold
        )
    )

    -- Partition-specific compaction
    CALL hadoop_catalog.system.rewrite_data_files(
        table => 'orders',
        where => 'status = "completed"',          -- Only compact completed orders
        strategy => 'binpack'
    )

    # Production compaction schedule
    COMPACTION_SCHEDULE = {
        'high_frequency_tables': {
            'schedule': 'hourly',
            'target_file_size': 128 * 1024 * 1024,  # 128MB
            'small_file_threshold': 10
        },
        'medium_frequency_tables': {
            'schedule': 'daily',
            'target_file_size': 256 * 1024 * 1024,  # 256MB
            'small_file_threshold': 20
        },
        'low_frequency_tables': {
            'schedule': 'weekly',
            'target_file_size': 512 * 1024 * 1024,  # 512MB
            'small_file_threshold': 50
        }
    }
    """

    print("\n=== Apache Iceberg: CALL rewrite_data_files ===")

    table_fq = "hadoop_catalog.ubereats.orders"

    # TODO create table and generate small files
    print("🏗️ creating table and generating small files...")
    spark.sql(f"""
              CREATE TABLE IF NOT EXISTS {table_fq}
              (
                  order_id STRING,
                  amount DOUBLE,
                  status STRING
              ) USING iceberg
              TBLPROPERTIES (
                  'write.target-file-size-bytes' = '1048576'
              )
              """)

    # TODO insert data in small batches to create multiple files
    print("💾 inserting data in small batches...")
    for i in range(5):
        spark.sql(f"""
                  INSERT INTO {table_fq} VALUES
                  ('ORD-{i + 1:03d}', {25.0 + i * 5}, 'completed')
                  """)

    # TODO show files before compaction
    print("🔍 files before compaction...")
    spark.sql(f"SELECT COUNT(*) as file_count FROM {table_fq}.files").show()

    # TODO rewrite data files
    print("🔄 rewriting data files...")
    spark.sql(f"""
              CALL hadoop_catalog.system.rewrite_data_files(
                  table => '{table_fq}'
              )
              """)

    # TODO show files after compaction
    print("🔍 files after compaction...")
    spark.sql(f"SELECT COUNT(*) as file_count FROM {table_fq}.files").show()

    print("✅ data files rewritten!")


def file_compaction_strategies(spark):
    """Demonstrate File Compaction Strategies

    CALL hadoop_catalog.system.rewrite_data_files(
    table => 'orders_compaction',
    strategy => 'sort',
        options => map(
            'sort-order', 'order_id ASC, amount DESC',
            'target-file-size-bytes', '134217728'
        )
    )

    CALL hadoop_catalog.system.rewrite_data_files(
    table => 'orders_compaction',
    strategy => 'sort',
        options => map(
            'sort-order', 'zorder(status, amount)',  -- Multi-dimensional sorting
            'target-file-size-bytes', '134217728'
        )
    )

    -- High-frequency small files from streaming
    TBLPROPERTIES (
        'write.target-file-size-bytes' = '67108864',     -- 64MB (smaller for faster commits)
        'format.version' = '2',
        'write.delete.mode' = 'merge-on-read'
    )

    -- Compaction strategy
    CALL system.rewrite_data_files(
        strategy => 'binpack',
        options => map(
            'target-file-size-bytes', '134217728',       -- 128MB for compaction
            'max-concurrent-file-group-rewrites', '10'   -- Parallel compaction
        )
    )
    """

    print("\n=== Apache Iceberg: File Compaction Strategies ===")

    table_fq = "hadoop_catalog.ubereats.orders_compaction"

    # TODO create table with compaction settings
    print("🏗️ creating table with compaction settings...")
    spark.sql(f"""
              CREATE TABLE IF NOT EXISTS {table_fq}
              (
                  order_id STRING,
                  amount DOUBLE,
                  status STRING
              ) USING iceberg
              TBLPROPERTIES (
                  'write.target-file-size-bytes' = '134217728',
                  'write.delete.target-file-size-bytes' = '67108864'
              )
              """)

    # TODO insert data to trigger compaction
    print("💾 inserting data...")
    spark.sql(f"""
              INSERT INTO {table_fq} VALUES
              ('ORD-001', 25.50, 'completed'),
              ('ORD-002', 18.75, 'pending'),
              ('ORD-003', 32.00, 'processing')
              """)

    # TODO show compaction properties
    print("🔍 compaction properties...")
    spark.sql(f"SHOW TBLPROPERTIES {table_fq}").filter(
        col("key").contains("target-file-size")
    ).show()

    # TODO manual compaction
    print("🔄 manual compaction...")
    spark.sql(f"""
              CALL hadoop_catalog.system.rewrite_data_files(
                  table => '{table_fq}',
                  strategy => 'binpack'
              )
              """)

    print("✅ compaction strategies demonstrated!")


def fanout_writers(spark):
    """Demonstrate Fanout Writers: Parallel Writing Strategies"""

    print("\n=== Apache Iceberg: Fanout Writers ===")

    table_fq = "hadoop_catalog.ubereats.orders_fanout"

    # TODO create table with fanout settings
    print("🏗️ creating table with fanout settings...")
    spark.sql(f"""
              CREATE TABLE IF NOT EXISTS {table_fq}
              (
                  order_id STRING,
                  amount DOUBLE,
                  status STRING,
                  region STRING
              ) USING iceberg
              PARTITIONED BY (region)
              TBLPROPERTIES (
                  'write.distribution-mode' = 'hash'
              )
              """)

    # TODO insert data across multiple partitions
    print("💾 inserting data across regions...")
    spark.sql(f"""
              INSERT INTO {table_fq} VALUES
              ('ORD-001', 25.50, 'completed', 'north'),
              ('ORD-002', 18.75, 'pending', 'south'),
              ('ORD-003', 32.00, 'processing', 'east'),
              ('ORD-004', 45.90, 'completed', 'west')
              """)

    # TODO show partition distribution
    print("🔍 partition distribution...")
    spark.sql(f"SELECT region, COUNT(*) as count FROM {table_fq} GROUP BY region").show()

    # TODO show fanout properties
    print("🔍 fanout properties...")
    spark.sql(f"SHOW TBLPROPERTIES {table_fq}").filter(
        col("key").contains("distribution")
    ).show()

    print("✅ fanout writers demonstrated!")


def manifest_caching(spark):
    """Demonstrate Manifest Caching for Metadata Performance Boost

    sql-- Test query performance with caching
    SELECT
        COUNT(*) as total_orders,
        AVG(amount) as avg_amount,
        MAX(amount) as max_amount
    FROM orders

    -- First execution: Cache miss (slower)
    -- Time: 2.5 seconds

    -- Second execution: Cache hit (faster)
    -- Time: 0.3 seconds (8x improvement)


    Without Manifest Caching:
    Query Execution:
    1. Read metadata.json → S3 read
    2. Read snapshot metadata → S3 read
    3. Read manifest list → S3 read
    4. Read manifest files → S3 read (multiple)
    5. Parse manifest contents → CPU processing
    6. Plan file access → Memory operations

    Total: 4+ S3 reads + parsing time for each query

    With Manifest Caching:
    First Query:
    1. Read metadata.json → S3 read
    2. Read snapshot metadata → S3 read
    3. Read manifest list → S3 read
    4. Read manifest files → S3 read (cached)
    5. Parse manifest contents → CPU processing (cached)
    6. Plan file access → Memory operations

    Subsequent Queries:
    1. Read metadata.json → S3 read
    2. Access cached manifest data → Memory read
    3. Plan file access → Memory operations

    Total: 1 S3 read + memory access (10-100x faster)
    """

    print("\n=== Apache Iceberg: Manifest Caching ===")

    table_fq = "hadoop_catalog.ubereats.orders"

    # TODO enable manifest caching
    print("🔧 enabling manifest caching...")
    spark.conf.set("spark.sql.iceberg.manifest-cache.enabled", "true")
    spark.conf.set("spark.sql.iceberg.manifest-cache.max-size", "104857600")

    # TODO perform query to populate cache
    print("🔍 performing query to populate cache...")
    spark.sql(f"SELECT COUNT(*) as count FROM {table_fq}").show()

    # TODO show cache configuration
    print("🔍 cache configuration...")
    cache_enabled = spark.conf.get("spark.sql.iceberg.manifest-cache.enabled")
    cache_size = spark.conf.get("spark.sql.iceberg.manifest-cache.max-size")

    print(f"   📊 Cache enabled: {cache_enabled}")
    print(f"   📊 Cache max size: {cache_size} bytes")

    # TODO perform same query again (should use cache)
    print("🔍 performing query again (using cache)...")
    spark.sql(f"SELECT COUNT(*) as count FROM {table_fq}").show()

    print("✅ manifest caching demonstrated!")


def table_metrics_collection(spark):
    """Demonstrate Table Metrics Collection"""

    print("\n=== Apache Iceberg: Table Metrics Collection ===")

    table_fq = "hadoop_catalog.ubereats.orders"

    # TODO show table metrics
    print("🔍 table metrics...")
    spark.sql(f"""
              SELECT 
                  COUNT(*) as total_files,
                  SUM(record_count) as total_records,
                  AVG(record_count) as avg_records_per_file,
                  SUM(file_size_in_bytes) as total_size_bytes
              FROM {table_fq}.files
              """).show()

    # TODO show snapshot metrics
    print("🔍 snapshot metrics...")
    spark.sql(f"""
              SELECT 
                  COUNT(*) as total_snapshots,
                  operation,
                  COUNT(*) as operation_count
              FROM {table_fq}.snapshots
              GROUP BY operation
              """).show()

    # TODO show partition metrics
    print("🔍 partition metrics...")
    spark.sql(f"""
              SELECT 
                  COUNT(*) as total_partitions,
                  AVG(record_count) as avg_records_per_partition
              FROM {table_fq}.partitions
              """).show()

    # TODO show table properties
    print("🔍 table properties...")
    spark.sql(f"SHOW TBLPROPERTIES {table_fq}").filter(
        col("key").rlike("(size|count|format)")
    ).show()

    print("✅ table metrics collection demonstrated!")


def cleanup_resources(spark):
    """Clean up demo resources"""

    print("\n=== Cleanup ===")

    try:
        # TODO drop tables
        tables = [
            'hadoop_catalog.ubereats.orders',
            'hadoop_catalog.ubereats.orders_deletes',
            'hadoop_catalog.ubereats.orders_compaction',
            'hadoop_catalog.ubereats.orders_fanout'
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

    print("🚀 Starting Apache Iceberg Demo 8: Maintenance & Optimization")
    print("=" * 70)

    # TODO create Spark session
    spark = spark_session()

    try:
        # TODO run demo sections
        setup_namespace(spark)
        rewrite_data_files(spark)
        file_compaction_strategies(spark)
        fanout_writers(spark)
        manifest_caching(spark)
        table_metrics_collection(spark)

        print("\n" + "=" * 70)
        print("🎉 Demo 8 completed successfully!")
        print("📚 Key concepts covered:")
        print("   ✓ Data files compaction and rewriting")
        print("   ✓ Snapshot expiration and cleanup")
        print("   ✓ Orphaned files removal")
        print("   ✓ Manifest optimization")
        print("   ✓ Position delete files management")
        print("   ✓ File compaction strategies")
        print("   ✓ Fanout writers for parallel processing")
        print("   ✓ Manifest caching for performance")
        print("   ✓ Table metrics and monitoring")

        print("\n🔗 What's Next:")
        print("   → Production deployment and monitoring")

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
