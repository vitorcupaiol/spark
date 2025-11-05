"""
Apache Iceberg Demo 4: Advanced Partitioning & Performance
==========================================================

This demo covers:
- Apache Iceberg: Hidden Partitioning
- Apache Iceberg: Partition Evolution
- Apache Iceberg: ALTER TABLE Add Partition Field
- Apache Iceberg: ALTER TABLE Drop Partition Field
- Apache Iceberg: Partition Transform Functions: bucket, truncate, year, month
- Apache Iceberg: Partition Pruning
- Apache Iceberg: Sort Orders
- Apache Iceberg: Write Distribution Modes: Hash, Range e None

Run with:
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/bitnami/spark/jobs/spark/mod-4/demo-10.py

Hidden Partitioning Example Output:
s3a://owshq-catalog/warehouse/ubereats/orders_partitioned/
├── data/
│   ├── order_date_month=2024-01/
│   │   ├── 00000-0-data.parquet
│   │   └── 00001-0-data.parquet
│   ├── order_date_month=2024-02/
│   │   └── 00000-0-data.parquet
│   └── order_date_month=2024-03/
│       └── 00000-0-data.parquet
└── metadata/
    ├── metadata.json
    └── snap-123.avro

# does work with partition pruning
SELECT * FROM orders WHERE order_date >= '2024-02-01' AND order_date < '2024-03-01'

# does not work with partition pruning
SELECT * FROM orders WHERE user_id = 1001
SELECT * FROM orders WHERE total_amount > 50.00

-- ✅ Works on both old and new data [hidden partitioning]
SELECT * FROM orders WHERE order_date >= '2024-01-01'

-- ✅ New partition field provides additional pruning for new data
SELECT * FROM orders WHERE restaurant_id = 5
-- Old data: Scans all files in 2024-01/, 2024-02/
-- New data: Only scans restaurant_id_bucket=1/ within 2024-04/

BEFORE (Spec ID: 0):
data/
├── order_date_month=2024-01/
└── order_date_month=2024-02/

AFTER (Spec ID: 1):
data/
├── order_date_month=2024-01/              # Old data unchanged
├── order_date_month=2024-02/              # Old data unchanged
└── order_date_month=2024-04/              # New data uses both partitions
    ├── restaurant_id_bucket=0/
    ├── restaurant_id_bucket=1/
    ├── restaurant_id_bucket=2/
    └── restaurant_id_bucket=3/

# Add Partition Field Example Output:
INITIAL (unpartitioned):
data/
└── 00000-0-data.parquet

AFTER adding category:
data/
├── category=Italian/
├── category=American/
└── category=Japanese/

AFTER adding year(created_date):
data/
├── category=Italian/
│   ├── created_date_year=2024/
│   └── created_date_year=2025/
├── category=American/
│   └── created_date_year=2024/
└── category=Japanese/
    └── created_date_year=2024/

-- ✅ Efficient after adding category partition
SELECT * FROM products WHERE category = 'Italian'
-- Only scans: category=Italian/ folder

-- ✅ Efficient after adding year partition
SELECT * FROM products WHERE created_date >= '2024-01-01'
-- Only scans: */created_date_year=2024/ folders

-- ✅ Super-efficient with both partitions
SELECT * FROM products WHERE category = 'Italian' AND created_date >= '2024-01-01'
-- Only scans: category=Italian/created_date_year=2024/
"""

import base64
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.functions import col, current_timestamp, year, month, date_format, expr


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


def hidden_partitioning(spark):
    """Demonstrate Apache Iceberg Hidden Partitioning"""

    print("\n=== Apache Iceberg: Hidden Partitioning ===")

    # TODO set the correct catalog and database
    spark.catalog.setCurrentCatalog("hadoop_catalog")
    spark.catalog.setCurrentDatabase("ubereats")
    table_fq = "hadoop_catalog.ubereats.orders_partitioned"

    # TODO create table with hidden partitioning
    print("🏗️ creating orders table with hidden partitioning...")
    spark.sql(f"""
              CREATE TABLE IF NOT EXISTS {table_fq}
              (
                  order_id STRING,
                  user_id INT,
                  restaurant_id INT,
                  total_amount DOUBLE,
                  order_date TIMESTAMP,
                  status STRING
              ) USING iceberg
              PARTITIONED BY (month(order_date))
              TBLPROPERTIES (
                  'write.format.default' = 'parquet',
                  'write.parquet.compression-codec' = 'snappy'
              )
              """)

    # TODO insert sample data across different months
    print("💾 inserting data across different months...")
    spark.sql(f"""
              INSERT INTO {table_fq} VALUES
              ('ORD-001', 1001, 1, 45.50, TIMESTAMP('2024-01-15 10:30:00'), 'completed'),
              ('ORD-002', 1002, 2, 32.75, TIMESTAMP('2024-02-20 14:15:00'), 'pending'),
              ('ORD-003', 1003, 3, 28.90, TIMESTAMP('2024-03-10 16:45:00'), 'completed'),
              ('ORD-004', 1004, 1, 55.25, TIMESTAMP('2024-01-25 12:00:00'), 'processing'),
              ('ORD-005', 1005, 2, 41.80, TIMESTAMP('2024-02-28 18:30:00'), 'completed')
              """)


    # TODO show partition structure
    print("🔍 showing partition structure...")
    spark.sql(f"SELECT * FROM {table_fq}.partitions").show()

    # TODO demonstrate query without partition specification
    print("🔍 querying without specifying partitions...")
    spark.sql(f"SELECT * FROM {table_fq} WHERE order_date >= '2024-02-01'").show()

    print("✅ hidden partitioning demonstrated!")


def partition_evolution(spark):
    """Demonstrate Apache Iceberg Partition Evolution"""

    print("\n=== Apache Iceberg: Partition Evolution ===")

    table_fq = "hadoop_catalog.ubereats.orders_partitioned"

    # TODO Show current partition spec via snapshots/history
    print("🔍 current partition specs in history:")
    spark.sql(f"SELECT * FROM {table_fq}.history").show(truncate=False)
    print("📋 current snapshots:")
    spark.sql(f"SELECT snapshot_id, committed_at, operation, summary FROM {table_fq}.snapshots ORDER BY committed_at").show(truncate=False)

    # TODO Add new partition field
    print("🔧 adding restaurant_id as partition field...")
    spark.sql(f"ALTER TABLE {table_fq} ADD PARTITION FIELD bucket(4, restaurant_id)")

    # TODO Insert new data
    print("💾 inserting new data with evolved partitioning...")
    spark.sql(f"""
        INSERT INTO {table_fq} VALUES
        ('ORD-006', 1006, 5, 67.30, TIMESTAMP('2024-04-05 11:15:00'), 'completed'),
        ('ORD-007', 1007, 6, 43.90, TIMESTAMP('2024-04-10 13:45:00'), 'processing')
    """)

    # TODO Show partition specs again
    print("🔍 updated partition specs in history:")
    spark.sql(f"SELECT * FROM {table_fq}.history").show(truncate=False)

    # TODO Query data using both old and new partition fields
    print("🔍 querying by month(order_date):")
    spark.sql(f"SELECT * FROM {table_fq} WHERE month(order_date) = 4").show()
    print("🔍 querying by restaurant_id bucket:")
    spark.sql(f"SELECT * FROM {table_fq} WHERE restaurant_id = 5").show()

    print("✅ partition evolution demonstrated!")


def alter_table_add_partition(spark):
    """Demonstrate ALTER TABLE Add Partition Field"""

    print("\n=== Apache Iceberg: ALTER TABLE Add Partition Field ===")

    table_fq = "hadoop_catalog.ubereats.products"

    # TODO create simple table for demonstration
    print("🏗️ creating products table...")
    spark.sql(f"""
              CREATE TABLE IF NOT EXISTS {table_fq}
              (
                  product_id INT,
                  name STRING,
                  category STRING,
                  price DOUBLE,
                  created_date TIMESTAMP
              ) USING iceberg
              """)

    # TODO insert initial data
    print("💾 inserting initial data...")
    spark.sql(f"""
              INSERT INTO {table_fq} VALUES
              (1, 'Pizza Margherita', 'Italian', 25.90, TIMESTAMP('2024-01-01 00:00:00')),
              (2, 'Burger Classic', 'American', 18.50, TIMESTAMP('2024-01-02 00:00:00')),
              (3, 'Sushi Roll', 'Japanese', 35.00, TIMESTAMP('2024-01-03 00:00:00'))
              """)

    # TODO show initial partition spec (none)
    print("🔍 initial partition specification...")
    spark.sql(f"SELECT * FROM {table_fq}.partitions").show()

    # TODO add partition field by category
    print("🔧 adding category as partition field...")
    spark.sql(f"ALTER TABLE {table_fq} ADD PARTITION FIELD category")

    # TODO add another partition field by year
    print("🔧 adding year(created_date) as partition field...")
    spark.sql(f"ALTER TABLE {table_fq} ADD PARTITION FIELD year(created_date)")

    # TODO show updated partition spec
    print("🔍 updated partition specification...")
    spark.sql(f"SELECT * FROM {table_fq}.partitions").show()

    print("✅ add partition field demonstrated!")


def alter_table_drop_partition(spark):
    """Demonstrate ALTER TABLE Drop Partition Field"""

    print("\n=== Apache Iceberg: ALTER TABLE Drop Partition Field ===")

    table_fq = "hadoop_catalog.ubereats.products"

    # TODO show current partitions
    print("🔍 current partition fields...")
    spark.sql(f"SELECT * FROM {table_fq}.partitions").show()

    # TODO drop category partition field
    print("🗑️ dropping category partition field...")
    spark.sql(f"ALTER TABLE {table_fq} DROP PARTITION FIELD category")

    # TODO show updated partitions
    print("🔍 partition fields after drop...")
    spark.sql(f"SELECT * FROM {table_fq}.partitions").show()

    # TODO insert more data to verify
    print("💾 inserting data after partition drop...")
    spark.sql(f"""
              INSERT INTO {table_fq} VALUES
              (4, 'Tacos', 'Mexican', 22.00, TIMESTAMP('2024-02-01 00:00:00'))
              """)

    print("✅ drop partition field demonstrated!")


def partition_transform_functions(spark):
    """Demonstrate Partition Transform Functions

        -- Extract year from timestamp
        year(activity_date)     → 2024, 2025, etc.

        -- Extract month from timestamp
        month(activity_date)    → 2024-01, 2024-02, etc.

        -- Extract day from timestamp
        day(activity_date)      → 2024-01-01, 2024-01-02, etc.

        -- Extract hour from timestamp
        hour(activity_date)     → 2024-01-01-14, 2024-01-01-15, etc.

        -- Distribute user_id across 10 buckets
        bucket(10, user_id)
        -- user_id=12345 → bucket 5 (12345 % 10 = 5)
        -- user_id=67890 → bucket 0 (67890 % 10 = 0)

        truncate(3, region)
        -- 'north_america' → 'nor'
        -- 'south_europe' → 'sou'
        -- 'east_asia' → 'eas'
    """

    print("\n=== Apache Iceberg: Partition Transform Functions ===")

    table_fq = "hadoop_catalog.ubereats.user_activity"

    # Create table with non-redundant partition transforms
    print("🏗️ creating table with transform functions...")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_fq}
        (
            user_id INT,
            activity_type STRING,
            amount DOUBLE,
            activity_date TIMESTAMP,
            region STRING
        ) USING iceberg
        PARTITIONED BY (
            month(activity_date),
            bucket(10, user_id),
            truncate(3, region)
        )
    """)

    # Insert sample data
    print("💾 inserting sample data...")
    spark.sql(f"""
        INSERT INTO {table_fq} VALUES
        (12345, 'order', 45.50, TIMESTAMP('2024-01-15 10:30:00'), 'north'),
        (67890, 'payment', 32.75, TIMESTAMP('2024-02-20 14:15:00'), 'south'),
        (11111, 'review', 0.00, TIMESTAMP('2024-01-25 16:45:00'), 'east'),
        (22222, 'order', 28.90, TIMESTAMP('2024-03-10 12:00:00'), 'west'),
        (33333, 'payment', 55.25, TIMESTAMP('2024-02-28 18:30:00'), 'north')
    """)

    # Show partition structure
    print("🔍 partition structure with transforms...")
    spark.sql(f"SELECT * FROM {table_fq}.partitions").show()

    # Demonstrate each transform
    print("🔍 month transform - data by month...")
    spark.sql(f"SELECT month(activity_date) as month, COUNT(*) FROM {table_fq} GROUP BY month(activity_date)").show()

    print("🔍 bucket transform - user distribution...")
    spark.sql(f"SELECT user_id, user_id % 10 as bucket FROM {table_fq}").show()

    print("🔍 truncate transform - region prefixes...")
    spark.sql(f"SELECT region, substring(region, 1, 3) as truncated FROM {table_fq}").show()

    print("✅ partition transform functions demonstrated!")


def partition_pruning(spark):
    """Demonstrate Partition Pruning

    SELECT * FROM user_activity
    WHERE activity_date >= '2024-02-01'
      AND user_id = 12345
      AND region LIKE 'north%'

    -- Pruning Result:
    -- ✅ activity_date_month=2024-02/user_id_bucket=5/region_trunc=nor/
    -- ❌ All other partition combinations

    SELECT * FROM user_activity
    WHERE user_id IN (12345, 67890, 11111)

    -- Pruning Result (bucket function):
    -- ✅ user_id_bucket=0/  # 67890 % 10 = 0
    -- ✅ user_id_bucket=1/  # 11111 % 10 = 1
    -- ✅ user_id_bucket=5/  # 12345 % 10 = 5
    -- ❌ user_id_bucket=2,3,4,6,7,8,9/
    """

    print("\n=== Apache Iceberg: Partition Pruning ===")

    table_fq = "hadoop_catalog.ubereats.user_activity"

    # TODO enable query metrics
    spark.conf.set("spark.sql.adaptive.enabled", "true")

    # TODO query with partition pruning
    print("🔍 query with partition pruning (year = 2024)...")
    result = spark.sql(f"SELECT * FROM {table_fq} WHERE activity_date >= '2024-01-01' AND activity_date < '2025-01-01'")
    result.show()

    # TODO query with multiple partition pruning
    print("🔍 query with multiple partition pruning...")
    result = spark.sql(f"""
                       SELECT * FROM {table_fq} 
                       WHERE year(activity_date) = 2024 
                       AND month(activity_date) = 2
                       """)
    result.show()

    # TODO show files accessed
    print("🔍 showing files accessed...")
    spark.sql(f"SELECT file_path, record_count FROM {table_fq}.files").show(truncate=False)

    print("✅ partition pruning demonstrated!")


def sort_orders(spark):
    """Demonstrate Sort Orders"""

    print("\n=== Apache Iceberg: Sort Orders ===")

    table_fq = "hadoop_catalog.ubereats.sorted_orders"

    # TODO create table with sort order
    print("🏗️ creating table with sort order...")
    spark.sql(f"""
              CREATE TABLE IF NOT EXISTS {table_fq}
              (
                  order_id STRING,
                  user_id INT,
                  restaurant_id INT,
                  total_amount DOUBLE,
                  order_date TIMESTAMP
              ) USING iceberg
              PARTITIONED BY (month(order_date))
              TBLPROPERTIES (
                  'write.format.default' = 'parquet',
                  'sort.order' = 'user_id ASC, total_amount DESC'
              )
              """)

    # TODO insert unsorted data
    print("💾 inserting unsorted data...")
    spark.sql(f"""
              INSERT INTO {table_fq} VALUES
              ('ORD-010', 1003, 1, 15.50, TIMESTAMP('2024-01-15 10:30:00')),
              ('ORD-011', 1001, 2, 45.75, TIMESTAMP('2024-01-15 11:00:00')),
              ('ORD-012', 1002, 3, 32.90, TIMESTAMP('2024-01-15 11:30:00')),
              ('ORD-013', 1001, 1, 67.25, TIMESTAMP('2024-01-15 12:00:00')),
              ('ORD-014', 1003, 2, 28.80, TIMESTAMP('2024-01-15 12:30:00'))
              """)

    # TODO show data (should be sorted)
    print("🔍 showing sorted data...")
    spark.sql(f"SELECT * FROM {table_fq} ORDER BY user_id, total_amount DESC").show()

    # TODO show sort order properties
    print("🔍 showing sort order properties...")
    spark.sql(f"SHOW TBLPROPERTIES {table_fq}").filter(col("key").contains("sort")).show()

    print("✅ sort orders demonstrated!")


def write_distribution_modes(spark):
    """Demonstrate Write Distribution Modes
    1. Hash Distribution Mode:
    sql-- How it works:
    -- 1. Calculates hash of partition columns
    -- 2. Distributes records evenly across Spark tasks
    -- 3. Each task writes to multiple partitions

    Write Process:
    Input: 1000 orders across 3 months
    ↓
    Hash distribution across 4 Spark tasks:
    Task 1: 250 orders → writes to month=2024-01/, 2024-02/, 2024-03/
    Task 2: 250 orders → writes to month=2024-01/, 2024-02/, 2024-03/
    Task 3: 250 orders → writes to month=2024-01/, 2024-02/, 2024-03/
    Task 4: 250 orders → writes to month=2024-01/, 2024-02/, 2024-03/

    2. Range Distribution Mode:
    sql-- How it works:
    -- 1. Sorts data by partition and sort columns
    -- 2. Distributes ranges of sorted data to tasks
    -- 3. Each task typically writes to fewer partitions

    Write Process:
    Input: 1000 orders sorted by month, then user_id
    ↓
    Range distribution:
    Task 1: month=2024-01 + early user_ids (1001-1250)
    Task 2: month=2024-01 + later user_ids (1251-1500) + month=2024-02 early users
    Task 3: month=2024-02 + month=2024-03 early users
    Task 4: month=2024-03 later users

    3. None Distribution Mode:
    sql-- How it works:
    -- 1. No special distribution strategy
    -- 2. Data written as-is from Spark tasks
    -- 3. May result in uneven partition sizes

    Write Process:
    Input: 1000 orders in random order
    ↓
    No redistribution:
    Task 1: Writes whatever data it has
    Task 2: Writes whatever data it has
    Task 3: Writes whatever data it has
    Task 4: Writes whatever data it has
    """

    print("\n=== Apache Iceberg: Write Distribution Modes ===")

    # TODO define table names
    table_hash = "hadoop_catalog.ubereats.orders_hash"
    table_range = "hadoop_catalog.ubereats.orders_range"
    table_none = "hadoop_catalog.ubereats.orders_none"

    # TODO create tables with different distribution modes
    print("🏗️ creating tables with different distribution modes...")

    # Hash distribution
    spark.sql(f"""
              CREATE TABLE IF NOT EXISTS {table_hash}
              (
                  order_id STRING,
                  user_id INT,
                  restaurant_id INT,
                  total_amount DOUBLE,
                  order_date TIMESTAMP
              ) USING iceberg
              PARTITIONED BY (month(order_date))
              TBLPROPERTIES (
                  'write.distribution-mode' = 'hash'
              )
              """)

    # Range distribution
    spark.sql(f"""
              CREATE TABLE IF NOT EXISTS {table_range}
              (
                  order_id STRING,
                  user_id INT,
                  restaurant_id INT,
                  total_amount DOUBLE,
                  order_date TIMESTAMP
              ) USING iceberg
              PARTITIONED BY (month(order_date))
              TBLPROPERTIES (
                  'write.distribution-mode' = 'range'
              )
              """)

    # None distribution
    spark.sql(f"""
              CREATE TABLE IF NOT EXISTS {table_none}
              (
                  order_id STRING,
                  user_id INT,
                  restaurant_id INT,
                  total_amount DOUBLE,
                  order_date TIMESTAMP
              ) USING iceberg
              PARTITIONED BY (month(order_date))
              TBLPROPERTIES (
                  'write.distribution-mode' = 'none'
              )
              """)

    # TODO insert same data to all tables
    print("💾 inserting data to all distribution modes...")
    sample_data = """
        ('ORD-020', 1001, 1, 45.50, TIMESTAMP('2024-01-15 10:30:00')),
        ('ORD-021', 1002, 2, 32.75, TIMESTAMP('2024-01-15 11:00:00')),
        ('ORD-022', 1003, 3, 28.90, TIMESTAMP('2024-01-15 11:30:00'))
    """

    for table in [table_hash, table_range, table_none]:
        spark.sql(f"INSERT INTO {table} VALUES {sample_data}")

    # TODO show table properties
    print("🔍 showing distribution mode properties...")
    tables = [
        ('orders_hash', table_hash),
        ('orders_range', table_range),
        ('orders_none', table_none)
    ]

    for name, table_fq in tables:
        print(f"\n--- {name} properties ---")
        spark.sql(f"SHOW TBLPROPERTIES {table_fq}").filter(col("key").contains("distribution")).show()

    print("✅ write distribution modes demonstrated!")


def cleanup_resources(spark):
    """Clean up demo resources"""

    print("\n=== Cleanup ===")

    try:
        # TODO drop tables with fully qualified names
        tables = [
            'hadoop_catalog.ubereats.orders_partitioned',
            'hadoop_catalog.ubereats.products',
            'hadoop_catalog.ubereats.user_activity',
            'hadoop_catalog.ubereats.sorted_orders',
            'hadoop_catalog.ubereats.orders_hash',
            'hadoop_catalog.ubereats.orders_range',
            'hadoop_catalog.ubereats.orders_none'
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

    print("🚀 Starting Apache Iceberg Demo 4: Advanced Partitioning & Performance")
    print("=" * 80)

    # TODO create Spark session
    spark = spark_session()

    try:
        # TODO run demo sections
        setup_namespace(spark)
        hidden_partitioning(spark)
        partition_evolution(spark)
        alter_table_add_partition(spark)
        alter_table_drop_partition(spark)
        partition_transform_functions(spark)
        partition_pruning(spark)
        sort_orders(spark)
        write_distribution_modes(spark)

        print("\n" + "=" * 80)
        print("🎉 Demo 4 completed successfully!")
        print("📚 Key concepts covered:")
        print("   ✓ Hidden Partitioning with month() transform")
        print("   ✓ Partition Evolution with ALTER TABLE")
        print("   ✓ Adding and dropping partition fields")
        print("   ✓ Transform functions: bucket, truncate, year, month")
        print("   ✓ Partition pruning optimization")
        print("   ✓ Sort orders for better performance")
        print("   ✓ Write distribution modes: Hash, Range, None")

        print("\n🔗 What's Next:")
        print("   → Demo 5: Time Travel & Snapshot Management")
        print("   → Demo 6: Schema Evolution & Data Quality")

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
