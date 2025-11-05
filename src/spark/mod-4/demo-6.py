"""
Demo 6: Performance Optimization
==============================

This demo covers:
- OPTIMIZE & Z-ORDER operations
- AQE (Adaptive Query Execution) tuning
- Partition Operations and strategies

Run command:
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/bitnami/spark/jobs/spark/mod-6/demo-6.py
"""

import base64
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, rand, expr, date_format
import time


def spark_session():
    """Create Spark Session with optimized configurations"""

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
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.adaptive.localShuffleReader.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark


def generate_large_dataset(spark, num_records=500000):
    """Generate large synthetic UberEats dataset for performance testing"""

    print(f"🏗️  Generating large dataset with {num_records:,} records...")
    start_time = time.time()

    # TODO Generate comprehensive UberEats data with realistic distributions
    large_dataset = spark.range(num_records).select(

        expr("concat('ORD', lpad(cast(id as string), 8, '0'))").alias("order_id"),
        expr("concat('USER', cast((id % 10000) as string))").alias("user_key"),
        expr("concat('REST', cast((id % 500) as string))").alias("restaurant_key"),
        expr("concat('DRIVER', cast((id % 2000) as string))").alias("driver_key"),

        (rand() * 80 + 15).alias("total_amount"),
        (rand() * 8 + 2).alias("delivery_fee"),
        (rand() * 5 + 1).alias("service_fee"),

        expr("date_add('2024-01-01', cast(rand() * 365 as int))").alias("order_date"),
        expr("array('placed', 'confirmed', 'preparing', 'out_for_delivery', 'delivered')[cast(rand() * 5 as int)]").alias("status"),

        expr("array('São Paulo', 'Rio de Janeiro', 'Belo Horizonte', 'Salvador', 'Brasília')[cast(rand() * 5 as int)]").alias("city"),
        expr("array('Italian', 'Brazilian', 'Japanese', 'Mexican', 'Chinese', 'American', 'Indian')[cast(rand() * 7 as int)]").alias("cuisine_type"),

        when(col("id") % 100 == 0, col("id") * 10).otherwise(col("id") % 1000).alias("skewed_column"),

        expr("concat('EMAIL', cast(id as string), '@domain', cast((id % 10) as string), '.com')").alias("customer_email"),
        expr("concat('PHONE', lpad(cast((id % 100000000) as string), 10, '0'))").alias("phone_number"),

        date_format(expr("date_add('2024-01-01', cast(rand() * 365 as int))"), "yyyy").alias("year"),
        date_format(expr("date_add('2024-01-01', cast(rand() * 365 as int))"), "MM").alias("month"),
        date_format(expr("date_add('2024-01-01', cast(rand() * 365 as int))"), "yyyy-MM").alias("year_month")
    )

    generation_time = time.time() - start_time
    print(f"✅ Dataset generated in {generation_time:.2f} seconds")

    return large_dataset


def optimize_zorder(spark):
    """Demo: OPTIMIZE and Z-ORDER operations"""

    lk_orders_path = "s3a://owshq-uber-eats-lakehouse/performance/orders_o"

    # TODO Generate large dataset for optimization testing
    orders_df = generate_large_dataset(spark, 500000)

    orders_df.write.format("delta").mode("overwrite").save(lk_orders_path)
    print(f"✅ Created orders table with {orders_df.count()} records")

    # TODO Check table details before optimization
    print("\n Table details before optimization:")
    spark.sql(f"DESCRIBE DETAIL delta.`{lk_orders_path}`").select("numFiles", "sizeInBytes").show()

    # TODO OPTIMIZE without Z-ORDER
    print("🔧 Running OPTIMIZE...")
    start_time = time.time()
    spark.sql(f"OPTIMIZE delta.`{lk_orders_path}`")
    optimize_time = time.time() - start_time
    print(f"✅ OPTIMIZE completed in {optimize_time:.2f} seconds")

    # TODO OPTIMIZE with Z-ORDER
    print("Running OPTIMIZE with Z-ORDER on user_key, restaurant_key...")
    start_time = time.time()
    spark.sql(f"OPTIMIZE delta.`{lk_orders_path}` ZORDER BY (user_key, restaurant_key)")
    zorder_time = time.time() - start_time
    print(f"✅ Z-ORDER completed in {zorder_time:.2f} seconds")

    # TODO Show improvements
    print("\n📊 Table details after optimization:")
    spark.sql(f"DESCRIBE DETAIL delta.`{lk_orders_path}`").select("numFiles", "sizeInBytes").show()

    # TODO Test query performance
    print("⚡ Testing query performance:")
    start_time = time.time()
    result = spark.sql(f"""
        SELECT restaurant_key, COUNT(*), AVG(total_amount)
        FROM delta.`{lk_orders_path}`
        WHERE user_key IN ('USER123', 'USER456', 'USER789')
        GROUP BY restaurant_key
    """).collect()
    query_time = time.time() - start_time
    print(f"✅ Query completed in {query_time:.3f} seconds")

    return spark.read.format("delta").load(lk_orders_path)


def aqe_optimization(spark):
    """Demo: Adaptive Query Execution tuning"""

    lk_large_table_path = "s3a://owshq-uber-eats-lakehouse/performance/orders_l"

    # TODO Generate larger dataset for AQE demonstration
    large_orders = generate_large_dataset(spark, 500000)

    large_orders.write.format("delta").mode("overwrite").save(lk_large_table_path)
    print(f"✅ Created large table with {large_orders.count()} records")

    print("⚡ Testing AQE optimizations:")

    # TODO Coalesce partitions
    print("📦 AQE Coalesce Partitions:")
    start_time = time.time()
    result1 = spark.sql(f"""
        SELECT restaurant_key, COUNT(*) as order_count
        FROM delta.`{lk_large_table_path}`
        GROUP BY restaurant_key
        HAVING COUNT(*) > 50
    """).collect()
    aqe_time = time.time() - start_time
    print(f"✅ AQE query completed in {aqe_time:.3f} seconds")

    # TODO Skew join optimization
    print("⚖️  AQE Skew Join Handling:")
    start_time = time.time()
    result2 = spark.sql(f"""
        SELECT a.restaurant_key, COUNT(*) as total_orders, AVG(a.total_amount) as avg_amount
        FROM delta.`{lk_large_table_path}` a
        JOIN (
            SELECT DISTINCT restaurant_key 
            FROM delta.`{lk_large_table_path}` 
            WHERE total_amount > 50
        ) b ON a.restaurant_key = b.restaurant_key
        GROUP BY a.restaurant_key
    """).collect()
    skew_time = time.time() - start_time
    print(f"✅ Skew join handled in {skew_time:.3f} seconds")

    # TODO Show AQE statistics
    print("AQE Configuration:")
    aqe_configs = [
        "spark.sql.adaptive.enabled",
        "spark.sql.adaptive.coalescePartitions.enabled",
        "spark.sql.adaptive.skewJoin.enabled",
        "spark.sql.adaptive.localShuffleReader.enabled"
    ]

    for config in aqe_configs:
        value = spark.conf.get(config, "not set")
        print(f"   • {config}: {value}")

    return spark.read.format("delta").load(lk_large_table_path)


def partition_operations(spark):
    """Demo: Partition strategies and operations"""

    lk_partitioned_path = "s3a://owshq-uber-eats-lakehouse/performance/orders_p"

    # TODO Generate partitioned dataset with proper date distribution
    orders_with_date = generate_large_dataset(spark, 150000).select(
        col("order_id"),
        col("user_key"),
        col("total_amount"),
        col("order_date"),
        col("year_month"),
        col("city"),
        col("status")
    )

    # TODO Write with partitioning
    print("📁 Creating partitioned table by year_month...")
    orders_with_date.write.format("delta") \
        .partitionBy("year_month") \
        .mode("overwrite") \
        .save(lk_partitioned_path)

    print("✅ Partitioned table created")

    # TODO Test partition pruning
    print("🎯 Testing partition pruning:")
    start_time = time.time()
    result = spark.sql(f"""
        SELECT year_month, COUNT(*) as orders, AVG(total_amount) as avg_amount
        FROM delta.`{lk_partitioned_path}`
        WHERE year_month IN ('2024-06', '2024-07', '2024-08')
        GROUP BY year_month
        ORDER BY year_month
    """)
    result.show()
    partition_time = time.time() - start_time
    print(f"✅ Partition pruning query completed in {partition_time:.3f} seconds")

    # TODO Show partition information using Delta Lake methods
    print("Partition analysis:")

    # TODO Method 1: Count records per partition
    partition_counts = spark.sql(f"""
        SELECT year_month, COUNT(*) as record_count
        FROM delta.`{lk_partitioned_path}`
        GROUP BY year_month
        ORDER BY year_month
    """)
    print("📈 Records per partition:")
    partition_counts.show(12)

    # TODO Method 2: Use DESCRIBE DETAIL to see partition structure
    try:
        detail_df = spark.sql(f"DESCRIBE DETAIL delta.`{lk_partitioned_path}`")
        partition_columns = detail_df.select("partitionColumns").collect()[0][0]
        num_files = detail_df.select("numFiles").collect()[0][0]

        print(f"📊 Table details:")
        print(f"   • Partition columns: {partition_columns}")
        print(f"   • Total files: {num_files}")
        print(f"   • Partitions created: {partition_counts.count()}")

    except Exception as e:
        print(f"⚠️  Could not retrieve detailed partition info: {str(e)[:50]}...")

    # TODO Test query performance comparison
    print("\n⚡ Comparing partitioned vs non-partitioned queries:")

    # TODO Partitioned query (should be fast)
    start_time = time.time()
    partitioned_result = spark.sql(f"""
        SELECT city, COUNT(*) as orders, AVG(total_amount) as avg_amount
        FROM delta.`{lk_partitioned_path}`
        WHERE year_month = '2024-06'
        GROUP BY city
    """).collect()
    partitioned_time = time.time() - start_time
    print(f"✅ Partitioned query (single partition) completed in {partitioned_time:.3f} seconds")

    # TODO Full table scan query (should be slower)
    start_time = time.time()
    full_scan_result = spark.sql(f"""
        SELECT status, COUNT(*) as orders
        FROM delta.`{lk_partitioned_path}`
        WHERE total_amount > 50.0
        GROUP BY status
    """).collect()
    full_scan_time = time.time() - start_time
    print(f"⚡ Full table scan query completed in {full_scan_time:.3f} seconds")

    # TODO Performance analysis
    if partitioned_time > 0:
        speedup = full_scan_time / partitioned_time
        print(f"Partition pruning speedup: {speedup:.1f}x faster")

    print("\n💡 Partitioning benefits demonstrated:")
    print("   • Partition pruning reduces data scanning")
    print("   • Query performance improves for partition-filtered queries")
    print("   • File organization enables efficient data access")

    return spark.read.format("delta").load(lk_partitioned_path)


def main():
    """Main execution function"""

    spark = spark_session()

    try:

        # TODO Run performance optimization demos
        optimize_zorder(spark)
        aqe_optimization(spark)
        partition_operations(spark)

        print("✅ OPTIMIZE & Z-ORDER: File compaction and data layout optimization")
        print("✅ AQE: Adaptive query execution for dynamic optimization")
        print("✅ Partitioning: Strategic data organization for query pruning")

    except Exception as e:
        print(f"❌ Demo failed: {e}")
        import traceback
        traceback.print_exc()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
