"""
Demo 1: Delta Lake Foundation & Setup
==================================

This demo covers:
- Delta Lake + Spark Setup
- Spark Session Configuration
- Table Creation (DataFrame API & Spark SQL)
- Read & Write Operations
- Converting Parquet to Delta Lake Tables

Run command:
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/bitnami/spark/jobs/spark/mod-4/demo-1.py
"""

import base64
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pyspark.sql.functions import col, current_timestamp


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
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark


def table_creation_api(spark):
    """Demo: Creating Delta Lake tables using DataFrame API"""

    restaurants_path = "s3a://owshq-shadow-traffic-uber-eats/mysql/restaurants/01JTKHGDFWAZPJ8BDQRHCGVPD3.jsonl"
    restaurants_df = spark.read.json(restaurants_path)

    print(f"✅ Loaded {restaurants_df.count()} restaurants")
    restaurants_df.show(3, truncate=False)

    lk_restaurants_path = "s3a://owshq-uber-eats-lakehouse/bronze/restaurants"
    restaurants_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save(lk_restaurants_path)

    lk_restaurants_df = spark.read.format("delta").load(lk_restaurants_path)
    print(f"✅ Delta table created with {lk_restaurants_df.count()} records")
    lk_restaurants_df.select("name", "cuisine_type", "city", "average_rating").show(5)

    return lk_restaurants_df


def table_creation_sql(spark):
    """Demo: Creating Delta Lake tables using Spark SQL"""

    drivers_path = "s3a://owshq-shadow-traffic-uber-eats/postgres/drivers/01JTKHGDF752KGHTSHSRG2HA41.jsonl"
    drivers_df = spark.read.json(drivers_path)

    drivers_df.createOrReplaceTempView("vw_drivers")

    lk_drivers_path = "s3a://owshq-uber-eats-lakehouse/bronze/drivers"

    spark.sql(f"""
        CREATE OR REPLACE TABLE drivers
        USING DELTA
        LOCATION '{lk_drivers_path}'
        AS SELECT 
            driver_id,
            first_name,
            last_name,
            license_number,
            vehicle_type,
            vehicle_make,
            vehicle_model,
            vehicle_year,
            city,
            country,
            date_birth,
            phone_number,
            uuid,
            dt_current_timestamp
        FROM vw_drivers
    """)

    result = spark.sql("""
        SELECT 
            vehicle_type,
            COUNT(*) as driver_count,
            AVG(vehicle_year) as avg_vehicle_year
        FROM drivers 
        GROUP BY vehicle_type 
        ORDER BY driver_count DESC
    """)

    print("🔍 Drivers by vehicle type:")
    result.show()

    return spark.read.format("delta").load(lk_drivers_path)


def read_write_operations(spark):
    """Demo: Delta Lake read and write operations"""

    orders_path = "s3a://owshq-shadow-traffic-uber-eats/kafka/orders/01JTKHGH9M9JCGTMP591VP7C2Z.jsonl"
    orders_df = spark.read.json(orders_path)

    lk_orders_path = "s3a://owshq-uber-eats-lakehouse/bronze/orders"

    # TODO overwrite existing orders table if it exists
    print("💾 Writing orders with OVERWRITE mode...")
    orders_df.write \
        .format("delta") \
        .mode("overwrite") \
        .save(lk_orders_path)

    initial_count = spark.read.format("delta").load(lk_orders_path).count()
    print(f"✅ Initial record count: {initial_count}")

    print("➕ Appending new orders...")
    new_orders_df = orders_df.limit(10).withColumn("total_amount", col("total_amount") + 100)

    # TODO append new orders to existing Delta table
    new_orders_df.write \
        .format("delta") \
        .mode("append") \
        .save(lk_orders_path)

    final_count = spark.read.format("delta").load(lk_orders_path).count()
    print(f"✅ Final record count after append: {final_count}")

    filtered_df = spark.read.format("delta").load(lk_orders_path).filter(col("total_amount") > 100)

    print(f"📈 Orders with amount > 100: {filtered_df.count()}")

    return spark.read.format("delta").load(lk_orders_path)


def parquet_to_delta_conversion(spark):
    """Demo: Converting Parquet to Delta Lake format using CONVERT TO DELTA"""

    users_path = "s3a://owshq-shadow-traffic-uber-eats/mongodb/users/01JTKHGFDKSYJ231RSMR6SVE2X.jsonl"
    users_df = spark.read.json(users_path)

    # TODO create parquet table first {ingestion system}
    parquet_users_path = "s3a://owshq-uber-eats-lakehouse/parquet/users"
    print("📁 Creating Parquet table...")
    users_df.write \
        .format("parquet") \
        .mode("overwrite") \
        .save(parquet_users_path)

    # TODO Method 1: Convert using SQL CONVERT TO DELTA
    lk_users_path = "s3a://owshq-uber-eats-lakehouse/bronze/users"
    print("🔄 Converting Parquet to Delta Lake using CONVERT TO DELTA...")

    spark.sql(f"""
        CONVERT TO DELTA parquet.`{parquet_users_path}`
    """)

    # TODO Method 2: Convert and specify new location (copy the data)
    print("🔄 Alternative: Converting with new location...")
    spark.sql(f"""
        CREATE OR REPLACE TABLE users
        USING DELTA
        LOCATION '{lk_users_path}'
        AS SELECT * FROM parquet.`{parquet_users_path}`
    """)

    lk_users_df = spark.read.format("delta").load(lk_users_path)
    print(f"✅ Converted {lk_users_df.count()} users to Delta format")
    lk_users_df.select("user_id", "email", "city", "country").show(5)

    print("\n📊 Comparison:")
    print(f"Original Parquet files: {parquet_users_path}")
    print(f"Converted Delta table: {lk_users_path}")

    return lk_users_df


def table_metadata_exploration(spark):
    """Demo: Exploring Delta Lake table metadata"""

    lk_restaurants_path = "s3a://owshq-uber-eats-lakehouse/bronze/restaurants"
    delta_table = DeltaTable.forPath(spark, lk_restaurants_path)

    print("📜 Delta table history:")
    delta_table.history().select("version", "timestamp", "operation", "operationMetrics").show(truncate=False)

    print("📋 Delta table details:")
    spark.sql(f"DESCRIBE DETAIL delta.`{lk_restaurants_path}`").show(truncate=False)

    return delta_table


def main():
    """Main execution function"""

    spark = spark_session()

    # TODO demos

    # TODO Demo 1: DataFrame API table creation
    # part_1_table_creation_api = table_creation_api(spark)

    # TODO Demo 2: Spark SQL table creation
    # part_2_table_creation_sql = table_creation_sql(spark)

    # TODO Demo 3: Read/Write operations
    # part_3_read_write_operations = read_write_operations(spark)

    # TODO Demo 4: Parquet to Delta conversion
    # part_4_parquet_to_delta = parquet_to_delta_conversion(spark)

    # TODO Demo 5: Metadata exploration
    part_5_metadata_exploration = table_metadata_exploration(spark)

    spark.stop()


if __name__ == "__main__":
    main()
