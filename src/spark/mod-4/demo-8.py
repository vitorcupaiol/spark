"""
Apache Iceberg Demo 2: Data Ingestion & Basic Operations
========================================================

This demo covers:
- Apache Iceberg: Reading Iceberg Tables with Spark DataFrames
- Apache Iceberg: Writing DataFrames to Iceberg
- Apache Iceberg: Spark Write Modes with Iceberg
- Apache Iceberg: Spark DataSourceV2 API
- Apache Iceberg: Iceberg SQL Extensions
- Apache Iceberg: CREATE TABLE with Iceberg
- Apache Iceberg: CREATE TABLE AS SELECT (CTAS)
- Apache Iceberg: INSERT INTO Iceberg Tables

Write Modes
- `append`
- `overwrite`
- `errorifexists`
- `ignore`

Run with:
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/bitnami/spark/jobs/spark/mod-4/demo-8.py
"""

import base64
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, TimestampType
from pyspark.sql.functions import col, current_timestamp, lit


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


def create_table_sql(spark):
    """Demonstrate CREATE TABLE with Iceberg using SQL"""

    print("\n=== CREATE TABLE with Iceberg ===")

    # TODO create table using SQL DDL
    print("🏗️ creating orders table with SQL...")
    spark.sql("""
              CREATE TABLE IF NOT EXISTS orders
              (
                  order_id STRING,
                  user_id INT,
                  restaurant_id INT,
                  total_amount DOUBLE,
                  order_date TIMESTAMP,
                  status STRING
              ) USING iceberg
                  TBLPROPERTIES
              (
                  'write.format.default' =
                  'parquet',
                  'write.parquet.compression-codec' =
                  'snappy'
              )
              """)

    # TODO verify table creation
    print("🔍 verifying table creation:")
    spark.sql("SHOW TABLES").show()
    spark.sql("DESCRIBE orders").show()

    print("✅ orders table created!")


def create_table_ctas(spark):
    """Demonstrate CREATE TABLE AS SELECT (CTAS)"""

    print("\n=== CREATE TABLE AS SELECT (CTAS) ===")

    # TODO read source data
    print("📖 reading restaurants data...")
    restaurants_df = spark.read.json(
        "s3a://owshq-shadow-traffic-uber-eats/mysql/restaurants/01JTKHGQ46BST7RAY6Q47YH7EJ.jsonl")

    # TODO create temp view
    restaurants_df.createOrReplaceTempView("restaurants_source")

    # TODO create table using CTAS
    print("🏗️ creating restaurants table with CTAS...")
    spark.sql("""
              CREATE TABLE IF NOT EXISTS restaurants
                  USING iceberg
                  TBLPROPERTIES
              (
                  'write.format.default' =
                  'parquet',
                  'write.parquet.compression-codec' =
                  'snappy'
              )
              AS
              SELECT CAST(restaurant_id AS INT)     as restaurant_id,
                     name,
                     cuisine_type,
                     city,
                     country,
                     CAST(average_rating AS DOUBLE) as average_rating,
                     CAST(num_reviews AS INT)       as num_reviews,
                     opening_time,
                     closing_time,
                     phone_number,
                     cnpj,
                     address,
                     uuid
              FROM restaurants_source
              """)

    # TODO verify CTAS result
    print("🔍 verifying CTAS result:")
    spark.sql("SELECT COUNT(*) as restaurant_count FROM restaurants").show()
    spark.sql("SELECT * FROM restaurants LIMIT 3").show()

    print("✅ restaurants table created with CTAS!")


def reading_iceberg_dataframes(spark):
    """Demonstrate reading Iceberg tables with Spark DataFrames"""

    print("\n=== Reading Iceberg Tables with DataFrames ===")

    # TODO read using DataFrame API
    print("📖 reading restaurants table with DataFrame API...")
    restaurants_df = spark.read.table("restaurants")

    print(f"📊 total restaurants: {restaurants_df.count()}")
    restaurants_df.show(3)

    # TODO read with specific columns
    print("📖 reading specific columns...")
    selected_df = spark.read.table("restaurants").select("name", "cuisine_type", "average_rating")
    selected_df.show(3)

    # TODO read with filters
    print("📖 reading with filters...")
    filtered_df = spark.read.table("restaurants").filter(col("average_rating") > 4.0)
    print(f"📊 high-rated restaurants: {filtered_df.count()}")
    filtered_df.select("name", "average_rating").show()

    print("✅ DataFrame reading demonstrated!")


def writing_dataframes_iceberg(spark):
    """Demonstrate writing DataFrames to Iceberg"""

    print("\n=== Writing DataFrames to Iceberg ===")

    # TODO Define schema explicitly
    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("restaurant_id", IntegerType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("order_date", TimestampType(), True),
        StructField("status", StringType(), True)
    ])

    # TODO Create sample data (order_date can be None)
    sample_orders = [
        ("ORD-001", 1001, 1, 45.50, None, "completed"),
        ("ORD-002", 1002, 2, 32.75, None, "pending"),
        ("ORD-003", 1003, 1, 28.90, None, "completed")
    ]

    # TODO Create DataFrame with explicit schema
    orders_df = spark.createDataFrame(sample_orders, schema=schema)

    # TODO Fill order_date with current timestamp
    from pyspark.sql.functions import current_timestamp
    orders_df = orders_df.withColumn("order_date", current_timestamp())

    # TODO Write to Iceberg table (replace 'orders' with your full table name if needed)
    print("💾 writing to orders table...")
    orders_df.write.mode("append").saveAsTable("orders")

    # TODO Verify write
    print("🔍 verifying write:")
    spark.sql("SELECT COUNT(*) as order_count FROM orders").show()
    spark.sql("SELECT * FROM orders").show()

    print("✅ DataFrame writing demonstrated!")


def spark_write_modes(spark):
    """Demonstrate Spark write modes with Iceberg"""

    print("\n=== Spark Write Modes with Iceberg ===")

    # Create test data
    print("📝 creating test data...")
    test_data = [
        (1, "Test Restaurant 1", "Italian", 4.5),
        (2, "Test Restaurant 2", "Chinese", 4.2)
    ]
    test_df = spark.createDataFrame(test_data, ["restaurant_id", "name", "cuisine_type", "rating"])

    # Create the Iceberg table if it doesn't exist
    print("📝 creating Iceberg table if not exists...")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS test_restaurants (
            restaurant_id INT,
            name STRING,
            cuisine_type STRING,
            rating DOUBLE
        ) USING iceberg
    """)

    # Demonstrate append mode
    print("📝 testing APPEND mode...")
    test_df.write.mode("append").saveAsTable("test_restaurants")
    print(f"   📊 after append: {spark.sql('SELECT COUNT(*) FROM test_restaurants').collect()[0][0]} rows")

    # Demonstrate overwrite mode
    print("📝 testing OVERWRITE mode...")
    new_data = [(3, "Replacement Restaurant", "Mexican", 4.8)]
    new_df = spark.createDataFrame(new_data, ["restaurant_id", "name", "cuisine_type", "rating"])
    new_df.write.mode("overwrite").saveAsTable("test_restaurants")
    print(f"   📊 after overwrite: {spark.sql('SELECT COUNT(*) FROM test_restaurants').collect()[0][0]} rows")
    spark.sql("SELECT * FROM test_restaurants").show()

    print("✅ write modes demonstrated!")


def datasource_v2_api(spark):
    """Demonstrate Spark DataSourceV2 API with Iceberg"""

    print("\n=== Spark DataSourceV2 API ===")

    # TODO read using DataSourceV2
    print("📖 reading with DataSourceV2 API...")
    df = spark.read \
        .format("iceberg") \
        .load("hadoop_catalog.ubereats.restaurants")

    print(f"📊 restaurants count: {df.count()}")

    # TODO write using DataSourceV2
    print("💾 writing with DataSourceV2 API...")
    sample_data = [(999, "DataSourceV2 Restaurant", "Fusion", 4.9)]
    sample_df = spark.createDataFrame(sample_data, ["restaurant_id", "name", "cuisine_type", "rating"])

    sample_df.write \
        .format("iceberg") \
        .mode("append") \
        .save("hadoop_catalog.ubereats.test_restaurants")

    # TODO verify write
    print("🔍 verifying DataSourceV2 write:")
    spark.sql("SELECT * FROM test_restaurants WHERE name LIKE '%DataSourceV2%'").show()

    print("✅ DataSourceV2 API demonstrated!")


def iceberg_sql_extensions(spark):
    """Demonstrate Iceberg SQL Extensions"""

    print("\n=== Iceberg SQL Extensions ===")

    # TODO Set the correct catalog and database if needed
    spark.catalog.setCurrentCatalog("hadoop_catalog")
    spark.catalog.setCurrentDatabase("ubereats")

    table_fq = "hadoop_catalog.ubereats.restaurants"

    # TODO Show table properties
    print("🔧 showing table properties...")
    spark.sql(f"SHOW TBLPROPERTIES {table_fq}").show(truncate=False)

    # TODO Show table history
    print("📊 showing table history...")
    spark.sql(f"SELECT * FROM {table_fq}.history").show(truncate=False)

    # TODO Show table snapshots
    print("📋 showing table snapshots...")
    spark.sql(f"SELECT snapshot_id, committed_at, operation FROM {table_fq}.snapshots").show(truncate=False)

    # TODO Show table files
    print("📁 showing table files...")
    spark.sql(f"SELECT file_path, record_count FROM {table_fq}.files LIMIT 3").show(truncate=False)

    print("✅ Iceberg SQL extensions demonstrated!")


def insert_operations(spark):
    """Demonstrate INSERT INTO operations"""

    print("\n=== INSERT INTO Iceberg Tables ===")

    # TODO INSERT with VALUES
    print("📝 INSERT with VALUES...")
    spark.sql("""
              INSERT INTO orders
              VALUES ('ORD-004', 1004, 3, 55.25, current_timestamp(), 'processing'),
                     ('ORD-005', 1005, 2, 41.80, current_timestamp(), 'completed')
              """)

    # TODO INSERT with SELECT
    print("📝 INSERT with SELECT...")
    spark.sql("""
              INSERT INTO orders
              SELECT 'ORD-006'           as order_id,
                     1006                as user_id,
                     restaurant_id,
                     average_rating * 10 as total_amount,
                     current_timestamp() as order_date,
                     'new'               as status
              FROM restaurants
              WHERE average_rating > 4.0 LIMIT 1
              """)

    # TODO verify inserts
    print("🔍 verifying inserts:")
    spark.sql("SELECT COUNT(*) as total_orders FROM orders").show()
    spark.sql("SELECT order_id, status, total_amount FROM orders ORDER BY order_id DESC LIMIT 3").show()

    print("✅ INSERT operations demonstrated!")


def cleanup_resources(spark):
    """Clean up demo resources"""

    print("\n=== Cleanup ===")

    try:
        # TODO drop tables
        spark.sql("DROP TABLE IF EXISTS orders")
        spark.sql("DROP TABLE IF EXISTS restaurants")
        spark.sql("DROP TABLE IF EXISTS test_restaurants")

        # TODO drop namespace
        spark.sql("DROP NAMESPACE IF EXISTS hadoop_catalog.ubereats CASCADE")

        print("✅ demo resources cleaned up successfully!")

    except Exception as e:
        print(f"⚠️ cleanup warning: {e}")


def main():
    """Main demo execution"""

    print("🚀 Starting Apache Iceberg Demo 2: Data Ingestion & Basic Operations")
    print("=" * 70)

    # TODO create Spark session
    spark = spark_session()

    try:
        # TODO run demo sections
        setup_namespace(spark)
        create_table_sql(spark)
        create_table_ctas(spark)
        reading_iceberg_dataframes(spark)
        writing_dataframes_iceberg(spark)
        spark_write_modes(spark)
        datasource_v2_api(spark)
        iceberg_sql_extensions(spark)
        insert_operations(spark)

        print("\n" + "=" * 70)
        print("🎉 Demo 2 completed successfully!")
        print("📚 Key concepts covered:")
        print("   ✓ Reading Iceberg tables with DataFrames")
        print("   ✓ Writing DataFrames to Iceberg")
        print("   ✓ Spark write modes (append, overwrite)")
        print("   ✓ DataSourceV2 API usage")
        print("   ✓ Iceberg SQL extensions")
        print("   ✓ CREATE TABLE and CTAS")
        print("   ✓ INSERT operations (VALUES, SELECT)")

        print("\n🔗 What's Next:")
        print("   → Demo 3: Schema Evolution & Data Quality")
        print("   → Demo 4: Advanced Partitioning & Performance")

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
