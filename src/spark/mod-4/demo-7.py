"""
Apache Iceberg Demo 1: Foundation & Setup
=========================================

This demo covers:
- Apache Iceberg + Spark Setup
- Spark Session Configuration with Hadoop Catalog
- Creating Your First Iceberg Table
- Iceberg Catalogs Deep Dive
- Converting Parquet to Iceberg
- Iceberg Table Identifiers

Catalog Types
- `hadoop`
- `hive`
- `rest`
- `glue`
- `jdbc`
- `nessie`

Run with:
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/bitnami/spark/jobs/spark/mod-4/demo-7.py
"""

import base64
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType
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


def catalogs(spark):
    """Demonstrate Iceberg Catalogs functionality"""

    print("\n=== Iceberg Catalogs Deep Dive ===")

    # TODO show available catalogs
    print("📋 available catalogs:")
    spark.sql("SHOW CATALOGS").show()

    # TODO get current catalog
    current_catalog = spark.sql("SELECT current_catalog()").collect()[0][0]
    print(f"🔍 current catalog: {current_catalog}")

    # TODO create namespace in hadoop catalog for UberEats demo
    print("\n📁 creating namespace for UberEats demo")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS hadoop_catalog.ubereats")

    # TODO show namespaces
    print("📋 available namespaces in hadoop_catalog:")
    spark.sql("SHOW NAMESPACES IN hadoop_catalog").show()

    # TODO get current namespace
    try:
        current_namespace = spark.sql("SELECT current_database()").collect()[0][0]
        print(f"🎯 current namespace: {current_namespace}")
    except:
        print("🎯 current namespace: default")


def create_table(spark):
    """Create your first Iceberg table from UberEats data"""

    print("\n=== Creating Your First Iceberg Table ===")

    # TODO read restaurants data from MinIO
    print("📖 reading restaurants data from minio...")
    restaurants_df = spark.read.json(
        "s3a://owshq-shadow-traffic-uber-eats/mysql/restaurants/01JTKHGQ46BST7RAY6Q47YH7EJ.jsonl")

    # TODO show sample data
    print("📊 sample restaurants data:")
    restaurants_df.show(3)
    print(f"📈 total restaurants: {restaurants_df.count()}")

    # TODO show original schema
    print("📋 original data schema:")
    restaurants_df.printSchema()

    # TODO cast problematic columns to proper types for Iceberg
    print("🔧 casting data types for iceberg compatibility...")
    restaurants_clean = restaurants_df \
        .withColumn("average_rating", col("average_rating").cast("double")) \
        .withColumn("restaurant_id", col("restaurant_id").cast("int")) \
        .withColumn("num_reviews", col("num_reviews").cast("int"))

    print("📋 cleaned data schema:")
    restaurants_clean.printSchema()

    # TODO create Iceberg table using DataFrame API
    print("\n🏗️ creating iceberg table...")
    try:
        restaurants_clean.writeTo("hadoop_catalog.ubereats.restaurants") \
            .using("iceberg") \
            .tableProperty("write.format.default", "parquet") \
            .tableProperty("write.parquet.compression-codec", "snappy") \
            .create()

        print("✅ iceberg table 'restaurants' created successfully!")

        # TODO verify table creation
        print("\n🔍 verifying table creation:")
        spark.sql("SHOW TABLES IN hadoop_catalog.ubereats").show()

        # TODO show final table schema
        print("📋 final table schema:")
        spark.sql("DESCRIBE hadoop_catalog.ubereats.restaurants").show()

        # TODO sample query with aggregations
        print("📊 sample analysis - restaurant cuisines:")
        spark.sql("""
                  SELECT cuisine_type, COUNT(*) as restaurant_count,
                         ROUND(AVG(average_rating), 2) as avg_rating,
                         MAX(num_reviews) as max_reviews
                  FROM hadoop_catalog.ubereats.restaurants
                  GROUP BY cuisine_type
                  ORDER BY restaurant_count DESC
                  """).show()

        return True

    except Exception as e:
        print(f"❌ table creation failed: {e}")
        return False


def table_identifiers(spark):
    """Demonstrate different ways to reference Iceberg tables"""

    print("\n=== Table Identifiers & Querying ===")

    # TODO show different identifier formats
    print("🔗 table identifier formats:")
    print("   - full: hadoop_catalog.ubereats.restaurants")
    print("   - short: restaurants (when catalog and namespace are set)")

    # TODO query using full identifier
    print("\n📊 querying with full identifier:")
    spark.sql("""
        SELECT city, COUNT(*) as restaurant_count
        FROM hadoop_catalog.ubereats.restaurants 
        WHERE country = 'BR'
        GROUP BY city 
        ORDER BY restaurant_count DESC
        LIMIT 3
    """).show()

    # TODO set catalog context and use short identifier
    print("\n🎯 setting catalog context:")
    spark.catalog.setCurrentCatalog("hadoop_catalog")
    spark.catalog.setCurrentDatabase("ubereats")

    print("📊 querying with short identifier:")
    spark.sql("""
        SELECT 
            cuisine_type,
            COUNT(*) as count,
            ROUND(AVG(average_rating), 2) as avg_rating
        FROM restaurants 
        GROUP BY cuisine_type
        HAVING COUNT(*) >= 1
        ORDER BY avg_rating DESC
    """).show()


def convert_parquet_to_iceberg(spark):
    """
    Convert existing Parquet data to Iceberg format using the latest best practices.
    """

    print("\n=== Converting Parquet to Iceberg (Latest Process) ===")

    # TODO Step 1: Read source data (JSON in this example)
    print("📖 Reading users data...")
    users_df = spark.read.json("s3a://owshq-shadow-traffic-uber-eats/mongodb/users/01JTKHGHC126PMGE8G819ST71N.jsonl")

    # TODO Step 2: Save as Parquet (if not already in Parquet)
    parquet_path = "s3a://owshq-catalog/temp/users_parquet"
    print(f"💾 Saving as Parquet: {parquet_path}")
    users_df.write.mode("overwrite").parquet(parquet_path)

    # TODO Step 3: Register Parquet as a temporary view
    print("🔗 Registering Parquet as temp view...")
    spark.read.parquet(parquet_path).createOrReplaceTempView("users_parquet_view")

    # TODO Step 4: Create Iceberg table using CTAS (CREATE TABLE AS SELECT)
    print("🔄 Creating Iceberg table from Parquet view...")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS hadoop_catalog.ubereats.users_from_parquet
        USING iceberg
        TBLPROPERTIES (
            'write.format.default' = 'parquet',
            'write.parquet.compression-codec' = 'snappy'
        )
        AS SELECT * FROM users_parquet_view
    """)

    print("✅ Conversion successful!")

    # TODO Step 5: Validation - Row count and sample data
    print("\n🔍 Validating converted Iceberg table:")
    parquet_count = spark.sql("SELECT COUNT(*) FROM users_parquet_view").collect()[0][0]
    iceberg_count = spark.sql("SELECT COUNT(*) FROM hadoop_catalog.ubereats.users_from_parquet").collect()[0][0]
    print(f"   Parquet row count: {parquet_count}")
    print(f"   Iceberg row count: {iceberg_count}")
    spark.sql("SELECT * FROM hadoop_catalog.ubereats.users_from_parquet LIMIT 3").show()

    # TODO Step 6: Inspect Iceberg metadata tables
    print("\n📂 Iceberg metadata (files):")
    spark.sql("SELECT * FROM hadoop_catalog.ubereats.users_from_parquet.files").show(3)
    print("\n📂 Iceberg metadata (snapshots):")
    spark.sql("SELECT * FROM hadoop_catalog.ubereats.users_from_parquet.snapshots").show(3)

    # TODO Step 7: Feature comparison
    print("\n📋 Parquet vs Iceberg comparison:")
    print("   Parquet:")
    print("     ✅ Columnar format")
    print("     ✅ Good compression")
    print("     ❌ No ACID transactions")
    print("     ❌ No schema evolution")
    print("     ❌ No time travel")
    print("   Iceberg:")
    print("     ✅ All Parquet benefits")
    print("     ✅ ACID transactions")
    print("     ✅ Schema evolution")
    print("     ✅ Time travel")
    print("     ✅ Hidden partitioning")


def table_properties(spark):
    """Demonstrate Iceberg table properties and metadata"""

    print("\n=== Iceberg Table Properties & Metadata ===")

    try:
        # TODO show table properties
        print("🔧 table properties:")
        spark.sql("SHOW TBLPROPERTIES hadoop_catalog.ubereats.restaurants").show(truncate=False)

        # TODO show table metadata using system tables
        print("\n📊 table history:")
        spark.sql("SELECT * FROM hadoop_catalog.ubereats.restaurants.history").show(truncate=False)

        # TODO show table snapshots
        print("\n📋 table snapshots:")
        spark.sql("""
            SELECT 
                snapshot_id,
                committed_at,
                operation,
                summary
            FROM hadoop_catalog.ubereats.restaurants.snapshots
        """).show(truncate=False)

        # TODO show table files
        print("\n📁 table files (sample):")
        spark.sql("""
            SELECT 
                file_path,
                file_format,
                record_count,
                file_size_in_bytes
            FROM hadoop_catalog.ubereats.restaurants.files
            LIMIT 3
        """).show(truncate=False)

    except Exception as e:
        print(f"⚠️ metadata access failed: {e}")


def basic_operations(spark):
    """Test basic Iceberg operations"""

    print("\n=== Basic Iceberg Operations ===")

    try:
        spark.sql("DESCRIBE hadoop_catalog.ubereats.restaurants").show()

        # TODO INSERT operation with proper data types
        print("📝 testing INSERT operation...")
        spark.sql("""
                  INSERT INTO hadoop_catalog.ubereats.restaurants
                  VALUES ('Rua dos Dados, 123', -- address (string)
                          4.8, -- average_rating (double)
                          'Demo City', -- city (string)
                          '11:00 PM', -- closing_time (string)
                          '99.999.999/9999-99', -- cnpj (string)
                          'BR', -- country (string)
                          'Educational', -- cuisine_type (string)
                          current_timestamp(), -- dt_current_timestamp (string)
                          'Academy Demo Restaurant', -- name (string)
                          150, -- num_reviews (int)
                          '09:00 AM', -- opening_time (string)
                          '(11) 9999-9999', -- phone_number (string)
                          9999, -- restaurant_id (int)
                          'demo-uuid-123' -- uuid (string)
                         )
                  """)

        print("✅ INSERT successful!")

        # TODO verify insert
        new_count = spark.sql("SELECT COUNT(*) FROM hadoop_catalog.ubereats.restaurants").collect()[0][0]
        print(f"📈 total restaurants after insert: {new_count}")

        # TODO show the inserted record
        print("\n🔍 verifying inserted record:")
        spark.sql("""
            SELECT name, cuisine_type, average_rating, city 
            FROM hadoop_catalog.ubereats.restaurants 
            WHERE name LIKE '%Academy%'
        """).show()

        # TODO UPDATE operation
        print("\n📝 testing UPDATE operation...")
        spark.sql("""
            UPDATE hadoop_catalog.ubereats.restaurants 
            SET average_rating = 5.0, num_reviews = 200
            WHERE name = 'Academy Demo Restaurant'
        """)

        print("✅ UPDATE successful!")

        # TODO verify update
        print("\n🔍 verifying update:")
        spark.sql("""
            SELECT name, average_rating, num_reviews 
            FROM hadoop_catalog.ubereats.restaurants 
            WHERE name = 'Academy Demo Restaurant'
        """).show()

    except Exception as e:
        print(f"⚠️ operations test failed: {e}")


def cleanup_resources(spark):
    """Clean up demo resources"""

    print("\n=== Cleanup ===")

    try:
        # TODO drop tables
        spark.sql("DROP TABLE IF EXISTS hadoop_catalog.ubereats.restaurants")
        spark.sql("DROP TABLE IF EXISTS hadoop_catalog.ubereats.users_from_parquet")

        # TODO drop namespace
        spark.sql("DROP NAMESPACE IF EXISTS hadoop_catalog.ubereats CASCADE")

        print("✅ demo resources cleaned up successfully!")

    except Exception as e:
        print(f"⚠️ cleanup warning: {e}")


def main():
    """Main demo execution"""

    print("🚀 Starting Apache Iceberg Demo 1: Foundation & Setup")
    print("=" * 60)

    # TODO create Spark session
    spark = spark_session()

    try:
        # TODO run demo sections
        catalogs(spark)
        table_created = create_table(spark)

        if table_created:
            table_identifiers(spark)
            convert_parquet_to_iceberg(spark)
            table_properties(spark)
            basic_operations(spark)

            print("\n" + "=" * 60)
            print("🎉 Demo 1 completed successfully!")
            print("📚 Key concepts covered:")
            print("   ✓ Spark + Iceberg configuration")
            print("   ✓ Hadoop catalog with MinIO")
            print("   ✓ Creating Iceberg tables from real data")
            print("   ✓ Table identifiers (full vs short)")
            print("   ✓ Parquet to Iceberg conversion")
            print("   ✓ Table properties and metadata")
            print("   ✓ Basic CRUD operations (INSERT, UPDATE)")
            print("   ✓ System tables exploration")

            print("\n🔗 What's Next:")
            print("   → Demo 2: Data Ingestion & Basic Operations")
            print("   → Schema evolution and time travel")
            print("   → Advanced partitioning strategies")
        else:
            print("\n⚠️ table creation failed - check warehouse permissions")

    except Exception as e:
        print(f"❌ Demo failed with error: {e}")
        import traceback
        traceback.print_exc()

    finally:
        # TODO cleanup
        # TODO to be removed cleanup_resources(spark)
        spark.stop()
        print("🔒 Spark session stopped")


if __name__ == "__main__":
    main()
