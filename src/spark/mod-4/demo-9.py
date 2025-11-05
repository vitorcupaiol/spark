"""
Apache Iceberg Demo 3: Schema Evolution & Data Quality
======================================================

This demo covers:
- Apache Iceberg: Schema Evolution
- Apache Iceberg: ALTER TABLE ADD COLUMN
- Apache Iceberg: ALTER TABLE DROP COLUMN
- Apache Iceberg: ALTER TABLE RENAME COLUMN
- Apache Iceberg: Type Evolution

Schema Evolution Types
- `add` - Add new columns
- `drop` - Remove columns
- `rename` - Rename columns
- `type` - Change column types
- `reorder` - Change column order

Run with:
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/bitnami/spark/jobs/spark/mod-4/demo-9.py
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


def setup_demo_table(spark):
    """Setup initial table for schema evolution demo"""

    print("\n=== Setting Up Demo Table ===")

    # TODO create namespace
    print("📁 creating namespace...")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS hadoop_catalog.ubereats")

    # TODO set catalog context
    spark.catalog.setCurrentCatalog("hadoop_catalog")
    spark.catalog.setCurrentDatabase("ubereats")

    # TODO create initial table
    print("🏗️ creating initial products table...")
    spark.sql("""
              CREATE TABLE IF NOT EXISTS products
              (
                  product_id INT,
                  name STRING,
                  price DOUBLE,
                  category STRING
              ) USING iceberg
                  TBLPROPERTIES
              (
                  'write.format.default' =
                  'parquet',
                  'write.parquet.compression-codec' =
                  'snappy'
              )
              """)

    # TODO insert initial data
    print("📝 inserting initial data...")
    spark.sql("""
              INSERT INTO products
              VALUES (1, 'Pizza Margherita', 25.50, 'Italian'),
                     (2, 'Sushi Roll', 18.75, 'Japanese'),
                     (3, 'Burger Deluxe', 22.90, 'American')
              """)

    # TODO verify setup
    print("🔍 verifying initial setup:")
    spark.sql("DESCRIBE products").show()
    spark.sql("SELECT * FROM products").show()

    print("✅ demo table ready!")


def schema_evolution_add_column(spark):
    """Demonstrate adding columns to Iceberg table"""

    print("\n=== ALTER TABLE ADD COLUMN ===")

    # TODO Show current schema
    print("📋 current schema:")
    spark.sql("DESCRIBE products").show()

    # TODO Add single column
    print("📝 adding description column...")
    spark.sql("ALTER TABLE products ADD COLUMN description STRING")

    # TODO Add multiple columns (one at a time)
    print("📝 adding multiple columns...")
    spark.sql("ALTER TABLE products ADD COLUMN stock_quantity INT")
    spark.sql("ALTER TABLE products ADD COLUMN is_available BOOLEAN")
    spark.sql("ALTER TABLE products ADD COLUMN created_at TIMESTAMP")

    # TODO Verify column additions
    print("🔍 verifying new schema:")
    spark.sql("DESCRIBE products").show()

    # TODO Insert data with new columns (set default for is_available in the insert)
    print("📝 inserting data with new columns...")
    spark.sql("""
        INSERT INTO products
        VALUES (4, 'Pasta Carbonara', 19.99, 'Italian', 'Creamy pasta with bacon', 25, true, current_timestamp())
              """)

    # TODO Show data with new columns
    print("📊 data with new columns:")
    spark.sql("SELECT * FROM products WHERE product_id = 4").show(truncate=False)

    print("✅ ADD COLUMN demonstrated!")


def schema_evolution_rename_column(spark):
    """Demonstrate renaming columns in Iceberg table"""

    print("\n=== ALTER TABLE RENAME COLUMN ===")

    # TODO rename single column
    print("📝 renaming category to cuisine_type...")
    spark.sql("ALTER TABLE products RENAME COLUMN category TO cuisine_type")

    # TODO rename another column
    print("📝 renaming stock_quantity to inventory_count...")
    spark.sql("ALTER TABLE products RENAME COLUMN stock_quantity TO inventory_count")

    # TODO verify renames
    print("🔍 verifying renamed columns:")
    spark.sql("DESCRIBE products").show()

    # TODO query with new column names
    print("📊 querying with new column names:")
    spark.sql("""
              SELECT product_id, name, cuisine_type, inventory_count
              FROM products
              WHERE cuisine_type = 'Italian'
              """).show()

    print("✅ RENAME COLUMN demonstrated!")


def schema_evolution_drop_column(spark):
    """Demonstrate dropping columns from Iceberg table"""

    print("\n=== ALTER TABLE DROP COLUMN ===")

    # TODO show current schema before drop
    print("📋 schema before dropping columns:")
    spark.sql("DESCRIBE products").show()

    # TODO drop single column
    print("📝 dropping description column...")
    spark.sql("ALTER TABLE products DROP COLUMN description")

    # TODO drop multiple columns
    print("📝 dropping created_at column...")
    spark.sql("ALTER TABLE products DROP COLUMN created_at")

    # TODO verify drops
    print("🔍 verifying schema after drops:")
    spark.sql("DESCRIBE products").show()

    # TODO show data still accessible
    print("📊 data still accessible:")
    spark.sql("SELECT * FROM products").show()

    print("✅ DROP COLUMN demonstrated!")


def type_evolution(spark):
    """Demonstrate type evolution in Iceberg using add-cast-copy pattern"""

    print("\n=== Type Evolution Demo (Iceberg Safe) ===")

    # TODO Show current schema
    print("📋 Current schema:")
    spark.sql("DESCRIBE products").show()

    # TODO Evolve price: DOUBLE -> DECIMAL(10,2) using add-cast-copy
    print("📝 Adding new column price_decimal (DECIMAL(10,2))...")
    try:
        spark.sql("ALTER TABLE products ADD COLUMN price_decimal DECIMAL(10,2)")
        spark.sql("UPDATE products SET price_decimal = CAST(price AS DECIMAL(10,2))")
        print("✅ price_decimal column added and populated!")
    except Exception as e:
        print(f"⚠️ Could not evolve price: {e}")

    # TODO Add a rating column as INT
    print("📝 Adding rating column as INT...")
    try:
        spark.sql("ALTER TABLE products ADD COLUMN rating INT")
        spark.sql("UPDATE products SET rating = CAST(price AS INT) WHERE rating IS NULL")
        print("✅ rating column added and populated!")
    except Exception as e:
        print(f"⚠️ Could not add/populate rating: {e}")

    # TODO Evolve rating: INT -> DOUBLE using add-cast-copy
    print("📝 Adding new column rating_double (DOUBLE)...")
    try:
        spark.sql("ALTER TABLE products ADD COLUMN rating_double DOUBLE")
        spark.sql("UPDATE products SET rating_double = CAST(rating AS DOUBLE)")
        print("✅ rating_double column added and populated!")
    except Exception as e:
        print(f"⚠️ Could not evolve rating: {e}")

    # TODO Show updated schema and sample data
    print("🔍 Updated schema:")
    spark.sql("DESCRIBE products").show()

    print("📊 Sample data:")
    spark.sql("SELECT product_id, name, price, price_decimal, rating, rating_double FROM products").show(truncate=False)

    print("✅ Type evolution demo complete!")


def schema_evolution_compatibility(spark):
    """Demonstrate schema evolution compatibility"""

    print("\n=== Schema Evolution Compatibility ===")

    # TODO Set the correct catalog and database if needed
    spark.catalog.setCurrentCatalog("hadoop_catalog")
    spark.catalog.setCurrentDatabase("ubereats")

    table_fq = "hadoop_catalog.ubereats.products"

    # TODO Show evolution history
    print("📊 table history showing schema changes:")
    spark.sql(f"SELECT * FROM {table_fq}.history").show(truncate=False)

    # TODO Show snapshots
    print("📋 snapshots showing schema evolution:")
    spark.sql(f"""
        SELECT snapshot_id, committed_at, operation, summary
        FROM {table_fq}.snapshots
        ORDER BY committed_at
    """).show(truncate=False)

    # TODO Demonstrate reading old data with new schema
    print("📖 reading data works across schema versions:")
    spark.sql(f"SELECT COUNT(*) as total_products FROM {table_fq}").show()

    # TODO Show final schema
    print("📋 final evolved schema:")
    spark.sql(f"DESCRIBE {table_fq}").show()

    print("✅ schema compatibility demonstrated!")


def data_quality_checks(spark):
    """Demonstrate data quality with schema evolution"""

    print("\n=== Data Quality with Schema Evolution ===")

    # TODO add constraint-like columns
    print("📝 adding columns with default values...")
    spark.sql("""
              ALTER TABLE products
                  ADD COLUMN last_updated TIMESTAMP DEFAULT current_timestamp()
              """)

    # TODO verify null handling
    print("🔍 checking null handling in evolved schema:")
    spark.sql("""
              SELECT product_id,
                     name,
                     is_available,
                     inventory_count,
                     last_updated IS NULL as has_null_timestamp
              FROM products
              """).show()

    # TODO update data to show evolution works
    print("📝 updating data to populate new fields...")
    spark.sql("""
              UPDATE products
              SET is_available = CASE WHEN inventory_count > 0 THEN true ELSE false END,
                  last_updated = current_timestamp()
              WHERE last_updated IS NULL
              """)

    # TODO show data quality after evolution
    print("📊 data quality after schema evolution:")
    spark.sql("""
              SELECT product_id,
                     name,
                     cuisine_type,
                     price,
                     inventory_count,
                     is_available,
                     rating
              FROM products
              ORDER BY product_id
              """).show()

    print("✅ data quality maintained!")


def cleanup_resources(spark):
    """Clean up demo resources"""

    print("\n=== Cleanup ===")

    try:
        # TODO drop tables
        spark.sql("DROP TABLE IF EXISTS products")

        # TODO drop namespace
        spark.sql("DROP NAMESPACE IF EXISTS hadoop_catalog.ubereats CASCADE")

        print("✅ demo resources cleaned up successfully!")

    except Exception as e:
        print(f"⚠️ cleanup warning: {e}")


def main():
    """Main demo execution"""

    print("🚀 Starting Apache Iceberg Demo 3: Schema Evolution & Data Quality")
    print("=" * 70)

    # TODO create Spark session
    spark = spark_session()

    try:
        # TODO run demo sections
        setup_demo_table(spark)
        schema_evolution_add_column(spark)
        schema_evolution_rename_column(spark)
        schema_evolution_drop_column(spark)
        type_evolution(spark)
        schema_evolution_compatibility(spark)
        data_quality_checks(spark)

        print("\n" + "=" * 70)
        print("🎉 Demo 3 completed successfully!")
        print("📚 Key concepts covered:")
        print("   ✓ Schema evolution fundamentals")
        print("   ✓ ALTER TABLE ADD COLUMN")
        print("   ✓ ALTER TABLE RENAME COLUMN")
        print("   ✓ ALTER TABLE DROP COLUMN")
        print("   ✓ Type evolution (compatible types)")
        print("   ✓ Schema compatibility across versions")
        print("   ✓ Data quality with evolving schemas")

        print("\n🔗 What's Next:")
        print("   → Demo 4: Advanced Partitioning & Performance")
        print("   → Demo 5: Time Travel & Version Control")

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
