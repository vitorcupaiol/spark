"""
Demo 4: Schema Management & Evolution
===================================

This demo covers:
- Schema Enforcement
- Schema Evolution with MergeSchema
- Column Mapping with Spark
- Type Widening Operations
- Generated Columns & Default Values
- CHECK Constraints

Run command:
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/bitnami/spark/jobs/spark/mod-4/demo-4.py
"""

import base64
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, expr, concat
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


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


def schema_enforcement(spark):
    """Demo: Schema enforcement"""

    lk_orders_path = "s3a://owshq-uber-eats-lakehouse/demo/orders"

    try:
        orders_source = "s3a://owshq-shadow-traffic-uber-eats/kafka/orders/01JVCRAA2N4XA6GBNFPN7KFXK9.jsonl"
        orders_raw = spark.read.json(orders_source).limit(20)
        print(f"Loaded {orders_raw.count()} UberEats orders")
    except:
        orders_raw = spark.createDataFrame([
            ("ORD001", "USER123", "REST456", 25.99, current_timestamp(), "PAY789"),
            ("ORD002", "USER124", "REST457", 18.50, current_timestamp(), "PAY790")
        ], ["order_id", "user_key", "restaurant_key", "total_amount", "order_date", "payment_key"])

    # TODO Create table with enforced schema
    orders_clean = orders_raw.select(
        col("order_id").cast("string"),
        col("user_key").cast("string"),
        col("restaurant_key").cast("string"),
        col("total_amount").cast("double"),
        col("order_date").cast("timestamp"),
        col("payment_key").cast("string")
    ).filter(
        col("order_id").isNotNull() &
        col("total_amount").isNotNull()
    )

    orders_clean.write.format("delta").mode("overwrite").save(lk_orders_path)
    print("✅ Orders table created with schema enforcement")

    # TODO Test schema enforcement
    print("\n🔍 Testing schema enforcement:")
    try:
        invalid_data = spark.createDataFrame([
            ("ORD999", "USER999", "REST999", "invalid_amount", current_timestamp(), None)
        ], ["order_id", "user_key", "restaurant_key", "total_amount", "order_date", "payment_key"])

        invalid_data.write.format("delta").mode("append").save(lk_orders_path)
        print("❌ Schema enforcement failed")
    except:
        print("✅ Schema enforcement blocked invalid data")

    result_df = spark.read.format("delta").load(lk_orders_path)
    print(f"Final table: {result_df.count()} valid records")
    result_df.show(3)

    return result_df


def schema_evolution(spark):
    """Demo: Schema evolution with business context"""

    lk_orders_evolved_path = "s3a://owshq-uber-eats-lakehouse/demo/orders"

    try:
        base_orders = spark.read.format("delta").load("s3a://owshq-uber-eats-lakehouse/demo/orders").limit(10)
    except:
        base_orders = spark.createDataFrame([
            ("ORD001", "USER123", "REST456", 25.99, current_timestamp(), "PAY789"),
            ("ORD002", "USER124", "REST457", 18.50, current_timestamp(), "PAY790")
        ], ["order_id", "user_key", "restaurant_key", "total_amount", "order_date", "payment_key"])

    # TODO Create initial table
    base_orders.write.format("delta").mode("overwrite").save(lk_orders_evolved_path)
    print("Created base orders table")

    # TODO Evolution: Add delivery context
    enhanced_orders = base_orders.withColumn("delivery_fee", lit(3.99)) \
        .withColumn("delivery_time_estimate", lit(30)) \
        .withColumn("customer_segment", lit("premium"))

    enhanced_orders.write.format("delta").mode("append").option("mergeSchema", "true").save(lk_orders_evolved_path)
    print("✅ Added delivery context columns with mergeSchema")

    # TODO Show results
    evolved_df = spark.read.format("delta").load(lk_orders_evolved_path)
    print("Evolved schema:")
    evolved_df.printSchema()

    print("Data evolution example:")
    evolved_df.select("order_id", "total_amount", "delivery_fee", "customer_segment").show(5)

    return evolved_df


def column_mapping(spark):
    """Demo: Column mapping for business-friendly names"""
    lk_products_path = "s3a://owshq-uber-eats-lakehouse/demo/products"

    # TODO Load products data
    try:
        products_source = "s3a://owshq-shadow-traffic-uber-eats/mysql/products/01JTKSZZX2QT518VEXJ22JH4ZZ.jsonl"
        products_raw = spark.read.json(products_source).limit(10)
        products_df = products_raw.select("product_id", "name", "price", "cuisine_type")
    except:
        products_df = spark.createDataFrame([
            ("PROD001", "Pizza Margherita", 12.99, "Italian"),
            ("PROD002", "Chicken Curry", 15.50, "Indian"),
            ("PROD003", "Caesar Salad", 8.99, "American")
        ], ["product_id", "name", "price", "cuisine_type"])

    # TODO Create initial table
    products_df.write.format("delta").mode("overwrite").save(lk_products_path)
    print("Created products table")

    # TODO Enable column mapping feature properly
    try:
        print("⚙️  Enabling column mapping feature...")

        # TODO Step 1: Upgrade table to support column mapping (requires writer version 5, reader version 2)
        spark.sql(f"""
            ALTER TABLE delta.`{lk_products_path}` 
            SET TBLPROPERTIES (
                'delta.columnMapping.mode' = 'name',
                'delta.minReaderVersion' = '2',
                'delta.minWriterVersion' = '5'
            )
        """)
        print("✅ Column mapping mode enabled")

        # TODO Step 2: Rename columns using ALTER TABLE RENAME COLUMN
        print("🔄 Renaming columns for business clarity:")

        spark.sql(f"""
            ALTER TABLE delta.`{lk_products_path}` 
            RENAME COLUMN name TO product_name
        """)
        print("   • 'name' → 'product_name'")

        spark.sql(f"""
            ALTER TABLE delta.`{lk_products_path}` 
            RENAME COLUMN price TO unit_price
        """)
        print("   • 'price' → 'unit_price'")

        spark.sql(f"""
            ALTER TABLE delta.`{lk_products_path}` 
            RENAME COLUMN cuisine_type TO food_category
        """)
        print("   • 'cuisine_type' → 'food_category'")

        print("✅ Column mapping completed successfully")

        # TODO Show the renamed columns
        renamed_df = spark.read.format("delta").load(lk_products_path)
        print("\n Schema after column mapping:")
        renamed_df.printSchema()

        print(" Data with business-friendly column names:")
        renamed_df.show(3)

        print("\n💡 Column mapping benefits:")
        print("   • Zero data movement - only metadata changes")
        print("   • Backward compatibility maintained")
        print("   • Business users get friendly column names")
        print("   • Physical storage unchanged")

        return renamed_df

    except Exception as e:
        error_msg = str(e)
        print(f"⚠️  Column mapping failed: {error_msg[:100]}...")

        # TODO Check if it's a protocol version issue
        if "protocol version" in error_msg.lower() or "writer version" in error_msg.lower():
            print("ℹ️  Current Delta Lake version doesn't support column mapping")
            print("ℹ️  Column mapping requires Delta Lake with writer version 5+")

        print("🔄 Using business view as alternative")

        # TODO Fallback: Create a view with business-friendly names
        spark.sql(f"""
            CREATE OR REPLACE VIEW products_business_view AS
            SELECT 
                product_id,
                name AS product_name,
                price AS unit_price,
                cuisine_type AS food_category
            FROM delta.`{lk_products_path}`
        """)

        view_df = spark.sql("SELECT * FROM products_business_view")
        print("✅ Created business-friendly view with renamed columns")
        print("📊 Business view with friendly names:")
        view_df.show(3)

        print("\n💡 View-based approach:")
        print("   • Provides business-friendly names")
        print("   • Works with any Delta Lake version")
        print("   • No table structure changes needed")

        return spark.read.format("delta").load(lk_products_path)


def generated_columns(spark):
    """Demo: Generated columns and computed analytics"""

    lk_customers_path = "s3a://owshq-uber-eats-lakehouse/demo/customers"

    # TODO Enable generated columns feature
    try:
        # TODO Set the configuration for generated columns
        spark.conf.set("spark.databricks.delta.properties.defaults.enableColumnDefaults", "true")

        # TODO Create table with generated columns using proper syntax
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS customers_analytics (
                customer_id BIGINT,
                first_name STRING,
                last_name STRING,
                email STRING,
                full_name STRING GENERATED ALWAYS AS (first_name || ' ' || last_name),
                email_domain STRING GENERATED ALWAYS AS (split(email, '@')[1])
            ) USING DELTA 
            LOCATION '{lk_customers_path}'
            TBLPROPERTIES (
                'delta.feature.allowColumnDefaults' = 'supported',
                'delta.columnMapping.mode' = 'none'
            )
        """)

        print("✅ Created table with generated columns")

        # TODO Insert data - generated columns will be computed automatically
        spark.sql("""
                  INSERT INTO customers_analytics (customer_id, first_name, last_name, email)
                  VALUES (1, 'John', 'Doe', 'john@gmail.com'),
                         (2, 'Maria', 'Silva', 'maria@yahoo.com'),
                         (3, 'Carlos', 'Santos', 'carlos@hotmail.com')
                  """)

        print("✅ Data inserted - generated columns computed automatically")
        result_df = spark.sql("SELECT * FROM customers_analytics")

        # TODO Show the power of generated columns
        print("💡 Generated columns automatically computed:")
        print("   • full_name = first_name || ' ' || last_name")
        print("   • email_domain = split(email, '@')[1]")

    except Exception as e:
        print(f"⚠️  Generated columns failed: {str(e)[:100]}...")
        print("🔄 Using manual computation approach")

        # TODO Fallback: Manual computed columns
        customer_schema = StructType([
            StructField("customer_id", IntegerType(), True),
            StructField("first_name", StringType(), True),
            StructField("last_name", StringType(), True),
            StructField("email", StringType(), True)
        ])

        manual_data = spark.createDataFrame([
            (1, "John", "Doe", "john@gmail.com"),
            (2, "Maria", "Silva", "maria@yahoo.com"),
            (3, "Carlos", "Santos", "carlos@hotmail.com")
        ], customer_schema)

        # TODO Add computed columns manually
        result_df = manual_data.withColumn(
            "full_name", concat(col("first_name"), lit(" "), col("last_name"))
        ).withColumn(
            "email_domain", expr("split(email, '@')[1]")
        )

        result_df.write.format("delta").mode("overwrite").save(lk_customers_path)
        print("✅ Manual computed columns created")

    print("Customer analytics with computed fields:")
    result_df.show(truncate=False)

    return result_df


def check_constraints(spark):
    """Demo: CHECK constraints for business rules"""

    lk_orders_constraints_path = "s3a://owshq-uber-eats-lakehouse/demo/orders"

    # TODO Try CHECK constraints
    try:
        spark.sql(f"""
            CREATE OR REPLACE TABLE orders_business_rules (
                order_id STRING, total_amount DOUBLE, delivery_fee DOUBLE, order_status STRING
            ) USING DELTA LOCATION '{lk_orders_constraints_path}'
        """)

        # TODO Add business constraints
        spark.sql(f"ALTER TABLE delta.`{lk_orders_constraints_path}` ADD CONSTRAINT positive_amount CHECK (total_amount > 0)")
        spark.sql(f"ALTER TABLE delta.`{lk_orders_constraints_path}` ADD CONSTRAINT valid_fee CHECK (delivery_fee >= 0 AND delivery_fee <= 15.99)")

        print("✅ Business rules added:")
        print("   • total_amount > 0")
        print("   • delivery_fee between 0 and 15.99")

        # TODO Test valid data
        spark.sql(f"""
            INSERT INTO delta.`{lk_orders_constraints_path}` VALUES
            ('ORD001', 25.99, 3.99, 'delivered'),
            ('ORD002', 18.50, 2.99, 'confirmed')
        """)

        print("✅ Valid orders inserted")

        # TODO Test constraint violation
        try:
            spark.sql(f"INSERT INTO delta.`{lk_orders_constraints_path}` VALUES ('ORD003', -10.00, 3.99, 'placed')")
            print("❌ Constraint enforcement failed")
        except:
            print("✅ Business rule blocked invalid order amount")

        result_df = spark.read.format("delta").load(lk_orders_constraints_path)

    except:
        print("⚠️  CHECK constraints not supported - using manual validation")

        # TODO Manual validation
        manual_data = spark.createDataFrame([
            ("ORD001", 25.99, 3.99, "delivered"),
            ("ORD002", 18.50, 2.99, "confirmed")
        ], ["order_id", "total_amount", "delivery_fee", "order_status"])

        # TODO Apply business rules
        result_df = manual_data.filter(
            (col("total_amount") > 0) &
            (col("delivery_fee") >= 0) & (col("delivery_fee") <= 15.99)
        )

        result_df.write.format("delta").mode("overwrite").save(lk_orders_constraints_path)
        print("✅ Manual business rules validation applied")

    print("📊 Orders following business rules:")
    result_df.show()

    return result_df


def main():
    """Main execution function"""

    spark = spark_session()

    # TODO demos
    # schema_enforcement(spark)
    # schema_evolution(spark)
    # column_mapping(spark)
    # generated_columns(spark)
    # check_constraints(spark)

    spark.stop()


if __name__ == "__main__":
    main()
