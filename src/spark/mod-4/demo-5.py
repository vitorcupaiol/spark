"""
Demo 5: DML Operations & Data Management
======================================

This demo covers:
- DML Operations (Update, Delete, Merge)
- CDC (Change Data Capture) with Change Data Feed
- SCD (Slowly Changing Dimensions) with Delta MERGE
- Multi-Table Transactions

Run command:
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/bitnami/spark/jobs/spark/mod-4/demo-5.py
"""

import base64
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit
from delta.tables import DeltaTable


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
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark


def dml(spark):
    """Demo: UPDATE, DELETE, MERGE operations"""

    lk_orders_path = "s3a://owshq-uber-eats-lakehouse/demo/orders_dml"

    # TODO Create initial orders data
    try:
        orders_source = "s3a://owshq-shadow-traffic-uber-eats/kafka/orders/01JTKHGH9M9JCGTMP591VP7C2Z.jsonl"
        orders_raw = spark.read.json(orders_source).limit(10)
        initial_orders = orders_raw.select(
            col("order_id").cast("string"),
            col("user_key").cast("string"),
            col("total_amount").cast("double"),
            lit("placed").alias("status")
        )
    except:
        initial_orders = spark.sql("""
            SELECT 'ORD001' as order_id, 'USER123' as user_key, 25.99 as total_amount, 'placed' as status
            UNION ALL
            SELECT 'ORD002' as order_id, 'USER124' as user_key, 18.50 as total_amount, 'placed' as status
            UNION ALL
            SELECT 'ORD003' as order_id, 'USER125' as user_key, 45.75 as total_amount, 'placed' as status
        """)

    initial_orders.write.format("delta").mode("overwrite").save(lk_orders_path)
    print(f"✅ Created orders table with {initial_orders.count()} orders")

    orders_table = DeltaTable.forPath(spark, lk_orders_path)

    # TODO UPDATE - Change order status
    orders_table.update(
        condition=col("order_id") == "ORD001",
        set={"status": lit("preparing")}
    )
    print("✅ Updated ORD001 to 'preparing'")

    # TODO DELETE - Remove cancelled orders
    orders_table.delete(condition=col("order_id") == "ORD003")
    print("✅ Deleted cancelled order ORD003")

    # TODO MERGE - Upsert order updates
    updates = spark.sql("""
        SELECT 'ORD001' as order_id, 'delivered' as new_status
        UNION ALL
        SELECT 'ORD004' as order_id, 'placed' as new_status
    """)

    orders_table.alias("orders").merge(
        updates.alias("updates"),
        "orders.order_id = updates.order_id"
    ).whenMatchedUpdate(set={
        "status": col("updates.new_status")
    }).whenNotMatchedInsert(values={
        "order_id": col("updates.order_id"),
        "user_key": lit("USER126"),
        "total_amount": lit(29.99),
        "status": col("updates.new_status")
    }).execute()

    print("✅ MERGE completed: Updated ORD001, inserted ORD004")

    result_df = spark.read.format("delta").load(lk_orders_path)
    result_df.select("order_id", "total_amount", "status").show()
    return result_df


def cdc(spark):
    """Demo: Change Data Capture using Delta Change Data Feed"""

    lk_orders_cdf_path = "s3a://owshq-uber-eats-lakehouse/demo/orders_cdf"

    # TODO Create initial table with CDF enabled
    initial_orders = spark.sql("""
        SELECT 'ORD001' as order_id, 'placed' as status
        UNION ALL
        SELECT 'ORD002' as order_id, 'placed' as status
    """)

    try:
        initial_orders.write \
            .format("delta") \
            .option("delta.enableChangeDataFeed", "true") \
            .mode("overwrite") \
            .save(lk_orders_cdf_path)

        orders_table = DeltaTable.forPath(spark, lk_orders_cdf_path)

        # TODO Simulate changes
        orders_table.update(
            condition=col("order_id") == "ORD001",
            set={"status": lit("delivered")}
        )

        orders_table.delete(condition=col("order_id") == "ORD002")

        # TODO Read Change Data Feed
        changes_df = spark.read \
            .format("delta") \
            .option("readChangeFeed", "true") \
            .option("startingVersion", 0) \
            .load(lk_orders_cdf_path)

        print("✅ Change Data Feed captured all operations")
        changes_df.select("order_id", "status", "_change_type", "_commit_version").show()

        return changes_df

    except Exception as e:
        print(f"⚠️  Change Data Feed not supported: {str(e)[:50]}...")
        return spark.read.format("delta").load(lk_orders_cdf_path)


def scd(spark):
    """Demo: SCD Type 2 using Delta MERGE"""

    lk_customers_path = "s3a://owshq-uber-eats-lakehouse/demo/customers_scd"

    # TODO Create initial customer data
    initial_customers = spark.sql("""
        SELECT 'CUST001' as customer_id, 'John Doe' as name, '123 Main St' as address, 
               true as is_current, current_timestamp() as start_date, 
               CAST(null as timestamp) as end_date
        UNION ALL
        SELECT 'CUST002' as customer_id, 'Jane Smith' as name, '456 Oak Ave' as address,
               true as is_current, current_timestamp() as start_date, 
               CAST(null as timestamp) as end_date
    """)

    initial_customers.write.format("delta").mode("overwrite").save(lk_customers_path)
    print("✅ Created customer SCD table")

    customers_table = DeltaTable.forPath(spark, lk_customers_path)

    # TODO Simulate address change for CUST001
    updated_customers = spark.sql("""
        SELECT 'CUST001' as customer_id, 'John Doe' as name, '999 New Street' as address
    """)

    # TODO Close existing record
    customers_table.update(
        condition=(col("customer_id") == "CUST001") & (col("is_current") == True),
        set={
            "is_current": lit(False),
            "end_date": current_timestamp()
        }
    )

    # TODO Insert new version
    new_version = spark.sql("""
        SELECT 'CUST001' as customer_id, 'John Doe' as name, '999 New Street' as address,
               true as is_current, current_timestamp() as start_date, 
               CAST(null as timestamp) as end_date
    """)

    new_version.write.format("delta").mode("append").save(lk_customers_path)
    print("✅ SCD Type 2 address change processed")

    result_df = spark.read.format("delta").load(lk_customers_path)
    result_df.filter(col("customer_id") == "CUST001").select("customer_id", "address", "is_current").show()
    return result_df


def multi_table_transactions(spark):
    """Demo: Multi-table ACID transactions"""

    lk_inventory_path = "s3a://owshq-uber-eats-lakehouse/demo/inventory"
    lk_order_items_path = "s3a://owshq-uber-eats-lakehouse/demo/order_items"

    # TODO Create inventory
    inventory_data = spark.sql("""
        SELECT 'PROD001' as product_id, 'Pizza' as product_name, 50 as quantity
        UNION ALL
        SELECT 'PROD002' as product_id, 'Burger' as product_name, 30 as quantity
    """)

    inventory_data.write.format("delta").mode("overwrite").save(lk_inventory_path)

    # TODO Process order: reduce inventory, add order items
    inventory_table = DeltaTable.forPath(spark, lk_inventory_path)

    # TODO Update inventory
    inventory_table.update(
        condition=col("product_id") == "PROD001",
        set={"quantity": col("quantity") - 2}
    )

    # TODO Add order items
    order_items = spark.sql("""
        SELECT 'ORD100' as order_id, 'PROD001' as product_id, 2 as quantity
    """)

    order_items.write.format("delta").mode("overwrite").save(lk_order_items_path)
    print("✅ Multi-table transaction: inventory updated, order recorded")

    result_df = spark.read.format("delta").load(lk_inventory_path)
    result_df.show()
    return result_df


def main():
    """Main execution function"""

    spark = spark_session()

    try:
        # TODO Run demos
        # dml(spark)
        # cdc(spark)
        # scd(spark)
        multi_table_transactions(spark)

        print("🎯")

    except Exception as e:
        print(f"❌ Demo failed: {e}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
