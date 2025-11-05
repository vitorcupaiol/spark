"""
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/jobs/app/mod-2-pr-18-data-delivery.py
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .config("spark.sql.warehouse.dir", "/opt/bitnami/spark/jobs/app/warehouse") \
    .enableHiveSupport() \
    .getOrCreate()

# TODO set config
spark.sparkContext.setLogLevel("ERROR")
spark.sql("SET spark.sql.echo=true")
spark.conf.set("spark.sql.legacy.allowNonEmptyLocationInCTAS", "true")

spark.sql("""
CREATE OR REPLACE TEMPORARY VIEW restaurants_temp
USING json
OPTIONS (path "./storage/mysql/restaurants/01JS4W5A7YWTYRQKDA7F7N95VY.jsonl")
""")

spark.sql("""
CREATE OR REPLACE TEMPORARY VIEW drivers_temp
USING json
OPTIONS (path "./storage/postgres/drivers/01JS4W5A74BK7P4BPTJV1D3MHA.jsonl")
""")

spark.sql("""
CREATE OR REPLACE TEMPORARY VIEW orders_temp
USING json
OPTIONS (path "./storage/kafka/orders/01JS4W5A7XY65S9Z69BY51BEJ4.jsonl")
""")

# TODO 1. creating permanent tables
spark.sql("CREATE DATABASE IF NOT EXISTS analytics")
spark.sql("USE analytics")

spark.sql("""
CREATE TABLE IF NOT EXISTS restaurants
USING PARQUET
AS SELECT * FROM restaurants_temp
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS restaurant_summary
USING PARQUET
AS
SELECT 
    restaurant_id,
    name,
    cuisine_type,
    city,
    average_rating,
    num_reviews,
    average_rating * LOG(10, GREATEST(num_reviews, 10)) AS popularity_score
FROM restaurants_temp
""")

spark.sql("SHOW TABLES IN analytics").show()
spark.sql("DESCRIBE FORMATTED analytics.restaurants").show(10, truncate=False)

# TODO 2. managed vs. external tables
spark.sql("""
CREATE TABLE IF NOT EXISTS managed_orders
USING PARQUET
AS SELECT * FROM orders_temp
""")

orders_path = "/opt/bitnami/spark/jobs/app/warehouse/external_orders"
spark.sql("SELECT * FROM orders_temp").write \
    .format("parquet") \
    .mode("overwrite") \
    .save(orders_path)

spark.sql(f"""
CREATE EXTERNAL TABLE IF NOT EXISTS external_orders
USING PARQUET
LOCATION '{orders_path}'
""")

spark.sql("SHOW TABLES IN analytics").show()

# TODO 3. partitioning for performance
spark.sql("""
CREATE TABLE IF NOT EXISTS orders_by_cuisine
USING PARQUET
PARTITIONED BY (cuisine_type)
AS
SELECT 
    o.order_id,
    o.total_amount,
    o.order_date,
    r.name AS restaurant_name,
    r.cuisine_type
FROM orders_temp o
JOIN restaurants_temp r ON o.restaurant_key = r.cnpj
""")

spark.sql("SHOW PARTITIONS orders_by_cuisine").show(10)

spark.sql("""
CREATE TABLE IF NOT EXISTS orders_by_cuisine_and_date
USING PARQUET
PARTITIONED BY (cuisine_type, order_year)
AS
SELECT 
    o.order_id,
    o.total_amount,
    o.order_date,
    r.name AS restaurant_name,
    r.cuisine_type,
    YEAR(o.order_date) AS order_year
FROM orders_temp o
JOIN restaurants_temp r ON o.restaurant_key = r.cnpj
""")

spark.sql("SHOW PARTITIONS orders_by_cuisine_and_date").show(10)

italian_orders = spark.sql("""
SELECT * FROM orders_by_cuisine
WHERE cuisine_type = 'Italian'
""")
print(f"Found {italian_orders.count()} Italian orders")

# TODO 4. bucketing for performance
spark.sql("""
CREATE TABLE IF NOT EXISTS restaurants_bucketed
USING PARQUET
CLUSTERED BY (cuisine_type) INTO 4 BUCKETS
AS SELECT * FROM restaurants_temp
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS orders_bucketed
USING PARQUET
CLUSTERED BY (restaurant_key) INTO 4 BUCKETS
AS SELECT * FROM orders_temp
""")

bucketed_join = spark.sql("""
SELECT 
    o.order_id,
    r.name AS restaurant_name,
    o.total_amount
FROM orders_bucketed o
JOIN restaurants_bucketed r ON o.restaurant_key = r.cnpj
LIMIT 5
""")

# TODO 5. file format selection
for format in ["parquet", "orc", "csv"]:
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS restaurants_{format}
    USING {format}
    AS SELECT * FROM restaurants_temp
    """)

# TODO 6. table statistics for optimization
spark.sql("ANALYZE TABLE restaurants COMPUTE STATISTICS")

spark.sql("""
ANALYZE TABLE restaurants 
COMPUTE STATISTICS FOR COLUMNS cuisine_type, average_rating, num_reviews
""")

spark.stop()
