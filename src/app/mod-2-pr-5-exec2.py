from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType, TimestampType
from pyspark.sql.functions import col, count, when, isnan, max, min, concat_ws, current_date, year, trim, avg, round

spark = SparkSession.builder \
    .appName("Data Ingestion") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

source_path_orders = "./storage/kafka/orders/01JS4W5A7XY65S9Z69BY51BEJ4.jsonl"
source_path_drivers = "./storage/postgres/drivers/01JS4W5A74BK7P4BPTJV1D3MHA.jsonl"
source_path_restaurants = "./storage/mysql/restaurants/01JS4W5A7YWTYRQKDA7F7N95VY.jsonl"
target_path = "./storage/parquet/drivers/"

drivers_schema = StructType([
    StructField("country", StringType(), True),
    StructField("date_birth", DateType(), True),
    StructField("city", StringType(), True),
    StructField("vehicle_year", IntegerType(), True),
    StructField("phone_number", StringType(), True),
    StructField("license_number", StringType(), True),
    StructField("vehicle_make", DateType(), True),
    StructField("uuid", StringType(), True),
    StructField("vehicle_model", StringType(), True),
    StructField("driver_id", IntegerType(), True),
    StructField("last_name", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("vehicle_type", StringType(), True),
    StructField("dt_current_timestamp", TimestampType(), True)
])

df_orders = spark.read \
    .json(source_path_orders)

df_rest = spark.read \
    .json(source_path_restaurants)

df_drivers = spark.read \
    .schema(drivers_schema) \
    .json(source_path_drivers)

#df_rest.show(5, truncate=False)
#df_drivers.show(5, truncate=False)

for c in df_orders.columns:
    df_orders = df_orders.withColumn(c,trim(col(c)))

#df_orders.show(5, truncate=False)

def analyze_orders():
    """Generate an analysis report of the orders
    """
    df_high_value_orders = df_orders.join(df_rest, df_orders.restaurant_key == df_rest.cnpj, 'inner') \
        .filter(col('total_amount') > 100) \
        .select('order_id', 'name', round(col('total_amount'),2).alias('total_amount')) \
        .orderBy(col('total_amount').desc())

    df_orders_analyzed = df_orders.join(df_drivers, df_orders.driver_key == df_drivers.license_number, 'inner') \
        .groupBy(concat_ws(" ", df_drivers.first_name, df_drivers.last_name).alias("driver_name")) \
        .agg(count('order_id').alias('total_orders'), \
             max('total_amount').alias('max_order_amount'), \
             min('total_amount').alias('min_order_amount'),
             round(avg('total_amount'),2).alias('average')) \
        .select('driver_name', 'total_orders', 'max_order_amount', 'min_order_amount','average') \
        .orderBy(col('total_orders').desc())

    return {
        "high_value_orders": df_high_value_orders,
        "orders_analyzed": df_orders_analyzed
        }

report_orders = analyze_orders()

print("More Expensive Orders...")
report_orders["high_value_orders"].show(10, truncate=False)

print("Orders by Drivers...")
report_orders["orders_analyzed"].show(10, truncate=False)

spark.stop()