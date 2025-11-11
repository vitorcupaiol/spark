from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType, TimestampType
from pyspark.sql.functions import col, count, when, isnan, max, min, concat_ws, current_date, year

spark = SparkSession.builder \
    .appName("Data Ingestion") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

source_path = "./storage/postgres/drivers/01JS4W5A74BK7P4BPTJV1D3MHA.jsonl"
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

df_drivers = spark.read \
    .schema(drivers_schema) \
    .json(source_path)

df_drivers.printSchema()

def driver_detail_report(df_drivers):
    """Generate a detailed report of the drivers DataFrame"""
    
    df_drivers_detail = df_drivers.select(
        concat_ws(" ", df_drivers.first_name, df_drivers.last_name).alias("name"),
        "city",
        "vehicle_type"
    )

    df_category = df_drivers.select('vehicle_type','vehicle_year').withColumn('this_year', year(current_date())) \
        .withColumn('age', col('this_year') - col('vehicle_year')) \
        .withColumn('vehicle_age_category', \
            when(col('age') < 5, 'new') \
            .when((col('age') >= 5) & (col('age') <= 10), 'mid_age') \
            .otherwise('old').alias('vehicle_age_category') )

    df_category_report = df_category.groupBy('vehicle_age_category').count().orderBy('count')

    return {
        "drivers_detail": df_drivers_detail,
        "vehicle_age_category_report": df_category_report
        }

# Generate the report
report = driver_detail_report(df_drivers)

# Display the results
print("Best Drivers:")
report["drivers_detail"].show(10, truncate=False)

print("\nCars Age Category:")
report["vehicle_age_category_report"].show(10, truncate=False)


#df_drivers.select('vehicle_type').distinct().show() 



#df_drivers.groupBy('vehicle_type').agg (\
#    max('vehicle_year').alias('max_vehicle_year'), \
#    min('vehicle_year').alias('min_vehicle_year') \
#    ).show() 

#df_drivers.agg (\
#    max('vehicle_year').alias('max_vehicle_year'), \
#    min('vehicle_year').alias('min_vehicle_year') \
#    ).show() 

spark.stop()