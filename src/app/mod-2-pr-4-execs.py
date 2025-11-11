from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType, TimestampType
from pyspark.sql.functions import col, count, when, isnan

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
    .json(source_path)

df_drivers.printSchema()

df_drivers.show(10)
print(f'Número de motoristas: {df_drivers.count()}')

df = df_drivers

def analyze_null_values(df):
    """Analyze missing values in each column of a DataFrame"""
    # Count records with null/nan values in each column
    null_counts = df.select([
        count(when(col(c).isNull() | isnan(c), c)).alias(c)
        for c in df.columns
    ]).collect()[0].asDict()
    
    # Print results
    print("Missing Value Analysis:")
    for col_name, null_count in null_counts.items():
        percentage = 100.0 * null_count / df.count()
        print(f"- {col_name}: {null_count} missing values ({percentage:.2f}%)")
    
    return null_counts

#null_counts = analyze_null_values(df_drivers)

#print(null_counts)

df_drivers.groupBy('vehicle_type','vehicle_year').count() \
    .filter(col('count') > 1).show(10)

df_drivers.write.mode("overwrite").parquet(target_path)

spark.stop()