from pyspark.sql import SparkSession

# Basic configuration
spark = SparkSession.builder \
    .appName("UberEatsAnalytics") \
    .master("local[*]") \
    .getOrCreate()

input_path = './storage/postgres/drivers/01JS4W5A74BK7P4BPTJV1D3MHA.jsonl'

# Load data from the repository storage
df_drivers = spark.read.json(input_path)
df_drivers.show()

# Always stop the session when done
spark.stop()