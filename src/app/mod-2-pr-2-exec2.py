from pyspark.sql import SparkSession

# Basic configuration
spark = SparkSession.builder \
    .appName("ResourceOptimizedApp") \
    .master("local[*]") \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.cores", "2") \
    .config("spark.default.parallelism", "8") \
    .config("spark.sql.shuffle.partitions", "20") \
    .getOrCreate()

input_path = './storage/kafka/orders/01JS4W5A7XY65S9Z69BY51BEJ4.jsonl'

# Load data from the repository storage
df_orders = spark.read.json(input_path)
df_orders.show()

# Always stop the session when done
spark.stop()