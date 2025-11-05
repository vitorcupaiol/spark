"""
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/jobs/app/mod-2-pr-21-minio-s3.py

# =======================================================
# Spark Integration with MinIO/S3
# =======================================================

To integrate Spark with MinIO/S3, the following JAR files are required:

1. hadoop-aws.jar:
   - Contains the S3A filesystem implementation (org.apache.hadoop.fs.s3a.S3AFileSystem)
   - Version should match your Hadoop version
   - Example: hadoop-aws-3.3.1.jar for Hadoop 3.3.1

2. aws-java-sdk-bundle.jar:
   - Contains the AWS SDK libraries used by hadoop-aws
   - Version must be compatible with your hadoop-aws version
   - Example: aws-java-sdk-bundle-1.11.901.jar

3. Optional format-specific JARs:
   - For Avro: spark-avro.jar
   - For ORC: orc-core.jar and orc-mapreduce.jar

## CONFIGURATION CATEGORIES

### 1. Basic Connection Configuration
.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") # S3A filesystem implementation
.config("spark.hadoop.fs.s3a.endpoint", "http://24.144.65.249:80") # MinIO server endpoint
.config("spark.hadoop.fs.s3a.access.key", "miniolake") # Access key
.config("spark.hadoop.fs.s3a.secret.key", "LakE142536@@") # Secret key
.config("spark.hadoop.fs.s3a.path.style.access", "true") # Use path-style instead of virtual-hosted style
.config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") # Disable SSL for testing environments

### 2. Connection Optimizations
.config("spark.hadoop.fs.s3a.connection.maximum", "100") # Connection pool size
.config("spark.hadoop.fs.s3a.threads.max", "20") # Maximum thread count for parallel operations
.config("spark.hadoop.fs.s3a.connection.timeout", "300000") # Connection timeout in milliseconds (5 minutes)
.config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000") # Timeout to establish connection (5 seconds)

### 3. Read Optimizations
.config("spark.hadoop.fs.s3a.readahead.range", "256K") # Readahead buffer size

### 4. Write Optimizations
.config("spark.hadoop.fs.s3a.fast.upload", "true") # Enable fast upload path
.config("spark.hadoop.fs.s3a.fast.upload.buffer", "disk") # Buffer type (memory, disk, array)
.config("spark.hadoop.fs.s3a.multipart.size", "64M") # Size of each multipart chunk
.config("spark.hadoop.fs.s3a.multipart.threshold", "64M") # Threshold to trigger multipart upload

### 5. Committer Optimizations
.config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") # Faster commit algorithm
.config("spark.hadoop.fs.s3a.committer.name", "directory") # Committer type

### 6. Parquet Optimizations
.config("spark.sql.parquet.filterPushdown", "true") # Enable pushdown of filters to Parquet
.config("spark.sql.parquet.mergeSchema", "false") # Disable schema merging (improves performance)
.config("spark.sql.parquet.columnarReaderBatchSize", "4096") # Batch size for columnar reader

### 7. Adaptive Query Execution
```python
.config("spark.sql.adaptive.enabled", "true") # Enable adaptive query execution
.config("spark.sql.adaptive.coalescePartitions.enabled", "true") # Enable partition coalescing
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from pyspark.sql.functions import col, count, avg, sum, min, max, stddev, percentile_approx
from pyspark.sql.functions import year, month, dayofmonth, hour, date_format, to_timestamp
from pyspark.sql.functions import when, expr, datediff, current_date, lit, unix_timestamp
from pyspark.sql.window import Window
import time

# TODO Initialize Spark session
spark = SparkSession.builder \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://24.144.65.249:80") \
    .config("spark.hadoop.fs.s3a.access.key", "miniolake") \
    .config("spark.hadoop.fs.s3a.secret.key", "LakE142536@@") \
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
    .getOrCreate()

# TODO configs
spark.sparkContext.setLogLevel("ERROR")

# TODO define schema
ratings_schema = StructType([
    StructField("rating_id", IntegerType(), True),
    StructField("uuid", StringType(), True),
    StructField("restaurant_identifier", StringType(), True),
    StructField("rating", IntegerType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("dt_current_timestamp", TimestampType(), True)
])

# TODO 1) reading data from MinIO S3
source_path = "s3a://owshq-shadow-traffic-uber-eats/mysql/ratings"
ratings_df = spark.read.schema(ratings_schema).json(source_path)

count = ratings_df.count()
print(f"Successfully read {count} ratings records")

print("\nRatings Schema:")
ratings_df.printSchema()

print("\nSample Ratings Data:")
ratings_df.show(5, truncate=False)

print("\nRatings Distribution:")
ratings_df.groupBy("rating").count().orderBy("rating").show()

# TODO 2) write data to MinIO S3
ratings_analysis = ratings_df.withColumn(
    "rating_category",
    when(col("rating") >= 4, "High")
    .when(col("rating") >= 3, "Medium")
    .otherwise("Low")
)

ratings_time_analysis = ratings_analysis.withColumn(
    "day_of_week", date_format(col("timestamp"), "EEEE")
).withColumn(
    "hour_of_day", hour(col("timestamp"))
)

print("\nTransformed Data Sample:")
ratings_time_analysis.show(5, truncate=False)

restaurant_ratings = ratings_df.groupBy("restaurant_identifier").agg(
    avg("rating").alias("avg_rating"),
    min("rating").alias("min_rating"),
    max("rating").alias("max_rating")
)

print("\nRestaurant Ratings Summary:")
restaurant_ratings.show(5, truncate=False)

start_time = time.time()
target_path = "s3a://ubereats-analytics/ratings_parquet"

print(f"\nWriting ratings data to {target_path}...")
ratings_df.write.mode("overwrite").partitionBy("rating").parquet(target_path)
print(f"Data written to Parquet in {time.time() - start_time:.2f} seconds")

analysis_path = "s3a://ubereats-analytics/ratings_analysis"
print(f"\nWriting analysis data to {analysis_path}...")
start_time = time.time()
restaurant_ratings.write.mode("overwrite").csv(analysis_path)
print(f"Analysis data written to CSV in {time.time() - start_time:.2f} seconds")

print("\nVerifying written data by reading it back:")
parquet_count = spark.read.parquet(target_path).count()
csv_count = spark.read.csv(analysis_path, header=True, inferSchema=True).count()

print(f"Read {parquet_count} records from Parquet files")
print(f"Read {csv_count} records from CSV files")

json_path = "s3a://ubereats-analytics/ratings_time_analysis"
print(f"\nWriting time analysis data to {json_path}...")
start_time = time.time()
ratings_time_analysis.write.mode("overwrite").option("compression", "gzip").json(json_path)
print(f"Time analysis data written to JSON in {time.time() - start_time:.2f} seconds")

# TODO Stop Spark session
spark.stop()
