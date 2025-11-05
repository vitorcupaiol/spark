# PySpark Foundations - Part 5: Data Delivery

## Introduction

After ingesting, transforming, and analyzing data, the final crucial step is delivering the processed data to various destinations in an optimized format. This lesson covers how to write data efficiently, optimize storage, and implement strategies for handling large volumes.

## Setting Up Our Environment

Let's set up our Spark session and load our processed UberEats datasets:

```python
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import col, count, sum, avg, max, min, round, desc

# Create a Spark session
spark = SparkSession.builder \
    .appName("Data Delivery") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "20") \
    .getOrCreate()

# Load datasets from previous lessons
restaurants_df = spark.read.json("./storage/mysql/restaurants/01JS4W5A7YWTYRQKDA7F7N95VY.jsonl")
drivers_df = spark.read.json("./storage/postgres/drivers/01JS4W5A74BK7P4BPTJV1D3MHA.jsonl")
orders_df = spark.read.json("./storage/kafka/orders/01JS4W5A7XY65S9Z69BY51BEJ4.jsonl")

# Prepare some processed data for delivery examples
restaurant_analytics = restaurants_df.select(
    "name", "cuisine_type", "city", "country", "average_rating", "num_reviews"
).withColumn(
    "popularity_score", 
    round(col("average_rating") * F.sqrt(col("num_reviews") / 1000), 2)
)

# Show sample data
print("Restaurant Analytics Data:")
restaurant_analytics.show(5)
```

## 1. Writing Data to Different Destinations

PySpark supports writing data to various formats and destinations.

### Basic File Formats

```python
# Define base output path
output_path = "./storage/output"

# Writing to CSV
restaurant_analytics.write \
    .option("header", "true") \
    .csv(f"{output_path}/restaurant_analytics_csv")

# Writing to JSON
restaurant_analytics.write \
    .json(f"{output_path}/restaurant_analytics_json")

# Writing to Parquet
restaurant_analytics.write \
    .parquet(f"{output_path}/restaurant_analytics_parquet")

# Writing to ORC
restaurant_analytics.write \
    .orc(f"{output_path}/restaurant_analytics_orc")

print("Data written to various file formats!")
```

### Write Modes

```python
# Define a path for demonstrating write modes
demo_path = f"{output_path}/write_modes_demo"

# Error if exists (default)
try:
    restaurant_analytics.write.mode("errorifexists").parquet(f"{demo_path}/error_mode")
    print("Data written with 'errorifexists' mode")
except Exception as e:
    print(f"Error with 'errorifexists' mode: {e}")

# Overwrite - replaces existing data
restaurant_analytics.write.mode("overwrite").parquet(f"{demo_path}/overwrite_mode")
print("Data written with 'overwrite' mode")

# Append - adds to existing data
restaurant_analytics.write.mode("append").parquet(f"{demo_path}/append_mode")
print("Data written with 'append' mode")

# Ignore - does nothing if data exists
restaurant_analytics.write.mode("ignore").parquet(f"{demo_path}/ignore_mode")
print("Data written with 'ignore' mode")
```

### Writing to Databases

```python
# Writing to a relational database (example with PostgreSQL)
# Note: This requires the appropriate JDBC driver in the classpath
# restaurant_analytics.write \
#     .format("jdbc") \
#     .option("url", "jdbc:postgresql://localhost:5432/ubereats") \
#     .option("dbtable", "restaurant_analytics") \
#     .option("user", "username") \
#     .option("password", "password") \
#     .mode("overwrite") \
#     .save()

# Example with SQLite (simpler for demonstration)
# restaurant_analytics.write \
#     .format("jdbc") \
#     .option("url", "jdbc:sqlite:ubereats.db") \
#     .option("dbtable", "restaurant_analytics") \
#     .mode("overwrite") \
#     .save()
```

## 2. Storage Optimization Techniques

Optimize your data storage for better performance and reduced size.

### Compression Options

```python
# Write with different compression codecs
# GZIP compression (good compression ratio, but not splittable)
restaurant_analytics.write \
    .option("compression", "gzip") \
    .parquet(f"{output_path}/compressed/gzip")

# SNAPPY compression (moderate compression, splittable)
restaurant_analytics.write \
    .option("compression", "snappy") \
    .parquet(f"{output_path}/compressed/snappy")

# ZSTD compression (good compression ratio and performance)
restaurant_analytics.write \
    .option("compression", "zstd") \
    .parquet(f"{output_path}/compressed/zstd")

print("Compressed data written!")
```

### File Size Optimization

```python
# Control the maximum records per file
restaurant_analytics.repartition(1).write \
    .option("maxRecordsPerFile", 100) \
    .parquet(f"{output_path}/file_size_optimized")

print("Size-optimized data written!")
```

## 3. Partitioning and Bucketing

These techniques optimize both storage and query performance.

### Partitioning Data

```python
# Partition by a single column
restaurant_analytics.write \
    .partitionBy("cuisine_type") \
    .parquet(f"{output_path}/partitioned_by_cuisine")

# Partition by multiple columns
restaurant_analytics.write \
    .partitionBy("country", "city") \
    .parquet(f"{output_path}/partitioned_by_location")

print("Partitioned data written!")

# Reading partitioned data with filtering
# This will leverage partition pruning for better performance
italian_restaurants = spark.read.parquet(f"{output_path}/partitioned_by_cuisine") \
    .filter(col("cuisine_type") == "Italian")

print(f"Italian restaurants count (from partitioned data): {italian_restaurants.count()}")
```

### Dynamic Partitioning

```python
# Enable dynamic partitioning
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# Write with dynamic partitioning
restaurant_analytics.write \
    .partitionBy("cuisine_type") \
    .mode("overwrite") \
    .parquet(f"{output_path}/dynamic_partitioned")

print("Dynamic partitioned data written!")
```

### Bucketing

```python
# Bucketing (requires saving as table)
# Create a temporary database
spark.sql("CREATE DATABASE IF NOT EXISTS ubereats_demo")
spark.sql("USE ubereats_demo")

# Save as bucketed table
restaurant_analytics.write \
    .bucketBy(4, "city") \
    .sortBy("average_rating") \
    .saveAsTable("restaurant_analytics_bucketed")

print("Bucketed table created!")

# Query the bucketed table
bucketed_data = spark.sql("SELECT * FROM restaurant_analytics_bucketed LIMIT 5")
bucketed_data.show()
```

## 4. Strategies for Large Volumes

When dealing with large datasets, specific strategies can help optimize delivery.

### Optimizing Partitioning for Large Datasets

```python
# For large datasets, choose partition columns carefully
# - Use columns with moderate cardinality
# - Avoid too many small partitions
# - Consider data skew

# Example: Partition by year + month for time-series data
orders_df = orders_df.withColumn(
    "order_timestamp", 
    F.to_timestamp("order_date")
)

orders_df = orders_df.withColumn("year", F.year("order_timestamp")) \
    .withColumn("month", F.month("order_timestamp"))

# Write partitioned by year and month
orders_df.write \
    .partitionBy("year", "month") \
    .parquet(f"{output_path}/orders_by_time")

print("Time-partitioned orders written!")
```

### Controlling Partition Sizes

```python
# Repartition before writing to control file sizes
# For a 10GB dataset, aiming for ~128MB files:
# num_partitions = 10GB / 128MB = ~80 partitions

# For our small demo, we'll use a small number
balanced_data = restaurant_analytics.repartition(4)

balanced_data.write \
    .parquet(f"{output_path}/balanced_partitions")

print("Size-balanced data written!")
```

### Writing Large Datasets Efficiently

```python
# For very large datasets:
# 1. Use appropriate compression (e.g., Snappy for balance of speed and size)
# 2. Use columnar formats (Parquet or ORC)
# 3. Apply partitioning based on common query filters
# 4. Control partition sizes

# Example - complete strategy for large dataset
def write_large_dataset(df, path, partition_cols=None):
    """
    Write a large dataset efficiently with best practices:
    - Use Parquet with Snappy compression
    - Apply appropriate partitioning
    - Balance partition sizes
    """
    # Estimate a good partition count (example logic)
    # In production, you'd base this on estimated dataset size
    partition_count = 4  # For demo; would be larger in production
    
    # Prepare the dataframe
    if partition_cols:
        # When partitioning by columns, we don't need as many partitions
        # for the underlying data
        write_df = df.repartition(max(2, partition_count // len(partition_cols)))
        write_method = write_df.write.partitionBy(*partition_cols)
    else:
        # Without column partitioning, use regular partitions
        write_df = df.repartition(partition_count)
        write_method = write_df.write
        
    # Write with optimized settings
    write_method \
        .format("parquet") \
        .option("compression", "snappy") \
        .mode("overwrite") \
        .save(path)
    
    return path

# Apply the function
large_dataset_path = write_large_dataset(
    restaurant_analytics, 
    f"{output_path}/large_dataset_optimized", 
    partition_cols=["cuisine_type"]
)

print(f"Large dataset written efficiently to {large_dataset_path}")
```

## 5. Practical Application: Complete UberEats ETL Pipeline

Let's build a complete pipeline that processes and delivers UberEats data optimally:

```python
def ubereats_etl_pipeline(spark, input_path, output_path):
    """
    Complete ETL pipeline for UberEats data:
    1. Ingest data from various sources
    2. Transform and analyze the data
    3. Deliver to optimized storage formats
    """
    # Step 1: Data Ingestion
    print("Starting data ingestion...")
    restaurants_df = spark.read.json(f"{input_path}/mysql/restaurants/01JS4W5A7YWTYRQKDA7F7N95VY.jsonl")
    drivers_df = spark.read.json(f"{input_path}/postgres/drivers/01JS4W5A74BK7P4BPTJV1D3MHA.jsonl")
    orders_df = spark.read.json(f"{input_path}/kafka/orders/01JS4W5A7XY65S9Z69BY51BEJ4.jsonl")
    
    # Step 2: Data Transformation and Analysis
    print("Performing transformations and analysis...")
    # Join data
    joined_data = orders_df.join(
        restaurants_df,
        orders_df.restaurant_key == restaurants_df.cnpj,
        "inner"
    ).join(
        drivers_df,
        orders_df.driver_key == drivers_df.license_number,
        "left"
    )
    
    # Create analytics views
    # 1. Restaurant performance
    restaurant_performance = joined_data.groupBy(
        "name", "cuisine_type", "city", "country"
    ).agg(
        count("order_id").alias("order_count"),
        round(sum("total_amount"), 2).alias("total_revenue"),
        round(avg("total_amount"), 2).alias("avg_order_value"),
        round(avg("average_rating"), 2).alias("avg_rating")
    ).withColumn(
        "performance_score",
        round((col("avg_rating") * 0.3) + (col("order_count") / 10 * 0.7), 2)
    )
    
    # 2. Driver performance
    driver_performance = joined_data.filter(col("driver_id").isNotNull()).groupBy(
        "driver_id", "first_name", "last_name", "vehicle_type", "city"
    ).agg(
        count("order_id").alias("delivery_count"),
        round(sum("total_amount"), 2).alias("delivery_value"),
        round(avg("total_amount"), 2).alias("avg_delivery_value")
    )
    
    # 3. Location analytics
    location_analytics = joined_data.groupBy(
        "country", "city"
    ).agg(
        count("order_id").alias("order_count"),
        count("restaurant_id").alias("restaurant_count"),
        round(avg("total_amount"), 2).alias("avg_order_value"),
        round(sum("total_amount"), 2).alias("total_revenue")
    )
    
    # Step 3: Data Delivery
    print("Delivering data...")
    
    # 1. Restaurant Performance - partitioned by cuisine_type
    restaurant_performance \
        .repartition(4) \
        .write \
        .partitionBy("cuisine_type") \
        .format("parquet") \
        .option("compression", "snappy") \
        .mode("overwrite") \
        .save(f"{output_path}/analytics/restaurant_performance")
    
    # 2. Driver Performance - partitioned by city
    driver_performance \
        .repartition(2) \
        .write \
        .partitionBy("city") \
        .format("parquet") \
        .option("compression", "snappy") \
        .mode("overwrite") \
        .save(f"{output_path}/analytics/driver_performance")
    
    # 3. Location Analytics - partitioned by country
    location_analytics \
        .repartition(1) \
        .write \
        .partitionBy("country") \
        .format("parquet") \
        .option("compression", "snappy") \
        .mode("overwrite") \
        .save(f"{output_path}/analytics/location_analytics")
    
    # 4. Summary Statistics - as CSV for easy sharing
    restaurant_summary = restaurant_performance \
        .groupBy("cuisine_type") \
        .agg(
            count("*").alias("restaurant_count"),
            round(avg("total_revenue"), 2).alias("avg_revenue"),
            round(avg("avg_rating"), 2).alias("avg_rating")
        ).orderBy(desc("restaurant_count"))
    
    restaurant_summary \
        .coalesce(1) \
        .write \
        .option("header", "true") \
        .mode("overwrite") \
        .csv(f"{output_path}/reports/restaurant_summary")
    
    print("ETL pipeline completed successfully!")
    
    return {
        "restaurant_performance": restaurant_performance,
        "driver_performance": driver_performance,
        "location_analytics": location_analytics,
        "restaurant_summary": restaurant_summary
    }

# Run the pipeline
result = ubereats_etl_pipeline(spark, "./storage", "./storage/output")

# Display some results
print("\nRestaurant Summary:")
result["restaurant_summary"].show(10)

print("\nTop Performing Restaurants:")
result["restaurant_performance"].orderBy(desc("performance_score")).show(5)
```

## Conclusion and Best Practices

In this lesson, we've covered the essential aspects of data delivery in PySpark:

1. **Writing to Different Destinations**: Supporting various file formats and storage systems
2. **Storage Optimization**: Using compression and file size management
3. **Partitioning and Bucketing**: Organizing data for optimal query performance
4. **Large Volume Strategies**: Techniques for handling big datasets efficiently
5. **Complete ETL Pipeline**: Putting it all together in a production-ready workflow

**Best Practices for Data Delivery:**

- **Choose the right format**:
  - Use Parquet or ORC for analytical workloads
  - Use CSV for data that needs to be human-readable or compatible with other tools
  - Use JSON for semi-structured data or API integrations

- **Optimize partitioning**:
  - Partition by columns used in frequent filters
  - Avoid over-partitioning with high-cardinality columns
  - Consider multi-level partitioning for time-series data (year/month/day)

- **Manage resources and performance**:
  - Control the number of output files with repartition/coalesce
  - Use appropriate compression (Snappy for balanced performance, ZSTD for better ratios)
  - Set appropriate write modes (overwrite, append) based on your use case

- **Consider the downstream consumers**:
  - Optimize for how the data will be queried
  - Document the schema and partitioning strategy
  - Consider creating both detailed and aggregated views

This completes our PySpark Foundations series! You now have a solid understanding of the entire data processing lifecycle with PySpark: ingestion, basic transformations, advanced transformations, advanced techniques, and data delivery.

## Exercise

1. Extend the UberEats ETL pipeline to:
   - Handle incremental data processing (append mode with date partitioning)
   - Create different output formats for different consumers (Parquet for data scientists, CSV for business analysts)
   - Implement optimized partitioning based on expected query patterns

2. Build a complete data pipeline that:
   - Processes the ratings data
   - Combines it with the restaurant data
   - Delivers it in an optimized format with appropriate partitioning
   - Includes a summary report in CSV format

## Resources

- [Spark SQL Data Sources](https://spark.apache.org/docs/latest/sql-data-sources.html)
- [Parquet Files](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html)
- [Performance Tuning](https://spark.apache.org/docs/latest/sql-performance-tuning.html)
