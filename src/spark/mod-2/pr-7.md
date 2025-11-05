# PySpark Foundations - Part 4: Advanced Techniques

## Introduction

Building on our understanding of data ingestion and transformations, we now explore advanced PySpark techniques essential for implementing complex logic, optimizing performance, and conducting sophisticated time-series analysis.

## Setting Up Our Environment

Let's set up our Spark session and load our UberEats datasets:

```python
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import col, count, sum, avg, max, min, round, desc

# Create a Spark session
spark = SparkSession.builder \
    .appName("Advanced Techniques") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "20") \
    .getOrCreate()

# Load datasets
restaurants_df = spark.read.json("./storage/mysql/restaurants/01JS4W5A7YWTYRQKDA7F7N95VY.jsonl")
drivers_df = spark.read.json("./storage/postgres/drivers/01JS4W5A74BK7P4BPTJV1D3MHA.jsonl")
orders_df = spark.read.json("./storage/kafka/orders/01JS4W5A7XY65S9Z69BY51BEJ4.jsonl")
ratings_df = spark.read.json("./storage/mysql/ratings/01JS4W5A7YWTYRQKDA7F7N95VZ.jsonl")

# Display record counts
print(f"Restaurants: {restaurants_df.count()} records")
print(f"Drivers: {drivers_df.count()} records")
print(f"Orders: {orders_df.count()} records")
print(f"Ratings: {ratings_df.count()} records")
```

## 1. User Defined Functions (UDFs)

UDFs allow you to apply custom logic that isn't available in the built-in functions.

### Creating a Basic UDF

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, DoubleType, IntegerType

# Create a simple UDF to categorize restaurants by rating
@udf(returnType=StringType())
def categorize_rating(rating):
    if rating is None:
        return "Unknown"
    elif rating >= 4.5:
        return "Exceptional"
    elif rating >= 4.0:
        return "Excellent"
    elif rating >= 3.5:
        return "Very Good"
    elif rating >= 3.0:
        return "Good"
    elif rating >= 2.0:
        return "Average"
    else:
        return "Poor"

# Apply the UDF to our data
restaurants_with_category = restaurants_df.withColumn(
    "rating_category",
    categorize_rating(col("average_rating"))
)

# Display results
print("Restaurants with Rating Categories:")
restaurants_with_category.select(
    "name", "average_rating", "rating_category"
).show(10)
```

### UDFs with Complex Logic

```python
# Create a UDF for a custom restaurant score
@udf(returnType=DoubleType())
def calculate_restaurant_score(rating, reviews):
    import math
    if rating is None or reviews is None:
        return None
    
    # Custom algorithm: rating ^ 2 * log(reviews + 1)
    # - Emphasizes higher ratings exponentially
    # - Uses logarithmic scale for review count to balance impact
    return float(rating ** 2 * math.log(reviews + 1))

# Apply the complex UDF
restaurants_with_score = restaurants_df.withColumn(
    "custom_score",
    calculate_restaurant_score(col("average_rating"), col("num_reviews"))
)

# Display results sorted by score
print("Restaurants with Custom Scores:")
restaurants_with_score.select(
    "name", "average_rating", "num_reviews", "custom_score"
).orderBy(desc("custom_score")).show(10)
```

### Using Pandas UDFs for Performance

Pandas UDFs leverage vectorized operations for better performance.

```python
from pyspark.sql.functions import pandas_udf
import pandas as pd

# Create a Pandas UDF for calculating scores
@pandas_udf(DoubleType())
def calculate_score_pandas(ratings, reviews):
    import numpy as np
    return ratings ** 2 * np.log1p(reviews)

# Apply the Pandas UDF
restaurants_with_pandas_score = restaurants_df.withColumn(
    "pandas_score",
    calculate_score_pandas(col("average_rating"), col("num_reviews"))
)

# Display results
print("Restaurants with Pandas UDF Scores:")
restaurants_with_pandas_score.select(
    "name", "average_rating", "num_reviews", "pandas_score"
).orderBy(desc("pandas_score")).show(10)
```

## 2. Partition Operations

Managing partitions is crucial for optimizing performance in Spark.

### Understanding Partitions

```python
# Check the current number of partitions
num_partitions = restaurants_df.rdd.getNumPartitions()
print(f"Current number of partitions: {num_partitions}")

# Repartition to a specific number
restaurants_repartitioned = restaurants_df.repartition(10)
print(f"New number of partitions: {restaurants_repartitioned.rdd.getNumPartitions()}")

# Repartition by a specific column (e.g., cuisine_type)
restaurants_by_cuisine = restaurants_df.repartition("cuisine_type")
```

### Coalesce vs. Repartition

```python
# Repartition (may involve a full shuffle)
restaurants_more_partitions = restaurants_df.repartition(20)

# Coalesce (optimized for reducing partitions)
restaurants_fewer_partitions = restaurants_df.coalesce(2)

# Check partition counts
print(f"Original partitions: {restaurants_df.rdd.getNumPartitions()}")
print(f"After repartition: {restaurants_more_partitions.rdd.getNumPartitions()}")
print(f"After coalesce: {restaurants_fewer_partitions.rdd.getNumPartitions()}")
```

### Partitioning for Join Optimization

```python
# Repartition before a join to optimize performance
repartitioned_restaurants = restaurants_df.repartition(10, "cnpj")
repartitioned_orders = orders_df.repartition(10, "restaurant_key")

# Now perform the join
optimized_join = repartitioned_restaurants.join(
    repartitioned_orders,
    repartitioned_restaurants.cnpj == repartitioned_orders.restaurant_key,
    "inner"
)

print(f"Optimized join result count: {optimized_join.count()}")
```

## 3. Window Functions

Window functions allow you to perform calculations across related rows.

### Basic Window Functions

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rank, dense_rank, lag, lead

# Define a window specification
window_spec = Window.partitionBy("cuisine_type").orderBy(desc("average_rating"))

# Rank restaurants within each cuisine type
restaurants_ranked = restaurants_df.withColumn(
    "rank_in_cuisine", 
    rank().over(window_spec)
)

# Show top restaurants in each cuisine
print("Top Restaurants by Cuisine:")
top_by_cuisine = restaurants_ranked.filter(col("rank_in_cuisine") <= 3)
top_by_cuisine.select(
    "cuisine_type", "name", "average_rating", "rank_in_cuisine"
).orderBy("cuisine_type", "rank_in_cuisine").show(10)
```

### Running Totals and Moving Averages

```python
# Window for cumulative sum
revenue_window = Window.partitionBy("cuisine_type").orderBy("restaurant_id").rowsBetween(
    Window.unboundedPreceding, Window.currentRow
)

# Calculate cumulative metrics by cuisine
restaurant_revenue = restaurants_df.withColumn(
    "cumulative_restaurants", 
    count("restaurant_id").over(revenue_window)
).withColumn(
    "cuisine_percentage", 
    col("cumulative_restaurants") / count("restaurant_id").over(Window.partitionBy("cuisine_type"))
)

print("Cumulative Restaurant Metrics:")
restaurant_revenue.select(
    "cuisine_type", "restaurant_id", "cumulative_restaurants", "cuisine_percentage"
).orderBy("cuisine_type", "restaurant_id").show(10)
```

### Accessing Previous and Next Rows

```python
# Define window for lag/lead functions
ordered_window = Window.partitionBy("cuisine_type").orderBy("average_rating")

# Add columns with previous and next ratings
with_neighbors = restaurants_df.withColumn(
    "prev_rating", 
    lag("average_rating", 1).over(ordered_window)
).withColumn(
    "next_rating", 
    lead("average_rating", 1).over(ordered_window)
).withColumn(
    "rating_diff_from_prev", 
    col("average_rating") - col("prev_rating")
)

print("Restaurants with Previous/Next Ratings:")
with_neighbors.select(
    "cuisine_type", "name", "average_rating", 
    "prev_rating", "next_rating", "rating_diff_from_prev"
).orderBy("cuisine_type", "average_rating").show(10)
```

## 4. Caching and Persistence

Properly managing caching can significantly improve performance.

### Basic Caching

```python
# Cache a DataFrame in memory
restaurants_df.cache()

# Alternative syntax
restaurants_cached = restaurants_df.cache()

# Force evaluation
print(f"Cached restaurant count: {restaurants_cached.count()}")
```

### Storage Level Options

```python
from pyspark.storagelevel import StorageLevel

# Different persistence options
restaurants_df.persist(StorageLevel.MEMORY_ONLY)  # Memory only (same as cache())
drivers_df.persist(StorageLevel.MEMORY_AND_DISK)  # Memory and disk if needed
orders_df.persist(StorageLevel.DISK_ONLY)         # Disk only
ratings_df.persist(StorageLevel.MEMORY_ONLY_SER)  # Memory only, serialized
```

### When to Use Caching

```python
# Example: Multiple operations on the same DataFrame
# Without caching, each action would recompute from scratch

# First, let's join data (expensive operation)
joined_data = restaurants_df.join(
    orders_df,
    restaurants_df.cnpj == orders_df.restaurant_key,
    "inner"
)

# Cache the joined data
joined_data.cache()

# Now perform multiple operations using the cached data
cuisine_analysis = joined_data.groupBy("cuisine_type").agg(
    count("order_id").alias("order_count"),
    round(sum("total_amount"), 2).alias("total_revenue")
)

city_analysis = joined_data.groupBy("city").agg(
    count("order_id").alias("order_count"),
    round(avg("total_amount"), 2).alias("avg_order_value")
)

# Display results
print("Cuisine Analysis:")
cuisine_analysis.show(5)

print("City Analysis:")
city_analysis.show(5)

# Unpersist when done
joined_data.unpersist()
```

## 5. Practical Application: Advanced UberEats Analysis

Let's combine these advanced techniques to build a comprehensive analysis:

```python
def advanced_ubereats_analysis(restaurants_df, orders_df, drivers_df):
    """
    Comprehensive UberEats analysis using advanced techniques
    """
    from pyspark.sql.window import Window
    from pyspark.sql.functions import dense_rank, lag, sum, avg
    
    # Join data and cache for multiple operations
    joined_data = restaurants_df.join(
        orders_df,
        restaurants_df.cnpj == orders_df.restaurant_key,
        "inner"
    ).join(
        drivers_df,
        orders_df.driver_key == drivers_df.license_number,
        "left"
    ).cache()  # Cache for performance
    
    # Create a UDF for categorizing order amounts
    @udf(returnType=StringType())
    def categorize_order(amount):
        if amount is None:
            return "Unknown"
        elif amount >= 100:
            return "Large Order"
        elif amount >= 50:
            return "Medium Order"
        else:
            return "Small Order"
    
    # Apply UDF for order size categorization
    orders_with_size = joined_data.withColumn(
        "order_size_category",
        categorize_order(col("total_amount"))
    )
    
    # Window functions for restaurant rankings
    cuisine_window = Window.partitionBy("cuisine_type").orderBy(desc("average_rating"))
    city_window = Window.partitionBy("city").orderBy(desc("average_rating"))
    
    # Add rankings
    ranked_restaurants = joined_data.withColumn(
        "rank_in_cuisine", 
        dense_rank().over(cuisine_window)
    ).withColumn(
        "rank_in_city",
        dense_rank().over(city_window)
    )
    
    # Top restaurants in each cuisine and city
    top_restaurants = ranked_restaurants.filter(
        (col("rank_in_cuisine") <= 3) | (col("rank_in_city") <= 3)
    )
    
    # Order trend analysis (compare to previous orders)
    order_window = Window.partitionBy("restaurant_key").orderBy("order_date")
    
    order_trends = joined_data.withColumn(
        "prev_order_amount",
        lag("total_amount", 1).over(order_window)
    ).withColumn(
        "amount_change",
        col("total_amount") - col("prev_order_amount")
    )
    
    # Optimized aggregation with appropriate partitioning
    joined_data.repartition("cuisine_type")
    
    cuisine_stats = joined_data.groupBy("cuisine_type").agg(
        count("order_id").alias("order_count"),
        round(sum("total_amount"), 2).alias("total_revenue"),
        round(avg("total_amount"), 2).alias("avg_order_value")
    )
    
    # Unpersist when done
    joined_data.unpersist()
    
    return {
        "orders_by_size": orders_with_size,
        "top_restaurants": top_restaurants,
        "order_trends": order_trends,
        "cuisine_stats": cuisine_stats
    }

# Run the analysis
analysis_results = advanced_ubereats_analysis(restaurants_df, orders_df, drivers_df)

# Display the results
print("Orders by Size Category:")
analysis_results["orders_by_size"].select(
    "name", "cuisine_type", "total_amount", "order_size_category"
).show(10)

print("\nTop Restaurants in Each Cuisine/City:")
analysis_results["top_restaurants"].select(
    "name", "cuisine_type", "city", "average_rating", "rank_in_cuisine", "rank_in_city"
).orderBy("cuisine_type", "rank_in_cuisine").show(10)

print("\nOrder Trends (Amount Changes):")
analysis_results["order_trends"].select(
    "name", "order_date", "total_amount", "prev_order_amount", "amount_change"
).orderBy("restaurant_key", "order_date").show(10)

print("\nCuisine Statistics:")
analysis_results["cuisine_stats"].orderBy(desc("total_revenue")).show(10)
```

## Conclusion and Best Practices

In this lesson, we've covered advanced PySpark techniques:

1. **User Defined Functions (UDFs)**: Implementing custom logic when built-in functions aren't enough
2. **Partition Operations**: Managing data distribution for performance optimization
3. **Window Functions**: Performing calculations across related rows
4. **Caching and Persistence**: Optimizing performance by storing intermediate results

**Best Practices:**

- Use Pandas UDFs over regular UDFs when possible for better performance
- Choose appropriate partition counts based on your data size and available resources
- Apply window functions to avoid multiple passes over the data
- Cache strategically: only persist DataFrames used multiple times
- Use the appropriate storage level based on your memory constraints
- Always unpersist cached data when no longer needed
- Monitor execution plans to understand how Spark processes your operations

In the next lesson, we'll explore how to deliver processed data from PySpark to various output formats and systems.

## Exercise

1. Create a custom UDF that calculates a "driver efficiency score" based on their vehicle type, year, and other attributes.

2. Implement window functions to analyze trends in order values over time for each restaurant.

3. Build an advanced analysis that:
   - Ranks drivers by the number and value of orders they've delivered
   - Uses appropriate caching strategies
   - Applies custom UDFs for categorization
   - Optimizes partitioning for performance

## Resources

- [PySpark UDF Documentation](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.udf.html)
- [Pandas UDF Documentation](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.pandas_udf.html)
- [Window Functions Documentation](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Window.html)
