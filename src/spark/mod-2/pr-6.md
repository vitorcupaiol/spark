# PySpark Foundations - Part 3: Advanced Transformation Operations

## Introduction

Building on our knowledge of basic transformations, we now explore more advanced operations that allow us to implement complex business logic by transforming and combining data from different sources.

## Setting Up Our Environment

Let's set up our Spark session and load our UberEats datasets:

```python
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import col, count, avg, sum, max, min, round, desc

# Create a Spark session
spark = SparkSession.builder \
    .appName("Advanced Transformations") \
    .master("local[*]") \
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

## 1. Aggregations and Grouping

Aggregations allow you to summarize data across groups.

### Basic Aggregations

```python
# Count restaurants by cuisine type
cuisine_count = restaurants_df.groupBy("cuisine_type") \
    .count() \
    .orderBy(desc("count"))

print("Restaurant Count by Cuisine Type:")
cuisine_count.show(5)

# Multiple aggregations
cuisine_stats = restaurants_df.groupBy("cuisine_type") \
    .agg(
        count("*").alias("restaurant_count"),
        round(avg("average_rating"), 2).alias("avg_rating"),
        round(avg("num_reviews"), 0).alias("avg_reviews"),
        max("average_rating").alias("max_rating"),
        min("average_rating").alias("min_rating")
    ) \
    .orderBy(desc("restaurant_count"))

print("Cuisine Type Statistics:")
cuisine_stats.show(5)
```

### Grouping by Multiple Columns

```python
# Group by multiple columns
location_cuisine = restaurants_df.groupBy("country", "city", "cuisine_type") \
    .agg(
        count("*").alias("restaurant_count"),
        round(avg("average_rating"), 2).alias("avg_rating")
    ) \
    .orderBy(desc("restaurant_count"))

print("Restaurants by Location and Cuisine:")
location_cuisine.show(5)
```

### Having Clause Equivalent

```python
# Filter groups (similar to SQL HAVING clause)
popular_cuisine_locations = restaurants_df.groupBy("city", "cuisine_type") \
    .agg(count("*").alias("restaurant_count")) \
    .filter(col("restaurant_count") > 1) \
    .orderBy(desc("restaurant_count"))

print("Popular Cuisine Locations:")
popular_cuisine_locations.show(5)
```

## 2. Joins and Operations Between DataFrames

Joins allow you to combine data from different sources.

### Inner Join

```python
# Join orders with restaurants
orders_with_restaurants = orders_df.join(
    restaurants_df,
    orders_df.restaurant_key == restaurants_df.cnpj,
    "inner"
)

print("Orders with Restaurant Details:")
orders_with_restaurants.select(
    "order_id", 
    "name", 
    "cuisine_type", 
    "total_amount"
).show(5)
```

### Left, Right, and Full Outer Joins

```python
# Left join (keep all records from the left side)
all_restaurants_with_orders = restaurants_df.join(
    orders_df,
    restaurants_df.cnpj == orders_df.restaurant_key,
    "left"
)

print("All Restaurants With Orders (including those without orders):")
all_restaurants_with_orders.select(
    "name", 
    "cuisine_type", 
    "order_id"
).orderBy(col("order_id").isNull(), "name").show(5)

# Right join example
all_orders_with_restaurants = restaurants_df.join(
    orders_df,
    restaurants_df.cnpj == orders_df.restaurant_key,
    "right"
)

# Full outer join example
complete_orders_restaurants = restaurants_df.join(
    orders_df,
    restaurants_df.cnpj == orders_df.restaurant_key,
    "full"
)
```

### Multiple Joins

```python
# Join across multiple tables
complete_order_details = orders_df.join(
    restaurants_df,
    orders_df.restaurant_key == restaurants_df.cnpj,
    "inner"
).join(
    drivers_df,
    orders_df.driver_key == drivers_df.license_number,
    "inner"
)

print("Complete Order Details:")
complete_order_details.select(
    "order_id",
    "name",
    "cuisine_type",
    F.concat("first_name", F.lit(" "), "last_name").alias("driver_name"),
    "vehicle_type",
    "total_amount"
).show(5)
```

### Anti-Joins and Semi-Joins

```python
# Anti-join: restaurants without orders
restaurants_without_orders = restaurants_df.join(
    orders_df,
    restaurants_df.cnpj == orders_df.restaurant_key,
    "left_anti"
)

print(f"Restaurants without orders: {restaurants_without_orders.count()}")

# Semi-join: restaurants with at least one order
restaurants_with_orders = restaurants_df.join(
    orders_df,
    restaurants_df.cnpj == orders_df.restaurant_key,
    "left_semi"
)

print(f"Restaurants with orders: {restaurants_with_orders.count()}")
```

## 3. Complex Column Functions

PySpark provides various functions to perform complex operations on columns.

### String Functions

```python
from pyspark.sql.functions import upper, lower, concat, lit, split, regexp_extract

# Extract city from address using regex
city_extraction = restaurants_df.select(
    "name",
    "address",
    regexp_extract("address", r"([A-Za-z ]+) - [A-Z]{2}", 1).alias("extracted_city")
)

print("City Extraction:")
city_extraction.show(5, truncate=False)

# Split address into components
address_parts = restaurants_df.select(
    "name",
    "address",
    split("address", "\n").getItem(0).alias("street"),
    split("address", "\n").getItem(1).alias("city_state"),
    split("address", "\n").getItem(2).alias("postal_code")
)

print("Address Components:")
address_parts.show(5, truncate=False)
```

### Mathematical Functions

```python
from pyspark.sql.functions import sqrt, pow, round, log

# Calculate popularity metrics
restaurant_metrics = restaurants_df.select(
    "name",
    "average_rating",
    "num_reviews",
    round(col("average_rating") * sqrt(col("num_reviews") / 1000), 2).alias("popularity_score"),
    round(pow(col("average_rating"), 2) * log(col("num_reviews") + 1), 2).alias("weighted_score")
)

print("Restaurant Metrics:")
restaurant_metrics.orderBy(desc("weighted_score")).show(5)
```

### Date and Time Functions

```python
from pyspark.sql.functions import to_timestamp, date_format, year, month, dayofweek, hour

# Convert timestamp string to timestamp
time_df = restaurants_df.select(
    "name",
    "dt_current_timestamp",
    to_timestamp("dt_current_timestamp").alias("timestamp")
)

# Extract date components
date_parts = time_df.select(
    "name",
    "timestamp",
    year("timestamp").alias("year"),
    month("timestamp").alias("month"),
    dayofweek("timestamp").alias("day_of_week"),
    hour("timestamp").alias("hour")
)

print("Date Components:")
date_parts.show(5)
```

### Conditional Expressions

```python
from pyspark.sql.functions import when, expr

# Categorize restaurants by rating and popularity
categorized_restaurants = restaurants_df.select(
    "name",
    "cuisine_type",
    "average_rating",
    "num_reviews",
    # Complex condition for rating category
    when(col("average_rating") >= 4.5, "Exceptional")
    .when(col("average_rating") >= 4.0, "Excellent")
    .when(col("average_rating") >= 3.5, "Very Good")
    .when(col("average_rating") >= 3.0, "Good")
    .when(col("average_rating") >= 2.0, "Average")
    .otherwise("Poor").alias("rating_category"),
    
    # Conditional expression for popularity
    when(col("num_reviews") > 5000, "Extremely Popular")
    .when(col("num_reviews") > 3000, "Very Popular")
    .when(col("num_reviews") > 1000, "Popular")
    .otherwise("Standard").alias("popularity_level")
)

print("Categorized Restaurants:")
categorized_restaurants.show(5)
```

## 4. Handling Nested Data

PySpark provides powerful functions to work with nested data structures like arrays and structs.

### Working with Arrays

```python
from pyspark.sql.functions import array, explode, array_contains, size

# Create an array from multiple columns
drivers_with_arrays = drivers_df.select(
    F.concat("first_name", F.lit(" "), "last_name").alias("driver_name"),
    "vehicle_type",
    array("vehicle_make", "vehicle_model", "vehicle_type").alias("vehicle_info")
)

print("Drivers with Vehicle Info Array:")
drivers_with_arrays.show(5, truncate=False)

# Explode array into separate rows
exploded_info = drivers_with_arrays.select(
    "driver_name",
    explode("vehicle_info").alias("vehicle_detail")
)

print("Exploded Vehicle Info:")
exploded_info.show(5)
```

### Working with Structs

```python
from pyspark.sql.functions import struct, col

# Create a struct to group related fields
restaurants_struct = restaurants_df.select(
    "name",
    struct(
        "cuisine_type", 
        "average_rating", 
        "num_reviews"
    ).alias("restaurant_metrics"),
    struct(
        "city",
        "country",
        "address"
    ).alias("location")
)

print("Restaurants with Structs:")
restaurants_struct.show(5, truncate=True)

# Accessing struct fields
metrics_access = restaurants_struct.select(
    "name",
    "restaurant_metrics.cuisine_type",
    "restaurant_metrics.average_rating",
    "location.city"
)

print("Accessing Struct Fields:")
metrics_access.show(5)
```

### Handling JSON and Complex Nested Structures

```python
from pyspark.sql.functions import from_json, to_json, schema_of_json

# Let's work with orders data
orders_sample = orders_df.limit(1).collect()[0].asDict()
sample_json = spark.sparkContext.parallelize([orders_sample]).toDF()

# Convert to JSON string
orders_json = orders_df.select(to_json(struct("*")).alias("json_data"))

# Parse JSON data
parsed_orders = orders_json.select(
    from_json(
        "json_data", 
        schema_of_json(to_json(struct("*")))
    ).alias("parsed_data")
)

# Access nested fields from parsed JSON
nested_fields = parsed_orders.select(
    "parsed_data.order_id",
    "parsed_data.total_amount",
    "parsed_data.user_key"
)

print("Nested Fields from JSON:")
nested_fields.show(5)
```

## 5. Practical Application: UberEats Order Analysis

Let's combine all these advanced transformations to create a comprehensive order analysis:

```python
def analyze_orders(orders_df, restaurants_df, drivers_df):
    """
    Perform a comprehensive analysis of UberEats orders
    """
    # Join all datasets
    complete_orders = orders_df.join(
        restaurants_df,
        orders_df.restaurant_key == restaurants_df.cnpj,
        "inner"
    ).join(
        drivers_df,
        orders_df.driver_key == drivers_df.license_number,
        "left"  # Some orders might not have drivers yet
    )
    
    # Order summary by cuisine type
    cuisine_summary = complete_orders.groupBy("cuisine_type") \
        .agg(
            count("order_id").alias("order_count"),
            round(avg("total_amount"), 2).alias("avg_order_value"),
            round(sum("total_amount"), 2).alias("total_revenue"),
            count(F.expr("distinct restaurant_id")).alias("restaurant_count")
        ) \
        .orderBy(desc("total_revenue"))
    
    # Driver performance analysis
    driver_performance = complete_orders \
        .filter(col("driver_id").isNotNull()) \
        .groupBy(
            F.concat("first_name", F.lit(" "), "last_name").alias("driver_name"),
            "vehicle_type"
        ) \
        .agg(
            count("order_id").alias("delivery_count"),
            round(avg("total_amount"), 2).alias("avg_order_value"),
            round(sum("total_amount"), 2).alias("total_revenue")
        ) \
        .orderBy(desc("delivery_count"))
    
    # Restaurant performance
    restaurant_performance = complete_orders.groupBy(
        "name", "cuisine_type", "city"
    ).agg(
        count("order_id").alias("order_count"),
        round(sum("total_amount"), 2).alias("total_revenue"),
        round(avg("total_amount"), 2).alias("avg_order_value")
    ).orderBy(desc("total_revenue"))
    
    # Restaurant rating vs. revenue analysis
    rating_vs_revenue = complete_orders.groupBy(
        "name", "average_rating"
    ).agg(
        round(sum("total_amount"), 2).alias("total_revenue"),
        count("order_id").alias("order_count")
    ).orderBy(desc("total_revenue"))
    
    return {
        "cuisine_summary": cuisine_summary,
        "driver_performance": driver_performance,
        "restaurant_performance": restaurant_performance,
        "rating_vs_revenue": rating_vs_revenue
    }

# Run the analysis
analysis_results = analyze_orders(orders_df, restaurants_df, drivers_df)

# Display the results
print("Cuisine Type Summary:")
analysis_results["cuisine_summary"].show(5)

print("\nTop Performing Drivers:")
analysis_results["driver_performance"].show(5)

print("\nTop Performing Restaurants:")
analysis_results["restaurant_performance"].show(5)

print("\nRating vs. Revenue:")
analysis_results["rating_vs_revenue"].show(5)
```

## Conclusion and Best Practices

In this lesson, we've explored advanced transformation operations in PySpark:

1. **Aggregations and Grouping**: Summarizing data across groups
2. **Joins**: Combining data from different sources
3. **Complex Column Functions**: Manipulating data with advanced functions
4. **Nested Data Handling**: Working with arrays, structs, and complex nested structures
5. **Comprehensive Analysis**: Applying multiple transformations for business insights

**Best Practices:**

- Use appropriate join types to avoid data loss or duplication
- Optimize your join conditions to improve performance
- Chain transformations in a logical order
- Create reusable functions for complex transformation patterns
- Consider readability when using complex operations
- Test your transformations with small samples before applying to large datasets

In the next lesson, we'll explore even more advanced techniques in PySpark.

## Exercise

1. Analyze the relationship between driver vehicle types and order amounts:
   - Are certain vehicle types associated with higher-value orders?
   - Is there a correlation between vehicle age and order count?

2. Create a comprehensive restaurant performance dashboard that includes:
   - Rating vs. Revenue analysis
   - Location-based performance metrics
   - Cuisine popularity by city
   - Top and bottom performers by custom metrics

3. Implement a customer segmentation analysis using the orders data:
   - Group by user_key
   - Calculate metrics like total spend, average order value, order frequency
   - Create customer segments based on these metrics

## Resources

- [PySpark SQL Functions Documentation](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html)
- [PySpark Join Types](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.join.html)
