# PySpark Foundations - Part 2: Basic Transformation Operations

## Introduction

After ingesting data into PySpark, the next step is to transform and manipulate it to suit your analytical needs. In this lesson, we'll explore the fundamental transformation operations that form the building blocks of any PySpark application.

## Setting Up Our Environment

First, let's set up our Spark session and load the UberEats dataset from our previous lesson:

```python
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Create a Spark session
spark = SparkSession.builder \
    .appName("Basic Transformations") \
    .master("local[*]") \
    .getOrCreate()

# Load datasets
restaurants_df = spark.read.json("./storage/mysql/restaurants/01JS4W5A7YWTYRQKDA7F7N95VY.jsonl")
drivers_df = spark.read.json("./storage/postgres/drivers/01JS4W5A74BK7P4BPTJV1D3MHA.jsonl")
orders_df = spark.read.json("./storage/kafka/orders/01JS4W5A7XY65S9Z69BY51BEJ4.jsonl")

# Display the schemas for reference
print("Restaurants Schema:")
restaurants_df.printSchema()

print("\nDrivers Schema:")
drivers_df.printSchema()

print("\nOrders Schema:")
orders_df.printSchema()
```

## 1. Column Selection and Projection

Selecting specific columns is one of the most basic operations in data transformation.

### Basic Column Selection

```python
# Select specific columns
restaurant_details = restaurants_df.select("name", "cuisine_type", "city", "average_rating")

# Show the results
print("Restaurant Details:")
restaurant_details.show(5)
```

### Using Column Objects

For more flexibility, you can use the `col` function:

```python
from pyspark.sql.functions import col

# Select columns using col() function
restaurant_locations = restaurants_df.select(
    col("name"),
    col("city"),
    col("address"),
    col("country")
)

# Show the results
print("Restaurant Locations:")
restaurant_locations.show(5, truncate=False)
```

### Renaming Columns

```python
# Rename columns using alias
renamed_df = restaurants_df.select(
    col("name").alias("restaurant_name"),
    col("cuisine_type").alias("cuisine"),
    col("average_rating").alias("rating"),
    col("num_reviews").alias("review_count")
)

# Show the results
print("Renamed Columns:")
renamed_df.show(5)
```

## 2. Filtering Data

Filtering allows you to select rows that match specific conditions.

### Basic Filtering

```python
# Filter restaurants with high ratings
high_rated = restaurants_df.filter(col("average_rating") > 4.0)

print(f"Number of high-rated restaurants: {high_rated.count()}")
high_rated.select("name", "cuisine_type", "average_rating").show(5)
```

### Multiple Filter Conditions

```python
# Filter with multiple conditions
popular_italian = restaurants_df.filter(
    (col("cuisine_type") == "Italian") & 
    (col("num_reviews") > 3000)
)

print(f"Number of popular Italian restaurants: {popular_italian.count()}")
popular_italian.select("name", "city", "average_rating", "num_reviews").show(5)
```

### Using SQL-like Syntax

```python
# Filter using SQL-like expressions
french_restaurants = restaurants_df.filter("cuisine_type = 'French' AND average_rating > 3.0")

print(f"Number of good French restaurants: {french_restaurants.count()}")
french_restaurants.select("name", "city", "average_rating").show(5)
```

## 3. Logical and Comparison Operators

PySpark supports various operators for building complex conditions.

### Comparison Operators

```python
# Examples of comparison operators
# Equal: ==
# Not equal: !=
# Greater than: >
# Less than: <
# Greater than or equal: >=
# Less than or equal: <=

# Restaurants with exactly 4.2 rating
exact_rating = restaurants_df.filter(col("average_rating") == 4.2)
print(f"Restaurants with exactly 4.2 rating: {exact_rating.count()}")

# Restaurants with rating not equal to 3.0
not_average = restaurants_df.filter(col("average_rating") != 3.0)
print(f"Restaurants with rating not equal to 3.0: {not_average.count()}")
```

### Logical Operators

```python
# Examples of logical operators
# AND: &
# OR: |
# NOT: ~

# Restaurants that are either Italian or have high ratings
italian_or_high = restaurants_df.filter(
    (col("cuisine_type") == "Italian") | 
    (col("average_rating") >= 4.5)
)
print(f"Italian or high-rated restaurants: {italian_or_high.count()}")

# Restaurants that are not Indian
not_indian = restaurants_df.filter(~(col("cuisine_type") == "Indian"))
print(f"Non-Indian restaurants: {not_indian.count()}")
```

### Complex Conditions

```python
# Combining multiple conditions
target_restaurants = restaurants_df.filter(
    ((col("cuisine_type") == "Italian") | (col("cuisine_type") == "French")) &
    (col("average_rating") > 3.5) &
    (col("num_reviews") > 2000)
)

print("Target Restaurants:")
target_restaurants.select("name", "cuisine_type", "average_rating", "num_reviews").show(5)
```

## 4. Simple Column Transformations

PySpark provides functions to transform column values.

### Basic String Operations

```python
from pyspark.sql.functions import upper, lower, concat, lit

# Transform restaurant names to uppercase
uppercase_names = restaurants_df.select(
    upper(col("name")).alias("restaurant_name_upper"),
    col("name"),
    col("cuisine_type")
)

print("Uppercase Restaurant Names:")
uppercase_names.show(5, truncate=False)

# Concatenate columns
full_info = restaurants_df.select(
    col("name"),
    concat(col("city"), lit(", "), col("country")).alias("location")
)

print("Restaurant with Location:")
full_info.show(5, truncate=False)
```

### Numeric Operations

```python
from pyspark.sql.functions import round, sqrt, abs

# Round ratings to whole numbers
rounded_ratings = restaurants_df.select(
    col("name"),
    col("average_rating"),
    round(col("average_rating"), 0).alias("rounded_rating")
)

print("Rounded Ratings:")
rounded_ratings.show(5)

# Calculate a simple score from ratings and reviews
restaurant_scores = restaurants_df.select(
    col("name"),
    col("average_rating"),
    col("num_reviews"),
    round(col("average_rating") * sqrt(col("num_reviews") / 1000), 2).alias("score")
)

print("Restaurant Scores:")
restaurant_scores.orderBy(col("score").desc()).show(5)
```

### Date and Time Operations

```python
from pyspark.sql.functions import to_timestamp, date_format, current_timestamp, datediff

# Convert string timestamp to date
with_dates = restaurants_df.select(
    col("name"),
    col("dt_current_timestamp"),
    to_timestamp(col("dt_current_timestamp")).alias("timestamp")
)

print("With Timestamp:")
with_dates.show(5)

# Format dates
formatted_dates = with_dates.select(
    col("name"),
    col("timestamp"),
    date_format(col("timestamp"), "yyyy-MM-dd").alias("date_only"),
    date_format(col("timestamp"), "HH:mm:ss").alias("time_only")
)

print("Formatted Dates:")
formatted_dates.show(5)
```

## 5. Adding and Dropping Columns

You can add new columns or remove existing ones from your DataFrame.

### Adding Columns

```python
# Add a new column
with_category = restaurants_df.withColumn(
    "rating_category",
    when(col("average_rating") >= 4.5, "Excellent")
    .when(col("average_rating") >= 4.0, "Very Good")
    .when(col("average_rating") >= 3.5, "Good")
    .when(col("average_rating") >= 3.0, "Average")
    .otherwise("Poor")
)

print("With Rating Category:")
with_category.select("name", "average_rating", "rating_category").show(10)
```

### Dropping Columns

```python
# Drop columns
simplified_df = restaurants_df.drop("uuid", "dt_current_timestamp")

# Check the columns after dropping
print("Columns after dropping:")
print(simplified_df.columns)
```

## 6. Combining Operations

In real-world scenarios, you'll typically chain multiple operations together.

```python
# Let's create a useful view of our restaurant data
restaurant_analysis = restaurants_df.select(
    col("name"),
    col("cuisine_type"),
    col("city"),
    col("average_rating"),
    col("num_reviews")
).filter(
    col("num_reviews") > 1000
).withColumn(
    "rating_category",
    when(col("average_rating") >= 4.5, "Excellent")
    .when(col("average_rating") >= 4.0, "Very Good")
    .when(col("average_rating") >= 3.5, "Good")
    .when(col("average_rating") >= 3.0, "Average")
    .otherwise("Poor")
).withColumn(
    "popularity_score", 
    round(col("average_rating") * sqrt(col("num_reviews") / 1000), 2)
).orderBy(
    col("popularity_score").desc()
)

print("Restaurant Analysis:")
restaurant_analysis.show(10, truncate=False)
```

## Practical Exercise: UberEats Restaurant Report

Let's combine what we've learned to create a simple restaurant report from our UberEats data:

```python
def create_restaurant_report(restaurant_df):
    """
    Creates a summary report of restaurants with useful transformations
    """
    from pyspark.sql.functions import when, count, round, sqrt, desc
    
    # Process the data
    report_df = restaurant_df.select(
        col("name"),
        col("cuisine_type"),
        col("city"),
        col("average_rating"),
        col("num_reviews"),
        col("opening_time"),
        col("closing_time")
    ).withColumn(
        "rating_category",
        when(col("average_rating") >= 4.5, "Excellent")
        .when(col("average_rating") >= 4.0, "Very Good")
        .when(col("average_rating") >= 3.5, "Good")
        .when(col("average_rating") >= 3.0, "Average")
        .otherwise("Poor")
    ).withColumn(
        "popularity_score", 
        round(col("average_rating") * sqrt(col("num_reviews") / 1000), 2)
    ).withColumn(
        "hours_of_operation",
        concat(col("opening_time"), lit(" - "), col("closing_time"))
    ).drop("opening_time", "closing_time")
    
    # Generate summary by cuisine type
    cuisine_summary = restaurant_df.groupBy("cuisine_type").agg(
        count("*").alias("restaurant_count"),
        round(avg("average_rating"), 2).alias("avg_rating"),
        round(avg("num_reviews"), 0).alias("avg_reviews")
    ).orderBy(desc("restaurant_count"))
    
    return {
        "restaurant_details": report_df.orderBy(desc("popularity_score")),
        "cuisine_summary": cuisine_summary
    }

# Generate the report
report = create_restaurant_report(restaurants_df)

# Display the results
print("Top Restaurants by Popularity:")
report["restaurant_details"].show(10, truncate=False)

print("\nCuisine Type Summary:")
report["cuisine_summary"].show(10, truncate=False)
```

## Conclusion and Best Practices

In this lesson, we've covered the fundamental transformation operations in PySpark:

1. **Column Selection**: Selecting and renaming columns
2. **Filtering**: Using conditions to select specific rows
3. **Logical and Comparison Operators**: Building complex filter conditions
4. **Simple Transformations**: Manipulating column values
5. **Adding and Dropping Columns**: Modifying DataFrame structure
6. **Combining Operations**: Chaining multiple transformations together

**Best Practices:**

- Name your columns and transformations clearly for readability
- Chain operations in a logical order for better performance and readability
- Use column objects (via `col()`) for complex operations
- Consider using SQL expressions for simple operations when more readable
- Create reusable functions for common transformation patterns
- Use appropriate data types for better performance

In the next lesson, we'll explore advanced transformation operations to handle more complex data manipulation tasks.

## Exercise

1. Create a detailed report for drivers data that includes:
   - Basic driver information (name, city, vehicle type)
   - A categorization based on vehicle type and year
   - A score based on some combination of available metrics
   
2. Analyze orders data to find:
   - Orders with total_amount above a certain threshold
   - Group orders by their driver and calculate metrics
   - Create a combined report with both restaurants and orders

## Resources

- [PySpark SQL Functions Documentation](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html)
- [PySpark DataFrame API](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html)
