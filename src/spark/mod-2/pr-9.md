# Pandas API on Spark Foundations - Part 1: Data Ingestion

## Introduction

The Pandas API on Spark combines the familiar Pandas syntax with Spark's distributed computing power. This lets data scientists use their existing Pandas knowledge while scaling to much larger datasets. In this lesson, we'll explore how to set up the environment and ingest data from various sources.

## Setting Up Your Environment

Unlike regular PySpark, the Pandas API on Spark requires a specific import:

```python
# Import Pandas API on Spark
import pyspark.pandas as ps
import pandas as pd

# For comparison with regular PySpark
from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder \
    .appName("Pandas API Ingestion") \
    .master("local[*]") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .getOrCreate()

# Print the Pandas API on Spark version
print(f"Pandas API on Spark version: {ps.__version__}")
```

Note the configuration `spark.sql.execution.arrow.pyspark.enabled`: this enables Apache Arrow, which optimizes data transfer between Pandas and Spark.

## Configuring Pandas API on Spark

The Pandas API on Spark has some additional settings that control its behavior:

```python
# Display more rows
ps.set_option("display.max_rows", 20)

# Set index type (distributed or default)
ps.set_option("compute.default_index_type", "distributed")

# Show Spark progress
ps.set_option("compute.ops_on_diff_frames.enabled", True)

# Set number of partitions
ps.set_option("compute.default_index_type", 10)
```

## Reading Data from Different Sources

### 1. Reading from JSON

Let's load our UberEats dataset using Pandas API on Spark:

```python
# Read JSON data
restaurants_ps = ps.read_json("./storage/mysql/restaurants/01JS4W5A7YWTYRQKDA7F7N95VY.jsonl")
drivers_ps = ps.read_json("./storage/postgres/drivers/01JS4W5A74BK7P4BPTJV1D3MHA.jsonl")
orders_ps = ps.read_json("./storage/kafka/orders/01JS4W5A7XY65S9Z69BY51BEJ4.jsonl")

# Display basic information
print(f"Restaurants: {len(restaurants_ps)} records")
print(f"Drivers: {len(drivers_ps)} records")
print(f"Orders: {len(orders_ps)} records")

# Show the first few rows
print("\nRestaurant Data Sample:")
print(restaurants_ps.head())
```

Notice the Pandas-like syntax: `len()` instead of `count()` and `head()` instead of `show()`.

### 2. Reading from CSV

```python
# If we had a CSV file in our storage directory:
# Read CSV data
# ratings_ps = ps.read_csv("./storage/ratings.csv")

# For demonstration, let's write our JSON data to CSV first
restaurants_ps.to_csv("./storage/restaurants.csv", index=False)

# Now read it back
restaurants_from_csv = ps.read_csv("./storage/restaurants.csv")

# Verify the data
print("\nRestaurants from CSV:")
print(restaurants_from_csv.head(3))
```

### 3. Reading from Parquet

```python
# First, write to Parquet
restaurants_ps.to_parquet("./storage/restaurants.parquet")

# Read from Parquet
restaurants_from_parquet = ps.read_parquet("./storage/restaurants.parquet")

# Verify
print("\nRestaurants from Parquet:")
print(restaurants_from_parquet.head(3))
```

### 4. Reading from Databases

```python
# Read from a database (example with PostgreSQL)
# connection_uri = "postgresql://username:password@localhost:5432/database"
# table_name = "restaurants"
# restaurants_from_db = ps.read_sql(f"SELECT * FROM {table_name}", connection_uri)
```

## Converting from Native Pandas

One of the main advantages of Pandas API on Spark is the ability to use existing Pandas code. Here's how to convert between them:

### 1. From Native Pandas to Pandas API on Spark

```python
# Create a native pandas DataFrame
pd_df = pd.DataFrame({
    'name': ['Restaurant A', 'Restaurant B', 'Restaurant C'],
    'cuisine': ['Italian', 'French', 'Japanese'],
    'rating': [4.5, 4.2, 4.8]
})

# Convert to Pandas API on Spark
ps_df = ps.from_pandas(pd_df)

print("\nConverted from native Pandas:")
print(ps_df)
```

### 2. From Pandas API on Spark to Native Pandas

```python
# Take a small subset for demonstration
small_restaurants = restaurants_ps.head(5)

# Convert to native pandas
pd_restaurants = small_restaurants.to_pandas()

print("\nConverted to native Pandas:")
print(type(pd_restaurants))
print(pd_restaurants.head())
```

⚠️ **Important**: Only convert to native Pandas when working with small datasets that fit in memory.

## Converting from PySpark DataFrames

You can also convert between PySpark DataFrames and Pandas API on Spark:

### 1. From PySpark DataFrame to Pandas API on Spark

```python
# Create a PySpark DataFrame
spark_df = spark.createDataFrame([
    (1, "John", 25),
    (2, "Alice", 30),
    (3, "Bob", 22)
], ["id", "name", "age"])

# Convert to Pandas API on Spark
ps_from_spark = ps.DataFrame(spark_df)

print("\nConverted from PySpark DataFrame:")
print(ps_from_spark)
```

### 2. From Pandas API on Spark to PySpark DataFrame

```python
# Convert back to PySpark
spark_from_ps = restaurants_ps.to_spark()

print("\nConverted to PySpark DataFrame:")
spark_from_ps.show(5)
```

## Understanding Key Differences from Native Pandas

While the syntax is similar, there are some important differences:

```python
# 1. Index behavior - often distributed
print("\nIndex information:")
print(restaurants_ps.index)

# 2. Execution is lazy (like Spark)
lazy_result = restaurants_ps[restaurants_ps.average_rating > 4.0]
print("\nLazy execution - nothing happens until needed:")
print(type(lazy_result))

# 3. Some operations may behave differently
# For example, sorting requires explicitly resetting index in many cases
sorted_restaurants = restaurants_ps.sort_values("average_rating", ascending=False)
print("\nSorted restaurants (note the index):")
print(sorted_restaurants.head(3))

# Reset index after sorting
sorted_restaurants_reset = sorted_restaurants.reset_index(drop=True)
print("\nSorted restaurants with reset index:")
print(sorted_restaurants_reset.head(3))
```

## Practical Exercise: UberEats Data Ingestion Pipeline

Let's combine what we've learned to create a simple ingestion pipeline:

```python
def ingest_data_with_pandas_api(json_paths, output_dir):
    """
    Ingest data from JSON files using Pandas API on Spark
    
    Args:
        json_paths: Dict with dataset name as key and path as value
        output_dir: Directory for processed output
    
    Returns:
        Dict of ingested DataFrames
    """
    import os
    
    # Results dictionary
    dataframes = {}
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Process each dataset
    for name, path in json_paths.items():
        print(f"Processing {name} from {path}")
        
        # Read the data
        df = ps.read_json(path)
        
        # Basic information
        print(f"  - Shape: {df.shape}")
        print(f"  - Columns: {df.columns.tolist()}")
        
        # Check for missing values
        missing_counts = df.isna().sum()
        missing_cols = missing_counts[missing_counts > 0]
        if len(missing_cols) > 0:
            print(f"  - Missing values in columns: {missing_cols.to_dict()}")
        
        # Store in dictionary
        dataframes[name] = df
        
        # Save as parquet for efficiency
        parquet_path = os.path.join(output_dir, f"{name}.parquet")
        df.to_parquet(parquet_path)
        print(f"  - Saved to {parquet_path}")
        
        # Also save a small sample as CSV for easy inspection
        sample_path = os.path.join(output_dir, f"{name}_sample.csv")
        df.head(10).to_csv(sample_path, index=False)
        print(f"  - Sample saved to {sample_path}")
    
    return dataframes

# Define input paths
json_paths = {
    "restaurants": "./storage/mysql/restaurants/01JS4W5A7YWTYRQKDA7F7N95VY.jsonl",
    "drivers": "./storage/postgres/drivers/01JS4W5A74BK7P4BPTJV1D3MHA.jsonl",
    "orders": "./storage/kafka/orders/01JS4W5A7XY65S9Z69BY51BEJ4.jsonl"
}

# Run the ingestion
output_dir = "./storage/processed_pandas_api"
ingested_data = ingest_data_with_pandas_api(json_paths, output_dir)

# Display a summary
for name, df in ingested_data.items():
    print(f"\n{name.title()} Summary:")
    print(df.describe().round(2))
```

## Conclusion and Best Practices

In this lesson, we've covered the fundamentals of data ingestion with Pandas API on Spark:

1. **Environment Setup**: Setting up and configuring Pandas API on Spark
2. **Reading Data**: Loading data from various sources
3. **Conversion**: Moving between native Pandas, Pandas API on Spark, and PySpark
4. **Key Differences**: Understanding how Pandas API on Spark differs from native Pandas
5. **Practical Pipeline**: Building a data ingestion workflow

**Best Practices:**

- Use `ps.read_parquet()` for best performance when possible
- Remember that execution is lazy (like Spark) – operations are only executed when needed
- Keep index considerations in mind (distributed vs. default)
- Only convert to native Pandas for small datasets that fit in memory
- Use Apache Arrow for efficient conversion (enabled by default in recent versions)

In the next lesson, we'll explore basic transformation operations using Pandas API on Spark.

## Exercise

1. Load the ratings data from `./storage/mysql/ratings/01JS4W5A7YWTYRQKDA7F7N95VZ.jsonl`
2. Convert it to different formats (CSV, Parquet) and compare the file sizes
3. Create a conversion workflow that:
   - Reads data from JSON
   - Performs basic exploration (shape, missing values)
   - Saves to optimized Parquet format
   - Creates a small CSV sample for quick inspection

## Resources

- [Pandas API on Spark Documentation](https://spark.apache.org/docs/latest/api/python/reference/pyspark.pandas/index.html)
- [Apache Arrow](https://arrow.apache.org/)
