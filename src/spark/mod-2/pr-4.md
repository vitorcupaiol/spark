# PySpark Foundations - Part 1: Data Ingestion

## Introduction

Data ingestion is the first step in any data processing workflow. With PySpark, you can efficiently read data from various sources and formats. This lesson introduces the fundamental operations for loading, parsing, structuring, and validating data using PySpark.

## Setting Up Your Environment

First, let's create a SparkSession, the entry point for PySpark applications:

```python
from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder \
    .appName("Data Ingestion") \
    .master("local[*]") \
    .getOrCreate()

# Display information about the Spark session
print(f"Spark version: {spark.version}")
print(f"App name: {spark.conf.get('spark.app.name')}")
```

## Reading Data from Various Sources

PySpark supports multiple data sources out of the box. Let's explore the most common ones using our UberEats dataset.

### 1. Reading JSON Data

JSON is a widely used format for semi-structured data.

```python
# Read JSON data
restaurants_df = spark.read.json("./storage/mysql/restaurants/01JS4W5A7YWTYRQKDA7F7N95VY.jsonl")

# Display basic information
print(f"Number of records: {restaurants_df.count()}")
print("Schema:")
restaurants_df.printSchema()

# Show a sample of the data
restaurants_df.show(5, truncate=False)
```

Key options for JSON reading:
- `multiLine`: Set to `True` for multi-line JSON files
- `primitivesAsString`: Convert primitives to string
- `allowUnquotedFieldNames`: Allow fields without quotes

### 2. Reading CSV Data

CSV is one of the most common formats for tabular data.

```python
# Read CSV data (assuming we have a CSV file)
# If you don't have a CSV, uncomment the line below to generate one from JSON
# restaurants_df.write.csv("./storage/restaurants.csv", header=True)

drivers_csv = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("./storage/drivers.csv")

drivers_csv.show(5)
```

Important CSV options:
- `header`: Set to `true` if the file has a header
- `inferSchema`: Automatically detect column types
- `sep`: Delimiter character (default is comma)
- `quote`: Quote character (default is double-quote)
- `escape`: Escape character
- `nullValue`: String to interpret as null

### 3. Reading Parquet Data

Parquet is a columnar storage format optimized for analytics.

```python
# Convert JSON to Parquet for demonstration
restaurants_df.write.parquet("./storage/restaurants.parquet")

# Read Parquet data
parquet_df = spark.read.parquet("./storage/restaurants.parquet")
parquet_df.show(5)
```

Benefits of Parquet:
- Compressed columnar format
- Preserves schema
- Supports predicate pushdown
- Excellent for analytical workloads

### 4. Reading from Databases (JDBC)

PySpark can connect directly to relational databases:

```python
# Reading from a database (example with PostgreSQL)
# Note: This requires the appropriate JDBC driver
# jdbc_df = spark.read \
#     .format("jdbc") \
#     .option("url", "jdbc:postgresql://localhost:5432/dbname") \
#     .option("dbtable", "schema.tablename") \
#     .option("user", "username") \
#     .option("password", "password") \
#     .load()
```

## Specifying and Handling Schemas

While PySpark can infer schemas, explicitly defining them improves performance and ensures correctness.

### 1. Creating a Schema Definition

```python
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Define schema for restaurants data
restaurant_schema = StructType([
    StructField("country", StringType(), True),
    StructField("city", StringType(), True),
    StructField("restaurant_id", IntegerType(), True),
    StructField("phone_number", StringType(), True),
    StructField("cnpj", StringType(), True),
    StructField("average_rating", DoubleType(), True),
    StructField("name", StringType(), True),
    StructField("uuid", StringType(), True),
    StructField("address", StringType(), True),
    StructField("opening_time", StringType(), True),
    StructField("cuisine_type", StringType(), True),
    StructField("closing_time", StringType(), True),
    StructField("num_reviews", IntegerType(), True),
    StructField("dt_current_timestamp", StringType(), True)
])

# Read with explicit schema
restaurants_with_schema = spark.read \
    .schema(restaurant_schema) \
    .json("./storage/mysql/restaurants/01JS4W5A7YWTYRQKDA7F7N95VY.jsonl")

restaurants_with_schema.printSchema()
```

### 2. Schema Validation

```python
# Basic schema validation approach
def validate_schema(df, expected_schema):
    """Validate if DataFrame schema matches expected schema"""
    actual_fields = set(df.columns)
    expected_fields = set([field.name for field in expected_schema.fields])
    
    # Check missing fields
    missing_fields = expected_fields - actual_fields
    
    # Check extra fields
    extra_fields = actual_fields - expected_fields
    
    # Check data types (for common fields)
    type_mismatches = []
    for field in expected_schema.fields:
        if field.name in actual_fields:
            actual_type = df.schema[field.name].dataType
            expected_type = field.dataType
            if str(actual_type) != str(expected_type):
                type_mismatches.append(
                    f"Field '{field.name}' has type '{actual_type}', expected '{expected_type}'"
                )
    
    # Print validation results
    print("Schema Validation Results:")
    print(f"- Missing fields: {missing_fields if missing_fields else 'None'}")
    print(f"- Unexpected fields: {extra_fields if extra_fields else 'None'}")
    print(f"- Type mismatches: {len(type_mismatches)}")
    for mismatch in type_mismatches:
        print(f"  - {mismatch}")
    
    return len(missing_fields) == 0 and len(type_mismatches) == 0

# Validate our DataFrame
validate_schema(restaurants_with_schema, restaurant_schema)
```

## Data Format Conversions

PySpark makes it easy to convert between different data formats.

```python
# Define target path
base_path = "./storage/converted"

# Convert JSON to various formats
# 1. To CSV
restaurants_df.write.option("header", "true") \
    .csv(f"{base_path}/restaurants.csv")

# 2. To Parquet
restaurants_df.write.parquet(f"{base_path}/restaurants.parquet")

# 3. To ORC (another columnar format)
restaurants_df.write.orc(f"{base_path}/restaurants.orc")

# 4. To Avro (requires avro package)
# restaurants_df.write.format("avro").save(f"{base_path}/restaurants.avro")

# 5. To Delta (requires delta package)
# restaurants_df.write.format("delta").save(f"{base_path}/restaurants.delta")

print("Conversion complete. Files saved to:")
for fmt in ["csv", "parquet", "orc"]:
    print(f"- {base_path}/restaurants.{fmt}")
```

## Handling Different File Options

### Reading Multiple Files

```python
# Reading multiple files at once
# Assuming we have similar JSON files in a directory
all_restaurants = spark.read.json("./storage/mysql/restaurants/*.jsonl")
print(f"Total records: {all_restaurants.count()}")
```

### Handling Compression

```python
# Writing with compression
restaurants_df.write \
    .option("compression", "gzip") \
    .parquet(f"{base_path}/restaurants_compressed.parquet")
```

### Reading Partitioned Data

```python
# Write partitioned data
restaurants_df.write \
    .partitionBy("cuisine_type") \
    .parquet(f"{base_path}/restaurants_partitioned")

# Read partitioned data
partitioned_df = spark.read.parquet(f"{base_path}/restaurants_partitioned")
partitioned_df.explain()  # Shows partition pruning
```

## Common Data Quality Issues and Solutions

### Handling Missing Values

```python
from pyspark.sql.functions import col, count, when, isnan

# Check for null or empty values in each column
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

# Analyze restaurants data
analyze_null_values(restaurants_df)

# Handle missing values (example)
cleaned_df = restaurants_df.na.fill({
    "average_rating": 0.0,
    "num_reviews": 0
})
```

### Handling Data Type Conversions

```python
from pyspark.sql.functions import to_timestamp

# Convert string timestamp to proper timestamp
with_timestamp = restaurants_df.withColumn(
    "timestamp", 
    to_timestamp("dt_current_timestamp", "yyyy-MM-dd HH:mm:ss.SSS")
)

# Show the result
with_timestamp.select("dt_current_timestamp", "timestamp").show(5, truncate=False)
```

## Practical Exercise: Building a Complete Ingestion Pipeline

Let's combine what we've learned to build a simple ingestion pipeline for the UberEats data:

```python
def ingest_data(spark, source_path, target_path, validate=True):
    """
    A simple data ingestion pipeline that:
    1. Reads data from source
    2. Validates the schema
    3. Performs basic cleaning
    4. Saves to a destination format
    """
    print(f"Processing data from: {source_path}")
    
    # Determine file type from extension
    file_type = source_path.split(".")[-1]
    
    # Read the source data
    if file_type == "json" or file_type == "jsonl":
        df = spark.read.json(source_path)
    elif file_type == "csv":
        df = spark.read.option("header", "true").csv(source_path)
    elif file_type == "parquet":
        df = spark.read.parquet(source_path)
    else:
        raise ValueError(f"Unsupported file type: {file_type}")
    
    print(f"Loaded {df.count()} records")
    
    # Basic data quality checks
    null_counts = analyze_null_values(df)
    
    # Basic cleaning
    cleaned_df = df.na.drop()  # Drop rows with null values
    print(f"After cleaning: {cleaned_df.count()} records")
    
    # Save to target (as Parquet)
    cleaned_df.write.mode("overwrite").parquet(target_path)
    print(f"Data saved to: {target_path}")
    
    return cleaned_df

# Test our pipeline
ingest_data(
    spark, 
    "./storage/mysql/restaurants/01JS4W5A7YWTYRQKDA7F7N95VY.jsonl",
    "./storage/processed/restaurants"
)
```

## Conclusion and Best Practices

In this lesson, we've covered the fundamentals of data ingestion with PySpark:

1. **Reading from various sources**: JSON, CSV, Parquet, and databases
2. **Schema management**: Defining and validating schemas
3. **Format conversion**: Converting between different data formats
4. **Data quality**: Handling missing values and data type conversions
5. **Building a simple ingestion pipeline**

**Best Practices for Data Ingestion:**

- Always define schemas explicitly for production code
- Choose the right file format for your use case (Parquet for analytics)
- Implement proper data validation and quality checks
- Document your data pipeline
- Consider partitioning for large datasets
- Use appropriate compression methods

In the next lesson, we'll explore basic transformation operations to process and clean our ingested data.

## Exercise

1. Load the drivers data from `./storage/postgres/drivers/01JS4W5A74BK7P4BPTJV1D3MHA.jsonl`
2. Define an explicit schema for the drivers data
3. Perform basic data quality checks
4. Convert it to Parquet format with appropriate partitioning
5. Create a function to validate the date_birth field and identify potential issues

## Resources

- [Spark SQL Documentation](https://spark.apache.org/docs/latest/sql-programming-guide.html)
- [PySpark Read/Write Documentation](https://spark.apache.org/docs/latest/sql-data-sources.html)