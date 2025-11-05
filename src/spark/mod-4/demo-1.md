# Demo 1: Delta Lake Foundation & Setup

## Overview

This comprehensive demo introduces Delta Lake fundamentals within the Apache Spark ecosystem. You'll learn how to configure Spark with Delta Lake, create tables using different approaches, perform basic read/write operations, and convert existing Parquet data to Delta format using real UberEats data.

## Learning Objectives

By the end of this demo, you will be able to:

- ✅ Configure Spark Session with Delta Lake support
- ✅ Create Delta Lake tables using DataFrame API and Spark SQL
- ✅ Perform read and write operations with different modes
- ✅ Convert existing Parquet tables to Delta Lake format
- ✅ Understand Delta Lake metadata and transaction logs

## Prerequisites

- Docker environment with Spark cluster running
- Access to MinIO S3-compatible storage
- UberEats sample data loaded in MinIO

## File Structure

```
src/spark/mod-4/
├── demo-1.py
└── demo-1.md
```

## Demo Structure

### 1. Spark Session Configuration with Delta Lake

The foundation of working with Delta Lake is proper Spark session configuration:

```python
spark = SparkSession.builder \
    .appName("Demo-1-DeltaLake-Foundation") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    # ... S3/MinIO configurations
    .getOrCreate()
```

**Key Delta Lake Configurations:**
- `spark.sql.extensions`: Enables Delta Lake SQL extensions
- `spark.sql.catalog.spark_catalog`: Sets Delta Lake as the default catalog

### 2. Creating Delta Tables with DataFrame API

Learn to create Delta tables programmatically using the DataFrame API:

```python
# Load source data
restaurants_df = spark.read.json("s3a://owshq-shadow-traffic-uber-eats/mysql/restaurants/")

# Write as Delta table
restaurants_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("s3a://owshq-uber-eats-lakehouse/bronze/restaurants_delta")
```

**Benefits:**
- Programmatic control over table creation
- Easy integration with existing DataFrame workflows
- Support for schema evolution options

### 3. Creating Delta Tables with Spark SQL

Use familiar SQL DDL syntax to create Delta tables:

```sql
CREATE TABLE IF NOT EXISTS drivers_bronze
USING DELTA
LOCATION 's3a://owshq-uber-eats-lakehouse/bronze/drivers_delta'
AS SELECT * FROM drivers_source
```

**Advantages:**
- Familiar SQL syntax for database practitioners
- Declarative approach with clear table structure
- Built-in support for table metadata

### 4. Read and Write Operations

Master different read/write patterns essential for data lakehouse operations:

#### Write Modes:
- **Overwrite**: Replaces entire table contents
- **Append**: Adds new data to existing table
- **ErrorIfExists**: Fails if table already exists
- **Ignore**: Skips write if table exists

#### Read Operations:
- **Predicate Pushdown**: Filters applied at storage level
- **Column Pruning**: Only reads required columns
- **Schema Evolution**: Handles schema changes gracefully

### 5. Parquet to Delta Conversion

Migrate existing Parquet data lakes to Delta Lake format using the built-in Delta Lake utility:

```sql
-- Method 1: Convert in-place (modifies existing Parquet table)
CONVERT TO DELTA parquet.`s3a://path/to/parquet/table`

-- Method 2: Convert with partitioning
CONVERT TO DELTA parquet.`s3a://path/to/parquet/table` 
PARTITIONED BY (year INT, month INT)

-- Method 3: Convert and create new table at different location
CREATE OR REPLACE TABLE new_table
USING DELTA
LOCATION 's3a://new/delta/location'
AS SELECT * FROM parquet.`s3a://path/to/parquet/table`
```

**Key Benefits of CONVERT TO DELTA:**
- **In-place conversion**: No data copying required
- **Preserves partitioning**: Maintains existing partition structure
- **Atomic operation**: All-or-nothing conversion
- **Metadata preservation**: Keeps existing schema and statistics

### 6. Understanding Delta Lake Metadata

Explore the Delta transaction log and table metadata:

```python
# Access table history
delta_table = DeltaTable.forPath(spark, table_path)
delta_table.history().show()

# View table details
spark.sql(f"DESCRIBE DETAIL delta.`{table_path}`").show()
```

## Running the Demo

### Step 1: Navigate to Demo Directory
```bash
cd /path/to/frm-spark-databricks-mec/src/spark/mod-4/
```

### Step 2: Execute the Demo
```bash
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/bitnami/spark/jobs/spark/mod-4/demo-1.py
```

### Step 3: Monitor Execution
Watch the console output for:
- ✅ Successful Spark session creation
- 📊 Data loading and processing statistics
- 💾 Delta table creation confirmations
- 📋 Table metadata information

## Expected Output

The demo will create several Delta Lake tables in your MinIO lakehouse:

```
owshq-uber-eats-lakehouse/
├── bronze/
│   ├── restaurants_delta/
│   ├── drivers_delta/
│   ├── orders_delta/
│   └── users_delta/
└── temp/
    └── users_parquet/
```

## Key Concepts Learned

### Delta Lake Architecture
- **Transaction Log**: `_delta_log/` directory containing JSON files
- **Data Files**: Parquet files with actual data
- **Metadata**: Schema, statistics, and versioning information

### ACID Properties
- **Atomicity**: All-or-nothing operations
- **Consistency**: Data integrity maintained
- **Isolation**: Concurrent operations don't interfere
- **Durability**: Committed changes are permanent

### Performance Features
- **Predicate Pushdown**: Filters applied at file level
- **Z-Ordering**: Data clustering for better performance
- **Liquid Clustering**: Automatic data organization
- **File Compaction**: Optimizes small files

## Troubleshooting

### Common Issues and Solutions

1. **Missing Delta Lake Dependencies**
   ```
   Error: ClassNotFoundException: io.delta.sql.DeltaSparkSessionExtension
   ```
   **Solution**: Ensure `--packages io.delta:delta-core_2.12:2.4.0` is included

2. **S3 Connection Issues**
   ```
   Error: Unable to execute HTTP request: Connect to endpoint
   ```
   **Solution**: Verify MinIO endpoint and credentials in configuration

3. **Permission Errors**
   ```
   Error: Access Denied
   ```
   **Solution**: Check S3 access key and secret key configuration

4. **Schema Mismatch**
   ```
   Error: A schema mismatch detected
   ```
   **Solution**: Use `.option("overwriteSchema", "true")` for schema evolution

## Exercises

### Exercise 1: Basic Delta Table Creation
**Objective**: Create a Delta table from the MongoDB support tickets data.

**Tasks**:
1. Read support tickets data from `s3a://owshq-shadow-traffic-uber-eats/mongodb/support/`
2. Create a Delta table at `s3a://owshq-uber-eats-lakehouse/bronze/support_delta`
3. Query the table to find the distribution of ticket categories
4. Display the table schema using `printSchema()`

**Expected Output**:
```
+----------------+-----+
|        category|count|
+----------------+-----+
|   Late Delivery|   15|
|  Payment Issue|   12|
|    Wrong Item|    8|
|   Missing Item|    5|
|          Other|   10|
+----------------+-----+
```

### Exercise 2: Write Mode Comparison
**Objective**: Understand different write modes behavior.

**Tasks**:
1. Create a small subset of the orders data (10 records)
2. Write it as a Delta table using `overwrite` mode
3. Try to write the same data again using `errorIfExists` mode
4. Write additional data using `append` mode
5. Compare record counts after each operation

**Questions to Answer**:
- What happens when you use `errorIfExists` on an existing table?
- How does the record count change with each write mode?
- Which mode would you use for daily data ingestion?

### Exercise 3: Parquet Migration with CONVERT TO DELTA
**Objective**: Practice the proper way to convert Parquet data to Delta format.

**Tasks**:
1. Read the GPS tracking data from `s3a://owshq-shadow-traffic-uber-eats/kafka/gps/`
2. Save it as Parquet format in a temporary location
3. Use `CONVERT TO DELTA` to convert the Parquet table to Delta Lake format
4. Compare the file structure between Parquet and Delta locations
5. Verify data integrity by comparing record counts
6. Try both in-place conversion and conversion to new location

**Investigation Points**:
- What additional files/directories does Delta Lake create after conversion?
- How does the `_delta_log` directory structure look?
- What metadata information is available in Delta that wasn't in Parquet?
- Does `CONVERT TO DELTA` preserve the original Parquet files?

**Advanced Challenge**:
Try converting a partitioned Parquet table:
```sql
-- First create a partitioned Parquet table
CREATE OR REPLACE TEMPORARY VIEW gps_data AS 
SELECT *, date_format(timestamp, 'yyyy-MM-dd') as date_partition
FROM parquet.`s3a://path/to/gps/data`

-- Save as partitioned Parquet
INSERT OVERWRITE DIRECTORY 's3a://temp/gps_partitioned'
USING PARQUET
OPTIONS (path 's3a://temp/gps_partitioned')
PARTITION BY (date_partition)
SELECT * FROM gps_data

-- Convert partitioned Parquet to Delta
CONVERT TO DELTA parquet.`s3a://temp/gps_partitioned`
PARTITIONED BY (date_partition STRING)
```

### Exercise 4: Table Metadata Exploration
**Objective**: Explore Delta Lake's metadata capabilities.

**Tasks**:
1. Use any Delta table created in previous exercises
2. Run `DESCRIBE DETAIL` on the table
3. Examine the table history using `.history()`
4. List the physical files in the table directory
5. Explore the transaction log JSON files

**Analysis Questions**:
- How many files make up your Delta table?
- What information is stored in the transaction log?
- How does Delta Lake track schema evolution?
- What statistics are automatically collected?

### Exercise 5: Advanced Configuration
**Objective**: Optimize Spark configuration for Delta Lake workloads.

**Tasks**:
1. Create a new Spark session with optimized configurations for Delta Lake
2. Add configurations for better S3 performance
3. Enable automatic optimize for small files
4. Test the configuration with a larger dataset (all restaurants data)
5. Compare performance with default configuration

**Configuration Areas to Explore**:
- Connection pooling for S3
- Multipart upload settings
- Delta Lake automatic optimization
- Shuffle partitions for your data size

## Next Steps

After completing this foundation demo, you'll be ready for:

- **Demo 2**: ACID Transactions & Concurrency - Learn about Delta Lake's transaction capabilities
- **Demo 3**: Time Travel & History Management - Explore versioning and data recovery
- **Demo 4**: Schema Management & Evolution - Handle changing data structures
- **Demo 5**: DML Operations & Data Management - Master data manipulation operations

## Additional Resources

- [Delta Lake Documentation](https://docs.delta.io/)
- [Apache Spark SQL Guide](https://spark.apache.org/docs/latest/sql-programming-guide.html)
- [S3A Configuration Reference](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html)

---

🎯 **Remember**: Delta Lake provides ACID transactions, scalable metadata handling, and time travel to data lakes. These features make it an ideal choice for building reliable data lakehouses that can handle both batch and streaming workloads efficiently.
