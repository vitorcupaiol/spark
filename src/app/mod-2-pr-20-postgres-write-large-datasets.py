"""
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/jobs/app/mod-2-pr-20-postgres-write-large-datasets.py

# EFFICIENT LARGE DATASET WRITING FROM SPARK TO POSTGRESQL
# ======================================================

This script demonstrates the most efficient method for writing large datasets from Apache Spark
to PostgreSQL databases. It showcases a technique using staging tables and PostgreSQL's COPY
command which significantly outperforms standard JDBC writes.

## KEY CONCEPTS DEMONSTRATED

1. OPTIMAL PARTITIONING:
   - Dynamically calculates the ideal number of partitions based on dataset size
   - Balances parallelism and overhead to maximize throughput
   - Distributes workload evenly across Spark executors

2. CSV INTERMEDIARY + COPY COMMAND:
   - Uses CSV as an efficient intermediary format
   - Leverages PostgreSQL's COPY command which is 5-10x faster than INSERT statements
   - Minimizes database transaction overhead during bulk loading

3. STAGING TABLE APPROACH:
   - Writes data to a temporary staging table
   - Performs atomic table swap for zero-downtime updates
   - Ensures data consistency and availability throughout the process

4. PERFORMANCE BENCHMARKING:
   - Compares three different approaches: standard JDBC, batch writing, and staging+COPY
   - Measures execution times to quantify performance improvements
   - Demonstrates 4-20x speedup for large datasets

## WHEN TO USE THIS APPROACH

This technique is ideal for:
- Full table refreshes where entire tables need to be replaced
- Large datasets with hundreds of thousands to millions of rows
- Production ETL processes with strict performance requirements
- Time-sensitive operations where loading windows are limited

## EXECUTION FLOW

1. Creates a large sample dataset of driver records (100,000 rows)
2. Ensures target table exists with correct schema
3. Benchmarks three different writing methods:
   a. Standard JDBC direct write
   b. Batch-based writing with smaller chunks
   c. Staging table with COPY command (the optimized approach)
4. Compares performance metrics to demonstrate efficiency gains

## LIMITATIONS & CONSIDERATIONS

- Requires appropriate PostgreSQL privileges (CREATE TABLE, DROP TABLE, COPY)
- Uses CSV as intermediate format which may not be ideal for complex data types
- Designed for full table refreshes rather than incremental updates
- Would need additional error handling for production use

## EXPECTED PERFORMANCE GAINS

With a 100,000 row dataset, typical improvements are:
- Standard JDBC: ~60-120 seconds
- Batch Writing: ~30-60 seconds
- Staging Table + COPY: ~5-15 seconds

This represents a 4-20x performance improvement, depending on environment and data characteristics.
"""

from pyspark.sql import SparkSession
import psycopg2
from psycopg2.extras import execute_batch
import time
import os
import tempfile
import csv

# TODO Initialize Spark session
spark = SparkSession.builder \
    .getOrCreate()

# TODO PostgreSQL connection parameters
pg_host = "159.203.159.29"
pg_port = "5432"
pg_database = "postgres"
pg_user = "postgres"
pg_password = "6e8e5979-25c5-44e2-ad76-7a4e8ee68c6f"
jdbc_url = f"jdbc:postgresql://{pg_host}:{pg_port}/{pg_database}"

# TODO Connection properties
connection_properties = {
    "user": pg_user,
    "password": pg_password,
    "driver": "org.postgresql.Driver",
    "host": pg_host,
    "port": pg_port,
    "database": pg_database
}

# TODO Target table name
target_table = "drivers_data"

# TODO Create sample data for demonstration
drivers_data = []
for i in range(1, 100001):
    drivers_data.append((
        f"LIC-{i:06d}",
        f"({i % 100:02d}) 9{i % 10000:04d}-{i % 9999:04d}",
        f"2023-01-{i % 31 + 1}",
        f"driver{i}@example.com",
        4.0 + (i % 10) / 10,
        f"Driver {i}",
        1000.00 + (i % 5000),
        "active" if i % 5 != 0 else "inactive",
        f"2022-{i % 12 + 1}-{i % 28 + 1}",
        f"DRV-{i:06d}",
        i % 1000,
        ["Car", "Motorcycle", "Bicycle"][i % 3]
    ))

# TODO Create DataFrame with the schema matching our drivers table
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType
from pyspark.sql import Row

drivers_schema = StructType([
    StructField("license_plate", StringType(), True),
    StructField("phone_number", StringType(), True),
    StructField("last_login", StringType(), True),
    StructField("email", StringType(), True),
    StructField("average_rating", FloatType(), True),
    StructField("name", StringType(), True),
    StructField("total_earnings", FloatType(), True),
    StructField("status", StringType(), True),
    StructField("registration_date", StringType(), True),
    StructField("driver_id", StringType(), False),
    StructField("total_deliveries", IntegerType(), True),
    StructField("vehicle_type", StringType(), True)
])

# TODO Convert to Row objects for DataFrame creation
rows = [Row(**dict(zip(drivers_schema.names, driver))) for driver in drivers_data]
large_drivers_df = spark.createDataFrame(rows, schema=drivers_schema)

print(f"Created sample DataFrame with {large_drivers_df.count()} rows")
large_drivers_df.show(5)


def get_connection(conn_params):
    """Create a connection to PostgreSQL"""
    return psycopg2.connect(
        host=conn_params["host"],
        port=conn_params["port"],
        database=conn_params["database"],
        user=conn_params["user"],
        password=conn_params["password"]
    )


def ensure_table_exists(conn_params, table_name):
    """Ensure the target table exists with the correct schema"""
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        license_plate VARCHAR(20),
        phone_number VARCHAR(20),
        last_login DATE,
        email VARCHAR(100),
        average_rating FLOAT,
        name VARCHAR(100),
        total_earnings FLOAT,
        status VARCHAR(20),
        registration_date DATE,
        driver_id VARCHAR(20) PRIMARY KEY,
        total_deliveries INTEGER,
        vehicle_type VARCHAR(20)
    )
    """

    conn = get_connection(conn_params)
    try:
        with conn:
            with conn.cursor() as cursor:
                cursor.execute(create_table_sql)
                print(f"Ensured table {table_name} exists")
    finally:
        conn.close()


def write_with_staging_and_copy(df, final_table_name, conn_params):
    """
    Ultra-efficient writing using staging table and COPY command

    Args:
        df: DataFrame to write
        final_table_name: Name of the final table
        conn_params: Connection parameters
    """
    # TODO Calculate optimal partition count
    total_rows = df.count()
    estimated_row_size_mb = 0.001
    total_size_mb = total_rows * estimated_row_size_mb
    optimal_partition_count = max(4, min(200, int(total_size_mb / 500)))

    print(f"Calculated optimal partition count: {optimal_partition_count}")

    # TODO Repartition the DataFrame for optimal processing
    df = df.repartition(optimal_partition_count)

    # TODO Create a unique staging table name using timestamp
    staging_table = f"{final_table_name}_staging_{int(time.time())}"

    # TODO SQL to create the staging table with the same structure
    create_staging_sql = f"CREATE TABLE {staging_table} (LIKE {final_table_name})"

    # TODO Define the function to process each partition
    def write_partition_with_copy(iterator):
        """Process a single partition of data"""
        # TODO Create a temporary CSV file for this partition
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as temp_file:
            writer = csv.writer(temp_file)
            # TODO Write rows to CSV
            row_count = 0
            for row in iterator:
                # TODO Extract values in column order
                values = [row[col] for col in df.schema.names]
                writer.writerow(values)
                row_count += 1

            temp_file_path = temp_file.name

        print(f"Partition CSV file created with {row_count} rows: {temp_file_path}")

        # TODO Connect and use COPY
        conn = get_connection(conn_params)
        try:
            with conn:
                with conn.cursor() as cursor:
                    # TODO Check if staging table exists
                    cursor.execute(f"SELECT to_regclass('{staging_table}')")
                    table_exists = cursor.fetchone()[0] is not None

                    # TODO Create staging table if it doesn't exist yet
                    if not table_exists:
                        cursor.execute(create_staging_sql)
                        print(f"Created staging table: {staging_table}")

                    # TODO Use COPY command for fast loading
                    with open(temp_file_path, 'r') as file:
                        cursor.copy_expert(
                            f"COPY {staging_table} FROM STDIN WITH CSV",
                            file
                        )

                    print(f"Copied {row_count} rows to staging table")
        except Exception as e:
            print(f"Error in partition processing: {str(e)}")
            raise
        finally:
            conn.close()
            # TODO Clean up temp file
            os.unlink(temp_file_path)
            print(f"Temporary file removed: {temp_file_path}")

    # TODO Process all partitions in parallel
    print(f"Starting parallel processing of {optimal_partition_count} partitions...")
    start_time = time.time()
    df.foreachPartition(write_partition_with_copy)
    partition_time = time.time() - start_time
    print(f"All partitions processed in {partition_time:.2f} seconds")

    # TODO Swap tables atomically
    start_time = time.time()
    conn = get_connection(conn_params)
    try:
        with conn:
            with conn.cursor() as cursor:
                # TODO Begin transaction
                cursor.execute("BEGIN")

                # TODO Rename tables to swap them (atomic operation)
                cursor.execute(f"ALTER TABLE IF EXISTS {final_table_name} RENAME TO {final_table_name}_old")
                cursor.execute(f"ALTER TABLE {staging_table} RENAME TO {final_table_name}")
                cursor.execute(f"DROP TABLE IF EXISTS {final_table_name}_old")

                # TODO Commit transaction
                cursor.execute("COMMIT")

                print(f"Tables swapped successfully")
    except Exception as e:
        print(f"Error during table swap: {str(e)}")
        raise
    finally:
        conn.close()

    swap_time = time.time() - start_time
    print(f"Table swap completed in {swap_time:.2f} seconds")

    return partition_time + swap_time


def benchmark_writing_methods():
    """Compare different writing methods for performance"""
    print("\n===== BENCHMARK: WRITING METHODS =====\n")

    # TODO Ensure the table exists
    ensure_table_exists(connection_properties, target_table)

    # TODO Method 1: Standard JDBC write (for comparison)
    print("\n----- Method 1: Standard JDBC write -----")
    start_time = time.time()
    try:
        large_drivers_df.write.jdbc(
            url=jdbc_url,
            table=target_table,
            mode="overwrite",
            properties=connection_properties
        )
        std_time = time.time() - start_time
        print(f"Standard JDBC write completed in {std_time:.2f} seconds")
    except Exception as e:
        print(f"Error in standard JDBC write: {str(e)}")
        std_time = float('inf')

    # TODO Method 2: Batch writing (simplified for demo)
    print("\n----- Method 2: Batch writing -----")
    batch_size = 5000
    start_time = time.time()
    try:
        # TODO Collect data (for demonstration only - not recommended for very large data)
        rows = large_drivers_df.limit(50000).collect()
        total_rows = len(rows)

        for i in range(0, total_rows, batch_size):
            end_idx = min(i + batch_size, total_rows)
            batch_rows = rows[i:end_idx]

            # TODO Create DataFrame from batch
            batch_df = spark.createDataFrame(batch_rows, large_drivers_df.schema)

            # TODO Write batch
            mode = "overwrite" if i == 0 else "append"
            batch_df.write.jdbc(
                url=jdbc_url,
                table=target_table,
                mode=mode,
                properties=connection_properties
            )

            print(f"Batch {i//batch_size + 1} written ({end_idx}/{total_rows} rows)")

        batch_time = time.time() - start_time
        print(f"Batch writing completed in {batch_time:.2f} seconds")
    except Exception as e:
        print(f"Error in batch writing: {str(e)}")
        batch_time = float('inf')

    # TODO Method 3: Staging table + COPY approach
    print("\n----- Method 3: Staging Table + COPY approach -----")
    try:
        staging_time = write_with_staging_and_copy(
            large_drivers_df,
            target_table,
            connection_properties
        )
        print(f"Staging table + COPY completed in {staging_time:.2f} seconds")
    except Exception as e:
        print(f"Error in staging table approach: {str(e)}")
        staging_time = float('inf')

    # TODO Compare results
    print("\n===== BENCHMARK RESULTS =====")
    print(f"Standard JDBC:       {std_time:.2f} seconds")
    print(f"Batch Writing:       {batch_time:.2f} seconds")
    print(f"Staging Table+COPY:  {staging_time:.2f} seconds")

    # TODO Calculate improvement percentages
    if std_time != float('inf'):
        std_improvement = (std_time - staging_time) / std_time * 100
        print(f"Improvement over standard JDBC: {std_improvement:.2f}%")

    if batch_time != float('inf'):
        batch_improvement = (batch_time - staging_time) / batch_time * 100
        print(f"Improvement over batch writing: {batch_improvement:.2f}%")

# TODO Run the benchmark
benchmark_writing_methods()

# TODO Stop Spark session
spark.stop()
