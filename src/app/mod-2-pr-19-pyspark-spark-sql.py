"""
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/jobs/app/mod-2-pr-19-pyspark-spark-sql.py
"""

import time
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .getOrCreate()

# TODO configs
spark.sparkContext.setLogLevel("ERROR")

# TODO read data into dataframes
restaurants_df = spark.read.json("./storage/mysql/restaurants/01JS4W5A7YWTYRQKDA7F7N95VY.jsonl")
drivers_df = spark.read.json("./storage/postgres/drivers/01JS4W5A74BK7P4BPTJV1D3MHA.jsonl")
orders_df = spark.read.json("./storage/kafka/orders/01JS4W5A7XY65S9Z69BY51BEJ4.jsonl")
ratings_df = spark.read.json("./storage/mysql/ratings/01JS4W5A7YWTYRQKDA7F7N95VZ.jsonl")

# TODO create temporary views (sql tables) for each dataframe
restaurants_df.createOrReplaceTempView("restaurants")
drivers_df.createOrReplaceTempView("drivers")
orders_df.createOrReplaceTempView("orders")
ratings_df.createOrReplaceTempView("ratings")


# TODO create benchmarking utility
def benchmark_operation(name, operation_func, iterations=3, warmup=1):
    """Run an operation multiple times and return the average execution time."""

    global result

    print(f"\nBenchmarking: {name}")

    print(f"Running {warmup} warmup iterations...")
    for i in range(warmup):
        operation_func()
        print(f"  Warmup {i + 1}/{warmup} completed")

    print(f"Running {iterations} timed iterations...")
    times = []
    for i in range(iterations):
        start_time = time.time()
        result = operation_func()
        end_time = time.time()
        execution_time = end_time - start_time
        times.append(execution_time)
        print(f"  Run {i + 1}/{iterations}: {execution_time:.4f} seconds")

    avg_time = sum(times) / len(times)
    print(f"Average execution time: {avg_time:.4f} seconds")
    return avg_time, result

# ============================================== #
# TODO 1. simple filtering & aggregation
# ============================================== #


# TODO: PySpark Engine
def pyspark_operation():
    from pyspark.sql.functions import avg, count, round

    result = restaurants_df.filter("average_rating > 3.0") \
        .groupBy("cuisine_type") \
        .agg(
        round(avg("average_rating"), 2).alias("avg_rating"),
        count("*").alias("count")
    ) \
        .orderBy("cuisine_type")

    return result

pyspark_time, pyspark_result = benchmark_operation("PySpark API", lambda: pyspark_operation().collect())
print("\nPySpark Result:")
pyspark_operation().show(5)

# TODO: Spark SQL Engine
print("\n--- Spark SQL Implementation ---")


def sql_operation():
    result = spark.sql("""
        SELECT cuisine_type,
        ROUND(AVG(average_rating), 2) AS avg_rating,
        COUNT(*) AS count
        FROM restaurants
        WHERE average_rating > 3.0
        GROUP BY cuisine_type
        ORDER BY cuisine_type
    """)

    return result

sql_time, sql_result = benchmark_operation("Spark SQL",lambda: sql_operation().collect())
print("\nSpark SQL Result:")
sql_operation().show(5)

print("\n--- Performance Comparison ---")
fastest = min(pyspark_time, sql_time)
print(f"PySpark: {pyspark_time:.4f}s ({pyspark_time/fastest:.2f}x)")
print(f"Spark SQL: {sql_time:.4f}s ({sql_time/fastest:.2f}x)")

# ============================================== #
# TODO 2. join operations
# ============================================== #


# TODO: PySpark Engine
def pyspark_join_operation():
    from pyspark.sql.functions import col, avg, count, round

    result = restaurants_df.join(
        ratings_df,
        restaurants_df.cnpj == ratings_df.restaurant_identifier,
        "inner"
    ).groupBy(
        "cuisine_type", "city"
    ).agg(
        round(avg("rating"), 2).alias("avg_rating"),
        count("rating_id").alias("rating_count"),
        round(avg("average_rating"), 2).alias("avg_restaurant_rating")
    ).orderBy("cuisine_type", "city")

    return result

pyspark_join_time, pyspark_join_result = benchmark_operation("PySpark Join", lambda: pyspark_join_operation().collect())
print("\nPySpark Join Result:")


# TODO: Spark SQL Engine
print("\n--- Spark SQL Implementation ---")


def sql_join_operation():
    result = spark.sql("""
       SELECT r.cuisine_type,
              r.city,
              ROUND(AVG(rt.rating), 2)        AS avg_rating,
              COUNT(rt.rating_id)             AS rating_count,
              ROUND(AVG(r.average_rating), 2) AS avg_restaurant_rating
       FROM restaurants r
                INNER JOIN ratings rt ON r.cnpj = rt.restaurant_identifier
       GROUP BY r.cuisine_type, r.city
       ORDER BY r.cuisine_type, r.city
       """)

    return result

sql_join_time, sql_join_result = benchmark_operation("Spark SQL Join", lambda: sql_join_operation().collect())
print("\nSpark SQL Join Result:")
sql_join_operation().show(5)

print("\n--- Performance Comparison ---")
fastest_join = min(pyspark_join_time, sql_join_time)
print(f"PySpark: {pyspark_join_time:.4f}s ({pyspark_join_time / fastest_join:.2f}x)")
print(f"Spark SQL: {sql_join_time:.4f}s ({sql_join_time / fastest_join:.2f}x)")

# ============================================== #
# TODO 3. window functions
# ============================================== #


# TODO: PySpark Engine
def pyspark_window_operation():
    from pyspark.sql.functions import rank, col, desc
    from pyspark.sql.window import Window

    window_spec = Window.partitionBy("city").orderBy(desc("average_rating"))

    result = restaurants_df.withColumn(
        "rank",
        rank().over(window_spec)
    ).filter(
        col("rank") <= 3
    ).select(
        "city", "name", "cuisine_type", "average_rating", "rank"
    ).orderBy("city", "rank")

    return result

pyspark_window_time, pyspark_window_result = benchmark_operation("PySpark Window Functions", lambda: pyspark_window_operation().collect())
print("\nPySpark Window Functions Result:")
pyspark_window_operation().show(5)


# TODO: Spark SQL Engine
def sql_window_operation():
    result = spark.sql("""
       WITH RankedRestaurants AS (
           SELECT city,
                  name,
                  cuisine_type,
                  average_rating,
                 RANK() OVER (PARTITION BY city ORDER BY average_rating DESC) as rank
          FROM restaurants
       )
       SELECT city, name, cuisine_type, average_rating, rank
       FROM RankedRestaurants
       WHERE rank <= 3
       ORDER BY city, rank
       """)

    return result

sql_window_time, sql_window_result = benchmark_operation("Spark SQL Window Functions", lambda: sql_window_operation().collect())
print("\nSpark SQL Window Functions Result:")
sql_window_operation().show(5)

print("\n--- Performance Comparison ---")
fastest_window = min(pyspark_window_time, sql_window_time)
print(f"PySpark: {pyspark_window_time:.4f}s ({pyspark_window_time/fastest_window:.2f}x)")
print(f"Spark SQL: {sql_window_time:.4f}s ({sql_window_time/fastest_window:.2f}x)")


# ============================================== #
# TODO 4. execution plans
# ============================================== #

# TODO PySpark DataFrame API
pyspark_query = restaurants_df.filter("average_rating > 3.0") \
    .groupBy("cuisine_type") \
    .agg({"average_rating": "avg"}) \
    .orderBy("cuisine_type")

# TODO Spark SQL
sql_query = spark.sql("""
    SELECT 
        cuisine_type,
        AVG(average_rating) as avg_rating
    FROM restaurants
    WHERE average_rating > 3.0
    GROUP BY cuisine_type
    ORDER BY cuisine_type
""")

print("\n--- PySpark DataFrame Logical Plan ---")
pyspark_query.explain(mode="extended")

print("\n--- Spark SQL Logical Plan ---")
sql_query.explain(mode="extended")

spark.stop()