"""
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/jobs/app/mod-2-pr-8-data-delivery.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, sqrt, to_timestamp, year, month

spark = SparkSession.builder \
    .getOrCreate()

restaurants_df = spark.read.json("./storage/mysql/restaurants/01JS4W5A7YWTYRQKDA7F7N95VY.jsonl")
drivers_df = spark.read.json("./storage/postgres/drivers/01JS4W5A74BK7P4BPTJV1D3MHA.jsonl")
orders_df = spark.read.json("./storage/kafka/orders/01JS4W5A7XY65S9Z69BY51BEJ4.jsonl")

restaurant_analytics = restaurants_df.select(
    "name", "cuisine_type", "city", "country", "average_rating", "num_reviews"
).withColumn(
    "popularity_score",
    round(col("average_rating") * sqrt(col("num_reviews") / 1000), 2)
)

print("Restaurant Analytics:")
restaurant_analytics.show(5)

# TODO 1. write file formats
output_path_formats = "./storage/output"

restaurant_analytics.write \
    .option("header", "true") \
    .csv(f"{output_path_formats}/csv")

restaurant_analytics.write \
    .json(f"{output_path_formats}/json")

restaurant_analytics.write \
    .parquet(f"{output_path_formats}/parquet")

restaurant_analytics.write \
    .orc(f"{output_path_formats}/orc")

# TODO 2. write modes
output_path_modes = "./storage/output"

restaurant_analytics.write.mode("errorifexists").parquet(f"{output_path_modes}/errorifexists")
restaurant_analytics.write.mode("overwrite").parquet(f"{output_path_modes}/overwrite")
restaurant_analytics.write.mode("append").parquet(f"{output_path_modes}/append")

# TODO = file-system-level, not data-content-level
restaurant_analytics.write.mode("ignore").parquet(f"{output_path_modes}/ignore")

# TODO 3. compression options
output_path_compressed = "./storage/output"

restaurant_analytics.write.option("compression", "gzip").parquet(f"{output_path_compressed}/gzip")
restaurant_analytics.write.option("compression", "snappy").parquet(f"{output_path_compressed}/snappy")
restaurant_analytics.write.option("compression", "zstd").parquet(f"{output_path_compressed}/zstd")
restaurant_analytics.write.option("compression", "none").parquet(f"{output_path_compressed}/none")

# TODO 4. partitioning data
output_path_partitioned = "./storage/output"

restaurant_analytics.write.partitionBy("cuisine_type").parquet(f"{output_path_partitioned}/by_cuisine")
restaurant_analytics.write.partitionBy("country", "city").parquet(f"{output_path_partitioned}/by_location")

# TODO 5. controlling file sizes
output_path_file_size = "./storage/output"

restaurant_analytics.write.parquet(f"{output_path_file_size}/default")
restaurant_analytics.repartition(2).write.parquet(f"{output_path_file_size}/repartitioned")
restaurant_analytics.coalesce(1).write.parquet(f"{output_path_file_size}/coalesced")
restaurant_analytics.repartition(1).write.option("maxRecordsPerFile", 100).parquet(f"{output_path_file_size}/records")

# TODO 6. time-based partitioning
output_path_date = "./storage/output"

orders_with_time = orders_df.withColumn(
    "order_timestamp",
    to_timestamp("order_date")
).withColumn(
    "year", year("order_timestamp")
).withColumn(
    "month", month("order_timestamp")
)

orders_with_time.write \
    .partitionBy("year", "month") \
    .parquet(f"{output_path_date}/orders_by_time")

spark.stop()
