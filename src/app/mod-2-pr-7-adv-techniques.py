"""
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/jobs/app/mod-2-pr-7-adv-techniques.py
"""

import pandas as pd
import numpy as np

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, pandas_udf
from pyspark.sql.types import StringType, DoubleType, IntegerType
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rank, dense_rank, desc

spark = SparkSession.builder \
    .getOrCreate()

restaurants_df = spark.read.json("./storage/mysql/restaurants/01JS4W5A7YWTYRQKDA7F7N95VY.jsonl")
drivers_df = spark.read.json("./storage/postgres/drivers/01JS4W5A74BK7P4BPTJV1D3MHA.jsonl")
orders_df = spark.read.json("./storage/kafka/orders/01JS4W5A7XY65S9Z69BY51BEJ4.jsonl")


# TODO 1. udf's = simple python functions
@udf(returnType=StringType())
def rating_category(rating):
    """Categorize restaurants based on their rating"""
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
    else:
        return "Average or Below"


restaurants_with_category = restaurants_df.withColumn(
    "rating_category",
    rating_category(col("average_rating"))
)

restaurants_with_category.select(
    "name", "average_rating", "rating_category"
).show(10)


# TODO 2. udf's = complex logic
@udf(returnType=DoubleType())
def restaurant_score(rating, reviews):
    """
    Calculate a custom score based on rating and number of reviews
    Formula: rating^2 * log(reviews + 1)
    This gives higher weight to rating while considering review count
    """
    import math
    if rating is None or reviews is None:
        return None

    return float(rating ** 2 * math.log(reviews + 1))


scored_restaurants = restaurants_df.withColumn(
    "custom_score",
    restaurant_score(col("average_rating"), col("num_reviews"))
)

scored_restaurants.select(
    "name", "cuisine_type", "average_rating", "num_reviews", "custom_score"
).orderBy(col("custom_score").desc()).show(10)


# TODO 3. pandas udf's
@pandas_udf(DoubleType())
def pandas_restaurant_score(ratings, reviews):
    """Same scoring logic but implemented as a Pandas UDF for better performance"""
    return ratings**2 * np.log1p(reviews)


pandas_scored_restaurants = restaurants_df.withColumn(
    "pandas_score",
    pandas_restaurant_score(col("average_rating"), col("num_reviews"))
)

pandas_scored_restaurants.select(
    "name", "average_rating", "num_reviews", "pandas_score"
).orderBy(col("pandas_score").desc()).show(10)

# TODO 4. partition management
current_partitions = restaurants_df.rdd.getNumPartitions()
print(f"Current number of partitions in restaurants_df: {current_partitions}")

restaurants_more = restaurants_df.repartition(10)
print(f"After repartition to 10: {restaurants_more.rdd.getNumPartitions()} partitions")

restaurants_fewer = restaurants_df.coalesce(2)
print(f"After coalesce to 2: {restaurants_fewer.rdd.getNumPartitions()} partitions")

# TODO 5. window functions

window_spec = Window.partitionBy("cuisine_type").orderBy(desc("average_rating"))
ranked_restaurants = restaurants_df.withColumn(
    "rank_within_cuisine",
    rank().over(window_spec)
)

top_by_cuisine = ranked_restaurants.filter(col("rank_within_cuisine") <= 3)
top_by_cuisine.select(
    "cuisine_type", "name", "average_rating", "rank_within_cuisine"
).orderBy("cuisine_type", "rank_within_cuisine").show(15)

restaurant_rankings = restaurants_df.select(
    "name",
    "cuisine_type",
    "average_rating"
).withColumn(
    "row_number", row_number().over(window_spec)
).withColumn(
    "rank", rank().over(window_spec)
).withColumn(
    "dense_rank", dense_rank().over(window_spec)
)

restaurant_rankings.filter(col("cuisine_type") == "Italian").orderBy("row_number").show(10)

spark.stop()
