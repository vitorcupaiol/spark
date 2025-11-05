"""
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/jobs/app/mod-2-pr-4-data-ingestion.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder \
    .getOrCreate()

get_rest_json_path = "./storage/mysql/restaurants/*.jsonl"

rest_sch_json = StructType([
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

df_rest = spark.read \
    .schema(rest_sch_json) \
    .json(get_rest_json_path)

print(df_rest.count())

spark.stop()
