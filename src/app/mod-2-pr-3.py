"""
# local development mode
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master local \
  --name "Local Development" \
  /opt/bitnami/spark/jobs/app/mod-2-pr-3.py

# resource allocation
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --driver-memory 1g \
  --executor-memory 2g \
  --executor-cores 2 \
  --total-executor-cores 4 \
  /opt/bitnami/spark/jobs/app/mod-2-pr-3.py

# performance tuning
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.sql.shuffle.partitions=10 \
  --conf spark.default.parallelism=20 \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  /opt/bitnami/spark/jobs/app/mod-2-pr-3.py

# with python dependencies
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --py-files /opt/bitnami/spark/jobs/app/utils.py,/opt/bitnami/spark/jobs/app/helpers.py \
  --files /opt/bitnami/spark/jobs/app/config.json \
  /opt/bitnami/spark/jobs/app/mod-2-pr-3.py

# with external python packages
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 \
  /opt/bitnami/spark/jobs/app/mod-2-pr-3.py

# with arguments
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/jobs/app/mod-2-pr-3.py \
  --input /data/input.csv \
  --output /data/results \
  --date 2025-04-22

# full production env
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --name "UberEats Data Processing" \
  --driver-memory 4g \
  --executor-memory 8g \
  --executor-cores 4 \
  --total-executor-cores 16 \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.dynamicAllocation.minExecutors=2 \
  --conf spark.dynamicAllocation.maxExecutors=8 \
  --conf spark.memory.fraction=0.8 \
  --conf spark.sql.shuffle.partitions=100 \
  --py-files /opt/bitnami/spark/jobs/app/utils.py \
  --files /opt/bitnami/spark/jobs/app/config.json \
  /opt/bitnami/spark/jobs/app/mod-2-pr-3.py \
  --env production
"""

from pyspark.sql import SparkSession

# TODO 1 = create spark session
spark = SparkSession.builder \
    .getOrCreate()

# TODO 2 = create spark context
sc = spark.sparkContext

for item in sorted(spark.sparkContext.getConf().getAll()):
    print(f"  {item[0]}: {item[1]}")
# TODO 3 = the spark code here

# TODO 4 = stop the spark context
spark.stop()
