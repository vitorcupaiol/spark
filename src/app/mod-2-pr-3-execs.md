1 - Basic Configuration: Create a spark-submit command to run a Python application (process_users.py) locally using all available cores, 2GB of driver memory, and with an application name "UberEats User Processing".

spark-submit \
  --master local[*] \
  --driver-memory 3g \
  --executor-cores 3 \
  --name "UberEats User Processing" \
    process_users.py

2 - Resource Allocation: Configure a spark-submit command for a YARN cluster with 5 executors, each with 2 cores and 4GB of memory, setting appropriate shuffle partitions for a dataset with approximately 1TB of data.

spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --driver-memory 8g \
  --executor-memory 4g \
  --executor-cores 2 \
  --num-executors 5 \
  --conf spark.sql.shuffle.partitions=200 \
  heavy_analytics.py

3 - Environment Setup: Create three different spark-submit commands for dev, test, and prod environments, with appropriate resource allocations and deployment modes for each.

# dev
spark-submit \
  --master local[*] \
  --driver-memory  2g \
  --conf spark.sql.shuffle.partitions=10 \
  app.py

# test
spark-submit \
  --master yarn \
  --deploy-mode client \
  --driver-memory 4g \
  --executor-memory 2g \
  --executor-cores 2 \
  --num-executors 2 \
  --queue test \
  --conf spark.dynamicAllocation.enabled=false \
  app.py

# prod
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --driver-memory  8g \
  --executor-memory 16g \
  --conf spark.sql.shuffle.partitions=10\
  app.py

4 - Dependency Management: Create a spark-submit command that includes external Python modules (utils.py and data_processing.py) and a configuration file (config.json).

spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --py-files utils.py,data_processing.py \
  --files config.json \
  app.py


5 - Configuration File: Create a spark-defaults.conf file with appropriate settings for a production UberEats data processing application, and the corresponding spark-submit command that uses this configuration file.

spark-submit \
  --properties-file spark-defaults.conf \
 app.py