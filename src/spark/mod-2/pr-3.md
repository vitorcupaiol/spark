# Advanced Configurations for Spark-Submit

## Overview

`spark-submit` is the command-line tool used to deploy Spark applications to a cluster. Properly configuring this tool is essential for:

- Optimizing resource allocation
- Controlling application behavior
- Managing dependencies
- Ensuring successful deployment across different environments

This guide explores essential parameters and configurations needed to effectively deploy Spark applications in various scenarios.

## Prerequisites

- Basic understanding of Spark architecture
- Familiarity with basic command-line operations
- Access to a Spark installation (local or cluster)

## Basic spark-submit Syntax

The general syntax for spark-submit is:

```bash
spark-submit [options] <app jar | python file> [app arguments]
```

Example:
```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --driver-memory 4g \
  --executor-memory 2g \
  --executor-cores 2 \
  app.py
```

## Essential Parameters

### Deployment Configuration

These parameters control where and how your application runs:

| Parameter | Description | Examples |
|-----------|-------------|----------|
| `--master` | The cluster manager to connect to | `local[*]`, `spark://host:7077`, `yarn` |
| `--deploy-mode` | Whether to deploy driver on worker nodes (cluster) or locally (client) | `client`, `cluster` |
| `--name` | Application name | `--name "UberEats Data Processing"` |

Examples:

```bash
# Local execution using all cores
spark-submit --master local[*] --name "Local Processing" app.py

# Submit to a standalone Spark cluster
spark-submit --master spark://spark-master:7077 --name "Cluster Processing" app.py

# Run on YARN in cluster mode
spark-submit --master yarn --deploy-mode cluster --name "YARN Processing" app.py
```

### Resource Configuration

These parameters determine how much computing resources your application uses:

| Parameter | Description | Example |
|-----------|-------------|---------|
| `--driver-memory` | Memory for driver process | `--driver-memory 4g` |
| `--executor-memory` | Memory per executor | `--executor-memory 2g` |
| `--executor-cores` | Cores per executor | `--executor-cores 2` |
| `--total-executor-cores` | Total cores for all executors (standalone mode) | `--total-executor-cores 10` |
| `--num-executors` | Number of executors (YARN mode) | `--num-executors 5` |

Example for a data processing job:

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --driver-memory 4g \
  --executor-memory 2g \
  --executor-cores 2 \
  --num-executors 5 \
  process_data.py
```

### Performance Tuning

These parameters affect application performance:

| Parameter | Description | Example |
|-----------|-------------|---------|
| `--conf spark.default.parallelism` | Default number of partitions | `--conf spark.default.parallelism=20` |
| `--conf spark.sql.shuffle.partitions` | Number of partitions for joins/aggregations | `--conf spark.sql.shuffle.partitions=50` |
| `--conf spark.serializer` | Class for serializing objects | `--conf spark.serializer=org.apache.spark.serializer.KryoSerializer` |
| `--conf spark.memory.fraction` | Fraction of heap for execution and storage | `--conf spark.memory.fraction=0.8` |

Example with performance tuning:

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --driver-memory 4g \
  --executor-memory 2g \
  --executor-cores 2 \
  --num-executors 5 \
  --conf spark.default.parallelism=20 \
  --conf spark.sql.shuffle.partitions=50 \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  process_data.py
```

## Environment-Specific Configurations

### Development Environment

For local development and testing, focus on ease of debugging:

```bash
spark-submit \
  --master local[*] \
  --driver-memory 2g \
  --conf spark.ui.port=4040 \
  --conf spark.sql.shuffle.partitions=10 \
  --conf spark.driver.extraJavaOptions="-Dlog4j.configuration=file:log4j-dev.properties" \
  app.py
```

Key considerations:
- Use `local[*]` to run everything in a single JVM
- Lower shuffle partitions to match local resources
- Configure logging for detailed debugging
- Expose Spark UI on a known port

### Testing Environment

For test clusters, balance between development and production:

```bash
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
```

Key considerations:
- Use `client` mode for easier debugging
- Allocate moderate resources
- Specify test queue if available
- Disable dynamic allocation for predictable resource usage

### Production Environment

For production, optimize for performance and reliability:

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --driver-memory 8g \
  --executor-memory 16g \
  --executor-cores 4 \
  --num-executors 10 \
  --queue production \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.dynamicAllocation.minExecutors=5 \
  --conf spark.dynamicAllocation.maxExecutors=20 \
  --conf spark.speculation=true \
  --files hdfs:///configs/log4j-prod.properties \
  --conf spark.driver.extraJavaOptions="-Dlog4j.configuration=log4j-prod.properties" \
  app.py
```

Key considerations:
- Use `cluster` mode for better resource distribution
- Enable dynamic allocation for elasticity
- Enable speculation for handling slow tasks
- Configure production-level logging
- Allocate sufficient resources for performance

## Managing Dependencies

### JAR Dependencies

Package external Java/Scala libraries with your application:

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --jars lib/dependency1.jar,lib/dependency2.jar \
  app.py
```

### Python Dependencies

For Python applications, you have several options:

1. **Using --py-files** for Python modules:

   ```bash
   spark-submit \
   --master yarn \
   --deploy-mode cluster \
   --py-files dependencies.zip,utils.py \
   app.py
   ```

2. **Using a virtual environment** (more complex but comprehensive):

   ```bash
   # Create and package virtual environment
   python -m venv spark_env
   source spark_env/bin/activate
   pip install -r requirements.txt
   venv-pack -o spark_venv.tar.gz

   # Submit with virtual environment
   spark-submit \
   --master yarn \
   --deploy-mode cluster \
   --archives spark_venv.tar.gz#environment \
   --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./environment/bin/python \
   --conf spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON=./environment/bin/python \
   --conf spark.executorEnv.PYSPARK_PYTHON=./environment/bin/python \
   app.py
   ```

### Configuration and Data Files

Include additional files needed by your application:

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --files config.json,lookup_table.csv \
  app.py
```

## Best Practices: Using Configuration Files

Instead of specifying many parameters on the command line, use configuration files:

1. **Create `spark-defaults.conf`**:
   ```
   spark.master                 yarn
   spark.deploy-mode            cluster
   spark.driver.memory          8g
   spark.executor.memory        16g
   spark.executor.cores         4
   spark.executor.instances     10
   spark.dynamicAllocation.enabled  true
   spark.dynamicAllocation.minExecutors  5
   spark.dynamicAllocation.maxExecutors  20
   spark.serializer             org.apache.spark.serializer.KryoSerializer
   ```

2. **Create environment-specific configuration files**:
   ```
   # dev.conf
   spark.master                 local[*]
   spark.driver.memory          2g
   spark.sql.shuffle.partitions 10
   
   # prod.conf
   spark.master                 yarn
   spark.deploy-mode            cluster
   spark.driver.memory          8g
   spark.executor.memory        16g
   ```

3. **Submit using a configuration file**:
   ```bash
   spark-submit \
     --properties-file prod.conf \
     --class com.example.MainClass \
     app.jar
   ```

## Using Spark-Submit with Scripts

For enhanced flexibility, create wrapper scripts:

```bash
#!/bin/bash
# submit_app.sh

# Default values
MASTER="yarn"
DEPLOY_MODE="cluster"
DRIVER_MEM="4g"
EXECUTOR_MEM="2g"
NUM_EXECUTORS="5"
ENV="dev"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --env) ENV="$2"; shift 2 ;;
        --app) APP="$2"; shift 2 ;;
        *) shift ;;
    esac
done

# Load environment-specific settings
if [[ "$ENV" == "dev" ]]; then
    MASTER="local[*]"
    DEPLOY_MODE="client"
    DRIVER_MEM="2g"
    NUM_EXECUTORS="1"
elif [[ "$ENV" == "prod" ]]; then
    MASTER="yarn"
    DEPLOY_MODE="cluster"
    DRIVER_MEM="8g"
    EXECUTOR_MEM="16g"
    NUM_EXECUTORS="10"
fi

# Submit the application
spark-submit \
    --master $MASTER \
    --deploy-mode $DEPLOY_MODE \
    --driver-memory $DRIVER_MEM \
    --executor-memory $EXECUTOR_MEM \
    --num-executors $NUM_EXECUTORS \
    $APP
```

Usage:
```bash
./submit_app.sh --env prod --app process_data.py
```

## Practical Examples

### 1. Basic Data Processing Job

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --driver-memory 4g \
  --executor-memory 2g \
  --executor-cores 2 \
  --num-executors 5 \
  process_data.py
```

### 2. Memory-Intensive Analysis Job

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --driver-memory 8g \
  --executor-memory 16g \
  --executor-cores 4 \
  --num-executors 10 \
  --conf spark.memory.fraction=0.8 \
  --conf spark.memory.storageFraction=0.3 \
  --conf spark.sql.shuffle.partitions=200 \
  heavy_analytics.py
```

### 3. Real-Time Streaming Job

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --driver-memory 4g \
  --executor-memory 4g \
  --executor-cores 2 \
  --num-executors 10 \
  --conf spark.streaming.backpressure.enabled=true \
  --conf spark.streaming.kafka.maxRatePerPartition=10000 \
  --conf spark.cleaner.ttl=1200 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1 \
  streaming_app.py
```

## Exercises

1. **Basic Configuration**: Create a spark-submit command to run a Python application (`process_users.py`) locally using all available cores, 2GB of driver memory, and with an application name "UberEats User Processing".

2. **Resource Allocation**: Configure a spark-submit command for a YARN cluster with 5 executors, each with 2 cores and 4GB of memory, setting appropriate shuffle partitions for a dataset with approximately 1TB of data.

3. **Environment Setup**: Create three different spark-submit commands for dev, test, and prod environments, with appropriate resource allocations and deployment modes for each.

4. **Dependency Management**: Create a spark-submit command that includes external Python modules (`utils.py` and `data_processing.py`) and a configuration file (`config.json`).

5. **Configuration File**: Create a `spark-defaults.conf` file with appropriate settings for a production UberEats data processing application, and the corresponding spark-submit command that uses this configuration file.

## Summary

- `spark-submit` is the standard way to deploy Spark applications
- Essential parameters control deployment mode, resources, and performance
- Environment-specific configurations optimize for development, testing, and production
- Proper dependency management ensures all required libraries are available
- Configuration files and scripts enhance reusability and maintainability

In the next module, we'll explore monitoring and debugging Spark applications to ensure optimal performance and troubleshoot issues efficiently.
