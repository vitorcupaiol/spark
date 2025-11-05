# Quick Start Guide: Apache Spark with Docker

This guide provides step-by-step instructions to start the Apache Spark environment using Docker for the Databricks & Apache Spark course.

## Prerequisites

- Docker installed (version 20.10+)
- Git installed
- Basic terminal/command line knowledge

## Step 1: Clone the Repository

If you haven't already cloned the repository:

```bash
git clone [REPOSITORY-URL]
cd frm-spark-databricks-mec
```

## Step 2: Start the Docker Environment

Navigate to the build directory:

```bash
cd build
```

Start the environment using Docker Compose:

```bash
# For newer Docker versions
docker compose up -d

# For older Docker versions
docker-compose up -d
```

This command starts all services defined in the `docker-compose.yml` file in detached mode.

## Step 3: Verify Running Containers

Check if all containers are running properly:

```bash
docker ps
```

You should see the Spark master, worker, and other related containers.

## Step 4: Access the Spark Web UI

Open your browser and navigate to:
- Spark Master UI: http://localhost:8080
- Spark Application UI (when jobs are running): http://localhost:4040

## Step 5: Execute a Spark Application

To run a Spark application using the sample data:

```bash
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/jobs/app/get-users-json.py
```

## Available Data Files

The following JSON data files are available in the `/frm-spark-databricks-mec/src/storage` directory and are mounted to the container:

- `postgres_drivers.json`: Driver information
- `mysql_restaurants.json`: Restaurant details
- `mysql_ratings.json`: Restaurant ratings
- `mssql_users.json`: User information
- `mongodb_users.json`: Delivery user profiles
- `kafka_status.json`: Order status events
- `kafka_orders.json`: Order information

## Common Commands

### Access Container Shell

```bash
docker exec -it spark-master bash
```

### View Container Logs

```bash
docker logs spark-master
```

### Run Interactive PySpark Shell

```bash
docker exec -it spark-master pyspark
```

### Stop the Environment

```bash
# From the build directory
docker compose down
```

## Troubleshooting

If you encounter issues:

1. Check if Docker is running
2. Verify that ports 8080 and 4040 are not in use by other applications
3. Check container logs for error messages
4. Ensure you're in the correct directory when running commands

---

Once the environment is up and running, you can start developing and executing Spark applications using the provided sample data.