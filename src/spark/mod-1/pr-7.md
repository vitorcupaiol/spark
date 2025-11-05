# pr-7: Running Your First Distributed Spark Application with Docker Compose

In `pr-6.md`, you set up a distributed Spark cluster with Docker Compose. Now, let’s harness that cluster to run a PySpark application! This class uses `get-users-json.py` in `src/app/` to process `users.json` from `src/storage/`, executing it on the cluster from outside Docker. We’ll monitor the job and dive into hands-on exercises to master distributed Spark.

---

## Prerequisites

- **Docker and Docker Compose**: Installed and running (Windows, macOS, or Linux).
- **Spark Cluster**: Running from `pr-6.md`. Start it from `/build/` if needed:
  ```bash
  cd /Users/luanmorenomaciel/GitHub/frm-spark-databricks-mec/build/
  docker-compose up -d
  ```
- **Files**:
  - Application: `src/app/get-users-json.py`
  - Data: `src/storage/users.json`
- **Terminal Access**: Command Prompt (Windows) or Terminal (macOS/Linux).

---

## Step 1: Prepare the Application Script

We’ll use your provided script, ensuring it’s ready for the cluster.

1. **Create `get-users-json.py`**:
   - Navigate to `src/app/` (create it if it doesn’t exist):
     ```bash
     mkdir -p /Users/luanmorenomaciel/GitHub/frm-spark-databricks-mec/src/app/
     cd /Users/luanmorenomaciel/GitHub/frm-spark-databricks-mec/src/app/
     ```
   - Create `get-users-json.py`:
     ```python
     """
     docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
       --master spark://spark-master:7077 \
       --deploy-mode client \
       /opt/bitnami/spark/jobs/app/get-users-json.py
     """

     from pyspark.sql import SparkSession

     spark = SparkSession.builder \
         .getOrCreate()

     df_users = spark.read.json("./storage/users.json")
     count = df_users.count()
     df_users.show(3)

     spark.stop()
     ```
   - **Note**: The master isn’t specified here; `spark-submit` will handle it. The docstring shows the intended command.

2. **Verify Data**:
   - Ensure `users.json` is in `src/storage/`:
     ```bash
     ls -la /Users/luanmorenomaciel/GitHub/frm-spark-databricks-mec/src/storage/
     ```

---

## Step 2: Run the Application on the Cluster

We’ll execute the script from outside the container, targeting the cluster’s master.

1. **Copy Script to Cluster**:
   - For simplicity, copy `get-users-json.py` to `/build/` (mapped to `/app/` in `pr-6.md`):
     ```bash
     cp /Users/luanmorenomaciel/GitHub/frm-spark-databricks-mec/src/app/get-users-json.py /Users/luanmorenomaciel/GitHub/frm-spark-databricks-mec/build/
     ```

2. **Run `spark-submit`**:
   - Use your specified command (adjusted for path):
     ```bash
     docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
       --master spark://spark-master:7077 \
       --deploy-mode client \
       /app/get-users-json.py
     ```
   - **Breakdown**:
     - `docker exec -it spark-master`: Runs inside the `spark-master` container.
     - `/opt/bitnami/spark/bin/spark-submit`: Path to `spark-submit`.
     - `--master spark://spark-master:7077`: Connects to the cluster.
     - `--deploy-mode client`: Driver runs via the container’s CLI.
     - `/app/get-users-json.py`: Script path in the container (mapped from `/build/`).

3. **Expected Output**:
   - After logs:
     ```
     +--------------------+----+-----+--------------------+--------------------+--------------------+
     |    delivery_address|city|country|               email|         phone_number|                uuid|
     +--------------------+----+-----+--------------------+--------------------+--------------------+
     |Sobrado 76 0225 V...|Palmas|   BR|ofelia.barbosa@bo...|(51) 4463-9821|94a1eff2-4dce-c26...|
     +--------------------+----+-----+--------------------+--------------------+--------------------+
     ```

---

## Step 3: Monitor the Job

The Spark Web UI offers a window into your distributed job’s performance.

1. **Access the UI**:
   - Open `http://localhost:8080` (mapped from `spark-master:8080` in `pr-6.md`).
   - **Key Sections**:
     - **Workers**: Lists active workers (e.g., `spark-worker-1`). Check their status, cores, and memory usage.
     - **Running Applications**: Displays the job if still active.
     - **Completed Applications**: Shows `get-users-json` post-run with an Application ID (e.g., `app-202304...`).

2. **Explore Details**:
   - Click the Application ID:
     - **Stages**: Breaks down tasks (e.g., reading JSON, counting rows). Check task durations and parallelism.
     - **Executors**: Shows which workers executed tasks, with metrics like input data size and shuffle activity.
     - **Environment**: Lists Spark configs (e.g., master URL, memory settings).
   - Confirm tasks were distributed (e.g., split across workers if multiple are active).

3. **Why Monitor**:
   - Identifies bottlenecks (e.g., slow workers), verifies distribution, and aids optimization.

---

## Step 4: Hands-On Exercises

Let’s deepen your distributed Spark skills with three exercises.

### Exercise 1: Filter by Country
1. **Modify `get-users-json.py`**:
   - Update to filter by country:
     ```python
     """
     docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
       --master spark://spark-master:7077 \
       --deploy-mode client \
       /opt/bitnami/spark/jobs/app/get-users-json.py
     """

     from pyspark.sql import SparkSession

     spark = SparkSession.builder \
         .getOrCreate()

     df_users = spark.read.json("./storage/users.json")
     df_users.filter(df_users.country == "BR").show()

     spark.stop()
     ```
2. **Copy and Run**:
   ```bash
   cp /Users/luanmorenomaciel/GitHub/frm-spark-databricks-mec/src/app/get-users-json.py /Users/luanmorenomaciel/GitHub/frm-spark-databricks-mec/build/
   docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
     --master spark://spark-master:7077 \
     --deploy-mode client \
     /app/get-users-json.py
   ```
3. **Check Output**: Shows only rows with `country = "BR"`.
4. **Monitor**: Check the UI for the new job.

### Exercise 2: Aggregate by City
1. **Create `get-users-by-city.py`**:
   - In `src/app/`:
     ```python
     """
     docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
       --master spark://spark-master:7077 \
       --deploy-mode client \
       /opt/bitnami/spark/jobs/app/get-users-by-city.py
     """

     from pyspark.sql import SparkSession

     spark = SparkSession.builder \
         .getOrCreate()

     df_users = spark.read.json("./storage/users.json")
     df_users.groupBy("city").count().show()

     spark.stop()
     ```
2. **Copy and Run**:
   ```bash
   cp /Users/luanmorenomaciel/GitHub/frm-spark-databricks-mec/src/app/get-users-by-city.py /Users/luanmorenomaciel/GitHub/frm-spark-databricks-mec/build/
   docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
     --master spark://spark-master:7077 \
     --deploy-mode client \
     /app/get-users-by-city.py
   ```
3. **Expected Output**:
   ```
   +------+-----+
   |  city|count|
   +------+-----+
   |Palmas|    1|
   +------+-----+
   ```
4. **Monitor**: Verify task distribution in the UI.

---

## Troubleshooting

- **"Connection Refused"**:
  - Check cluster status:
    ```bash
    docker ps
    ```
  - View logs:
    ```bash
    docker logs spark-master
    ```
- **"FileNotFoundException"**:
  - Verify `users.json` in `src/storage/` and volume mapping in `docker-compose.yml`.
- **No Distribution**:
  - Ensure multiple workers are listed in the UI.
