# pr-4: First Steps with Spark & Docker

Welcome to the fourth module of this training course! After mastering local Spark installation (`pr-1.md`), Spark Shell (`pr-2.md`), and `spark-submit` (`pr-3.md`), let’s run Spark in a Docker container using `bitnami/spark:latest`. We’ll map our `src/spark/mod-1/scripts/` directory (containing `pr-3-app.py` and `users.json`) to `/app`, set the working directory correctly, and keep the container running for easy access. This is a GOAT (Greatest of All Time) class—let’s get it right!

---

## Prerequisites

- **Docker**: Installed and running (Windows, macOS, or Linux).
  - Get it from [docker.com](https://www.docker.com/get-started).
- Terminal access: Command Prompt (Windows), Terminal (macOS/Linux).
- The project files in `src/spark/mod-1/scripts/`:
  - `pr-3-app.py`
  - `users.json`
- Internet access to pull the image.

---

## Why Docker?

Docker provides a consistent, pre-configured Spark environment. With `bitnami/spark:latest`, we’ll run your app without local setup hassles, ensuring files are mapped and accessible.

---

## Step 1: Pull the Docker Image

1. Open your terminal.
2. Pull the image:
   ```bash
   docker pull bitnami/spark:latest
   ```
3. Verify:
   ```bash
   docker images
   ```
   - Look for `bitnami/spark` with `latest`.

---

## Step 2: Prepare Your Files

The script `pr-3-app.py` expects `users.json` in its working directory:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("pr-3-app") \
    .getOrCreate()

df_users = spark.read.json("users.json")  # Relative path
count = df_users.count()
df_users.show(3)

spark.stop()
```

- **Location**: Ensure `pr-3-app.py` and `users.json` are in `src/spark/mod-1/scripts/`.
- **Mapping**: We’ll map this to `/app` and set it as the working directory.

---

## Step 3: Run a Persistent Container

Let’s run the container in the background with the correct working directory:

1. Start the container:
   ```bash
   docker run -d --name spark-container -v /absolute/path/to/src/spark/mod-1/scripts:/app -w /app bitnami/spark:latest tail -f /dev/null
   ```
   - `-d`: Detached mode (background).
   - `--name spark-container`: Easy reference.
   - `-v`: Maps `scripts/` to `/app`.
   - `-w /app`: Sets `/app` as the working directory.
   - `tail -f /dev/null`: Keeps it running.
   - Replace `/absolute/path/to/` with your path.

   **Your Specific Command**:
   ```bash
   docker run -d --name spark-container -v /Users/luanmorenomaciel/GitHub/frm-spark-databricks-mec/src/spark/mod-1/scripts:/app -w /app bitnami/spark:latest tail -f /dev/null
   ```

2. Verify it’s running:
   ```bash
   docker ps
   ```
   - Look for `spark-container`.

3. Check the files:
   ```bash
   docker exec spark-container ls -la /app
   ```
   - Confirms `pr-3-app.py` and `users.json` are present.

---

## Step 4: Execute Spark-Submit

Run the script in the running container:

1. Execute:
   ```bash
   docker exec spark-container spark-submit pr-3-app.py
   ```
   - Since the working directory is `/app`, `users.json` is found automatically.

2. **Expected Output**:
   ```
   1
   +--------------------+----+-----+--------------------+--------------------+--------------------+---------+--------------------+--------------------+
   |    delivery_address|city|country|               email|         phone_number|                uuid|  user_id|    user_identifier| dt_current_timestamp|
   +--------------------+----+-----+--------------------+--------------------+--------------------+---------+--------------------+--------------------+
   |Sobrado 76 0225 V...|Palmas|   BR|ofelia.barbosa@bo...|(51) 4463-9821|94a1eff2-4dce-c26...|        1|    709.528.582-65|2025-02-05 21:50:...|
   +--------------------+----+-----+--------------------+--------------------+--------------------+---------+--------------------+--------------------+
   ```

---

## Step 5: Customizing with Spark-Submit

Add options from the running container:

1. **Set Master**:
   ```bash
   docker exec spark-container spark-submit --master local[2] pr-3-app.py
   ```

2. **Add Configuration**:
   ```bash
   docker exec spark-container spark-submit --conf spark.driver.memory=2g pr-3-app.py
   ```

3. **Verbose Mode**:
   ```bash
   docker exec spark-container spark-submit --verbose pr-3-app.py
   ```

---

## Step 6: Hands-On Exercise

1. **New Script**:
   - Copy `pr-3-app.py` to `pr-4-exercise.py` in `scripts/`.
   - Add before `spark.stop()`:
     ```python
     df_users.select("city", "phone_number").show()
     ```

2. **Run It**:
   ```bash
   docker exec spark-container spark-submit pr-4-exercise.py
   ```

3. **Expected Output**:
   - Original output, then:
     ```
     +------+--------------------+
     |  city|        phone_number|
     +------+--------------------+
     |Palmas|    (51) 4463-9821|
     +------+--------------------+
     ```

4. **Challenge**:
   - Modify `pr-4-exercise.py` to filter `country == "BR"` and show `email`. Run:
     ```bash
     docker exec spark-container spark-submit --master local[4] pr-4-exercise.py
     ```
     - Hint: `df_users.filter(df_users.country == "BR").select("email").show()`.

---

## Step 7: Stop the Container

When finished:
```bash
docker stop spark-container
docker rm spark-container
```

---

## Troubleshooting

- **Path Not Found Error**:
  - **Check Mapping**:
    ```bash
    docker exec spark-container ls -la /app
    ```
    - If empty, verify the local path:
      ```bash
      ls -la /Users/luanmorenomaciel/GitHub/frm-spark-databricks-mec/src/spark/mod-1/scripts
      ```
  - **Docker Permissions (macOS)**:
    - Docker Desktop > Settings > Resources > File Sharing.
    - Add `/Users/luanmorenomaciel/GitHub/` and restart Docker.
- **Container Exited**:
  - Check `docker ps -a`. Restart with:
    ```bash
    docker start spark-container
    ```
- **Wrong Directory**: The `-w /app` flag ensures `users.json` is in the working directory.
