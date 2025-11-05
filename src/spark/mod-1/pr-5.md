# pr-5: Building Your First Docker Custom Spark Image

Welcome to the fifth module of this training course! After running Spark in a Docker container (`pr-4.md`), let’s build a custom Docker image based on `bitnami/spark:latest`. We’ll create a `Dockerfile` in `src/spark/mod-1/scripts/`, add layers with our app files (`pr-3-app.py` and `users.json`), and run it. This is a simple, step-by-step GOAT (Greatest of All Time) class to prep you for distributed systems next!

---

## Prerequisites

- **Docker**: Installed and running (Windows, macOS, or Linux).
  - Get it from [docker.com](https://www.docker.com/get-started).
- Terminal access: Command Prompt (Windows), Terminal (macOS/Linux).
- The project files in `src/spark/mod-1/scripts/`:
  - `pr-3-app.py`
  - `users.json`
- Internet access to pull `bitnami/spark:latest`.

---

## Why Build a Custom Image?

A custom image packages your app with Spark, ensuring portability and consistency. By placing the `Dockerfile` in `scripts/`, we’ll streamline file inclusion and build a reusable image.

---

## Step 1: Set Up Your Dockerfile

1. **Navigate to Scripts**:
   - Go to `src/spark/mod-1/scripts/`:
     ```bash
     cd /Users/luanmorenomaciel/GitHub/frm-spark-databricks-mec/src/spark/mod-1/scripts/
     ```

2. **Verify Files**:
   - Check the directory:
     ```bash
     ls -la
     ```
   - Ensure `pr-3-app.py` and `users.json` are present.

3. **Create the Dockerfile**:
   - Create `Dockerfile` with:
     ```Dockerfile
     # Base image
     FROM bitnami/spark:latest

     # Set working directory
     WORKDIR /app

     # Copy application files from current directory
     COPY pr-3-app.py /app/
     COPY users.json /app/

     # Install a simple dependency (optional)
     RUN pip install --no-cache-dir numpy

     # Keep container running
     CMD ["tail", "-f", "/dev/null"]
     ```
   - **Notes**:
     - `COPY pr-3-app.py /app/`: Copies from `scripts/` (build context) to `/app`.
     - No complex paths since files are local to the `Dockerfile`.

---

## Step 2: Build the Custom Image

1. **Build the Image**:
   - From `src/spark/mod-1/scripts/`:
     ```bash
     docker build -t my-spark-app:latest .
     ```
   - `-t my-spark-app:latest`: Names the image.
   - `.`: Uses `scripts/` as the build context.

2. **Verify**:
   ```bash
   docker images
   ```
   - Look for `my-spark-app:latest`.

---

## Step 3: Run Your Custom Image

1. **Start the Container**:
   ```bash
   docker run -d --name my-spark-container my-spark-app:latest
   ```

2. **Check Files**:
   ```bash
   docker exec my-spark-container ls -la /app
   ```
   - Confirms `pr-3-app.py` and `users.json`.

3. **Run Spark-Submit**:
   ```bash
   docker exec my-spark-container spark-submit pr-3-app.py
   ```

4. **Expected Output**:
   ```
   1
   +--------------------+----+-----+--------------------+--------------------+--------------------+---------+--------------------+--------------------+
   |    delivery_address|city|country|               email|         phone_number|                uuid|  user_id|    user_identifier| dt_current_timestamp|
   +--------------------+----+-----+--------------------+--------------------+--------------------+---------+--------------------+--------------------+
   |Sobrado 76 0225 V...|Palmas|   BR|ofelia.barbosa@bo...|(51) 4463-9821|94a1eff2-4dce-c26...|        1|    709.528.582-65|2025-02-05 21:50:...|
   +--------------------+----+-----+--------------------+--------------------+--------------------+---------+--------------------+--------------------+
   ```

---

## Step 4: Customize with Spark-Submit

1. **Set Master**:
   ```bash
   docker exec my-spark-container spark-submit --master local[2] pr-3-app.py
   ```

2. **Verbose Mode**:
   ```bash
   docker exec my-spark-container spark-submit --verbose pr-3-app.py
   ```

---

## Step 5: Hands-On Exercise

1. **New Script**:
   - In `scripts/`, create `pr-5-exercise.py`:
     ```python
     from pyspark.sql import SparkSession

     spark = SparkSession.builder \
         .appName("pr-5-exercise") \
         .getOrCreate()

     df_users = spark.read.json("users.json")
     df_users.select("email", "city").show()

     spark.stop()
     ```

2. **Update Dockerfile**:
   ```Dockerfile
   FROM bitnami/spark:latest
   WORKDIR /app
   COPY pr-3-app.py /app/
   COPY users.json /app/
   COPY pr-5-exercise.py /app/
   RUN pip install --no-cache-dir numpy
   CMD ["tail", "-f", "/dev/null"]
   ```

3. **Rebuild**:
   ```bash
   docker build -t my-spark-app:latest .
   ```

4. **Run It**:
   - Stop and remove:
     ```bash
     docker stop my-spark-container
     docker rm my-spark-container
     ```
   - Start:
     ```bash
     docker run -d --name my-spark-container my-spark-app:latest
     ```
   - Execute:
     ```bash
     docker exec my-spark-container spark-submit pr-5-exercise.py
     ```

5. **Expected Output**:
   ```
   +--------------------+------+
   |               email|  city|
   +--------------------+------+
   |ofelia.barbosa@bo...|Palmas|
   +--------------------+------+
   ```

6. **Challenge**:
   - Add `RUN pip install pandas` to the `Dockerfile`, rebuild, and rerun `pr-5-exercise.py`.

---

## Step 6: Stop the Container

```bash
docker stop my-spark-container
docker rm my-spark-container
```

---

## Troubleshooting

- **COPY Error**:
  - Verify files:
    ```bash
    ls -la /Users/luanmorenomaciel/GitHub/frm-spark-databricks-mec/src/spark/mod-1/scripts/
    ```
  - Ensure `pr-3-app.py` and `users.json` are in `scripts/`.
- **Permission Issues (macOS)**:
  - Docker Desktop > Settings > Resources > File Sharing > Add `/Users/luanmorenomaciel/GitHub/`.
- **Build Fails**:
  - Add `--no-cache` if needed:
    ```bash
    docker build -t my-spark-app:latest --no-cache .
    ```
