# pr-3: First Steps with Spark-Submit

Welcome to the third module of this training course! After installing Spark (`pr-1.md`) and exploring the Spark Shell (`pr-2.md`), it’s time to master `spark-submit`—the tool for running Spark applications like a pro. We’ll use the project’s `pr-3-app.py` script, now updated to load `users.json` from the `scripts/` directory, and dive into `spark-submit` options, including insights from `spark-submit --help`. This is the GOAT (Greatest of All Time) Spark-Submit class—let’s get started!

---

## Prerequisites

- Spark 3.5.5 installed locally (see `pr-1.md`).
- Terminal access: Command Prompt (Windows), Terminal (macOS/Linux).
- The project files in `src/spark/mod-1/scripts/`:
  - `pr-3-app.py`
  - `users.json` (moved from `data/` to `scripts/`)

---

## What is Spark-Submit?

`spark-submit` is Spark’s command-line tool for submitting applications to a Spark cluster—or running them locally, as we’ll do here. It’s the bridge from interactive exploration (Spark Shell) to scripted execution, perfect for production workflows.

---

## Step 1: Understanding the Updated Application

Since `users.json` is now in `scripts/`, here’s the updated `pr-3-app.py`:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .getOrCreate()

df_users = spark.read.json("users.json")  
count = df_users.count()
df_users.show(3)

spark.stop()
```

### Breakdown
- **SparkSession**: Initializes Spark with the name "pr-3-app".
- **Reading Data**: Loads `users.json` from the same directory (`scripts/`) into a DataFrame (`df_users`).
- **Operations**:
  - `count()`: Counts rows (1 in our case).
  - `show(3)`: Displays up to 3 rows (only 1 exists).
- **Cleanup**: `spark.stop()` closes the session.

The `users.json` content remains:

```json
{"user_id":1,"country":"BR","city":"Palmas","phone_number":"(51) 4463-9821","email":"ofelia.barbosa@bol.com.br","uuid":"94a1eff2-4dce-c26e-cea4-3c55b1f8418b","delivery_address":"Sobrado 76 0225 Viela Pérola, Córrego do Bom Jesus, AL 13546-174","user_identifier":"709.528.582-65","dt_current_timestamp":"2025-02-05 21:50:45.932"}
```

---

## Step 2: Preparing to Run

1. **Navigate to the Directory**:
   - Open your terminal and change to `src/spark/mod-1/scripts/`:
     ```bash
     cd path/to/src/spark/mod-1/scripts
     ```
     Replace `path/to/` with your repo’s location.

2. **Check Files**:
   - Confirm `pr-3-app.py` and `users.json` are both in `scripts/`.

3. **Verify Spark**:
   - Run `spark-submit --version` to ensure Spark 3.5.5 is ready.

---

## Step 3: Running with Spark-Submit

### Basic Command
Execute the script:

```bash
spark-submit pr-3-app.py
```

### What Happens?
1. Spark starts a local cluster.
2. The script runs:
   - Loads `users.json` from `scripts/`.
   - Outputs the row count (`1`).
   - Displays the DataFrame.
3. Spark shuts down.

### Expected Output
After logs, you’ll see:

```
1
+--------------------+----+-----+--------------------+--------------------+--------------------+---------+--------------------+--------------------+
|    delivery_address|city|country|               email|         phone_number|                uuid|  user_id|    user_identifier| dt_current_timestamp|
+--------------------+----+-----+--------------------+--------------------+--------------------+---------+--------------------+--------------------+
|Sobrado 76 0225 V...|Palmas|   BR|ofelia.barbosa@bo...|(51) 4463-9821|94a1eff2-4dce-c26...|        1|    709.528.582-65|2025-02-05 21:50:...|
+--------------------+----+-----+--------------------+--------------------+--------------------+---------+--------------------+--------------------+
```

---

## Step 4: Exploring Spark-Submit --help

Run `spark-submit --help` in your terminal to see all options. Here are some interesting ones to discuss:

### Key Options
1. **`--master`**:
   - Specifies where to run the app (e.g., `local`, a cluster URL).
   - Example:
     ```bash
     spark-submit --master local[2] pr-3-app.py
     ```
     - `local[2]`: Runs locally with 2 cores. Try `local[*]` for all available cores.

2. **`--deploy-mode`**:
   - Chooses where the driver runs: `client` (local machine) or `cluster` (on a cluster).
   - Example (local default is `client`):
     ```bash
     spark-submit --deploy-mode client pr-3-app.py
     ```

3. **`--conf`**:
   - Sets custom Spark configurations.
   - Example: Limit memory and enable logging:
     ```bash
     spark-submit --conf spark.driver.memory=2g --conf spark.eventLog.enabled=true pr-3-app.py
     ```
     - `spark.driver.memory=2g`: Sets driver memory to 2 GB.
     - `spark.eventLog.enabled=true`: Logs events (check `SPARK_HOME/logs` if configured).

4. **`--py-files`**:
   - Adds Python dependencies (e.g., `.py` or `.zip` files).
   - Example: If `pr-3-app.py` used a helper module `utils.py`:
     ```bash
     spark-submit --py-files utils.py pr-3-app.py
     ```

5. **`--files`**:
   - Uploads files to the working directory (useful for data or configs).
   - Example: If `users.json` were elsewhere:
     ```bash
     spark-submit --files /path/to/users.json pr-3-app.py
     ```
     - Note: Our script assumes `users.json` is local, so this isn’t needed now.

### Try It!
Combine options:
```bash
spark-submit --master local[4] --name "GOATJob" --conf spark.driver.memory=4g pr-3-app.py
```
- Uses 4 cores, names the job "GOATJob," and allocates 4 GB to the driver.

---

## Step 5: Hands-On Exercise

Let’s make this class legendary with a practical task!

1. **Modify the Script**:
   - Copy `pr-3-app.py` to `pr-3-exercise.py`.
   - Add this before `spark.stop()`:
     ```python
     df_users.select("email", "user_identifier").show()
     ```
   - Save it.

2. **Run with Options**:
   ```bash
   spark-submit --master local[2] --name "UserExtract" pr-3-exercise.py
   ```

3. **Expected Output**:
   - Original `count` and `show(3)` output, then:
     ```
     +--------------------+--------------------+
     |               email|    user_identifier|
     +--------------------+--------------------+
     |ofelia.barbosa@bo...|    709.528.582-65|
     +--------------------+--------------------+
     ```

4. **Challenge**:
   - Update `pr-3-exercise.py` to filter for `city == "Palmas"` and show `email` and `city`. Run it with `--verbose`:
     ```bash
     spark-submit --verbose pr-3-exercise.py
     ```
     - Hint: Use `df_users.filter(df_users.city == "Palmas").select("email", "city").show()`.

---

## Troubleshooting

- **FileNotFoundException**: Confirm `users.json` is in `scripts/`. Use an absolute path if needed (e.g., `/path/to/scripts/users.json`).
- **Option Errors**: Check `spark-submit --help` for correct syntax.
- **Resource Issues**: Adjust `--driver-memory` (e.g., `4g`) if it fails.
