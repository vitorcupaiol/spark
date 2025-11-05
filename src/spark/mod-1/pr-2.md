# pr-2: First Steps with Spark-Shell

Now that you’ve installed Apache Spark 3.5.5 (see `pr-1.md`), let’s explore the Spark Shell—a powerful interactive tool for running Spark commands. This guide introduces the Scala-based `spark-shell` and the Python-based `pyspark` shell, showing you how to perform basic operations. We’ll also connect this to the project by loading sample data.

---

## Prerequisites

- Spark 3.5.5 installed locally (Windows, macOS, or Linux).
- Terminal access: Command Prompt (Windows), Terminal (macOS/Linux).
- Optional: The project’s `src/spark/mod-1/data/users.json` file for testing.

---

## Launching Spark Shell

### Scala Shell (`spark-shell`)
1. Open your terminal.
2. Run:
   ```bash
   spark-shell
   ```
3. You’ll see the Spark logo and a Scala prompt (`scala>`). This is the interactive Scala shell for Spark.

### Python Shell (`pyspark`)
1. In your terminal, run:
   ```bash
   pyspark
   ```
2. You’ll see a Python prompt (`>>>`) with Spark initialized. This is the PySpark shell.

**Note**: Use `spark-shell` for Scala or `pyspark` for Python, depending on your preference. The project’s `pr-3-app.py` uses Python, so `pyspark` aligns with that.

---

## Basic Commands

### Scala Shell Examples
1. **Check Spark Version**:
   ```scala
   scala> spark.version
   ```
   Output: `"3.5.5"`

2. **Create a Simple Dataset**:
   ```scala
   scala> val data = Seq((1, "Alice"), (2, "Bob"))
   scala> val df = spark.createDataFrame(data).toDF("id", "name")
   scala> df.show()
   ```
   Output:
   ```
   +---+-----+
   | id| name|
   +---+-----+
   |  1|Alice|
   |  2|  Bob|
   +---+-----+
   ```

3. **Exit**:
   ```scala
   scala> :q
   ```

### PySpark Shell Examples
1. **Check Spark Version**:
   ```python
   >>> spark.version
   ```
   Output: `'3.5.5'`

2. **Create a Simple Dataset**:
   ```python
   >>> data = [(1, "Alice"), (2, "Bob")]
   >>> df = spark.createDataFrame(data, ["id", "name"])
   >>> df.show()
   ```
   Output:
   ```
   +---+-----+
   | id| name|
   +---+-----+
   |  1|Alice|
   |  2|  Bob|
   +---+-----+
   ```

3. **Exit**:
   ```python
   >>> exit()
   ```

---

## Working with Project Data

Let’s load the `users.json` file from the project (`src/spark/mod-1/data/users.json`) to see Spark in action.

1. **Copy the File**:
   - Place `users.json` in an accessible directory (e.g., `C:\spark-data` on Windows, `/home/user/spark-data` on macOS/Linux).

2. **Load in Scala Shell**:
   ```scala
   scala> val df = spark.read.json("C:/spark-data/users.json")  // Windows path
   scala> val df = spark.read.json("/home/user/spark-data/users.json")  // macOS/Linux path
   scala> df.show(1)
   ```
   Output (partial):
   ```
   +--------------------+----+-----+--------------------+--------------------+--------------------+---------+--------------------+--------------------+
   |    delivery_address|city|country|               email|         phone_number|                uuid|  user_id|    user_identifier| dt_current_timestamp|
   +--------------------+----+-----+--------------------+--------------------+--------------------+---------+--------------------+--------------------+
   |Sobrado 76 0225 V...|Palmas|   BR|ofelia.barbosa@bo...|(51) 4463-9821|94a1eff2-4dce-c26...|        1|    709.528.582-65|2025-02-05 21:50:...|
   +--------------------+----+-----+--------------------+--------------------+--------------------+---------+--------------------+--------------------+
   ```

3. **Load in PySpark Shell**:
   ```python
   >>> df = spark.read.json("C:/spark-data/users.json")  # Windows path
   >>> df = spark.read.json("/home/user/spark-data/users.json")  # macOS/Linux path
   >>> df.show(1)
   ```
   Output: Same as above.

4. **Count Rows**:
   - Scala: `df.count()`
   - Python: `df.count()`
   Output: `1` (since `users.json` has one record).

---

## Tips
- **Paths**: Use forward slashes (`/`) in file paths, even on Windows, or escape backslashes (e.g., `C:\\spark-data\\users.json`).
- **Errors**: If you get a `FileNotFoundException`, double-check the file path.
- **Stop Spark**: After exiting, Spark stops automatically in the shell.
