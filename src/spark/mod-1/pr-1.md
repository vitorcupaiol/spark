# pr-1: Local Spark Installation

This guide covers installing Apache Spark locally on **Windows**, **macOS**, and **Linux**. Follow the steps for your operating system to set up Spark.

---

## Prerequisites

- **Java 8 or 11**: Spark requires Java.
- **Python 3.6+**: For PySpark compatibility.
- **Terminal**: Command Prompt (Windows), Terminal (macOS/Linux).

---

## Windows

### Step 1: Install Java
1. Download OpenJDK 11 from [AdoptOpenJDK](https://adoptopenjdk.net/) (`.msi` installer).
2. Run the installer.
3. Set `JAVA_HOME`:
   - Open "Edit the system environment variables" from the Start menu.
   - Add a new system variable:
     - Name: `JAVA_HOME`
     - Value: `C:\Program Files\AdoptOpenJDK\jdk-11.0.11.9-hotspot` (adjust path as needed).
   - Edit `Path`, add: `%JAVA_HOME%\bin`.
4. Verify: In a new Command Prompt, run `java -version`.

### Step 2: Install Python
1. Download Python 3.6+ from [python.org](https://www.python.org/downloads/).
2. Run the installer, checking "Add Python to PATH."
3. Verify: In a new Command Prompt, run `python --version`.

### Step 3: Install Spark
1. Download Spark 3.5.5 (Hadoop 3.x) from [spark.apache.org](https://spark.apache.org/downloads.html) (`.tgz` file).
2. Extract to `C:\spark-3.5.5-bin-hadoop3` using [7-Zip](https://www.7-zip.org/).
3. Set `SPARK_HOME`:
   - Add a new system variable:
     - Name: `SPARK_HOME`
     - Value: `C:\spark-3.5.5-bin-hadoop3`.
   - Edit `Path`, add: `%SPARK_HOME%\bin`.
4. Install `winutils`:
   - Download `winutils.exe` for Hadoop 3.x from [this GitHub repo](https://github.com/cdarlint/winutils).
   - Place in `C:\hadoop\bin`.
   - Set variable:
     - Name: `HADOOP_HOME`
     - Value: `C:\hadoop`.
   - Add `%HADOOP_HOME%\bin` to `Path`.

### Step 4: Verify
- In a new Command Prompt, run `spark-shell`. Look for the Spark logo. Exit with `:q`.

---

## macOS

### Step 1: Install Java
1. Install Homebrew: `/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"` (if not installed).
2. Run `brew install openjdk@11`.
3. Set `JAVA_HOME`:
   - Add to `~/.zshrc` or `~/.bash_profile`:
     ```bash
     export JAVA_HOME=$(/usr/libexec/java_home -v 11)
     ```
   - Run `source ~/.zshrc` (or appropriate file).
4. Verify: Run `java -version`.

### Step 2: Install Python
1. Run `brew install python`.
2. Verify: Run `python3 --version`.

### Step 3: Install Spark
1. Run `brew install apache-spark` (installs the latest version, e.g., 3.5.5).
2. Alternatively, download manually:
   - Get Spark 3.5.5 (Hadoop 3.x) from [spark.apache.org](https://spark.apache.org/downloads.html).
   - Extract: `tar -xzf spark-3.5.5-bin-hadoop3.tgz`.
   - Move to `/usr/local/spark-3.5.5-bin-hadoop3`.
   - Add to `~/.zshrc`:
     ```bash
     export SPARK_HOME=/usr/local/spark-3.5.5-bin-hadoop3
     export PATH=$SPARK_HOME/bin:$PATH
     ```
   - Run `source ~/.zshrc`.

### Step 4: Verify
- Run `spark-shell`. Look for the Spark logo. Exit with `:q`.

---

## Linux (Ubuntu/Debian-based)

### Step 1: Install Java
1. Update packages: `sudo apt update`.
2. Install: `sudo apt install openjdk-11-jdk`.
3. Set `JAVA_HOME`:
   - Add to `~/.bashrc`:
     ```bash
     export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
     export PATH=$JAVA_HOME/bin:$PATH
     ```
   - Run `source ~/.bashrc`.
4. Verify: Run `java -version`.

### Step 2: Install Python
1. Install: `sudo apt install python3 python3-pip`.
2. Verify: Run `python3 --version`.

### Step 3: Install Spark
1. Download Spark 3.5.5 (Hadoop 3.x) from [spark.apache.org](https://spark.apache.org/downloads.html):
   ```bash
   wget https://downloads.apache.org/spark/spark-3.5.5/spark-3.5.5-bin-hadoop3.tgz
   tar -xzf spark-3.5.5-bin-hadoop3.tgz
   sudo mv spark-3.5.5-bin-hadoop3 /opt/spark
   ```
2. Set environment:
   - Add to `~/.bashrc`:
     ```bash
     export SPARK_HOME=/opt/spark
     export PATH=$SPARK_HOME/bin:$PATH
     ```
   - Run `source ~/.bashrc`.

### Step 4: Verify
- Run `spark-shell`. Look for the Spark logo. Exit with `:q`.