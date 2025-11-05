# Creating Session and Context in Spark

## Overview

The SparkSession is the entry point for any Spark application. Configuring it correctly is fundamental for:
- Performance optimization
- Resource allocation
- Scalability
- Efficient cluster utilization

This guide explains how to create and configure SparkSession objects, manage resources properly, and implement best practices for production environments.

## Prerequisites

- Apache Spark 3.5+ installed
- Python 3.6+
- Basic understanding of Spark architecture

## SparkContext vs SparkSession

### Historical Context

**SparkContext (Legacy API):**
- Primary entry point in Spark 1.x
- Used for low-level operations and RDD creation
- Requires separate contexts for different functionality (SQLContext, HiveContext)

**SparkSession (Modern API):**
- Introduced in Spark 2.0
- Unified interface that encapsulates all contexts
- Preferred entry point for all modern Spark applications

### Basic Examples

#### SparkContext (Legacy)

```python
from pyspark import SparkConf, SparkContext

# Configure Spark
conf = SparkConf().setAppName("LegacyApp").setMaster("local[*]")
sc = SparkContext(conf=conf)

# Create an RDD and perform operations
rdd = sc.parallelize([1, 2, 3, 4, 5])
print(f"Sum: {rdd.sum()}")  # 15

# Always stop when done
sc.stop()
```

#### SparkSession (Recommended)

```python
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("ModernApp") \
    .master("local[*]") \
    .getOrCreate()

# SparkContext is available as a property
sc = spark.sparkContext

# Create a DataFrame
df = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
df.show()

# Stop when done
spark.stop()
```

## Creating a Basic SparkSession

The simplest way to create a SparkSession:

```python
from pyspark.sql import SparkSession

# Basic configuration
spark = SparkSession.builder \
    .appName("UberEatsAnalytics") \
    .master("local[*]") \
    .getOrCreate()

# Load data from the repository storage
users_df = spark.read.json("./storage/mongodb/users/01JS4W5A7WWZBQ6Y1C465EYR76.jsonl")
users_df.show()

# Always stop the session when done
spark.stop()
```

### Key Builder Methods

- `.appName(name)`: Sets the application name shown in Spark UI
- `.master(url)`: Specifies the cluster manager URL
  - `local[*]`: Use all available cores locally
  - `local[n]`: Use n cores locally
  - `spark://host:port`: Connect to a Spark standalone cluster
  - `yarn`: Connect to a YARN cluster
- `.config(key, value)`: Set configuration parameters
- `.enableHiveSupport()`: Enable Hive integration
- `.getOrCreate()`: Returns existing session or creates a new one

## Resource Management

Properly configuring resources is essential for optimal performance:

```python
# Resource configuration
spark = SparkSession.builder \
    .appName("ResourceOptimizedApp") \
    .master("local[*]") \
    .config("spark.executor.memory", "2g") \          # Memory per executor
    .config("spark.driver.memory", "4g") \            # Driver memory
    .config("spark.executor.cores", "2") \            # Cores per executor
    .config("spark.default.parallelism", "8") \       # Default parallelism for RDDs
    .config("spark.sql.shuffle.partitions", "20") \   # Partitions for DataFrame operations
    .getOrCreate()
```

### Key Memory Parameters

- `spark.driver.memory`: Memory allocated to the driver process
- `spark.executor.memory`: Memory allocated to each executor
- `spark.memory.fraction`: Fraction of heap used for execution and storage (0.6 default)
- `spark.memory.storageFraction`: Fraction of execution memory used for storage (0.5 default)

### Computational Parameters

- `spark.default.parallelism`: Default number of partitions for RDDs
- `spark.sql.shuffle.partitions`: Number of partitions for shuffle operations (200 default)
- `spark.executor.cores`: Number of cores per executor
- `spark.task.cpus`: CPUs allocated per task (1 default)

## Best Practices: Design Patterns for SparkSession Management

### 1. Singleton Pattern

Ensures only one SparkSession exists across your application:

```python
# spark_manager.py
from pyspark.sql import SparkSession

class SparkManager:
    """A singleton manager for SparkSession"""
    
    _session = None
    
    @classmethod
    def get_session(cls, app_name="UberEatsApp"):
        """Get or create a SparkSession"""
        if cls._session is None:
            cls._session = SparkSession.builder \
                .appName(app_name) \
                .master("local[*]") \
                .getOrCreate()
        
        return cls._session
    
    @classmethod
    def stop_session(cls):
        """Stop the SparkSession if it exists"""
        if cls._session is not None:
            cls._session.stop()
            cls._session = None
```

Usage:
```python
from spark_manager import SparkManager

# Get the singleton session
spark = SparkManager.get_session()

# Use it for data processing
restaurants_df = spark.read.json("./storage/mysql/restaurants/01JS4W5A7YWTYRQKDA7F7N95VY.jsonl")
restaurants_df.show(5)

# Stop when done
SparkManager.stop_session()
```

### 2. Environment-Specific Configuration

Configure Spark differently based on deployment environment:

```python
# env_config.py
import os
from pyspark.sql import SparkSession

def create_session():
    """Create a session based on environment"""
    env = os.environ.get("SPARK_ENV", "dev")
    print(f"Creating session for: {env}")
    
    # Start building the session
    builder = SparkSession.builder.appName(f"UberEats-{env}")
    
    # Apply environment-specific configs
    if env == "dev":
        builder = builder \
            .master("local[*]") \
            .config("spark.driver.memory", "2g") \
            .config("spark.sql.shuffle.partitions", "10")
    elif env == "test":
        builder = builder \
            .master("yarn") \
            .config("spark.submit.deployMode", "client") \
            .config("spark.executor.instances", "2") \
            .config("spark.executor.memory", "4g")
    elif env == "prod":
        builder = builder \
            .master("yarn") \
            .config("spark.submit.deployMode", "cluster") \
            .config("spark.executor.instances", "5") \
            .config("spark.executor.memory", "8g")
    
    return builder.getOrCreate()
```

Usage:
```python
import os
from env_config import create_session

# Set environment
os.environ["SPARK_ENV"] = "dev"  # or "test" or "prod"

# Create environment-specific session
spark = create_session()

# Process data
drivers_df = spark.read.json("./storage/postgres/drivers/01JS4W5A74BK7P4BPTJV1D3MHA.jsonl")
print(f"Drivers count: {drivers_df.count()}")

# Clean up
spark.stop()
```

### 3. Production-Grade Pattern: External Configuration

For production environments, use external YAML configuration files:

Create `config/spark_config.yaml`:

```yaml
# Common settings for all environments
common:
  spark.sql.adaptive.enabled: "true"
  spark.serializer: org.apache.spark.serializer.KryoSerializer
  spark.network.timeout: 800s

# Development environment
dev:
  spark.master: local[*]
  spark.driver.memory: 2g
  spark.sql.shuffle.partitions: 10
  spark.ui.port: 4040

# Test environment
test:
  spark.master: yarn
  spark.submit.deployMode: client
  spark.executor.instances: 2
  spark.executor.memory: 4g
  spark.yarn.queue: test

# Production environment
prod:
  spark.master: yarn
  spark.submit.deployMode: cluster
  spark.executor.instances: 10
  spark.executor.memory: 16g
  spark.driver.memory: 8g
  spark.dynamicAllocation.enabled: true
  spark.dynamicAllocation.minExecutors: 5
  spark.dynamicAllocation.maxExecutors: 20
```

Create a manager that loads this configuration:

```python
# spark_factory.py
import os
import yaml
import logging
from typing import Dict, Any, Callable, Optional
from pyspark.sql import SparkSession

class SparkFactory:
    """Production-grade Spark session factory"""
    
    _session = None
    _logger = logging.getLogger(__name__)
    
    @classmethod
    def get_session(cls, app_name: str = "SparkApp", config_path: str = None) -> SparkSession:
        """
        Get or create a SparkSession using config from YAML file.
        
        Args:
            app_name: Application name
            config_path: Path to YAML config (default: config/spark_config.yaml)
        """
        if cls._session is None:
            # Determine environment
            env = os.environ.get("SPARK_ENV", "dev")
            cls._logger.info(f"Creating session for: {env}")
            
            # Start building the session
            builder = SparkSession.builder.appName(f"{app_name}-{env}")
            
            # Load configs from YAML
            config_path = config_path or os.path.join("config", "spark_config.yaml")
            configs = cls._load_configs(config_path, env)
            
            # Apply all configs
            for key, value in configs.items():
                builder = builder.config(key, value)
                
            # Create the session
            cls._session = builder.getOrCreate()
            
        return cls._session
    
    @classmethod
    def _load_configs(cls, config_path: str, env: str) -> Dict[str, str]:
        """Load configurations from YAML file."""
        try:
            with open(config_path, 'r') as file:
                all_configs = yaml.safe_load(file)
            
            # Start with common configs
            configs = all_configs.get('common', {}).copy()
            
            # Apply environment-specific configs
            if env in all_configs:
                configs.update(all_configs[env])
            
            return configs
            
        except Exception as e:
            cls._logger.error(f"Error loading config: {str(e)}")
            # Return minimal defaults
            return {
                "spark.master": "local[*]" if env == "dev" else "yarn",
                "spark.sql.adaptive.enabled": "true"
            }
    
    @classmethod
    def run_job(cls, job_func: Callable[[SparkSession], Any], 
                app_name: str = "SparkJob", config_path: str = None) -> Any:
        """Run a Spark job with proper session management."""
        try:
            # Get session
            spark = cls.get_session(app_name, config_path)
            
            # Run the job
            result = job_func(spark)
            return result
            
        except Exception as e:
            cls._logger.error(f"Error in job: {str(e)}", exc_info=True)
            raise
        finally:
            # Keep session by default in prod environments
            pass
    
    @classmethod
    def stop_session(cls) -> None:
        """Explicitly stop the SparkSession."""
        if cls._session is not None:
            cls._logger.info("Stopping SparkSession")
            cls._session.stop()
            cls._session = None
```

Usage example:

```python
# analysis_job.py
import os
import logging
from spark_factory import SparkFactory

# Set up logging
logging.basicConfig(level=logging.INFO)

def analyze_restaurants(spark):
    """Analyze restaurant data from UberEats."""
    # Load data from storage directory
    restaurants = spark.read.json("./storage/mysql/restaurants/01JS4W5A7YWTYRQKDA7F7N95VY.jsonl")
    
    # Register as temp view for SQL
    restaurants.createOrReplaceTempView("restaurants")
    
    # Perform analysis
    result = spark.sql("""
        SELECT 
            cuisine_type, 
            COUNT(*) as count,
            ROUND(AVG(average_rating), 2) as avg_rating
        FROM restaurants
        GROUP BY cuisine_type
        ORDER BY avg_rating DESC
    """)
    
    return result

if __name__ == "__main__":
    # Set environment
    os.environ["SPARK_ENV"] = "dev"  # "test" or "prod" in other environments
    
    # Run the analysis
    result = SparkFactory.run_job(analyze_restaurants, "RestaurantAnalysis")
    
    # Display results
    result.show()
    
    # Clean up
    SparkFactory.stop_session()
```

## Practical Example: Analyzing UberEats Data

Let's apply what we've learned to analyze the UberEats datasets:

```python
# ubereats_analysis.py
from pyspark.sql import SparkSession

def create_optimized_session():
    """Create an optimized session for UberEats data analysis."""
    return SparkSession.builder \
        .appName("UberEatsAnalysis") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "20") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()

# Create session
spark = create_optimized_session()

try:
    # Load data from storage directory
    restaurants = spark.read.json("./storage/mysql/restaurants/01JS4W5A7YWTYRQKDA7F7N95VY.jsonl")
    ratings = spark.read.json("./storage/mysql/ratings/01JS4W5A7YWTYRQKDA7F7N95VZ.jsonl")
    
    # Register temporary views
    restaurants.createOrReplaceTempView("restaurants")
    ratings.createOrReplaceTempView("ratings")
    
    # Join data and analyze
    results = spark.sql("""
        SELECT 
            r.name as restaurant_name,
            r.cuisine_type,
            r.city,
            r.average_rating,
            r.num_reviews
        FROM restaurants r
        WHERE r.average_rating > 4.0
        ORDER BY r.average_rating DESC, r.num_reviews DESC
        LIMIT 10
    """)
    
    # Show results
    print("===== Top Rated Restaurants =====")
    results.show(truncate=False)
    
    # Output session info
    sc = spark.sparkContext
    print(f"\nSpark Version: {spark.version}")
    print(f"Application ID: {sc.applicationId}")
    print(f"Number of configured executors: {sc.getConf().get('spark.executor.instances', '1')}")
    
finally:
    # Always stop the session
    spark.stop()
    print("\nSparkSession stopped")
```

## Exercises

1. **Basic Session**: Create a SparkSession and load the PostgreSQL drivers data from `./storage/postgres/drivers/01JS4W5A74BK7P4BPTJV1D3MHA.jsonl`. Count the number of records and display the first 5 rows.

2. **Memory Configuration**: Create a session with optimized memory settings for a machine with 16GB RAM. Load and process the Kafka orders data from `./storage/kafka/orders/01JS4W5A7XY65S9Z69BY51BEJ4.jsonl`.

3. **Singleton Implementation**: Implement the singleton pattern for SparkSession management and use it to analyze both restaurant data from `./storage/mysql/restaurants/01JS4W5A7YWTYRQKDA7F7N95VY.jsonl` and ratings data from `./storage/mysql/ratings/01JS4W5A7YWTYRQKDA7F7N95VZ.jsonl`.

4. **Environment-Specific**: Create a script that uses different SparkSession configurations based on an environment variable (`dev`, `test`, `prod`).

5. **Production-Ready**: Extend the production-grade example to include proper logging and error handling. Process datasets from multiple sources in the UberEats data directory structure (e.g., MongoDB users, PostgreSQL drivers, MySQL restaurants).

## Summary

- SparkSession is the unified entry point for modern Spark applications
- Proper configuration is crucial for performance and resource utilization
- Design patterns like Singleton and Factory improve maintainability
- Environment-specific configuration enables smooth deployment across environments
- External configuration files are best practice for production environments

In the next practical session, we'll explore connecting to remote Spark clusters and optimizing data access patterns.
