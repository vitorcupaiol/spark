# Frameworks & Design Patterns for Data Pipelines

## Introduction

Building on our previous Spark sessions, we'll now explore how to structure data pipelines for maintainability, scalability, and reusability. This guide focuses on practical design patterns, config management, and best practices for production-grade data pipelines.

## Project Structure

A well-organized structure is the foundation of any scalable project:

```
/src/app/
├── config/                  # Configuration files
│   ├── __init__.py
│   └── config.yaml          # Environment-based configuration
├── core/                    # Core components
│   ├── __init__.py
│   ├── session.py           # Spark session management
│   └── pipeline.py          # Pipeline base class
├── etl/                     # ETL components
│   ├── __init__.py
│   ├── extract.py           # Data extraction
│   ├── transform.py         # Data transformation
│   └── load.py              # Data loading
├── utils/                   # Utilities
│   ├── __init__.py
│   └── logger.py            # Logging setup
├── main.py                  # Entry point
└── pipeline.py              # Pipeline implementation
```

## Configuration Management with YAML

### Environment-Based Configuration

Create a YAML configuration file that supports different environments:

```yaml
# File: /src/app/config/config.yaml
default: &default
  app:
    name: "UberEats Data Pipeline"
    log_level: "INFO"
  spark:
    log_level: "WARN"
  storage:
    input_path: "./storage"
    output_path: "./output"

dev:
  <<: *default
  spark:
    master: "local[*]"
    driver_memory: "1g"
    executor_memory: "1g"
    shuffle_partitions: 10

test:
  <<: *default
  spark:
    master: "local[*]"
    driver_memory: "2g"
    executor_memory: "2g"
    shuffle_partitions: 20

prod:
  <<: *default
  spark:
    master: "yarn"
    deploy_mode: "cluster"
    driver_memory: "4g"
    executor_memory: "4g"
    executor_instances: 2
    shuffle_partitions: 100
  storage:
    input_path: "s3a://ubereats-data/input"
    output_path: "s3a://ubereats-data/output"
```

### Configuration Loader

```python
# File: /src/app/utils/config.py
import os
import yaml

def load_config(env=None):
    """Load configuration based on environment"""
    env = env or os.environ.get("ENV", "dev")
    
    with open("config/config.yaml", "r") as file:
        config = yaml.safe_load(file)
    
    if env not in config:
        raise ValueError(f"Environment '{env}' not found in config")
    
    return config[env]
```

## Design Patterns for Data Pipelines

Let's implement key design patterns used in data engineering:

### 1. Singleton Pattern - Spark Session Manager

```python
# File: /src/app/core/session.py
from functools import lru_cache
from pyspark.sql import SparkSession

class SparkSessionManager:
    """Singleton manager for Spark sessions"""
    
    @staticmethod
    @lru_cache(maxsize=1)
    def get_session(config):
        """Get or create a Spark session"""
        builder = SparkSession.builder.appName(config["app"]["name"])
        
        # Apply Spark configurations
        for key, value in config["spark"].items():
            if key == "master":
                builder = builder.master(value)
            else:
                builder = builder.config(f"spark.{key}", value)
        
        return builder.getOrCreate()
    
    @staticmethod
    def stop_session():
        """Stop the current Spark session"""
        SparkSession.getActiveSession().stop()
```

### 2. Template Method Pattern - Pipeline Base Class

```python
# File: /src/app/core/pipeline.py
import logging
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)

class Pipeline(ABC):
    """Base class for all data pipelines - Template Method Pattern"""
    
    def __init__(self, config):
        self.config = config
        logger.info(f"Initialized pipeline with config: {config}")
    
    @abstractmethod
    def extract(self):
        """Extract data from source"""
        pass
    
    @abstractmethod
    def transform(self, data):
        """Transform the data"""
        pass
    
    @abstractmethod
    def load(self, data):
        """Load data to destination"""
        pass
    
    def run(self):
        """Execute the pipeline - Template Method"""
        try:
            logger.info("Starting extraction phase")
            extracted_data = self.extract()
            
            logger.info("Starting transformation phase")
            transformed_data = self.transform(extracted_data)
            
            logger.info("Starting loading phase")
            self.load(transformed_data)
            
            logger.info("Pipeline execution completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Pipeline execution failed: {str(e)}", exc_info=True)
            return False
```

### 3. Strategy Pattern - ETL Components

```python
# File: /src/app/etl/extract.py
from abc import ABC, abstractmethod

class Extractor(ABC):
    """Base class for all extractors - Strategy Pattern"""
    
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config
    
    @abstractmethod
    def extract(self):
        """Extract data from source"""
        pass

class JsonExtractor(Extractor):
    """JSON file extractor implementation"""
    
    def extract(self):
        """Extract data from JSON file"""
        path = self.config["storage"]["input_path"]
        return self.spark.read.json(f"{path}/{self.config['source_path']}")

class PostgresExtractor(Extractor):
    """PostgreSQL extractor implementation"""
    
    def extract(self):
        """Extract data from PostgreSQL"""
        # PostgreSQL connection implementation
        pass

# File: /src/app/etl/transform.py
from abc import ABC, abstractmethod
from pyspark.sql.functions import col, when

class Transformer(ABC):
    """Base class for all transformers - Strategy Pattern"""
    
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config
    
    @abstractmethod
    def transform(self, data):
        """Transform the data"""
        pass

class RestaurantTransformer(Transformer):
    """Restaurant data transformer implementation"""
    
    def transform(self, df):
        """Transform restaurant data"""
        return (df
            .select("restaurant_id", "name", "cuisine_type", "city", 
                    "average_rating", "num_reviews")
            .filter(col("average_rating").isNotNull())
            .withColumn("rating_category", 
                when(col("average_rating") >= 4.5, "Excellent")
                .when(col("average_rating") >= 4.0, "Very Good")
                .when(col("average_rating") >= 3.5, "Good")
                .when(col("average_rating") >= 3.0, "Average")
                .otherwise("Below Average"))
        )

# File: /src/app/etl/load.py
from abc import ABC, abstractmethod

class Loader(ABC):
    """Base class for all loaders - Strategy Pattern"""
    
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config
    
    @abstractmethod
    def load(self, data):
        """Load data to destination"""
        pass

class ParquetLoader(Loader):
    """Parquet file loader implementation"""
    
    def load(self, df):
        """Load data to Parquet file"""
        path = f"{self.config['storage']['output_path']}/{self.config['destination_path']}"
        
        writer = df.write.mode("overwrite")
        
        # Apply partitioning if configured
        if "partition_by" in self.config:
            writer = writer.partitionBy(self.config["partition_by"])
        
        writer.parquet(path)
```

### 4. Factory Pattern - Component Creation

```python
# File: /src/app/etl/factory.py
from etl.extract import JsonExtractor, PostgresExtractor
from etl.transform import RestaurantTransformer
from etl.load import ParquetLoader

class ETLFactory:
    """Factory for creating ETL components"""
    
    @staticmethod
    def create_extractor(extractor_type, spark, config):
        """Create an extractor instance based on type"""
        extractors = {
            "json": JsonExtractor,
            "postgres": PostgresExtractor
        }
        
        if extractor_type not in extractors:
            raise ValueError(f"Unknown extractor type: {extractor_type}")
        
        return extractors[extractor_type](spark, config)
    
    @staticmethod
    def create_transformer(transformer_type, spark, config):
        """Create a transformer instance based on type"""
        transformers = {
            "restaurant": RestaurantTransformer
        }
        
        if transformer_type not in transformers:
            raise ValueError(f"Unknown transformer type: {transformer_type}")
        
        return transformers[transformer_type](spark, config)
    
    @staticmethod
    def create_loader(loader_type, spark, config):
        """Create a loader instance based on type"""
        loaders = {
            "parquet": ParquetLoader
        }
        
        if loader_type not in loaders:
            raise ValueError(f"Unknown loader type: {loader_type}")
        
        return loaders[loader_type](spark, config)
```

## Complete End-to-End ETL Pipeline

Now, let's implement a complete pipeline using these patterns:

```python
# File: /src/app/pipeline.py
import logging
from core.pipeline import Pipeline
from core.session import SparkSessionManager
from etl.factory import ETLFactory

logger = logging.getLogger(__name__)

class UberEatsPipeline(Pipeline):
    """UberEats data processing pipeline"""
    
    def __init__(self, config):
        """Initialize the pipeline"""
        super().__init__(config)
        self.spark = SparkSessionManager.get_session(config)
        
        # Pipeline-specific configuration
        self.pipeline_config = {
            **config,
            "source_path": "mysql/restaurants/01JS4W5A7YWTYRQKDA7F7N95VY.jsonl",
            "destination_path": "restaurants",
            "partition_by": "cuisine_type"
        }
    
    def extract(self):
        """Extract data from source"""
        extractor = ETLFactory.create_extractor(
            "json", 
            self.spark, 
            self.pipeline_config
        )
        
        data = extractor.extract()
        logger.info(f"Extracted {data.count()} records")
        return data
    
    def transform(self, data):
        """Transform the data"""
        transformer = ETLFactory.create_transformer(
            "restaurant", 
            self.spark, 
            self.pipeline_config
        )
        
        transformed_data = transformer.transform(data)
        logger.info(f"Transformed data has {transformed_data.count()} records")
        return transformed_data
    
    def load(self, data):
        """Load data to destination"""
        loader = ETLFactory.create_loader(
            "parquet", 
            self.spark, 
            self.pipeline_config
        )
        
        loader.load(data)
        logger.info(f"Loaded {data.count()} records")
```

## Main Application

Finally, let's create the main entry point that runs our pipeline:

```python
# File: /src/app/main.py
import argparse
import logging
import os

from utils.config import load_config
from utils.logger import setup_logging
from pipeline import UberEatsPipeline

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="UberEats Data Pipeline")
    
    parser.add_argument(
        "--env",
        type=str,
        default=os.environ.get("ENV", "dev"),
        choices=["dev", "test", "prod"],
        help="Environment to run in"
    )
    
    return parser.parse_args()

def main():
    """Main entry point"""
    args = parse_args()
    
    # Load configuration
    config = load_config(args.env)
    
    # Setup logging
    setup_logging(config["app"]["log_level"])
    logger = logging.getLogger(__name__)
    
    logger.info(f"Starting pipeline in {args.env} environment")
    
    # Create and run pipeline
    pipeline = UberEatsPipeline(config)
    success = pipeline.run()
    
    if success:
        logger.info("Pipeline completed successfully")
        return 0
    else:
        logger.error("Pipeline execution failed")
        return 1

if __name__ == "__main__":
    exit(main())
```

```python
# File: /src/app/utils/logger.py
import logging

def setup_logging(level="INFO"):
    """Setup logging configuration"""
    numeric_level = getattr(logging, level.upper(), None)
    
    if not isinstance(numeric_level, int):
        raise ValueError(f"Invalid log level: {level}")
    
    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
```

## Core Design Patterns Explained

### 1. Singleton Pattern

**Purpose**: Ensures a class has only one instance and provides global access.

**Usage in Data Pipelines**: Managing Spark sessions to avoid the overhead of multiple sessions.

**Benefits**:
- Saves resources by sharing a single SparkSession
- Provides consistent access to configuration
- Controls initialization process

### 2. Template Method Pattern

**Purpose**: Defines the skeleton of an algorithm, deferring some steps to subclasses.

**Usage in Data Pipelines**: Standard ETL flow with customizable extract, transform, and load phases.

**Benefits**:
- Enforces a consistent pipeline structure
- Allows customization of specific phases
- Centralizes common logic (logging, error handling)

### 3. Strategy Pattern

**Purpose**: Defines a family of algorithms, encapsulates each one, and makes them interchangeable.

**Usage in Data Pipelines**: Different implementations for extraction, transformation, and loading.

**Benefits**:
- Easily swap data sources (JSON, PostgreSQL, S3)
- Change transformation logic without changing pipeline
- Support different output formats

### 4. Factory Pattern

**Purpose**: Creates objects without specifying the exact class.

**Usage in Data Pipelines**: Creating appropriate ETL components based on configuration.

**Benefits**:
- Centralizes object creation logic
- Decouples pipeline from component implementation
- Makes adding new components easier

## Best Practices Summary

### Code Organization
- **Modular Design**: Separate ETL stages into distinct modules
- **Logical Layers**: Structure code in layers (core, ETL, utilities)
- **Dependency Injection**: Inject dependencies (Spark session, config)

### Configuration
- **Environment-Based Config**: Different settings for dev/test/prod
- **External Config Files**: YAML for human-readable configuration
- **Configuration Decoupling**: Separate code from configuration

### Error Handling
- **Consistent Logging**: Structured logging throughout the pipeline
- **Graceful Failure**: Proper exception handling at each stage
- **Detailed Error Messages**: Context-rich error information

### Maintainability
- **Small, Focused Classes**: Each class has a single responsibility
- **Clear Interfaces**: Well-defined input/output for each component
- **Reusable Components**: Generic base classes with specific implementations

## Running the Pipeline

```bash
# Run in development environment
python main.py --env dev

# Run in production environment
python main.py --env prod
```

## Conclusion

By applying these design patterns and best practices, we've built a modular, maintainable, and scalable data pipeline framework. This approach enables:

1. **Easy adaptation** to different data sources and destinations
2. **Consistent structure** across multiple pipelines
3. **Configuration flexibility** for different environments
4. **Code reusability** through component abstraction
5. **Clear separation** of concerns for better maintainability

These software engineering principles dramatically improve the quality of data engineering code, making it easier to develop, test, deploy, and maintain complex data pipelines.
