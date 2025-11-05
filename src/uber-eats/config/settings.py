"""
Configuration settings module

Defines application settings for different environments.
"""

DEFAULT_CONFIG = {
    "app": {
        "name": "uber-eats-orders-pipeline",
        "log_level": "INFO"
    },
    "spark": {
        "log_level": "WARN",
        "shuffle_partitions": 10,
        "adaptive_execution": True
    },
    "paths": {
        "input": "/opt/bitnami/spark/jobs/uber-eats/data/orders.json",
        "output": "/opt/bitnami/spark/jobs/uber-eats/output/orders"
    }
}

DEV_CONFIG = {
    **DEFAULT_CONFIG,
    "spark": {
        **DEFAULT_CONFIG["spark"],
        "master": "local[*]",
        "driver_memory": "1g",
        "executor_memory": "1g"
    }
}

PROD_CONFIG = {
    **DEFAULT_CONFIG,
    "spark": {
        **DEFAULT_CONFIG["spark"],
        "master": "spark://spark-master:7077",
        "driver_memory": "1g",
        "executor_memory": "2g",
        "executor_instances": 2,
        "shuffle_partitions": 50
    },
    "paths": {
        "input": "/opt/bitnami/spark/jobs/uber-eats/data/orders.json",
        "output": "/opt/bitnami/spark/jobs/uber-eats/output/orders"
    }
}

CONFIGS = {
    "dev": DEV_CONFIG,
    "prod": PROD_CONFIG
}
