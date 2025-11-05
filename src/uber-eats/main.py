"""
Main application module

Entry point for the UberEats orders pipeline. Demonstrates the integration
of multiple design patterns to create a maintainable data pipeline.

# TODO dev
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/jobs/uber-eats/main.py --env dev

# TODO prod
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/jobs/uber-eats/main.py --env prod
"""

import argparse
import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))

if current_dir not in sys.path:
    sys.path.append(current_dir)

from config.config import load_config
from core.session import SparkSessionFactory
from core.pipeline import Pipeline
from repo.orders import OrderRepository
from transform.order import EnrichOrderTransformer, CalculateETATransformer


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="UberEats Data Pipeline")

    parser.add_argument(
        "--env",
        type=str,
        default=os.environ.get("ENV", "dev"),
        choices=["dev", "prod"],
        help="Environment to run in (dev, prod)"
    )

    return parser.parse_args()


class UberEatsOrdersPipeline(Pipeline):
    """
    UberEats orders processing pipeline

    Implements the Template Method Pattern by extending the Pipeline base class
    and providing specific implementations for each phase.
    """

    def __init__(self, spark, config):
        """
        Initialize the UberEats orders pipeline

        Args:
            spark: SparkSession
            config (dict): Application configuration
        """
        super().__init__(spark)
        self.config = config

        self.repository = OrderRepository(
            spark,
            config["paths"]["input"],
            config["paths"]["output"]
        )

        self.enricher = EnrichOrderTransformer()
        self.eta_calculator = CalculateETATransformer()

    def extract(self):
        """
        Extract orders data

        Returns:
            DataFrame: Raw orders data
        """
        return self.repository.read()

    def transform(self, data):
        """
        Transform orders data

        Applies multiple transformation strategies sequentially.

        Args:
            data: Orders DataFrame

        Returns:
            DataFrame: Transformed orders data
        """
        enriched_data = self.enricher.transform(data)
        return self.eta_calculator.transform(enriched_data)

    def load(self, data):
        """
        Save processed orders data

        Args:
            data: Processed orders DataFrame
        """
        self.repository.write(
            data=data,
            partition_by=["order_date"]
        )


def main():
    """
    Main function

    Creates and runs the UberEats orders pipeline.
    """
    args = parse_args()
    print(f"Using environment: {args.env}")

    config = load_config(args.env)
    print(f"Configuration loaded successfully")
    print(f"App name: {config['app']['name']}")
    print(f"Input path: {config['paths']['input']}")
    print(f"Output path: {config['paths']['output']}")

    spark = SparkSessionFactory.create_session(
        config["app"]["name"],
        config
    )

    pipeline = UberEatsOrdersPipeline(spark, config)
    success = pipeline.run()

    print(f"Execution status: {'Success' if success else 'Failure'}")

    spark.stop()


if __name__ == "__main__":
    main()
