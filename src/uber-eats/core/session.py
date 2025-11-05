"""
SparkSession Factory module

Implements the Factory Pattern for creating and configuring SparkSession objects.
This centralizes session creation logic and ensures consistent configuration.
"""

from pyspark.sql import SparkSession


class SparkSessionFactory:
    """
    Factory for creating SparkSession instances

    This factory centralizes the creation of SparkSession objects, ensuring
    consistent configuration across the application.
    """

    @staticmethod
    def create_session(app_name, config=None):
        """
        Create a configured SparkSession

        Args:
            app_name (str): Name of the application
            config (dict): Spark configuration parameters

        Returns:
            SparkSession: Configured Spark session
        """
        builder = SparkSession.builder.appName(app_name)

        if config and 'spark' in config:
            spark_config = config['spark']

            for key, value in spark_config.items():
                if key == "master":
                    builder = builder.master(value)
                else:
                    if isinstance(value, bool):
                        value = str(value).lower()
                    builder = builder.config(f"spark.{key}", value)

        return builder.getOrCreate()
