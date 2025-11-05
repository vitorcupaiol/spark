"""
Pipeline Template module

Implements the Template Method Pattern for data processing pipelines.
Defines the skeleton of the ETL process while allowing subclasses to
override specific steps.
"""

from abc import ABC, abstractmethod


class Pipeline(ABC):
    """
    Abstract base class for data pipelines

    Implements the Template Method Pattern by defining the standard
    pipeline flow while allowing subclasses to implement specific
    extraction, transformation, and loading logic.
    """

    def __init__(self, spark):
        """
        Initialize the pipeline

        Args:
            spark: SparkSession to use for this pipeline
        """
        self.spark = spark

    @abstractmethod
    def extract(self):
        """
        Extract data from source

        Returns:
            DataFrame: Data extracted from source
        """
        pass

    @abstractmethod
    def transform(self, data):
        """
        Transform the data

        Args:
            data: DataFrame to transform

        Returns:
            DataFrame: Transformed data
        """
        pass

    @abstractmethod
    def load(self, data):
        """
        Load data to destination

        Args:
            data: DataFrame to load
        """
        pass

    def run(self):
        """
        Run the complete pipeline

        This is the Template Method that defines the standard execution
        flow: extract -> transform -> load, with error handling.

        Returns:
            bool: True if pipeline completed successfully, False otherwise
        """
        print("Starting pipeline...")

        try:
            print("Extraction phase...")
            data = self.extract()
            print(f"Extracted {data.count()} records")

            print("Transformation phase...")
            transformed_data = self.transform(data)
            print(f"Transformed data has {transformed_data.count()} records")

            print("Loading phase...")
            self.load(transformed_data)

            print("Pipeline completed successfully!")
            return True

        except Exception as e:
            print(f"Error during pipeline execution: {str(e)}")
            return False
