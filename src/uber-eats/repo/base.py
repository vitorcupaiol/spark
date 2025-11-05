"""
Repository base module

Implements the Repository Pattern base classes for data access.
Abstracts the details of data sources and destinations from the business logic.
"""

from abc import ABC, abstractmethod


class Repository(ABC):
    """
    Abstract base class for repositories

    Implements the Repository Pattern by defining standard interfaces
    for reading and writing data, regardless of the underlying storage.
    """

    def __init__(self, spark):
        """
        Initialize the repository

        Args:
            spark: SparkSession to use for data access
        """
        self.spark = spark

    @abstractmethod
    def read(self, **kwargs):
        """
        Read data from source

        Args:
            **kwargs: Additional parameters for reading

        Returns:
            DataFrame: Data read from source
        """
        pass

    @abstractmethod
    def write(self, data, **kwargs):
        """
        Write data to destination

        Args:
            data: DataFrame to write
            **kwargs: Additional parameters for writing
        """
        pass
