"""
Orders repository module

Implements the Repository Pattern for order data access.
Handles reading from and writing to various data sources.
"""

from repo.base import Repository


class OrderRepository(Repository):
    """
    Repository for orders data

    Implements specific data access logic for order data,
    abstracting the details of the underlying storage.
    """

    def __init__(self, spark, source_path, output_path):
        """
        Initialize the orders repository

        Args:
            spark: SparkSession to use for data access
            source_path (str): Path to source data
            output_path (str): Path for output data
        """
        super().__init__(spark)
        self.source_path = source_path
        self.output_path = output_path

    def read(self, format="json", **kwargs):
        """
        Read orders data

        Args:
            format (str): Format of the source data
            **kwargs: Additional read options

        Returns:
            DataFrame: Orders data
        """
        print(f"Reading orders data from {self.source_path}")
        return self.spark.read.format(format).load(self.source_path)

    def write(self, data, format="parquet", mode="overwrite", **kwargs):
        """
        Write processed orders data

        Args:
            data: DataFrame to write
            format (str): Output format
            mode (str): Write mode (overwrite, append, etc.)
            **kwargs: Additional write options
        """
        partition_cols = kwargs.get("partition_by", None)

        writer = data.write.format(format).mode(mode)

        if partition_cols:
            writer = writer.partitionBy(partition_cols)

        print(f"Writing processed orders to {self.output_path}")
        writer.save(self.output_path)
