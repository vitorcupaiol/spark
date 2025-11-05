"""
Order transformation module

Implements specific transformation strategies for order data.
Each class represents a different transformation algorithm.
"""

from pyspark.sql.functions import col, when, lit
from transform.base import Transformer


class EnrichOrderTransformer(Transformer):
    """
    Transformer that enriches order data

    Adds value categorization and fee calculations to order data.
    """

    def transform(self, data):
        """
        Add value categories and calculate fees

        Args:
            data: Orders DataFrame

        Returns:
            DataFrame: Enriched orders with categories and fees
        """
        print("Applying transformation: EnrichOrderTransformer")

        return data.withColumn(
            "order_value_category",
            when(col("total_amount") > 50, "High")
            .when(col("total_amount") > 25, "Medium")
            .otherwise("Low")
        ).withColumn(
            "delivery_fee",
            col("total_amount") * 0.1
        )


class CalculateETATransformer(Transformer):
    """
    Transformer that calculates delivery time estimates

    Adds estimated delivery time based on distance and other factors.
    """

    def transform(self, data):
        """
        Calculate and add estimated delivery time

        Args:
            data: Orders DataFrame

        Returns:
            DataFrame: Orders with estimated delivery times
        """
        print("Applying transformation: CalculateETATransformer")

        return data.withColumn(
            "estimated_delivery_minutes",
            col("distance_km") * 2 + 15
        )


class CustomerSegmentationTransformer(Transformer):
    """
    Transformer that segments customers based on order behavior

    Adds customer segmentation based on order amount and frequency.
    """

    def transform(self, data):
        """
        Add customer segmentation

        Args:
            data: Orders DataFrame

        Returns:
            DataFrame: Orders with customer segmentation
        """
        print("Applying transformation: CustomerSegmentationTransformer")

        return data.withColumn(
            "customer_segment",
            when(col("total_amount") > 75, "Premium")
            .when((col("total_amount") > 40) & (col("items_count") >= 4), "High Value")
            .when(col("total_amount") > 25, "Regular")
            .otherwise("Basic")
        )
