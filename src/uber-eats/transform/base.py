"""
Transformer base module

Implements the Strategy Pattern for data transformations.
Defines the interface for interchangeable transformation algorithms.
"""

from abc import ABC, abstractmethod


class Transformer(ABC):
    """
    Abstract base class for data transformers

    Implements the Strategy Pattern by defining a common interface
    for different transformation algorithms that can be selected
    and swapped at runtime.
    """

    @abstractmethod
    def transform(self, data):
        """
        Transform input data

        Args:
            data: DataFrame to transform

        Returns:
            DataFrame: Transformed data
        """
        pass
