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