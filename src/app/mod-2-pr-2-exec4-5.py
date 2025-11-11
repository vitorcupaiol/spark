# analysis_job.py
import os
import logging
from spark_factory import SparkFactory

# Set up logging
logging.basicConfig(level=logging.INFO)

# Set up logging
logging.basicConfig(level=logging.INFO)

def analyze_restaurants(spark):
    """Analyze restaurant data from UberEats."""
    # Load data from storage directory
    df_restaurants = spark.read.json("./storage/mysql/restaurants/01JS4W5A7YWTYRQKDA7F7N95VY.jsonl")
    
    # Register as temp view for SQL
    df_restaurants.createOrReplaceTempView("restaurants")
    
    # Perform analysis
    result = spark.sql("""
        SELECT count(*)
        FROM restaurants r
    """)
    
    return result

def analyze_users(spark):
    """Analyze restaurant data from UberEats."""
    # Load data from storage directory
    df_users = spark.read.json("./storage/mongodb/users/01JS4W5A7WWZBQ6Y1C465EYR76.jsonl")
    
    # Register as temp view for SQL
    df_users.createOrReplaceTempView("users")
    
    # Perform analysis
    result = spark.sql("""
        SELECT count(*)
        FROM users r
    """)
    
    return result

def analyze_ratings(spark):
    """Analyze restaurant data from UberEats."""
    # Load data from storage directory
    df_ratings = spark.read.json("./storage/mysql/ratings/01JS4W5A7YWTYRQKDA7F7N95VZ.jsonl")
    
    # Register as temp view for SQL
    df_ratings.createOrReplaceTempView("ratings")
    
    # Perform analysis
    result = spark.sql("""
        SELECT count(*)
        FROM ratings r
    """)
    
    return result

if __name__ == "__main__":
    # Set environment
    os.environ["SPARK_ENV"] = "dev"  # "test" or "prod" in other environments
    
    # Show results
    # Run the analysis
    result = SparkFactory.run_job(analyze_restaurants, "RestaurantAnalysis","config/spark_config.yaml")
    print("===== Qtde Restaurantes =====")
    result.show() 

    result = SparkFactory.run_job(analyze_users, "UsersAnalysis","config/spark_config.yaml")    
    print("===== Qtde Users =====")
    result.show() 
    
    result = SparkFactory.run_job(analyze_ratings, "RatingAnalysis","config/config/spark_config.yaml")
    print("===== Qtde Ratings =====")
    result.show() 


    # Clean up
    SparkFactory.stop_session()