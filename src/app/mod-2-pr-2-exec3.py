from spark_manager import SparkManager

# Get the singleton session
spark = SparkManager.get_session()

# Use it for data processing
df_restaurants = spark.read.json("./storage/mysql/restaurants/01JS4W5A7YWTYRQKDA7F7N95VY.jsonl")
df_ratings = spark.read.json("./storage/mysql/ratings/01JS4W5A7YWTYRQKDA7F7N95VZ.jsonl")

df_restaurants.createOrReplaceTempView("restaurants")
df_ratings.createOrReplaceTempView("ratings")

spark.sql("""
    SELECT r.name AS restaurant_name, AVG(rt.rating) AS average_rating
    FROM restaurants r
    JOIN ratings rt ON r.restaurant_id = rt.rating_id
    GROUP BY r.name
    ORDER BY average_rating DESC
""").show()

# Stop when done
SparkManager.stop_session()