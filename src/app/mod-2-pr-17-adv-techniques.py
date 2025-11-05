"""
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  /opt/bitnami/spark/jobs/app/mod-2-pr-17-adv-techniques.py
"""

import math
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, DoubleType

spark = SparkSession.builder \
    .getOrCreate()

# TODO set config
spark.sparkContext.setLogLevel("ERROR")
spark.sql("SET spark.sql.echo=true")

spark.sql("""
CREATE OR REPLACE TEMPORARY VIEW restaurants
USING json
OPTIONS (path "./storage/mysql/restaurants/01JS4W5A7YWTYRQKDA7F7N95VY.jsonl")
""")

spark.sql("""
CREATE OR REPLACE TEMPORARY VIEW drivers
USING json
OPTIONS (path "./storage/postgres/drivers/01JS4W5A74BK7P4BPTJV1D3MHA.jsonl")
""")

spark.sql("""
CREATE OR REPLACE TEMPORARY VIEW orders
USING json
OPTIONS (path "./storage/kafka/orders/01JS4W5A7XY65S9Z69BY51BEJ4.jsonl")
""")

spark.sql("""
CREATE OR REPLACE TEMPORARY VIEW driver_shifts AS
SELECT 
    driver_id, 
    'Day' as shift_type,
    CURRENT_DATE() - CAST(RAND()*30 AS INT) as start_time,
    RAND()*500 as earnings_brl,
    RAND()*300 as shift_duration_min,
    RAND()*100 as distance_covered_km
FROM drivers
""")

# TODO 1. window functions
spark.sql("""
SELECT 
    driver_id,
    TO_DATE(start_time) as shift_date,
    earnings_brl,
    ROUND(AVG(earnings_brl) OVER (
        PARTITION BY driver_id 
        ORDER BY TO_DATE(start_time) 
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ), 2) as earnings_3day_avg,
    ROUND(SUM(earnings_brl) OVER (
        PARTITION BY driver_id 
        ORDER BY TO_DATE(start_time)
    ), 2) as running_total,
    ROUND(earnings_brl - LAG(earnings_brl, 1, 0) OVER (
        PARTITION BY driver_id ORDER BY TO_DATE(start_time)
    ), 2) as earnings_change
FROM driver_shifts
ORDER BY driver_id, shift_date
LIMIT 10
""").show()

# TODO 2. user-defined functions
spark.udf.register(
    "rating_category",
    lambda r: "Excellent" if r >= 4.5 else
              "Very Good" if r >= 4.0 else
              "Good" if r >= 3.5 else
              "Average" if r >= 3.0 else
              "Below Average",
    StringType()
)

spark.sql("""
SELECT 
    name,
    cuisine_type,
    average_rating,
    rating_category(average_rating) as category
FROM restaurants
ORDER BY average_rating DESC
LIMIT 10
""").show()


# TODO haversine distance calculation {reat-circle distance between two points on the surface of a sphere}
def haversine_distance(lat1, lon1, lat2, lon2):
    """Calculate distance between two points in kilometers"""
    lat1_rad = math.radians(float(lat1))
    lon1_rad = math.radians(float(lon1))
    lat2_rad = math.radians(float(lat2))
    lon2_rad = math.radians(float(lon2))

    dlon = lon2_rad - lon1_rad
    dlat = lat2_rad - lat1_rad
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2) ** 2
    c = 2 * math.asin(math.sqrt(a))
    r = 6371

    return c * r


spark.udf.register("haversine", haversine_distance, DoubleType())

# TODO popularity score calculation
spark.udf.register(
    "popularity_score",
    lambda rating, reviews: float(rating) * math.sqrt(float(reviews)/1000) if rating and reviews else 0,
    DoubleType()
)

spark.sql("""
SELECT 
    name,
    cuisine_type,
    average_rating,
    num_reviews,
    popularity_score(average_rating, num_reviews) as score
FROM restaurants
ORDER BY score DESC
LIMIT 10
""").show()

# TODO 3. advanced analytics functions

# TODO percentile analysis
spark.sql("""
SELECT 
    cuisine_type,
    COUNT(*) as restaurant_count,
    ROUND(AVG(average_rating), 2) as mean_rating,
    PERCENTILE(average_rating, 0.25) as p25_rating,
    PERCENTILE(average_rating, 0.5) as median_rating,
    PERCENTILE(average_rating, 0.75) as p75_rating,
    MIN(average_rating) as min_rating,
    MAX(average_rating) as max_rating
FROM restaurants
GROUP BY cuisine_type
HAVING COUNT(*) > 5
ORDER BY restaurant_count DESC
LIMIT 10
""").show()

# TODO distribution analysis with CUME_DIST and PERCENT_RANK
spark.sql("""
SELECT 
    ROUND(average_rating, 1) as rating_rounded,
    COUNT(*) as restaurant_count,
    ROUND(100 * CUME_DIST() OVER (ORDER BY ROUND(average_rating, 1)), 2) as cumulative_pct,
    ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as pct_of_total
FROM restaurants
GROUP BY ROUND(average_rating, 1)
ORDER BY rating_rounded
""").show()


# TODO outlier detection using standard deviation
spark.sql("""
WITH cuisine_stats AS (
    SELECT 
        cuisine_type,
        AVG(average_rating) as mean_rating,
        STDDEV(average_rating) as stddev_rating
    FROM restaurants
    GROUP BY cuisine_type
    HAVING COUNT(*) > 2
)
SELECT 
    r.name,
    r.cuisine_type,
    r.average_rating,
    ROUND(cs.mean_rating, 2) as cuisine_avg,
    ROUND((r.average_rating - cs.mean_rating) / cs.stddev_rating, 2) as z_score
FROM restaurants r
JOIN cuisine_stats cs ON r.cuisine_type = cs.cuisine_type
WHERE ABS((r.average_rating - cs.mean_rating) / cs.stddev_rating) > 2
ORDER BY ABS(z_score) DESC
LIMIT 10
""").show()

# TODO 4. pivot & unpivot operations
spark.sql("""
SELECT * FROM (
    SELECT city, cuisine_type 
    FROM restaurants
) PIVOT (
    COUNT(*) as count
    FOR cuisine_type IN (
        'Italian', 'Chinese', 'Japanese', 'Mexican', 'Indian'
    )
)
ORDER BY city
LIMIT 10
""").show()

spark.sql("""
SELECT * FROM (
    SELECT 
        cuisine_type,
        CASE 
            WHEN average_rating >= 4.5 THEN 'Excellent (4.5+)'
            WHEN average_rating >= 4.0 THEN 'Very Good (4.0-4.4)'
            WHEN average_rating >= 3.5 THEN 'Good (3.5-3.9)'
            WHEN average_rating >= 3.0 THEN 'Average (3.0-3.4)'
            ELSE 'Below Average (<3.0)'
        END as rating_category
    FROM restaurants
    WHERE cuisine_type IN ('Italian', 'Chinese', 'Japanese', 'Mexican', 'Indian')
) PIVOT (
    COUNT(*) as count
    FOR rating_category IN (
        'Excellent (4.5+)', 
        'Very Good (4.0-4.4)', 
        'Good (3.5-3.9)', 
        'Average (3.0-3.4)', 
        'Below Average (<3.0)'
    )
)
ORDER BY cuisine_type
""").show()

spark.stop()
