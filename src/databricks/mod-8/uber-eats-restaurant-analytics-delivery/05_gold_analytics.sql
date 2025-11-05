-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Gold Layer - Analytics
-- MAGIC Pre-aggregated metrics for dashboards

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Analytics: Restaurant Performance

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE gold_restaurant_summary
COMMENT "Restaurant performance metrics"
TBLPROPERTIES ("quality" = "gold", "layer" = "analytics")
AS SELECT
  restaurant_id,
  restaurant_name,
  cuisine_type,
  city,
  country,
  COUNT(rating_id) AS total_ratings,
  ROUND(AVG(rating), 2) AS avg_rating,
  SUM(CASE WHEN rating >= 4 THEN 1 ELSE 0 END) AS high_ratings_count,
  SUM(CASE WHEN rating <= 2 THEN 1 ELSE 0 END) AS low_ratings_count,
  ROUND(SUM(CASE WHEN rating >= 4 THEN 1 ELSE 0 END) * 100.0 / COUNT(rating_id), 2) AS high_ratings_pct,
  ROUND(SUM(CASE WHEN rating <= 2 THEN 1 ELSE 0 END) * 100.0 / COUNT(rating_id), 2) AS low_ratings_pct
FROM LIVE.fact_ratings
GROUP BY restaurant_id, restaurant_name, cuisine_type, city, country;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Analytics: Product Inventory

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE gold_product_summary
COMMENT "Product inventory and performance metrics"
TBLPROPERTIES ("quality" = "gold", "layer" = "analytics")
AS SELECT
  product_id,
  product_name,
  product_type,
  COUNT(DISTINCT restaurant_id) AS restaurants_offering,
  SUM(quantity_available) AS total_quantity,
  ROUND(SUM(inventory_value), 2) AS total_inventory_value,
  ROUND(AVG(quantity_available), 2) AS avg_quantity_per_restaurant
FROM LIVE.fact_inventory
GROUP BY product_id, product_name, product_type;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Analytics: Cuisine Performance

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE gold_cuisine_summary
COMMENT "Performance metrics by cuisine type"
TBLPROPERTIES ("quality" = "gold", "layer" = "analytics")
AS SELECT
  cuisine_type,
  COUNT(DISTINCT restaurant_id) AS restaurant_count,
  COUNT(rating_id) AS total_ratings,
  ROUND(AVG(rating), 2) AS avg_rating,
  ROUND(SUM(CASE WHEN rating >= 4 THEN 1 ELSE 0 END) * 100.0 / COUNT(rating_id), 2) AS satisfaction_rate
FROM LIVE.fact_ratings
GROUP BY cuisine_type
HAVING COUNT(rating_id) >= 10
ORDER BY avg_rating DESC;
