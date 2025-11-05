-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Silver Layer - Minimal Cleaning
-- MAGIC Simple quality checks and standardization

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Silver: Restaurants

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE silver_restaurants (
  CONSTRAINT valid_restaurant_id EXPECT (restaurant_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_name EXPECT (name IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "Cleaned restaurant data"
TBLPROPERTIES ("quality" = "silver")
AS SELECT
  restaurant_id,
  trim(name) AS name,
  upper(trim(cnpj)) AS cnpj,
  cuisine_type,
  country,
  city,
  address,
  phone_number,
  opening_time,
  closing_time,
  average_rating,
  num_reviews,
  ingestion_timestamp
FROM STREAM(LIVE.bronze_restaurants);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Silver: Ratings

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE silver_ratings (
  CONSTRAINT valid_rating_id EXPECT (rating_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_rating_range EXPECT (rating BETWEEN 0 AND 5) ON VIOLATION DROP ROW
)
COMMENT "Cleaned rating data"
TBLPROPERTIES ("quality" = "silver")
AS SELECT
  rating_id,
  upper(trim(restaurant_identifier)) AS restaurant_identifier,
  rating,
  timestamp AS rating_timestamp,
  ingestion_timestamp
FROM STREAM(LIVE.bronze_ratings);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Silver: Products

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE silver_products (
  CONSTRAINT valid_product_id EXPECT (product_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_price EXPECT (price > 0) ON VIOLATION DROP ROW
)
COMMENT "Cleaned product data"
TBLPROPERTIES ("quality" = "silver")
AS SELECT
  product_id,
  restaurant_id,
  trim(name) AS name,
  product_type,
  cuisine_type,
  price,
  unit_cost,
  (price - unit_cost) AS profit_margin,
  calories,
  prep_time_min,
  is_vegetarian,
  is_gluten_free,
  ingestion_timestamp
FROM STREAM(LIVE.bronze_products);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Silver: Inventory

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE silver_inventory (
  CONSTRAINT valid_stock_id EXPECT (stock_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_quantity EXPECT (quantity_available >= 0) ON VIOLATION DROP ROW
)
COMMENT "Cleaned inventory data"
TBLPROPERTIES ("quality" = "silver")
AS SELECT
  stock_id,
  restaurant_id,
  product_id,
  quantity_available,
  last_updated,
  ingestion_timestamp
FROM STREAM(LIVE.bronze_inventory);
