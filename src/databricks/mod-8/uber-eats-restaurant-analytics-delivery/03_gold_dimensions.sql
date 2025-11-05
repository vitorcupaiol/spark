-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Gold Layer - Dimensions
-- MAGIC Star schema dimension tables (SCD Type 1)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Dimension: Restaurant

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE dim_restaurant (
  CONSTRAINT pk_restaurant EXPECT (restaurant_id IS NOT NULL) ON VIOLATION FAIL
)
COMMENT "Restaurant dimension - SCD Type 1"
TBLPROPERTIES ("quality" = "gold", "layer" = "dimension")
AS SELECT DISTINCT
  restaurant_id,
  name AS restaurant_name,
  cnpj,
  cuisine_type,
  country,
  city,
  address,
  phone_number,
  opening_time,
  closing_time,
  average_rating,
  num_reviews,
  current_timestamp() AS updated_at
FROM LIVE.silver_restaurants;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Dimension: Product

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE dim_product (
  CONSTRAINT pk_product EXPECT (product_id IS NOT NULL) ON VIOLATION FAIL
)
COMMENT "Product dimension - SCD Type 1"
TBLPROPERTIES ("quality" = "gold", "layer" = "dimension")
AS SELECT DISTINCT
  product_id,
  restaurant_id,
  name AS product_name,
  product_type,
  cuisine_type,
  price,
  unit_cost,
  profit_margin,
  calories,
  prep_time_min,
  is_vegetarian,
  is_gluten_free,
  current_timestamp() AS updated_at
FROM LIVE.silver_products;
