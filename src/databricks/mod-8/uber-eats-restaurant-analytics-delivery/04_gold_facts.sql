-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Gold Layer - Facts
-- MAGIC Star schema fact tables with denormalized dimensions

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Fact: Ratings

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE fact_ratings (
  CONSTRAINT fk_restaurant EXPECT (restaurant_id IS NOT NULL) ON VIOLATION FAIL
)
COMMENT "Rating facts with denormalized restaurant attributes"
TBLPROPERTIES ("quality" = "gold", "layer" = "fact")
AS SELECT
  r.rating_id,
  rest.restaurant_id,
  r.rating,
  rest.restaurant_name,
  rest.cuisine_type,
  rest.city,
  rest.country,
  r.rating_timestamp,
  r.ingestion_timestamp
FROM LIVE.silver_ratings r
INNER JOIN LIVE.silver_restaurants rest
  ON r.restaurant_identifier = rest.cnpj;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Fact: Inventory

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE fact_inventory (
  CONSTRAINT valid_inventory EXPECT (quantity_available >= 0) ON VIOLATION FAIL
)
COMMENT "Current inventory with product and restaurant details"
TBLPROPERTIES ("quality" = "gold", "layer" = "fact")
AS SELECT
  inv.stock_id,
  inv.restaurant_id,
  inv.product_id,
  inv.quantity_available,
  (inv.quantity_available * prod.price) AS inventory_value,
  rest.restaurant_name,
  rest.city,
  prod.product_name,
  prod.product_type,
  prod.price,
  inv.last_updated,
  inv.ingestion_timestamp
FROM LIVE.silver_inventory inv
INNER JOIN LIVE.silver_products prod
  ON inv.restaurant_id = prod.restaurant_id
  AND inv.product_id = prod.product_id
INNER JOIN LIVE.silver_restaurants rest
  ON inv.restaurant_id = rest.restaurant_id;
