-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Bronze Layer - Raw Ingestion
-- MAGIC All 4 source tables using Auto Loader

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Bronze: Restaurants

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE bronze_restaurants
COMMENT "Raw restaurant data from MySQL"
TBLPROPERTIES ("quality" = "bronze")
AS SELECT
  *,
  current_timestamp() AS ingestion_timestamp,
  _metadata.file_path AS source_file
FROM cloud_files(
  "abfss://owshq-shadow-traffic@owshqblobstg.dfs.core.windows.net/mysql/restaurants/",
  "json",
  map("cloudFiles.inferColumnTypes", "true")
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Bronze: Ratings

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE bronze_ratings
COMMENT "Raw rating data from MySQL"
TBLPROPERTIES ("quality" = "bronze")
AS SELECT
  *,
  current_timestamp() AS ingestion_timestamp,
  _metadata.file_path AS source_file
FROM cloud_files(
  "abfss://owshq-shadow-traffic@owshqblobstg.dfs.core.windows.net/mysql/ratings/",
  "json",
  map("cloudFiles.inferColumnTypes", "true")
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Bronze: Products

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE bronze_products
COMMENT "Raw product data from MySQL"
TBLPROPERTIES ("quality" = "bronze")
AS SELECT
  *,
  current_timestamp() AS ingestion_timestamp,
  _metadata.file_path AS source_file
FROM cloud_files(
  "abfss://owshq-shadow-traffic@owshqblobstg.dfs.core.windows.net/mysql/products/",
  "json",
  map("cloudFiles.inferColumnTypes", "true")
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Bronze: Inventory

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE bronze_inventory
COMMENT "Raw inventory data from PostgreSQL"
TBLPROPERTIES ("quality" = "bronze")
AS SELECT
  *,
  current_timestamp() AS ingestion_timestamp,
  _metadata.file_path AS source_file
FROM cloud_files(
  "abfss://owshq-shadow-traffic@owshqblobstg.dfs.core.windows.net/postgres/inventory/",
  "json",
  map("cloudFiles.inferColumnTypes", "true")
);
