# Databricks notebook source
# MAGIC %md
# MAGIC ##SECURE SALES (RLS + CLS)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW maven_market_uc.gold.v_secure_fact_sales AS
# MAGIC SELECT
# MAGIC     transaction_date,
# MAGIC     f.store_id,
# MAGIC     product_id,
# MAGIC
# MAGIC     -- region from dim_store
# MAGIC     s.sales_region,
# MAGIC
# MAGIC     quantity,
# MAGIC
# MAGIC     -- CLS (mask financials)
# MAGIC     CASE 
# MAGIC         WHEN is_member('admins') OR is_member('data_engineers')
# MAGIC         THEN total_revenue ELSE NULL END AS total_revenue,
# MAGIC
# MAGIC     CASE 
# MAGIC         WHEN is_member('admins') OR is_member('data_engineers')
# MAGIC         THEN total_cost ELSE NULL END AS total_cost,
# MAGIC
# MAGIC     CASE 
# MAGIC         WHEN is_member('admins') OR is_member('data_engineers')
# MAGIC         THEN total_profit ELSE NULL END AS total_profit
# MAGIC
# MAGIC FROM maven_market_uc.gold.fact_sales f
# MAGIC LEFT JOIN maven_market_uc.gold.dim_store s
# MAGIC ON f.store_id = s.store_id
# MAGIC
# MAGIC WHERE
# MAGIC     is_member('admins')
# MAGIC     OR is_member('data_engineers')
# MAGIC     OR is_member('data_analysts')
# MAGIC     OR is_member('business_users')
# MAGIC     OR (is_member('central_managers') AND s.sales_region = 'Central')
# MAGIC     OR (is_member('other_region_managers') AND s.sales_region != 'Central')

# COMMAND ----------

# MAGIC %md
# MAGIC ##SECURE RETURNS (RLS)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW maven_market_uc.gold.v_secure_fact_returns AS
# MAGIC SELECT
# MAGIC     r.*,
# MAGIC     s.sales_region
# MAGIC FROM maven_market_uc.gold.fact_returns r
# MAGIC LEFT JOIN maven_market_uc.gold.dim_store s
# MAGIC ON r.store_id = s.store_id
# MAGIC
# MAGIC WHERE
# MAGIC     is_member('admins')
# MAGIC     OR is_member('data_engineers')
# MAGIC     OR is_member('data_analysts')
# MAGIC     OR is_member('business_users')
# MAGIC     OR (is_member('central_managers') AND s.sales_region = 'Central')
# MAGIC     OR (is_member('other_region_managers') AND s.sales_region != 'Central')

# COMMAND ----------

# MAGIC %md
# MAGIC ##SECURE CUSTOMER (CLS)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW maven_market_uc.gold.v_secure_dim_customer AS
# MAGIC SELECT
# MAGIC     customer_id,
# MAGIC
# MAGIC     -- mask name
# MAGIC     CASE 
# MAGIC         WHEN is_member('admins') THEN customer_name
# MAGIC         ELSE 'REDACTED'
# MAGIC     END AS customer_name,
# MAGIC
# MAGIC     customer_city,
# MAGIC     customer_state_province,
# MAGIC     customer_country,
# MAGIC
# MAGIC     gender,
# MAGIC     marital_status,
# MAGIC
# MAGIC     -- mask income
# MAGIC     CASE 
# MAGIC         WHEN is_member('admins') OR is_member('data_engineers')
# MAGIC         THEN yearly_income
# MAGIC         ELSE 'RESTRICTED'
# MAGIC     END AS yearly_income,
# MAGIC
# MAGIC     education,
# MAGIC     occupation,
# MAGIC     homeowner
# MAGIC
# MAGIC FROM maven_market_uc.gold.dim_customer

# COMMAND ----------

# MAGIC %md
# MAGIC ##SECURE PRODUCT (CLS)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW maven_market_uc.gold.v_secure_dim_product AS
# MAGIC SELECT
# MAGIC     product_id,
# MAGIC     product_name,
# MAGIC     product_brand,
# MAGIC     product_retail_price,
# MAGIC
# MAGIC     CASE 
# MAGIC         WHEN is_member('admins') OR is_member('data_engineers')
# MAGIC         THEN product_cost
# MAGIC         ELSE NULL
# MAGIC     END AS product_cost,
# MAGIC
# MAGIC     product_weight,
# MAGIC     recyclable,
# MAGIC     low_fat
# MAGIC
# MAGIC FROM maven_market_uc.gold.dim_product

# COMMAND ----------

# MAGIC %md
# MAGIC ##AGG TABLES (EXECUTIVE SAFE)

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT SELECT ON maven_market_uc.gold.agg_daily_sales TO `executives`;
# MAGIC GRANT SELECT ON maven_market_uc.gold.agg_monthly_sales TO `executives`;
# MAGIC GRANT SELECT ON maven_market_uc.gold.agg_store_performance TO `executives`;
# MAGIC GRANT SELECT ON maven_market_uc.gold.agg_return_kpi TO `executives`;

# COMMAND ----------

# MAGIC %md
# MAGIC ##

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   is_member('admins') AS account_check,
# MAGIC   is_member('admins') AS workspace_check
