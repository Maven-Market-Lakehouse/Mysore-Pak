# Databricks notebook source
# MAGIC %md
# MAGIC #IMPORTS + CONFIG LOAD + ENV ROUTER

# COMMAND ----------

import yaml
from pyspark.sql.functions import *
import dlt

# -------------------------
# LOAD CONFIG
# -------------------------
def load_config(path="/Workspace/maven_market/Mysore-Pak/config/config.yml"):
    with open(path, "r") as f:
        return yaml.safe_load(f)

config = load_config()
'''
# -------------------------
# ENV RESOLUTION
# -------------------------
def get_env():
    try:
        return spark.conf.get("env")
    except:
        return config.get("env", "dev")

env = get_env()
'''
# -------------------------
# CATALOG RESOLUTION
# -------------------------
#catalog = config[f"catalog_{env}"]
catalog = config["catalog"]
# -------------------------
# TABLE BUILDER
# -------------------------
def build_table_name(schema_key, table_key):
    return f"{catalog}.{config[schema_key]}_{'' if schema_key.endswith('_schema') else ''}{config[schema_key]}.{config[table_key]}"

# COMMAND ----------

# MAGIC %md
# MAGIC ##CUSTOMERS

# COMMAND ----------

@dlt.table(
    name=f"{catalog}.{config['bronze_schema']}.{config['bronze_customers']}"
)
def bronze_customers():

    return (
        spark.read.table("maven_market_uc.bronze.bronze_raw_customers")
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("_source_system", lit("fivetran"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ##PRODUCTS

# COMMAND ----------

@dlt.table(
    name=f"{catalog}.{config['bronze_schema']}.{config['bronze_products']}"
)
def bronze_products():

    return (
        spark.read.table("maven_market_uc.bronze.bronze_raw_products")
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("_source_system", lit("fivetran"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ##
