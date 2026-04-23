# Databricks notebook source
# # IMPORTS + CONFIG LOAD + ENV ROUTER
# import yaml
# from pyspark.sql.functions import *
# import dlt

# # -------------------------
# # LOAD CONFIG
# # -------------------------
# def load_config(path="/Workspace/maven_market/Mysore-Pak/config/config.yml"):
#     with open(path, "r") as f:
#         return yaml.safe_load(f)

# config = load_config()
# catalog = config["catalog"]

# # -------------------------
# # BRONZE: STREAMING INGESTION
# # -------------------------

# @dlt.table(
#     name=f"{catalog}.{config['bronze_schema']}.{config['bronze_customers']}",
#     comment="Streaming bronze ingestion for Mongo Customers via Fivetran"
# )
# def bronze_customers():
#     # Use readStream to ensure this becomes a Streaming Table
#     return (
#         spark.readStream.table("maven_market_uc.bronze.bronze_raw_customers")
#         .withColumn("ingestion_timestamp", current_timestamp())
#         .withColumn("_source_system", lit("fivetran"))
#     )

# @dlt.table(
#     name=f"{catalog}.{config['bronze_schema']}.{config['bronze_products']}",
#     comment="Streaming bronze ingestion for Mongo Products via Fivetran"
# )
# def bronze_products():
#     # Use readStream to ensure this becomes a Streaming Table
#     return (
#         spark.readStream.table("maven_market_uc.bronze.bronze_raw_products")
#         .withColumn("ingestion_timestamp", current_timestamp())
#         .withColumn("_source_system", lit("fivetran"))
#     )

# COMMAND ----------

# MAGIC %md
# MAGIC ##DLT Bronze Layer

# COMMAND ----------

# MAGIC %md
# MAGIC #IMPORTS + CONFIG LOAD

# COMMAND ----------

# IMPORTS + CONFIG LOAD + ENV ROUTER
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
catalog = config["catalog"]

# COMMAND ----------

# DBTITLE 1,Pipeline Logger
# -------------------------
# PIPELINE LOGGER
# -------------------------
import sys
sys.path.insert(0, "/Workspace/maven_market/Mysore-Pak/src")
from utils.custom_logger import PipelineLogger

logger = PipelineLogger(spark, config, buffer_size=3, echo=True)
logger.log_info("Bronze Mongo Pipeline — registering tables: customers, products", layer="bronze")

# COMMAND ----------

# MAGIC %skip
# MAGIC import yaml
# MAGIC from pyspark.sql.functions import *
# MAGIC import dlt
# MAGIC
# MAGIC # -------------------------
# MAGIC # LOAD CONFIG
# MAGIC # -------------------------
# MAGIC def load_config(path="/Workspace/maven_market/Mysore-Pak/config/config.yml"):
# MAGIC     with open(path, "r") as f:
# MAGIC         return yaml.safe_load(f)
# MAGIC
# MAGIC config = load_config()
# MAGIC '''
# MAGIC # -------------------------
# MAGIC # ENV RESOLUTION
# MAGIC # -------------------------
# MAGIC def get_env():
# MAGIC     try:
# MAGIC         return spark.conf.get("env")
# MAGIC     except:
# MAGIC         return config.get("env", "dev")
# MAGIC
# MAGIC env = get_env()
# MAGIC '''
# MAGIC # -------------------------
# MAGIC # CATALOG RESOLUTION
# MAGIC # -------------------------
# MAGIC #catalog = config[f"catalog_{env}"]
# MAGIC catalog = config["catalog"]
# MAGIC # -------------------------
# MAGIC # TABLE BUILDER
# MAGIC # -------------------------
# MAGIC def build_table_name(schema_key, table_key):
# MAGIC     return f"{catalog}.{config[schema_key]}_{'' if schema_key.endswith('_schema') else ''}{config[schema_key]}.{config[table_key]}"

# COMMAND ----------

# MAGIC %md
# MAGIC ##CUSTOMERS

# COMMAND ----------



@dlt.table(
    name=f"{catalog}.{config['bronze_schema']}.{config['bronze_customers']}",
    comment="Streaming bronze ingestion for Mongo Customers via Fivetran"
)
def bronze_customers():
    # Use readStream to ensure this becomes a Streaming Table
    return (
        spark.readStream.table("maven_market_uc.bronze.bronze_raw_customers")
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("_source_system", lit("fivetran"))
    )

# COMMAND ----------

# MAGIC %skip
# MAGIC @dlt.table(
# MAGIC     name=f"{catalog}.{config['bronze_schema']}.{config['bronze_customers']}"
# MAGIC )
# MAGIC def bronze_customers():
# MAGIC
# MAGIC     return (
# MAGIC         spark.read.table("maven_market_uc.bronze.bronze_raw_customers")
# MAGIC         .withColumn("ingestion_timestamp", current_timestamp())
# MAGIC         .withColumn("_source_system", lit("fivetran"))
# MAGIC     )

# COMMAND ----------

# MAGIC %md
# MAGIC ##PRODUCTS

# COMMAND ----------

@dlt.table(
    name=f"{catalog}.{config['bronze_schema']}.{config['bronze_products']}",
    comment="Streaming bronze ingestion for Mongo Products via Fivetran"
)
def bronze_products():
    # Use readStream to ensure this becomes a Streaming Table
    return (
        spark.readStream.table("maven_market_uc.bronze.bronze_raw_products")
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("_source_system", lit("fivetran"))
    )

# COMMAND ----------

# MAGIC %skip
# MAGIC @dlt.table(
# MAGIC     name=f"{catalog}.{config['bronze_schema']}.{config['bronze_products']}"
# MAGIC )
# MAGIC def bronze_products():
# MAGIC
# MAGIC     return (
# MAGIC         spark.read.table("maven_market_uc.bronze.bronze_raw_products")
# MAGIC         .withColumn("ingestion_timestamp", current_timestamp())
# MAGIC         .withColumn("_source_system", lit("fivetran"))
# MAGIC     )

# COMMAND ----------

# MAGIC %md
# MAGIC ##
