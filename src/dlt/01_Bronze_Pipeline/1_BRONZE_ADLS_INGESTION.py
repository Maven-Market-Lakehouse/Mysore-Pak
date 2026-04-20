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

# -------------------------
# CATALOG RESOLUTION
# -------------------------
catalog = config[f"catalog_{env}"]
'''
catalog = config["catalog"]
# -------------------------
# TABLE BUILDER
# -------------------------
def build_table_name(schema_key, table_key):
    return f"{catalog}.{config[schema_key]}_{'' if schema_key.endswith('_schema') else ''}{config[schema_key]}.{config[table_key]}"

# COMMAND ----------

# MAGIC %md
# MAGIC ##PATH & CHECKPOINT RESOLVERS

# COMMAND ----------

def build_path(relative_path_key):
    return config["base_path"] + config[relative_path_key]

def build_checkpoint(checkpoint_key):
    return config["base_path"] + config[checkpoint_key]

# COMMAND ----------

# MAGIC %md
# MAGIC ##BRONZE: TRANSACTIONS

# COMMAND ----------

@dlt.table(
    name=f"{catalog}.{config['bronze_schema']}.{config['bronze_transactions']}"
)
def bronze_transactions():

    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("header", "true")
        .load(build_path("transactions_path"))
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_file_path", col("_metadata.file_path"))
        .withColumn("source_file_name", regexp_extract(col("_metadata.file_path"), r"([^/]+$)", 1))
        .withColumn("_source_system", lit("adls"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ##BRONZE: RETURNS

# COMMAND ----------

@dlt.table(
    name=f"{catalog}.{config['bronze_schema']}.{config['bronze_returns']}"
)
def bronze_returns():

    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("header", "true")
        .load(build_path("returns_path"))
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_file_path", col("_metadata.file_path"))
        .withColumn("source_file_name", regexp_extract(col("_metadata.file_path"), r"([^/]+$)", 1))
        .withColumn("_source_system", lit("adls"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ##BRONZE: STORES

# COMMAND ----------

@dlt.table(
    name=f"{catalog}.{config['bronze_schema']}.{config['bronze_stores']}"
)
def bronze_stores():

    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("header", "true")
        .load(build_path("stores_path"))
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_file_path", col("_metadata.file_path"))
        .withColumn("source_file_name", regexp_extract(col("_metadata.file_path"), r"([^/]+$)", 1))
        .withColumn("_source_system", lit("adls"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ##BRONZE: REGIONS

# COMMAND ----------

@dlt.table(
    name=f"{catalog}.{config['bronze_schema']}.{config['bronze_regions']}"
)
def bronze_regions():

    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("header", "true")
        .load(build_path("regions_path"))
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_file_path", col("_metadata.file_path"))
        .withColumn("source_file_name", regexp_extract(col("_metadata.file_path"), r"([^/]+$)", 1))
        .withColumn("_source_system", lit("adls"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ##BRONZE: CALENDAR

# COMMAND ----------

@dlt.table(
    name=f"{catalog}.{config['bronze_schema']}.{config['bronze_calendar']}"
)
def bronze_calendar():

    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("header", "true")
        .load(build_path("calendar_path"))
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_file_path", col("_metadata.file_path"))
        .withColumn("source_file_name", regexp_extract(col("_metadata.file_path"), r"([^/]+$)", 1))
        .withColumn("_source_system", lit("adls"))
    )
