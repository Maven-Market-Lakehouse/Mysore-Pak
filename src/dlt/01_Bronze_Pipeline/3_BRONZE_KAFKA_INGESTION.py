# Databricks notebook source
# MAGIC %md
# MAGIC #IMPORTS + CONFIG LOAD + ENV ROUTER

# COMMAND ----------

# MAGIC %md
# MAGIC ### Summary of the notebook:
# MAGIC  - Loads configuration from a YAML file to resolve environment, catalog, schema, and table names.
# MAGIC  - Defines two Lakeflow Declarative Pipelines streaming tables:
# MAGIC    1. bronze_raw_orders: Ingests raw order data from Kafka, adds ingestion timestamp and source system metadata.
# MAGIC    2. bronze_raw_inventory: Ingests raw inventory data from Kafka, adds ingestion timestamp and source system metadata.
# MAGIC  - Uses Lakeflow Declarative Pipelines (@dlt.table) to manage streaming ingestion and table creation.

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

# DBTITLE 1,Pipeline Logger
# -------------------------
# PIPELINE LOGGER
# -------------------------
import sys
sys.path.insert(0, "/Workspace/maven_market/Mysore-Pak/src")
from utils.custom_logger import PipelineLogger

logger = PipelineLogger(spark, config, buffer_size=3, echo=True)
logger.log_info("Bronze Kafka Pipeline — registering tables: orders, inventory", layer="bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC ##BRONZE: KAFKA (ORDERS)

# COMMAND ----------

@dlt.table(
    name=f"{catalog}.{config['bronze_schema']}.bronze_raw_orders"
)
def bronze_orders():
    kafka_server = dbutils.secrets.get(scope=config["kafka_scope"], key=config["kafka_bootstrap_key"])
    kafka_api_key = dbutils.secrets.get(scope=config["kafka_scope"], key=config["kafka_api_key_name"])
    kafka_secret = dbutils.secrets.get(scope=config["kafka_scope"], key=config["kafka_api_secret_name"])

    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_server)
        .option("subscribe", config["orders_topic"])
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.sasl.jaas.config", 
            f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{kafka_api_key}' password='{kafka_secret}';")
        .option("startingOffsets", "earliest")
        .load()
        .selectExpr(
            "CAST(value AS STRING) as raw_payload",
            "topic",
            "partition",
            "offset",
            "timestamp"
        )
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("_source_system", lit("kafka"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ##BRONZE: KAFKA (INVENTORY)

# COMMAND ----------

@dlt.table(
    name=f"{catalog}.{config['bronze_schema']}.bronze_raw_inventory"
)
def bronze_inventory():

    kafka_server = dbutils.secrets.get(scope=config["kafka_scope"], key=config["kafka_bootstrap_key"])
    kafka_api_key = dbutils.secrets.get(scope=config["kafka_scope"], key=config["kafka_api_key_name"])
    kafka_secret = dbutils.secrets.get(scope=config["kafka_scope"], key=config["kafka_api_secret_name"])

    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_server)
        .option("subscribe", config["inventory_topic"])
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.sasl.jaas.config", 
            f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{kafka_api_key}' password='{kafka_secret}';")
        .option("startingOffsets", "earliest")
        .load()
        .selectExpr(
            "CAST(value AS STRING) as raw_payload",
            "topic",
            "partition",
            "offset",
            "timestamp"
        )
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("_source_system", lit("kafka"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ##
