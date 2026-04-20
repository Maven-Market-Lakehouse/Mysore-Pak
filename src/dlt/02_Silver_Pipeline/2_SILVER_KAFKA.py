# Databricks notebook source
# MAGIC %md
# MAGIC #Config and Import

# COMMAND ----------

import dlt
import yaml
from pyspark.sql.functions import *
from pyspark.sql.types import * 

def load_config(path="/Workspace/maven_market/Mysore-Pak/config/config.yml"):
    with open(path, "r") as f:
        return yaml.safe_load(f)

config = load_config()
'''
def get_env():
    try:
        return spark.conf.get("env")
    except:
        return config.get("env", "dev")

env = get_env()
catalog = config[f"catalog_{env}"]
'''
catalog = config["catalog"]

def tbl(layer, name):
    return f"{catalog}.{config[f'{layer}_schema']}.{config[name]}"

# COMMAND ----------

# MAGIC %md
# MAGIC ##ORDERS

# COMMAND ----------

orders_schema = StructType([
    StructField("order_id", StringType()),
    StructField("product_id", IntegerType()),
    StructField("customer_id", IntegerType()),
    StructField("store_id", IntegerType()),
    StructField("quantity", IntegerType()),
    StructField("order_date", StringType()),
    StructField("payment_type", StringType()),
    StructField("order_channel", StringType())
])

# COMMAND ----------

@dlt.table(
    name=f"{catalog}.{config['silver_schema']}.silver_orders",
    comment="Stage-2 cleaned Kafka Orders"
)
@dlt.expect("valid_order_id", "order_id IS NOT NULL")
@dlt.expect("valid_quantity", "quantity > 0")
def silver_orders():

    df = dlt.read_stream(tbl("bronze", "bronze_orders"))

    parsed = (
        df
        # PARSE JSON
        .withColumn("json", from_json(col("raw_payload"), config["orders_schema"]))
        .select("json.*", "timestamp")

        # EVENT TIME
        .withColumn("event_timestamp", col("timestamp"))

        # TYPE CAST
        .withColumn("order_id", col("order_id").cast("string"))
        .withColumn("quantity", col("quantity").cast("int"))

        # DEDUP (STREAM SAFE)
        .withWatermark("event_timestamp", "10 minutes")
        .dropDuplicates(["order_id", "event_timestamp"])

        # METADATA
        .withColumn("processing_timestamp", current_timestamp())
        .withColumn("ingestion_source", lit("kafka"))
        .withColumn("record_status", lit("cleaned"))

        # RESCUED DATA SAFE
        .withColumn("_rescued_data", col("_rescued_data"))
    )

    return parsed

# COMMAND ----------

# MAGIC %md
# MAGIC ##INVENTORY

# COMMAND ----------

inventory_schema = StructType([
    StructField("inventory_id", StringType()),
    StructField("event_timestamp", StringType()),
    StructField("product_id", IntegerType()),
    StructField("store_id", IntegerType()),
    StructField("quantity_added", IntegerType()),
    StructField("quantity_remaining", IntegerType())
])

# COMMAND ----------

@dlt.table(
    name=f"{catalog}.{config['silver_schema']}.silver_inventory",
    comment="Stage-2 cleaned Kafka Inventory"
)
@dlt.expect("valid_inventory_id", "inventory_id IS NOT NULL")
def silver_inventory():

    df = dlt.read_stream(tbl("bronze", "bronze_inventory"))

    parsed = (
        df
        .withColumn("json", from_json(col("raw_payload"), config["inventory_schema"]))
        .select("json.*", "timestamp")

        # EVENT TIME
        .withColumn("event_timestamp", col("timestamp"))

        # TYPE CAST
        .withColumn("inventory_id", col("inventory_id").cast("string"))

        # DEDUP
        .withWatermark("event_timestamp", "10 minutes")
        .dropDuplicates(["inventory_id", "event_timestamp"])

        # METADATA
        .withColumn("processing_timestamp", current_timestamp())
        .withColumn("ingestion_source", lit("kafka"))
        .withColumn("record_status", lit("cleaned"))

        # RESCUED
        .withColumn("_rescued_data", col("_rescued_data"))
    )

    return parsed

# COMMAND ----------

def silver(name):
    return f"{catalog}.{config['silver_schema']}.{name}"

# COMMAND ----------

# MAGIC %md
# MAGIC ##ORDERS (STREAM + FK VALIDATION)

# COMMAND ----------

@dlt.table(name=silver("silver_orders_valid"))
def silver_orders_valid():

    ord = dlt.read_stream(silver("silver_orders"))
    cust = dlt.read(silver("silver_dim_customers")).filter(col("__IS_CURRENT") == True)

    return (
        ord.join(broadcast(cust), "customer_id", "inner")
        .withColumn("record_status", lit("valid"))
        .withColumn("pipeline_run_id", PIPELINE_RUN_ID)
        .withColumn("processing_timestamp", current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ##QUARANTINE — ORDERS

# COMMAND ----------

@dlt.table(name=silver("silver_quarantine_orders"))
def quarantine_orders():

    ord = dlt.read_stream(silver("silver_orders"))
    cust = dlt.read(silver("silver_dim_customers")).select("customer_id")

    return (
        ord.join(cust, "customer_id", "left_anti")
        .withColumn("failed_fk_column", lit("customer_id"))
        .withColumn("failure_reason", lit("INVALID_CUSTOMER_FK"))
        .withColumn("record_status", lit("quarantined"))
        .withColumn("processing_timestamp", current_timestamp())
        .withColumn("pipeline_run_id", PIPELINE_RUN_ID)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ##INVENTORY

# COMMAND ----------

@dlt.table(name=silver("silver_inventory_valid"))
def silver_inventory_valid():

    inv = dlt.read_stream(silver("silver_inventory"))
    prod = dlt.read(silver("silver_dim_products")).filter(col("__IS_CURRENT") == True)

    return (
        inv.join(broadcast(prod), "product_id", "inner")
        .withColumn("record_status", lit("valid"))
        .withColumn("pipeline_run_id", PIPELINE_RUN_ID)
        .withColumn("processing_timestamp", current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ##QUARANTINE — INVENTORY

# COMMAND ----------

@dlt.table(name=silver("silver_quarantine_inventory"))
def quarantine_inventory():

    inv = dlt.read_stream(silver("silver_inventory"))
    prod = dlt.read(silver("silver_dim_products")).select("product_id")

    return (
        inv.join(prod, "product_id", "left_anti")
        .withColumn("failed_fk_column", lit("product_id"))
        .withColumn("failure_reason", lit("INVALID_PRODUCT_FK"))
        .withColumn("record_status", lit("quarantined"))
        .withColumn("processing_timestamp", current_timestamp())
        .withColumn("pipeline_run_id", PIPELINE_RUN_ID)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC
