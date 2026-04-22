# Databricks notebook source
# MAGIC %md
# MAGIC #DLT Silver Layer — Orders & Inventory

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notebook Summary
# MAGIC
# MAGIC This notebook defines a **Lakeflow Declarative Pipeline** for ingesting, parsing, validating, and deduplicating orders and inventory data from **Kafka streams**. It uses **Delta Live Tables (DLT)** to create streaming tables for:
# MAGIC
# MAGIC - `silver_orders`
# MAGIC - `silver_inventory`
# MAGIC
# MAGIC Key features:
# MAGIC
# MAGIC - **Schema definitions**
# MAGIC - **Date parsing**
# MAGIC - **Watermarks**
# MAGIC - **Deduplication**
# MAGIC - **Business logic**
# MAGIC
# MAGIC **Data quality rules** are enforced using expectations and a quarantine mechanism, separating valid and invalid inventory records into:
# MAGIC
# MAGIC - `silver_inventory_valid`
# MAGIC - `silver_quarantine_inventory`
# MAGIC
# MAGIC The pipeline tracks metadata such as:
# MAGIC
# MAGIC - **Pipeline run ID**
# MAGIC - **Processing timestamps**
# MAGIC
# MAGIC for auditability.
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ##Imports & Setup

# COMMAND ----------

import dlt
import yaml
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ##Load Config

# COMMAND ----------

config = yaml.safe_load(open("/Workspace/maven_market/Mysore-Pak/config/config.yml", "r"))
catalog = config["catalog"]
PIPELINE_RUN_ID = current_timestamp()

def tbl(layer, name):
    return f"{catalog}.{config[f'{layer}_schema']}.{config[name]}"

def silver(name):
    return f"{catalog}.{config['silver_schema']}.{name}"

# COMMAND ----------

# MAGIC %md
# MAGIC ##Orders Schema

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

# MAGIC %md
# MAGIC ##Silver Orders Table

# COMMAND ----------


@dlt.table(name=silver("silver_orders"))
def silver_orders():
    return (
        dlt.read_stream(tbl("bronze", "bronze_orders"))
        .withColumn("json", from_json(col("raw_payload"), orders_schema))
        .select("json.*", "timestamp")
        # DATE PARSING
        .withColumn("order_date", to_date("order_date", "M/d/yyyy"))
        .withColumn("event_timestamp", col("timestamp"))
        .withWatermark("event_timestamp", "10 minutes")
        .dropDuplicates(["order_id", "event_timestamp"])
        .withColumn("pipeline_run_id", PIPELINE_RUN_ID)
        .withColumn("processing_timestamp", current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ##Inventory Schema

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

# MAGIC %md
# MAGIC ##Quarantine Utility Function

# COMMAND ----------


from pyspark.sql.functions import expr

def quarantine_filter(df, rules):
    valid_condition = " AND ".join(f"({v})" for v in rules.values())
    invalid_condition = f"NOT ({valid_condition})"

    clean_df = df.filter(expr(valid_condition))

    quarantine_df = (
        df.filter(expr(invalid_condition))
        .withColumn("_quarantine_timestamp", current_timestamp())
        .withColumn(
            "_quarantine_reason",
            concat_ws(", ", *[
                when(~expr(cond), lit(name)) for name, cond in rules.items()
            ])
        )
    )

    return clean_df, quarantine_df

# COMMAND ----------

# MAGIC %md
# MAGIC ##Silver Inventory Table

# COMMAND ----------

@dlt.table(
    name=silver("silver_inventory"),
    comment="Parsed Kafka inventory with validation, deduplication, and stock intelligence"
)
@dlt.expect_or_drop("valid_product", "product_id IS NOT NULL")
@dlt.expect_or_drop("valid_store", "store_id IS NOT NULL")
@dlt.expect("valid_stock", "quantity_remaining >= 0")
def silver_inventory():

    return (
        dlt.read_stream(tbl("bronze", "bronze_inventory"))

        #  Parse JSON payload
        .withColumn("json", from_json(col("raw_payload"), inventory_schema))

        #  Flatten
        .select("json.*", "timestamp")

        #  Proper timestamp handling
        .withColumn("event_timestamp", to_timestamp(col("event_timestamp")))
        .withColumn("processing_time", col("timestamp"))

        #  Watermark (CRITICAL for Kafka)
        .withWatermark("event_timestamp", "10 minutes")

        #  Deduplication (Kafka = at least once delivery)
        .dropDuplicates(["inventory_id"])

        #  Business logic
        .withColumn(
            "stock_status",
            when(col("quantity_remaining") == 0, "OUT_OF_STOCK")
            .when(col("quantity_remaining") < 20, "LOW_STOCK")
            .when(col("quantity_remaining") < 50, "MEDIUM")
            .otherwise("HEALTHY")
        )

        .withColumn(
            "is_restock",
            when(col("quantity_added") > 0, True).otherwise(False)
        )
        .withColumn("ingestion_source", lit("kafka"))
        .withColumn("record_status", lit("cleaned"))

        #  Metadata
        .withColumn("pipeline_run_id", PIPELINE_RUN_ID)
        .withColumn("processing_timestamp", current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ##Inventory Rules

# COMMAND ----------

INVENTORY_RULES = {
    "valid_product": "product_id IS NOT NULL",
    "valid_store": "store_id IS NOT NULL",
    "valid_stock": "quantity_remaining >= 0"
}

# COMMAND ----------

# MAGIC %md
# MAGIC ##Valid Inventory Table

# COMMAND ----------

@dlt.table(name=silver("silver_inventory_valid"))
def silver_inventory_valid():

    raw = dlt.read_stream(silver("silver_inventory"))

    clean, _ = quarantine_filter(raw, INVENTORY_RULES)

    return (
        clean
        .withColumn("record_status", lit("valid"))
        .withColumn("pipeline_run_id", PIPELINE_RUN_ID)
        .withColumn("processing_timestamp", current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ##Quarantine Inventory Table

# COMMAND ----------

@dlt.table(name=silver("silver_quarantine_inventory"))
def quarantine_inventory():

    raw = dlt.read_stream(silver("silver_inventory"))

    _, quarantined = quarantine_filter(raw, INVENTORY_RULES)

    return quarantined

# COMMAND ----------

# MAGIC %md
# MAGIC ##Orders Rules

# COMMAND ----------

ORDERS_RULES = {
    "valid_order_id": "order_id IS NOT NULL",
    "valid_product": "product_id IS NOT NULL",
    "valid_customer": "customer_id IS NOT NULL",
    "valid_store": "store_id IS NOT NULL",
    "positive_qty": "quantity > 0"
}

# COMMAND ----------

# MAGIC %md
# MAGIC ##Valid Orders Table

# COMMAND ----------

@dlt.table(
    name=silver("silver_orders_valid"),
    comment="Valid orders for downstream Gold layer"
)
def silver_orders_valid():

    raw = dlt.read_stream(silver("silver_orders"))

    clean, _ = quarantine_filter(raw, ORDERS_RULES)

    return (
        clean
        .withColumn("record_status", lit("valid"))
        .withColumn("pipeline_run_id", PIPELINE_RUN_ID)
        .withColumn("processing_timestamp", current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ##Quarantine Orders Table

# COMMAND ----------

@dlt.table(
    name=silver("silver_quarantine_orders"),
    comment="Invalid orders captured for debugging and audit"
)
def quarantine_orders():

    raw = dlt.read_stream(silver("silver_orders"))

    _, quarantined = quarantine_filter(raw, ORDERS_RULES)

    return quarantined
