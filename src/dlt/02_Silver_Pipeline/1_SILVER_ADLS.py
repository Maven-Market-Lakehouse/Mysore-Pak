# Databricks notebook source
# MAGIC %md
# MAGIC # Summary of the Lakeflow Declarative Pipelines notebook
# MAGIC
# MAGIC summary = """
# MAGIC This notebook defines a Lakeflow Declarative Pipeline for ingesting, cleaning, and transforming retail data using streaming tables and materialized views. Key components include:
# MAGIC
# MAGIC - Configuration loading and table naming utilities.
# MAGIC - Stage-1 cleaning tables for transactions, stores, regions, calendar, and returns, each with data quality expectations.
# MAGIC - Quarantine logic to separate valid and invalid records based on configurable rules.
# MAGIC - Auto CDC flow for stores to maintain SCD Type 2 history.
# MAGIC - Data quality expectations applied declaratively.
# MAGIC - Streaming tables and materialized views are defined using Lakeflow Declarative Pipelines decorators.
# MAGIC """
# MAGIC
# MAGIC display(summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Imports

# COMMAND ----------

import dlt
import yaml
from pyspark.sql.functions import *
from pyspark.sql.functions import expr

# COMMAND ----------

# MAGIC %md
# MAGIC ##Load Config

# COMMAND ----------

def load_config(path="/Workspace/maven_market/Mysore-Pak/config/config.yml"):
    with open(path, "r") as f:
        return yaml.safe_load(f)

config = load_config()
catalog = config["catalog"]
PIPELINE_RUN_ID = current_timestamp()

def tbl(layer, name):
    return f"{catalog}.{config[f'{layer}_schema']}.{config[name]}"

def silver(name):
    return f"{catalog}.{config['silver_schema']}.{name}"

# COMMAND ----------

# MAGIC %md
# MAGIC ##Silver Transactions Table

# COMMAND ----------


@dlt.table(
    name=silver("silver_transactions"),
    comment="Stage-1 cleaned transactions"
)
@dlt.expect("valid_quantity", "quantity > 0")
@dlt.expect("valid_customer", "customer_id IS NOT NULL")
@dlt.expect("valid_product", "product_id IS NOT NULL")
@dlt.expect("valid_date", "transaction_date IS NOT NULL")
def silver_transactions():

    df = dlt.read_stream(tbl("bronze", "bronze_transactions"))

    return (
        df
        .withColumn("transaction_date", to_date("transaction_date", "M/d/yyyy"))

        # TYPE CAST
        .withColumn("quantity", col("quantity").cast("int"))

        # DEDUP
        .dropDuplicates(["product_id","customer_id","store_id","transaction_date"])

        # METADATA
        .withColumn("pipeline_run_id", current_timestamp())
        .withColumn("processing_timestamp", current_timestamp())
        .withColumn("source_table", lit("transactions"))

        # STATUS
        .withColumn(
            "record_status",
            when(col("quantity") <= 0, "invalid").otherwise("cleaned")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ##Silver Stores Table

# COMMAND ----------

@dlt.table(
    name=silver("silver_stores"),
    comment="Stage-1 cleaned stores"
)
@dlt.expect("valid_store", "store_id IS NOT NULL")
def silver_stores():

    df = dlt.read_stream(tbl("bronze", "bronze_stores"))

    return (
        df
        # TYPE CAST
        .withColumn("store_id", col("store_id").cast("int"))
        .withColumn("region_id", col("region_id").cast("int"))

        # STRING CLEANING
        .withColumn("store_name", trim(col("store_name")))
        .withColumn("store_city", initcap(col("store_city")))
        .withColumn("store_state", upper(col("store_state")))

        # DATE STANDARDIZATION
        .withColumn("first_opened_date", to_date("first_opened_date"))
        .withColumn("last_remodel_date", to_date("last_remodel_date"))

        # DEDUP
        .dropDuplicates(["store_id"])

        # RESCUED
        # .withColumn("_rescued_data", col("_rescued_data"))
        .withColumn("pipeline_run_id", current_timestamp())

        # METADATA
        .withColumn("processing_timestamp", current_timestamp())
        .withColumn("source_table", lit("stores"))
        .withColumn("record_status", lit("cleaned"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ##Silver Regions Table

# COMMAND ----------

@dlt.table(
    name=silver("silver_regions"),
    comment="Stage-1 cleaned regions"
)
@dlt.expect("valid_region", "region_id IS NOT NULL")
def silver_regions():

    df = dlt.read_stream(tbl("bronze", "bronze_regions"))

    return (
        df
        # TYPE CAST
        .withColumn("region_id", col("region_id").cast("int"))

        # STANDARDIZATION
        .withColumn("sales_region", initcap(col("sales_region")))
        .withColumn("sales_district", initcap(col("sales_district")))

        # DEDUP
        .dropDuplicates(["region_id"])
        .withColumn("pipeline_run_id", current_timestamp())

        # METADATA
        .withColumn("processing_timestamp", current_timestamp())
        .withColumn("source_table", lit("regions"))
        .withColumn("record_status", lit("cleaned"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ##Silver Calendar Table

# COMMAND ----------


@dlt.table(
    name=silver("silver_calendar"),
    comment="Stage-1 cleaned calendar"
)
@dlt.expect("valid_date", "date IS NOT NULL")
def silver_calendar():

    df = dlt.read_stream(tbl("bronze", "bronze_calendar"))

    return (
        df
        .withColumn("date", to_date("date"))
        .withColumn("pipeline_run_id", current_timestamp())
        .withColumn("processing_timestamp", current_timestamp())
        .withColumn("source_table", lit("calendar"))
        .withColumn("record_status", lit("cleaned"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ##Silver Returns Table

# COMMAND ----------


@dlt.table(
    name=silver("silver_returns"),
    comment="Stage-1 cleaned returns"
)
@dlt.expect("valid_quantity", "quantity > 0")
@dlt.expect("valid_date", "return_date IS NOT NULL")
def silver_returns():

    df = dlt.read_stream(tbl("bronze", "bronze_returns"))

    return (
        df
        # FIXED DATE PARSING
        .withColumn("return_date", to_date("return_date", "M/d/yyyy"))

        # TYPE CAST
        .withColumn("quantity", col("quantity").cast("int"))

        # DEDUP
        .dropDuplicates(["product_id","store_id","return_date"])

        # METADATA
        .withColumn("pipeline_run_id", current_timestamp())
        .withColumn("processing_timestamp", current_timestamp())
        .withColumn("source_table", lit("returns"))

        # STATUS
        .withColumn(
            "record_status",
            when(col("quantity") <= 0, "invalid").otherwise("cleaned")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ##SCD Type 2: Stores

# COMMAND ----------

dlt.create_streaming_table(silver("silver_scd_stores"))

dlt.apply_changes(
    target=silver("silver_scd_stores"),
    source=silver("silver_stores"),
    keys=[config["stores_pk"]],
    sequence_by=col("processing_timestamp"),
    stored_as_scd_type=2,
    track_history_column_list=config["stores_scd_columns"]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Quarantine Utility Function

# COMMAND ----------

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
# MAGIC ##Transaction Rules

# COMMAND ----------

TRANSACTION_RULES = {
    "valid_product": "product_id IS NOT NULL",
    "valid_customer": "customer_id IS NOT NULL",
    "valid_store": "store_id IS NOT NULL",
    "positive_qty": "quantity > 0",
    "valid_date": "transaction_date IS NOT NULL"
}

# COMMAND ----------

# MAGIC %md
# MAGIC ##Valid Transactions Table

# COMMAND ----------

@dlt.table(name=silver("silver_transactions_valid"))
def silver_transactions_valid():

    raw = dlt.read_stream(silver("silver_transactions"))

    clean, _ = quarantine_filter(raw, TRANSACTION_RULES)

    return (
        clean
        .withColumn("record_status", lit("valid"))
        .withColumn("pipeline_run_id", PIPELINE_RUN_ID)
        .withColumn("processing_timestamp", current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ##Quarantine Transactions Table

# COMMAND ----------

@dlt.table(name=silver("silver_quarantine_transactions"))
def quarantine_transactions():

    raw = dlt.read_stream(silver("silver_transactions"))

    _, quarantined = quarantine_filter(raw, TRANSACTION_RULES)

    return quarantined
