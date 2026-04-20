# Databricks notebook source
# MAGIC %md
# MAGIC #(CONFIG + HELPERS)

# COMMAND ----------

import dlt
import yaml
from pyspark.sql.functions import *
from pyspark.sql.window import Window

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

PIPELINE_RUN_ID = current_timestamp()

def tbl(layer, name):
    return f"{catalog}.{config[f'{layer}_schema']}.{config[name]}"  # ← plain string, no .withColumn()

df = dlt.read(tbl("bronze", "bronze_transactions"))
df = df.withColumn("pipeline_run_id", PIPELINE_RUN_ID)

# COMMAND ----------

# MAGIC %md
# MAGIC ##SILVER: TRANSACTIONS

# COMMAND ----------

@dlt.table(
    name=f"{catalog}.{config['silver_schema']}.silver_transactions",
    comment="Stage-1 cleaned transactions"
)
@dlt.expect("valid_quantity", "quantity > 0")
@dlt.expect("valid_customer", "customer_id IS NOT NULL")
@dlt.expect("valid_product", "product_id IS NOT NULL")
def silver_transactions():

    df = dlt.read_stream(tbl("bronze", "bronze_transactions"))

    return (
        df
        # TYPE ENFORCEMENT
        .withColumn("transaction_date", to_date("transaction_date"))
        .withColumn("quantity", col("quantity").cast("int"))

        # NULL HANDLING
        .fillna({"quantity": 0})

        # STANDARDIZATION
        .withColumn("transaction_date", to_date("transaction_date"))

        # DEDUP
        .dropDuplicates(["product_id","customer_id","store_id","transaction_date"])

        # RESCUED DATA SAFE KEEP
        .withColumn("_rescued_data", col("_rescued_data"))

        # METADATA
        .withColumn("processing_timestamp", current_timestamp())
        .withColumn("source_table", lit("transactions"))
        .withColumn(
            "record_status",
            when(col("quantity") <= 0, "invalid").otherwise("cleaned")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ##RETURNS

# COMMAND ----------

@dlt.table(
    name=f"{catalog}.{config['silver_schema']}.silver_returns",
    comment="Stage-1 cleaned returns"
)
@dlt.expect("valid_quantity", "quantity > 0")
def silver_returns():

    df = dlt.read_stream(tbl("bronze", "bronze_returns"))

    return (
        df
        # TYPE ENFORCEMENT
        .withColumn("return_date", to_date("return_date"))
        .withColumn("quantity", col("quantity").cast("int"))

        # NULL HANDLING
        .fillna({"quantity": 0})

        # DEDUP
        .dropDuplicates(["product_id","store_id","return_date"])

        # RESCUED DATA
        .withColumn("_rescued_data", col("_rescued_data"))

        # METADATA
        .withColumn("processing_timestamp", current_timestamp())
        .withColumn("source_table", lit("returns"))
        .withColumn(
            "record_status",
            when(col("quantity") <= 0, "invalid").otherwise("cleaned")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ##STORES

# COMMAND ----------

@dlt.table(
    name=f"{catalog}.{config['silver_schema']}.silver_stores",
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
        .withColumn("_rescued_data", col("_rescued_data"))

        # METADATA
        .withColumn("processing_timestamp", current_timestamp())
        .withColumn("source_table", lit("stores"))
        .withColumn("record_status", lit("cleaned"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ##REGIONS

# COMMAND ----------

@dlt.table(
    name=f"{catalog}.{config['silver_schema']}.silver_regions",
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

        # RESCUED
        .withColumn("_rescued_data", col("_rescued_data"))

        # METADATA
        .withColumn("processing_timestamp", current_timestamp())
        .withColumn("source_table", lit("regions"))
        .withColumn("record_status", lit("cleaned"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ##CALENDAR

# COMMAND ----------

@dlt.table(
    name=f"{catalog}.{config['silver_schema']}.silver_calendar",
    comment="Stage-1 cleaned calendar"
)
@dlt.expect("valid_date", "date IS NOT NULL")
def silver_calendar():

    df = dlt.read_stream(tbl("bronze", "bronze_calendar"))

    return (
        df
        # TYPE CAST
        .withColumn("date", to_date("date"))

        # DEDUP
        .dropDuplicates(["date"])

        # RESCUED
        .withColumn("_rescued_data", col("_rescued_data"))

        # METADATA
        .withColumn("processing_timestamp", current_timestamp())
        .withColumn("source_table", lit("calendar"))
        .withColumn("record_status", lit("cleaned"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC **Function**

# COMMAND ----------

def silver(name):
    return f"{catalog}.{config['silver_schema']}.{name}"

# COMMAND ----------

# MAGIC %md
# MAGIC ##SCD TYPE 2 — STORES

# COMMAND ----------

dlt.create_streaming_table(silver("silver_dim_stores"))

dlt.apply_changes(
    target=silver("silver_dim_stores"),
    source=silver("silver_stores"),
    keys=[config["stores_pk"]],
    sequence_by=col("processing_timestamp"),
    stored_as_scd_type=2,
    track_history_column_list=config["stores_scd_columns"]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##STORES ↔ REGIONS (FK VALIDATION)

# COMMAND ----------

@dlt.table(name=silver("silver_stores_valid"))
def silver_stores_valid():

    stores = dlt.read(silver("silver_stores"))
    regions = dlt.read(silver("silver_regions")).select("region_id")

    return (
        stores
        .join(regions, "region_id", "inner")
        .withColumn("record_status", lit("valid"))
        .withColumn("pipeline_run_id", PIPELINE_RUN_ID)
        .withColumn("processing_timestamp", current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ##QUARANTINE — STORES

# COMMAND ----------

@dlt.table(name=silver("silver_quarantine_stores"))
def quarantine_stores():

    stores = dlt.read(silver("silver_stores"))
    regions = dlt.read(silver("silver_regions")).select("region_id")

    return (
        stores
        .join(regions, "region_id", "left_anti")
        .withColumn("failed_fk_column", lit("region_id"))
        .withColumn("failure_reason", lit("INVALID_REGION_FK"))
        .withColumn("record_status", lit("quarantined"))
        .withColumn("processing_timestamp", current_timestamp())
        .withColumn("pipeline_run_id", PIPELINE_RUN_ID)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ##TRANSACTIONS — FULL FK VALIDATION

# COMMAND ----------

@dlt.table(name=silver("silver_transactions_valid"))
def silver_transactions_valid():

    tx = dlt.read(silver("silver_transactions"))

    prod = dlt.read(silver("silver_dim_products")) \
              .filter(col("__END_AT").isNull()) \
              .select("product_id")

    cust = dlt.read(silver("silver_dim_customers")) \
              .filter(col("__END_AT").isNull()) \
              .select("customer_id")

    store = dlt.read(silver("silver_dim_stores")) \
               .filter(col("__END_AT").isNull()) \
               .select("store_id")

    joined = (
        tx
        .join(broadcast(prod), "product_id", "left")
        .join(broadcast(cust), "customer_id", "left")
        .join(broadcast(store), "store_id", "left")
    )

    valid = joined.filter(
        col("product_id").isNotNull() &
        col("customer_id").isNotNull() &
        col("store_id").isNotNull()
    )

    return (
        valid
        .withColumn("record_status", lit(config["record_status_valid"]))
        .withColumn("pipeline_run_id", current_timestamp())
        .withColumn("processing_timestamp", current_timestamp())
    )

# @dlt.table(name=silver("silver_transactions_valid"))
# def silver_transactions_valid():

#     tx = dlt.read(silver("silver_transactions"))

#     prod = dlt.read(silver("silver_dim_products")) \
#               .filter(col("__IS_CURRENT") == True) \
#               .select("product_id")

#     cust = dlt.read(silver("silver_dim_customers")) \
#               .filter(col("__IS_CURRENT") == True) \
#               .select("customer_id")

#     store = dlt.read(silver("silver_dim_stores")) \
#                .filter(col("__IS_CURRENT") == True) \
#                .select("store_id")

#     # 🔶 LEFT JOIN (NO DATA LOSS)
#     joined = (
#         tx
#         .join(broadcast(prod), "product_id", "left")
#         .join(broadcast(cust), "customer_id", "left")
#         .join(broadcast(store), "store_id", "left")
#     )

#     # 🔶 KEEP ONLY VALID RECORDS
#     valid = joined.filter(
#         col("product_id").isNotNull() &
#         col("customer_id").isNotNull() &
#         col("store_id").isNotNull()
#     )

#     return (
#         valid
#         .withColumn("record_status", lit(config["record_status_valid"]))
#         .withColumn("pipeline_run_id", current_timestamp())   # ✅ FIXED
#         .withColumn("processing_timestamp", current_timestamp())
#     )

# COMMAND ----------

# MAGIC %md
# MAGIC ##QUARANTINE — TRANSACTIONS (ALL FK CASES)

# COMMAND ----------

@dlt.table(name=silver("silver_quarantine_transactions"))
def quarantine_transactions():

    tx = dlt.read(silver("silver_transactions"))

    prod = dlt.read(silver("silver_dim_products")) \
              .filter(col("__IS_CURRENT") == True) \
              .select("product_id")

    cust = dlt.read(silver("silver_dim_customers")) \
              .filter(col("__IS_CURRENT") == True) \
              .select("customer_id")

    store = dlt.read(silver("silver_dim_stores")) \
               .filter(col("__IS_CURRENT") == True) \
               .select("store_id")

    # 🔶 SINGLE JOIN (NO DUPLICATION ISSUE)
    joined = (
        tx
        .join(prod, "product_id", "left")
        .join(cust, "customer_id", "left")
        .join(store, "store_id", "left")
    )

    # 🔶 IDENTIFY INVALID RECORDS
    invalid = joined.filter(
        col("product_id").isNull() |
        col("customer_id").isNull() |
        col("store_id").isNull()
    )

    return (
        invalid
        .withColumn(
            "failed_fk_column",
            when(col("product_id").isNull(), lit("product_id"))
            .when(col("customer_id").isNull(), lit("customer_id"))
            .when(col("store_id").isNull(), lit("store_id"))
        )
        .withColumn(
            "failure_reason",
            when(col("product_id").isNull(), lit("INVALID_PRODUCT_FK"))
            .when(col("customer_id").isNull(), lit("INVALID_CUSTOMER_FK"))
            .when(col("store_id").isNull(), lit("INVALID_STORE_FK"))
        )
        .withColumn("record_status", lit(config["record_status_quarantined"]))
        .withColumn("processing_timestamp", current_timestamp())
        .withColumn("pipeline_run_id", current_timestamp())   # ✅ FIXED
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ##RETURNS — FK VALIDATION

# COMMAND ----------

@dlt.table(name=silver("silver_returns_valid"))
def silver_returns_valid():

    ret = dlt.read(silver("silver_returns"))
    prod = dlt.read(silver("silver_dim_products")).filter(col("__IS_CURRENT") == True)
    store = dlt.read(silver("silver_dim_stores")).filter(col("__IS_CURRENT") == True)

    return (
        ret
        .join(prod, "product_id", "inner")
        .join(store, "store_id", "inner")
        .withColumn("record_status", lit("valid"))
        .withColumn("pipeline_run_id", PIPELINE_RUN_ID)
        .withColumn("processing_timestamp", current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ##QUARANTINE — RETURNS

# COMMAND ----------

@dlt.table(name=silver("silver_quarantine_returns"))
def quarantine_returns():

    ret = dlt.read(silver("silver_returns"))

    prod = dlt.read(silver("silver_dim_products")).select("product_id")
    store = dlt.read(silver("silver_dim_stores")).select("store_id")

    invalid_product = ret.join(prod, "product_id", "left_anti") \
        .withColumn("failed_fk_column", lit("product_id")) \
        .withColumn("failure_reason", lit("INVALID_PRODUCT_FK"))

    invalid_store = ret.join(store, "store_id", "left_anti") \
        .withColumn("failed_fk_column", lit("store_id")) \
        .withColumn("failure_reason", lit("INVALID_STORE_FK"))

    return (
        invalid_product
        .unionByName(invalid_store)
        .withColumn("record_status", lit("quarantined"))
        .withColumn("processing_timestamp", current_timestamp())
        .withColumn("pipeline_run_id", PIPELINE_RUN_ID)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC
