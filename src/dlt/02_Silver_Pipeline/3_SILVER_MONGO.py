# Databricks notebook source
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



products_schema = StructType([
    StructField("product_id", IntegerType()),
    StructField("product_brand", StringType()),
    StructField("product_name", StringType()),
    StructField("product_sku", LongType()),
    StructField("product_retail_price", DoubleType()),
    StructField("product_cost", DoubleType()),
    StructField("product_weight", DoubleType()),
    StructField("recyclable", IntegerType()),
    StructField("low_fat", IntegerType())
])

customers_schema = StructType([
    StructField("customer_id", IntegerType()),
    StructField("customer_acct_num", LongType()),
    StructField("first_name", StringType()),
    StructField("last_name", StringType()),
    StructField("customer_address", StringType()),
    StructField("customer_city", StringType()),
    StructField("customer_state_province", StringType()),
    StructField("customer_postal_code", IntegerType()),
    StructField("customer_country", StringType()),
    StructField("birthdate", StringType()),   # convert later
    StructField("marital_status", StringType()),
    StructField("yearly_income", StringType()),
    StructField("gender", StringType()),
    StructField("total_children", IntegerType()),
    StructField("num_children_at_home", IntegerType()),
    StructField("education", StringType()),
    StructField("acct_open_date", StringType()),  # convert later
    StructField("member_card", StringType()),
    StructField("occupation", StringType()),
    StructField("homeowner", StringType())
])

def silver(name):
    return f"{catalog}.{config['silver_schema']}.{name}"

# COMMAND ----------

# MAGIC %md
# MAGIC ##CUSTOMERS

# COMMAND ----------

# @dlt.table(
#     name=silver("silver_customers"),
#     comment="Stage-2 cleaned MongoDB Customers"
# )
# @dlt.expect("valid_customer_id", "customer_id IS NOT NULL")
# def silver_customers():

#     df = dlt.read(tbl("bronze", config["bronze_customers"]))

#     return (
#         df
#         # 🔶 FLATTEN NESTED JSON
#         .withColumn("first_name", col("data.first_name"))
#         .withColumn("last_name", col("data.last_name"))

#         # 🔶 SAFE ID
#         .withColumn("customer_id", col("_id").cast("string"))

#         # 🔶 NAME CREATION
#         .withColumn(
#             "customer_name",
#             trim(concat_ws(" ", col("first_name"), col("last_name")))
#         )

#         # 🔶 DEDUP
#         .dropDuplicates(["customer_id"])

#         # 🔶 RESCUED
#         .withColumn("_rescued_data", col("_rescued_data"))

#         # 🔶 METADATA
#         .withColumn("processing_timestamp", current_timestamp())
#         .withColumn("ingestion_source", lit(config["default_ingestion_source_mongo"]))
#         .withColumn("record_status", lit(config["default_record_status"]))
#     )

# COMMAND ----------

@dlt.table(
    name=silver("silver_customers"),
    comment="Stage-2 cleaned MongoDB Customers"
)
@dlt.expect("valid_customer_id", "customer_id IS NOT NULL")
def silver_customers():

    df = dlt.read(tbl("bronze", config["bronze_customers"]))

    return (
        df
        # 🔶 PARSE JSON STRING → STRUCT
        .withColumn("json", from_json(col("data"), customers_schema))

        # 🔶 EXTRACT FIELDS
        .withColumn("first_name", col("json.first_name"))
        .withColumn("last_name", col("json.last_name"))

        # 🔶 ID
        .withColumn("customer_id", col("_id").cast("string"))

        # 🔶 NAME
        .withColumn(
            "customer_name",
            trim(concat_ws(" ", col("first_name"), col("last_name")))
        )

        # 🔶 DEDUP
        .dropDuplicates(["customer_id"])

        # 🔶 METADATA
        .withColumn("processing_timestamp", current_timestamp())
        .withColumn("ingestion_source", lit(config["default_ingestion_source_mongo"]))
        .withColumn("record_status", lit(config["default_record_status"]))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# @dlt.table(
#     name=silver("silver_customers"),
#     comment="Stage-2 cleaned MongoDB Customers"
# )
# @dlt.expect("valid_customer_id", "customer_id IS NOT NULL")
# def silver_customers():

#     df = dlt.read(tbl("bronze", config["bronze_customers"]))

#     return (
#         df
#         # 🔶 SAFE ID EXTRACTION
#         .withColumn("customer_id", col("_id").cast("string"))

#         # 🔶 HANDLE NAME SAFELY
#         .withColumn(
#             "customer_name",
#             trim(
#                 coalesce(
#                     col("customer_name"),
#                     concat_ws(" ", col("first_name"), col("last_name"))
#                 )
#             )
#         )

#         # 🔶 DEDUP
#         .dropDuplicates(["customer_id"])

#         # 🔶 RESCUED
#         .withColumn("_rescued_data", col("_rescued_data"))

#         # 🔶 METADATA
#         .withColumn("processing_timestamp", current_timestamp())
#         .withColumn("ingestion_source", lit(config["default_ingestion_source_mongo"]))
#         .withColumn("record_status", lit(config["default_record_status"]))
#     )

# COMMAND ----------

# MAGIC %md
# MAGIC ##PRODUCTS

# COMMAND ----------

@dlt.table(
    name=silver("silver_products"),
    comment="Stage-2 cleaned MongoDB Products"
)
@dlt.expect("valid_product_id", "product_id IS NOT NULL")
def silver_products():

    df = dlt.read(tbl("bronze", config["bronze_products"]))

    return (
        df
        # 🔶 PARSE JSON
        .withColumn("json", from_json(col("data"), products_schema))

        # 🔶 EXTRACT
        .withColumn("product_name", col("json.product_name"))

        # 🔶 ID
        .withColumn("product_id", col("_id").cast("string"))

        # 🔶 CLEAN
        .withColumn("product_name", trim(col("product_name")))

        # 🔶 DEDUP
        .dropDuplicates(["product_id"])

        # 🔶 METADATA
        .withColumn("processing_timestamp", current_timestamp())
        .withColumn("ingestion_source", lit(config["default_ingestion_source_mongo"]))
        .withColumn("record_status", lit(config["default_record_status"]))
    )

# COMMAND ----------

# @dlt.table(
#     name=silver("silver_products"),
#     comment="Stage-2 cleaned MongoDB Products"
# )
# @dlt.expect("valid_product_id", "product_id IS NOT NULL")
# def silver_products():

#     df = dlt.read(tbl("bronze", config["bronze_products"]))

#     return (
#         df
#         # 🔶 FLATTEN
#         .withColumn("product_name", col("data.product_name"))

#         # 🔶 ID
#         .withColumn("product_id", col("_id").cast("string"))

#         # 🔶 CLEAN
#         .withColumn("product_name", trim(col("product_name")))

#         # 🔶 DEDUP
#         .dropDuplicates(["product_id"])

#         # 🔶 RESCUED
#         .withColumn("_rescued_data", col("_rescued_data"))

#         # 🔶 METADATA
#         .withColumn("processing_timestamp", current_timestamp())
#         .withColumn("ingestion_source", lit(config["default_ingestion_source_mongo"]))
#         .withColumn("record_status", lit(config["default_record_status"]))
#     )

# COMMAND ----------

# # -------------------------
# # PRODUCTS (SILVER)
# # -------------------------
# @dlt.table(
#     name=silver("silver_products"),
#     comment="Stage-2 cleaned MongoDB Products"
# )
# @dlt.expect("valid_product_id", "product_id IS NOT NULL")
# def silver_products():

#     df = dlt.read(tbl("bronze", config["bronze_products"]))

#     return (
#         df
#         # 🔶 SAFE ID EXTRACTION
#         .withColumn("product_id", col("_id").cast("string"))

#         # 🔶 CLEANING
#         .withColumn("product_name", trim(col("product_name")))

#         # 🔶 DEDUP
#         .dropDuplicates(["product_id"])

#         # 🔶 RESCUED
#         .withColumn("_rescued_data", col("_rescued_data"))

#         # 🔶 METADATA
#         .withColumn("processing_timestamp", current_timestamp())
#         .withColumn("ingestion_source", lit(config["default_ingestion_source_mongo"]))
#         .withColumn("record_status", lit(config["default_record_status"]))
#     )

# COMMAND ----------

# MAGIC %md
# MAGIC ##SCD TYPE 2 — CUSTOMERS

# COMMAND ----------

dlt.create_streaming_table(silver("silver_dim_customers"))

dlt.apply_changes(
    target=silver("silver_dim_customers"),
    source=silver("silver_customers"),
    keys=[config.get("customers_pk", "customer_id")],
    sequence_by=col("processing_timestamp"),
    stored_as_scd_type=2,
    track_history_column_list=config.get(
        "customers_scd_columns",
        ["customer_name"]
    ),
    ignore_null_updates=True   # 🔥 CRITICAL FIX
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##SCD TYPE 2 — PRODUCTS

# COMMAND ----------

dlt.create_streaming_table(silver("silver_dim_products"))

dlt.apply_changes(
    target=silver("silver_dim_products"),
    source=silver("silver_products"),
    keys=[config.get("products_pk", "product_id")],
    sequence_by=col("processing_timestamp"),
    stored_as_scd_type=2,
    track_history_column_list=config.get(
        "products_scd_columns",
        ["product_name"]
    ),
    ignore_null_updates=True   # 🔥 CRITICAL FIX
)
