# Databricks notebook source
import dlt
import yaml
from pyspark.sql.functions import *
from pyspark.sql.types import *

# -------------------------
# LOAD CONFIG
# -------------------------
config = yaml.safe_load(open("/Workspace/maven_market/Mysore-Pak/config/config.yml", "r"))
catalog = config["catalog"]
PIPELINE_RUN_ID = current_timestamp()

def tbl(layer, name):
    return f"{catalog}.{config[f'{layer}_schema']}.{config[name]}"

def silver(name):
    return f"{catalog}.{config['silver_schema']}.{name}"

# -------------------------
# SCHEMAS
# -------------------------
customers_data_schema = StructType([
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

products_data_schema = StructType([
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


# -------------------------
# STREAMING SILVER TABLES
# -------------------------

# @dlt.table(
#     name=silver("silver_customers"),
#     comment="Streaming silver customers from Mongo"
# )
# def silver_customers():
#     # Changed dlt.read to dlt.read_stream
#     return (
#         dlt.read_stream(tbl("bronze", "bronze_customers"))
#         .withColumn("parsed_data", from_json(col("data"), customers_data_schema))
#         .withColumn("customer_id", col("_id").cast("string"))
#         .withColumn("customer_name", 
#             trim(concat_ws(" ", col("parsed_data.first_name"), col("parsed_data.last_name")))
#         )
#         .dropDuplicates(["customer_id"])
#         .withColumn("pipeline_run_id", PIPELINE_RUN_ID)
#         .withColumn("processing_timestamp", current_timestamp())
#     )

@dlt.table(
    name=silver("silver_customers"),
    comment="Streaming silver customers from Mongo (flattened)"
)
def silver_customers():

    return (
        dlt.read_stream(tbl("bronze", "bronze_customers"))

        # Parse JSON
        .withColumn("parsed_data", from_json(col("data"), customers_data_schema))

        # 🔥 FLATTEN EVERYTHING
        .select(
            col("_id").cast("string").alias("customer_id"),
            # col("parsed_data.customer_id").cast("int").alias("customer_id"),
            col("parsed_data.customer_acct_num").alias("customer_acct_num"),
            col("parsed_data.first_name").alias("first_name"),
            col("parsed_data.last_name").alias("last_name"),

            col("parsed_data.customer_address").alias("customer_address"),
            col("parsed_data.customer_city").alias("customer_city"),
            col("parsed_data.customer_state_province").alias("customer_state_province"),
            col("parsed_data.customer_postal_code").alias("customer_postal_code"),
            col("parsed_data.customer_country").alias("customer_country"),

            col("parsed_data.birthdate").alias("birthdate"),
            col("parsed_data.marital_status").alias("marital_status"),
            col("parsed_data.yearly_income").alias("yearly_income"),
            col("parsed_data.gender").alias("gender"),

            col("parsed_data.total_children").alias("total_children"),
            col("parsed_data.num_children_at_home").alias("num_children_at_home"),

            col("parsed_data.education").alias("education"),
            col("parsed_data.acct_open_date").alias("acct_open_date"),
            col("parsed_data.member_card").alias("member_card"),
            col("parsed_data.occupation").alias("occupation"),
            col("parsed_data.homeowner").alias("homeowner"),

            # metadata
            col("ingestion_timestamp"),
            col("_source_system")
        )

        # 🔥 DERIVED COLUMN
        .withColumn(
            "customer_name",
            trim(concat_ws(" ", col("first_name"), col("last_name")))
        )

        # TYPE FIXES (IMPORTANT)
        .withColumn("birthdate", to_date("birthdate", "M/d/yyyy"))
        .withColumn("acct_open_date", to_date("acct_open_date", "M/d/yyyy"))

        # DEDUP
        .dropDuplicates(["customer_id"])

        # METADATA
        .withColumn("pipeline_run_id", PIPELINE_RUN_ID)
        .withColumn("processing_timestamp", current_timestamp())
    )

# @dlt.table(
#     name=silver("silver_products"),
#     comment="Streaming silver products from Mongo"
# )
# def silver_products():
#     # Changed dlt.read to dlt.read_stream
#     return (
#         dlt.read_stream(tbl("bronze", "bronze_products"))
#         .withColumn("parsed_data", from_json(col("data"), products_data_schema))
#         .withColumn("product_id", col("_id").cast("string"))
#         .withColumn("product_name", trim(col("parsed_data.product_name")))
#         .dropDuplicates(["product_id"])
#         .withColumn("pipeline_run_id", PIPELINE_RUN_ID)
#         .withColumn("processing_timestamp", current_timestamp())
#     )
@dlt.table(
    name=silver("silver_products"),
    comment="Streaming silver products from Mongo (flattened)"
)
def silver_products():

    return (
        dlt.read_stream(tbl("bronze", "bronze_products"))

        # Parse JSON
        .withColumn("parsed_data", from_json(col("data"), products_data_schema))

        # 🔥 FLATTEN EVERYTHING
        .select(
            col("_id").cast("string").alias("product_id"),
            # col("parsed_data.product_id").cast("int").alias("product_id"),

            col("parsed_data.product_brand").alias("product_brand"),
            col("parsed_data.product_name").alias("product_name"),
            col("parsed_data.product_sku").alias("product_sku"),
            col("parsed_data.product_retail_price").alias("product_retail_price"),
            col("parsed_data.product_cost").alias("product_cost"),
            col("parsed_data.product_weight").alias("product_weight"),
            col("parsed_data.recyclable").alias("recyclable"),
            col("parsed_data.low_fat").alias("low_fat"),

            # metadata
            col("ingestion_timestamp"),
            col("_source_system")
        )

        # CLEAN
        .withColumn("product_name", trim(col("product_name")))

        # DEDUP
        .dropDuplicates(["product_id"])

        # METADATA
        .withColumn("pipeline_run_id", PIPELINE_RUN_ID)
        .withColumn("processing_timestamp", current_timestamp())
    )

# -------------------------
# SCD TYPE 2 TARGETS
# -------------------------

dlt.create_streaming_table(silver("silver_scd_customers"))
dlt.apply_changes(
    target=silver("silver_scd_customers"),
    source=silver("silver_customers"),
    keys=["customer_id"],
    sequence_by=col("processing_timestamp"),
    stored_as_scd_type=2,
    track_history_column_list=[
        "customer_name",
        "customer_city",
        "customer_state_province",
        "customer_country",
        "marital_status",
        "yearly_income",
        "gender",
        "education",
        "occupation",
        "homeowner"
    ],
    ignore_null_updates=True
)
# dlt.apply_changes(
#     target=silver("silver_scd_customers"),
#     source=silver("silver_customers"),
#     keys=["customer_id"],
#     sequence_by=col("processing_timestamp"),
#     stored_as_scd_type=2,
#     track_history_column_list=["customer_name"],
#     ignore_null_updates=True
# )

dlt.create_streaming_table(silver("silver_scd_products"))
dlt.apply_changes(
    target=silver("silver_scd_products"),
    source=silver("silver_products"),
    keys=["product_id"],
    sequence_by=col("processing_timestamp"),
    stored_as_scd_type=2,
    track_history_column_list=[
        "product_name",
        "product_brand",
        "product_retail_price",
        "product_cost",
        "product_weight",
        "recyclable",
        "low_fat"
    ],
    ignore_null_updates=True
)
# dlt.apply_changes(
#     target=silver("silver_scd_products"),
#     source=silver("silver_products"),
#     keys=["product_id"],
#     sequence_by=col("processing_timestamp"),
#     stored_as_scd_type=2,
#     track_history_column_list=["product_name"],
#     ignore_null_updates=True
# )

# import dlt
# import yaml
# from pyspark.sql.functions import *
# from pyspark.sql.types import *

# config = yaml.safe_load(open("/Workspace/maven_market/Mysore-Pak/config/config.yml", "r"))
# catalog = config["catalog"]
# PIPELINE_RUN_ID = current_timestamp()

# def tbl(layer, name):
#     return f"{catalog}.{config[f'{layer}_schema']}.{config[name]}"

# def silver(name):
#     return f"{catalog}.{config['silver_schema']}.{name}"

# # Define schemas for parsing the JSON 'data' column
# customers_data_schema = StructType([
#     StructField("first_name", StringType()),
#     StructField("last_name", StringType())
# ])

# products_data_schema = StructType([
#     StructField("product_name", StringType())
# ])

# @dlt.table(name=silver("silver_customers"))
# def silver_customers():
#     return (
#         dlt.read(tbl("bronze", "bronze_customers"))
#         # 1. Parse the nested JSON in the 'data' column
#         .withColumn("parsed_data", from_json(col("data"), customers_data_schema))
#         # 2. Extract ID
#         .withColumn("customer_id", col("_id").cast("string"))
#         # 3. Concatenate using the parsed fields
#         .withColumn("customer_name", 
#             trim(concat_ws(" ", col("parsed_data.first_name"), col("parsed_data.last_name")))
#         )
#         .dropDuplicates(["customer_id"])
#         .withColumn("pipeline_run_id", PIPELINE_RUN_ID)
#         .withColumn("processing_timestamp", current_timestamp())
#     )

# @dlt.table(name=silver("silver_products"))
# def silver_products():
#     return (
#         dlt.read(tbl("bronze", "bronze_products"))
#         .withColumn("parsed_data", from_json(col("data"), products_data_schema))
#         .withColumn("product_id", col("_id").cast("string"))
#         .withColumn("product_name", trim(col("parsed_data.product_name")))
#         .dropDuplicates(["product_id"])
#         .withColumn("pipeline_run_id", PIPELINE_RUN_ID)
#         .withColumn("processing_timestamp", current_timestamp())
#     )

# # SCD TYPE 2 Targets
# dlt.create_streaming_table(silver("silver_dim_customers"))
# dlt.apply_changes(
#     target=silver("silver_dim_customers"),
#     source=silver("silver_customers"),
#     keys=["customer_id"],
#     sequence_by=col("processing_timestamp"),
#     stored_as_scd_type=2,
#     track_history_column_list=["customer_name"],
#     ignore_null_updates=True
# )

# dlt.create_streaming_table(silver("silver_dim_products"))
# dlt.apply_changes(
#     target=silver("silver_dim_products"),
#     source=silver("silver_products"),
#     keys=["product_id"],
#     sequence_by=col("processing_timestamp"),
#     stored_as_scd_type=2,
#     track_history_column_list=["product_name"],
#     ignore_null_updates=True
# )

# COMMAND ----------

# MAGIC %skip
# MAGIC import dlt
# MAGIC import yaml
# MAGIC from pyspark.sql.functions import *
# MAGIC from pyspark.sql.types import *
# MAGIC
# MAGIC def load_config(path="/Workspace/maven_market/Mysore-Pak/config/config.yml"):
# MAGIC     with open(path, "r") as f:
# MAGIC         return yaml.safe_load(f)
# MAGIC
# MAGIC config = load_config()
# MAGIC '''
# MAGIC def get_env():
# MAGIC     try:
# MAGIC         return spark.conf.get("env")
# MAGIC     except:
# MAGIC         return config.get("env", "dev")
# MAGIC
# MAGIC env = get_env()
# MAGIC catalog = config[f"catalog_{env}"]
# MAGIC '''
# MAGIC catalog = config["catalog"]
# MAGIC def tbl(layer, name):
# MAGIC     return f"{catalog}.{config[f'{layer}_schema']}.{config[name]}"

# COMMAND ----------

# MAGIC %skip
# MAGIC
# MAGIC
# MAGIC products_schema = StructType([
# MAGIC     StructField("product_id", IntegerType()),
# MAGIC     StructField("product_brand", StringType()),
# MAGIC     StructField("product_name", StringType()),
# MAGIC     StructField("product_sku", LongType()),
# MAGIC     StructField("product_retail_price", DoubleType()),
# MAGIC     StructField("product_cost", DoubleType()),
# MAGIC     StructField("product_weight", DoubleType()),
# MAGIC     StructField("recyclable", IntegerType()),
# MAGIC     StructField("low_fat", IntegerType())
# MAGIC ])
# MAGIC
# MAGIC customers_schema = StructType([
# MAGIC     StructField("customer_id", IntegerType()),
# MAGIC     StructField("customer_acct_num", LongType()),
# MAGIC     StructField("first_name", StringType()),
# MAGIC     StructField("last_name", StringType()),
# MAGIC     StructField("customer_address", StringType()),
# MAGIC     StructField("customer_city", StringType()),
# MAGIC     StructField("customer_state_province", StringType()),
# MAGIC     StructField("customer_postal_code", IntegerType()),
# MAGIC     StructField("customer_country", StringType()),
# MAGIC     StructField("birthdate", StringType()),   # convert later
# MAGIC     StructField("marital_status", StringType()),
# MAGIC     StructField("yearly_income", StringType()),
# MAGIC     StructField("gender", StringType()),
# MAGIC     StructField("total_children", IntegerType()),
# MAGIC     StructField("num_children_at_home", IntegerType()),
# MAGIC     StructField("education", StringType()),
# MAGIC     StructField("acct_open_date", StringType()),  # convert later
# MAGIC     StructField("member_card", StringType()),
# MAGIC     StructField("occupation", StringType()),
# MAGIC     StructField("homeowner", StringType())
# MAGIC ])
# MAGIC
# MAGIC def silver(name):
# MAGIC     return f"{catalog}.{config['silver_schema']}.{name}"

# COMMAND ----------

# MAGIC %md
# MAGIC ##CUSTOMERS

# COMMAND ----------

# MAGIC %skip
# MAGIC # @dlt.table(
# MAGIC #     name=silver("silver_customers"),
# MAGIC #     comment="Stage-2 cleaned MongoDB Customers"
# MAGIC # )
# MAGIC # @dlt.expect("valid_customer_id", "customer_id IS NOT NULL")
# MAGIC # def silver_customers():
# MAGIC
# MAGIC #     df = dlt.read(tbl("bronze", config["bronze_customers"]))
# MAGIC
# MAGIC #     return (
# MAGIC #         df
# MAGIC #         # 🔶 FLATTEN NESTED JSON
# MAGIC #         .withColumn("first_name", col("data.first_name"))
# MAGIC #         .withColumn("last_name", col("data.last_name"))
# MAGIC
# MAGIC #         # 🔶 SAFE ID
# MAGIC #         .withColumn("customer_id", col("_id").cast("string"))
# MAGIC
# MAGIC #         # 🔶 NAME CREATION
# MAGIC #         .withColumn(
# MAGIC #             "customer_name",
# MAGIC #             trim(concat_ws(" ", col("first_name"), col("last_name")))
# MAGIC #         )
# MAGIC
# MAGIC #         # 🔶 DEDUP
# MAGIC #         .dropDuplicates(["customer_id"])
# MAGIC
# MAGIC #         # 🔶 RESCUED
# MAGIC #         .withColumn("_rescued_data", col("_rescued_data"))
# MAGIC
# MAGIC #         # 🔶 METADATA
# MAGIC #         .withColumn("processing_timestamp", current_timestamp())
# MAGIC #         .withColumn("ingestion_source", lit(config["default_ingestion_source_mongo"]))
# MAGIC #         .withColumn("record_status", lit(config["default_record_status"]))
# MAGIC #     )

# COMMAND ----------

# MAGIC %skip
# MAGIC @dlt.table(
# MAGIC     name=silver("silver_customers"),
# MAGIC     comment="Stage-2 cleaned MongoDB Customers"
# MAGIC )
# MAGIC @dlt.expect("valid_customer_id", "customer_id IS NOT NULL")
# MAGIC def silver_customers():
# MAGIC
# MAGIC     df = dlt.read(tbl("bronze", config["bronze_customers"]))
# MAGIC
# MAGIC     return (
# MAGIC         df
# MAGIC         # 🔶 PARSE JSON STRING → STRUCT
# MAGIC         .withColumn("json", from_json(col("data"), customers_schema))
# MAGIC
# MAGIC         # 🔶 EXTRACT FIELDS
# MAGIC         .withColumn("first_name", col("json.first_name"))
# MAGIC         .withColumn("last_name", col("json.last_name"))
# MAGIC
# MAGIC         # 🔶 ID
# MAGIC         .withColumn("customer_id", col("_id").cast("string"))
# MAGIC
# MAGIC         # 🔶 NAME
# MAGIC         .withColumn(
# MAGIC             "customer_name",
# MAGIC             trim(concat_ws(" ", col("first_name"), col("last_name")))
# MAGIC         )
# MAGIC
# MAGIC         # 🔶 DEDUP
# MAGIC         .dropDuplicates(["customer_id"])
# MAGIC
# MAGIC         # 🔶 METADATA
# MAGIC         .withColumn("processing_timestamp", current_timestamp())
# MAGIC         .withColumn("ingestion_source", lit(config["default_ingestion_source_mongo"]))
# MAGIC         .withColumn("record_status", lit(config["default_record_status"]))
# MAGIC     )

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %skip
# MAGIC # @dlt.table(
# MAGIC #     name=silver("silver_customers"),
# MAGIC #     comment="Stage-2 cleaned MongoDB Customers"
# MAGIC # )
# MAGIC # @dlt.expect("valid_customer_id", "customer_id IS NOT NULL")
# MAGIC # def silver_customers():
# MAGIC
# MAGIC #     df = dlt.read(tbl("bronze", config["bronze_customers"]))
# MAGIC
# MAGIC #     return (
# MAGIC #         df
# MAGIC #         # 🔶 SAFE ID EXTRACTION
# MAGIC #         .withColumn("customer_id", col("_id").cast("string"))
# MAGIC
# MAGIC #         # 🔶 HANDLE NAME SAFELY
# MAGIC #         .withColumn(
# MAGIC #             "customer_name",
# MAGIC #             trim(
# MAGIC #                 coalesce(
# MAGIC #                     col("customer_name"),
# MAGIC #                     concat_ws(" ", col("first_name"), col("last_name"))
# MAGIC #                 )
# MAGIC #             )
# MAGIC #         )
# MAGIC
# MAGIC #         # 🔶 DEDUP
# MAGIC #         .dropDuplicates(["customer_id"])
# MAGIC
# MAGIC #         # 🔶 RESCUED
# MAGIC #         .withColumn("_rescued_data", col("_rescued_data"))
# MAGIC
# MAGIC #         # 🔶 METADATA
# MAGIC #         .withColumn("processing_timestamp", current_timestamp())
# MAGIC #         .withColumn("ingestion_source", lit(config["default_ingestion_source_mongo"]))
# MAGIC #         .withColumn("record_status", lit(config["default_record_status"]))
# MAGIC #     )

# COMMAND ----------

# MAGIC %md
# MAGIC ##PRODUCTS

# COMMAND ----------

# MAGIC %skip
# MAGIC @dlt.table(
# MAGIC     name=silver("silver_products"),
# MAGIC     comment="Stage-2 cleaned MongoDB Products"
# MAGIC )
# MAGIC @dlt.expect("valid_product_id", "product_id IS NOT NULL")
# MAGIC def silver_products():
# MAGIC
# MAGIC     df = dlt.read(tbl("bronze", config["bronze_products"]))
# MAGIC
# MAGIC     return (
# MAGIC         df
# MAGIC         # 🔶 PARSE JSON
# MAGIC         .withColumn("json", from_json(col("data"), products_schema))
# MAGIC
# MAGIC         # 🔶 EXTRACT
# MAGIC         .withColumn("product_name", col("json.product_name"))
# MAGIC
# MAGIC         # 🔶 ID
# MAGIC         .withColumn("product_id", col("_id").cast("string"))
# MAGIC
# MAGIC         # 🔶 CLEAN
# MAGIC         .withColumn("product_name", trim(col("product_name")))
# MAGIC
# MAGIC         # 🔶 DEDUP
# MAGIC         .dropDuplicates(["product_id"])
# MAGIC
# MAGIC         # 🔶 METADATA
# MAGIC         .withColumn("processing_timestamp", current_timestamp())
# MAGIC         .withColumn("ingestion_source", lit(config["default_ingestion_source_mongo"]))
# MAGIC         .withColumn("record_status", lit(config["default_record_status"]))
# MAGIC     )

# COMMAND ----------

# MAGIC %skip
# MAGIC # @dlt.table(
# MAGIC #     name=silver("silver_products"),
# MAGIC #     comment="Stage-2 cleaned MongoDB Products"
# MAGIC # )
# MAGIC # @dlt.expect("valid_product_id", "product_id IS NOT NULL")
# MAGIC # def silver_products():
# MAGIC
# MAGIC #     df = dlt.read(tbl("bronze", config["bronze_products"]))
# MAGIC
# MAGIC #     return (
# MAGIC #         df
# MAGIC #         # 🔶 FLATTEN
# MAGIC #         .withColumn("product_name", col("data.product_name"))
# MAGIC
# MAGIC #         # 🔶 ID
# MAGIC #         .withColumn("product_id", col("_id").cast("string"))
# MAGIC
# MAGIC #         # 🔶 CLEAN
# MAGIC #         .withColumn("product_name", trim(col("product_name")))
# MAGIC
# MAGIC #         # 🔶 DEDUP
# MAGIC #         .dropDuplicates(["product_id"])
# MAGIC
# MAGIC #         # 🔶 RESCUED
# MAGIC #         .withColumn("_rescued_data", col("_rescued_data"))
# MAGIC
# MAGIC #         # 🔶 METADATA
# MAGIC #         .withColumn("processing_timestamp", current_timestamp())
# MAGIC #         .withColumn("ingestion_source", lit(config["default_ingestion_source_mongo"]))
# MAGIC #         .withColumn("record_status", lit(config["default_record_status"]))
# MAGIC #     )

# COMMAND ----------

# MAGIC %skip
# MAGIC # # -------------------------
# MAGIC # # PRODUCTS (SILVER)
# MAGIC # # -------------------------
# MAGIC # @dlt.table(
# MAGIC #     name=silver("silver_products"),
# MAGIC #     comment="Stage-2 cleaned MongoDB Products"
# MAGIC # )
# MAGIC # @dlt.expect("valid_product_id", "product_id IS NOT NULL")
# MAGIC # def silver_products():
# MAGIC
# MAGIC #     df = dlt.read(tbl("bronze", config["bronze_products"]))
# MAGIC
# MAGIC #     return (
# MAGIC #         df
# MAGIC #         # 🔶 SAFE ID EXTRACTION
# MAGIC #         .withColumn("product_id", col("_id").cast("string"))
# MAGIC
# MAGIC #         # 🔶 CLEANING
# MAGIC #         .withColumn("product_name", trim(col("product_name")))
# MAGIC
# MAGIC #         # 🔶 DEDUP
# MAGIC #         .dropDuplicates(["product_id"])
# MAGIC
# MAGIC #         # 🔶 RESCUED
# MAGIC #         .withColumn("_rescued_data", col("_rescued_data"))
# MAGIC
# MAGIC #         # 🔶 METADATA
# MAGIC #         .withColumn("processing_timestamp", current_timestamp())
# MAGIC #         .withColumn("ingestion_source", lit(config["default_ingestion_source_mongo"]))
# MAGIC #         .withColumn("record_status", lit(config["default_record_status"]))
# MAGIC #     )

# COMMAND ----------

# MAGIC %md
# MAGIC ##SCD TYPE 2 — CUSTOMERS

# COMMAND ----------

# MAGIC %skip
# MAGIC dlt.create_streaming_table(silver("silver_dim_customers"))
# MAGIC
# MAGIC dlt.apply_changes(
# MAGIC     target=silver("silver_dim_customers"),
# MAGIC     source=silver("silver_customers"),
# MAGIC     keys=[config.get("customers_pk", "customer_id")],
# MAGIC     sequence_by=col("processing_timestamp"),
# MAGIC     stored_as_scd_type=2,
# MAGIC     track_history_column_list=config.get(
# MAGIC         "customers_scd_columns",
# MAGIC         ["customer_name"]
# MAGIC     ),
# MAGIC     ignore_null_updates=True   # 🔥 CRITICAL FIX
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ##SCD TYPE 2 — PRODUCTS

# COMMAND ----------

# MAGIC %skip
# MAGIC dlt.create_streaming_table(silver("silver_dim_products"))
# MAGIC
# MAGIC dlt.apply_changes(
# MAGIC     target=silver("silver_dim_products"),
# MAGIC     source=silver("silver_products"),
# MAGIC     keys=[config.get("products_pk", "product_id")],
# MAGIC     sequence_by=col("processing_timestamp"),
# MAGIC     stored_as_scd_type=2,
# MAGIC     track_history_column_list=config.get(
# MAGIC         "products_scd_columns",
# MAGIC         ["product_name"]
# MAGIC     ),
# MAGIC     ignore_null_updates=True   # 🔥 CRITICAL FIX
# MAGIC )
