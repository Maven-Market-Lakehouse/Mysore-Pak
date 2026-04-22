# Databricks notebook source
# MAGIC %md
# MAGIC #DLT Silver Layer Pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ----------------------------------------------
# MAGIC 1. Configuration:
# MAGIC    - Loads pipeline config from YAML.
# MAGIC    - Sets catalog, schemas, and table names dynamically.
# MAGIC
# MAGIC 2. Schemas:
# MAGIC    - Defines customer and product schemas for structured ingestion.
# MAGIC
# MAGIC 3. Silver Streaming Tables:
# MAGIC    - silver_customers: Streams, parses, flattens, and deduplicates customer data from MongoDB.
# MAGIC    - silver_products: Streams, parses, flattens, and deduplicates product data from MongoDB.
# MAGIC
# MAGIC 4. SCD Type 2 CDC Flows:
# MAGIC    - silver_scd_customers: Applies SCD Type 2 CDC to customer data, tracking history for key attributes.
# MAGIC    - silver_scd_products: Applies SCD Type 2 CDC to product data, tracking history for key attributes.
# MAGIC
# MAGIC 5. Metadata & Data Quality:
# MAGIC    - Adds pipeline run and processing timestamps.
# MAGIC    - Ensures deduplication and type conversions.
# MAGIC
# MAGIC 6. Lakeflow Declarative Pipelines Features Used:
# MAGIC    - Streaming tables, Auto CDC, schema evolution, and dynamic table naming.
# MAGIC
# MAGIC 7. Imports:
# MAGIC    - Uses Lakeflow Declarative Pipelines (`import dlt`), YAML, and Spark SQL functions/types.
# MAGIC
# MAGIC ----------------------------------------------
# MAGIC """
# MAGIC
# MAGIC display(summary)

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
# MAGIC ##Define Schemas

# COMMAND ----------

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

# COMMAND ----------

# MAGIC %md
# MAGIC ##Silver Customers Table

# COMMAND ----------

@dlt.table(
    name=silver("silver_customers"),
    comment="Streaming silver customers from Mongo (flattened)"
)
def silver_customers():

    return (
        dlt.read_stream(tbl("bronze", "bronze_customers"))

        # Parse JSON
        .withColumn("parsed_data", from_json(col("data"), customers_data_schema))

        #  FLATTEN EVERYTHING
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

        #  DERIVED COLUMN
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

# COMMAND ----------

# MAGIC %md
# MAGIC ##Silver Products Table

# COMMAND ----------


@dlt.table(
    name=silver("silver_products"),
    comment="Streaming silver products from Mongo (flattened)"
)
def silver_products():

    return (
        dlt.read_stream(tbl("bronze", "bronze_products"))

        # Parse JSON
        .withColumn("parsed_data", from_json(col("data"), products_data_schema))

        #  FLATTEN EVERYTHING
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

# COMMAND ----------

# MAGIC %md
# MAGIC ##SCD Type 2: Customers

# COMMAND ----------

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

# COMMAND ----------

# MAGIC %md
# MAGIC ##SCD Type 2: Products

# COMMAND ----------

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
