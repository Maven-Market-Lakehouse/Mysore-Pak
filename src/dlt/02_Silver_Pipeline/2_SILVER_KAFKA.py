# Databricks notebook source
import dlt
import yaml
from pyspark.sql.functions import *
from pyspark.sql.types import *

config = yaml.safe_load(open("/Workspace/maven_market/Mysore-Pak/config/config.yml", "r"))
catalog = config["catalog"]
PIPELINE_RUN_ID = current_timestamp()

def tbl(layer, name):
    return f"{catalog}.{config[f'{layer}_schema']}.{config[name]}"

def silver(name):
    return f"{catalog}.{config['silver_schema']}.{name}"


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

# @dlt.table(name=silver("silver_orders"))
# def silver_orders():
#     return (
#         dlt.read_stream(tbl("bronze", "bronze_orders"))
#         .withColumn("json", from_json(col("raw_payload"), orders_schema))
#         .select("json.*", "timestamp")
#         .withColumn("event_timestamp", col("timestamp"))
#         .withWatermark("event_timestamp", "10 minutes")
#         .dropDuplicates(["order_id", "event_timestamp"])
#         .withColumn("pipeline_run_id", PIPELINE_RUN_ID)
#         .withColumn("processing_timestamp", current_timestamp())
#     )

@dlt.table(name=silver("silver_orders"))
def silver_orders():
    return (
        dlt.read_stream(tbl("bronze", "bronze_orders"))
        .withColumn("json", from_json(col("raw_payload"), orders_schema))
        .select("json.*", "timestamp")

        # 🔥 FIX 1: DATE PARSING
        .withColumn("order_date", to_date("order_date", "M/d/yyyy"))

        # # 🔥 FIX 2: STANDARDIZE ORDER ID
        # .withColumn(
        #     "order_id",
        #     when(col("order_id").startswith("ORDERID-"), col("order_id"))
        #     .otherwise(concat(lit("ORDERID-"), col("order_id")))
        # )

        .withColumn("event_timestamp", col("timestamp"))
        .withWatermark("event_timestamp", "10 minutes")
        .dropDuplicates(["order_id", "event_timestamp"])

        .withColumn("pipeline_run_id", PIPELINE_RUN_ID)
        .withColumn("processing_timestamp", current_timestamp())
    )

inventory_schema = StructType([
    StructField("inventory_id", StringType()),
    StructField("event_timestamp", StringType()),
    StructField("product_id", IntegerType()),
    StructField("store_id", IntegerType()),
    StructField("quantity_added", IntegerType()),
    StructField("quantity_remaining", IntegerType())
])
#common quarantine function:



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

        # ✅ Parse JSON payload
        .withColumn("json", from_json(col("raw_payload"), inventory_schema))

        # ✅ Flatten
        .select("json.*", "timestamp")

        # ✅ Proper timestamp handling
        .withColumn("event_timestamp", to_timestamp(col("event_timestamp")))
        .withColumn("processing_time", col("timestamp"))

        # ✅ Watermark (CRITICAL for Kafka)
        .withWatermark("event_timestamp", "10 minutes")

        # ✅ Deduplication (Kafka = at least once delivery)
        .dropDuplicates(["inventory_id"])

        # ✅ Business logic (from your friend's pattern)
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

        # ✅ Metadata
        .withColumn("pipeline_run_id", PIPELINE_RUN_ID)
        .withColumn("processing_timestamp", current_timestamp())
    )

INVENTORY_RULES = {
    "valid_product": "product_id IS NOT NULL",
    "valid_store": "store_id IS NOT NULL",
    "valid_stock": "quantity_remaining >= 0"
}

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

@dlt.table(name=silver("silver_quarantine_inventory"))
def quarantine_inventory():

    raw = dlt.read_stream(silver("silver_inventory"))

    _, quarantined = quarantine_filter(raw, INVENTORY_RULES)

    return quarantined


ORDERS_RULES = {
    "valid_order_id": "order_id IS NOT NULL",
    "valid_product": "product_id IS NOT NULL",
    "valid_customer": "customer_id IS NOT NULL",
    "valid_store": "store_id IS NOT NULL",
    "positive_qty": "quantity > 0"
}

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

@dlt.table(
    name=silver("silver_quarantine_orders"),
    comment="Invalid orders captured for debugging and audit"
)
def quarantine_orders():

    raw = dlt.read_stream(silver("silver_orders"))

    _, quarantined = quarantine_filter(raw, ORDERS_RULES)

    return quarantined

# #VALID ORDERS
# @dlt.table(
#     name=silver("silver_orders_valid"),
#     comment="Valid Kafka orders for Gold layer"
# )
# def silver_orders_valid():

#     ord = dlt.read_stream(silver("silver_orders"))

#     prod = dlt.read(silver("silver_scd_products")) \
#           .filter(col("__END_AT").isNull()) \
#           .select("product_id")

#     cust = dlt.read(silver("silver_scd_customers")) \
#           .filter(col("__END_AT").isNull()) \
#           .select("customer_id")

#     store = dlt.read(silver("silver_scd_stores")) \
#            .filter(col("__END_AT").isNull()) \
#            .select("store_id")

#     # prod = dlt.read(silver("silver_scd_products")).filter(col("__END_AT").isNull())
#     # cust = dlt.read(silver("silver_scd_customers")).filter(col("__END_AT").isNull())
#     # store = dlt.read(silver("silver_scd_stores")).filter(col("__END_AT").isNull())

#     return (
#         ord
#         .join(broadcast(prod), "product_id", "inner")
#         .join(broadcast(cust), "customer_id", "inner")
#         .join(broadcast(store), "store_id", "inner")

#         .filter(col("quantity") > 0)

#         .withColumn("record_status", lit("valid"))
#         .withColumn("pipeline_run_id", PIPELINE_RUN_ID)
#         .withColumn("processing_timestamp", current_timestamp())
#     )

# #QUARANTINE ORDERS

# @dlt.table(
#     name=silver("silver_quarantine_orders"),
#     comment="Invalid Kafka orders"
# )
# def quarantine_orders():

#     ord = dlt.read_stream(silver("silver_orders"))
#     prod = dlt.read(silver("silver_scd_products")) \
#           .filter(col("__END_AT").isNull()) \
#           .select("product_id")

#     cust = dlt.read(silver("silver_scd_customers")) \
#           .filter(col("__END_AT").isNull()) \
#           .select("customer_id")

#     store = dlt.read(silver("silver_scd_stores")) \
#            .filter(col("__END_AT").isNull()) \
#            .select("store_id")

#     # prod = dlt.read(silver("silver_scd_products")).select("product_id")
#     # cust = dlt.read(silver("silver_scd_customers")).select("customer_id")
#     # store = dlt.read(silver("silver_scd_stores")).select("store_id")

#     joined = (
#         ord
#         .join(prod, "product_id", "left")
#         .join(cust, "customer_id", "left")
#         .join(store, "store_id", "left")
#     )

#     return (
#         joined
#         .filter(
#             col("product_id").isNull() |
#             col("customer_id").isNull() |
#             col("store_id").isNull() |
#             (col("quantity") <= 0)
#         )
#         .withColumn(
#             "failure_reason",
#             when(col("product_id").isNull(), "INVALID_PRODUCT")
#             .when(col("customer_id").isNull(), "INVALID_CUSTOMER")
#             .when(col("store_id").isNull(), "INVALID_STORE")
#             .when(col("quantity") <= 0, "INVALID_QUANTITY")
#         )
#         .withColumn("record_status", lit("quarantined"))
#         .withColumn("pipeline_run_id", PIPELINE_RUN_ID)
#         .withColumn("processing_timestamp", current_timestamp())
#     )


# #VALID INVENTORY

# @dlt.table(
#     name=silver("silver_inventory_valid"),
#     comment="Valid inventory records for analytics"
# )
# def silver_inventory_valid():

#     inv = dlt.read_stream(silver("silver_inventory"))

#     prod = dlt.read(silver("silver_scd_products")) \
#               .filter(col("__END_AT").isNull()) \
#               .select("product_id")   # 🔥 ONLY KEY

#     store = dlt.read(silver("silver_scd_stores")) \
#                .filter(col("__END_AT").isNull()) \
#                .select("store_id")   # 🔥 ONLY KEY

#     return (
#         inv
#         .join(broadcast(prod), "product_id", "inner")
#         .join(broadcast(store), "store_id", "inner")

#         .filter(col("quantity_remaining") >= 0)

#         .withColumn("record_status", lit(config["record_status_valid"]))
#         .withColumn("pipeline_run_id", current_timestamp())
#         .withColumn("processing_timestamp", current_timestamp())
#     )
# # @dlt.table(
# #     name=silver("silver_inventory_valid"),
# #     comment="Valid inventory records for analytics"
# # )
# # def silver_inventory_valid():

# #     inv = dlt.read_stream(silver("silver_inventory"))

# #     prod = dlt.read(silver("silver_scd_products")).filter(col("__END_AT").isNull())
# #     store = dlt.read(silver("silver_scd_stores")).filter(col("__END_AT").isNull())

# #     return (
# #         inv
# #         .join(broadcast(prod), "product_id", "inner")
# #         .join(broadcast(store), "store_id", "inner")

# #         .filter(col("quantity_remaining") >= 0)

# #         .withColumn("record_status", lit("valid"))
# #         .withColumn("pipeline_run_id", PIPELINE_RUN_ID)
# #         .withColumn("processing_timestamp", current_timestamp())
# #     )

# #QUARANTINE INVENTORY

# @dlt.table(
#     name=silver("silver_quarantine_inventory"),
#     comment="Invalid inventory records"
# )
# def quarantine_inventory():

#     inv = dlt.read_stream(silver("silver_inventory"))

#     prod = dlt.read(silver("silver_scd_products")).select("product_id")
#     store = dlt.read(silver("silver_scd_stores")).select("store_id")

#     joined = (
#         inv
#         .join(prod, "product_id", "left")
#         .join(store, "store_id", "left")
#     )

#     return (
#         joined
#         .filter(
#             col("product_id").isNull() |
#             col("store_id").isNull() |
#             (col("quantity_remaining") < 0)
#         )
#         .withColumn(
#             "failure_reason",
#             when(col("product_id").isNull(), "INVALID_PRODUCT")
#             .when(col("store_id").isNull(), "INVALID_STORE")
#             .when(col("quantity_remaining") < 0, "NEGATIVE_STOCK")
#         )
#         .withColumn("record_status", lit("quarantined"))
#         .withColumn("pipeline_run_id", PIPELINE_RUN_ID)
#         .withColumn("processing_timestamp", current_timestamp())
#     )

# COMMAND ----------

# MAGIC %md
# MAGIC #Config and Import

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
# MAGIC
# MAGIC def tbl(layer, name):
# MAGIC     return f"{catalog}.{config[f'{layer}_schema']}.{config[name]}"

# COMMAND ----------

# MAGIC %md
# MAGIC ##ORDERS

# COMMAND ----------

# MAGIC %skip
# MAGIC orders_schema = StructType([
# MAGIC     StructField("order_id", StringType()),
# MAGIC     StructField("product_id", IntegerType()),
# MAGIC     StructField("customer_id", IntegerType()),
# MAGIC     StructField("store_id", IntegerType()),
# MAGIC     StructField("quantity", IntegerType()),
# MAGIC     StructField("order_date", StringType()),
# MAGIC     StructField("payment_type", StringType()),
# MAGIC     StructField("order_channel", StringType())
# MAGIC ])

# COMMAND ----------

# MAGIC %skip
# MAGIC @dlt.table(
# MAGIC     name=f"{catalog}.{config['silver_schema']}.silver_orders",
# MAGIC     comment="Stage-2 cleaned Kafka Orders"
# MAGIC )
# MAGIC @dlt.expect("valid_order_id", "order_id IS NOT NULL")
# MAGIC @dlt.expect("valid_quantity", "quantity > 0")
# MAGIC def silver_orders():
# MAGIC
# MAGIC     df = dlt.read_stream(tbl("bronze", "bronze_orders"))
# MAGIC
# MAGIC     parsed = (
# MAGIC         df
# MAGIC         # PARSE JSON
# MAGIC         .withColumn("json", from_json(col("raw_payload"), config["orders_schema"]))
# MAGIC         .select("json.*", "timestamp")
# MAGIC
# MAGIC         # EVENT TIME
# MAGIC         .withColumn("event_timestamp", col("timestamp"))
# MAGIC
# MAGIC         # TYPE CAST
# MAGIC         .withColumn("order_id", col("order_id").cast("string"))
# MAGIC         .withColumn("quantity", col("quantity").cast("int"))
# MAGIC
# MAGIC         # DEDUP (STREAM SAFE)
# MAGIC         .withWatermark("event_timestamp", "10 minutes")
# MAGIC         .dropDuplicates(["order_id", "event_timestamp"])
# MAGIC
# MAGIC         # METADATA
# MAGIC         .withColumn("processing_timestamp", current_timestamp())
# MAGIC         .withColumn("ingestion_source", lit("kafka"))
# MAGIC         .withColumn("record_status", lit("cleaned"))
# MAGIC
# MAGIC         # RESCUED DATA SAFE
# MAGIC         .withColumn("_rescued_data", col("_rescued_data"))
# MAGIC     )
# MAGIC
# MAGIC     return parsed

# COMMAND ----------

# MAGIC %md
# MAGIC ##INVENTORY

# COMMAND ----------

# MAGIC %skip
# MAGIC inventory_schema = StructType([
# MAGIC     StructField("inventory_id", StringType()),
# MAGIC     StructField("event_timestamp", StringType()),
# MAGIC     StructField("product_id", IntegerType()),
# MAGIC     StructField("store_id", IntegerType()),
# MAGIC     StructField("quantity_added", IntegerType()),
# MAGIC     StructField("quantity_remaining", IntegerType())
# MAGIC ])

# COMMAND ----------

# MAGIC %skip
# MAGIC @dlt.table(
# MAGIC     name=f"{catalog}.{config['silver_schema']}.silver_inventory",
# MAGIC     comment="Stage-2 cleaned Kafka Inventory"
# MAGIC )
# MAGIC @dlt.expect("valid_inventory_id", "inventory_id IS NOT NULL")
# MAGIC def silver_inventory():
# MAGIC
# MAGIC     df = dlt.read_stream(tbl("bronze", "bronze_inventory"))
# MAGIC
# MAGIC     parsed = (
# MAGIC         df
# MAGIC         .withColumn("json", from_json(col("raw_payload"), config["inventory_schema"]))
# MAGIC         .select("json.*", "timestamp")
# MAGIC
# MAGIC         # EVENT TIME
# MAGIC         .withColumn("event_timestamp", col("timestamp"))
# MAGIC
# MAGIC         # TYPE CAST
# MAGIC         .withColumn("inventory_id", col("inventory_id").cast("string"))
# MAGIC
# MAGIC         # DEDUP
# MAGIC         .withWatermark("event_timestamp", "10 minutes")
# MAGIC         .dropDuplicates(["inventory_id", "event_timestamp"])
# MAGIC
# MAGIC         # METADATA
# MAGIC         .withColumn("processing_timestamp", current_timestamp())
# MAGIC         .withColumn("ingestion_source", lit("kafka"))
# MAGIC         .withColumn("record_status", lit("cleaned"))
# MAGIC
# MAGIC         # RESCUED
# MAGIC         .withColumn("_rescued_data", col("_rescued_data"))
# MAGIC     )
# MAGIC
# MAGIC     return parsed

# COMMAND ----------

# MAGIC %skip
# MAGIC def silver(name):
# MAGIC     return f"{catalog}.{config['silver_schema']}.{name}"

# COMMAND ----------

# MAGIC %md
# MAGIC ##ORDERS (STREAM + FK VALIDATION)

# COMMAND ----------

# MAGIC %skip
# MAGIC @dlt.table(name=silver("silver_orders_valid"))
# MAGIC def silver_orders_valid():
# MAGIC
# MAGIC     ord = dlt.read_stream(silver("silver_orders"))
# MAGIC     cust = dlt.read(silver("silver_scd_customers")).filter(col("__IS_CURRENT") == True)
# MAGIC
# MAGIC     return (
# MAGIC         ord.join(broadcast(cust), "customer_id", "inner")
# MAGIC         .withColumn("record_status", lit("valid"))
# MAGIC         .withColumn("pipeline_run_id", PIPELINE_RUN_ID)
# MAGIC         .withColumn("processing_timestamp", current_timestamp())
# MAGIC     )

# COMMAND ----------

# MAGIC %md
# MAGIC ##QUARANTINE — ORDERS

# COMMAND ----------

# MAGIC %skip
# MAGIC @dlt.table(name=silver("silver_quarantine_orders"))
# MAGIC def quarantine_orders():
# MAGIC
# MAGIC     ord = dlt.read_stream(silver("silver_orders"))
# MAGIC     cust = dlt.read(silver("silver_scd_customers")).select("customer_id")
# MAGIC
# MAGIC     return (
# MAGIC         ord.join(cust, "customer_id", "left_anti")
# MAGIC         .withColumn("failed_fk_column", lit("customer_id"))
# MAGIC         .withColumn("failure_reason", lit("INVALID_CUSTOMER_FK"))
# MAGIC         .withColumn("record_status", lit("quarantined"))
# MAGIC         .withColumn("processing_timestamp", current_timestamp())
# MAGIC         .withColumn("pipeline_run_id", PIPELINE_RUN_ID)
# MAGIC     )

# COMMAND ----------

# MAGIC %md
# MAGIC ##INVENTORY

# COMMAND ----------

# MAGIC %skip
# MAGIC @dlt.table(name=silver("silver_inventory_valid"))
# MAGIC def silver_inventory_valid():
# MAGIC
# MAGIC     inv = dlt.read_stream(silver("silver_inventory"))
# MAGIC     prod = dlt.read(silver("silver_scd_products")).filter(col("__IS_CURRENT") == True)
# MAGIC
# MAGIC     return (
# MAGIC         inv.join(broadcast(prod), "product_id", "inner")
# MAGIC         .withColumn("record_status", lit("valid"))
# MAGIC         .withColumn("pipeline_run_id", PIPELINE_RUN_ID)
# MAGIC         .withColumn("processing_timestamp", current_timestamp())
# MAGIC     )

# COMMAND ----------

# MAGIC %md
# MAGIC ##QUARANTINE — INVENTORY

# COMMAND ----------

# MAGIC %skip
# MAGIC @dlt.table(name=silver("silver_quarantine_inventory"))
# MAGIC def quarantine_inventory():
# MAGIC
# MAGIC     inv = dlt.read_stream(silver("silver_inventory"))
# MAGIC     prod = dlt.read(silver("silver_scd_products")).select("product_id")
# MAGIC
# MAGIC     return (
# MAGIC         inv.join(prod, "product_id", "left_anti")
# MAGIC         .withColumn("failed_fk_column", lit("product_id"))
# MAGIC         .withColumn("failure_reason", lit("INVALID_PRODUCT_FK"))
# MAGIC         .withColumn("record_status", lit("quarantined"))
# MAGIC         .withColumn("processing_timestamp", current_timestamp())
# MAGIC         .withColumn("pipeline_run_id", PIPELINE_RUN_ID)
# MAGIC     )

# COMMAND ----------

# MAGIC %md
# MAGIC
