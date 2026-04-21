# Databricks notebook source
import dlt
import yaml
from pyspark.sql.functions import *
from pyspark.sql.functions import expr

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




# @dlt.table(
#     name=silver("silver_transactions"),
#     comment="Stage-1 cleaned transactions"
# )
# @dlt.expect("valid_quantity", "quantity > 0")
# @dlt.expect("valid_customer", "customer_id IS NOT NULL")
# @dlt.expect("valid_product", "product_id IS NOT NULL")
# def silver_transactions():

#     df = dlt.read_stream(tbl("bronze", "bronze_transactions"))

#     return (
#         df
#         # TYPE ENFORCEMENT
#         .withColumn("transaction_date", to_date("transaction_date"))
#         .withColumn("quantity", col("quantity").cast("int"))

#         # NULL HANDLING
#         #.fillna({"quantity": 0})
#         .withColumn("quantity", col("quantity").cast("int"))

#         # STANDARDIZATION
#         .withColumn("transaction_date", to_date("transaction_date"))

#         # DEDUP
#         .dropDuplicates(["product_id","customer_id","store_id","transaction_date"])

#         # RESCUED DATA SAFE KEEP
#         .withColumn("_rescued_data", col("_rescued_data"))

#         .withColumn("pipeline_run_id", current_timestamp())

#         # METADATA
#         .withColumn("processing_timestamp", current_timestamp())
#         .withColumn("source_table", lit("transactions"))
#         .withColumn(
#             "record_status",
#             when(col("quantity") <= 0, "invalid").otherwise("cleaned")
#         )
#     )

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
        # ✅ FIXED DATE PARSING
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

        # RESCUED
        # .withColumn("_rescued_data", col("_rescued_data"))

        # METADATA
        .withColumn("processing_timestamp", current_timestamp())
        .withColumn("source_table", lit("regions"))
        .withColumn("record_status", lit("cleaned"))
    )




# @dlt.table(
#     name=silver("silver_calendar"),
#     comment="Stage-1 cleaned calendar"
# )
# @dlt.expect("valid_date", "date IS NOT NULL")
# def silver_calendar():

#     df = dlt.read_stream(tbl("bronze", "bronze_calendar"))

#     return (
#         df
#         # TYPE CAST
#         .withColumn("date", to_date("date"))

#         # DEDUP
#         .dropDuplicates(["date"])

#         # RESCUED
#         .withColumn("_rescued_data", col("_rescued_data"))
#         .withColumn("pipeline_run_id", current_timestamp())

#         # METADATA
#         .withColumn("processing_timestamp", current_timestamp())
#         .withColumn("source_table", lit("calendar"))
#         .withColumn("record_status", lit("cleaned"))
#     )
# @dlt.table(
#     name=silver("silver_calendar"),
#     comment="Stage-1 cleaned calendar (streaming-safe)"
# )
# @dlt.expect("valid_date", "date IS NOT NULL")
# def silver_calendar():

#     df = dlt.read_stream(tbl("bronze", "bronze_calendar"))

#     return (
#         df
#         # TYPE CAST
#         .withColumn("date", to_date("date"))

#         # 🔥 REQUIRED FOR STREAMING DEDUP
#         .withColumn("event_ts", col("date").cast("timestamp"))

#         # 🔥 WATERMARK (CRITICAL)
#         .withWatermark("event_ts", "1 day")

#         # 🔥 STREAMING DEDUP
#         .dropDuplicates(["date"])

#         # CLEANUP HELPER COLUMN
#         .drop("event_ts")

#         # RESCUED
#         .withColumn("_rescued_data", col("_rescued_data"))

#         # METADATA
#         .withColumn("pipeline_run_id", current_timestamp())
#         .withColumn("processing_timestamp", current_timestamp())
#         .withColumn("source_table", lit("calendar"))
#         .withColumn("record_status", lit("cleaned"))
#     )

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

        # ❌ REMOVE _rescued_data

        .withColumn("pipeline_run_id", current_timestamp())
        .withColumn("processing_timestamp", current_timestamp())
        .withColumn("source_table", lit("calendar"))
        .withColumn("record_status", lit("cleaned"))
    )

# @dlt.table(
#     name=silver("silver_returns"),
#     comment="Stage-1 cleaned returns"
# )
# @dlt.expect("valid_quantity", "quantity > 0")
# def silver_returns():

#     df = dlt.read_stream(tbl("bronze", "bronze_returns"))

#     return (
#         df
#         # TYPE ENFORCEMENT
#         .withColumn("return_date", to_date("return_date"))
#         .withColumn("quantity", col("quantity").cast("int"))

#         # NULL HANDLING
#         .fillna({"quantity": 0})

#         # DEDUP
#         .dropDuplicates(["product_id","store_id","return_date"])

#         # RESCUED DATA
#         .withColumn("_rescued_data", col("_rescued_data"))
#         .withColumn("pipeline_run_id", current_timestamp())

#         # METADATA
#         .withColumn("processing_timestamp", current_timestamp())
#         .withColumn("source_table", lit("returns"))
#         .withColumn(
#             "record_status",
#             when(col("quantity") <= 0, "invalid").otherwise("cleaned")
#         )
#     )

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
        # 🔥 FIXED DATE PARSING
        .withColumn("return_date", to_date("return_date", "M/d/yyyy"))

        # TYPE CAST
        .withColumn("quantity", col("quantity").cast("int"))

        # ❌ REMOVE THIS (IMPORTANT)
        # .fillna({"quantity": 0})

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

dlt.create_streaming_table(silver("silver_scd_stores"))

dlt.apply_changes(
    target=silver("silver_scd_stores"),
    source=silver("silver_stores"),
    keys=[config["stores_pk"]],
    sequence_by=col("processing_timestamp"),
    stored_as_scd_type=2,
    track_history_column_list=config["stores_scd_columns"]
)




# @dlt.table(name=silver("silver_transactions_valid"))
# def silver_transactions_valid():

#     tx = dlt.read_stream(silver("silver_transactions"))

#     prod = dlt.read_stream(silver("silver_scd_products")) \
#               .filter(col("__IS_CURRENT") == True) \
#               .select("product_id")

#     cust = dlt.read_stream(silver("silver_scd_customers")) \
#               .filter(col("__IS_CURRENT") == True) \
#               .select("customer_id")

#     store = dlt.read_stream(silver("silver_scd_stores")) \
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


#VALID TRANSACTIONS

# @dlt.table(
#     name=silver("silver_transactions_valid"),
#     comment="Only valid transactions used for Gold layer"
# )
# def silver_transactions_valid():

#     tx = dlt.read_stream(silver("silver_transactions"))

#     prod = dlt.read(silver("silver_scd_products")) \
#               .filter(col("__END_AT").isNull()) \
#               .select("product_id")

#     cust = dlt.read(silver("silver_scd_customers")) \
#               .filter(col("__END_AT").isNull()) \
#               .select("customer_id")

#     store = dlt.read(silver("silver_scd_stores")) \
#                .filter(col("__END_AT").isNull()) \
#                .select("store_id")

#     return (
#         tx
#         .join(broadcast(prod), "product_id", "inner")
#         .join(broadcast(cust), "customer_id", "inner")
#         .join(broadcast(store), "store_id", "inner")

#         .filter(col("quantity") > 0)

#         .withColumn("record_status", lit("valid"))
#         .withColumn("pipeline_run_id", current_timestamp())
#         .withColumn("processing_timestamp", current_timestamp())
#     )

#QUARANTINE TRANSACTIONS
# @dlt.table(
#     name=silver("silver_quarantine_transactions"),
#     comment="Invalid transactions (FK + data quality failures)"
# )
# def quarantine_transactions():

#     tx = dlt.read_stream(silver("silver_transactions"))

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
#         tx
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
#         .withColumn("pipeline_run_id", current_timestamp())
#         .withColumn("processing_timestamp", current_timestamp())
#     )

#     def quarantine_filter(df, rules):
#     valid_condition = " AND ".join(f"({v})" for v in rules.values())
#     invalid_condition = f"NOT ({valid_condition})"

#     clean_df = df.filter(expr(valid_condition))

#     quarantine_df = (
#         df.filter(expr(invalid_condition))
#         .withColumn("_quarantine_timestamp", current_timestamp())
#         .withColumn(
#             "_quarantine_reason",
#             concat_ws(", ", *[
#                 when(~expr(cond), lit(name)) for name, cond in rules.items()
#             ])
#         )
#     )

#     return clean_df, quarantine_df


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

TRANSACTION_RULES = {
    "valid_product": "product_id IS NOT NULL",
    "valid_customer": "customer_id IS NOT NULL",
    "valid_store": "store_id IS NOT NULL",
    "positive_qty": "quantity > 0",
    "valid_date": "transaction_date IS NOT NULL"
}
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

@dlt.table(name=silver("silver_quarantine_transactions"))
def quarantine_transactions():

    raw = dlt.read_stream(silver("silver_transactions"))

    _, quarantined = quarantine_filter(raw, TRANSACTION_RULES)

    return quarantined
    


# COMMAND ----------

# MAGIC %md
# MAGIC #(CONFIG + HELPERS)

# COMMAND ----------

# MAGIC %skip
# MAGIC import dlt
# MAGIC import yaml
# MAGIC from pyspark.sql.functions import *
# MAGIC from pyspark.sql.window import Window
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
# MAGIC     return (f"{catalog}.{config[f'{layer}_schema']}.{config[name]}"
# MAGIC     )
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##SILVER: TRANSACTIONS

# COMMAND ----------

# MAGIC %skip
# MAGIC @dlt.table(
# MAGIC     name=f"{catalog}.{config['silver_schema']}.silver_transactions",
# MAGIC     comment="Stage-1 cleaned transactions"
# MAGIC )
# MAGIC @dlt.expect("valid_quantity", "quantity > 0")
# MAGIC @dlt.expect("valid_customer", "customer_id IS NOT NULL")
# MAGIC @dlt.expect("valid_product", "product_id IS NOT NULL")
# MAGIC def silver_transactions():
# MAGIC
# MAGIC     df = dlt.read_stream(tbl("bronze", "bronze_transactions"))
# MAGIC
# MAGIC     return (
# MAGIC         df
# MAGIC         # TYPE ENFORCEMENT
# MAGIC         .withColumn("transaction_date", to_date("transaction_date"))
# MAGIC         .withColumn("quantity", col("quantity").cast("int"))
# MAGIC
# MAGIC         # NULL HANDLING
# MAGIC         .fillna({"quantity": 0})
# MAGIC
# MAGIC         # STANDARDIZATION
# MAGIC         .withColumn("transaction_date", to_date("transaction_date"))
# MAGIC
# MAGIC         # DEDUP
# MAGIC         .dropDuplicates(["product_id","customer_id","store_id","transaction_date"])
# MAGIC
# MAGIC         # RESCUED DATA SAFE KEEP
# MAGIC         .withColumn("_rescued_data", col("_rescued_data"))
# MAGIC
# MAGIC         .withColumn("pipeline_run_id", current_timestamp())
# MAGIC
# MAGIC         # METADATA
# MAGIC         .withColumn("processing_timestamp", current_timestamp())
# MAGIC         .withColumn("source_table", lit("transactions"))
# MAGIC         .withColumn(
# MAGIC             "record_status",
# MAGIC             when(col("quantity") <= 0, "invalid").otherwise("cleaned")
# MAGIC         )
# MAGIC     )

# COMMAND ----------

# MAGIC %md
# MAGIC ##RETURNS

# COMMAND ----------

# MAGIC %skip
# MAGIC @dlt.table(
# MAGIC     name=f"{catalog}.{config['silver_schema']}.silver_returns",
# MAGIC     comment="Stage-1 cleaned returns"
# MAGIC )
# MAGIC @dlt.expect("valid_quantity", "quantity > 0")
# MAGIC def silver_returns():
# MAGIC
# MAGIC     df = dlt.read_stream(tbl("bronze", "bronze_returns"))
# MAGIC
# MAGIC     return (
# MAGIC         df
# MAGIC         # TYPE ENFORCEMENT
# MAGIC         .withColumn("return_date", to_date("return_date"))
# MAGIC         .withColumn("quantity", col("quantity").cast("int"))
# MAGIC
# MAGIC         # NULL HANDLING
# MAGIC         .fillna({"quantity": 0})
# MAGIC
# MAGIC         # DEDUP
# MAGIC         .dropDuplicates(["product_id","store_id","return_date"])
# MAGIC
# MAGIC         # RESCUED DATA
# MAGIC         .withColumn("_rescued_data", col("_rescued_data"))
# MAGIC         .withColumn("pipeline_run_id", current_timestamp())
# MAGIC
# MAGIC         # METADATA
# MAGIC         .withColumn("processing_timestamp", current_timestamp())
# MAGIC         .withColumn("source_table", lit("returns"))
# MAGIC         .withColumn(
# MAGIC             "record_status",
# MAGIC             when(col("quantity") <= 0, "invalid").otherwise("cleaned")
# MAGIC         )
# MAGIC     )

# COMMAND ----------

# MAGIC %md
# MAGIC ##STORES

# COMMAND ----------

# MAGIC %skip
# MAGIC @dlt.table(
# MAGIC     name=f"{catalog}.{config['silver_schema']}.silver_stores",
# MAGIC     comment="Stage-1 cleaned stores"
# MAGIC )
# MAGIC @dlt.expect("valid_store", "store_id IS NOT NULL")
# MAGIC def silver_stores():
# MAGIC
# MAGIC     df = dlt.read_stream(tbl("bronze", "bronze_stores"))
# MAGIC
# MAGIC     return (
# MAGIC         df
# MAGIC         # TYPE CAST
# MAGIC         .withColumn("store_id", col("store_id").cast("int"))
# MAGIC         .withColumn("region_id", col("region_id").cast("int"))
# MAGIC
# MAGIC         # STRING CLEANING
# MAGIC         .withColumn("store_name", trim(col("store_name")))
# MAGIC         .withColumn("store_city", initcap(col("store_city")))
# MAGIC         .withColumn("store_state", upper(col("store_state")))
# MAGIC
# MAGIC         # DATE STANDARDIZATION
# MAGIC         .withColumn("first_opened_date", to_date("first_opened_date"))
# MAGIC         .withColumn("last_remodel_date", to_date("last_remodel_date"))
# MAGIC
# MAGIC         # DEDUP
# MAGIC         .dropDuplicates(["store_id"])
# MAGIC
# MAGIC         # RESCUED
# MAGIC         .withColumn("_rescued_data", col("_rescued_data"))
# MAGIC         .withColumn("pipeline_run_id", current_timestamp())
# MAGIC
# MAGIC         # METADATA
# MAGIC         .withColumn("processing_timestamp", current_timestamp())
# MAGIC         .withColumn("source_table", lit("stores"))
# MAGIC         .withColumn("record_status", lit("cleaned"))
# MAGIC     )

# COMMAND ----------

# MAGIC %md
# MAGIC ##REGIONS

# COMMAND ----------

# MAGIC %skip
# MAGIC @dlt.table(
# MAGIC     name=f"{catalog}.{config['silver_schema']}.silver_regions",
# MAGIC     comment="Stage-1 cleaned regions"
# MAGIC )
# MAGIC @dlt.expect("valid_region", "region_id IS NOT NULL")
# MAGIC def silver_regions():
# MAGIC
# MAGIC     df = dlt.read_stream(tbl("bronze", "bronze_regions"))
# MAGIC
# MAGIC     return (
# MAGIC         df
# MAGIC         # TYPE CAST
# MAGIC         .withColumn("region_id", col("region_id").cast("int"))
# MAGIC
# MAGIC         # STANDARDIZATION
# MAGIC         .withColumn("sales_region", initcap(col("sales_region")))
# MAGIC         .withColumn("sales_district", initcap(col("sales_district")))
# MAGIC
# MAGIC         # DEDUP
# MAGIC         .dropDuplicates(["region_id"])
# MAGIC         .withColumn("pipeline_run_id", current_timestamp())
# MAGIC
# MAGIC         # RESCUED
# MAGIC         .withColumn("_rescued_data", col("_rescued_data"))
# MAGIC
# MAGIC         # METADATA
# MAGIC         .withColumn("processing_timestamp", current_timestamp())
# MAGIC         .withColumn("source_table", lit("regions"))
# MAGIC         .withColumn("record_status", lit("cleaned"))
# MAGIC     )

# COMMAND ----------

# MAGIC %md
# MAGIC ##CALENDAR

# COMMAND ----------

# MAGIC %skip
# MAGIC @dlt.table(
# MAGIC     name=f"{catalog}.{config['silver_schema']}.silver_calendar",
# MAGIC     comment="Stage-1 cleaned calendar"
# MAGIC )
# MAGIC @dlt.expect("valid_date", "date IS NOT NULL")
# MAGIC def silver_calendar():
# MAGIC
# MAGIC     df = dlt.read_stream(tbl("bronze", "bronze_calendar"))
# MAGIC
# MAGIC     return (
# MAGIC         df
# MAGIC         # TYPE CAST
# MAGIC         .withColumn("date", to_date("date"))
# MAGIC
# MAGIC         # DEDUP
# MAGIC         .dropDuplicates(["date"])
# MAGIC
# MAGIC         # RESCUED
# MAGIC         .withColumn("_rescued_data", col("_rescued_data"))
# MAGIC         .withColumn("pipeline_run_id", current_timestamp())
# MAGIC
# MAGIC         # METADATA
# MAGIC         .withColumn("processing_timestamp", current_timestamp())
# MAGIC         .withColumn("source_table", lit("calendar"))
# MAGIC         .withColumn("record_status", lit("cleaned"))
# MAGIC     )

# COMMAND ----------

# MAGIC %md
# MAGIC **Function**

# COMMAND ----------

# MAGIC %skip
# MAGIC def silver(name):
# MAGIC     return f"{catalog}.{config['silver_schema']}.{name}"

# COMMAND ----------

# MAGIC %md
# MAGIC ##SCD TYPE 2 — STORES

# COMMAND ----------

# MAGIC %skip
# MAGIC dlt.create_streaming_table(silver("silver_scd_stores"))
# MAGIC
# MAGIC dlt.apply_changes(
# MAGIC     target=silver("silver_scd_stores"),
# MAGIC     source=silver("silver_stores"),
# MAGIC     keys=[config["stores_pk"]],
# MAGIC     sequence_by=col("processing_timestamp"),
# MAGIC     stored_as_scd_type=2,
# MAGIC     track_history_column_list=config["stores_scd_columns"]
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ##STORES ↔ REGIONS (FK VALIDATION)

# COMMAND ----------

# MAGIC %skip
# MAGIC @dlt.table(name=silver("silver_stores_valid"))
# MAGIC def silver_stores_valid():
# MAGIC
# MAGIC     stores = dlt.read_stream(silver("silver_stores"))
# MAGIC     regions = dlt.read_stream(silver("silver_regions")).select("region_id")
# MAGIC
# MAGIC     return (
# MAGIC         stores
# MAGIC         .join(regions, "region_id", "inner")
# MAGIC         .withColumn("record_status", lit("valid"))
# MAGIC         .withColumn("pipeline_run_id", current_timestamp())
# MAGIC         
# MAGIC         .withColumn("processing_timestamp", current_timestamp())
# MAGIC     )

# COMMAND ----------

# MAGIC %md
# MAGIC ##QUARANTINE — STORES

# COMMAND ----------

# MAGIC %skip
# MAGIC @dlt.table(name=silver("silver_quarantine_stores"))
# MAGIC def quarantine_stores():
# MAGIC
# MAGIC     stores = dlt.read_stream(silver("silver_stores"))
# MAGIC     regions = dlt.read_stream(silver("silver_regions")).select("region_id")
# MAGIC
# MAGIC     return (
# MAGIC         stores
# MAGIC         .join(regions, "region_id", "left_anti")
# MAGIC         .withColumn("failed_fk_column", lit("region_id"))
# MAGIC         .withColumn("failure_reason", lit("INVALID_REGION_FK"))
# MAGIC         .withColumn("record_status", lit("quarantined"))
# MAGIC         .withColumn("processing_timestamp", current_timestamp())
# MAGIC         .withColumn("pipeline_run_id", PIPELINE_RUN_ID)
# MAGIC         .withColumn("pipeline_run_id", current_timestamp())
# MAGIC     )

# COMMAND ----------

# MAGIC %md
# MAGIC ##TRANSACTIONS — FULL FK VALIDATION

# COMMAND ----------

# MAGIC %skip
# MAGIC @dlt.table(name=silver("silver_transactions_valid"))
# MAGIC def silver_transactions_valid():
# MAGIC
# MAGIC     tx = dlt.read_stream(silver("silver_transactions"))
# MAGIC
# MAGIC     prod = dlt.read_stream(silver("silver_scd_products")) \
# MAGIC               .filter(col("__END_AT").isNull()) \
# MAGIC               .select("product_id")
# MAGIC
# MAGIC     cust = dlt.read_stream(silver("silver_scd_customers")) \
# MAGIC               .filter(col("__END_AT").isNull()) \
# MAGIC               .select("customer_id")
# MAGIC
# MAGIC     store = dlt.read_stream(silver("silver_scd_stores")) \
# MAGIC                .filter(col("__END_AT").isNull()) \
# MAGIC                .select("store_id")
# MAGIC
# MAGIC     joined = (
# MAGIC         tx
# MAGIC         .join(broadcast(prod), "product_id", "left")
# MAGIC         .join(broadcast(cust), "customer_id", "left")
# MAGIC         .join(broadcast(store), "store_id", "left")
# MAGIC     )
# MAGIC
# MAGIC     valid = joined.filter(
# MAGIC         col("product_id").isNotNull() &
# MAGIC         col("customer_id").isNotNull() &
# MAGIC         col("store_id").isNotNull()
# MAGIC     )
# MAGIC
# MAGIC     return (
# MAGIC         valid
# MAGIC         .withColumn("record_status", lit(config["record_status_valid"]))
# MAGIC         .withColumn("pipeline_run_id", current_timestamp())
# MAGIC         .withColumn("processing_timestamp", current_timestamp())
# MAGIC     )
# MAGIC
# MAGIC # @dlt.table(name=silver("silver_transactions_valid"))
# MAGIC # def silver_transactions_valid():
# MAGIC
# MAGIC #     tx = dlt.read_stream(silver("silver_transactions"))
# MAGIC
# MAGIC #     prod = dlt.read_stream(silver("silver_scd_products")) \
# MAGIC #               .filter(col("__IS_CURRENT") == True) \
# MAGIC #               .select("product_id")
# MAGIC
# MAGIC #     cust = dlt.read_stream(silver("silver_scd_customers")) \
# MAGIC #               .filter(col("__IS_CURRENT") == True) \
# MAGIC #               .select("customer_id")
# MAGIC
# MAGIC #     store = dlt.read_stream(silver("silver_scd_stores")) \
# MAGIC #                .filter(col("__IS_CURRENT") == True) \
# MAGIC #                .select("store_id")
# MAGIC
# MAGIC #     # 🔶 LEFT JOIN (NO DATA LOSS)
# MAGIC #     joined = (
# MAGIC #         tx
# MAGIC #         .join(broadcast(prod), "product_id", "left")
# MAGIC #         .join(broadcast(cust), "customer_id", "left")
# MAGIC #         .join(broadcast(store), "store_id", "left")
# MAGIC #     )
# MAGIC
# MAGIC #     # 🔶 KEEP ONLY VALID RECORDS
# MAGIC #     valid = joined.filter(
# MAGIC #         col("product_id").isNotNull() &
# MAGIC #         col("customer_id").isNotNull() &
# MAGIC #         col("store_id").isNotNull()
# MAGIC #     )
# MAGIC
# MAGIC #     return (
# MAGIC #         valid
# MAGIC #         .withColumn("record_status", lit(config["record_status_valid"]))
# MAGIC #         .withColumn("pipeline_run_id", current_timestamp())   # ✅ FIXED
# MAGIC #         .withColumn("processing_timestamp", current_timestamp())
# MAGIC #     )

# COMMAND ----------

# MAGIC %md
# MAGIC ##QUARANTINE — TRANSACTIONS (ALL FK CASES)

# COMMAND ----------

# MAGIC %skip
# MAGIC @dlt.table(name=silver("silver_quarantine_transactions"))
# MAGIC def quarantine_transactions():
# MAGIC
# MAGIC     tx = dlt.read_stream(silver("silver_transactions"))
# MAGIC
# MAGIC     prod = dlt.read_stream(silver("silver_scd_products")) \
# MAGIC               .filter(col("__END_AT").isNull()) \
# MAGIC               .select("product_id")
# MAGIC
# MAGIC     cust = dlt.read_stream(silver("silver_scd_customers")) \
# MAGIC               .filter(col("__END_AT").isNull()) \
# MAGIC               .select("customer_id")
# MAGIC
# MAGIC     store = dlt.read_stream(silver("silver_scd_stores")) \
# MAGIC                .filter(col("__END_AT").isNull()) \
# MAGIC                .select("store_id")
# MAGIC
# MAGIC     # 🔶 SINGLE JOIN (NO DUPLICATION ISSUE)
# MAGIC     joined = (
# MAGIC         tx
# MAGIC         .join(prod, "product_id", "left")
# MAGIC         .join(cust, "customer_id", "left")
# MAGIC         .join(store, "store_id", "left")
# MAGIC     )
# MAGIC
# MAGIC     # 🔶 IDENTIFY INVALID RECORDS
# MAGIC     invalid = joined.filter(
# MAGIC         col("product_id").isNull() |
# MAGIC         col("customer_id").isNull() |
# MAGIC         col("store_id").isNull()
# MAGIC     )
# MAGIC
# MAGIC     return (
# MAGIC         invalid
# MAGIC         .withColumn(
# MAGIC             "failed_fk_column",
# MAGIC             when(col("product_id").isNull(), lit("product_id"))
# MAGIC             .when(col("customer_id").isNull(), lit("customer_id"))
# MAGIC             .when(col("store_id").isNull(), lit("store_id"))
# MAGIC         )
# MAGIC         .withColumn(
# MAGIC             "failure_reason",
# MAGIC             when(col("product_id").isNull(), lit("INVALID_PRODUCT_FK"))
# MAGIC             .when(col("customer_id").isNull(), lit("INVALID_CUSTOMER_FK"))
# MAGIC             .when(col("store_id").isNull(), lit("INVALID_STORE_FK"))
# MAGIC         )
# MAGIC         .withColumn("record_status", lit(config["record_status_quarantined"]))
# MAGIC         .withColumn("processing_timestamp", current_timestamp())
# MAGIC         .withColumn("pipeline_run_id", current_timestamp())   # ✅ FIXED
# MAGIC     )

# COMMAND ----------

# MAGIC %md
# MAGIC ##RETURNS — FK VALIDATION

# COMMAND ----------

# MAGIC %skip
# MAGIC @dlt.table(name=silver("silver_returns_valid"))
# MAGIC def silver_returns_valid():
# MAGIC
# MAGIC     ret = dlt.read_stream(silver("silver_returns"))
# MAGIC     prod = dlt.read_stream(silver("silver_scd_products")).filter(col("__END_AT").isNull())
# MAGIC     store = dlt.read_stream(silver("silver_scd_stores")).filter(col("__END_AT").isNull())
# MAGIC
# MAGIC     return (
# MAGIC         ret
# MAGIC         .join(prod, "product_id", "inner")
# MAGIC         .join(store, "store_id", "inner")
# MAGIC         .withColumn("record_status", lit("valid"))
# MAGIC         .withColumn("pipeline_run_id", current_timestamp())
# MAGIC         .withColumn("processing_timestamp", current_timestamp())
# MAGIC     )

# COMMAND ----------

# MAGIC %md
# MAGIC ##QUARANTINE — RETURNS

# COMMAND ----------

# MAGIC %skip
# MAGIC @dlt.table(name=silver("silver_quarantine_returns"))
# MAGIC def quarantine_returns():
# MAGIC
# MAGIC     ret = dlt.read_stream(silver("silver_returns"))
# MAGIC
# MAGIC     prod = dlt.read_stream(silver("silver_scd_products")).select("product_id")
# MAGIC     store = dlt.read_stream(silver("silver_scd_stores")).select("store_id")
# MAGIC
# MAGIC     invalid_product = ret.join(prod, "product_id", "left_anti") \
# MAGIC         .withColumn("failed_fk_column", lit("product_id")) \
# MAGIC         .withColumn("failure_reason", lit("INVALID_PRODUCT_FK"))
# MAGIC
# MAGIC     invalid_store = ret.join(store, "store_id", "left_anti") \
# MAGIC         .withColumn("failed_fk_column", lit("store_id")) \
# MAGIC         .withColumn("failure_reason", lit("INVALID_STORE_FK"))
# MAGIC
# MAGIC     return (
# MAGIC         invalid_product
# MAGIC         .unionByName(invalid_store)
# MAGIC         .withColumn("record_status", lit("quarantined"))
# MAGIC         .withColumn("processing_timestamp", current_timestamp())
# MAGIC         .withColumn("pipeline_run_id", PIPELINE_RUN_ID)
# MAGIC     )

# COMMAND ----------

# MAGIC %md
# MAGIC
