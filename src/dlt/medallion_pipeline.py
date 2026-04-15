import dlt
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from common.config_loader import load_config


spark = SparkSession.getActiveSession()
if spark is None:
    spark = SparkSession.builder.getOrCreate()

CONFIG = load_config(spark.conf.get("config.path", "config/config.yml"))
EXPECTATIONS = CONFIG["dlt"]["expectations"]


@dlt.table(name="transactions_bronze", comment="Raw transactions from ADLS CSV")
def transactions_bronze():
    table = CONFIG["sources"]["csv"]["datasets"]["transactions"]["target_table"]
    return spark.read.table(table)


@dlt.table(name="customers_bronze", comment="Raw customers from ADLS CSV")
def customers_bronze():
    table = CONFIG["sources"]["csv"]["datasets"]["customers"]["target_table"]
    return spark.read.table(table)


@dlt.table(name="products_bronze", comment="Raw products from MongoDB")
def products_bronze():
    table = CONFIG["sources"]["mongodb"]["target_table"]
    return spark.read.table(table)


@dlt.table(name="transactions_silver", comment="Validated transactions")
@dlt.expect("valid_quantity", EXPECTATIONS["valid_quantity"])
@dlt.expect("valid_price", EXPECTATIONS["valid_price"])
def transactions_silver():
    return (
        dlt.read("transactions_bronze")
        .withColumn("transaction_date", F.to_date("transaction_ts"))
        .withColumn("revenue", F.col("quantity") * F.col("unit_price"))
    )


@dlt.table(name="customers_silver", comment="Latest customer records")
def customers_silver():
    return (
        dlt.read("customers_bronze")
        .withColumn("updated_at", F.coalesce(F.col("updated_at"), F.current_timestamp()))
    )


@dlt.table(name="fact_sales", comment="Gold fact table for sales")
def fact_sales():
    tx = dlt.read("transactions_silver")
    cust = dlt.read("customers_silver").select("customer_id", "region_id")
    return (
        tx.join(cust, on="customer_id", how="left")
        .groupBy("transaction_date", "store_id", "region_id", "product_id")
        .agg(
            F.sum("quantity").alias("units_sold"),
            F.sum("revenue").alias("gross_revenue"),
            F.countDistinct("transaction_id").alias("order_count"),
        )
    )
