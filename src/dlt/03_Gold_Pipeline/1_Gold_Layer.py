# Databricks notebook source
# DBTITLE 1,Cell 1
import dlt
import yaml
from pyspark.sql.functions import *
from pyspark.sql.window import Window
def load_config(path="/Workspace/maven_market/Mysore-Pak/config/config.yml"):
    with open(path, "r") as f:
        return yaml.safe_load(f)

config = load_config()
catalog = config["catalog"]

def silver(name):
    return f"{catalog}.{config['silver_schema']}.{name}"

@dlt.table(
    name=f"{catalog}.{config['gold_schema']}.dim_customer",
    comment="Gold Dimension - Customer (SCD Type 2 Current Snapshot)"
)
def dim_customer():

    return (
        dlt.read(silver("silver_scd_customers"))
        .filter(col("__END_AT").isNull())
        .select(
            col("customer_id"),
            col("customer_name"),
            col("customer_city"),
            col("customer_state_province"),
            col("customer_country"),
            col("gender"),
            col("marital_status"),
            col("yearly_income"),
            col("education"),
            col("occupation"),
            col("homeowner")
        )
    )

# COMMAND ----------

@dlt.table(
    name=f"{catalog}.{config['gold_schema']}.dim_product",
    comment="Gold Dimension - Product"
)
def dim_product():

    return (
        dlt.read(silver("silver_scd_products"))
        .filter(col("__END_AT").isNull())

        .select(
            col("product_id"),
            col("product_name"),
            col("product_brand"),
            col("product_retail_price"),
            col("product_cost"),
            col("product_weight"),
            col("recyclable"),
            col("low_fat")
        )
    )

# COMMAND ----------

@dlt.table(
    name=f"{catalog}.{config['gold_schema']}.dim_store",
    comment="Gold Dimension - Store"
)
def dim_store():

    stores = dlt.read(silver("silver_scd_stores")) \
        .filter(col("__END_AT").isNull())

    regions = dlt.read(silver("silver_regions"))

    return (
        stores
        .join(regions, "region_id", "left")

        .select(
            "store_id",
            "store_name",
            "store_city",
            "store_state",
            "region_id",
            "sales_region",
            "sales_district"
        )
    )

# COMMAND ----------

@dlt.table(
    name=f"{catalog}.{config['gold_schema']}.fact_sales",
    comment="Gold Fact - Sales"
)
def fact_sales():

    # Transactions
    tx = dlt.read(silver("silver_transactions_valid")).select(
        "product_id",
        "customer_id",
        "store_id",
        col("transaction_date"),
        "quantity"
    )

    # Orders (FIXED HERE)
    orders = dlt.read(silver("silver_orders_valid")).select(
        "product_id",
        "customer_id",
        "store_id",
        col("order_date").alias("transaction_date"),  # ✅ correct column
        "quantity"
    )

    # Union
    df = tx.unionByName(orders)

    # Product price
    prod = dlt.read(silver("silver_scd_products")) \
        .filter(col("__END_AT").isNull()) \
        .select("product_id", "product_retail_price", "product_cost")
# .select("product_id", "product_retail_price")
    return (
        df
        .join(prod, "product_id", "left")
        # .withColumn(
        #     "sales_amount",
        #     col("quantity") * col("product_retail_price")
        # )
        .withColumn("total_revenue", col("quantity") * col("product_retail_price"))
        .withColumn("total_cost", col("quantity") * col("product_cost"))
        .withColumn("total_profit", col("total_revenue") - col("total_cost"))
        .select(
            "transaction_date",
            "customer_id",
            "product_id",
            "store_id",
            "quantity",
            "total_revenue",
            "total_cost",
            "total_profit"
        )
        # .select(
        #     "transaction_date",
        #     "customer_id",
        #     "product_id",
        #     "store_id",
        #     "quantity",
        #     "sales_amount"
        # )
    )

# COMMAND ----------

# MAGIC %skip
# MAGIC @dlt.table(
# MAGIC     name=f"{catalog}.{config['gold_schema']}.fact_sales",
# MAGIC     comment="Gold Fact - Sales"
# MAGIC )
# MAGIC def fact_sales():
# MAGIC
# MAGIC     # Transactions
# MAGIC     tx = dlt.read(silver("silver_transactions_valid")).select(
# MAGIC         "product_id",
# MAGIC         "customer_id",
# MAGIC         "store_id",
# MAGIC         col("transaction_date"),
# MAGIC         "quantity"
# MAGIC     )
# MAGIC
# MAGIC     # Orders → FIX COLUMN NAME HERE
# MAGIC     orders = dlt.read(silver("silver_orders_valid")).select(
# MAGIC         "product_id",
# MAGIC         "customer_id",
# MAGIC         "store_id",
# MAGIC         col("stock_date").alias("transaction_date"),   # 🔥 FIX
# MAGIC         "quantity"
# MAGIC     )
# MAGIC
# MAGIC     # Now union works
# MAGIC     df = tx.unionByName(orders)
# MAGIC
# MAGIC     prod = dlt.read(silver("silver_scd_products")) \
# MAGIC         .filter(col("__END_AT").isNull()) \
# MAGIC         .select("product_id", "product_retail_price")
# MAGIC
# MAGIC     return (
# MAGIC         df
# MAGIC         .join(prod, "product_id", "left")
# MAGIC         .withColumn(
# MAGIC             "sales_amount",
# MAGIC             col("quantity") * col("product_retail_price")
# MAGIC         )
# MAGIC     )

# COMMAND ----------

# DBTITLE 1,Cell 4
# MAGIC %skip
# MAGIC @dlt.table(
# MAGIC     name=f"{catalog}.{config['gold_schema']}.fact_sales",
# MAGIC     comment="Gold Fact - Sales"
# MAGIC )
# MAGIC def fact_sales():
# MAGIC
# MAGIC     tx = dlt.read(silver("silver_transactions_valid"))
# MAGIC
# MAGIC     orders = dlt.read(silver("silver_orders_valid")).select(
# MAGIC         "product_id",
# MAGIC         "customer_id",
# MAGIC         "store_id",
# MAGIC         col("order_date").alias("transaction_date"),
# MAGIC         "quantity"
# MAGIC     )
# MAGIC
# MAGIC     df = tx.unionByName(orders)
# MAGIC     prod = dlt.read(silver("silver_scd_products")) \
# MAGIC         .filter(col("__END_AT").isNull()) \
# MAGIC         .select("product_id", "product_retail_price")
# MAGIC
# MAGIC     return (
# MAGIC         df
# MAGIC         .join(prod, "product_id", "left")
# MAGIC
# MAGIC         .withColumn(
# MAGIC             "sales_amount",
# MAGIC             col("quantity") * col("product_retail_price")
# MAGIC         )
# MAGIC
# MAGIC         .select(
# MAGIC             
# MAGIC             col("transaction_date"),
# MAGIC             col("customer_id"),
# MAGIC             col("product_id"),
# MAGIC             col("store_id"),
# MAGIC             col("quantity"),
# MAGIC             col("sales_amount")
# MAGIC         )
# MAGIC     )

# COMMAND ----------

@dlt.table(
    name=f"{catalog}.{config['gold_schema']}.fact_returns",
    comment="Gold Fact - Returns"
)
def fact_returns():

    returns = dlt.read(silver("silver_returns"))

    prod = dlt.read(silver("silver_scd_products")) \
        .filter(col("__END_AT").isNull()) \
        .select("product_id", "product_retail_price")

    return (
        returns
        .join(prod, "product_id", "left")

        .withColumn(
            "return_amount",
            col("quantity") * col("product_retail_price")
        )

        .select(
            col("product_id"),
            col("store_id"),
            col("return_date"),
            col("quantity").alias("return_quantity"),
            col("return_amount")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC #AGG DAILY SALES (OPTIMIZED)

# COMMAND ----------


@dlt.table(
    name=f"{catalog}.{config['gold_schema']}.agg_daily_sales",
    comment="Daily sales by store"
)
def agg_daily_sales():

    sales = dlt.read(f"{catalog}.{config['gold_schema']}.fact_sales")
    stores = dlt.read(f"{catalog}.{config['gold_schema']}.dim_store")

    return (
        sales
        .groupBy("transaction_date", "store_id")
        .agg(
            sum("quantity").alias("total_units"),
            sum("total_revenue").alias("total_revenue"),
            sum("total_cost").alias("total_cost"),
            sum("total_profit").alias("total_profit"),
            countDistinct("customer_id").alias("unique_customers"),
            count("*").alias("total_transactions")
        )
        .join(stores, "store_id", "left")
        .withColumn(
            "avg_transaction_value",
            round(col("total_revenue") / col("total_transactions"), 2)
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC #AGG MONTHLY SALES (OPTIMIZED)

# COMMAND ----------


@dlt.table(
    name=f"{catalog}.{config['gold_schema']}.agg_monthly_sales",
    comment="Monthly sales trends"
)
def agg_monthly_sales():

    sales = dlt.read(f"{catalog}.{config['gold_schema']}.fact_sales")
    products = dlt.read(f"{catalog}.{config['gold_schema']}.dim_product")

    return (
        sales
        .join(products, "product_id", "left")
        .withColumn("year", year("transaction_date"))
        .withColumn("month", month("transaction_date"))
        .groupBy("year", "month", "store_id", "product_brand")
        .agg(
            sum("quantity").alias("total_units"),
            sum("total_revenue").alias("total_revenue"),
            sum("total_cost").alias("total_cost"),
            sum("total_profit").alias("total_profit"),
            countDistinct("customer_id").alias("unique_customers"),
            count("*").alias("total_transactions")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC #AGG STORE PERFORMANCE (OPTIMIZED)

# COMMAND ----------


@dlt.table(
    name=f"{catalog}.{config['gold_schema']}.agg_store_performance",
    comment="Store performance"
)
def agg_store_performance():

    sales = dlt.read(f"{catalog}.{config['gold_schema']}.fact_sales")
    returns = dlt.read(f"{catalog}.{config['gold_schema']}.fact_returns")

    sales_agg = (
        sales.groupBy("store_id")
        .agg(
            sum("quantity").alias("total_units_sold"),
            sum("total_revenue").alias("total_revenue"),
            sum("total_profit").alias("total_profit")
        )
    )

    returns_agg = (
        returns.groupBy("store_id")
        .agg(sum("return_quantity").alias("total_units_returned"))
    )

    return (
        sales_agg
        .join(returns_agg, "store_id", "left")
        .withColumn(
            "return_rate",
            round(col("total_units_returned") / col("total_units_sold") * 100, 2)
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC #RETURN ANALYSIS
# MAGIC

# COMMAND ----------

@dlt.table(name=f"{catalog}.{config['gold_schema']}.agg_return_kpi")
def agg_return_kpi():

    sales = dlt.read(f"{catalog}.{config['gold_schema']}.fact_sales")
    returns = dlt.read(f"{catalog}.{config['gold_schema']}.fact_returns")

    sales_agg = (
        sales.groupBy("product_id")
        .agg(sum("quantity").alias("sold_qty"))
    )

    returns_agg = (
        returns.groupBy("product_id")
        .agg(sum("return_quantity").alias("returned_qty"))
    )

    return (
        sales_agg
        .join(returns_agg, "product_id", "left")
        .withColumn(
            "return_rate",
            col("returned_qty") / col("sold_qty")
        )
    )
