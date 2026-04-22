from utils import *
from pyspark.sql.functions import col, abs
from utils import tbl

# =========================================================
# 1) DIMENSION TABLES
# =========================================================

def test_dim_customer(spark, config):
    df = spark.table(tbl(config, "gold", "dim_customer"))
    assert_not_empty(df)
    assert_unique(df, "customer_id")
    assert_no_nulls(df, ["customer_id"])


def test_dim_product(spark, config):
    df = spark.table(tbl(config, "gold", "dim_product"))
    assert_not_empty(df)
    assert_unique(df, "product_id")
    assert_no_nulls(df, ["product_id"])


def test_dim_store(spark, config):
    df = spark.table(tbl(config, "gold", "dim_store"))
    assert_not_empty(df)
    assert_unique(df, "store_id")
    assert_no_nulls(df, ["store_id", "region_id"])


# =========================================================
# 2) FACT SALES
# =========================================================

def test_fact_sales_basic(spark, config):
    df = spark.table(tbl(config, "gold", "fact_sales"))

    assert_not_empty(df)
    assert_no_nulls(df, ["product_id", "customer_id", "store_id"])

    assert df.filter(col("quantity") <= 0).count() == 0
    assert df.filter(col("total_revenue") < 0).count() == 0
    assert df.filter(col("total_cost") < 0).count() == 0


def test_fact_sales_not_null_measures(spark, config):
    df = spark.table(tbl(config, "gold", "fact_sales"))

    assert df.filter(col("total_revenue").isNull()).count() == 0
    assert df.filter(col("total_cost").isNull()).count() == 0


def test_fact_sales_profit_logic(spark, config):
    df = spark.table(tbl(config, "gold", "fact_sales"))

    check = df.filter(
        abs(col("total_profit") - (col("total_revenue") - col("total_cost"))) > 0.01
    )
    assert check.count() == 0


def test_revenue_calculation(spark, config):
    df = spark.table(tbl(config, "gold", "fact_sales"))
    prod = spark.table(tbl(config, "gold", "dim_product"))

    joined = df.join(prod, "product_id")

    check = joined.filter(
        abs(col("total_revenue") - col("quantity") * col("product_retail_price")) > 0.01
    )
    assert check.count() == 0


def test_cost_calculation(spark, config):
    df = spark.table(tbl(config, "gold", "fact_sales"))
    prod = spark.table(tbl(config, "gold", "dim_product"))

    joined = df.join(prod, "product_id")

    check = joined.filter(
        abs(col("total_cost") - col("quantity") * col("product_cost")) > 0.01
    )
    assert check.count() == 0


def test_fact_sales_grain_unique(spark, config):
    df = spark.table(tbl(config, "gold", "fact_sales"))

    total = df.count()
    distinct = df.dropDuplicates(
        ["product_id", "customer_id", "store_id", "transaction_date"]
    ).count()

    assert total == distinct


def test_fact_sales_date_not_null(spark, config):
    df = spark.table(tbl(config, "gold", "fact_sales"))
    assert df.filter(col("transaction_date").isNull()).count() == 0


def test_fact_sales_join_integrity(spark, config):
    fact = spark.table(tbl(config, "gold", "fact_sales"))
    dim = spark.table(tbl(config, "gold", "dim_product"))

    joined = fact.join(dim, "product_id", "left")
    assert joined.filter(col("product_name").isNull()).count() == 0


# =========================================================
# 3) FACT RETURNS
# =========================================================

def test_fact_returns_basic(spark, config):
    df = spark.table(tbl(config, "gold", "fact_returns"))

    assert_not_empty(df)
    assert df.filter(col("return_quantity") <= 0).count() == 0
    assert df.filter(col("return_amount").isNull()).count() == 0


def test_return_amount_logic(spark, config):
    df = spark.table(tbl(config, "gold", "fact_returns"))
    prod = spark.table(tbl(config, "gold", "dim_product"))

    joined = df.join(prod, "product_id")

    check = joined.filter(
        abs(col("return_amount") - col("return_quantity") * col("product_retail_price")) > 0.01
    )
    assert check.count() == 0


# =========================================================
# 4) FK VALIDATION (STAR SCHEMA)
# =========================================================

def test_fact_sales_fk(spark, config):
    fact = spark.table(tbl(config, "gold", "fact_sales"))

    cust = spark.table(tbl(config, "gold", "dim_customer"))
    prod = spark.table(tbl(config, "gold", "dim_product"))
    store = spark.table(tbl(config, "gold", "dim_store"))

    assert_fk(fact, cust, "customer_id")
    assert_fk(fact, prod, "product_id")
    assert_fk(fact, store, "store_id")


def test_fact_returns_fk(spark, config):
    fact = spark.table(tbl(config, "gold", "fact_returns"))

    prod = spark.table(tbl(config, "gold", "dim_product"))
    store = spark.table(tbl(config, "gold", "dim_store"))

    assert_fk(fact, prod, "product_id")
    assert_fk(fact, store, "store_id")


# =========================================================
# 5) AGGREGATIONS
# =========================================================

def test_daily_aggregation(spark, config):
    df = spark.table(tbl(config, "gold", "agg_daily_sales"))

    assert_not_empty(df)
    assert df.filter(col("total_revenue") < 0).count() == 0
    assert df.filter(col("total_profit") < 0).count() == 0


def test_daily_aggregation_consistency(spark, config):
    fact = spark.table(tbl(config, "gold", "fact_sales"))
    agg = spark.table(tbl(config, "gold", "agg_daily_sales"))

    f_sum = fact.agg({"total_revenue": "sum"}).collect()[0][0]
    a_sum = agg.agg({"total_revenue": "sum"}).collect()[0][0]

    assert abs(float(f_sum) - float(a_sum)) < 1


def test_avg_transaction_value(spark, config):
    df = spark.table(tbl(config, "gold", "agg_daily_sales"))

    check = df.filter(
        abs(col("avg_transaction_value") - (col("total_revenue") / col("total_transactions"))) > 0.01
    )
    assert check.count() == 0


def test_monthly_aggregation(spark, config):
    df = spark.table(tbl(config, "gold", "agg_monthly_sales"))

    assert_not_empty(df)
    assert df.filter(col("year").isNull()).count() == 0
    assert df.filter(col("month").isNull()).count() == 0


# =========================================================
# 6) KPI TABLES
# =========================================================

def test_store_performance(spark, config):
    df = spark.table(tbl(config, "gold", "agg_store_performance"))

    assert_not_empty(df)
    assert df.filter(col("return_rate") < 0).count() == 0
    assert df.filter(col("return_rate") > 100).count() == 0


def test_return_kpi(spark, config):
    df = spark.table(tbl(config, "gold", "agg_return_kpi"))

    assert_not_empty(df)
    assert df.filter(col("return_rate") < 0).count() == 0
    assert df.filter(col("return_rate") > 1).count() == 0


# =========================================================
# 7) RECONCILIATION
# =========================================================

def test_sales_reconciliation(spark, config):
    silver = spark.table(tbl(config, "silver", "silver_transactions_valid"))
    gold = spark.table(tbl(config, "gold", "fact_sales"))

    assert_reconciliation(silver, gold)


def test_returns_reconciliation(spark, config):
    silver = spark.table(tbl(config, "silver", "silver_returns"))
    gold = spark.table(tbl(config, "gold", "fact_returns"))

    assert_reconciliation(silver, gold)


# =========================================================
# 8) DIMENSION COVERAGE
# =========================================================

def test_dim_product_coverage(spark, config):
    fact = spark.table(tbl(config, "gold", "fact_sales"))
    dim = spark.table(tbl(config, "gold", "dim_product"))

    missing = fact.select("product_id").subtract(dim.select("product_id"))
    assert missing.count() == 0


def test_dim_store_coverage(spark, config):
    fact = spark.table(tbl(config, "gold", "fact_sales"))
    dim = spark.table(tbl(config, "gold", "dim_store"))

    missing = fact.select("store_id").subtract(dim.select("store_id"))
    assert missing.count() == 0


def test_dim_customer_coverage(spark, config):
    fact = spark.table(tbl(config, "gold", "fact_sales"))
    dim = spark.table(tbl(config, "gold", "dim_customer"))

    missing = fact.select("customer_id").subtract(dim.select("customer_id"))
    assert missing.count() == 0


def test_unused_products(spark, config):
    fact = spark.table(tbl(config, "gold", "fact_sales"))
    dim = spark.table(tbl(config, "gold", "dim_product"))

    unused = dim.select("product_id").subtract(fact.select("product_id"))
    assert unused.count() == 0