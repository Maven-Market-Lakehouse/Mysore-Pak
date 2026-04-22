from utils import *
from pyspark.sql.functions import col, abs
from utils import tbl

# =========================================================
# 1) TRANSACTIONS
# =========================================================

def test_transactions_unit(spark, config):
    df = spark.table(tbl(config, "silver", "silver_transactions"))

    assert_not_empty(df)
    assert_no_nulls(df, ["product_id", "customer_id", "store_id"])
    assert df.filter(col("quantity") <= 0).count() == 0


def test_transactions_schema(spark, config):
    df = spark.table(tbl(config, "silver", "silver_transactions"))
    expected = ["product_id", "customer_id", "store_id", "quantity", "transaction_date"]
    for c in expected:
        assert c in df.columns


def test_transactions_dedup(spark, config):
    df = spark.table(tbl(config, "silver", "silver_transactions"))
    assert df.count() == df.dropDuplicates(
        ["product_id", "customer_id", "store_id", "transaction_date"]
    ).count()


def test_transactions_valid_clean(spark, config):
    df = spark.table(tbl(config, "silver", "silver_transactions_valid"))
    assert_not_empty(df)
    assert df.filter(col("quantity") <= 0).count() == 0
    assert df.filter(col("customer_id").isNull()).count() == 0
    assert df.filter(col("product_id").isNull()).count() == 0
    assert df.filter(col("store_id").isNull()).count() == 0


def test_transactions_quarantine_split(spark, config):
    raw = spark.table(tbl(config, "silver", "silver_transactions"))
    valid = spark.table(tbl(config, "silver", "silver_transactions_valid"))
    quarantine = spark.table(tbl(config, "silver", "silver_quarantine_transactions"))
    assert raw.count() == valid.count() + quarantine.count()


def test_transactions_quarantine_quality(spark, config):
    q = spark.table(tbl(config, "silver", "silver_quarantine_transactions"))
    # ensure key columns exist for debugging bad rows
    for c in ["product_id", "customer_id", "store_id"]:
        assert c in q.columns


def test_transactions_reconciliation(spark, config):
    bronze = spark.table(tbl(config, "bronze", "bronze_transactions"))
    silver = spark.table(tbl(config, "silver", "silver_transactions"))
    assert_reconciliation(bronze, silver)


def test_transactions_quantity_consistency(spark, config):
    bronze = spark.table(tbl(config, "bronze", "bronze_transactions"))
    silver = spark.table(tbl(config, "silver", "silver_transactions"))
    b_sum = bronze.agg({"quantity": "sum"}).collect()[0][0]
    s_sum = silver.agg({"quantity": "sum"}).collect()[0][0]
    if b_sum is not None and s_sum is not None:
        assert abs(float(b_sum) - float(s_sum)) < 1


def test_transaction_date_type(spark, config):
    df = spark.table(tbl(config, "silver", "silver_transactions"))
    assert dict(df.dtypes)["transaction_date"] == "date"


# =========================================================
# 2) RETURNS
# =========================================================

def test_returns_unit(spark, config):
    df = spark.table(tbl(config, "silver", "silver_returns"))
    assert_not_empty(df)
    assert df.filter(col("quantity") <= 0).count() == 0
    assert df.filter(col("return_date").isNull()).count() == 0


def test_returns_reconciliation(spark, config):
    bronze = spark.table(tbl(config, "bronze", "bronze_returns"))
    silver = spark.table(tbl(config, "silver", "silver_returns"))
    assert_reconciliation(bronze, silver)


def test_return_date_type(spark, config):
    df = spark.table(tbl(config, "silver", "silver_returns"))
    assert dict(df.dtypes)["return_date"] == "date"


# =========================================================
# 3) MASTER TABLES (CUSTOMERS / PRODUCTS)
# =========================================================

def test_customers_basic(spark, config):
    df = spark.table(tbl(config, "silver", "silver_customers"))
    assert_not_empty(df)
    assert_unique(df, "customer_id")
    assert_no_nulls(df, ["customer_id"])


def test_products_basic(spark, config):
    df = spark.table(tbl(config, "silver", "silver_products"))
    assert_not_empty(df)
    assert_unique(df, "product_id")
    assert_no_nulls(df, ["product_id"])


# =========================================================
# 4) SCD TYPE 2 — CUSTOMERS / PRODUCTS / STORES
# =========================================================

def _assert_scd_common(df, key_col):
    # 1) exactly one active record per key
    active = df.filter(col("__END_AT").isNull()).groupBy(key_col).count()
    assert active.filter(col("count") != 1).count() == 0

    # 2) validity window correct
    assert df.filter(col("__END_AT").isNotNull() & (col("__START_AT") > col("__END_AT"))).count() == 0

    # 3) no overlapping windows
    overlaps = df.alias("a").join(
        df.alias("b"),
        (col(f"a.{key_col}") == col(f"b.{key_col}")) &
        (col("a.__START_AT") < col("b.__END_AT")) &
        (col("b.__START_AT") < col("a.__END_AT")) &
        (col("a.__START_AT") != col("b.__START_AT"))
    )
    assert overlaps.count() == 0


def test_scd_customers_all(spark, config):
    df = spark.table(tbl(config, "silver", "silver_scd_customers"))
    assert_not_empty(df)
    _assert_scd_common(df, "customer_id")

    # surrogate uniqueness (using processing timestamp as versioning column)
    total = df.count()
    distinct = df.select("customer_id", "processing_timestamp").distinct().count()
    assert total == distinct


def test_scd_products_all(spark, config):
    df = spark.table(tbl(config, "silver", "silver_scd_products"))
    assert_not_empty(df)
    _assert_scd_common(df, "product_id")


def test_scd_stores_all(spark, config):
    df = spark.table(tbl(config, "silver", "silver_scd_stores"))
    assert_not_empty(df)
    _assert_scd_common(df, "store_id")


# =========================================================
# 5) KAFKA — ORDERS
# =========================================================

def test_orders_basic(spark, config):
    df = spark.table(tbl(config, "silver", "silver_orders"))
    assert_not_empty(df)
    assert df.count() == df.dropDuplicates(["order_id"]).count()


def test_orders_quarantine_split(spark, config):
    raw = spark.table(tbl(config, "silver", "silver_orders"))
    valid = spark.table(tbl(config, "silver", "silver_orders_valid"))
    quarantine = spark.table(tbl(config, "silver", "silver_quarantine_orders"))
    assert raw.count() == valid.count() + quarantine.count()


def test_orders_valid_clean(spark, config):
    df = spark.table(tbl(config, "silver", "silver_orders_valid"))
    assert_not_empty(df)
    assert df.filter(col("order_id").isNull()).count() == 0


def test_orders_fk(spark, config):
    orders = spark.table(tbl(config, "silver", "silver_orders_valid"))
    prod = spark.table(tbl(config, "silver", "silver_scd_products")).filter(col("__END_AT").isNull())
    cust = spark.table(tbl(config, "silver", "silver_scd_customers")).filter(col("__END_AT").isNull())
    assert_fk(orders, prod, "product_id")
    assert_fk(orders, cust, "customer_id")


def test_orders_reconciliation(spark, config):
    bronze = spark.table(tbl(config, "bronze", "bronze_orders"))
    silver = spark.table(tbl(config, "silver", "silver_orders"))
    assert_reconciliation(bronze, silver)


# =========================================================
# 6) KAFKA — INVENTORY
# =========================================================

def test_inventory_basic(spark, config):
    df = spark.table(tbl(config, "silver", "silver_inventory"))
    assert_not_empty(df)
    assert df.filter(col("quantity_remaining") < 0).count() == 0


def test_inventory_quarantine_split(spark, config):
    raw = spark.table(tbl(config, "silver", "silver_inventory"))
    valid = spark.table(tbl(config, "silver", "silver_inventory_valid"))
    quarantine = spark.table(tbl(config, "silver", "silver_quarantine_inventory"))
    assert raw.count() == valid.count() + quarantine.count()


def test_inventory_valid_clean(spark, config):
    df = spark.table(tbl(config, "silver", "silver_inventory_valid"))
    assert_not_empty(df)
    assert df.filter(col("product_id").isNull()).count() == 0


def test_inventory_fk(spark, config):
    inv = spark.table(tbl(config, "silver", "silver_inventory_valid"))
    prod = spark.table(tbl(config, "silver", "silver_scd_products")).filter(col("__END_AT").isNull())
    store = spark.table(tbl(config, "silver", "silver_scd_stores")).filter(col("__END_AT").isNull())
    assert_fk(inv, prod, "product_id")
    assert_fk(inv, store, "store_id")


def test_inventory_reconciliation(spark, config):
    bronze = spark.table(tbl(config, "bronze", "bronze_inventory"))
    silver = spark.table(tbl(config, "silver", "silver_inventory"))
    assert_reconciliation(bronze, silver)


# =========================================================
# 7) GLOBAL FK (TRANSACTIONS → DIMS)
# =========================================================

def test_fk_transactions_all(spark, config):
    tx = spark.table(tbl(config, "silver", "silver_transactions_valid"))
    cust = spark.table(tbl(config, "silver", "silver_scd_customers")).filter(col("__END_AT").isNull())
    prod = spark.table(tbl(config, "silver", "silver_scd_products")).filter(col("__END_AT").isNull())
    store = spark.table(tbl(config, "silver", "silver_scd_stores")).filter(col("__END_AT").isNull())

    assert_fk(tx, cust, "customer_id")
    assert_fk(tx, prod, "product_id")
    assert_fk(tx, store, "store_id")