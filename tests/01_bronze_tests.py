# Schema
# Metadata
# Raw ingestion sanity
# Source validation

from utils import *
from pyspark.sql.functions import col


# -------------------------
# UNIT TESTS
# -------------------------
def test_bronze_transactions_not_empty(spark, config):
    df = spark.table(tbl(config, "bronze", "bronze_transactions"))
    assert_not_empty(df)


def test_bronze_schema_columns(spark, config):
    df = spark.table(tbl(config, "bronze", "bronze_transactions"))

    expected = [
        "product_id", "customer_id", "store_id",
        "quantity", "ingestion_timestamp", "_source_system"
    ]

    for c in expected:
        assert c in df.columns


def test_bronze_metadata_columns(spark, config):
    df = spark.table(tbl(config, "bronze", "bronze_transactions"))

    assert "ingestion_timestamp" in df.columns
    assert "_source_system" in df.columns


def test_bronze_source_system_values(spark, config):
    df = spark.table(tbl(config, "bronze", "bronze_transactions"))

    valid_sources = ["adls", "kafka", "mongodb"]
    invalid = df.filter(~col("_source_system").isin(valid_sources))

    assert invalid.count() == 0


def test_bronze_no_corrupt_records(spark, config):
    df = spark.table(tbl(config, "bronze", "bronze_transactions"))

    if "_rescued_data" in df.columns:
        assert df.filter(col("_rescued_data").isNotNull()).count() == 0


# -------------------------
# DATA INTEGRITY TESTS
# -------------------------
def test_bronze_primary_keys_not_null(spark, config):
    df = spark.table(tbl(config, "bronze", "bronze_transactions"))
    assert_no_nulls(df, ["product_id", "customer_id", "store_id"])


def test_bronze_quantity_not_null(spark, config):
    df = spark.table(tbl(config, "bronze", "bronze_transactions"))
    assert df.filter(col("quantity").isNull()).count() == 0


def test_bronze_duplicate_threshold(spark, config):
    df = spark.table(tbl(config, "bronze", "bronze_transactions"))

    duplicates = df.groupBy(
        "product_id", "customer_id", "store_id", "transaction_date"
    ).count().filter(col("count") > 1)

    total = df.count()
    dup_count = duplicates.count()

    # Allow some duplicates (raw layer), but not excessive
    assert dup_count <= total * 0.1


# -------------------------
# DATA TYPE VALIDATION
# -------------------------
def test_bronze_quantity_type(spark, config):
    df = spark.table(tbl(config, "bronze", "bronze_transactions"))

    dtype = dict(df.dtypes)["quantity"]
    assert dtype in ["int", "bigint", "double", "float"]


# -------------------------
# SCHEMA DRIFT VALIDATION
# -------------------------
def test_bronze_schema_drift_safe(spark, config):
    df = spark.table(tbl(config, "bronze", "bronze_transactions"))

    critical_cols = ["product_id", "customer_id", "store_id"]

    for c in critical_cols:
        assert c in df.columns


# -------------------------
# RECONCILIATION TESTS (REAL)
# -------------------------

# 1. Row count sanity
def test_bronze_row_count_non_zero(spark, config):
    df = spark.table(tbl(config, "bronze", "bronze_transactions"))
    assert df.count() > 0


# 2. SOURCE → BRONZE COUNT RECONCILIATION
def test_source_to_bronze_count(spark, config):
    source_path = config["base_path"] + config["transactions_path"]

    source_df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(source_path)

    bronze_df = spark.table(tbl(config, "bronze", "bronze_transactions"))

    # Bronze can be >= source due to streaming/reprocessing
    assert bronze_df.count() >= source_df.count()


# 3. AGGREGATE RECONCILIATION
def test_bronze_quantity_sum(spark, config):
    source_path = config["base_path"] + config["transactions_path"]

    source_df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(source_path)

    bronze_df = spark.table(tbl(config, "bronze", "bronze_transactions"))

    source_sum = source_df.selectExpr("sum(quantity)").collect()[0][0]
    bronze_sum = bronze_df.selectExpr("sum(quantity)").collect()[0][0]

    if source_sum is not None and bronze_sum is not None:
        tolerance = max(1, 0.01 * float(source_sum))
        assert abs(float(source_sum) - float(bronze_sum)) <= tolerance


# 4. KEY COVERAGE VALIDATION (COMPOSITE KEYS)
def test_bronze_key_coverage(spark, config):
    source_path = config["base_path"] + config["transactions_path"]

    source_df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(source_path)

    bronze_df = spark.table(tbl(config, "bronze", "bronze_transactions"))

    keys = ["product_id", "customer_id", "store_id"]

    source_keys = source_df.select(keys).distinct()
    bronze_keys = bronze_df.select(keys).distinct()

    missing = source_keys.subtract(bronze_keys)

    assert missing.count() == 0


# -------------------------
# PERFORMANCE SANITY
# -------------------------
def test_bronze_queryable(spark, config):
    df = spark.table(tbl(config, "bronze", "bronze_transactions"))
    assert df.limit(1).count() == 1