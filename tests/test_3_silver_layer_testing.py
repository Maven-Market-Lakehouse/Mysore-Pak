
# ==========================================
# IMPORTS --------------CHATGPT TESTING SILVER SANJANA
# ==========================================
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# ==========================================
# SPARK SESSION FIXTURE
# ==========================================
@pytest.fixture(scope="session")
def spark():
    """Reuse the active SparkSession on Databricks;
    fall back to a local session for CI."""
    existing = SparkSession.getActiveSession()
    if existing is not None:
        return existing
    return (
        SparkSession.builder
        .master("local[*]")
        .appName("silver-layer-test")
        .getOrCreate()
    )


# ==========================================
# HELPER: QUARANTINE FILTER (MATCH YOUR CODE)
# ==========================================
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


# ==========================================
# MOCK DATA (COMMON)
# ==========================================
@pytest.fixture
def sample_orders_df(spark):
    data = [
        ("O1", 101, 1, 1, 2, "1/1/2024"),
        ("O2", 102, 2, 1, 3, "1/2/2024"),
        ("O2", 102, 2, 1, 3, "1/2/2024"),  # duplicate
        (None, 103, 3, 1, 1, "1/3/2024")   # invalid
    ]

    schema = ["order_id", "product_id", "customer_id", "store_id", "quantity", "order_date"]

    return spark.createDataFrame(data, schema)


# ==========================================
# UNIT TESTS
# ==========================================

# ------------------------------
# TEST 1: DATE PARSING
# ------------------------------
def test_order_date_parsing(sample_orders_df):

    df = sample_orders_df.withColumn("order_date", to_date("order_date", "M/d/yyyy"))

    assert dict(df.dtypes)["order_date"] == "date"


# ------------------------------
# TEST 2: DEDUPLICATION
# ------------------------------
def test_deduplication(sample_orders_df):

    df = sample_orders_df.dropDuplicates(["order_id"])

    assert df.count() == 3  # duplicate removed


# ------------------------------
# TEST 3: NULL HANDLING (AS-IS)
# ------------------------------
def test_null_handling(sample_orders_df):

    row = sample_orders_df.filter(col("order_id").isNull()).collect()[0]

    assert row["order_id"] is None


# ------------------------------
# TEST 4: QUARANTINE FILTER LOGIC
# ------------------------------
def test_quarantine_filter(sample_orders_df):

    rules = {
        "valid_order": "order_id IS NOT NULL",
        "positive_qty": "quantity > 0"
    }

    clean, quarantine = quarantine_filter(sample_orders_df, rules)

    assert clean.count() == 3
    assert quarantine.count() == 1


# ------------------------------
# TEST 5: SCHEMA VALIDATION
# ------------------------------
def test_schema(sample_orders_df):

    expected_cols = {"order_id", "product_id", "customer_id", "store_id", "quantity", "order_date"}

    assert set(sample_orders_df.columns) == expected_cols


# ------------------------------
# TEST 6: TYPE CASTING
# ------------------------------
def test_quantity_cast(sample_orders_df):

    df = sample_orders_df.withColumn("quantity", col("quantity").cast("int"))

    assert dict(df.dtypes)["quantity"] == "int"


# ==========================================
# INTEGRATION TESTS
# ==========================================

# ------------------------------
# TEST 7: FULL SILVER PIPELINE (ORDERS)
# ------------------------------
def test_silver_orders_pipeline(spark, sample_orders_df):

    df = (
        sample_orders_df
        .withColumn("order_date", to_date("order_date", "M/d/yyyy"))
        .withColumn("event_timestamp", current_timestamp())
        .dropDuplicates(["order_id"])
        .withColumn("pipeline_run_id", current_timestamp())
        .withColumn("processing_timestamp", current_timestamp())
    )

    assert df.count() == 3
    assert "processing_timestamp" in df.columns


# ------------------------------
# TEST 8: VALID VS QUARANTINE FLOW
# ------------------------------
def test_valid_vs_quarantine_flow(sample_orders_df):

    rules = {
        "valid_order": "order_id IS NOT NULL",
        "positive_qty": "quantity > 0"
    }

    clean, quarantine = quarantine_filter(sample_orders_df, rules)

    assert clean.count() + quarantine.count() == sample_orders_df.count()


# ------------------------------
# TEST 9: EMPTY INPUT
# ------------------------------
def test_empty_input(spark):

    df = spark.createDataFrame([], StructType([
        StructField("order_id", StringType(), True)
    ]))

    clean, quarantine = quarantine_filter(df, {"valid": "order_id IS NOT NULL"})

    assert clean.count() == 0
    assert quarantine.count() == 0


# ------------------------------
# TEST 10: DATA CONSISTENCY
# ------------------------------
def test_data_consistency(sample_orders_df):

    df = sample_orders_df.dropDuplicates(["order_id"])

    unique_ids = df.select("order_id").distinct().count()

    assert unique_ids == df.count()
