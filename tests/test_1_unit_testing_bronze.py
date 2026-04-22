
# ================================
# IMPORTS ----------------CHATGPT UNIT TESTING FILE BRONZE LAYER
# ================================
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
from pyspark.sql.types import *

# ================================
# SPARK SESSION FIXTURE
# ================================
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
        .appName("bronze-layer-test")
        .getOrCreate()
    )


# ================================
# HELPER FUNCTION (SIMULATES BRONZE LOGIC)
# ================================
def apply_bronze_logic(df, source="test"):
    return (
        df
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("_source_system", lit(source))
    )


# ================================
# TEST 1: SCHEMA VALIDATION
# ================================
def test_schema_validation(spark):
    data = [("1", "A"), ("2", "B")]
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("value", StringType(), True)
    ])

    df = spark.createDataFrame(data, schema)
    result = apply_bronze_logic(df)

    expected_columns = {"id", "value", "ingestion_timestamp", "_source_system"}

    assert set(result.columns) == expected_columns


# ================================
# TEST 2: METADATA COLUMN EXISTS
# ================================
def test_metadata_columns(spark):
    df = spark.createDataFrame([("1",)], ["id"])
    result = apply_bronze_logic(df)

    assert "ingestion_timestamp" in result.columns
    assert "_source_system" in result.columns


# ================================
# TEST 3: SOURCE SYSTEM VALUE
# ================================
def test_source_system_value(spark):
    df = spark.createDataFrame([("1",)], ["id"])
    result = apply_bronze_logic(df, source="kafka")

    rows = result.collect()
    assert rows[0]["_source_system"] == "kafka"


# ================================
# TEST 4: DATA INGESTION PRESERVED
# ================================
def test_data_integrity(spark):
    data = [(1, "A"), (2, "B")]
    df = spark.createDataFrame(data, ["id", "value"])

    result = apply_bronze_logic(df)

    assert result.count() == 2


# ================================
# TEST 5: NULL HANDLING (NO CHANGE EXPECTED)
# ================================
def test_null_handling(spark):
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("value", StringType(), True)
    ])
    data = [(1, None)]
    df = spark.createDataFrame(data, schema)

    result = apply_bronze_logic(df)

    row = result.collect()[0]
    assert row["value"] is None


# ================================
# TEST 6: EMPTY DATAFRAME
# ================================
def test_empty_dataframe(spark):
    schema = StructType([
        StructField("id", IntegerType(), True)
    ])
    df = spark.createDataFrame([], schema)

    result = apply_bronze_logic(df)

    assert result.count() == 0
    assert "ingestion_timestamp" in result.columns


# ================================
# TEST 7: DUPLICATES (NO DEDUP IN BRONZE)
# ================================
def test_duplicates_preserved(spark):
    data = [(1,), (1,)]
    df = spark.createDataFrame(data, ["id"])

    result = apply_bronze_logic(df)

    assert result.count() == 2


# ================================
# TEST 8: FILE METADATA SIMULATION (ADLS CASE)
# ================================
def test_file_metadata_columns(spark):
    data = [("file1.csv",)]
    df = spark.createDataFrame(data, ["source_file_name"])

    result = apply_bronze_logic(df)

    assert "_source_system" in result.columns


# ================================
# TEST 9: SCHEMA TYPE CHECK
# ================================
def test_schema_types(spark):
    df = spark.createDataFrame([(1,)], ["id"])
    result = apply_bronze_logic(df)

    schema = dict(result.dtypes)

    assert schema["_source_system"] == "string"


# ================================
# TEST 10: INGESTION TIMESTAMP NOT NULL
# ================================
def test_ingestion_timestamp_not_null(spark):
    df = spark.createDataFrame([(1,)], ["id"])
    result = apply_bronze_logic(df)

    row = result.collect()[0]

    assert row["ingestion_timestamp"] is not None
