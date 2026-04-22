
# ==========================================
# IMPORTS---------------CHATGPT TESTING INTEGRATION
# ==========================================
import pytest
import tempfile
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os

# ==========================================
# DETECT ENVIRONMENT
# ==========================================
_ON_DATABRICKS = "DATABRICKS_RUNTIME_VERSION" in os.environ

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
        .appName("bronze-integration-test")
        .getOrCreate()
    )


# ==========================================
# TEMP DIRECTORY FIXTURE
# ==========================================
@pytest.fixture
def temp_dir():
    dirpath = tempfile.mkdtemp()
    yield dirpath
    shutil.rmtree(dirpath)


# ==========================================
# HELPER: SIMULATE AUTO LOADER INPUT (ADLS)
# ==========================================
def write_csv(spark, path, data, schema):
    df = spark.createDataFrame(data, schema)
    df.write.mode("append").option("header", "true").csv(path)


# ==========================================
# TEST 1: BRONZE TRANSACTIONS (ADLS)
# ==========================================
@pytest.mark.skipif(_ON_DATABRICKS, reason="CSV file I/O not supported on Databricks with USER_ISOLATION")
def test_bronze_transactions_integration(spark, temp_dir):

    schema = "transaction_id INT, product_id INT, quantity INT"

    data = [(1, 101, 2), (2, 102, 3)]
    write_csv(spark, temp_dir, data, schema)

    df = (
        spark.read
        .option("header", "true")
        .schema(schema)
        .csv(temp_dir)
    )

    result = (
        df
        .withColumn("ingestion_timestamp", col("transaction_id") * 0 + 1)  # mock timestamp
        .withColumn("_source_system", col("transaction_id") * 0 + 1)
    )

    assert result.count() == 2
    assert "_source_system" in result.columns


# ==========================================
# TEST 2: EMPTY FILE HANDLING
# ==========================================
@pytest.mark.skipif(_ON_DATABRICKS, reason="CSV file I/O not supported on Databricks with USER_ISOLATION")
def test_empty_file_integration(spark, temp_dir):

    schema = "id INT, value STRING"
    write_csv(spark, temp_dir, [], schema)

    df = spark.read.option("header", "true").schema(schema).csv(temp_dir)

    result = df.withColumn("ingestion_timestamp", col("id") * 0)

    assert result.count() == 0


# ==========================================
# TEST 3: DUPLICATE RECORDS PRESERVED
# ==========================================
@pytest.mark.skipif(_ON_DATABRICKS, reason="CSV file I/O not supported on Databricks with USER_ISOLATION")
def test_duplicates_integration(spark, temp_dir):

    schema = "id INT"
    data = [(1,), (1,)]
    write_csv(spark, temp_dir, data, schema)

    df = spark.read.option("header", "true").schema(schema).csv(temp_dir)

    assert df.count() == 2  # Bronze does NOT deduplicate


# ==========================================
# TEST 4: SCHEMA EVOLUTION (SIMULATION)
# ==========================================
@pytest.mark.skipif(_ON_DATABRICKS, reason="CSV file I/O not supported on Databricks with USER_ISOLATION")
def test_schema_evolution_integration(spark, temp_dir):

    schema1 = "id INT"
    schema2 = "id INT, new_col STRING"

    write_csv(spark, temp_dir, [(1,)], schema1)
    write_csv(spark, temp_dir, [(2, "A")], schema2)

    df = spark.read.option("header", "true").option("mergeSchema", "true").csv(temp_dir)

    assert "new_col" in df.columns


# ==========================================
# TEST 5: METADATA COLUMN VALIDATION
# ==========================================
@pytest.mark.skipif(_ON_DATABRICKS, reason="CSV file I/O not supported on Databricks with USER_ISOLATION")
def test_metadata_columns_integration(spark, temp_dir):

    schema = "id INT"
    data = [(1,)]
    write_csv(spark, temp_dir, data, schema)

    df = spark.read.option("header", "true").schema(schema).csv(temp_dir)

    result = df.withColumn("_source_system", col("id") * 0 + 1)

    assert "_source_system" in result.columns


# ==========================================
# TEST 6: FIVETRAN TABLE SIMULATION
# ==========================================
def test_fivetran_table_integration(spark):

    df = spark.createDataFrame([(1, "A")], ["id", "name"])

    df.createOrReplaceTempView("bronze_raw_customers")

    result = spark.table("bronze_raw_customers")

    assert result.count() == 1


# ==========================================
# TEST 7: KAFKA PAYLOAD SIMULATION
# ==========================================
def test_kafka_payload_integration(spark):

    data = [("sample_json",)]
    df = spark.createDataFrame(data, ["raw_payload"])

    result = df.withColumn("_source_system", col("raw_payload"))

    assert result.count() == 1
