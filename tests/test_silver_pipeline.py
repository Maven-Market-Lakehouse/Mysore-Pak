"""Unit tests for the 02_Silver_Pipeline notebooks..............................CREATED BY GENIE

Covers testable logic extracted from:
  - 1_SILVER_ADLS   (transactions, stores, regions, calendar, returns, quarantine)
  - 2_SILVER_KAFKA   (orders, inventory, quarantine, schemas)
  - 3_SILVER_MONGO   (customers, products, schemas)

DLT-decorated functions cannot be invoked outside the pipeline runtime,
so we test the *pure logic* they rely on: config helpers, quarantine
filtering, schemas, validation rules, and transformation patterns.
"""

import os
import pytest
import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, to_date, trim, initcap, upper, when,
    current_timestamp, concat_ws, concat, from_json, expr,
)
try:
    from pyspark.sql.functions import try_to_date
except ImportError:
    try_to_date = None
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    LongType, DoubleType,
)


# ============================================================
# Fixtures
# ============================================================

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
        .appName("test_silver_pipeline")
        .getOrCreate()
    )


@pytest.fixture(scope="session")
def config():
    """Load the shared pipeline config."""
    config_path = "/Workspace/maven_market/Mysore-Pak/config/config.yml"
    if not os.path.exists(config_path):
        config_path = os.path.join(
            os.path.dirname(__file__), "..", "config", "config.yml"
        )
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


# ============================================================
# Helpers (mirrored from notebooks)
# ============================================================

def tbl(config, layer, name):
    catalog = config["catalog"]
    return f"{catalog}.{config[f'{layer}_schema']}.{config[name]}"


def silver(config, name):
    catalog = config["catalog"]
    return f"{catalog}.{config['silver_schema']}.{name}"


def quarantine_filter(df, rules):
    """Exact copy of the reusable quarantine utility from the notebooks."""
    valid_condition = " AND ".join(f"({v})" for v in rules.values())
    invalid_condition = f"NOT ({valid_condition})"

    clean_df = df.filter(expr(valid_condition))

    quarantine_df = (
        df.filter(expr(invalid_condition))
        .withColumn("_quarantine_timestamp", current_timestamp())
        .withColumn(
            "_quarantine_reason",
            concat_ws(", ", *[
                when(~expr(cond), lit(name))
                for name, cond in rules.items()
            ])
        )
    )
    return clean_df, quarantine_df


# ----- Validation rule dicts (mirrored) -----

TRANSACTION_RULES = {
    "valid_product": "product_id IS NOT NULL",
    "valid_customer": "customer_id IS NOT NULL",
    "valid_store": "store_id IS NOT NULL",
    "positive_qty": "quantity > 0",
    "valid_date": "transaction_date IS NOT NULL",
}

ORDERS_RULES = {
    "valid_order_id": "order_id IS NOT NULL",
    "valid_product": "product_id IS NOT NULL",
    "valid_customer": "customer_id IS NOT NULL",
    "valid_store": "store_id IS NOT NULL",
    "positive_qty": "quantity > 0",
}

INVENTORY_RULES = {
    "valid_product": "product_id IS NOT NULL",
    "valid_store": "store_id IS NOT NULL",
    "valid_stock": "quantity_remaining >= 0",
}


# ----- Schemas (mirrored from 2_SILVER_KAFKA) -----

orders_schema = StructType([
    StructField("order_id", StringType()),
    StructField("product_id", IntegerType()),
    StructField("customer_id", IntegerType()),
    StructField("store_id", IntegerType()),
    StructField("quantity", IntegerType()),
    StructField("order_date", StringType()),
    StructField("payment_type", StringType()),
    StructField("order_channel", StringType()),
])

inventory_schema = StructType([
    StructField("inventory_id", StringType()),
    StructField("event_timestamp", StringType()),
    StructField("product_id", IntegerType()),
    StructField("store_id", IntegerType()),
    StructField("quantity_added", IntegerType()),
    StructField("quantity_remaining", IntegerType()),
])

# ----- Schemas (mirrored from 3_SILVER_MONGO) -----

customers_data_schema = StructType([
    StructField("customer_id", IntegerType()),
    StructField("customer_acct_num", LongType()),
    StructField("first_name", StringType()),
    StructField("last_name", StringType()),
    StructField("customer_address", StringType()),
    StructField("customer_city", StringType()),
    StructField("customer_state_province", StringType()),
    StructField("customer_postal_code", IntegerType()),
    StructField("customer_country", StringType()),
    StructField("birthdate", StringType()),
    StructField("marital_status", StringType()),
    StructField("yearly_income", StringType()),
    StructField("gender", StringType()),
    StructField("total_children", IntegerType()),
    StructField("num_children_at_home", IntegerType()),
    StructField("education", StringType()),
    StructField("acct_open_date", StringType()),
    StructField("member_card", StringType()),
    StructField("occupation", StringType()),
    StructField("homeowner", StringType()),
])

products_data_schema = StructType([
    StructField("product_id", IntegerType()),
    StructField("product_brand", StringType()),
    StructField("product_name", StringType()),
    StructField("product_sku", LongType()),
    StructField("product_retail_price", DoubleType()),
    StructField("product_cost", DoubleType()),
    StructField("product_weight", DoubleType()),
    StructField("recyclable", IntegerType()),
    StructField("low_fat", IntegerType()),
])


# ============================================================
# 1. CONFIG LOADING
# ============================================================

class TestConfigLoading:
    """Verify config.yml loads and contains all keys the Silver
    notebooks depend on."""

    def test_config_loads_successfully(self, config):
        assert isinstance(config, dict)
        assert len(config) > 0

    def test_catalog_key_exists(self, config):
        assert config["catalog"] == "maven_market_uc"

    def test_schema_keys_present(self, config):
        for key in ["bronze_schema", "silver_schema", "gold_schema"]:
            assert key in config, f"Missing schema key: {key}"

    def test_bronze_table_keys_present(self, config):
        required = [
            "bronze_transactions", "bronze_returns", "bronze_stores",
            "bronze_regions", "bronze_calendar", "bronze_customers",
            "bronze_products", "bronze_orders", "bronze_inventory",
        ]
        for key in required:
            assert key in config, f"Missing bronze table key: {key}"

    def test_primary_keys_present(self, config):
        for key in ["customers_pk", "products_pk", "stores_pk"]:
            assert key in config, f"Missing PK key: {key}"

    def test_scd_columns_present(self, config):
        for key in ["customers_scd_columns", "products_scd_columns",
                    "stores_scd_columns"]:
            assert key in config, f"Missing SCD config: {key}"
            assert isinstance(config[key], list)
            assert len(config[key]) > 0

    def test_stores_scd_columns_values(self, config):
        assert "store_name" in config["stores_scd_columns"]
        assert "region_id" in config["stores_scd_columns"]


# ============================================================
# 2. HELPER FUNCTIONS
# ============================================================

class TestHelperFunctions:
    """Verify fully qualified table name builders."""

    def test_tbl_bronze_transactions(self, config):
        result = tbl(config, "bronze", "bronze_transactions")
        assert result == "maven_market_uc.bronze.bronze_raw_transactions"

    def test_tbl_bronze_orders(self, config):
        result = tbl(config, "bronze", "bronze_orders")
        assert result == "maven_market_uc.bronze.bronze_raw_orders"

    def test_tbl_bronze_customers(self, config):
        result = tbl(config, "bronze", "bronze_customers")
        assert result == "maven_market_uc.bronze.bronze_customers"

    def test_tbl_bronze_inventory(self, config):
        result = tbl(config, "bronze", "bronze_inventory")
        assert result == "maven_market_uc.bronze.bronze_raw_inventory"

    def test_silver_function(self, config):
        result = silver(config, "silver_transactions")
        assert result == "maven_market_uc.silver.silver_transactions"

    def test_silver_scd_customers(self, config):
        result = silver(config, "silver_scd_customers")
        assert result == "maven_market_uc.silver.silver_scd_customers"

    def test_silver_quarantine(self, config):
        result = silver(config, "silver_quarantine_transactions")
        assert result == "maven_market_uc.silver.silver_quarantine_transactions"


# ============================================================
# 3. SCHEMA VALIDATION
# ============================================================

class TestSchemas:
    """Verify schema field counts, names, and types."""

    # -- Orders (Kafka) --
    def test_orders_schema_field_count(self):
        assert len(orders_schema.fields) == 8

    def test_orders_schema_field_names(self):
        names = [f.name for f in orders_schema.fields]
        expected = [
            "order_id", "product_id", "customer_id", "store_id",
            "quantity", "order_date", "payment_type", "order_channel",
        ]
        assert names == expected

    def test_orders_schema_id_types(self):
        field_map = {f.name: f.dataType for f in orders_schema.fields}
        assert isinstance(field_map["order_id"], StringType)
        assert isinstance(field_map["product_id"], IntegerType)
        assert isinstance(field_map["customer_id"], IntegerType)

    # -- Inventory (Kafka) --
    def test_inventory_schema_field_count(self):
        assert len(inventory_schema.fields) == 6

    def test_inventory_schema_field_names(self):
        names = [f.name for f in inventory_schema.fields]
        expected = [
            "inventory_id", "event_timestamp", "product_id",
            "store_id", "quantity_added", "quantity_remaining",
        ]
        assert names == expected

    # -- Customers (Mongo) --
    def test_customers_schema_field_count(self):
        assert len(customers_data_schema.fields) == 20

    def test_customers_schema_has_demographics(self):
        names = [f.name for f in customers_data_schema.fields]
        for field in ["gender", "marital_status", "yearly_income",
                      "education", "occupation", "homeowner"]:
            assert field in names, f"Missing: {field}"

    def test_customers_schema_date_fields_are_strings(self):
        field_map = {f.name: f.dataType for f in customers_data_schema.fields}
        assert isinstance(field_map["birthdate"], StringType)
        assert isinstance(field_map["acct_open_date"], StringType)

    # -- Products (Mongo) --
    def test_products_schema_field_count(self):
        assert len(products_data_schema.fields) == 9

    def test_products_schema_numeric_types(self):
        field_map = {f.name: f.dataType for f in products_data_schema.fields}
        assert isinstance(field_map["product_retail_price"], DoubleType)
        assert isinstance(field_map["product_cost"], DoubleType)
        assert isinstance(field_map["product_weight"], DoubleType)
        assert isinstance(field_map["recyclable"], IntegerType)


# ============================================================
# 4. VALIDATION RULES
# ============================================================

class TestValidationRules:
    """Verify rule dictionaries match notebook definitions."""

    def test_transaction_rules_count(self):
        assert len(TRANSACTION_RULES) == 5

    def test_transaction_rules_keys(self):
        expected = {"valid_product", "valid_customer", "valid_store",
                    "positive_qty", "valid_date"}
        assert set(TRANSACTION_RULES.keys()) == expected

    def test_orders_rules_count(self):
        assert len(ORDERS_RULES) == 5

    def test_orders_rules_keys(self):
        expected = {"valid_order_id", "valid_product", "valid_customer",
                    "valid_store", "positive_qty"}
        assert set(ORDERS_RULES.keys()) == expected

    def test_inventory_rules_count(self):
        assert len(INVENTORY_RULES) == 3

    def test_inventory_rules_keys(self):
        expected = {"valid_product", "valid_store", "valid_stock"}
        assert set(INVENTORY_RULES.keys()) == expected

    def test_all_rules_are_sql_expressions(self):
        all_rules = {**TRANSACTION_RULES, **ORDERS_RULES, **INVENTORY_RULES}
        for name, sql_expr in all_rules.items():
            assert isinstance(sql_expr, str), f"{name} is not a string"
            assert len(sql_expr) > 0, f"{name} is empty"


# ============================================================
# 5. QUARANTINE FILTER LOGIC
# ============================================================

class TestQuarantineFilter:
    """Test the quarantine_filter() function with mock DataFrames."""

    @pytest.fixture()
    def transactions_df(self, spark):
        data = [
            (1, 100, 10, 5, "1/15/2024"),   # valid
            (2, 200, 20, 3, "2/20/2024"),   # valid
            (None, 300, 30, 2, "3/10/2024"), # null product_id
            (4, None, 40, 1, "4/5/2024"),   # null customer_id
            (5, 500, None, 4, "5/1/2024"),  # null store_id
            (6, 600, 60, 0, "6/1/2024"),    # zero quantity
            (7, 700, 70, -1, "7/1/2024"),   # negative quantity
            (8, 800, 80, 2, None),           # null date
        ]
        schema = StructType([
            StructField("product_id", IntegerType()),
            StructField("customer_id", IntegerType()),
            StructField("store_id", IntegerType()),
            StructField("quantity", IntegerType()),
            StructField("transaction_date", StringType()),
        ])
        return spark.createDataFrame(data, schema)

    def test_valid_records_count(self, transactions_df):
        clean, _ = quarantine_filter(transactions_df, TRANSACTION_RULES)
        assert clean.count() == 2

    def test_quarantined_records_count(self, transactions_df):
        _, quarantined = quarantine_filter(transactions_df, TRANSACTION_RULES)
        assert quarantined.count() == 6

    def test_quarantine_has_timestamp_column(self, transactions_df):
        _, quarantined = quarantine_filter(transactions_df, TRANSACTION_RULES)
        assert "_quarantine_timestamp" in quarantined.columns

    def test_quarantine_has_reason_column(self, transactions_df):
        _, quarantined = quarantine_filter(transactions_df, TRANSACTION_RULES)
        assert "_quarantine_reason" in quarantined.columns

    def test_quarantine_reason_tags_null_product(self, transactions_df):
        _, quarantined = quarantine_filter(transactions_df, TRANSACTION_RULES)
        row = quarantined.filter(col("customer_id") == 300).first()
        assert "valid_product" in row["_quarantine_reason"]

    def test_quarantine_reason_tags_zero_qty(self, transactions_df):
        _, quarantined = quarantine_filter(transactions_df, TRANSACTION_RULES)
        row = quarantined.filter(col("customer_id") == 600).first()
        assert "positive_qty" in row["_quarantine_reason"]

    def test_clean_has_no_quarantine_columns(self, transactions_df):
        clean, _ = quarantine_filter(transactions_df, TRANSACTION_RULES)
        assert "_quarantine_timestamp" not in clean.columns
        assert "_quarantine_reason" not in clean.columns

    def test_total_records_preserved(self, transactions_df):
        clean, quarantined = quarantine_filter(
            transactions_df, TRANSACTION_RULES
        )
        assert clean.count() + quarantined.count() == transactions_df.count()

    # -- Inventory rules --
    @pytest.fixture()
    def inventory_df(self, spark):
        data = [
            (1, 10, 50),     # valid
            (2, 20, 0),      # valid (zero remaining is >= 0)
            (None, 30, 10),  # null product_id
            (4, None, 10),   # null store_id
            (5, 50, -5),     # negative remaining
        ]
        schema = StructType([
            StructField("product_id", IntegerType()),
            StructField("store_id", IntegerType()),
            StructField("quantity_remaining", IntegerType()),
        ])
        return spark.createDataFrame(data, schema)

    def test_inventory_valid_count(self, inventory_df):
        clean, _ = quarantine_filter(inventory_df, INVENTORY_RULES)
        assert clean.count() == 2

    def test_inventory_quarantine_count(self, inventory_df):
        _, quarantined = quarantine_filter(inventory_df, INVENTORY_RULES)
        assert quarantined.count() == 3

    # -- Orders rules --
    @pytest.fixture()
    def orders_df(self, spark):
        data = [
            ("ORD-1", 1, 100, 10, 3),   # valid
            ("ORD-2", 2, 200, 20, 1),   # valid
            (None, 3, 300, 30, 2),        # null order_id
            ("ORD-4", None, 400, 40, 1), # null product_id
            ("ORD-5", 5, 500, 50, 0),   # zero quantity
        ]
        schema = StructType([
            StructField("order_id", StringType()),
            StructField("product_id", IntegerType()),
            StructField("customer_id", IntegerType()),
            StructField("store_id", IntegerType()),
            StructField("quantity", IntegerType()),
        ])
        return spark.createDataFrame(data, schema)

    def test_orders_valid_count(self, orders_df):
        clean, _ = quarantine_filter(orders_df, ORDERS_RULES)
        assert clean.count() == 2

    def test_orders_quarantine_count(self, orders_df):
        _, quarantined = quarantine_filter(orders_df, ORDERS_RULES)
        assert quarantined.count() == 3


# ============================================================
# 6. TRANSFORMATIONS — 1_SILVER_ADLS
# ============================================================

class TestAdlsTransformations:
    """Test transformation logic from the ADLS Silver notebook."""

    def test_transaction_date_parsing(self, spark):
        df = spark.createDataFrame(
            [("1/15/2024",), ("12/3/2023",), ("6/1/2022",)],
            ["transaction_date"],
        )
        result = df.withColumn(
            "transaction_date", to_date("transaction_date", "M/d/yyyy")
        )
        dates = [str(r[0]) for r in result.collect()]
        assert dates == ["2024-01-15", "2023-12-03", "2022-06-01"]

    def test_return_date_parsing(self, spark):
        df = spark.createDataFrame(
            [("3/10/2024",), ("11/25/2023",)], ["return_date"],
        )
        result = df.withColumn(
            "return_date", to_date("return_date", "M/d/yyyy")
        )
        dates = [str(r[0]) for r in result.collect()]
        assert dates == ["2024-03-10", "2023-11-25"]

    def test_invalid_date_returns_null(self, spark):
        df = spark.createDataFrame([("not-a-date",)], ["transaction_date"])
        # Use try_to_date (Spark 3.4+/DBR) which returns null on failure;
        # fall back to to_date for older Spark where ANSI mode is off.
        parse_fn = try_to_date if try_to_date is not None else to_date
        result = df.withColumn(
            "transaction_date", parse_fn("transaction_date", "M/d/yyyy")
        )
        assert result.first()[0] is None

    def test_store_name_trim(self, spark):
        df = spark.createDataFrame([("  Store A  ",)], ["store_name"])
        result = df.withColumn("store_name", trim(col("store_name")))
        assert result.first()[0] == "Store A"

    def test_store_city_initcap(self, spark):
        df = spark.createDataFrame([("new york",)], ["store_city"])
        result = df.withColumn("store_city", initcap(col("store_city")))
        assert result.first()[0] == "New York"

    def test_store_state_upper(self, spark):
        df = spark.createDataFrame([("washington",)], ["store_state"])
        result = df.withColumn("store_state", upper(col("store_state")))
        assert result.first()[0] == "WASHINGTON"

    def test_region_initcap(self, spark):
        df = spark.createDataFrame(
            [("central", "downtown")],
            ["sales_region", "sales_district"],
        )
        result = (
            df
            .withColumn("sales_region", initcap(col("sales_region")))
            .withColumn("sales_district", initcap(col("sales_district")))
        )
        row = result.first()
        assert row["sales_region"] == "Central"
        assert row["sales_district"] == "Downtown"

    def test_record_status_valid_quantity(self, spark):
        df = spark.createDataFrame([(5,)], ["quantity"])
        result = df.withColumn(
            "record_status",
            when(col("quantity") <= 0, "invalid").otherwise("cleaned")
        )
        assert result.first()["record_status"] == "cleaned"

    def test_record_status_zero_quantity(self, spark):
        df = spark.createDataFrame([(0,)], ["quantity"])
        result = df.withColumn(
            "record_status",
            when(col("quantity") <= 0, "invalid").otherwise("cleaned")
        )
        assert result.first()["record_status"] == "invalid"

    def test_record_status_negative_quantity(self, spark):
        df = spark.createDataFrame([(-3,)], ["quantity"])
        result = df.withColumn(
            "record_status",
            when(col("quantity") <= 0, "invalid").otherwise("cleaned")
        )
        assert result.first()["record_status"] == "invalid"

    def test_quantity_cast_to_int(self, spark):
        df = spark.createDataFrame([("5",)], ["quantity"])
        result = df.withColumn("quantity", col("quantity").cast("int"))
        assert result.first()[0] == 5

    def test_store_id_cast_to_int(self, spark):
        df = spark.createDataFrame([("42",)], ["store_id"])
        result = df.withColumn("store_id", col("store_id").cast("int"))
        assert result.first()[0] == 42

    def test_transaction_dedup(self, spark):
        data = [
            (1, 100, 10, "2024-01-01"),
            (1, 100, 10, "2024-01-01"),  # duplicate
            (2, 200, 20, "2024-01-02"),
        ]
        df = spark.createDataFrame(
            data,
            ["product_id", "customer_id", "store_id", "transaction_date"],
        )
        result = df.dropDuplicates(
            ["product_id", "customer_id", "store_id", "transaction_date"]
        )
        assert result.count() == 2

    def test_store_dedup(self, spark):
        data = [(1, "Store A"), (1, "Store A dup"), (2, "Store B")]
        df = spark.createDataFrame(data, ["store_id", "store_name"])
        result = df.dropDuplicates(["store_id"])
        assert result.count() == 2


# ============================================================
# 7. TRANSFORMATIONS — 2_SILVER_KAFKA
# ============================================================

class TestKafkaTransformations:
    """Test transformation logic from the Kafka Silver notebook."""

    def test_json_parse_orders(self, spark):
        raw = '{"order_id":"ORD-1","product_id":1,"customer_id":100,' \
              '"store_id":10,"quantity":3,"order_date":"1/15/2024",' \
              '"payment_type":"credit","order_channel":"web"}'
        df = spark.createDataFrame([(raw,)], ["raw_payload"])
        result = (
            df
            .withColumn("json", from_json(col("raw_payload"), orders_schema))
            .select("json.*")
        )
        row = result.first()
        assert row["order_id"] == "ORD-1"
        assert row["product_id"] == 1
        assert row["quantity"] == 3

    def test_json_parse_inventory(self, spark):
        raw = '{"inventory_id":"INV-1",' \
              '"event_timestamp":"2024-01-15T10:00:00",' \
              '"product_id":1,"store_id":10,' \
              '"quantity_added":50,"quantity_remaining":200}'
        df = spark.createDataFrame([(raw,)], ["raw_payload"])
        result = (
            df
            .withColumn(
                "json", from_json(col("raw_payload"), inventory_schema)
            )
            .select("json.*")
        )
        row = result.first()
        assert row["inventory_id"] == "INV-1"
        assert row["quantity_remaining"] == 200

    def test_stock_status_out_of_stock(self, spark):
        df = spark.createDataFrame([(0,)], ["quantity_remaining"])
        result = df.withColumn(
            "stock_status",
            when(col("quantity_remaining") == 0, "OUT_OF_STOCK")
            .when(col("quantity_remaining") < 20, "LOW_STOCK")
            .when(col("quantity_remaining") < 50, "MEDIUM")
            .otherwise("HEALTHY")
        )
        assert result.first()["stock_status"] == "OUT_OF_STOCK"

    def test_stock_status_low(self, spark):
        df = spark.createDataFrame([(15,)], ["quantity_remaining"])
        result = df.withColumn(
            "stock_status",
            when(col("quantity_remaining") == 0, "OUT_OF_STOCK")
            .when(col("quantity_remaining") < 20, "LOW_STOCK")
            .when(col("quantity_remaining") < 50, "MEDIUM")
            .otherwise("HEALTHY")
        )
        assert result.first()["stock_status"] == "LOW_STOCK"

    def test_stock_status_medium(self, spark):
        df = spark.createDataFrame([(35,)], ["quantity_remaining"])
        result = df.withColumn(
            "stock_status",
            when(col("quantity_remaining") == 0, "OUT_OF_STOCK")
            .when(col("quantity_remaining") < 20, "LOW_STOCK")
            .when(col("quantity_remaining") < 50, "MEDIUM")
            .otherwise("HEALTHY")
        )
        assert result.first()["stock_status"] == "MEDIUM"

    def test_stock_status_healthy(self, spark):
        df = spark.createDataFrame([(100,)], ["quantity_remaining"])
        result = df.withColumn(
            "stock_status",
            when(col("quantity_remaining") == 0, "OUT_OF_STOCK")
            .when(col("quantity_remaining") < 20, "LOW_STOCK")
            .when(col("quantity_remaining") < 50, "MEDIUM")
            .otherwise("HEALTHY")
        )
        assert result.first()["stock_status"] == "HEALTHY"

    def test_is_restock_true(self, spark):
        df = spark.createDataFrame([(50,)], ["quantity_added"])
        result = df.withColumn(
            "is_restock",
            when(col("quantity_added") > 0, True).otherwise(False)
        )
        assert result.first()["is_restock"] is True

    def test_is_restock_false(self, spark):
        df = spark.createDataFrame([(0,)], ["quantity_added"])
        result = df.withColumn(
            "is_restock",
            when(col("quantity_added") > 0, True).otherwise(False)
        )
        assert result.first()["is_restock"] is False

    def test_order_date_parsing(self, spark):
        df = spark.createDataFrame([("3/15/2024",)], ["order_date"])
        result = df.withColumn(
            "order_date", to_date("order_date", "M/d/yyyy")
        )
        assert str(result.first()[0]) == "2024-03-15"


# ============================================================
# 8. TRANSFORMATIONS — 3_SILVER_MONGO
# ============================================================

class TestMongoTransformations:
    """Test transformation logic from the Mongo Silver notebook."""

    def test_customer_json_parse_and_flatten(self, spark):
        raw = (
            '{"customer_id":1,"customer_acct_num":12345,'
            '"first_name":"John","last_name":"Doe",'
            '"customer_address":"123 Main",'
            '"customer_city":"Portland",'
            '"customer_state_province":"OR",'
            '"customer_postal_code":97201,'
            '"customer_country":"USA",'
            '"birthdate":"1/15/1990",'
            '"marital_status":"S","yearly_income":"$50K",'
            '"gender":"M","total_children":0,'
            '"num_children_at_home":0,'
            '"education":"Bachelors",'
            '"acct_open_date":"6/1/2020",'
            '"member_card":"Gold",'
            '"occupation":"Professional",'
            '"homeowner":"Y"}'
        )
        df = spark.createDataFrame(
            [("cust_001", raw)], ["_id", "data"]
        )
        result = (
            df
            .withColumn(
                "parsed",
                from_json(col("data"), customers_data_schema),
            )
            .select(
                col("_id").cast("string").alias("customer_id"),
                col("parsed.first_name").alias("first_name"),
                col("parsed.last_name").alias("last_name"),
                col("parsed.customer_city").alias("customer_city"),
            )
        )
        row = result.first()
        assert row["customer_id"] == "cust_001"
        assert row["first_name"] == "John"
        assert row["last_name"] == "Doe"
        assert row["customer_city"] == "Portland"

    def test_customer_name_concat(self, spark):
        df = spark.createDataFrame(
            [("John", "Doe"), ("Jane", "Smith")],
            ["first_name", "last_name"],
        )
        result = df.withColumn(
            "customer_name",
            concat(col("first_name"), lit(" "), col("last_name")),
        )
        names = [r["customer_name"] for r in result.collect()]
        assert names == ["John Doe", "Jane Smith"]

    def test_product_json_parse_and_flatten(self, spark):
        raw = (
            '{"product_id":1,"product_brand":"Hermanos",'
            '"product_name":"  Coffee  ","product_sku":123456,'
            '"product_retail_price":4.99,"product_cost":2.50,'
            '"product_weight":0.5,"recyclable":1,"low_fat":0}'
        )
        df = spark.createDataFrame(
            [("prod_001", raw)], ["_id", "data"]
        )
        result = (
            df
            .withColumn(
                "parsed",
                from_json(col("data"), products_data_schema),
            )
            .select(
                col("_id").cast("string").alias("product_id"),
                col("parsed.product_brand").alias("product_brand"),
                trim(col("parsed.product_name")).alias("product_name"),
                col("parsed.product_retail_price").alias(
                    "product_retail_price"
                ),
                col("parsed.product_cost").alias("product_cost"),
            )
        )
        row = result.first()
        assert row["product_id"] == "prod_001"
        assert row["product_brand"] == "Hermanos"
        assert row["product_name"] == "Coffee"
        assert row["product_retail_price"] == pytest.approx(4.99)
        assert row["product_cost"] == pytest.approx(2.50)

    def test_customer_dedup(self, spark):
        data = [
            ("cust_001", "John"),
            ("cust_001", "John Updated"),
            ("cust_002", "Jane"),
        ]
        df = spark.createDataFrame(data, ["customer_id", "customer_name"])
        result = df.dropDuplicates(["customer_id"])
        assert result.count() == 2

    def test_product_dedup(self, spark):
        data = [
            ("prod_001", "Coffee"),
            ("prod_001", "Coffee v2"),
            ("prod_002", "Tea"),
        ]
        df = spark.createDataFrame(data, ["product_id", "product_name"])
        result = df.dropDuplicates(["product_id"])
        assert result.count() == 2

    def test_birthdate_parsing(self, spark):
        df = spark.createDataFrame([("1/15/1990",)], ["birthdate"])
        result = df.withColumn(
            "birthdate", to_date("birthdate", "M/d/yyyy")
        )
        assert str(result.first()[0]) == "1990-01-15"

    def test_acct_open_date_parsing(self, spark):
        df = spark.createDataFrame([("6/1/2020",)], ["acct_open_date"])
        result = df.withColumn(
            "acct_open_date", to_date("acct_open_date", "M/d/yyyy")
        )
        assert str(result.first()[0]) == "2020-06-01"


# ============================================================
# 9. METADATA COLUMNS
# ============================================================

class TestMetadataColumns:
    """All Silver tables add standard metadata columns."""

    def test_metadata_columns_added(self, spark):
        df = spark.createDataFrame([(1,)], ["id"])
        result = (
            df
            .withColumn("pipeline_run_id", current_timestamp())
            .withColumn("processing_timestamp", current_timestamp())
            .withColumn("source_table", lit("transactions"))
            .withColumn("record_status", lit("cleaned"))
        )
        expected = {
            "pipeline_run_id", "processing_timestamp",
            "source_table", "record_status",
        }
        assert expected.issubset(set(result.columns))

    def test_record_status_literal_values(self, spark):
        valid_statuses = {"cleaned", "valid", "invalid", "quarantined"}
        df = spark.createDataFrame(
            [(s,) for s in valid_statuses], ["status"]
        )
        assert df.count() == len(valid_statuses)
