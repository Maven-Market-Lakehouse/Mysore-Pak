"""
Unit tests for src/utils/config_loader.py

Validates that both config.yml and base_config.yml load correctly,
contain all required sections/datasets, and that secret tokens
remain as raw strings (not resolved at load time).

TODO: Add test_transformations.py once the DLT pipelines (Bronze,
      Silver, Gold) are running correctly. That file will cover
      unit tests for transformation logic and data quality rules.

See also:
    test_data_validator.py — unit tests for table_exists,
    column_exists, and num_rows_for_value (data-validation helpers).
"""
from utils.config_loader import load_config


# -- config.yml tests --

def test_config_loads():
    """Core fields in config.yml should match expected values."""
    config = load_config("config/config.yml")
    assert config["project"]["name"] == "maven-market-lakehouse"
    assert config["unity_catalog"]["catalog"] == "maven_market_uc"


def test_config_has_all_sections():
    """Every required top-level section must be present."""
    config = load_config("config/config.yml")
    required_sections = ["project", "databricks", "unity_catalog",
                         "storage", "sources", "dlt", "tables", "logging"]
    for section in required_sections:
        assert section in config, f"Missing config section: {section}"


def test_config_token_not_resolved():
    """Tokens like ${secrets.*} should remain as raw strings after loading."""
    config = load_config("config/config.yml")
    mongo_uri = config["sources"]["mongodb"]["options"][
        "spark.mongodb.read.connection.uri"
    ]
    assert "${secrets.mongo-username}" in mongo_uri
    assert "${secrets.mongo-password}" in mongo_uri


# -- base_config.yml tests --

def test_base_config_loads():
    """Storage account details in base_config.yml should match ADLS setup."""
    config = load_config("config/base_config.yml")
    assert config["storage"]["container"] == "historicaldata"
    assert config["storage"]["storage_account"] == "sgmavenmarket"


def test_base_config_datasets():
    """All 5 CSV datasets must have path, checkpoint, and table defined."""
    config = load_config("config/base_config.yml")
    expected_datasets = ["transactions", "returns", "stores",
                         "regions", "calendar"]
    for ds in expected_datasets:
        assert ds in config["datasets"], f"Missing dataset: {ds}"
        assert "path" in config["datasets"][ds]
        assert "checkpoint" in config["datasets"][ds]
        assert "table" in config["datasets"][ds]
