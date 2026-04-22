import os
import yaml

@pytest.fixture(scope="session")
def config():
    possible_paths = [
        "/Workspace/maven_market/Mysore-Pak/config/config.yml",  # Databricks
        os.path.join(os.getcwd(), "config", "config.yml"),      # GitHub Actions
        os.path.join(os.path.dirname(__file__), "..", "config", "config.yml")  # local
    ]

    for path in possible_paths:
        if os.path.exists(path):
            with open(path) as f:
                return yaml.safe_load(f)

    raise FileNotFoundError("config.yml not found in expected locations")

# import pytest
# from pyspark.sql import SparkSession
# import yaml

# @pytest.fixture(scope="session")
# def spark():
#     return SparkSession.builder \
#         .appName("maven-market-tests") \
#         .getOrCreate()

# @pytest.fixture(scope="session")
# def config():
#     with open("/Workspace/maven_market/Mysore-Pak/config/config.yml") as f:
#         return yaml.safe_load(f)

# def tbl(config, layer, name):
#     return f"{config['catalog']}.{config[f'{layer}_schema']}.{config[name]}"
