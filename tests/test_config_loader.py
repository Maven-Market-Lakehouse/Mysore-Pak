from common.config_loader import load_config


def test_config_loads():
    config = load_config("config/config.yml")
    assert config["project"]["name"] == "maven-market-lakehouse"
    assert config["unity_catalog"]["catalog"] == "maven_market_uc"


def test_config_token_resolution():
    config = load_config("config/config.yml")
    assert "maven_market" in config["sources"]["csv"]["datasets"]["transactions"]["source_path"]
    assert "{{secrets/mongo-username}}" in config["sources"]["mongodb"]["options"]["spark.mongodb.read.connection.uri"]
