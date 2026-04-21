# """
# Unit tests for src/utils/secret_manager.py

# Uses unittest.mock to simulate Databricks dbutils.secrets,
# since dbutils is only available inside the Databricks runtime.
# Verifies that Kafka credentials are read from the correct
# secret scope and keys as defined in base_config.yml.
# """
# from unittest.mock import MagicMock
# from utils.secret_manager import get_kafka_credentials


# # -- Test helpers --

# def _make_mock_dbutils(secret_map):
#     """Create a mock dbutils that returns values from secret_map."""
#     mock = MagicMock()
#     mock.secrets.get.side_effect = lambda scope, key: secret_map.get(
#         (scope, key), None
#     )
#     return mock


# def _sample_config():
#     """Mirror the kafka section from base_config.yml."""
#     return {
#         "kafka": {
#             "scope": "mongo-kafka-connector",
#             "bootstrap_key": "kafka-bootstrap",
#             "api_key_name": "kafka-api-key",
#             "api_secret_name": "kafka-api-secret",
#         }
#     }


# # -- Tests --

# def test_returns_three_values():
#     """Should return (bootstrap_server, api_key, api_secret)."""
#     config = _sample_config()
#     dbutils = _make_mock_dbutils({
#         ("mongo-kafka-connector", "kafka-bootstrap"): "broker:9092",
#         ("mongo-kafka-connector", "kafka-api-key"): "my-key",
#         ("mongo-kafka-connector", "kafka-api-secret"): "my-secret",
#     })
#     bootstrap, api_key, api_secret = get_kafka_credentials(dbutils, config)
#     assert bootstrap == "broker:9092"
#     assert api_key == "my-key"
#     assert api_secret == "my-secret"


# def test_reads_correct_scope():
#     """All secret lookups must use the 'mongo-kafka-connector' scope."""
#     config = _sample_config()
#     dbutils = _make_mock_dbutils({})
#     get_kafka_credentials(dbutils, config)
#     calls = dbutils.secrets.get.call_args_list
#     for call in calls:
#         assert call[0][0] == "mongo-kafka-connector"


# def test_reads_correct_keys():
#     """Must request exactly these three secret keys."""
#     config = _sample_config()
#     dbutils = _make_mock_dbutils({})
#     get_kafka_credentials(dbutils, config)
#     keys_accessed = [call[0][1] for call in dbutils.secrets.get.call_args_list]
#     assert "kafka-bootstrap" in keys_accessed
#     assert "kafka-api-key" in keys_accessed
#     assert "kafka-api-secret" in keys_accessed

    
