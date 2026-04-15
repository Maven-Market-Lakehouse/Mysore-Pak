import dlt
from pyspark.sql import SparkSession

from common.config_loader import load_config


spark = SparkSession.getActiveSession()
if spark is None:
    spark = SparkSession.builder.getOrCreate()

CONFIG = load_config(spark.conf.get("config.path", "config/config.yml"))


@dlt.table(name="<bronze_table_name>")
def bronze_template():
    # Replace source key and source table with a config entry.
    source_table = CONFIG["sources"]["csv"]["datasets"]["transactions"]["target_table"]
    return spark.read.table(source_table)
