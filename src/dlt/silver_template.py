import dlt
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from common.config_loader import load_config


spark = SparkSession.getActiveSession()
if spark is None:
    spark = SparkSession.builder.getOrCreate()

CONFIG = load_config(spark.conf.get("config.path", "config/config.yml"))


@dlt.table(name="<silver_table_name>")
@dlt.expect("quality_rule", "<replace_with_rule>")
def silver_template():
    bronze = dlt.read("<bronze_table_name>")
    return bronze.withColumn("processed_at", F.current_timestamp())
