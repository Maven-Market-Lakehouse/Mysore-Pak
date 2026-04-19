import sys
sys.path.append("/Workspace/maven_market/Mysore-Pak")

from src.ingestion.CSV-Ingestion.schema_registry import SCHEMAS
from src.ingestion.CSV-Ingestion.reader import read_data
from src.ingestion.CSV-Ingestion.writer import write_data
from src.utils.logger import get_logger
from pyspark.sql.functions import current_timestamp, col
from src.utils.config_loader import load_config


logger = get_logger()

def run_adls_ingestion(spark, config):

    for dataset in config["datasets"]:

        logger.info(f"Starting ingestion: {dataset}")

        ds = config["datasets"][dataset]

        df = read_data(spark, ds["path"], SCHEMAS[dataset])

        df = df.withColumn("ingestion_time", current_timestamp()) \
               .withColumn("source_file", col("_metadata.file_path"))

        write_data(df, ds["table"], ds["checkpoint"])

        logger.info(f"Completed ingestion: {dataset}")

config = load_config("/Workspace/maven_market/Mysore-Pak/config/base_config.yml")

run_adls_ingestion(spark, config)
