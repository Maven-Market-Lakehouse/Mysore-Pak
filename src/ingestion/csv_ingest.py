from typing import Dict

from pyspark.sql.functions import current_timestamp, input_file_name, lit

from common.logger import StructuredLogger


def ingest_csv_source(spark, dataset_name: str, dataset_config: Dict, logger: StructuredLogger):
    logger.info("Starting CSV ingestion", {"dataset": dataset_name})

    options = dataset_config.get("options", {})
    reader = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", dataset_config["format"])
        .option("cloudFiles.schemaLocation", f"/tmp/schema/{dataset_name}")
    )

    for key, value in options.items():
        reader = reader.option(key, value)

    if dataset_config.get("schema"):
        reader = reader.schema(dataset_config["schema"])

    df = (
        reader.load(dataset_config["source_path"])
        .withColumn("ingestion_ts", current_timestamp())
        .withColumn("source_file", input_file_name())
        .withColumn("source_system", lit("adls_csv"))
    )

    return (
        df.writeStream
        .format("delta")
        .option("checkpointLocation", f"/tmp/checkpoints/{dataset_name}")
        .trigger(availableNow=True)
        .toTable(dataset_config["target_table"])
    )
