from typing import Dict

from pyspark.sql.functions import current_timestamp, lit

from common.logger import StructuredLogger


def ingest_mongodb(spark, source_config: Dict, logger: StructuredLogger) -> None:
    collection = source_config["options"]["spark.mongodb.read.collection"]
    logger.info("Starting MongoDB ingestion", {"collection": collection})

    reader = spark.read.format(source_config["format"])
    for key, value in source_config["options"].items():
        reader = reader.option(key, value)

    df = (
        reader.load()
        .withColumn("ingestion_ts", current_timestamp())
        .withColumn("source_system", lit("mongodb"))
    )

    df.write.mode("overwrite").format("delta").saveAsTable(source_config["target_table"])
    logger.info(
        "MongoDB ingestion completed",
        {"target_table": source_config["target_table"]},
    )
