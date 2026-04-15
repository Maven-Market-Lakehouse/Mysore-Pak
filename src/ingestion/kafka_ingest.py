from typing import Dict

from pyspark.sql.functions import col, current_timestamp, lit

from common.logger import StructuredLogger


def ingest_kafka_stream(spark, source_config: Dict, logger: StructuredLogger):
    topics = source_config["options"].get("subscribe", "")
    logger.info("Starting Kafka ingestion", {"topics": topics})

    reader = spark.readStream.format(source_config["format"])
    for key, value in source_config["options"].items():
        reader = reader.option(key, value)

    df = (
        reader.load()
        .selectExpr(
            "CAST(key AS STRING) as event_key",
            "CAST(value AS STRING) as event_payload",
            "topic",
            "timestamp",
        )
        .withColumn("ingestion_ts", current_timestamp())
        .withColumn("source_system", lit("kafka"))
        .withColumn("kafka_topic", col("topic"))
    )

    return (
        df.writeStream
        .format("delta")
        .option("checkpointLocation", "/tmp/checkpoints/kafka_events")
        .trigger(availableNow=True)
        .toTable(source_config["target_table"])
    )
