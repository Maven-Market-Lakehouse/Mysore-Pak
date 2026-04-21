from datetime import datetime


class AuditLogger:
    def __init__(self, spark, config):
        self.spark = spark
        self.table = f"{config['catalog']}.{config['audit_schema']}.{config['audit_table']}"

    def log_event(self, event_type, table_name, status, record_count=None, error_message=None):
        data = [{
            "event_timestamp": datetime.utcnow(),
            "event_type": event_type,
            "table_name": table_name,
            "status": status,
            "record_count": record_count,
            "error_message": error_message
        }]
        df = self.spark.createDataFrame(data)
        df.write.mode("append").saveAsTable(self.table)