import json
import logging
import traceback
from datetime import datetime, timezone
from typing import Any, Dict, Optional


class StructuredLogger:
    def __init__(self, app_name: str, level: str = "INFO") -> None:
        self.app_name = app_name
        self.logger = logging.getLogger(app_name)
        self.logger.setLevel(level.upper())

        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter("%(message)s")
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

    def _emit(self, level: str, message: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        payload = {
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "app_name": self.app_name,
            "level": level,
            "message": message,
            "metadata": metadata or {},
        }
        self.logger.log(getattr(logging, level, logging.INFO), json.dumps(payload, default=str))

    def info(self, message: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        self._emit("INFO", message, metadata)

    def warning(self, message: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        self._emit("WARNING", message, metadata)

    def error(self, message: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        self._emit("ERROR", message, metadata)


class DeltaAuditSink:
    def __init__(self, spark, audit_table: str) -> None:
        self.spark = spark
        self.audit_table = audit_table

    def write_event(self, event: Dict[str, Any]) -> None:
        event_row = {
            "event_time_utc": datetime.now(timezone.utc).isoformat(),
            **event,
        }
        self.spark.createDataFrame([event_row]).write.mode("append").saveAsTable(self.audit_table)

    def write_error(self, stage: str, exc: Exception, metadata: Optional[Dict[str, Any]] = None) -> None:
        self.write_event(
            {
                "event_type": "error",
                "stage": stage,
                "error_type": type(exc).__name__,
                "error_message": str(exc),
                "traceback": traceback.format_exc(),
                "metadata": json.dumps(metadata or {}, default=str),
            }
        )
