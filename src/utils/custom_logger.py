"""
custom_logger.py — Centralized structured logging framework for Maven Market.

==========================================================================
  THIS IS A **NEW** STANDALONE FILE.
  It does NOT replace or modify the existing logger.py or audit_logger.py.
==========================================================================

Writes every log entry to a **centralized Delta log table** at:
    {catalog}.{audit_schema}.pipeline_logs

Captures pipeline metrics, row counts, error states, durations, and
arbitrary KPIs — all queryable via SQL after the run.

Integration
-----------
    from utils.custom_logger import PipelineLogger

Quick Usage
-----------
    # 1. Initialise (once per pipeline run)
    logger = PipelineLogger(spark, config)

    # 2. Wrap a table operation
    with logger.table_context("bronze", "transactions") as ctx:
        df = spark.read.csv(...)
        ctx.set_row_counts(input=df.count())
        df_clean = df.filter(...)
        ctx.set_row_counts(output=df_clean.count())

    # 3. Log a one-off metric
    logger.log_metric("bronze", "transactions", "avg_order_value", 42.5)

    # 4. Log an error
    logger.log_error("silver", "stores", error=e)

    # 5. Flush remaining buffer (auto-flushed periodically)
    logger.flush()
"""

from __future__ import annotations

import json
import logging
import traceback
import uuid
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from time import perf_counter
from typing import Any, Dict, List, Optional

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_LOG_TABLE_KEY = "pipeline_logs"  # config key; falls back to this literal

LOG_SCHEMA = StructType([
    StructField("log_id", StringType(), False),
    StructField("timestamp", TimestampType(), False),
    StructField("pipeline_name", StringType(), True),
    StructField("pipeline_run_id", StringType(), True),
    StructField("layer", StringType(), True),
    StructField("table_name", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("log_level", StringType(), True),
    StructField("message", StringType(), True),
    StructField("row_count_input", LongType(), True),
    StructField("row_count_output", LongType(), True),
    StructField("row_count_quarantined", LongType(), True),
    StructField("duration_seconds", DoubleType(), True),
    StructField("error_type", StringType(), True),
    StructField("error_message", StringType(), True),
    StructField("error_traceback", StringType(), True),
    StructField("extra_metrics", StringType(), True),
    StructField("env", StringType(), True),
    StructField("catalog", StringType(), True),
])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _extract_traceback(error: BaseException) -> str:
    """Extract the full traceback from an exception object itself,
    so it works even OUTSIDE an active ``except`` block."""
    return "".join(
        traceback.format_exception(type(error), error, error.__traceback__)
    )


def _error_type_name(error: BaseException) -> str:
    """Return the fully qualified class name of an exception.

    Example: ``pyspark.errors.exceptions.captured.AnalysisException``
    """
    cls = type(error)
    module = cls.__module__ or ""
    qualname = cls.__qualname__
    return f"{module}.{qualname}" if module and module != "builtins" else qualname


# ---------------------------------------------------------------------------
# Stdout echo (real-time visibility in notebook / driver logs)
# ---------------------------------------------------------------------------

def _get_stdout_logger() -> logging.Logger:
    """Shared stdlib logger for real-time console echo."""
    logger = logging.getLogger("pipeline_logger_echo")
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter("[%(asctime)s] %(levelname)-8s %(message)s",
                              datefmt="%Y-%m-%d %H:%M:%S")
        )
        logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    return logger


_echo = _get_stdout_logger()


# ---------------------------------------------------------------------------
# Table-operation context (returned by table_context)
# ---------------------------------------------------------------------------

class _TableContext:
    """Mutable bag that accumulates row counts and metrics for one table
    operation.  Returned by :meth:`PipelineLogger.table_context`.
    """

    def __init__(self, layer: str, table_name: str) -> None:
        self.layer = layer
        self.table_name = table_name
        self.row_count_input: Optional[int] = None
        self.row_count_output: Optional[int] = None
        self.row_count_quarantined: Optional[int] = None
        self.extra_metrics: Dict[str, Any] = {}
        self._start: float = perf_counter()

    def set_row_counts(
        self,
        input: Optional[int] = None,
        output: Optional[int] = None,
        quarantined: Optional[int] = None,
    ) -> None:
        """Set row counts incrementally (call multiple times if needed)."""
        if input is not None:
            self.row_count_input = input
        if output is not None:
            self.row_count_output = output
        if quarantined is not None:
            self.row_count_quarantined = quarantined

    def add_metric(self, key: str, value: Any) -> None:
        """Attach an arbitrary KPI (stored as JSON in ``extra_metrics``)."""
        self.extra_metrics[key] = value

    @property
    def elapsed(self) -> float:
        return round(perf_counter() - self._start, 3)


# ---------------------------------------------------------------------------
# PipelineLogger
# ---------------------------------------------------------------------------

class PipelineLogger:
    """Centralized structured logger that writes to a Delta table.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.
    config : dict
        Parsed ``config.yml``.  Expected keys: ``catalog``, ``audit_schema``,
        ``pipeline_name``, ``env``.  Optional: ``pipeline_logs`` (custom table
        name override).
    buffer_size : int
        Number of log entries to buffer before auto-flushing to Delta
        (default 25).  Call :meth:`flush` at the end of the pipeline to
        write any remaining entries.
    echo : bool
        If True (default), also print every log line to stdout so you get
        real-time visibility in notebooks / driver logs.
    """

    def __init__(
        self,
        spark: SparkSession,
        config: Dict[str, Any],
        buffer_size: int = 25,
        echo: bool = True,
    ) -> None:
        self.spark = spark
        self.config = config
        self.buffer_size = buffer_size
        self.echo = echo

        # Resolve table name from config
        catalog = config.get("catalog", "maven_market_uc")
        schema = config.get("audit_schema", "audit")
        table = config.get("pipeline_logs", _LOG_TABLE_KEY)
        self.log_table = f"{catalog}.{schema}.{table}"

        # Pipeline-level metadata
        self.pipeline_name = config.get("pipeline_name", "unknown")
        self.pipeline_run_id = str(uuid.uuid4())
        self.env = config.get("env", "dev")
        self.catalog = catalog

        # Internal buffer + lock (thread-safe)
        self._buffer: List[Dict[str, Any]] = []
        self._lock = threading.Lock()

        # Try to ensure the log table exists.
        # In SDP (DLT) contexts, spark.sql("CREATE TABLE") is blocked,
        # so we gracefully skip — the table will be auto-created on
        # first flush() via saveAsTable().
        self._ensure_table()

        # Log pipeline start
        self._add_entry(
            event_type="PIPELINE_START",
            log_level="INFO",
            message=f"Pipeline '{self.pipeline_name}' started  |  run_id={self.pipeline_run_id}",
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @contextmanager
    def table_context(self, layer: str, table_name: str):
        """Context manager that times a table operation and records row
        counts, metrics, and errors automatically.

        Usage::

            with logger.table_context("bronze", "transactions") as ctx:
                df = spark.read.csv(path)
                ctx.set_row_counts(input=df.count())
                # ... transformations ...
                ctx.set_row_counts(output=result.count())
        """
        ctx = _TableContext(layer, table_name)
        self._add_entry(
            layer=layer,
            table_name=table_name,
            event_type="TABLE_START",
            log_level="INFO",
            message=f"[{layer.upper()}] {table_name} — processing started",
        )
        try:
            yield ctx
            # Success path
            self._add_entry(
                layer=layer,
                table_name=table_name,
                event_type="TABLE_SUCCESS",
                log_level="INFO",
                message=f"[{layer.upper()}] {table_name} — completed in {ctx.elapsed}s",
                row_count_input=ctx.row_count_input,
                row_count_output=ctx.row_count_output,
                row_count_quarantined=ctx.row_count_quarantined,
                duration_seconds=ctx.elapsed,
                extra_metrics=ctx.extra_metrics or None,
            )
        except Exception as exc:
            self._add_entry(
                layer=layer,
                table_name=table_name,
                event_type="TABLE_ERROR",
                log_level="ERROR",
                message=f"[{layer.upper()}] {table_name} — FAILED after {ctx.elapsed}s: {_error_type_name(exc)}: {exc}",
                row_count_input=ctx.row_count_input,
                row_count_output=ctx.row_count_output,
                row_count_quarantined=ctx.row_count_quarantined,
                duration_seconds=ctx.elapsed,
                error_type=_error_type_name(exc),
                error_message=str(exc),
                error_traceback=_extract_traceback(exc),
            )
            raise  # re-raise so the pipeline still fails visibly

    def log_row_counts(
        self,
        layer: str,
        table_name: str,
        *,
        input: Optional[int] = None,
        output: Optional[int] = None,
        quarantined: Optional[int] = None,
    ) -> None:
        """Log row counts outside of a ``table_context``."""
        parts = []
        if input is not None:
            parts.append(f"input={input}")
        if output is not None:
            parts.append(f"output={output}")
        if quarantined is not None:
            parts.append(f"quarantined={quarantined}")
        msg = f"[{layer.upper()}] {table_name} — row_counts: {', '.join(parts)}"

        self._add_entry(
            layer=layer,
            table_name=table_name,
            event_type="ROW_COUNT",
            log_level="INFO",
            message=msg,
            row_count_input=input,
            row_count_output=output,
            row_count_quarantined=quarantined,
        )

    def log_metric(
        self, layer: str, table_name: str, metric_name: str, value: Any
    ) -> None:
        """Log a single pipeline metric (e.g. avg latency, null ratio)."""
        self._add_entry(
            layer=layer,
            table_name=table_name,
            event_type="METRIC",
            log_level="INFO",
            message=f"[{layer.upper()}] {table_name} — {metric_name}={value}",
            extra_metrics={metric_name: value},
        )

    def log_dq_check(
        self,
        layer: str,
        table_name: str,
        check_name: str,
        passed: bool,
        details: Optional[str] = None,
    ) -> None:
        """Log a data-quality expectation result."""
        status = "PASSED" if passed else "FAILED"
        lvl = "INFO" if passed else "WARNING"
        self._add_entry(
            layer=layer,
            table_name=table_name,
            event_type="DQ_CHECK",
            log_level=lvl,
            message=f"[{layer.upper()}] {table_name} — DQ '{check_name}': {status}"
                    + (f" — {details}" if details else ""),
            extra_metrics={"check": check_name, "passed": passed, "details": details},
        )

    def log_error(
        self,
        layer: str,
        table_name: str,
        error: Optional[Exception] = None,
        message: Optional[str] = None,
    ) -> None:
        """Log an error with full traceback extracted from the exception object.

        Works correctly both INSIDE and OUTSIDE an ``except`` block — the
        traceback is pulled from ``error.__traceback__``, not from
        ``sys.exc_info()``.
        """
        err_msg = message or (str(error) if error else "Unknown error")
        err_type = _error_type_name(error) if error else None
        tb = _extract_traceback(error) if error else None
        self._add_entry(
            layer=layer,
            table_name=table_name,
            event_type="ERROR",
            log_level="ERROR",
            message=f"[{layer.upper()}] {table_name} — {err_type or 'ERROR'}: {err_msg}",
            error_type=err_type,
            error_message=err_msg,
            error_traceback=tb,
        )

    def log_info(self, message: str, layer: Optional[str] = None,
                 table_name: Optional[str] = None) -> None:
        """Log a general informational message."""
        self._add_entry(
            layer=layer,
            table_name=table_name,
            event_type="INFO",
            log_level="INFO",
            message=message,
        )

    def log_warning(self, message: str, layer: Optional[str] = None,
                    table_name: Optional[str] = None) -> None:
        """Log a warning."""
        self._add_entry(
            layer=layer,
            table_name=table_name,
            event_type="WARNING",
            log_level="WARNING",
            message=message,
        )

    def end_pipeline(self, status: str = "SUCCESS") -> None:
        """Mark the pipeline run as finished and flush all remaining logs."""
        self._add_entry(
            event_type="PIPELINE_END",
            log_level="INFO" if status == "SUCCESS" else "ERROR",
            message=f"Pipeline '{self.pipeline_name}' ended  |  status={status}  |  run_id={self.pipeline_run_id}",
        )
        self.flush()

    def flush(self) -> None:
        """Write all buffered log entries to the Delta table.

        Uses ``saveAsTable`` which auto-creates the table on first write
        if it does not exist — no prior ``CREATE TABLE`` needed.
        """
        with self._lock:
            if not self._buffer:
                return
            batch = list(self._buffer)
            self._buffer.clear()

        try:
            df = self.spark.createDataFrame(batch, schema=LOG_SCHEMA)
            df.write.format("delta").mode("append").saveAsTable(self.log_table)
        except Exception as exc:
            _echo.error(f"Failed to flush logs to {self.log_table}: {exc}")
            # Put entries back so we don't lose them
            with self._lock:
                self._buffer = batch + self._buffer

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _add_entry(self, **fields: Any) -> None:
        """Build a log row, buffer it, and optionally echo to stdout."""
        entry: Dict[str, Any] = {
            "log_id": str(uuid.uuid4()),
            "timestamp": datetime.now(timezone.utc),
            "pipeline_name": self.pipeline_name,
            "pipeline_run_id": self.pipeline_run_id,
            "layer": fields.get("layer"),
            "table_name": fields.get("table_name"),
            "event_type": fields.get("event_type"),
            "log_level": fields.get("log_level", "INFO"),
            "message": fields.get("message"),
            "row_count_input": fields.get("row_count_input"),
            "row_count_output": fields.get("row_count_output"),
            "row_count_quarantined": fields.get("row_count_quarantined"),
            "duration_seconds": fields.get("duration_seconds"),
            "error_type": fields.get("error_type"),
            "error_message": fields.get("error_message"),
            "error_traceback": fields.get("error_traceback"),
            "extra_metrics": (
                json.dumps(fields["extra_metrics"], default=str)
                if fields.get("extra_metrics")
                else None
            ),
            "env": self.env,
            "catalog": self.catalog,
        }

        # Stdout echo for real-time visibility
        if self.echo:
            lvl = getattr(logging, entry["log_level"], logging.INFO)
            _echo.log(lvl, entry["message"])

        with self._lock:
            self._buffer.append(entry)
            should_flush = len(self._buffer) >= self.buffer_size

        if should_flush:
            self.flush()

    def _ensure_table(self) -> None:
        """Best-effort table creation.

        In normal Spark contexts, runs ``CREATE TABLE IF NOT EXISTS``.
        In SDP (DLT) contexts, ``spark.sql("CREATE TABLE ...")`` is
        blocked — so we catch the error and skip gracefully.  The table
        will be auto-created on the first ``flush()`` call via
        ``saveAsTable()``.
        """
        try:
            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {self.log_table} (
                    log_id              STRING      NOT NULL,
                    timestamp           TIMESTAMP   NOT NULL,
                    pipeline_name       STRING,
                    pipeline_run_id     STRING,
                    layer               STRING,
                    table_name          STRING,
                    event_type          STRING,
                    log_level           STRING,
                    message             STRING,
                    row_count_input     BIGINT,
                    row_count_output    BIGINT,
                    row_count_quarantined BIGINT,
                    duration_seconds    DOUBLE,
                    error_type          STRING,
                    error_message       STRING,
                    error_traceback     STRING,
                    extra_metrics       STRING,
                    env                 STRING,
                    catalog             STRING
                )
                USING DELTA
                COMMENT 'Centralized pipeline execution logs'
            """)
        except Exception:
            # SDP/DLT blocks CREATE TABLE via spark.sql().
            # Table will be auto-created by saveAsTable() on first flush.
            _echo.info(
                f"Skipped CREATE TABLE for {self.log_table} "
                f"(likely SDP/DLT context — table will be created on first flush)"
            )
