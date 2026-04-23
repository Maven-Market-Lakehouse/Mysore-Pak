# """
# Tests for custom_logger.py  —  PipelineLogger (Delta-backed)

# Uses a mocked SparkSession so tests run without a cluster.

# Run with:
#     python -m pytest tests/test_custom_logger.py -v -p no:cacheprovider
# Or:
#     python tests/test_custom_logger.py
# """

# import json
# import unittest
# from datetime import datetime, timezone
# from unittest.mock import MagicMock, patch, call
# import sys
# import os

# # Ensure src is on path
# sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

# from utils.custom_logger import (
#     PipelineLogger, LOG_SCHEMA, _TableContext,
#     _extract_traceback, _error_type_name,
# )


# SAMPLE_CONFIG = {
#     "env": "dev",
#     "catalog": "maven_market_uc",
#     "audit_schema": "audit",
#     "pipeline_name": "maven_market_pipeline",
# }


# def _make_logger(buffer_size=100, echo=False, config=None):
#     """Create a PipelineLogger with a mocked Spark session."""
#     spark = MagicMock()
#     cfg = config or dict(SAMPLE_CONFIG)
#     logger = PipelineLogger(spark, cfg, buffer_size=buffer_size, echo=echo)
#     return logger, spark


# # -----------------------------------------------------------------------
# # Helper functions
# # -----------------------------------------------------------------------

# class TestExtractTraceback(unittest.TestCase):

#     def test_traceback_from_exception_object(self):
#         """_extract_traceback must work from the exception object itself,
#         even OUTSIDE an except block."""
#         try:
#             raise ValueError("test error")
#         except ValueError as e:
#             stored_error = e  # store it

#         # We are now OUTSIDE the except block
#         tb = _extract_traceback(stored_error)
#         self.assertIn("ValueError", tb)
#         self.assertIn("test error", tb)
#         self.assertIn("Traceback", tb)

#     def test_traceback_includes_chain(self):
#         try:
#             raise RuntimeError("root cause")
#         except RuntimeError:
#             try:
#                 raise TypeError("secondary") from None
#             except TypeError as e:
#                 stored = e
#         tb = _extract_traceback(stored)
#         self.assertIn("TypeError", tb)
#         self.assertIn("secondary", tb)


# class TestErrorTypeName(unittest.TestCase):

#     def test_builtin_exception(self):
#         self.assertEqual(_error_type_name(ValueError("x")), "ValueError")

#     def test_custom_exception(self):
#         class MyCustomError(Exception):
#             pass
#         err = MyCustomError("oops")
#         name = _error_type_name(err)
#         # Should include the module path
#         self.assertIn("MyCustomError", name)

#     def test_nested_class(self):
#         name = _error_type_name(OSError("fail"))
#         self.assertEqual(name, "OSError")


# # -----------------------------------------------------------------------
# # Table resolution
# # -----------------------------------------------------------------------

# class TestTableResolution(unittest.TestCase):

#     def test_default_table_name(self):
#         logger, _ = _make_logger()
#         self.assertEqual(logger.log_table, "maven_market_uc.audit.pipeline_logs")

#     def test_custom_table_name_via_config(self):
#         cfg = {**SAMPLE_CONFIG, "pipeline_logs": "custom_run_log"}
#         logger, _ = _make_logger(config=cfg)
#         self.assertEqual(logger.log_table, "maven_market_uc.audit.custom_run_log")


# # -----------------------------------------------------------------------
# # Pipeline run ID
# # -----------------------------------------------------------------------

# class TestRunId(unittest.TestCase):

#     def test_run_id_is_uuid(self):
#         logger, _ = _make_logger()
#         self.assertEqual(len(logger.pipeline_run_id), 36)
#         self.assertEqual(logger.pipeline_run_id.count("-"), 4)

#     def test_pipeline_start_logged_on_init(self):
#         logger, _ = _make_logger()
#         # Buffer should have exactly 1 entry (PIPELINE_START)
#         self.assertEqual(len(logger._buffer), 1)
#         self.assertEqual(logger._buffer[0]["event_type"], "PIPELINE_START")


# # -----------------------------------------------------------------------
# # Buffer and flush
# # -----------------------------------------------------------------------

# class TestBufferFlush(unittest.TestCase):

#     def test_buffer_accumulates(self):
#         logger, _ = _make_logger(buffer_size=50)
#         logger.log_info("test 1")
#         logger.log_info("test 2")
#         # 1 (PIPELINE_START) + 2 = 3
#         self.assertEqual(len(logger._buffer), 3)

#     def test_auto_flush_at_buffer_size(self):
#         logger, spark = _make_logger(buffer_size=3)
#         # Buffer already has 1 (PIPELINE_START). 2 more will trigger flush.
#         logger.log_info("msg1")
#         logger.log_info("msg2")
#         # flush should have been called, buffer cleared
#         spark.createDataFrame.assert_called()
#         self.assertEqual(len(logger._buffer), 0)

#     def test_manual_flush(self):
#         logger, spark = _make_logger()
#         logger.log_info("something")
#         logger.flush()
#         spark.createDataFrame.assert_called()
#         self.assertEqual(len(logger._buffer), 0)

#     def test_flush_empty_buffer_is_noop(self):
#         logger, spark = _make_logger()
#         logger._buffer.clear()
#         logger.flush()
#         spark.createDataFrame.assert_not_called()

#     def test_flush_failure_preserves_buffer(self):
#         logger, spark = _make_logger()
#         logger.log_info("important")
#         count_before = len(logger._buffer)
#         spark.createDataFrame.side_effect = Exception("write failed")
#         logger.flush()
#         # Entries should be put back
#         self.assertEqual(len(logger._buffer), count_before)
#         spark.createDataFrame.side_effect = None


# # -----------------------------------------------------------------------
# # Entry structure
# # -----------------------------------------------------------------------

# class TestEntryStructure(unittest.TestCase):

#     def test_entry_has_all_fields(self):
#         logger, _ = _make_logger()
#         entry = logger._buffer[0]  # PIPELINE_START
#         expected_keys = {
#             "log_id", "timestamp", "pipeline_name", "pipeline_run_id",
#             "layer", "table_name", "event_type", "log_level", "message",
#             "row_count_input", "row_count_output", "row_count_quarantined",
#             "duration_seconds", "error_type", "error_message",
#             "error_traceback", "extra_metrics", "env", "catalog",
#         }
#         self.assertEqual(set(entry.keys()), expected_keys)

#     def test_env_and_catalog(self):
#         logger, _ = _make_logger()
#         entry = logger._buffer[0]
#         self.assertEqual(entry["env"], "dev")
#         self.assertEqual(entry["catalog"], "maven_market_uc")


# # -----------------------------------------------------------------------
# # log_row_counts
# # -----------------------------------------------------------------------

# class TestLogRowCounts(unittest.TestCase):

#     def test_row_counts_entry(self):
#         logger, _ = _make_logger()
#         logger.log_row_counts("bronze", "transactions", input=5000, output=4800, quarantined=200)
#         entry = logger._buffer[-1]
#         self.assertEqual(entry["event_type"], "ROW_COUNT")
#         self.assertEqual(entry["row_count_input"], 5000)
#         self.assertEqual(entry["row_count_output"], 4800)
#         self.assertEqual(entry["row_count_quarantined"], 200)
#         self.assertEqual(entry["layer"], "bronze")
#         self.assertEqual(entry["table_name"], "transactions")


# # -----------------------------------------------------------------------
# # log_metric
# # -----------------------------------------------------------------------

# class TestLogMetric(unittest.TestCase):

#     def test_metric_entry(self):
#         logger, _ = _make_logger()
#         logger.log_metric("silver", "orders", "avg_order_value", 42.5)
#         entry = logger._buffer[-1]
#         self.assertEqual(entry["event_type"], "METRIC")
#         metrics = json.loads(entry["extra_metrics"])
#         self.assertEqual(metrics["avg_order_value"], 42.5)


# # -----------------------------------------------------------------------
# # log_dq_check
# # -----------------------------------------------------------------------

# class TestLogDqCheck(unittest.TestCase):

#     def test_dq_passed(self):
#         logger, _ = _make_logger()
#         logger.log_dq_check("silver", "stores", "valid_store_id", passed=True)
#         entry = logger._buffer[-1]
#         self.assertEqual(entry["event_type"], "DQ_CHECK")
#         self.assertEqual(entry["log_level"], "INFO")
#         self.assertIn("PASSED", entry["message"])

#     def test_dq_failed(self):
#         logger, _ = _make_logger()
#         logger.log_dq_check("silver", "stores", "valid_store_id", passed=False, details="12 nulls found")
#         entry = logger._buffer[-1]
#         self.assertEqual(entry["log_level"], "WARNING")
#         self.assertIn("FAILED", entry["message"])
#         self.assertIn("12 nulls found", entry["message"])


# # -----------------------------------------------------------------------
# # log_error
# # -----------------------------------------------------------------------

# class TestLogError(unittest.TestCase):

#     def test_error_with_exception_inside_except(self):
#         """Standard case: log_error called inside an except block."""
#         logger, _ = _make_logger()
#         try:
#             raise ValueError("bad data")
#         except ValueError as e:
#             logger.log_error("bronze", "calendar", error=e)
#         entry = logger._buffer[-1]
#         self.assertEqual(entry["event_type"], "ERROR")
#         self.assertEqual(entry["log_level"], "ERROR")
#         self.assertEqual(entry["error_message"], "bad data")
#         self.assertEqual(entry["error_type"], "ValueError")
#         self.assertIn("ValueError", entry["error_traceback"])
#         self.assertIn("bad data", entry["error_traceback"])

#     def test_error_with_exception_outside_except(self):
#         """Critical test: traceback must work even OUTSIDE the except block.
#         This is the bug that was fixed — traceback.format_exc() would
#         return 'NoneType: None' here, but _extract_traceback uses
#         error.__traceback__ directly."""
#         logger, _ = _make_logger()
#         try:
#             raise RuntimeError("connection lost")
#         except RuntimeError as e:
#             stored_error = e

#         # Deliberately OUTSIDE the except block
#         logger.log_error("silver", "stores", error=stored_error)
#         entry = logger._buffer[-1]
#         self.assertEqual(entry["error_message"], "connection lost")
#         self.assertEqual(entry["error_type"], "RuntimeError")
#         self.assertIn("RuntimeError", entry["error_traceback"])
#         self.assertIn("connection lost", entry["error_traceback"])
#         self.assertNotIn("NoneType", entry["error_traceback"])

#     def test_error_with_message_only(self):
#         logger, _ = _make_logger()
#         logger.log_error("bronze", "calendar", message="custom error msg")
#         entry = logger._buffer[-1]
#         self.assertEqual(entry["error_message"], "custom error msg")
#         self.assertIsNone(entry["error_type"])
#         self.assertIsNone(entry["error_traceback"])

#     def test_error_type_in_message(self):
#         """The human-readable message should include the error type."""
#         logger, _ = _make_logger()
#         try:
#             raise KeyError("missing_column")
#         except KeyError as e:
#             logger.log_error("silver", "transactions", error=e)
#         entry = logger._buffer[-1]
#         self.assertIn("KeyError", entry["message"])


# # -----------------------------------------------------------------------
# # table_context
# # -----------------------------------------------------------------------

# class TestTableContext(unittest.TestCase):

#     def test_success_path(self):
#         logger, _ = _make_logger()
#         with logger.table_context("bronze", "transactions") as ctx:
#             ctx.set_row_counts(input=1000, output=950, quarantined=50)
#             ctx.add_metric("null_ratio", 0.05)

#         # Should have: PIPELINE_START + TABLE_START + TABLE_SUCCESS = 3
#         events = [e["event_type"] for e in logger._buffer]
#         self.assertIn("TABLE_START", events)
#         self.assertIn("TABLE_SUCCESS", events)

#         success_entry = [e for e in logger._buffer if e["event_type"] == "TABLE_SUCCESS"][0]
#         self.assertEqual(success_entry["row_count_input"], 1000)
#         self.assertEqual(success_entry["row_count_output"], 950)
#         self.assertEqual(success_entry["row_count_quarantined"], 50)
#         self.assertIsNotNone(success_entry["duration_seconds"])
#         metrics = json.loads(success_entry["extra_metrics"])
#         self.assertEqual(metrics["null_ratio"], 0.05)

#     def test_error_path(self):
#         logger, _ = _make_logger()
#         with self.assertRaises(RuntimeError):
#             with logger.table_context("silver", "stores") as ctx:
#                 ctx.set_row_counts(input=500)
#                 raise RuntimeError("transform failed")

#         events = [e["event_type"] for e in logger._buffer]
#         self.assertIn("TABLE_ERROR", events)
#         err_entry = [e for e in logger._buffer if e["event_type"] == "TABLE_ERROR"][0]
#         self.assertEqual(err_entry["error_message"], "transform failed")
#         self.assertEqual(err_entry["error_type"], "RuntimeError")
#         self.assertIn("RuntimeError", err_entry["error_traceback"])
#         self.assertIn("transform failed", err_entry["error_traceback"])
#         self.assertEqual(err_entry["row_count_input"], 500)

#     def test_error_path_message_includes_type(self):
#         """The TABLE_ERROR message should include the exception type for
#         quick identification in stdout echo."""
#         logger, _ = _make_logger()
#         with self.assertRaises(TypeError):
#             with logger.table_context("gold", "dim_store") as ctx:
#                 raise TypeError("column mismatch")
#         err_entry = [e for e in logger._buffer if e["event_type"] == "TABLE_ERROR"][0]
#         self.assertIn("TypeError", err_entry["message"])
#         self.assertIn("column mismatch", err_entry["message"])


# # -----------------------------------------------------------------------
# # end_pipeline
# # -----------------------------------------------------------------------

# class TestEndPipeline(unittest.TestCase):

#     def test_end_pipeline_success(self):
#         logger, spark = _make_logger()
#         logger.end_pipeline(status="SUCCESS")
#         # flush is called, so createDataFrame should have been invoked
#         spark.createDataFrame.assert_called()

#     def test_end_pipeline_failure(self):
#         logger, _ = _make_logger()
#         logger._buffer.clear()
#         logger.end_pipeline(status="FAILURE")
#         self.assertEqual(logger._buffer, [])  # flushed


# # -----------------------------------------------------------------------
# # _TableContext unit tests
# # -----------------------------------------------------------------------

# class TestTableContextObject(unittest.TestCase):

#     def test_set_row_counts_incremental(self):
#         ctx = _TableContext("bronze", "customers")
#         ctx.set_row_counts(input=100)
#         self.assertEqual(ctx.row_count_input, 100)
#         self.assertIsNone(ctx.row_count_output)
#         ctx.set_row_counts(output=90)
#         self.assertEqual(ctx.row_count_output, 90)
#         self.assertEqual(ctx.row_count_input, 100)  # still set

#     def test_elapsed_time(self):
#         ctx = _TableContext("silver", "orders")
#         import time
#         time.sleep(0.05)
#         self.assertGreater(ctx.elapsed, 0.0)

#     def test_add_metric(self):
#         ctx = _TableContext("gold", "summary")
#         ctx.add_metric("revenue_total", 1_000_000)
#         self.assertEqual(ctx.extra_metrics["revenue_total"], 1_000_000)


# # -----------------------------------------------------------------------
# # _ensure_table
# # -----------------------------------------------------------------------

# class TestEnsureTable(unittest.TestCase):

#     def test_create_table_sql_called(self):
#         logger, spark = _make_logger()
#         spark.sql.assert_called()
#         sql_text = spark.sql.call_args[0][0]
#         self.assertIn("CREATE TABLE IF NOT EXISTS", sql_text)
#         self.assertIn("maven_market_uc.audit.pipeline_logs", sql_text)

#     def test_create_table_includes_error_type_column(self):
#         logger, spark = _make_logger()
#         sql_text = spark.sql.call_args[0][0]
#         self.assertIn("error_type", sql_text)


# if __name__ == "__main__":
#     unittest.main()
