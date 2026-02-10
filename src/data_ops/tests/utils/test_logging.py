"""Tests for DatabricksLogger — unit (mock-based) and Databricks integration."""

import os
import unittest
from datetime import date, datetime
from unittest.mock import MagicMock, patch

from databricks.connect import DatabricksSession
from pyspark.sql.types import Row

from data_ops.utils.logging import DatabricksLogger, LogStatus, create_logger


# ---------------------------------------------------------------------------
# Unit tests (mock-based, no Databricks connection required)
# ---------------------------------------------------------------------------


class TestDatabricksLoggerInitialization(unittest.TestCase):

    def setUp(self):
        self.mock_spark = MagicMock()
        self.log_table_path = "test_catalog.test_schema.test_logs"

    def test_initialization_with_spark_session(self):
        logger = DatabricksLogger(
            domain="test_domain",
            process="test_process",
            log_table_path=self.log_table_path,
            spark=self.mock_spark,
        )
        self.assertEqual(logger.domain, "test_domain")
        self.assertEqual(logger.process, "test_process")
        self.assertEqual(logger.log_table_path, self.log_table_path)
        self.assertIsNotNone(logger.spark)
        self.assertIsNotNone(logger._user)

    @patch("data_ops.utils.logging.SparkSession")
    def test_initialization_without_spark_session(self, mock_spark_class):
        mock_spark_class.getActiveSession.return_value = MagicMock()
        logger = DatabricksLogger(
            domain="test_domain",
            process="test_process",
            log_table_path=self.log_table_path,
        )
        self.assertIsNotNone(logger.spark)
        mock_spark_class.getActiveSession.assert_called_once()

    def test_initialization_validation(self):
        cases = [
            {"domain": "", "process": "test", "error_msg": "domain and process must be non-empty strings"},
            {"domain": "test", "process": "", "error_msg": "domain and process must be non-empty strings"},
        ]
        for case in cases:
            with self.subTest(domain=case["domain"], process=case["process"]):
                with self.assertRaises(ValueError) as ctx:
                    DatabricksLogger(
                        domain=case["domain"],
                        process=case["process"],
                        log_table_path=self.log_table_path,
                        spark=self.mock_spark,
                    )
                self.assertIn(case["error_msg"], str(ctx.exception))


class TestDatabricksLoggerAutoDetection(unittest.TestCase):

    def setUp(self):
        self.mock_spark = MagicMock()
        self.log_table_path = "test_catalog.test_schema.test_logs"

    def test_user_detection_from_sql(self):
        mock_result = MagicMock()
        mock_result.collect.return_value = [Row(current_user="test_user@example.com")]
        self.mock_spark.sql.return_value = mock_result
        self.mock_spark.conf.get.return_value = "workspace_url"

        logger = DatabricksLogger(
            domain="test", process="test", log_table_path=self.log_table_path, spark=self.mock_spark
        )
        self.assertEqual(logger._user, "test_user@example.com")

    def test_user_detection_from_environment_fallback(self):
        self.mock_spark.conf.get.return_value = None
        self.mock_spark.sql.side_effect = Exception("No SQL support")

        with patch.dict(os.environ, {"USER": "test_user"}):
            logger = DatabricksLogger(
                domain="test",
                process="test",
                log_table_path=self.log_table_path,
                spark=self.mock_spark,
            )
            self.assertEqual(logger._user, "test_user")

    def test_source_detection(self):
        logger = DatabricksLogger(
            domain="test", process="test", log_table_path=self.log_table_path, spark=self.mock_spark
        )
        source = logger._detect_source()
        self.assertIsNotNone(source)
        self.assertNotEqual(source, "unknown_source")
        self.assertIn("test_logging", source)


class TestDatabricksLoggerLogging(unittest.TestCase):

    def setUp(self):
        self.mock_spark = MagicMock()
        self.log_table_path = "test_catalog.test_schema.test_logs"
        self.logger = DatabricksLogger(
            domain="test_domain",
            process="test_process",
            log_table_path=self.log_table_path,
            spark=self.mock_spark,
        )

    def test_log_with_different_statuses(self):
        cases: list[tuple[LogStatus, str]] = [
            ("success", "Test success message"),
            ("warning", "Test warning message"),
            ("failure", "Test failure message"),
        ]
        for status, message in cases:
            with self.subTest(status=status):
                self.logger.log(step="test_step", status=status, message=message)
                log_data = self.mock_spark.createDataFrame.call_args[0][0][0]
                self.assertEqual(log_data[3], "test_domain")
                self.assertEqual(log_data[4], "test_process")
                self.assertEqual(log_data[5], "test_step")
                self.assertEqual(log_data[7], status)
                self.assertEqual(log_data[8], message)

    def test_log_validation(self):
        cases = [
            {"step": "", "status": "success", "message": "Test", "error": "step and message must be non-empty strings"},
            {"step": "test", "status": "success", "message": "", "error": "step and message must be non-empty strings"},
            {"step": "test", "status": "invalid", "message": "Test", "error": "Invalid status"},
        ]
        for case in cases:
            with self.subTest(step=case["step"], status=case["status"]):
                with self.assertRaises(ValueError) as ctx:
                    self.logger.log(step=case["step"], status=case["status"], message=case["message"])  # type: ignore
                self.assertIn(case["error"], str(ctx.exception))

    def test_log_contains_date_and_time(self):
        before_log = datetime.now()
        self.logger.log(step="test_step", status="success", message="Test message")
        after_log = datetime.now()

        log_data = self.mock_spark.createDataFrame.call_args[0][0][0]
        self.assertEqual(log_data[0], date.today().isoformat())
        self.assertGreaterEqual(log_data[1], before_log)
        self.assertLessEqual(log_data[1], after_log)

    def test_log_with_source_override(self):
        custom_source = "custom/source/file.py"
        self.logger.log(
            step="test_step", status="success", message="Test message", source_override=custom_source
        )
        log_data = self.mock_spark.createDataFrame.call_args[0][0][0]
        self.assertEqual(log_data[6], custom_source)


class TestDatabricksLoggerConvenienceMethods(unittest.TestCase):

    def setUp(self):
        self.mock_spark = MagicMock()
        self.logger = DatabricksLogger(
            domain="test_domain",
            process="test_process",
            log_table_path="test_catalog.test_schema.test_logs",
            spark=self.mock_spark,
        )

    def test_convenience_methods(self):
        cases = [
            {"method": "success", "expected_status": "success", "message": "Success message"},
            {"method": "warning", "expected_status": "warning", "message": "Warning message"},
            {"method": "failure", "expected_status": "failure", "message": "Failure message"},
        ]
        for case in cases:
            with self.subTest(method=case["method"]):
                getattr(self.logger, case["method"])(step="test_step", message=case["message"])
                log_data = self.mock_spark.createDataFrame.call_args[0][0][0]
                self.assertEqual(log_data[7], case["expected_status"])
                self.assertEqual(log_data[8], case["message"])


class TestDatabricksLoggerWriteToDelta(unittest.TestCase):

    def setUp(self):
        self.mock_spark = MagicMock()
        self.log_table_path = "test_catalog.test_schema.test_logs"
        self.logger = DatabricksLogger(
            domain="test_domain",
            process="test_process",
            log_table_path=self.log_table_path,
            spark=self.mock_spark,
        )

    def test_write_modes(self):
        cases = [
            {"table_exists": True, "expected_mode": "append"},
            {"table_exists": False, "expected_mode": "overwrite"},
        ]
        for case in cases:
            with self.subTest(table_exists=case["table_exists"]):
                self.mock_spark.catalog.tableExists.return_value = case["table_exists"]
                mock_df = MagicMock()
                self.mock_spark.createDataFrame.return_value = mock_df

                self.logger.log(step="test_step", status="success", message="Test message")

                self.mock_spark.catalog.tableExists.assert_called_with(self.log_table_path)
                mock_df.write.format.assert_called_with("delta")
                mock_df.write.format.return_value.mode.assert_called_with(case["expected_mode"])


class TestDatabricksLoggerGetLogs(unittest.TestCase):

    def setUp(self):
        self.mock_spark = MagicMock()
        self.log_table_path = "test_catalog.test_schema.test_logs"
        self.logger = DatabricksLogger(
            domain="test_domain",
            process="test_process",
            log_table_path=self.log_table_path,
            spark=self.mock_spark,
        )

    def test_get_logs_without_filters(self):
        mock_df = MagicMock()
        self.mock_spark.read.format.return_value.table.return_value = mock_df
        self.logger.get_logs()
        self.mock_spark.read.format.assert_called_once_with("delta")
        self.mock_spark.read.format.return_value.table.assert_called_once_with(self.log_table_path)
        mock_df.filter.assert_not_called()
        mock_df.limit.assert_not_called()

    def test_get_logs_with_filters(self):
        cases = [
            {"filters": {"status": "failure"}, "expected_filter_count": 1},
            {"filters": {"status": "failure", "step": "test_step"}, "expected_filter_count": 2},
        ]
        for case in cases:
            with self.subTest(filters=case["filters"]):
                mock_df = MagicMock()
                mock_df.filter.return_value = mock_df
                self.mock_spark.read.format.return_value.table.return_value = mock_df
                self.logger.get_logs(filters=case["filters"])
                self.assertEqual(mock_df.filter.call_count, case["expected_filter_count"])
                mock_df.reset_mock()

    def test_get_logs_with_limit(self):
        mock_df = MagicMock()
        mock_df.limit.return_value = mock_df
        self.mock_spark.read.format.return_value.table.return_value = mock_df
        self.logger.get_logs(limit=10)
        mock_df.limit.assert_called_once_with(10)

    def test_get_logs_with_filters_and_limit(self):
        mock_df = MagicMock()
        mock_df.filter.return_value = mock_df
        mock_df.limit.return_value = mock_df
        self.mock_spark.read.format.return_value.table.return_value = mock_df
        self.logger.get_logs(filters={"status": "failure"}, limit=5)
        mock_df.filter.assert_called_once()
        mock_df.limit.assert_called_once_with(5)


class TestCreateLoggerFactory(unittest.TestCase):

    def setUp(self):
        self.mock_spark = MagicMock()
        self.log_table_path = "test_catalog.test_schema.test_logs"

    def test_create_logger_returns_instance(self):
        logger = create_logger(
            domain="test_domain",
            process="test_process",
            log_table_path=self.log_table_path,
            spark=self.mock_spark,
        )
        self.assertIsInstance(logger, DatabricksLogger)
        self.assertEqual(logger.domain, "test_domain")
        self.assertEqual(logger.process, "test_process")

    @patch("data_ops.utils.logging.SparkSession")
    def test_create_logger_without_spark(self, mock_spark_class):
        mock_spark_class.getActiveSession.return_value = MagicMock()
        logger = create_logger(
            domain="test_domain", process="test_process", log_table_path=self.log_table_path
        )
        self.assertIsInstance(logger, DatabricksLogger)
        self.assertIsNotNone(logger.spark)


class TestLogStatusType(unittest.TestCase):

    def test_valid_statuses_accepted(self):
        mock_spark = MagicMock()
        logger = DatabricksLogger(
            domain="test", process="test", log_table_path="test.test.logs", spark=mock_spark
        )
        for status in ["success", "warning", "failure"]:
            with self.subTest(status=status):
                try:
                    logger.log(step="test", status=status, message="test")  # type: ignore
                except ValueError as e:
                    if "Invalid status" in str(e):
                        self.fail(f"Valid status '{status}' raised ValueError")


# ---------------------------------------------------------------------------
# Integration tests (requires Databricks connectivity)
# ---------------------------------------------------------------------------


class TestDatabricksLoggerIntegration(unittest.TestCase):
    """Integration tests for DatabricksLogger with a real Databricks cluster.

    Uses Databricks Serverless by default. Set DATABRICKS_CLUSTER_ID to target
    a specific cluster.
    """

    @classmethod
    def setUpClass(cls):
        cluster_id = os.getenv("DATABRICKS_CLUSTER_ID")
        serverless_id = os.getenv("DATABRICKS_SERVERLESS_COMPUTE_ID")
        builder = DatabricksSession.builder
        if cluster_id:
            builder = builder.clusterId(cluster_id)
        elif not serverless_id:
            builder = builder.serverless(True)
        cls.spark = builder.getOrCreate()

        cls.test_table_path = "dev.bronze.test_logging"

    @classmethod
    def tearDownClass(cls):
        try:
            cls.spark.sql(f"DROP TABLE IF EXISTS {cls.test_table_path}")
        except Exception:
            pass
        cls.spark.stop()

    def setUp(self):
        try:
            self.spark.sql(f"DROP TABLE IF EXISTS {self.test_table_path}")
        except Exception:
            pass

    def test_initialization_with_databricks_session(self):
        logger = DatabricksLogger(
            domain="test_domain",
            process="test_process",
            log_table_path=self.test_table_path,
            spark=self.spark,
        )
        self.assertIsNotNone(logger)
        self.assertEqual(logger.domain, "test_domain")
        self.assertEqual(logger.process, "test_process")
        self.assertIsNotNone(logger._user)

    def test_log_writes_to_delta_table(self):
        logger = create_logger(
            domain="integration_test",
            process="test_logging",
            log_table_path=self.test_table_path,
            spark=self.spark,
        )
        cases = [
            {"method": "success", "step": "step_1", "message": "Success message"},
            {"method": "warning", "step": "step_2", "message": "Warning message"},
            {"method": "failure", "step": "step_3", "message": "Failure message"},
        ]
        for case in cases:
            getattr(logger, case["method"])(step=case["step"], message=case["message"])

        df = self.spark.read.format("delta").table(self.test_table_path)
        self.assertEqual(df.count(), len(cases))

        for case in cases:
            with self.subTest(method=case["method"]):
                row = df.filter(f"step = '{case['step']}'").collect()[0]
                self.assertEqual(row["message"], case["message"])

    def test_log_metadata(self):
        logger = create_logger(
            domain="metadata_test",
            process="verification",
            log_table_path=self.test_table_path,
            spark=self.spark,
        )
        before = datetime.now()
        logger.success(step="metadata_check", message="Testing metadata")
        after = datetime.now()

        row = self.spark.read.format("delta").table(self.test_table_path).collect()[0]

        self.assertEqual(row["date"], date.today().isoformat())
        self.assertGreaterEqual(row["time"], before)
        self.assertLessEqual(row["time"], after)
        self.assertNotIn(row["user"], (None, "", "unknown_user"))
        self.assertIn("test_logging", row["source"])

    def test_get_logs_with_filters_and_limit(self):
        logger = create_logger(
            domain="filter_test",
            process="get_logs_test",
            log_table_path=self.test_table_path,
            spark=self.spark,
        )
        logger.success(step="read_data", message="Read completed")
        logger.success(step="transform_data", message="Transform completed")
        logger.failure(step="read_data", message="Read failed")
        logger.failure(step="write_data", message="Write failed")

        filter_cases = [
            {"filters": {}, "expected": 4},
            {"filters": {"status": "success"}, "expected": 2},
            {"filters": {"status": "failure"}, "expected": 2},
            {"filters": {"step": "read_data"}, "expected": 2},
            {"filters": {"status": "success", "step": "read_data"}, "expected": 1},
        ]
        for case in filter_cases:
            with self.subTest(filters=case["filters"]):
                self.assertEqual(logger.get_logs(filters=case["filters"]).count(), case["expected"])

        limit_cases = [{"limit": 3, "expected": 3}, {"limit": 10, "expected": 4}, {"limit": 1, "expected": 1}]
        for case in limit_cases:
            with self.subTest(limit=case["limit"]):
                self.assertEqual(logger.get_logs(limit=case["limit"]).count(), case["expected"])

    def test_source_override(self):
        logger = create_logger(
            domain="source_test",
            process="override_test",
            log_table_path=self.test_table_path,
            spark=self.spark,
        )
        custom_source = "pipelines/custom_pipeline.py"
        logger.success(step="custom_step", message="Test message", source_override=custom_source)
        row = self.spark.read.format("delta").table(self.test_table_path).collect()[0]
        self.assertEqual(row["source"], custom_source)

    def test_table_schema(self):
        logger = create_logger(
            domain="schema_test",
            process="schema_check",
            log_table_path=self.test_table_path,
            spark=self.spark,
        )
        logger.success(step="init", message="Initialize table")
        field_names = [f.name for f in self.spark.read.format("delta").table(self.test_table_path).schema.fields]
        for field in ["date", "time", "user", "domain", "process", "step", "source", "status", "message"]:
            with self.subTest(field=field):
                self.assertIn(field, field_names)

    def test_edge_cases(self):
        logger = create_logger(
            domain="edge_case",
            process="edge_test",
            log_table_path=self.test_table_path,
            spark=self.spark,
        )
        cases = [
            {"step": "long_message", "message": "A" * 10000},
            {"step": "special_chars", "message": 'Chars: 日本語, 🎉, "quotes", newlines\ntest'},
        ]
        for case in cases:
            logger.success(step=case["step"], message=case["message"])

        df = self.spark.read.format("delta").table(self.test_table_path)
        for case in cases:
            with self.subTest(step=case["step"]):
                row = df.filter(f"step = '{case['step']}'").collect()[0]
                self.assertEqual(row["message"], case["message"])


if __name__ == "__main__":
    unittest.main()
