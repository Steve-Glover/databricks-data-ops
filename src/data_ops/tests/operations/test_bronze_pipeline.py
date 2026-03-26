"""Unit tests for BronzePipelineConfig and run()."""

import unittest
from unittest.mock import MagicMock, patch

from data_ops.operations.bronze_pipeline import BronzePipelineConfig, run

BASE_CFG = dict(
    catalog="dev",
    domain="member",
    tables=["table_a", "table_b"],
    source_volume_path="/Volumes/dev/bronze/external/mft/",
    archive_volume_path="/Volumes/dev/bronze/external/archive/",
    log_table_path="dev.ua.logs",
)


def _make_config(**overrides) -> BronzePipelineConfig:
    return BronzePipelineConfig(**{**BASE_CFG, **overrides})


def _make_spark() -> MagicMock:
    return MagicMock()


class TestBronzePipelineConfig(unittest.TestCase):
    def test_valid_config(self):
        config = _make_config()
        self.assertEqual(config.domain, "member")
        self.assertEqual(config.tables, ["table_a", "table_b"])
        self.assertEqual(config.catalog, "dev")

    def test_empty_tables_allowed(self):
        config = _make_config(tables=[])
        self.assertEqual(config.tables, [])

    def test_empty_domain_raises(self):
        for bad in ("", "  "):
            with self.subTest(domain=repr(bad)):
                with self.assertRaises(ValueError):
                    _make_config(domain=bad)

    def test_config_is_immutable(self):
        config = _make_config()
        with self.assertRaises(Exception):
            config.domain = "other"

    def test_inherited_volume_path_validator(self):
        with self.assertRaises(ValueError):
            _make_config(source_volume_path="not/a/volume/path")

    def test_inherited_catalog_validator(self):
        with self.assertRaises(ValueError):
            _make_config(catalog="")


class TestRun(unittest.TestCase):
    def _run_with_results(self, results: dict, config: BronzePipelineConfig | None = None):
        """Invoke run() with a mocked extractor returning the given results dict."""
        if config is None:
            config = _make_config()
        spark = _make_spark()

        with (
            patch("data_ops.operations.bronze_pipeline.VolumeExtractor") as mock_extractor_cls,
            patch("data_ops.operations.bronze_pipeline.DatabricksLogger") as mock_logger_cls,
        ):
            mock_extractor = MagicMock()
            mock_extractor.extract.return_value = results
            mock_extractor_cls.return_value = mock_extractor

            mock_logger = MagicMock()
            mock_logger_cls.return_value = mock_logger

            return run(config, spark), mock_logger, mock_extractor, mock_extractor_cls

    def test_all_succeed_no_raise(self):
        results = {"table_a": "success", "table_b": "success"}
        _, mock_logger, _, _ = self._run_with_results(results)
        mock_logger.success.assert_called_once()
        mock_logger.failure.assert_not_called()

    def test_partial_failure_raises(self):
        results = {"table_a": "success", "table_b": "some error"}
        with self.assertRaises(RuntimeError):
            self._run_with_results(results)

    def test_all_failure_raises(self):
        results = {"table_a": "error a", "table_b": "error b"}
        with self.assertRaises(RuntimeError):
            self._run_with_results(results)

    def test_empty_tables_raises(self):
        config = _make_config(tables=[])
        spark = _make_spark()
        with (
            patch("data_ops.operations.bronze_pipeline.VolumeExtractor") as mock_extractor_cls,
            patch("data_ops.operations.bronze_pipeline.DatabricksLogger") as mock_logger_cls,
        ):
            mock_logger_cls.return_value = MagicMock()
            with self.assertRaises(RuntimeError):
                run(config, spark)
            mock_extractor_cls.assert_not_called()

    def test_failure_step_name_format(self):
        results = {"table_a": "success", "table_b": "some error"}
        config = _make_config(domain="member")
        with (
            patch("data_ops.operations.bronze_pipeline.VolumeExtractor") as mock_extractor_cls,
            patch("data_ops.operations.bronze_pipeline.DatabricksLogger") as mock_logger_cls,
        ):
            mock_extractor = MagicMock()
            mock_extractor.extract.return_value = results
            mock_extractor_cls.return_value = mock_extractor
            mock_logger = MagicMock()
            mock_logger_cls.return_value = mock_logger

            with self.assertRaises(RuntimeError):
                run(config, _make_spark())

        step_calls = [c.kwargs["step"] for c in mock_logger.failure.call_args_list]
        self.assertIn("bronze_ingestion - member - table_b", step_calls)

    def test_extractor_receives_pipeline_config(self):
        config = _make_config()
        spark = _make_spark()

        with (
            patch("data_ops.operations.bronze_pipeline.VolumeExtractor") as mock_extractor_cls,
            patch("data_ops.operations.bronze_pipeline.DatabricksLogger"),
        ):
            mock_extractor_cls.return_value.extract.return_value = {
                t: "success" for t in config.tables
            }
            run(config, spark)

        extraction_config_arg = mock_extractor_cls.call_args[0][0]
        self.assertIs(extraction_config_arg, config)

    def test_extractor_called_with_config_tables(self):
        tables = ["table_a", "table_b"]
        config = _make_config(tables=tables)
        _, _, mock_extractor, _ = self._run_with_results(
            {t: "success" for t in tables}, config=config
        )
        mock_extractor.extract.assert_called_once_with(tables)

    def test_logger_constructed_with_domain_and_process(self):
        config = _make_config(domain="member")
        spark = _make_spark()

        with (
            patch("data_ops.operations.bronze_pipeline.VolumeExtractor") as mock_extractor_cls,
            patch("data_ops.operations.bronze_pipeline.DatabricksLogger") as mock_logger_cls,
        ):
            mock_extractor_cls.return_value.extract.return_value = {
                t: "success" for t in config.tables
            }
            mock_logger_cls.return_value = MagicMock()
            run(config, spark)

        mock_logger_cls.assert_called_once_with(
            domain="member",
            process="bronze_ingestion",
            log_table_path=config.log_table_path,
            spark=spark,
        )


if __name__ == "__main__":
    unittest.main()
