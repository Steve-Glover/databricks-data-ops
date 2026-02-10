"""Unit tests for VolumeExtractionConfig and VolumeExtractor."""

import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from data_ops.operations.volume_extractor import (
    VolumeExtractionConfig,
    VolumeExtractor,
    _FILE_PATTERN,
)
from data_ops.utils.errors import DataValidationError

BASE_CFG = dict(
    catalog="dev",
    source_volume_path="/Volumes/dev/bronze/external/mft/",
    archive_volume_path="/Volumes/dev/bronze/external/archive/",
    log_table_path="dev.bronze.extraction_logs",
)


def _make_extractor():
    ext = VolumeExtractor.__new__(VolumeExtractor)
    ext.config = VolumeExtractionConfig(**BASE_CFG)
    ext.spark = MagicMock()
    ext.logger = MagicMock()
    ext.dbutils = MagicMock()
    return ext


def _make_df(rows=1000, cols=15, unique=500):
    df = MagicMock()
    df.count.return_value = rows
    df.columns = [f"c{i}" for i in range(cols)]
    df.select.return_value.distinct.return_value.count.return_value = unique
    return df


META = {
    "number_of_rows": "1000",
    "number_of_columns": "15",
    "number_of_unique_ids": "500",
    "id_column": "member_id",
}


class TestVolumeExtractionConfig(unittest.TestCase):

    def test_valid_config_and_get_table_path(self):
        cfg = VolumeExtractionConfig(**BASE_CFG)
        self.assertEqual(cfg.get_table_path("customers"), "dev.bronze.customers")

    def test_path_normalization(self):
        for path, expected_suffix in [("/Volumes/x", "/"), ("/Volumes/x/", "/")]:
            with self.subTest(path=path):
                cfg = VolumeExtractionConfig(**{**BASE_CFG, "source_volume_path": path})
                self.assertTrue(cfg.source_volume_path.endswith(expected_suffix))

    def test_invalid_inputs(self):
        cases = [
            ("bad path", {**BASE_CFG, "source_volume_path": "/dbfs/mnt/"}),
            ("empty catalog", {**BASE_CFG, "catalog": "  "}),
        ]
        for label, kwargs in cases:
            with self.subTest(label=label):
                with self.assertRaises(ValueError):
                    VolumeExtractionConfig(**kwargs)

    def test_immutability(self):
        cfg = VolumeExtractionConfig(**BASE_CFG)
        with self.assertRaises(Exception):
            cfg.catalog = "prod"


class TestFilePattern(unittest.TestCase):

    def test_matches_and_extractions(self):
        cases = [
            ("customers_202401_202403", "customers"),
            ("order_items_202401_202403", "order_items"),
        ]
        for filename, expected_table in cases:
            with self.subTest(filename=filename):
                m = _FILE_PATTERN.match(filename)
                self.assertIsNotNone(m)
                self.assertEqual(m.group(1), expected_table)

    def test_non_matches(self):
        for name in ["customers.meta", "customers_20240101_20240131", "README.md"]:
            with self.subTest(name=name):
                self.assertIsNone(_FILE_PATTERN.match(name))


class TestDiscoverTables(unittest.TestCase):

    def setUp(self):
        self.ext = _make_extractor()

    def test_groups_files_and_skips_meta(self):
        self.ext.dbutils.fs.ls.return_value = [
            SimpleNamespace(name="customers_202401_202403", path="p1"),
            SimpleNamespace(name="customers_202404_202406", path="p2"),
            SimpleNamespace(name="orders_202401_202403", path="p3"),
            SimpleNamespace(name="customers.meta", path="ignored"),
        ]
        tables = self.ext.discover_tables()
        self.assertEqual(len(tables["customers"]), 2)
        self.assertEqual(len(tables["orders"]), 1)

    def test_empty_or_meta_only_raises(self):
        for files in [[], [SimpleNamespace(name="x.meta", path="p")]]:
            with self.subTest(files=files):
                self.ext.dbutils.fs.ls.return_value = files
                with self.assertRaises(FileNotFoundError):
                    self.ext.discover_tables()


class TestReadMeta(unittest.TestCase):

    def setUp(self):
        self.ext = _make_extractor()

    def test_successful_read(self):
        rows = [MagicMock(), MagicMock()]
        rows[0].__getitem__ = lambda s, k: {"test_name": "number_of_rows", "value": "1000"}[k]
        rows[1].__getitem__ = lambda s, k: {"test_name": "number_of_columns", "value": "15"}[k]
        self.ext.spark.read.parquet.return_value.collect.return_value = rows

        meta = self.ext.read_meta("customers")
        self.assertEqual(meta, {"number_of_rows": "1000", "number_of_columns": "15"})

    def test_missing_meta_raises(self):
        self.ext.spark.read.parquet.side_effect = Exception("not found")
        with self.assertRaises(FileNotFoundError):
            self.ext.read_meta("customers")


class TestValidation(unittest.TestCase):

    def setUp(self):
        self.ext = _make_extractor()

    def test_all_pass(self):
        results = self.ext.validate("t", _make_df(), META)
        self.assertTrue(all(r.passed for r in results))

    def test_failures_captured(self):
        cases = [
            ("single", _make_df(cols=12), 1),
            ("all", _make_df(rows=1, cols=1, unique=1), 3),
        ]
        for label, df, expected_fails in cases:
            with self.subTest(label=label):
                with self.assertRaises(DataValidationError) as ctx:
                    self.ext.validate("t", df, META)
                self.assertEqual(len(ctx.exception.failed_results), expected_fails)
                self.assertEqual(len(ctx.exception.results), 3)

    def test_logs_each_check(self):
        self.ext.validate("t", _make_df(), META)
        steps = [c.kwargs["step"] for c in self.ext.logger.log.call_args_list]
        for s in ["validate_row_count", "validate_column_count", "validate_unique_id_count"]:
            with self.subTest(step=s):
                self.assertIn(s, steps)


class TestWriteToBronze(unittest.TestCase):

    def test_writes_delta_overwrite(self):
        ext = _make_extractor()
        df = MagicMock()
        ext.write_to_bronze("customers", df)
        df.write.format.assert_called_with("delta")
        df.write.format().mode.assert_called_with("overwrite")
        df.write.format().mode().saveAsTable.assert_called_with("dev.bronze.customers")

    def test_failure_raises(self):
        ext = _make_extractor()
        df = MagicMock()
        df.write.format().mode().saveAsTable.side_effect = Exception("boom")
        with self.assertRaises(RuntimeError):
            ext.write_to_bronze("customers", df)


class TestArchiveFiles(unittest.TestCase):

    @patch("data_ops.operations.volume_extractor.date")
    def test_renames_data_and_meta(self, mock_date):
        mock_date.today.return_value.strftime.return_value = "20260210"
        ext = _make_extractor()
        paths = ["/Volumes/dev/bronze/external/mft/customers_202401_202403"]

        ext.archive_files("customers", paths)

        calls = [(c[0][0], c[0][1]) for c in ext.dbutils.fs.mv.call_args_list]
        expected = [
            (paths[0], "/Volumes/dev/bronze/external/archive/customers_202401_202403_processed20260210"),
            ("/Volumes/dev/bronze/external/mft/customers.meta", "/Volumes/dev/bronze/external/archive/customers_processed20260210.meta"),
        ]
        for (src, dst) in expected:
            with self.subTest(src=src.rsplit("/", 1)[-1]):
                self.assertIn((src, dst), calls)

    @patch("data_ops.operations.volume_extractor.date")
    def test_failure_raises(self, mock_date):
        mock_date.today.return_value.strftime.return_value = "20260210"
        ext = _make_extractor()
        ext.dbutils.fs.mv.side_effect = Exception("denied")
        with self.assertRaises(RuntimeError):
            ext.archive_files("t", ["/Volumes/dev/bronze/external/mft/t_202401_202403"])


class TestExtractAll(unittest.TestCase):

    def test_continues_on_failure(self):
        ext = _make_extractor()
        ext.dbutils.fs.ls.return_value = [
            SimpleNamespace(name="a_202401_202403", path="p1"),
            SimpleNamespace(name="b_202401_202403", path="p2"),
        ]
        ext.extract_table = MagicMock(side_effect=[RuntimeError("fail"), None])

        results = ext.extract_all()
        self.assertIn("fail", results["a"])
        self.assertEqual(results["b"], "success")


if __name__ == "__main__":
    unittest.main()
