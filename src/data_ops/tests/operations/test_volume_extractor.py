"""Unit tests for VolumeExtractionConfig and VolumeExtractor."""

import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from data_ops.operations.volume_extractor import (
    VolumeExtractionConfig,
    VolumeExtractor,
    _CHUNKED_PATTERN,
    _NON_CHUNKED_PATTERN,
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
        self.assertEqual(cfg.get_table_path("test__ess"), "dev.bronze.test__ess")

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


class TestFilePatterns(unittest.TestCase):

    def test_chunked_pattern(self):
        cases = [
            ("test__ess_20240101_20240331", "test__ess", "20240101", "20240331"),
            ("test__dxcg_20240101_20240331", "test__dxcg", "20240101", "20240331"),
        ]
        for filename, expected_table, start, end in cases:
            with self.subTest(filename=filename):
                m = _CHUNKED_PATTERN.match(filename)
                self.assertIsNotNone(m)
                self.assertEqual(m.group(1), expected_table)
                self.assertEqual(m.group(2), start)
                self.assertEqual(m.group(3), end)

    def test_non_chunked_pattern(self):
        for name, expected in [("test__ess", "test__ess"), ("test__dxcg", "test__dxcg")]:
            with self.subTest(name=name):
                m = _NON_CHUNKED_PATTERN.match(name)
                self.assertIsNotNone(m)
                self.assertEqual(m.group(1), expected)

    def test_chunked_rejects_invalid_dates(self):
        # 6-digit dates (old YYYYMM format) should not match
        self.assertIsNone(_CHUNKED_PATTERN.match("test__ess_202401_202403"))

    def test_non_chunked_rejects_extensions(self):
        for name in ["test__ess.meta", "README.md"]:
            with self.subTest(name=name):
                self.assertIsNone(_NON_CHUNKED_PATTERN.match(name))


class TestDiscoverTables(unittest.TestCase):

    def setUp(self):
        self.ext = _make_extractor()

    def test_groups_chunked_files_and_skips_meta(self):
        self.ext.dbutils.fs.ls.return_value = [
            SimpleNamespace(name="test__ess_20240101_20240331", path="p1"),
            SimpleNamespace(name="test__ess_20240401_20240630", path="p2"),
            SimpleNamespace(name="test__dxcg_20240101_20240331", path="p3"),
            SimpleNamespace(name="test__ess.meta", path="ignored"),
        ]
        tables = self.ext.discover_tables()
        self.assertEqual(len(tables["test__ess"]), 2)
        self.assertEqual(len(tables["test__dxcg"]), 1)

    def test_groups_non_chunked_files(self):
        self.ext.dbutils.fs.ls.return_value = [
            SimpleNamespace(name="test__eligibility", path="p1"),
            SimpleNamespace(name="test__eligibility.meta", path="ignored"),
        ]
        tables = self.ext.discover_tables()
        self.assertEqual(tables, {"test__eligibility": ["p1"]})

    def test_mixed_chunked_and_non_chunked(self):
        self.ext.dbutils.fs.ls.return_value = [
            SimpleNamespace(name="test__member_20240101_20240331", path="p1"),
            SimpleNamespace(name="test__member_20240401_20240630", path="p2"),
            SimpleNamespace(name="test__claims_20240101_20240331", path="p3"),
            SimpleNamespace(name="test__eligibility", path="p4"),
            SimpleNamespace(name="test__member.meta", path="ignored"),
            SimpleNamespace(name="test__claims.meta", path="ignored"),
            SimpleNamespace(name="test__eligibility.meta", path="ignored"),
        ]
        tables = self.ext.discover_tables()
        self.assertEqual(len(tables["test__member"]), 2)
        self.assertEqual(len(tables["test__claims"]), 1)
        self.assertEqual(len(tables["test__eligibility"]), 1)

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

        meta = self.ext.read_meta("test__ess")
        self.assertEqual(meta, {"number_of_rows": "1000", "number_of_columns": "15"})

    def test_missing_meta_raises(self):
        self.ext.spark.read.parquet.side_effect = Exception("not found")
        with self.assertRaises(FileNotFoundError):
            self.ext.read_meta("test__ess")


class TestValidation(unittest.TestCase):

    def setUp(self):
        self.ext = _make_extractor()

    def test_all_pass(self):
        results = self.ext.validate("test__t", _make_df(), META)
        self.assertTrue(all(r.passed for r in results))

    def test_failures_captured(self):
        cases = [
            ("single", _make_df(cols=12), 1),
            ("all", _make_df(rows=1, cols=1, unique=1), 3),
        ]
        for label, df, expected_fails in cases:
            with self.subTest(label=label):
                with self.assertRaises(DataValidationError) as ctx:
                    self.ext.validate("test__t", df, META)
                self.assertEqual(len(ctx.exception.failed_results), expected_fails)
                self.assertEqual(len(ctx.exception.results), 3)

    def test_logs_each_check(self):
        self.ext.validate("test__t", _make_df(), META)
        steps = [c.kwargs["step"] for c in self.ext.logger.log.call_args_list]
        for s in ["validate_row_count", "validate_column_count", "validate_unique_id_count"]:
            with self.subTest(step=s):
                self.assertIn(s, steps)


class TestWriteToBronze(unittest.TestCase):

    def test_writes_delta_overwrite(self):
        ext = _make_extractor()
        df = MagicMock()
        ext.write_to_bronze("test__ess", df)
        df.write.format.assert_called_with("delta")
        df.write.format().mode.assert_called_with("overwrite")
        df.write.format().mode().saveAsTable.assert_called_with("dev.bronze.test__ess")

    def test_failure_raises(self):
        ext = _make_extractor()
        df = MagicMock()
        df.write.format().mode().saveAsTable.side_effect = Exception("boom")
        with self.assertRaises(RuntimeError):
            ext.write_to_bronze("test__ess", df)


class TestArchiveFiles(unittest.TestCase):

    @patch("data_ops.operations.volume_extractor.date")
    def test_renames_data_and_meta(self, mock_date):
        mock_date.today.return_value.strftime.return_value = "20260210"
        ext = _make_extractor()
        paths = ["/Volumes/dev/bronze/external/mft/test__ess_20240101_20240331"]

        ext.archive_files("test__ess", paths)

        calls = [(c[0][0], c[0][1]) for c in ext.dbutils.fs.mv.call_args_list]
        expected = [
            (paths[0], "/Volumes/dev/bronze/external/archive/test__ess_20240101_20240331_processed20260210"),
            ("/Volumes/dev/bronze/external/mft/test__ess.meta", "/Volumes/dev/bronze/external/archive/test__ess_processed20260210.meta"),
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
            ext.archive_files("test__t", ["/Volumes/dev/bronze/external/mft/test__t_20240101_20240331"])


class TestExtract(unittest.TestCase):

    def setUp(self):
        self.ext = _make_extractor()
        self.ext.dbutils.fs.ls.return_value = [
            SimpleNamespace(name="test__a_20240101_20240331", path="p1"),
            SimpleNamespace(name="test__b_20240101_20240331", path="p2"),
        ]

    def test_continues_on_failure(self):
        self.ext.extract_table = MagicMock(side_effect=[RuntimeError("fail"), None])
        results = self.ext.extract(["test__a", "test__b"])
        self.assertIn("fail", results["test__a"])
        self.assertEqual(results["test__b"], "success")

    def test_table_not_found_recorded_as_failure(self):
        self.ext.extract_table = MagicMock()
        results = self.ext.extract(["test__a", "test__missing"])
        self.assertEqual(results["test__a"], "success")
        self.assertIn("test__missing", results["test__missing"])

    def test_only_requested_tables_processed(self):
        self.ext.extract_table = MagicMock()
        self.ext.extract(["test__a"])
        self.ext.extract_table.assert_called_once()
        call_args = self.ext.extract_table.call_args
        self.assertEqual(call_args[0][0], "test__a")


if __name__ == "__main__":
    unittest.main()
