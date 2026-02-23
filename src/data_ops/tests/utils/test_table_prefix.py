"""Unit tests for table_prefix utilities."""

import os
import unittest
from unittest.mock import MagicMock, patch

from data_ops.utils.table_prefix import (
    _extract_last_name,
    _get_email_from_spark,
    _sanitize_prefix,
    prefixed_table_name,
    resolve_table_prefix,
)


class TestExtractLastName(unittest.TestCase):

    def test_extracts_last_name(self):
        cases = [
            ("steve.glover@company.org", "glover"),
            ("first.middle.last@company.org", "last"),
            ("steve.glover", "glover"),
            ("admin@databricks.com", "admin"),
            ("glover", "glover"),
            ("plain_user", "plain_user"),
        ]
        for raw, expected in cases:
            with self.subTest(raw=raw):
                self.assertEqual(_extract_last_name(raw), expected)


class TestSanitizePrefix(unittest.TestCase):

    def test_common_formats(self):
        cases = [
            ("glover", "glover_"),
            ("O'Brien", "O_Brien_"),
            ("smith-jones", "smith_jones_"),
            ("GLOVER", "GLOVER_"),
            ("steve__glover", "steve_glover_"),
        ]
        for raw, expected in cases:
            with self.subTest(raw=raw):
                self.assertEqual(_sanitize_prefix(raw), expected)

    def test_empty_and_whitespace(self):
        for raw in ["", "_", "__", "..."]:
            with self.subTest(raw=raw):
                self.assertEqual(_sanitize_prefix(raw), "")


class TestPrefixedTableName(unittest.TestCase):

    def test_with_and_without_prefix(self):
        cases = [
            (("dev", "silver", "member_clean", "glover_"), "dev.silver.glover_member_clean"),
            (("dev", "silver", "member_clean", ""), "dev.silver.member_clean"),
            (("prod", "gold", "summary", ""), "prod.gold.summary"),
            (("dev", "gold", "report", "jones_"), "dev.gold.jones_report"),
        ]
        for (cat, sch, tbl, pfx), expected in cases:
            with self.subTest(expected=expected):
                self.assertEqual(prefixed_table_name(cat, sch, tbl, pfx), expected)


class TestResolveTablePrefix(unittest.TestCase):

    def test_explicit_email_extracts_last_name(self):
        result = resolve_table_prefix(prefix="steve.glover@company.org")
        self.assertEqual(result, "glover_")

    def test_explicit_empty_string_means_no_prefix(self):
        with patch.dict(os.environ, {"DATA_OPS_TABLE_PREFIX": "ignored"}):
            self.assertEqual(resolve_table_prefix(prefix=""), "")

    def test_explicit_takes_priority_over_env(self):
        with patch.dict(os.environ, {"DATA_OPS_TABLE_PREFIX": "alice.smith@co.org"}):
            self.assertEqual(resolve_table_prefix(prefix="bob.jones@co.org"), "jones_")

    def test_env_var_fallback(self):
        cases = [
            ("steve.glover@company.org", "glover_"),
            ("glover", "glover_"),
            ("", ""),
        ]
        for env_val, expected in cases:
            with self.subTest(env_val=env_val):
                with patch.dict(os.environ, {"DATA_OPS_TABLE_PREFIX": env_val}, clear=False):
                    self.assertEqual(resolve_table_prefix(), expected)

    def test_spark_auto_detect_fallback(self):
        mock_spark = MagicMock()
        mock_row = MagicMock()
        mock_row.__getitem__ = lambda self, i: "alice.jones@example.com"
        mock_spark.sql.return_value.collect.return_value = [mock_row]

        env = {k: v for k, v in os.environ.items() if k != "DATA_OPS_TABLE_PREFIX"}
        with patch.dict(os.environ, env, clear=True):
            result = resolve_table_prefix(spark=mock_spark)
        self.assertEqual(result, "jones_")

    def test_no_source_raises(self):
        env = {k: v for k, v in os.environ.items() if k != "DATA_OPS_TABLE_PREFIX"}
        with patch.dict(os.environ, env, clear=True):
            with self.assertRaises(RuntimeError):
                resolve_table_prefix()


class TestGetEmailFromSpark(unittest.TestCase):

    def test_returns_email(self):
        mock_spark = MagicMock()
        mock_row = MagicMock()
        mock_row.__getitem__ = lambda self, i: "steve.glover@company.org"
        mock_spark.sql.return_value.collect.return_value = [mock_row]
        self.assertEqual(_get_email_from_spark(mock_spark), "steve.glover@company.org")

    def test_spark_failure_raises(self):
        mock_spark = MagicMock()
        mock_spark.sql.side_effect = Exception("no connection")
        with self.assertRaises(RuntimeError):
            _get_email_from_spark(mock_spark)


if __name__ == "__main__":
    unittest.main()
