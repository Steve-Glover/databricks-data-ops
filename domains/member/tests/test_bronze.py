"""Unit tests for the member bronze pipeline entrypoint."""

import sys
import unittest
from unittest.mock import MagicMock, patch

import domains.member.pipelines.bronze as bronze_module
from domains.member.pipelines.bronze import TABLES, main, parse_args


class TestParseArgs(unittest.TestCase):
    def test_catalog_required(self):
        with self.assertRaises(SystemExit):
            parse_args([])

    def test_catalog_parsed(self):
        args = parse_args(["--catalog", "dev"])
        self.assertEqual(args.catalog, "dev")


class TestMain(unittest.TestCase):
    def _run_main(self, catalog="dev", run_side_effect=None):
        spark = MagicMock()
        with (
            patch("domains.member.pipelines.bronze.SparkSession") as mock_spark_cls,
            patch("domains.member.pipelines.bronze.run") as mock_run,
        ):
            mock_spark_cls.builder.getOrCreate.return_value = spark
            if run_side_effect is not None:
                mock_run.side_effect = run_side_effect
            main(["--catalog", catalog])
            return mock_run, spark

    def test_run_called_with_correct_catalog(self):
        mock_run, spark = self._run_main(catalog="sit")
        config_arg = mock_run.call_args[0][0]
        self.assertEqual(config_arg.catalog, "sit")

    def test_run_called_with_member_domain(self):
        mock_run, _ = self._run_main()
        config_arg = mock_run.call_args[0][0]
        self.assertEqual(config_arg.domain, "member")

    def test_run_called_with_module_tables(self):
        mock_run, _ = self._run_main()
        config_arg = mock_run.call_args[0][0]
        self.assertEqual(config_arg.tables, TABLES)

    def test_empty_tables_exits_with_1(self):
        # Regression guard: TABLES placeholder is currently empty; main() must fail loudly.
        self.assertEqual(TABLES, [], "Update this test once real table names are added")
        with self.assertRaises(SystemExit) as ctx:
            self._run_main(run_side_effect=RuntimeError("No tables configured for domain 'member'."))
        self.assertEqual(ctx.exception.code, 1)

    def test_volume_paths_use_catalog(self):
        mock_run, _ = self._run_main(catalog="prod")
        config_arg = mock_run.call_args[0][0]
        self.assertIn("prod", config_arg.source_volume_path)
        self.assertIn("prod", config_arg.archive_volume_path)
        self.assertIn("prod", config_arg.log_table_path)

    def test_run_failure_exits_with_1(self):
        with self.assertRaises(SystemExit) as ctx:
            self._run_main(run_side_effect=RuntimeError("2 of 2 tables failed"))
        self.assertEqual(ctx.exception.code, 1)

    def test_run_success_does_not_exit(self):
        self._run_main()  # must not raise SystemExit


if __name__ == "__main__":
    unittest.main()
