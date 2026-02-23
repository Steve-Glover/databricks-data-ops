"""Unit tests for cleanup_prefixed_tables utility."""

import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock

from data_ops.utils.cleanup_prefixed_tables import cleanup_prefixed_tables


def _mock_spark(tables_by_schema: dict[str, list[str]]) -> MagicMock:
    """Build a mock SparkSession that returns given tables for SHOW TABLES."""
    spark = MagicMock()

    def sql_side_effect(query):
        if query.startswith("SHOW TABLES IN"):
            schema = query.split("SHOW TABLES IN ")[-1]
            rows = [SimpleNamespace(tableName=t) for t in tables_by_schema.get(schema, [])]
            mock_df = MagicMock()
            mock_df.collect.return_value = rows
            return mock_df
        return MagicMock()

    spark.sql.side_effect = sql_side_effect
    return spark


class TestCleanupPrefixedTables(unittest.TestCase):

    def test_drops_matching_tables(self):
        spark = _mock_spark({
            "dev.silver": ["glover_member_clean", "glover_enrollment", "shared_lookup"],
            "dev.gold": ["glover_summary", "other_report"],
        })
        dropped = cleanup_prefixed_tables(spark, catalog="dev", prefix="glover_")
        self.assertEqual(sorted(dropped), [
            "dev.gold.glover_summary",
            "dev.silver.glover_enrollment",
            "dev.silver.glover_member_clean",
        ])

    def test_dry_run_does_not_drop(self):
        spark = _mock_spark({
            "dev.silver": ["glover_member_clean"],
            "dev.gold": [],
        })
        dropped = cleanup_prefixed_tables(spark, catalog="dev", prefix="glover_", dry_run=True)
        self.assertEqual(len(dropped), 1)
        drop_calls = [c for c in spark.sql.call_args_list if "DROP" in str(c)]
        self.assertEqual(len(drop_calls), 0)

    def test_empty_prefix_raises(self):
        with self.assertRaises(ValueError):
            cleanup_prefixed_tables(MagicMock(), catalog="dev", prefix="")

    def test_no_matching_tables(self):
        spark = _mock_spark({
            "dev.silver": ["jones_member_clean"],
            "dev.gold": ["jones_summary"],
        })
        dropped = cleanup_prefixed_tables(spark, catalog="dev", prefix="glover_")
        self.assertEqual(dropped, [])

    def test_custom_schemas(self):
        spark = _mock_spark({
            "dev.silver": ["glover_member_clean"],
            "dev.gold": ["glover_summary"],
        })
        dropped = cleanup_prefixed_tables(
            spark, catalog="dev", prefix="glover_", schemas=("silver",),
        )
        self.assertEqual(dropped, ["dev.silver.glover_member_clean"])


if __name__ == "__main__":
    unittest.main()
