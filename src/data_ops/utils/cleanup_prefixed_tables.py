"""Cleanup utility for dropping developer-prefixed tables from dev schemas.

Usage as a script::

    python -m data_ops.utils.cleanup_prefixed_tables --catalog dev --prefix glover_
    python -m data_ops.utils.cleanup_prefixed_tables --catalog dev --dry-run

Usage from a notebook::

    from data_ops.utils.cleanup_prefixed_tables import cleanup_prefixed_tables
    cleanup_prefixed_tables(spark, catalog="dev", prefix="glover_")
"""

import argparse
import sys
from typing import Optional

from pyspark.sql import SparkSession

from data_ops.utils.table_prefix import resolve_table_prefix

_TARGET_SCHEMAS = ("silver", "gold")


def cleanup_prefixed_tables(
    spark: SparkSession,
    catalog: str,
    prefix: Optional[str] = None,
    schemas: tuple[str, ...] = _TARGET_SCHEMAS,
    dry_run: bool = False,
) -> list[str]:
    """Drop all tables matching a developer prefix from the given schemas.

    Args:
        spark: Active SparkSession.
        catalog: Unity Catalog name (e.g. ``"dev"``).
        prefix: Developer prefix (e.g. ``"glover_"``).  If ``None``,
                auto-detects via :func:`resolve_table_prefix`.
        schemas: Schemas to clean (default: silver, gold).
        dry_run: If ``True``, only lists tables without dropping them.

    Returns:
        List of fully-qualified table names that were (or would be) dropped.

    Raises:
        ValueError: If resolved prefix is empty (safety guard against
            dropping all tables).
    """
    resolved = prefix if prefix is not None else resolve_table_prefix(spark=spark)
    if not resolved:
        raise ValueError(
            "Refusing to clean up with an empty prefix -- "
            "this would drop ALL tables in the target schemas. "
            "Provide an explicit non-empty prefix."
        )

    dropped: list[str] = []

    for schema in schemas:
        full_schema = f"{catalog}.{schema}"
        try:
            tables_df = spark.sql(f"SHOW TABLES IN {full_schema}")
        except Exception as e:
            print(f"WARNING: Could not list tables in {full_schema}: {e}")
            continue

        for row in tables_df.collect():
            table_name = row.tableName
            if not table_name.startswith(resolved):
                continue

            fq_name = f"{full_schema}.{table_name}"
            if dry_run:
                print(f"[DRY RUN] Would drop: {fq_name}")
            else:
                try:
                    spark.sql(f"DROP TABLE IF EXISTS {fq_name}")
                    print(f"Dropped: {fq_name}")
                except Exception as e:
                    print(f"ERROR dropping {fq_name}: {e}")
                    continue
            dropped.append(fq_name)

    summary = "Would drop" if dry_run else "Dropped"
    print(f"\n{summary} {len(dropped)} table(s) with prefix '{resolved}'")
    return dropped


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Drop developer-prefixed tables from dev silver/gold schemas",
    )
    parser.add_argument("--catalog", required=True, help="Unity Catalog name (e.g. dev)")
    parser.add_argument(
        "--prefix",
        default=None,
        help="Developer prefix (e.g. glover_). Auto-detects if omitted.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List tables without dropping them",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    spark = SparkSession.builder.getOrCreate()
    cleanup_prefixed_tables(
        spark=spark,
        catalog=args.catalog,
        prefix=args.prefix,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
