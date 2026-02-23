"""Member domain silver transformation pipeline.

Reads from shared bronze tables, applies transformations, and writes to
silver Delta tables. Uses table prefix for developer isolation in dev.
"""

import argparse
import sys

from pyspark.sql import SparkSession

from data_ops import DatabricksLogger, prefixed_table_name, resolve_table_prefix


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Member silver transform")
    parser.add_argument("--catalog", required=True, help="Unity Catalog name (dev, sit, prod)")
    parser.add_argument(
        "--table-prefix",
        default=None,
        help="Developer table prefix (e.g. email). Empty string for no prefix.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    catalog = args.catalog

    spark = SparkSession.builder.getOrCreate()
    log_table = f"{catalog}.ua.logs"

    logger = DatabricksLogger(
        domain="member", process="silver_transform", log_table_path=log_table, spark=spark
    )

    prefix = resolve_table_prefix(prefix=args.table_prefix, spark=spark)

    # Read from shared bronze (no prefix)
    bronze_table = f"{catalog}.bronze.member"
    logger.success(step="read_bronze", message=f"Reading from {bronze_table}")
    bronze_df = spark.table(bronze_table)

    # TODO: Apply silver transformations here
    silver_df = bronze_df

    # Write to prefixed silver table
    target = prefixed_table_name(catalog, "silver", "member", prefix)
    silver_df.write.format("delta").mode("overwrite").saveAsTable(target)
    logger.success(step="write_silver", message=f"Wrote to {target}")


if __name__ == "__main__":
    main()
