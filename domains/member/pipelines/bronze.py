"""Member domain bronze ingestion pipeline.

Extracts parquet data from the member landing volume into bronze Delta tables.
Uses VolumeExtractor from the data_ops shared library.
"""

import argparse
import sys

from pyspark.sql import SparkSession

from data_ops import BronzePipelineConfig, run

# TODO: Replace with the actual member table names to extract.
TABLES: list[str] = [
    # "table_name_1",
    # "table_name_2",
]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Member bronze ingestion")
    parser.add_argument("--catalog", required=True, help="Unity Catalog name (dev, sit, prod)")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    catalog = args.catalog

    spark = SparkSession.builder.getOrCreate()

    config = BronzePipelineConfig(
        catalog=catalog,
        domain="member",
        tables=TABLES,
        source_volume_path=f"/Volumes/{catalog}/bronze/external/mft/",
        archive_volume_path=f"/Volumes/{catalog}/bronze/external/archive/",
        log_table_path=f"{catalog}.ua.logs",
    )

    try:
        run(config, spark)
    except RuntimeError as e:
        print(str(e), file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
