"""Member domain bronze ingestion pipeline.

Extracts parquet data from the member landing volume into bronze Delta tables.
Uses VolumeExtractor from the data_ops shared library.
"""

import argparse
import sys

from pyspark.sql import SparkSession

from data_ops import DatabricksLogger, VolumeExtractionConfig, VolumeExtractor


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Member bronze ingestion")
    parser.add_argument("--catalog", required=True, help="Unity Catalog name (dev, sit, prod)")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    catalog = args.catalog

    spark = SparkSession.builder.getOrCreate()
    log_table = f"{catalog}.ua.logs"

    logger = DatabricksLogger(
        domain="member", process="bronze_ingestion", log_table_path=log_table, spark=spark
    )

    config = VolumeExtractionConfig(
        catalog=catalog,
        source_volume_path=f"/Volumes/{catalog}/bronze/external/mft/",
        archive_volume_path=f"/Volumes/{catalog}/bronze/external/archive/",
        log_table_path=log_table,
    )

    extractor = VolumeExtractor(config, spark=spark)
    results = extractor.extract_all()

    # Fail the job if any table extraction failed
    succeeded = [k for k, v in results.items() if v == "success"]
    failures = {k: v for k, v in results.items() if v != "success"}

    if failures:
        logger.failure(
            step="pipeline",
            message=(
                f"{len(failures)} of {len(results)} tables failed: "
                + ", ".join(failures.keys())
            ),
        )
        for table, error in failures.items():
            print(f"FAILED: {table} -- {error}", file=sys.stderr)
        sys.exit(1)

    logger.success(
        step="pipeline",
        message=f"All {len(succeeded)} tables extracted: {', '.join(succeeded)}",
    )
    for table in succeeded:
        print(f"OK: {table}")


if __name__ == "__main__":
    main()
