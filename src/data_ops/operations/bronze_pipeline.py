"""Generic bronze pipeline runner for all domains.

Provides BronzePipelineConfig and run() so each domain's bronze.py
is a thin wrapper — define config, call run().
"""

import sys

from pydantic import field_validator
from pyspark.sql import SparkSession

from data_ops.operations.volume_extractor import VolumeExtractionConfig, VolumeExtractor
from data_ops.utils.logging import DatabricksLogger


class BronzePipelineConfig(VolumeExtractionConfig):
    """Configuration for the generic bronze pipeline runner.

    Extends VolumeExtractionConfig with domain identity and the table list,
    which are the only values that differ between domain bronze pipelines.

    Attributes:
        domain: Domain name used for logging, e.g. 'member', 'claims'.
        tables: Table names to extract from the source volume. An empty list
            is valid during initial setup; run() will log a warning and return.
    """

    domain: str
    tables: list[str]

    @field_validator("domain")
    @classmethod
    def domain_must_not_be_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("domain must be a non-empty string")
        return v


def run(config: BronzePipelineConfig, spark: SparkSession) -> None:
    """Run the bronze ingestion pipeline for a domain.

    Orchestrates: discover → extract → validate → write → archive for each table.
    Logs per-table failures and a pipeline summary.

    Args:
        config: Pipeline configuration including domain, tables, and volume paths.
        spark: Active SparkSession (from SparkSession.builder.getOrCreate() on cluster).

    Raises:
        RuntimeError: If any table extraction fails. The caller is responsible for
            handling this (e.g. sys.exit(1) in a CLI entrypoint).
    """
    logger = DatabricksLogger(
        domain=config.domain,
        process="bronze_ingestion",
        log_table_path=config.log_table_path,
        spark=spark,
    )

    if not config.tables:
        logger.failure(
            step="pipeline",
            message=f"No tables configured for domain '{config.domain}'.",
        )
        raise RuntimeError(f"No tables configured for domain '{config.domain}'.")

    extractor = VolumeExtractor(config, spark=spark)
    results = extractor.extract(config.tables)

    succeeded = [k for k, v in results.items() if v == "success"]
    failures = {k: v for k, v in results.items() if v != "success"}

    if failures:
        for table_name, error in failures.items():
            logger.failure(
                step=f"bronze_ingestion - {config.domain} - {table_name}",
                message=error,
            )
            print(f"FAILED: {table_name} -- {error}", file=sys.stderr)
        failure_summary = (
            f"{len(failures)} of {len(results)} tables failed: "
            + ", ".join(failures.keys())
        )
        logger.failure(step="pipeline", message=failure_summary)
        raise RuntimeError(failure_summary)

    logger.success(
        step="pipeline",
        message=f"All {len(succeeded)} tables extracted: {', '.join(succeeded)}",
    )
    for table_name in succeeded:
        print(f"OK: {table_name}")
