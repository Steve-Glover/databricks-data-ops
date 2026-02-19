"""Databricks environment utilities for interacting with the workspace.

Provides helpers for Spark session access, catalog discovery, DBUtils,
and notebook execution with retry logic.
"""

from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession


def get_spark() -> SparkSession:
    """Get or create the active Spark session.

    On Databricks, this returns the existing cluster session.
    For local development, use DatabricksSession.builder.getOrCreate() directly.

    Returns:
        Active SparkSession instance.
    """
    return SparkSession.builder.getOrCreate()


def get_catalog(spark: SparkSession, match_str: str) -> str:
    """Return the first catalog name containing match_str.

    Args:
        spark: Active Spark session.
        match_str: Substring to match catalog names against.

    Returns:
        First catalog name containing match_str.

    Raises:
        IndexError: If no catalog matches match_str.
    """
    catalogs_df = spark.sql("SHOW CATALOGS")
    matches = [row.catalog for row in catalogs_df.collect() if match_str in row.catalog]
    return matches[0]


def get_dbutils(spark: SparkSession) -> DBUtils:
    """Get DBUtils for interacting with the Databricks file system and secrets.

    Args:
        spark: Active Spark session.

    Returns:
        DBUtils instance.
    """
    return DBUtils(spark)


def run_with_retry(
    spark: SparkSession,
    notebook: str,
    timeout: int,
    args: dict | None = None,
    max_retries: int = 3,
) -> str:
    """Run a Databricks notebook with automatic retry on failure.

    Args:
        spark: Active Spark session.
        notebook: Path to the notebook to run.
        timeout: Timeout in seconds.
        args: Arguments to pass to the notebook. Defaults to empty dict.
        max_retries: Maximum number of retry attempts before re-raising.

    Returns:
        Notebook exit value.

    Raises:
        Exception: Re-raises the last exception after max_retries exhausted.
    """
    dbutils = get_dbutils(spark)
    args = args or {}
    num_retries = 0
    while True:
        try:
            return dbutils.notebook.run(notebook, timeout, args)
        except Exception as e:
            if num_retries >= max_retries:
                raise
            print(f"Retrying error: {e}")
            num_retries += 1
