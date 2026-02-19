"""Volume extraction utility for ingesting parquet data from Databricks Volumes.

Discovers parquet file chunks in an external volume, validates against companion
.meta files, writes to bronze Delta tables, and archives processed files.
"""

import re
from datetime import date

from pydantic import BaseModel, field_validator
from pyspark.sql import DataFrame, SparkSession

from data_ops.utils.errors import DataValidationError, ValidationResult
from data_ops.utils.logging import DatabricksLogger


class VolumeExtractionConfig(BaseModel, frozen=True):
    """Configuration for volume extraction.

    Attributes:
        catalog: Unity Catalog name (e.g., 'dev', 'sit', 'prod')
        source_volume_path: Path to the source volume with parquet chunks
        archive_volume_path: Path to the archive volume for processed files
        bronze_schema: Target schema name (default: 'bronze')
        log_table_path: Fully qualified path for the extraction log table
    """

    catalog: str
    source_volume_path: str
    archive_volume_path: str
    bronze_schema: str = "bronze"
    log_table_path: str

    @field_validator("catalog")
    @classmethod
    def catalog_must_not_be_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("catalog must be a non-empty string")
        return v

    @field_validator("source_volume_path", "archive_volume_path")
    @classmethod
    def volume_path_must_be_valid(cls, v: str) -> str:
        if not v.startswith("/Volumes/"):
            raise ValueError(f"Volume path must start with '/Volumes/', got '{v}'")
        # Normalize trailing slash
        return v if v.endswith("/") else v + "/"

    def get_table_path(self, table_name: str) -> str:
        """Return the fully qualified table path for a given table name.

        Args:
            table_name: Raw table name

        Returns:
            Fully qualified path: '{catalog}.{bronze_schema}.{table_name}'
        """
        return f"{self.catalog}.{self.bronze_schema}.{table_name}"


# Chunked files: "customers_202401_202403" -> table="customers"
_CHUNKED_PATTERN = re.compile(r"^(.+?)_(\d{6})_(\d{6})$")
# Non-chunked files: filename IS the table name (no extension, no date suffix)
_NON_CHUNKED_PATTERN = re.compile(r"^([A-Za-z_][A-Za-z0-9_]*)$")


class VolumeExtractor:
    """Extracts parquet data from Databricks Volumes into bronze Delta tables.

    Workflow:
        1. Discover data files and group by table name
        2. For each table: read_meta -> read_and_union_chunks -> validate -> write -> archive

    Args:
        config: Extraction configuration
        spark: SparkSession instance to use for all Spark operations.
    """

    def __init__(
        self,
        config: VolumeExtractionConfig,
        spark: SparkSession,
    ):
        self.config = config
        self.spark = spark

        self.logger = DatabricksLogger(
            domain="volume_extraction",
            process="extract",
            log_table_path=config.log_table_path,
            spark=self.spark,
        )

        from pyspark.dbutils import DBUtils

        self.dbutils = DBUtils(self.spark)

    def discover_tables(self) -> dict[str, list[str]]:
        """Discover data files in the source volume and group by table name.

        Files follow one of two naming conventions (no extension):
        - Chunked: tablename_YYYYMM_YYYYMM (e.g., customers_202401_202403)
        - Non-chunked: tablename (e.g., customers)
        Files with extensions (.meta, etc.) are excluded.

        Returns:
            Dict mapping table name to list of file paths.

        Raises:
            FileNotFoundError: If no data files are found in the source volume.
        """
        files = self.dbutils.fs.ls(self.config.source_volume_path)
        tables: dict[str, list[str]] = {}

        for file_info in files:
            name = file_info.name
            # Skip files with extensions (.meta, etc.)
            if "." in name:
                continue
            # Try chunked pattern first (has date range suffix)
            match = _CHUNKED_PATTERN.match(name)
            if match:
                table_name = match.group(1)
            else:
                # Non-chunked: entire filename is the table name
                match = _NON_CHUNKED_PATTERN.match(name)
                if match:
                    table_name = match.group(1)
                else:
                    continue
            tables.setdefault(table_name, []).append(file_info.path)

        if not tables:
            self.logger.failure(
                step="discover_tables",
                message=f"No data files found in {self.config.source_volume_path}",
            )
            raise FileNotFoundError(f"No data files found in {self.config.source_volume_path}")

        table_summary = ", ".join(f"{name} ({len(paths)} files)" for name, paths in tables.items())
        self.logger.success(
            step="discover_tables",
            message=f"Discovered {len(tables)} tables: {table_summary}",
        )

        return tables

    def read_meta(self, table_name: str) -> dict[str, str]:
        """Read the companion .meta parquet file for a table.

        The meta file is long-form with columns 'test_name' and 'value'.

        Args:
            table_name: Name of the table to read meta for.

        Returns:
            Dict mapping test_name to value.

        Raises:
            FileNotFoundError: If the meta file does not exist.
        """
        meta_path = f"{self.config.source_volume_path}{table_name}.meta"

        try:
            meta_df = self.spark.read.parquet(meta_path)
            rows = meta_df.collect()
            meta = {row["test_name"]: row["value"] for row in rows}
        except Exception as e:
            self.logger.failure(
                step="read_meta",
                message=f"Failed to read meta file for '{table_name}': {e}",
            )
            raise FileNotFoundError(f"Meta file not found or unreadable: {meta_path}") from e

        self.logger.success(
            step="read_meta",
            message=f"Read meta for '{table_name}': {len(meta)} checks defined",
        )
        return meta

    def read_and_union_chunks(self, table_name: str, file_paths: list[str]) -> DataFrame:
        """Read and union all parquet chunks for a table.

        Args:
            table_name: Name of the table being read.
            file_paths: List of file paths to read.

        Returns:
            Unioned DataFrame of all chunks.

        Raises:
            RuntimeError: If reading fails.
        """
        try:
            df = self.spark.read.parquet(*file_paths)
        except Exception as e:
            self.logger.failure(
                step="read_chunks",
                message=f"Failed to read chunks for '{table_name}': {e}",
            )
            raise RuntimeError(f"Failed to read parquet chunks for '{table_name}': {e}") from e

        row_count = df.count()
        self.logger.success(
            step="read_chunks",
            message=(f"Read {len(file_paths)} chunks for '{table_name}': " f"{row_count} rows"),
        )
        return df

    def validate(
        self, table_name: str, df: DataFrame, meta: dict[str, str]
    ) -> list[ValidationResult]:
        """Validate a DataFrame against meta expectations.

        Runs ALL checks regardless of individual failures.

        Args:
            table_name: Name of the table being validated.
            df: DataFrame to validate.
            meta: Dict of test_name -> value from the meta file.

        Returns:
            List of ValidationResult objects (all passed).

        Raises:
            DataValidationError: If any validation check fails (contains ALL results).
        """
        results: list[ValidationResult] = []

        # 1. Row count check
        expected_rows = int(meta["number_of_rows"])
        actual_rows = df.count()
        row_passed = actual_rows == expected_rows
        results.append(
            ValidationResult(
                check_name="row_count",
                expected=expected_rows,
                actual=actual_rows,
                passed=row_passed,
                message=f"Expected {expected_rows} rows, got {actual_rows}",
            )
        )
        self.logger.log(
            step="validate_row_count",
            status="success" if row_passed else "failure",
            message=f"Expected {expected_rows} rows, got {actual_rows}",
        )

        # 2. Column count check
        expected_cols = int(meta["number_of_columns"])
        actual_cols = len(df.columns)
        col_passed = actual_cols == expected_cols
        results.append(
            ValidationResult(
                check_name="column_count",
                expected=expected_cols,
                actual=actual_cols,
                passed=col_passed,
                message=f"Expected {expected_cols} columns, got {actual_cols}",
            )
        )
        self.logger.log(
            step="validate_column_count",
            status="success" if col_passed else "failure",
            message=f"Expected {expected_cols} columns, got {actual_cols}",
        )

        # 3. Unique ID count check
        id_column = meta["id_column"]
        expected_unique = int(meta["number_of_unique_ids"])
        actual_unique = df.select(id_column).distinct().count()
        unique_passed = actual_unique == expected_unique
        results.append(
            ValidationResult(
                check_name="unique_id_count",
                expected=expected_unique,
                actual=actual_unique,
                passed=unique_passed,
                message=(
                    f"Expected {expected_unique} unique values in '{id_column}', "
                    f"got {actual_unique}"
                ),
            )
        )
        self.logger.log(
            step="validate_unique_id_count",
            status="success" if unique_passed else "failure",
            message=(
                f"Expected {expected_unique} unique values in '{id_column}', "
                f"got {actual_unique}"
            ),
        )

        if all(r.passed for r in results):
            self.logger.success(
                step="validate",
                message=f"All {len(results)} checks passed for '{table_name}'",
            )
            return results

        raise DataValidationError(table_name, results)

    def write_to_bronze(self, table_name: str, df: DataFrame) -> None:
        """Write a validated DataFrame to a bronze Delta table.

        Args:
            table_name: Name of the table to write.
            df: DataFrame to write.

        Raises:
            RuntimeError: If writing fails.
        """
        table_path = self.config.get_table_path(table_name)

        try:
            df.write.format("delta").mode("overwrite").saveAsTable(table_path)
        except Exception as e:
            self.logger.failure(
                step="write_to_bronze",
                message=f"Failed to write '{table_name}' to {table_path}: {e}",
            )
            raise RuntimeError(f"Failed to write '{table_name}' to {table_path}: {e}") from e

        self.logger.success(
            step="write_to_bronze",
            message=f"Wrote '{table_name}' to {table_path}",
        )

    def archive_files(self, table_name: str, file_paths: list[str]) -> None:
        """Archive processed data files and companion meta file.

        Data files (no extension) get '_processedYYYYMMDD' appended:
            customers_202401_202403 -> customers_202401_202403_processed20260210

        Meta files get '_processedYYYYMMDD' inserted before .meta:
            customers.meta -> customers_processed20260210.meta

        Args:
            table_name: Name of the table whose files are being archived.
            file_paths: List of data file paths to archive.

        Raises:
            RuntimeError: If archiving fails.
        """
        today = date.today().strftime("%Y%m%d")

        # Include the companion .meta file
        meta_source = f"{self.config.source_volume_path}{table_name}.meta"
        meta_dest = f"{self.config.archive_volume_path}{table_name}_processed{today}.meta"

        all_moves = []

        for path in file_paths:
            # Extract filename from path (no extension)
            filename = path.rsplit("/", 1)[-1]
            archived_name = f"{filename}_processed{today}"
            dest = f"{self.config.archive_volume_path}{archived_name}"
            all_moves.append((path, dest))

        all_moves.append((meta_source, meta_dest))

        try:
            for source, dest in all_moves:
                self.dbutils.fs.mv(source, dest)
        except Exception as e:
            self.logger.failure(
                step="archive_files",
                message=f"Failed to archive files for '{table_name}': {e}",
            )
            raise RuntimeError(f"Failed to archive files for '{table_name}': {e}") from e

        self.logger.success(
            step="archive_files",
            message=(
                f"Archived {len(all_moves)} files for '{table_name}' "
                f"to {self.config.archive_volume_path}"
            ),
        )

    def extract_table(self, table_name: str, file_paths: list[str]) -> None:
        """Orchestrate extraction for a single table.

        Steps: read_meta -> read_and_union_chunks -> validate -> write_to_bronze -> archive_files

        Args:
            table_name: Name of the table to extract.
            file_paths: List of file paths for this table.
        """
        self.logger.log(
            step="extract_table",
            status="success",
            message=f"Starting extraction for '{table_name}' ({len(file_paths)} files)",
        )

        meta = self.read_meta(table_name)
        df = self.read_and_union_chunks(table_name, file_paths)
        self.validate(table_name, df, meta)
        self.write_to_bronze(table_name, df)
        self.archive_files(table_name, file_paths)

        self.logger.success(
            step="extract_table",
            message=f"Completed extraction for '{table_name}'",
        )

    def extract_all(self) -> dict[str, str]:
        """Top-level entry point: discover and extract all tables.

        Continues processing remaining tables if one fails.

        Returns:
            Dict mapping table_name to 'success' or an error message.
        """
        tables = self.discover_tables()
        results: dict[str, str] = {}

        for table_name, file_paths in tables.items():
            try:
                self.extract_table(table_name, file_paths)
                results[table_name] = "success"
            except Exception as e:
                results[table_name] = str(e)
                self.logger.failure(
                    step="extract_all",
                    message=f"Failed to extract '{table_name}': {e}",
                )

        succeeded = sum(1 for v in results.values() if v == "success")
        failed = len(results) - succeeded
        self.logger.log(
            step="extract_all",
            status="success" if failed == 0 else "warning",
            message=(
                f"Extraction complete: {succeeded} succeeded, {failed} failed "
                f"out of {len(results)} tables"
            ),
        )

        return results
