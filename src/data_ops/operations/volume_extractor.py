"""Volume extraction utility for ingesting parquet data from Databricks Volumes.

Discovers parquet file chunks in an external volume, validates against companion
.meta files, writes to bronze Delta tables, and archives processed files.
"""

import re
from datetime import datetime, date
# from datetime import date
from pydantic import BaseModel, field_validator
from pyspark.dbutils import DBUtils
from pyspark.sql import DataFrame, SparkSession
from data_ops.utils.errors import DataValidationError, ValidationResult
from data_ops.utils.logging import DatabricksLogger
from pyspark.sql import functions as F


class VolumeExtractionConfig(BaseModel, frozen=True):
    """Configuration for volume extraction.

    Attributes:
        catalog: Unity Catalog name (e.g., 'dev_cda_ds', 'tst_cda_ds', 'cda_ds')
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


# Chunked files: "claims_20240101_20240331" -> table="claims"
_CHUNKED_PATTERN = re.compile(r"^(.+?)_(\d{8})_(\d{8})$")
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
        if "n_rows" in meta:
            expected_rows = int(meta["n_rows"])
            actual_rows = df.count()
            row_passed = actual_rows == expected_rows
            results.append(
                ValidationResult(
                    check_name="validate_row_count",
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

        # 2. n members check
        if "n_mbrs" in meta:
            expected_cols = int(meta["n_mbrs"])
            actual_cols = len(df.columns)
            col_passed = actual_cols == expected_cols
            results.append(
                ValidationResult(
                    check_name="validate_n_members",
                    expected=expected_cols,
                    actual=actual_cols,
                    passed=col_passed,
                    message=f"Expected {expected_cols} columns, got {actual_cols}",
                )
            )
            self.logger.log(
                step="validate_n_members",
                status="success" if col_passed else "failure",
                message=f"Expected {expected_cols} members, got {actual_cols}",
            )

        # 3. Unique ID count check
        if "n_unique_id" in meta:
            if "id_col" not in meta:
                results.append(
                    ValidationResult(
                        check_name="validate_unique_id_count",
                        expected="id_col in metadata",
                        actual="id_col missing",
                        passed=False,
                        message="Metadata contains 'n_unique_id' but missing required 'id_col'",
                    )
                )
                self.logger.log(
                    step="validate_unique_id_count",
                    status="failure",
                    message="Metadata contains 'n_unique_id' but missing required 'id_col'",
                )
            else:
                id_column = meta["id_col"]
                expected_unique = int(meta["n_unique_id"])
                actual_unique = df.select(id_column).distinct().count()
                unique_passed = actual_unique == expected_unique
                results.append(
                    ValidationResult(
                        check_name="validate_unique_id_count",
                        expected=expected_unique,
                        actual=actual_unique,
                        passed=unique_passed,
                        message=(
                            f"Expected {expected_unique} unique values in '{id_column}', "
                            f"got {actual_unique}"
                        )
                    )
                )
                self.logger.log(
                    step="validate_unique_id_count",
                    status="success" if unique_passed else "failure",
                    message=(
                        f"Expected {expected_unique} unique values in '{id_column}', "
                        f"got {actual_unique}"
                    )
                )
        # 4. Max Date
        if "max_date" in meta and "date_col" in meta:
            date_column = meta["date_col"]
            expected_max = datetime.strptime(meta["max_date"], "%Y-%m-%d")
            actual_max = df.select(date_column).agg(F.max(date_column)).collect()[0][0]
            
            # Convert actual_max to datetime for consistent comparison
            if isinstance(actual_max, str):
                actual_max = datetime.strptime(actual_max, "%Y-%m-%d")
            elif isinstance(actual_max, date) and not isinstance(actual_max, datetime):
                actual_max = datetime.combine(actual_max, datetime.min.time())
            
            max_passed = actual_max == expected_max
            results.append(
                ValidationResult(
                    check_name="validate_max_date",
                    expected=expected_max,
                    actual=actual_max,
                    passed=max_passed,
                    message=(
                        f"expected {expected_max} max date in '{date_column}', "
                        f"got {actual_max}"
                    )
                )
            )
            self.logger.log(
                step="validate_max_date",
                status="success" if max_passed else "failure",
                message=(
                    f"expected {expected_max} max date in '{date_column}', "
                    f"got {actual_max}"
                )
            )
        # 5. Min Date
        if "min_date" in meta and "date_col" in meta:
            date_column = meta["date_col"]
            expected_min = datetime.strptime(meta["min_date"], "%Y-%m-%d")
            actual_min = df.select(date_column).agg(F.min(date_column)).collect()[0][0]
            
            # Convert actual_min to datetime for consistent comparison
            if isinstance(actual_min, str):
                actual_min = datetime.strptime(actual_min, "%Y-%m-%d")
            elif isinstance(actual_min, date) and not isinstance(actual_min, datetime):
                actual_min = datetime.combine(actual_min, datetime.min.time())
            
            min_passed = actual_min == expected_min
            results.append(
                ValidationResult(
                    check_name="validate_min_date",
                    expected=expected_min,
                    actual=actual_min,
                    passed=min_passed,
                    message=(
                        f"expected {expected_min} min date in '{date_column}', "
                        f"got {actual_min}"
                    )
                )
            )
            self.logger.log(
                step="validate_min_date",
                status="success" if min_passed else "failure",
                message=(
                    f"expected {expected_min} min date in '{date_column}', "
                    f"got {actual_min}"
                )
            )

        if not results:
            self.logger.failure(
                step="validate",
                message=f"No validation checks matched metadata keys for '{table_name}'",
            )
            raise DataValidationError(table_name, results)

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
            df.write.mode("overwrite").saveAsTable(table_path)
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

    def extract(self, tables: list[str]) -> dict[str, str]:
        """Extract the specified tables from the source volume.

        Discovers available files then processes only the requested tables.
        Continues processing remaining tables if one fails. Tables not found
        in the source volume are recorded as failures.

        Args:
            tables: Table names to extract.

        Returns:
            Dict mapping table_name to 'success' or an error message.
        """
        discovered = self.discover_tables()
        results: dict[str, str] = {}

        for table_name in tables:
            if table_name not in discovered:
                msg = f"Table '{table_name}' not found in {self.config.source_volume_path}"
                results[table_name] = msg
                self.logger.failure(step="extract", message=msg)
                continue

            try:
                self.extract_table(table_name, discovered[table_name])
                results[table_name] = "success"
            except Exception as e:
                results[table_name] = str(e)
                self.logger.failure(
                    step="extract",
                    message=f"Failed to extract '{table_name}': {e}",
                )

        succeeded = sum(1 for v in results.values() if v == "success")
        failed = len(results) - succeeded
        self.logger.log(
            step="extract",
            status="success" if failed == 0 else "warning",
            message=(
                f"Extraction complete: {succeeded} succeeded, {failed} failed "
                f"out of {len(results)} tables"
            ),
        )

        return results
