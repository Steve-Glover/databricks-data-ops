"""Centralized logging utilities for Databricks data pipelines.

This module provides a custom logger that writes log entries to Delta tables
with automatic detection of runtime context and structured log information.
"""

import inspect
import os
from datetime import date, datetime
from pathlib import Path
from typing import Literal, Optional

from databricks.connect import DatabricksSession
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

LogStatus = Literal["success", "warning", "failure"]


class DatabricksLogger:
    """Custom logger that emits logs to Delta tables in Databricks.

    This logger automatically captures runtime context (date, user, source file)
    and allows structured logging with domain, process, step, status, and message.

    Attributes:
        domain: Data domain being processed
        process: The process being executed (e.g., ingestion, bronze, silver, gold)
        log_table_path: Path to the Delta table where logs are stored
        spark: SparkSession instance (DatabricksSession)
    """

    # Schema for the log Delta table
    LOG_SCHEMA = StructType(
        [
            StructField("date", StringType(), nullable=False),
            StructField("time", TimestampType(), nullable=False),
            StructField("user", StringType(), nullable=False),
            StructField("domain", StringType(), nullable=False),
            StructField("process", StringType(), nullable=False),
            StructField("step", StringType(), nullable=False),
            StructField("source", StringType(), nullable=False),
            StructField("status", StringType(), nullable=False),
            StructField("message", StringType(), nullable=True),
        ]
    )

    def __init__(
        self,
        domain: str,
        process: str,
        log_table_path: str,
        spark: Optional[SparkSession] = None,
    ):
        """Initialize the Databricks logger.

        Args:
            domain: Data domain being processed (e.g., 'customer', 'sales', 'marketing')
            process: Process being executed (e.g., 'ingestion', 'bronze', 'silver',
                    'gold', 'ml-preprocessing', 'eda')
            log_table_path: Path to the Delta table for logs
                           (e.g., 'catalog.schema.logs' or 'dbfs:/path/to/logs')
            spark: SparkSession instance (DatabricksSession). If None, will get or create.

        Raises:
            ValueError: If domain or process are empty strings
            RuntimeError: If SparkSession cannot be obtained
        """
        if not domain or not process:
            raise ValueError("domain and process must be non-empty strings")

        self.domain = domain
        self.process = process
        self.log_table_path = log_table_path

        # Get or create DatabricksSession (subclass of SparkSession)
        if spark is None:
            try:
                # Try to get active session first
                active_session = SparkSession.getActiveSession()
                if active_session is None:
                    # Create new DatabricksSession
                    self.spark: SparkSession = DatabricksSession.builder.getOrCreate()
                else:
                    self.spark: SparkSession = active_session
            except Exception as e:
                raise RuntimeError(f"Failed to obtain SparkSession: {e}") from e
        else:
            self.spark: SparkSession = spark

        # Auto-detect user at initialization
        self._user = self._detect_user()

        # Auto-detect source file at initialization (calling file, not this module)
        self._source = self._detect_source()

    def _detect_user(self) -> str:
        """Auto-detect the Databricks user or runtime identity.

        Returns:
            User identifier (Databricks user email, username, or system user)
        """
        # Try to get Databricks user from Spark configuration
        try:
            user = self.spark.conf.get("spark.databricks.workspaceUrl", None)
            if user:
                # Try to get actual user email/name
                try:
                    user = self.spark.sql("SELECT current_user()").collect()[0][0]
                    return user
                except Exception:
                    pass
        except Exception:
            pass

        # Fallback to environment variables
        user = (
            os.environ.get("DATABRICKS_USER")
            or os.environ.get("USER")
            or os.environ.get("USERNAME")
            or "unknown_user"
        )

        return user
    
    def _detect_source(self) -> str:
        """Auto-detect the source file making the logger call.
        
        Handles both traditional Python file execution and Databricks notebook execution.
        In notebooks, attempts to detect the notebook path from the execution context.
        
        Returns:
            Path to the source file relative to the project root, notebook path,
            or absolute path if unable to determine relative path
        """
        try:
            # First, try to detect if we're in a Databricks notebook
            notebook_path = self._get_notebook_path()
            if notebook_path:
                return notebook_path
            
            # Get the call stack
            stack = inspect.stack()
            
            # Get the path of this logging module to skip it
            this_file = Path(__file__).resolve()
            
            # Find the first frame outside this module and standard library
            for frame_info in stack:
                frame_path = Path(frame_info.filename).resolve()
                frame_filename = frame_path.name
                
                # Skip frames from this logging module
                if frame_path == this_file:
                    continue
                
                # Skip internal Python library frames
                if "site-packages" in str(frame_path) or "lib/python" in str(frame_path):
                    continue
                    
                # Skip unittest/testing framework frames (but not the test file itself)
                if "unittest" in str(frame_path) and not frame_filename.startswith("test_"):
                    continue
                
                # Handle Databricks notebook command execution pattern
                # These are dynamically generated filenames like "command-4640576040890880-321306660"
                if frame_filename.startswith("command-"):
                    # Try to get a better name from the frame's code context
                    # Check if there's a __file__ in the frame's globals
                    if frame_info.frame.f_globals.get("__file__"):
                        try:
                            actual_file = Path(frame_info.frame.f_globals["__file__"]).resolve()
                            if actual_file != this_file:
                                try:
                                    return str(actual_file.relative_to(Path.cwd()))
                                except (ValueError, RuntimeError):
                                    return actual_file.name
                        except Exception:
                            pass
                    
                    # If we can't resolve it, try the next frame
                    continue
                
                # Try to get relative path for cleaner logging
                try:
                    # Try to get relative to current working directory
                    relative_path = frame_path.relative_to(Path.cwd())
                    return str(relative_path)
                except (ValueError, RuntimeError):
                    # If can't get relative path, use filename only
                    return frame_path.name
            
            # Fallback if we somehow don't find a suitable frame
            return "unknown_source"
    
        except Exception:
            return "unknown_source"
    
    def _get_notebook_path(self) -> Optional[str]:
        """Attempt to get the current notebook path in Databricks.
        
        Returns:
            Notebook path if running in a Databricks notebook, None otherwise
        """
        try:
            # Try to get notebook path from Spark tags
            notebook_path = self.spark.conf.get("spark.databricks.notebook.path", None)
            # Validate it's actually a string path (not a mock or other object)
            if notebook_path and isinstance(notebook_path, str) and notebook_path.strip():
                return notebook_path
        except Exception:
            pass
        
        try:
            # Try using dbutils (if available)
            from pyspark.dbutils import DBUtils
            dbutils = DBUtils(self.spark)
            notebook_info = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
            notebook_path = notebook_info.notebookPath().get()
            # Validate it's actually a string path
            if notebook_path and isinstance(notebook_path, str) and notebook_path.strip():
                return notebook_path
        except Exception:
            pass
        
        return None

    def log(
        self,
        step: str,
        status: LogStatus,
        message: str,
        source_override: Optional[str] = None,
    ) -> None:
        """Log an entry to the Delta table.

        Args:
            step: The action/step in the process being performed
                 (e.g., 'read_source', 'validate_schema', 'write_output')
            status: Status of the operation ('success', 'warning', or 'failure')
            message: Descriptive log message
            source_override: Optional override for the source file path.
                           If None, uses auto-detected source.

        Raises:
            ValueError: If step, status, or message are empty
            RuntimeError: If writing to Delta table fails
        """
        if not step or not message:
            raise ValueError("step and message must be non-empty strings")

        if status not in ("success", "warning", "failure"):
            raise ValueError(f"Invalid status: {status}. Must be success, warning, or failure")

        # Capture current timestamp
        log_time = datetime.now()
        log_date = date.today().isoformat()

        # Re-detect source at log time if not overridden
        # This allows capturing the actual calling location
        source = source_override if source_override else self._detect_source()

        # Create log entry as a DataFrame
        log_data = [
            (
                log_date,
                log_time,
                self._user,
                self.domain,
                self.process,
                step,
                source,
                status,
                message,
            )
        ]

        log_df = self.spark.createDataFrame(log_data, schema=self.LOG_SCHEMA)

        # Write to Delta table
        try:
            self._write_to_delta(log_df)
        except Exception as e:
            raise RuntimeError(f"Failed to write log to Delta table: {e}") from e


    def _write_to_delta(self, log_df: DataFrame) -> None:
        """Write log DataFrame to Delta table.

        Creates the table if it doesn't exist, otherwise appends to existing table.

        Args:
            log_df: DataFrame containing log entry
        """
        try:
            # Check if table exists
            table_exists = self.spark.catalog.tableExists(self.log_table_path)

            if table_exists:
                # Append to existing table
                log_df.write.format("delta").mode("append").saveAsTable(self.log_table_path)
            else:
                # Create new table
                log_df.write.format("delta").mode("overwrite").option(
                    "overwriteSchema", "true"
                ).saveAsTable(self.log_table_path)

        except Exception as e:
            # Fallback: try writing to path instead of table name
            # This handles cases where catalog/schema might not be accessible
            try:
                log_df.write.format("delta").mode("append").save(self.log_table_path)
            except Exception as fallback_error:
                raise RuntimeError(
                    f"Failed to write to Delta table '{self.log_table_path}': {e}. "
                    f"Fallback also failed: {fallback_error}"
                ) from e

    def success(self, step: str, message: str, source_override: Optional[str] = None) -> None:
        """Log a successful operation.

        Args:
            step: The action/step in the process
            message: Success message
            source_override: Optional override for source file path
        """
        self.log(step, "success", message, source_override)

    def warning(self, step: str, message: str, source_override: Optional[str] = None) -> None:
        """Log a warning.

        Args:
            step: The action/step in the process
            message: Warning message
            source_override: Optional override for source file path
        """
        self.log(step, "warning", message, source_override)

    def failure(self, step: str, message: str, source_override: Optional[str] = None) -> None:
        """Log a failure.

        Args:
            step: The action/step in the process
            message: Failure/error message
            source_override: Optional override for source file path
        """
        self.log(step, "failure", message, source_override)

    def get_logs(
        self,
        filters: Optional[dict] = None,
        limit: Optional[int] = None,
    ) -> DataFrame:
        """Retrieve logs from the Delta table.

        Args:
            filters: Optional dictionary of column:value pairs to filter logs
                    (e.g., {'status': 'failure', 'step': 'validation'})
            limit: Optional limit on number of rows to return

        Returns:
            DataFrame containing filtered log entries

        Raises:
            RuntimeError: If reading from Delta table fails
        """
        try:
            df = self.spark.read.format("delta").table(self.log_table_path)

            # Apply filters if provided
            if filters:
                for column, value in filters.items():
                    df = df.filter(df[column] == value)

            # Apply limit if provided
            if limit:
                df = df.limit(limit)

            return df

        except Exception as e:
            raise RuntimeError(f"Failed to read logs from Delta table: {e}") from e


def create_logger(
    domain: str,
    process: str,
    log_table_path: str,
    spark: Optional[SparkSession] = None,
) -> DatabricksLogger:
    """Factory function to create a DatabricksLogger instance.

    Args:
        domain: Data domain being processed
        process: Process being executed
        log_table_path: Path to the Delta table for logs
        spark: Optional SparkSession instance (can be DatabricksSession)

    Returns:
        Configured DatabricksLogger instance
    """
    return DatabricksLogger(
        domain=domain,
        process=process,
        log_table_path=log_table_path,
        spark=spark,
    )
