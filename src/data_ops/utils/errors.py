"""Custom error classes for data validation in Databricks pipelines.

Provides structured validation results and a custom exception that collects
all validation outcomes (pass and fail) before raising.
"""

from dataclasses import dataclass
from typing import Any


@dataclass
class ValidationResult:
    """Stores the outcome of a single validation check.

    Attributes:
        check_name: Identifier for the check (e.g., 'row_count', 'column_count')
        expected: The expected value
        actual: The actual observed value
        passed: Whether the check passed
        message: Human-readable description of the result
    """

    check_name: str
    expected: Any
    actual: Any
    passed: bool
    message: str


class DataValidationError(Exception):
    """Raised when one or more data validation checks fail.

    Collects ALL validation results (passed and failed) so the caller
    can see the full picture, not just the first failure.

    Attributes:
        table_name: Name of the table that failed validation
        results: All validation results (passed and failed)
        passed_results: Only the results that passed
        failed_results: Only the results that failed
    """

    def __init__(self, table_name: str, results: list[ValidationResult]):
        self.table_name = table_name
        self.results = results
        self.passed_results = [r for r in results if r.passed]
        self.failed_results = [r for r in results if not r.passed]
        super().__init__(self._build_message())

    def _build_message(self) -> str:
        """Format all validation results with [PASS]/[FAIL] prefixes."""
        total = len(self.results)
        failed = len(self.failed_results)
        lines = [
            f"Validation failed for table '{self.table_name}': "
            f"{failed} of {total} checks failed"
        ]
        for r in self.results:
            prefix = "[PASS]" if r.passed else "[FAIL]"
            lines.append(f"  {prefix} {r.check_name}: {r.message}")
        return "\n".join(lines)
