"""Unit tests for custom error classes."""

import unittest

from data_ops.utils.errors import DataValidationError, ValidationResult


class TestValidationResult(unittest.TestCase):
    """Test cases for ValidationResult dataclass."""

    def test_field_assignment(self):
        """Test that all fields are correctly assigned for passed and failed results."""
        test_cases = [
            {
                "check_name": "row_count",
                "expected": 1000,
                "actual": 1000,
                "passed": True,
                "message": "Expected 1000 rows, got 1000",
            },
            {
                "check_name": "column_count",
                "expected": 15,
                "actual": 12,
                "passed": False,
                "message": "Expected 15 columns, got 12",
            },
        ]

        for case in test_cases:
            with self.subTest(check_name=case["check_name"], passed=case["passed"]):
                result = ValidationResult(**case)
                self.assertEqual(result.check_name, case["check_name"])
                self.assertEqual(result.expected, case["expected"])
                self.assertEqual(result.actual, case["actual"])
                self.assertEqual(result.passed, case["passed"])
                self.assertEqual(result.message, case["message"])


class TestDataValidationError(unittest.TestCase):
    """Test cases for DataValidationError exception."""

    def setUp(self):
        """Set up common test data."""
        self.results = [
            ValidationResult("row_count", 1000, 1000, True, "Expected 1000 rows, got 1000"),
            ValidationResult("column_count", 15, 12, False, "Expected 15 columns, got 12"),
            ValidationResult("unique_id_count", 500, 500, True, "Expected 500 unique values, got 500"),
        ]

    def test_is_exception(self):
        """Test that DataValidationError is an Exception subclass."""
        error = DataValidationError("customers", self.results)
        self.assertIsInstance(error, Exception)

    def test_table_name_stored(self):
        """Test that table_name is stored on the error."""
        error = DataValidationError("orders", self.results)
        self.assertEqual(error.table_name, "orders")

    def test_result_separation(self):
        """Test that results are correctly separated into passed and failed."""
        test_cases = [
            {
                "label": "mixed_results",
                "results": self.results,
                "expected_total": 3,
                "expected_passed": 2,
                "expected_failed": 1,
            },
            {
                "label": "all_failed",
                "results": [r for r in self.results if not r.passed] + [
                    ValidationResult("extra", 500, 300, False, "Extra failure"),
                ],
                "expected_total": 2,
                "expected_passed": 0,
                "expected_failed": 2,
            },
            {
                "label": "single_failure",
                "results": [self.results[1]],
                "expected_total": 1,
                "expected_passed": 0,
                "expected_failed": 1,
            },
        ]

        for case in test_cases:
            with self.subTest(label=case["label"]):
                error = DataValidationError("test_table", case["results"])
                self.assertEqual(len(error.results), case["expected_total"])
                self.assertEqual(len(error.passed_results), case["expected_passed"])
                self.assertEqual(len(error.failed_results), case["expected_failed"])

    def test_message_formatting(self):
        """Test that error message contains expected markers."""
        error = DataValidationError("customers", self.results)
        message = str(error)

        expected_fragments = [
            ("header", "Validation failed for table 'customers'"),
            ("failure_count", "1 of 3 checks failed"),
            ("pass_marker", "[PASS] row_count:"),
            ("fail_marker", "[FAIL] column_count:"),
            ("pass_marker_2", "[PASS] unique_id_count:"),
        ]

        for label, fragment in expected_fragments:
            with self.subTest(fragment=label):
                self.assertIn(fragment, message)


if __name__ == "__main__":
    unittest.main()
