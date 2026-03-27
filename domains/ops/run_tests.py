"""Run data_ops unit tests as part of the deployment pipeline.

Discovers and runs all unit tests packaged with data_ops using the installed
wheel location, so tests always run against the exact version being deployed.
Exits non-zero on any failure, blocking the downstream deploy task.
"""

import importlib.util
import sys
import unittest
from pathlib import Path


def main() -> None:
    spec = importlib.util.find_spec("data_ops")
    if spec is None or spec.origin is None:
        print("ERROR: data_ops package not found in environment")
        sys.exit(1)

    package_dir = Path(spec.origin).parent
    tests_dir = package_dir / "tests"
    top_level_dir = package_dir.parent  # site-packages root

    if not tests_dir.is_dir():
        print(f"ERROR: tests directory not found at {tests_dir}")
        sys.exit(1)

    print(f"Running data_ops unit tests from {tests_dir}\n")
    suite = unittest.TestLoader().discover(
        start_dir=str(tests_dir),
        pattern="test_*.py",
        top_level_dir=str(top_level_dir),
    )
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)


if __name__ == "__main__":
    main()
