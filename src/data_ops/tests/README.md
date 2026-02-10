# Data Ops Tests

This directory contains tests organized by type: unit tests and integration tests.

## Directory Structure

```
tests/
├── unit/              # Fast, isolated unit tests (use mocks)
│   └── utils/
│       └── test_logging.py
└── integration/       # Slower tests against real Databricks
    └── utils/
        └── test_logging.py
```

## Running Tests

### Run All Tests
```bash
python -m unittest discover -s src/data_ops/tests
```

### Run Only Unit Tests (Fast)
```bash
python -m unittest discover -s src/data_ops/tests/unit
```

### Run Only Integration Tests (Requires Databricks Connection)
```bash
# Using serverless compute (default)
python -m unittest discover -s src/data_ops/tests/integration

# Using specific cluster
DATABRICKS_CLUSTER_ID=<cluster-id> python -m unittest discover -s src/data_ops/tests/integration
```

### Run Tests for Specific Module
```bash
# Unit tests for logging
python -m unittest src.data_ops.tests.unit.utils.test_logging

# Integration tests for logging
python -m unittest src.data_ops.tests.integration.utils.test_logging
```

### Run with Verbose Output
```bash
python -m unittest discover -s src/data_ops/tests/unit -v
```

## Test Guidelines

### Unit Tests (`tests/unit/`)
- **Purpose**: Test logic in isolation
- **Speed**: Fast (seconds)
- **Dependencies**: None (uses mocks)
- **When to run**: On every code change, in CI/CD for every PR
- **Characteristics**:
  - Use `unittest.mock.MagicMock()` for external dependencies
  - No network calls or database connections
  - Test edge cases, validation, error handling

### Integration Tests (`tests/integration/`)
- **Purpose**: Test end-to-end functionality with real systems
- **Speed**: Slower (requires Databricks connection)
- **Dependencies**: Databricks workspace, valid credentials
- **When to run**: Before commits, in CI/CD on main branch, nightly builds
- **Characteristics**:
  - Use real `DatabricksSession`
  - Actually read/write to Delta tables
  - Test real-world scenarios and edge cases
  - Clean up resources in `tearDown`/`tearDownClass`

## Adding New Tests

When adding tests for a new module:

1. **Create unit tests**: `tests/unit/<module_path>/test_<module_name>.py`
2. **Create integration tests** (if needed): `tests/integration/<module_path>/test_<module_name>.py`
3. **Keep file names the same** in both directories (only the directory differs)
4. **Add `__init__.py`** files to new directories for proper test discovery

Example for a new module `src/data_ops/transforms/clean.py`:
```
tests/
├── unit/
│   └── transforms/
│       ├── __init__.py
│       └── test_clean.py
└── integration/
    └── transforms/
        ├── __init__.py
        └── test_clean.py
```
