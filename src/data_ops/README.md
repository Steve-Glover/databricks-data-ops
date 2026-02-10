# data_ops

Shared utilities library for Databricks data pipelines. Packaged as a wheel (`data_ops-*.whl`) and deployed via Databricks Asset Bundles. Domain-specific code does **not** belong here — only reusable utilities.

## Structure

```
src/data_ops/
├── operations/
│   └── volume_extractor.py   # Volume ingestion: discover, validate, write, archive
├── utils/
│   ├── logging.py             # DatabricksLogger — structured logging to Delta tables
│   └── errors.py              # ValidationResult, DataValidationError
└── tests/
    ├── unit/                  # Fast, mocked tests
    └── integration/           # Requires Databricks connectivity
```

## Modules

### `utils.logging` — DatabricksLogger

Structured logger that writes to Delta tables. Auto-detects date, time, user, and source file.

```python
from data_ops.utils.logging import DatabricksLogger

logger = DatabricksLogger(
    domain="sales", process="bronze", log_table_path="dev.bronze.logs"
)
logger.success(step="read_data", message="Read 1000 rows from source")
logger.failure(step="validate", message="Schema mismatch detected")
```

### `utils.errors` — Validation Errors

`ValidationResult` captures a single check outcome (pass/fail with expected vs actual). `DataValidationError` collects **all** results before raising, so callers see the full picture — not just the first failure.

```python
from data_ops.utils.errors import DataValidationError, ValidationResult
```

### `operations.volume_extractor` — VolumeExtractor

Ingests parquet file chunks from Databricks Volumes into bronze Delta tables.

**Pipeline**: discover files → read meta → read & union chunks → validate → write to bronze → archive

**File naming convention**: `tablename_YYYYMM_YYYYMM` (no file extension), with a companion `tablename.meta` parquet file containing validation expectations (row count, column count, unique ID count).

```python
from data_ops.operations.volume_extractor import VolumeExtractionConfig, VolumeExtractor

config = VolumeExtractionConfig(
    catalog="dev",
    source_volume_path="/Volumes/dev/bronze/external/mft/",
    archive_volume_path="/Volumes/dev/bronze/external/archive/",
    log_table_path="dev.bronze.extraction_logs",
)
extractor = VolumeExtractor(config)
results = extractor.extract_all()
# {"customers": "success", "orders": "success"}
```

## Running Tests

```bash
# All unit tests (fast, no Databricks needed)
uv run poe test-unit

# All integration tests (requires Databricks connectivity)
uv run poe test-integration

# All tests
uv run poe test
```

## Code Quality

```bash
# Lint + format check + type check
uv run poe check

# Auto-format
uv run poe fix
```

## Building

```bash
uv build
# Output: dist/data_ops-*.whl
```
