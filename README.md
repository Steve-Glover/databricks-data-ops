# Databricks Data Ops

A production-ready ETL/data pipeline repository for Databricks with domain-driven architecture, supporting Spark jobs, Delta Lake tables, DLT pipelines, and workflow orchestration.

## Overview

This repository implements a comprehensive data operations framework using:
- **Domain-Driven Design**: Separate domains for member and claims data
- **Medallion Architecture**: Bronze → Silver → Gold data layers
- **Deployable Library**: Shared `data_ops` library packaged as a wheel
- **Databricks Asset Bundles**: Infrastructure as code for Databricks resources

## Repository Structure

```
databricks-data-ops/
├── src/
│   ├── data_ops/              # Shared library (packaged as wheel)
│   │   ├── operations/        # Volume extraction, common operations
│   │   ├── utils/             # Logging, errors, Spark helpers
│   │   └── tests/             # Unit tests for the shared library
│   └── config/                # Configuration management
├── domains/                   # Domain-specific ETL pipelines (NOT in wheel)
│   └── member/                # Member domain (pipelines, schemas, validations)
├── notebooks/                 # Databricks notebooks
├── resources/                 # DAB job and pipeline YAML definitions
└── docs/                      # Documentation
```

## Domain Architecture

### Member Domain
```
Raw Data → Bronze → Silver (Demographics, Enrollment, Eligibility) → Gold
```

### Claims Domain
```
Raw Data → Bronze → Silver (Medical, Pharmacy, Professional) → Gold
```

## Prerequisites

- Python 3.12+
- [uv](https://github.com/astral-sh/uv) package manager
- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html)
- Access to Databricks workspace

## Local Development Setup

### 1. Install Dependencies

```bash
# Install dependencies with uv
uv sync

# Install development dependencies
uv sync --extra dev
```

### 2. Build the Library

```bash
# Build the data_ops library wheel (only packages src/data_ops/)
uv build
```

### 3. Run Tests

```bash
# Run all unit tests
uv run python -m unittest discover -s src/data_ops/tests -p 'test_*.py' -v
```

### 4. Linting and Formatting

```bash
# Run linter
ruff check src/

# Format code
black src/

# Type checking
mypy src/data_ops/
```

## Deployment

### Deploy to Databricks

```bash
# Validate bundle configuration
databricks bundle validate

# Deploy to dev environment
databricks bundle deploy -t dev

# Deploy to SIT (staging)
databricks bundle deploy -t sit

# Deploy to production
databricks bundle deploy -t prod
```

### What Gets Deployed

- **data_ops library**: Packaged as wheel and installed on all clusters
- **Domain code**: Deployed as source files (notebooks, DLT pipelines)
- **Jobs & Pipelines**: Databricks Jobs and DLT Pipelines configured in resources/

## Usage

### Volume Extraction

```python
from data_ops.operations.volume_extractor import VolumeExtractionConfig, VolumeExtractor

config = VolumeExtractionConfig(
    catalog="dev",
    source_volume_path="/Volumes/dev/bronze/external/mft/",
    archive_volume_path="/Volumes/dev/bronze/external/archive/",
    log_table_path="dev.bronze.extraction_logs",
)
extractor = VolumeExtractor(config, spark)
results = extractor.extract(["members", "claims"])
```

### Logging

```python
from data_ops.utils.logging import DatabricksLogger

logger = DatabricksLogger(
    domain="member",
    process="silver_transform",
    log_table_path="dev.bronze.pipeline_logs",
    spark=spark,
)
logger.success(step="transform", message="Completed transformation")
```

## Project Configuration

### Environment Variables

Create a `.env` file for local development:

```bash
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=your-token
ENVIRONMENT=dev
```

### Catalog Configuration

Each environment uses a Unity Catalog with shared schemas:

- **dev**: `dev.bronze`, `dev.silver`, `dev.gold`
- **sit**: `sit.bronze`, `sit.silver`, `sit.gold`
- **prod**: `prod.bronze`, `prod.silver`, `prod.gold`

## Development Workflow

### Adding a New Domain

1. Create domain structure under `domains/your_domain/` (pipelines, schemas, validations)
2. Add job/pipeline YAML definitions in `resources/`
3. Update tests
4. Update documentation

## Testing

### Unit Tests

```bash
# Run all unit tests
uv run python -m unittest discover -s src/data_ops/tests -p 'test_*.py' -v
```

### Integration Tests

Integration tests run on Databricks via the VS Code Databricks extension (requires workspace credentials).

## Documentation

- [Architecture](docs/architecture.md) - Architectural decisions and patterns
- [Getting Started](docs/getting_started.md) - Developer onboarding guide
- [Pipeline Design](docs/pipeline_design.md) - Pipeline design and data flow
- [CHANGELOG](CHANGELOG.md) - Version history

## Design Decisions

1. **Domain-Driven Design**: Business domains (member, claims) own their data pipelines
2. **Medallion Architecture**: Industry-standard Bronze/Silver/Gold layers
3. **Library for Common Only**: Only `data_ops` packaged as wheel; domain code as source files
4. **Multiple Silver Scripts**: Support complex transformations with multiple files per domain
5. **DAB-Native**: Leverage Databricks Asset Bundles for deployment
6. **Type Safety**: Pydantic for configuration validation
7. **Comprehensive Testing**: Domain-specific and library tests

## Technology Stack

- **Runtime**: Python 3.12
- **Package Manager**: uv
- **Platform**: Databricks Asset Bundles, Delta Live Tables
- **Data**: Delta Lake, Apache Spark
- **Testing**: unittest, chispa
- **Code Quality**: ruff, black, mypy
- **Configuration**: Pydantic, PyYAML

## Contributing

1. Create a feature branch
2. Make changes with tests
3. Run linting and tests locally
4. Submit pull request
5. Deploy to dev for validation

## License

MIT

## Support

For questions or issues, please contact the Data Engineering Team.
