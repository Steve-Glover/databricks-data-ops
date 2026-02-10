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
│   │   ├── operations/        # Common readers, writers, transformers
│   │   ├── delta/             # Delta table management
│   │   └── utils/             # Logging, errors, Spark helpers
│   ├── domains/               # Domain-specific code (NOT in wheel)
│   │   ├── member/            # Member domain
│   │   └── claims/            # Claims domain
│   └── config/                # Configuration management
├── notebooks/                 # Databricks notebooks
├── dlt/                       # Delta Live Tables pipelines
├── resources/                 # DAB resource definitions
├── tests/                     # Test suite
├── configs/                   # Environment-specific configs
├── scripts/                   # Utility scripts
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
# Run all tests
pytest tests/

# Run specific test suite
pytest tests/unit/data_ops/
pytest tests/unit/domains/member/
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

# Deploy to staging
databricks bundle deploy -t staging

# Deploy to production
databricks bundle deploy -t prod
```

### What Gets Deployed

- **data_ops library**: Packaged as wheel and installed on all clusters
- **Domain code**: Deployed as source files (notebooks, DLT pipelines)
- **Jobs & Pipelines**: Databricks Jobs and DLT Pipelines configured in resources/

## Usage

### In Notebooks

```python
# Import data_ops library from deployed wheel
from databricks_data_ops.operations import readers, writers
from databricks_data_ops.delta import table_manager
from databricks_data_ops.utils import logging

# Read data
df = readers.read_parquet("/data/raw/member")

# Write to Delta
writers.write_delta(df, "catalog.schema.table")
```

### In DLT Pipelines

```python
import dlt
from databricks_data_ops.operations import readers, quality

@dlt.table(name="member_bronze")
def member_bronze():
    return readers.read_parquet("/data/raw/member")

@dlt.table(name="member_silver")
@dlt.expect_all(quality.standard_expectations())
def member_silver():
    return dlt.read("member_bronze")
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

Each environment has its own catalog and schemas:

- **dev**: `dev_catalog.dev_member`, `dev_catalog.dev_claims`
- **staging**: `staging_catalog.staging_member`, `staging_catalog.staging_claims`
- **prod**: `prod_catalog.prod_member`, `prod_catalog.prod_claims`

## Development Workflow

### Adding a New Domain

1. Create domain structure under `src/domains/your_domain/`
2. Add pipelines (bronze.py, silver/, gold.py)
3. Add validations and schemas
4. Create DLT pipeline definitions in `dlt/your_domain/`
5. Add job definitions in `resources/jobs/your_domain/`
6. Update tests
7. Update documentation

### Adding New Silver Layer Scripts

For domains with complex transformations:

```
src/domains/member/pipelines/silver/
├── __init__.py
├── demographics.py
├── enrollment.py
└── eligibility.py
```

## Testing

### Unit Tests

```bash
# Test data_ops library
pytest tests/unit/data_ops/

# Test domain logic
pytest tests/unit/domains/member/
pytest tests/unit/domains/claims/
```

### Integration Tests

```bash
# End-to-end tests
pytest tests/integration/
```

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
- **Testing**: pytest, pytest-spark, chispa
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
