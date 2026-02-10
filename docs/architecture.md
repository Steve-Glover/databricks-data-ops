# Architecture Documentation

## Overview

This document describes the architectural decisions and patterns used in the Databricks Data Ops project.

## Core Architectural Principles

### 1. Domain-Driven Design (DDD)

**Rationale**: Business domains (member, claims) own their data pipelines and transformations.

**Benefits**:
- **Separation of Concerns**: Each domain is self-contained with its own pipelines, validations, and schemas
- **Team Autonomy**: Domain teams can work independently without affecting other domains
- **Clear Ownership**: Business logic lives with the domain, not scattered across shared code
- **Scalability**: New domains can be added without modifying existing ones

**Structure**:
```
domains/
├── member/               # Member domain
│   ├── pipelines/       # Bronze, Silver, Gold transformations
│   ├── validations/     # Domain-specific quality checks
│   └── schemas/         # Domain data schemas
└── claims/              # Claims domain
    ├── pipelines/
    ├── validations/
    └── schemas/
```

### 2. Medallion Architecture

**Pattern**: Bronze → Silver → Gold data layers

```
Raw Data → Bronze (Raw) → Silver (Cleaned) → Gold (Analytics-Ready)
```

**Layer Definitions**:

- **Bronze Layer** (`{catalog}.bronze.*`)
  - Raw, unprocessed data as ingested from source systems
  - Minimal transformations (schema enforcement, basic parsing)
  - Historical record of all data received
  - External volumes: `{catalog}.bronze.external` for landing files

- **Silver Layer** (`{catalog}.silver.*`)
  - Cleaned, validated, and conformed data
  - Deduplication, standardization, type conversions
  - Business rules applied
  - Queryable by data analysts and data scientists

- **Gold Layer** (`{catalog}.gold.*`)
  - Aggregated, business-level data
  - Optimized for analytics and reporting
  - Pre-computed metrics and KPIs
  - Directly powers dashboards and reports

**Benefits**:
- **Progressive Refinement**: Each layer adds value incrementally
- **Flexibility**: Can reprocess from any layer if business rules change
- **Performance**: Gold layer optimized for query performance
- **Auditability**: Full lineage from raw to analytics-ready

### 3. Library vs Domain Code Separation

**Critical Design Decision**: Only shared utilities go in `src/data_ops/`, domain logic stays in `domains/`

**Why This Matters**:

```
src/data_ops/  →  Packaged as wheel  →  Installed on all clusters  →  Rarely changes
domains/       →  Synced as source   →  Deployed directly          →  Changes frequently
```

**Benefits**:
- ✅ **Fast Iteration**: Update domain pipelines without rebuilding the wheel
- ✅ **No Version Conflicts**: Domain code doesn't have version dependencies
- ✅ **Clear Boundary**: Shared utilities are truly generic, not domain-specific
- ✅ **Deployment Efficiency**: Wheel builds are slow; source sync is fast

**What Goes Where**:

| Location | Contains | Examples |
|----------|----------|----------|
| `src/data_ops/` | Generic, reusable utilities | Readers, writers, Delta helpers, logging, config |
| `domains/` | Business logic, transformations | Member demographics pipeline, claims processing |

**Anti-Pattern to Avoid**:
```python
# ❌ DON'T: Put domain logic in src/data_ops/
# src/data_ops/operations/member_transform.py
def transform_member_demographics(df):
    # This is domain-specific, NOT a generic utility
    return df.filter("age >= 18").withColumn("is_adult", lit(True))

# ✅ DO: Put domain logic in domains/
# domains/member/pipelines/silver/demographics.py
def transform_demographics(df):
    return df.filter("age >= 18").withColumn("is_adult", lit(True))
```

### 4. Unity Catalog Structure

**Standard Schema Structure Across All Environments**:

```
Catalog (environment-specific): dev, sit, prod
├── ua                      # User Acceptance / landing zone
├── bronze                  # Raw data layer
│   └── external            # External tables/volumes for files
├── silver                  # Cleaned data layer
├── gold                    # Analytics-ready layer
│   └── wheels              # Deployed wheel tracking
├── default                 # Default schema (auto-created)
└── information_schema      # System metadata (auto-created)
```

**Environment Isolation**:
- Each environment (`dev`, `sit`, `prod`) has its own catalog
- Same schema structure in each catalog ensures consistency
- Promotes code reusability across environments

**Key Locations**:
- Landing zone: `{catalog}.bronze.external` (for raw files)
- Wheel tracking: `{catalog}.gold.wheels` (deployment metadata)
- Logs: Delta tables with structured schema (location TBD per domain)

### 5. Databricks Asset Bundle (DAB) Pattern

**Infrastructure as Code**: All Databricks resources defined in YAML

```
resources/
├── jobs/                 # Databricks Jobs definitions
│   ├── member/
│   └── claims/
└── pipelines/            # DLT Pipeline definitions
    ├── member/
    └── claims/
```

**Benefits**:
- **Version Control**: All infrastructure in git
- **Reproducibility**: Environments are consistent
- **CI/CD Ready**: Deploy via `databricks bundle deploy`
- **Multi-Environment**: Same definitions, different targets

**Deployment Flow**:
```bash
databricks bundle validate      # Validate configuration
databricks bundle deploy -t dev # Deploy to dev
databricks bundle deploy -t sit # Deploy to staging
databricks bundle deploy -t prod # Deploy to production
```

## Data Flow Diagrams

### High-Level Data Flow

```
External Systems
      ↓
Landing Zone ({catalog}.bronze.external)
      ↓
Bronze Layer (Raw ingestion)
      ↓
Silver Layer (Cleaning & Transformation)
      ↓
Gold Layer (Aggregation & Analytics)
      ↓
Dashboards & Reports
```

### Member Domain Flow

```
Member Source Data (CSV/Parquet)
      ↓
dev.bronze.external.member_files
      ↓
dev.bronze.member_raw (Raw table)
      ↓
Silver Layer Transformations:
├── dev.silver.member_demographics (Clean demographics)
├── dev.silver.member_enrollment (Enrollment history)
└── dev.silver.member_eligibility (Eligibility status)
      ↓
dev.gold.member_summary (Aggregated metrics)
```

### Claims Domain Flow

```
Claims Source Data (JSON/Parquet)
      ↓
dev.bronze.external.claims_files
      ↓
dev.bronze.claims_raw (Raw table)
      ↓
Silver Layer Transformations:
├── dev.silver.claims_medical (Medical claims)
├── dev.silver.claims_pharmacy (Pharmacy claims)
└── dev.silver.claims_professional (Professional claims)
      ↓
dev.gold.claims_summary (Aggregated metrics)
```

## Logging Architecture

**Structured Logging to Delta Tables**:

```python
# Custom logger: src/data_ops/utils/logging.py
logger = get_logger(domain="member", process="silver_transform")

# Log entry structure:
{
    "date": "2026-02-06",           # Auto-detected
    "time": "14:30:00",             # Auto-detected
    "user": "sglover",              # Auto-detected
    "source_file": "demographics.py", # Auto-detected
    "domain": "member",             # User-provided
    "process": "silver_transform",  # User-provided
    "step": "deduplication",        # User-provided
    "status": "success",            # User-provided
    "message": "Removed 150 dupes"  # User-provided
}
```

**Benefits**:
- **Queryable**: Use SQL to analyze pipeline execution
- **Structured**: Consistent schema across all logs
- **Auditable**: Full history of pipeline runs
- **Debugging**: Easy to trace errors and performance issues

## Deployment Strategy

### Library Deployment (src/data_ops/)

1. **Build**: `uv build` creates `dist/data_ops-*.whl`
2. **Deploy**: Databricks Asset Bundle uploads wheel
3. **Install**: Wheel installed on all clusters automatically
4. **Track**: Wheel metadata saved to `{catalog}.gold.wheels` table

**When to Rebuild**:
- Only when shared utilities change
- Not when domain code changes (domains are synced directly)

### Domain Code Deployment (domains/)

1. **Sync**: DAB syncs domain code directly to workspace
2. **No Build**: Domain code deployed as source files
3. **Fast Iteration**: Changes are immediate

## Technology Stack

- **Runtime**: Python 3.12+
- **Package Manager**: uv (faster, more reliable than pip)
- **Platform**: Databricks (Asset Bundles, Delta Live Tables, Unity Catalog)
- **Data**: Delta Lake, Apache Spark
- **Testing**: unittest (not pytest)
- **Code Quality**: ruff, black, mypy
- **Configuration**: Pydantic, PyYAML

## Key Design Trade-offs

| Decision | Trade-off | Why We Chose This |
|----------|-----------|-------------------|
| Domain-driven design | More directories/files | Clear ownership, team autonomy |
| Separate wheel for utilities | Two codebases to manage | Fast domain iteration |
| Medallion architecture | More storage/compute | Data quality, auditability |
| Unity Catalog per environment | Data duplication | Environment isolation, safety |
| Databricks Asset Bundles | YAML configuration overhead | Infrastructure as code, reproducibility |
| unittest over pytest | More verbose tests | Databricks standard, better mocking |

## Future Considerations

- **CI/CD Pipeline**: Automate testing and deployment
- **Data Quality Framework**: Automated validation and monitoring
- **Schema Evolution**: Strategy for handling schema changes
- **Performance Optimization**: Query optimization, caching strategies
- **Multi-Region**: Strategy for deploying across regions
- **Data Governance**: Access controls, data lineage, compliance