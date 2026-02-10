# Changelog

All notable changes to this project will be documented in this file.

## [0.1.0] - Unreleased

### Added
- Initial repository structure
- Domain-driven architecture for member and claims domains
- Data ops library (operations, delta, utils)
- Placeholder files for pipelines, validations, and schemas
- Testing infrastructure
- Example notebooks and DLT pipelines
- Databricks Asset Bundle configuration
- Custom logging utility with Delta table integration
- Comprehensive unit and integration tests for logging

### Changed
- **BREAKING**: Restructured project to follow Databricks Asset Bundle best practices
  - Moved `src/domains/` → `domains/` (domain pipelines now at root level)
  - `src/data_ops/` remains as shared library (packaged as wheel)
  - Updated `databricks.yml` to sync domain code separately (not packaged in wheel)
  - Updated `pyproject.toml` project name from `databricks-data-ops` → `data-ops`
  - Added `domains` to PYTHONPATH for proper imports
- Wheel artifact now contains ONLY shared library code, not domain-specific logic
- Domain pipelines are now synced directly to Databricks workspace, allowing updates without wheel rebuild

### Technical Details
- **Package separation**: `src/data_ops/` builds to `data_ops-*.whl` for reusable utilities
- **Application code**: `domains/` contains business logic, synced via DAB includes
- **Flexibility**: Domain updates no longer require rebuilding/redeploying the library wheel
- Follows official Databricks monorepo patterns for multi-domain data engineering projects
