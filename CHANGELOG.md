# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.10]
### Chore
- Tidied up pyroject.toml for clarity on dependencies
## [0.1.9]

### Fixed
- NOTICE file now correctly references CC-BY-4.0 (was Apache 2.0)
- Backfilled CHANGELOG entries for v0.1.1 through v0.1.8

## [0.1.8] - 2025-03-12

### Fixed
- Pinned GitHub Action SHAs for CodeQL compliance in publish workflows
- Automatic dev branch sync after version-tag bumps on main
- Test fixes for curtailment column names, LTDS substation columns, CLI parser, and Record model

### Added
- Production PyPI publish workflow (`publish-pypi.yml`) with trusted publishing
- TestPyPI pre-release publish workflow (`publish-testpypi.yml`)

## [0.1.7] - 2025-03-11

### Changed
- Updated GitHub Actions to use pinned commit SHAs instead of version tags
- Added PyPI production publish workflow

## [0.1.6] - 2025-03-11

### Changed
- Version control workflow improvements

## [0.1.5] - 2025-03-11

### Fixed
- CI pipeline: switched from `uv.lock` to `pyproject.toml` dependency resolution
- Full dependency installation with `--all-extras` in CI
- Integration tests now skip gracefully in CI instead of failing
- Fixed `test_cli` FakeParser signature, LTDS coverage test column names
- Ruff lint and format fixes

## [0.1.2] - 2025-03-10

### Changed
- Improved documentation for Tutorial 01 (Getting Started)

## [0.1.1] - 2025-03-10

### Added
- Automatic version bump and tagging workflow for main branch
- Docs deployment workflow for GitHub Pages
- Local ignore rules for development artifacts

## [0.1.0-beta] - 2024-03-11

### Added
- **LTDS (Long Term Development Statement) module** (`ukpyn.ltds`)
  - `get_table_2a()` - Fetch 2-winding transformer data
  - `get_table_2b()` - Fetch 3-winding transformer data
  - `get_table_3a()` - Fetch substation observed peak demand data
- **Powerflow orchestrators** (`ukpyn.orchestrators.powerflow`)
  - `get_half_hourly_timeseries()` - Fetch transformer powerflow data with automatic pagination
  - Auto-detection of licence area from LTDS topology
  - Support for both grid and primary substations
  - Debug mode for troubleshooting data retrieval
- **Time series utilities** (`ukpyn.utils`)
  - `records_to_timeseries()` - Convert API records to pandas DataFrame
  - `detect_step_changes()` - Identify sudden load profile shifts
  - `identify_gaps()` - Detect missing data periods
  - `fill_gaps()` - Interpolate missing values
  - `flag_outliers()` - IQR-based anomaly detection
  - `quality_control()` - Comprehensive data quality assessment
- **Example notebook** (`examples/forecasting.ipynb`)
  - End-to-end substation load forecasting workflow
  - Data acquisition from UKPN Open Data Portal
  - Quality control and anomaly detection
  - Feature engineering for ML models
  - Multiple forecasting models (Linear Regression, Random Forest, Gradient Boosting)
  - Probabilistic forecasting (P10/P50/P90 quantiles)
  - Operational flexibility market analysis
- **PyPI package configuration**
  - `pyproject.toml` with full metadata
  - Build system configuration
  - Development dependencies specification
- **Documentation**
  - README with installation and usage examples
  - API reference documentation
  - Contributing guidelines
  - Security policy

### Changed
- `UKPNClient` now supports both dataset and record-level operations
- Enhanced error handling with detailed exception messages
- Improved pagination handling for large datasets

### Fixed
- Timezone handling in time series conversion
- Edge cases in gap detection for irregular sampling intervals
- Rate-of-change detection for stuck sensor identification

### Security
- API key management via environment variables
- No secrets committed to repository
- Dependency vulnerability scanning in CI

## [0.0.1] - 2024-03-05

### Added
- Initial project structure
- `UKPNClient` async client for OpenDataSoft API v2.1
- Pydantic models for API responses (Dataset, Record, etc.)
- Custom exception classes
- Configuration management with environment variable support
- Pre-commit hooks (ruff, detect-secrets)
- GitHub Actions CI workflow
- 30 unit tests with 70% coverage

### Security
- API key management via environment variables
- No secrets committed to repository
- Dependency vulnerability scanning in CI
