# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Initial project structure
- `UKPNClient` async client for OpenDataSoft API v2.1
- Pydantic models for API responses (Dataset, Record, etc.)
- Custom exception classes
- Configuration management with environment variable support
- Pre-commit hooks (ruff, detect-secrets)
- GitHub Actions CI workflow
- Security policy (SECURITY.md)
- 30 unit tests with 70% coverage

### Security
- API key management via environment variables
- No secrets committed to repository
- Dependency vulnerability scanning in CI
