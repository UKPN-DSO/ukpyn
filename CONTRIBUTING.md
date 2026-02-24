# Contributing to ukpyn

Thanks for helping improve `ukpyn`.

This project is for both novice and experienced coders, so contributions that improve accessibility, clarity, and reliability are all valuable.

## Ways to Contribute

- Improve docs and examples (great first contribution)
- Report bugs or confusing API behavior
- Add tests and fix issues
- Improve orchestrator ergonomics and typing

## Development Setup

```bash
git clone https://github.com/ukpyn/ukpyn.git
cd ukpyn
uv sync
```

## Before Opening a PR

Run the core checks locally:

```bash
pytest
ruff check .
pre-commit run --all-files
```

## Branch Strategy

- Create branches from `dev`
- Use `feature/*` branch names
- Open PRs into `dev`
- `dev`and `main` are protected
- `main` is release-only

## Pull Request Guidance

- Keep changes focused and small where possible
- Include or update tests for behavior changes
- Update docs when public behavior changes
- Avoid committing secrets (`.env`, API keys, tokens)

## Automated Registry Triage Issues

- A scheduled GitHub Action audits ODP metadata and opens triage issues when new unmanaged datasets are discovered.
- These issues are routed to `copilot` with a standardized task brief to classify datasets, implement orchestrator + registry updates, add tutorial usage, and open a PR into `dev`.
- Maintainers review those PRs before merge.

## Need Help?

If you are new to open source, open an issue first and label it as a beginner question. We are happy to help you scope a first PR.
