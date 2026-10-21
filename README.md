# ukpyn

[![PyPI version](https://badge.fury.io/py/ukpyn.svg)](https://badge.fury.io/py/ukpyn)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: CC BY 4.0](https://img.shields.io/badge/License-CC%20BY%204.0-lightgrey.svg)](https://creativecommons.org/licenses/by/4.0/)

**ukpyn** is the official Python client for [UK Power Networks Open Data Portal](https://ukpowernetworks.opendatasoft.com). It provides simple, ergonomic access to distribution network data to support energy sector analysis and decarbonisation efforts.

> This README is intentionally concise for PyPI. Full guides and reference pages are published via GitHub Pages docs.

## Documentation

- **Docs site (GitHub Pages):** https://ukpn-dso.github.io/ukpyn/
- **Tutorials:** https://ukpn-dso.github.io/ukpyn/tutorials/
- **API source:** https://github.com/UKPN-DSO/ukpyn/tree/main/src/ukpyn
- **Changelog:** [CHANGELOG.md](CHANGELOG.md)

## Mission

ukpyn is built for **accessibility** to UK Power Networks open data:

- **If you are new to coding**: start with clear defaults, copy-paste examples, and tutorials.
- **If you are experienced**: use typed APIs, async workflows, exports, and low-level client control.

The package is designed to reduce friction without hiding power.

## Features

- **Simple orchestrator-based API** - Access data by category (LTDS, flexibility, GIS, powerflow, etc.)
- **Beginner-friendly defaults** - Useful defaults for common queries, then optional filtering
- **Both sync and async support** - Use whichever fits your workflow
- **Export to multiple formats** - CSV, JSON, Excel, GeoJSON, Parquet, and more
- **Jupyter notebook friendly** - Works in notebooks and scripts
- **Type hints throughout** - Full IDE support with autocompletion

## Installation

### Recommended workflow: `uv` + your preferred environment

#### 1) Create and activate an environment

Option A: conda

```bash
# Create and activate a Python 3.11 environment
conda create -n ukpyn python=3.11 -y
conda activate ukpyn
```

Option B: venv

```bash
# Create a virtual environment
python -m venv .venv

# Activate on macOS / Linux
source .venv/bin/activate

# Activate on Windows PowerShell
.venv\Scripts\Activate.ps1
```

#### 2) Install `uv`

```bash
python -m pip install --upgrade "pip>=26.2.2" "uv>=0.13.4"
```

#### 3) Install from this repository

```bash
# Clone and enter the repository (if you haven't already)
git clone https://github.com/UKPN-DSO/ukpyn.git
cd ukpyn

# Resolve dependencies and create or update the local lockfile
uv sync
```

Alternative `uv pip` installs:

```bash
# Editable install (base)
uv pip install -e .

# Editable install with dev tools
uv pip install -e ".[dev]"

# Editable install with all optional extras
uv pip install -e ".[all]"
```

Or install from PyPI with extras:

```bash
uv pip install ukpyn

uv pip install "ukpyn[dev]"

uv pip install "ukpyn[optional]"

uv pip install "ukpyn[all]"
```

Shell note: use double quotes for extras in PowerShell and bash. In Windows CMD, prefer `uv pip install -e .[all]`.

### TLS certificate maintenance

Use `pip>=26.2.2`, `uv>=0.13.4`, and `certifi>=2026.10.19`.
The certifi dependency floor also protects optional integrations that use
Requests or HTTPX; ukpyn's HTTPX2 client uses operating system certificates
by default instead. Keep the operating system trust store and any custom
`SSL_CERT_FILE` / `SSL_CERT_DIR` bundles updated independently.

For an existing environment, activate it and upgrade its tools as above, then
run `uv sync --upgrade-package certifi` for a repository checkout.
The local `uv.lock` is not committed; the minimum versions are enforced by
`pyproject.toml`, including when installing with pip. Environments created
without pip can use a trusted, updated uv installation to run
`uv pip install --python <environment-python> --upgrade "pip>=26.2.2" "uv>=0.13.4"`.
Bootstrap these upgrades using a trusted installer and an updated system or
custom CA store; do not disable TLS verification.

If using sigstore separately, upgrade it to `sigstore>=4.5.1` in its own
environment. The PyPI publishing workflows use the patched v1.14.3 publishing
action, which pins certifi 2026.10.19 and sigstore 4.5.1. That action's separate
container still pins pip 26.2, below the recommended minimum; upgrading pip on
the workflow runner does not update the container. Full compliance for that
embedded pip requires an upstream action/image update.

This repository does not define application container images. Downstream image
owners must refresh base images and system CA packages, rebuild without stale
layers (for example, `docker build --pull --no-cache .`), verify the resulting
certificate stores and package versions, and redeploy. A rebuild alone does
not prove that compromised certificates have been removed.

## Quick Start

### 1. Get an API key

1. Create an account at [UK Power Networks Open Data Portal](https://ukpowernetworks.opendatasoft.com)
2. Go to your profile and navigate to API keys
3. Generate a new key

### 2. Set up your API key

Create a `.env` file in your project root:

```bash
UKPN_API_KEY=your_api_key_here
```

Or set it as an environment variable:

```bash
# macOS / Linux
export UKPN_API_KEY=your_api_key_here

# Windows PowerShell
$env:UKPN_API_KEY="your_api_key_here"

# Windows CMD
set UKPN_API_KEY=your_api_key_here
```

### 3. Start using ukpyn

#### Beginner path (first working call)

```python
from ukpyn import ltds

# Fetch LTDS Table 3A with one filter
table_3a = ltds.get_table_3a(licence_area="EPN")
print(f"Rows returned: {len(table_3a.records)}")
```

#### Advanced path (multi-domain workflow)

```python
from ukpyn import ltds, flexibility, gis, powerflow, curtailment

# Get LTDS Table 3A (observed peak demand at primary substations)
table_3a = ltds.get_table_3a(licence_area="EPN")

# Get flexibility dispatch events
dispatches = flexibility.get_dispatches(start_date="2024-01-01")

# Get primary substation locations
substations = gis.get_primary_substations(licence_area="SPN")

# Get 132kV circuit time series
circuits = powerflow.get_circuit_timeseries(voltage="132kv", granularity="half_hourly")

# Get curtailment events
events = curtailment.get_events(start_date="2024-01-01")
```

### Command-line usage

```bash
ukpyn list domains
ukpyn list datasets --domain ltds
ukpyn fetch table_3a
ukpyn fetch dispatches --output csv
```

Dataset listings are sorted and contain each dataset ID once, including when
filtering by domain. `fetch` accepts friendly registry keys or full dataset IDs
and prints a summary by default. With `--output`, it writes
`<dataset-id>.<format>` in the current directory. Invalid datasets, formats, or
domains return a non-zero exit code.

## Learning Paths

> **Note:** To run the tutorials, install the full set of dependencies with `uv pip install "ukpyn[all]"` (or `pip install "ukpyn[all]"`).

- **New to Python or APIs?** Start with [01-getting-started](tutorials/01-getting-started.ipynb), then [02-fetching-data](tutorials/02-fetching-data.ipynb).
- **Comfortable with Python?** Use [03-analysis-patterns](tutorials/03-analysis-patterns.ipynb) and the docs site.
- **Power users?** Use async APIs and low-level client patterns documented in GitHub Pages docs.

## Supported Domains

ukpyn provides themed orchestrators for each major data category on the UK Power Networks Open Data Portal:

- **`ltds`** — Long Term Development Statement tables, published twice yearly: circuit data, transformer specs, observed and forecast demand, fault levels, distributed generation, connection queues, operational restrictions, infrastructure projects, and CIM network models.
- **`dfes`** — Distribution Future Energy Scenarios: network capacity headroom by scenario and local authority, and peak demand forecasts.
- **`dnoa`** — Distribution Network Options Assessment: reinforcement and flexibility option assessments and low-voltage DNOA results.
- **`network`** — Aggregated network performance metrics: annual statistics, network losses, power quality indicators, and IIS interruption data.
- **`flexibility`** — Flexibility market records: dispatch events (when services were called) and tender procurement data.
- **`curtailment`** — Site-specific curtailment event records: when generation or demand was curtailed by network instruction.
- **`ders`** (alias: `resources`) — Distributed Energy Resources: embedded capacity register (≥ 1 MW and < 1 MW), large demand customer list, and embedded power stations.
- **`gis`** — Geospatial / infrastructure assets: substation sites and coverage areas, overhead lines by voltage, poles and towers, licence boundaries, and IDNO areas.
- **`powerflow`** — Half-hourly and monthly operational time series for circuits and transformers at 132 kV, 33 kV (split by EPN/SPN), and primary (33/11 kV).

Additional dataset groups are accessible directly via the registry (no dedicated orchestrator required):

- **Equipment** — Grid, primary, secondary, and super-grid transformer specifications and secondary site utilisation.
- **Connections** — Connection queue insights, GSP-level status and narratives, wide area planning status, and modification applications.
- **Operations** — Real-time constraint meter readings, constraint breach history, EHV outages, live faults, and rota disconnection schedules.
- **Sensitivity** — Network sensitivity factors for import and export.
- **Smart Meter & LCT** — Smart meter consumption at substation and LV feeder level, installation volumes, and low carbon technology deployment by LSOA and secondary substation.
- **Profiles** — Standard electricity demand profiles, technology-specific generation profiles, and data centre demand profiles.
- **Appendix G** — Active Network Management (ANM) configuration: settings, aggregated views, project progression, and site-specific conditions.
- **Reference** — Supporting metadata: local authority boundaries, business glossary, data roadmap, data maturity scores, and EV chargepoint register.

See full API usage patterns in the docs site: https://ukpn-dso.github.io/ukpyn/

## Licence Areas

UK Power Networks operates three distribution licence areas:

- **EPN** - Eastern Power Networks (East of England)
- **LPN** - London Power Networks (Greater London)
- **SPN** - South Eastern Power Networks (South East England)

Many datasets can be filtered by licence area using the `licence_area` parameter. Use the abbreviation EPN/SPN/LPN

## Contributing

We welcome contributions from beginners and experienced developers.

See [Contributing Guidelines](CONTRIBUTING.md) for full details.

- **Found a bug or have a question?** Please [open an issue](https://github.com/UKPN-DSO/ukpyn/issues) — it helps us improve ukpyn for everyone.
- **Have ideas for better utilities?** Our `ukpyn.utils` module provides time series, statistical, and powerflow helpers — if you think we're missing something or could improve what's there, [tell us in an issue](https://github.com/UKPN-DSO/ukpyn/issues). Community input shapes what we build next.
- Open a pull request with focused changes and tests.
- If you are new to open source, small documentation improvements are a great first contribution.

### Development Setup

```bash
# Clone the repository
git clone https://github.com/UKPN-DSO/ukpyn.git
cd ukpyn

# Install development dependencies
pip install ukpyn[dev]

# (Optional) install full optional feature set as well
pip install ukpyn[optional]

# Install dependencies with uv
uv sync

# Run tests
pytest

# Run linter
ruff check .

# Fix linting issues
ruff check --fix .

# Run pre-commit hooks
pre-commit run --all-files
```

### Git Workflow

- `main` - Protected branch for releases only
- `dev` - Integration branch for development
- `feature/*` - Feature branches for all work

### Automated Registry Triage

- Daily registry audit runs via GitHub Actions to detect new unmanaged ODP datasets.
- When new unmanaged datasets are found, automation opens a triage issue assigned to `copilot`.
- The issue includes standardized tasks: decide orchestrator/registry placement, implement support, add a tutorial example, and open a PR into `dev` with maintainers assigned for review.

## Security

Please see [SECURITY.md](SECURITY.md) for our security policy.

**Important**: Never commit API keys or `.env` files to version control.

## Acknowledgements

The vision for ukpyn as an accessible Python interface for UK Power Networks open data, along with the value proposition and initial scope, was developed with consultation from **Dr Daniel Donaldson**, Electronic, Electrical and Systems Engineering, University of Birmingham, Birmingham, United Kingdom.

## License

This project is licensed under the Creative Commons Attribution 4.0 International License (CC BY 4.0) - see the [LICENSE](LICENSE) file for details.

Redistributions and derivative works should retain attribution notices, including the
[NOTICE](NOTICE) file, to acknowledge UK Power Networks Distribution System Operator.

## Links

- [UK Power Networks Open Data Portal](https://ukpowernetworks.opendatasoft.com)
- [GitHub Repository](https://github.com/UKPN-DSO/ukpyn)
- [PyPI Package](https://pypi.org/project/ukpyn/)
- [Changelog](CHANGELOG.md)
