# UKPN Tutorials

Official tutorials by UK Power Networks showing how to use the ukpyn package.

## Prerequisites

1. Python 3.11+
2. ukpyn package installed (`pip install -e ".[all]"` from a local clone, or `pip install "ukpyn[all]"`)
3. UKPN API key (get yours at https://ukpowernetworks.opendatasoft.com)

## Setup

```bash
# Set your API key
export UKPN_API_KEY=your_api_key_here

# Or on Windows
set UKPN_API_KEY=your_api_key_here
```

If you use a `.env` file, add this line at repository root:

```bash
UKPN_API_KEY=your_api_key_here
```

## Kernel setup

Use the same Python environment for terminal installs and notebook execution:

```bash
python -m ipykernel install --user --name ukpyn --display-name "Python (ukpyn)"
```

In VS Code/Jupyter, select the **Python (ukpyn)** kernel before running tutorials.

## Tutorials

| # | Tutorial | Description |
|---|----------|-------------|
| 01 | [Getting Started](01-getting-started.ipynb) | Install, configure, and make your first API call |
| 02 | Fetching Data | Query datasets, filter records, pagination |
| 03 | Analysis Patterns | Common analysis workflows with pandas |
| 10 | [Powerflow Quality Control](10-powerflow-quality-control.ipynb) | LTDS-to-powerflow QC with redaction, anomalies, step changes, and balance checks |

## Running Tutorials

```bash
# Install full notebook dependencies (if not already installed)
pip install -e ".[all]"

# Start notebook server
jupyter notebook tutorials/
```

## Contributing

Tutorials are maintained by the UKPN team. For suggestions or corrections, please open an issue.
