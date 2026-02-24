# Tutorials

Official notebooks live in the `tutorials/` directory and are rendered into the
website.

If you are new, work through them in numeric order. Each notebook builds on the
previous one.

## Before running notebooks

1. Install `ukpyn` in your active environment.
2. Set `UKPN_API_KEY`.
3. Select the same environment kernel in VS Code/Jupyter.
4. Run each notebook from the first cell to the last cell.

If cells fail with import errors, revisit [Getting Started](getting-started.md).

## Notebook learning path

### 01-getting-started.ipynb

- Audience: complete beginners
- Outcome: run first API calls and inspect response objects
- Focus: setup confidence and basic query flow

### 02-fetching-data.ipynb

- Audience: beginners who completed notebook 01
- Outcome: fetch domain data with practical filters
- Focus: requesting and comparing subsets of records

### 03-analysis-patterns.ipynb

- Audience: users comfortable with basic Python data handling
- Outcome: repeatable analysis patterns for ODP records
- Focus: lightweight transformations and interpretation

### 04-ltds-network-planning.ipynb

- Audience: users exploring planning datasets
- Outcome: LTDS-oriented analysis workflows
- Focus: planning-centric tables and network context

### 05-flexibility-markets.ipynb

- Audience: users interested in flexibility operations
- Outcome: inspect dispatch and related market activity
- Focus: flexibility domain queries and trend exploration

### 06-powerflow-timeseries.ipynb

- Audience: users needing time-series network insights
- Outcome: run circuit/transformer style time-series queries
- Focus: granularity, period selection, and signal interpretation

### 07-curtailment-events.ipynb

- Audience: users studying constraint and curtailment behavior
- Outcome: fetch and analyse event-based datasets
- Focus: event windows, frequency, and context

### 08-geospatial-data.ipynb

- Audience: users who want spatial/network geography workflows
- Outcome: explore GIS-style datasets and map-ready records
- Focus: substations, lines, and location-driven analysis

### 09-pandapower-cim-import.ipynb

- Audience: advanced users integrating with power systems tooling
- Outcome: import and work with CIM/pandapower-related assets
- Focus: interoperability and deeper modelling workflows

### 10-powerflow-quality-control.ipynb

- Audience: users validating operational powerflow data quality
- Outcome: cross-reference LTDS assets to monthly powerflow and run QC checks
- Focus: redaction detection, month selection, gap/anomaly/step diagnostics, and line-vs-transformer balance

## Troubleshooting tips for beginners

- If a notebook imports fail, confirm kernel selection matches your install env.
- If API calls fail, verify `UKPN_API_KEY` in the same environment/session.
- If outputs differ from expectations, rerun all cells in order to reset state.

## Notebook files in this docs section

Notebook pages available in the site navigation:

- `01-getting-started.ipynb`
- `02-fetching-data.ipynb`
- `03-analysis-patterns.ipynb`
- `04-ltds-network-planning.ipynb`
- `05-flexibility-markets.ipynb`
- `06-powerflow-timeseries.ipynb`
- `07-curtailment-events.ipynb`
- `08-geospatial-data.ipynb`
- `09-pandapower-cim-import.ipynb`
- `10-powerflow-quality-control.ipynb`

Use these notebooks as end-to-end, reproducible examples that complement the
API guides in [Orchestrators](orchestrators.md).
