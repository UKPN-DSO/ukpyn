# CIM × ODP Infrastructure Map — Project Brief

Build a unified map of UK Power Networks infrastructure by fusing the **LTDS CIM** publication (electrical topology + parameters) with the **UKPN Open Data Portal** (geographic coordinates + secondary/LV layers).

## Goal

A single artefact (interactive web map + topology graph) that lets a user click any substation and see:

- Where it is on a map (from ODP).
- What it connects to electrically (from CIM).
- Voltage level, thermal ratings, connected generation/load (from CIM).
- Downstream secondary sites and LV network (from ODP).

## Inputs

### 1. CIM file — `cim-example.xml`

- **Format:** RDF/XML, IEC 61970 CIM100 + ENTSO-E European + Ofgem LTDS GB extensions.
- **Profile:** LTDS Equipment (EQ) profile, version 6.0.
- **Scope:** EPN licence area, snapshot 2025-10-03 (~95k objects, 50 MB).
- **Producer:** PowerFactory 2025 SP4.

Namespaces:

| Prefix | URI | Purpose |
|---|---|---|
| `cim:` | `http://iec.ch/TC57/CIM100#` | Core CIM classes |
| `eu:` | `http://iec.ch/TC57/CIM100-European#` | ENTSO-E profile |
| `gb:` | `http://ofgem.gov.uk/ns/CIM/LTDS/Extensions#` | Ofgem LTDS extensions (rating series) |
| `nc:` | `https://cim4.eu/ns/nc#` | CIM4EU extensions |

Key class inventory:

| Domain | Classes | Count |
|---|---|---|
| Topology | `Terminal`, `ConnectivityNode`, `Substation`, `VoltageLevel`, `BusbarSection` | 27.8k / 9.9k / 863 / 2.7k / 2.6k |
| Lines | `ACLineSegment`, `Line`, `EquivalentBranch` | 2.9k / 3.0k / 536 |
| Transformers | `PowerTransformer`, `PowerTransformerEnd`, `RatioTapChanger` | 1.2k / 2.5k / 1.2k |
| Switchgear | `Breaker`, `Disconnector` | 4.6k / 2.2k |
| Generation | `SynchronousMachine`, `PhotoVoltaicUnit`, `BatteryUnit`, `WindGeneratingUnit`, `PowerElectronicsConnection` | 76 / 129 / 31 / 13 / 167 |
| Load | `EnergyConsumer` | 1.8k |
| Ratings | `CurrentLimit*`, `ApparentPowerLimit*` series | ~21k data points |

**Critical gap:** the EQ profile contains **no geographic coordinates** (no `Location`, `PositionPoint`, `xPosition`). Geo lives in a separate **GL (Geographical Location) profile** that may or may not be published — confirm with UKPN LTDS team.

### 2. UKPN ODP via `ukpyn`

The Python client at `https://github.com/UKPN-DSO/ukpyn` wraps the ODP. Relevant orchestrator: `ukpyn.orchestrators.gis`.

Useful datasets:

- `grid-and-primary-sites` — primary substation points (lat/lon, site name, licence area).
- `ukpn-secondary-sites` — secondary substation points.
- `hv_overhead_lines`, `lv_overhead_lines` — line geometries.
- `hv_poles`, `lv_poles` — pole points.

```python
from ukpyn.orchestrators import gis
primaries = gis.get_primary_substations(licence_area="EPN", limit=2000)
hv_lines  = gis.export_geojson("hv_overhead_lines", dimensions="2d")
```

## Join strategy

CIM has no `mRID` that ODP also carries, so the join is **by substation name**:

- CIM: `cim:Substation` → `cim:IdentifiedObject.name` (e.g., "South Witham Primary").
- ODP: `grid-and-primary-sites.sitefunctionallocation`.

Expect noise: trailing "Primary", casing, abbreviations, "Compact Grid" suffixes. Build a normaliser + fuzzy matcher (`rapidfuzz`) and surface unmatched names for manual reconciliation.

If a CIM **GL profile** is available later, switch the join to `mRID` and bin the fuzzy matcher.

## Pipeline

1. **Parse CIM** — `rdflib` (general) or `cimpy` (CIM-aware). Build a `networkx.MultiGraph` keyed on `ConnectivityNode` ↔ `Terminal` ↔ equipment.
2. **Lift to substation graph** — aggregate equipment per `Substation`; keep inter-substation `Line`/`ACLineSegment` edges with voltage + thermal rating attributes.
3. **Enrich with ODP geometry** — name-match substations onto `grid-and-primary-sites`; attach lat/lon. Pull `hv_overhead_lines` GeoJSON for line routes.
4. **Layer LV** — overlay `ukpn-secondary-sites` + `lv_*` datasets (CIM has no LV equivalent).
5. **Render**
   - **Geo map:** `folium` or `pydeck` — substations as points, lines styled by voltage and rating headroom.
   - **Topology graph:** `pyvis` / `cytoscape.js` — electrical schematic clickable from the map.

## Risks / open questions

- **EPN only.** SPN and LPN are separate CIM publications. Decide whether the map is national or EPN-only for v1.
- **Name-match fidelity unknown.** Spike needed before committing to the approach (see below).
- **No LV in CIM.** LTDS is 33 kV+. LV layers are ODP-only — no electrical model behind them.
- **GL profile availability.** Ask UKPN whether they publish a GL companion file for LTDS CIM; would replace the fuzzy join with `mRID`.
- **Snapshot drift.** CIM is dated 2025-10-03; ODP updates continuously. Document the snapshot date on the map.

## Spike (do this first)

Before building anything, validate the join is viable:

1. Parse the EQ file, extract all 863 `cim:Substation` names.
2. `gis.get_primary_substations(licence_area="EPN", limit=2000)`.
3. Fuzzy-match (`rapidfuzz.process.extractOne`, threshold ~85).
4. Report:
   - Match rate at threshold.
   - List of unmatched CIM substations (CIM has names ODP doesn't).
   - List of unmatched ODP sites (likely secondary or non-LTDS).

A match rate >90% → name-join approach is fine. <70% → chase the GL profile from UKPN before continuing.

## Tech stack

- Python 3.11+
- `rdflib` or `cimpy` for CIM parsing
- `networkx` for topology
- `ukpyn` for ODP access
- `geopandas` + `shapely` for spatial joins
- `folium` / `pydeck` / `kepler.gl` for the map
- `pyvis` / `cytoscape.js` for the schematic

## Repo layout (suggested)

```
cim-odp-map/
├── data/
│   ├── raw/                  # CIM XML files (gitignored, large)
│   └── derived/              # parquet/geojson outputs
├── notebooks/
│   ├── 01-cim-explore.ipynb  # class inventory, sanity checks
│   ├── 02-join-spike.ipynb   # name-match feasibility
│   └── 03-map-prototype.ipynb
├── src/
│   ├── cim_parse.py          # rdflib/cimpy → networkx
│   ├── odp_fetch.py          # ukpyn wrappers
│   ├── join.py               # name normalisation + fuzzy match
│   └── render.py             # folium/pydeck map builders
├── pyproject.toml
└── README.md
```
