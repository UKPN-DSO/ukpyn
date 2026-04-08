# Orchestrators

Orchestrators are beginner-friendly entry points grouped by data domain.

Instead of memorising dataset IDs, you import a domain module (for example
`ltds` or `gis`) and call a named method.

## Why this matters for beginners

- Lower cognitive load: call descriptive functions rather than raw endpoint URLs
- Fewer mistakes: common parameters and response structures are consistent
- Easier learning curve: start with one domain, then expand gradually

## Available orchestrators

- `ltds` - Long Term Development Statement tables and related assets
- `dfes` - Distributed Future Energy Scenarios data access
- `dnoa` - Distribution Network Options Assessment data access
- `network` - Network-level circuit and demand profile datasets
- `flexibility` - Flexibility dispatches and curtailment-related records
- `curtailment` - Curtailment event datasets
- `ders` (`resources` alias) - Distributed energy resource / embedded capacity data
- `gis` - Geospatial assets (substations, lines, poles, sites)
- `powerflow` - Time-series network loading and transformer/circuit flows

## First orchestrator call (recommended)

```python
from ukpyn import ltds

table_3a = ltds.get_table_3a(licence_area="EPN")
print(table_3a.total_count)
print(len(table_3a.records))
```

## Domain examples (copy/paste)

```python
from ukpyn import flexibility, gis, powerflow, network, dfes, dnoa, curtailment, ders

dispatches = flexibility.get_dispatches(start_date="2024-01-01")
substations = gis.get_primary_substations(licence_area="SPN")
flows = powerflow.get_circuit_timeseries(voltage="132kv", granularity="half_hourly")
profiles = network.get_demand_profiles()
headroom = dfes.get_headroom()
assessment = dnoa.get_assessment()
events = curtailment.get_events(start_date="2024-01-01")
embedded = ders.get_embedded_capacity()
```

## Sync and async patterns

For most beginners, start with sync functions like `get_table_3a()` or
`get_dispatches()`.

When you need higher throughput or integration into async applications, use each
orchestrator's `get_async(...)` method.

## Choosing the right orchestrator

- Planning and LTDS tables → `ltds`
- Scenario-style capacity/headroom views → `dfes`
- Options assessment and planning decisions → `dnoa`
- Operational flexibility activity → `flexibility` or `curtailment`
- Asset location and spatial workflows → `gis`
- Time-series loading and flows → `powerflow`
- Embedded generation / demand resources → `ders`

## GIS orchestrator — geometry handling

UKPN's ODP stores geometry in several different field formats across datasets.
ukpyn normalises all of these so you don't have to.

### Normalised geometry on every record

Every `Record` object exposes a `.geometry` property that returns a standard
GeoJSON geometry dict (or `None` if the record has no spatial data):

```python
from ukpyn import gis

result = gis.get_primary_substations(licence_area="EPN", limit=5)
for rec in result.records:
    print(rec.geometry)  # {"type": "Point", "coordinates": [-0.12, 51.51]}
```

The property resolves geometry from whichever raw field the ODP happens to use,
in priority order: `geo_shape` → `spatial_coordinates` → `geo_point_2d` →
`geo_point` → `geopoint`. Point dicts like `{"lat": 51.5, "lon": -0.1}` are
automatically converted to GeoJSON `Point` format.

### Feature unwrapping

Some datasets (e.g. poles) return `geo_shape` as a full GeoJSON Feature:
```json
{"type": "Feature", "geometry": {"type": "Point", "coordinates": [...]}, "properties": {}}
```
ukpyn automatically unwraps this to the bare geometry dict during parsing.

### NaN sanitisation

The ODP occasionally returns `NaN` for missing values in fields like
`grid_site` or `grid_site_floc`. Python's `float('nan')` is invalid JSON and
will break PostgreSQL, `json.dumps()`, and any serialisation step. ukpyn
replaces `NaN` and `Infinity` with `None` on every record automatically.

### Coordinate dimensions (2D vs 3D)

The GeoJSON export path returns 3D geometries (with Z elevation), while the
records API returns 2D points. Use the `dimensions` parameter to normalise:

```python
# Strip Z values for a flat 2D map
geojson_bytes = gis.export_geojson("hv_overhead_lines", dimensions="2d")

# Ensure all coordinates have Z (defaults to 0.0 where missing)
geojson_bytes = gis.export_geojson("hv_overhead_lines", dimensions="3d")

# Pass through as-is (default)
geojson_bytes = gis.export_geojson("hv_overhead_lines", dimensions="raw")
```

## Practical beginner workflow

1. Pick one orchestrator tied to your question.
2. Run one minimal call with no optional parameters.
3. Print `total_count` and `len(records)`.
4. Add one filter (for example `licence_area`).
5. Compare outputs and iterate.

## Next steps

- Follow [Tutorials](tutorials.md) for full notebook workflows
- Revisit [Getting Started](getting-started.md) if environment or key setup fails
