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

## Practical beginner workflow

1. Pick one orchestrator tied to your question.
2. Run one minimal call with no optional parameters.
3. Print `total_count` and `len(records)`.
4. Add one filter (for example `licence_area`).
5. Compare outputs and iterate.

## Next steps

- Follow [Tutorials](tutorials.md) for full notebook workflows
- Revisit [Getting Started](getting-started.md) if environment or key setup fails
