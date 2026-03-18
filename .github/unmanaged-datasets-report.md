## ODP metadata audit findings

Detected **0** new dataset(s) not currently managed in the registry.
Audit run: 2026-03-18 14:08 UTC

<!-- unmanaged-datasets: -->

## Copilot task instruction
@copilot determine which orchestrator and registry block each dataset should live in,
build out the orchestrator to handle the dataset, add a sensible tutorial example using
the new data, and open a PR into `dev` with maintainers assigned for review.

## Required triage workflow
- Confirm each dataset is in scope for `ukpyn`.
- Choose target orchestrator/module and naming alias(es).
- Add managed mapping in `src/ukpyn/dataset_registry.py`.
- Implement or extend orchestrator coverage and tests.
- Add/update one sensible tutorial usage example.
- Open PR targeting `dev` and assign maintainers for review.

## Acceptance criteria
- [ ] Dataset classified to the correct orchestrator and registry block
- [ ] Orchestrator implementation and tests added/updated
- [ ] Tutorial example added/updated and runnable
- [ ] PR opened against `dev` with maintainers assigned

## New unmanaged datasets

## Context
The registry now includes these entries in auto-generated `UNMANAGED_DATASETS` for visibility until fully integrated.

## Breaking field changes detected

The following datasets have had fields **removed** on ODP. Orchestrator queries referencing these fields will fail.

- **dfes-network-headroom-report**
  - Removed: `bulksupplypoin` — check `src/ukpyn/orchestrators/dfes.py`
  - Added: `bulksupplypoint`

## New fields detected (non-breaking)

- **grid-and-primary-sites**: `parish`
- **ltds-table-3a-load-data-observed-transposed**: `maximum_demand_24_25_mw`, `maximum_demand_24_25_pf`
- **ukpn-curtailment-events-site-specific**: `der_name_act`