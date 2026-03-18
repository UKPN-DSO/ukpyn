"""Synchronize unmanaged ODP datasets into the registry file.

This module fetches ODP metadata, compares dataset IDs against managed
registry mappings, updates an auto-generated unmanaged section in
``src/ukpyn/dataset_registry.py``, and emits triage report artifacts.
"""

from __future__ import annotations

import argparse
import ast
import asyncio
import csv
import json
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from urllib.request import urlopen

DEFAULT_METADATA_URL = (
    "https://ukpowernetworks.opendatasoft.com/explore/dataset/"
    "domain-dataset0/download/?format=csv&timezone=Europe%2FLondon&lang=en"
)

BEGIN_MARKER = "# BEGIN AUTO-GENERATED UNMANAGED DATASETS"
END_MARKER = "# END AUTO-GENERATED UNMANAGED DATASETS"
NETWORK_DATASETS_HEADER = "# NETWORK_DATASETS (Legacy - backward compatibility)"

DEFAULT_SCHEMA_PATH = Path("src/ukpyn/field_schemas.json")


@dataclass(frozen=True)
class FieldChange:
    """Detected schema change for a single dataset."""

    dataset_id: str
    added: list[str]
    removed: list[str]


@dataclass(frozen=True)
class SyncResult:
    """Result of unmanaged dataset synchronization."""

    changed: bool
    new_dataset_ids: list[str]
    unmanaged_dataset_ids: list[str]
    field_changes: list[FieldChange] = ()

    def __post_init__(self) -> None:
        # frozen dataclass workaround: coerce tuple default to list
        if isinstance(self.field_changes, tuple):
            object.__setattr__(self, "field_changes", list(self.field_changes))


def fetch_metadata_csv(url: str) -> str:
    """Fetch metadata CSV from ODP."""
    with urlopen(url, timeout=60) as response:  # noqa: S310 - fixed trusted URL
        return response.read().decode("utf-8-sig")


def parse_metadata_rows(csv_text: str) -> dict[str, str]:
    """Parse metadata CSV into dataset_id -> title mapping."""
    reader = csv.DictReader(csv_text.splitlines())
    records: dict[str, str] = {}
    for row in reader:
        dataset_id = (row.get("datasetid") or "").strip()
        if not dataset_id:
            continue
        title = (row.get("default.title") or "").strip()
        records.setdefault(dataset_id, title)
    return records


def _extract_literal_string_dict(node: ast.AST) -> dict[str, str]:
    if not isinstance(node, ast.Dict):
        return {}

    result: dict[str, str] = {}
    for key_node, value_node in zip(node.keys, node.values, strict=True):
        if not isinstance(key_node, ast.Constant) or not isinstance(
            key_node.value, str
        ):
            continue
        if not isinstance(value_node, ast.Constant) or not isinstance(
            value_node.value, str
        ):
            continue
        result[key_node.value] = value_node.value
    return result


def extract_registry_maps(registry_source: str) -> dict[str, dict[str, str]]:
    """Extract literal *_DATASETS maps from registry source."""
    tree = ast.parse(registry_source)
    maps: dict[str, dict[str, str]] = {}

    for node in tree.body:
        target_name: str | None = None
        value_node: ast.AST | None = None

        if isinstance(node, ast.Assign) and len(node.targets) == 1:
            target = node.targets[0]
            if isinstance(target, ast.Name):
                target_name = target.id
                value_node = node.value
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            target_name = node.target.id
            value_node = node.value

        if (
            not target_name
            or not target_name.endswith("_DATASETS")
            or value_node is None
        ):
            continue

        parsed = _extract_literal_string_dict(value_node)
        if parsed:
            maps[target_name] = parsed

    return maps


def extract_managed_and_unmanaged_ids(
    registry_source: str,
) -> tuple[set[str], set[str]]:
    """Return managed and unmanaged dataset IDs from registry source."""
    maps = extract_registry_maps(registry_source)

    unmanaged_ids = set(maps.get("UNMANAGED_DATASETS", {}).values())
    managed_ids: set[str] = set()
    for name, values in maps.items():
        if name in {"UNMANAGED_DATASETS", "ALL_DATASETS"}:
            continue
        managed_ids.update(values.values())

    return managed_ids, unmanaged_ids


def _safe_comment(text: str) -> str:
    cleaned = " ".join(text.replace("#", " ").split())
    return cleaned or "Title unavailable"


def render_unmanaged_section(dataset_ids: Iterable[str], titles: dict[str, str]) -> str:
    """Render auto-generated unmanaged datasets section."""
    generated_date = date.today().isoformat()
    rows = sorted(set(dataset_ids))

    lines = [
        BEGIN_MARKER,
        "# =============================================================================",
        "# Unmanaged ODP Datasets",
        "# Auto-generated from ODP metadata (domain-dataset0)",
        f"# Last sync: {generated_date}",
        "# =============================================================================",
        "UNMANAGED_DATASETS: dict[str, str] = {",
    ]

    for dataset_id in rows:
        title = _safe_comment(titles.get(dataset_id, ""))
        lines.append(f"    # {title}")
        lines.append(f'    "{dataset_id}": "{dataset_id}",')

    if rows:
        lines.extend(["}", END_MARKER])
    else:
        # Collapse to single-line empty dict to satisfy ruff/linter
        lines[-1] = "UNMANAGED_DATASETS: dict[str, str] = {}"
        lines.append(END_MARKER)
    return "\n".join(lines)


def inject_unmanaged_section(registry_source: str, section: str) -> str:
    """Insert or replace unmanaged section in registry source."""
    if BEGIN_MARKER in registry_source and END_MARKER in registry_source:
        start_index = registry_source.index(BEGIN_MARKER)
        end_index = registry_source.index(END_MARKER) + len(END_MARKER)
        return registry_source[:start_index] + section + registry_source[end_index:]

    anchor = registry_source.find(NETWORK_DATASETS_HEADER)
    if anchor == -1:
        raise ValueError("Could not find insertion point for unmanaged section")

    prefix = registry_source[:anchor].rstrip("\n")
    suffix = registry_source[anchor:]
    return f"{prefix}\n\n{section}\n\n{suffix}"


def suggest_update_targets(dataset_id: str) -> list[str]:
    """Suggest likely files for triaging a dataset integration."""
    checks: list[tuple[tuple[str, ...], list[str]]] = [
        (
            ("ltds",),
            [
                "src/ukpyn/orchestrators/ltds.py",
                "tutorials/04-ltds-network-planning.ipynb",
            ],
        ),
        (("dfes",), ["src/ukpyn/orchestrators/dfes.py"]),
        (("dnoa",), ["src/ukpyn/orchestrators/dnoa.py"]),
        (
            ("flexibility",),
            [
                "src/ukpyn/orchestrators/flexibility.py",
                "tutorials/05-flexibility-markets.ipynb",
            ],
        ),
        (
            ("curtailment",),
            [
                "src/ukpyn/orchestrators/curtailment.py",
                "tutorials/07-curtailment-events.ipynb",
            ],
        ),
        (
            (
                "power-flow",
                "powerflow",
                "circuit-operational",
                "transformer-operational",
            ),
            [
                "src/ukpyn/orchestrators/powerflow.py",
                "tutorials/06-powerflow-timeseries.ipynb",
            ],
        ),
        (
            (
                "postcode",
                "sites",
                "boundaries",
                "overhead",
                "poles",
                "grid-supply",
                "shapefile",
            ),
            ["src/ukpyn/orchestrators/gis.py", "tutorials/08-geospatial-data.ipynb"],
        ),
        (
            ("capacity-register", "large-demand", "embedded"),
            ["src/ukpyn/orchestrators/ders.py"],
        ),
        (
            ("constraints", "fault", "outages", "rota"),
            ["src/ukpyn/orchestrators/network.py"],
        ),
    ]

    lowered = dataset_id.lower()
    for keywords, targets in checks:
        if any(keyword in lowered for keyword in keywords):
            return targets

    return [
        "src/ukpyn/orchestrators/registry.py",
        "src/ukpyn/orchestrators/base.py",
        "README.md",
    ]


# =============================================================================
# Field schema audit
# =============================================================================


def load_field_schemas(schema_path: Path) -> dict[str, list[str]]:
    """Load the field schema snapshot from JSON."""
    if not schema_path.exists():
        return {}
    text = schema_path.read_text(encoding="utf-8")
    return json.loads(text)


async def fetch_live_schemas(dataset_ids: Iterable[str]) -> dict[str, list[str]]:
    """Fetch field names for each dataset ID from the live ODP API."""
    from .client import UKPNClient

    schemas: dict[str, list[str]] = {}
    unique_ids = sorted(set(dataset_ids))

    async with UKPNClient() as client:
        for dataset_id in unique_ids:
            try:
                dataset = await client.get_dataset(dataset_id)
                schemas[dataset_id] = sorted(dataset.field_ids)
            except Exception:
                # Dataset may have been removed from ODP; skip it
                schemas[dataset_id] = []

    return dict(sorted(schemas.items()))


def diff_field_schemas(
    stored: dict[str, list[str]],
    live: dict[str, list[str]],
) -> list[FieldChange]:
    """Compare stored snapshot against live schemas and return changes.

    Only datasets present in *both* dicts are compared; new or removed
    datasets are outside the scope of this function (handled by the
    unmanaged-dataset sync).
    """
    changes: list[FieldChange] = []
    for dataset_id in sorted(stored):
        if dataset_id not in live:
            continue
        stored_set = set(stored[dataset_id])
        live_set = set(live[dataset_id])
        added = sorted(live_set - stored_set)
        removed = sorted(stored_set - live_set)
        if added or removed:
            changes.append(
                FieldChange(dataset_id=dataset_id, added=added, removed=removed)
            )
    return changes


def update_field_snapshot(
    schema_path: Path,
    live: dict[str, list[str]],
    stored: dict[str, list[str]],
) -> None:
    """Merge live fields into the stored snapshot and write back."""
    merged = {**stored, **{k: sorted(v) for k, v in live.items()}}
    schema_path.write_text(
        json.dumps(dict(sorted(merged.items())), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def render_field_changes_report(changes: list[FieldChange]) -> str:
    """Render a markdown section describing field schema changes."""
    if not changes:
        return ""

    breaking = [c for c in changes if c.removed]
    additive = [c for c in changes if c.added and not c.removed]

    lines: list[str] = []
    if breaking:
        lines.append("")
        lines.append("## Breaking field changes detected")
        lines.append("")
        lines.append(
            "The following datasets have had fields **removed** on ODP. "
            "Orchestrator queries referencing these fields will fail."
        )
        lines.append("")
        for change in breaking:
            lines.append(f"- **{change.dataset_id}**")
            for field in change.removed:
                targets = suggest_update_targets(change.dataset_id)
                target_str = ", ".join(f"`{t}`" for t in targets)
                lines.append(f"  - Removed: `{field}` — check {target_str}")
            if change.added:
                lines.append(f"  - Added: {', '.join(f'`{f}`' for f in change.added)}")

    if additive:
        lines.append("")
        lines.append("## New fields detected (non-breaking)")
        lines.append("")
        for change in additive:
            fields_str = ", ".join(f"`{f}`" for f in change.added)
            lines.append(f"- **{change.dataset_id}**: {fields_str}")

    return "\n".join(lines)


def build_issue_report(new_dataset_ids: list[str], titles: dict[str, str]) -> str:
    """Build markdown issue body with findings and triage instructions."""
    now = datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC")
    marker = ",".join(new_dataset_ids)

    lines = [
        "## ODP metadata audit findings",
        "",
        f"Detected **{len(new_dataset_ids)}** new dataset(s) not currently managed in the registry.",
        f"Audit run: {now}",
        "",
        f"<!-- unmanaged-datasets:{marker} -->",
        "",
        "## Copilot task instruction",
        "@copilot determine which orchestrator and registry block each dataset should live in,",
        "build out the orchestrator to handle the dataset, add a sensible tutorial example using",
        "the new data, and open a PR into `dev` with maintainers assigned for review.",
        "",
        "## Required triage workflow",
        "- Confirm each dataset is in scope for `ukpyn`.",
        "- Choose target orchestrator/module and naming alias(es).",
        "- Add managed mapping in `src/ukpyn/dataset_registry.py`.",
        "- Implement or extend orchestrator coverage and tests.",
        "- Add/update one sensible tutorial usage example.",
        "- Open PR targeting `dev` and assign maintainers for review.",
        "",
        "## Acceptance criteria",
        "- [ ] Dataset classified to the correct orchestrator and registry block",
        "- [ ] Orchestrator implementation and tests added/updated",
        "- [ ] Tutorial example added/updated and runnable",
        "- [ ] PR opened against `dev` with maintainers assigned",
        "",
        "## New unmanaged datasets",
    ]

    for dataset_id in new_dataset_ids:
        title = titles.get(dataset_id) or "Title unavailable"
        lines.append(f"- **{dataset_id}** — {title}")
        lines.append("  - Suggested update targets:")
        for target in suggest_update_targets(dataset_id):
            lines.append(f"    - `{target}`")

    lines.append("")
    lines.append("## Context")
    lines.append(
        "The registry now includes these entries in auto-generated "
        "`UNMANAGED_DATASETS` for visibility until fully integrated."
    )

    return "\n".join(lines)


def build_full_report(
    new_dataset_ids: list[str],
    titles: dict[str, str],
    field_changes: list[FieldChange] | None = None,
) -> str:
    """Build combined issue report with dataset + field change sections."""
    report = build_issue_report(new_dataset_ids, titles)
    if field_changes:
        report += "\n" + render_field_changes_report(field_changes)
    return report


def synchronize_registry(
    registry_path: Path,
    metadata_url: str,
    report_path: Path,
    json_output_path: Path,
    schema_path: Path | None = None,
) -> SyncResult:
    """Run unmanaged-dataset synchronization end-to-end.

    When *schema_path* is provided the function also performs a field
    schema audit: it fetches live field lists from the ODP API, diffs
    against the stored snapshot, reports changes, and updates the
    snapshot in-place.
    """
    registry_source = registry_path.read_text(encoding="utf-8")
    metadata_text = fetch_metadata_csv(metadata_url)
    metadata_titles = parse_metadata_rows(metadata_text)

    managed_ids, unmanaged_ids = extract_managed_and_unmanaged_ids(registry_source)
    known_ids = managed_ids | unmanaged_ids
    metadata_ids = set(metadata_titles)
    new_ids = sorted(metadata_ids - known_ids)

    merged_unmanaged = sorted(unmanaged_ids | set(new_ids))
    new_section = render_unmanaged_section(merged_unmanaged, metadata_titles)
    updated_source = inject_unmanaged_section(registry_source, new_section)

    changed = updated_source != registry_source
    if changed:
        registry_path.write_text(updated_source, encoding="utf-8")

    # --- Field schema audit ---
    field_changes: list[FieldChange] = []
    if schema_path is not None:
        stored = load_field_schemas(schema_path)
        if stored:
            print(f"Fetching live field schemas for {len(stored)} datasets...")
            live_schemas = asyncio.run(fetch_live_schemas(stored.keys()))
            field_changes = diff_field_schemas(stored, live_schemas)
            if field_changes:
                update_field_snapshot(schema_path, live_schemas, stored)
                print(f"Schema snapshot updated: {schema_path}")
            else:
                print("No field schema changes detected.")
        else:
            print(f"WARNING: {schema_path} is empty or missing, skipping field audit.")

    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(
        build_full_report(new_ids, metadata_titles, field_changes),
        encoding="utf-8",
    )

    payload = {
        "new_dataset_ids": new_ids,
        "new_count": len(new_ids),
        "registry_changed": changed,
        "field_changes": [
            {
                "dataset_id": c.dataset_id,
                "added": c.added,
                "removed": c.removed,
            }
            for c in field_changes
        ],
        "breaking_field_changes": sum(1 for c in field_changes if c.removed),
    }
    json_output_path.parent.mkdir(parents=True, exist_ok=True)
    json_output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    return SyncResult(
        changed=changed,
        new_dataset_ids=new_ids,
        unmanaged_dataset_ids=merged_unmanaged,
        field_changes=field_changes,
    )


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--registry-path",
        type=Path,
        default=Path("src/ukpyn/dataset_registry.py"),
        help="Path to registry.py",
    )
    parser.add_argument(
        "--metadata-url",
        default=DEFAULT_METADATA_URL,
        help="ODP metadata CSV URL",
    )
    parser.add_argument(
        "--report-path",
        type=Path,
        default=Path(".github/unmanaged-datasets-report.md"),
        help="Output markdown report path",
    )
    parser.add_argument(
        "--json-output-path",
        type=Path,
        default=Path(".github/unmanaged-datasets.json"),
        help="Output JSON summary path",
    )
    parser.add_argument(
        "--schema-path",
        type=Path,
        default=DEFAULT_SCHEMA_PATH,
        help=f"Path to field_schemas.json snapshot (default: {DEFAULT_SCHEMA_PATH})",
    )
    parser.add_argument(
        "--skip-field-audit",
        action="store_true",
        default=False,
        help="Skip the live field schema audit",
    )
    return parser.parse_args()


def main() -> int:
    """CLI entry point."""
    args = parse_args()

    schema_path = None if args.skip_field_audit else args.schema_path

    result = synchronize_registry(
        registry_path=args.registry_path,
        metadata_url=args.metadata_url,
        report_path=args.report_path,
        json_output_path=args.json_output_path,
        schema_path=schema_path,
    )

    print(f"registry_changed={str(result.changed).lower()}")
    print(f"new_dataset_count={len(result.new_dataset_ids)}")
    if result.new_dataset_ids:
        print("new_dataset_ids=" + ",".join(result.new_dataset_ids))

    if result.field_changes:
        breaking = [c for c in result.field_changes if c.removed]
        print(f"field_changes={len(result.field_changes)}")
        print(f"breaking_field_changes={len(breaking)}")
        for change in breaking:
            print(f"  BREAKING: {change.dataset_id} removed={change.removed}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
