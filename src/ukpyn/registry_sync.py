"""Synchronize unmanaged ODP datasets into the registry file.

This module fetches ODP metadata, compares dataset IDs against managed
registry mappings, updates an auto-generated unmanaged section in
``src/ukpyn/dataset_registry.py``, and emits triage report artifacts.
"""

from __future__ import annotations

import argparse
import ast
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


@dataclass(frozen=True)
class SyncResult:
    """Result of unmanaged dataset synchronization."""

    changed: bool
    new_dataset_ids: list[str]
    unmanaged_dataset_ids: list[str]


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

    lines.extend(["}", END_MARKER])
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


def synchronize_registry(
    registry_path: Path,
    metadata_url: str,
    report_path: Path,
    json_output_path: Path,
) -> SyncResult:
    """Run unmanaged-dataset synchronization end-to-end."""
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

    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(
        build_issue_report(new_ids, metadata_titles), encoding="utf-8"
    )

    payload = {
        "new_dataset_ids": new_ids,
        "new_count": len(new_ids),
        "registry_changed": changed,
    }
    json_output_path.parent.mkdir(parents=True, exist_ok=True)
    json_output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    return SyncResult(
        changed=changed,
        new_dataset_ids=new_ids,
        unmanaged_dataset_ids=merged_unmanaged,
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
    return parser.parse_args()


def main() -> int:
    """CLI entry point."""
    args = parse_args()
    result = synchronize_registry(
        registry_path=args.registry_path,
        metadata_url=args.metadata_url,
        report_path=args.report_path,
        json_output_path=args.json_output_path,
    )

    print(f"registry_changed={str(result.changed).lower()}")
    print(f"new_dataset_count={len(result.new_dataset_ids)}")
    if result.new_dataset_ids:
        print("new_dataset_ids=" + ",".join(result.new_dataset_ids))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
