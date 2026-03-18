"""Snapshot ODP field schemas for all managed datasets.

Fetches the live field list from the OpenDataSoft API for every dataset
in the registry and writes the result to a JSON file.

Usage:
    python scripts/snapshot_field_schemas.py
    python scripts/snapshot_field_schemas.py --output schemas.json
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

from ukpyn.client import UKPNClient
from ukpyn.dataset_registry import ALL_DATASETS

# Ensure src/ is importable when running the script directly
_src = Path(__file__).resolve().parent.parent / "src"
if str(_src) not in sys.path:
    sys.path.insert(0, str(_src))

DEFAULT_OUTPUT = (
    Path(__file__).resolve().parent.parent / "src" / "ukpyn" / "field_schemas.json"
)


async def fetch_all_schemas(
    client: UKPNClient,
    datasets: dict[str, str],
) -> dict[str, list[str]]:
    """Fetch field names for every dataset in the registry.

    Args:
        client: An authenticated UKPNClient.
        datasets: Mapping of friendly name → ODP dataset ID.

    Returns:
        Mapping of ODP dataset ID → sorted list of field names.
    """
    # Deduplicate: multiple friendly names can point to the same ODP ID
    unique_ids = sorted(set(datasets.values()))

    schemas: dict[str, list[str]] = {}
    total = len(unique_ids)

    for idx, dataset_id in enumerate(unique_ids, 1):
        print(f"[{idx}/{total}] Fetching fields for {dataset_id} ... ", end="")
        try:
            dataset = await client.get_dataset(dataset_id)
            fields = sorted(dataset.field_ids)
            schemas[dataset_id] = fields
            print(f"{len(fields)} fields")
        except Exception as exc:
            print(f"ERROR: {exc}")
            schemas[dataset_id] = []

    return dict(sorted(schemas.items()))


async def run(output_path: Path) -> int:
    """Main async entry point."""
    async with UKPNClient() as client:
        schemas = await fetch_all_schemas(client, ALL_DATASETS)

    output_path.write_text(
        json.dumps(schemas, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(f"\nWrote {len(schemas)} dataset schemas to {output_path}")

    empty = [did for did, fields in schemas.items() if not fields]
    if empty:
        print(f"WARNING: {len(empty)} dataset(s) returned no fields: {empty}")

    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"Output JSON path (default: {DEFAULT_OUTPUT})",
    )
    args = parser.parse_args()
    return asyncio.run(run(args.output))


if __name__ == "__main__":
    raise SystemExit(main())
