"""Additional tests for registry synchronization utilities."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

from ukpyn import registry_sync
from ukpyn.registry_sync import (
    BEGIN_MARKER,
    END_MARKER,
    SyncResult,
    _safe_comment,
    extract_registry_maps,
    inject_unmanaged_section,
    main,
    parse_args,
    suggest_update_targets,
    synchronize_registry,
)


def test_extract_registry_maps_parses_assign_and_annassign() -> None:
    """Registry map extraction handles both Assign and AnnAssign forms."""
    source = """
A_DATASETS = {
    "a": "a-id",
}

B_DATASETS: dict[str, str] = {
    "b": "b-id",
}
"""

    maps = extract_registry_maps(source)
    assert maps["A_DATASETS"]["a"] == "a-id"
    assert maps["B_DATASETS"]["b"] == "b-id"


def test_safe_comment_strips_hashes_and_whitespace() -> None:
    """Comment sanitizer removes hashes and normalizes spacing."""
    assert _safe_comment("  # hello   world # ") == "hello world"
    assert _safe_comment("") == "Title unavailable"


def test_inject_unmanaged_section_raises_when_anchor_missing() -> None:
    """Injection raises when no insertion anchor and no markers exist."""
    with pytest.raises(ValueError):
        inject_unmanaged_section("plain source", f"{BEGIN_MARKER}\nX\n{END_MARKER}")


@pytest.mark.parametrize(
    ("dataset_id", "expected_target"),
    [
        ("ukpn-ltds-foo", "src/ukpyn/orchestrators/ltds.py"),
        ("ukpn-dfes-foo", "src/ukpyn/orchestrators/dfes.py"),
        ("ukpn-dnoa-foo", "src/ukpyn/orchestrators/dnoa.py"),
        ("ukpn-flexibility-foo", "src/ukpyn/orchestrators/flexibility.py"),
        ("ukpn-curtailment-foo", "src/ukpyn/orchestrators/curtailment.py"),
        ("ukpn-powerflow-foo", "src/ukpyn/orchestrators/powerflow.py"),
        ("ukpn-overhead-lines", "src/ukpyn/orchestrators/gis.py"),
        ("ukpn-embedded-capacity-register", "src/ukpyn/orchestrators/ders.py"),
        ("ukpn-constraints-foo", "src/ukpyn/orchestrators/network.py"),
    ],
)
def test_suggest_update_targets_keyword_routes(
    dataset_id: str, expected_target: str
) -> None:
    """Dataset ID heuristics route to expected orchestrator target files."""
    assert expected_target in suggest_update_targets(dataset_id)


def test_suggest_update_targets_default_fallback() -> None:
    """Unknown dataset naming falls back to generic orchestrator targets."""
    targets = suggest_update_targets("ukpn-unknown-dataset")
    assert "src/ukpyn/orchestrators/registry.py" in targets


def test_parse_args_custom_values(monkeypatch) -> None:
    """CLI argument parser honors custom path and URL options."""
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "registry_sync.py",
            "--registry-path",
            "x.py",
            "--metadata-url",
            "https://example.com/data.csv",
            "--report-path",
            "report.md",
            "--json-output-path",
            "out.json",
        ],
    )
    args = parse_args()

    assert args.registry_path == Path("x.py")
    assert args.metadata_url == "https://example.com/data.csv"
    assert args.report_path == Path("report.md")
    assert args.json_output_path == Path("out.json")


def test_synchronize_registry_end_to_end(tmp_path: Path, monkeypatch) -> None:
    """Synchronization writes updated registry and report/json artifacts."""
    registry_path = tmp_path / "dataset_registry.py"
    report_path = tmp_path / "report.md"
    json_path = tmp_path / "out.json"

    registry_path.write_text(
        """
LTDS_DATASETS: dict[str, str] = {
    \"table_1\": \"ltds-table-1\",
}

# NETWORK_DATASETS (Legacy - backward compatibility)
NETWORK_DATASETS: dict[str, str] = {}
""".strip(),
        encoding="utf-8",
    )

    csv_text = "datasetid,default.title\nukpn-new,New Title\nltds-table-1,Existing\n"
    monkeypatch.setattr(registry_sync, "fetch_metadata_csv", lambda _url: csv_text)

    result = synchronize_registry(
        registry_path=registry_path,
        metadata_url="https://example.com/mock.csv",
        report_path=report_path,
        json_output_path=json_path,
    )

    assert isinstance(result, SyncResult)
    assert result.changed is True
    assert result.new_dataset_ids == ["ukpn-new"]
    assert "ukpn-new" in result.unmanaged_dataset_ids

    updated = registry_path.read_text(encoding="utf-8")
    assert BEGIN_MARKER in updated
    assert END_MARKER in updated
    assert '"ukpn-new": "ukpn-new"' in updated

    report = report_path.read_text(encoding="utf-8")
    assert "Detected **1** new dataset(s)" in report

    summary = json_path.read_text(encoding="utf-8")
    assert '"new_count": 1' in summary


def test_synchronize_registry_no_change_when_no_new(
    tmp_path: Path, monkeypatch
) -> None:
    """Synchronization keeps registry unchanged when there are no new datasets."""
    section = (
        f"{BEGIN_MARKER}\n"
        "UNMANAGED_DATASETS: dict[str, str] = {\n"
        '    "ukpn-old": "ukpn-old",\n'
        "}\n"
        f"{END_MARKER}"
    )
    registry_path = tmp_path / "dataset_registry.py"
    report_path = tmp_path / "report.md"
    json_path = tmp_path / "out.json"

    registry_path.write_text(section, encoding="utf-8")
    monkeypatch.setattr(
        registry_sync,
        "fetch_metadata_csv",
        lambda _url: "datasetid,default.title\nukpn-old,Old\n",
    )

    result = synchronize_registry(
        registry_path=registry_path,
        metadata_url="https://example.com/mock.csv",
        report_path=report_path,
        json_output_path=json_path,
    )

    assert result.changed is True
    assert result.new_dataset_ids == []


def test_main_uses_parse_and_sync(monkeypatch, capsys) -> None:
    """Main entry point prints summary values from synchronization result."""
    fake_args = type(
        "Args",
        (),
        {
            "registry_path": Path("registry.py"),
            "metadata_url": "https://example.com",
            "report_path": Path("report.md"),
            "json_output_path": Path("out.json"),
        },
    )

    monkeypatch.setattr(registry_sync, "parse_args", lambda: fake_args)
    monkeypatch.setattr(
        registry_sync,
        "synchronize_registry",
        lambda **_kwargs: SyncResult(
            changed=True,
            new_dataset_ids=["x", "y"],
            unmanaged_dataset_ids=["x", "y"],
        ),
    )

    code = main()
    captured = capsys.readouterr().out

    assert code == 0
    assert "registry_changed=true" in captured
    assert "new_dataset_count=2" in captured
    assert "new_dataset_ids=x,y" in captured
