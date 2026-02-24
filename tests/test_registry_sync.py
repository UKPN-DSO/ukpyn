"""Tests for registry unmanaged dataset synchronization helpers."""

from ukpyn.registry_sync import (
    BEGIN_MARKER,
    END_MARKER,
    build_issue_report,
    extract_managed_and_unmanaged_ids,
    inject_unmanaged_section,
    parse_metadata_rows,
    render_unmanaged_section,
)


def test_parse_metadata_rows_extracts_dataset_ids() -> None:
    """CSV metadata parser extracts dataset IDs and titles."""
    csv_text = "datasetid,default.title\nukpn-a,Dataset A\nukpn-b,Dataset B\n"
    parsed = parse_metadata_rows(csv_text)
    assert parsed == {"ukpn-a": "Dataset A", "ukpn-b": "Dataset B"}


def test_extract_managed_and_unmanaged_ids() -> None:
    """Registry parser separates managed and unmanaged IDs."""
    source = '''
LTDS_DATASETS: dict[str, str] = {
    "table_1": "ltds-table-1",
}

UNMANAGED_DATASETS: dict[str, str] = {
    "ukpn-new": "ukpn-new",
}

ALL_DATASETS: dict[str, str] = {
    **LTDS_DATASETS,
    **UNMANAGED_DATASETS,
}
'''
    managed, unmanaged = extract_managed_and_unmanaged_ids(source)
    assert managed == {"ltds-table-1"}
    assert unmanaged == {"ukpn-new"}


def test_inject_unmanaged_section_replaces_marked_block() -> None:
    """Marked auto-generated section is replaced in-place."""
    source = f"before\n{BEGIN_MARKER}\nold\n{END_MARKER}\nafter\n"
    section = f"{BEGIN_MARKER}\nnew\n{END_MARKER}"
    updated = inject_unmanaged_section(source, section)
    assert updated == "before\n" + section + "\nafter\n"


def test_inject_unmanaged_section_inserts_before_network_datasets() -> None:
    """Unmanaged section is inserted before legacy network section when missing."""
    source = "header\n# NETWORK_DATASETS (Legacy - backward compatibility)\ntrailer\n"
    section = f"{BEGIN_MARKER}\nblock\n{END_MARKER}"
    updated = inject_unmanaged_section(source, section)
    assert section in updated
    assert updated.index(section) < updated.index("# NETWORK_DATASETS")


def test_render_unmanaged_section_includes_titles() -> None:
    """Rendered section includes titles and identity mappings."""
    section = render_unmanaged_section(["ukpn-new"], {"ukpn-new": "New Dataset"})
    assert "UNMANAGED_DATASETS: dict[str, str]" in section
    assert "# New Dataset" in section
    assert '"ukpn-new": "ukpn-new"' in section


def test_build_issue_report_contains_targets_and_marker() -> None:
    """Issue report includes hidden marker and update target instructions."""
    report = build_issue_report(["ukpn-ltds-example"], {"ukpn-ltds-example": "Example"})
    assert "<!-- unmanaged-datasets:ukpn-ltds-example -->" in report
    assert "## Copilot task instruction" in report
    assert "@copilot determine which orchestrator" in report
    assert "open a PR into `dev`" in report
    assert "## Acceptance criteria" in report
    assert "[ ] Orchestrator implementation and tests added/updated" in report
    assert "Suggested update targets" in report
    assert "src/ukpyn/orchestrators/ltds.py" in report
