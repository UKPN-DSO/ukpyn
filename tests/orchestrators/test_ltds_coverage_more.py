"""Additional LTDS coverage tests for table helper methods and module wrappers."""

from __future__ import annotations

from typing import Any, cast

import pytest

from ukpyn.orchestrators import ltds
from ukpyn.orchestrators.ltds import LTDSOrchestrator


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("method_name", "kwargs", "expected_dataset", "expected_snippet"),
    [
        (
            "get_table_2a_async",
            {"licence_area": "EPN", "substation": "ASHFORD"},
            "table_2a",
            "substation LIKE '%ASHFORD%'",
        ),
        (
            "get_table_2b_async",
            {"licence_area": "SPN", "substation": "BROMLEY"},
            "table_2b",
            "substation LIKE '%BROMLEY%'",
        ),
        (
            "get_table_3a_async",
            {"licence_area": "LPN", "year": 2024},
            "table_3a",
            "year = 2024",
        ),
        (
            "get_table_3b_async",
            {"licence_area": "EPN"},
            "table_3b",
            "licence_area = 'EPN'",
        ),
        (
            "get_table_4a_async",
            {"licence_area": "SPN"},
            "table_4a",
            "licence_area = 'SPN'",
        ),
        (
            "get_table_4b_async",
            {"licence_area": "LPN"},
            "table_4b",
            "licence_area = 'LPN'",
        ),
        (
            "get_table_7_async",
            {"licence_area": "EPN"},
            "table_7",
            "licence_area = 'EPN'",
        ),
        (
            "get_table_8_async",
            {"licence_area": "SPN"},
            "table_8",
            "licence_area = 'SPN'",
        ),
    ],
)
async def test_ltds_async_helpers_build_where(
    method_name: str,
    kwargs: dict[str, object],
    expected_dataset: str,
    expected_snippet: str,
    monkeypatch,
) -> None:
    """LTDS async helper methods build dataset key and where clauses correctly."""
    orchestrator = LTDSOrchestrator()
    captured: dict[str, object] = {}

    async def fake_get_async(**inner_kwargs):
        captured.update(inner_kwargs)
        return "ok"

    monkeypatch.setattr(orchestrator, "get_async", fake_get_async)

    result = await getattr(orchestrator, method_name)(**kwargs)

    assert result == "ok"
    assert captured["dataset"] == expected_dataset
    assert expected_snippet in str(captured.get("where"))


@pytest.mark.parametrize(
    "func_name",
    [
        "get_table_2a",
        "get_table_2b",
        "get_table_3a",
        "get_table_3b",
        "get_table_4a",
        "get_table_4b",
        "get_table_7",
        "get_table_8",
    ],
)
def test_ltds_sync_helpers_call_async_versions(func_name: str, monkeypatch) -> None:
    """LTDS sync helper methods delegate through _run_sync to async versions."""
    orchestrator = LTDSOrchestrator()

    async def fake_async(**kwargs):
        return kwargs

    monkeypatch.setattr(orchestrator, f"{func_name}_async", fake_async)

    result = getattr(orchestrator, func_name)(limit=5, offset=1)
    assert result["limit"] == 5
    assert result["offset"] == 1


def test_ltds_module_get_get_async_export_delegate(monkeypatch) -> None:
    """LTDS module-level get/get_async/export functions delegate to singleton."""

    class FakeOrchestrator:
        def get(self, dataset, limit=100, **kwargs):
            return dataset, limit, kwargs

        async def get_async(self, dataset, limit=100, **kwargs):
            return dataset, limit, kwargs

        def export(self, dataset, format="csv", **kwargs):
            return dataset, format, kwargs

    monkeypatch.setattr(ltds, "_get_orchestrator", lambda: FakeOrchestrator())

    result = cast(Any, ltds.get("table_1", limit=2))
    assert result[0] == "table_1"

    exported = ltds.export("table_1", format="json", where="x")
    assert exported == ("table_1", "json", {"where": "x"})


@pytest.mark.asyncio
async def test_ltds_module_get_async_delegate(monkeypatch) -> None:
    """LTDS module-level async getter delegates to singleton async method."""

    class FakeOrchestrator:
        async def get_async(self, dataset, limit=100, **kwargs):
            return dataset, limit, kwargs

    monkeypatch.setattr(ltds, "_get_orchestrator", lambda: FakeOrchestrator())

    result = await ltds.get_async("table_1", limit=3, where="y")
    assert result == ("table_1", 3, {"where": "y"})


def test_ltds_singleton_get_orchestrator(monkeypatch) -> None:
    """_get_orchestrator initializes singleton only once."""
    created: list[str] = []

    class FakeLTDS:
        def __init__(self):
            created.append("x")

    monkeypatch.setattr(ltds, "LTDSOrchestrator", FakeLTDS)
    monkeypatch.setattr(ltds, "_default_orchestrator", None)

    first = ltds._get_orchestrator()
    second = ltds._get_orchestrator()

    assert first is second
    assert len(created) == 1
