"""Coverage-oriented tests for wrapper-heavy orchestrator modules."""

from __future__ import annotations

from datetime import date, datetime
from types import SimpleNamespace

import pytest

from ukpyn.orchestrators import flexibility, gis, ltds
from ukpyn.orchestrators.flexibility import (
    FlexibilityOrchestrator,
    _format_date_for_where,
)
from ukpyn.orchestrators.gis import GISOrchestrator
from ukpyn.orchestrators.ltds import LTDSOrchestrator


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, None),
        ("2024-02-03", "2024-02-03"),
        (date(2024, 2, 3), "2024-02-03"),
        (datetime(2024, 2, 3, 12, 0), "2024-02-03"),
    ],
)
def test_flexibility_date_format_valid(value, expected) -> None:
    """Flexibility date parser accepts supported formats."""
    assert _format_date_for_where(value) == expected


@pytest.mark.asyncio
async def test_flexibility_dispatch_and_curtailment_where(monkeypatch) -> None:
    """Flexibility async methods build expected where clauses."""
    orchestrator = FlexibilityOrchestrator()
    calls: list[dict[str, object]] = []

    async def fake_get_async(**kwargs):
        calls.append(kwargs)
        return "ok"

    monkeypatch.setattr(orchestrator, "get_async", fake_get_async)

    await orchestrator.get_dispatches_async(
        start_date="2024-01-01",
        end_date="2024-01-31",
        product="Secure",
    )
    await orchestrator.get_curtailment_async(
        site_id="SITE-1",
        start_date="2024-01-01",
        end_date="2024-01-02",
    )

    assert calls[0]["dataset"] == "dispatches"
    assert calls[0]["where"] == (
        "start_time_local >= '2024-01-01' AND "
        "start_time_local <= '2024-01-31' AND "
        "product = 'Secure'"
    )
    assert calls[1]["dataset"] == "curtailment"
    assert calls[1]["where"] == (
        "der_name = 'SITE-1' AND "
        "start_time_local >= '2024-01-01' AND "
        "start_time_local <= '2024-01-02'"
    )


@pytest.mark.parametrize(
    "func_name",
    [
        "get_dispatches",
        "get_curtailment",
    ],
)
def test_flexibility_sync_wrappers(func_name: str, monkeypatch) -> None:
    """Flexibility sync wrappers invoke matching async methods via _run_sync."""
    orchestrator = FlexibilityOrchestrator()

    async def fake_async(**kwargs):
        return kwargs

    monkeypatch.setattr(orchestrator, f"{func_name}_async", fake_async)

    result = getattr(orchestrator, func_name)(limit=3, offset=1)
    assert result["limit"] == 3
    assert result["offset"] == 1


@pytest.mark.parametrize(
    "module_func,method_name,kwargs",
    [
        ("get_dispatches", "get_dispatches", {"product": "SECURE", "limit": 2}),
        ("get_curtailment", "get_curtailment", {"site_id": "S1", "limit": 2}),
    ],
)
def test_flexibility_module_specific_functions(
    module_func, method_name, kwargs, monkeypatch
) -> None:
    """Flexibility module convenience functions delegate to singleton methods."""
    fake = SimpleNamespace(**{method_name: lambda **k: (method_name, k)})
    monkeypatch.setattr(flexibility, "_get_orchestrator", lambda: fake)

    result = getattr(flexibility, module_func)(**kwargs)
    assert result[0] == method_name


@pytest.mark.asyncio
async def test_gis_async_methods_select_dataset_keys(monkeypatch) -> None:
    """GIS async methods map high-level inputs to dataset keys."""
    orchestrator = GISOrchestrator()
    calls: list[dict[str, object]] = []

    async def fake_get_async(**kwargs):
        calls.append(kwargs)
        return "ok"

    async def fake_export_async(**kwargs):
        calls.append(kwargs)
        return b"geo"

    monkeypatch.setattr(orchestrator, "get_async", fake_get_async)
    monkeypatch.setattr(orchestrator, "export_async", fake_export_async)

    await orchestrator.get_primary_substations_async(licence_area="EPN")
    await orchestrator.get_secondary_sites_async(primary_substation="A")
    await orchestrator.get_overhead_lines_async(voltage="lv")
    await orchestrator.get_poles_async(voltage="hv")
    await orchestrator.export_geojson_async("primary_substations")

    assert calls[0]["dataset"] == "grid-and-primary-sites"
    assert calls[0]["refine"] == {
        "sitetype": "Primary Substation",
        "licencearea": "Eastern Power Networks (EPN)",
    }
    assert calls[1]["dataset"] == "ukpn-secondary-sites"
    assert calls[1]["where"] == "primaryfeederfunctionallocation = 'A'"
    assert calls[2]["dataset"] == "lv_overhead_lines"
    assert calls[3]["dataset"] == "hv_poles"
    assert calls[4]["format"] == "geojson"


@pytest.mark.parametrize(
    "func_name",
    [
        "get_primary_substations",
        "get_secondary_sites",
        "get_overhead_lines",
        "get_poles",
        "export_geojson",
    ],
)
def test_gis_sync_wrappers(func_name: str, monkeypatch) -> None:
    """GIS sync wrappers call corresponding async methods."""
    orchestrator = GISOrchestrator()

    async def fake_async(*args, **kwargs):
        return {"args": args, "kwargs": kwargs}

    monkeypatch.setattr(orchestrator, f"{func_name}_async", fake_async)

    result = (
        getattr(orchestrator, func_name)("x", limit=2)
        if "export" in func_name
        else getattr(orchestrator, func_name)(limit=2)
    )
    assert "kwargs" in result


def test_gis_module_specific_functions(monkeypatch) -> None:
    """GIS module convenience functions delegate to singleton methods."""
    fake = SimpleNamespace(
        get_primary_substations=lambda **k: ("ps", k),
        get_secondary_sites=lambda **k: ("ss", k),
        get_overhead_lines=lambda **k: ("ol", k),
        get_poles=lambda **k: ("poles", k),
        export_geojson=lambda dataset, **k: (dataset, k),
    )
    monkeypatch.setattr(gis, "_get_geo_instance", lambda: fake)

    assert gis.get_primary_substations(limit=1)[0] == "ps"
    assert gis.get_secondary_sites(limit=1)[0] == "ss"
    assert gis.get_overhead_lines(limit=1)[0] == "ol"
    assert gis.get_poles(limit=1)[0] == "poles"
    assert gis.export_geojson("primary_substations")[0] == "primary_substations"


@pytest.mark.asyncio
async def test_ltds_async_where_builders_for_high_impact_tables(monkeypatch) -> None:
    """LTDS async methods build where/refine clauses for major table helpers."""
    orchestrator = LTDSOrchestrator()
    calls: list[dict[str, object]] = []

    async def fake_get_async(**kwargs):
        calls.append(kwargs)
        return "ok"

    monkeypatch.setattr(orchestrator, "get_async", fake_get_async)

    await orchestrator.get_table_5_async(
        licence_area="EPN",
        fuel_type="Solar",
        substation="ASHFORD",
    )
    await orchestrator.get_table_6_async(licence_area="SPN", substation="BATTERSEA")
    await orchestrator.get_projects_async(
        local_authority="Cambridge", expected_start_year=2024
    )
    await orchestrator.get_cim_async(licence_area="LPN")

    assert calls[0]["dataset"] == "table_5"
    assert "fuel_type LIKE '%Solar%'" in calls[0]["where"]
    assert "substation='ASHFORD'" in calls[0]["where"]
    assert calls[1]["dataset"] == "table_6"
    assert calls[2]["dataset"] == "projects"
    assert "local_authority LIKE '%Cambridge%'" in str(calls[2]["where"])
    assert "expected_start_year = 2024" in str(calls[2]["where"])
    assert calls[3]["dataset"] == "cim"
    assert calls[3].get("refine") is None


@pytest.mark.parametrize(
    "func_name",
    [
        "get_table_1",
        "get_table_2a",
        "get_table_2b",
        "get_table_3a",
        "get_table_3b",
        "get_table_4a",
        "get_table_4b",
        "get_table_5",
        "get_table_6",
        "get_table_7",
        "get_table_8",
        "get_projects",
        "get_cim",
    ],
)
def test_ltds_sync_wrappers(func_name: str, monkeypatch) -> None:
    """LTDS sync wrappers invoke matching async methods."""
    orchestrator = LTDSOrchestrator()

    async def fake_async(**kwargs):
        return kwargs

    monkeypatch.setattr(orchestrator, f"{func_name}_async", fake_async)

    result = getattr(orchestrator, func_name)(limit=4, offset=1)
    assert result["limit"] == 4


def test_ltds_module_specific_functions_delegate(monkeypatch) -> None:
    """LTDS module-level table helpers delegate to singleton orchestrator."""

    class FakeLTDS:
        def __getattr__(self, name):
            def _method(**kwargs):
                return name, kwargs

            return _method

    monkeypatch.setattr(ltds, "_get_orchestrator", lambda: FakeLTDS())

    assert ltds.get_table_1(limit=1)[0] == "get_table_1"
    assert ltds.get_table_2a(limit=1)[0] == "get_table_2a"
    assert ltds.get_table_2b(limit=1)[0] == "get_table_2b"
    assert ltds.get_table_3a(limit=1)[0] == "get_table_3a"
    assert ltds.get_table_3b(limit=1)[0] == "get_table_3b"
    assert ltds.get_table_4a(limit=1)[0] == "get_table_4a"
    assert ltds.get_table_4b(limit=1)[0] == "get_table_4b"
    assert ltds.get_table_5(limit=1)[0] == "get_table_5"
    assert ltds.get_table_6(limit=1)[0] == "get_table_6"
    assert ltds.get_table_7(limit=1)[0] == "get_table_7"
    assert ltds.get_table_8(limit=1)[0] == "get_table_8"
    assert ltds.get_projects(limit=1)[0] == "get_projects"
    assert ltds.get_cim(limit=1)[0] == "get_cim"
