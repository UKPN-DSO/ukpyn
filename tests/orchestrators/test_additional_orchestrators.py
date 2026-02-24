"""Additional coverage tests for orchestrator modules."""

from __future__ import annotations

from datetime import date, datetime
from types import SimpleNamespace

import pytest

from ukpyn.orchestrators import curtailment, dfes, dnoa, network, powerflow, resources
from ukpyn.orchestrators.curtailment import (
    CurtailmentOrchestrator,
    _format_date_for_where,
)
from ukpyn.orchestrators.dfes import DFESOrchestrator
from ukpyn.orchestrators.dnoa import DNOAOrchestrator
from ukpyn.orchestrators.network import NetworkOrchestrator
from ukpyn.orchestrators.powerflow import PowerflowOrchestrator
from ukpyn.orchestrators.resources import ResourcesOrchestrator


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, None),
        ("2024-01-02", "2024-01-02"),
        (date(2024, 1, 2), "2024-01-02"),
        (datetime(2024, 1, 2, 6, 30), "2024-01-02"),
        ("2024-01-02T08:30:00Z", "2024-01-02"),
    ],
)
def test_format_date_for_where_valid(value, expected) -> None:
    """Curtailment date formatting accepts valid date/date-time inputs."""
    assert _format_date_for_where(value) == expected


def test_format_date_for_where_invalid_string_raises() -> None:
    """Curtailment date formatting rejects invalid string inputs."""
    with pytest.raises(ValueError):
        _format_date_for_where("not-a-date")


def test_format_date_for_where_invalid_type_raises() -> None:
    """Curtailment date formatting rejects unsupported types."""
    with pytest.raises(TypeError):
        _format_date_for_where(123)


@pytest.mark.asyncio
async def test_curtailment_get_events_async_builds_where(monkeypatch) -> None:
    """Curtailment async method builds expected where clause."""
    orchestrator = CurtailmentOrchestrator()
    captured: dict[str, object] = {}

    async def fake_get_async(**kwargs):
        captured.update(kwargs)
        return "ok"

    monkeypatch.setattr(orchestrator, "get_async", fake_get_async)

    result = await orchestrator.get_events_async(
        site_id="SITE001",
        start_date="2024-01-01",
        end_date="2024-01-31",
        driver="thermal",
        limit=5,
        offset=2,
    )

    assert result == "ok"
    assert captured["dataset"] == "events"
    assert captured["limit"] == 5
    assert captured["offset"] == 2
    assert captured["where"] == (
        "der_name = 'SITE001' AND "
        "start_time_local >= '2024-01-01' AND "
        "start_time_local <= '2024-01-31' AND "
        "curtailment_driver = 'thermal'"
    )


@pytest.mark.asyncio
async def test_dfes_get_headroom_async_combines_where(monkeypatch) -> None:
    """DFES async method combines scenario/year and existing where."""
    orchestrator = DFESOrchestrator()
    captured: dict[str, object] = {}

    async def fake_get_async(**kwargs):
        captured.update(kwargs)
        return "ok"

    monkeypatch.setattr(orchestrator, "get_async", fake_get_async)

    await orchestrator.get_headroom_async(
        scenario="Leading the Way",
        year=2035,
        where="licence_area = 'EPN'",
        order_by="year",
    )

    assert captured["dataset"] == "headroom"
    assert captured["where"] == (
        "scenario = 'Leading the Way' AND "
        "year = 2035 AND licence_area = 'EPN'"
    )
    assert captured["order_by"] == "year"


@pytest.mark.asyncio
async def test_dnoa_get_assessment_async_where(monkeypatch) -> None:
    """DNOA async method applies licence area filter."""
    orchestrator = DNOAOrchestrator()
    captured: dict[str, object] = {}

    async def fake_get_async(**kwargs):
        captured.update(kwargs)
        return "ok"

    monkeypatch.setattr(orchestrator, "get_async", fake_get_async)

    await orchestrator.get_assessment_async(licence_area="SPN", order_by="year")

    assert captured["dataset"] == "dnoa"
    assert captured["where"] == "licence_area = 'SPN'"
    assert captured["order_by"] == "year"


@pytest.mark.asyncio
async def test_network_get_circuit_data_async_merges_where(monkeypatch) -> None:
    """Network async circuit query merges explicit and generated where."""
    orchestrator = NetworkOrchestrator()
    captured: dict[str, object] = {}

    async def fake_get_async(**kwargs):
        captured.update(kwargs)
        return "ok"

    monkeypatch.setattr(orchestrator, "get_async", fake_get_async)

    await orchestrator.get_circuit_data_async(
        voltage="132kv",
        granularity="monthly",
        circuit_id="C1",
        start_date="2024-01-01",
        end_date="2024-01-02",
        where="licence_area = 'EPN'",
    )

    assert captured["dataset"] == "132kv_monthly"
    assert captured["where"] == (
        "(licence_area = 'EPN') AND "
        "(circuit_id = 'C1' AND date >= '2024-01-01' AND date <= '2024-01-02')"
    )


@pytest.mark.asyncio
async def test_network_invalid_voltage_granularity_raises() -> None:
    """Network orchestrator rejects unsupported 33kv half-hourly combination."""
    orchestrator = NetworkOrchestrator()

    with pytest.raises(ValueError):
        await orchestrator.get_circuit_data_async(
            voltage="33kv",
            granularity="half_hourly",
        )


@pytest.mark.asyncio
async def test_powerflow_circuit_and_transformer_default_order_by(monkeypatch) -> None:
    """Powerflow async methods apply default timestamp ordering."""
    orchestrator = PowerflowOrchestrator()
    captured_calls: list[dict[str, object]] = []

    async def fake_get_async(**kwargs):
        captured_calls.append(kwargs)
        return "ok"

    monkeypatch.setattr(orchestrator, "get_async", fake_get_async)

    await orchestrator.get_circuit_timeseries_async(voltage="33kv", granularity="monthly")
    await orchestrator.get_transformer_timeseries_async(transformer_type="grid")

    assert captured_calls[0]["dataset"] == "33kv_monthly"
    assert captured_calls[0]["order_by"] == "timestamp"
    assert captured_calls[1]["dataset"] == "grid_monthly"
    assert captured_calls[1]["order_by"] == "timestamp"


@pytest.mark.asyncio
async def test_powerflow_discovery_uses_monthly(monkeypatch) -> None:
    """Powerflow discovery methods delegate to monthly timeseries methods."""
    orchestrator = PowerflowOrchestrator()

    async def fake_circuits(**kwargs):
        return kwargs

    async def fake_transformers(**kwargs):
        return kwargs

    monkeypatch.setattr(orchestrator, "get_circuit_timeseries_async", fake_circuits)
    monkeypatch.setattr(orchestrator, "get_transformer_timeseries_async", fake_transformers)

    circuits = await orchestrator.discover_circuits_async(voltage="132kv", licence_area="LPN")
    transformers = await orchestrator.discover_transformers_async(transformer_type="primary")

    assert circuits["granularity"] == "monthly"
    assert circuits["voltage"] == "132kv"
    assert transformers["granularity"] == "monthly"
    assert transformers["transformer_type"] == "primary"


@pytest.mark.asyncio
async def test_resources_filters_build_where(monkeypatch) -> None:
    """Resources async methods build expected where clauses."""
    orchestrator = ResourcesOrchestrator()
    captured_calls: list[dict[str, object]] = []

    async def fake_get_async(**kwargs):
        captured_calls.append(kwargs)
        return "ok"

    monkeypatch.setattr(orchestrator, "get_async", fake_get_async)

    await orchestrator.get_embedded_capacity_async(
        technology_type="Solar",
        licence_area="EPN",
        min_capacity_mw=5.5,
    )
    await orchestrator.get_large_demand_async(licence_area="SPN", min_demand_mw=10.0)

    assert captured_calls[0]["dataset"] == "embedded_capacity"
    assert captured_calls[0]["where"] == (
        "technology_type = 'Solar' AND licence_area = 'EPN' AND capacity_mw >= 5.5"
    )
    assert captured_calls[1]["dataset"] == "large_demand"
    assert captured_calls[1]["where"] == "licence_area = 'SPN' AND demand_mw >= 10.0"


@pytest.mark.parametrize(
    "module",
    [curtailment, dfes, dnoa, network, powerflow, resources],
)
def test_module_level_get_and_export_delegate(monkeypatch, module) -> None:
    """Module-level get/export convenience functions delegate to singleton."""
    fake = SimpleNamespace(
        get=lambda dataset, limit=100, **kwargs: (dataset, limit, kwargs),
        export=lambda dataset, format="csv", **kwargs: (
            dataset,
            format,
            kwargs,
        ),
    )
    monkeypatch.setattr(module, "_get_orchestrator", lambda: fake)

    got = module.get("x", limit=7, where="a")
    exported = module.export("x", format="json", where="b")

    assert got == ("x", 7, {"where": "a"})
    assert exported == ("x", "json", {"where": "b"})


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "module",
    [curtailment, dfes, dnoa, network, powerflow, resources],
)
async def test_module_level_get_async_delegates(monkeypatch, module) -> None:
    """Module-level get_async convenience functions delegate to singleton."""

    class FakeOrchestrator:
        async def get_async(self, dataset, limit=100, **kwargs):
            return (dataset, limit, kwargs)

    monkeypatch.setattr(module, "_get_orchestrator", lambda: FakeOrchestrator())

    result = await module.get_async("dataset", limit=3, where="x")
    assert result == ("dataset", 3, {"where": "x"})
