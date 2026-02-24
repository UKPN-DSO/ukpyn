"""Additional tests for network and powerflow orchestrator wrappers."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from ukpyn.orchestrators import network, powerflow
from ukpyn.orchestrators.network import NetworkOrchestrator
from ukpyn.orchestrators.powerflow import PowerflowOrchestrator


@pytest.mark.asyncio
async def test_network_statistics_and_profiles_async_where(monkeypatch) -> None:
    """Network async methods build/merge expected where clauses."""
    orchestrator = NetworkOrchestrator()
    calls: list[dict[str, object]] = []

    async def fake_get_async(**kwargs):
        calls.append(kwargs)
        return "ok"

    monkeypatch.setattr(orchestrator, "get_async", fake_get_async)

    await orchestrator.get_statistics_async(year=2024, region="EPN")
    await orchestrator.get_demand_profiles_async(profile_class="1", where="licence_area = 'SPN'")

    assert calls[0]["dataset"] == "statistics"
    assert calls[0]["where"] == "year = 2024 AND region = 'EPN'"
    assert calls[1]["dataset"] == "demand_profiles"
    assert calls[1]["where"] == "(licence_area = 'SPN') AND (profile_class = '1')"


@pytest.mark.parametrize(
    "func_name",
    ["get_circuit_data", "get_statistics", "get_demand_profiles"],
)
def test_network_sync_wrappers(func_name: str, monkeypatch) -> None:
    """Network sync wrappers invoke corresponding async methods."""
    orchestrator = NetworkOrchestrator()

    async def fake_async(**kwargs):
        return kwargs

    monkeypatch.setattr(orchestrator, f"{func_name}_async", fake_async)

    result = getattr(orchestrator, func_name)(limit=2)
    assert result["limit"] == 2


def test_network_module_specific_functions(monkeypatch) -> None:
    """Network module-level specific convenience functions delegate."""
    fake = SimpleNamespace(
        get_circuit_data=lambda **k: ("circuit", k),
        get_statistics=lambda **k: ("stats", k),
        get_demand_profiles=lambda **k: ("profiles", k),
    )
    monkeypatch.setattr(network, "_get_orchestrator", lambda: fake)

    assert network.get_circuit_data(limit=1)[0] == "circuit"
    assert network.get_statistics(limit=1)[0] == "stats"
    assert network.get_demand_profiles(limit=1)[0] == "profiles"


@pytest.mark.asyncio
async def test_powerflow_where_merge_and_order_override(monkeypatch) -> None:
    """Powerflow methods merge where clauses and preserve explicit order_by."""
    orchestrator = PowerflowOrchestrator()
    calls: list[dict[str, object]] = []

    async def fake_get_async(**kwargs):
        calls.append(kwargs)
        return "ok"

    monkeypatch.setattr(orchestrator, "get_async", fake_get_async)

    await orchestrator.get_circuit_timeseries_async(
        voltage="132kv",
        granularity="half_hourly",
        circuit_id="C-1",
        where="licence_area = 'EPN'",
        order_by="custom",
    )
    await orchestrator.get_transformer_timeseries_async(
        transformer_type="primary",
        transformer_id="T-1",
        where="licence_area = 'LPN'",
        order_by="custom2",
    )

    assert calls[0]["dataset"] == "132kv_half_hourly"
    assert calls[0]["where"] == "(licence_area = 'EPN') AND (circuit_id = 'C-1')"
    assert calls[0]["order_by"] == "custom"
    assert calls[1]["dataset"] == "primary_monthly"
    assert calls[1]["where"] == "(licence_area = 'LPN') AND (transformer_id = 'T-1')"
    assert calls[1]["order_by"] == "custom2"


@pytest.mark.parametrize(
    "func_name",
    ["get_circuit_timeseries", "get_transformer_timeseries", "discover_circuits", "discover_transformers"],
)
def test_powerflow_sync_wrappers(func_name: str, monkeypatch) -> None:
    """Powerflow sync wrappers invoke matching async methods."""
    orchestrator = PowerflowOrchestrator()

    async def fake_async(**kwargs):
        return kwargs

    monkeypatch.setattr(orchestrator, f"{func_name}_async", fake_async)

    result = getattr(orchestrator, func_name)(limit=4)
    assert result["limit"] == 4


def test_powerflow_module_specific_functions(monkeypatch) -> None:
    """Powerflow module-level specific convenience functions delegate."""
    fake = SimpleNamespace(
        get_circuit_timeseries=lambda **k: ("circuits", k),
        get_transformer_timeseries=lambda **k: ("transformers", k),
        discover_circuits=lambda **k: ("discover-circuits", k),
        discover_transformers=lambda **k: ("discover-transformers", k),
    )
    monkeypatch.setattr(powerflow, "_get_orchestrator", lambda: fake)

    assert powerflow.get_circuit_timeseries(limit=1)[0] == "circuits"
    assert powerflow.get_transformer_timeseries(limit=1)[0] == "transformers"
    assert powerflow.discover_circuits(limit=1)[0] == "discover-circuits"
    assert powerflow.discover_transformers(limit=1)[0] == "discover-transformers"
