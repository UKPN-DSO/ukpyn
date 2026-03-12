"""Final wrapper coverage tests for remaining orchestrator modules."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from ukpyn.orchestrators import curtailment, dfes, dnoa, resources
from ukpyn.orchestrators.curtailment import CurtailmentOrchestrator
from ukpyn.orchestrators.dfes import DFESOrchestrator
from ukpyn.orchestrators.dnoa import DNOAOrchestrator
from ukpyn.orchestrators.resources import ResourcesOrchestrator


def test_curtailment_sync_and_module_wrappers(monkeypatch) -> None:
    """Curtailment sync/module wrappers delegate correctly."""
    orchestrator = CurtailmentOrchestrator()

    async def fake_async(**kwargs):
        return kwargs

    monkeypatch.setattr(orchestrator, "get_events_async", fake_async)
    assert orchestrator.get_events(limit=2, offset=1)["limit"] == 2

    fake_module = SimpleNamespace(get_events=lambda **k: ("events", k))
    monkeypatch.setattr(curtailment, "_get_orchestrator", lambda: fake_module)
    assert curtailment.get_events(limit=3)[0] == "events"


@pytest.mark.asyncio
async def test_curtailment_module_get_events_async(monkeypatch) -> None:
    """Curtailment module-level async getter delegates correctly."""

    class Fake:
        async def get_async(self, dataset, limit=100, **kwargs):
            return dataset, limit, kwargs

    monkeypatch.setattr(curtailment, "_get_orchestrator", lambda: Fake())
    result = await curtailment.get_async("events", limit=4)
    assert result == ("events", 4, {})


def test_dfes_sync_and_module_wrappers(monkeypatch) -> None:
    """DFES sync/module wrappers delegate correctly."""
    orchestrator = DFESOrchestrator()

    async def fake_async(**kwargs):
        return kwargs

    monkeypatch.setattr(orchestrator, "get_headroom_async", fake_async)
    assert orchestrator.get_headroom(limit=6)["limit"] == 6

    fake_module = SimpleNamespace(
        get_headroom=lambda **k: ("headroom", k),
        export=lambda dataset, format="csv", **k: (dataset, format, k),
    )
    monkeypatch.setattr(dfes, "_get_orchestrator", lambda: fake_module)

    assert dfes.get_headroom(limit=7)[0] == "headroom"
    assert dfes.export("headroom", format="json")[1] == "json"


def test_dnoa_sync_and_module_wrappers(monkeypatch) -> None:
    """DNOA sync/module wrappers delegate correctly."""
    orchestrator = DNOAOrchestrator()

    async def fake_async(**kwargs):
        return kwargs

    monkeypatch.setattr(orchestrator, "get_assessment_async", fake_async)
    assert orchestrator.get_assessment(limit=6)["limit"] == 6

    fake_module = SimpleNamespace(
        get_assessment=lambda **k: ("assessment", k),
        export=lambda dataset, format="csv", **k: (dataset, format, k),
    )
    monkeypatch.setattr(dnoa, "_get_orchestrator", lambda: fake_module)

    assert dnoa.get_assessment(limit=5)[0] == "assessment"
    assert dnoa.export("dnoa", format="xlsx")[1] == "xlsx"


def test_resources_sync_and_module_wrappers(monkeypatch) -> None:
    """Resources sync/module wrappers delegate correctly."""
    orchestrator = ResourcesOrchestrator()

    async def fake_embedded(**kwargs):
        return kwargs

    async def fake_demand(**kwargs):
        return kwargs

    monkeypatch.setattr(orchestrator, "get_embedded_capacity_async", fake_embedded)
    monkeypatch.setattr(orchestrator, "get_large_demand_async", fake_demand)

    assert orchestrator.get_embedded_capacity(limit=2)["limit"] == 2
    assert orchestrator.get_large_demand(limit=3)["limit"] == 3

    fake_module = SimpleNamespace(
        get_embedded_capacity=lambda **k: ("embedded", k),
        get_large_demand=lambda **k: ("demand", k),
    )
    monkeypatch.setattr(resources, "_get_orchestrator", lambda: fake_module)

    assert resources.get_embedded_capacity(limit=1)[0] == "embedded"
    assert resources.get_large_demand(limit=1)[0] == "demand"
