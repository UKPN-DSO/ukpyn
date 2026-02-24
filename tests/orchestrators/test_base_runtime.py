"""Runtime-behavior tests for BaseOrchestrator and _run_sync."""

from __future__ import annotations

import asyncio

import pytest

import ukpyn.orchestrators.base as base_module
from ukpyn.orchestrators.base import BaseOrchestrator, _run_sync


class DemoOrchestrator(BaseOrchestrator):
    """Concrete orchestrator for testing base runtime paths."""

    DATASETS = {"demo": "dataset-demo"}


class FakeClient:
    """Minimal async client implementing required UKPNClient surface."""

    def __init__(self, api_key=None, config=None):
        self.api_key = api_key
        self.config = config
        self.calls = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return None

    async def get_records(self, **kwargs):
        self.calls.append(("get_records", kwargs))
        return {"kind": "records", "kwargs": kwargs}

    async def export_data(self, **kwargs):
        self.calls.append(("export_data", kwargs))
        return b"export"

    async def get_dataset(self, dataset_id):
        self.calls.append(("get_dataset", {"dataset_id": dataset_id}))
        return {"dataset_id": dataset_id}


@pytest.mark.asyncio
async def test_base_orchestrator_get_export_metadata_async(monkeypatch) -> None:
    """Base async methods resolve dataset IDs and call underlying client."""
    monkeypatch.setattr(base_module, "UKPNClient", FakeClient)

    orchestrator = DemoOrchestrator(api_key="k")

    records = await orchestrator.get_async("demo", limit=3, offset=1, where="x")
    exported = await orchestrator.export_async("demo", format="csv")
    metadata = await orchestrator.get_metadata_async("demo")

    assert records["kind"] == "records"
    assert records["kwargs"]["dataset_id"] == "dataset-demo"
    assert exported == b"export"
    assert metadata["dataset_id"] == "dataset-demo"


def test_base_orchestrator_sync_wrappers_use_run_sync(monkeypatch) -> None:
    """Base sync wrappers call _run_sync for get/export/metadata."""
    orchestrator = DemoOrchestrator()

    def fake_run_sync(coro):
        coro.close()
        return "sync-result"

    monkeypatch.setattr(base_module, "_run_sync", fake_run_sync)

    assert orchestrator.get("demo") == "sync-result"
    assert orchestrator.export("demo") == "sync-result"
    assert orchestrator.get_metadata("demo") == "sync-result"


def test_run_sync_without_existing_event_loop() -> None:
    """_run_sync executes coroutines directly when no loop is running."""

    async def sample():
        return 123

    assert _run_sync(sample()) == 123


def test_run_sync_with_existing_event_loop(monkeypatch) -> None:
    """_run_sync falls back to thread executor when a loop is already running."""

    class FakeFuture:
        def __init__(self, value):
            self._value = value

        def result(self):
            return self._value

    class FakeExecutor:
        def __init__(self, max_workers):
            self.max_workers = max_workers

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def submit(self, fn, *args, **kwargs):
            return FakeFuture(fn(*args, **kwargs))

    async def sample():
        return "threaded"

    monkeypatch.setattr(asyncio, "get_running_loop", lambda: object())
    monkeypatch.setattr(base_module, "ThreadPoolExecutor", FakeExecutor)

    assert _run_sync(sample()) == "threaded"
