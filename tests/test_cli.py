"""Tests for ukpyn CLI entry point."""

from types import SimpleNamespace

import pytest

from ukpyn import __version__, cli
from ukpyn.cli import main
from ukpyn.dataset_registry import ALL_DATASETS


def test_cli_version(capsys) -> None:
    """CLI version command prints package version."""
    exit_code = main(["version"])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert captured.out.strip() == __version__


def test_cli_quickstart(capsys) -> None:
    """CLI quickstart command prints beginner-friendly pointers."""
    exit_code = main(["quickstart"])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "tutorials/01-getting-started.ipynb" in captured.out


def test_cli_unknown_command_prints_help(monkeypatch, capsys) -> None:
    """CLI prints help for unknown command branch."""

    class FakeParser:
        def parse_args(self, args=None):  # NOQA ARG002
            return SimpleNamespace(command="unknown")

        def print_help(self):
            print("USAGE: ukpyn ...")

    monkeypatch.setattr(cli, "build_parser", lambda: FakeParser())

    exit_code = cli.main(["unknown"])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "USAGE: ukpyn ..." in captured.out


# ---------------------------------------------------------------------------
# Regression tests for the `fetch` subcommand (PR #94).
#
# These assert the *intended* behaviour and currently FAIL against the PR,
# each one pinpointing a blocking issue. They should pass once the issues
# are fixed.
# ---------------------------------------------------------------------------


class _FakeDataset:
    """Stand-in for a Dataset exposing a summary() method."""

    def __init__(self, dataset_id: str) -> None:
        self.dataset_id = dataset_id

    def summary(self) -> str:
        return f"SUMMARY::{self.dataset_id}"


class _FakeClient:
    """Async UKPNClient stand-in that records calls and close state."""

    instances: list["_FakeClient"] = []

    def __init__(self, *args, **kwargs) -> None:
        self.closed = False
        self.calls: list[tuple] = []
        _FakeClient.instances.append(self)

    async def __aenter__(self) -> "_FakeClient":
        return self

    async def __aexit__(self, *exc) -> None:
        self.closed = True

    async def close(self) -> None:
        self.closed = True

    async def get_dataset(self, dataset_id: str) -> _FakeDataset:
        self.calls.append(("get_dataset", dataset_id))
        return _FakeDataset(dataset_id)

    async def export_data(self, dataset_id: str, format: str) -> bytes:
        self.calls.append(("export_data", dataset_id, format))
        return b"BYTES"


@pytest.fixture
def fake_client(monkeypatch):
    """Patch cli.UKPNClient with a recording fake and reset the instance log."""
    _FakeClient.instances.clear()
    monkeypatch.setattr(cli, "UKPNClient", _FakeClient)
    return _FakeClient


# A real dataset ID (a *value* in the registry) that passes current validation.
_VALID_DATASET_ID = ALL_DATASETS["dispatches"]  # "ukpn-flexibility-dispatches"


def test_fetch_closes_client(fake_client) -> None:
    """BLOCKING #1: `fetch` leaks the client it opens (never closed)."""
    main(["fetch", _VALID_DATASET_ID])

    assert fake_client.instances, "fetch should instantiate a UKPNClient"
    client = fake_client.instances[-1]
    assert client.closed is True, (
        "UKPNClient opened by `fetch` was never closed - the bare "
        "`client = UKPNClient()` leaks the underlying httpx connection pool"
    )


def test_fetch_summary_prints_and_returns_zero(fake_client, capsys) -> None:
    """BLOCKING #2: summary path returns a str (exit 1) and prints nothing useful."""
    exit_code = main(["fetch", _VALID_DATASET_ID])
    captured = capsys.readouterr()

    assert exit_code == 0, (
        f"fetch returned {exit_code!r} instead of int 0; through the console "
        "entry point this becomes sys.exit(<str>) -> non-zero exit status"
    )
    assert f"SUMMARY::{_VALID_DATASET_ID}" in captured.out, (
        "dataset summary was returned rather than printed - the user sees nothing"
    )


def test_fetch_accepts_friendly_dataset_key(fake_client, capsys) -> None:
    """BLOCKING #3: friendly keys advertised in --help are wrongly rejected."""
    main(["fetch", "table_3a"])
    captured = capsys.readouterr()

    assert "An invalid dataset was provided" not in captured.out, (
        "friendly key 'table_3a' (shown in `fetch --help`) is rejected because "
        "validation checks ALL_DATASETS.values() instead of resolving keys"
    )
    assert fake_client.instances, "friendly key should still reach a client fetch"
    assert any(call[0] == "get_dataset" for call in fake_client.instances[-1].calls), (
        "friendly key 'table_3a' was not resolved and fetched"
    )


def test_fetch_invalid_dataset_returns_nonzero(fake_client, capsys) -> None:
    """Invalid dataset input reports an error and exits non-zero."""
    exit_code = main(["fetch", "not-a-real-dataset"])
    captured = capsys.readouterr()

    assert exit_code != 0
    assert "An invalid dataset was provided" in captured.out


def test_fetch_invalid_output_extension_returns_nonzero(fake_client, capsys) -> None:
    """Invalid --output extension reports an error and exits non-zero."""
    exit_code = main(["fetch", _VALID_DATASET_ID, "--output", "xml"])
    captured = capsys.readouterr()

    assert exit_code != 0
    assert "An invalid extension was provided" in captured.out


def test_list_invalid_domain_returns_nonzero(capsys) -> None:
    """Invalid list --domain reports an error and exits non-zero."""
    exit_code = main(["list", "datasets", "--domain", "not-a-domain"])
    captured = capsys.readouterr()

    assert exit_code != 0
    assert "An invalid domain was provided" in captured.out
