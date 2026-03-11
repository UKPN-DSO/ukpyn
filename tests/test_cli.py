"""Tests for ukpyn CLI entry point."""

from types import SimpleNamespace

from ukpyn import __version__, cli
from ukpyn.cli import main


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
        def parse_args(self):
            return SimpleNamespace(command="unknown")

        def print_help(self):
            print("USAGE: ukpyn ...")

    monkeypatch.setattr(cli, "build_parser", lambda: FakeParser())

    exit_code = cli.main(["unknown"])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "USAGE: ukpyn ..." in captured.out
