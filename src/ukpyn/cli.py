"""Command-line interface for ukpyn."""

from __future__ import annotations

import argparse

from . import __version__


def build_parser() -> argparse.ArgumentParser:
    """Build and return the top-level argument parser."""
    parser = argparse.ArgumentParser(
        prog="ukpyn",
        description=(
            "Accessible command-line entry point for UK Power Networks open data."
        ),
    )
    subparsers = parser.add_subparsers(dest="command")

    subparsers.add_parser("version", help="Show installed ukpyn version")
    subparsers.add_parser(
        "quickstart",
        help="Show beginner and advanced getting-started resources",
    )

    return parser


def _print_quickstart() -> None:
    print("ukpyn quickstart")
    print("- Beginner: tutorials/01-getting-started.ipynb")
    print("- Data fetching: tutorials/02-fetching-data.ipynb")
    print("- Advanced workflows: tutorials/03-analysis-patterns.ipynb")
    print("- Full docs: README.md")


def main(argv: list[str] | None = None) -> int:
    """Run the ukpyn CLI."""
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "version":
        print(__version__)
        return 0

    if args.command in (None, "quickstart"):
        _print_quickstart()
        return 0

    parser.print_help()
    return 0
