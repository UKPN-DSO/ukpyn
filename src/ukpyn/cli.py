"""Command-line interface for ukpyn."""

from __future__ import annotations

import argparse
import asyncio

from . import EXPORT_FORMATS, UKPNClient, __version__
from .dataset_registry import ALL_DATASETS, DOMAIN_MAP


async def _handle_fetch(args, parser):
    dataset = args.dataset
    if dataset is None:
        parser.print_help()
        return 0

    if dataset not in ALL_DATASETS.values():
        print(f"An invalid dataset was provided: {dataset}")
        parser.print_help()
        return 0

    client = UKPNClient()
    output = args.output
    if output:
        output = output.strip(".")
        if output not in EXPORT_FORMATS:
            print(f"An invalid extension was provided: {output}. Please use a valid extensions: {', '.join(EXPORT_FORMATS)}")
            parser.print_help()
            return 0

        output_filename = f"{dataset}.{output}"

        print(f"Exporting {dataset} to {output}")
        exported_dataset = await client.export_data(dataset_id=dataset, format=output)

        with open(output_filename, "wb") as output_file:
            output_file.write(exported_dataset)
        print(f"Successfully wrote to {output_filename}")

        return 0


    print(f"Fetching {dataset}, output: {output}")
    dataset = await client.get_dataset(dataset)
    return dataset.summary()


def _handle_list(args, parser):
    if not args.list_target:
        parser.print_help()
        return 0
    if args.list_target == "datasets":
        if args.domain:
            if args.domain in DOMAIN_MAP:
                domain_filtered_datasets = DOMAIN_MAP[args.domain].values()
                print("\n".join(sorted(domain_filtered_datasets)))
            else:
                valid_domains = ", ".join(sorted(DOMAIN_MAP.keys()))
                print(
                    f"An invalid domain was provided: '{args.domain}'. Please use a valid domain: {valid_domains}"
                )
        else:
            all_datasets = set(ALL_DATASETS.values())
            print("\n".join(sorted(all_datasets)))
    elif args.list_target == "domains":
        all_domains = DOMAIN_MAP.keys()
        print("\n".join(sorted(all_domains)))
    return 0


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

    # -- Fetch --
    fetch_parser = subparsers.add_parser("fetch", help="Fetch records from a dataset")

    fetch_parser.add_argument(
        "dataset", nargs="?", help="Name of the dataset (e.g dispatches, table_3a)"
    )
    fetch_parser.add_argument(
        "--output",
        help="Saves the dataset to a file inferred from the extension (csv, json)",
    )
    fetch_parser.set_defaults(func=_handle_fetch, parser=fetch_parser)

    # -- List --
    list_parser = subparsers.add_parser(
        "list", help="Lists all available datasets or domains"
    )
    list_subparsers = list_parser.add_subparsers(dest="list_target")

    list_datasets_parser = list_subparsers.add_parser(
        "datasets", help="Lists all available datasets"
    )
    list_datasets_parser.add_argument("--domain", help="Filter by domain, e.g ltds")

    list_subparsers.add_parser("domains", help="Lists all available domains")

    list_parser.set_defaults(func=_handle_list, parser=list_parser)

    return parser


def _print_quickstart() -> None:
    print("ukpyn quickstart")
    print("- Beginner: tutorials/01-getting-started.ipynb")
    print("- Data fetching: tutorials/02-fetching-data.ipynb")
    print("- Advanced workflows: tutorials/03-analysis-patterns.ipynb")
    print("- Full docs: README.md")


def fetch_dataset(args) -> None:
    """
    Fetches a specified dataset, allowing for parameters
    to filter the given dataset and specify the output type.

    By default this will present a summary of the dataset.
    """
    if args.dataset:
        print(args.dataset)
    print("Fetch datasets")


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

    if hasattr(args, "func"):
        result = args.func(args, args.parser)
        if asyncio.iscoroutine(result):
            return asyncio.run(result)
        return result

    parser.print_help()
    return 0
