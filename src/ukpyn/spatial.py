"""Spatial query module for UK Power Networks data.

Enables users to query across multiple UKPN datasets using geographic bounds.

Usage:
    from ukpyn import spatial

    # Query all UKPN data within a bounding box
    results = spatial.query_bounds(
        bounds={"north": 51.6, "south": 51.4, "east": 0.1, "west": -0.2},
        datasets=["primary_substations", "secondary_sites", "hv_overhead_lines"],
    )

    # Returns dict of {dataset_name: RecordListResponse}
    for name, data in results.items():
        print(f"{name}: {data.total_count} records")
"""

import asyncio
from typing import Any, TypedDict

from .client import UKPNClient
from .config import Config
from .models import RecordListResponse
from .orchestrators.base import _run_sync
from .registry import ALL_DATASETS

# Datasets known to support geo filtering (have geo_shape or geo_point fields)
GEO_ENABLED_DATASETS: set[str] = {
    # GEO datasets - substation areas
    "primary_areas",
    "secondary_areas",
    "grid_areas",
    # GEO datasets - sites
    "secondary_sites",
    "grid_primary_sites",
    "grid_supply_points",
    # GEO datasets - overhead lines
    "hv_overhead_lines",
    "lv_overhead_lines",
    "33kv_overhead_lines",
    "132kv_overhead_lines",
    # GEO datasets - poles
    "hv_poles",
    "lv_poles",
    "33kv_poles",
    "132kv_poles",
    # GEO datasets - boundaries
    "licence_boundaries",
    # LTDS datasets with location
    "projects",
    "infrastructure_projects",
    # DFES with local authority boundaries
    "by_local_authority",
}


class Bounds(TypedDict, total=False):
    """Geographic bounding box."""

    north: float  # Maximum latitude
    south: float  # Minimum latitude
    east: float  # Maximum longitude
    west: float  # Minimum longitude


class SpatialQueryResult(TypedDict):
    """Result of a spatial query across multiple datasets."""

    results: dict[str, RecordListResponse]
    errors: dict[str, str]


def _bounds_to_polygon(bounds: Bounds) -> str:
    """
    Convert bounds dict to WKT polygon string for ODS geofilter.

    Args:
        bounds: Bounding box with north, south, east, west

    Returns:
        Polygon coordinates as "(lon1,lat1),(lon2,lat2),..." format
    """
    north = bounds["north"]
    south = bounds["south"]
    east = bounds["east"]
    west = bounds["west"]

    # ODS expects polygon as: (lon1,lat1),(lon2,lat2),...
    # Clockwise from SW corner
    return f"({west},{south}),({west},{north}),({east},{north}),({east},{south}),({west},{south})"


async def query_bounds_async(
    bounds: Bounds,
    datasets: list[str] | None = None,
    limit: int = 1000,
    api_key: str | None = None,
    config: Config | None = None,
    **kwargs: Any,
) -> SpatialQueryResult:
    """
    Query multiple UKPN datasets within geographic bounds.

    Args:
        bounds: Bounding box with north, south, east, west coordinates (WGS84)
        datasets: List of dataset friendly names to query. If None, queries all
            geo-enabled datasets.
        limit: Maximum records per dataset (default 1000)
        api_key: Optional API key (uses env var if not provided)
        config: Optional Config object
        **kwargs: Additional query parameters passed to each dataset query

    Returns:
        SpatialQueryResult with:
            - results: dict mapping dataset name to RecordListResponse
            - errors: dict mapping dataset name to error message (if any failed)

    Example:
        results = await spatial.query_bounds_async(
            bounds={"north": 51.6, "south": 51.4, "east": 0.1, "west": -0.2},
            datasets=["primary_substations", "secondary_sites"],
        )
        for name, data in results["results"].items():
            print(f"{name}: {data.total_count} records")
    """
    # Default to all geo-enabled datasets
    if datasets is None:
        datasets = list(GEO_ENABLED_DATASETS)

    # Validate datasets exist in registry
    invalid_datasets = [ds for ds in datasets if ds not in ALL_DATASETS]
    if invalid_datasets:
        raise ValueError(
            f"Unknown dataset(s): {invalid_datasets}. "
            f"Available: {list(ALL_DATASETS.keys())}"
        )

    # Convert bounds to polygon filter
    polygon = _bounds_to_polygon(bounds)

    # Create client
    client_kwargs: dict[str, Any] = {}
    if api_key:
        client_kwargs["api_key"] = api_key
    if config:
        client_kwargs["config"] = config

    results: dict[str, RecordListResponse] = {}
    errors: dict[str, str] = {}

    async with UKPNClient(**client_kwargs) as client:
        # Query each dataset in parallel
        async def query_dataset(
            dataset_name: str,
        ) -> tuple[str, RecordListResponse | str]:
            """Query a single dataset and return (name, result_or_error)."""
            try:
                dataset_id = ALL_DATASETS[dataset_name]

                # Make request with geofilter
                response = await client.get_records(
                    dataset_id,
                    limit=limit,
                    geofilter_polygon=polygon,
                    **kwargs,
                )
                return (dataset_name, response)
            except Exception as e:
                return (dataset_name, str(e))

        # Run all queries concurrently
        tasks = [query_dataset(ds) for ds in datasets]
        query_results = await asyncio.gather(*tasks, return_exceptions=False)

        # Separate successes from errors
        for dataset_name, result in query_results:
            if isinstance(result, RecordListResponse):
                results[dataset_name] = result
            else:
                errors[dataset_name] = result

    return SpatialQueryResult(results=results, errors=errors)


def query_bounds(
    bounds: Bounds,
    datasets: list[str] | None = None,
    limit: int = 1000,
    api_key: str | None = None,
    config: Config | None = None,
    **kwargs: Any,
) -> SpatialQueryResult:
    """
    Query multiple UKPN datasets within geographic bounds (synchronous).

    Args:
        bounds: Bounding box with north, south, east, west coordinates (WGS84)
        datasets: List of dataset friendly names to query. If None, queries all
            geo-enabled datasets.
        limit: Maximum records per dataset (default 1000)
        api_key: Optional API key (uses env var if not provided)
        config: Optional Config object
        **kwargs: Additional query parameters passed to each dataset query

    Returns:
        SpatialQueryResult with:
            - results: dict mapping dataset name to RecordListResponse
            - errors: dict mapping dataset name to error message (if any failed)

    Example:
        from ukpyn import spatial

        # Query London area
        london_bounds = {
            "north": 51.7,
            "south": 51.3,
            "east": 0.2,
            "west": -0.5,
        }

        results = spatial.query_bounds(
            bounds=london_bounds,
            datasets=["primary_substations", "secondary_sites"],
        )

        # Access results
        for name, data in results["results"].items():
            print(f"{name}: {data.total_count} records")

        # Check for errors
        if results["errors"]:
            print(f"Errors: {results['errors']}")
    """
    return _run_sync(
        query_bounds_async(
            bounds=bounds,
            datasets=datasets,
            limit=limit,
            api_key=api_key,
            config=config,
            **kwargs,
        )
    )


def list_geo_datasets() -> list[str]:
    """
    List datasets that support spatial filtering.

    Returns:
        List of dataset friendly names that can be used with query_bounds()
    """
    return sorted(GEO_ENABLED_DATASETS)


# Module exports
__all__ = [
    "query_bounds",
    "query_bounds_async",
    "list_geo_datasets",
    "Bounds",
    "SpatialQueryResult",
    "GEO_ENABLED_DATASETS",
]
