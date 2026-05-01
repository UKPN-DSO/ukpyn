"""
DFES (Distribution Future Energy Scenarios) orchestrator.

Provides easy access to DFES datasets including network headroom reports.

Usage:
    from ukpyn.orchestrators import dfes

    # Simple sync access
    data = dfes.get('headroom')
    headroom = dfes.get_headroom(scenario='Leading the Way', year=2030)

    # Async access
    data = await dfes.get_async('headroom')
    headroom = await dfes.get_headroom_async(scenario='Consumer Transformation')

    # Export data
    csv_bytes = dfes.export('headroom', format='csv')

    # List available datasets
    print(dfes.available_datasets)
"""

from typing import Any

from ..models import FacetListResponse, RecordListResponse
from .base import BaseOrchestrator, _install_module_repr, sync_pair
from .registry import DFES_DATASETS


class DFESOrchestrator(BaseOrchestrator):
    """
    Orchestrator for DFES (Distribution Future Energy Scenarios) datasets.

    DFES provides long-term scenario-based projections for network planning,
    including network headroom reports that show available capacity under
    different future energy scenarios.

    Available datasets:
        - headroom_report / headroom / nshr: DFES Network Scenario Headroom Report

    Scenarios typically include:
        - Leading the Way
        - Consumer Transformation
        - System Transformation
        - Falling Short
    """

    DATASETS = DFES_DATASETS

    @sync_pair
    async def get_headroom_async(
        self,
        scenario: str | None = None,
        year: int | None = None,
        limit: int = 100,
        offset: int = 0,
        select: str | None = None,
        order_by: str | None = None,
        **kwargs: Any,
    ) -> RecordListResponse:
        """
        Get network headroom data asynchronously.

        The headroom report provides information about available network
        capacity under different DFES scenarios.

        Args:
            scenario: Filter by scenario name (e.g., 'Leading the Way',
                     'Consumer Transformation', 'System Transformation',
                     'Falling Short')
            year: Filter by projection year (e.g., 2030, 2035, 2040)
            limit: Maximum number of records to return
            offset: Pagination offset
            select: Fields to include in response
            order_by: Field to sort by
            **kwargs: Additional query parameters

        Returns:
            RecordListResponse containing headroom records

        Example:
            >>> orchestrator = DFESOrchestrator()
            >>> data = await orchestrator.get_headroom_async(
            ...     scenario='Leading the Way',
            ...     year=2030,
            ...     limit=50
            ... )
        """
        where_clauses: list[str] = []

        if scenario is not None:
            # Escape single quotes in scenario name
            safe_scenario = scenario.replace("'", "''")
            where_clauses.append(f"scenario = '{safe_scenario}'")

        if year is not None:
            where_clauses.append(f"year = {year}")

        # Combine with existing where clause if provided
        existing_where = kwargs.pop("where", None)
        if existing_where:
            where_clauses.append(existing_where)

        where = " AND ".join(where_clauses) if where_clauses else None

        return await self.get_async(
            dataset="headroom",
            limit=limit,
            offset=offset,
            where=where,
            select=select,
            order_by=order_by,
            **kwargs,
        )


# =============================================================================
# Module-level singleton and convenience functions
# =============================================================================

_default_orchestrator: DFESOrchestrator | None = None


def _get_orchestrator() -> DFESOrchestrator:
    """Get or create the default orchestrator instance."""
    global _default_orchestrator
    if _default_orchestrator is None:
        _default_orchestrator = DFESOrchestrator()
    return _default_orchestrator


def get(
    dataset: str,
    limit: int = 100,
    **kwargs: Any,
) -> RecordListResponse:
    """
    Get records from a DFES dataset.

    Convenience function using the default orchestrator.

    Args:
        dataset: Dataset name ('headroom', 'headroom_report', or 'nshr')
        limit: Maximum number of records to return
        **kwargs: Additional query parameters (where, select, order_by, etc.)

    Returns:
        RecordListResponse containing the records

    Example:
        >>> from ukpyn.orchestrators import dfes
        >>> data = dfes.get('headroom', limit=50, where="year = 2030")
    """
    return _get_orchestrator().get(dataset, limit=limit, **kwargs)


async def get_async(
    dataset: str,
    limit: int = 100,
    **kwargs: Any,
) -> RecordListResponse:
    """
    Get records from a DFES dataset asynchronously.

    Convenience function using the default orchestrator.

    Args:
        dataset: Dataset name ('headroom', 'headroom_report', or 'nshr')
        limit: Maximum number of records to return
        **kwargs: Additional query parameters (where, select, order_by, etc.)

    Returns:
        RecordListResponse containing the records

    Example:
        >>> from ukpyn.orchestrators import dfes
        >>> data = await dfes.get_async('headroom', limit=50)
    """
    return await _get_orchestrator().get_async(dataset, limit=limit, **kwargs)


def get_headroom(
    scenario: str | None = None,
    year: int | None = None,
    limit: int = 100,
    **kwargs: Any,
) -> RecordListResponse:
    """
    Get network headroom data.

    Convenience function for accessing DFES network headroom reports.

    Args:
        scenario: Filter by scenario name (e.g., 'Leading the Way',
                 'Consumer Transformation', 'System Transformation',
                 'Falling Short')
        year: Filter by projection year (e.g., 2030, 2035, 2040)
        limit: Maximum number of records to return
        **kwargs: Additional query parameters

    Returns:
        RecordListResponse containing headroom records

    Example:
        >>> from ukpyn.orchestrators import dfes
        >>> data = dfes.get_headroom(scenario='Leading the Way', year=2030)
    """
    return _get_orchestrator().get_headroom(
        scenario=scenario,
        year=year,
        limit=limit,
        **kwargs,
    )


def export(
    dataset: str,
    format: str = "csv",
    **kwargs: Any,
) -> bytes:
    """
    Export DFES dataset data.

    Convenience function for exporting data in various formats.

    Args:
        dataset: Dataset name ('headroom', 'headroom_report', or 'nshr')
        format: Export format ('csv', 'json', 'xlsx', 'geojson', 'parquet')
        **kwargs: Additional export parameters (where, select, etc.)

    Returns:
        bytes: Raw export data in the requested format

    Example:
        >>> from ukpyn.orchestrators import dfes
        >>> csv_data = dfes.export('headroom', format='csv')
        >>> with open('headroom.csv', 'wb') as f:
        ...     f.write(csv_data)
    """
    return _get_orchestrator().export(dataset, format=format, **kwargs)


# List of available datasets for easy reference
available_datasets: list[str] = list(DFES_DATASETS.keys())

def get_facets(dataset: str) -> FacetListResponse:
    """
    Get facet values for a DFES dataset.

    Convenience function using the default orchestrator.

    Args:
        dataset: Dataset name

    Returns:
        FacetListResponse containing facet groups and their values.
    """
    return _get_orchestrator().get_facets(dataset)


_install_module_repr(__name__, "DFESOrchestrator", DFES_DATASETS)
