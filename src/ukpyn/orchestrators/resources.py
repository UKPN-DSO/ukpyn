"""
Resources (Embedded Capacity / Large Demand) orchestrator.

Provides ergonomic access to resource datasets from UK Power Networks,
including the Embedded Capacity Register (ECR) and Large Demand List (LDL).

Usage:
    from ukpyn import resources

    # Simple sync access
    data = resources.get('embedded_capacity')
    ecr_data = resources.get_embedded_capacity(technology_type='Solar')
    demand = resources.get_large_demand(licence_area='EPN')

    # Async access
    data = await resources.get_async('embedded_capacity')
    ecr_data = await resources.get_embedded_capacity_async(min_capacity_mw=5.0)
"""

from typing import Any

from ..models import RecordListResponse
from ..registry import RESOURCES_DATASETS
from .base import BaseOrchestrator, _install_module_repr, sync_pair

# Module-level list of available datasets
available_datasets: list[str] = list(RESOURCES_DATASETS.keys())


class ResourcesOrchestrator(BaseOrchestrator):
    """
    Orchestrator for Resources (Embedded Capacity / Large Demand) datasets.

    Provides access to UK Power Networks embedded generation and large demand
    connection data, including the Embedded Capacity Register (ECR) and
    Large Demand List (LDL).

    Available datasets:
        - ecr_small: Embedded Capacity Register (small scale)
        - ecr_large: Embedded Capacity Register (large scale)
        - ecr1: Embedded Capacity Register variant 1
        - ecr2: Embedded Capacity Register variant 2
        - embedded_capacity: Full Embedded Capacity Register
        - large_demand: Large Demand List
        - ldl: Alias for large_demand
    """

    DATASETS = RESOURCES_DATASETS

    # -------------------------------------------------------------------------
    # Embedded Capacity Register (ECR)
    # -------------------------------------------------------------------------

    @sync_pair
    async def get_embedded_capacity_async(
        self,
        technology_type: str | None = None,
        licence_area: str | None = None,
        min_capacity_mw: float | None = None,
        limit: int = 100,
        offset: int = 0,
        **kwargs: Any,
    ) -> RecordListResponse:
        """
        Get Embedded Capacity Register data asynchronously.

        The ECR contains information about distributed generation connected
        to UK Power Networks' distribution network, including solar, wind,
        battery storage, and other technologies.

        Args:
            technology_type: Filter by technology (e.g., 'Solar', 'Wind', 'Battery')
            licence_area: Filter by licence area (e.g., 'EPN', 'SPN', 'LPN')
            min_capacity_mw: Filter for capacity >= this value in MW
            limit: Maximum records to return (default 100)
            offset: Pagination offset
            **kwargs: Additional query parameters

        Returns:
            RecordListResponse containing ECR records
        """
        where_clauses: list[str] = []

        if technology_type is not None:
            where_clauses.append(f"technology_type = '{technology_type}'")

        if licence_area is not None:
            where_clauses.append(f"licence_area = '{licence_area}'")

        if min_capacity_mw is not None:
            where_clauses.append(f"capacity_mw >= {min_capacity_mw}")

        where = " AND ".join(where_clauses) if where_clauses else None

        return await self.get_async(
            dataset="embedded_capacity",
            limit=limit,
            offset=offset,
            where=where,
            **kwargs,
        )

    # -------------------------------------------------------------------------
    # Large Demand List (LDL)
    # -------------------------------------------------------------------------

    @sync_pair
    async def get_large_demand_async(
        self,
        licence_area: str | None = None,
        min_demand_mw: float | None = None,
        limit: int = 100,
        offset: int = 0,
        **kwargs: Any,
    ) -> RecordListResponse:
        """
        Get Large Demand List data asynchronously.

        The LDL contains information about large demand customers connected
        to UK Power Networks' distribution network.

        Args:
            licence_area: Filter by licence area (e.g., 'EPN', 'SPN', 'LPN')
            min_demand_mw: Filter for demand >= this value in MW
            limit: Maximum records to return (default 100)
            offset: Pagination offset
            **kwargs: Additional query parameters

        Returns:
            RecordListResponse containing large demand records
        """
        where_clauses: list[str] = []

        if licence_area is not None:
            where_clauses.append(f"licence_area = '{licence_area}'")

        if min_demand_mw is not None:
            where_clauses.append(f"demand_mw >= {min_demand_mw}")

        where = " AND ".join(where_clauses) if where_clauses else None

        return await self.get_async(
            dataset="large_demand",
            limit=limit,
            offset=offset,
            where=where,
            **kwargs,
        )


# =============================================================================
# Module-level singleton and convenience functions
# =============================================================================

_default_orchestrator: ResourcesOrchestrator | None = None


def _get_orchestrator() -> ResourcesOrchestrator:
    """Get or create the default orchestrator instance."""
    global _default_orchestrator
    if _default_orchestrator is None:
        _default_orchestrator = ResourcesOrchestrator()
    return _default_orchestrator


def get(
    dataset: str,
    limit: int = 100,
    **kwargs: Any,
) -> RecordListResponse:
    """
    Get records from a Resources dataset.

    Convenience function using the default orchestrator.

    Args:
        dataset: Dataset name ('embedded_capacity', 'large_demand', etc.)
        limit: Maximum records to return
        **kwargs: Additional query parameters

    Returns:
        RecordListResponse containing the records

    Example:
        from ukpyn import resources
        data = resources.get('embedded_capacity', limit=50)
    """
    return _get_orchestrator().get(dataset, limit=limit, **kwargs)


async def get_async(
    dataset: str,
    limit: int = 100,
    **kwargs: Any,
) -> RecordListResponse:
    """
    Get records from a Resources dataset asynchronously.

    Convenience function using the default orchestrator.

    Args:
        dataset: Dataset name ('embedded_capacity', 'large_demand', etc.)
        limit: Maximum records to return
        **kwargs: Additional query parameters

    Returns:
        RecordListResponse containing the records

    Example:
        from ukpyn import resources
        data = await resources.get_async('embedded_capacity', limit=50)
    """
    return await _get_orchestrator().get_async(dataset, limit=limit, **kwargs)


def get_embedded_capacity(
    technology_type: str | None = None,
    licence_area: str | None = None,
    min_capacity_mw: float | None = None,
    limit: int = 100,
    **kwargs: Any,
) -> RecordListResponse:
    """
    Get Embedded Capacity Register data.

    Convenience function using the default orchestrator.

    Args:
        technology_type: Filter by technology (e.g., 'Solar', 'Wind', 'Battery')
        licence_area: Filter by licence area (e.g., 'EPN', 'SPN', 'LPN')
        min_capacity_mw: Filter for capacity >= this value in MW
        limit: Maximum records to return
        **kwargs: Additional query parameters

    Returns:
        RecordListResponse containing ECR records

    Example:
        from ukpyn import resources
        solar_data = resources.get_embedded_capacity(technology_type='Solar')
    """
    return _get_orchestrator().get_embedded_capacity(
        technology_type=technology_type,
        licence_area=licence_area,
        min_capacity_mw=min_capacity_mw,
        limit=limit,
        **kwargs,
    )


def get_large_demand(
    licence_area: str | None = None,
    min_demand_mw: float | None = None,
    limit: int = 100,
    **kwargs: Any,
) -> RecordListResponse:
    """
    Get Large Demand List data.

    Convenience function using the default orchestrator.

    Args:
        licence_area: Filter by licence area (e.g., 'EPN', 'SPN', 'LPN')
        min_demand_mw: Filter for demand >= this value in MW
        limit: Maximum records to return
        **kwargs: Additional query parameters

    Returns:
        RecordListResponse containing large demand records

    Example:
        from ukpyn import resources
        demand_data = resources.get_large_demand(licence_area='EPN')
    """
    return _get_orchestrator().get_large_demand(
        licence_area=licence_area,
        min_demand_mw=min_demand_mw,
        limit=limit,
        **kwargs,
    )


def export(
    dataset: str,
    format: str = "csv",
    **kwargs: Any,
) -> bytes:
    """
    Export data from a Resources dataset.

    Convenience function using the default orchestrator.

    Args:
        dataset: Dataset name ('embedded_capacity', 'large_demand', etc.)
        format: Export format ('csv', 'json', 'xlsx', 'geojson', etc.)
        **kwargs: Additional query parameters

    Returns:
        Raw bytes of the exported data

    Example:
        from ukpyn import resources
        csv_data = resources.export('embedded_capacity', format='csv')
        with open('ecr.csv', 'wb') as f:
            f.write(csv_data)
    """
    return _get_orchestrator().export(dataset, format=format, **kwargs)


_install_module_repr(__name__, "ResourcesOrchestrator", RESOURCES_DATASETS)
