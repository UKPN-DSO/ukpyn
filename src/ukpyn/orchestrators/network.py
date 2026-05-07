"""Network orchestrator for UK Power Networks operational datasets.

Provides easy access to circuit operational data, network statistics,
and demand profiles through a unified interface.

Usage:
    from ukpyn.orchestrators import network

    # Simple sync access
    data = network.get('132kv_monthly')
    stats = network.get_statistics(year=2024)

    # Async access
    data = await network.get_async('132kv_monthly')

    # Convenience functions
    circuit_data = network.get_circuit_data(voltage='132kv', granularity='monthly')
"""

from typing import Any, Literal

from ..models import FacetListResponse, RecordListResponse
from .base import BaseOrchestrator, _install_module_repr, sync_pair
from .registry import NETWORK_DATASETS

# Type definitions
VoltageLevel = Literal["132kv", "33kv"]
Granularity = Literal["monthly", "half_hourly"]


class NetworkOrchestrator(BaseOrchestrator):
    """
    Orchestrator for network operational datasets.

    Provides methods for accessing:
    - 132kV circuit operational data (monthly and half-hourly)
    - 33kV circuit operational data (monthly)
    - Network statistics
    - Demand profiles

    Example:
        >>> from ukpyn.orchestrators.network import NetworkOrchestrator
        >>> net = NetworkOrchestrator()
        >>> data = net.get_circuit_data(voltage='132kv', granularity='monthly')
    """

    DATASETS = NETWORK_DATASETS

    # =========================================================================
    # Circuit Data Methods
    # =========================================================================

    @sync_pair
    async def get_circuit_data_async(
        self,
        voltage: VoltageLevel = "132kv",
        granularity: Granularity = "monthly",
        circuit_id: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        limit: int = 100,
        offset: int = 0,
        **kwargs: Any,
    ) -> RecordListResponse:
        """
        Get circuit operational data asynchronously.

        Args:
            voltage: Voltage level - '132kv' or '33kv'
            granularity: Data granularity - 'monthly' or 'half_hourly'
                        Note: 33kv only supports 'monthly'
            circuit_id: Optional circuit identifier to filter by
            start_date: Optional start date (ISO format: YYYY-MM-DD)
            end_date: Optional end date (ISO format: YYYY-MM-DD)
            limit: Maximum records to return (default: 100)
            offset: Pagination offset
            **kwargs: Additional query parameters

        Returns:
            RecordListResponse with circuit operational data

        Raises:
            ValueError: If invalid voltage/granularity combination

        Example:
            >>> data = await net.get_circuit_data_async(
            ...     voltage='132kv',
            ...     granularity='monthly',
            ...     start_date='2024-01-01'
            ... )
        """
        # Validate voltage/granularity combination
        if voltage == "33kv" and granularity == "half_hourly":
            raise ValueError(
                "33kV data is only available at monthly granularity. "
                "Use granularity='monthly' for 33kV circuits."
            )

        # Determine dataset based on voltage and granularity
        dataset_key = f"{voltage}_{granularity}"

        # Build where clause
        where_parts = []

        if circuit_id is not None:
            where_parts.append(f"circuit_id = '{circuit_id}'")

        if start_date is not None:
            where_parts.append(f"date >= '{start_date}'")

        if end_date is not None:
            where_parts.append(f"date <= '{end_date}'")

        where_clause = " AND ".join(where_parts) if where_parts else None

        # Merge where clause with any existing where in kwargs
        if where_clause and kwargs.get("where"):
            kwargs["where"] = f"({kwargs['where']}) AND ({where_clause})"
        elif where_clause:
            kwargs["where"] = where_clause

        return await self.get_async(
            dataset=dataset_key,
            limit=limit,
            offset=offset,
            **kwargs,
        )

    # =========================================================================
    # Statistics Methods
    # =========================================================================

    @sync_pair
    async def get_statistics_async(
        self,
        year: int | None = None,
        region: str | None = None,
        limit: int = 100,
        offset: int = 0,
        **kwargs: Any,
    ) -> RecordListResponse:
        """
        Get network statistics data asynchronously.

        Args:
            year: Optional year to filter by
            region: Optional region/licence area to filter by
            limit: Maximum records to return (default: 100)
            offset: Pagination offset
            **kwargs: Additional query parameters

        Returns:
            RecordListResponse with network statistics

        Example:
            >>> stats = await net.get_statistics_async(year=2024, region='EPN')
        """
        where_parts = []

        if year is not None:
            where_parts.append(f"year = {year}")

        if region is not None:
            where_parts.append(f"region = '{region}'")

        where_clause = " AND ".join(where_parts) if where_parts else None

        # Merge where clause with any existing where in kwargs
        if where_clause and kwargs.get("where"):
            kwargs["where"] = f"({kwargs['where']}) AND ({where_clause})"
        elif where_clause:
            kwargs["where"] = where_clause

        return await self.get_async(
            dataset="statistics",
            limit=limit,
            offset=offset,
            **kwargs,
        )

    # =========================================================================
    # Demand Profiles Methods
    # =========================================================================

    @sync_pair
    async def get_demand_profiles_async(
        self,
        profile_class: str | None = None,
        limit: int = 100,
        offset: int = 0,
        **kwargs: Any,
    ) -> RecordListResponse:
        """
        Get demand profile data asynchronously.

        Args:
            profile_class: Optional profile class to filter by
            limit: Maximum records to return (default: 100)
            offset: Pagination offset
            **kwargs: Additional query parameters

        Returns:
            RecordListResponse with demand profile data

        Example:
            >>> profiles = await net.get_demand_profiles_async(profile_class='1')
        """
        where_clause = None

        if profile_class is not None:
            where_clause = f"profile_class = '{profile_class}'"

        # Merge where clause with any existing where in kwargs
        if where_clause and kwargs.get("where"):
            kwargs["where"] = f"({kwargs['where']}) AND ({where_clause})"
        elif where_clause:
            kwargs["where"] = where_clause

        return await self.get_async(
            dataset="demand_profiles",
            limit=limit,
            offset=offset,
            **kwargs,
        )


# =============================================================================
# Module-level singleton
# =============================================================================

_default_orchestrator: NetworkOrchestrator | None = None


def _get_orchestrator() -> NetworkOrchestrator:
    """Get or create the default NetworkOrchestrator singleton."""
    global _default_orchestrator
    if _default_orchestrator is None:
        _default_orchestrator = NetworkOrchestrator()
    return _default_orchestrator


# =============================================================================
# Module-level convenience functions
# =============================================================================

# Available datasets for easy reference
available_datasets: list[str] = list(NETWORK_DATASETS.keys())


def get(
    dataset: str,
    limit: int = 100,
    **kwargs: Any,
) -> RecordListResponse:
    """
    Get records from a network dataset.

    Convenience function using the module-level singleton.

    Args:
        dataset: Dataset name (e.g., '132kv_monthly', 'statistics')
        limit: Maximum records to return
        **kwargs: Additional query parameters

    Returns:
        RecordListResponse with the data

    Example:
        >>> from ukpyn.orchestrators import network
        >>> data = network.get('132kv_monthly', limit=50)
    """
    return _get_orchestrator().get(dataset, limit=limit, **kwargs)


async def get_async(
    dataset: str,
    limit: int = 100,
    **kwargs: Any,
) -> RecordListResponse:
    """
    Get records from a network dataset asynchronously.

    Convenience function using the module-level singleton.

    Args:
        dataset: Dataset name (e.g., '132kv_monthly', 'statistics')
        limit: Maximum records to return
        **kwargs: Additional query parameters

    Returns:
        RecordListResponse with the data

    Example:
        >>> from ukpyn.orchestrators import network
        >>> data = await network.get_async('132kv_monthly', limit=50)
    """
    return await _get_orchestrator().get_async(dataset, limit=limit, **kwargs)


def get_circuit_data(
    voltage: VoltageLevel = "132kv",
    granularity: Granularity = "monthly",
    circuit_id: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    limit: int = 100,
    **kwargs: Any,
) -> RecordListResponse:
    """
    Get circuit operational data.

    Convenience function using the module-level singleton.

    Args:
        voltage: Voltage level - '132kv' or '33kv'
        granularity: Data granularity - 'monthly' or 'half_hourly'
        circuit_id: Optional circuit identifier to filter by
        start_date: Optional start date (ISO format: YYYY-MM-DD)
        end_date: Optional end date (ISO format: YYYY-MM-DD)
        limit: Maximum records to return
        **kwargs: Additional query parameters

    Returns:
        RecordListResponse with circuit data

    Example:
        >>> from ukpyn.orchestrators import network
        >>> data = network.get_circuit_data(voltage='132kv', granularity='monthly')
    """
    return _get_orchestrator().get_circuit_data(
        voltage=voltage,
        granularity=granularity,
        circuit_id=circuit_id,
        start_date=start_date,
        end_date=end_date,
        limit=limit,
        **kwargs,
    )


def get_statistics(
    year: int | None = None,
    region: str | None = None,
    limit: int = 100,
    **kwargs: Any,
) -> RecordListResponse:
    """
    Get network statistics.

    Convenience function using the module-level singleton.

    Args:
        year: Optional year to filter by
        region: Optional region/licence area to filter by
        limit: Maximum records to return
        **kwargs: Additional query parameters

    Returns:
        RecordListResponse with statistics data

    Example:
        >>> from ukpyn.orchestrators import network
        >>> stats = network.get_statistics(year=2024)
    """
    return _get_orchestrator().get_statistics(
        year=year,
        region=region,
        limit=limit,
        **kwargs,
    )


def get_demand_profiles(
    profile_class: str | None = None,
    limit: int = 100,
    **kwargs: Any,
) -> RecordListResponse:
    """
    Get demand profile data.

    Convenience function using the module-level singleton.

    Args:
        profile_class: Optional profile class to filter by
        limit: Maximum records to return
        **kwargs: Additional query parameters

    Returns:
        RecordListResponse with demand profile data

    Example:
        >>> from ukpyn.orchestrators import network
        >>> profiles = network.get_demand_profiles(profile_class='1')
    """
    return _get_orchestrator().get_demand_profiles(
        profile_class=profile_class,
        limit=limit,
        **kwargs,
    )


def export(
    dataset: str,
    format: str = "csv",
    **kwargs: Any,
) -> bytes:
    """
    Export dataset data.

    Convenience function using the module-level singleton.

    Args:
        dataset: Dataset name (e.g., '132kv_monthly', 'statistics')
        format: Export format ('csv', 'json', 'xlsx', etc.)
        **kwargs: Additional export parameters

    Returns:
        Raw bytes of the exported data

    Example:
        >>> from ukpyn.orchestrators import network
        >>> csv_data = network.export('statistics', format='csv')
    """
    return _get_orchestrator().export(dataset, format=format, **kwargs)


def get_facets(dataset: str) -> FacetListResponse:
    """
    Get facet values for a network dataset.

    Convenience function using the default orchestrator.

    Args:
        dataset: Dataset name

    Returns:
        FacetListResponse containing facet groups and their values.
    """
    return _get_orchestrator().get_facets(dataset)


_install_module_repr(__name__, "NetworkOrchestrator", NETWORK_DATASETS)
