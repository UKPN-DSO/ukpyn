"""Powerflow orchestrator for power flow time series data.

This orchestrator provides access to operational time series data for
power flow analysis. It is a purist time series orchestrator - use LTDS
separately for network topology.

Available time series data:
- 132kV circuits (monthly and half-hourly)
- 33kV circuits (monthly and half-hourly)
- Grid transformers (monthly and half-hourly)
- Primary transformers (monthly and half-hourly)

Often separated by licence area (EPN, SPN, LPN).

Usage:
    from ukpyn.orchestrators import powerflow

    # Get 132kV circuit time series
    data = powerflow.get_circuit_timeseries(voltage='132kv', granularity='half_hourly')

    # Get transformer time series
    data = powerflow.get_transformer_timeseries(
        transformer_type='primary',
        granularity='monthly'
    )

    # Discover available data
    circuits = powerflow.discover_circuits(voltage='132kv')
"""

import asyncio
from typing import Any, Literal

from ..models import RecordListResponse
from .base import BaseOrchestrator
from .registry import POWERFLOW_DATASETS

# Type definitions
VoltageLevel = Literal["132kv", "33kv"]
Granularity = Literal["monthly", "half_hourly"]
TransformerType = Literal["grid", "primary"]


class PowerflowOrchestrator(BaseOrchestrator):
    """
    Orchestrator for power flow time series data.

    Provides pure time series data access for:
    - 132kV and 33kV circuit operational data
    - Grid and primary transformer operational data
    - Both monthly and half-hourly granularity

    For network topology, use the LTDS orchestrator separately.

    Example:
        >>> from ukpyn.orchestrators.powerflow import PowerflowOrchestrator
        >>> pf = PowerflowOrchestrator()
        >>> # Get circuit data
        >>> circuits = pf.get_circuit_timeseries(voltage='132kv')
        >>> # Get transformer data
        >>> transformers = pf.get_transformer_timeseries(transformer_type='primary')
    """

    DATASETS = POWERFLOW_DATASETS

    # =========================================================================
    # Circuit Time Series Methods
    # =========================================================================

    async def get_circuit_timeseries_async(
        self,
        voltage: VoltageLevel = "132kv",
        granularity: Granularity = "monthly",
        circuit_id: str | None = None,
        licence_area: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        limit: int = 1000,
        offset: int = 0,
        **kwargs: Any,
    ) -> RecordListResponse:
        """
        Get circuit time series data.

        Args:
            voltage: Voltage level - '132kv' or '33kv'
            granularity: Data granularity - 'monthly' or 'half_hourly'
            circuit_id: Filter by specific circuit ID
            licence_area: Filter by licence area (EPN, SPN, LPN)
            start_date: Start date (ISO format: YYYY-MM-DD)
            end_date: End date (ISO format: YYYY-MM-DD)
            limit: Maximum records (default 1000)
            offset: Pagination offset
            **kwargs: Additional query parameters

        Returns:
            RecordListResponse with time series data

        Example:
            >>> data = await pf.get_circuit_timeseries_async(
            ...     voltage='132kv',
            ...     granularity='half_hourly',
            ...     start_date='2024-01-01'
            ... )
        """
        dataset_key = f"{voltage}_{granularity}"

        where_parts = []

        if circuit_id is not None:
            where_parts.append(f"circuit_id = '{circuit_id}'")

        if licence_area is not None:
            where_parts.append(f"licence_area = '{licence_area}'")

        if start_date is not None:
            where_parts.append(f"timestamp >= '{start_date}'")

        if end_date is not None:
            where_parts.append(f"timestamp <= '{end_date}'")

        where_clause = " AND ".join(where_parts) if where_parts else None

        if where_clause and kwargs.get("where"):
            kwargs["where"] = f"({kwargs['where']}) AND ({where_clause})"
        elif where_clause:
            kwargs["where"] = where_clause

        if "order_by" not in kwargs:
            kwargs["order_by"] = "timestamp"

        return await self.get_async(
            dataset=dataset_key,
            limit=limit,
            offset=offset,
            **kwargs,
        )

    def get_circuit_timeseries(
        self,
        voltage: VoltageLevel = "132kv",
        granularity: Granularity = "monthly",
        circuit_id: str | None = None,
        licence_area: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        limit: int = 1000,
        **kwargs: Any,
    ) -> RecordListResponse:
        """Synchronous wrapper for get_circuit_timeseries_async."""
        return asyncio.run(
            self.get_circuit_timeseries_async(
                voltage=voltage,
                granularity=granularity,
                circuit_id=circuit_id,
                licence_area=licence_area,
                start_date=start_date,
                end_date=end_date,
                limit=limit,
                **kwargs,
            )
        )

    # =========================================================================
    # Transformer Time Series Methods
    # =========================================================================

    async def get_transformer_timeseries_async(
        self,
        transformer_type: TransformerType = "primary",
        granularity: Granularity = "monthly",
        transformer_id: str | None = None,
        licence_area: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        limit: int = 1000,
        offset: int = 0,
        **kwargs: Any,
    ) -> RecordListResponse:
        """
        Get transformer time series data.

        Args:
            transformer_type: Type - 'grid' or 'primary'
            granularity: Data granularity - 'monthly' or 'half_hourly'
            transformer_id: Filter by specific transformer ID
            licence_area: Filter by licence area (EPN, SPN, LPN)
            start_date: Start date (ISO format: YYYY-MM-DD)
            end_date: End date (ISO format: YYYY-MM-DD)
            limit: Maximum records (default 1000)
            offset: Pagination offset
            **kwargs: Additional query parameters

        Returns:
            RecordListResponse with transformer time series data

        Example:
            >>> data = await pf.get_transformer_timeseries_async(
            ...     transformer_type='grid',
            ...     granularity='monthly'
            ... )
        """
        dataset_key = f"{transformer_type}_{granularity}"

        where_parts = []

        if transformer_id is not None:
            where_parts.append(f"transformer_id = '{transformer_id}'")

        if licence_area is not None:
            where_parts.append(f"licence_area = '{licence_area}'")

        if start_date is not None:
            where_parts.append(f"timestamp >= '{start_date}'")

        if end_date is not None:
            where_parts.append(f"timestamp <= '{end_date}'")

        where_clause = " AND ".join(where_parts) if where_parts else None

        if where_clause and kwargs.get("where"):
            kwargs["where"] = f"({kwargs['where']}) AND ({where_clause})"
        elif where_clause:
            kwargs["where"] = where_clause

        if "order_by" not in kwargs:
            kwargs["order_by"] = "timestamp"

        return await self.get_async(
            dataset=dataset_key,
            limit=limit,
            offset=offset,
            **kwargs,
        )

    def get_transformer_timeseries(
        self,
        transformer_type: TransformerType = "primary",
        granularity: Granularity = "monthly",
        transformer_id: str | None = None,
        licence_area: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        limit: int = 1000,
        **kwargs: Any,
    ) -> RecordListResponse:
        """Synchronous wrapper for get_transformer_timeseries_async."""
        return asyncio.run(
            self.get_transformer_timeseries_async(
                transformer_type=transformer_type,
                granularity=granularity,
                transformer_id=transformer_id,
                licence_area=licence_area,
                start_date=start_date,
                end_date=end_date,
                limit=limit,
                **kwargs,
            )
        )

    # =========================================================================
    # Discovery Methods
    # =========================================================================

    async def discover_circuits_async(
        self,
        voltage: VoltageLevel = "132kv",
        licence_area: str | None = None,
        limit: int = 1000,
        **kwargs: Any,
    ) -> RecordListResponse:
        """
        Discover available circuits from monthly data.

        Use monthly data to find available circuits before
        fetching half-hourly time series.

        Args:
            voltage: Voltage level - '132kv' or '33kv'
            licence_area: Filter by licence area (EPN, SPN, LPN)
            limit: Maximum records
            **kwargs: Additional query parameters

        Returns:
            RecordListResponse with circuit information
        """
        return await self.get_circuit_timeseries_async(
            voltage=voltage,
            granularity="monthly",
            licence_area=licence_area,
            limit=limit,
            **kwargs,
        )

    def discover_circuits(
        self,
        voltage: VoltageLevel = "132kv",
        licence_area: str | None = None,
        limit: int = 1000,
        **kwargs: Any,
    ) -> RecordListResponse:
        """Synchronous wrapper for discover_circuits_async."""
        return asyncio.run(
            self.discover_circuits_async(
                voltage=voltage,
                licence_area=licence_area,
                limit=limit,
                **kwargs,
            )
        )

    async def discover_transformers_async(
        self,
        transformer_type: TransformerType = "primary",
        licence_area: str | None = None,
        limit: int = 1000,
        **kwargs: Any,
    ) -> RecordListResponse:
        """
        Discover available transformers from monthly data.

        Args:
            transformer_type: Type - 'grid' or 'primary'
            licence_area: Filter by licence area (EPN, SPN, LPN)
            limit: Maximum records
            **kwargs: Additional query parameters

        Returns:
            RecordListResponse with transformer information
        """
        return await self.get_transformer_timeseries_async(
            transformer_type=transformer_type,
            granularity="monthly",
            licence_area=licence_area,
            limit=limit,
            **kwargs,
        )

    def discover_transformers(
        self,
        transformer_type: TransformerType = "primary",
        licence_area: str | None = None,
        limit: int = 1000,
        **kwargs: Any,
    ) -> RecordListResponse:
        """Synchronous wrapper for discover_transformers_async."""
        return asyncio.run(
            self.discover_transformers_async(
                transformer_type=transformer_type,
                licence_area=licence_area,
                limit=limit,
                **kwargs,
            )
        )


# =============================================================================
# Module-level singleton
# =============================================================================

_default_orchestrator: PowerflowOrchestrator | None = None


def _get_orchestrator() -> PowerflowOrchestrator:
    """Get or create the default PowerflowOrchestrator singleton."""
    global _default_orchestrator
    if _default_orchestrator is None:
        _default_orchestrator = PowerflowOrchestrator()
    return _default_orchestrator


# =============================================================================
# Module-level convenience functions
# =============================================================================

available_datasets: list[str] = list(POWERFLOW_DATASETS.keys())


def get(
    dataset: str,
    limit: int = 100,
    **kwargs: Any,
) -> RecordListResponse:
    """Get records from a powerflow dataset."""
    return _get_orchestrator().get(dataset, limit=limit, **kwargs)


async def get_async(
    dataset: str,
    limit: int = 100,
    **kwargs: Any,
) -> RecordListResponse:
    """Get records from a powerflow dataset asynchronously."""
    return await _get_orchestrator().get_async(dataset, limit=limit, **kwargs)


def get_circuit_timeseries(
    voltage: VoltageLevel = "132kv",
    granularity: Granularity = "monthly",
    circuit_id: str | None = None,
    licence_area: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    limit: int = 1000,
    **kwargs: Any,
) -> RecordListResponse:
    """
    Get circuit time series data.

    Example:
        >>> from ukpyn.orchestrators import powerflow
        >>> data = powerflow.get_circuit_timeseries(
        ...     voltage='132kv',
        ...     granularity='half_hourly',
        ...     start_date='2024-01-01'
        ... )
    """
    return _get_orchestrator().get_circuit_timeseries(
        voltage=voltage,
        granularity=granularity,
        circuit_id=circuit_id,
        licence_area=licence_area,
        start_date=start_date,
        end_date=end_date,
        limit=limit,
        **kwargs,
    )


def get_transformer_timeseries(
    transformer_type: TransformerType = "primary",
    granularity: Granularity = "monthly",
    transformer_id: str | None = None,
    licence_area: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    limit: int = 1000,
    **kwargs: Any,
) -> RecordListResponse:
    """
    Get transformer time series data.

    Example:
        >>> from ukpyn.orchestrators import powerflow
        >>> data = powerflow.get_transformer_timeseries(
        ...     transformer_type='grid',
        ...     granularity='monthly'
        ... )
    """
    return _get_orchestrator().get_transformer_timeseries(
        transformer_type=transformer_type,
        granularity=granularity,
        transformer_id=transformer_id,
        licence_area=licence_area,
        start_date=start_date,
        end_date=end_date,
        limit=limit,
        **kwargs,
    )


def discover_circuits(
    voltage: VoltageLevel = "132kv",
    licence_area: str | None = None,
    limit: int = 1000,
    **kwargs: Any,
) -> RecordListResponse:
    """Discover available circuits from monthly data."""
    return _get_orchestrator().discover_circuits(
        voltage=voltage,
        licence_area=licence_area,
        limit=limit,
        **kwargs,
    )


def discover_transformers(
    transformer_type: TransformerType = "primary",
    licence_area: str | None = None,
    limit: int = 1000,
    **kwargs: Any,
) -> RecordListResponse:
    """Discover available transformers from monthly data."""
    return _get_orchestrator().discover_transformers(
        transformer_type=transformer_type,
        licence_area=licence_area,
        limit=limit,
        **kwargs,
    )


def export(
    dataset: str,
    format: str = "csv",
    **kwargs: Any,
) -> bytes:
    """Export powerflow dataset data."""
    return _get_orchestrator().export(dataset, format=format, **kwargs)
