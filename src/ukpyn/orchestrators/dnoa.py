"""DNOA (Distribution Network Options Assessment) orchestrator.

Provides ergonomic access to UKPN Distribution Network Options Assessment data.

Usage:
    from ukpyn.orchestrators import dnoa

    # Simple sync access
    data = dnoa.get('dnoa')
    assessment = dnoa.get_assessment(licence_area='EPN')

    # Async access
    data = await dnoa.get_async('dnoa')
    assessment = await dnoa.get_assessment_async(licence_area='SPN')

    # Export data
    csv_bytes = dnoa.export('dnoa', format='csv')
"""

import asyncio
from typing import Any

from ..models import RecordListResponse
from .base import BaseOrchestrator
from .registry import DNOA_DATASETS


class DNOAOrchestrator(BaseOrchestrator):
    """
    Orchestrator for Distribution Network Options Assessment datasets.

    DNOA provides transparency on network constraints and options
    for addressing them across UK Power Networks' licence areas.
    """

    DATASETS = DNOA_DATASETS

    async def get_assessment_async(
        self,
        licence_area: str | None = None,
        limit: int = 100,
        offset: int = 0,
        order_by: str | None = None,
        **kwargs: Any,
    ) -> RecordListResponse:
        """
        Get DNOA assessment records asynchronously.

        Args:
            licence_area: Filter by licence area (e.g., 'EPN', 'SPN', 'LPN').
                         If None, returns data for all licence areas.
            limit: Maximum number of records to return.
            offset: Pagination offset.
            order_by: Sort field.
            **kwargs: Additional query parameters.

        Returns:
            RecordListResponse containing DNOA assessment records.
        """
        where_clauses = []

        if licence_area is not None:
            where_clauses.append(f"licence_area = '{licence_area}'")

        where = " AND ".join(where_clauses) if where_clauses else None

        return await self.get_async(
            dataset="dnoa",
            limit=limit,
            offset=offset,
            where=where,
            order_by=order_by,
            **kwargs,
        )

    def get_assessment(
        self,
        licence_area: str | None = None,
        limit: int = 100,
        offset: int = 0,
        order_by: str | None = None,
        **kwargs: Any,
    ) -> RecordListResponse:
        """
        Synchronous wrapper for get_assessment_async.

        Args:
            licence_area: Filter by licence area (e.g., 'EPN', 'SPN', 'LPN').
                         If None, returns data for all licence areas.
            limit: Maximum number of records to return.
            offset: Pagination offset.
            order_by: Sort field.
            **kwargs: Additional query parameters.

        Returns:
            RecordListResponse containing DNOA assessment records.
        """
        return asyncio.run(
            self.get_assessment_async(
                licence_area=licence_area,
                limit=limit,
                offset=offset,
                order_by=order_by,
                **kwargs,
            )
        )


# =============================================================================
# Module-level singleton and convenience functions
# =============================================================================

_default_orchestrator: DNOAOrchestrator | None = None


def _get_orchestrator() -> DNOAOrchestrator:
    """Get or create the default DNOA orchestrator instance."""
    global _default_orchestrator
    if _default_orchestrator is None:
        _default_orchestrator = DNOAOrchestrator()
    return _default_orchestrator


def get(
    dataset: str,
    limit: int = 100,
    **kwargs: Any,
) -> RecordListResponse:
    """
    Get records from a DNOA dataset.

    Module-level convenience function using the default orchestrator.

    Args:
        dataset: Friendly name or dataset ID (e.g., 'dnoa', 'options_assessment').
        limit: Maximum number of records to return.
        **kwargs: Additional query parameters.

    Returns:
        RecordListResponse containing the requested records.

    Example:
        >>> from ukpyn.orchestrators import dnoa
        >>> data = dnoa.get('dnoa', limit=50)
    """
    return _get_orchestrator().get(dataset, limit=limit, **kwargs)


async def get_async(
    dataset: str,
    limit: int = 100,
    **kwargs: Any,
) -> RecordListResponse:
    """
    Get records from a DNOA dataset asynchronously.

    Module-level convenience function using the default orchestrator.

    Args:
        dataset: Friendly name or dataset ID (e.g., 'dnoa', 'options_assessment').
        limit: Maximum number of records to return.
        **kwargs: Additional query parameters.

    Returns:
        RecordListResponse containing the requested records.

    Example:
        >>> from ukpyn.orchestrators import dnoa
        >>> data = await dnoa.get_async('dnoa', limit=50)
    """
    return await _get_orchestrator().get_async(dataset, limit=limit, **kwargs)


def get_assessment(
    licence_area: str | None = None,
    limit: int = 100,
    **kwargs: Any,
) -> RecordListResponse:
    """
    Get DNOA assessment records.

    Module-level convenience function using the default orchestrator.

    Args:
        licence_area: Filter by licence area (e.g., 'EPN', 'SPN', 'LPN').
                     If None, returns data for all licence areas.
        limit: Maximum number of records to return.
        **kwargs: Additional query parameters.

    Returns:
        RecordListResponse containing DNOA assessment records.

    Example:
        >>> from ukpyn.orchestrators import dnoa
        >>> epn_data = dnoa.get_assessment(licence_area='EPN')
    """
    return _get_orchestrator().get_assessment(
        licence_area=licence_area,
        limit=limit,
        **kwargs,
    )


def export(
    dataset: str,
    format: str = "csv",
    **kwargs: Any,
) -> bytes:
    """
    Export DNOA dataset data.

    Module-level convenience function using the default orchestrator.

    Args:
        dataset: Friendly name or dataset ID (e.g., 'dnoa', 'options_assessment').
        format: Export format ('csv', 'json', 'xlsx', 'geojson', 'parquet', etc.).
        **kwargs: Additional export parameters.

    Returns:
        Raw bytes of the exported data.

    Example:
        >>> from ukpyn.orchestrators import dnoa
        >>> csv_data = dnoa.export('dnoa', format='csv')
        >>> with open('dnoa_data.csv', 'wb') as f:
        ...     f.write(csv_data)
    """
    return _get_orchestrator().export(dataset, format=format, **kwargs)


# List of available datasets for easy discovery
available_datasets: list[str] = list(DNOA_DATASETS.keys())
