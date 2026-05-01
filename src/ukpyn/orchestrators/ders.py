"""DERS (Distributed Energy Resources) orchestrator.

Canonical naming for embedded capacity and large demand datasets.

Usage:
    from ukpyn import ders

    # Simple sync access
    data = ders.get('ecr')
    ecr_data = ders.get_embedded_capacity(technology_type='Solar')
    demand = ders.get_large_demand(licence_area='EPN')

    # Async access
    data = await ders.get_async('ecr')
"""

from typing import Any

from ..models import RecordListResponse
from ..registry import DER_DATASETS
from .base import _install_module_repr
from .resources import ResourcesOrchestrator


class DERSOrchestrator(ResourcesOrchestrator):
    """Canonical orchestrator name for DER datasets."""

    DATASETS = DER_DATASETS


# =============================================================================
# Module-level singleton and convenience functions
# =============================================================================

_default_orchestrator: DERSOrchestrator | None = None


def _get_orchestrator() -> DERSOrchestrator:
    """Get or create the default orchestrator instance."""
    global _default_orchestrator
    if _default_orchestrator is None:
        _default_orchestrator = DERSOrchestrator()
    return _default_orchestrator


def get(
    dataset: str,
    limit: int = 100,
    **kwargs: Any,
) -> RecordListResponse:
    """
    Get records from a DER dataset.

    Args:
        dataset: Dataset name ('ecr', 'embedded_capacity', 'large_demand', etc.)
        limit: Maximum records to return
        **kwargs: Additional query parameters

    Returns:
        RecordListResponse containing the records
    """
    return _get_orchestrator().get(dataset, limit=limit, **kwargs)


async def get_async(
    dataset: str,
    limit: int = 100,
    **kwargs: Any,
) -> RecordListResponse:
    """
    Get records from a DER dataset asynchronously.

    Args:
        dataset: Dataset name ('ecr', 'embedded_capacity', 'large_demand', etc.)
        limit: Maximum records to return
        **kwargs: Additional query parameters

    Returns:
        RecordListResponse containing the records
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

    Args:
        technology_type: Filter by technology (e.g., 'Solar', 'Wind', 'Battery')
        licence_area: Filter by licence area (e.g., 'EPN', 'SPN', 'LPN')
        min_capacity_mw: Filter for capacity >= this value in MW
        limit: Maximum records to return
        **kwargs: Additional query parameters

    Returns:
        RecordListResponse containing ECR records
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

    Args:
        licence_area: Filter by licence area (e.g., 'EPN', 'SPN', 'LPN')
        min_demand_mw: Filter for demand >= this value in MW
        limit: Maximum records to return
        **kwargs: Additional query parameters

    Returns:
        RecordListResponse containing large demand records
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
    Export data from a DER dataset.

    Args:
        dataset: Dataset name ('ecr', 'embedded_capacity', 'large_demand', etc.)
        format: Export format ('csv', 'json', 'xlsx', 'geojson', etc.)
        **kwargs: Additional query parameters

    Returns:
        Raw bytes of the exported data
    """
    return _get_orchestrator().export(dataset, format=format, **kwargs)


available_datasets: list[str] = list(DER_DATASETS.keys())

_install_module_repr(__name__, "DERSOrchestrator", DER_DATASETS)
