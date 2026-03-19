"""GIS orchestrator for geospatial/infrastructure datasets."""

from typing import Any, Literal

from ..models import RecordListResponse
from .base import BaseOrchestrator, _install_module_repr, sync_pair
from .registry import GEO_DATASETS

# Type alias for voltage level parameter
VoltageLevel = Literal["hv", "lv"]


class GISOrchestrator(BaseOrchestrator):
    """
    Orchestrator for UK Power Networks geospatial and infrastructure datasets.

    Provides access to:
    - Primary substation areas
    - Secondary sites
    - High voltage (HV) and Low voltage (LV) overhead lines
    - HV and LV poles

    Example:
        >>> from ukpyn.orchestrators.gis import gis
        >>> # Get all primary substations
        >>> substations = gis.get_primary_substations()
        >>> # Get secondary sites for a specific primary
        >>> sites = gis.get_secondary_sites(primary_substation="EXAMPLE")
        >>> # Export as GeoJSON
        >>> geojson_data = gis.export_geojson("hv_overhead_lines")
    """

    DATASETS = GEO_DATASETS

    # -------------------------------------------------------------------------
    # Primary Substations
    # -------------------------------------------------------------------------

    @sync_pair
    async def get_primary_substations_async(
        self,
        site: str | None = None,
        licence_area: str | None = None,
        limit: int = 100,
        offset: int = 0,
        **kwargs: Any,
    ) -> RecordListResponse:
        """
        Get primary substation areas asynchronously.

        Args:
            site: Filter by site name
            licence_area: Filter by licence area (e.g., Eastern Power Networks (EPN))
            limit: Maximum number of records to return
            offset: Pagination offset
            **kwargs: Additional query parameters

        Returns:
            RecordListResponse containing primary substation data
        """
        refine = {"sitetype": "Primary Substation"}

        if site:
            refine["sitefunctionallocation"] = site
        if licence_area:
            refine["licencearea"] = licence_area

        return await self.get_async(
            dataset="grid-and-primary-sites",
            limit=limit,
            offset=offset,
            refine=refine,
            **kwargs,
        )

    # -------------------------------------------------------------------------
    # Secondary Sites
    # -------------------------------------------------------------------------

    @sync_pair
    async def get_secondary_sites_async(
        self,
        primary_substation: str | None = None,
        limit: int = 100,
        offset: int = 0,
        **kwargs: Any,
    ) -> RecordListResponse:
        """
        Get secondary sites asynchronously.

        Args:
            primary_substation: Filter by parent primary substation field "sitefunctionallocation"
            limit: Maximum number of records to return
            offset: Pagination offset
            **kwargs: Additional query parameters

        Returns:
            RecordListResponse containing secondary site data
        """
        where_clauses: list[str] = []

        if primary_substation:
            where_clauses.append(
                f"primaryfeederfunctionallocation = '{primary_substation}'"
            )

        where = " AND ".join(where_clauses) if where_clauses else None

        return await self.get_async(
            dataset="ukpn-secondary-sites",
            limit=limit,
            offset=offset,
            where=where,
            **kwargs,
        )

    # -------------------------------------------------------------------------
    # Overhead Lines
    # -------------------------------------------------------------------------

    @sync_pair
    async def get_overhead_lines_async(
        self,
        voltage: VoltageLevel = "hv",
        limit: int = 100,
        offset: int = 0,
        **kwargs: Any,
    ) -> RecordListResponse:
        """
        Get overhead lines asynchronously by voltage level.

        Args:
            voltage: Voltage level - 'hv' (high voltage) or 'lv' (low voltage)
            limit: Maximum number of records to return
            offset: Pagination offset
            **kwargs: Additional query parameters

        Returns:
            RecordListResponse containing overhead line data
        """
        dataset = f"{voltage}_overhead_lines"

        return await self.get_async(
            dataset=dataset,
            limit=limit,
            offset=offset,
            **kwargs,
        )

    # -------------------------------------------------------------------------
    # Poles
    # -------------------------------------------------------------------------

    @sync_pair
    async def get_poles_async(
        self,
        voltage: VoltageLevel = "hv",
        limit: int = 100,
        offset: int = 0,
        **kwargs: Any,
    ) -> RecordListResponse:
        """
        Get poles asynchronously by voltage level.

        Args:
            voltage: Voltage level - 'hv' (high voltage) or 'lv' (low voltage)
            limit: Maximum number of records to return
            offset: Pagination offset
            **kwargs: Additional query parameters

        Returns:
            RecordListResponse containing pole data
        """
        dataset = f"{voltage}_poles"

        return await self.get_async(
            dataset=dataset,
            limit=limit,
            offset=offset,
            **kwargs,
        )

    # -------------------------------------------------------------------------
    # GeoJSON Export
    # -------------------------------------------------------------------------

    @sync_pair
    async def export_geojson_async(
        self,
        dataset: str,
        **kwargs: Any,
    ) -> bytes:
        """
        Export dataset as GeoJSON asynchronously.

        Args:
            dataset: Dataset friendly name or ID
            **kwargs: Additional export parameters

        Returns:
            GeoJSON data as bytes
        """
        return await self.export_async(
            dataset=dataset,
            format="geojson",
            **kwargs,
        )

    # -------------------------------------------------------------------------
    # Shapefile Export
    # -------------------------------------------------------------------------

    @sync_pair
    async def export_shapefile_async(
        self,
        dataset: str,
        **kwargs: Any,
    ) -> bytes:
        """
        Export dataset as Shapefile asynchronously.

        Args:
            dataset: Dataset friendly name or ID
            **kwargs: Additional export parameters

        Returns:
            Shapefile data as bytes (ZIP archive)
        """
        return await self.export_async(
            dataset=dataset,
            format="shapefile",
            **kwargs,
        )


# =============================================================================
# Module-level singleton and convenience functions
# =============================================================================

# Module-level singleton instance
_geo_instance: GISOrchestrator | None = None


def _get_geo_instance() -> GISOrchestrator:
    """Get or create the module-level GISOrchestrator singleton."""
    global _geo_instance
    if _geo_instance is None:
        _geo_instance = GISOrchestrator()
    return _geo_instance


def get(
    dataset: str,
    limit: int = 100,
    **kwargs: Any,
) -> RecordListResponse:
    """
    Get records from a geo dataset.

    Convenience function using the module-level singleton.

    Args:
        dataset: Friendly name or dataset ID
        limit: Maximum number of records to return
        **kwargs: Additional query parameters

    Returns:
        RecordListResponse with the data
    """
    return _get_geo_instance().get(dataset, limit=limit, **kwargs)


async def get_async(
    dataset: str,
    limit: int = 100,
    **kwargs: Any,
) -> RecordListResponse:
    """
    Get records from a geo dataset asynchronously.

    Convenience function using the module-level singleton.

    Args:
        dataset: Friendly name or dataset ID
        limit: Maximum number of records to return
        **kwargs: Additional query parameters

    Returns:
        RecordListResponse with the data
    """
    return await _get_geo_instance().get_async(dataset, limit=limit, **kwargs)


def get_primary_substations(
    licence_area: str | None = None,
    limit: int = 100,
    **kwargs: Any,
) -> RecordListResponse:
    """
    Get primary substation areas.

    Convenience function using the module-level singleton.

    Args:
        licence_area: Filter by licence area (e.g., 'EPN', 'SPN', 'LPN')
        limit: Maximum number of records to return
        **kwargs: Additional query parameters

    Returns:
        RecordListResponse containing primary substation data
    """
    return _get_geo_instance().get_primary_substations(
        licence_area=licence_area,
        limit=limit,
        **kwargs,
    )


def get_secondary_sites(
    primary_substation: str | None = None,
    limit: int = 100,
    **kwargs: Any,
) -> RecordListResponse:
    """
    Get secondary sites.

    Convenience function using the module-level singleton.

    Args:
        primary_substation: Filter by parent primary substation name
        limit: Maximum number of records to return
        **kwargs: Additional query parameters

    Returns:
        RecordListResponse containing secondary site data
    """
    return _get_geo_instance().get_secondary_sites(
        primary_substation=primary_substation,
        limit=limit,
        **kwargs,
    )


def get_overhead_lines(
    voltage: VoltageLevel = "hv",
    limit: int = 100,
    **kwargs: Any,
) -> RecordListResponse:
    """
    Get overhead lines by voltage level.

    Convenience function using the module-level singleton.

    Args:
        voltage: Voltage level - 'hv' (high voltage) or 'lv' (low voltage)
        limit: Maximum number of records to return
        **kwargs: Additional query parameters

    Returns:
        RecordListResponse containing overhead line data
    """
    return _get_geo_instance().get_overhead_lines(
        voltage=voltage,
        limit=limit,
        **kwargs,
    )


def get_poles(
    voltage: VoltageLevel = "hv",
    limit: int = 100,
    **kwargs: Any,
) -> RecordListResponse:
    """
    Get poles by voltage level.

    Convenience function using the module-level singleton.

    Args:
        voltage: Voltage level - 'hv' (high voltage) or 'lv' (low voltage)
        limit: Maximum number of records to return
        **kwargs: Additional query parameters

    Returns:
        RecordListResponse containing pole data
    """
    return _get_geo_instance().get_poles(
        voltage=voltage,
        limit=limit,
        **kwargs,
    )


def export_geojson(
    dataset: str,
    **kwargs: Any,
) -> bytes:
    """
    Export dataset as GeoJSON.

    Convenience function using the module-level singleton.

    Args:
        dataset: Dataset friendly name or ID
        **kwargs: Additional export parameters

    Returns:
        GeoJSON data as bytes
    """
    return _get_geo_instance().export_geojson(dataset, **kwargs)


# Available datasets for this orchestrator
available_datasets: list[str] = list(GEO_DATASETS.keys())


__all__ = [
    # Class
    "GISOrchestrator",
    # Type hints
    "VoltageLevel",
    # Module-level convenience functions
    "get",
    "get_async",
    "get_primary_substations",
    "get_secondary_sites",
    "get_overhead_lines",
    "get_poles",
    "export_geojson",
    # Available datasets
    "available_datasets",
]

_install_module_repr(__name__, "GISOrchestrator", GEO_DATASETS)
