"""
LTDS (Long Term Development Statement) orchestrator.

Provides ergonomic access to LTDS datasets from UK Power Networks.

The Long Term Development Statement (LTDS) is published twice yearly and
contains detailed information about the distribution network including
load data, transformer specifications, generation capacity, and development plans.

Usage:
    from ukpyn import ltds

    # Simple sync access
    data = ltds.get('table_3a')
    table_3a = ltds.get_table_3a(licence_area='EPN')
    transformers = ltds.get_table_2a(licence_area='SPN')
    generation = ltds.get_table_5(fuel_type='Solar')
    projects = ltds.get_projects(local_authority='Cambridge')

    # Async access
    data = await ltds.get_async('table_3a')
    table_3a = await ltds.get_table_3a_async(year=2024)
"""

from typing import Any

from ..models import RecordListResponse
from .base import BaseOrchestrator, _install_module_repr, sync_pair
from .registry import LTDS_DATASETS

# Module-level list of available datasets
available_datasets: list[str] = list(LTDS_DATASETS.keys())


def _merge_where(built: str | None, extra: str | None) -> str | None:
    """Combine an internally-built where clause with a user-supplied one."""
    parts = [p for p in (built, extra) if p]
    if not parts:
        return None
    return " AND ".join(parts)


class LTDSOrchestrator(BaseOrchestrator):
    """
    Orchestrator for LTDS (Long Term Development Statement) datasets.

    LTDS provides information about UK Power Networks' distribution network
    including load data, infrastructure projects, and future development plans.
    Published twice yearly (May and November).

    Available datasets:
        - table_1: Circuit data (lines, cables, parameters)
        - table_2a: 2-winding transformer specifications
        - table_2b: 3-winding transformer specifications
        - table_3a: Observed peak demand at primary substations
        - table_3b: Forecast (true/firm) demand
        - table_4a: Three-phase fault levels
        - table_4b: Earth fault levels
        - table_5: Distributed generation by substation
        - table_6: New connection interest queue
        - table_7: Operational restrictions
        - table_8: Fault data (>95th percentile)
        - projects: Infrastructure development projects
        - cim: IEC Common Information Model representation
    """

    DATASETS = LTDS_DATASETS

    # -------------------------------------------------------------------------
    # Table 1 - Circuit Data
    # -------------------------------------------------------------------------

    @sync_pair
    async def get_table_1_async(
        self,
        licence_area: str | None = None,
        limit: int = 100,
        offset: int = 0,
        **kwargs: Any,
    ) -> RecordListResponse:
        """
        Get Table 1 circuit data asynchronously.

        Table 1 contains circuit parameters including line/cable impedances,
        ratings, and connectivity for 132kV and 33kV circuits.

        Args:
            licence_area: Filter by licence area (e.g., 'EPN', 'SPN', 'LPN')
            limit: Maximum records to return (default 100)
            offset: Pagination offset
            **kwargs: Additional query parameters

        Returns:
            RecordListResponse containing Table 1 records
        """
        refine = {}
        if licence_area is not None:
            refine["licencearea"] = licence_area

        return await self.get_async(
            dataset="table_1",
            limit=limit,
            offset=offset,
            refine=refine if refine else None,
            **kwargs,
        )

    # -------------------------------------------------------------------------
    # Table 2A - Transformer Data (2-winding)
    # -------------------------------------------------------------------------

    @sync_pair
    async def get_table_2a_async(
        self,
        licence_area: str | None = None,
        substation: str | None = None,
        limit: int = 100,
        offset: int = 0,
        **kwargs: Any,
    ) -> RecordListResponse:
        """
        Get Table 2A transformer data (2-winding) asynchronously.

        Table 2A contains transformer information for two-winding transformers
        (1x High Voltage, 1x Low Voltage) at Grid and Primary substations.

        Args:
            licence_area: Filter by licence area (e.g., 'EPN', 'SPN', 'LPN')
            substation: Filter by substation name
            limit: Maximum records to return (default 100)
            offset: Pagination offset
            **kwargs: Additional query parameters

        Returns:
            RecordListResponse containing Table 2A records
        """
        refine = {}
        where_clauses: list[str] = []

        if licence_area is not None:
            refine["licencearea"] = licence_area

        if substation is not None:
            _sub = substation.replace("'", "''")
            where_clauses.append(f"lv_substation='{_sub}'")

        built_where = " AND ".join(where_clauses) if where_clauses else None
        where = _merge_where(built_where, kwargs.pop("where", None))

        return await self.get_async(
            dataset="table_2a",
            limit=limit,
            offset=offset,
            refine=refine if refine else None,
            where=where,
            **kwargs,
        )

    # -------------------------------------------------------------------------
    # Table 2B - Transformer Data (3-winding)
    # -------------------------------------------------------------------------

    @sync_pair
    async def get_table_2b_async(
        self,
        licence_area: str | None = None,
        substation: str | None = None,
        limit: int = 100,
        offset: int = 0,
        **kwargs: Any,
    ) -> RecordListResponse:
        """
        Get Table 2B transformer data (3-winding) asynchronously.

        Table 2B contains transformer information for three-winding transformers
        (1x High Voltage, 2x Low Voltage) at Grid and Primary substations.

        Args:
            licence_area: Filter by licence area (e.g., 'EPN', 'SPN', 'LPN')
            substation: Filter by substation name
            limit: Maximum records to return (default 100)
            offset: Pagination offset
            **kwargs: Additional query parameters

        Returns:
            RecordListResponse containing Table 2B records
        """
        refine = {}
        where_clauses: list[str] = []

        if licence_area is not None:
            refine["licencearea"] = licence_area

        if substation is not None:
            # Table 2B has lv_substation_1 and lv_substation_2 (3-winding transformers)
            _sub = substation.replace("'", "''")
            where_clauses.append(
                f"(lv_substation_1='{_sub}' OR lv_substation_2='{_sub}')"
            )

        built_where = " AND ".join(where_clauses) if where_clauses else None
        where = _merge_where(built_where, kwargs.pop("where", None))

        return await self.get_async(
            dataset="table_2b",
            limit=limit,
            offset=offset,
            refine=refine if refine else None,
            where=where,
            **kwargs,
        )

    # -------------------------------------------------------------------------
    # Table 3A - Observed Peak Demand
    # -------------------------------------------------------------------------

    @sync_pair
    async def get_table_3a_async(
        self,
        licence_area: str | None = None,
        year: int | None = None,
        limit: int = 100,
        offset: int = 0,
        **kwargs: Any,
    ) -> RecordListResponse:
        """
        Get Table 3A observed peak demand data asynchronously.

        Table 3A contains observed load data for primary substations,
        including peak demand values and utilization metrics.

        Args:
            licence_area: Filter by licence area (e.g., 'EPN', 'SPN', 'LPN')
            year: Filter by year of observation
            limit: Maximum records to return (default 100)
            offset: Pagination offset
            **kwargs: Additional query parameters

        Returns:
            RecordListResponse containing Table 3A records
        """
        refine = {}
        where_clauses: list[str] = []

        if licence_area is not None:
            refine["licencearea"] = licence_area

        if year is not None:
            where_clauses.append(f"year = {year}")

        built_where = " AND ".join(where_clauses) if where_clauses else None
        where = _merge_where(built_where, kwargs.pop("where", None))

        return await self.get_async(
            dataset="table_3a",
            limit=limit,
            offset=offset,
            refine=refine if refine else None,
            where=where,
            **kwargs,
        )

    # -------------------------------------------------------------------------
    # Table 3B - Forecast (True/Firm) Demand
    # -------------------------------------------------------------------------

    @sync_pair
    async def get_table_3b_async(
        self,
        licence_area: str | None = None,
        limit: int = 100,
        offset: int = 0,
        **kwargs: Any,
    ) -> RecordListResponse:
        """
        Get Table 3B forecast demand data asynchronously.

        Table 3B contains true/firm demand forecasts at primary substations,
        representing expected future load growth.

        Args:
            licence_area: Filter by licence area (e.g., 'EPN', 'SPN', 'LPN')
            limit: Maximum records to return (default 100)
            offset: Pagination offset
            **kwargs: Additional query parameters

        Returns:
            RecordListResponse containing Table 3B records
        """
        refine = {}
        if licence_area is not None:
            refine["licencearea"] = licence_area

        return await self.get_async(
            dataset="table_3b",
            limit=limit,
            offset=offset,
            refine=refine if refine else None,
            **kwargs,
        )

    # -------------------------------------------------------------------------
    # Table 4A - Three-Phase Fault Levels
    # -------------------------------------------------------------------------

    @sync_pair
    async def get_table_4a_async(
        self,
        licence_area: str | None = None,
        limit: int = 100,
        offset: int = 0,
        **kwargs: Any,
    ) -> RecordListResponse:
        """
        Get Table 4A three-phase fault level data asynchronously.

        Table 4A contains three-phase short-circuit fault levels
        at Grid and Primary substations.

        Args:
            licence_area: Filter by licence area (e.g., 'EPN', 'SPN', 'LPN')
            limit: Maximum records to return (default 100)
            offset: Pagination offset
            **kwargs: Additional query parameters

        Returns:
            RecordListResponse containing Table 4A records
        """
        refine = {}
        if licence_area is not None:
            refine["licencearea"] = licence_area

        return await self.get_async(
            dataset="table_4a",
            limit=limit,
            offset=offset,
            refine=refine if refine else None,
            **kwargs,
        )

    # -------------------------------------------------------------------------
    # Table 4B - Earth Fault Levels
    # -------------------------------------------------------------------------

    @sync_pair
    async def get_table_4b_async(
        self,
        licence_area: str | None = None,
        limit: int = 100,
        offset: int = 0,
        **kwargs: Any,
    ) -> RecordListResponse:
        """
        Get Table 4B earth fault level data asynchronously.

        Table 4B contains earth (single-phase-to-ground) fault levels
        at Grid and Primary substations.

        Args:
            licence_area: Filter by licence area (e.g., 'EPN', 'SPN', 'LPN')
            limit: Maximum records to return (default 100)
            offset: Pagination offset
            **kwargs: Additional query parameters

        Returns:
            RecordListResponse containing Table 4B records
        """
        refine = {}
        if licence_area is not None:
            refine["licencearea"] = licence_area

        return await self.get_async(
            dataset="table_4b",
            limit=limit,
            offset=offset,
            refine=refine if refine else None,
            **kwargs,
        )

    # -------------------------------------------------------------------------
    # Table 7 - Operational Restrictions
    # -------------------------------------------------------------------------

    @sync_pair
    async def get_table_7_async(
        self,
        licence_area: str | None = None,
        limit: int = 100,
        offset: int = 0,
        **kwargs: Any,
    ) -> RecordListResponse:
        """
        Get Table 7 operational restrictions data asynchronously.

        Table 7 contains operational restrictions on the network,
        such as thermal limits, voltage constraints, or equipment limitations.

        Args:
            licence_area: Filter by licence area (e.g., 'EPN', 'SPN', 'LPN')
            limit: Maximum records to return (default 100)
            offset: Pagination offset
            **kwargs: Additional query parameters

        Returns:
            RecordListResponse containing Table 7 records
        """
        refine = {}
        if licence_area is not None:
            refine["licencearea"] = licence_area

        return await self.get_async(
            dataset="table_7",
            limit=limit,
            offset=offset,
            refine=refine if refine else None,
            **kwargs,
        )

    # -------------------------------------------------------------------------
    # Table 8 - Fault Data (>95th Percentile)
    # -------------------------------------------------------------------------

    @sync_pair
    async def get_table_8_async(
        self,
        licence_area: str | None = None,
        limit: int = 100,
        offset: int = 0,
        **kwargs: Any,
    ) -> RecordListResponse:
        """
        Get Table 8 fault data asynchronously.

        Table 8 contains fault data for events exceeding the 95th percentile,
        representing significant fault incidents on the network.

        Args:
            licence_area: Filter by licence area (e.g., 'EPN', 'SPN', 'LPN')
            limit: Maximum records to return (default 100)
            offset: Pagination offset
            **kwargs: Additional query parameters

        Returns:
            RecordListResponse containing Table 8 records
        """
        refine = {}
        if licence_area is not None:
            refine["licencearea"] = licence_area

        return await self.get_async(
            dataset="table_8",
            limit=limit,
            offset=offset,
            refine=refine if refine else None,
            **kwargs,
        )

    # -------------------------------------------------------------------------
    # Infrastructure Projects
    # -------------------------------------------------------------------------

    @sync_pair
    async def get_projects_async(
        self,
        licence_area: str | None = None,
        local_authority: str | None = None,
        expected_start_year: int | None = None,
        limit: int = 100,
        offset: int = 0,
        **kwargs: Any,
    ) -> RecordListResponse:
        """
        Get infrastructure projects data asynchronously.

        Contains information about UK Power Networks infrastructure projects
        including reinforcement schemes, new connections, and upgrades.

        Args:
            licence_area: Filter by licence area (e.g., 'EPN', 'SPN', 'LPN')
            local_authority: Filter by local authority name
            expected_start_year: Filter by expected start year
            limit: Maximum records to return (default 100)
            offset: Pagination offset
            **kwargs: Additional query parameters

        Returns:
            RecordListResponse containing infrastructure project records
        """
        refine = {}
        where_clauses: list[str] = []

        if licence_area is not None:
            refine["dno"] = licence_area

        if local_authority is not None:
            where_clauses.append(f"local_authority LIKE '%{local_authority}%'")

        if expected_start_year is not None:
            where_clauses.append(f"expected_start_year = {expected_start_year}")

        built_where = " AND ".join(where_clauses) if where_clauses else None
        where = _merge_where(built_where, kwargs.pop("where", None))

        return await self.get_async(
            dataset="projects",
            limit=limit,
            offset=offset,
            refine=refine if refine else None,
            where=where,
            **kwargs,
        )

    # -------------------------------------------------------------------------
    # Table 5 - Generation
    # -------------------------------------------------------------------------

    @sync_pair
    async def get_table_5_async(
        self,
        licence_area: str | None = None,
        fuel_type: str | None = None,
        substation: str | None = None,
        limit: int = 100,
        offset: int = 0,
        **kwargs: Any,
    ) -> RecordListResponse:
        """
        Get Table 5 generation data asynchronously.

        Table 5 shows the capacity of existing distributed generation connected
        at each Primary substation, including a snapshot of accepted connections.

        Args:
            licence_area: Filter by licence area (e.g., 'EPN', 'SPN', 'LPN')
            fuel_type: Filter by fuel type (e.g., 'Photovoltaic (>=1MW)', 'Wind')
            substation: Filter by substation name
            limit: Maximum records to return (default 100)
            offset: Pagination offset
            **kwargs: Additional query parameters

        Returns:
            RecordListResponse containing Table 5 records
        """
        refine = {}
        where_clauses: list[str] = []

        if licence_area is not None:
            refine["licencearea"] = licence_area

        if fuel_type is not None:
            where_clauses.append(f"fuel_type LIKE '%{fuel_type}%'")

        if substation is not None:
            _sub = substation.replace("'", "''")
            where_clauses.append(f"substation='{_sub}'")

        built_where = " AND ".join(where_clauses) if where_clauses else None
        where = _merge_where(built_where, kwargs.pop("where", None))

        return await self.get_async(
            dataset="table_5",
            limit=limit,
            offset=offset,
            refine=refine if refine else None,
            where=where,
            **kwargs,
        )

    # -------------------------------------------------------------------------
    # Table 6 - New Connection Interest
    # -------------------------------------------------------------------------

    @sync_pair
    async def get_table_6_async(
        self,
        licence_area: str | None = None,
        substation: str | None = None,
        limit: int = 100,
        offset: int = 0,
        **kwargs: Any,
    ) -> RecordListResponse:
        """
        Get Table 6 new connection interest data asynchronously.

        Table 6 indicates the level of new connections interest at each
        Primary substation, showing queued generation and demand connections.

        Args:
            licence_area: Filter by licence area (e.g., 'EPN', 'SPN', 'LPN')
            substation: Filter by substation name
            limit: Maximum records to return (default 100)
            offset: Pagination offset
            **kwargs: Additional query parameters

        Returns:
            RecordListResponse containing Table 6 records
        """
        refine = {}
        where_clauses: list[str] = []

        if licence_area is not None:
            refine["licencearea"] = licence_area

        if substation is not None:
            _sub = substation.replace("'", "''")
            where_clauses.append(f"substation='{_sub}'")

        built_where = " AND ".join(where_clauses) if where_clauses else None
        where = _merge_where(built_where, kwargs.pop("where", None))

        return await self.get_async(
            dataset="table_6",
            limit=limit,
            offset=offset,
            refine=refine if refine else None,
            where=where,
            **kwargs,
        )

    # -------------------------------------------------------------------------
    # CIM - Common Information Model
    # -------------------------------------------------------------------------

    @sync_pair
    async def get_cim_async(
        self,
        licence_area: str | None = None,  # noqa: ARG002
        limit: int = 100,
        offset: int = 0,
        **kwargs: Any,
    ) -> RecordListResponse:
        """
        Get CIM (Common Information Model) data asynchronously.

        The CIM dataset is an attachment-only "Shared" dataset that requires
        special access. It contains no tabular records — the CIM XML files are
        provided as dataset attachments. This method queries the dataset endpoint
        but will typically return zero records.

        Args:
            licence_area: Ignored — the CIM dataset has no ``licencearea`` field.
                Accepted for call-site compatibility but not sent to the API.
            limit: Maximum records to return (default 100)
            offset: Pagination offset
            **kwargs: Additional query parameters

        Returns:
            RecordListResponse (usually empty for this attachment-only dataset)
        """
        return await self.get_async(
            dataset="cim",
            limit=limit,
            offset=offset,
            **kwargs,
        )


# =============================================================================
# Module-level singleton and convenience functions
# =============================================================================

_default_orchestrator: LTDSOrchestrator | None = None


def _get_orchestrator() -> LTDSOrchestrator:
    """Get or create the default orchestrator instance."""
    global _default_orchestrator
    if _default_orchestrator is None:
        _default_orchestrator = LTDSOrchestrator()
    return _default_orchestrator


def get(
    dataset: str,
    limit: int = 100,
    **kwargs: Any,
) -> RecordListResponse:
    """
    Get records from an LTDS dataset.

    Convenience function using the default orchestrator.

    Args:
        dataset: Dataset name ('table_3a', 'projects', etc.)
        limit: Maximum records to return
        **kwargs: Additional query parameters

    Returns:
        RecordListResponse containing the records

    Example:
        from ukpyn import ltds
        data = ltds.get('table_3a', limit=50)
    """
    return _get_orchestrator().get(dataset, limit=limit, **kwargs)


async def get_async(
    dataset: str,
    limit: int = 100,
    **kwargs: Any,
) -> RecordListResponse:
    """
    Get records from an LTDS dataset asynchronously.

    Convenience function using the default orchestrator.

    Args:
        dataset: Dataset name ('table_3a', 'projects', etc.)
        limit: Maximum records to return
        **kwargs: Additional query parameters

    Returns:
        RecordListResponse containing the records

    Example:
        from ukpyn import ltds
        data = await ltds.get_async('table_3a', limit=50)
    """
    return await _get_orchestrator().get_async(dataset, limit=limit, **kwargs)


def get_table_1(
    licence_area: str | None = None,
    limit: int = 100,
    **kwargs: Any,
) -> RecordListResponse:
    """
    Get Table 1 circuit data.

    Convenience function using the default orchestrator.

    Args:
        licence_area: Filter by licence area (e.g., 'EPN', 'SPN', 'LPN')
        limit: Maximum records to return
        **kwargs: Additional query parameters

    Returns:
        RecordListResponse containing Table 1 records

    Example:
        from ukpyn import ltds
        data = ltds.get_table_1(licence_area='EPN')
    """
    return _get_orchestrator().get_table_1(
        licence_area=licence_area,
        limit=limit,
        **kwargs,
    )


def get_table_2a(
    licence_area: str | None = None,
    substation: str | None = None,
    limit: int = 100,
    **kwargs: Any,
) -> RecordListResponse:
    """
    Get Table 2A transformer data (2-winding).

    Convenience function using the default orchestrator.

    Args:
        licence_area: Filter by licence area (e.g., 'EPN', 'SPN', 'LPN')
        substation: Filter by substation name (searches lv_substation field in the dataset)
        limit: Maximum records to return
        **kwargs: Additional query parameters

    Returns:
        RecordListResponse containing Table 2A records

    Example:
        from ukpyn import ltds
        data = ltds.get_table_2a(licence_area='EPN')
    """
    return _get_orchestrator().get_table_2a(
        licence_area=licence_area,
        substation=substation,
        limit=limit,
        **kwargs,
    )


def get_table_2b(
    licence_area: str | None = None,
    substation: str | None = None,
    limit: int = 100,
    **kwargs: Any,
) -> RecordListResponse:
    """
    Get Table 2B transformer data (3-winding).

    Convenience function using the default orchestrator.

    Args:
        licence_area: Filter by licence area (e.g., 'EPN', 'SPN', 'LPN')
        substation: Filter by substation name (searches lv_substation_1 and lv_substation_2 fields)
        limit: Maximum records to return
        **kwargs: Additional query parameters

    Returns:
        RecordListResponse containing Table 2B records

    Example:
        from ukpyn import ltds
        data = ltds.get_table_2b(licence_area='SPN')
    """
    return _get_orchestrator().get_table_2b(
        licence_area=licence_area,
        substation=substation,
        limit=limit,
        **kwargs,
    )


def get_table_3a(
    licence_area: str | None = None,
    year: int | None = None,
    limit: int = 100,
    **kwargs: Any,
) -> RecordListResponse:
    """
    Get Table 3A observed peak demand data.

    Convenience function using the default orchestrator.

    Args:
        licence_area: Filter by licence area (e.g., 'EPN', 'SPN', 'LPN')
        year: Filter by year of observation
        limit: Maximum records to return
        **kwargs: Additional query parameters

    Returns:
        RecordListResponse containing Table 3A records

    Example:
        from ukpyn import ltds
        data = ltds.get_table_3a(licence_area='EPN', year=2024)
    """
    return _get_orchestrator().get_table_3a(
        licence_area=licence_area,
        year=year,
        limit=limit,
        **kwargs,
    )


def get_table_3b(
    licence_area: str | None = None,
    limit: int = 100,
    **kwargs: Any,
) -> RecordListResponse:
    """
    Get Table 3B forecast demand data.

    Convenience function using the default orchestrator.

    Args:
        licence_area: Filter by licence area (e.g., 'EPN', 'SPN', 'LPN')
        limit: Maximum records to return
        **kwargs: Additional query parameters

    Returns:
        RecordListResponse containing Table 3B records

    Example:
        from ukpyn import ltds
        data = ltds.get_table_3b(licence_area='EPN')
    """
    return _get_orchestrator().get_table_3b(
        licence_area=licence_area,
        limit=limit,
        **kwargs,
    )


def get_table_4a(
    licence_area: str | None = None,
    limit: int = 100,
    **kwargs: Any,
) -> RecordListResponse:
    """
    Get Table 4A three-phase fault level data.

    Convenience function using the default orchestrator.

    Args:
        licence_area: Filter by licence area (e.g., 'EPN', 'SPN', 'LPN')
        limit: Maximum records to return
        **kwargs: Additional query parameters

    Returns:
        RecordListResponse containing Table 4A records

    Example:
        from ukpyn import ltds
        data = ltds.get_table_4a(licence_area='SPN')
    """
    return _get_orchestrator().get_table_4a(
        licence_area=licence_area,
        limit=limit,
        **kwargs,
    )


def get_table_4b(
    licence_area: str | None = None,
    limit: int = 100,
    **kwargs: Any,
) -> RecordListResponse:
    """
    Get Table 4B earth fault level data.

    Convenience function using the default orchestrator.

    Args:
        licence_area: Filter by licence area (e.g., 'EPN', 'SPN', 'LPN')
        limit: Maximum records to return
        **kwargs: Additional query parameters

    Returns:
        RecordListResponse containing Table 4B records

    Example:
        from ukpyn import ltds
        data = ltds.get_table_4b(licence_area='LPN')
    """
    return _get_orchestrator().get_table_4b(
        licence_area=licence_area,
        limit=limit,
        **kwargs,
    )


def get_table_5(
    licence_area: str | None = None,
    fuel_type: str | None = None,
    substation: str | None = None,
    limit: int = 100,
    **kwargs: Any,
) -> RecordListResponse:
    """
    Get Table 5 generation data.

    Convenience function using the default orchestrator.

    Args:
        licence_area: Filter by licence area (e.g., 'EPN', 'SPN', 'LPN')
        fuel_type: Filter by fuel type (e.g., 'Photovoltaic (>=1MW)', 'Wind')
        substation: Filter by substation name
        limit: Maximum records to return
        **kwargs: Additional query parameters

    Returns:
        RecordListResponse containing Table 5 records

    Example:
        from ukpyn import ltds
        solar_gen = ltds.get_table_5(fuel_type='Photovoltaic (>=1MW)')
    """
    return _get_orchestrator().get_table_5(
        licence_area=licence_area,
        fuel_type=fuel_type,
        substation=substation,
        limit=limit,
        **kwargs,
    )


def get_table_6(
    licence_area: str | None = None,
    substation: str | None = None,
    limit: int = 100,
    **kwargs: Any,
) -> RecordListResponse:
    """
    Get Table 6 new connection interest data.

    Convenience function using the default orchestrator.

    Args:
        licence_area: Filter by licence area (e.g., 'EPN', 'SPN', 'LPN')
        substation: Filter by substation name
        limit: Maximum records to return
        **kwargs: Additional query parameters

    Returns:
        RecordListResponse containing Table 6 records

    Example:
        from ukpyn import ltds
        connections = ltds.get_table_6(licence_area='LPN')
    """
    return _get_orchestrator().get_table_6(
        licence_area=licence_area,
        substation=substation,
        limit=limit,
        **kwargs,
    )


def get_table_7(
    licence_area: str | None = None,
    limit: int = 100,
    **kwargs: Any,
) -> RecordListResponse:
    """
    Get Table 7 operational restrictions data.

    Convenience function using the default orchestrator.

    Args:
        licence_area: Filter by licence area (e.g., 'EPN', 'SPN', 'LPN')
        limit: Maximum records to return
        **kwargs: Additional query parameters

    Returns:
        RecordListResponse containing Table 7 records

    Example:
        from ukpyn import ltds
        restrictions = ltds.get_table_7(licence_area='EPN')
    """
    return _get_orchestrator().get_table_7(
        licence_area=licence_area,
        limit=limit,
        **kwargs,
    )


def get_table_8(
    licence_area: str | None = None,
    limit: int = 100,
    **kwargs: Any,
) -> RecordListResponse:
    """
    Get Table 8 fault data (>95th percentile events).

    Convenience function using the default orchestrator.

    Args:
        licence_area: Filter by licence area (e.g., 'EPN', 'SPN', 'LPN')
        limit: Maximum records to return
        **kwargs: Additional query parameters

    Returns:
        RecordListResponse containing Table 8 records

    Example:
        from ukpyn import ltds
        faults = ltds.get_table_8(licence_area='SPN')
    """
    return _get_orchestrator().get_table_8(
        licence_area=licence_area,
        limit=limit,
        **kwargs,
    )


def get_cim(
    licence_area: str | None = None,
    limit: int = 100,
    **kwargs: Any,
) -> RecordListResponse:
    """
    Get CIM (Common Information Model) data.

    Convenience function using the default orchestrator.

    The CIM dataset is an attachment-only "Shared" dataset (no tabular records).
    This will typically return an empty result. Use the UKPN portal to request
    access and download the CIM XML attachments directly.

    Args:
        licence_area: Ignored — kept for compatibility but not sent to the API.
        limit: Maximum records to return
        **kwargs: Additional query parameters

    Returns:
        RecordListResponse (usually empty for this attachment-only dataset)

    Example:
        from ukpyn import ltds
        cim_data = ltds.get_cim()
    """
    return _get_orchestrator().get_cim(
        licence_area=licence_area,
        limit=limit,
        **kwargs,
    )


def get_projects(
    licence_area: str | None = None,
    local_authority: str | None = None,
    expected_start_year: int | None = None,
    limit: int = 100,
    **kwargs: Any,
) -> RecordListResponse:
    """
    Get infrastructure projects data.

    Convenience function using the default orchestrator.

    Args:
        licence_area: Filter by licence area (e.g., 'EPN', 'SPN', 'LPN')
        local_authority: Filter by local authority name
        expected_start_year: Filter by expected start year
        limit: Maximum records to return
        **kwargs: Additional query parameters

    Returns:
        RecordListResponse containing infrastructure project records

    Example:
        from ukpyn import ltds
        epn_projects = ltds.get_projects(licence_area='EPN')
        cambridge_projects = ltds.get_projects(local_authority='Cambridge')
        projects_2024 = ltds.get_projects(expected_start_year=2024)
    """
    return _get_orchestrator().get_projects(
        licence_area=licence_area,
        local_authority=local_authority,
        expected_start_year=expected_start_year,
        limit=limit,
        **kwargs,
    )


def export(
    dataset: str,
    format: str = "csv",
    **kwargs: Any,
) -> bytes:
    """
    Export data from an LTDS dataset.

    Convenience function using the default orchestrator.

    Args:
        dataset: Dataset name ('table_3a', 'projects', etc.)
        format: Export format ('csv', 'json', 'xlsx', 'geojson', etc.)
        **kwargs: Additional query parameters

    Returns:
        Raw bytes of the exported data

    Example:
        from ukpyn import ltds
        csv_data = ltds.export('table_3a', format='csv')
        with open('table_3a.csv', 'wb') as f:
            f.write(csv_data)
    """
    return _get_orchestrator().export(dataset, format=format, **kwargs)


_install_module_repr(__name__, "LTDSOrchestrator", LTDS_DATASETS)
