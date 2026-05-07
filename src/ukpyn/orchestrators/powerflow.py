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

from typing import Any, Literal

from ..models import FacetListResponse, RecordListResponse
from .base import BaseOrchestrator, _install_module_repr, sync_pair
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

    @sync_pair
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
        # Construct base dataset key
        dataset_key = f"{voltage}_{granularity}"

        # Some datasets are split by licence area (e.g., 33kV half-hourly)
        # If the base key doesn't exist, try with licence_area suffix
        dataset_is_area_specific = False
        if dataset_key not in self.DATASETS and licence_area is not None:
            dataset_key_with_area = f"{dataset_key}_{licence_area.lower()}"
            if dataset_key_with_area in self.DATASETS:
                dataset_key = dataset_key_with_area
                dataset_is_area_specific = True
        elif dataset_key not in self.DATASETS:
            # Check if any licence-area-specific versions exist
            area_variants = [f"{dataset_key}_{area}" for area in ["epn", "spn", "lpn"]]
            available_variants = [v for v in area_variants if v in self.DATASETS]
            if available_variants:
                raise ValueError(
                    f"Dataset '{dataset_key}' not found. This data is only available "
                    f"split by licence area. Please specify licence_area parameter. "
                    f"Available: {', '.join(available_variants)}"
                )

        where_parts = []

        if circuit_id is not None:
            where_parts.append(f"circuit_id = '{circuit_id}'")

        # Only filter by licence_area if the dataset has that field
        # Area-specific datasets (e.g., 33kv_half_hourly_epn) don't have it
        if licence_area is not None and not dataset_is_area_specific:
            where_parts.append(f"licence_area = '{licence_area}'")

        if start_date is not None:
            where_parts.append(f"timestamp >= '{start_date}'")

        if end_date is not None:
            where_parts.append(f"timestamp <= '{end_date}'")

        where_clause = " AND ".join(where_parts) if where_parts else None

        # Get user-provided WHERE clause, but strip empty/whitespace-only values
        user_where = kwargs.get("where", "").strip()
        # Also remove empty parentheses patterns
        if user_where in ("", "()", "( )"):
            user_where = None

        if where_clause and user_where:
            kwargs["where"] = f"({user_where}) AND ({where_clause})"
        elif where_clause:
            kwargs["where"] = where_clause
        elif user_where:
            kwargs["where"] = user_where
        else:
            kwargs.pop("where", None)  # Remove if empty

        if "order_by" not in kwargs:
            kwargs["order_by"] = "timestamp"

        return await self.get_async(
            dataset=dataset_key,
            limit=limit,
            offset=offset,
            **kwargs,
        )

    # =========================================================================
    # Transformer Time Series Methods
    # =========================================================================

    @sync_pair
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
        # Construct base dataset key
        dataset_key = f"{transformer_type}_{granularity}"

        # Some datasets are split by licence area (e.g., primary half-hourly)
        # If the base key doesn't exist, try with licence_area suffix
        dataset_is_area_specific = False
        if dataset_key not in self.DATASETS and licence_area is not None:
            dataset_key_with_area = f"{dataset_key}_{licence_area.lower()}"
            if dataset_key_with_area in self.DATASETS:
                dataset_key = dataset_key_with_area
                dataset_is_area_specific = True
        elif dataset_key not in self.DATASETS:
            # Check if any licence-area-specific versions exist
            area_variants = [f"{dataset_key}_{area}" for area in ["epn", "spn", "lpn"]]
            available_variants = [v for v in area_variants if v in self.DATASETS]
            if available_variants:
                raise ValueError(
                    f"Dataset '{dataset_key}' not found. This data is only available "
                    f"split by licence area. Please specify licence_area parameter. "
                    f"Available: {', '.join(available_variants)}"
                )

        where_parts = []

        if transformer_id is not None:
            where_parts.append(f"transformer_id = '{transformer_id}'")

        # Only filter by licence_area if the dataset has that field
        # Area-specific datasets (e.g., primary_half_hourly_epn) don't have it
        if licence_area is not None and not dataset_is_area_specific:
            where_parts.append(f"licence_area = '{licence_area}'")
        if start_date is not None:
            where_parts.append(f"timestamp >= '{start_date}'")

        if end_date is not None:
            where_parts.append(f"timestamp <= '{end_date}'")

        where_clause = " AND ".join(where_parts) if where_parts else None

        # Get user-provided WHERE clause, but strip empty/whitespace-only values
        user_where = kwargs.get("where", "").strip()
        # Also remove empty parentheses patterns
        if user_where in ("", "()", "( )"):
            user_where = None

        if where_clause and user_where:
            kwargs["where"] = f"({user_where}) AND ({where_clause})"
        elif where_clause:
            kwargs["where"] = where_clause
        elif user_where:
            kwargs["where"] = user_where
        else:
            kwargs.pop("where", None)  # Remove if empty

        if "order_by" not in kwargs:
            kwargs["order_by"] = "timestamp"

        return await self.get_async(
            dataset=dataset_key,
            limit=limit,
            offset=offset,
            **kwargs,
        )

    # =========================================================================
    # Substation Time Series Methods
    # =========================================================================

    @sync_pair
    async def get_half_hourly_timeseries_async(
        self,
        substation: str,
        granularity: Granularity = "half_hourly",
        licence_area: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        limit: int = 100000,
        debug: bool = False,
        **kwargs: Any,
    ) -> RecordListResponse:
        """
        Get half-hourly time series data for all transformers at a substation.

        This method automatically:
        1. Queries LTDS Tables 2A and 2B to determine transformer types
        2. Queries monthly powerflow data filtering by ltds_name (substation name)
        3. Extracts tx_id values for the substation's transformers
        4. Retrieves time series data for those specific transformers

        Args:
            substation: Substation name (e.g., 'Addenbrookes Primary 11kV')
            granularity: Data granularity - 'monthly' or 'half_hourly'
            licence_area: Licence area (EPN, SPN, LPN) - auto-detected if not provided
            start_date: Start date (ISO format: YYYY-MM-DD)
            end_date: End date (ISO format: YYYY-MM-DD)
            limit: Maximum records (default 100000)
            debug: If True, print detailed debug information
            **kwargs: Additional query parameters

        Returns:
            RecordListResponse with time series data for all transformers

        Example:
            >>> data = await pf.get_half_hourly_timeseries_async(
            ...     substation='Addenbrookes Primary 11kV',
            ...     granularity='half_hourly',
            ...     start_date='2023-01-01',
            ...     end_date='2024-01-01',
            ...     debug=True
            ... )

        Note:
            The monthly powerflow datasets contain ltds_name field that matches
            the substation name, making direct filtering possible.
        """
        from ..utils.powerflow_helpers import (
            determine_transformer_type,
            extract_lv_nodes_and_voltages,
            parse_voltage,
        )
        from .ltds import _get_orchestrator as _get_ltds_orchestrator

        if debug:
            print(f"\n{'=' * 80}")
            print("[DEBUG] get_half_hourly_timeseries() called")
            print(f"  Substation: {substation}")
            print(f"  Granularity: {granularity}")
            print(f"  Licence Area: {licence_area or 'Auto-detect'}")
            print(f"  Date Range: {start_date} to {end_date}")
            print(f"{'=' * 80}")

        # Step 1: Query LTDS to determine transformer types and licence area
        if debug:
            print(f"\n[STEP 1] Querying LTDS Tables 2A and 2B for '{substation}'")

        ltds = _get_ltds_orchestrator()

        # Query both tables to understand transformer configuration
        table_2a_response = await ltds.get_table_2a_async(
            substation=substation, limit=100
        )
        table_2b_response = await ltds.get_table_2b_async(
            substation=substation, limit=100
        )

        # Determine licence area from LTDS data (if not explicitly provided)
        if not licence_area:
            if table_2a_response.records:
                first_record = table_2a_response.records[0].fields
                licence_area_field = (
                    first_record.get(
                        "licencearea"
                    )  # All lowercase, no underscore (actual field name)
                    or first_record.get(
                        "licence_area"
                    )  # With underscore (some datasets)
                    or first_record.get("license_area")  # US spelling
                    or first_record.get("licence_area_name")  # Alternative name
                )

                if licence_area_field:
                    licence_area_upper = str(licence_area_field).upper()
                    if "EPN" in licence_area_upper or "EASTERN" in licence_area_upper:
                        licence_area = "EPN"
                    elif (
                        "SPN" in licence_area_upper
                        or "SOUTH EASTERN" in licence_area_upper
                    ):
                        licence_area = "SPN"
                    elif "LPN" in licence_area_upper or "LONDON" in licence_area_upper:
                        licence_area = "LPN"

            if not licence_area and table_2b_response.records:
                first_record = table_2b_response.records[0].fields
                licence_area_field = (
                    first_record.get(
                        "licencearea"
                    )  # All lowercase, no underscore (actual field name)
                    or first_record.get(
                        "licence_area"
                    )  # With underscore (some datasets)
                    or first_record.get("license_area")  # US spelling
                    or first_record.get("licence_area_name")  # Alternative name
                )

                if licence_area_field:
                    licence_area_upper = str(licence_area_field).upper()
                    if "EPN" in licence_area_upper or "EASTERN" in licence_area_upper:
                        licence_area = "EPN"
                    elif (
                        "SPN" in licence_area_upper
                        or "SOUTH EASTERN" in licence_area_upper
                    ):
                        licence_area = "SPN"
                    elif "LPN" in licence_area_upper or "LONDON" in licence_area_upper:
                        licence_area = "LPN"

            if not licence_area:
                raise ValueError(
                    f"Could not determine licence area for substation '{substation}'. "
                    "Please specify licence_area explicitly (EPN, SPN, or LPN)."
                )

        if debug:
            print(f"  Licence area: {licence_area}")

        # Step 2: Determine transformer types from voltage levels
        if debug:
            print("\n[STEP 2] Determining transformer types from LTDS data")

        nodes_info = extract_lv_nodes_and_voltages(
            table_2a_response, table_2b_response, debug=False
        )

        if not nodes_info:
            if debug:
                print(f"[WARNING] No transformers found in LTDS for '{substation}'")
            return RecordListResponse(records=[], total_count=0)

        # Determine which transformer types we need (grid vs primary)
        has_grid = False
        has_primary = False

        for node_info in nodes_info:
            voltage = parse_voltage(node_info["voltage_lv"])
            if voltage is not None:
                tx_type = determine_transformer_type(voltage)
                if tx_type == "grid":
                    has_grid = True
                else:
                    has_primary = True

        if debug:
            print(
                f"  Transformer types: {'grid ' if has_grid else ''}{'primary' if has_primary else ''}"
            )

        # Step 3: Query monthly powerflow data filtering by ltds_name
        if debug:
            print(f"\n[STEP 3] Querying monthly data for ltds_name = '{substation}'")

        all_tx_ids = set()

        # Query grid monthly if needed (with pagination)
        if has_grid:
            if debug:
                print("  Querying grid monthly data...")

            offset = 0
            total_records = 0
            while True:
                grid_monthly = await self.get_transformer_timeseries_async(
                    transformer_type="grid",
                    granularity="monthly",
                    licence_area=licence_area,
                    limit=100,  # API max is 100
                    offset=offset,
                    where=f"ltds_name = '{substation}'",
                )

                # Extract unique tx_id values
                for record in grid_monthly.records:
                    tx_id = record.fields.get("tx_id")
                    if tx_id:
                        all_tx_ids.add(tx_id)

                total_records += len(grid_monthly.records)

                # Break if we got fewer records than requested (no more data)
                if len(grid_monthly.records) < 100:
                    break

                offset += 100

            if debug:
                print(
                    f"    Found {total_records} records, {len(all_tx_ids)} unique tx_ids"
                )

        # Query primary monthly if needed (with pagination)
        if has_primary:
            if debug:
                print("  Querying primary monthly data...")

            grid_count = len(all_tx_ids)
            offset = 0
            total_records = 0
            while True:
                primary_monthly = await self.get_transformer_timeseries_async(
                    transformer_type="primary",
                    granularity="monthly",
                    licence_area=licence_area,
                    limit=100,  # API max is 100
                    offset=offset,
                    where=f"ltds_name = '{substation}'",
                )

                # Extract unique tx_id values
                for record in primary_monthly.records:
                    tx_id = record.fields.get("tx_id")
                    if tx_id:
                        all_tx_ids.add(tx_id)

                total_records += len(primary_monthly.records)

                # Break if we got fewer records than requested (no more data)
                if len(primary_monthly.records) < 100:
                    break

                offset += 100

            if debug:
                print(
                    f"    Found {total_records} records, {len(all_tx_ids) - grid_count} unique tx_ids"
                )

        if not all_tx_ids:
            if debug:
                print(f"\n[WARNING] No transformer IDs found for '{substation}'")
            return RecordListResponse(records=[], total_count=0)

        if debug:
            print(f"\n  Total unique transformer IDs: {len(all_tx_ids)}")
            print(f"  tx_ids: {list(all_tx_ids)}")

        # Step 4: Query time series data for the specific tx_ids
        if debug:
            print(
                f"\n[STEP 4] Fetching {granularity} data for {len(all_tx_ids)} transformers"
            )

        # Build WHERE clause for tx_ids
        tx_id_list = list(all_tx_ids)
        tx_id_conditions = [f"tx_id = '{tx_id}'" for tx_id in tx_id_list]
        tx_id_where = "(" + " OR ".join(tx_id_conditions) + ")"

        # Add date filters
        where_parts = [tx_id_where]
        if start_date:
            where_parts.append(f"timestamp >= '{start_date}'")
        if end_date:
            where_parts.append(f"timestamp <= '{end_date}'")

        where_clause = " AND ".join(where_parts)

        # Merge with user-provided WHERE clause
        if kwargs.get("where"):
            where_clause = f"({kwargs['where']}) AND ({where_clause})"

        kwargs_copy = kwargs.copy()
        kwargs_copy.pop("limit", None)
        kwargs_copy.pop("offset", None)
        kwargs_copy["where"] = where_clause

        # Fetch data with pagination
        page_size = min(limit, 100)
        all_records = []

        # Fetch grid data if needed
        # NOTE: Don't pass licence_area here - we're already filtering by specific tx_ids
        # which implicitly limits to the correct area. Some datasets (grid_half_hourly)
        # don't have licence_area field even though they're documented as "combined".
        if has_grid:
            offset = 0
            while True:
                grid_response = await self.get_transformer_timeseries_async(
                    transformer_type="grid",
                    granularity=granularity,
                    licence_area=None,  # Don't filter by area - tx_ids are already area-specific
                    limit=page_size,
                    offset=offset,
                    **kwargs_copy,
                )
                all_records.extend(grid_response.records)

                if debug:
                    print(
                        f"  Fetched {len(grid_response.records)} grid records (offset {offset})"
                    )

                if len(grid_response.records) < page_size:
                    break
                offset += page_size

        # Fetch primary data if needed
        if has_primary:
            offset = 0
            while True:
                primary_response = await self.get_transformer_timeseries_async(
                    transformer_type="primary",
                    granularity=granularity,
                    licence_area=licence_area,  # Primary datasets ARE split by area
                    limit=page_size,
                    offset=offset,
                    **kwargs_copy,
                )
                all_records.extend(primary_response.records)

                if debug:
                    print(
                        f"  Fetched {len(primary_response.records)} primary records (offset {offset})"
                    )

                if len(primary_response.records) < page_size:
                    break
                offset += page_size

        if debug:
            print(f"\n[COMPLETE] Retrieved {len(all_records)} total records")

        return RecordListResponse(records=all_records, total_count=len(all_records))

    # =========================================================================
    # Discovery Methods
    # =========================================================================

    @sync_pair
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

    @sync_pair
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


def get_half_hourly_timeseries(
    substation: str,
    granularity: Granularity = "half_hourly",
    licence_area: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    limit: int = 100000,
    debug: bool = False,
    **kwargs: Any,
) -> RecordListResponse:
    """
    Get half-hourly time series data for all transformers at a substation.

    This convenience function automatically:
    1. Queries LTDS Tables 2A and 2B to determine transformer types
    2. Queries monthly powerflow data filtering by ltds_name (substation name)
    3. Extracts tx_id values for the substation's transformers
    4. Retrieves time series data for those specific transformers

    Args:
        substation: Substation name (e.g., 'Addenbrookes Primary 11kV')
        granularity: Data granularity - 'monthly' or 'half_hourly'
        licence_area: Licence area (EPN, SPN, LPN) - auto-detected if not provided
        start_date: Start date (ISO format: YYYY-MM-DD)
        end_date: End date (ISO format: YYYY-MM-DD)
        limit: Maximum records (default 100000)
        debug: If True, print detailed debug information
        **kwargs: Additional query parameters

    Returns:
        RecordListResponse with time series data for all transformers

    Example:
        >>> from ukpyn.orchestrators import powerflow
        >>> data = powerflow.get_half_hourly_timeseries(
        ...     substation='Addenbrookes Primary 11kV',
        ...     granularity='half_hourly',
        ...     licence_area='EPN',
        ...     start_date='2023-01-01',
        ...     end_date='2024-01-01',
        ...     debug=True
        ... )

    Note:
        Monthly powerflow datasets contain ltds_name field for direct filtering.
        Requires LTDS orchestrator to determine transformer configuration.
    """
    return _get_orchestrator().get_half_hourly_timeseries(
        substation=substation,
        granularity=granularity,
        licence_area=licence_area,
        start_date=start_date,
        end_date=end_date,
        limit=limit,
        debug=debug,
        **kwargs,
    )


def get_facets(dataset: str) -> FacetListResponse:
    """
    Get facet values for a powerflow dataset.

    Convenience function using the default orchestrator.

    Args:
        dataset: Dataset name

    Returns:
        FacetListResponse containing facet groups and their values.
    """
    return _get_orchestrator().get_facets(dataset)


_install_module_repr(__name__, "PowerflowOrchestrator", POWERFLOW_DATASETS)
