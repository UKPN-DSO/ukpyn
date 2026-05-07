"""
Flexibility orchestrator.

Provides ergonomic access to flexibility market datasets from UK Power Networks.

Usage:
    from ukpyn import flexibility

    # Simple sync access
    data = flexibility.get('dispatches')
    dispatches = flexibility.get_dispatches(start_date='2024-01-01')
    curtailment = flexibility.get_curtailment(site_id='SITE001')

    # Async access
    data = await flexibility.get_async('dispatches')
    dispatches = await flexibility.get_dispatches_async(product='SECURE')
"""

from datetime import date, datetime
from typing import Any

from ..models import FacetListResponse, RecordListResponse
from .base import BaseOrchestrator, _install_module_repr, sync_pair
from .registry import FLEXIBILITY_DATASETS

# Module-level list of available datasets
available_datasets: list[str] = list(FLEXIBILITY_DATASETS.keys())

# Type alias for date inputs
DateInput = str | date | datetime | None


def _format_date_for_where(value: DateInput) -> str | None:
    """
    Convert a date input to ISO format string for ODSQL where clauses.

    Args:
        value: Date as string (ISO format), date, datetime, or None

    Returns:
        ISO format date string or None if input is None

    Raises:
        ValueError: If string format is invalid
    """
    if value is None:
        return None

    if isinstance(value, datetime):
        return value.date().isoformat()

    if isinstance(value, date):
        return value.isoformat()

    if isinstance(value, str):
        # Validate string format by parsing it
        try:
            # Try parsing as date first
            parsed = datetime.strptime(value, "%Y-%m-%d")
            return parsed.date().isoformat()
        except ValueError:
            try:
                # Try parsing as datetime
                parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
                return parsed.date().isoformat()
            except ValueError as err:
                raise ValueError(
                    f"Invalid date format: '{value}'. "
                    "Expected ISO format (YYYY-MM-DD) or datetime string."
                ) from err

    raise TypeError(f"Expected str, date, or datetime, got {type(value).__name__}")


class FlexibilityOrchestrator(BaseOrchestrator):
    """
    Orchestrator for Flexibility market datasets.

    Provides access to UK Power Networks flexibility services data including
    dispatch events and curtailment records.

    Available datasets:
        - dispatches: Flexibility dispatch events
        - dispatch_events: Alias for dispatches
        - curtailment: Curtailment events by site
        - curtailment_events: Alias for curtailment
    """

    DATASETS = FLEXIBILITY_DATASETS

    # -------------------------------------------------------------------------
    # Dispatches - Flexibility Dispatch Events
    # -------------------------------------------------------------------------

    @sync_pair
    async def get_dispatches_async(
        self,
        start_date: DateInput = None,
        end_date: DateInput = None,
        product: str | None = None,
        limit: int = 100,
        offset: int = 0,
        **kwargs: Any,
    ) -> RecordListResponse:
        """
        Get flexibility dispatch events asynchronously.

        Dispatch events represent instances where flexibility services
        were called upon by UK Power Networks.

        Args:
            start_date: Filter by start date (inclusive).
                Accepts ISO format string, date, or datetime.
            end_date: Filter by end date (inclusive).
                Accepts ISO format string, date, or datetime.
            product: Filter by flexibility service type
                (e.g., 'STOR', 'DSR', 'ANM')
            limit: Maximum records to return (default 100)
            offset: Pagination offset
            **kwargs: Additional query parameters

        Returns:
            RecordListResponse containing dispatch event records

        Example:
            # Get all dispatches in January 2024
            dispatches = await flexibility.get_dispatches_async(
                start_date='2024-01-01',
                end_date='2024-01-31'
            )

            # Get STOR dispatches
            stor_dispatches = await flexibility.get_dispatches_async(
                product='STOR'
            )
        """
        where_clauses: list[str] = []

        if start_date is not None:
            formatted_start = _format_date_for_where(start_date)
            where_clauses.append(f"start_time_local >= '{formatted_start}'")

        if end_date is not None:
            formatted_end = _format_date_for_where(end_date)
            where_clauses.append(f"start_time_local <= '{formatted_end}'")

        if product is not None:
            # product maps to 'product' field in the dataset
            where_clauses.append(f"product = '{product}'")

        where = " AND ".join(where_clauses) if where_clauses else None

        return await self.get_async(
            dataset="dispatches",
            limit=limit,
            offset=offset,
            where=where,
            **kwargs,
        )

    # -------------------------------------------------------------------------
    # Curtailment - Site-Specific Curtailment Events
    # -------------------------------------------------------------------------

    @sync_pair
    async def get_curtailment_async(
        self,
        site_id: str | None = None,
        start_date: DateInput = None,
        end_date: DateInput = None,
        limit: int = 100,
        offset: int = 0,
        **kwargs: Any,
    ) -> RecordListResponse:
        """
        Get curtailment events asynchronously.

        Curtailment events represent instances where generation or demand
        was curtailed at specific sites.

        Args:
            site_id: Filter by site identifier
            start_date: Filter by start date (inclusive).
                Accepts ISO format string, date, or datetime.
            end_date: Filter by end date (inclusive).
                Accepts ISO format string, date, or datetime.
            limit: Maximum records to return (default 100)
            offset: Pagination offset
            **kwargs: Additional query parameters

        Returns:
            RecordListResponse containing curtailment event records

        Example:
            # Get curtailment events for a specific site
            events = await flexibility.get_curtailment_async(
                site_id='SITE001'
            )

            # Get curtailment events in Q1 2024
            q1_events = await flexibility.get_curtailment_async(
                start_date='2024-01-01',
                end_date='2024-03-31'
            )
        """
        where_clauses: list[str] = []

        if site_id is not None:
            # site_id maps to 'der_name' (distributed energy resource name) in the dataset
            where_clauses.append(f"der_name = '{site_id}'")

        if start_date is not None:
            formatted_start = _format_date_for_where(start_date)
            where_clauses.append(f"start_time_local >= '{formatted_start}'")

        if end_date is not None:
            formatted_end = _format_date_for_where(end_date)
            where_clauses.append(f"start_time_local <= '{formatted_end}'")

        where = " AND ".join(where_clauses) if where_clauses else None

        return await self.get_async(
            dataset="curtailment",
            limit=limit,
            offset=offset,
            where=where,
            **kwargs,
        )


# =============================================================================
# Module-level singleton and convenience functions
# =============================================================================

_default_orchestrator: FlexibilityOrchestrator | None = None


def _get_orchestrator() -> FlexibilityOrchestrator:
    """Get or create the default orchestrator instance."""
    global _default_orchestrator
    if _default_orchestrator is None:
        _default_orchestrator = FlexibilityOrchestrator()
    return _default_orchestrator


def get(
    dataset: str,
    limit: int = 100,
    **kwargs: Any,
) -> RecordListResponse:
    """
    Get records from a flexibility dataset.

    Convenience function using the default orchestrator.

    Args:
        dataset: Dataset name ('dispatches', 'curtailment', etc.)
        limit: Maximum records to return
        **kwargs: Additional query parameters

    Returns:
        RecordListResponse containing the records

    Example:
        from ukpyn import flexibility
        data = flexibility.get('dispatches', limit=50)
    """
    return _get_orchestrator().get(dataset, limit=limit, **kwargs)


async def get_async(
    dataset: str,
    limit: int = 100,
    **kwargs: Any,
) -> RecordListResponse:
    """
    Get records from a flexibility dataset asynchronously.

    Convenience function using the default orchestrator.

    Args:
        dataset: Dataset name ('dispatches', 'curtailment', etc.)
        limit: Maximum records to return
        **kwargs: Additional query parameters

    Returns:
        RecordListResponse containing the records

    Example:
        from ukpyn import flexibility
        data = await flexibility.get_async('dispatches', limit=50)
    """
    return await _get_orchestrator().get_async(dataset, limit=limit, **kwargs)


def get_dispatches(
    start_date: DateInput = None,
    end_date: DateInput = None,
    product: str | None = None,
    limit: int = 100,
    **kwargs: Any,
) -> RecordListResponse:
    """
    Get flexibility dispatch events.

    Convenience function using the default orchestrator.

    Args:
        start_date: Filter by start date (inclusive).
            Accepts ISO format string, date, or datetime.
        end_date: Filter by end date (inclusive).
            Accepts ISO format string, date, or datetime.
        product: Filter by flexibility service type
        limit: Maximum records to return
        **kwargs: Additional query parameters

    Returns:
        RecordListResponse containing dispatch event records

    Example:
        from ukpyn import flexibility
        dispatches = flexibility.get_dispatches(
            start_date='2024-01-01',
            product='STOR'
        )
    """
    return _get_orchestrator().get_dispatches(
        start_date=start_date,
        end_date=end_date,
        product=product,
        limit=limit,
        **kwargs,
    )


def get_curtailment(
    site_id: str | None = None,
    start_date: DateInput = None,
    end_date: DateInput = None,
    limit: int = 100,
    **kwargs: Any,
) -> RecordListResponse:
    """
    Get curtailment events.

    Convenience function using the default orchestrator.

    Args:
        site_id: Filter by site identifier
        start_date: Filter by start date (inclusive).
            Accepts ISO format string, date, or datetime.
        end_date: Filter by end date (inclusive).
            Accepts ISO format string, date, or datetime.
        limit: Maximum records to return
        **kwargs: Additional query parameters

    Returns:
        RecordListResponse containing curtailment event records

    Example:
        from ukpyn import flexibility
        events = flexibility.get_curtailment(
            site_id='SITE001',
            start_date='2024-01-01'
        )
    """
    return _get_orchestrator().get_curtailment(
        site_id=site_id,
        start_date=start_date,
        end_date=end_date,
        limit=limit,
        **kwargs,
    )


def export(
    dataset: str,
    format: str = "csv",
    **kwargs: Any,
) -> bytes:
    """
    Export data from a flexibility dataset.

    Convenience function using the default orchestrator.

    Args:
        dataset: Dataset name ('dispatches', 'curtailment', etc.)
        format: Export format ('csv', 'json', 'xlsx', 'geojson', etc.)
        **kwargs: Additional query parameters

    Returns:
        Raw bytes of the exported data

    Example:
        from ukpyn import flexibility
        csv_data = flexibility.export('dispatches', format='csv')
        with open('dispatches.csv', 'wb') as f:
            f.write(csv_data)
    """
    return _get_orchestrator().export(dataset, format=format, **kwargs)


def get_facets(dataset: str) -> FacetListResponse:
    """
    Get facet values for a flexibility dataset.

    Convenience function using the default orchestrator.

    Args:
        dataset: Dataset name

    Returns:
        FacetListResponse containing facet groups and their values.
    """
    return _get_orchestrator().get_facets(dataset)


_install_module_repr(__name__, "FlexibilityOrchestrator", FLEXIBILITY_DATASETS)
