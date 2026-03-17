"""
Curtailment orchestrator.

Provides ergonomic access to curtailment datasets from UK Power Networks.

Usage:
    from ukpyn import curtailment

    # Simple sync access
    data = curtailment.get('events')
    events = curtailment.get_events(start_date='2024-01-01')

    # Async access
    data = await curtailment.get_async('events')
    events = await curtailment.get_events_async(site_id='SITE001')
"""

from datetime import date, datetime
from typing import Any

from ..dataset_registry import CURTAILMENT_DATASETS
from ..models import RecordListResponse
from .base import BaseOrchestrator, _run_sync

# Module-level list of available datasets
available_datasets: list[str] = list(CURTAILMENT_DATASETS.keys())

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


class CurtailmentOrchestrator(BaseOrchestrator):
    """
    Orchestrator for Curtailment datasets.

    Provides access to UK Power Networks curtailment data including
    site-specific curtailment events.

    Available datasets:
        - events: Curtailment events by site
        - site_specific: Alias for events
    """

    DATASETS = CURTAILMENT_DATASETS

    # -------------------------------------------------------------------------
    # Events - Site-Specific Curtailment Events
    # -------------------------------------------------------------------------

    async def get_events_async(
        self,
        site_id: str | None = None,
        start_date: DateInput = None,
        end_date: DateInput = None,
        driver: str | None = None,
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
            driver: Filter by curtailment driver/reason
            limit: Maximum records to return (default 100)
            offset: Pagination offset
            **kwargs: Additional query parameters

        Returns:
            RecordListResponse containing curtailment event records

        Example:
            # Get curtailment events for a specific site
            events = await curtailment.get_events_async(
                site_id='SITE001'
            )

            # Get curtailment events in Q1 2024
            q1_events = await curtailment.get_events_async(
                start_date='2024-01-01',
                end_date='2024-03-31'
            )

            # Get curtailment events by driver
            thermal_events = await curtailment.get_events_async(
                driver='thermal'
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

        if driver is not None:
            # driver maps to 'driver' in the dataset
            where_clauses.append(f"driver = '{driver}'")

        where = " AND ".join(where_clauses) if where_clauses else None

        return await self.get_async(
            dataset="events",
            limit=limit,
            offset=offset,
            where=where,
            **kwargs,
        )

    def get_events(
        self,
        site_id: str | None = None,
        start_date: DateInput = None,
        end_date: DateInput = None,
        driver: str | None = None,
        limit: int = 100,
        offset: int = 0,
        **kwargs: Any,
    ) -> RecordListResponse:
        """
        Synchronous wrapper for get_events_async.

        See get_events_async for full documentation.
        """
        return _run_sync(
            self.get_events_async(
                site_id=site_id,
                start_date=start_date,
                end_date=end_date,
                driver=driver,
                limit=limit,
                offset=offset,
                **kwargs,
            )
        )


# =============================================================================
# Module-level singleton and convenience functions
# =============================================================================

_default_orchestrator: CurtailmentOrchestrator | None = None


def _get_orchestrator() -> CurtailmentOrchestrator:
    """Get or create the default orchestrator instance."""
    global _default_orchestrator
    if _default_orchestrator is None:
        _default_orchestrator = CurtailmentOrchestrator()
    return _default_orchestrator


def get(
    dataset: str,
    limit: int = 100,
    **kwargs: Any,
) -> RecordListResponse:
    """
    Get records from a curtailment dataset.

    Convenience function using the default orchestrator.

    Args:
        dataset: Dataset name ('events', 'site_specific')
        limit: Maximum records to return
        **kwargs: Additional query parameters

    Returns:
        RecordListResponse containing the records

    Example:
        from ukpyn import curtailment
        data = curtailment.get('events', limit=50)
    """
    return _get_orchestrator().get(dataset, limit=limit, **kwargs)


async def get_async(
    dataset: str,
    limit: int = 100,
    **kwargs: Any,
) -> RecordListResponse:
    """
    Get records from a curtailment dataset asynchronously.

    Convenience function using the default orchestrator.

    Args:
        dataset: Dataset name ('events', 'site_specific')
        limit: Maximum records to return
        **kwargs: Additional query parameters

    Returns:
        RecordListResponse containing the records

    Example:
        from ukpyn import curtailment
        data = await curtailment.get_async('events', limit=50)
    """
    return await _get_orchestrator().get_async(dataset, limit=limit, **kwargs)


def get_events(
    site_id: str | None = None,
    start_date: DateInput = None,
    end_date: DateInput = None,
    driver: str | None = None,
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
        driver: Filter by curtailment driver/reason
        limit: Maximum records to return
        **kwargs: Additional query parameters

    Returns:
        RecordListResponse containing curtailment event records

    Example:
        from ukpyn import curtailment
        events = curtailment.get_events(
            site_id='SITE001',
            start_date='2024-01-01'
        )
    """
    return _get_orchestrator().get_events(
        site_id=site_id,
        start_date=start_date,
        end_date=end_date,
        driver=driver,
        limit=limit,
        **kwargs,
    )


def export(
    dataset: str,
    format: str = "csv",
    **kwargs: Any,
) -> bytes:
    """
    Export data from a curtailment dataset.

    Convenience function using the default orchestrator.

    Args:
        dataset: Dataset name ('events', 'site_specific')
        format: Export format ('csv', 'json', 'xlsx', 'geojson', etc.)
        **kwargs: Additional query parameters

    Returns:
        Raw bytes of the exported data

    Example:
        from ukpyn import curtailment
        csv_data = curtailment.export('events', format='csv')
        with open('curtailment_events.csv', 'wb') as f:
            f.write(csv_data)
    """
    return _get_orchestrator().export(dataset, format=format, **kwargs)
