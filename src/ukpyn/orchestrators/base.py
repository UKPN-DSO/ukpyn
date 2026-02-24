"""Base orchestrator class for dataset-specific modules."""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import Any, TypeVar

from ..client import UKPNClient
from ..config import Config
from ..models import Dataset, RecordListResponse

T = TypeVar("T")


def _run_sync(coro: Any) -> Any:
    """
    Run an async coroutine synchronously.

    Handles both regular Python and Jupyter notebook environments.
    In Jupyter, asyncio.run() fails because there's already a running loop.
    This function detects that case and runs in a separate thread instead.

    Args:
        coro: The coroutine to run

    Returns:
        The result of the coroutine
    """
    try:
        # Check if there's already a running event loop
        asyncio.get_running_loop()
    except RuntimeError:
        # No running loop - use asyncio.run() directly
        return asyncio.run(coro)

    # There's a running loop (e.g., Jupyter) - run in a thread pool
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(asyncio.run, coro)
        return future.result()


class BaseOrchestrator:
    """
    Abstract base class for dataset orchestrators.

    Provides common functionality for wrapping UKPNClient
    with dataset-specific methods and friendly names.

    Subclasses must define DATASETS mapping friendly names to ODP dataset IDs.
    """

    DATASETS: dict[str, str] = {}

    def __init__(
        self,
        api_key: str | None = None,
        client: UKPNClient | None = None,
        config: Config | None = None,
    ) -> None:
        """
        Initialize the orchestrator.

        Args:
            api_key: API key (uses env var if not provided)
            client: Existing UKPNClient to reuse
            config: Config object for client
        """
        self._api_key = api_key
        self._config = config
        self._client = client
        self._owned_client = client is None

    def _get_client(self) -> UKPNClient:
        """Get or create the UKPNClient instance."""
        if self._client is None:
            self._client = UKPNClient(
                api_key=self._api_key,
                config=self._config,
            )
            self._owned_client = True
        return self._client

    def resolve_dataset_id(self, friendly_name: str) -> str:
        """
        Resolve a friendly name to the actual ODP dataset ID.

        Args:
            friendly_name: User-friendly dataset name (e.g., 'table_3a')

        Returns:
            The actual OpenDataSoft dataset ID

        Raises:
            ValueError: If the friendly name is not recognized
        """
        if friendly_name in self.DATASETS.values():
            return friendly_name

        if friendly_name not in self.DATASETS:
            available = ", ".join(self.DATASETS.keys())
            raise ValueError(
                f"Unknown dataset: '{friendly_name}'. "
                f"Available datasets: {available}"
            )
        return self.DATASETS[friendly_name]

    @property
    def available_datasets(self) -> list[str]:
        """List all available friendly dataset names."""
        return list(self.DATASETS.keys())

    async def get_async(
        self,
        dataset: str,
        limit: int = 100,
        offset: int = 0,
        where: str | None = None,
        select: str | None = None,
        order_by: str | None = None,
        **kwargs: Any,
    ) -> RecordListResponse:
        """
        Get records from a dataset asynchronously.

        Args:
            dataset: Friendly name or dataset ID
            limit: Max records to return
            offset: Pagination offset
            where: ODSQL filter expression
            select: Fields to include
            order_by: Sort field
            **kwargs: Additional parameters

        Returns:
            RecordListResponse with the data
        """
        dataset_id = self.resolve_dataset_id(dataset)
        client = self._get_client()

        async with client:
            return await client.get_records(
                dataset_id=dataset_id,
                limit=limit,
                offset=offset,
                where=where,
                select=select,
                order_by=order_by,
                **kwargs,
            )

    def get(
        self,
        dataset: str,
        limit: int = 100,
        **kwargs: Any,
    ) -> RecordListResponse:
        """
        Synchronous wrapper for get_async.

        Works in both regular Python and Jupyter notebook environments.
        """
        return _run_sync(self.get_async(dataset, limit=limit, **kwargs))

    async def export_async(
        self,
        dataset: str,
        format: str = "csv",
        **kwargs: Any,
    ) -> bytes:
        """Export dataset data asynchronously."""
        dataset_id = self.resolve_dataset_id(dataset)
        client = self._get_client()

        async with client:
            return await client.export_data(
                dataset_id=dataset_id,
                format=format,
                **kwargs,
            )

    def export(
        self,
        dataset: str,
        format: str = "csv",
        **kwargs: Any,
    ) -> bytes:
        """Synchronous wrapper for export_async."""
        return _run_sync(self.export_async(dataset, format=format, **kwargs))

    async def get_metadata_async(self, dataset: str) -> Dataset:
        """Get dataset metadata asynchronously."""
        dataset_id = self.resolve_dataset_id(dataset)
        client = self._get_client()

        async with client:
            return await client.get_dataset(dataset_id)

    def get_metadata(self, dataset: str) -> Dataset:
        """Synchronous wrapper for get_metadata_async."""
        return _run_sync(self.get_metadata_async(dataset))
