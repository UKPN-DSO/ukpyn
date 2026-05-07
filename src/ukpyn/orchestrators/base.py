"""Base orchestrator class for dataset-specific modules."""

import asyncio
import sys
import types
from concurrent.futures import ThreadPoolExecutor
from typing import Any, TypeVar

from ..client import UKPNClient
from ..config import Config
from ..models import Dataset, FacetListResponse, RecordListResponse

T = TypeVar("T")

# ── Licence area abbreviation resolver ──────────────────────────────────
_LICENCE_AREA_ALIASES: dict[str, str] = {
    "EPN": "Eastern Power Networks (EPN)",
    "SPN": "South Eastern Power Networks (SPN)",
    "LPN": "London Power Networks (LPN)",
}


def resolve_licence_area(value: str | None) -> str | None:
    """Expand a licence-area abbreviation to the full API value.

    Accepts ``'EPN'``, ``'SPN'``, or ``'LPN'`` (case-insensitive) and
    returns the full string the UKPN API expects.  If *value* is already
    the full string or is ``None``, it is returned unchanged.
    """
    if value is None:
        return None
    return _LICENCE_AREA_ALIASES.get(value.strip().upper(), value)


def validate_licence_area_abbrev(value: str | None) -> str | None:
    """Ensure *value* is a recognised licence-area abbreviation.

    Raises :class:`ValueError` with a helpful message when the caller
    passes a full name instead of the short code.
    """
    if value is None:
        return None
    stripped = value.strip().upper()
    if stripped in _LICENCE_AREA_ALIASES:
        return stripped
    # Check if the user passed the full name
    _reverse = {v.upper(): k for k, v in _LICENCE_AREA_ALIASES.items()}
    abbrev = _reverse.get(stripped)
    if abbrev is not None:
        raise ValueError(
            f"This dataset uses abbreviated licence areas. "
            f"Use '{abbrev}' instead of '{value}'."
        )
    return value


def sync_pair(async_method: Any) -> Any:
    """Decorator that auto-generates a sync wrapper for an async orchestrator method.

    The sync method is named by stripping the ``_async`` suffix.  It is
    installed automatically on the class via
    ``BaseOrchestrator.__init_subclass__``.

    Usage::

        class MyOrchestrator(BaseOrchestrator):
            @sync_pair
            async def get_stuff_async(self, ...) -> RecordListResponse:
                ...
            # get_stuff() is created automatically
    """
    async_method._is_sync_pair = True
    return async_method


class _OrchestratorModule(types.ModuleType):
    """Module subclass that provides a useful repr for orchestrator modules."""

    _orchestrator_name: str = ""
    _datasets: dict[str, str] = {}

    def _public_functions(self) -> list[str]:
        """Return sorted names of public function attributes defined in this module."""
        return sorted(
            name
            for name, obj in vars(self).items()
            if isinstance(obj, types.FunctionType)
            and not name.startswith("_")
            and obj.__module__ == self.__name__
        )

    def __repr__(self) -> str:
        methods = ", ".join(self._public_functions())
        return f"{self._orchestrator_name}(methods=[{methods}])"

    def __str__(self) -> str:
        entries = ", ".join(f"'{k}': '{v}'" for k, v in self._datasets.items())
        return f"{self._orchestrator_name}(datasets={{{entries}}})"


def _install_module_repr(
    module_name: str,
    orchestrator_name: str,
    datasets: dict[str, str],
) -> None:
    """Give an orchestrator module a useful repr by changing its __class__."""
    module = sys.modules[module_name]
    module.__class__ = _OrchestratorModule
    module._orchestrator_name = orchestrator_name
    module._datasets = dict(datasets)


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

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Auto-install sync wrappers for methods decorated with @sync_pair."""
        super().__init_subclass__(**kwargs)
        for name in list(vars(cls)):
            method = vars(cls)[name]
            if callable(method) and getattr(method, "_is_sync_pair", False):
                if not name.endswith("_async"):
                    continue
                async_name = name
                sync_name = name.removesuffix("_async")

                def _make_sync(aname: str) -> Any:
                    def sync_wrapper(self: Any, *args: Any, **kw: Any) -> Any:
                        # Look up the async method on the instance so that
                        # monkeypatching on an instance works correctly.
                        async_method = getattr(self, aname)
                        return _run_sync(async_method(*args, **kw))

                    return sync_wrapper

                wrapper = _make_sync(async_name)
                wrapper.__name__ = sync_name
                wrapper.__qualname__ = method.__qualname__.replace("_async", "")
                wrapper.__doc__ = method.__doc__
                setattr(cls, sync_name, wrapper)

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
                f"Unknown dataset: '{friendly_name}'. Available datasets: {available}"
            )
        return self.DATASETS[friendly_name]

    @property
    def available_datasets(self) -> list[str]:
        """List all available friendly dataset names."""
        return list(self.DATASETS.keys())

    def __repr__(self) -> str:
        """Return a concise summary of this orchestrator."""
        name = type(self).__name__
        datasets = ", ".join(self.DATASETS.keys())
        return f"{name}(datasets=[{datasets}])"

    def __str__(self) -> str:
        """Return a detailed summary including ODP dataset IDs."""
        name = type(self).__name__
        entries = ", ".join(f"'{k}': '{v}'" for k, v in self.DATASETS.items())
        return f"{name}(datasets={{{entries}}})"

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

    async def get_facets_async(self, dataset: str) -> FacetListResponse:
        """Get facet values for a dataset asynchronously."""
        dataset_id = self.resolve_dataset_id(dataset)
        client = self._get_client()

        async with client:
            return await client.get_facets(dataset_id)

    def get_facets(self, dataset: str) -> FacetListResponse:
        """Synchronous wrapper for get_facets_async."""
        return _run_sync(self.get_facets_async(dataset))

    async def get_metadata_async(self, dataset: str) -> Dataset:
        """Get dataset metadata asynchronously."""
        dataset_id = self.resolve_dataset_id(dataset)
        client = self._get_client()

        async with client:
            return await client.get_dataset(dataset_id)

    def get_metadata(self, dataset: str) -> Dataset:
        """Synchronous wrapper for get_metadata_async."""
        return _run_sync(self.get_metadata_async(dataset))
