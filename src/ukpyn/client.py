"""Async HTTP client for the UK Power Networks OpenDataSoft API."""

from typing import Any

import httpx

from .config import Config
from .exceptions import (
    AuthenticationError,
    NotFoundError,
    RateLimitError,
    ServerError,
    UKPNError,
    ValidationError,
)
from .models import (
    EXPORT_FORMATS,
    Dataset,
    DatasetListResponse,
    DatasetResponse,
    RecordListResponse,
)


class UKPNClient:
    """
    Async client for the UK Power Networks OpenDataSoft API v2.1.

    Usage:
        async with UKPNClient(api_key="your-key") as client:
            datasets = await client.list_datasets()
            records = await client.get_records("dataset-id")

    Or without context manager:
        client = UKPNClient(api_key="your-key")
        try:
            datasets = await client.list_datasets()
        finally:
            await client.close()
    """

    def __init__(
        self,
        api_key: str | None = None,
        base_url: str | None = None,
        timeout: int | None = None,
        config: Config | None = None,
    ) -> None:
        """
        Initialize the UKPN API client.

        Args:
            api_key: API key for authentication. If not provided, will attempt
                     to load from UKPN_API_KEY environment variable.
            base_url: Override the default API base URL.
            timeout: Request timeout in seconds.
            config: Optional Config object. If provided, other parameters are ignored.
        """
        if config is not None:
            self._config = config
        else:
            kwargs: dict[str, Any] = {}
            if api_key is not None:
                kwargs["api_key"] = api_key
            if base_url is not None:
                kwargs["base_url"] = base_url
            if timeout is not None:
                kwargs["timeout"] = timeout
            self._config = Config(**kwargs)

        self._client: httpx.AsyncClient | None = None

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create the HTTP client."""
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                base_url=self._config.api_url,
                headers=self._config.get_headers(),
                timeout=self._config.timeout,
            )
        return self._client

    async def close(self) -> None:
        """Close the HTTP client."""
        if self._client is not None and not self._client.is_closed:
            await self._client.aclose()
            self._client = None

    async def __aenter__(self) -> "UKPNClient":
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit."""
        await self.close()

    def summary(self) -> str:
        """Return a concise, human-readable summary of client configuration/state."""
        client_state = (
            "open"
            if self._client is not None and not self._client.is_closed
            else "closed"
        )
        return (
            f"UKPNClient(api_url='{self._config.api_url}', "
            f"timeout={self._config.timeout}, "
            f"has_api_key={self._config.has_api_key}, "
            f"client_state='{client_state}')"
        )

    def _handle_error(self, response: httpx.Response) -> None:
        """
        Handle HTTP error responses.

        Args:
            response: The HTTP response object.

        Raises:
            AuthenticationError: For 401 responses.
            NotFoundError: For 404 responses.
            RateLimitError: For 429 responses.
            ValidationError: For 400 responses.
            ServerError: For 5xx responses.
            UKPNError: For other error responses.
        """
        status_code = response.status_code
        try:
            error_data = response.json()
            message = error_data.get("message", response.text)
        except Exception:
            message = response.text or f"HTTP {status_code} error"

        if status_code == 401:
            raise AuthenticationError(message)
        elif status_code == 404:
            raise NotFoundError(message)
        elif status_code == 429:
            retry_after = response.headers.get("Retry-After")
            raise RateLimitError(
                message,
                retry_after=int(retry_after) if retry_after else None,
            )
        elif status_code == 400:
            lowered = message.lower()
            if "unknown field" in lowered or "invalid field" in lowered:
                from .exceptions import UnrecognisedFieldError

                raise UnrecognisedFieldError(message)
            raise ValidationError(message)
        elif status_code >= 500:
            raise ServerError(message)
        else:
            raise UKPNError(message, status_code=status_code)

    async def _request(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        """
        Make an HTTP request to the API.

        Args:
            method: HTTP method (GET, POST, etc.).
            path: API endpoint path.
            params: Query parameters.
            **kwargs: Additional arguments passed to httpx.

        Returns:
            Parsed JSON response.

        Raises:
            UKPNError: On API errors.
        """
        if not self._config.has_api_key:
            from .config import check_api_key

            check_api_key()  # raises AuthenticationError with guidance

        client = await self._get_client()
        response = await client.request(method, path, params=params, **kwargs)

        if not response.is_success:
            self._handle_error(response)

        return response.json()

    async def _request_raw(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> bytes:
        """
        Make an HTTP request and return raw bytes.

        Args:
            method: HTTP method (GET, POST, etc.).
            path: API endpoint path.
            params: Query parameters.
            **kwargs: Additional arguments passed to httpx.

        Returns:
            Raw response content as bytes.

        Raises:
            UKPNError: On API errors.
        """
        if not self._config.has_api_key:
            from .config import check_api_key

            check_api_key()  # raises AuthenticationError with guidance

        client = await self._get_client()
        response = await client.request(method, path, params=params, **kwargs)

        if not response.is_success:
            self._handle_error(response)

        return response.content

    async def list_datasets(
        self,
        limit: int = 10,
        offset: int = 0,
        where: str | None = None,
        order_by: str | None = None,
        refine: dict[str, str] | None = None,
        exclude: dict[str, str] | None = None,
    ) -> DatasetListResponse:
        """
        List available datasets.

        Args:
            limit: Maximum number of datasets to return (default 10).
            offset: Number of datasets to skip for pagination.
            where: Filter expression (ODSQL).
            order_by: Field to sort by.
            refine: Facet refinement filters.
            exclude: Facet exclusion filters.

        Returns:
            DatasetListResponse containing the list of datasets.
        """
        params: dict[str, Any] = {
            "limit": limit,
            "offset": offset,
        }

        if where:
            params["where"] = where
        if order_by:
            params["order_by"] = order_by
        if refine:
            params["refine"] = [f"{k}:{v}" for k, v in refine.items()]
        if exclude:
            params["exclude"] = [f"{k}:{v}" for k, v in exclude.items()]

        data = await self._request("GET", "/catalog/datasets", params=params)
        return DatasetListResponse(**data)

    async def get_dataset(self, dataset_id: str) -> Dataset:
        """
        Get a specific dataset by ID.

        Args:
            dataset_id: The dataset identifier.

        Returns:
            Dataset object with full metadata.

        Raises:
            NotFoundError: If the dataset does not exist.
        """
        data = await self._request("GET", f"/catalog/datasets/{dataset_id}")
        response = DatasetResponse(**data)
        return response.dataset

    async def get_records(
        self,
        dataset_id: str,
        limit: int = 10,
        offset: int = 0,
        columns: str | list[str] | tuple[str, ...] | None = None,
        select: str | None = None,
        where: str | None = None,
        order_by: str | None = None,
        group_by: str | None = None,
        refine: dict[str, str] | None = None,
        exclude: dict[str, str] | None = None,
        geofilter_polygon: str | None = None,
        geofilter_distance: str | None = None,
    ) -> RecordListResponse:
        """
        Get records from a dataset.

        Args:
            dataset_id: The dataset identifier.
            limit: Maximum number of records to return (default 10).
            offset: Number of records to skip for pagination.
            columns: Convenience alias for selecting columns. Accepts a
                comma-separated string or list/tuple of column names.
            select: Fields to include in the response.
            where: SQL-like filter expression (ODSQL WHERE clause semantics).
            order_by: Field to sort by.
            group_by: Field to group results by.
            refine: Facet refinement filters.
            exclude: Facet exclusion filters.
            geofilter_polygon: Geographic polygon filter in format
                "(lon1,lat1),(lon2,lat2),...". Records with geo fields
                intersecting this polygon will be returned.
            geofilter_distance: Geographic distance filter in format
                "lon,lat,distance_in_meters". Records within the specified
                distance from the point will be returned.

        Returns:
            RecordListResponse containing the list of records.

        Raises:
            ValueError: If both columns and select are provided.
            NotFoundError: If the dataset does not exist.
        """
        params: dict[str, Any] = {
            "limit": limit,
            "offset": offset,
        }

        if columns is not None and select is not None:
            raise ValueError("Use either 'columns' or 'select', not both.")

        if columns is not None:
            if isinstance(columns, (list, tuple)):
                select = ", ".join(columns)
            else:
                select = columns

        if select:
            params["select"] = select
        if where:
            params["where"] = where
        if order_by:
            params["order_by"] = order_by
        if group_by:
            params["group_by"] = group_by
        if refine:
            params["refine"] = [f"{k}:{v}" for k, v in refine.items()]
        if exclude:
            params["exclude"] = [f"{k}:{v}" for k, v in exclude.items()]
        if geofilter_polygon:
            params["geofilter.polygon"] = geofilter_polygon
        if geofilter_distance:
            params["geofilter.distance"] = geofilter_distance

        data = await self._request(
            "GET", f"/catalog/datasets/{dataset_id}/records", params=params
        )
        return RecordListResponse(**data)

    async def export_data(
        self,
        dataset_id: str,
        format: str = "json",
        where: str | None = None,
        select: str | None = None,
        limit: int | None = None,
        offset: int | None = None,
        order_by: str | None = None,
        refine: dict[str, str] | None = None,
        exclude: dict[str, str] | None = None,
    ) -> bytes:
        """
        Export dataset data in various formats.

        Args:
            dataset_id: The dataset identifier.
            format: Export format (json, csv, xlsx, geojson, shapefile, kml).
            where: Filter expression (ODSQL).
            select: Fields to include in the export.
            limit: Maximum number of records to export.
            offset: Number of records to skip.
            order_by: Field to sort by.
            refine: Facet refinement filters.
            exclude: Facet exclusion filters.

        Returns:
            Raw export data as bytes.

        Raises:
            ValueError: If format is not supported.
            NotFoundError: If the dataset does not exist.
        """
        if format not in EXPORT_FORMATS:
            raise ValueError(
                f"Unsupported format: {format}. "
                f"Supported formats: {', '.join(EXPORT_FORMATS)}"
            )

        params: dict[str, Any] = {}

        if where:
            params["where"] = where
        if select:
            params["select"] = select
        if limit:
            params["limit"] = limit
        if offset:
            params["offset"] = offset
        if order_by:
            params["order_by"] = order_by
        if refine:
            params["refine"] = [f"{k}:{v}" for k, v in refine.items()]
        if exclude:
            params["exclude"] = [f"{k}:{v}" for k, v in exclude.items()]

        return await self._request_raw(
            "GET",
            f"/catalog/datasets/{dataset_id}/exports/{format}",
            params=params if params else None,
        )
