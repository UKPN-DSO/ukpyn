"""Tests for UKPNClient."""

from typing import Any

import pytest
from pytest_httpx import HTTPXMock

from ukpyn.client import UKPNClient
from ukpyn.exceptions import (
    AuthenticationError,
    NotFoundError,
    RateLimitError,
)

# Test constants
TEST_API_KEY = "test-api-key-12345"


# Mock responses that match the actual OpenDataSoft API format
@pytest.fixture
def mock_catalog_response() -> dict[str, Any]:
    """Mock response matching OpenDataSoft catalog/datasets format."""
    return {
        "total_count": 2,
        "datasets": [
            {
                "dataset": {
                    "dataset_id": "ukpn-smart-meter-data",
                    "has_records": True,
                    "data_visible": True,
                }
            },
            {
                "dataset": {
                    "dataset_id": "ukpn-substation-locations",
                    "has_records": True,
                    "data_visible": True,
                }
            },
        ],
    }


@pytest.fixture
def mock_records_api_response() -> dict[str, Any]:
    """Mock response matching OpenDataSoft records format."""
    return {
        "total_count": 100,
        "records": [
            {
                "id": "rec-001",
                "fields": {
                    "timestamp": "2024-01-15T12:00:00Z",
                    "consumption_kwh": 150.5,
                    "region": "London",
                },
            },
            {
                "id": "rec-002",
                "fields": {
                    "timestamp": "2024-01-15T13:00:00Z",
                    "consumption_kwh": 175.2,
                    "region": "London",
                },
            },
        ],
    }


class TestClientInitialization:
    """Tests for UKPNClient initialization."""

    def test_client_init_with_api_key(self) -> None:
        """Test client initializes with API key."""
        client = UKPNClient(api_key=TEST_API_KEY)
        assert client._config.api_key == TEST_API_KEY

    def test_client_init_default_base_url(self) -> None:
        """Test client uses default base URL when not specified."""
        client = UKPNClient(api_key=TEST_API_KEY)
        assert "ukpowernetworks.opendatasoft.com" in client._config.base_url


class TestListDatasets:
    """Tests for list_datasets method."""

    @pytest.mark.asyncio
    async def test_list_datasets_success(
        self,
        httpx_mock: HTTPXMock,
        mock_catalog_response: dict[str, Any],
    ) -> None:
        """Test successful listing of datasets."""
        httpx_mock.add_response(json=mock_catalog_response)

        async with UKPNClient(api_key=TEST_API_KEY) as client:
            result = await client.list_datasets()

        assert result.total_count == 2
        assert len(result.datasets) == 2
        assert result.datasets[0].dataset.dataset_id == "ukpn-smart-meter-data"

    @pytest.mark.asyncio
    async def test_list_datasets_parses_results_key(
        self,
        httpx_mock: HTTPXMock,
    ) -> None:
        """Test list_datasets parses OpenDataSoft 'results' key (wrapped format)."""
        httpx_mock.add_response(
            json={
                "total_count": 2,
                "results": [
                    {
                        "dataset": {
                            "dataset_id": "ukpn-smart-meter-data",
                            "has_records": True,
                            "data_visible": True,
                        }
                    },
                    {
                        "dataset": {
                            "dataset_id": "ukpn-substation-locations",
                            "has_records": True,
                            "data_visible": True,
                        }
                    },
                ],
            }
        )

        async with UKPNClient(api_key=TEST_API_KEY) as client:
            result = await client.list_datasets()

        assert result.total_count == 2
        assert len(result.datasets) == 2
        assert result.datasets[1].dataset.dataset_id == "ukpn-substation-locations"

    @pytest.mark.asyncio
    async def test_list_datasets_parses_flat_results_items(
        self,
        httpx_mock: HTTPXMock,
    ) -> None:
        """Test list_datasets parses OpenDataSoft 'results' items when unwrapped."""
        httpx_mock.add_response(
            json={
                "total_count": 2,
                "results": [
                    {
                        "dataset_id": "ukpn-smart-meter-data",
                        "has_records": True,
                        "data_visible": True,
                        "metas": {"default": {"title": "Smart Meter Data"}},
                    },
                    {
                        "dataset_id": "ukpn-substation-locations",
                        "has_records": True,
                        "data_visible": True,
                    },
                ],
            }
        )

        async with UKPNClient(api_key=TEST_API_KEY) as client:
            result = await client.list_datasets()

        assert result.total_count == 2
        assert len(result.datasets) == 2
        assert result.datasets[0].dataset.dataset_id == "ukpn-smart-meter-data"
        assert result.datasets[0].dataset.metas is not None


class TestGetRecords:
    """Tests for get_records method."""

    @pytest.mark.asyncio
    async def test_get_records_success(
        self,
        httpx_mock: HTTPXMock,
        mock_records_api_response: dict[str, Any],
    ) -> None:
        """Test successful retrieval of records."""
        httpx_mock.add_response(json=mock_records_api_response)

        async with UKPNClient(api_key=TEST_API_KEY) as client:
            result = await client.get_records("ukpn-smart-meter-data")

        assert result.total_count == 100
        assert len(result.records) == 2
        assert result.records[0].fields["region"] == "London"

    @pytest.mark.asyncio
    async def test_get_records_parses_results_key(
        self,
        httpx_mock: HTTPXMock,
    ) -> None:
        """Test get_records parses OpenDataSoft 'results' key."""
        httpx_mock.add_response(
            json={
                "total_count": 2,
                "results": [
                    {"id": "rec-001", "fields": {"region": "London"}},
                    {"id": "rec-002", "fields": {"region": "SPN"}},
                ],
            }
        )

        async with UKPNClient(api_key=TEST_API_KEY) as client:
            result = await client.get_records("ukpn-smart-meter-data")

        assert result.total_count == 2
        assert len(result.records) == 2
        assert result.records[0].fields["region"] == "London"


class TestGetDataset:
    """Tests for get_dataset method."""

    @pytest.mark.asyncio
    async def test_get_dataset_parses_wrapped_response(
        self,
        httpx_mock: HTTPXMock,
    ) -> None:
        """Test get_dataset parses wrapped 'dataset' response."""
        httpx_mock.add_response(
            json={
                "dataset": {
                    "dataset_id": "ukpn-smart-meter-data",
                    "has_records": True,
                    "data_visible": True,
                }
            }
        )

        async with UKPNClient(api_key=TEST_API_KEY) as client:
            result = await client.get_dataset("ukpn-smart-meter-data")

        assert result.dataset_id == "ukpn-smart-meter-data"
        assert result.has_records is True

    @pytest.mark.asyncio
    async def test_get_dataset_parses_flat_response(
        self,
        httpx_mock: HTTPXMock,
    ) -> None:
        """Test get_dataset parses flat response without 'dataset' wrapper."""
        httpx_mock.add_response(
            json={
                "dataset_id": "ukpn-smart-meter-installation-volumes",
                "has_records": True,
                "data_visible": True,
                "metas": {"default": {"title": "Smart Meter Installation Volumes"}},
            }
        )

        async with UKPNClient(api_key=TEST_API_KEY) as client:
            result = await client.get_dataset("ukpn-smart-meter-installation-volumes")

        assert result.dataset_id == "ukpn-smart-meter-installation-volumes"
        assert result.metas is not None

class TestErrorHandling:
    """Tests for error handling."""

    @pytest.mark.asyncio
    async def test_authentication_error(
        self,
        httpx_mock: HTTPXMock,
    ) -> None:
        """Test handling of authentication error (401)."""
        httpx_mock.add_response(
            json={"message": "Invalid API key"},
            status_code=401,
        )

        async with UKPNClient(api_key="invalid-key") as client:
            with pytest.raises(AuthenticationError):
                await client.list_datasets()

    @pytest.mark.asyncio
    async def test_not_found_error(
        self,
        httpx_mock: HTTPXMock,
    ) -> None:
        """Test handling of not found error (404)."""
        httpx_mock.add_response(
            json={"message": "Dataset not found"},
            status_code=404,
        )

        async with UKPNClient(api_key=TEST_API_KEY) as client:
            with pytest.raises(NotFoundError):
                await client.get_records("nonexistent-dataset")

    @pytest.mark.asyncio
    async def test_rate_limit_error(
        self,
        httpx_mock: HTTPXMock,
    ) -> None:
        """Test handling of rate limit error (429)."""
        httpx_mock.add_response(
            json={"message": "Rate limit exceeded"},
            status_code=429,
            headers={"Retry-After": "60"},
        )

        async with UKPNClient(api_key=TEST_API_KEY) as client:
            with pytest.raises(RateLimitError) as exc_info:
                await client.list_datasets()

        assert exc_info.value.retry_after == 60


class TestClientContextManager:
    """Tests for async context manager functionality."""

    @pytest.mark.asyncio
    async def test_client_as_context_manager(
        self,
        httpx_mock: HTTPXMock,
        mock_catalog_response: dict[str, Any],
    ) -> None:
        """Test client works as async context manager."""
        httpx_mock.add_response(json=mock_catalog_response)

        async with UKPNClient(api_key=TEST_API_KEY) as client:
            result = await client.list_datasets()
            assert result is not None
