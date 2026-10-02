"""Pytest fixtures and configuration for ukpyn tests."""

import os
from collections import deque
from collections.abc import Iterator
from typing import Any

import httpx2
import pytest
from dotenv import load_dotenv

load_dotenv()


class HTTPXMock:
    """Queue httpx2 responses and record requests without network access."""

    def __init__(self) -> None:
        self._responses: deque[httpx2.Response] = deque()
        self._requests: list[httpx2.Request] = []

    def add_response(self, status_code: int = 200, **kwargs: Any) -> None:
        self._responses.append(httpx2.Response(status_code, **kwargs))

    def get_requests(self) -> list[httpx2.Request]:
        return list(self._requests)

    def handle_request(self, request: httpx2.Request) -> httpx2.Response:
        self._requests.append(request)
        if not self._responses:
            raise AssertionError(f"Unexpected request: {request.method} {request.url}")
        return self._responses.popleft()

    def assert_all_responses_used(self) -> None:
        assert not self._responses, f"{len(self._responses)} mocked responses unused"


@pytest.fixture
def httpx_mock(monkeypatch: pytest.MonkeyPatch) -> Iterator[HTTPXMock]:
    """Mock the async HTTP transport using httpx2's built-in MockTransport."""
    mock = HTTPXMock()
    transport = httpx2.MockTransport(mock.handle_request)

    async def handle_async_request(
        self: httpx2.AsyncHTTPTransport, request: httpx2.Request
    ) -> httpx2.Response:
        return await transport.handle_async_request(request)

    monkeypatch.setattr(
        httpx2.AsyncHTTPTransport, "handle_async_request", handle_async_request
    )
    yield mock
    mock.assert_all_responses_used()


# Test constants
TEST_API_KEY = "test-api-key-12345"
TEST_BASE_URL = "https://ukpowernetworks.opendatasoft.com/api/explore/v2.1"


@pytest.fixture
def api_key() -> str:
    """Provide a fake API key for testing."""
    return TEST_API_KEY


@pytest.fixture(scope="session")
def api_key_available() -> bool:
    """Check if UK Power Networks API key is available from environment/.env."""
    return bool((os.getenv("UKPN_API_KEY") or "").strip())


@pytest.fixture(scope="session")
def run_slow_notebook_tests() -> bool:
    """Check whether slow notebook execution tests are explicitly enabled."""
    return (os.getenv("UKPYN_RUN_SLOW_NOTEBOOK_TESTS") or "").strip() in {
        "1",
        "true",
        "True",
        "yes",
        "on",
    }


@pytest.fixture
def base_url() -> str:
    """Provide the base URL for the UK Power Networks API."""
    return TEST_BASE_URL


# Mock API response data
@pytest.fixture
def mock_datasets_response() -> dict[str, Any]:
    """Mock response for list_datasets endpoint."""
    return {
        "total_count": 2,
        "results": [
            {
                "dataset_id": "ukpn-smart-meter-data",
                "metas": {
                    "default": {
                        "title": "Smart Meter Data",
                        "description": "Aggregated smart meter consumption data",
                        "modified": "2024-01-15T10:30:00Z",
                        "records_count": 10000,
                    }
                },
            },
            {
                "dataset_id": "ukpn-substation-locations",
                "metas": {
                    "default": {
                        "title": "Substation Locations",
                        "description": "Geographic locations of substations",
                        "modified": "2024-01-10T08:00:00Z",
                        "records_count": 5000,
                    }
                },
            },
        ],
    }


@pytest.fixture
def mock_records_response() -> dict[str, Any]:
    """Mock response for get_records endpoint."""
    return {
        "total_count": 100,
        "results": [
            {
                "record_id": "rec-001",
                "fields": {
                    "timestamp": "2024-01-15T12:00:00Z",
                    "consumption_kwh": 150.5,
                    "region": "London",
                },
            },
            {
                "record_id": "rec-002",
                "fields": {
                    "timestamp": "2024-01-15T13:00:00Z",
                    "consumption_kwh": 175.2,
                    "region": "London",
                },
            },
        ],
    }


@pytest.fixture
def mock_single_dataset_response() -> dict[str, Any]:
    """Mock response for a single dataset metadata request."""
    return {
        "dataset_id": "ukpn-smart-meter-data",
        "metas": {
            "default": {
                "title": "Smart Meter Data",
                "description": "Aggregated smart meter consumption data",
                "modified": "2024-01-15T10:30:00Z",
                "records_count": 10000,
            }
        },
        "fields": [
            {"name": "timestamp", "type": "datetime"},
            {"name": "consumption_kwh", "type": "double"},
            {"name": "region", "type": "text"},
        ],
    }


@pytest.fixture
def mock_error_response_401() -> dict[str, Any]:
    """Mock response for authentication error."""
    return {
        "error": "Unauthorized",
        "message": "Invalid API key provided",
        "status_code": 401,
    }


@pytest.fixture
def mock_error_response_404() -> dict[str, Any]:
    """Mock response for not found error."""
    return {
        "error": "Not Found",
        "message": "The requested dataset does not exist",
        "status_code": 404,
    }


@pytest.fixture
def mock_error_response_429() -> dict[str, Any]:
    """Mock response for rate limit error."""
    return {
        "error": "Too Many Requests",
        "message": "Rate limit exceeded. Please wait before making more requests.",
        "status_code": 429,
    }
