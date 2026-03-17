"""Focused high-value coverage tests for UKPNClient core behavior."""

from __future__ import annotations

import httpx
import pytest
from pytest_httpx import HTTPXMock

from ukpyn.client import UKPNClient
from ukpyn.config import Config
from ukpyn.exceptions import (
    NotFoundError,
    RateLimitError,
    ServerError,
    UKPNError,
    UnrecognisedFieldError,
    ValidationError,
)


@pytest.mark.asyncio
async def test_client_init_uses_explicit_config_object() -> None:
    """Passing a Config instance should bypass constructor kwargs logic."""
    config = Config(api_key="from-config", base_url="https://example.test", timeout=12)
    client = UKPNClient(
        api_key="ignored",
        base_url="https://ignored.test",
        timeout=99,
        config=config,
    )

    assert client._config is config
    assert client._config.api_key == "from-config"


@pytest.mark.asyncio
async def test_client_init_applies_base_url_and_timeout_kwargs() -> None:
    """Constructor kwargs should be forwarded into Config when config is not passed."""
    client = UKPNClient(api_key="k", base_url="https://api.example.test", timeout=7)

    assert client._config.base_url == "https://api.example.test"
    assert client._config.timeout == 7


def test_handle_error_validation_with_non_json_body() -> None:
    """ValidationError should use raw response text when JSON decoding fails."""
    client = UKPNClient(api_key="k")
    response = httpx.Response(400, text="invalid request payload")

    with pytest.raises(ValidationError, match="invalid request payload"):
        client._handle_error(response)


def test_handle_error_unknown_field_raises_unrecognised_field_error() -> None:
    """400 with 'unknown field' in the message should raise UnrecognisedFieldError."""
    client = UKPNClient(api_key="k")

    response = httpx.Response(400, json={"message": "Unknown field: foo_bar"})
    with pytest.raises(UnrecognisedFieldError, match="foo_bar"):
        client._handle_error(response)


def test_handle_error_invalid_field_raises_unrecognised_field_error() -> None:
    """400 with 'invalid field' in the message should raise UnrecognisedFieldError."""
    client = UKPNClient(api_key="k")

    response = httpx.Response(400, json={"message": "Invalid field name in select"})
    with pytest.raises(UnrecognisedFieldError, match="pip install --upgrade ukpyn"):
        client._handle_error(response)


def test_handle_error_rate_limit_without_retry_after() -> None:
    """RateLimitError should set retry_after to None when header is absent."""
    client = UKPNClient(api_key="k")
    response = httpx.Response(429, json={"message": "too many requests"})

    with pytest.raises(RateLimitError) as exc_info:
        client._handle_error(response)

    assert exc_info.value.retry_after is None


def test_handle_error_server_and_generic_statuses() -> None:
    """Server and unknown statuses should map to their respective exception types."""
    client = UKPNClient(api_key="k")

    with pytest.raises(ServerError):
        client._handle_error(httpx.Response(503, json={"message": "upstream down"}))

    with pytest.raises(UKPNError) as exc_info:
        client._handle_error(httpx.Response(418, json={"message": "teapot"}))

    assert exc_info.value.status_code == 418


@pytest.mark.asyncio
async def test_get_dataset_success_parses_dataset_response(
    httpx_mock: HTTPXMock,
) -> None:
    """get_dataset should parse DatasetResponse and return the dataset payload."""
    httpx_mock.add_response(
        json={
            "dataset": {
                "dataset_id": "ukpn-demo-dataset",
                "has_records": True,
                "data_visible": True,
            }
        }
    )

    async with UKPNClient(api_key="k") as client:
        dataset = await client.get_dataset("ukpn-demo-dataset")

    assert dataset.dataset_id == "ukpn-demo-dataset"


@pytest.mark.asyncio
async def test_export_data_uses_none_params_when_no_filters(
    httpx_mock: HTTPXMock,
) -> None:
    """export_data should call raw request with params=None when no filters are passed."""
    httpx_mock.add_response(content=b"{}")

    async with UKPNClient(api_key="k") as client:
        blob = await client.export_data(dataset_id="dataset-1", format="json")

    request = httpx_mock.get_requests()[0]
    assert blob == b"{}"
    assert request.url.path.endswith("/catalog/datasets/dataset-1/exports/json")
    assert len(request.url.params) == 0


@pytest.mark.asyncio
async def test_get_client_recreates_when_existing_client_is_closed() -> None:
    """_get_client should recreate a client when current instance is already closed."""
    client = UKPNClient(api_key="k")

    first = await client._get_client()
    reused = await client._get_client()
    assert reused is first

    await first.aclose()
    second = await client._get_client()

    assert second is not first
    assert second.is_closed is False

    await client.close()


@pytest.mark.asyncio
async def test_request_raw_error_branch_raises_not_found(httpx_mock: HTTPXMock) -> None:
    """_request_raw should route non-success responses through _handle_error."""
    httpx_mock.add_response(status_code=404, json={"message": "dataset missing"})

    async with UKPNClient(api_key="k") as client:
        with pytest.raises(NotFoundError, match="dataset missing"):
            await client.export_data(dataset_id="missing-dataset", format="csv")
