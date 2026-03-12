"""Additional client tests for parameter and raw request branches."""

from __future__ import annotations

import pytest
from pytest_httpx import HTTPXMock

from ukpyn.client import UKPNClient


@pytest.mark.asyncio
async def test_list_datasets_passes_all_supported_params(httpx_mock: HTTPXMock) -> None:
    """list_datasets sends where/q/order/refine/exclude query params."""
    httpx_mock.add_response(
        json={
            "total_count": 0,
            "datasets": [],
        }
    )

    async with UKPNClient(api_key="k") as client:
        await client.list_datasets(
            limit=5,
            offset=2,
            where="records_count > 0",
            search="network",
            order_by="modified",
            refine={"theme": "power"},
            exclude={"licence_area": "LPN"},
        )

    request = httpx_mock.get_requests()[0]
    params = dict(request.url.params)

    assert params["limit"] == "5"
    assert params["offset"] == "2"
    assert params["where"] == "records_count > 0"
    assert params["q"] == "network"
    assert params["search"] == "network"
    assert params["order_by"] == "modified"
    assert "theme:power" in request.url.params.get_list("refine")
    assert "licence_area:LPN" in request.url.params.get_list("exclude")


@pytest.mark.asyncio
async def test_get_records_includes_geo_and_group_params(httpx_mock: HTTPXMock) -> None:
    """get_records forwards select/group/geofilter params correctly."""
    httpx_mock.add_response(
        json={
            "total_count": 1,
            "records": [{"id": "r1", "fields": {"value": 1}}],
        }
    )

    async with UKPNClient(api_key="k") as client:
        await client.get_records(
            dataset_id="dataset-1",
            limit=10,
            offset=3,
            select="value",
            where="value > 0",
            order_by="value",
            group_by="value",
            geofilter_polygon="(0,0),(1,0),(1,1),(0,1)",
            geofilter_distance="0,0,1000",
            refine={"region": "EPN"},
            exclude={"status": "archived"},
        )

    request = httpx_mock.get_requests()[0]
    params = dict(request.url.params)

    assert params["select"] == "value"
    assert params["group_by"] == "value"
    assert params["geofilter.polygon"] == "(0,0),(1,0),(1,1),(0,1)"
    assert params["geofilter.distance"] == "0,0,1000"
    assert "region:EPN" in request.url.params.get_list("refine")
    assert "status:archived" in request.url.params.get_list("exclude")


@pytest.mark.asyncio
async def test_get_records_columns_list_maps_to_select(httpx_mock: HTTPXMock) -> None:
    """get_records maps columns list to select query parameter."""
    httpx_mock.add_response(
        json={
            "total_count": 1,
            "records": [{"id": "r1", "fields": {"value": 1, "status": "ok"}}],
        }
    )

    async with UKPNClient(api_key="k") as client:
        await client.get_records(dataset_id="dataset-1", columns=["value", "status"])

    request = httpx_mock.get_requests()[0]
    params = dict(request.url.params)
    assert params["select"] == "value, status"


@pytest.mark.asyncio
async def test_get_records_rejects_columns_and_select_together() -> None:
    """get_records raises when both columns and select are provided."""
    async with UKPNClient(api_key="k") as client:
        with pytest.raises(ValueError, match="either 'columns' or 'select'"):
            await client.get_records(
                dataset_id="dataset-1",
                columns=["value"],
                select="value",
            )


@pytest.mark.asyncio
async def test_export_data_uses_raw_request_and_params(httpx_mock: HTTPXMock) -> None:
    """export_data returns raw bytes and includes export params."""
    httpx_mock.add_response(content=b"a,b\n1,2\n")

    async with UKPNClient(api_key="k") as client:
        blob = await client.export_data(
            dataset_id="dataset-1",
            format="csv",
            where="value > 0",
            select="value",
            limit=20,
            offset=5,
            order_by="value",
            refine={"region": "EPN"},
            exclude={"status": "archived"},
        )

    request = httpx_mock.get_requests()[0]
    params = dict(request.url.params)

    assert blob.startswith(b"a,b")
    assert request.url.path.endswith("/catalog/datasets/dataset-1/exports/csv")
    assert params["where"] == "value > 0"
    assert params["select"] == "value"
    assert params["limit"] == "20"
    assert params["offset"] == "5"
    assert params["order_by"] == "value"
    assert "region:EPN" in request.url.params.get_list("refine")
    assert "status:archived" in request.url.params.get_list("exclude")


@pytest.mark.asyncio
async def test_export_data_invalid_format_raises() -> None:
    """export_data rejects unsupported format values."""
    async with UKPNClient(api_key="k") as client:
        with pytest.raises(ValueError):
            await client.export_data(dataset_id="dataset-1", format="exe")
