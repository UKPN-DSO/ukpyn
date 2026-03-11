"""Shared fixtures for orchestrator tests."""

from typing import Any

import pytest

from ukpyn.models import Record, RecordListResponse

# Test constants
TEST_API_KEY = "test-api-key-12345"


@pytest.fixture
def mock_records_response() -> RecordListResponse:
    """Mock RecordListResponse for testing orchestrator methods."""
    return RecordListResponse(
        total_count=2,
        records=[
            Record(
                id="rec-001",
                fields={
                    "timestamp": "2024-01-15T12:00:00Z",
                    "value": 150.5,
                    "region": "EPN",
                },
            ),
            Record(
                id="rec-002",
                fields={
                    "timestamp": "2024-01-15T13:00:00Z",
                    "value": 175.2,
                    "region": "SPN",
                },
            ),
        ],
    )


@pytest.fixture
def mock_records_api_response() -> dict[str, Any]:
    """Mock API response for get_records endpoint (raw JSON format)."""
    return {
        "total_count": 2,
        "records": [
            {
                "id": "rec-001",
                "fields": {
                    "timestamp": "2024-01-15T12:00:00Z",
                    "value": 150.5,
                    "region": "EPN",
                },
            },
            {
                "id": "rec-002",
                "fields": {
                    "timestamp": "2024-01-15T13:00:00Z",
                    "value": 175.2,
                    "region": "SPN",
                },
            },
        ],
    }


@pytest.fixture
def mock_dataset_response() -> dict[str, Any]:
    """Mock response for a single dataset metadata request."""
    return {
        "dataset_id": "test-dataset-id",
        "has_records": True,
        "data_visible": True,
        "metas": {
            "default": {
                "title": "Test Dataset",
                "description": "A test dataset for unit testing",
                "modified": "2024-01-15T10:30:00Z",
                "records_count": 1000,
            }
        },
        "fields": [
            {"name": "timestamp", "type": "datetime"},
            {"name": "value", "type": "double"},
            {"name": "region", "type": "text"},
        ],
    }
