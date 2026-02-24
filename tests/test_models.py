"""Tests for Pydantic models."""

from typing import Any

import pytest
from pydantic import ValidationError

from ukpyn.models import (
    Dataset,
    DatasetField,
    DatasetListResponse,
    DatasetMetas,
    DatasetResponse,
    Record,
    RecordListResponse,
)


class TestDatasetMetas:
    """Tests for DatasetMetas model."""

    def test_dataset_metas_valid(self) -> None:
        """Test valid DatasetMetas creation."""
        metas = DatasetMetas(
            default={"title": "Smart Meter Data", "description": "Test"},
        )
        assert metas.default is not None
        assert metas.default["title"] == "Smart Meter Data"

    def test_dataset_metas_optional_fields(self) -> None:
        """Test DatasetMetas with optional fields."""
        metas = DatasetMetas()
        assert metas.default is None
        assert metas.custom is None


class TestDatasetField:
    """Tests for DatasetField model."""

    def test_field_valid(self) -> None:
        """Test valid DatasetField creation."""
        field = DatasetField(name="consumption_kwh", type="double")
        assert field.name == "consumption_kwh"
        assert field.type == "double"

    def test_field_with_label(self) -> None:
        """Test DatasetField with optional label."""
        field = DatasetField(
            name="consumption_kwh",
            type="double",
            label="Consumption (kWh)",
        )
        assert field.label == "Consumption (kWh)"

    def test_field_missing_required_fields(self) -> None:
        """Test DatasetField raises error when required fields missing."""
        with pytest.raises(ValidationError):
            DatasetField(name="test")  # type: ignore - missing 'type' field

    def test_field_serialization(self) -> None:
        """Test DatasetField serializes to dict correctly."""
        field = DatasetField(name="region", type="text", label="Region")
        data = field.model_dump()
        assert data["name"] == "region"
        assert data["type"] == "text"
        assert data["label"] == "Region"


class TestDataset:
    """Tests for Dataset model."""

    def test_dataset_valid(self) -> None:
        """Test valid Dataset creation."""
        dataset = Dataset(dataset_id="ukpn-smart-meter-data")
        assert dataset.dataset_id == "ukpn-smart-meter-data"

    def test_dataset_with_metas(self) -> None:
        """Test Dataset with metadata."""
        dataset = Dataset(
            dataset_id="ukpn-smart-meter-data",
            metas=DatasetMetas(
                default={"title": "Smart Meter Data"},
            ),
        )
        assert dataset.metas is not None
        assert dataset.metas.default["title"] == "Smart Meter Data"

    def test_dataset_with_fields(self) -> None:
        """Test Dataset with field definitions."""
        dataset = Dataset(
            dataset_id="ukpn-smart-meter-data",
            fields=[
                DatasetField(name="timestamp", type="datetime"),
                DatasetField(name="consumption_kwh", type="double"),
            ],
        )
        assert dataset.fields is not None
        assert len(dataset.fields) == 2
        assert dataset.fields[0].name == "timestamp"

    def test_dataset_missing_id_raises_error(self) -> None:
        """Test Dataset raises error without dataset_id."""
        with pytest.raises(ValidationError):
            Dataset()  # type: ignore

    def test_dataset_serialization(self) -> None:
        """Test Dataset serializes to dict correctly."""
        dataset = Dataset(dataset_id="test-dataset")
        data = dataset.model_dump()
        assert data["dataset_id"] == "test-dataset"


class TestDatasetListResponse:
    """Tests for DatasetListResponse model."""

    def test_dataset_list_valid(self) -> None:
        """Test valid DatasetListResponse creation."""
        response = DatasetListResponse(total_count=2, datasets=[])
        assert response.total_count == 2
        assert len(response.datasets) == 0

    def test_dataset_list_empty(self) -> None:
        """Test DatasetListResponse with no results."""
        response = DatasetListResponse(total_count=0, datasets=[])
        assert response.total_count == 0
        assert len(response.datasets) == 0


class TestRecord:
    """Tests for Record model."""

    def test_record_valid(self) -> None:
        """Test valid Record creation."""
        record = Record(
            id="rec-001",
            fields={
                "timestamp": "2024-01-15T12:00:00Z",
                "consumption_kwh": 150.5,
                "region": "London",
            },
        )
        assert record.id == "rec-001"
        assert record.fields["consumption_kwh"] == 150.5

    def test_record_empty_fields(self) -> None:
        """Test Record with empty fields dict."""
        record = Record(id="rec-001", fields={})
        assert record.id == "rec-001"
        assert record.fields == {}

    def test_record_field_access(self) -> None:
        """Test accessing fields from Record."""
        record = Record(
            id="rec-001",
            fields={"region": "London", "value": 100},
        )
        assert record.fields["region"] == "London"
        assert record.fields["value"] == 100

    def test_record_serialization(self) -> None:
        """Test Record serializes to dict correctly."""
        record = Record(
            id="rec-001",
            fields={"test_field": "test_value"},
        )
        data = record.model_dump()
        assert data["id"] == "rec-001"
        assert data["fields"]["test_field"] == "test_value"


class TestRecordListResponse:
    """Tests for RecordListResponse model."""

    def test_record_list_valid(self) -> None:
        """Test valid RecordListResponse creation."""
        response = RecordListResponse(
            total_count=100,
            records=[
                Record(id="rec-001", fields={"value": 1}),
                Record(id="rec-002", fields={"value": 2}),
            ],
        )
        assert response.total_count == 100
        assert len(response.records) == 2

    def test_record_list_empty(self) -> None:
        """Test RecordListResponse with no results."""
        response = RecordListResponse(total_count=0, records=[])
        assert response.total_count == 0
        assert len(response.records) == 0


class TestModelSerialization:
    """Tests for model serialization and deserialization."""

    def test_round_trip_serialization_dataset(self) -> None:
        """Test Dataset survives round-trip serialization."""
        original = Dataset(
            dataset_id="test-dataset",
            metas=DatasetMetas(default={"title": "Test"}),
            fields=[DatasetField(name="field1", type="text")],
        )
        serialized = original.model_dump()
        restored = Dataset.model_validate(serialized)
        assert restored.dataset_id == original.dataset_id

    def test_round_trip_serialization_record(self) -> None:
        """Test Record survives round-trip serialization."""
        original = Record(
            id="test-record",
            fields={"key": "value", "number": 42, "nested": {"a": 1}},
        )
        serialized = original.model_dump()
        restored = Record.model_validate(serialized)
        assert restored.id == original.id
        assert restored.fields == original.fields

    def test_json_serialization(self) -> None:
        """Test model can be serialized to JSON string."""
        dataset = Dataset(dataset_id="test-dataset")
        json_str = dataset.model_dump_json()
        assert isinstance(json_str, str)
        assert "test-dataset" in json_str
