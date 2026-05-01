"""Tests for Pydantic models."""

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
    RecordResponse,
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

    def test_dataset_summary_contains_key_insights(self) -> None:
        """Test Dataset summary includes key user-facing fields."""
        dataset = Dataset(
            dataset_id="ukpn-smart-meter-data",
            has_records=True,
            data_visible=True,
            fields=[
                DatasetField(
                    name="region",
                    type="text",
                    description="UKPN licence area",
                )
            ],
            metas=DatasetMetas(
                default={"title": "Smart Meter Data", "records_count": 154}
            ),
        )

        summary = dataset.summary()

        assert "Dataset(" in summary
        assert "ukpn-smart-meter-data" in summary
        assert "title='Smart Meter Data'" in summary
        assert "has_records=True" in summary
        assert "records=154" in summary
        assert "fields=1" in summary

    def test_dataset_properties_expose_common_metadata(self) -> None:
        """Test Dataset convenience properties for title/description/url/count."""
        dataset = Dataset(
            dataset_id="ukpn-smart-meter-data",
            fields=[
                DatasetField(name="region", type="text"),
                DatasetField(name="record_timestamp", type="datetime"),
            ],
            metas=DatasetMetas(
                default={
                    "title": "Smart Meter Data",
                    "description": "Half-hourly metering volumes.",
                    "records_count": 154,
                }
            ),
        )

        assert dataset.title == "Smart Meter Data"
        assert dataset.description == "Half-hourly metering volumes."
        assert dataset.record_count == 154
        assert dataset.field_ids == ["region", "record_timestamp"]
        assert (
            dataset.url
            == "https://ukpowernetworks.opendatasoft.com/explore/dataset/ukpn-smart-meter-data"
        )

    def test_dataset_field_ids_empty_when_no_fields(self) -> None:
        """Test Dataset.field_ids returns empty list when no fields exist."""
        dataset = Dataset(dataset_id="ukpn-smart-meter-data")

        assert dataset.field_ids == []

    def test_dataset_check_fields_returns_only_available_fields(self) -> None:
        """Test Dataset.check_fields drops values not present in dataset fields."""
        dataset = Dataset(
            dataset_id="ukpn-smart-meter-data",
            fields=[
                DatasetField(name="x", type="text"),
                DatasetField(name="y", type="text"),
            ],
        )

        assert dataset.check_fields(["x", "y", "z"]) == ["x", "y"]

    def test_dataset_check_fields_preserves_duplicates(self) -> None:
        """Test Dataset.check_fields keeps duplicate valid columns in order."""
        dataset = Dataset(
            dataset_id="ukpn-smart-meter-data",
            fields=[
                DatasetField(name="x", type="text"),
                DatasetField(name="y", type="text"),
            ],
        )

        assert dataset.check_fields(["x", "x", "y", "z"]) == ["x", "x", "y"]

    def test_dataset_check_fields_is_case_sensitive(self) -> None:
        """Test Dataset.check_fields uses exact case-sensitive matching."""
        dataset = Dataset(
            dataset_id="ukpn-smart-meter-data",
            fields=[DatasetField(name="x", type="text")],
        )

        assert dataset.check_fields(["X", "x"]) == ["x"]

    def test_dataset_check_fields_returns_empty_when_no_fields(self) -> None:
        """Test Dataset.check_fields returns empty list when schema has no fields."""
        dataset = Dataset(dataset_id="ukpn-smart-meter-data")

        assert dataset.check_fields(["x", "y"]) == []

    def test_dataset_details_includes_human_readable_summary(self) -> None:
        """Test Dataset.details includes concise top-level metadata."""
        dataset = Dataset(
            dataset_id="ukpn-smart-meter-data",
            metas=DatasetMetas(
                default={
                    "title": "Smart Meter Data",
                    "description": "Half-hourly metering volumes.",
                    "records_count": 154,
                }
            ),
            fields=[
                DatasetField(
                    name="record_timestamp",
                    label="Record Timestamp",
                    type="datetime",
                    description="Timestamp of the record",
                    annotations={"unit": "utc", "indexed": True},
                )
            ],
            links=[
                {
                    "rel": "self",
                    "href": "https://example.com/datasets/ukpn-smart-meter-data",
                }
            ],
        )

        details = dataset.details()
        details_text = str(details)
        details_html = details._repr_html_()

        assert "Dataset: Smart Meter Data" in details_text
        assert "Dataset ID: ukpn-smart-meter-data" in details_text
        assert "Has records: True" in details_text
        assert "Records: 154" in details_text
        assert (
            "URL: https://ukpowernetworks.opendatasoft.com/explore/dataset/"
            "ukpn-smart-meter-data" in details_text
        )
        assert "Fields (total): 1" in details_text
        assert "- record_timestamp (datetime), Timestamp of the record" in details_text
        assert "Description:" not in details_text
        assert "max-height: 240px" in details_html
        assert (
            "<a href='https://ukpowernetworks.opendatasoft.com/explore/dataset/ukpn-smart-meter-data'"
            in details_html
        )
        assert "<ul>" in details_html
        assert "record_timestamp (datetime), Timestamp of the record" in details_html
        assert "<b>Description:</b>" not in details_html

    def test_dataset_details_omits_description_in_repr_html(self) -> None:
        """Test Dataset.details omits description block in notebook rendering."""
        dataset = Dataset(
            dataset_id="ukpn-html",
            metas=DatasetMetas(
                default={
                    "title": "HTML Dataset",
                    "description": "<p><b>Formatted</b> description</p>",
                }
            ),
        )

        details_html = dataset.details()._repr_html_()

        assert "<b>Formatted</b> description" not in details_html
        assert "<b>Description:</b>" not in details_html

    def test_dataset_details_handles_missing_fields(self) -> None:
        """Test Dataset.details handles missing metadata gracefully."""
        dataset = Dataset(dataset_id="no-fields")

        details = dataset.details()
        details_text = str(details)

        assert "Dataset: N/A" in details_text
        assert "Dataset ID: no-fields" in details_text
        assert "Has records: True" in details_text
        assert "Records: N/A" in details_text
        assert (
            "URL: https://ukpowernetworks.opendatasoft.com/explore/dataset/no-fields"
            in details_text
        )
        assert "Fields (total): 0" in details_text
        assert "- None" in details_text
        assert "Description:" not in details_text


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

    def test_dataset_list_summary_contains_counts_and_ids(self) -> None:
        """Test DatasetListResponse summary includes counts and sample IDs."""
        response = DatasetListResponse(
            total_count=2,
            datasets=[
                {"dataset": {"dataset_id": "d1", "has_records": True}},
                {"dataset": {"dataset_id": "d2", "has_records": True}},
            ],
        )

        summary = response.summary()

        assert "DatasetListResponse(" in summary
        assert "total_count=2" in summary
        assert "returned=2" in summary
        assert "d1" in summary and "d2" in summary

    def test_dataset_list_item_property_proxies(self) -> None:
        """Test response.datasets[i] exposes convenience metadata properties."""
        response = DatasetListResponse(
            total_count=1,
            datasets=[
                {
                    "dataset": {
                        "dataset_id": "ukpn-smart-meter-data",
                        "has_records": True,
                        "metas": {
                            "default": {
                                "title": "Smart Meter Data",
                                "description": "Half-hourly metering volumes.",
                                "records_count": 154,
                            }
                        },
                    }
                }
            ],
        )

        dataset = response.datasets[0]

        assert dataset.id == "ukpn-smart-meter-data"
        assert dataset.title == "Smart Meter Data"
        assert dataset.description == "Half-hourly metering volumes."
        assert dataset.record_count == 154
        assert (
            dataset.url
            == "https://ukpowernetworks.opendatasoft.com/explore/dataset/ukpn-smart-meter-data"
        )


class TestDatasetResponse:
    """Tests for DatasetResponse model."""

    def test_dataset_response_summary_delegates_to_dataset(self) -> None:
        """Test DatasetResponse summary wraps dataset summary output."""
        response = DatasetResponse(
            dataset={"dataset_id": "ukpn-smart-meter-data", "has_records": True}
        )

        summary = response.summary()

        assert summary.startswith("DatasetResponse(")
        assert "ukpn-smart-meter-data" in summary


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

    def test_record_summary_contains_key_insights(self) -> None:
        """Test Record summary includes id and field preview."""
        record = Record(id="rec-001", fields={"b": 2, "a": 1})

        summary = record.summary()

        assert "Record(" in summary
        assert "rec-001" in summary
        assert "field_count=2" in summary
        assert "a, b" in summary


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

    def test_record_list_summary_contains_counts_and_ids(self) -> None:
        """Test RecordListResponse summary includes counts and sample IDs."""
        response = RecordListResponse(
            total_count=100,
            records=[
                Record(id="rec-001", fields={"value": 1}),
                Record(id="rec-002", fields={"value": 2}),
            ],
        )

        summary = response.summary()

        assert "RecordListResponse(" in summary
        assert "total_count=100" in summary
        assert "returned=2" in summary
        assert "rec-001" in summary and "rec-002" in summary

    def test_record_list_parses_flat_results_without_id(self) -> None:
        """Test flat ODS results rows without id are normalized into Record objects."""
        response = RecordListResponse(
            total_count=2,
            results=[
                {"smart": 10, "not_smart": 5},
                {"recordid": "row-2", "smart": 20, "not_smart": 7},
            ],
        )

        assert len(response.records) == 2
        assert response.records[0].id == "unknown"
        assert response.records[0].fields == {"smart": 10, "not_smart": 5}
        assert response.records[1].id == "row-2"
        assert response.records[1].fields == {"smart": 20, "not_smart": 7}


class TestRecordResponse:
    """Tests for RecordResponse model."""

    def test_record_response_summary_delegates_to_record(self) -> None:
        """Test RecordResponse summary wraps record summary output."""
        response = RecordResponse(record={"id": "rec-123", "fields": {"x": 1}})

        summary = response.summary()

        assert summary.startswith("RecordResponse(")
        assert "rec-123" in summary


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


class TestRecordNanSanitisation:
    """Tests for NaN / Infinity sanitisation in Record fields."""

    def test_nan_replaced_with_none(self) -> None:
        """NaN float values are replaced with None."""
        record = Record(id="r1", fields={"grid_site": float("nan"), "name": "ok"})
        assert record.fields["grid_site"] is None
        assert record.fields["name"] == "ok"

    def test_inf_replaced_with_none(self) -> None:
        """Infinity float values are replaced with None."""
        record = Record(id="r1", fields={"val": float("inf")})
        assert record.fields["val"] is None

    def test_neg_inf_replaced_with_none(self) -> None:
        """Negative Infinity float values are replaced with None."""
        record = Record(id="r1", fields={"val": float("-inf")})
        assert record.fields["val"] is None

    def test_nested_nan_replaced(self) -> None:
        """NaN inside nested dicts/lists is sanitised."""
        record = Record(
            id="r1",
            fields={
                "nested": {"a": float("nan"), "b": [1, float("nan"), 3]},
            },
        )
        assert record.fields["nested"]["a"] is None
        assert record.fields["nested"]["b"] == [1, None, 3]

    def test_normal_floats_preserved(self) -> None:
        """Normal float values pass through untouched."""
        record = Record(id="r1", fields={"val": 3.14})
        assert record.fields["val"] == 3.14

    def test_none_fields_not_touched(self) -> None:
        """Record with None fields does not raise."""
        record = Record(id="r1", fields=None)
        assert record.fields is None


class TestFeatureUnwrap:
    """Tests for GeoJSON Feature → Geometry unwrapping."""

    def test_geo_shape_feature_unwrapped(self) -> None:
        """geo_shape wrapped in a Feature is unwrapped to bare geometry."""
        record = Record(
            id="r1",
            fields={
                "geo_shape": {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [0.1, 51.5]},
                    "properties": {"foo": "bar"},
                },
            },
        )
        assert record.fields["geo_shape"] == {
            "type": "Point",
            "coordinates": [0.1, 51.5],
        }

    def test_bare_geometry_unchanged(self) -> None:
        """geo_shape that is already a bare geometry is not modified."""
        geom = {"type": "Point", "coordinates": [0.1, 51.5]}
        record = Record(id="r1", fields={"geo_shape": geom})
        assert record.fields["geo_shape"] == geom

    def test_non_geo_fields_not_unwrapped(self) -> None:
        """Fields that aren't geo fields are not touched by Feature unwrap."""
        feature_like = {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [0, 0]},
        }
        record = Record(id="r1", fields={"some_other_field": feature_like})
        assert record.fields["some_other_field"] == feature_like


class TestRecordGeometry:
    """Tests for Record.geometry property."""

    def test_geometry_from_geo_shape(self) -> None:
        """Geometry resolved from geo_shape field."""
        geom = {"type": "Point", "coordinates": [0.1, 51.5]}
        record = Record(id="r1", fields={"geo_shape": geom})
        assert record.geometry == geom

    def test_geometry_from_spatial_coordinates(self) -> None:
        """Geometry resolved from spatial_coordinates field."""
        geom = {"type": "Point", "coordinates": [-0.12, 51.51]}
        record = Record(id="r1", fields={"spatial_coordinates": geom})
        assert record.geometry == geom

    def test_geometry_from_geo_point_2d_dict(self) -> None:
        """lat/lon dict in geo_point_2d is converted to GeoJSON Point."""
        record = Record(
            id="r1", fields={"geo_point_2d": {"lat": 51.5, "lon": -0.1}}
        )
        assert record.geometry == {
            "type": "Point",
            "coordinates": [-0.1, 51.5],
        }

    def test_geometry_from_geopoint_dict(self) -> None:
        """lat/lon dict in geopoint is converted to GeoJSON Point."""
        record = Record(
            id="r1", fields={"geopoint": {"lat": 51.5, "lon": -0.1}}
        )
        assert record.geometry == {
            "type": "Point",
            "coordinates": [-0.1, 51.5],
        }

    def test_geometry_from_geo_point(self) -> None:
        """lat/lon dict in geo_point is converted to GeoJSON Point."""
        record = Record(
            id="r1", fields={"geo_point": {"lat": 51.5, "lon": -0.1}}
        )
        assert record.geometry == {
            "type": "Point",
            "coordinates": [-0.1, 51.5],
        }

    def test_geometry_priority_order(self) -> None:
        """geo_shape takes priority over geo_point_2d."""
        geom = {"type": "Polygon", "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 0]]]}
        record = Record(
            id="r1",
            fields={
                "geo_shape": geom,
                "geo_point_2d": {"lat": 51.5, "lon": -0.1},
            },
        )
        assert record.geometry == geom

    def test_geometry_none_when_no_geo_fields(self) -> None:
        """geometry is None when record has no geo fields."""
        record = Record(id="r1", fields={"name": "SubstationA"})
        assert record.geometry is None

    def test_geometry_none_when_fields_none(self) -> None:
        """geometry is None when fields is None."""
        record = Record(id="r1", fields=None)
        assert record.geometry is None

    def test_geometry_unwraps_feature_in_geo_shape(self) -> None:
        """geometry property works when geo_shape was a Feature wrapper."""
        record = Record(
            id="r1",
            fields={
                "geo_shape": {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [0.1, 51.5]},
                    "properties": {},
                },
            },
        )
        assert record.geometry == {"type": "Point", "coordinates": [0.1, 51.5]}

    def test_geometry_skips_none_fields(self) -> None:
        """Geometry resolution skips None-valued geo fields gracefully."""
        record = Record(
            id="r1",
            fields={
                "geo_shape": None,
                "geo_point_2d": {"lat": 51.5, "lon": -0.1},
            },
        )
        assert record.geometry == {
            "type": "Point",
            "coordinates": [-0.1, 51.5],
        }


class TestStripZ:
    """Tests for _strip_z coordinate dimension helper."""

    def test_strip_z_from_point(self) -> None:
        from ukpyn.models import _strip_z

        geom = {"type": "Point", "coordinates": [0.1, 51.5, 10.0]}
        result = _strip_z(geom)
        assert result["coordinates"] == [0.1, 51.5]

    def test_strip_z_from_linestring(self) -> None:
        from ukpyn.models import _strip_z

        geom = {
            "type": "LineString",
            "coordinates": [[0, 1, 10], [2, 3, 20]],
        }
        result = _strip_z(geom)
        assert result["coordinates"] == [[0, 1], [2, 3]]

    def test_strip_z_noop_on_2d(self) -> None:
        from ukpyn.models import _strip_z

        geom = {"type": "Point", "coordinates": [0.1, 51.5]}
        result = _strip_z(geom)
        assert result["coordinates"] == [0.1, 51.5]


class TestEnsureZ:
    """Tests for _ensure_z coordinate dimension helper."""

    def test_ensure_z_adds_z_to_point(self) -> None:
        from ukpyn.models import _ensure_z

        geom = {"type": "Point", "coordinates": [0.1, 51.5]}
        result = _ensure_z(geom)
        assert result["coordinates"] == [0.1, 51.5, 0.0]

    def test_ensure_z_preserves_existing_z(self) -> None:
        from ukpyn.models import _ensure_z

        geom = {"type": "Point", "coordinates": [0.1, 51.5, 10.0]}
        result = _ensure_z(geom)
        assert result["coordinates"] == [0.1, 51.5, 10.0]

    def test_ensure_z_on_linestring(self) -> None:
        from ukpyn.models import _ensure_z

        geom = {
            "type": "LineString",
            "coordinates": [[0, 1], [2, 3]],
        }
        result = _ensure_z(geom)
        assert result["coordinates"] == [[0, 1, 0.0], [2, 3, 0.0]]
