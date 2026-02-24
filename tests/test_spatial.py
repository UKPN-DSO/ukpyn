"""Tests for spatial query module."""

from typing import Any

import pytest
from pytest_httpx import HTTPXMock

from ukpyn import spatial
from ukpyn.spatial import (
    Bounds,
    GEO_ENABLED_DATASETS,
    _bounds_to_polygon,
    list_geo_datasets,
    query_bounds,
)

# Test constants
TEST_API_KEY = "test-api-key-12345"


@pytest.fixture
def mock_geo_records_response() -> dict[str, Any]:
    """Mock response for geo dataset records."""
    return {
        "total_count": 2,
        "records": [
            {
                "id": "rec-001",
                "fields": {
                    "name": "Substation A",
                    "licence_area": "LPN",
                    "geo_shape": {
                        "type": "Polygon",
                        "coordinates": [[[-0.1, 51.5], [-0.1, 51.6], [0.0, 51.6], [0.0, 51.5], [-0.1, 51.5]]],
                    },
                },
            },
            {
                "id": "rec-002",
                "fields": {
                    "name": "Substation B",
                    "licence_area": "LPN",
                    "geo_shape": {
                        "type": "Polygon",
                        "coordinates": [[[-0.2, 51.4], [-0.2, 51.5], [-0.1, 51.5], [-0.1, 51.4], [-0.2, 51.4]]],
                    },
                },
            },
        ],
    }


@pytest.fixture
def london_bounds() -> Bounds:
    """London bounding box for tests."""
    return {
        "north": 51.7,
        "south": 51.3,
        "east": 0.2,
        "west": -0.5,
    }


class TestBoundsToPolygon:
    """Tests for _bounds_to_polygon helper function."""

    def test_bounds_to_polygon_format(self) -> None:
        """Test polygon string format is correct for ODS API."""
        bounds: Bounds = {
            "north": 51.6,
            "south": 51.4,
            "east": 0.1,
            "west": -0.2,
        }
        polygon = _bounds_to_polygon(bounds)

        # Should be in format: (lon,lat),(lon,lat),...
        # Clockwise from SW corner, closed polygon
        expected = "(-0.2,51.4),(-0.2,51.6),(0.1,51.6),(0.1,51.4),(-0.2,51.4)"
        assert polygon == expected

    def test_bounds_to_polygon_with_integers(self) -> None:
        """Test bounds with integer coordinates."""
        bounds: Bounds = {
            "north": 52,
            "south": 51,
            "east": 1,
            "west": 0,
        }
        polygon = _bounds_to_polygon(bounds)

        # Should handle integers correctly
        expected = "(0,51),(0,52),(1,52),(1,51),(0,51)"
        assert polygon == expected

    def test_bounds_to_polygon_negative_coords(self) -> None:
        """Test bounds with negative coordinates."""
        bounds: Bounds = {
            "north": 52.0,
            "south": 51.0,
            "east": -0.5,
            "west": -1.5,
        }
        polygon = _bounds_to_polygon(bounds)

        expected = "(-1.5,51.0),(-1.5,52.0),(-0.5,52.0),(-0.5,51.0),(-1.5,51.0)"
        assert polygon == expected


class TestListGeoDatasets:
    """Tests for list_geo_datasets function."""

    def test_list_geo_datasets_returns_sorted_list(self) -> None:
        """Test that geo datasets list is sorted."""
        datasets = list_geo_datasets()
        assert datasets == sorted(datasets)

    def test_list_geo_datasets_matches_constant(self) -> None:
        """Test that list matches GEO_ENABLED_DATASETS constant."""
        datasets = list_geo_datasets()
        assert set(datasets) == GEO_ENABLED_DATASETS

    def test_expected_datasets_present(self) -> None:
        """Test that expected geo datasets are present."""
        datasets = list_geo_datasets()
        assert "primary_areas" in datasets
        assert "secondary_sites" in datasets
        assert "hv_overhead_lines" in datasets
        assert "lv_poles" in datasets

    def test_geo_datasets_not_empty(self) -> None:
        """Test that there are geo datasets available."""
        datasets = list_geo_datasets()
        assert len(datasets) > 0
        assert len(datasets) >= 10  # Should have at least 10 geo datasets


class TestQueryBoundsValidation:
    """Tests for query_bounds input validation."""

    def test_invalid_dataset_raises_error(self, london_bounds: Bounds) -> None:
        """Test that invalid dataset names raise ValueError."""
        with pytest.raises(ValueError, match="Unknown dataset"):
            query_bounds(
                bounds=london_bounds,
                datasets=["nonexistent_dataset"],
                api_key=TEST_API_KEY,
            )

    def test_multiple_invalid_datasets_listed(self, london_bounds: Bounds) -> None:
        """Test that multiple invalid datasets are listed in error."""
        with pytest.raises(ValueError, match="invalid1.*invalid2|invalid2.*invalid1"):
            query_bounds(
                bounds=london_bounds,
                datasets=["invalid1", "invalid2"],
                api_key=TEST_API_KEY,
            )

    def test_valid_dataset_accepted(self, london_bounds: Bounds, httpx_mock: HTTPXMock, mock_geo_records_response: dict[str, Any]) -> None:
        """Test that valid dataset names don't raise validation errors."""
        # Mock the response so we don't actually call the API
        httpx_mock.add_response(json=mock_geo_records_response)

        # This should not raise
        result = query_bounds(
            bounds=london_bounds,
            datasets=["primary_areas"],  # Valid dataset
            api_key=TEST_API_KEY,
        )

        # Should return a result (even if mocked)
        assert "results" in result
        assert "errors" in result


class TestQueryBoundsSync:
    """Tests for synchronous query_bounds function."""

    def test_query_bounds_returns_dict(
        self,
        httpx_mock: HTTPXMock,
        mock_geo_records_response: dict[str, Any],
        london_bounds: Bounds,
    ) -> None:
        """Test that query_bounds returns a properly structured dict."""
        httpx_mock.add_response(json=mock_geo_records_response)

        result = query_bounds(
            bounds=london_bounds,
            datasets=["primary_areas"],
            api_key=TEST_API_KEY,
        )

        # Should return SpatialQueryResult structure
        assert isinstance(result, dict)
        assert "results" in result
        assert "errors" in result
        assert isinstance(result["results"], dict)
        assert isinstance(result["errors"], dict)

    def test_query_bounds_success(
        self,
        httpx_mock: HTTPXMock,
        mock_geo_records_response: dict[str, Any],
        london_bounds: Bounds,
    ) -> None:
        """Test successful query returns data."""
        httpx_mock.add_response(json=mock_geo_records_response)

        result = query_bounds(
            bounds=london_bounds,
            datasets=["primary_areas"],
            api_key=TEST_API_KEY,
        )

        # Should have results for the dataset
        assert "primary_areas" in result["results"]
        assert result["results"]["primary_areas"].total_count == 2
        assert len(result["errors"]) == 0

    def test_query_bounds_handles_api_error(
        self,
        httpx_mock: HTTPXMock,
        london_bounds: Bounds,
    ) -> None:
        """Test that API errors are captured in errors dict."""
        # Mock an error response
        httpx_mock.add_response(status_code=500, json={"message": "Server error"})

        result = query_bounds(
            bounds=london_bounds,
            datasets=["primary_areas"],
            api_key=TEST_API_KEY,
        )

        # Should have error captured
        assert "primary_areas" in result["errors"]
        assert len(result["results"]) == 0


class TestModuleExports:
    """Tests for module exports."""

    def test_spatial_module_importable(self) -> None:
        """Test spatial module can be imported from ukpyn."""
        from ukpyn import spatial as sp

        assert hasattr(sp, "query_bounds")
        assert hasattr(sp, "query_bounds_async")
        assert hasattr(sp, "list_geo_datasets")

    def test_bounds_type_available(self) -> None:
        """Test Bounds TypedDict is exported."""
        from ukpyn.spatial import Bounds

        # Should be usable as a type hint
        bounds: Bounds = {"north": 51.5, "south": 51.4, "east": 0.1, "west": -0.1}
        assert bounds["north"] == 51.5

    def test_spatial_in_ukpyn_all(self) -> None:
        """Test that spatial is exported from ukpyn.__all__."""
        import ukpyn

        assert "spatial" in ukpyn.__all__

    def test_geo_enabled_datasets_exported(self) -> None:
        """Test GEO_ENABLED_DATASETS constant is exported."""
        from ukpyn.spatial import GEO_ENABLED_DATASETS

        assert isinstance(GEO_ENABLED_DATASETS, set)
        assert len(GEO_ENABLED_DATASETS) > 0


class TestIntegration:
    """Integration tests that require API access."""

    @pytest.mark.integration
    def test_query_bounds_real_api(
        self,
        london_bounds: Bounds,
        api_key_available: bool,
    ) -> None:
        """Test query_bounds with real API (requires API key)."""
        if not api_key_available:
            pytest.fail(
                "UKPN_API_KEY is required for integration tests. "
                "Set it in your environment or .env file, then rerun with: "
                "pytest -m integration"
            )

        result = query_bounds(
            bounds=london_bounds,
            datasets=["primary_areas"],
            limit=5,
        )

        # Should return data without errors
        assert "results" in result
        assert len(result["errors"]) == 0
        assert "primary_areas" in result["results"]
