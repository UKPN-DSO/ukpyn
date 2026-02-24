"""Tests for BaseOrchestrator."""

import pytest

from ukpyn.orchestrators.base import BaseOrchestrator


class OrchestratorForTesting(BaseOrchestrator):
    """Concrete implementation of BaseOrchestrator for testing."""

    DATASETS = {
        "friendly_name": "actual-dataset-id",
        "another_name": "another-dataset-id",
        "alias": "actual-dataset-id",  # Alias pointing to same dataset
    }


class TestResolveDatasetId:
    """Tests for resolve_dataset_id method."""

    def test_resolve_dataset_id_with_friendly_name(self) -> None:
        """Test resolving a friendly name to actual dataset ID."""
        orchestrator = OrchestratorForTesting()

        result = orchestrator.resolve_dataset_id("friendly_name")

        assert result == "actual-dataset-id"

    def test_resolve_dataset_id_with_alias(self) -> None:
        """Test resolving an alias to actual dataset ID."""
        orchestrator = OrchestratorForTesting()

        result = orchestrator.resolve_dataset_id("alias")

        assert result == "actual-dataset-id"

    def test_resolve_dataset_id_with_actual_id(self) -> None:
        """Test that passing an actual dataset ID returns it unchanged."""
        orchestrator = OrchestratorForTesting()

        # If the value is already the actual dataset ID, it should be returned
        result = orchestrator.resolve_dataset_id("actual-dataset-id")

        assert result == "actual-dataset-id"

    def test_resolve_dataset_id_with_different_actual_id(self) -> None:
        """Test resolving when actual ID is passed directly."""
        orchestrator = OrchestratorForTesting()

        result = orchestrator.resolve_dataset_id("another-dataset-id")

        assert result == "another-dataset-id"


class TestUnknownDatasetRaisesError:
    """Tests for error handling with unknown datasets."""

    def test_unknown_dataset_raises_value_error(self) -> None:
        """Test that unknown dataset name raises ValueError."""
        orchestrator = OrchestratorForTesting()

        with pytest.raises(ValueError) as exc_info:
            orchestrator.resolve_dataset_id("nonexistent_dataset")

        assert "Unknown dataset" in str(exc_info.value)
        assert "nonexistent_dataset" in str(exc_info.value)

    def test_unknown_dataset_error_lists_available(self) -> None:
        """Test that error message lists available datasets."""
        orchestrator = OrchestratorForTesting()

        with pytest.raises(ValueError) as exc_info:
            orchestrator.resolve_dataset_id("bad_name")

        error_message = str(exc_info.value)
        assert "Available datasets" in error_message
        # Check that at least some available datasets are listed
        assert "friendly_name" in error_message or "another_name" in error_message

    def test_empty_string_raises_error(self) -> None:
        """Test that empty string raises ValueError."""
        orchestrator = OrchestratorForTesting()

        with pytest.raises(ValueError) as exc_info:
            orchestrator.resolve_dataset_id("")

        assert "Unknown dataset" in str(exc_info.value)


class TestAvailableDatasetsProperty:
    """Tests for available_datasets property."""

    def test_available_datasets_returns_list(self) -> None:
        """Test that available_datasets returns a list."""
        orchestrator = OrchestratorForTesting()

        result = orchestrator.available_datasets

        assert isinstance(result, list)

    def test_available_datasets_contains_friendly_names(self) -> None:
        """Test that available_datasets contains all friendly names."""
        orchestrator = OrchestratorForTesting()

        result = orchestrator.available_datasets

        assert "friendly_name" in result
        assert "another_name" in result
        assert "alias" in result

    def test_available_datasets_count(self) -> None:
        """Test that available_datasets has correct count."""
        orchestrator = OrchestratorForTesting()

        result = orchestrator.available_datasets

        assert len(result) == 3

    def test_available_datasets_does_not_contain_dataset_ids(self) -> None:
        """Test that available_datasets contains friendly names, not IDs."""
        orchestrator = OrchestratorForTesting()

        result = orchestrator.available_datasets

        # Should contain friendly names, not the actual dataset IDs
        assert "actual-dataset-id" not in result
        assert "another-dataset-id" not in result


class TestBaseOrchestratorEmptyDatasets:
    """Tests for BaseOrchestrator with empty DATASETS."""

    def test_empty_datasets_available_datasets(self) -> None:
        """Test available_datasets with empty DATASETS mapping."""
        orchestrator = BaseOrchestrator()

        result = orchestrator.available_datasets

        assert result == []

    def test_empty_datasets_resolve_raises_error(self) -> None:
        """Test resolve_dataset_id raises error with empty DATASETS."""
        orchestrator = BaseOrchestrator()

        with pytest.raises(ValueError) as exc_info:
            orchestrator.resolve_dataset_id("any_name")

        assert "Unknown dataset" in str(exc_info.value)
