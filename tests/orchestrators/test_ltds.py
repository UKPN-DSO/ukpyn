"""Tests for LTDS (Long Term Development Statement) orchestrator."""

from typing import Any

import pytest
from pytest_httpx import HTTPXMock

from ukpyn.orchestrators import ltds
from ukpyn.orchestrators.ltds import LTDSOrchestrator
from ukpyn.registry import LTDS_DATASETS

# Test constants
TEST_API_KEY = "test-api-key-12345"


class TestLTDSDatasetsMapping:
    """Tests for LTDS datasets mapping."""

    def test_ltds_datasets_mapping_exists(self) -> None:
        """Test that LTDS_DATASETS mapping is defined."""
        assert LTDS_DATASETS is not None
        assert isinstance(LTDS_DATASETS, dict)

    def test_ltds_datasets_mapping_not_empty(self) -> None:
        """Test that LTDS_DATASETS mapping is not empty."""
        assert len(LTDS_DATASETS) > 0

    def test_ltds_datasets_contains_table_3a(self) -> None:
        """Test that table_3a is in LTDS datasets."""
        assert "table_3a" in LTDS_DATASETS

    def test_ltds_datasets_contains_observed_peak_demand(self) -> None:
        """Test that observed_peak_demand is in LTDS datasets."""
        assert "observed_peak_demand" in LTDS_DATASETS

    def test_ltds_datasets_contains_projects(self) -> None:
        """Test that projects is in LTDS datasets."""
        assert "projects" in LTDS_DATASETS

    def test_ltds_datasets_contains_infrastructure_projects(self) -> None:
        """Test that infrastructure_projects is in LTDS datasets."""
        assert "infrastructure_projects" in LTDS_DATASETS

    def test_table_3a_and_observed_peak_demand_same_id(self) -> None:
        """Test that table_3a and observed_peak_demand map to same dataset ID."""
        assert LTDS_DATASETS["table_3a"] == LTDS_DATASETS["observed_peak_demand"]

    def test_projects_and_infrastructure_projects_same_id(self) -> None:
        """Test that projects and infrastructure_projects map to same dataset ID."""
        assert LTDS_DATASETS["projects"] == LTDS_DATASETS["infrastructure_projects"]

    def test_orchestrator_uses_ltds_datasets(self) -> None:
        """Test that LTDSOrchestrator uses LTDS_DATASETS mapping."""
        orchestrator = LTDSOrchestrator(api_key=TEST_API_KEY)

        assert orchestrator.DATASETS == LTDS_DATASETS


class TestGetFunctionExists:
    """Tests for LTDS get function existence and interface."""

    def test_get_function_exists(self) -> None:
        """Test that ltds.get function exists."""
        assert hasattr(ltds, "get")
        assert callable(ltds.get)

    def test_get_async_function_exists(self) -> None:
        """Test that ltds.get_async function exists."""
        assert hasattr(ltds, "get_async")
        assert callable(ltds.get_async)

    def test_export_function_exists(self) -> None:
        """Test that ltds.export function exists."""
        assert hasattr(ltds, "export")
        assert callable(ltds.export)


class TestGetTable3aFunctionExists:
    """Tests for LTDS get_table_3a function existence and interface."""

    def test_get_table_3a_function_exists(self) -> None:
        """Test that ltds.get_table_3a function exists."""
        assert hasattr(ltds, "get_table_3a")
        assert callable(ltds.get_table_3a)

    def test_orchestrator_has_get_table_3a_method(self) -> None:
        """Test that LTDSOrchestrator has get_table_3a method."""
        orchestrator = LTDSOrchestrator(api_key=TEST_API_KEY)

        assert hasattr(orchestrator, "get_table_3a")
        assert callable(orchestrator.get_table_3a)

    def test_orchestrator_has_get_table_3a_async_method(self) -> None:
        """Test that LTDSOrchestrator has get_table_3a_async method."""
        orchestrator = LTDSOrchestrator(api_key=TEST_API_KEY)

        assert hasattr(orchestrator, "get_table_3a_async")
        assert callable(orchestrator.get_table_3a_async)

    def test_get_projects_function_exists(self) -> None:
        """Test that ltds.get_projects function exists."""
        assert hasattr(ltds, "get_projects")
        assert callable(ltds.get_projects)

    def test_orchestrator_has_get_projects_method(self) -> None:
        """Test that LTDSOrchestrator has get_projects method."""
        orchestrator = LTDSOrchestrator(api_key=TEST_API_KEY)

        assert hasattr(orchestrator, "get_projects")
        assert callable(orchestrator.get_projects)


class TestAvailableDatasets:
    """Tests for LTDS available_datasets."""

    def test_module_level_available_datasets(self) -> None:
        """Test that ltds.available_datasets is defined."""
        assert hasattr(ltds, "available_datasets")
        assert isinstance(ltds.available_datasets, list)

    def test_available_datasets_contains_expected_names(self) -> None:
        """Test that available_datasets contains expected dataset names."""
        assert "table_3a" in ltds.available_datasets
        assert "projects" in ltds.available_datasets

    def test_orchestrator_available_datasets(self) -> None:
        """Test that LTDSOrchestrator.available_datasets works."""
        orchestrator = LTDSOrchestrator(api_key=TEST_API_KEY)

        result = orchestrator.available_datasets

        assert isinstance(result, list)
        assert "table_3a" in result
        assert "projects" in result

    def test_available_datasets_matches_orchestrator(self) -> None:
        """Test that module and orchestrator available_datasets match."""
        orchestrator = LTDSOrchestrator(api_key=TEST_API_KEY)

        assert set(ltds.available_datasets) == set(orchestrator.available_datasets)


class TestLTDSOrchestratorInitialization:
    """Tests for LTDSOrchestrator initialization."""

    def test_orchestrator_init_with_api_key(self) -> None:
        """Test LTDSOrchestrator can be initialized with API key."""
        orchestrator = LTDSOrchestrator(api_key=TEST_API_KEY)

        assert orchestrator._api_key == TEST_API_KEY

    def test_orchestrator_init_without_api_key(self) -> None:
        """Test LTDSOrchestrator can be initialized without API key."""
        orchestrator = LTDSOrchestrator()

        assert orchestrator._api_key is None

    def test_orchestrator_resolve_table_3a(self) -> None:
        """Test resolving table_3a to actual dataset ID."""
        orchestrator = LTDSOrchestrator(api_key=TEST_API_KEY)

        result = orchestrator.resolve_dataset_id("table_3a")

        assert result == LTDS_DATASETS["table_3a"]

    def test_orchestrator_resolve_projects(self) -> None:
        """Test resolving projects to actual dataset ID."""
        orchestrator = LTDSOrchestrator(api_key=TEST_API_KEY)

        result = orchestrator.resolve_dataset_id("projects")

        assert result == LTDS_DATASETS["projects"]
