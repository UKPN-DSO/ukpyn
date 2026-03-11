"""Tests for Flexibility orchestrator."""

from ukpyn.orchestrators import flexibility
from ukpyn.orchestrators.flexibility import FlexibilityOrchestrator
from ukpyn.registry import FLEXIBILITY_DATASETS

# Test constants
TEST_API_KEY = "test-api-key-12345"


class TestFlexibilityDatasetsMapping:
    """Tests for Flexibility datasets mapping."""

    def test_flexibility_datasets_mapping_exists(self) -> None:
        """Test that FLEXIBILITY_DATASETS mapping is defined."""
        assert FLEXIBILITY_DATASETS is not None
        assert isinstance(FLEXIBILITY_DATASETS, dict)

    def test_flexibility_datasets_mapping_not_empty(self) -> None:
        """Test that FLEXIBILITY_DATASETS mapping is not empty."""
        assert len(FLEXIBILITY_DATASETS) > 0

    def test_flexibility_datasets_contains_dispatches(self) -> None:
        """Test that dispatches is in Flexibility datasets."""
        assert "dispatches" in FLEXIBILITY_DATASETS

    def test_flexibility_datasets_contains_dispatch_events(self) -> None:
        """Test that dispatch_events is in Flexibility datasets."""
        assert "dispatch_events" in FLEXIBILITY_DATASETS

    def test_dispatches_and_dispatch_events_same_id(self) -> None:
        """Test that dispatches and dispatch_events map to same dataset ID."""
        assert (
            FLEXIBILITY_DATASETS["dispatches"]
            == FLEXIBILITY_DATASETS["dispatch_events"]
        )

    def test_orchestrator_uses_flexibility_datasets(self) -> None:
        """Test that FlexibilityOrchestrator uses FLEXIBILITY_DATASETS mapping."""
        orchestrator = FlexibilityOrchestrator(api_key=TEST_API_KEY)

        assert orchestrator.DATASETS == FLEXIBILITY_DATASETS


class TestGetDispatchesFunctionExists:
    """Tests for Flexibility get_dispatches function existence and interface."""

    def test_get_dispatches_function_exists(self) -> None:
        """Test that flexibility.get_dispatches function exists."""
        assert hasattr(flexibility, "get_dispatches")
        assert callable(flexibility.get_dispatches)

    def test_orchestrator_has_get_dispatches_method(self) -> None:
        """Test that FlexibilityOrchestrator has get_dispatches method."""
        orchestrator = FlexibilityOrchestrator(api_key=TEST_API_KEY)

        assert hasattr(orchestrator, "get_dispatches")
        assert callable(orchestrator.get_dispatches)

    def test_orchestrator_has_get_dispatches_async_method(self) -> None:
        """Test that FlexibilityOrchestrator has get_dispatches_async method."""
        orchestrator = FlexibilityOrchestrator(api_key=TEST_API_KEY)

        assert hasattr(orchestrator, "get_dispatches_async")
        assert callable(orchestrator.get_dispatches_async)

    def test_get_function_exists(self) -> None:
        """Test that flexibility.get function exists."""
        assert hasattr(flexibility, "get")
        assert callable(flexibility.get)

    def test_get_async_function_exists(self) -> None:
        """Test that flexibility.get_async function exists."""
        assert hasattr(flexibility, "get_async")
        assert callable(flexibility.get_async)

    def test_export_function_exists(self) -> None:
        """Test that flexibility.export function exists."""
        assert hasattr(flexibility, "export")
        assert callable(flexibility.export)


class TestFlexibilityOrchestratorInitialization:
    """Tests for FlexibilityOrchestrator initialization."""

    def test_orchestrator_init_with_api_key(self) -> None:
        """Test FlexibilityOrchestrator can be initialized with API key."""
        orchestrator = FlexibilityOrchestrator(api_key=TEST_API_KEY)

        assert orchestrator._api_key == TEST_API_KEY

    def test_orchestrator_init_without_api_key(self) -> None:
        """Test FlexibilityOrchestrator can be initialized without API key."""
        orchestrator = FlexibilityOrchestrator()

        assert orchestrator._api_key is None

    def test_orchestrator_resolve_dispatches(self) -> None:
        """Test resolving dispatches to actual dataset ID."""
        orchestrator = FlexibilityOrchestrator(api_key=TEST_API_KEY)

        result = orchestrator.resolve_dataset_id("dispatches")

        assert result == FLEXIBILITY_DATASETS["dispatches"]


class TestFlexibilityAvailableDatasets:
    """Tests for Flexibility available_datasets."""

    def test_module_level_available_datasets(self) -> None:
        """Test that flexibility.available_datasets is defined."""
        assert hasattr(flexibility, "available_datasets")
        assert isinstance(flexibility.available_datasets, list)

    def test_available_datasets_contains_expected_names(self) -> None:
        """Test that available_datasets contains expected dataset names."""
        assert "dispatches" in flexibility.available_datasets

    def test_orchestrator_available_datasets(self) -> None:
        """Test that FlexibilityOrchestrator.available_datasets works."""
        orchestrator = FlexibilityOrchestrator(api_key=TEST_API_KEY)

        result = orchestrator.available_datasets

        assert isinstance(result, list)
        assert "dispatches" in result

    def test_available_datasets_matches_orchestrator(self) -> None:
        """Test that module and orchestrator available_datasets match."""
        orchestrator = FlexibilityOrchestrator(api_key=TEST_API_KEY)

        assert set(flexibility.available_datasets) == set(
            orchestrator.available_datasets
        )
