"""Tests for DERS orchestrator naming and mappings."""

from ukpyn import ders
from ukpyn.orchestrators.ders import DERSOrchestrator
from ukpyn.registry import DER_DATASETS, RESOURCES_DATASETS

# Test constants
TEST_API_KEY = "test-api-key-12345"


def test_der_datasets_alias_backward_compatible() -> None:
    """DER_DATASETS and RESOURCES_DATASETS should resolve to same mapping."""
    assert DER_DATASETS == RESOURCES_DATASETS


def test_ders_orchestrator_uses_der_datasets() -> None:
    """DERSOrchestrator should use DER_DATASETS mapping."""
    orchestrator = DERSOrchestrator(api_key=TEST_API_KEY)
    assert orchestrator.DATASETS == DER_DATASETS


def test_ders_module_get_exists() -> None:
    """Canonical ders module should expose get convenience function."""
    orchestrator = DERSOrchestrator(api_key=TEST_API_KEY)
    assert hasattr(orchestrator, "get")
    assert callable(orchestrator.get)
