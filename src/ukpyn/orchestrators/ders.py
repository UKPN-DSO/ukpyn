"""DERS (Distributed Energy Resources) orchestrator.

Canonical naming for embedded capacity and large demand datasets.
"""

from ..registry import DER_DATASETS
from .resources import (  # Backward-compatible implementation reuse
    ResourcesOrchestrator,
)

available_datasets = list(DER_DATASETS.keys())


class DERSOrchestrator(ResourcesOrchestrator):
    """Canonical orchestrator name for DER datasets."""

    DATASETS = DER_DATASETS
