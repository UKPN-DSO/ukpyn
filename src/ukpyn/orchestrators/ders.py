"""DERS (Distributed Energy Resources) orchestrator.

Canonical naming for embedded capacity and large demand datasets.
"""

from ..registry import DER_DATASETS
from .base import _install_module_repr
from .resources import (  # Backward-compatible implementation reuse
    ResourcesOrchestrator,
)

available_datasets = list(DER_DATASETS.keys())

_install_module_repr(__name__, "DERSOrchestrator", DER_DATASETS)


class DERSOrchestrator(ResourcesOrchestrator):
    """Canonical orchestrator name for DER datasets."""

    DATASETS = DER_DATASETS
