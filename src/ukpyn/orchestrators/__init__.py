"""
Dataset orchestrators for ukpyn.

Provides themed, ergonomic access to UK Power Networks datasets.

Usage:
    from ukpyn import ltds, flexibility, network, powerflow, curtailment

    # Simple sync access
    data = ltds.get('table_3a')
    dispatches = flexibility.get_dispatches(start_date='2024-01-01')
    events = curtailment.get_events(start_date='2024-01-01')

    # Power flow time series
    circuits = powerflow.discover_circuits(voltage='132kv')
    timeseries = powerflow.get_circuit_timeseries(voltage='132kv')

    # Async access
    data = await ltds.get_async('table_3a')
"""

from ..registry import (
    ALL_DATASETS,
    CURTAILMENT_DATASETS,
    DER_DATASETS,
    DFES_DATASETS,
    DNOA_DATASETS,
    FLEXIBILITY_DATASETS,
    GEO_DATASETS,
    LTDS_DATASETS,
    NETWORK_DATASETS,
    POWERFLOW_DATASETS,
    RESOURCES_DATASETS,
)
from . import (
    curtailment,
    ders,
    dfes,
    dnoa,
    flexibility,
    gis,
    ltds,
    network,
    powerflow,
    resources,
)
from .base import BaseOrchestrator
from .curtailment import CurtailmentOrchestrator
from .ders import DERSOrchestrator
from .dfes import DFESOrchestrator
from .dnoa import DNOAOrchestrator
from .flexibility import FlexibilityOrchestrator
from .gis import GISOrchestrator
from .ltds import LTDSOrchestrator
from .network import NetworkOrchestrator
from .powerflow import PowerflowOrchestrator
from .resources import ResourcesOrchestrator

__all__ = [
    # Base
    "BaseOrchestrator",
    # Registry
    "ALL_DATASETS",
    "LTDS_DATASETS",
    "DFES_DATASETS",
    "DNOA_DATASETS",
    "NETWORK_DATASETS",
    "FLEXIBILITY_DATASETS",
    "CURTAILMENT_DATASETS",
    "DER_DATASETS",
    "RESOURCES_DATASETS",
    "GEO_DATASETS",
    "POWERFLOW_DATASETS",
    # Orchestrator modules
    "ltds",
    "dfes",
    "dnoa",
    "network",
    "flexibility",
    "curtailment",
    "ders",
    "resources",
    "gis",
    "powerflow",
    # Orchestrator classes
    "LTDSOrchestrator",
    "DFESOrchestrator",
    "DNOAOrchestrator",
    "NetworkOrchestrator",
    "FlexibilityOrchestrator",
    "CurtailmentOrchestrator",
    "DERSOrchestrator",
    "ResourcesOrchestrator",
    "GISOrchestrator",
    "PowerflowOrchestrator",
]
