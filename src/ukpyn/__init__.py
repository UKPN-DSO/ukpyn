"""
ukpyn - Python client for UK Power Networks OpenDataSoft API.

A Python client library for accessing UK Power Networks data through
the OpenDataSoft API v2.1.

Usage:
    # Low-level client
    from ukpyn import UKPNClient

    async with UKPNClient(api_key="your-key") as client:
        datasets = await client.list_datasets()
        records = await client.get_records("dataset-id")

    # Orchestrators (ergonomic access)
    from ukpyn import ltds, flexibility, network, powerflow, curtailment

    # Simple sync access
    data = ltds.get('table_3a')
    dispatches = flexibility.get_dispatches(start_date='2024-01-01')
    events = curtailment.get_events(start_date='2024-01-01')

    # Power flow time series
    timeseries = powerflow.get_circuit_timeseries(voltage='132kv')

    # Spatial queries across datasets
    from ukpyn import spatial
    results = spatial.query_bounds(
        bounds={"north": 51.6, "south": 51.4, "east": 0.1, "west": -0.2},
        datasets=["primary_substations", "secondary_sites"],
    )

    # Async access
    data = await ltds.get_async('table_3a')
"""

from importlib.metadata import version

__version__ = version("ukpyn")
__author__ = "Dr Jamie M. Bright"

# Import spatial query module
# Import utility modules
from . import spatial, utils
from .client import UKPNClient
from .config import Config
from .exceptions import (
    AuthenticationError,
    NotFoundError,
    RateLimitError,
    ServerError,
    UKPNError,
    ValidationError,
)
from .models import (
    EXPORT_FORMATS,
    Dataset,
    DatasetField,
    DatasetListResponse,
    DatasetMetas,
    DatasetResponse,
    Record,
    RecordListResponse,
    RecordResponse,
)

# Import orchestrator modules for easy access
# Import orchestrator classes
from .orchestrators import (
    BaseOrchestrator,
    CurtailmentOrchestrator,
    DERSOrchestrator,
    DFESOrchestrator,
    DNOAOrchestrator,
    FlexibilityOrchestrator,
    GISOrchestrator,
    LTDSOrchestrator,
    NetworkOrchestrator,
    PowerflowOrchestrator,
    ResourcesOrchestrator,
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

__all__ = [
    # Version
    "__version__",
    # Client
    "UKPNClient",
    # Config
    "Config",
    # Exceptions
    "UKPNError",
    "AuthenticationError",
    "RateLimitError",
    "NotFoundError",
    "ValidationError",
    "ServerError",
    # Models
    "Dataset",
    "DatasetField",
    "DatasetListResponse",
    "DatasetMetas",
    "DatasetResponse",
    "Record",
    "RecordListResponse",
    "RecordResponse",
    "EXPORT_FORMATS",
    # Orchestrator modules (for `from ukpyn import ltds`)
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
    # Spatial query module
    "spatial",
    # Utility modules
    "utils",
    # Orchestrator classes
    "BaseOrchestrator",
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
