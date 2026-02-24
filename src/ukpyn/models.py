"""Pydantic models for OpenDataSoft API v2.1 responses."""

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class Link(BaseModel):
    """API link model."""

    href: str
    rel: str


class DatasetField(BaseModel):
    """Dataset field definition."""

    name: str
    label: str | None = None
    type: str
    description: str | None = None
    annotations: dict[str, Any] | None = None


class DatasetMetas(BaseModel):
    """Dataset metadata."""

    default: dict[str, Any] | None = None
    custom: dict[str, Any] | None = None
    admin: dict[str, Any] | None = None


class Dataset(BaseModel):
    """OpenDataSoft Dataset model."""

    dataset_id: str
    dataset_uid: str | None = None
    attachments: list[dict[str, Any]] | None = None
    has_records: bool = True
    data_visible: bool = True
    fields: list[DatasetField] | None = None
    metas: DatasetMetas | None = None
    features: list[str] | None = None
    links: list[Link] | None = None


class DatasetListItem(BaseModel):
    """Dataset item in a list response."""

    dataset: Dataset
    links: list[Link] | None = None


class DatasetListResponse(BaseModel):
    """Response from list_datasets endpoint."""

    total_count: int
    links: list[Link] | None = None
    datasets: list[DatasetListItem] = Field(default_factory=list)


class DatasetResponse(BaseModel):
    """Response from get_dataset endpoint."""

    dataset: Dataset
    links: list[Link] | None = None


class RecordFields(BaseModel):
    """Record field values - dynamic based on dataset."""

    model_config = {"extra": "allow"}


class Record(BaseModel):
    """Individual record from a dataset."""

    id: str
    timestamp: datetime | None = None
    size: int | None = None
    fields: dict[str, Any] | None = None
    record_timestamp: datetime | None = None
    links: list[Link] | None = None


class RecordListResponse(BaseModel):
    """Response from get_records endpoint."""

    total_count: int
    links: list[Link] | None = None
    records: list[Record] = Field(default_factory=list)


class RecordResponse(BaseModel):
    """Response from get single record endpoint."""

    record: Record
    links: list[Link] | None = None


class Facet(BaseModel):
    """Facet model for filtering."""

    name: str
    count: int
    state: str | None = None
    value: str | None = None
    path: str | None = None


class FacetGroup(BaseModel):
    """Group of facets for a field."""

    name: str
    facets: list[Facet] = Field(default_factory=list)


class ErrorResponse(BaseModel):
    """API error response model."""

    error_code: str | None = None
    message: str
    status_code: int | None = None


class ExportFormat(BaseModel):
    """Available export format."""

    format: str
    mime_type: str
    extension: str


# Export formats supported by the API
EXPORT_FORMATS: list[str] = [
    "json",
    "csv",
    "xlsx",
    "geojson",
    "shapefile",
    "kml",
]
