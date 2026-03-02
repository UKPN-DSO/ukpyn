"""Pydantic models for OpenDataSoft API v2.1 responses."""

from datetime import datetime
from html import escape
from typing import Any

from pydantic import AliasChoices, BaseModel, Field, model_validator


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


class DatasetDescription:
    """Notebook-friendly dataset description with text and rich HTML repr."""

    def __init__(self, text: str, html: str) -> None:
        self._text = text
        self._html = html

    def __str__(self) -> str:
        return self._text

    def __repr__(self) -> str:
        return self._text

    def _repr_html_(self) -> str:
        return self._html


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

    @property
    def title(self) -> str:
        """Return dataset title from metadata, when available."""
        if self.metas and self.metas.default:
            maybe_title = self.metas.default.get("title")
            if isinstance(maybe_title, str) and maybe_title:
                return maybe_title
        return "N/A"

    @property
    def description(self) -> str:
        """Return dataset description from metadata, when available."""
        if self.metas and self.metas.default:
            maybe_description = self.metas.default.get("description")
            if isinstance(maybe_description, str) and maybe_description:
                return maybe_description
        return "N/A"

    @property
    def record_count(self) -> Any:
        """Return record count from metadata, when available."""
        if self.metas and self.metas.default:
            return self.metas.default.get("records_count", "N/A")
        return "N/A"

    @property
    def url(self) -> str:
        """Return canonical OpenDataSoft explore URL for this dataset."""
        return f"https://ukpowernetworks.opendatasoft.com/explore/dataset/{self.dataset_id}"

    @property
    def field_ids(self) -> list[str]:
        """Return dataset field identifiers as a simple list."""
        if not self.fields:
            return []
        return [field.name for field in self.fields]

    def check_fields(self, cols: list[str]) -> list[str]:
        """Return only requested columns that exist on this dataset."""
        available_fields = set(self.field_ids)
        if not available_fields:
            return []
        return [col for col in cols if col in available_fields]

    def summary(self) -> str:
        """Return a concise, human-readable summary of this dataset."""
        title_text = f", title='{self.title}'" if self.title != "N/A" else ""
        return (
            f"Dataset(id='{self.dataset_id}'{title_text}, "
            f"has_records={self.has_records}, records={self.record_count}, "
            f"fields={len(self.fields) if self.fields else 0})"
        )

    def details(self) -> DatasetDescription:
        """Return detailed dataset information with automatic notebook HTML rendering."""
        title = self.title
        record_count = self.record_count
        field_count = len(self.fields) if self.fields else 0
        dataset_url = self.url
        field_lines = []
        if self.fields:
            for field in self.fields:
                field_description = field.description or "No description"
                field_lines.append(
                    f"- {field.name} ({field.type}), {field_description}"
                )
        else:
            field_lines.append("- None")

        text = "\n".join(
            [
                f"Dataset: {title}",
                f"Dataset ID: {self.dataset_id}",
                f"Has records: {self.has_records}",
                f"Records: {record_count}",
                f"URL: {dataset_url}",
                f"Fields (total): {field_count}",
                *field_lines,
            ]
        )
        html_fields = "".join(
            [
                "<li>None</li>"
                if not self.fields
                else "".join(
                    f"<li>{escape(field.name)} ({escape(field.type)}), "
                    f"{escape(field.description or 'No description')}</li>"
                    for field in self.fields
                )
            ]
        )
        html = (
            "<div style='max-height: 240px; overflow-y: auto; border: 1px solid #ddd; "
            "padding: 10px; border-radius: 6px; font-family: system-ui, sans-serif;'>"
            f"<p><b>Dataset:</b> {escape(str(title))}</p>"
            f"<p><b>Dataset ID:</b> {escape(str(self.dataset_id))}</p>"
            f"<p><b>Has records:</b> {escape(str(self.has_records))}</p>"
            f"<p><b>Records:</b> {escape(str(record_count))}</p>"
            f"<p><b>URL:</b> <a href='{escape(str(dataset_url), quote=True)}' "
            "target='_blank' rel='noopener noreferrer'>"
            f"{escape(str(dataset_url))}</a></p>"
            f"<p><b>Fields (total):</b> {field_count}</p>"
            f"<ul>{html_fields}</ul>"
            "</div>"
        )

        return DatasetDescription(text=text, html=html)


class DatasetListItem(BaseModel):
    """Dataset item in a list response."""

    dataset: Dataset
    links: list[Link] | None = None

    @property
    def id(self) -> str:
        """Return dataset identifier alias for convenience."""
        return self.dataset.dataset_id

    @property
    def title(self) -> str:
        """Proxy dataset title for convenience."""
        return self.dataset.title

    @property
    def description(self) -> str:
        """Proxy dataset description for convenience."""
        return self.dataset.description

    @property
    def record_count(self) -> Any:
        """Proxy dataset record count for convenience."""
        return self.dataset.record_count

    @property
    def url(self) -> str:
        """Proxy dataset URL for convenience."""
        return self.dataset.url

    def summary(self) -> str:
        """Proxy dataset summary for convenience."""
        return self.dataset.summary()

    def details(self) -> DatasetDescription:
        """Proxy dataset details for convenience."""
        return self.dataset.details()

    @model_validator(mode="before")
    @classmethod
    def _normalize_dataset_item(cls, value: Any) -> Any:
        """Support both wrapped and flat dataset list item formats."""
        if isinstance(value, dict) and "dataset" not in value:
            return {"dataset": value}
        return value


class DatasetListResponse(BaseModel):
    """Response from list_datasets endpoint."""

    total_count: int
    links: list[Link] | None = None
    datasets: list[DatasetListItem] = Field(
        default_factory=list,
        validation_alias=AliasChoices("datasets", "results"),
    )

    def summary(self) -> str:
        """Return a concise, human-readable summary of dataset list results."""
        returned_count = len(self.datasets)
        sample_ids = [item.dataset.dataset_id for item in self.datasets[:5]]
        sample_text = ", ".join(sample_ids) if sample_ids else "none"
        suffix = " ..." if returned_count > 5 else ""
        return (
            f"DatasetListResponse(total_count={self.total_count}, "
            f"returned={returned_count}, sample_ids=[{sample_text}{suffix}])"
        )


class DatasetResponse(BaseModel):
    """Response from get_dataset endpoint."""

    dataset: Dataset
    links: list[Link] | None = None

    @model_validator(mode="before")
    @classmethod
    def _normalize_dataset_response(cls, value: Any) -> Any:
        """Support both wrapped and flat dataset response formats."""
        if isinstance(value, dict) and "dataset" not in value:
            return {"dataset": value}
        return value

    def summary(self) -> str:
        """Return a concise, human-readable summary of this dataset response."""
        return f"DatasetResponse({self.dataset.summary()})"


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

    @model_validator(mode="before")
    @classmethod
    def _normalize_record(cls, value: Any) -> Any:
        """Support both wrapped and flat record response formats."""
        if not isinstance(value, dict):
            return value

        if "id" in value:
            return value

        if "fields" in value and isinstance(value["fields"], dict):
            candidate_id = (
                value.get("recordid")
                or value.get("record_id")
                or value["fields"].get("id")
                or "unknown"
            )
            return {
                "id": str(candidate_id),
                "fields": value["fields"],
                "timestamp": value.get("timestamp"),
                "record_timestamp": value.get("record_timestamp"),
                "size": value.get("size"),
                "links": value.get("links"),
            }

        fields = {
            key: val
            for key, val in value.items()
            if key
            not in {
                "recordid",
                "record_id",
                "timestamp",
                "record_timestamp",
                "size",
                "links",
            }
        }
        candidate_id = value.get("recordid") or value.get("record_id") or "unknown"
        return {
            "id": str(candidate_id),
            "fields": fields,
            "timestamp": value.get("timestamp"),
            "record_timestamp": value.get("record_timestamp"),
            "size": value.get("size"),
            "links": value.get("links"),
        }

    def summary(self) -> str:
        """Return a concise, human-readable summary of this record."""
        field_names = sorted(self.fields.keys()) if self.fields else []
        field_count = len(field_names)
        preview_names = field_names[:5]
        preview_text = ", ".join(preview_names) if preview_names else "none"
        suffix = " ..." if field_count > 5 else ""
        return (
            f"Record(id='{self.id}', field_count={field_count}, "
            f"fields=[{preview_text}{suffix}])"
        )


class RecordListResponse(BaseModel):
    """Response from get_records endpoint."""

    total_count: int
    links: list[Link] | None = None
    records: list[Record] = Field(
        default_factory=list,
        validation_alias=AliasChoices("records", "results"),
    )

    def summary(self) -> str:
        """Return a concise, human-readable summary of record list results."""
        returned_count = len(self.records)
        sample_ids = [record.id for record in self.records[:5]]
        sample_text = ", ".join(sample_ids) if sample_ids else "none"
        suffix = " ..." if returned_count > 5 else ""
        return (
            f"RecordListResponse(total_count={self.total_count}, "
            f"returned={returned_count}, sample_ids=[{sample_text}{suffix}])"
        )


class RecordResponse(BaseModel):
    """Response from get single record endpoint."""

    record: Record
    links: list[Link] | None = None

    def summary(self) -> str:
        """Return a concise, human-readable summary of this record response."""
        return f"RecordResponse({self.record.summary()})"


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
