"""Time series analysis utilities for UK Power Networks data.

Provides functions for:
- Step change detection (identify sudden shifts in data)
- Quality control (outlier detection, data validation)
- Gap filling (interpolation and imputation methods)

These utilities are designed to work with powerflow time series data
from the UKPN Open Data Portal.

Note:
    These functions require pandas. Install with: pip install ukpyn[notebooks]
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal

# Lazy pandas import
_pd = None


def _get_pandas():
    """Lazy load pandas with helpful error message."""
    global _pd
    if _pd is None:
        try:
            import pandas as pd

            _pd = pd
        except ImportError as err:
            raise ImportError(
                "pandas is required for time series utilities.\n"
                "Install with: pip install ukpyn[notebooks]\n"
                "Or: pip install pandas"
            ) from err
    return _pd


# =============================================================================
# Data Structures
# =============================================================================


@dataclass
class StepChange:
    """Detected step change in time series data."""

    timestamp: datetime
    """When the step change occurred."""

    value_before: float
    """Mean value before the change."""

    value_after: float
    """Mean value after the change."""

    magnitude: float
    """Absolute change magnitude (value_after - value_before)."""

    relative_change: float
    """Relative change as percentage of value_before."""

    direction: Literal["increase", "decrease"]
    """Direction of the change."""

    confidence: float
    """Confidence score (0-1) based on statistical significance."""


@dataclass
class GapInfo:
    """Information about a gap in time series data."""

    start: datetime
    """Start of the gap (last valid timestamp before gap)."""

    end: datetime
    """End of the gap (first valid timestamp after gap)."""

    duration_hours: float
    """Duration of the gap in hours."""

    missing_points: int
    """Estimated number of missing data points."""


@dataclass
class QualityReport:
    """Quality control report for time series data."""

    total_points: int
    """Total number of data points."""

    valid_points: int
    """Number of valid (non-flagged) points."""

    missing_points: int
    """Number of missing/null values."""

    outlier_points: int
    """Number of detected outliers."""

    gaps: list[GapInfo] = field(default_factory=list)
    """List of detected gaps."""

    quality_score: float = 0.0
    """Overall quality score (0-100)."""

    issues: list[str] = field(default_factory=list)
    """List of identified quality issues."""


# =============================================================================
# Conversion Utilities
# =============================================================================


def records_to_timeseries(
    records: list[Any],
    timestamp_field: str = "timestamp",
    value_field: str | None = None,
    value_fields: list[str] | None = None,
) -> Any:
    """
    Convert ukpyn records to a pandas DataFrame/Series for analysis.

    Args:
        records: List of Record objects from ukpyn (e.g., from RecordListResponse.records)
        timestamp_field: Name of the timestamp field in records
        value_field: Name of the value field (returns Series if specified)
        value_fields: List of value fields (returns DataFrame with these columns)

    Returns:
        pandas.Series if value_field specified, otherwise pandas.DataFrame

    Example:
        from ukpyn import powerflow
        from ukpyn.utils import records_to_timeseries

        # Get powerflow data
        data = powerflow.get_circuit_timeseries(voltage='132kv', limit=1000)

        # Convert to time series
        ts = records_to_timeseries(
            data.records,
            timestamp_field='timestamp',
            value_field='active_power_mw'
        )

        # Now use pandas operations
        print(ts.describe())
    """
    pd = _get_pandas()

    # Extract fields from records
    rows = []
    for record in records:
        if hasattr(record, "fields") and record.fields:
            rows.append(record.fields)
        elif isinstance(record, dict):
            rows.append(record)

    if not rows:
        # Return empty DataFrame/Series
        if value_field:
            return pd.Series(dtype=float, name=value_field)
        return pd.DataFrame()

    # Create DataFrame
    df = pd.DataFrame(rows)

    # Parse timestamp
    if timestamp_field in df.columns:
        df[timestamp_field] = pd.to_datetime(df[timestamp_field], errors="coerce")
        df = df.set_index(timestamp_field).sort_index()

    # Return Series or DataFrame based on args
    if value_field:
        if value_field not in df.columns:
            raise ValueError(f"Field '{value_field}' not found. Available: {list(df.columns)}")
        return df[value_field]

    if value_fields:
        missing = [f for f in value_fields if f not in df.columns]
        if missing:
            raise ValueError(f"Fields not found: {missing}. Available: {list(df.columns)}")
        return df[value_fields]

    return df


# =============================================================================
# Step Change Detection
# =============================================================================


def detect_step_changes(
    series: Any,
    threshold: float = 0.1,
    window_size: int = 24,
    min_confidence: float = 0.7,
) -> list[StepChange]:
    """
    Detect step changes in a time series.

    Step changes are sudden, sustained shifts in the data level that may indicate:
    - Equipment upgrades or replacements
    - Network reconfiguration
    - Measurement errors or sensor changes
    - Load connection/disconnection

    Args:
        series: pandas Series with datetime index
        threshold: Minimum relative change to detect (0.1 = 10%)
        window_size: Number of points before/after for comparison
        min_confidence: Minimum confidence score to include (0-1)

    Returns:
        List of StepChange objects describing detected changes

    Example:
        from ukpyn.utils import detect_step_changes, records_to_timeseries

        ts = records_to_timeseries(data.records, value_field='active_power_mw')
        changes = detect_step_changes(ts, threshold=0.15)

        for change in changes:
            print(f"{change.timestamp}: {change.direction} of {change.relative_change:.1%}")
    """
    pd = _get_pandas()

    if not isinstance(series, pd.Series):
        raise TypeError("series must be a pandas Series")

    if len(series) < window_size * 2:
        return []  # Not enough data

    # Remove nulls for analysis
    series = series.dropna()
    if len(series) < window_size * 2:
        return []

    changes: list[StepChange] = []

    # Calculate differences between consecutive windows
    for i in range(window_size, len(series) - window_size):
        before_start = max(0, i - window_size)
        before_end = i
        after_start = i
        after_end = min(len(series), i + window_size)

        mean_before = series.iloc[before_start:before_end].mean()
        mean_after = series.iloc[after_start:after_end].mean()
        std_before = series.iloc[before_start:before_end].std()

        # Skip if values are too small (avoid division issues)
        if abs(mean_before) < 1e-10:
            continue

        magnitude = mean_after - mean_before
        relative_change = abs(magnitude / mean_before)

        # Check if change exceeds threshold
        if relative_change >= threshold:
            # Calculate confidence based on statistical significance
            if std_before > 0:
                z_score = abs(magnitude) / std_before
                confidence = min(1.0, z_score / 3.0)  # Normalize to 0-1
            else:
                confidence = 1.0 if abs(magnitude) > 0 else 0.0

            if confidence >= min_confidence:
                changes.append(
                    StepChange(
                        timestamp=series.index[i],
                        value_before=mean_before,
                        value_after=mean_after,
                        magnitude=magnitude,
                        relative_change=relative_change,
                        direction="increase" if magnitude > 0 else "decrease",
                        confidence=confidence,
                    )
                )

    # Deduplicate nearby changes (keep highest confidence)
    if changes:
        changes = _deduplicate_changes(changes, window_size)

    return changes


def _deduplicate_changes(
    changes: list[StepChange], min_gap_points: int
) -> list[StepChange]:
    """Remove duplicate step changes that are too close together."""
    if not changes:
        return changes

    # Sort by timestamp
    changes = sorted(changes, key=lambda c: c.timestamp)

    # Keep only the highest confidence change within each window
    result: list[StepChange] = []
    current_group: list[StepChange] = [changes[0]]

    for change in changes[1:]:
        time_diff = (change.timestamp - current_group[0].timestamp).total_seconds()
        # Assume hourly data as default, adjust gap threshold accordingly
        gap_hours = min_gap_points  # Approximate

        if time_diff < gap_hours * 3600:
            current_group.append(change)
        else:
            # Pick best from current group
            best = max(current_group, key=lambda c: c.confidence)
            result.append(best)
            current_group = [change]

    # Don't forget last group
    if current_group:
        best = max(current_group, key=lambda c: c.confidence)
        result.append(best)

    return result


# =============================================================================
# Quality Control
# =============================================================================


def flag_outliers(
    series: Any,
    method: Literal["zscore", "iqr", "mad"] = "iqr",
    threshold: float = 3.0,
) -> Any:
    """
    Flag outliers in a time series.

    Args:
        series: pandas Series to analyze
        method: Outlier detection method:
            - 'zscore': Points more than threshold standard deviations from mean
            - 'iqr': Points outside threshold * IQR from quartiles
            - 'mad': Points more than threshold * MAD from median
        threshold: Sensitivity threshold (default 3.0)

    Returns:
        pandas Series of boolean flags (True = outlier)

    Example:
        from ukpyn.utils import flag_outliers

        outliers = flag_outliers(ts, method='iqr', threshold=2.5)
        print(f"Found {outliers.sum()} outliers")

        # Remove outliers
        clean_ts = ts[~outliers]
    """
    pd = _get_pandas()

    if not isinstance(series, pd.Series):
        raise TypeError("series must be a pandas Series")

    series = series.dropna()
    if len(series) == 0:
        return pd.Series(dtype=bool)

    if method == "zscore":
        mean = series.mean()
        std = series.std()
        if std == 0:
            return pd.Series(False, index=series.index)
        z_scores = (series - mean).abs() / std
        return z_scores > threshold

    elif method == "iqr":
        q1 = series.quantile(0.25)
        q3 = series.quantile(0.75)
        iqr = q3 - q1
        lower = q1 - threshold * iqr
        upper = q3 + threshold * iqr
        return (series < lower) | (series > upper)

    elif method == "mad":
        median = series.median()
        mad = (series - median).abs().median()
        if mad == 0:
            return pd.Series(False, index=series.index)
        modified_z = 0.6745 * (series - median).abs() / mad
        return modified_z > threshold

    else:
        raise ValueError(f"Unknown method: {method}. Use 'zscore', 'iqr', or 'mad'.")


def detect_redaction(
    series: Any,
    markers: list[str | int | float] | None = None,
    include_missing: bool = True,
) -> Any:
    """
    Detect likely redacted points in a series.

    Redaction is detected from explicit marker values first (e.g., "REDACTED",
    "N/A", "SUPPRESSED"). When include_missing=True, null values are also
    treated as redacted as a fallback heuristic.

    Args:
        series: pandas Series to inspect
        markers: Explicit redaction marker values
        include_missing: Whether null values count as redacted points

    Returns:
        pandas Series of boolean flags (True = redacted)
    """
    pd = _get_pandas()

    if not isinstance(series, pd.Series):
        raise TypeError("series must be a pandas Series")

    default_markers: list[str | int | float] = [
        "redacted",
        "suppressed",
        "masked",
        "n/a",
        "na",
        "null",
        "confidential",
    ]
    marker_values = markers if markers is not None else default_markers

    text_markers = {
        str(value).strip().lower()
        for value in marker_values
        if isinstance(value, str)
    }
    literal_markers = [value for value in marker_values if not isinstance(value, str)]

    flags = pd.Series(False, index=series.index, dtype=bool)

    if literal_markers:
        flags = flags | series.isin(literal_markers)

    if text_markers:
        text_values = series.astype("string").str.strip().str.lower()
        flags = flags | text_values.isin(text_markers).fillna(False)

    if include_missing:
        flags = flags | series.isna()

    return flags.fillna(False)


def summarize_redaction_by_period(
    df: Any,
    timestamp_field: str = "timestamp",
    value_fields: list[str] | None = None,
    period: str = "M",
    markers: list[str | int | float] | None = None,
    include_missing_fallback: bool = True,
) -> Any:
    """
    Summarize redaction rates by time period.

    Args:
        df: pandas DataFrame with timestamp and value fields
        timestamp_field: Name of timestamp column
        value_fields: Fields to evaluate (default = all non-timestamp fields)
        period: Pandas period code, e.g. 'M' for month
        markers: Explicit marker values indicating redaction
        include_missing_fallback: Treat missing values as redacted fallback

    Returns:
        pandas DataFrame with period, total_points, redacted_points, redaction_rate
    """
    pd = _get_pandas()

    if not isinstance(df, pd.DataFrame):
        raise TypeError("df must be a pandas DataFrame")

    if df.empty:
        return pd.DataFrame(
            columns=["period", "total_points", "redacted_points", "redaction_rate"]
        )

    working = df.copy()

    if timestamp_field in working.columns:
        timestamps = pd.to_datetime(working[timestamp_field], errors="coerce")
    elif isinstance(working.index, pd.DatetimeIndex):
        timestamps = pd.to_datetime(working.index, errors="coerce")
    else:
        raise ValueError(
            "A datetime timestamp is required via timestamp_field or DatetimeIndex"
        )

    if value_fields is None:
        excluded = {timestamp_field}
        value_fields = [col for col in working.columns if col not in excluded]

    missing_fields = [field for field in value_fields if field not in working.columns]
    if missing_fields:
        raise ValueError(f"Fields not found: {missing_fields}")

    if not value_fields:
        raise ValueError("value_fields must contain at least one column")

    valid_ts = timestamps.notna()
    working = working.loc[valid_ts].copy()
    timestamps = timestamps.loc[valid_ts]

    if working.empty:
        return pd.DataFrame(
            columns=["period", "total_points", "redacted_points", "redaction_rate"]
        )

    redaction_flags = pd.Series(False, index=working.index, dtype=bool)
    for value_column in value_fields:
        redaction_flags = redaction_flags | detect_redaction(
            working[value_column],
            markers=markers,
            include_missing=include_missing_fallback,
        )

    periods = timestamps.dt.to_period(period)
    summary = pd.DataFrame({"period": periods, "redacted": redaction_flags})

    grouped = summary.groupby("period", observed=False).agg(
        total_points=("redacted", "size"),
        redacted_points=("redacted", "sum"),
    )

    grouped["redaction_rate"] = (
        grouped["redacted_points"] / grouped["total_points"]
    ).fillna(0.0)
    grouped = grouped.reset_index()
    grouped["period"] = grouped["period"].dt.to_timestamp()

    return grouped.sort_values("period").reset_index(drop=True)


def detect_rolling_anomalies(
    series: Any,
    window_size: int = 24,
    threshold: float = 6.0,
    min_periods: int | None = None,
) -> Any:
    """
    Detect anomalies using rolling robust z-scores (median + MAD).

    Args:
        series: pandas Series with datetime index
        window_size: Rolling window size in points
        threshold: Robust z-score threshold
        min_periods: Minimum points in rolling windows

    Returns:
        pandas Series of boolean flags (True = anomaly)
    """
    pd = _get_pandas()

    if not isinstance(series, pd.Series):
        raise TypeError("series must be a pandas Series")
    if window_size < 3:
        raise ValueError("window_size must be >= 3")

    effective_min_periods = min_periods or max(3, window_size // 2)

    baseline = series.rolling(window=window_size, min_periods=effective_min_periods).median()
    abs_deviation = (series - baseline).abs()
    rolling_mad = abs_deviation.rolling(
        window=window_size,
        min_periods=effective_min_periods,
    ).median()

    robust_z = 0.6745 * abs_deviation / rolling_mad.replace(0, pd.NA)

    rolling_std = series.rolling(
        window=window_size,
        min_periods=effective_min_periods,
    ).std()
    std_z = abs_deviation / rolling_std.replace(0, pd.NA)

    robust_flags = (robust_z > threshold).fillna(False)

    # Fallback for flat windows where MAD is zero (common in operational data)
    mad_unusable = rolling_mad.isna() | (rolling_mad == 0)
    std_fallback_flags = (std_z > max(3.0, threshold / 2)).fillna(False)

    return robust_flags | (mad_unusable & std_fallback_flags)


def summarize_flow_balance(
    line_power: Any,
    transformer_power: Any,
    tolerance: float = 0.2,
) -> dict[str, Any]:
    """
    Summarize how well aggregated line and transformer power align.

    Args:
        line_power: pandas Series of aggregated line power (e.g., MW)
        transformer_power: pandas Series of aggregated transformer power (e.g., MW)
        tolerance: Relative error tolerance for "in-balance" points

    Returns:
        Dictionary of balance diagnostics
    """
    pd = _get_pandas()

    if not isinstance(line_power, pd.Series):
        raise TypeError("line_power must be a pandas Series")
    if not isinstance(transformer_power, pd.Series):
        raise TypeError("transformer_power must be a pandas Series")
    if tolerance < 0:
        raise ValueError("tolerance must be >= 0")

    aligned = pd.concat(
        [line_power.rename("line_power"), transformer_power.rename("transformer_power")],
        axis=1,
        join="inner",
    ).dropna()

    if aligned.empty:
        return {
            "points_compared": 0,
            "line_total": 0.0,
            "transformer_total": 0.0,
            "mean_abs_difference": 0.0,
            "mean_relative_difference": 0.0,
            "within_tolerance_ratio": 0.0,
            "tolerance": tolerance,
        }

    difference = aligned["line_power"] - aligned["transformer_power"]
    abs_difference = difference.abs()
    relative_difference = abs_difference / aligned["transformer_power"].abs().replace(0, pd.NA)
    within_tolerance = (relative_difference <= tolerance).fillna(False)

    return {
        "points_compared": int(len(aligned)),
        "line_total": float(aligned["line_power"].sum()),
        "transformer_total": float(aligned["transformer_power"].sum()),
        "mean_abs_difference": float(abs_difference.mean()),
        "mean_relative_difference": float(relative_difference.mean(skipna=True) or 0.0),
        "within_tolerance_ratio": float(within_tolerance.mean()),
        "tolerance": float(tolerance),
    }


def quality_control(
    series: Any,
    expected_frequency: str = "30min",
    outlier_method: Literal["zscore", "iqr", "mad"] = "iqr",
    outlier_threshold: float = 3.0,
) -> QualityReport:
    """
    Perform comprehensive quality control on a time series.

    Args:
        series: pandas Series with datetime index
        expected_frequency: Expected data frequency (e.g., '30min', '1h', '1D')
        outlier_method: Method for outlier detection
        outlier_threshold: Threshold for outlier detection

    Returns:
        QualityReport with detailed quality metrics

    Example:
        from ukpyn.utils import quality_control

        report = quality_control(ts, expected_frequency='30min')

        print(f"Quality score: {report.quality_score:.1f}%")
        print(f"Missing: {report.missing_points}")
        print(f"Outliers: {report.outlier_points}")
        print(f"Gaps: {len(report.gaps)}")

        for issue in report.issues:
            print(f"  - {issue}")
    """
    pd = _get_pandas()

    if not isinstance(series, pd.Series):
        raise TypeError("series must be a pandas Series")

    issues: list[str] = []

    # Count totals
    total_points = len(series)
    missing_points = series.isna().sum()
    valid_series = series.dropna()
    valid_points = len(valid_series)

    # Detect outliers
    if valid_points > 0:
        outlier_flags = flag_outliers(valid_series, method=outlier_method, threshold=outlier_threshold)
        outlier_points = outlier_flags.sum()
    else:
        outlier_points = 0

    # Identify gaps
    gaps = identify_gaps(series, expected_frequency=expected_frequency)

    # Identify issues
    if missing_points > 0:
        missing_pct = 100 * missing_points / total_points
        issues.append(f"Missing data: {missing_points} points ({missing_pct:.1f}%)")

    if outlier_points > 0:
        outlier_pct = 100 * outlier_points / valid_points if valid_points > 0 else 0
        issues.append(f"Outliers detected: {outlier_points} points ({outlier_pct:.1f}%)")

    if gaps:
        total_gap_hours = sum(g.duration_hours for g in gaps)
        issues.append(f"Data gaps: {len(gaps)} gaps totaling {total_gap_hours:.1f} hours")

    # Calculate quality score (simple weighted average)
    if total_points > 0:
        completeness = 100 * valid_points / total_points
        outlier_penalty = min(50, 100 * outlier_points / max(1, valid_points))
        gap_penalty = min(30, len(gaps) * 5)
        quality_score = max(0, completeness - outlier_penalty - gap_penalty)
    else:
        quality_score = 0.0

    return QualityReport(
        total_points=total_points,
        valid_points=valid_points,
        missing_points=missing_points,
        outlier_points=outlier_points,
        gaps=gaps,
        quality_score=quality_score,
        issues=issues,
    )


# =============================================================================
# Gap Filling
# =============================================================================


def identify_gaps(
    series: Any,
    expected_frequency: str = "30min",
    min_gap_hours: float = 1.0,
) -> list[GapInfo]:
    """
    Identify gaps in a time series based on expected frequency.

    Args:
        series: pandas Series with datetime index
        expected_frequency: Expected data frequency (e.g., '30min', '1h', '1D')
        min_gap_hours: Minimum gap duration to report (hours)

    Returns:
        List of GapInfo objects describing detected gaps

    Example:
        from ukpyn.utils import identify_gaps

        gaps = identify_gaps(ts, expected_frequency='30min', min_gap_hours=2.0)

        for gap in gaps:
            print(f"Gap from {gap.start} to {gap.end}: {gap.duration_hours:.1f}h")
    """
    pd = _get_pandas()

    if not isinstance(series, pd.Series):
        raise TypeError("series must be a pandas Series")

    valid_series = series.dropna()
    if len(valid_series) < 2:
        return []

    # Calculate expected interval
    freq = pd.Timedelta(expected_frequency)
    min_gap = pd.Timedelta(hours=min_gap_hours)

    # Find gaps
    gaps: list[GapInfo] = []
    timestamps = valid_series.index.sort_values()

    for i in range(len(timestamps) - 1):
        time_diff = timestamps[i + 1] - timestamps[i]

        if time_diff > min_gap:
            duration_hours = time_diff.total_seconds() / 3600
            missing_points = int(time_diff / freq) - 1

            gaps.append(
                GapInfo(
                    start=timestamps[i].to_pydatetime(),
                    end=timestamps[i + 1].to_pydatetime(),
                    duration_hours=duration_hours,
                    missing_points=max(0, missing_points),
                )
            )

    return gaps


def fill_gaps(
    series: Any,
    method: Literal["linear", "forward", "backward", "mean", "median"] = "linear",
    max_gap_hours: float | None = None,
) -> Any:
    """
    Fill gaps in a time series using interpolation or imputation.

    Args:
        series: pandas Series with datetime index
        method: Gap filling method:
            - 'linear': Linear interpolation between valid points
            - 'forward': Forward fill (carry last valid value)
            - 'backward': Backward fill (carry next valid value)
            - 'mean': Fill with series mean
            - 'median': Fill with series median
        max_gap_hours: Maximum gap size to fill (None = fill all)

    Returns:
        pandas Series with gaps filled

    Example:
        from ukpyn.utils import fill_gaps, identify_gaps

        # Check gaps first
        gaps = identify_gaps(ts)
        print(f"Found {len(gaps)} gaps")

        # Fill small gaps with linear interpolation
        filled = fill_gaps(ts, method='linear', max_gap_hours=4.0)

        # Fill remaining with forward fill
        filled = fill_gaps(filled, method='forward')
    """
    pd = _get_pandas()

    if not isinstance(series, pd.Series):
        raise TypeError("series must be a pandas Series")

    result = series.copy()

    if max_gap_hours is not None:
        # Only fill gaps smaller than max_gap_hours
        max_gap = pd.Timedelta(hours=max_gap_hours)

        # Find gap boundaries
        is_null = result.isna()
        gap_groups = (is_null != is_null.shift()).cumsum()

        for group_id in gap_groups[is_null].unique():
            mask = gap_groups == group_id
            gap_idx = result.index[mask]

            if len(gap_idx) > 0:
                # get_loc can return int, slice, or boolean array
                # Handle all cases to get integer position
                loc_start = result.index.get_loc(gap_idx[0])
                loc_end = result.index.get_loc(gap_idx[-1])
                
                # Convert to integer position if needed
                if isinstance(loc_start, slice):
                    gap_start_idx = loc_start.start if loc_start.start is not None else 0
                else:
                    gap_start_idx = int(loc_start) if not isinstance(loc_start, int) else loc_start
                    
                if isinstance(loc_end, slice):
                    gap_end_idx = loc_end.stop - 1 if loc_end.stop is not None else len(result) - 1
                else:
                    gap_end_idx = int(loc_end) if not isinstance(loc_end, int) else loc_end

                # Get surrounding timestamps
                if gap_start_idx > 0 and gap_end_idx < len(result) - 1:
                    before = result.index[gap_start_idx - 1]
                    after = result.index[gap_end_idx + 1]
                    gap_duration = after - before

                    if gap_duration > max_gap:
                        continue  # Skip this gap

        # Apply method to fillable gaps
        result = _apply_fill_method(result, method)
    else:
        result = _apply_fill_method(result, method)

    return result


def _apply_fill_method(series: Any, method: str) -> Any:
    """Apply the specified fill method to a series."""
    if method == "linear":
        return series.interpolate(method="time")
    elif method == "forward":
        return series.ffill()
    elif method == "backward":
        return series.bfill()
    elif method == "mean":
        return series.fillna(series.mean())
    elif method == "median":
        return series.fillna(series.median())
    else:
        raise ValueError(
            f"Unknown method: {method}. Use 'linear', 'forward', 'backward', 'mean', or 'median'."
        )
