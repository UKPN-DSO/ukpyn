"""Statistical analysis utilities for UK Power Networks time series data.

Provides functions for:
- Comprehensive statistical summaries (describe_timeseries)
- Autocorrelation analysis (autocorrelation)
- Seasonal pattern extraction (seasonal_pattern)
- Peak analysis (peak_analysis)

These utilities are designed to work with powerflow time series data
from the UK Power Networks Open Data Portal.

Note:
    These functions require pandas. Install with: pip install ukpyn[notebooks]
"""

from dataclasses import dataclass, field
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
                "pandas is required for statistical utilities.\n"
                "Install with: pip install ukpyn[notebooks]\n"
                "Or: pip install pandas"
            ) from err
    return _pd


# =============================================================================
# Data Structures
# =============================================================================


@dataclass
class TimeseriesStats:
    """Comprehensive statistical summary of a time series."""

    # Basic statistics
    count: int
    """Number of valid (non-null) data points."""

    mean: float
    """Arithmetic mean of the series."""

    median: float
    """Median (50th percentile) of the series."""

    std: float
    """Standard deviation of the series."""

    min: float
    """Minimum value in the series."""

    max: float
    """Maximum value in the series."""

    # Distribution shape
    skewness: float
    """Skewness of the distribution (0 = symmetric)."""

    kurtosis: float
    """Kurtosis of the distribution (0 = normal, >0 = heavy tails)."""

    # Percentiles
    p10: float
    """10th percentile."""

    p25: float
    """25th percentile (Q1)."""

    p50: float
    """50th percentile (median)."""

    p75: float
    """75th percentile (Q3)."""

    p90: float
    """90th percentile."""

    p95: float
    """95th percentile."""

    p99: float
    """99th percentile."""

    # Seasonal hints
    seasonal_hints: dict[str, Any] = field(default_factory=dict)
    """Hints about seasonal patterns detected in the data."""


@dataclass
class SeasonalProfile:
    """Seasonal pattern profiles extracted from time series."""

    period: Literal["daily", "weekly", "annual"]
    """The seasonal period analyzed."""

    profile_mean: dict[Any, float]
    """Mean value for each period unit (hour, day, month)."""

    profile_std: dict[Any, float]
    """Standard deviation for each period unit."""

    profile_min: dict[Any, float]
    """Minimum value for each period unit."""

    profile_max: dict[Any, float]
    """Maximum value for each period unit."""

    dominant_period_unit: Any
    """The period unit with the highest mean value."""

    range_ratio: float
    """Ratio of max to min mean values (measure of seasonal variation)."""


@dataclass
class PeakAnalysis:
    """Analysis of peak characteristics in time series data."""

    # Peak values
    peak_value: float
    """Maximum value in the series."""

    peak_timestamp: Any
    """Timestamp of the peak value."""

    trough_value: float
    """Minimum value in the series."""

    trough_timestamp: Any
    """Timestamp of the trough value."""

    # Peak statistics
    peak_to_trough_ratio: float
    """Ratio of peak to trough values."""

    mean_to_peak_ratio: float
    """Ratio of mean to peak value (load factor proxy)."""

    # Threshold-based analysis
    threshold_percentile: float
    """Percentile used for threshold analysis (default 90th)."""

    threshold_value: float
    """Value at the threshold percentile."""

    time_above_threshold_pct: float
    """Percentage of time series is above threshold."""

    peak_hours: list[int]
    """Hours of day (0-23) when peaks most commonly occur."""

    peak_days: list[int]
    """Days of week (0=Mon, 6=Sun) when peaks most commonly occur."""

    peak_months: list[int]
    """Months (1-12) when peaks most commonly occur."""

    # Sustained peak analysis
    max_consecutive_above_threshold: int
    """Maximum consecutive periods above threshold."""


# =============================================================================
# Statistical Summary
# =============================================================================


def describe_timeseries(series: Any) -> TimeseriesStats:
    """
    Generate a comprehensive statistical summary of a time series.

    Computes basic statistics (mean, median, std, min, max), distribution
    shape metrics (skewness, kurtosis), and percentiles. Also provides
    hints about potential seasonal patterns in the data.

    Args:
        series: pandas Series with numeric data (datetime index optional)

    Returns:
        TimeseriesStats dataclass with comprehensive statistics

    Example:
        from ukpyn.utils.stats import describe_timeseries
        from ukpyn.utils import records_to_timeseries

        ts = records_to_timeseries(data.records, value_field='active_power_mw')
        stats = describe_timeseries(ts)

        print(f"Mean: {stats.mean:.2f}")
        print(f"Std: {stats.std:.2f}")
        print(f"Skewness: {stats.skewness:.2f}")
        print(f"P95: {stats.p95:.2f}")

        # Check seasonal hints
        if stats.seasonal_hints.get('has_daily_pattern'):
            print("Daily pattern detected")
    """
    pd = _get_pandas()

    if not isinstance(series, pd.Series):
        raise TypeError("series must be a pandas Series")

    # Handle empty or all-NaN series
    valid = series.dropna()
    if len(valid) == 0:
        return TimeseriesStats(
            count=0,
            mean=float("nan"),
            median=float("nan"),
            std=float("nan"),
            min=float("nan"),
            max=float("nan"),
            skewness=float("nan"),
            kurtosis=float("nan"),
            p10=float("nan"),
            p25=float("nan"),
            p50=float("nan"),
            p75=float("nan"),
            p90=float("nan"),
            p95=float("nan"),
            p99=float("nan"),
            seasonal_hints={},
        )

    # Treat non-finite values (inf/-inf) as invalid for statistics
    finite = valid[~valid.isin([float("inf"), float("-inf")])]
    excluded_non_finite_count = len(valid) - len(finite)

    if len(finite) == 0:
        return TimeseriesStats(
            count=0,
            mean=float("nan"),
            median=float("nan"),
            std=float("nan"),
            min=float("nan"),
            max=float("nan"),
            skewness=float("nan"),
            kurtosis=float("nan"),
            p10=float("nan"),
            p25=float("nan"),
            p50=float("nan"),
            p75=float("nan"),
            p90=float("nan"),
            p95=float("nan"),
            p99=float("nan"),
            seasonal_hints={
                "has_non_finite_values": excluded_non_finite_count > 0,
                "excluded_non_finite_count": excluded_non_finite_count,
            },
        )

    # Basic statistics
    count = len(finite)
    mean = finite.mean()
    median = finite.median()
    std = finite.std()
    min_val = finite.min()
    max_val = finite.max()

    # Distribution shape
    skewness = _calculate_skewness(finite)
    kurtosis = _calculate_kurtosis(finite)

    # Percentiles
    percentiles = finite.quantile([0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99])

    # Seasonal hints (only if datetime index)
    seasonal_hints = _detect_seasonal_hints(finite)
    if excluded_non_finite_count > 0:
        seasonal_hints["has_non_finite_values"] = True
        seasonal_hints["excluded_non_finite_count"] = excluded_non_finite_count

    return TimeseriesStats(
        count=count,
        mean=mean,
        median=median,
        std=std,
        min=min_val,
        max=max_val,
        skewness=skewness,
        kurtosis=kurtosis,
        p10=percentiles[0.10],
        p25=percentiles[0.25],
        p50=percentiles[0.50],
        p75=percentiles[0.75],
        p90=percentiles[0.90],
        p95=percentiles[0.95],
        p99=percentiles[0.99],
        seasonal_hints=seasonal_hints,
    )


def _calculate_skewness(series: Any) -> float:
    """Calculate skewness of a series."""
    n = len(series)
    if n < 3:
        return float("nan")

    mean = series.mean()
    std = series.std()

    if std == 0:
        return 0.0

    # Fisher-Pearson standardized moment coefficient
    m3 = ((series - mean) ** 3).mean()
    return m3 / (std**3)


def _calculate_kurtosis(series: Any) -> float:
    """Calculate excess kurtosis of a series (Fisher's definition)."""
    n = len(series)
    if n < 4:
        return float("nan")

    mean = series.mean()
    std = series.std()

    if std == 0:
        return 0.0

    # Excess kurtosis (normal = 0)
    m4 = ((series - mean) ** 4).mean()
    return (m4 / (std**4)) - 3


def _detect_seasonal_hints(series: Any) -> dict[str, Any]:
    """Detect hints about seasonal patterns in the data."""
    pd = _get_pandas()

    hints: dict[str, Any] = {}

    # Check if index is datetime
    if not isinstance(series.index, pd.DatetimeIndex):
        return hints

    # Need at least 48 points for daily pattern (assuming half-hourly data)
    if len(series) < 48:
        return hints

    # Check for daily pattern by comparing variance by hour
    try:
        hourly_means = series.groupby(series.index.hour).mean()
        if len(hourly_means) >= 12:
            hourly_cv = (
                hourly_means.std() / hourly_means.mean()
                if hourly_means.mean() != 0
                else 0
            )
            hints["has_daily_pattern"] = hourly_cv > 0.05
            hints["daily_variation_cv"] = hourly_cv
    except Exception:
        pass

    # Check for weekly pattern (need at least 2 weeks of data)
    if len(series) >= 336:  # 2 weeks of half-hourly data
        try:
            daily_means = series.groupby(series.index.dayofweek).mean()
            if len(daily_means) >= 5:
                weekly_cv = (
                    daily_means.std() / daily_means.mean()
                    if daily_means.mean() != 0
                    else 0
                )
                hints["has_weekly_pattern"] = weekly_cv > 0.03
                hints["weekly_variation_cv"] = weekly_cv
        except Exception:
            pass

    return hints


# =============================================================================
# Autocorrelation Analysis
# =============================================================================


def autocorrelation(series: Any, lags: int = 48) -> Any:
    """
    Calculate autocorrelation for given lags.

    Autocorrelation measures how correlated a time series is with lagged
    versions of itself. High autocorrelation at lag 48 (assuming half-hourly
    data) indicates a strong daily pattern.

    Args:
        series: pandas Series with numeric data
        lags: Maximum number of lags to calculate (default 48 for daily pattern
              with half-hourly data)

    Returns:
        pandas Series with autocorrelation values indexed by lag number

    Example:
        from ukpyn.utils.stats import autocorrelation

        acf = autocorrelation(ts, lags=96)

        print(f"Lag 1 autocorrelation: {acf[1]:.3f}")
        print(f"Lag 48 (daily) autocorrelation: {acf[48]:.3f}")

        # Plot autocorrelation
        acf.plot(kind='bar', title='Autocorrelation Function')
    """
    pd = _get_pandas()

    if not isinstance(series, pd.Series):
        raise TypeError("series must be a pandas Series")

    valid = series.dropna()

    # Handle edge cases
    if len(valid) == 0:
        return pd.Series(dtype=float)

    if len(valid) < lags + 1:
        lags = len(valid) - 1

    if lags < 1:
        return pd.Series([1.0], index=[0])

    # Calculate autocorrelations
    mean = valid.mean()
    var = valid.var()

    if var == 0:
        # Constant series has perfect autocorrelation
        return pd.Series([1.0] * (lags + 1), index=range(lags + 1))

    acf_values = []
    n = len(valid)
    values = valid.values

    for lag in range(lags + 1):
        if lag == 0:
            acf_values.append(1.0)
        else:
            # Calculate autocorrelation for this lag
            cov = sum(
                (values[i] - mean) * (values[i - lag] - mean) for i in range(lag, n)
            )
            cov /= n
            acf_values.append(cov / var)

    return pd.Series(acf_values, index=range(lags + 1), name="autocorrelation")


# =============================================================================
# Seasonal Pattern Analysis
# =============================================================================


def seasonal_pattern(
    series: Any,
    period: Literal["daily", "weekly", "annual"] = "daily",
) -> SeasonalProfile:
    """
    Extract seasonal patterns from a time series.

    Calculates average profiles by hour-of-day (daily), day-of-week (weekly),
    or month (annual). Useful for understanding typical load shapes and
    identifying when peaks typically occur.

    Args:
        series: pandas Series with datetime index
        period: Seasonal period to analyze:
            - 'daily': Average by hour of day (0-23)
            - 'weekly': Average by day of week (0=Monday, 6=Sunday)
            - 'annual': Average by month (1-12)

    Returns:
        SeasonalProfile dataclass with profile statistics

    Example:
        from ukpyn.utils.stats import seasonal_pattern

        # Get daily load shape
        daily = seasonal_pattern(ts, period='daily')

        print(f"Peak hour: {daily.dominant_period_unit}")
        print(f"Peak/trough ratio: {daily.range_ratio:.2f}")

        # Access the profile
        for hour, mean_load in daily.profile_mean.items():
            print(f"Hour {hour}: {mean_load:.2f} MW")

        # Get weekly pattern
        weekly = seasonal_pattern(ts, period='weekly')
        print(f"Peak day: {weekly.dominant_period_unit}")  # 0=Mon, 6=Sun
    """
    pd = _get_pandas()

    if not isinstance(series, pd.Series):
        raise TypeError("series must be a pandas Series")

    if not isinstance(series.index, pd.DatetimeIndex):
        raise TypeError("series must have a DatetimeIndex for seasonal analysis")

    valid = series.dropna()

    # Handle empty series
    if len(valid) == 0:
        return SeasonalProfile(
            period=period,
            profile_mean={},
            profile_std={},
            profile_min={},
            profile_max={},
            dominant_period_unit=None,
            range_ratio=float("nan"),
        )

    # Group by the appropriate period
    if period == "daily":
        grouper = valid.index.hour
    elif period == "weekly":
        grouper = valid.index.dayofweek
    elif period == "annual":
        grouper = valid.index.month
    else:
        raise ValueError(
            f"Unknown period: {period}. Use 'daily', 'weekly', or 'annual'."
        )

    # Calculate statistics by group
    grouped = valid.groupby(grouper)
    profile_mean = grouped.mean().to_dict()
    profile_std = grouped.std().to_dict()
    profile_min = grouped.min().to_dict()
    profile_max = grouped.max().to_dict()

    # Find dominant period unit (highest mean)
    if profile_mean:
        dominant_period_unit = max(profile_mean, key=profile_mean.get)
    else:
        dominant_period_unit = None

    # Calculate range ratio
    mean_values = list(profile_mean.values())
    if mean_values and min(mean_values) != 0:
        range_ratio = max(mean_values) / min(mean_values)
    elif mean_values:
        range_ratio = float("inf") if max(mean_values) != 0 else 1.0
    else:
        range_ratio = float("nan")

    return SeasonalProfile(
        period=period,
        profile_mean=profile_mean,
        profile_std=profile_std,
        profile_min=profile_min,
        profile_max=profile_max,
        dominant_period_unit=dominant_period_unit,
        range_ratio=range_ratio,
    )


# =============================================================================
# Peak Analysis
# =============================================================================


def peak_analysis(
    series: Any,
    threshold_percentile: float = 90.0,
) -> PeakAnalysis:
    """
    Analyze peak characteristics of a time series.

    Identifies peak values, peak timing patterns, and sustained peak periods.
    Useful for understanding demand patterns and planning for peak capacity.

    Args:
        series: pandas Series with datetime index
        threshold_percentile: Percentile to use as threshold for "high" values
                             (default 90 = 90th percentile)

    Returns:
        PeakAnalysis dataclass with comprehensive peak statistics

    Example:
        from ukpyn.utils.stats import peak_analysis

        peaks = peak_analysis(ts, threshold_percentile=95)

        print(f"Peak value: {peaks.peak_value:.2f} MW at {peaks.peak_timestamp}")
        print(f"Load factor: {peaks.mean_to_peak_ratio:.2%}")
        print(f"Time above P95: {peaks.time_above_threshold_pct:.1f}%")

        # When do peaks occur?
        print(f"Peak hours: {peaks.peak_hours}")
        print(f"Peak days: {peaks.peak_days}")
        print(f"Max consecutive periods above threshold: {peaks.max_consecutive_above_threshold}")
    """
    pd = _get_pandas()

    if not isinstance(series, pd.Series):
        raise TypeError("series must be a pandas Series")

    valid = series.dropna()

    # Handle empty series
    if len(valid) == 0:
        return PeakAnalysis(
            peak_value=float("nan"),
            peak_timestamp=None,
            trough_value=float("nan"),
            trough_timestamp=None,
            peak_to_trough_ratio=float("nan"),
            mean_to_peak_ratio=float("nan"),
            threshold_percentile=threshold_percentile,
            threshold_value=float("nan"),
            time_above_threshold_pct=0.0,
            peak_hours=[],
            peak_days=[],
            peak_months=[],
            max_consecutive_above_threshold=0,
        )

    # Basic peak/trough identification
    peak_value = valid.max()
    peak_timestamp = valid.idxmax()
    trough_value = valid.min()
    trough_timestamp = valid.idxmin()

    # Ratios
    if trough_value != 0:
        peak_to_trough_ratio = peak_value / trough_value
    else:
        peak_to_trough_ratio = float("inf") if peak_value != 0 else 1.0

    mean_value = valid.mean()
    if peak_value != 0:
        mean_to_peak_ratio = mean_value / peak_value
    else:
        mean_to_peak_ratio = 1.0 if mean_value == 0 else float("inf")

    # Threshold analysis
    threshold_value = valid.quantile(threshold_percentile / 100.0)
    above_threshold = valid > threshold_value
    time_above_threshold_pct = 100.0 * above_threshold.sum() / len(valid)

    # Peak timing patterns (requires datetime index)
    peak_hours: list[int] = []
    peak_days: list[int] = []
    peak_months: list[int] = []

    if isinstance(valid.index, pd.DatetimeIndex) and above_threshold.any():
        peak_times = valid[above_threshold]

        # Most common peak hours (top 3)
        hour_counts = peak_times.groupby(peak_times.index.hour).size()
        if len(hour_counts) > 0:
            peak_hours = hour_counts.nlargest(min(3, len(hour_counts))).index.tolist()

        # Most common peak days
        day_counts = peak_times.groupby(peak_times.index.dayofweek).size()
        if len(day_counts) > 0:
            peak_days = day_counts.nlargest(min(3, len(day_counts))).index.tolist()

        # Most common peak months
        month_counts = peak_times.groupby(peak_times.index.month).size()
        if len(month_counts) > 0:
            peak_months = month_counts.nlargest(
                min(3, len(month_counts))
            ).index.tolist()

    # Calculate maximum consecutive periods above threshold
    max_consecutive = _max_consecutive_true(above_threshold)

    return PeakAnalysis(
        peak_value=peak_value,
        peak_timestamp=peak_timestamp,
        trough_value=trough_value,
        trough_timestamp=trough_timestamp,
        peak_to_trough_ratio=peak_to_trough_ratio,
        mean_to_peak_ratio=mean_to_peak_ratio,
        threshold_percentile=threshold_percentile,
        threshold_value=threshold_value,
        time_above_threshold_pct=time_above_threshold_pct,
        peak_hours=peak_hours,
        peak_days=peak_days,
        peak_months=peak_months,
        max_consecutive_above_threshold=max_consecutive,
    )


def _max_consecutive_true(bool_series: Any) -> int:
    """Calculate the maximum number of consecutive True values in a boolean series."""
    if len(bool_series) == 0 or not bool_series.any():
        return 0

    # Convert to numeric and use groupby to find consecutive runs
    values = bool_series.astype(int).values
    max_count = 0
    current_count = 0

    for val in values:
        if val == 1:
            current_count += 1
            max_count = max(max_count, current_count)
        else:
            current_count = 0

    return max_count
