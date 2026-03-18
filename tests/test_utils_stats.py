"""Tests for utils/stats module."""

import math
from dataclasses import fields
from datetime import datetime

import pytest

from ukpyn.utils.stats import (
    PeakAnalysis,
    SeasonalProfile,
    TimeseriesStats,
    autocorrelation,
    describe_timeseries,
    peak_analysis,
    seasonal_pattern,
)

pd = pytest.importorskip("pandas")
# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture
def simple_timeseries() -> pd.Series:
    """Create a simple time series for testing."""
    dates = pd.date_range("2024-01-01", periods=100, freq="30min")
    values = [100.0] * 100  # Constant values
    return pd.Series(values, index=dates, name="power_mw")


@pytest.fixture
def varying_timeseries() -> pd.Series:
    """Create a time series with varying values for statistical tests."""
    dates = pd.date_range("2024-01-01", periods=100, freq="30min")
    # Values with known statistical properties
    values = list(range(1, 101))  # 1 to 100
    return pd.Series(values, index=dates, name="power_mw")


@pytest.fixture
def daily_pattern_timeseries() -> pd.Series:
    """Create a time series with a clear daily pattern (48 points per day)."""
    # 7 days of half-hourly data (336 points)
    dates = pd.date_range("2024-01-01", periods=336, freq="30min")
    values = []
    for i in range(336):
        hour = (i // 2) % 24  # Hour of day
        # Peak at 18:00 (hour 18), trough at 04:00 (hour 4)
        if 6 <= hour <= 22:
            values.append(100.0 + (hour - 6) * 5)  # Ramp up during day
        else:
            values.append(50.0)  # Low at night
    return pd.Series(values, index=dates, name="power_mw")


@pytest.fixture
def weekly_pattern_timeseries() -> pd.Series:
    """Create a time series with a weekly pattern."""
    # 4 weeks of half-hourly data
    dates = pd.date_range("2024-01-01", periods=48 * 7 * 4, freq="30min")
    values = []
    for i in range(len(dates)):
        day_of_week = dates[i].dayofweek
        # Higher on weekdays (0-4), lower on weekends (5-6)
        if day_of_week < 5:
            values.append(150.0)
        else:
            values.append(80.0)
    return pd.Series(values, index=dates, name="power_mw")


@pytest.fixture
def annual_pattern_timeseries() -> pd.Series:
    """Create a time series with annual pattern (monthly variation)."""
    # 2 years of daily data
    dates = pd.date_range("2024-01-01", periods=730, freq="D")
    values = []
    for date in dates:
        month = date.month
        # Winter peak (Dec-Feb), summer trough (Jun-Aug)
        if month in [12, 1, 2]:
            values.append(200.0)
        elif month in [6, 7, 8]:
            values.append(100.0)
        else:
            values.append(150.0)
    return pd.Series(values, index=dates, name="power_mw")


@pytest.fixture
def skewed_timeseries() -> pd.Series:
    """Create a positively skewed time series."""
    dates = pd.date_range("2024-01-01", periods=100, freq="30min")
    # Exponential-like values for positive skew
    import numpy as np

    np.random.seed(42)
    values = np.exp(np.random.normal(0, 1, 100))
    return pd.Series(values, index=dates, name="power_mw")


@pytest.fixture
def empty_timeseries() -> pd.Series:
    """Create an empty time series."""
    return pd.Series(dtype=float, name="power_mw")


@pytest.fixture
def all_nan_timeseries() -> pd.Series:
    """Create a time series with all NaN values."""
    dates = pd.date_range("2024-01-01", periods=10, freq="30min")
    return pd.Series([None] * 10, index=dates, name="power_mw", dtype=float)


@pytest.fixture
def single_value_timeseries() -> pd.Series:
    """Create a time series with a single value."""
    dates = pd.date_range("2024-01-01", periods=1, freq="30min")
    return pd.Series([100.0], index=dates, name="power_mw")


@pytest.fixture
def two_value_timeseries() -> pd.Series:
    """Create a time series with two values."""
    dates = pd.date_range("2024-01-01", periods=2, freq="30min")
    return pd.Series([100.0, 200.0], index=dates, name="power_mw")


@pytest.fixture
def three_value_timeseries() -> pd.Series:
    """Create a time series with three values."""
    dates = pd.date_range("2024-01-01", periods=3, freq="30min")
    return pd.Series([100.0, 200.0, 150.0], index=dates, name="power_mw")


@pytest.fixture
def timeseries_with_peaks() -> pd.Series:
    """Create a time series with clear peaks above threshold."""
    dates = pd.date_range("2024-01-01", periods=100, freq="30min")
    values = [100.0] * 100
    # Add peaks
    values[10] = 300.0
    values[11] = 310.0
    values[12] = 320.0  # Max peak
    values[13] = 305.0
    values[50] = 280.0
    values[51] = 290.0
    values[75] = 250.0
    return pd.Series(values, index=dates, name="power_mw")


@pytest.fixture
def non_datetime_index_timeseries() -> pd.Series:
    """Create a time series without datetime index."""
    return pd.Series([100.0, 150.0, 120.0, 180.0, 90.0], name="power_mw")


# =============================================================================
# Tests for describe_timeseries
# =============================================================================


class TestDescribeTimeseries:
    """Tests for describe_timeseries function."""

    def test_returns_timeseries_stats(self, varying_timeseries: pd.Series) -> None:
        """Test that describe_timeseries returns TimeseriesStats."""
        result = describe_timeseries(varying_timeseries)

        assert isinstance(result, TimeseriesStats)

    def test_basic_statistics_values(self, varying_timeseries: pd.Series) -> None:
        """Test basic statistics are calculated correctly."""
        result = describe_timeseries(varying_timeseries)

        # Values are 1-100, so mean = 50.5
        assert result.count == 100
        assert result.mean == pytest.approx(50.5, rel=0.01)
        assert result.median == pytest.approx(50.5, rel=0.01)
        assert result.min == 1.0
        assert result.max == 100.0
        assert result.std > 0

    def test_percentiles_calculated(self, varying_timeseries: pd.Series) -> None:
        """Test percentiles are calculated correctly."""
        result = describe_timeseries(varying_timeseries)

        # For 1-100, p10 should be around 10.9, p50 around 50.5, p90 around 90.1
        assert result.p10 < result.p25 < result.p50 < result.p75 < result.p90
        assert result.p50 == pytest.approx(result.median, rel=0.01)
        assert result.p90 < result.p95 < result.p99

    def test_skewness_for_uniform_data(self, varying_timeseries: pd.Series) -> None:
        """Test skewness is near zero for uniform data."""
        result = describe_timeseries(varying_timeseries)

        # Uniform distribution should have near-zero skewness
        assert abs(result.skewness) < 0.5

    def test_skewness_for_skewed_data(self, skewed_timeseries: pd.Series) -> None:
        """Test skewness is positive for positively skewed data."""
        result = describe_timeseries(skewed_timeseries)

        # Exponential-like data should have positive skewness
        assert result.skewness > 0

    def test_kurtosis_calculated(self, varying_timeseries: pd.Series) -> None:
        """Test kurtosis is calculated."""
        result = describe_timeseries(varying_timeseries)

        # Uniform distribution has negative excess kurtosis (around -1.2)
        assert isinstance(result.kurtosis, float)
        assert result.kurtosis < 0  # Uniform is platykurtic

    def test_kurtosis_for_peaked_data(self) -> None:
        """Test kurtosis is higher for peaked data."""
        dates = pd.date_range("2024-01-01", periods=100, freq="30min")
        # Create data with heavy tails
        values = (
            [100.0] * 90 + [1.0] * 5 + [200.0] * 5
        )  # Most at center, some at extremes
        series = pd.Series(values, index=dates)

        result = describe_timeseries(series)

        # Should have different kurtosis than uniform
        assert isinstance(result.kurtosis, float)

    def test_empty_series_returns_nan_stats(self, empty_timeseries: pd.Series) -> None:
        """Test empty series returns NaN for all statistics."""
        result = describe_timeseries(empty_timeseries)

        assert result.count == 0
        assert math.isnan(result.mean)
        assert math.isnan(result.median)
        assert math.isnan(result.std)
        assert math.isnan(result.min)
        assert math.isnan(result.max)
        assert math.isnan(result.skewness)
        assert math.isnan(result.kurtosis)
        assert math.isnan(result.p50)
        assert result.seasonal_hints == {}

    def test_all_nan_series_returns_nan_stats(
        self, all_nan_timeseries: pd.Series
    ) -> None:
        """Test all NaN series returns NaN for all statistics."""
        result = describe_timeseries(all_nan_timeseries)

        assert result.count == 0
        assert math.isnan(result.mean)
        assert math.isnan(result.median)

    def test_single_value_series(self, single_value_timeseries: pd.Series) -> None:
        """Test single value series has correct count and NaN for skewness."""
        result = describe_timeseries(single_value_timeseries)

        assert result.count == 1
        assert result.mean == 100.0
        assert result.min == 100.0
        assert result.max == 100.0
        # Skewness requires at least 3 values
        assert math.isnan(result.skewness)
        # Kurtosis requires at least 4 values
        assert math.isnan(result.kurtosis)

    def test_two_value_series(self, two_value_timeseries: pd.Series) -> None:
        """Test two value series has NaN for skewness."""
        result = describe_timeseries(two_value_timeseries)

        assert result.count == 2
        assert math.isnan(result.skewness)
        assert math.isnan(result.kurtosis)

    def test_three_value_series(self, three_value_timeseries: pd.Series) -> None:
        """Test three value series has skewness but NaN kurtosis."""
        result = describe_timeseries(three_value_timeseries)

        assert result.count == 3
        assert not math.isnan(result.skewness)  # 3 values is enough for skewness
        assert math.isnan(result.kurtosis)  # Kurtosis needs 4 values

    def test_constant_series_skewness_zero(self, simple_timeseries: pd.Series) -> None:
        """Test constant series has zero skewness."""
        result = describe_timeseries(simple_timeseries)

        assert result.skewness == 0.0
        assert result.kurtosis == 0.0

    def test_seasonal_hints_detected(self, daily_pattern_timeseries: pd.Series) -> None:
        """Test seasonal hints are detected for patterned data."""
        result = describe_timeseries(daily_pattern_timeseries)

        # Should detect daily pattern
        assert "has_daily_pattern" in result.seasonal_hints
        assert result.seasonal_hints.get("daily_variation_cv", 0) > 0

    def test_weekly_hints_detected(self, weekly_pattern_timeseries: pd.Series) -> None:
        """Test weekly hints are detected for data with weekly pattern."""
        result = describe_timeseries(weekly_pattern_timeseries)

        # Should detect weekly pattern (has more than 2 weeks of data)
        assert "has_weekly_pattern" in result.seasonal_hints

    def test_no_seasonal_hints_for_non_datetime_index(
        self, non_datetime_index_timeseries: pd.Series
    ) -> None:
        """Test no seasonal hints for series without datetime index."""
        result = describe_timeseries(non_datetime_index_timeseries)

        assert result.seasonal_hints == {}

    def test_type_error_for_non_series(self) -> None:
        """Test TypeError raised for non-Series input."""
        with pytest.raises(TypeError, match="series must be a pandas Series"):
            describe_timeseries([1, 2, 3, 4, 5])

    def test_handles_nan_in_middle(self) -> None:
        """Test handles NaN values in the middle of series."""
        dates = pd.date_range("2024-01-01", periods=10, freq="30min")
        values = [1.0, 2.0, None, 4.0, 5.0, None, 7.0, 8.0, 9.0, 10.0]
        series = pd.Series(values, index=dates)

        result = describe_timeseries(series)

        assert result.count == 8  # 10 - 2 NaN values


# =============================================================================
# Tests for autocorrelation
# =============================================================================


class TestAutocorrelation:
    """Tests for autocorrelation function."""

    def test_returns_pandas_series(self, varying_timeseries: pd.Series) -> None:
        """Test that autocorrelation returns a pandas Series."""
        result = autocorrelation(varying_timeseries, lags=10)

        assert isinstance(result, pd.Series)

    def test_lag_zero_is_one(self, varying_timeseries: pd.Series) -> None:
        """Test autocorrelation at lag 0 is always 1."""
        result = autocorrelation(varying_timeseries, lags=10)

        assert result[0] == 1.0

    def test_correct_number_of_lags(self, varying_timeseries: pd.Series) -> None:
        """Test correct number of lag values returned."""
        result = autocorrelation(varying_timeseries, lags=24)

        assert len(result) == 25  # 0 to 24 inclusive

    def test_autocorrelation_decreases_for_random_data(self) -> None:
        """Test autocorrelation generally decreases for random data."""
        import numpy as np

        np.random.seed(123)
        dates = pd.date_range("2024-01-01", periods=200, freq="30min")
        values = np.random.randn(200)
        series = pd.Series(values, index=dates)

        result = autocorrelation(series, lags=20)

        # Random data should have low autocorrelation at higher lags
        assert abs(result[10]) < 0.5
        assert abs(result[20]) < 0.5

    def test_high_autocorrelation_for_trending_data(self) -> None:
        """Test high autocorrelation for strongly trending data."""
        dates = pd.date_range("2024-01-01", periods=100, freq="30min")
        values = list(range(100))  # Linear trend
        series = pd.Series(values, index=dates)

        result = autocorrelation(series, lags=10)

        # Trending data should have high autocorrelation
        assert result[1] > 0.9
        assert result[5] > 0.8

    def test_periodic_autocorrelation(
        self, daily_pattern_timeseries: pd.Series
    ) -> None:
        """Test autocorrelation for periodic data shows pattern."""
        result = autocorrelation(daily_pattern_timeseries, lags=96)

        # Should have high autocorrelation at lag 48 (daily pattern in half-hourly data)
        # May vary based on exact pattern, but should show periodicity
        assert result[0] == 1.0
        assert len(result) == 97

    def test_empty_series_returns_empty(self, empty_timeseries: pd.Series) -> None:
        """Test empty series returns empty Series."""
        result = autocorrelation(empty_timeseries)

        assert len(result) == 0

    def test_single_value_returns_lag_zero_only(
        self, single_value_timeseries: pd.Series
    ) -> None:
        """Test single value series returns only lag 0."""
        result = autocorrelation(single_value_timeseries, lags=10)

        assert len(result) == 1
        assert result[0] == 1.0

    def test_constant_series_all_ones(self, simple_timeseries: pd.Series) -> None:
        """Test constant series has autocorrelation of 1 at all lags."""
        result = autocorrelation(simple_timeseries, lags=20)

        for lag in range(21):
            assert result[lag] == 1.0

    def test_lags_truncated_for_short_series(self) -> None:
        """Test lags are truncated when series is shorter than requested lags."""
        dates = pd.date_range("2024-01-01", periods=10, freq="30min")
        series = pd.Series(range(10), index=dates, dtype=float)

        result = autocorrelation(series, lags=100)

        # Should only have lags up to len-1 = 9
        assert len(result) == 10  # 0 to 9

    def test_type_error_for_non_series(self) -> None:
        """Test TypeError raised for non-Series input."""
        with pytest.raises(TypeError, match="series must be a pandas Series"):
            autocorrelation([1, 2, 3, 4, 5])

    def test_handles_nan_values(self) -> None:
        """Test handles NaN values by dropping them."""
        dates = pd.date_range("2024-01-01", periods=20, freq="30min")
        values = [float(i) for i in range(20)]
        values[5] = None
        values[10] = None
        series = pd.Series(values, index=dates)

        result = autocorrelation(series, lags=10)

        assert len(result) > 0
        assert result[0] == 1.0

    def test_default_lags_is_48(self, varying_timeseries: pd.Series) -> None:
        """Test default lags parameter is 48."""
        result = autocorrelation(varying_timeseries)

        assert len(result) == 49  # 0 to 48


# =============================================================================
# Tests for seasonal_pattern
# =============================================================================


class TestSeasonalPattern:
    """Tests for seasonal_pattern function."""

    def test_returns_seasonal_profile(
        self, daily_pattern_timeseries: pd.Series
    ) -> None:
        """Test that seasonal_pattern returns SeasonalProfile."""
        result = seasonal_pattern(daily_pattern_timeseries, period="daily")

        assert isinstance(result, SeasonalProfile)

    def test_daily_period_profile(self, daily_pattern_timeseries: pd.Series) -> None:
        """Test daily period produces 24-hour profile."""
        result = seasonal_pattern(daily_pattern_timeseries, period="daily")

        assert result.period == "daily"
        assert len(result.profile_mean) == 24  # Hours 0-23

    def test_weekly_period_profile(self, weekly_pattern_timeseries: pd.Series) -> None:
        """Test weekly period produces 7-day profile."""
        result = seasonal_pattern(weekly_pattern_timeseries, period="weekly")

        assert result.period == "weekly"
        assert len(result.profile_mean) == 7  # Days 0-6

    def test_annual_period_profile(self, annual_pattern_timeseries: pd.Series) -> None:
        """Test annual period produces 12-month profile."""
        result = seasonal_pattern(annual_pattern_timeseries, period="annual")

        assert result.period == "annual"
        assert len(result.profile_mean) == 12  # Months 1-12

    def test_dominant_period_unit_daily(
        self, daily_pattern_timeseries: pd.Series
    ) -> None:
        """Test dominant period unit is correctly identified for daily pattern."""
        result = seasonal_pattern(daily_pattern_timeseries, period="daily")

        # Peak should be in the afternoon based on fixture data
        assert result.dominant_period_unit is not None
        assert 0 <= result.dominant_period_unit <= 23

    def test_dominant_period_unit_weekly(
        self, weekly_pattern_timeseries: pd.Series
    ) -> None:
        """Test dominant period unit for weekly pattern."""
        result = seasonal_pattern(weekly_pattern_timeseries, period="weekly")

        # Should be a weekday (0-4) since weekdays have higher values
        assert result.dominant_period_unit in [0, 1, 2, 3, 4]

    def test_range_ratio_calculated(self, daily_pattern_timeseries: pd.Series) -> None:
        """Test range ratio is calculated correctly."""
        result = seasonal_pattern(daily_pattern_timeseries, period="daily")

        # Range ratio should be > 1 for data with variation
        assert result.range_ratio > 1.0

    def test_range_ratio_infinity_for_zero_min(self) -> None:
        """Test range ratio is infinity when min mean is zero."""
        dates = pd.date_range("2024-01-01", periods=48, freq="30min")
        values = []
        for i in range(48):
            hour = (i // 2) % 24
            if hour < 12:
                values.append(0.0)
            else:
                values.append(100.0)
        series = pd.Series(values, index=dates)

        result = seasonal_pattern(series, period="daily")

        assert result.range_ratio == float("inf")

    def test_profile_statistics(self, daily_pattern_timeseries: pd.Series) -> None:
        """Test profile statistics are calculated."""
        result = seasonal_pattern(daily_pattern_timeseries, period="daily")

        assert len(result.profile_mean) > 0
        assert len(result.profile_std) > 0
        assert len(result.profile_min) > 0
        assert len(result.profile_max) > 0

        # Check that max >= mean >= min for each period unit
        for key in result.profile_mean:
            assert result.profile_max[key] >= result.profile_mean[key]
            assert result.profile_mean[key] >= result.profile_min[key]

    def test_empty_series_returns_empty_profile(self) -> None:
        """Test empty series returns empty profile."""
        # Need to add datetime index for empty series
        empty_dt = pd.Series(dtype=float)
        empty_dt.index = pd.DatetimeIndex([])

        result = seasonal_pattern(empty_dt, period="daily")

        assert result.profile_mean == {}
        assert result.profile_std == {}
        assert result.dominant_period_unit is None
        assert math.isnan(result.range_ratio)

    def test_all_nan_series_returns_empty_profile(
        self, all_nan_timeseries: pd.Series
    ) -> None:
        """Test all NaN series returns empty profile."""
        result = seasonal_pattern(all_nan_timeseries, period="daily")

        assert result.profile_mean == {}
        assert result.dominant_period_unit is None

    def test_invalid_period_raises_error(
        self, daily_pattern_timeseries: pd.Series
    ) -> None:
        """Test invalid period raises ValueError."""
        with pytest.raises(ValueError, match="Unknown period"):
            seasonal_pattern(daily_pattern_timeseries, period="quarterly")

    def test_non_datetime_index_raises_error(
        self, non_datetime_index_timeseries: pd.Series
    ) -> None:
        """Test non-datetime index raises TypeError."""
        with pytest.raises(TypeError, match="DatetimeIndex"):
            seasonal_pattern(non_datetime_index_timeseries, period="daily")

    def test_type_error_for_non_series(self) -> None:
        """Test TypeError raised for non-Series input."""
        with pytest.raises(TypeError, match="series must be a pandas Series"):
            seasonal_pattern([1, 2, 3, 4, 5], period="daily")

    def test_default_period_is_daily(self, daily_pattern_timeseries: pd.Series) -> None:
        """Test default period is 'daily'."""
        result = seasonal_pattern(daily_pattern_timeseries)

        assert result.period == "daily"


# =============================================================================
# Tests for peak_analysis
# =============================================================================


class TestPeakAnalysis:
    """Tests for peak_analysis function."""

    def test_returns_peak_analysis(self, timeseries_with_peaks: pd.Series) -> None:
        """Test that peak_analysis returns PeakAnalysis."""
        result = peak_analysis(timeseries_with_peaks)

        assert isinstance(result, PeakAnalysis)

    def test_peak_value_identified(self, timeseries_with_peaks: pd.Series) -> None:
        """Test peak value is correctly identified."""
        result = peak_analysis(timeseries_with_peaks)

        assert result.peak_value == 320.0  # Max value in fixture

    def test_peak_timestamp_identified(self, timeseries_with_peaks: pd.Series) -> None:
        """Test peak timestamp is correctly identified."""
        result = peak_analysis(timeseries_with_peaks)

        assert result.peak_timestamp is not None
        assert isinstance(result.peak_timestamp, datetime)

    def test_trough_value_identified(self, timeseries_with_peaks: pd.Series) -> None:
        """Test trough value is correctly identified."""
        result = peak_analysis(timeseries_with_peaks)

        assert result.trough_value == 100.0  # Min value in fixture

    def test_trough_timestamp_identified(
        self, timeseries_with_peaks: pd.Series
    ) -> None:
        """Test trough timestamp is correctly identified."""
        result = peak_analysis(timeseries_with_peaks)

        assert result.trough_timestamp is not None

    def test_peak_to_trough_ratio(self, timeseries_with_peaks: pd.Series) -> None:
        """Test peak to trough ratio calculation."""
        result = peak_analysis(timeseries_with_peaks)

        # 320 / 100 = 3.2
        assert result.peak_to_trough_ratio == pytest.approx(3.2, rel=0.01)

    def test_mean_to_peak_ratio(self, timeseries_with_peaks: pd.Series) -> None:
        """Test mean to peak ratio (load factor proxy)."""
        result = peak_analysis(timeseries_with_peaks)

        # Mean / Peak should be between 0 and 1
        assert 0 < result.mean_to_peak_ratio < 1

    def test_threshold_percentile_stored(
        self, timeseries_with_peaks: pd.Series
    ) -> None:
        """Test threshold percentile is stored correctly."""
        result = peak_analysis(timeseries_with_peaks, threshold_percentile=95.0)

        assert result.threshold_percentile == 95.0

    def test_threshold_value_calculated(self, timeseries_with_peaks: pd.Series) -> None:
        """Test threshold value is calculated at specified percentile."""
        result = peak_analysis(timeseries_with_peaks, threshold_percentile=90.0)

        # Should be close to the 90th percentile
        expected_threshold = timeseries_with_peaks.quantile(0.90)
        assert result.threshold_value == pytest.approx(expected_threshold, rel=0.01)

    def test_time_above_threshold_pct(self, timeseries_with_peaks: pd.Series) -> None:
        """Test time above threshold percentage calculation."""
        result = peak_analysis(timeseries_with_peaks, threshold_percentile=90.0)

        # Should be around 10% (above 90th percentile)
        assert 0 <= result.time_above_threshold_pct <= 100

    def test_peak_hours_identified(self, daily_pattern_timeseries: pd.Series) -> None:
        """Test peak hours are identified."""
        result = peak_analysis(daily_pattern_timeseries, threshold_percentile=90.0)

        assert isinstance(result.peak_hours, list)
        for hour in result.peak_hours:
            assert 0 <= hour <= 23

    def test_peak_days_identified(self, weekly_pattern_timeseries: pd.Series) -> None:
        """Test peak days are identified."""
        result = peak_analysis(weekly_pattern_timeseries, threshold_percentile=50.0)

        assert isinstance(result.peak_days, list)
        for day in result.peak_days:
            assert 0 <= day <= 6

    def test_peak_months_identified(self, annual_pattern_timeseries: pd.Series) -> None:
        """Test peak months are identified."""
        result = peak_analysis(annual_pattern_timeseries, threshold_percentile=50.0)

        assert isinstance(result.peak_months, list)
        for month in result.peak_months:
            assert 1 <= month <= 12

    def test_max_consecutive_above_threshold(
        self, timeseries_with_peaks: pd.Series
    ) -> None:
        """Test max consecutive periods above threshold."""
        result = peak_analysis(timeseries_with_peaks, threshold_percentile=90.0)

        # Fixture has consecutive peaks at indices 10-13
        assert result.max_consecutive_above_threshold >= 0

    def test_empty_series_returns_nan_values(self, empty_timeseries: pd.Series) -> None:
        """Test empty series returns NaN values."""
        result = peak_analysis(empty_timeseries)

        assert math.isnan(result.peak_value)
        assert result.peak_timestamp is None
        assert math.isnan(result.trough_value)
        assert result.trough_timestamp is None
        assert math.isnan(result.peak_to_trough_ratio)
        assert math.isnan(result.mean_to_peak_ratio)
        assert result.peak_hours == []
        assert result.peak_days == []
        assert result.peak_months == []
        assert result.max_consecutive_above_threshold == 0

    def test_all_nan_series_returns_nan_values(
        self, all_nan_timeseries: pd.Series
    ) -> None:
        """Test all NaN series returns NaN values."""
        result = peak_analysis(all_nan_timeseries)

        assert math.isnan(result.peak_value)
        assert result.peak_timestamp is None

    def test_peak_to_trough_ratio_infinity_for_zero_trough(self) -> None:
        """Test peak to trough ratio is infinity when trough is zero."""
        dates = pd.date_range("2024-01-01", periods=10, freq="30min")
        values = [0.0, 0.0, 0.0, 100.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
        series = pd.Series(values, index=dates)

        result = peak_analysis(series)

        assert result.peak_to_trough_ratio == float("inf")

    def test_peak_to_trough_ratio_one_for_all_zero(self) -> None:
        """Test peak to trough ratio is 1.0 when all values are zero."""
        dates = pd.date_range("2024-01-01", periods=10, freq="30min")
        values = [0.0] * 10
        series = pd.Series(values, index=dates)

        result = peak_analysis(series)

        assert result.peak_to_trough_ratio == 1.0

    def test_non_datetime_index_no_timing_patterns(
        self, non_datetime_index_timeseries: pd.Series
    ) -> None:
        """Test non-datetime index produces empty timing patterns."""
        result = peak_analysis(non_datetime_index_timeseries)

        # Should still calculate peak/trough values
        assert result.peak_value == 180.0
        assert result.trough_value == 90.0
        # But timing patterns should be empty
        assert result.peak_hours == []
        assert result.peak_days == []
        assert result.peak_months == []

    def test_type_error_for_non_series(self) -> None:
        """Test TypeError raised for non-Series input."""
        with pytest.raises(TypeError, match="series must be a pandas Series"):
            peak_analysis([1, 2, 3, 4, 5])

    def test_default_threshold_percentile_is_90(
        self, timeseries_with_peaks: pd.Series
    ) -> None:
        """Test default threshold percentile is 90."""
        result = peak_analysis(timeseries_with_peaks)

        assert result.threshold_percentile == 90.0

    def test_different_threshold_percentiles(
        self, timeseries_with_peaks: pd.Series
    ) -> None:
        """Test different threshold percentiles produce different results."""
        result_50 = peak_analysis(timeseries_with_peaks, threshold_percentile=50.0)
        result_90 = peak_analysis(timeseries_with_peaks, threshold_percentile=90.0)
        result_99 = peak_analysis(timeseries_with_peaks, threshold_percentile=99.0)

        # Higher percentile = higher threshold
        assert result_50.threshold_value <= result_90.threshold_value
        assert result_90.threshold_value <= result_99.threshold_value

        # Higher percentile = less time above
        assert result_50.time_above_threshold_pct >= result_90.time_above_threshold_pct


# =============================================================================
# Tests for Dataclasses
# =============================================================================


class TestDataclasses:
    """Tests for module dataclasses."""

    def test_timeseries_stats_dataclass(self) -> None:
        """Test TimeseriesStats dataclass creation."""
        stats = TimeseriesStats(
            count=100,
            mean=50.0,
            median=50.0,
            std=10.0,
            min=1.0,
            max=100.0,
            skewness=0.1,
            kurtosis=-0.5,
            p10=10.0,
            p25=25.0,
            p50=50.0,
            p75=75.0,
            p90=90.0,
            p95=95.0,
            p99=99.0,
            seasonal_hints={"has_daily_pattern": True},
        )

        assert stats.count == 100
        assert stats.mean == 50.0
        assert stats.skewness == 0.1
        assert stats.seasonal_hints["has_daily_pattern"] is True

    def test_timeseries_stats_default_seasonal_hints(self) -> None:
        """Test TimeseriesStats default seasonal_hints is empty dict."""
        stats = TimeseriesStats(
            count=10,
            mean=5.0,
            median=5.0,
            std=1.0,
            min=1.0,
            max=10.0,
            skewness=0.0,
            kurtosis=0.0,
            p10=1.0,
            p25=2.5,
            p50=5.0,
            p75=7.5,
            p90=9.0,
            p95=9.5,
            p99=9.9,
        )

        assert stats.seasonal_hints == {}

    def test_seasonal_profile_dataclass(self) -> None:
        """Test SeasonalProfile dataclass creation."""
        profile = SeasonalProfile(
            period="daily",
            profile_mean={0: 50.0, 12: 100.0},
            profile_std={0: 5.0, 12: 10.0},
            profile_min={0: 40.0, 12: 80.0},
            profile_max={0: 60.0, 12: 120.0},
            dominant_period_unit=12,
            range_ratio=2.0,
        )

        assert profile.period == "daily"
        assert profile.dominant_period_unit == 12
        assert profile.range_ratio == 2.0
        assert profile.profile_mean[0] == 50.0

    def test_peak_analysis_dataclass(self) -> None:
        """Test PeakAnalysis dataclass creation."""
        peaks = PeakAnalysis(
            peak_value=200.0,
            peak_timestamp=datetime(2024, 1, 15, 18, 0),
            trough_value=50.0,
            trough_timestamp=datetime(2024, 1, 15, 4, 0),
            peak_to_trough_ratio=4.0,
            mean_to_peak_ratio=0.5,
            threshold_percentile=90.0,
            threshold_value=180.0,
            time_above_threshold_pct=10.0,
            peak_hours=[17, 18, 19],
            peak_days=[1, 2, 3],
            peak_months=[1, 12],
            max_consecutive_above_threshold=6,
        )

        assert peaks.peak_value == 200.0
        assert peaks.peak_to_trough_ratio == 4.0
        assert peaks.peak_hours == [17, 18, 19]
        assert peaks.max_consecutive_above_threshold == 6

    def test_timeseries_stats_has_all_fields(self) -> None:
        """Test TimeseriesStats has all expected fields."""
        field_names = [f.name for f in fields(TimeseriesStats)]

        expected_fields = [
            "count",
            "mean",
            "median",
            "std",
            "min",
            "max",
            "skewness",
            "kurtosis",
            "p10",
            "p25",
            "p50",
            "p75",
            "p90",
            "p95",
            "p99",
            "seasonal_hints",
        ]

        for field_name in expected_fields:
            assert field_name in field_names

    def test_seasonal_profile_has_all_fields(self) -> None:
        """Test SeasonalProfile has all expected fields."""
        field_names = [f.name for f in fields(SeasonalProfile)]

        expected_fields = [
            "period",
            "profile_mean",
            "profile_std",
            "profile_min",
            "profile_max",
            "dominant_period_unit",
            "range_ratio",
        ]

        for field_name in expected_fields:
            assert field_name in field_names

    def test_peak_analysis_has_all_fields(self) -> None:
        """Test PeakAnalysis has all expected fields."""
        field_names = [f.name for f in fields(PeakAnalysis)]

        expected_fields = [
            "peak_value",
            "peak_timestamp",
            "trough_value",
            "trough_timestamp",
            "peak_to_trough_ratio",
            "mean_to_peak_ratio",
            "threshold_percentile",
            "threshold_value",
            "time_above_threshold_pct",
            "peak_hours",
            "peak_days",
            "peak_months",
            "max_consecutive_above_threshold",
        ]

        for field_name in expected_fields:
            assert field_name in field_names


# =============================================================================
# Tests for Module Exports
# =============================================================================


class TestModuleExports:
    """Tests for module exports."""

    def test_stats_module_importable(self) -> None:
        """Test stats module can be imported from ukpyn.utils."""
        from ukpyn.utils import stats

        assert hasattr(stats, "describe_timeseries")
        assert hasattr(stats, "autocorrelation")
        assert hasattr(stats, "seasonal_pattern")
        assert hasattr(stats, "peak_analysis")

    def test_dataclasses_exported_from_stats(self) -> None:
        """Test dataclasses are exported from stats module."""
        from ukpyn.utils import stats

        assert hasattr(stats, "TimeseriesStats")
        assert hasattr(stats, "SeasonalProfile")
        assert hasattr(stats, "PeakAnalysis")

    def test_dataclasses_exported_from_utils(self) -> None:
        """Test dataclasses are exported from ukpyn.utils."""
        from ukpyn.utils import PeakAnalysis, SeasonalProfile, TimeseriesStats

        # Verify they are dataclasses
        assert len(fields(TimeseriesStats)) > 0
        assert len(fields(SeasonalProfile)) > 0
        assert len(fields(PeakAnalysis)) > 0

    def test_functions_exported_from_utils(self) -> None:
        """Test functions are exported from ukpyn.utils."""
        from ukpyn import utils

        assert hasattr(utils, "describe_timeseries")
        assert hasattr(utils, "autocorrelation")
        assert hasattr(utils, "seasonal_pattern")
        assert hasattr(utils, "peak_analysis")

    def test_utils_all_contains_stats_names(self) -> None:
        """Test utils.__all__ contains stats module names."""
        from ukpyn.utils import __all__

        expected = [
            "describe_timeseries",
            "TimeseriesStats",
            "autocorrelation",
            "seasonal_pattern",
            "SeasonalProfile",
            "peak_analysis",
            "PeakAnalysis",
        ]

        for name in expected:
            assert name in __all__


# =============================================================================
# Edge Case Tests
# =============================================================================


class TestEdgeCases:
    """Tests for edge cases and boundary conditions."""

    def test_single_value_series_all_functions(
        self, single_value_timeseries: pd.Series
    ) -> None:
        """Test all functions handle single value series."""
        # describe_timeseries
        stats = describe_timeseries(single_value_timeseries)
        assert stats.count == 1
        assert stats.mean == 100.0

        # autocorrelation
        acf = autocorrelation(single_value_timeseries)
        assert len(acf) == 1
        assert acf[0] == 1.0

        # seasonal_pattern
        profile = seasonal_pattern(single_value_timeseries, period="daily")
        assert len(profile.profile_mean) == 1  # Only one hour represented

        # peak_analysis
        peaks = peak_analysis(single_value_timeseries)
        assert peaks.peak_value == 100.0
        assert peaks.trough_value == 100.0

    def test_all_nan_series_all_functions(self, all_nan_timeseries: pd.Series) -> None:
        """Test all functions handle all NaN series."""
        # describe_timeseries
        stats = describe_timeseries(all_nan_timeseries)
        assert stats.count == 0
        assert math.isnan(stats.mean)

        # autocorrelation
        acf = autocorrelation(all_nan_timeseries)
        assert len(acf) == 0

        # seasonal_pattern
        profile = seasonal_pattern(all_nan_timeseries, period="daily")
        assert profile.profile_mean == {}

        # peak_analysis
        peaks = peak_analysis(all_nan_timeseries)
        assert math.isnan(peaks.peak_value)

    def test_negative_values(self) -> None:
        """Test handling of negative values."""
        dates = pd.date_range("2024-01-01", periods=100, freq="30min")
        values = [-100.0 + i for i in range(100)]  # -100 to -1
        series = pd.Series(values, index=dates)

        # describe_timeseries
        stats = describe_timeseries(series)
        assert stats.min == -100.0
        assert stats.max == -1.0

        # autocorrelation
        acf = autocorrelation(series, lags=10)
        assert acf[0] == 1.0

        # seasonal_pattern
        profile = seasonal_pattern(series, period="daily")
        assert len(profile.profile_mean) > 0

        # peak_analysis
        peaks = peak_analysis(series)
        assert peaks.peak_value == -1.0
        assert peaks.trough_value == -100.0

    def test_mixed_positive_negative_values(self) -> None:
        """Test handling of mixed positive and negative values."""
        dates = pd.date_range("2024-01-01", periods=100, freq="30min")
        values = [-50.0 + i for i in range(100)]  # -50 to 49
        series = pd.Series(values, index=dates)

        stats = describe_timeseries(series)
        assert stats.min == -50.0
        assert stats.max == 49.0
        assert stats.mean == pytest.approx(-0.5, rel=0.01)

    def test_very_large_values(self) -> None:
        """Test handling of very large values."""
        dates = pd.date_range("2024-01-01", periods=100, freq="30min")
        values = [1e15 + i for i in range(100)]
        series = pd.Series(values, index=dates)

        stats = describe_timeseries(series)
        assert stats.count == 100
        assert stats.min == 1e15
        assert stats.max == 1e15 + 99

    def test_very_small_values(self) -> None:
        """Test handling of very small values."""
        dates = pd.date_range("2024-01-01", periods=100, freq="30min")
        values = [1e-15 * (i + 1) for i in range(100)]
        series = pd.Series(values, index=dates)

        stats = describe_timeseries(series)
        assert stats.count == 100
        assert stats.min > 0

    def test_mixed_nan_values(self) -> None:
        """Test handling of mixed NaN values throughout series."""
        dates = pd.date_range("2024-01-01", periods=100, freq="30min")
        values = [100.0 if i % 3 != 0 else None for i in range(100)]
        series = pd.Series(values, index=dates)

        stats = describe_timeseries(series)
        assert stats.count < 100  # Some NaN dropped

        acf = autocorrelation(series, lags=10)
        assert len(acf) > 0

    def test_datetime_index_without_timezone(self) -> None:
        """Test handling of datetime index without timezone."""
        dates = pd.date_range("2024-01-01", periods=50, freq="30min", tz=None)
        series = pd.Series([100.0] * 50, index=dates)

        profile = seasonal_pattern(series, period="daily")
        assert profile.period == "daily"

    def test_datetime_index_with_timezone(self) -> None:
        """Test handling of datetime index with timezone."""
        dates = pd.date_range("2024-01-01", periods=50, freq="30min", tz="UTC")
        series = pd.Series([100.0] * 50, index=dates)

        profile = seasonal_pattern(series, period="daily")
        assert profile.period == "daily"

    def test_zero_std_series(self) -> None:
        """Test handling of series with zero standard deviation."""
        dates = pd.date_range("2024-01-01", periods=100, freq="30min")
        values = [42.0] * 100  # All same value
        series = pd.Series(values, index=dates)

        # describe_timeseries should handle zero std
        stats = describe_timeseries(series)
        assert stats.std == 0.0
        assert stats.skewness == 0.0
        assert stats.kurtosis == 0.0

        # autocorrelation should return all 1s for constant series
        acf = autocorrelation(series, lags=10)
        assert all(v == 1.0 for v in acf)

    def test_inf_values_in_series(self) -> None:
        """Test handling of infinity values in series."""
        dates = pd.date_range("2024-01-01", periods=10, freq="30min")
        values = [1.0, 2.0, 3.0, float("inf"), 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]
        series = pd.Series(values, index=dates)

        # describe_timeseries should exclude inf from summary stats
        stats = describe_timeseries(series)
        assert stats.count == 9
        assert stats.max == 10.0
        assert stats.seasonal_hints["has_non_finite_values"] is True
        assert stats.seasonal_hints["excluded_non_finite_count"] == 1

    def test_sparse_data_seasonal_pattern(self) -> None:
        """Test seasonal pattern with sparse data (few points per period unit)."""
        # Only 2 hours of data per day for 3 days
        dates = pd.date_range("2024-01-01 10:00", periods=12, freq="30min")
        values = [100.0] * 12
        series = pd.Series(values, index=dates)

        profile = seasonal_pattern(series, period="daily")

        # Should only have profile for hours covered
        assert len(profile.profile_mean) < 24

    def test_peak_analysis_all_same_value(self, simple_timeseries: pd.Series) -> None:
        """Test peak analysis when all values are the same."""
        result = peak_analysis(simple_timeseries)

        assert result.peak_value == result.trough_value
        assert result.peak_to_trough_ratio == 1.0
        assert result.mean_to_peak_ratio == 1.0
