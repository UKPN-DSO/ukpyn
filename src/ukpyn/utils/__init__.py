"""Utility functions for working with UK Power Networks data.

This module provides analysis and processing utilities for UKPN datasets,
particularly for time series analysis of powerflow data.

Modules:
    timeseries: Time series analysis (step change detection, QC, gap filling)
    stats: Statistical analysis (descriptive stats, autocorrelation, seasonal patterns)
"""

from .stats import (
    PeakAnalysis,
    SeasonalProfile,
    TimeseriesStats,
    # Statistical analysis
    autocorrelation,
    describe_timeseries,
    peak_analysis,
    seasonal_pattern,
)
from .timeseries import (
    GapInfo,
    QualityReport,
    StepChange,
    detect_redaction,
    detect_rolling_anomalies,
    # Step change analysis
    detect_step_changes,
    # Gap filling
    fill_gaps,
    flag_outliers,
    identify_gaps,
    # Quality control
    quality_control,
    # Utilities
    records_to_timeseries,
    summarize_flow_balance,
    summarize_redaction_by_period,
)

__all__ = [
    # Step change
    "detect_step_changes",
    "StepChange",
    # Quality control
    "quality_control",
    "QualityReport",
    "flag_outliers",
    "detect_redaction",
    "detect_rolling_anomalies",
    # Gap filling
    "fill_gaps",
    "identify_gaps",
    "GapInfo",
    # Utilities
    "records_to_timeseries",
    "summarize_redaction_by_period",
    "summarize_flow_balance",
    # Statistical analysis
    "describe_timeseries",
    "TimeseriesStats",
    "autocorrelation",
    "seasonal_pattern",
    "SeasonalProfile",
    "peak_analysis",
    "PeakAnalysis",
]
