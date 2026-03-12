# Forecasting Example Plan

## Overview

Create `examples/forecasting.ipynb` - A comprehensive Jupyter notebook demonstrating how to combine UKPN data sources for substation load forecasting with quality control and flexibility market analysis.

## Two Themes

### Theme 1: Raw Time Series Forecaster
End-to-end workflow from data acquisition to trained ML model.

### Theme 2: Operational Flexibility Market Analysis
Apply the trained model to determine flexibility market needs.

---

## Detailed Implementation Plan

### Section 1: Data Acquisition

**1.1 Select a Substation**
- Use LTDS Table 3A to browse available primary substations
- Pick a substation (e.g., in EPN licence area)
- Display substation metadata

**1.2 Get Transformer Data from LTDS Table 2**
- Query LTDS Table 2A (2-winding transformers) for the selected substation
- Query LTDS Table 2B (3-winding transformers) if applicable
- Extract transformer IDs, ratings (MVA), and configuration
- Calculate N-1 rating (sum of all transformer ratings minus the largest)

**1.3 Find Transformers in Monthly Powerflow Data**
- Use `powerflow.get_transformer_timeseries()` to discover available data
- Match LTDS transformer IDs to powerflow dataset identifiers
- Validate that transformers are present in powerflow data

**1.4 Get 2 Years of Data (2023-2025)**
- Query monthly powerflow data for the matched transformers
- Date range: 2023-01-01 to 2025-01-01
- Collect active power (MW) time series for each transformer

---

### Section 2: Data Visualisation

**2.1 Stacked Area Chart**
- X-axis: Time (2 years)
- Y-axis: Power (MW)
- Stack: T1, T2, ... Tx (each transformer as a layer)
- Total line showing aggregate substation power flow
- Title: Substation name as label
- Use matplotlib/plotly for professional styling

---

### Section 3: Quality Control (using `ukpyn.utils.timeseries`)

**3.1 Step Change Detection**
- Use `detect_step_changes()` from utils.timeseries
- Identify sudden shifts indicating:
  - Abnormal running arrangements
  - Equipment changes
  - Data quality issues
- **Visualisation**: Line chart with highlighted area bands over detected step changes

**3.2 Gap Filling**
- Use `identify_gaps()` to find missing data periods
- Use `fill_gaps()` with max_gap_hours equivalent to 2 days (48 hours)
- Options: linear interpolation, forward fill, or synthetic data
- **Visualisation**:
  - Zoom to a 5-day window containing a filled gap
  - Show filled data in a different colour on line chart
  - Legend distinguishing original vs. filled data

**3.3 Anomaly Detection & Removal**
- Use `flag_outliers()` with IQR or MAD method
- Use `quality_control()` for comprehensive QC report
- Mark anomalous points for removal
- **Visualisation**:
  - Clean data in tasteful gray
  - Removed anomalies highlighted in colour (red/orange)
  - Show before/after comparison

---

### Section 4: Statistical Analysis (NEW: `ukpyn.utils.stats`)

**4.1 New Module Required: `src/ukpyn/utils/stats.py`**

Functions to implement:
```python
def describe_timeseries(series) -> dict:
    """Comprehensive statistical summary."""
    # Returns: mean, median, std, min, max, skewness, kurtosis
    # Percentiles: P10, P25, P50, P75, P90, P95, P99
    # Seasonal decomposition hints

def autocorrelation(series, lags: int = 48) -> pd.Series:
    """Calculate autocorrelation for given lags."""

def seasonal_pattern(series, period: str = 'daily') -> pd.DataFrame:
    """Extract seasonal patterns (daily, weekly, annual)."""
    # Average profile by hour-of-day, day-of-week, month

def peak_analysis(series) -> dict:
    """Analyze peak characteristics."""
    # Peak times, magnitudes, frequency, duration above threshold
```

**4.2 Statistical Interrogation**
- Run `describe_timeseries()` on QC'd data
- Analyze autocorrelation structure
- Extract daily/weekly/seasonal patterns
- Identify peak load characteristics

---

### Section 5: Feature Engineering for Forecasting

**5.1 Temporal Features**
- Hour of day (0-23)
- Day of week (0-6)
- Month (1-12)
- Is weekend (bool)
- UK Bank Holidays (bool) - use `holidays` library

**5.2 Weather Features (using `ukpyn.integrations.weather`)**
Historical data for the substation location:
- Ambient temperature (°C)
- Relative humidity (%)
- Wind speed (m/s)
- Wind gust (m/s)
- Global horizontal irradiance (W/m²)

Note: If weather integration not available, use synthetic/placeholder data or skip.

**5.3 Lagged Features**
- Load at t-1, t-2, t-24, t-48 (previous values)
- Same hour yesterday, same hour last week

---

### Section 6: ML Forecasting with sklearn

**6.1 Data Preparation**
- Use 1 year of QC'd time series for model development
- 80:20 train/test split
- **Important**: Split on whole days to avoid data leakage
  - e.g., 292 days training, 73 days testing
- Normalize features appropriately

**6.2 Models to Implement**

| Model Type | Description | Library |
|------------|-------------|---------|
| Smart Persistence | Same time yesterday/last week | Manual |
| Linear Regression | Baseline multivariate | sklearn |
| Random Forest | Tree ensemble | sklearn |
| Gradient Boosting | XGBoost-style | sklearn |
| Pure Time Series | ARIMA/Prophet-style | statsmodels (optional) |

**6.3 Training**
- Fit each model on training data
- Use time series cross-validation where appropriate
- Track training time

**6.4 Testing & Metrics**
Report for each model:
- MAE (Mean Absolute Error)
- RMSE (Root Mean Square Error)
- MAPE (Mean Absolute Percentage Error)
- R² (Coefficient of determination)

Select best performing model based on RMSE.

---

### Section 7: Probabilistic Forecasting

**7.1 Quantile Regression**
- Train quantile regressors for P10, P50, P90
- Or use prediction intervals from tree models

**7.2 10am Daily Forecast Scenario**
- Simulate operational scenario: forecast made at 10am each day
- Forecast horizon: rest of day (14 hours ahead)
- Generate point forecast + P90 upper bound

---

### Section 8: Flexibility Market Analysis

**8.1 Rating Calculation**
- Sum of all transformer ratings from LTDS Table 2
- N-1 rating = Total - Largest transformer rating
- This is the constraint for flexibility needs

**8.2 Apply Model to Unseen Year**
- Use the second year of data (not used in training)
- Generate daily 10am forecasts with P90

**8.3 Identify Flex Market Need Days**
- Days where P90 forecast > N-1 rating
- These are days requiring flexibility procurement

**8.4 Visualisation**
For each flex-need day, show:
- **Line**: Actual load
- **Line**: Point forecast
- **Dashed line**: N-1 Rating threshold
- **Shaded area**: Region between P90 and rating (when P90 > rating)
- Highlight the constraint violation period

---

## New Code Required

### 1. `src/ukpyn/utils/stats.py`
Statistical analysis functions for time series.

### 2. Update `src/ukpyn/utils/__init__.py`
Export stats module functions.

### 3. `examples/forecasting.ipynb`
The Jupyter notebook implementing all the above.

---

## Dependencies

Add to `pyproject.toml` under `[project.optional-dependencies]`:

```toml
forecasting = [
    "scikit-learn>=1.3.0",
    "statsmodels>=0.14.0",
    "holidays>=0.40",
]
```

Update `notebooks` extra to include forecasting deps or keep separate.

---

## Visualisation Style Guide

- Use consistent colour palette throughout
- Primary data: Navy blue (#1f4e79)
- Filled gaps: Teal (#2aa198)
- Anomalies/removed: Coral (#dc322f)
- Forecast: Green (#859900)
- P90 band: Light green with alpha
- Rating line: Orange dashed (#cb4b16)
- Use white background, minimal gridlines
- Clear axis labels with units
- Informative titles

---

## Notebook Structure

```
examples/forecasting.ipynb
│
├── 1. Introduction & Setup
│   ├── Import libraries
│   └── Configure API key
│
├── 2. Data Acquisition
│   ├── Select substation
│   ├── Get LTDS transformer data
│   └── Get powerflow time series
│
├── 3. Initial Visualisation
│   └── Stacked area chart
│
├── 4. Quality Control
│   ├── Step change detection
│   ├── Gap filling
│   └── Anomaly removal
│
├── 5. Statistical Analysis
│   ├── Descriptive statistics
│   ├── Autocorrelation
│   └── Seasonal patterns
│
├── 6. Feature Engineering
│   ├── Temporal features
│   ├── Weather features (optional)
│   └── Lagged features
│
├── 7. Model Training
│   ├── Data split
│   ├── Train models
│   └── Evaluate performance
│
├── 8. Probabilistic Forecasting
│   └── P90 predictions
│
├── 9. Flexibility Market Analysis
│   ├── Calculate N-1 rating
│   ├── Apply to unseen year
│   └── Identify flex-need days
│
└── 10. Summary & Conclusions
```

---

## Implementation Order

1. Create `src/ukpyn/utils/stats.py` with statistical functions
2. Update utils `__init__.py` to export stats
3. Add `forecasting` optional dependency group
4. Create `examples/forecasting.ipynb` notebook
5. Write tests for stats module
6. Verify notebook runs end-to-end (with mock data if needed)
