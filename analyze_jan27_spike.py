import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# Read the data
df = pd.read_csv('examples/load_filled_clean_jan2024.csv')
df['timestamp'] = pd.to_datetime(df['timestamp'])
df.set_index('timestamp', inplace=True)

# Focus on the problematic period
problem_window = df.loc['2024-01-27 16:00':'2024-01-27 22:00']

print("=" * 70)
print("DETAILED VIEW: Jan 27th 4 PM - 10 PM")
print("=" * 70)
print(problem_window)
print()

# Calculate rate of change
problem_window_copy = problem_window.copy()
problem_window_copy['change_mw'] = problem_window_copy['load_mw'].diff()
problem_window_copy['change_pct'] = problem_window_copy['load_mw'].pct_change() * 100

print("=" * 70)
print("RATE OF CHANGE ANALYSIS")
print("=" * 70)
print(problem_window_copy[['load_mw', 'change_mw', 'change_pct']])
print()

# Find the spike
spike_time = pd.Timestamp('2024-01-27 19:30:00+00:00')
spike_value = df.loc[spike_time, 'load_mw']
before_value = df.loc[spike_time - pd.Timedelta('30min'), 'load_mw']
after_value = df.loc[spike_time + pd.Timedelta('30min'), 'load_mw']

print("=" * 70)
print("SPIKE CHARACTERISTICS")
print("=" * 70)
print(f"Time of spike: {spike_time}")
print(f"Value before (19:00): {before_value:.2f} MW")
print(f"Spike value  (19:30): {spike_value:.2f} MW")
print(f"Value after  (20:00): {after_value:.2f} MW")
print()
print(f"Drop from previous:  {spike_value - before_value:.2f} MW ({(spike_value/before_value - 1)*100:.1f}%)")
print(f"Jump to next:        {after_value - spike_value:.2f} MW ({(after_value/spike_value - 1)*100:.1f}%)")
print()

# Check if it's exactly half
print(f"Is spike exactly half of previous? {spike_value:.2f} ≈ {before_value/2:.2f}? ", end="")
if abs(spike_value - before_value/2) < 1:
    print("YES! ⚠️ Possible meter/data transmission error (half-reading)")
else:
    print(f"NO (difference: {abs(spike_value - before_value/2):.2f} MW)")
print()

# Detect repeated values (stuck sensor)
repeated_mask = problem_window_copy['change_mw'] == 0
repeated_count = repeated_mask.sum()
print("=" * 70)
print("STUCK/REPEATED VALUE DETECTION")
print("=" * 70)
print(f"Number of repeated values (no change): {repeated_count}")
if repeated_count > 0:
    print("\nRepeated values:")
    print(problem_window_copy[repeated_mask][['load_mw', 'change_mw']])
print()

# Calculate expected value using interpolation
expected_value = (before_value + after_value) / 2
print("=" * 70)
print("CLEANING RECOMMENDATION")
print("=" * 70)
print(f"Current (anomalous) value:  {spike_value:.2f} MW")
print(f"Expected (interpolated):    {expected_value:.2f} MW")
print(f"Correction needed:          {expected_value - spike_value:.2f} MW")
print()

# Use moving average for better estimate
ma_window = df.loc['2024-01-27 17:00':'2024-01-27 21:00'].copy()
ma_window['rolling_mean'] = ma_window['load_mw'].rolling(window=3, center=True).mean()
ma_estimate = ma_window.loc[spike_time, 'rolling_mean']
print(f"Moving average estimate:    {ma_estimate:.2f} MW")
print()

# Check against typical evening pattern for Jan 27 day-of-week
dow = spike_time.dayofweek  # 0=Monday, 6=Sunday
same_dow = df[df.index.dayofweek == dow]
same_time = same_dow[same_dow.index.time == spike_time.time()]
typical_value = same_time['load_mw'].median()
print(f"Typical value for {spike_time.strftime('%A')} at 19:30: {typical_value:.2f} MW")
print()

print("=" * 70)
print("DETECTION METHOD PERFORMANCE")
print("=" * 70)

# 1. Would IQR catch it?
evening_data = df.between_time('17:00', '21:00')['load_mw']
q1 = evening_data.quantile(0.25)
q3 = evening_data.quantile(0.75)
iqr = q3 - q1
lower_bound_25 = q1 - 2.5 * iqr
upper_bound_25 = q3 + 2.5 * iqr
lower_bound_30 = q1 - 3.0 * iqr
upper_bound_30 = q3 + 3.0 * iqr

print(f"Evening data IQR: {iqr:.2f} MW")
print(f"  Threshold 2.5: [{lower_bound_25:.2f}, {upper_bound_25:.2f}] MW")
print(f"  Threshold 3.0: [{lower_bound_30:.2f}, {upper_bound_30:.2f}] MW")
print(f"  Spike value: {spike_value:.2f} MW")
if spike_value < lower_bound_25:
    print(f"  ✓ IQR (2.5) would DETECT this anomaly")
elif spike_value < lower_bound_30:
    print(f"  ✓ IQR (3.0) would DETECT this anomaly")
else:
    print(f"  ✗ IQR would MISS this anomaly")
print()

# 2. Would moving average deviation catch it?
rolling_mean = df['load_mw'].rolling(window=48, center=True).mean()  # 24 hours
rolling_std = df['load_mw'].rolling(window=48, center=True).std()
ma_lower = rolling_mean - 3 * rolling_std
ma_upper = rolling_mean + 3 * rolling_std

if spike_time in rolling_mean.index:
    local_mean = rolling_mean.loc[spike_time]
    local_std = rolling_std.loc[spike_time]
    local_lower = ma_lower.loc[spike_time]
    deviation = abs(spike_value - local_mean) / local_std
    
    print(f"Moving Average (24hr window):")
    print(f"  Local mean: {local_mean:.2f} MW")
    print(f"  Local std:  {local_std:.2f} MW")
    print(f"  Deviation:  {deviation:.2f}σ")
    if deviation > 3:
        print(f"  ✓ Moving average (3σ) would DETECT this anomaly")
    else:
        print(f"  ✗ Moving average (3σ) would MISS this anomaly")
print()

# 3. Rate of change detection
print(f"Rate of Change Detection:")
max_realistic_change = 10  # MW per 30 min for substations
change_19_30 = abs(spike_value - before_value)
change_20_00 = abs(after_value - spike_value)
print(f"  Change at 19:30: {change_19_30:.2f} MW")
print(f"  Change at 20:00: {change_20_00:.2f} MW")
print(f"  Max realistic:   {max_realistic_change:.2f} MW")
if change_19_30 > max_realistic_change or change_20_00 > max_realistic_change:
    print(f"  ✓ Rate-of-change detection would CATCH this")
else:
    print(f"  ✗ Rate-of-change detection would MISS this")
print()

# Visualize
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10))

# Plot 1: The problematic window
ax1.plot(problem_window.index, problem_window['load_mw'], 
         marker='o', linewidth=2, markersize=8, label='Actual Data')
ax1.scatter([spike_time], [spike_value], 
            color='red', s=400, zorder=5, label='Anomaly', marker='x', linewidth=4)
ax1.axhline(y=expected_value, color='green', linestyle='--', alpha=0.7, label=f'Expected ({expected_value:.1f} MW)')
ax1.set_xlabel('Time')
ax1.set_ylabel('Power (MW)')
ax1.set_title('Jan 27th Evening - Data Anomaly at 19:30')
ax1.legend()
ax1.grid(True, alpha=0.3)

# Plot 2: Rate of change
ax2.plot(problem_window_copy.index, problem_window_copy['change_mw'], 
         marker='o', linewidth=2, markersize=8, color='orange', label='Change from Previous')
ax2.axhline(y=0, color='black', linestyle='-', alpha=0.3, linewidth=1)
ax2.axhline(y=max_realistic_change, color='red', linestyle='--', alpha=0.5, label='Max Realistic Change')
ax2.axhline(y=-max_realistic_change, color='red', linestyle='--', alpha=0.5)
ax2.set_xlabel('Time')
ax2.set_ylabel('Change (MW)')
ax2.set_title('Rate of Change Detection - Highlighting Unrealistic Jumps')
ax2.legend()
ax2.grid(True, alpha=0.3)

plt.tight_layout()
plt.savefig('jan27_spike_analysis.png', dpi=150, bbox_inches='tight')
print("Saved visualization to: jan27_spike_analysis.png")
plt.show()
