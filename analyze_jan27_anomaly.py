import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# Read the data
df = pd.read_csv('examples/load_filled_clean_jan2024.csv')
df['timestamp'] = pd.to_datetime(df['timestamp'])
df.set_index('timestamp', inplace=True)

# Focus on evening peak hours (5-9 PM)
evening_hours = df.between_time('17:00', '21:00')

# Get Jan 27th data
jan27_evening = df.loc['2024-01-27 17:00':'2024-01-27 21:00']

print("=" * 60)
print("JANUARY 27th EVENING DATA (5 PM - 9 PM)")
print("=" * 60)
print(jan27_evening)
print()

# Calculate daily evening averages for comparison
daily_evening_stats = evening_hours.groupby(evening_hours.index.date).agg({
    'load_mw': ['mean', 'min', 'max', 'std']
})
daily_evening_stats.columns = ['mean', 'min', 'max', 'std']

print("=" * 60)
print("DAILY EVENING STATISTICS (5 PM - 9 PM) FOR ALL OF JANUARY")
print("=" * 60)
print(daily_evening_stats)
print()

# Get Jan 27th stats
jan27_stats = daily_evening_stats.loc[pd.Timestamp('2024-01-27').date()]
overall_mean = daily_evening_stats['mean'].mean()
overall_std = daily_evening_stats['mean'].std()

print("=" * 60)
print("ANOMALY ANALYSIS")
print("=" * 60)
print(f"Jan 27th evening mean: {jan27_stats['mean']:.2f} MW")
print(f"Overall January evening mean: {overall_mean:.2f} MW")
print(f"Overall January evening std: {overall_std:.2f} MW")
print(f"Deviation: {jan27_stats['mean'] - overall_mean:.2f} MW")
print(f"Z-score: {(jan27_stats['mean'] - overall_mean) / overall_std:.2f}")
print()

# Check if it's an outlier
if abs((jan27_stats['mean'] - overall_mean) / overall_std) > 2:
    print("⚠️  WARNING: Jan 27th evening is a statistical outlier!")
    if jan27_stats['mean'] < overall_mean:
        print("   → Values are unusually LOW")
    else:
        print("   → Values are unusually HIGH")
else:
    print("✓ Jan 27th evening appears normal")
print()

# Compare with neighboring days
print("=" * 60)
print("COMPARISON WITH NEIGHBORING DAYS (Evening Means)")
print("=" * 60)
for day in range(25, 30):
    day_date = pd.Timestamp(f'2024-01-{day}').date()
    if day_date in daily_evening_stats.index:
        stats = daily_evening_stats.loc[day_date]
        marker = " ← TARGET" if day == 27 else ""
        print(f"Jan {day}: {stats['mean']:6.2f} MW (range: {stats['min']:6.2f} - {stats['max']:6.2f}){marker}")
print()

# Visualize
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10))

# Plot 1: Daily evening means across the month
ax1.plot(daily_evening_stats.index, daily_evening_stats['mean'], 
         marker='o', linewidth=2, markersize=6, label='Daily Evening Mean')
# Highlight Jan 27th
jan27_idx = pd.Timestamp('2024-01-27').date()
ax1.scatter([jan27_idx], [daily_evening_stats.loc[jan27_idx, 'mean']], 
            color='red', s=200, zorder=5, label='Jan 27th', marker='x', linewidth=3)
ax1.axhline(y=overall_mean, color='green', linestyle='--', alpha=0.7, label='January Average')
ax1.fill_between(daily_evening_stats.index, 
                 overall_mean - 2*overall_std, 
                 overall_mean + 2*overall_std,
                 alpha=0.2, color='green', label='±2σ (Normal Range)')
ax1.set_xlabel('Date')
ax1.set_ylabel('Power (MW)')
ax1.set_title('Evening Peak Loads (5-9 PM) - Daily Averages')
ax1.legend()
ax1.grid(True, alpha=0.3)

# Plot 2: Compare Jan 27th with typical days
typical_days = [20, 21, 22, 23, 24]  # Weekdays before
for day in typical_days:
    day_data = df.loc[f'2024-01-{day} 17:00':f'2024-01-{day} 21:00']
    ax2.plot(day_data.index.hour + day_data.index.minute/60, 
             day_data['load_mw'], 
             alpha=0.3, linewidth=1, color='gray', label='Other days' if day == typical_days[0] else '')

# Plot Jan 27th
ax2.plot(jan27_evening.index.hour + jan27_evening.index.minute/60, 
         jan27_evening['load_mw'], 
         linewidth=3, color='red', marker='o', markersize=8, label='Jan 27th', zorder=5)

ax2.set_xlabel('Hour of Day')
ax2.set_ylabel('Power (MW)')
ax2.set_title('Evening Load Profile Comparison: Jan 27th vs Previous Week')
ax2.set_xticks([17, 18, 19, 20, 21])
ax2.legend()
ax2.grid(True, alpha=0.3)

plt.tight_layout()
plt.savefig('jan27_anomaly_analysis.png', dpi=150, bbox_inches='tight')
print("Saved visualization to: jan27_anomaly_analysis.png")
plt.show()
