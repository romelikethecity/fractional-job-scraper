"""
Generate Fractional Executive Market Trend Charts
Reads from historical_data.json and creates chart images
Brand colors: Slate #2c3e50, Teal #0d9488
"""

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import json
import os
from datetime import datetime, timedelta

# Brand colors
SLATE = '#2c3e50'
TEAL = '#0d9488'
BG_COLOR = '#ffffff'
GRID_COLOR = '#e0e0e0'

# Set up matplotlib style
plt.style.use('default')
plt.rcParams['figure.facecolor'] = BG_COLOR
plt.rcParams['axes.facecolor'] = BG_COLOR
plt.rcParams['axes.edgecolor'] = SLATE
plt.rcParams['grid.color'] = GRID_COLOR
plt.rcParams['text.color'] = SLATE
plt.rcParams['axes.labelcolor'] = SLATE
plt.rcParams['xtick.color'] = SLATE
plt.rcParams['ytick.color'] = SLATE
plt.rcParams['font.family'] = 'sans-serif'
plt.rcParams['font.weight'] = 'bold'
plt.rcParams['xtick.labelsize'] = 18
plt.rcParams['ytick.labelsize'] = 18


def load_historical_data():
    """Load historical data from JSON file"""
    with open('historical_data.json', 'r') as f:
        data = json.load(f)
    
    dates = [datetime.strptime(d['date'], '%Y-%m-%d') for d in data]
    values = [d['total'] for d in data]
    return dates, values


def create_chart(dates, values, title, filename, time_filter=None):
    """Create a single trend chart"""
    
    # Filter data based on time range
    if time_filter:
        now = datetime.now()
        if time_filter == '30d':
            cutoff = now - timedelta(days=30)
        elif time_filter == '90d':
            cutoff = now - timedelta(days=90)
        elif time_filter == '6m':
            cutoff = now - timedelta(days=180)
        elif time_filter == '12m':
            cutoff = now - timedelta(days=365)
        else:
            cutoff = None
        
        if cutoff:
            filtered = [(d, v) for d, v in zip(dates, values) if d >= cutoff]
            if filtered:
                dates = [x[0] for x in filtered]
                values = [x[1] for x in filtered]
    
    fig, ax = plt.subplots(figsize=(14, 7))
    
    # Create gradient fill under the line
    ax.fill_between(dates, values, alpha=0.3, color=TEAL)
    
    # Plot the main line
    ax.plot(dates, values, color=TEAL, linewidth=2.5, solid_capstyle='round')
    
    # Mark the current point with teal dot
    ax.scatter([dates[-1]], [values[-1]], color=TEAL, s=100, zorder=5, edgecolors=SLATE, linewidths=2)
    
    # Mark the peak point
    max_val = max(values)
    max_idx = values.index(max_val)
    if max_idx != len(values) - 1:  # Don't double-mark if current is peak
        ax.scatter([dates[max_idx]], [values[max_idx]], color=TEAL, s=80, zorder=5, edgecolors=SLATE, linewidths=2)
    
    # Title
    ax.set_title(title, fontsize=24, fontweight='bold', color=SLATE, pad=20)
    
    # Y-axis label
    ax.set_ylabel('Openings', fontsize=28, fontweight='bold', color=SLATE)
    
    # Grid
    ax.grid(True, alpha=0.3, linestyle='-')
    ax.set_axisbelow(True)
    
    # Format x-axis dates
    if time_filter in ['30d', '90d']:
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
        ax.xaxis.set_major_locator(mdates.WeekdayLocator(interval=1))
    elif time_filter in ['6m', '12m']:
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
        ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    else:  # All time
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
        ax.xaxis.set_major_locator(mdates.YearLocator())
    
    plt.xticks(rotation=45, ha='right', fontsize=18)
    
    # Add legend
    legend_text = 'Fractional\nOpenings'
    ax.annotate(legend_text, xy=(0.85, 0.3), xycoords='axes fraction',
                fontsize=24, color=TEAL, fontweight='bold',
                bbox=dict(boxstyle='round', facecolor=BG_COLOR, edgecolor='none'))
    
    # Tight layout
    plt.tight_layout()
    
    # Save
    plt.savefig(filename, dpi=150, facecolor=BG_COLOR, edgecolor='none', 
                bbox_inches='tight', pad_inches=0.2)
    plt.close()
    print(f"Created: {filename}")


def create_highlight_card(value, subtitle, brand_text, filename):
    """Create a highlight card"""
    
    fig, ax = plt.subplots(figsize=(10, 7))
    ax.set_facecolor(BG_COLOR)
    fig.patch.set_facecolor(BG_COLOR)
    
    # Remove axes
    ax.axis('off')
    
    # Main value
    ax.text(0.5, 0.65, value, fontsize=80, fontweight='bold', color=SLATE,
            ha='center', va='center', transform=ax.transAxes)
    
    # Subtitle
    ax.text(0.5, 0.35, subtitle, fontsize=28, fontweight='bold', color=SLATE,
            ha='center', va='center', transform=ax.transAxes)
    
    # Brand text
    ax.text(0.5, 0.15, brand_text, fontsize=22, fontweight='bold', color=TEAL,
            ha='center', va='center', transform=ax.transAxes)
    
    plt.tight_layout()
    plt.savefig(filename, dpi=150, facecolor=BG_COLOR, edgecolor='none',
                bbox_inches='tight', pad_inches=0.3)
    plt.close()
    print(f"Created: {filename}")


if __name__ == "__main__":
    # Create charts directory
    os.makedirs('charts', exist_ok=True)
    
    # Load data
    print("Loading historical data...")
    dates, values = load_historical_data()
    print(f"Loaded {len(dates)} data points")
    
    # Create all charts
    print("\nCreating charts...")
    
    create_chart(dates, values, 
                 'Fractional Executive Market Trends - Complete History',
                 'charts/trend_all_time.png')
    
    create_chart(dates, values,
                 'Fractional Executive Trends - Last 12 Months', 
                 'charts/trend_12_months.png',
                 time_filter='12m')
    
    create_chart(dates, values,
                 'Fractional Executive Trends - Last 6 Months',
                 'charts/trend_6_months.png', 
                 time_filter='6m')
    
    create_chart(dates, values,
                 'Fractional Executive Trends - Last 90 Days',
                 'charts/trend_90_days.png',
                 time_filter='90d')
    
    create_chart(dates, values,
                 'Fractional Executive Trends - Last 30 Days',
                 'charts/trend_30_days.png',
                 time_filter='30d')
    
    # Create highlight cards with current stats
    current_total = values[-1]
    peak_total = max(values)
    
    create_highlight_card(str(current_total), "Fractional Openings\nThis Week",
                          'The Fractional Report',
                          'charts/highlight_current.png')
    
    create_highlight_card('$213/hr', "Average Fractional\nHourly Rate",
                          'The Fractional Report',
                          'charts/highlight_rate.png')
    
    create_highlight_card('120K', "Fractional Executives\nin 2024",
                          'The Fractional Report',
                          'charts/highlight_market.png')
    
    print("\n✅ All charts created in charts/")
