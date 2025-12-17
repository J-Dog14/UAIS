import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
# GridSpec removed - using direct positioning instead
from matplotlib import image as mpimg
from matplotlib.lines import Line2D
from matplotlib.patches import Rectangle
from sqlalchemy import create_engine
from scipy.stats import linregress, percentileofscore
from datetime import datetime
import os

# ------------------------------------------------
# CONFIG
# ------------------------------------------------
DB_URI = "sqlite:////Users/karinasquilanti/Downloads/Athletic_Screen_All_data_v2-1.db"
ATHLETE_NAME = "Matt Solter"
OUTPUT_PDF = f"/Users/karinasquilanti/Downloads/{ATHLETE_NAME.replace(' ', '_')}_report.pdf"
LOGO_PATH = "/8ctnae - Faded 8 to Blue.png"
REPORT_DATE = datetime.now().strftime("%Y-%m-%d")
ACCENT_COLOR = '#2c99d4'  # Light blue accent color

TABLES = {
    "DJ": "DJ",
    "CMJ": "CMJ",
    "PPU": "PPU",
    "SLV": "SLV"
}

RADAR_METRICS = [
    "JH_IN",
    "PP_W_per_kg",
    "auc_j",
    "kurtosis",
    "rpd_max_w_per_s",
    "time_to_rpd_max_s"
]

BAR_METRICS = ["JH_IN", "PP_W_per_kg"]

# Metric label mapping for display
METRIC_LABELS = {
    "JH_IN": "Jump Height",
    "PP_W_per_kg": "Peak Power (Norm)",
    "auc_j": "Work (AUC)",
    "kurtosis": "Kurtosis",
    "rpd_max_w_per_s": "Max RPD",
    "time_to_rpd_max_s": "Time to Max RPD"
}

# ------------------------------------------------
# Database Pull
# ------------------------------------------------
engine = create_engine(DB_URI)

def get_table(name):
    return pd.read_sql(f"SELECT * FROM {name}", engine)

def pull_all_data():
    data = {}
    for mov, tbl in TABLES.items():
        try:
            df = get_table(tbl)
            data[mov] = df
        except Exception as e:
            print(f"Warning: Could not load table '{tbl}': {e}")
            print(f"Skipping {mov}...")
    return data

all_data = pull_all_data()

# ------------------------------------------------
# Helper functions to add headers (page-specific)
# ------------------------------------------------
def add_header_dj(fig, athlete_name, test_date):
    """Add header for DJ page - can be adjusted independently"""
    # Vertical accent bar on the left
    bar_width = 0.03
    bar = Rectangle((0, 0), bar_width, 1, transform=fig.transFigure, 
                    facecolor=ACCENT_COLOR, zorder=1001)
    fig.add_artist(bar)
    
    # Athlete name and date in top left
    fig.text(0.04, 0.985, f"Name: {athlete_name}", 
            fontsize=54, color='white', ha='left', va='top', fontweight='bold', zorder=1002)
    fig.text(0.04, 0.97, f"Test Date: {test_date}", 
            fontsize=54, color='white', ha='left', va='top', fontweight='bold', zorder=1002)
    
    # Horizontal accent line below header
    header_line = Line2D([0.05, 0.98], [0.925, 0.925], transform=fig.transFigure, 
                        color=ACCENT_COLOR, linewidth=9, zorder=1002)
    fig.add_artist(header_line)
    
    # Logo in top right
    add_logo(fig, position='top-right')

def add_header_cmj_ppu(fig, athlete_name, test_date):
    """Add header for CMJ and PPU pages - can be adjusted independently"""
    # Vertical accent bar on the left
    bar_width = 0.03
    bar = Rectangle((0, 0), bar_width, 1, transform=fig.transFigure, 
                    facecolor=ACCENT_COLOR, zorder=1001)
    fig.add_artist(bar)
    
    # Athlete name and date in top left
    fig.text(0.04, 0.98, f"Name: {athlete_name}", 
            fontsize=54, color='white', ha='left', va='top', fontweight='bold', zorder=1002)
    fig.text(0.04, 0.96, f"Test Date: {test_date}", 
            fontsize=54, color='white', ha='left', va='top', fontweight='bold', zorder=1002)
    
    # Horizontal accent line below header
    header_line = Line2D([0.05, 0.98], [0.89, 0.89], transform=fig.transFigure, 
                        color=ACCENT_COLOR, linewidth=9, zorder=1002)
    fig.add_artist(header_line)
    
    # Logo in top right
    add_logo(fig, position='top-right')

def add_header_slv(fig, athlete_name, test_date):
    """Add header for SLV page - can be adjusted independently"""
    # Vertical accent bar on the left
    bar_width = 0.03
    bar = Rectangle((0, 0), bar_width, 1, transform=fig.transFigure, 
                    facecolor=ACCENT_COLOR, zorder=1001)
    fig.add_artist(bar)
    
    # Athlete name and date in top left
    fig.text(0.04, 0.98, f"Name: {athlete_name}", 
            fontsize=54, color='white', ha='left', va='top', fontweight='bold', zorder=1002)
    fig.text(0.04, 0.96, f"Test Date: {test_date}", 
            fontsize=54, color='white', ha='left', va='top', fontweight='bold', zorder=1002)
    
    # Horizontal accent line below header
    header_line = Line2D([0.05, 0.98], [0.89, 0.89], transform=fig.transFigure, 
                        color=ACCENT_COLOR, linewidth=9, zorder=1002)
    fig.add_artist(header_line)
    
    # Logo in top right
    add_logo(fig, position='top-right')

# ------------------------------------------------
# Helper function to add logo
# ------------------------------------------------
def add_logo(fig, position='top-right'):
    """Add 8ctane logo to figure"""
    if os.path.exists(LOGO_PATH):
        try:
            img = mpimg.imread(LOGO_PATH)
        
            logo_size = 0.28  # Increased from 0.18 to make logo larger 
            
            if position == 'top-right':
                # Position moved more to the left and lower to prevent cutoff
                # Using anchor='NE' means the position is the top-right corner of the logo
                # Lower y position to ensure full visibility
                ax_logo = fig.add_axes([0.70, 0.88, logo_size, logo_size * (img.shape[0]/img.shape[1])], 
                                      anchor='NE', zorder=1000)
            elif position == 'top-left':
                # Position in top-left corner
                ax_logo = fig.add_axes([0.02, 0.92, logo_size, logo_size * (img.shape[0]/img.shape[1])], 
                                      anchor='NW', zorder=1000)
            
            ax_logo.imshow(img)
            ax_logo.axis('off')
        except Exception as e:
            print(f"Warning: Could not load logo: {e}")

# ------------------------------------------------
# Radar Plot (Spider/Radial Chart) - Percentile-based
# ------------------------------------------------
def radar_chart(ax, row, title, population=None):
    labels = RADAR_METRICS
    values = [row[m] for m in labels]

    N = len(labels)
    angles = np.linspace(0, 2 * np.pi, N, endpoint=False)
    
    # Convert values to percentiles if population data is provided
    if population is not None:
        percentile_values = []
        for i, metric in enumerate(labels):
            if metric in population.columns:
                pop_values = population[metric].dropna()
                if len(pop_values) > 0 and values[i] is not None:
                    pct = percentileofscore(pop_values, values[i])
                    percentile_values.append(pct / 100.0)  # Convert to 0-1 scale
                else:
                    percentile_values.append(0)
            else:
                percentile_values.append(0)
    else:
        # Fallback to normalization if no population data
        max_val = max(values) if max(values) > 0 else 1
        percentile_values = [v / max_val for v in values]
    
    percentile_values += [percentile_values[0]]
    angles = np.append(angles, angles[0])

    ax.set_theta_offset(np.pi / 2)
    ax.set_theta_direction(-1)

    # Set radial limits and add grid circles (0-100 percentile)
    ax.set_ylim(0, 1)
    ax.set_yticks([0.2, 0.4, 0.6, 0.8, 1.0])
    ax.set_yticklabels(['20th', '40th', '60th', '80th', '100th'], color='white', fontsize=21)
    ax.grid(True, color='white', alpha=0.3, linestyle='--')

    ax.plot(angles, percentile_values, linewidth=2.0, color='#2c99d4', marker='o', markersize=5)  # Blue line
    ax.fill(angles, percentile_values, alpha=0.25, color='#d62728')  # Red shading
    ax.set_xticks(angles[:-1])
    # Use display labels instead of raw metric names
    display_labels = [METRIC_LABELS.get(label, label) for label in labels]
    ax.set_xticklabels(display_labels, fontsize=27, color='white')
    # Move labels further from chart using tick_params pad
    ax.tick_params(colors='white', pad=60)
    # Removed title
    ax.spines['polar'].set_color('white')
    ax.set_facecolor('#373e43')

# ------------------------------------------------
# SLV Radar Chart (consolidated - shows both left and right)
# ------------------------------------------------
def slv_radar_chart(ax, left_row, right_row, population=None):
    """Create a consolidated radar chart for SLV showing both left (dark blue) and right (red) legs"""
    labels = RADAR_METRICS
    
    # Process left leg
    left_values = [left_row[m] for m in labels] if left_row is not None else None
    # Process right leg
    right_values = [right_row[m] for m in labels] if right_row is not None else None
    
    N = len(labels)
    angles = np.linspace(0, 2 * np.pi, N, endpoint=False)
    
    def get_percentile_values(values, population):
        """Convert values to percentiles"""
        if population is not None and values is not None:
            percentile_values = []
            for i, metric in enumerate(labels):
                if metric in population.columns:
                    pop_values = population[metric].dropna()
                    if len(pop_values) > 0 and values[i] is not None:
                        pct = percentileofscore(pop_values, values[i])
                        percentile_values.append(pct / 100.0)  # Convert to 0-1 scale
                    else:
                        percentile_values.append(0)
                else:
                    percentile_values.append(0)
        elif values is not None:
            # Fallback to normalization if no population data
            max_val = max(values) if max(values) > 0 else 1
            percentile_values = [v / max_val for v in values]
        else:
            percentile_values = [0] * N
        return percentile_values
    
    left_percentiles = get_percentile_values(left_values, population)
    right_percentiles = get_percentile_values(right_values, population)
    
    # Close the loop
    left_percentiles += [left_percentiles[0]]
    right_percentiles += [right_percentiles[0]]
    angles_closed = np.append(angles, angles[0])
    
    ax.set_theta_offset(np.pi / 2)
    ax.set_theta_direction(-1)
    
    # Set radial limits and add grid circles (0-100 percentile)
    ax.set_ylim(0, 1)
    ax.set_yticks([0.2, 0.4, 0.6, 0.8, 1.0])
    ax.set_yticklabels(['20th', '40th', '60th', '80th', '100th'], color='white', fontsize=21)
    ax.grid(True, color='white', alpha=0.3, linestyle='--')
    
    # Plot left leg in dark blue - thicker line, no shading
    if left_row is not None:
        ax.plot(angles_closed, left_percentiles, linewidth=3.0, color='#1a5f8a', marker='o', markersize=5, label='Left Leg')
    
    # Plot right leg in red - thicker line, no shading
    if right_row is not None:
        ax.plot(angles_closed, right_percentiles, linewidth=3.0, color='#d62728', marker='o', markersize=5, label='Right Leg')
    
    ax.set_xticks(angles)
    display_labels = [METRIC_LABELS.get(label, label) for label in labels]
    ax.set_xticklabels(display_labels, fontsize=29, color='white')  # Increased from 27 to 29 (2 font sizes bigger)
    # Move labels further from chart using tick_params pad
    ax.tick_params(colors='white', pad=60)
    # Removed title
    ax.spines['polar'].set_color('white')
    ax.set_facecolor('#373e43')
    
    # Add legend if both legs are present
    if left_row is not None and right_row is not None:
        ax.legend(loc='upper right', facecolor='#373e43', edgecolor='white', labelcolor='white', fontsize=21)

# ------------------------------------------------
# Performance Table (for all movements)
# ------------------------------------------------
def performance_table(ax, row, movement_name=None):
    """Create a table showing CMJ variables with performance tiers"""
    
    # Define thresholds and tier names
    rpd_thresholds = {
        'Low': (float('-inf'), 8400),
        'Below Average': (8400, 12280),
        'Average': (12280, 17846),
        'High': (17846, 25000),
        'Elite': (25000, float('inf'))
    }
    
    kurtosis_thresholds = {
        'Very Flat': (float('-inf'), -1.3),
        'Moderately Flat': (-1.3, -0.4),
        'Typical': (-0.4, 0.5),
        'Spiky': (0.5, float('inf'))
    }
    
    auc_thresholds = {
        'Low': (float('-inf'), 366),
        'Below Average': (366, 513),
        'Average': (513, 773),
        'High': (773, 1100),
        'Elite': (1100, float('inf'))
    }
    
    # Color mapping - pastel colors with reduced opacity for boxes, text stays white
    # Convert hex to RGB and make pastel (lighter) versions with opacity
    def hex_to_pastel_rgba(hex_color, alpha=0.3):
        """Convert hex color to pastel RGBA with opacity"""
        hex_color = hex_color.lstrip('#')
        r = int(hex_color[0:2], 16) / 255.0
        g = int(hex_color[2:4], 16) / 255.0
        b = int(hex_color[4:6], 16) / 255.0
        # Make pastel by blending with white (lighter, softer colors)
        white = 1.0
        r = r * 0.5 + white * 0.5
        g = g * 0.5 + white * 0.5
        b = b * 0.5 + white * 0.5
        return (r, g, b, alpha)
    
    color_map = {
        'Elite': hex_to_pastel_rgba('#00ff00', 0.5),  # pastel green with opacity
        'High': hex_to_pastel_rgba('#90ee90', 0.5),   # pastel light green with opacity
        'Average': hex_to_pastel_rgba('#808080', 0.5), # pastel gray with opacity
        'Below Average': hex_to_pastel_rgba('#ffff00', 0.5), # pastel yellow with opacity
        'Low': hex_to_pastel_rgba('#d62728', 0.5),    # pastel red with opacity
        'Very Flat': hex_to_pastel_rgba('#d62728', 0.5),
        'Moderately Flat': hex_to_pastel_rgba('#ffff00', 0.5),
        'Typical': hex_to_pastel_rgba('#808080', 0.5),
        'Spiky': hex_to_pastel_rgba('#90ee90', 0.5)
    }
    
    def get_tier(value, thresholds):
        """Determine tier based on value and thresholds"""
        for tier, (min_val, max_val) in thresholds.items():
            if min_val <= value < max_val:
                return tier
        return 'Unknown'
    
    # Get values from row
    rpd_value = row.get('rpd_max_w_per_s', 0)
    kurtosis_value = row.get('kurtosis', 0)
    auc_value = row.get('auc_j', 0)
    
    # Determine tiers
    rpd_tier = get_tier(rpd_value, rpd_thresholds)
    kurtosis_tier = get_tier(kurtosis_value, kurtosis_thresholds)
    auc_tier = get_tier(auc_value, auc_thresholds)
    
    # Helper function to format numbers with significant figures
    def format_sigfig(value, sigfigs):
        """Format a number with specified significant figures"""
        if value == 0:
            return "0"
        # Calculate the order of magnitude
        import math
        order = math.floor(math.log10(abs(value)))
        # Round to the appropriate number of significant figures
        rounded = round(value, sigfigs - 1 - order)
        # Format to avoid scientific notation for reasonable ranges
        if abs(rounded) >= 1:
            # For numbers >= 1, use integer formatting if no decimals needed
            if rounded == int(rounded):
                return str(int(rounded))
            # Otherwise format with appropriate decimal places
            decimal_places = max(0, sigfigs - len(str(int(abs(rounded)))))
            format_str = "{:." + str(decimal_places) + "f}"
            return format_str.format(rounded).rstrip('0').rstrip('.')
        else:
            # For numbers < 1, use decimal formatting
            decimal_places = sigfigs - 1 - order
            format_str = "{:." + str(decimal_places) + "f}"
            return format_str.format(rounded).rstrip('0').rstrip('.')
    
    # Get threshold values - just the values, no labels, based on movement type
    def get_threshold_text(thresholds, var_type, movement_name):
        """Get elite/typical threshold values only, based on movement type"""
        if var_type == 'rpd':
            # Elite RPD Max values by movement
            elite_rpd = {
                "DJ": 32708,
                "CMJ": 25189,
                "PPU": 7980,
                "SLV": 18212
            }
            elite_min = elite_rpd.get(movement_name, 25000)
            return f">{format_sigfig(elite_min, 4)}"  # 4 significant figures
        elif var_type == 'kurtosis':
            # Typical Kurtosis ranges by movement - format with 3 significant figures
            typical_ranges_raw = {
                "DJ": (-1.43, -1.12),
                "CMJ": (-1.30, -0.40),
                "PPU": (-1.58, -1.25),
                "SLV": (-1.02, -0.19)
            }
            if movement_name in typical_ranges_raw:
                low, high = typical_ranges_raw[movement_name]
                low_str = format_sigfig(low, 3)
                high_str = format_sigfig(high, 3)
                return f"{low_str} to {high_str}"
            return "-0.4 to 0.5"
        elif var_type == 'auc':
            # Elite AUC values by movement
            elite_auc = {
                "DJ": 571.55,
                "CMJ": 1261.33,
                "PPU": 701.51,
                "SLV": 1027.56
            }
            elite_min = elite_auc.get(movement_name, 1100)
            return f">{format_sigfig(elite_min, 4)}"  # 4 significant figures
        return ""
    
    # Create table data with significant figures formatting
    table_data = [
        ['Variable', 'Athlete Value', 'Tier / Threshold', 'Ideal Values'],
        ['Max RPD (W/s)', format_sigfig(rpd_value, 4), rpd_tier, get_threshold_text(rpd_thresholds, 'rpd', movement_name)],  # 4 significant figures
        ['Kurtosis', format_sigfig(kurtosis_value, 3), kurtosis_tier, get_threshold_text(kurtosis_thresholds, 'kurtosis', movement_name)],  # 3 significant figures
        ['Work (AUC)', format_sigfig(auc_value, 4), auc_tier, get_threshold_text(auc_thresholds, 'auc', movement_name)]  # 4 significant figures
    ]
    
    # Create table
    ax.axis('off')
    ax.set_facecolor('#373e43')
    # Adjust bbox height for DJ to make rows shorter
    if movement_name == "DJ":
        bbox_height = 0.4  # Shorter height for DJ
    else:
        bbox_height = 0.4  # Full height for other movements
    table = ax.table(cellText=table_data[1:], colLabels=table_data[0],
                    cellLoc='center', loc='center',
                    colWidths=[0.12, 0.12, 0.12, 0.12],  # All columns same size as Ideal Values column
                    bbox=[0.1, 0.3, 0.8, bbox_height])  # Adjustable height based on movement
    
    # Style the table
    table.auto_set_font_size(False)
    table.set_fontsize(36)  # Reduced from 38 to 36 (another 2 font sizes smaller)
    
    # Apply colors and styling first
    for i in range(len(table_data[0])):
        # Header row
        cell = table[(0, i)]
        cell.set_facecolor('#373e43')
        if i == 3:  # Ideal Values column header
            cell.set_text_props(weight='bold', color='white', fontsize=36)  # Reduced from 38 to 36
        else:
            cell.set_text_props(weight='bold', color='white')
        cell.set_edgecolor('white')
        cell.set_linewidth(1.5)
    
    # Data rows
    for i, (var, value, tier, thresholds) in enumerate(table_data[1:], 1):
        # Variable name
        cell = table[(i, 0)]
        cell.set_facecolor('#373e43')
        cell.set_text_props(color='white')
        cell.set_edgecolor('white')
        
        # Athlete value
        cell = table[(i, 1)]
        cell.set_facecolor('#373e43')
        cell.set_text_props(color='white')
        cell.set_edgecolor('white')
        
        # Tier cell with color (pastel with opacity, text stays white)
        cell = table[(i, 2)]
        tier_color = color_map.get(tier, (0.22, 0.24, 0.27, 0.3))  # Default to pastel #373e43 with opacity
        cell.set_facecolor(tier_color)
        # Text always white at full opacity
        cell.set_text_props(color='white', weight='bold')
        cell.set_edgecolor('white')
        
        # Elite/Average thresholds column
        cell = table[(i, 3)]
        cell.set_facecolor('#373e43')
        cell.set_text_props(color='white', fontsize=36)  # Reduced from 38 to 36 (another 2 font sizes smaller)
        cell.set_edgecolor('white')
    
    # Set all cell edges
    for key, cell in table.get_celld().items():
        cell.set_edgecolor('white')
        cell.set_linewidth(1)
    
    # Apply scaling AFTER all styling - use different scaling for DJ vs other movements
    # Reduce vertical scale to have less empty space around words
    if movement_name == "DJ":
        # DJ-specific scaling - make rows less tall
        table.scale(1, 0.7)  # DJ-specific scaling (smaller value = shorter rows)
    else:
        table.scale(1, 0.7)  # Same scaling for CMJ and PPU (can be adjusted separately if needed)

# ------------------------------------------------
# SLV Performance Table (shows L: and R: values)
# ------------------------------------------------
def slv_performance_table(ax, left_df, right_df, movement_name):
    """Create a table showing SLV variables with L: and R: values"""
    
    # Get best trials
    if not left_df.empty:
        left_best = left_df.iloc[left_df["PP_FORCEPLATE"].argmax()]
    else:
        left_best = None
    if not right_df.empty:
        right_best = right_df.iloc[right_df["PP_FORCEPLATE"].argmax()]
    else:
        right_best = None
    
    # Helper function to format numbers with significant figures
    def format_sigfig(value, sigfigs):
        """Format a number with specified significant figures"""
        if value == 0:
            return "0"
        # Calculate the order of magnitude
        import math
        order = math.floor(math.log10(abs(value)))
        # Round to the appropriate number of significant figures
        rounded = round(value, sigfigs - 1 - order)
        # Format to avoid scientific notation for reasonable ranges
        if abs(rounded) >= 1:
            # For numbers >= 1, use integer formatting if no decimals needed
            if rounded == int(rounded):
                return str(int(rounded))
            # Otherwise format with appropriate decimal places
            decimal_places = max(0, sigfigs - len(str(int(abs(rounded)))))
            format_str = "{:." + str(decimal_places) + "f}"
            return format_str.format(rounded).rstrip('0').rstrip('.')
        else:
            # For numbers < 1, use decimal formatting
            decimal_places = sigfigs - 1 - order
            format_str = "{:." + str(decimal_places) + "f}"
            return format_str.format(rounded).rstrip('0').rstrip('.')
    
    # Get threshold values function (same as regular performance table)
    def get_threshold_text(var_type, movement_name):
        """Get elite/typical threshold values only, based on movement type"""
        if var_type == 'rpd':
            elite_rpd = {"SLV": 18212}
            elite_min = elite_rpd.get(movement_name, 25000)
            return f">{format_sigfig(elite_min, 4)}"  # 4 significant figures
        elif var_type == 'kurtosis':
            # Format with 3 significant figures
            typical_ranges_raw = {"SLV": (-1.02, -0.19)}
            if movement_name in typical_ranges_raw:
                low, high = typical_ranges_raw[movement_name]
                low_str = format_sigfig(low, 3)
                high_str = format_sigfig(high, 3)
                return f"{low_str} to {high_str}"
            return "-0.4 to 0.5"
        elif var_type == 'auc':
            elite_auc = {"SLV": 1027.56}
            elite_min = elite_auc.get(movement_name, 1100)
            return f">{format_sigfig(elite_min, 4)}"  # 4 significant figures
        return ""
    
    # Define thresholds and tier names (same as regular performance table)
    rpd_thresholds = {
        'Low': (float('-inf'), 8400),
        'Below Average': (8400, 12280),
        'Average': (12280, 17846),
        'High': (17846, 25000),
        'Elite': (25000, float('inf'))
    }
    
    kurtosis_thresholds = {
        'Very Flat': (float('-inf'), -1.3),
        'Moderately Flat': (-1.3, -0.4),
        'Typical': (-0.4, 0.5),
        'Spiky': (0.5, float('inf'))
    }
    
    auc_thresholds = {
        'Low': (float('-inf'), 366),
        'Below Average': (366, 513),
        'Average': (513, 773),
        'High': (773, 1100),
        'Elite': (1100, float('inf'))
    }
    
    # Color mapping - pastel colors with reduced opacity for boxes, text stays white
    # Convert hex to RGB and make pastel (lighter) versions with opacity
    def hex_to_pastel_rgba(hex_color, alpha=0.3):
        """Convert hex color to pastel RGBA with opacity"""
        hex_color = hex_color.lstrip('#')
        r = int(hex_color[0:2], 16) / 255.0
        g = int(hex_color[2:4], 16) / 255.0
        b = int(hex_color[4:6], 16) / 255.0
        # Make pastel by blending with white (lighter, softer colors)
        white = 1.0
        r = r * 0.5 + white * 0.5
        g = g * 0.5 + white * 0.5
        b = b * 0.5 + white * 0.5
        return (r, g, b, alpha)
    
    color_map = {
        'Elite': hex_to_pastel_rgba('#00ff00', 0.3),  # pastel green with opacity
        'High': hex_to_pastel_rgba('#90ee90', 0.3),   # pastel light green with opacity
        'Average': hex_to_pastel_rgba('#808080', 0.3), # pastel gray with opacity
        'Below Average': hex_to_pastel_rgba('#ffff00', 0.3), # pastel yellow with opacity
        'Low': hex_to_pastel_rgba('#d62728', 0.3),    # pastel red with opacity
        'Very Flat': hex_to_pastel_rgba('#d62728', 0.3),
        'Moderately Flat': hex_to_pastel_rgba('#ffff00', 0.3),
        'Typical': hex_to_pastel_rgba('#808080', 0.3),
        'Spiky': hex_to_pastel_rgba('#90ee90', 0.3)
    }
    
    def get_tier(value, thresholds):
        """Determine tier based on value and thresholds"""
        for tier, (min_val, max_val) in thresholds.items():
            if min_val <= value < max_val:
                return tier
        return 'Unknown'
    
    # Get values from left and right
    rpd_left = left_best.get('rpd_max_w_per_s', 0) if left_best is not None else 0
    rpd_right = right_best.get('rpd_max_w_per_s', 0) if right_best is not None else 0
    kurtosis_left = left_best.get('kurtosis', 0) if left_best is not None else 0
    kurtosis_right = right_best.get('kurtosis', 0) if right_best is not None else 0
    auc_left = left_best.get('auc_j', 0) if left_best is not None else 0
    auc_right = right_best.get('auc_j', 0) if right_best is not None else 0
    
    # Determine tiers for left and right
    rpd_tier_left = get_tier(rpd_left, rpd_thresholds) if left_best is not None else 'N/A'
    rpd_tier_right = get_tier(rpd_right, rpd_thresholds) if right_best is not None else 'N/A'
    kurtosis_tier_left = get_tier(kurtosis_left, kurtosis_thresholds) if left_best is not None else 'N/A'
    kurtosis_tier_right = get_tier(kurtosis_right, kurtosis_thresholds) if right_best is not None else 'N/A'
    auc_tier_left = get_tier(auc_left, auc_thresholds) if left_best is not None else 'N/A'
    auc_tier_right = get_tier(auc_right, auc_thresholds) if right_best is not None else 'N/A'
    
    # Create table data with L: and R: values vertically aligned - using significant figures
    table_data = [
        ['Variable', 'Athlete Value', 'Tier / Threshold', 'Ideal Values'],
        ['Max RPD (W/s)', f'L: {format_sigfig(rpd_left, 4)}\nR: {format_sigfig(rpd_right, 4)}', f'L: {rpd_tier_left}\nR: {rpd_tier_right}', get_threshold_text('rpd', movement_name)],  # 4 significant figures
        ['Kurtosis', f'L: {format_sigfig(kurtosis_left, 3)}\nR: {format_sigfig(kurtosis_right, 3)}', f'L: {kurtosis_tier_left}\nR: {kurtosis_tier_right}', get_threshold_text('kurtosis', movement_name)],  # 3 significant figures
        ['Work (AUC)', f'L: {format_sigfig(auc_left, 4)}\nR: {format_sigfig(auc_right, 4)}', f'L: {auc_tier_left}\nR: {auc_tier_right}', get_threshold_text('auc', movement_name)]  # 4 significant figures
    ]
    
    # Create table - same size as other pages
    ax.axis('off')
    ax.set_facecolor('#373e43')
    table = ax.table(cellText=table_data[1:], colLabels=table_data[0],
                    cellLoc='center', loc='center',
                    colWidths=[0.12, 0.12, 0.16, 0.12],  # Made tier/threshold column (index 2) wider: 0.12 -> 0.16
                    bbox=[0.1, 0, 0.8, 1])
    
    # Style the table
    table.auto_set_font_size(False)
    table.set_fontsize(35)  # Reduced from 40 to 35 for column titles
    
    # Apply colors and styling
    for i in range(len(table_data[0])):
        cell = table[(0, i)]
        cell.set_facecolor('#373e43')
        if i == 3:
            cell.set_text_props(weight='bold', color='white', fontsize=35)  # Reduced from 40
        else:
            cell.set_text_props(weight='bold', color='white', fontsize=35)  # Reduced from default
        cell.set_edgecolor('white')
        cell.set_linewidth(1.5)
    
    # Data rows - apply tier colors to tier column
    for i in range(1, len(table_data)):
        for j in range(len(table_data[0])):
            cell = table[(i, j)]
            cell.set_facecolor('#373e43')
            
            # For tier column (j == 2), apply colors based on tiers
            if j == 2:
                # Extract left and right tiers from the cell text
                cell_text = table_data[i][j]
                if 'L:' in cell_text and 'R:' in cell_text:
                    # Get left tier (first tier after "L: ")
                    left_tier = cell_text.split('L: ')[1].split('\n')[0] if 'L:' in cell_text else ''
                    # Get right tier (after "R: ")
                    right_tier = cell_text.split('R: ')[1] if 'R:' in cell_text else ''
                    
                    # For now, use the left tier color (or we could use a gradient)
                    tier_color = color_map.get(left_tier, (0.22, 0.24, 0.27, 0.3))  # Default to pastel #373e43 with opacity
                    cell.set_facecolor(tier_color)
                    # Text always white at full opacity
                    cell.set_text_props(color='white', weight='bold', fontsize=35)
                else:
                    cell.set_text_props(color='white', fontsize=35)
            elif j == 3:
                cell.set_text_props(color='white', fontsize=35)
            else:
                cell.set_text_props(color='white', fontsize=35)
            cell.set_edgecolor('white')
    
    # Set all cell edges
    for key, cell in table.get_celld().items():
        cell.set_edgecolor('white')
        cell.set_linewidth(1)
    
    # Apply scaling - same as other pages
    table.scale(1, 0.7)

# ------------------------------------------------
# Power Curve (synthetic normalized)
# ------------------------------------------------
def power_curve(ax, df, pop_df=None):
    # synthesize a normalized curve using AUC & PP
    t = np.linspace(0, 1, 200)
    curves = []
    for _, r in df.iterrows():
        shape = np.exp(-((t - 0.35) ** 2) / (0.06)) * r["PP_FORCEPLATE"]
        curves.append(shape)

    # Plot individual trial curves as dotted lines
    for c in curves:
        ax.plot(t, c, color="white", linewidth=1, alpha=0.3, linestyle='--')

    # Calculate and plot mean power curve
    mean_curve = np.mean(curves, axis=0)
    ax.plot(t, mean_curve, color="#2c99d4", linewidth=2.5, label='Mean Power')
    
    # Add straight line from 10% to 90% of power along the curve
    max_power = np.max(mean_curve)
    power_10 = 0.1 * max_power
    power_90 = 0.9 * max_power
    
    # Find time points where curve reaches 10% and 90% of max power
    # Find first point where curve crosses 10% (rising phase)
    idx_10 = np.where(mean_curve >= power_10)[0]
    idx_90 = np.where(mean_curve >= power_90)[0]
    
    if len(idx_10) > 0 and len(idx_90) > 0:
        t_10 = t[idx_10[0]]  # First time point at 10%
        t_90 = t[idx_90[0]]  # First time point at 90%
        p_10 = mean_curve[idx_10[0]]
        p_90 = mean_curve[idx_90[0]]
        
        # Draw straight line between these two points (darker blue, solid)
        ax.plot([t_10, t_90], [p_10, p_90], color="#1a5f8a", linewidth=2, linestyle='-', alpha=0.7, label='10-90% rise')
    
    # Add single legend at the end (remove any existing legends first, after all plots)
    handles, labels = ax.get_legend_handles_labels()
    if ax.get_legend() is not None:
        ax.get_legend().remove()
    # Only create legend if we have labeled items
    if len(handles) > 0:
        ax.legend(handles, labels, loc='upper right', facecolor='#373e43', edgecolor='white', labelcolor='white', fontsize=27)

    ax.set_title("Power Curve", color='white', fontsize=45)  # Increased from 30 to 45
    ax.set_xlabel("Normalized Time", color='white', fontsize=27)
    ax.set_ylabel("Power (W)", color='white', fontsize=27)
    ax.tick_params(colors='white', labelsize=24)
    ax.spines['bottom'].set_color('white')
    ax.spines['top'].set_color('white')
    ax.spines['left'].set_color('white')
    ax.spines['right'].set_color('white')
    ax.grid(True, color='white', alpha=0.2)
    ax.set_facecolor('#373e43')

# ------------------------------------------------
# SLV Power Curve (shows left in dark blue and right in red)
# ------------------------------------------------
def slv_power_curve(ax, left_df, right_df, pop_df=None):
    """Power curve for SLV showing left (dark blue) and right (red) curves"""
    t = np.linspace(0, 1, 200)
    
    # Left leg curves (dark blue)
    if not left_df.empty:
        left_curves = []
        for _, r in left_df.iterrows():
            shape = np.exp(-((t - 0.35) ** 2) / (0.06)) * r["PP_FORCEPLATE"]
            left_curves.append(shape)
            ax.plot(t, shape, color="#1a5f8a", linewidth=1, alpha=0.3, linestyle='--')
        
        if len(left_curves) > 0:
            left_mean = np.mean(left_curves, axis=0)
            ax.plot(t, left_mean, color="#1a5f8a", linewidth=2.5, label='Left Mean')
    
    # Right leg curves (red)
    if not right_df.empty:
        right_curves = []
        for _, r in right_df.iterrows():
            shape = np.exp(-((t - 0.35) ** 2) / (0.06)) * r["PP_FORCEPLATE"]
            right_curves.append(shape)
            ax.plot(t, shape, color="#d62728", linewidth=1, alpha=0.3, linestyle='--')
        
        if len(right_curves) > 0:
            right_mean = np.mean(right_curves, axis=0)
            ax.plot(t, right_mean, color="#d62728", linewidth=2.5, label='Right Mean')
    
    # Add legend
    handles, labels = ax.get_legend_handles_labels()
    if len(handles) > 0:
        ax.legend(handles, labels, loc='upper right', facecolor='#373e43', edgecolor='white', labelcolor='white', fontsize=27)

    ax.set_title("Power Curve", color='white', fontsize=45)  # Increased from 30 to 45
    ax.set_xlabel("Normalized Time", color='white', fontsize=27)
    ax.set_ylabel("Power (W)", color='white', fontsize=27)
    ax.tick_params(colors='white', labelsize=24)
    ax.spines['bottom'].set_color('white')
    ax.spines['top'].set_color('white')
    ax.spines['left'].set_color('white')
    ax.spines['right'].set_color('white')
    ax.grid(True, color='white', alpha=0.2)
    ax.set_facecolor('#373e43')

# ------------------------------------------------
# F–V scatter
# ------------------------------------------------
def fv_scatter(ax, df, population):
    # Plot all athletes from population as blue dots
    pop_force = population["Force_at_PP"].dropna()
    pop_vel = population["Vel_at_PP"].dropna()
    # Align the arrays by index
    common_idx = pop_force.index.intersection(pop_vel.index)
    ax.scatter(pop_force.loc[common_idx], pop_vel.loc[common_idx], 
              s=480, color='#2c99d4', alpha=0.4, label='All Athletes')  # Half the size of athlete dot (960/2 = 480)
    
    # Calculate average force and velocity from population
    avg_force = pop_force.mean()
    avg_vel = pop_vel.mean()
    
    # Plot current athlete's mean F/V as single blue dot
    if len(df) > 0:
        athlete_mean_force = df["Force_at_PP"].mean()
        athlete_mean_vel = df["Vel_at_PP"].mean()
        ax.scatter([athlete_mean_force], [athlete_mean_vel], 
                  s=960, color='#2c99d4', alpha=0.9, label='Current Athlete', zorder=5, marker='o', edgecolors='white', linewidths=1.5)  # Doubled from 480 to 960
    
    # Add reference lines for average force and velocity (after scatter so they appear on top)
    # Vertical line for average force - gray
    ax.axvline(x=avg_force, color='gray', linestyle='--', linewidth=6.0, alpha=0.7, zorder=3)  # Doubled from 3.0 to 6.0
    # Horizontal line for average velocity - gray
    ax.axhline(y=avg_vel, color='gray', linestyle='--', linewidth=6.0, alpha=0.7, zorder=3)  # Doubled from 3.0 to 6.0

    avg_pp = df["PP_FORCEPLATE"].mean()
    pct = percentileofscore(population["PP_FORCEPLATE"].dropna(), avg_pp)

    ax.text(0.98, 0.98,
            f"PP Percentile:\n{pct:.0f}th",
            transform=ax.transAxes,
            ha="right", va="top",
            color='white',
            fontsize=27)  # Same size as axis titles, no box

    ax.set_xlabel("Force @ PP (N)", color='white', fontsize=27)
    ax.set_ylabel("Vel @ PP (m/s)", color='white', fontsize=27)
    ax.set_title("Force–Velocity Scatter", color='white', fontsize=45)  # Increased from 30 to 45
    ax.tick_params(colors='white', labelsize=24)
    ax.spines['bottom'].set_color('white')
    ax.spines['top'].set_color('white')
    ax.spines['left'].set_color('white')
    ax.spines['right'].set_color('white')
    ax.grid(True, color='white', alpha=0.2)
    ax.set_facecolor('#373e43')

# ------------------------------------------------
# SLV F-V Scatter (shows left in dark blue and right in red)
# ------------------------------------------------
def slv_fv_scatter(ax, left_df, right_df, population):
    """FV scatter for SLV showing left (dark blue) and right (red) dots"""
    # Plot all athletes from population as blue dots
    pop_force = population["Force_at_PP"].dropna()
    pop_vel = population["Vel_at_PP"].dropna()
    common_idx = pop_force.index.intersection(pop_vel.index)
    ax.scatter(pop_force.loc[common_idx], pop_vel.loc[common_idx], 
              s=480, color='#2c99d4', alpha=0.4, label='All Athletes')  # Half the size of athlete dot (960/2 = 480)
    
    # Calculate average force and velocity from population
    avg_force = pop_force.mean()
    avg_vel = pop_vel.mean()
    
    # Plot left leg mean F/V as dark blue dot
    if not left_df.empty and len(left_df) > 0:
        left_mean_force = left_df["Force_at_PP"].mean()
        left_mean_vel = left_df["Vel_at_PP"].mean()
        ax.scatter([left_mean_force], [left_mean_vel], 
                  s=960, color='#1a5f8a', alpha=0.9, label='Left Mean', zorder=5, marker='o', edgecolors='white', linewidths=1.5)  # Doubled from 480 to 960
    
    # Plot right leg mean F/V as red dot
    if not right_df.empty and len(right_df) > 0:
        right_mean_force = right_df["Force_at_PP"].mean()
        right_mean_vel = right_df["Vel_at_PP"].mean()
        ax.scatter([right_mean_force], [right_mean_vel], 
                  s=960, color='#d62728', alpha=0.9, label='Right Mean', zorder=5, marker='o', edgecolors='white', linewidths=1.5)  # Doubled from 480 to 960
    
    # Add reference lines for average force and velocity - gray
    ax.axvline(x=avg_force, color='gray', linestyle='--', linewidth=6.0, alpha=0.7, zorder=3)  # Doubled from 3.0 to 6.0
    ax.axhline(y=avg_vel, color='gray', linestyle='--', linewidth=6.0, alpha=0.7, zorder=3)  # Doubled from 3.0 to 6.0
    
    ax.set_xlabel("Force @ PP (N)", color='white', fontsize=27)
    ax.set_ylabel("Vel @ PP (m/s)", color='white', fontsize=27)
    ax.set_title("Force–Velocity Scatter", color='white', fontsize=45)  # Increased from 30 to 45
    ax.tick_params(colors='white', labelsize=24)
    ax.spines['bottom'].set_color('white')
    ax.spines['top'].set_color('white')
    ax.spines['left'].set_color('white')
    ax.spines['right'].set_color('white')
    ax.grid(True, color='white', alpha=0.2)
    ax.set_facecolor('#373e43')

# ------------------------------------------------
# Force & Velocity Progress Rings
# ------------------------------------------------
def joint_radial(ax, row, pop_df, metric=None):
    """
    Draw a single progress circle for Force or Velocity at Peak Power.
    
    Args:
        ax: Matplotlib axes
        row: DataFrame row with athlete data
        pop_df: Population DataFrame for percentile calculation
        metric: "force" or "velocity" - if None, draws both (for CMJ/PPU backward compatibility)
    """
    from matplotlib.patches import Arc, Circle
    from scipy.stats import percentileofscore

    # --- Extract metrics ---
    force_at_pp = row["Force_at_PP"]
    vel_at_pp = row["Vel_at_PP"]

    # --- Percentiles (0 to 1) ---
    force_pct = percentileofscore(pop_df["Force_at_PP"].dropna(), force_at_pp) / 100
    vel_pct = percentileofscore(pop_df["Vel_at_PP"].dropna(), vel_at_pp) / 100
    force_pct = max(0, min(1, force_pct))
    vel_pct = max(0, min(1, vel_pct))

    # --- Axes formatting ---
    ax.set_facecolor('#373e43')
    ax.set_aspect('equal')
    ax.axis("off")

    # --- FIXED CIRCLE SIZE (px). Adjust this to make circles bigger/smaller ---
    ring_radius = 100
    lw_base = 28
    lw_progress = 56
    fs_value = 54
    fs_unit = 33

    # --- Single circle mode (for DJ with metric parameter) ---
    if metric is not None:
        if metric == "force":
            pct = force_pct
            val = force_at_pp
            unit = "N"
        elif metric == "velocity":
            pct = vel_pct
            val = vel_at_pp
            unit = "m/s"
        else:
            raise ValueError(f"metric must be 'force' or 'velocity', got '{metric}'")
        
        # Center the circle in the axes - no axes limits, size controlled by radius_data and axes physical size
        # The visual size is controlled by: 1) axes physical size (circle_width/circle_height when created)
        # and 2) radius_data value below. To make circles bigger, increase radius_data or make axes bigger.
        cx, cy = 0.5, 0.5  # Center at 0.5, 0.5 in normalized 0-1 coordinates
        
        # Use a fixed radius in normalized coordinates (0-1 space)
        # Increase this value to make circles bigger - can go up to ~0.48 before hitting edges
        radius_data = 0.45  # 45% of axes size - increase this to make circles bigger
        
        # Draw single circle
        # Background ring
        ax.add_patch(
            Circle((cx, cy), radius_data,
                   fill=False, color='#2c99d4', linewidth=lw_base, alpha=0.25)
        )

        # Progress arc - starts at top (90°) and goes counterclockwise
        if pct > 0:
            # Start at 90° (top) and go counterclockwise (positive angles)
            theta_start = 90
            theta_end = 90 + (pct * 360)
            arc = Arc((cx, cy),
                      radius_data * 2,
                      radius_data * 2,
                      angle=0,
                      theta1=theta_start,
                      theta2=theta_end,
                      color='#2c99d4',
                      linewidth=lw_progress,
                      alpha=0.9)
            ax.add_patch(arc)

        # Percentile number in center
        ax.text(cx, cy + 0.06,
                f"{int(pct*100)}",
                color="white",
                fontsize=56,
                ha="center", va="center", weight="bold")

        # Value + unit inside circle, below percentile number
        ax.text(cx, cy - 0.06,
                f"{val:.1f} {unit}",
                color="white",
                fontsize=32,
                ha="center", va="center")
    
    # --- Dual circle mode (for CMJ/PPU backward compatibility) ---
    else:
        fig = ax.figure
        horizontal = 0.4
        centers = [(-horizontal, 0), (horizontal, 0)]
        ax.set_xlim(-1, 1)
        ax.set_ylim(-1, 1)

        # Title inside axes
        ax.set_title("@ Peak Power", color="white", fontsize=fs_unit*2, pad=35)

        labels = ["Force", "Velocity"]
        values = [force_at_pp, vel_at_pp]
        units = ["N", "m/s"]
        pcts = [force_pct, vel_pct]

        # --- DRAW RINGS ---
        for (label, pct, val, unit, (cx, cy)) in zip(labels, pcts, values, units, centers):

            # Background ring
            ax.add_patch(
                Circle((cx, cy), ring_radius,
                       fill=False, color='#2c99d4', linewidth=lw_base, alpha=0.25)
            )

            # Progress arc - starts at top (90°) and goes counterclockwise
            if pct > 0:
                # Start at 90° (top) and go counterclockwise (positive angles)
                theta_start = 90
                theta_end = 90 + (pct * 360)
                arc = Arc((cx, cy),
                          ring_radius * 2,
                          ring_radius * 2,
                          angle=0,
                          theta1=theta_start,
                          theta2=theta_end,
                          color='#2c99d4',
                          linewidth=lw_progress,
                          alpha=0.9)
                ax.add_patch(arc)

            # Percentile number in center (moved up slightly)
            ax.text(cx, cy + 0.04,
                    f"{int(pct*100)}",
                    color="white",
                    fontsize=fs_value,
                    ha="center", va="center", weight="bold")

            # Value + unit inside circle, below percentile number
            ax.text(cx, cy - 0.04,
                    f"{val:.1f} {unit}",
                    color="white",
                    fontsize=fs_unit,
                    ha="center", va="center")

# ------------------------------------------------
# SLV Joint Radial (with custom title and smaller percentile text)
# ------------------------------------------------
def slv_joint_radial(ax, row, pop_df, title="Left Leg"):
    from matplotlib.patches import Arc, Circle
    
    # Calculate percentiles for Force and Velocity at Peak Power
    force_at_pp = row["Force_at_PP"]
    vel_at_pp = row["Vel_at_PP"]
    
    # Calculate percentiles (0-100 scale, convert to 0-1 for ring display)
    force_percentile = percentileofscore(pop_df["Force_at_PP"].dropna(), force_at_pp) / 100.0
    vel_percentile = percentileofscore(pop_df["Vel_at_PP"].dropna(), vel_at_pp) / 100.0
    
    # Ensure percentiles are between 0 and 1
    force_percentile = max(0, min(1, force_percentile))
    vel_percentile = max(0, min(1, vel_percentile))
    
    ax.set_aspect('equal')
    ax.set_facecolor('#373e43')
    ax.axis('off')
    
    # Scale factor for text and linewidth (keep original sizing)
    scale_factor = 2.6
    
    # Calculate circle size based on axes dimensions
    fig = ax.figure
    bbox = ax.get_position()
    fig_width = fig.get_figwidth() * fig.dpi
    fig_height = fig.get_figheight() * fig.dpi
    ax_width = bbox.width * fig_width
    ax_height = bbox.height * fig_height
    
    # Use 45% of the smaller dimension for ring radius
    ring_radius = min(ax_width, ax_height) * 0.45
    
    # Vertical spacing between circles (35% of dimension from center)
    vertical_spacing = min(ax_width, ax_height) * 0.35
    centers = [(0, vertical_spacing), (0, -vertical_spacing)]  # Force (top), Velocity (bottom)
    
    # Set axis limits to accommodate the circles
    margin = ring_radius * 0.1
    ax.set_xlim(-ring_radius - margin, ring_radius + margin)
    ax.set_ylim(-ring_radius - vertical_spacing - margin, ring_radius + vertical_spacing + margin)
    labels = ["Force", "Velocity"]
    percentiles = [force_percentile, vel_percentile]
    values = [force_at_pp, vel_at_pp]
    units = ["N", "m/s"]
    
    for i, (label, pct, val, unit, (cx, cy)) in enumerate(zip(labels, percentiles, values, units, centers)):
        # Background circle (full ring)
        bg_circle = Circle((cx, cy), ring_radius, fill=False, color='#2c99d4', 
                          linewidth=6 * scale_factor, alpha=0.2)
        ax.add_patch(bg_circle)
        
        # Progress arc - starts at top (90°) and goes counterclockwise
        if pct > 0:
            # Start at 90° (top) and go counterclockwise (positive angles)
            theta_start = 90
            theta_end = 90 + (360 * pct)
            arc = Arc((cx, cy), ring_radius*2, ring_radius*2, 
                     angle=0, theta1=theta_start, theta2=theta_end,
                     color='#2c99d4', linewidth=12 * scale_factor, alpha=0.9)
            ax.add_patch(arc)
        
        # Label above ring
        ax.text(cx, cy + ring_radius + 0.2 * scale_factor, label, 
               color='white', fontsize=36 * scale_factor, ha='center', va='bottom', fontweight='bold')
        
        # Percentile text in center (moved up slightly)
        ax.text(cx, cy + 0.06 * scale_factor, f'{int(pct*100)}', 
               color='white', fontsize=34 * scale_factor, ha='center', va='center', fontweight='bold')
        
        # Value inside circle, below percentile number
        ax.text(cx, cy - 0.06 * scale_factor, f'{val:.1f} {unit}', 
               color='white', fontsize=28 * scale_factor, ha='center', va='center')
    
    # Custom title (Left Leg or Right Leg)
    ax.set_title(title, color='white', fontsize=33 * scale_factor, pad=45 * scale_factor)

# ------------------------------------------------
# Bar comparisons (Histogram-style with frequency)
# ------------------------------------------------
def bar_graph(ax, metric, df_ath, population):
    # Check if metric exists in dataframes
    if metric not in population.columns or metric not in df_ath.columns:
        ax.text(0.5, 0.5, f"Metric '{metric}' not found", 
                transform=ax.transAxes, ha='center', va='center', color='white', fontsize=20)
        ax.set_facecolor('#373e43')
        return
    
    # Get all population values
    pop_values = population[metric].dropna().values
    
    # Get athlete's values
    athlete_values = df_ath[metric].dropna().values
    athlete_mean = athlete_values.mean() if len(athlete_values) > 0 else 0
    athlete_max = athlete_values.max() if len(athlete_values) > 0 else 0
    
    # Calculate mean percentile
    mean_pct = percentileofscore(pop_values, athlete_mean) if len(pop_values) > 0 else 0
    
    # Create histogram bins - use automatic binning but ensure reasonable number
    if len(pop_values) > 0:
        # Determine appropriate number of bins (not too many, not too few)
        n_bins = min(30, max(10, int(np.sqrt(len(pop_values)))))
        
        # Create histogram
        counts, bin_edges = np.histogram(pop_values, bins=n_bins)
        bin_centers = (bin_edges[:-1] + bin_edges[1:]) / 2
        
        # Plot histogram bars - blue
        bars = ax.bar(bin_centers, counts, width=(bin_edges[1] - bin_edges[0]) * 0.8, 
                     color='#2c99d4', alpha=0.6, edgecolor='#2c99d4', linewidth=0.5)
        
        # Add vertical lines for athlete mean and max
        ax.axvline(x=athlete_mean, color='pink', linestyle='--', linewidth=7.5, 
                  label=f'Mean: {athlete_mean:.1f}', alpha=0.9)
        ax.axvline(x=athlete_max, color='#d62728', linestyle='--', linewidth=7.5, 
                  label=f'Max: {athlete_max:.1f}', alpha=0.9)
    
    # Set labels and styling - use display label
    display_label = METRIC_LABELS.get(metric, metric)
    ax.set_title(display_label, color='white', fontsize=48, pad=8)  # Font size 48, pad=8 adds space between title and graph
    # Removed xlabel 'Value'
    ax.set_ylabel('Frequency', color='white', fontsize=30)  # Increased from 27 to 30
    ax.tick_params(colors='white', labelsize=24)  # Increased from 21 to 24
    ax.spines['bottom'].set_color('white')
    ax.spines['top'].set_color('white')
    ax.spines['left'].set_color('white')
    ax.spines['right'].set_color('white')
    ax.grid(True, color='white', alpha=0.2, axis='y')
    ax.set_facecolor('#373e43')
    
    # Add summary box at the top right corner - bigger text and less opaque
    summary_text = f'Mean: {athlete_mean:.1f} ({int(mean_pct)}th %ile)\nMax: {athlete_max:.1f}'
    ax.text(0.98, 0.98, summary_text, transform=ax.transAxes,
            ha='right', va='top', color='white', fontsize=30,  # Increased from 24 to 30
            bbox=dict(boxstyle='round', facecolor='black', edgecolor='white', alpha=0.5))  # Reduced from 0.8 to 0.5

# ------------------------------------------------
# SLV Bar Graph (shows dark blue line for mean left, red line for mean right)
# ------------------------------------------------
def slv_bar_graph(ax, metric, left_df, right_df, population):
    """Bar graph for SLV showing dark blue line for mean left, red line for mean right"""
    # Check if metric exists
    if metric not in population.columns:
        ax.text(0.5, 0.5, f"Metric '{metric}' not found", 
                transform=ax.transAxes, ha='center', va='center', color='white', fontsize=20)
        ax.set_facecolor('#373e43')
        return
    
    # Get all population values
    pop_values = population[metric].dropna().values
    
    # Get left and right means
    left_mean = left_df[metric].mean() if not left_df.empty and metric in left_df.columns else 0
    right_mean = right_df[metric].mean() if not right_df.empty and metric in right_df.columns else 0
    
    # Create histogram
    if len(pop_values) > 0:
        n_bins = min(30, max(10, int(np.sqrt(len(pop_values)))))
        counts, bin_edges = np.histogram(pop_values, bins=n_bins)
        bin_centers = (bin_edges[:-1] + bin_edges[1:]) / 2
        
        # Plot histogram bars - blue
        bars = ax.bar(bin_centers, counts, width=(bin_edges[1] - bin_edges[0]) * 0.8, 
                     color='#2c99d4', alpha=0.6, edgecolor='#2c99d4', linewidth=0.5)
        
        # Add vertical lines for left mean (dark blue) and right mean (red)
        if left_mean > 0:
            ax.axvline(x=left_mean, color='#1a5f8a', linestyle='--', linewidth=7.5, 
                      label=f'Left Mean: {left_mean:.1f}', alpha=0.9)
        if right_mean > 0:
            ax.axvline(x=right_mean, color='#d62728', linestyle='--', linewidth=7.5, 
                      label=f'Right Mean: {right_mean:.1f}', alpha=0.9)
    
    # Set labels and styling - use display label
    display_label = METRIC_LABELS.get(metric, metric)
    ax.set_title(display_label, color='white', fontsize=48, pad=8)  # Font size 48, pad=8 adds space between title and graph
    # Removed xlabel 'Value'
    ax.set_ylabel('Frequency', color='white', fontsize=30)  # Increased from 27 to 30
    ax.tick_params(colors='white', labelsize=24)  # Increased from 21 to 24
    ax.spines['bottom'].set_color('white')
    ax.spines['top'].set_color('white')
    ax.spines['left'].set_color('white')
    ax.spines['right'].set_color('white')
    ax.grid(True, color='white', alpha=0.2, axis='y')
    ax.set_facecolor('#373e43')
    
    # Add legend if we have lines - bigger text and less opaque
    handles, labels = ax.get_legend_handles_labels()
    if len(handles) > 0:
        ax.legend(handles, labels, loc='upper right', facecolor='black', edgecolor='white', labelcolor='white', fontsize=27, framealpha=0.5)  # Increased fontsize from 21 to 27, changed facecolor to black, added framealpha=0.5 for transparency

# ------------------------------------------------
# Page Builder for One Movement
# ------------------------------------------------
def movement_page(pdf, movement_name, df, pop_df):
    df_ath = df[df["name"] == ATHLETE_NAME]

    if df_ath.empty:
        return

    # Determine bar metrics based on movement type
    if movement_name == "DJ":
        # DJ page layout - 4 bar graphs: JH_IN, PP_W_per_kg, RSI, CT
        dj_bar_metrics = ["JH_IN", "PP_W_per_kg", "RSI", "CT"]
        fig = plt.figure(figsize=(54, 90), facecolor='black')
        fig.patch.set_facecolor('black')
        
        # Top section: JH_IN and PP_W_per_kg bar graphs
        bar_graphs = []
        graph_width = 0.35
        graph_height = 0.16
        
        # Top section boundaries for bar graphs
        top_section_bottom = 0.73
        top_section_height = 0.19
        
        # Calculate horizontal spacing for top section
        top_horizontal_spacing = (1.0 - 2 * graph_width) / 3  # Equal spacing on all sides
        top_vertical_spacing = (top_section_height - graph_height) / 2  # Center vertically
        
        # Create JH_IN and PP_W_per_kg bar graphs in top section
        for i, metric in enumerate(["JH_IN", "PP_W_per_kg"]):
            left_pos = top_horizontal_spacing + i * (graph_width + top_horizontal_spacing)
            bottom_pos = top_section_bottom + top_vertical_spacing
            # [left, bottom, width, height] in figure coordinates
            ax = fig.add_axes([left_pos, bottom_pos, graph_width, graph_height])
            bar_graphs.append(ax)
            bar_graph(ax, metric, df_ath, pop_df)
        
        # Middle section: Radar chart and Power curve
        # Radar chart - left side
        # [left, bottom, width, height] in figure coordinates
        ax_radar = fig.add_axes([0.11, 0.52, 0.29, 0.22], polar=True)
        
        # Power curve - right side
        # [left, bottom, width, height] in figure coordinates
        ax_power = fig.add_axes([0.50, 0.595, 0.43, 0.13])
        
        # Performance table - positioned directly under power curve
        table_width = 0.535
        table_height = 0.10
        table_left = 0.45  # Aligned with power curve left edge
        table_bottom = 0.50  # Positioned directly under power curve
        # [left, bottom, width, height] in figure coordinates
        ax_table = fig.add_axes([table_left, table_bottom, table_width, table_height])
        
        # Middle-bottom section: RSI and CT bar graphs
        section_top = 0.51  # Top line (where radar/table are)
        section_bottom = 0.24  # Bottom line
        section_height = section_top - section_bottom
        
        # Calculate equal horizontal spacing: left margin = spacing between = right margin
        # 2 graphs + 3 equal spaces = 1.0
        horizontal_spacing = (1.0 - 2 * graph_width) / 3  # Equal spacing on all sides
        
        # Calculate vertical spacing for bottom row
        vertical_spacing = (section_height - graph_height) / 2  # Center in available space
        
        # Create RSI and CT bar graphs
        for i, metric in enumerate(["RSI", "CT"]):
            left_pos = horizontal_spacing + i * (graph_width + horizontal_spacing)
            bottom_pos = section_bottom + vertical_spacing + 0.03  # Offset to move graphs up slightly
            # [left, bottom, width, height] in figure coordinates
            ax = fig.add_axes([left_pos, bottom_pos, graph_width, graph_height])
            bar_graphs.append(ax)
            bar_graph(ax, metric, df_ath, pop_df)
        
        # Bottom section: FV scatter and Progress circles
        # FV scatter - left side
        # [left, bottom, width, height] in figure coordinates
        ax_fv = fig.add_axes([0.09, 0.04, 0.50, 0.24])
        
        # Progress Circles Section - right side, vertically aligned
        # Title above both circles
        progress_circles_x = 0.78  # Right side position
        fig.text(progress_circles_x, 0.28, "@ Peak Power", ha='center', va='center',
                 fontsize=48, color='white', fontweight='bold', transform=fig.transFigure)

        # Positions for circles - vertically aligned, Force above Velocity
        circle_width = 0.16
        circle_height = 0.16
        
        # Force circle (top)
        force_y = 0.13
        force_x = progress_circles_x - circle_width/2
        
        # Velocity circle (bottom)
        velocity_y = 0.005
        velocity_x = progress_circles_x - circle_width/2

        # Labels above each circle
        fig.text(0.78, 0.26, "Force", ha='center', va='bottom',
                 fontsize=41, color='white', fontweight='bold', transform=fig.transFigure)

        fig.text(0.78, 0.14, "Velocity", ha='center', va='bottom',
                 fontsize=41, color='white', fontweight='bold', transform=fig.transFigure)

        # Force circle axis (top)
        # [left, bottom, width, height] in figure coordinates
        ax_force = fig.add_axes([force_x, force_y, circle_width, circle_height])
        ax_force.set_aspect('equal')

        # Velocity circle axis (bottom)
        # [left, bottom, width, height] in figure coordinates
        ax_vel = fig.add_axes([velocity_x, velocity_y, circle_width, circle_height])
        ax_vel.set_aspect('equal')
    else:
        # CMJ and PPU page layout - 2 bar graphs
        dj_bar_metrics = None
        fig = plt.figure(figsize=(54, 65), facecolor='black')
        fig.patch.set_facecolor('black')
        
        # Top section: Bar graphs (JH_IN and PP_W_per_kg)
        graph_width = 0.38
        graph_height = 0.22
        bar_graph_bottom = 0.647  # Direct vertical position - adjust this value to move graphs up/down
        
        # Calculate equal horizontal spacing for 2 graphs
        horizontal_spacing = (1.0 - 2 * graph_width) / 3  # Equal spacing on all sides
        
        bar_graphs = []
        for i, metric in enumerate(BAR_METRICS):
            left_pos = horizontal_spacing + i * (graph_width + horizontal_spacing)
            # [left, bottom, width, height] in figure coordinates
            ax = fig.add_axes([left_pos, bar_graph_bottom, graph_width, graph_height])
            bar_graphs.append(ax)
            bar_graph(ax, metric, df_ath, pop_df)
        
        # Middle section: Radar chart and Power curve
        # Radar chart - left side
        # [left, bottom, width, height] in figure coordinates
        ax_radar = fig.add_axes([0.11, 0.38, 0.29, 0.22], polar=True)
        
        # Power curve - right side
        # [left, bottom, width, height] in figure coordinates
        ax_power = fig.add_axes([0.50, 0.45, 0.43, 0.16])
        
        # Performance table - positioned directly under power curve
        table_width = 0.53
        table_height = 0.15
        table_left = 0.45  # Aligned with power curve left edge
        table_bottom = 0.32  # Positioned directly under power curve
        # [left, bottom, width, height] in figure coordinates
        ax_table = fig.add_axes([table_left, table_bottom, table_width, table_height])
        
        # Bottom section: FV scatter and Progress circles
        # FV scatter - left side
        # [left, bottom, width, height] in figure coordinates
        ax_fv = fig.add_axes([0.09, 0.03, 0.50, 0.283])
        
        # Progress Circles Section - right side, vertically aligned
        # Title above both circles
        progress_circles_x = 0.78  # Right side position
        fig.text(progress_circles_x, 0.33, "@ Peak Power", ha='center', va='center',
                 fontsize=48, color='white', fontweight='bold', transform=fig.transFigure)

        # Positions for circles - vertically aligned, Force above Velocity
        circle_width = 0.16
        circle_height = 0.16
        
        # Force circle (top)
        force_y = 0.16
        force_x = progress_circles_x - circle_width/2
        
        # Velocity circle (bottom)
        velocity_y = 0.005
        velocity_x = progress_circles_x - circle_width/2

        # Labels above each circle
        fig.text(0.78, 0.313, "Force", ha='center', va='bottom',
                 fontsize=41, color='white', fontweight='bold', transform=fig.transFigure)

        fig.text(0.78, 0.153, "Velocity", ha='center', va='bottom',
                 fontsize=41, color='white', fontweight='bold', transform=fig.transFigure)

        # Force circle axis (top)
        # [left, bottom, width, height] in figure coordinates
        ax_force = fig.add_axes([force_x, force_y, circle_width, circle_height])
        ax_force.set_aspect('equal')

        # Velocity circle axis (bottom)
        # [left, bottom, width, height] in figure coordinates
        ax_vel = fig.add_axes([velocity_x, velocity_y, circle_width, circle_height])
        ax_vel.set_aspect('equal')

    # pick best trial
    best = df_ath.loc[df_ath["PP_FORCEPLATE"].idxmax()]

    # Create charts
    radar_chart(ax_radar, best, f"{movement_name} Radar", pop_df)
    performance_table(ax_table, best, movement_name)
    
    power_curve(ax_power, df_ath, pop_df)
    fv_scatter(ax_fv, df_ath, pop_df)
    
    # Progress circles - same approach for all movements (separate Force and Velocity)
    if movement_name == "DJ":
        joint_radial(ax_force, best, pop_df, metric="force")
        joint_radial(ax_vel, best, pop_df, metric="velocity")
    else:
        # CMJ and PPU also use separate Force and Velocity circles
        joint_radial(ax_force, best, pop_df, metric="force")
        joint_radial(ax_vel, best, pop_df, metric="velocity")

    # Map movement names to full titles
    title_map = {
        "DJ": "Drop Jump",
        "CMJ": "Counter-Movement Jump",
        "PPU": "Plyo-Pushup"
    }
    full_title = title_map.get(movement_name, movement_name)
    plt.suptitle(full_title, fontsize=70, color='white')
    
    # Add header with vertical bar, name, date, logo, and horizontal line - separate headers for different page types
    if movement_name == "DJ":
        add_header_dj(fig, ATHLETE_NAME, REPORT_DATE)
    else:
        add_header_cmj_ppu(fig, ATHLETE_NAME, REPORT_DATE)
    
    # Add blue line between sections
    if movement_name == "DJ":
        # Line between RSI/CT bar graphs section and bottom section (FV scatter/progress circles)
        # Line positioned at same distance from edge as header line (0.05 to 0.98)
        line2 = Line2D([0.05, 0.98], [0.30, 0.30], transform=fig.transFigure, color='#2c99d4', linewidth=9, zorder=1000)
        fig.add_artist(line2)
    else:
        # CMJ/PPU - line between bar graphs section and bottom section
        # Line positioned at same distance from edge as header line (0.05 to 0.98)
        line2 = Line2D([0.05, 0.98], [0.345, 0.345], transform=fig.transFigure, color='#2c99d4', linewidth=9, zorder=1000)
        fig.add_artist(line2)
    
    # Set all axes backgrounds to #373e43
    for ax in fig.get_axes():
        if hasattr(ax, 'set_facecolor'):
            ax.set_facecolor('#373e43')
    pdf.savefig(fig, facecolor='black', edgecolor='none')
    plt.close(fig)

# ------------------------------------------------
# SLV special page
# ------------------------------------------------
def slv_page(pdf, df, pop_df):
    df_ath = df[df["name"] == ATHLETE_NAME]
    left = df_ath[df_ath["side"] == "Left"]
    right = df_ath[df_ath["side"] == "Right"]

    if df_ath.empty:
        return

    fig = plt.figure(figsize=(54, 65), facecolor='black')
    fig.patch.set_facecolor('black')
    
    # Get best left and right trials separately for progress circles
    best_left = left.iloc[left["PP_FORCEPLATE"].argmax()] if not left.empty and len(left) > 0 else None
    best_right = right.iloc[right["PP_FORCEPLATE"].argmax()] if not right.empty and len(right) > 0 else None
    left_best_row = left.iloc[left["PP_FORCEPLATE"].argmax()] if not left.empty else None
    right_best_row = right.iloc[right["PP_FORCEPLATE"].argmax()] if not right.empty else None
    
    # Top section: Bar graphs
    graph_width = 0.38
    graph_height = 0.22
    bar_graph_bottom = 0.65  # Direct vertical position - adjust this value to move graphs up/down
    
    # Calculate equal horizontal spacing for 2 graphs
    horizontal_spacing = (1.0 - 2 * graph_width) / 3  # Equal spacing on all sides
    
    for i, metric in enumerate(BAR_METRICS):
        left_pos = horizontal_spacing + i * (graph_width + horizontal_spacing)
        # [left, bottom, width, height] in figure coordinates
        ax = fig.add_axes([left_pos, bar_graph_bottom, graph_width, graph_height])
        slv_bar_graph(ax, metric, left, right, pop_df)
    
    # Middle section: Radar chart and Power curve
    # Radar chart - left side
    # [left, bottom, width, height] in figure coordinates
    ax_radar = fig.add_axes([0.11, 0.38, 0.29, 0.22], polar=True)
    
    # Power curve - right side
    # [left, bottom, width, height] in figure coordinates
    ax_power = fig.add_axes([0.50, 0.495, 0.43, 0.13])
    
    # Performance table - positioned directly under power curve
    table_width = 0.542
    table_height = 0.10
    table_left = 0.45  # Aligned with power curve left edge
    table_bottom = 0.37  # Positioned directly under power curve
    # [left, bottom, width, height] in figure coordinates
    ax_table = fig.add_axes([table_left, table_bottom, table_width, table_height])
    
    # Bottom section: FV scatter and Progress circles
    # FV scatter - left side
    # [left, bottom, width, height] in figure coordinates
    ax_fv = fig.add_axes([0.09, 0.04, 0.49, 0.272])
    
    # Progress circles for both left and right legs - right side, vertically aligned
    progress_circles_x = 0.78  # Right side position
    circle_width = 0.16
    circle_height = 0.16
    
    # Left leg circles - vertically stacked
    left_force_y = 0.16
    left_velocity_y = 0.005
    left_circle_x = progress_circles_x - 0.10 - circle_width/2  # Offset left from center
    
    # Right leg circles - vertically stacked
    right_force_y = 0.16
    right_velocity_y = 0.005
    right_circle_x = progress_circles_x + 0.10 - circle_width/2  # Offset right from center
    
    # Create charts - order matches page layout
    slv_radar_chart(ax_radar, left_best_row, right_best_row, pop_df)
    slv_performance_table(ax_table, left, right, "SLV")
    slv_power_curve(ax_power, left, right, pop_df)
    slv_fv_scatter(ax_fv, left, right, pop_df)
    
    if best_left is not None:
        # Left leg Force circle (top)
        # [left, bottom, width, height] in figure coordinates
        ax_joint_left_force = fig.add_axes([left_circle_x, left_force_y, circle_width, circle_height])
        ax_joint_left_force.set_aspect('equal')
        joint_radial(ax_joint_left_force, best_left, pop_df, metric="force")
        
        # Left leg Velocity circle (bottom)
        # [left, bottom, width, height] in figure coordinates
        ax_joint_left_vel = fig.add_axes([left_circle_x, left_velocity_y, circle_width, circle_height])
        ax_joint_left_vel.set_aspect('equal')
        joint_radial(ax_joint_left_vel, best_left, pop_df, metric="velocity")
        
        # Labels for left leg
        fig.text(progress_circles_x - 0.10, 0.315, "Force", ha='center', va='bottom',
                 fontsize=40, color='white', fontweight='bold', transform=fig.transFigure)
        fig.text(progress_circles_x - 0.10, 0.152, "Velocity", ha='center', va='bottom',
                 fontsize=40, color='white', fontweight='bold', transform=fig.transFigure)
        fig.text(progress_circles_x - 0.10, 0.335, "Left Leg", ha='center', va='center',
                 fontsize=48, color='white', fontweight='bold', transform=fig.transFigure)
    
    if best_right is not None:
        # Right leg Force circle (top)
        # [left, bottom, width, height] in figure coordinates
        ax_joint_right_force = fig.add_axes([right_circle_x, right_force_y, circle_width, circle_height])
        ax_joint_right_force.set_aspect('equal')
        joint_radial(ax_joint_right_force, best_right, pop_df, metric="force")
        
        # Right leg Velocity circle (bottom)
        # [left, bottom, width, height] in figure coordinates
        ax_joint_right_vel = fig.add_axes([right_circle_x, right_velocity_y, circle_width, circle_height])
        ax_joint_right_vel.set_aspect('equal')
        joint_radial(ax_joint_right_vel, best_right, pop_df, metric="velocity")
        
        # Labels for right leg
        fig.text(progress_circles_x + 0.10, 0.315, "Force", ha='center', va='bottom',
                 fontsize=40, color='white', fontweight='bold', transform=fig.transFigure)
        fig.text(progress_circles_x + 0.10, 0.152, "Velocity", ha='center', va='bottom',
                 fontsize=40, color='white', fontweight='bold', transform=fig.transFigure)
        fig.text(progress_circles_x + 0.10, 0.335, "Right Leg", ha='center', va='center',
                 fontsize=48, color='white', fontweight='bold', transform=fig.transFigure)

    plt.suptitle("Single Leg Verticals", fontsize=70, color='white')

    # Add header with vertical bar, name, date, logo, and horizontal line
    add_header_slv(fig, ATHLETE_NAME, REPORT_DATE)
    
    # Add blue line between bar graphs section and bottom section
    # Line positioned at same distance from edge as header line (0.05 to 0.98)
    line2 = Line2D([0.05, 0.98], [0.35, 0.35], transform=fig.transFigure, color='#2c99d4', linewidth=9, zorder=1000)
    fig.add_artist(line2)
    
    # Set all axes backgrounds to #373e43
    for ax in fig.get_axes():
        if hasattr(ax, 'set_facecolor'):
            ax.set_facecolor('#373e43')
    pdf.savefig(fig, facecolor='black', edgecolor='none')
    plt.close(fig)

# ------------------------------------------------
# Build PDF
# ------------------------------------------------
import os

print(f"Output PDF path: {OUTPUT_PDF}")
print(f"Full absolute path: {os.path.abspath(OUTPUT_PDF)}")

# Delete old PDF if it exists
if os.path.exists(OUTPUT_PDF):
    os.remove(OUTPUT_PDF)
    print(f"Deleted old PDF: {OUTPUT_PDF}")

try:
    with PdfPages(OUTPUT_PDF) as pdf:

        # DJ, CMJ, PPU are identical page structures
        for key in ["DJ", "CMJ", "PPU"]:
            if key in all_data:
                movement_page(pdf, key, all_data[key], all_data[key])
            else:
                print(f"Skipping {key} - no data available")

        # SLV special
        if "SLV" in all_data:
            slv_page(pdf, all_data["SLV"], all_data["SLV"])
        else:
            print("Skipping SLV - no data available")

    print("PDF created successfully:", OUTPUT_PDF)
    if os.path.exists(OUTPUT_PDF):
        print(f"PDF file exists at: {os.path.abspath(OUTPUT_PDF)}")
        print(f"File size: {os.path.getsize(OUTPUT_PDF)} bytes")
    else:
        print("ERROR: PDF file was not created!")
except Exception as e:
    print(f"ERROR creating PDF: {e}")
    import traceback
    traceback.print_exc()
