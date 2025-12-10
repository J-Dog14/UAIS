"""
Main execution script for youth pitch design analysis.
Now integrated with warehouse database.
"""

import os
import sys
from pathlib import Path

# Add parent directory to path for imports
python_dir = Path(__file__).parent.parent
if str(python_dir) not in sys.path:
    sys.path.insert(0, str(python_dir))

from config import EVENTS_PATH
from database import (
    init_temp_table, clear_temp_table,
    ingest_pitches_with_events, get_warehouse_connection
)
from athletes import init_athletes_db, update_athletes_summary
from parsers import parse_events
from reports import generate_curve_report
from common.duplicate_detector import check_and_merge_duplicates


if __name__ == "__main__":
    print("=" * 80)
    print("YOUTH PITCH DESIGN ANALYSIS")
    print("=" * 80)
    print("Integrated with UAIS Warehouse Database")
    print()
    
    # Get warehouse connection to verify it works
    try:
        conn = get_warehouse_connection()
        print(f"Connected to warehouse database: {conn.info.dbname}")
        conn.close()
    except Exception as e:
        print(f"ERROR: Failed to connect to warehouse database: {e}")
        print("Please check your database configuration in config/db_connections.yaml")
        sys.exit(1)
    
    print()
    
    # Initialize temp table for current session data
    print("Initializing temporary table for current session...")
    conn = get_warehouse_connection()
    try:
        init_temp_table(conn)
        clear_temp_table(conn)
    finally:
        conn.close()
    print("Temporary table initialized.\n")
    
    # Initialize athletes (no-op now, but kept for compatibility)
    print("Initializing athlete management...")
    init_athletes_db()
    print("Athlete management initialized.\n")
    
    # Parse events from the events file
    print(f"Parsing events from: {EVENTS_PATH}")
    if not os.path.exists(EVENTS_PATH):
        print(f"ERROR: Events file not found: {EVENTS_PATH}")
        sys.exit(1)
    
    events_dict = parse_events(EVENTS_PATH)
    print(f"Found {len(events_dict)} pitch(es) to process.\n")
    
    # Ingest data
    print("Ingesting pitch data into warehouse...")
    ingest_pitches_with_events(events_dict)
    
    # Update athletes summary table with aggregated statistics
    print("\nUpdating athlete flags in warehouse...")
    update_athletes_summary()
    
    # Check for duplicate athletes and prompt to merge
    print("\nChecking for similar athlete names...")
    try:
        conn = get_warehouse_connection()
        check_and_merge_duplicates(conn=conn, athlete_uuids=None, min_similarity=0.80)
        conn.close()
    except Exception as e:
        print(f"Warning: Could not check for duplicates: {str(e)}")
        import traceback
        traceback.print_exc()
    
    # Generate report
    print("\nGenerating PDF report...")
    generate_curve_report()
    
    print("\n" + "=" * 80)
    print("Analysis complete!")
    print("=" * 80)
    print("\nData has been saved to:")
    print("  - Warehouse table: public.f_curveball_test")
    print("  - Athlete flags updated in: analytics.d_athletes")
    print("\nUse 'python checkDatabase.py' to view current database status.")
