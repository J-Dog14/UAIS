"""
Main orchestration script for Readiness Screen data processing.
Coordinates XML parsing, ASCII file processing, and optional dashboard launch.
"""
import os
from pathlib import Path
from common.config import get_raw_paths

from database import initialize_database, insert_participant, insert_trial_data
from file_parsers import (
    find_session_xml, parse_xml_file, parse_ascii_file,
    select_folder_dialog, ASCII_FILES
)
from database_utils import reorder_all_tables
from dashboard import run_dashboard


def process_xml_and_ascii(folder_path: str, db_path: str,
                          output_path: str, use_dialog: bool = True):
    """
    Process XML and ASCII files for a session.
    
    Args:
        folder_path: Path to folder containing Session XML file.
        db_path: Path to database file.
        output_path: Path to directory containing ASCII output files.
        use_dialog: If True, show folder selection dialog.
    
    Returns:
        Tuple of (participant_name, participant_id).
    """
    print("=" * 60)
    print("Readiness Screen Data Processing")
    print("=" * 60)
    
    # Select folder if using dialog
    if use_dialog:
        selected_folder = select_folder_dialog()
        if not selected_folder:
            print("No folder selected. Exiting...")
            return None, None
        folder_path = selected_folder
    
    # Initialize database
    print("\n1. Initializing database...")
    conn = initialize_database(db_path)
    
    # Find and parse XML file
    print("\n2. Processing XML file...")
    xml_file_path = find_session_xml(folder_path)
    if not xml_file_path:
        print("No XML file found. Exiting...")
        conn.close()
        return None, None
    
    xml_data = parse_xml_file(xml_file_path)
    name = xml_data['name']
    
    # Insert participant
    participant_id = insert_participant(
        conn,
        name=xml_data['name'],
        height=xml_data['height'],
        weight=xml_data['weight'],
        plyo_day=xml_data['plyo_day'],
        creation_date=xml_data['creation_date']
    )
    print(f"   Participant inserted: {name} (ID: {participant_id})")
    
    # Process ASCII files
    print("\n3. Processing ASCII files...")
    processed_count = 0
    skipped_count = 0
    
    for movement_type, filename in ASCII_FILES.items():
        file_path = os.path.join(output_path, filename)
        
        if not os.path.exists(file_path):
            print(f"   (skip) {filename} not found")
            skipped_count += 1
            continue
        
        # Parse ASCII file
        df = parse_ascii_file(file_path, movement_type)
        print(f"   {filename} preview:\n{df.head()}")
        
        # Insert each row
        for _, row in df.iterrows():
            insert_trial_data(
                conn,
                table_name=movement_type,
                name=name,
                participant_id=participant_id,
                data=row.to_dict(),
                creation_date=xml_data['creation_date']
            )
        
        processed_count += 1
        print(f"   Processed {filename} -> {movement_type}")
    
    conn.close()
    print(f"\n   Processed: {processed_count} files")
    print(f"   Skipped: {skipped_count} files")
    
    return name, participant_id


def main():
    """
    Main execution function.
    Configure paths and processing options here.
    """
    # Get paths from config (or use defaults)
    try:
        raw_paths = get_raw_paths()
        base_dir = raw_paths.get('readiness_screen', 'D:/Readiness Screen 3/')
    except:
        base_dir = 'D:/Readiness Screen 3/'
    
    db_path = os.path.join(base_dir, 'Readiness_Screen_Data_v2.db')
    output_path = os.path.join(base_dir, 'Output Files')
    
    # Processing options
    USE_FOLDER_DIALOG = True  # Show folder selection dialog
    REORDER_DATABASE = True  # Reorder tables alphabetically
    LAUNCH_DASHBOARD = False  # Launch Dash dashboard after processing
    
    # Step 1: Process XML and ASCII files
    name, participant_id = process_xml_and_ascii(
        folder_path=base_dir,
        db_path=db_path,
        output_path=output_path,
        use_dialog=USE_FOLDER_DIALOG
    )
    
    if name is None:
        print("Processing failed or cancelled.")
        return
    
    # Step 2: Reorder database (optional)
    if REORDER_DATABASE:
        print("\n4. Reordering database...")
        reorder_all_tables(db_path, sort_column="Name")
    
    # Step 3: Launch dashboard (optional)
    if LAUNCH_DASHBOARD:
        print("\n5. Launching dashboard...")
        print("   Dashboard will be available at http://127.0.0.1:8051")
        run_dashboard(db_path, port=8051, debug=True)
    else:
        print("\n" + "=" * 60)
        print("All processing complete!")
        print("=" * 60)
        print(f"\nTo launch the dashboard, run:")
        print(f"  python python/readinessScreen/dashboard.py")


if __name__ == "__main__":
    main()

