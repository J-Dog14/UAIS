"""
Main orchestration script for Athletic Screen data processing.
This script coordinates all processing steps: file parsing, database creation,
power analysis, and optional report generation.
"""
import os
import sys
from pathlib import Path

# Add python directory to path so imports work
project_root = Path(__file__).parent.parent.parent
python_dir = project_root / "python"
if str(python_dir) not in sys.path:
    sys.path.insert(0, str(python_dir))

from common.config import get_raw_paths

from database import create_database, create_tables, insert_row, get_connection
from file_parsers import parse_movement_file, classify_movement_type
from power_analysis import update_table_with_power_metrics
from database_replication import replicate_database, clear_processed_files

# Note: Report generation is in a separate module (report_generation.py)
# Import it if you need to generate reports:
# from report_generation import generate_report


def process_raw_files(folder_path: str, db_path: str, reset_db: bool = False, 
                     skip_if_exists: bool = True):
    """
    Process all raw movement files and populate the database.
    
    Args:
        folder_path: Directory containing raw .txt files.
        db_path: Path to output database.
        reset_db: If True, delete existing database and create fresh.
        skip_if_exists: If True, skip insertion if record already exists.
    """
    print("=" * 60)
    print("Athletic Screen Data Processing")
    print("=" * 60)
    
    # Create database and tables
    print("\n1. Setting up database...")
    conn = create_database(db_path, reset=reset_db)
    create_tables(conn)
    cursor = conn.cursor()
    
    # Process all .txt files (excluding Power.txt files)
    print("\n2. Processing movement files...")
    processed_count = 0
    skipped_count = 0
    duplicate_count = 0
    
    for file_name in os.listdir(folder_path):
        if not file_name.endswith('.txt'):
            continue
        if file_name.endswith('_Power.txt'):
            # Power files handled separately
            continue

        file_path = os.path.join(folder_path, file_name)
        parsed_data = parse_movement_file(file_path, folder_path)
        
        if not parsed_data:
            skipped_count += 1
            continue
        
        # Insert into appropriate table
        movement_type = parsed_data.pop('movement_type')
        cols = list(parsed_data.keys())
        vals = [parsed_data[col] for col in cols]
        
        inserted = insert_row(cursor, movement_type, cols, vals, skip_if_exists=skip_if_exists)
        if inserted:
            processed_count += 1
            print(f"  Processed {file_name} -> {movement_type}")
        else:
            duplicate_count += 1
    
    conn.commit()
    print(f"\n  Processed: {processed_count} files")
    print(f"  Skipped (duplicates): {duplicate_count} files")
    print(f"  Skipped (errors): {skipped_count} files")
    
    # Update with power metrics
    print("\n3. Computing power analysis metrics...")
    from database import POWER_ANALYSIS_TABLES
    for table in POWER_ANALYSIS_TABLES:
        update_table_with_power_metrics(conn, table, folder_path, fs_hz=1000.0)
    
    conn.close()
    print("\nData processing complete!")
    return db_path


def main():
    """
    Main execution function.
    Configure paths and processing options here.
    """
    # Get paths from config (or use defaults)
    try:
        raw_paths = get_raw_paths()
        folder_path = raw_paths.get('athletic_screen', r'D:/Athletic Screen 2.0/Output Files/')
    except:
        folder_path = r'D:/Athletic Screen 2.0/Output Files/'
    
    db_path = os.path.join(folder_path, 'movement_database_v2.db')
    
    # Processing options
    RESET_DB = False  # Set to True to start fresh
    RUN_POWER_ANALYSIS = True  # Compute power metrics
    REPLICATE_DB = False  # Copy to target databases
    CLEAR_FILES = False  # Delete processed .txt files
    
    # Step 1: Process raw files
    process_raw_files(folder_path, db_path, reset_db=RESET_DB)
    
    # Step 2: Optional - Replicate database
    if REPLICATE_DB:
        print("\n4. Replicating database...")
        output_folder = folder_path
        target_databases = ["Athletic_Screen_College_data_v2.db"]
        all_data_db_path = os.path.join(output_folder, "Athletic_Screen_All_data_v2.db")
        
        replicate_database(
            source_db_path=db_path,
            target_db_paths=[os.path.join(output_folder, db) for db in target_databases],
            all_data_db_path=all_data_db_path
        )
    
    # Step 3: Optional - Clear processed files
    if CLEAR_FILES:
        print("\n5. Clearing processed files...")
        clear_processed_files(folder_path, extensions=['.txt'])
    
    # Step 4: Optional - Generate report
    # Uncomment to generate reports:
    # print("\n6. Generating report...")
    # from report_generation import generate_report
    # generate_report(db_path, ...)
    
    print("\n" + "=" * 60)
    print("All processing complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()

