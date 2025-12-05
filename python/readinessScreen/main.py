"""
Main orchestration script for Readiness Screen data processing.
Coordinates XML parsing, ASCII file processing, and optional dashboard launch.
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
from common.athlete_manager import get_warehouse_connection, get_or_create_athlete
from common.athlete_matcher import update_athlete_data_flag
from common.athlete_utils import extract_source_athlete_id
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime

from database import initialize_database, insert_participant, insert_trial_data, get_participant_id
from file_parsers import (
    find_session_xml, parse_xml_file, parse_ascii_file, parse_txt_file,
    extract_name, extract_date, read_first_numeric_row_values,
    select_folder_dialog, ASCII_FILES
)
from database_utils import reorder_all_tables
from dashboard import run_dashboard

# Map movement types to PostgreSQL table names
MOVEMENT_TO_PG_TABLE = {
    "I": "f_readiness_screen_i",
    "Y": "f_readiness_screen_y",
    "T": "f_readiness_screen_t",
    "IR90": "f_readiness_screen_ir90",
    "CMJ": "f_readiness_screen_cmj",
    "PPU": "f_readiness_screen_ppu"
}


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
    
    # Insert participant (will use existing if found)
    participant_id = insert_participant(
        conn,
        name=xml_data['name'],
        height=xml_data['height'],
        weight=xml_data['weight'],
        plyo_day=xml_data['plyo_day'],
        creation_date=xml_data['creation_date'],
        skip_if_exists=True
    )
    print(f"   Participant: {name} (ID: {participant_id})")
    
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


def calculate_age_group(session_date, date_of_birth):
    """Calculate age group based on session_date and DOB."""
    if not session_date or not date_of_birth:
        return None
    
    try:
        if isinstance(session_date, str):
            session_date = datetime.strptime(session_date, "%Y-%m-%d").date()
        if isinstance(date_of_birth, str):
            date_of_birth = datetime.strptime(date_of_birth, "%Y-%m-%d").date()
        
        age = (session_date - date_of_birth).days / 365.25
        
        if age < 13:
            return "U13"
        elif age < 15:
            return "U15"
        elif age < 17:
            return "U17"
        elif age < 19:
            return "U19"
        elif age < 23:
            return "U23"
        else:
            return "23+"
    except:
        return None


def process_txt_files(output_path: str):
    """
    Process all txt files from Output Files directory and insert into PostgreSQL.
    Extracts name and date from first line of each txt file (like Athletic Screen).
    
    Args:
        output_path: Path to directory containing txt files (e.g., 'D:/Readiness Screen 3/Output Files/')
    
    Returns:
        List of tuples (participant_name, athlete_uuid) for processed files.
    """
    print("=" * 60)
    print("Readiness Screen Data Processing to PostgreSQL")
    print("=" * 60)
    print(f"\nScanning directory: {output_path}")
    
    if not os.path.exists(output_path):
        raise ValueError(f"Directory not found: {output_path}")
    
    # Connect to PostgreSQL
    print("Connecting to PostgreSQL warehouse...")
    pg_conn = get_warehouse_connection()
    
    # Find all txt files matching our movement types
    txt_files = {}
    for movement_type, filename in ASCII_FILES.items():
        file_path = os.path.join(output_path, filename)
        if os.path.exists(file_path):
            txt_files[movement_type] = file_path
    
    if not txt_files:
        print("No txt files found in Output Files directory.")
        pg_conn.close()
        return []
    
    print(f"Found {len(txt_files)} txt files to process")
    
    # Process each txt file - extract name and date from first line
    processed_athletes = {}  # Track athletes by (name, date) -> athlete_uuid
    processed = []
    errors = []
    inserted_count = 0
    updated_count = 0
    
    for movement_type, file_path in txt_files.items():
        try:
            print(f"\nProcessing {movement_type}: {os.path.basename(file_path)}")
            
            # Parse txt file - extracts name and date from first line
            parsed_data = parse_txt_file(file_path, movement_type)
            
            if not parsed_data:
                print(f"   Skipping {file_path} - failed to parse")
                continue
            
            name = parsed_data['name']
            date_str = parsed_data['date']
            athlete_key = (name, date_str)
            
            print(f"   Extracted: {name} ({date_str})")
            
            # Get or create athlete in PostgreSQL (with name cleaning and source ID extraction)
            # Note: Readiness screen txt files don't contain demographic data,
            # so we only pass name and source info. Demographic data will be filled
            # from other sources if available.
            if athlete_key not in processed_athletes:
                try:
                    # Extract source_athlete_id (initials if present, otherwise cleaned name)
                    source_athlete_id = extract_source_athlete_id(name)
                    
                    athlete_uuid = get_or_create_athlete(
                        name=name,  # Will be cleaned by get_or_create_athlete (removes dates, initials, etc.)
                        source_system="readiness_screen",
                        source_athlete_id=source_athlete_id
                    )
                    processed_athletes[athlete_key] = athlete_uuid
                    print(f"   Got/created athlete UUID: {athlete_uuid}")
                    
                    # Update data flag immediately
                    update_athlete_data_flag(pg_conn, athlete_uuid, "readiness_screen", has_data=True)
                except Exception as e:
                    print(f"   Error getting athlete UUID: {str(e)}")
                    import traceback
                    traceback.print_exc()
                    errors.append(f"{file_path}: Failed to get athlete UUID - {str(e)}")
                    continue
            
            athlete_uuid = processed_athletes[athlete_key]
            
            # Get DOB for age calculation
            with pg_conn.cursor() as cur:
                cur.execute("""
                    SELECT date_of_birth FROM analytics.d_athletes 
                    WHERE athlete_uuid = %s
                """, (athlete_uuid,))
                result = cur.fetchone()
                dob = result[0] if result else None
            
            # Calculate age_at_collection and age_group
            age_at_collection = None
            age_group = None
            if dob:
                try:
                    session_date = datetime.strptime(date_str, "%Y-%m-%d").date()
                    if isinstance(dob, str):
                        dob_date = datetime.strptime(dob, "%Y-%m-%d").date()
                    else:
                        dob_date = dob
                    age_at_collection = (session_date - dob_date).days / 365.25
                    age_group = calculate_age_group(session_date, dob_date)
                except:
                    pass
            
            # Map to PostgreSQL table
            pg_table = MOVEMENT_TO_PG_TABLE.get(movement_type)
            if not pg_table:
                print(f"   Warning: No PostgreSQL table mapping for {movement_type}")
                continue
            
            # Prepare data for insertion
            if movement_type in {"CMJ", "PPU"}:
                # CMJ/PPU columns
                insert_data = {
                    'athlete_uuid': athlete_uuid,
                    'session_date': date_str,
                    'source_system': 'readiness_screen',
                    'source_athlete_id': extract_source_athlete_id(name),
                    'age_at_collection': age_at_collection,
                    'age_group': age_group,
                    'jump_height': parsed_data.get('JH_IN'),
                    'peak_power': parsed_data.get('LEWIS_PEAK_POWER'),
                    'peak_force': parsed_data.get('Max_Force'),
                    'pp_w_per_kg': parsed_data.get('PP_W_per_kg'),
                    'pp_forceplate': parsed_data.get('PP_FORCEPLATE'),
                    'force_at_pp': parsed_data.get('Force_at_PP'),
                    'vel_at_pp': parsed_data.get('Vel_at_PP')
                }
            else:
                # I, Y, T, IR90 columns
                insert_data = {
                    'athlete_uuid': athlete_uuid,
                    'session_date': date_str,
                    'source_system': 'readiness_screen',
                    'source_athlete_id': extract_source_athlete_id(name),
                    'age_at_collection': age_at_collection,
                    'age_group': age_group,
                    'avg_force': parsed_data.get('Avg_Force'),
                    'avg_force_norm': parsed_data.get('Avg_Force_Norm'),
                    'max_force': parsed_data.get('Max_Force'),
                    'max_force_norm': parsed_data.get('Max_Force_Norm'),
                    'time_to_max': parsed_data.get('Time_to_Max')
                }
            
            # UPSERT: Check if row exists, then update or insert
            with pg_conn.cursor() as cur:
                # Check if row exists
                cur.execute(f"""
                    SELECT COUNT(*) FROM public.{pg_table}
                    WHERE athlete_uuid = %s AND session_date = %s
                """, (athlete_uuid, date_str))
                
                exists = cur.fetchone()[0] > 0
                
                if exists:
                    # Update existing row
                    if movement_type in {"CMJ", "PPU"}:
                        update_cols = ['jump_height', 'peak_power', 'peak_force', 'pp_w_per_kg', 
                                      'pp_forceplate', 'force_at_pp', 'vel_at_pp', 'age_at_collection', 'age_group']
                    else:
                        update_cols = ['avg_force', 'avg_force_norm', 'max_force', 'max_force_norm', 
                                      'time_to_max', 'age_at_collection', 'age_group']
                    
                    set_parts = [f"{col} = %s" for col in update_cols]
                    update_values = [insert_data[col] for col in update_cols]
                    update_values.extend([athlete_uuid, date_str])
                    
                    cur.execute(f"""
                        UPDATE public.{pg_table}
                        SET {', '.join(set_parts)}
                        WHERE athlete_uuid = %s AND session_date = %s
                    """, update_values)
                    updated_count += 1
                    print(f"   ✓ Updated {movement_type} data")
                else:
                    # Insert new row
                    cols = list(insert_data.keys())
                    placeholders = ', '.join(['%s'] * len(cols))
                    col_names = ', '.join(cols)
                    values = [insert_data[col] for col in cols]
                    
                    cur.execute(f"""
                        INSERT INTO public.{pg_table} ({col_names})
                        VALUES ({placeholders})
                    """, values)
                    inserted_count += 1
                    print(f"   ✓ Inserted {movement_type} data")
                
                pg_conn.commit()
            
            if athlete_key not in [p[0:2] for p in processed]:
                processed.append((name, athlete_uuid, date_str))
                
        except Exception as e:
            error_msg = f"{file_path}: {str(e)}"
            errors.append(error_msg)
            print(f"   ✗ Error: {str(e)}")
            import traceback
            traceback.print_exc()
    
    # Update athlete flags for all successfully processed athletes
    print("\nUpdating athlete data flags...")
    try:
        pg_conn = get_warehouse_connection()
        for _, athlete_uuid, _ in processed:
            update_athlete_data_flag(pg_conn, athlete_uuid, "readiness_screen", has_data=True)
        pg_conn.close()
        print("Athlete flags updated successfully")
    except Exception as e:
        print(f"Warning: Could not update athlete flags: {str(e)}")
    
    # Summary
    print("\n" + "=" * 60)
    print("Processing Summary")
    print("=" * 60)
    print(f"Processed: {len(processed)} athletes")
    print(f"Inserted: {inserted_count} rows")
    print(f"Updated: {updated_count} rows")
    print(f"Errors: {len(errors)} files")
    
    if errors:
        print("\nErrors:")
        for error in errors:
            print(f"  - {error}")
    
    return [(name, uuid) for name, uuid, _ in processed]


def main():
    """
    Main execution function.
    Configure paths and processing options here.
    """
    # Get paths from config (or use defaults)
    # Following notebook logic: base_dir is where Data folders are, output_path is shared Output Files
    try:
        raw_paths = get_raw_paths()
        base_dir = raw_paths.get('readiness_screen', 'D:/Readiness Screen 3/Data/')
        output_path = raw_paths.get('readiness_screen_output', 'D:/Readiness Screen 3/Output Files/')
    except:
        base_dir = 'D:/Readiness Screen 3/Data/'
        output_path = 'D:/Readiness Screen 3/Output Files/'
    
    # Database is in the parent directory (following notebook)
    # Use the standard path from the notebook: D:/Readiness Screen 3/Readiness_Screen_Data_v2.db
    # If base_dir is a placeholder path, use the default location
    if 'path/to' in base_dir or not os.path.exists(base_dir):
        # Use default location
        db_path = 'D:/Readiness Screen 3/Readiness_Screen_Data_v2.db'
    else:
        # Normalize the path to handle trailing slashes
        base_dir_normalized = base_dir.rstrip('/\\')
        if base_dir_normalized.endswith('Data'):
            db_dir = os.path.dirname(base_dir_normalized)
        else:
            db_dir = base_dir_normalized
        
        db_path = os.path.join(db_dir, 'Readiness_Screen_Data_v2.db')
    
    db_path = os.path.abspath(db_path)  # Use absolute path
    
    # Ensure directory exists
    db_dir_abs = os.path.dirname(db_path)
    if not os.path.exists(db_dir_abs):
        os.makedirs(db_dir_abs, exist_ok=True)
    
    # Print paths for debugging
    print(f"Database path: {db_path}")
    print(f"Database directory: {db_dir_abs}")
    print(f"Database directory exists: {os.path.exists(db_dir_abs)}")
    print(f"Database file exists: {os.path.exists(db_path)}")
    
    # Processing options
    BATCH_PROCESS = True  # Process all folders in directory (False = single folder)
    USE_FOLDER_DIALOG = True  # Show folder selection dialog (only if BATCH_PROCESS=False)
    REORDER_DATABASE = True  # Reorder tables alphabetically
    LAUNCH_DASHBOARD = False  # Launch Dash dashboard after processing
    
    if BATCH_PROCESS:
        # Step 1: Process txt files from Output Files directory
        # Extract name and date from first line of each txt file (like Athletic Screen)
        # Insert directly into PostgreSQL
        processed = process_txt_files(output_path)
        
        if not processed:
            print("No folders were processed.")
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
    else:
        # Single folder processing (original behavior)
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

