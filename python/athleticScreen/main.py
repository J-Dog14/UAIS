"""
Main orchestration script for Athletic Screen data processing.
Processes all txt files in a directory and inserts data directly into PostgreSQL.
Uses athlete matching logic to prevent duplicates and update existing records.
"""
import os
import sys
from pathlib import Path
from datetime import datetime
from typing import List, Tuple

# Add python directory to path so imports work
project_root = Path(__file__).parent.parent.parent
python_dir = project_root / "python"
if str(python_dir) not in sys.path:
    sys.path.insert(0, str(python_dir))

from common.config import get_raw_paths
from common.athlete_manager import get_warehouse_connection
from common.athlete_matcher import get_or_create_athlete_safe, update_athlete_data_flag
from file_parsers import parse_movement_file

# Map movement types to PostgreSQL table names
MOVEMENT_TO_PG_TABLE = {
    'CMJ': 'f_athletic_screen_cmj',
    'DJ': 'f_athletic_screen_dj',
    'PPU': 'f_athletic_screen_ppu',
    'SLV': 'f_athletic_screen_slv',
    'NMT': 'f_athletic_screen_nmt'
}


def calculate_age_group(session_date, dob_date):
    """
    Calculate age group based on age at session date.
    
    Args:
        session_date: Date of the session
        dob_date: Date of birth
    
    Returns:
        Age group string (U17, U19, U23, 23+) or None
    """
    try:
        age = (session_date - dob_date).days / 365.25
        if age < 17:
            return "U17"
        elif age < 19:
            return "U19"
        elif age < 23:
            return "U23"
        else:
            return "23+"
    except:
        return None


def process_txt_files(folder_path: str):
    """
    Process all txt files from folder and insert into PostgreSQL.
    Extracts name and date from first line of each txt file.
    
    Args:
        folder_path: Path to directory containing txt files (e.g., 'D:/Athletic Screen 2.0/Output Files/')
    
    Returns:
        List of tuples (athlete_name, athlete_uuid) for processed files.
    """
    print("=" * 60)
    print("Athletic Screen Data Processing to PostgreSQL")
    print("=" * 60)
    print(f"\nScanning directory: {folder_path}")
    
    if not os.path.exists(folder_path):
        raise ValueError(f"Directory not found: {folder_path}")
    
    # Connect to PostgreSQL
    print("Connecting to PostgreSQL warehouse...")
    pg_conn = get_warehouse_connection()
    
    # Find all txt files (excluding Power.txt files)
    txt_files = []
    for file_name in os.listdir(folder_path):
        if not file_name.endswith('.txt'):
            continue
        if file_name.endswith('_Power.txt'):
            # Power files handled separately (if needed)
            continue
        
        file_path = os.path.join(folder_path, file_name)
        txt_files.append(file_path)
    
    if not txt_files:
        print("No txt files found in directory.")
        pg_conn.close()
        return []
    
    print(f"Found {len(txt_files)} txt files to process")
    
    # Process each txt file
    processed_athletes = {}  # Track athletes by (name, date) -> athlete_uuid
    processed = []
    errors = []
    inserted_count = 0
    updated_count = 0
    
    for file_path in txt_files:
        try:
            file_name = os.path.basename(file_path)
            print(f"\nProcessing: {file_name}")
            
            # Parse movement file - extracts name, date, and metrics
            parsed_data = parse_movement_file(file_path, folder_path)
            
            if not parsed_data:
                print(f"   Skipping {file_name} - failed to parse")
                errors.append(f"{file_path}: Failed to parse")
                continue
            
            name = parsed_data.get('name')
            date_str = parsed_data.get('date')
            movement_type = parsed_data.get('movement_type')
            
            if not name or not date_str or not movement_type:
                print(f"   Skipping {file_name} - missing required data")
                errors.append(f"{file_path}: Missing required data")
                continue
            
            athlete_key = (name, date_str)
            
            print(f"   Extracted: {name} ({date_str}) - {movement_type}")
            
            # Get or create athlete in PostgreSQL (using safe matcher)
            if athlete_key not in processed_athletes:
                try:
                    athlete_uuid = get_or_create_athlete_safe(
                        name=name,
                        source_system="athletic_screen",
                        source_athlete_id=name,
                        # No demographic data available from txt files
                        conn=pg_conn
                    )
                    processed_athletes[athlete_key] = athlete_uuid
                    print(f"   Got/created athlete UUID: {athlete_uuid}")
                    
                    # Update data flag immediately
                    update_athlete_data_flag(pg_conn, athlete_uuid, "athletic_screen", has_data=True)
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
                errors.append(f"{file_path}: Unknown movement type {movement_type}")
                continue
            
            # Prepare data for insertion based on movement type
            if movement_type in {'CMJ', 'PPU'}:
                insert_data = {
                    'athlete_uuid': athlete_uuid,
                    'session_date': date_str,
                    'source_system': 'athletic_screen',
                    'source_athlete_id': name,
                    'trial_name': parsed_data.get('trial_name'),
                    'age_at_collection': age_at_collection,
                    'age_group': age_group,
                    'jh_in': parsed_data.get('JH_IN'),
                    'peak_power': parsed_data.get('Peak_Power'),
                    'pp_forceplate': parsed_data.get('PP_FORCEPLATE'),
                    'force_at_pp': parsed_data.get('Force_at_PP'),
                    'vel_at_pp': parsed_data.get('Vel_at_PP'),
                    'pp_w_per_kg': parsed_data.get('PP_W_per_kg')
                }
                update_cols = ['jh_in', 'peak_power', 'pp_forceplate', 'force_at_pp', 
                              'vel_at_pp', 'pp_w_per_kg', 'age_at_collection', 'age_group']
            
            elif movement_type == 'DJ':
                insert_data = {
                    'athlete_uuid': athlete_uuid,
                    'session_date': date_str,
                    'source_system': 'athletic_screen',
                    'source_athlete_id': name,
                    'trial_name': parsed_data.get('trial_name'),
                    'age_at_collection': age_at_collection,
                    'age_group': age_group,
                    'jh_in': parsed_data.get('JH_IN'),
                    'pp_forceplate': parsed_data.get('PP_FORCEPLATE'),
                    'force_at_pp': parsed_data.get('Force_at_PP'),
                    'vel_at_pp': parsed_data.get('Vel_at_PP'),
                    'pp_w_per_kg': parsed_data.get('PP_W_per_kg'),
                    'ct': parsed_data.get('CT'),
                    'rsi': parsed_data.get('RSI')
                }
                update_cols = ['jh_in', 'pp_forceplate', 'force_at_pp', 'vel_at_pp', 
                              'pp_w_per_kg', 'ct', 'rsi', 'age_at_collection', 'age_group']
            
            elif movement_type == 'SLV':
                insert_data = {
                    'athlete_uuid': athlete_uuid,
                    'session_date': date_str,
                    'source_system': 'athletic_screen',
                    'source_athlete_id': name,
                    'trial_name': parsed_data.get('trial_name'),
                    'side': parsed_data.get('side'),
                    'age_at_collection': age_at_collection,
                    'age_group': age_group,
                    'jh_in': parsed_data.get('JH_IN'),
                    'pp_forceplate': parsed_data.get('PP_FORCEPLATE'),
                    'force_at_pp': parsed_data.get('Force_at_PP'),
                    'vel_at_pp': parsed_data.get('Vel_at_PP'),
                    'pp_w_per_kg': parsed_data.get('PP_W_per_kg')
                }
                update_cols = ['jh_in', 'pp_forceplate', 'force_at_pp', 'vel_at_pp', 
                              'pp_w_per_kg', 'age_at_collection', 'age_group']
            
            elif movement_type == 'NMT':
                insert_data = {
                    'athlete_uuid': athlete_uuid,
                    'session_date': date_str,
                    'source_system': 'athletic_screen',
                    'source_athlete_id': name,
                    'trial_name': parsed_data.get('trial_name'),
                    'age_at_collection': age_at_collection,
                    'age_group': age_group,
                    'num_taps_10s': parsed_data.get('NUM_TAPS_10s'),
                    'num_taps_20s': parsed_data.get('NUM_TAPS_20s'),
                    'num_taps_30s': parsed_data.get('NUM_TAPS_30s'),
                    'num_taps': parsed_data.get('NUM_TAPS')
                }
                update_cols = ['num_taps_10s', 'num_taps_20s', 'num_taps_30s', 'num_taps',
                              'age_at_collection', 'age_group']
            
            # UPSERT: Check if row exists, then update or insert
            with pg_conn.cursor() as cur:
                # Build WHERE clause based on movement type
                if movement_type == 'SLV':
                    # SLV uses athlete_uuid, session_date, trial_name, and side
                    where_clause = "athlete_uuid = %s AND session_date = %s AND trial_name = %s AND side = %s"
                    where_params = (athlete_uuid, date_str, parsed_data.get('trial_name'), parsed_data.get('side'))
                else:
                    # Other movements use athlete_uuid, session_date, and trial_name
                    where_clause = "athlete_uuid = %s AND session_date = %s AND trial_name = %s"
                    where_params = (athlete_uuid, date_str, parsed_data.get('trial_name'))
                
                # Check if row exists
                cur.execute(f"""
                    SELECT COUNT(*) FROM public.{pg_table}
                    WHERE {where_clause}
                """, where_params)
                
                exists = cur.fetchone()[0] > 0
                
                if exists:
                    # Update existing row
                    set_parts = [f"{col} = %s" for col in update_cols]
                    update_values = [insert_data[col] for col in update_cols]
                    update_values.extend(where_params)
                    
                    cur.execute(f"""
                        UPDATE public.{pg_table}
                        SET {', '.join(set_parts)}
                        WHERE {where_clause}
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
    
    pg_conn.close()
    
    # Update athlete flags for all successfully processed athletes
    print("\nUpdating athlete data flags...")
    try:
        pg_conn = get_warehouse_connection()
        for _, athlete_uuid, _ in processed:
            update_athlete_data_flag(pg_conn, athlete_uuid, "athletic_screen", has_data=True)
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
    try:
        raw_paths = get_raw_paths()
        folder_path = raw_paths.get('athletic_screen', r'D:/Athletic Screen 2.0/Output Files/')
    except:
        folder_path = r'D:/Athletic Screen 2.0/Output Files/'
    
    # Ensure folder path exists
    if 'path/to' in folder_path or not os.path.exists(folder_path):
        folder_path = r'D:/Athletic Screen 2.0/Output Files/'
    
    folder_path = os.path.abspath(folder_path)
    
    # Processing options
    BATCH_PROCESS = True  # Process all files in directory
    
    if BATCH_PROCESS:
        # Process all txt files from Output Files directory
        # Extract name and date from first line of each txt file
        # Insert directly into PostgreSQL
        processed = process_txt_files(folder_path)
        
        if not processed:
            print("No files were processed.")
            return
        
        print("\n" + "=" * 60)
        print("All processing complete!")
        print("=" * 60)
    else:
        print("Batch processing is disabled. Set BATCH_PROCESS = True to process all files.")


if __name__ == "__main__":
    main()
