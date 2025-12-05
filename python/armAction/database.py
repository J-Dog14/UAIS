"""
Database initialization and data ingestion functions for Arm Action data.
Now integrated with warehouse database.
"""

import sys
import re
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any

# Add parent directory to path for imports
python_dir = Path(__file__).parent.parent
if str(python_dir) not in sys.path:
    sys.path.insert(0, str(python_dir))

from common.athlete_manager import get_or_create_athlete, get_warehouse_connection
from common.athlete_manager import normalize_name_for_matching
from common.athlete_utils import extract_source_athlete_id
from config import CAPTURE_RATE
from parsers import parse_events_from_aPlus, parse_aplus_kinematics, parse_file_info
from utils import compute_score

import psycopg2
from psycopg2.extras import execute_values


def get_temp_table_name() -> str:
    """Get the name of the temporary table for current session data."""
    return "temp_arm_action_current_session"


def init_temp_table(conn):
    """
    Create a temporary table for current session data (used for report generation).
    This table is cleared at the start of each run.
    """
    with conn.cursor() as cur:
        # Drop existing temp table if it exists
        cur.execute(f"DROP TABLE IF EXISTS {get_temp_table_name()}")
        
        # Create temp table with same structure as f_arm_action
        create_sql = f"""
        CREATE TEMPORARY TABLE {get_temp_table_name()} (
            id SERIAL PRIMARY KEY,
            athlete_uuid VARCHAR(36) NOT NULL,
            participant_name TEXT,
            session_date DATE NOT NULL,
            filename TEXT,
            movement_type TEXT,
            foot_contact_frame INTEGER,
            release_frame INTEGER,
            arm_abduction_at_footplant NUMERIC,
            max_abduction NUMERIC,
            shoulder_angle_at_footplant NUMERIC,
            max_er NUMERIC,
            arm_velo NUMERIC,
            max_torso_rot_velo NUMERIC,
            torso_angle_at_footplant NUMERIC,
            score NUMERIC
        )
        """
        cur.execute(create_sql)
        conn.commit()


def clear_temp_table(conn):
    """
    Clear all data from the temporary table.
    This function is called at the start of each analysis run.
    """
    with conn.cursor() as cur:
        cur.execute(f"DELETE FROM {get_temp_table_name()}")
        conn.commit()
        
        cur.execute(f"SELECT COUNT(*) FROM {get_temp_table_name()}")
        count = cur.fetchone()[0]
        print(f"Cleared temp table. Remaining records: {count}")


def ingest_data(aPlusDataPath: str, aPlusEventsPath: str):
    """
    Ingest data into the warehouse f_arm_action table and temp table.
    
    This function:
    1. Parses the input files
    2. Gets or creates athletes in the warehouse
    3. Writes data to both temp table (for reports) and warehouse table
    
    Args:
        aPlusDataPath: Path to APlusData.txt file
        aPlusEventsPath: Path to aPlus_events.txt file
    """
    events_dict = parse_events_from_aPlus(aPlusEventsPath, capture_rate=CAPTURE_RATE)
    kinematics = parse_aplus_kinematics(aPlusDataPath)
    
    conn = get_warehouse_connection()
    
    try:
        # Initialize temp table
        init_temp_table(conn)
        
        # Prepare data for bulk insert
        warehouse_rows = []
        temp_rows = []
        
        processed_count = 0
        for row in kinematics:
            fn = row.get("filename", "").strip()
            if not fn:
                continue
            
            fc = events_dict.get(fn, {}).get("foot_contact_frame")
            rel = events_dict.get(fn, {}).get("release_frame")
            p_name, p_date_str, m_type = parse_file_info(fn)
            
            # Parse date string to date object
            try:
                # Try to parse date (format may vary)
                if p_date_str and p_date_str != "UnknownDate":
                    # Try common date formats
                    for fmt in ["%Y-%m-%d", "%m-%d-%Y", "%m/%d/%Y", "%Y_%m_%d"]:
                        try:
                            session_date = datetime.strptime(p_date_str, fmt).date()
                            break
                        except ValueError:
                            continue
                    else:
                        # If no format matched, use today's date
                        session_date = datetime.now().date()
                else:
                    session_date = datetime.now().date()
            except Exception:
                session_date = datetime.now().date()
            
            # Get or create athlete in warehouse
            # Extract source_athlete_id (initials) from name if present
            # e.g., "Cody Yarborough CY" -> "CY", "John Smith" -> "John Smith"
            source_athlete_id = extract_source_athlete_id(p_name)
            
            athlete_uuid = get_or_create_athlete(
                name=p_name,  # Will be cleaned by get_or_create_athlete (removes initials, dates, etc.)
                source_system="arm_action",
                source_athlete_id=source_athlete_id  # Use extracted initials or cleaned name
            )
            
            # Pull the numeric fields from row
            abd_fp = row.get("Arm_Abduction@Footplant") or 0
            max_abd = row.get("Max_Abduction") or 0
            shld_fp = row.get("Shoulder_Angle@Footplant") or 0
            max_er = row.get("Max_ER") or 0
            arm_velo = row.get("Arm_Velo") or 0
            torso_velo = row.get("Max_Torso_Rot_Velo") or 0
            torso_ang = row.get("Torso_Angle@Footplant") or 0
            
            # Compute the score
            score_val = compute_score(
                arm_velo,
                torso_velo,
                abd_fp,
                shld_fp,
                max_er
            )
            
            # Prepare row for warehouse
            warehouse_row = (
                athlete_uuid,
                session_date,
                "arm_action",  # source_system
                source_athlete_id,  # source_athlete_id (initials if extracted)
                fn,  # filename
                m_type,  # movement_type
                fc,  # foot_contact_frame
                rel,  # release_frame
                abd_fp,  # arm_abduction_at_footplant
                max_abd,  # max_abduction
                shld_fp,  # shoulder_angle_at_footplant
                max_er,  # max_er
                arm_velo,  # arm_velo
                torso_velo,  # max_torso_rot_velo
                torso_ang,  # torso_angle_at_footplant
                score_val  # score
            )
            warehouse_rows.append(warehouse_row)
            
            # Prepare row for temp table (includes participant_name for reports)
            temp_row = (
                athlete_uuid,
                p_name,  # participant_name (for reports)
                session_date,
                fn,
                m_type,
                fc,
                rel,
                abd_fp,
                max_abd,
                shld_fp,
                max_er,
                arm_velo,
                torso_velo,
                torso_ang,
                score_val
            )
            temp_rows.append(temp_row)
            
            processed_count += 1
        
        # Bulk insert into warehouse
        if warehouse_rows:
            with conn.cursor() as cur:
                insert_sql = """
                INSERT INTO public.f_arm_action (
                    athlete_uuid, session_date, source_system, source_athlete_id,
                    filename, movement_type, foot_contact_frame, release_frame,
                    arm_abduction_at_footplant, max_abduction,
                    shoulder_angle_at_footplant, max_er,
                    arm_velo, max_torso_rot_velo, torso_angle_at_footplant,
                    score
                ) VALUES %s
                """
                execute_values(cur, insert_sql, warehouse_rows)
                conn.commit()
                print(f"Inserted {len(warehouse_rows)} record(s) into warehouse f_arm_action table")
        
        # Bulk insert into temp table
        if temp_rows:
            with conn.cursor() as cur:
                insert_sql = f"""
                INSERT INTO {get_temp_table_name()} (
                    athlete_uuid, participant_name, session_date,
                    filename, movement_type, foot_contact_frame, release_frame,
                    arm_abduction_at_footplant, max_abduction,
                    shoulder_angle_at_footplant, max_er,
                    arm_velo, max_torso_rot_velo, torso_angle_at_footplant,
                    score
                ) VALUES %s
                """
                execute_values(cur, insert_sql, temp_rows)
                conn.commit()
                print(f"Inserted {len(temp_rows)} record(s) into temp table")
        
        print(f"Processed {processed_count} movement record(s)")
        
    finally:
        conn.close()


def get_current_session_data(conn=None):
    """
    Get current session data from temp table for report generation.
    
    Args:
        conn: Optional database connection (creates new if not provided)
        
    Returns:
        List of dictionaries with current session data
    """
    close_conn = False
    if conn is None:
        conn = get_warehouse_connection()
        close_conn = True
    
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT 
                    participant_name, session_date, movement_type,
                    foot_contact_frame, release_frame,
                    arm_abduction_at_footplant, max_abduction,
                    shoulder_angle_at_footplant, max_er,
                    arm_velo, max_torso_rot_velo, torso_angle_at_footplant,
                    score
                FROM {get_temp_table_name()}
                ORDER BY id DESC
            """)
            
            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
            
            return [dict(zip(columns, row)) for row in rows]
    finally:
        if close_conn:
            conn.close()
