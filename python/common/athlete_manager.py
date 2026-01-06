#!/usr/bin/env python3
"""
Athlete Manager Module
Centralized athlete identity management for UAIS warehouse

This module provides functions to:
- Get or create athlete UUIDs
- Check app database for existing UUIDs
- Update athlete information without overwriting existing data
- Normalize names for matching

Usage:
    from python.common.athlete_manager import get_or_create_athlete
    
    athlete_uuid = get_or_create_athlete(
        name="Weiss, Ryan 11-25",
        date_of_birth="1996-12-10",
        source_system="pitching",
        source_athlete_id="RW-001"
    )
"""
# Run mass check with   python python/common/athlete_manager.py --backfill-uuids --dry-run

import os
import sys
import re
import uuid
import logging
from pathlib import Path
from typing import Optional, Dict, Any
from datetime import datetime, date

import psycopg2
from psycopg2.extras import RealDictCursor
import yaml

# Configure logging to stderr by default (so stdout can be used for data output)
# This can be overridden by callers if needed
logging.basicConfig(level=logging.INFO, stream=sys.stderr, format='%(levelname)s:%(name)s:%(message)s')
logger = logging.getLogger(__name__)


def normalize_name_for_display(name: str) -> str:
    """
    Convert name to "First Last" format (removes dates but keeps original case).
    
    Args:
        name: Original name (e.g., "Weiss, Ryan 11-25" or "Crider, Carson 12-24")
        
    Returns:
        Display name (e.g., "Ryan Weiss" or "Carson Crider")
    """
    if not name or name.strip() == "":
        return ""
    
    # Remove dates (various formats)
    # Full dates: MM/DD/YYYY, MM-DD-YYYY, YYYY-MM-DD, etc.
    name = re.sub(r'\s*\d{1,2}[/-]\d{1,2}[/-]\d{2,4}', '', name)
    name = re.sub(r'\s*\d{4}[/-]\d{1,2}[/-]\d{1,2}', '', name)
    # Month-day only: MM-DD, MM/DD (e.g., "11-25", "10-24", "12-24")
    name = re.sub(r'\s*\d{1,2}[/-]\d{1,2}(?![/-]\d)', '', name)
    # Standalone years
    name = re.sub(r'\s*\d{4}', '', name)
    name = name.strip()
    
    # Convert "LAST, FIRST" to "FIRST LAST"
    if ',' in name:
        parts = name.split(',')
        if len(parts) == 2:
            last = parts[0].strip()
            first = parts[1].strip()
            name = f"{first} {last}"
    
    # Normalize whitespace (keep original case)
    name = ' '.join(name.split())
    
    return name


def normalize_name_for_matching(name: str) -> str:
    """
    Normalize athlete name for matching.
    
    Converts "LAST, FIRST" to "FIRST LAST", removes dates, converts to uppercase.
    
    Args:
        name: Original name (e.g., "Weiss, Ryan 11-25" or "Ryan Weiss")
        
    Returns:
        Normalized name (e.g., "RYAN WEISS")
    """
    if not name or name.strip() == "":
        return ""
    
    # Remove dates (various formats)
    # Full dates: MM/DD/YYYY, MM-DD-YYYY, YYYY-MM-DD, etc.
    name = re.sub(r'\s*\d{1,2}[/-]\d{1,2}[/-]\d{2,4}', '', name)
    name = re.sub(r'\s*\d{4}[/-]\d{1,2}[/-]\d{1,2}', '', name)
    # Month-day only: MM-DD, MM/DD (e.g., "11-25", "10-24", "12-24")
    name = re.sub(r'\s*\d{1,2}[/-]\d{1,2}(?![/-]\d)', '', name)
    # Standalone years
    name = re.sub(r'\s*\d{4}', '', name)
    name = name.strip()
    
    # Convert "LAST, FIRST" to "FIRST LAST"
    if ',' in name:
        parts = name.split(',')
        if len(parts) == 2:
            last = parts[0].strip()
            first = parts[1].strip()
            name = f"{first} {last}"
    
    # Normalize whitespace and convert to uppercase
    name = ' '.join(name.split())
    name = name.upper()
    
    return name


def load_db_config() -> Dict[str, Any]:
    """
    Load database configuration from db_connections.yaml.
    
    Returns:
        Dictionary with database connection info
    """
    config_path = Path(__file__).parent.parent.parent / 'config' / 'db_connections.yaml'
    
    if not config_path.exists():
        raise FileNotFoundError(f"Database config not found: {config_path}")
    
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    return config


def get_warehouse_connection():
    """
    Get connection to warehouse database.
    
    Returns:
        psycopg2 connection object
    """
    config = load_db_config()
    wh_config = config['databases']['warehouse']['postgres']
    
    conn = psycopg2.connect(
        host=wh_config['host'],
        port=wh_config['port'],
        database=wh_config['database'],
        user=wh_config['user'],
        password=wh_config['password'],
        connect_timeout=10
    )
    conn.set_client_encoding('UTF8')
    
    return conn


def get_app_connection():
    """
    Get connection to app database.
    
    Returns:
        psycopg2 connection object
    """
    config = load_db_config()
    app_config = config['databases']['app']['postgres']
    
    conn = psycopg2.connect(
        host=app_config['host'],
        port=app_config['port'],
        database=app_config['database'],
        user=app_config['user'],
        password=app_config['password'],
        connect_timeout=10
    )
    conn.set_client_encoding('UTF8')
    
    return conn


def get_verceldb_connection():
    """
    Get connection to verceldb database (master source of truth for athlete UUIDs).
    This is a read-only connection.
    
    Returns:
        psycopg2 connection object
    """
    config = load_db_config()
    verceldb_config = config['databases']['verceldb']['postgres']
    
    conn = psycopg2.connect(
        host=verceldb_config['host'],
        port=verceldb_config['port'],
        database=verceldb_config['database'],
        user=verceldb_config['user'],
        password=verceldb_config['password'],
        connect_timeout=10
    )
    conn.set_client_encoding('UTF8')
    
    return conn


def check_verceldb_for_uuid(normalized_name: str) -> Optional[str]:
    """
    Check if athlete exists in verceldb User table by normalized name.
    This is the master source of truth for athlete UUIDs (read-only).
    
    Args:
        normalized_name: Normalized name to search for
        
    Returns:
        UUID from verceldb if found, None otherwise
    """
    try:
        conn = get_verceldb_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Query User table - try exact match first
            query = '''
                SELECT uuid, name
                FROM public."User"
                WHERE LOWER(TRIM(name)) = LOWER(%s)
                   OR LOWER(REPLACE(name, ',', '')) = LOWER(%s)
            '''
            
            cur.execute(query, (normalized_name, normalized_name.replace(',', '')))
            result = cur.fetchone()
            
            if result:
                logger.info(f"Found UUID in verceldb for {normalized_name}: {result['uuid']}")
                conn.close()
                return str(result['uuid'])
            
            # Try fuzzy match - normalize all names in User table
            cur.execute('SELECT uuid, name FROM public."User"')
            all_users = cur.fetchall()
            
            for user in all_users:
                user_normalized = normalize_name_for_matching(user['name'])
                if user_normalized == normalized_name:
                    logger.info(f"Found UUID in verceldb (fuzzy match) for {normalized_name}: {user['uuid']}")
                    conn.close()
                    return str(user['uuid'])
        
        conn.close()
        return None
        
    except Exception as e:
        logger.warning(f"Error checking verceldb: {e}")
        return None


def check_app_db_for_uuid(normalized_name: str) -> Optional[str]:
    """
    Check if athlete exists in app database User table by normalized name.
    DEPRECATED: Use check_verceldb_for_uuid() instead.
    
    Args:
        normalized_name: Normalized name to search for
        
    Returns:
        UUID from app database if found, None otherwise
    """
    try:
        conn = get_app_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Query User table
            query = '''
                SELECT uuid, name
                FROM public."User"
                WHERE LOWER(TRIM(name)) = LOWER(%s)
                   OR LOWER(REPLACE(name, ',', '')) = LOWER(%s)
            '''
            
            # Try exact match first
            cur.execute(query, (normalized_name, normalized_name.replace(',', '')))
            result = cur.fetchone()
            
            if result:
                logger.info(f"Found UUID in app DB for {normalized_name}: {result['uuid']}")
                return str(result['uuid'])
            
            # Try fuzzy match - normalize all names in User table
            cur.execute('SELECT uuid, name FROM public."User"')
            all_users = cur.fetchall()
            
            for user in all_users:
                user_normalized = normalize_name_for_matching(user['name'])
                if user_normalized == normalized_name:
                    logger.info(f"Found UUID in app DB (fuzzy match) for {normalized_name}: {user['uuid']}")
                    return str(user['uuid'])
        
        conn.close()
        return None
        
    except Exception as e:
        logger.warning(f"Error checking app database: {e}")
        return None


def get_athlete_from_warehouse(normalized_name: str, date_of_birth: Optional[str] = None, conn=None) -> Optional[Dict[str, Any]]:
    """
    Get athlete from warehouse by normalized name (and optionally DOB for better matching).
    
    Args:
        normalized_name: Normalized name to search for
        date_of_birth: Optional date of birth (YYYY-MM-DD) for additional matching
        conn: Optional database connection (creates new if not provided)
        
    Returns:
        Dictionary with athlete data if found, None otherwise
    """
    close_conn = False
    if conn is None:
        conn = get_warehouse_connection()
        close_conn = True
    
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if date_of_birth:
                # If DOB provided, prefer exact match on both name and DOB
                cur.execute('''
                    SELECT * FROM analytics.d_athletes
                    WHERE normalized_name = %s
                      AND date_of_birth = %s
                    ORDER BY app_db_uuid NULLS LAST, created_at ASC
                    LIMIT 1
                ''', (normalized_name, date_of_birth))
                
                result = cur.fetchone()
                if result:
                    return dict(result)
            
            # Fallback to name-only match
            cur.execute('''
                SELECT * FROM analytics.d_athletes
                WHERE normalized_name = %s
                ORDER BY app_db_uuid NULLS LAST, created_at ASC
                LIMIT 1
            ''', (normalized_name,))
            
            result = cur.fetchone()
            if result:
                return dict(result)
            return None
            
    finally:
        if close_conn:
            conn.close()


def create_athlete_in_warehouse(
    name: str,
    normalized_name: str,
    athlete_uuid: Optional[str] = None,
    date_of_birth: Optional[str] = None,
    age: Optional[float] = None,
    age_at_collection: Optional[float] = None,
    gender: Optional[str] = None,
    height: Optional[float] = None,
    weight: Optional[float] = None,
    email: Optional[str] = None,  # Not stored in d_athletes, kept for API compatibility
    phone: Optional[str] = None,  # Not stored in d_athletes, kept for API compatibility
    notes: Optional[str] = None,
    source_system: Optional[str] = None,
    source_athlete_id: Optional[str] = None,
    app_db_uuid: Optional[str] = None,
    conn=None
) -> str:
    """
    Create new athlete in warehouse.
    
    SAFEGUARD: Checks for existing athlete with same normalized_name before inserting.
    If found, raises ValueError to prevent duplicates.
    
    Args:
        name: Full name
        normalized_name: Normalized name for matching
        athlete_uuid: UUID (generates new if not provided)
        date_of_birth: Date of birth (YYYY-MM-DD)
        age: Age
        age_at_collection: Age at time of data collection
        gender: Gender
        height: Height
        weight: Weight
        email: Email
        phone: Phone
        notes: Notes
        source_system: Source system (pitching, hitting, etc.)
        source_athlete_id: Original ID from source system
        app_db_uuid: UUID from app database
        conn: Optional database connection
        
    Returns:
        athlete_uuid
        
    Raises:
        ValueError: If athlete with same normalized_name already exists
    """
    close_conn = False
    if conn is None:
        conn = get_warehouse_connection()
        close_conn = True
    
    try:
        # SAFEGUARD: Check for existing athlete with same normalized_name
        existing = get_athlete_from_warehouse(normalized_name, date_of_birth, conn)
        if existing:
            raise ValueError(
                f"Athlete with normalized name '{normalized_name}' already exists: "
                f"{existing['name']} ({existing['athlete_uuid']}). "
                f"Use get_or_create_athlete() instead of create_athlete_in_warehouse()."
            )
        
        # Check verceldb first (master source of truth)
        if athlete_uuid is None:
            verceldb_uuid = check_verceldb_for_uuid(normalized_name)
            if verceldb_uuid:
                athlete_uuid = verceldb_uuid
                logger.info(f"Using UUID from verceldb for {name}: {athlete_uuid}")
            else:
                athlete_uuid = str(uuid.uuid4())
                logger.info(f"Generated new UUID for {name}: {athlete_uuid}")
        
        with conn.cursor() as cur:
            # Use UPSERT to handle duplicate UUIDs gracefully
            # This prevents "duplicate key value violates unique constraint" errors
            cur.execute('''
                INSERT INTO analytics.d_athletes (
                    athlete_uuid, name, normalized_name,
                    date_of_birth, age, age_at_collection,
                    gender, height, weight, notes,
                    source_system, source_athlete_id, app_db_uuid, app_db_synced_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW()
                )
                ON CONFLICT (athlete_uuid) 
                DO UPDATE SET
                    name = COALESCE(EXCLUDED.name, analytics.d_athletes.name),
                    normalized_name = COALESCE(EXCLUDED.normalized_name, analytics.d_athletes.normalized_name),
                    date_of_birth = COALESCE(EXCLUDED.date_of_birth, analytics.d_athletes.date_of_birth),
                    age = COALESCE(EXCLUDED.age, analytics.d_athletes.age),
                    age_at_collection = COALESCE(EXCLUDED.age_at_collection, analytics.d_athletes.age_at_collection),
                    gender = COALESCE(EXCLUDED.gender, analytics.d_athletes.gender),
                    height = COALESCE(EXCLUDED.height, analytics.d_athletes.height),
                    weight = COALESCE(EXCLUDED.weight, analytics.d_athletes.weight),
                    notes = COALESCE(EXCLUDED.notes, analytics.d_athletes.notes),
                    source_system = COALESCE(EXCLUDED.source_system, analytics.d_athletes.source_system),
                    source_athlete_id = COALESCE(EXCLUDED.source_athlete_id, analytics.d_athletes.source_athlete_id),
                    app_db_uuid = COALESCE(EXCLUDED.app_db_uuid, analytics.d_athletes.app_db_uuid),
                    app_db_synced_at = CASE 
                        WHEN EXCLUDED.app_db_uuid IS NOT NULL THEN NOW()
                        ELSE analytics.d_athletes.app_db_synced_at
                    END
            ''', (
                athlete_uuid, name, normalized_name,
                date_of_birth, age, age_at_collection,
                gender, height, weight, notes,
                source_system, source_athlete_id, app_db_uuid
            ))
            
            conn.commit()
            logger.info(f"Created/updated athlete in warehouse: {name} ({athlete_uuid})")
            
            return athlete_uuid
            
    except psycopg2.IntegrityError as e:
        # Handle unique constraint violation (for normalized_name or other constraints)
        error_str = str(e)
        if 'unique_normalized_name' in error_str or 'normalized_name' in error_str:
            # Try to get the existing athlete
            existing = get_athlete_from_warehouse(normalized_name, conn)
            if existing:
                raise ValueError(
                    f"Athlete with normalized name '{normalized_name}' already exists: "
                    f"{existing['name']} ({existing['athlete_uuid']}). "
                    f"Use get_or_create_athlete() instead of create_athlete_in_warehouse()."
                )
        # For athlete_uuid conflicts, the UPSERT should have handled it, but if it still fails:
        if 'd_athletes_pkey' in error_str or 'athlete_uuid' in error_str:
            # Try to get existing athlete by UUID
            try:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute('''
                        SELECT * FROM analytics.d_athletes WHERE athlete_uuid = %s
                    ''', (athlete_uuid,))
                    existing = cur.fetchone()
                    if existing:
                        logger.warning(f"Athlete with UUID {athlete_uuid} already exists: {existing['name']}")
                        return athlete_uuid  # Return existing UUID
            except:
                pass
        raise
            
    finally:
        if close_conn:
            conn.close()


def update_athlete_in_warehouse(
    athlete_uuid: str,
    name: Optional[str] = None,
    date_of_birth: Optional[str] = None,
    age: Optional[float] = None,
    age_at_collection: Optional[float] = None,
    gender: Optional[str] = None,
    height: Optional[float] = None,
    weight: Optional[float] = None,
    email: Optional[str] = None,  # Not stored in d_athletes, kept for API compatibility
    phone: Optional[str] = None,  # Not stored in d_athletes, kept for API compatibility
    notes: Optional[str] = None,
    app_db_uuid: Optional[str] = None,
    conn=None
) -> None:
    """
    Update athlete in warehouse (only updates non-NULL fields, doesn't overwrite existing data).
    
    Args:
        athlete_uuid: UUID of athlete to update
        name: Full name (only updates if current value is NULL)
        date_of_birth: Date of birth (only updates if current value is NULL)
        age: Age (only updates if current value is NULL)
        age_at_collection: Age at collection (only updates if current value is NULL)
        gender: Gender (only updates if current value is NULL)
        height: Height (only updates if current value is NULL)
        weight: Weight (only updates if current value is NULL)
        email: Email (only updates if current value is NULL)
        phone: Phone (only updates if current value is NULL)
        notes: Notes (only updates if current value is NULL)
        app_db_uuid: App DB UUID (always updates if provided)
        conn: Optional database connection
    """
    close_conn = False
    if conn is None:
        conn = get_warehouse_connection()
        close_conn = True
    
    try:
        updates = []
        params = []
        
        if name is not None:
            updates.append("name = COALESCE(name, %s)")
            params.append(name)
        
        if date_of_birth is not None:
            updates.append("date_of_birth = COALESCE(date_of_birth, %s)")
            params.append(date_of_birth)
        
        if age is not None:
            updates.append("age = COALESCE(age, %s)")
            params.append(age)
        
        if age_at_collection is not None:
            updates.append("age_at_collection = COALESCE(age_at_collection, %s)")
            params.append(age_at_collection)
        
        if gender is not None:
            updates.append("gender = COALESCE(gender, %s)")
            params.append(gender)
        
        if height is not None:
            updates.append("height = COALESCE(height, %s)")
            params.append(height)
        
        if weight is not None:
            updates.append("weight = COALESCE(weight, %s)")
            params.append(weight)
        
        # Note: email and phone are not stored in d_athletes table
        # They are accepted for API compatibility but ignored
        
        if notes is not None:
            updates.append("notes = COALESCE(notes, %s)")
            params.append(notes)
        
        if app_db_uuid is not None:
            updates.append("app_db_uuid = %s, app_db_synced_at = NOW()")
            params.append(app_db_uuid)
        
        if updates:
            params.append(athlete_uuid)
            query = f'''
                UPDATE analytics.d_athletes
                SET {', '.join(updates)}
                WHERE athlete_uuid = %s
            '''
            
            with conn.cursor() as cur:
                cur.execute(query, params)
                conn.commit()
                logger.info(f"Updated athlete in warehouse: {athlete_uuid}")
    
    finally:
        if close_conn:
            conn.close()


def get_or_create_athlete(
    name: str,
    date_of_birth: Optional[str] = None,
    age: Optional[float] = None,
    age_at_collection: Optional[float] = None,
    gender: Optional[str] = None,
    height: Optional[float] = None,
    weight: Optional[float] = None,
    email: Optional[str] = None,  # Not stored in d_athletes, kept for API compatibility
    phone: Optional[str] = None,  # Not stored in d_athletes, kept for API compatibility
    notes: Optional[str] = None,
    source_system: Optional[str] = None,
    source_athlete_id: Optional[str] = None,
    check_app_db: bool = True
) -> str:
    """
    Get or create athlete UUID.
    
    This is the main function to use in ETL scripts.
    
    Flow:
    1. Clean and normalize name (removes dates, handles comma format, removes numbers/initials)
    2. Check warehouse for existing athlete by normalized name
    3. If found, update with any new info (non-destructive)
    4. If not found, check verceldb for UUID
    5. If found in verceldb, use that UUID; otherwise generate new
    6. Create athlete in warehouse with cleaned name
    7. Return UUID
    
    Args:
        name: Full name (e.g., "Weiss, Ryan 11-25")
        date_of_birth: Date of birth (YYYY-MM-DD)
        age: Age
        age_at_collection: Age at time of data collection
        gender: Gender
        height: Height
        weight: Weight
        email: Email
        phone: Phone
        notes: Notes
        source_system: Source system (pitching, hitting, etc.)
        source_athlete_id: Original ID from source system
        check_app_db: Whether to check app database for UUID
        
    Returns:
        athlete_uuid (string)
    """
    # Clean and normalize name using cleanup module (removes dates, initials, etc.)
    try:
        from python.common.athlete_cleanup import clean_athlete_name_for_processing
        cleaned_display_name, normalized_name = clean_athlete_name_for_processing(name)
        # Use cleaned display name for storage
        name = cleaned_display_name
    except ImportError:
        # Fallback to original normalization if cleanup module not available
        normalized_name = normalize_name_for_matching(name)
        name = normalize_name_for_display(name)
    
    if not normalized_name:
        raise ValueError("Name cannot be empty after normalization")
    
    conn = get_warehouse_connection()
    
    try:
        # Check warehouse first (with DOB if provided for better matching)
        existing = get_athlete_from_warehouse(normalized_name, date_of_birth, conn)
        
        if existing:
            # Athlete exists - update with any new info
            logger.info(f"Found existing athlete: {existing['name']} ({existing['athlete_uuid']})")
            
            # Check verceldb if not already synced (master source of truth)
            verceldb_uuid = None
            if check_app_db and not existing.get('app_db_uuid'):
                verceldb_uuid = check_verceldb_for_uuid(normalized_name)
            
            # Convert name to "First Last" format if provided
            display_name = normalize_name_for_display(name) if name else None
            
            update_athlete_in_warehouse(
                existing['athlete_uuid'],
                name=display_name,  # Store as "First Last" format
                date_of_birth=date_of_birth,
                age=age,
                age_at_collection=age_at_collection,
                gender=gender,
                height=height,
                weight=weight,
                email=email,
                phone=phone,
                notes=notes,
                app_db_uuid=verceldb_uuid,
                conn=conn
            )
            
            # CRITICAL: Add source_athlete_id mapping to preserve it across merges
            if source_system and source_athlete_id:
                try:
                    from python.common.source_athlete_map import add_source_mapping
                    add_source_mapping(conn, existing['athlete_uuid'], source_system, source_athlete_id)
                except Exception as e:
                    logger.warning(f"Failed to add source mapping: {e}")
            
            return existing['athlete_uuid']
        
        # Athlete doesn't exist - create new
        logger.info(f"Creating new athlete: {name}")
        
        # Check verceldb first (master source of truth)
        verceldb_uuid = None
        if check_app_db:
            verceldb_uuid = check_verceldb_for_uuid(normalized_name)
        
        # Use verceldb UUID if found, otherwise generate new
        athlete_uuid = verceldb_uuid if verceldb_uuid else str(uuid.uuid4())
        
        # Convert name to "First Last" format (removes dates, converts Last, First to First Last)
        display_name = normalize_name_for_display(name)
        
        # Create in warehouse
        create_athlete_in_warehouse(
            name=display_name,  # Store as "First Last" format
            normalized_name=normalized_name,
            athlete_uuid=athlete_uuid,
            date_of_birth=date_of_birth,
            age=age,
            age_at_collection=age_at_collection,
            gender=gender,
            height=height,
            weight=weight,
            email=email,
            phone=phone,
            notes=notes,
            source_system=source_system,
            source_athlete_id=source_athlete_id,
            app_db_uuid=verceldb_uuid,
            conn=conn
        )
        
        # CRITICAL: Add source_athlete_id mapping to preserve it across merges
        if source_system and source_athlete_id:
            try:
                from python.common.source_athlete_map import add_source_mapping
                add_source_mapping(conn, athlete_uuid, source_system, source_athlete_id)
            except Exception as e:
                logger.warning(f"Failed to add source mapping: {e}")
        
        return athlete_uuid
        
    finally:
        conn.close()


def update_athlete_flags(conn=None, verbose=True):
    """
    Update athlete data flags and session counts in d_athletes table.
    
    This function calls the PostgreSQL function to update all athlete data presence
    flags and session counts based on data in fact tables. Should be called after
    bulk data inserts to ensure flags are accurate.
    
    Args:
        conn: Optional database connection. If None, creates a new connection.
        verbose: Whether to print summary statistics (default: True)
        
    Returns:
        dict with 'success' (bool) and 'message' (str) keys, and optionally 'stats' (dict)
        
    Example:
        >>> result = update_athlete_flags()
        >>> if result['success']:
        ...     print("Flags updated successfully")
    """
    if conn is None:
        conn = get_warehouse_connection()
        close_conn = True
    else:
        close_conn = False
    
    try:
        with conn.cursor() as cur:
            # Call the PostgreSQL function
            cur.execute("SELECT update_athlete_data_flags()")
            conn.commit()
            
            if verbose:
                # Get summary statistics
                cur.execute("""
                    SELECT 
                        COUNT(*) as total_athletes,
                        COUNT(*) FILTER (WHERE has_pitching_data) as with_pitching,
                        COUNT(*) FILTER (WHERE has_athletic_screen_data) as with_athletic_screen,
                        COUNT(*) FILTER (WHERE has_pro_sup_data) as with_pro_sup,
                        COUNT(*) FILTER (WHERE has_readiness_screen_data) as with_readiness,
                        COUNT(*) FILTER (WHERE has_mobility_data) as with_mobility,
                        COUNT(*) FILTER (WHERE has_proteus_data) as with_proteus,
                        COUNT(*) FILTER (WHERE has_hitting_data) as with_hitting,
                        COUNT(*) FILTER (WHERE has_arm_action_data) as with_arm_action,
                        COUNT(*) FILTER (WHERE has_curveball_test_data) as with_curveball_test
                    FROM analytics.d_athletes
                """)
                
                stats = cur.fetchone()
                
                print("=" * 80)
                print("ATHLETE DATA FLAGS UPDATED")
                print("=" * 80)
                print(f"Total athletes: {stats[0]}")
                print()
                print("Athletes with data in each system:")
                print(f"  Pitching: {stats[1]}")
                print(f"  Athletic Screen: {stats[2]}")
                print(f"  Pro-Sup: {stats[3]}")
                print(f"  Readiness Screen: {stats[4]}")
                print(f"  Mobility: {stats[5]}")
                print(f"  Proteus: {stats[6]}")
                print(f"  Hitting: {stats[7]}")
                print(f"  Arm Action: {stats[8]}")
                print(f"  Curveball Test: {stats[9]}")
                print("=" * 80)
                
                return {
                    'success': True,
                    'message': 'Athlete flags updated successfully',
                    'stats': {
                        'total_athletes': stats[0],
                        'with_pitching': stats[1],
                        'with_athletic_screen': stats[2],
                        'with_pro_sup': stats[3],
                        'with_readiness': stats[4],
                        'with_mobility': stats[5],
                        'with_proteus': stats[6],
                        'with_hitting': stats[7],
                        'with_arm_action': stats[8],
                        'with_curveball_test': stats[9]
                    }
                }
            else:
                return {
                    'success': True,
                    'message': 'Athlete flags updated successfully'
                }
                
    except Exception as e:
        logger.error(f"Failed to update athlete flags: {e}")
        return {
            'success': False,
            'message': str(e)
        }
    finally:
        if close_conn:
            conn.close()


def update_uuid_from_verceldb(athlete_uuid: str, normalized_name: str, conn=None) -> bool:
    """
    Update athlete UUID from verceldb if a match is found.
    This is used for backfilling UUIDs for athletes that weren't in verceldb initially.
    
    Args:
        athlete_uuid: Current athlete UUID in warehouse
        normalized_name: Normalized name to search for in verceldb
        conn: Optional database connection
        
    Returns:
        True if UUID was updated, False otherwise
    """
    close_conn = False
    if conn is None:
        conn = get_warehouse_connection()
        close_conn = True
    
    try:
        # Check verceldb for UUID
        verceldb_uuid = check_verceldb_for_uuid(normalized_name)
        
        if not verceldb_uuid:
            logger.info(f"No match found in verceldb for {normalized_name}")
            return False
        
        # If UUIDs are the same, no update needed
        if athlete_uuid == verceldb_uuid:
            logger.info(f"UUID already matches verceldb for {normalized_name}")
            return False
        
        # Update UUID in d_athletes and all f_ tables
        logger.info(f"Updating UUID from {athlete_uuid} to {verceldb_uuid} for {normalized_name}")
        return update_uuid_across_tables(athlete_uuid, verceldb_uuid, conn)
        
    finally:
        if close_conn:
            conn.close()


def update_uuid_across_tables(old_uuid: str, new_uuid: str, conn=None) -> bool:
    """
    Update athlete UUID across all tables when UUID changes.
    Updates d_athletes first, then all f_ tables.
    
    Hierarchy: verceldb/User -> uais_warehouse/d_athletes -> f_ tables
    
    Args:
        old_uuid: Current athlete UUID
        new_uuid: New athlete UUID (from verceldb)
        conn: Optional database connection
        
    Returns:
        True if update was successful, False otherwise
    """
    close_conn = False
    if conn is None:
        conn = get_warehouse_connection()
        close_conn = True
    
    try:
        with conn.cursor() as cur:
            # Check if new_uuid already exists in d_athletes (conflict check)
            cur.execute('''
                SELECT athlete_uuid, name, normalized_name
                FROM analytics.d_athletes
                WHERE athlete_uuid = %s
            ''', (new_uuid,))
            
            existing_athlete = cur.fetchone()
            if existing_athlete and existing_athlete[0] != old_uuid:
                logger.error(
                    f"Cannot update UUID: {new_uuid} already exists in d_athletes "
                    f"for athlete '{existing_athlete[1]}' ({existing_athlete[2]}). "
                    f"This indicates a potential data conflict that needs manual resolution."
                )
                return False
            
            # List of all f_ tables that have athlete_uuid column
            fact_tables = [
                'f_athletic_screen',
                'f_athletic_screen_cmj',
                'f_athletic_screen_dj',
                'f_athletic_screen_slv',
                'f_athletic_screen_nmt',
                'f_athletic_screen_ppu',
                'f_pro_sup',
                'f_readiness_screen',
                'f_readiness_screen_i',
                'f_readiness_screen_y',
                'f_readiness_screen_t',
                'f_readiness_screen_ir90',
                'f_readiness_screen_cmj',
                'f_readiness_screen_ppu',
                'f_mobility',
                'f_proteus',
                'f_kinematics_pitching',
                'f_kinematics_hitting',
                'f_arm_action',
                'f_curveball_test'
            ]
            
            # First, update d_athletes
            cur.execute('''
                UPDATE analytics.d_athletes
                SET athlete_uuid = %s, app_db_uuid = %s, app_db_synced_at = NOW()
                WHERE athlete_uuid = %s
            ''', (new_uuid, new_uuid, old_uuid))
            
            rows_updated_d = cur.rowcount
            
            if rows_updated_d == 0:
                logger.warning(f"No athlete found with UUID {old_uuid} in d_athletes")
                return False
            
            logger.info(f"Updated d_athletes: {rows_updated_d} row(s)")
            
            # Update all f_ tables
            total_rows_updated = rows_updated_d
            for table in fact_tables:
                try:
                    # Check if table exists and has athlete_uuid column
                    cur.execute(f'''
                        SELECT COUNT(*) 
                        FROM information_schema.columns 
                        WHERE table_schema = 'public' 
                        AND table_name = %s 
                        AND column_name = 'athlete_uuid'
                    ''', (table,))
                    
                    if cur.fetchone()[0] == 0:
                        logger.debug(f"Table {table} does not exist or has no athlete_uuid column, skipping")
                        continue
                    
                    # Update athlete_uuid in this table
                    cur.execute(f'''
                        UPDATE public.{table}
                        SET athlete_uuid = %s
                        WHERE athlete_uuid = %s
                    ''', (new_uuid, old_uuid))
                    
                    rows_updated = cur.rowcount
                    if rows_updated > 0:
                        logger.info(f"Updated {table}: {rows_updated} row(s)")
                        total_rows_updated += rows_updated
                        
                except Exception as e:
                    logger.warning(f"Error updating {table}: {e}")
                    continue
            
            conn.commit()
            logger.info(f"Successfully updated UUID from {old_uuid} to {new_uuid} across all tables ({total_rows_updated} total rows)")
            return True
            
    except Exception as e:
        logger.error(f"Error updating UUID across tables: {e}")
        conn.rollback()
        return False
        
    finally:
        if close_conn:
            conn.close()


def backfill_uuids_from_verceldb(conn=None, dry_run: bool = False) -> Dict[str, Any]:
    """
    Backfill UUIDs from verceldb for all athletes in warehouse that don't have app_db_uuid set.
    This function checks verceldb for name matches and updates UUIDs if found.
    
    Args:
        conn: Optional database connection
        dry_run: If True, only reports what would be updated without making changes
        
    Returns:
        Dictionary with summary statistics
    """
    close_conn = False
    if conn is None:
        conn = get_warehouse_connection()
        close_conn = True
    
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Get all athletes without app_db_uuid
            cur.execute('''
                SELECT athlete_uuid, name, normalized_name
                FROM analytics.d_athletes
                WHERE app_db_uuid IS NULL
                ORDER BY created_at
            ''')
            
            athletes_to_check = cur.fetchall()
            
            logger.info(f"Found {len(athletes_to_check)} athletes without app_db_uuid to check")
            
            updated_count = 0
            matched_count = 0
            not_found_count = 0
            errors = []
            
            for athlete in athletes_to_check:
                athlete_uuid = athlete['athlete_uuid']
                normalized_name = athlete['normalized_name']
                name = athlete['name']
                
                # Check verceldb for match
                verceldb_uuid = check_verceldb_for_uuid(normalized_name)
                
                if verceldb_uuid:
                    matched_count += 1
                    logger.info(f"Found match in verceldb for {name}: {verceldb_uuid}")
                    
                    if not dry_run:
                        # Update UUID across all tables
                        if update_uuid_across_tables(athlete_uuid, verceldb_uuid, conn):
                            updated_count += 1
                        else:
                            errors.append(f"Failed to update {name} ({athlete_uuid})")
                    else:
                        logger.info(f"[DRY RUN] Would update {name} from {athlete_uuid} to {verceldb_uuid}")
                        updated_count += 1
                else:
                    not_found_count += 1
                    logger.debug(f"No match in verceldb for {name}")
            
            result = {
                'total_checked': len(athletes_to_check),
                'matched_in_verceldb': matched_count,
                'updated': updated_count,
                'not_found': not_found_count,
                'errors': errors,
                'dry_run': dry_run
            }
            
            logger.info("=" * 80)
            logger.info("UUID BACKFILL SUMMARY")
            logger.info("=" * 80)
            logger.info(f"Total athletes checked: {result['total_checked']}")
            logger.info(f"Matched in verceldb: {result['matched_in_verceldb']}")
            logger.info(f"Updated: {result['updated']}")
            logger.info(f"Not found in verceldb: {result['not_found']}")
            if errors:
                logger.warning(f"Errors: {len(errors)}")
            logger.info("=" * 80)
            
            return result
            
    finally:
        if close_conn:
            conn.close()


if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Athlete Manager - Get or create athlete UUIDs'
    )
    parser.add_argument(
        '--backfill-uuids',
        action='store_true',
        help='Backfill app_db_uuid for all athletes missing it (checks verceldb for matches)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Dry run mode - show what would be updated without making changes (only with --backfill-uuids)'
    )
    
    args = parser.parse_args()
    
    if args.backfill_uuids:
        # Mass backfill mode
        logger.info("=" * 80)
        logger.info("UUID BACKFILL FROM VERCELDB")
        logger.info("=" * 80)
        if args.dry_run:
            logger.info("DRY RUN MODE - No changes will be made")
        logger.info("=" * 80)
        logger.info("")
        
        result = backfill_uuids_from_verceldb(dry_run=args.dry_run)
        
        if result['errors']:
            logger.error("\nErrors encountered:")
            for error in result['errors']:
                logger.error(f"  - {error}")
        
        if args.dry_run:
            logger.info("\nRun without --dry-run to apply changes")
    else:
        # Default: Test the module with one athlete
        test_uuid = get_or_create_athlete(
            name="Weiss, Ryan 11-25",
            date_of_birth="1996-12-10",
            age=28,
            source_system="pitching",
            source_athlete_id="RW-001"
        )
        print(f"Test athlete UUID: {test_uuid}")

