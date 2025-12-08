"""
ETL pipeline for Proteus data.
"""
import os
import logging
import shutil
from pathlib import Path
from datetime import datetime
from typing import Optional
import pandas as pd
from common.config import get_warehouse_engine
from common.db_utils import write_df
from common.id_utils import attach_athlete_uuid
from common.athlete_manager import get_warehouse_connection, update_athlete_flags
from proteus.process_raw import clean_proteus, load_raw_proteus
from common.io_utils import load_csv, find_files

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_processed_files(conn) -> set:
    """
    Get set of already processed file paths from f_proteus table.
    
    Args:
        conn: PostgreSQL connection
        
    Returns:
        Set of processed file paths
    """
    try:
        with conn.cursor() as cur:
            # Check if source_file column exists
            cur.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = 'public' 
                AND table_name = 'f_proteus' 
                AND column_name = 'source_file'
            """)
            
            if cur.fetchone():
                # Column exists, get all processed files
                cur.execute("""
                    SELECT DISTINCT source_file 
                    FROM public.f_proteus 
                    WHERE source_file IS NOT NULL
                """)
                return {row[0] for row in cur.fetchall()}
            else:
                # Column doesn't exist yet, return empty set
                return set()
    except Exception as e:
        logger.warning(f"Could not get processed files: {e}")
        return set()


def ensure_column_exists(conn, table_name: str, column_name: str, column_type: str = 'TEXT'):
    """Ensure a column exists in a table."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = 'public' 
            AND table_name = %s 
            AND column_name = %s
        """, (table_name, column_name))
        
        if cur.fetchone() is None:
            try:
                cur.execute(f"""
                    ALTER TABLE public.{table_name} 
                    ADD COLUMN {column_name} {column_type}
                """)
                conn.commit()
                logger.info(f"Added column {column_name} to {table_name}")
            except Exception as e:
                logger.warning(f"Could not add column {column_name}: {e}")
                conn.rollback()


def process_proteus_csv_file(csv_path: Path, conn) -> bool:
    """
    Process a single Proteus CSV file.
    
    Args:
        csv_path: Path to CSV file
        conn: PostgreSQL connection
        
    Returns:
        True if successful, False otherwise
    """
    logger.info(f"Processing: {csv_path.name}")
    
    try:
        # Load CSV
        df = load_csv(csv_path)
        if df.empty:
            logger.warning(f"CSV file is empty: {csv_path.name}")
            return False
        
        # Clean data
        clean_df = clean_proteus(df)
        if clean_df.empty:
            logger.warning(f"No data after cleaning: {csv_path.name}")
            return False
        
        # Add source_file column
        clean_df['source_file'] = str(csv_path)
        
        # Attach athlete UUIDs
        clean_df = attach_athlete_uuid(
            clean_df,
            source_system='proteus',
            source_id_column='source_athlete_id'
        )
        
        clean_df['source_system'] = 'proteus'
        clean_df['created_at'] = datetime.now()
        
        # Ensure source_file column exists in database
        ensure_column_exists(conn, 'f_proteus', 'source_file', 'TEXT')
        
        # Write to database
        engine = get_warehouse_engine()
        write_df(clean_df, 'f_proteus', engine, if_exists='append')
        
        # Update athlete flags
        if 'athlete_uuid' in clean_df.columns:
            unique_uuids = clean_df['athlete_uuid'].dropna().unique()
            from common.athlete_matcher import update_athlete_data_flag
            for uuid in unique_uuids:
                try:
                    update_athlete_data_flag(conn, str(uuid), "proteus", has_data=True)
                except:
                    pass
        
        logger.info(f"✓ Processed {len(clean_df)} rows from {csv_path.name}")
        return True
        
    except Exception as e:
        logger.error(f"Error processing {csv_path.name}: {e}", exc_info=True)
        return False


def run_daily_proteus_ingest(inbox_dir: Optional[Path] = None, archive_dir: Optional[Path] = None):
    """
    Process all new CSV files in the inbox directory.
    
    Args:
        inbox_dir: Directory containing CSV files to process (default: from config)
        archive_dir: Directory to move processed files (default: from config)
    """
    # Get directories from config if not provided
    if inbox_dir is None:
        try:
            from proteus.web.config import get_proteus_inbox_dir
            inbox_dir = get_proteus_inbox_dir()
        except:
            from common.config import get_raw_paths
            paths = get_raw_paths()
            proteus_path = paths.get('proteus')
            if proteus_path:
                inbox_dir = Path(proteus_path) / "inbox"
            else:
                raise ValueError("Proteus inbox directory not configured")
    
    if archive_dir is None:
        try:
            from proteus.web.config import get_proteus_archive_dir
            archive_dir = get_proteus_archive_dir()
        except:
            from common.config import get_raw_paths
            paths = get_raw_paths()
            proteus_path = paths.get('proteus')
            if proteus_path:
                archive_dir = Path(proteus_path) / "archive"
            else:
                archive_dir = inbox_dir.parent / "archive"
    
    archive_dir.mkdir(parents=True, exist_ok=True)
    
    logger.info("=" * 80)
    logger.info("Proteus ETL Processing")
    logger.info("=" * 80)
    logger.info(f"Inbox directory: {inbox_dir}")
    logger.info(f"Archive directory: {archive_dir}")
    
    if not inbox_dir.exists():
        logger.warning(f"Inbox directory does not exist: {inbox_dir}")
        return
    
    # Connect to database
    conn = get_warehouse_connection()
    
    try:
        # Get processed files
        processed_files = get_processed_files(conn)
        logger.info(f"Found {len(processed_files)} already processed files")
        
        # Find CSV files in inbox
        csv_files = list(inbox_dir.glob("*.csv"))
        if not csv_files:
            logger.info("No CSV files found in inbox")
            return
        
        logger.info(f"Found {len(csv_files)} CSV files in inbox")
        
        # Filter out already processed files
        new_files = []
        for csv_file in csv_files:
            file_path_normalized = os.path.normpath(str(csv_file))
            if file_path_normalized not in processed_files:
                new_files.append(csv_file)
            else:
                logger.info(f"Skipping already processed: {csv_file.name}")
        
        if not new_files:
            logger.info("All files have already been processed")
            return
        
        logger.info(f"Processing {len(new_files)} new files...")
        
        # Process each file
        processed_count = 0
        failed_count = 0
        
        for csv_file in new_files:
            if process_proteus_csv_file(csv_file, conn):
                # Move to archive
                archive_path = archive_dir / csv_file.name
                if archive_path.exists():
                    import time
                    timestamp = int(time.time())
                    archive_path = archive_dir / f"{csv_file.stem}_{timestamp}{csv_file.suffix}"
                
                shutil.move(str(csv_file), str(archive_path))
                logger.info(f"✓ Archived: {archive_path.name}")
                processed_count += 1
            else:
                failed_count += 1
        
        # Update athlete flags
        logger.info("Updating athlete data flags...")
        try:
            from common.athlete_manager import update_athlete_flags
            update_athlete_flags(conn=conn, verbose=True)
        except Exception as e:
            logger.warning(f"Could not update athlete flags: {e}")
        
        # Summary
        logger.info("=" * 80)
        logger.info("Processing Summary")
        logger.info("=" * 80)
        logger.info(f"Processed: {processed_count} files")
        logger.info(f"Failed: {failed_count} files")
        
    finally:
        conn.close()


def etl_proteus():
    """
    Legacy ETL function for Proteus data.
    Uses the old method of loading from raw data directory.
    """
    logger.info("Starting Proteus ETL (legacy mode)...")
    
    try:
        raw_df = load_raw_proteus()
        if raw_df.empty:
            logger.warning("No raw data found. Skipping ETL.")
            return
        
        clean_df = clean_proteus(raw_df)
        clean_df = attach_athlete_uuid(
            clean_df,
            source_system='proteus',
            source_id_column='source_athlete_id'
        )
        
        clean_df['source_system'] = 'proteus'
        clean_df['created_at'] = datetime.now()
        
        engine = get_warehouse_engine()
        write_df(clean_df, 'f_proteus', engine, if_exists='append')
        
        logger.info(f"Successfully loaded {len(clean_df)} rows to f_proteus")
        
    except Exception as e:
        logger.error(f"Error in Proteus ETL: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    # Default to inbox/archive mode
    run_daily_proteus_ingest()

