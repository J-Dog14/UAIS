"""
Main runner for Proteus web automation and ETL integration.
"""
import logging
import shutil
from pathlib import Path
from datetime import date
from playwright.sync_api import sync_playwright, Browser
from .config import (
    get_proteus_download_dir,
    get_proteus_inbox_dir,
    get_proteus_archive_dir,
    get_date_range,
    is_headless
)
from .login import login_to_proteus
from .download import download_daily_csv

logger = logging.getLogger(__name__)


def run_daily_proteus_job() -> None:
    """
    Main function to run daily Proteus job:
    1. Launches Playwright browser
    2. Logs into Proteus
    3. Downloads CSV for date range (default: yesterday)
    4. Moves CSV to ETL inbox directory
    5. Triggers ETL processing
    6. Archives processed files
    """
    logger.info("=" * 80)
    logger.info("Starting daily Proteus job")
    logger.info("=" * 80)
    
    download_dir = get_proteus_download_dir()
    inbox_dir = get_proteus_inbox_dir()
    archive_dir = get_proteus_archive_dir()
    
    logger.info(f"Download directory: {download_dir}")
    logger.info(f"Inbox directory: {inbox_dir}")
    logger.info(f"Archive directory: {archive_dir}")
    
    # Get date range (default: yesterday)
    start_date, end_date = get_date_range()
    logger.info(f"Date range: {start_date} to {end_date}")
    
    # Launch browser
    logger.info("Launching browser...")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=is_headless())
        context = browser.new_context()
        page = context.new_page()
        
        try:
            # Login
            logger.info("Logging into Proteus...")
            if not login_to_proteus(page):
                logger.error("Login failed. Aborting.")
                return
            
            logger.info("Login successful!")
            
            # Download CSV
            logger.info("Downloading CSV...")
            try:
                csv_path = download_daily_csv(
                    browser=browser,
                    target_dir=download_dir,
                    start_date=start_date,
                    end_date=end_date
                )
                logger.info(f"✓ Downloaded CSV: {csv_path}")
            except Exception as e:
                logger.error(f"Failed to download CSV: {e}", exc_info=True)
                return
            
            # Move CSV to inbox
            inbox_path = inbox_dir / csv_path.name
            if inbox_path.exists():
                # Add timestamp if file exists
                import time
                timestamp = int(time.time())
                name_parts = csv_path.stem, csv_path.suffix
                inbox_path = inbox_dir / f"{name_parts[0]}_{timestamp}{name_parts[1]}"
            
            shutil.move(str(csv_path), str(inbox_path))
            logger.info(f"✓ Moved CSV to inbox: {inbox_path}")
            
        finally:
            context.close()
            browser.close()
        
        # Run ETL
        logger.info("Running ETL pipeline...")
        try:
            from proteus.etl_proteus import run_daily_proteus_ingest
            run_daily_proteus_ingest(inbox_dir=inbox_dir, archive_dir=archive_dir)
            logger.info("✓ ETL processing complete")
        except ImportError:
            # Fallback to existing ETL if new function doesn't exist
            logger.info("Using existing ETL function...")
            from proteus.etl_proteus import etl_proteus
            etl_proteus()
            logger.info("✓ ETL processing complete")
        except Exception as e:
            logger.error(f"ETL processing failed: {e}", exc_info=True)
            raise
    
    logger.info("=" * 80)
    logger.info("Daily Proteus job complete")
    logger.info("=" * 80)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    run_daily_proteus_job()
