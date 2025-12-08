"""
CSV download automation for Proteus web portal.
"""
import logging
import shutil
from pathlib import Path
from datetime import date
from playwright.sync_api import Page, Browser, TimeoutError as PlaywrightTimeoutError
from .config import get_proteus_base_url

logger = logging.getLogger(__name__)


def navigate_to_export_page(page: Page) -> bool:
    """
    Navigate to the data export/download page.
    
    Args:
        page: Playwright page object (should be logged in)
        
    Returns:
        True if navigation successful, False otherwise
    """
    base_url = get_proteus_base_url()
    
    # Try common export page URLs
    export_urls = [
        f"{base_url}/export",
        f"{base_url}/data/export",
        f"{base_url}/reports/export",
        f"{base_url}/download",
        f"{base_url}/data/download",
    ]
    
    # Also try to find export link in navigation
    try:
        # Look for export/download links in the page
        export_link_selectors = [
            'a:has-text("Export")',
            'a:has-text("Download")',
            'a:has-text("Data Export")',
            'a[href*="export"]',
            'a[href*="download"]',
            'button:has-text("Export")',
            'button:has-text("Download")',
        ]
        
        for selector in export_link_selectors:
            try:
                link = page.query_selector(selector)
                if link:
                    href = link.get_attribute('href')
                    if href:
                        if href.startswith('http'):
                            export_url = href
                        else:
                            export_url = f"{base_url}{href}"
                        logger.info(f"Found export link: {export_url}")
                        page.goto(export_url, wait_until="networkidle", timeout=30000)
                        return True
            except:
                continue
    except Exception as e:
        logger.warning(f"Could not find export link in navigation: {e}")
    
    # Try direct URLs
    for export_url in export_urls:
        try:
            logger.info(f"Trying export URL: {export_url}")
            page.goto(export_url, wait_until="networkidle", timeout=10000)
            # Check if we're on a valid page (not 404)
            if page.url != export_url or "404" not in page.content().lower():
                logger.info(f"Successfully navigated to export page: {page.url}")
                return True
        except:
            continue
    
    logger.error("Could not navigate to export page")
    return False


def set_date_range(page: Page, start_date: date, end_date: date) -> bool:
    """
    Set the date range for export.
    
    Args:
        page: Playwright page object
        start_date: Start date for export
        end_date: End date for export
        
    Returns:
        True if date range set successfully, False otherwise
    """
    try:
        # Format dates as strings (common formats)
        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")
        
        # Try to find date input fields
        date_selectors = [
            ('input[name*="start"]', 'input[name*="end"]'),
            ('input[name*="from"]', 'input[name*="to"]'),
            ('input[id*="start"]', 'input[id*="end"]'),
            ('input[id*="from"]', 'input[id*="to"]'),
            ('input[type="date"]', None),  # Single date picker
        ]
        
        for start_sel, end_sel in date_selectors:
            try:
                start_input = page.query_selector(start_sel)
                if start_input:
                    page.fill(start_sel, start_str)
                    logger.info(f"Set start date to {start_str}")
                    
                    if end_sel:
                        end_input = page.query_selector(end_sel)
                        if end_input:
                            page.fill(end_sel, end_str)
                            logger.info(f"Set end date to {end_str}")
                            return True
                    else:
                        # Single date field - might be end date
                        return True
            except:
                continue
        
        # If date inputs not found, might be using a date picker or dropdown
        # Try clicking on date fields to open picker
        logger.warning("Could not find date input fields - date range may need manual selection")
        return True  # Continue anyway - user might need to set manually
        
    except Exception as e:
        logger.error(f"Error setting date range: {e}")
        return False


def download_proteus_csv(
    browser: Browser,
    start_date: date,
    end_date: date,
    target_dir: Path
) -> Path:
    """
    Download Proteus CSV file for the given date range.
    
    Args:
        browser: Playwright browser object
        start_date: Start date for export
        end_date: End date for export
        target_dir: Directory where CSV should be saved
        
    Returns:
        Path to downloaded CSV file
        
    Raises:
        Exception if download fails
    """
    context = browser.new_context(accept_downloads=True)
    page = context.new_page()
    
    try:
        # Navigate to export page (assumes already logged in, but we'll check)
        if not navigate_to_export_page(page):
            raise Exception("Could not navigate to export page")
        
        # Set date range
        if not set_date_range(page, start_date, end_date):
            logger.warning("Could not set date range automatically")
        
        # Wait a moment for any date picker updates
        page.wait_for_timeout(1000)
        
        # Find and click export/download button
        export_button_selector = None
        for selector in [
            'button:has-text("Export")',
            'button:has-text("Download")',
            'button:has-text("Export CSV")',
            'button:has-text("Download CSV")',
            'a:has-text("Export CSV")',
            'a:has-text("Download CSV")',
            'button[type="submit"]',
            'input[type="submit"]',
            'button[id*="export"]',
            'button[id*="download"]',
        ]:
            try:
                if page.query_selector(selector):
                    export_button_selector = selector
                    break
            except:
                continue
        
        if not export_button_selector:
            # Take screenshot for debugging
            screenshot_path = target_dir / "proteus_export_page_debug.png"
            page.screenshot(path=str(screenshot_path))
            logger.error(f"Could not find export button. Screenshot saved to {screenshot_path}")
            raise Exception("Could not find export/download button on page")
        
        logger.info(f"Found export button with selector: {export_button_selector}")
        
        # Set up download tracking
        target_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate expected filename
        date_str = f"{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}"
        expected_filename = f"proteus_export_{date_str}.csv"
        target_path = target_dir / expected_filename
        
        logger.info(f"Starting download, expecting file: {expected_filename}")
        
        # Click export button and wait for download
        with page.expect_download(timeout=60000) as download_info:
            page.click(export_button_selector)
        
        download = download_info.value
        
        # Save the downloaded file
        downloaded_path = target_dir / download.suggested_filename
        download.save_as(str(downloaded_path))
        logger.info(f"Downloaded file: {downloaded_path}")
        
        # Rename to expected filename if different
        if downloaded_path != target_path:
            if target_path.exists():
                # Add timestamp if file exists
                import time
                timestamp = int(time.time())
                target_path = target_dir / f"proteus_export_{date_str}_{timestamp}.csv"
            shutil.move(str(downloaded_path), str(target_path))
            logger.info(f"Renamed to: {target_path}")
        
        return target_path
        
    finally:
        context.close()


def download_daily_csv(
    browser: Browser,
    target_dir: Path,
    start_date: date,
    end_date: date,
) -> Path:
    """
    Download Proteus CSV for the given date range.
    This is a convenience wrapper around download_proteus_csv.
    
    Args:
        browser: Playwright browser object (should have logged-in context)
        target_dir: Directory where CSV should be saved
        start_date: Start date for export
        end_date: End date for export
        
    Returns:
        Path to downloaded CSV file
    """
    return download_proteus_csv(browser, start_date, end_date, target_dir)
