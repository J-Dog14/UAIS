"""
Login automation for Proteus web portal.
"""
import logging
from playwright.sync_api import Page, TimeoutError as PlaywrightTimeoutError
from .config import get_proteus_base_url, get_proteus_email, get_proteus_password, get_proteus_location

logger = logging.getLogger(__name__)


def login_to_proteus(page: Page) -> bool:
    """
    Log into Proteus web portal.
    
    Args:
        page: Playwright page object
        
    Returns:
        True if login successful, False otherwise
    """
    base_url = get_proteus_base_url()
    login_url = f"{base_url}/login"
    
    logger.info(f"Navigating to login page: {login_url}")
    try:
        page.goto(login_url, wait_until="networkidle", timeout=30000)
    except PlaywrightTimeoutError as e:
        logger.error(f"Timeout loading login page: {e}")
        return False
    except Exception as e:
        logger.error(f"Error loading login page: {e}")
        return False
    
    # Wait for login form to be visible
    try:
        # Look for email/username input field
        # Common selectors: input[type="email"], input[name="email"], input[id*="email"]
        email_selector = None
        for selector in ['input[type="email"]', 'input[name="email"]', 'input[id*="email"]', 
                        'input[name="username"]', 'input[id*="username"]', 'input[placeholder*="email" i]']:
            try:
                page.wait_for_selector(selector, timeout=5000)
                email_selector = selector
                break
            except:
                continue
        
        if not email_selector:
            logger.error("Could not find email input field on login page")
            return False
        
        logger.info(f"Found email field with selector: {email_selector}")
        
        # Fill email
        email = get_proteus_email()
        page.fill(email_selector, email)
        logger.info("Filled email field")
        
        # Look for password field
        password_selector = None
        for selector in ['input[type="password"]', 'input[name="password"]', 'input[id*="password"]']:
            try:
                page.wait_for_selector(selector, timeout=5000)
                password_selector = selector
                break
            except:
                continue
        
        if not password_selector:
            logger.error("Could not find password input field on login page")
            return False
        
        logger.info(f"Found password field with selector: {password_selector}")
        
        # Fill password
        password = get_proteus_password()
        page.fill(password_selector, password)
        logger.info("Filled password field")
        
        # Look for location field (if present)
        location = get_proteus_location()
        location_selector = None
        for selector in ['input[name="location"]', 'input[id*="location"]', 'select[name="location"]', 
                        'select[id*="location"]', 'input[placeholder*="location" i]']:
            try:
                if page.query_selector(selector):
                    location_selector = selector
                    break
            except:
                continue
        
        if location_selector:
            logger.info(f"Found location field with selector: {location_selector}")
            if 'select' in location_selector:
                page.select_option(location_selector, location)
            else:
                page.fill(location_selector, location)
            logger.info("Filled location field")
        
        # Find and click login button
        login_button_selector = None
        for selector in ['button[type="submit"]', 'button:has-text("Login")', 'button:has-text("Sign In")',
                        'input[type="submit"]', 'button[id*="login"]', 'button[id*="signin"]']:
            try:
                if page.query_selector(selector):
                    login_button_selector = selector
                    break
            except:
                continue
        
        if not login_button_selector:
            logger.error("Could not find login button")
            return False
        
        logger.info(f"Found login button with selector: {login_button_selector}")
        logger.info("Clicking login button...")
        
        # Click login and wait for navigation
        with page.expect_navigation(wait_until="networkidle", timeout=30000):
            page.click(login_button_selector)
        
        # Wait a bit for any redirects
        page.wait_for_timeout(2000)
        
        # Check if we're logged in (not on login page anymore)
        current_url = page.url
        if "/login" in current_url.lower():
            logger.error("Still on login page after login attempt - login may have failed")
            # Check for error messages
            error_selectors = ['.error', '.alert', '[class*="error"]', '[class*="alert"]']
            for selector in error_selectors:
                error_elem = page.query_selector(selector)
                if error_elem:
                    error_text = error_elem.inner_text()
                    logger.error(f"Login error message: {error_text}")
            return False
        
        logger.info(f"Login successful! Current URL: {current_url}")
        return True
        
    except PlaywrightTimeoutError as e:
        logger.error(f"Timeout during login process: {e}")
        return False
    except Exception as e:
        logger.error(f"Error during login: {e}", exc_info=True)
        return False
