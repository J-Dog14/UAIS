"""
Main orchestration script for Proteus data processing.
Handles web automation (download) and ETL processing.
"""
import os
import sys
import logging
from pathlib import Path

# Add python directory to path
project_root = Path(__file__).parent.parent.parent
python_dir = project_root / "python"
if str(python_dir) not in sys.path:
    sys.path.insert(0, str(python_dir))

# Try to load .env file if python-dotenv is available
try:
    from dotenv import load_dotenv
    env_path = project_root / '.env'
    if env_path.exists():
        load_dotenv(env_path)
        logging.info(f"Loaded environment variables from {env_path}")
except ImportError:
    # python-dotenv not installed, skip .env loading
    pass

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """
    Main execution function.
    Runs the daily Proteus job: download + ETL.
    """
    # Check for required environment variables
    required_vars = ['PROTEUS_EMAIL', 'PROTEUS_PASSWORD']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
        logger.error("Set them with:")
        for var in missing_vars:
            logger.error(f"  set {var}=your_value")
        return
    
    try:
        # Run the daily job (download + ETL)
        from proteus.web.runner import run_daily_proteus_job
        run_daily_proteus_job()
    except Exception as e:
        logger.error(f"Error running Proteus job: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
