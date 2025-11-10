"""
ETL pipeline for Proteus data.
"""
import logging
from datetime import datetime
from common.config import get_warehouse_engine
from common.db_utils import write_df
from common.id_utils import attach_athlete_uuid
from proteus.process_raw import clean_proteus, load_raw_proteus

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def etl_proteus():
    """Main ETL function for Proteus data."""
    logger.info("Starting Proteus ETL...")
    
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
    etl_proteus()

