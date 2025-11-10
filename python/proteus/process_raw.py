"""
Raw data processing for Proteus domain.
"""
import pandas as pd
from pathlib import Path
from typing import Optional
from common.config import get_raw_paths
from common.io_utils import load_csv, find_files


def load_raw_proteus(raw_dir: Optional[str] = None) -> pd.DataFrame:
    """Load raw Proteus data files."""
    if raw_dir is None:
        paths = get_raw_paths()
        raw_dir = paths.get('proteus')
    
    if not raw_dir:
        raise ValueError("Proteus raw data path not configured")
    
    raw_path = Path(raw_dir)
    if not raw_path.exists():
        raise FileNotFoundError(f"Proteus raw data directory not found: {raw_path}")
    
    csv_files = find_files(raw_path, "*.csv", recursive=True)
    
    if not csv_files:
        return pd.DataFrame()
    
    dfs = []
    for csv_file in csv_files:
        try:
            df = load_csv(csv_file)
            df['source_file'] = str(csv_file)
            dfs.append(df)
        except Exception as e:
            print(f"Warning: Could not load {csv_file}: {e}")
    
    if not dfs:
        return pd.DataFrame()
    
    return pd.concat(dfs, ignore_index=True)


def clean_proteus(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and normalize Proteus data."""
    if df.empty:
        return df
    
    clean_df = df.copy()
    clean_df.columns = clean_df.columns.str.lower().str.replace(' ', '_')
    
    # Extract athlete identifier
    athlete_cols = ['name', 'athlete_name', 'athlete_id', 'subject_id']
    for col in athlete_cols:
        if col in clean_df.columns:
            clean_df['source_athlete_id'] = clean_df[col].astype(str)
            break
    else:
        clean_df['source_athlete_id'] = None
    
    # Extract session date
    date_cols = ['date', 'test_date', 'session_date']
    for col in date_cols:
        if col in clean_df.columns:
            clean_df['session_date'] = pd.to_datetime(clean_df[col]).dt.date
            break
    else:
        clean_df['session_date'] = None
    
    # TODO: Add Proteus-specific cleaning
    
    return clean_df


if __name__ == "__main__":
    try:
        raw_df = load_raw_proteus()
        print(f"Loaded {len(raw_df)} raw rows")
        clean_df = clean_proteus(raw_df)
        print(f"Cleaned {len(clean_df)} rows")
    except Exception as e:
        print(f"Error: {e}")

