"""
Database setup and management for Athletic Screen.
Handles schema creation, connections, and table operations.
"""
import sqlite3
import os
from typing import Dict, Optional


# Table schemas for each movement type
TABLE_SCHEMAS = {
    'CMJ': '''CREATE TABLE IF NOT EXISTS CMJ (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT,
                date TEXT,
                trial_name TEXT,
                JH_IN REAL,
                Peak_Power REAL,
                PP_FORCEPLATE REAL,
                Force_at_PP REAL,
                Vel_at_PP REAL,
                PP_W_per_kg REAL
              )''',
    'PPU': '''CREATE TABLE IF NOT EXISTS PPU (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT,
                date TEXT,
                trial_name TEXT,
                JH_IN REAL,
                Peak_Power REAL,
                PP_FORCEPLATE REAL,
                Force_at_PP REAL,
                Vel_at_PP REAL,
                PP_W_per_kg REAL
            )''',
    'DJ':  '''CREATE TABLE IF NOT EXISTS DJ (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT,
                date TEXT,
                trial_name TEXT,
                JH_IN REAL,
                PP_FORCEPLATE REAL,
                Force_at_PP REAL,
                Vel_at_PP REAL,
                PP_W_per_kg REAL,
                CT REAL,
                RSI REAL
              )''',
    'SLV': '''CREATE TABLE IF NOT EXISTS SLV (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT,
                date TEXT, 
                trial_name TEXT,
                side TEXT,
                JH_IN REAL,
                PP_FORCEPLATE REAL,
                Force_at_PP REAL,
                Vel_at_PP REAL,
                PP_W_per_kg REAL
              )''',
    'NMT': '''CREATE TABLE IF NOT EXISTS NMT (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT,
                date TEXT, 
                trial_name TEXT,
                NUM_TAPS_10s REAL,
                NUM_TAPS_20s REAL,
                NUM_TAPS_30s REAL,
                NUM_TAPS REAL
              )'''
}

# Power analysis metric columns to add to tables
POWER_METRIC_COLS = [
    ("peak_power_w",          "REAL"),
    ("time_to_peak_s",        "REAL"),
    ("rpd_max_w_per_s",       "REAL"),
    ("time_to_rpd_max_s",     "REAL"),
    ("rise_time_10_90_s",     "REAL"),
    ("fwhm_s",                "REAL"),
    ("auc_j",                 "REAL"),
    ("work_early_pct",        "REAL"),
    ("decay_90_10_s",         "REAL"),
    ("t_com_norm_0to1",       "REAL"),
    ("skewness",              "REAL"),
    ("kurtosis",              "REAL"),
    ("spectral_centroid_hz",  "REAL"),
]

# Tables that support power analysis
POWER_ANALYSIS_TABLES = ["CMJ", "DJ", "PPU", "SLV"]


def create_database(db_path: str, reset: bool = False) -> sqlite3.Connection:
    """
    Create or connect to the Athletic Screen database.
    
    Args:
        db_path: Path to the database file.
        reset: If True, delete existing database and create fresh.
    
    Returns:
        SQLite connection object.
    """
    if reset and os.path.exists(db_path):
        os.remove(db_path)
        print(f"Deleted existing database at {db_path}")
    
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout=5000;")
    
    return conn


def create_tables(conn: sqlite3.Connection):
    """
    Create all movement tables in the database.
    
    Args:
        conn: SQLite connection object.
    """
    cursor = conn.cursor()
    for schema in TABLE_SCHEMAS.values():
        cursor.execute(schema)
    conn.commit()


def column_exists(cursor: sqlite3.Cursor, table: str, col: str) -> bool:
    """
    Check if a column exists in a table.
    
    Args:
        cursor: SQLite cursor.
        table: Table name.
        col: Column name.
    
    Returns:
        True if column exists, False otherwise.
    """
    cursor.execute(f"PRAGMA table_info({table})")
    return any(row[1].lower() == col.lower() for row in cursor.fetchall())


def ensure_power_metric_columns(conn: sqlite3.Connection, table: str):
    """
    Ensure power analysis metric columns exist in a table.
    
    Args:
        conn: SQLite connection.
        table: Table name.
    """
    cursor = conn.cursor()
    for col, sqltype in POWER_METRIC_COLS:
        if not column_exists(cursor, table, col):
            cursor.execute(f"ALTER TABLE {table} ADD COLUMN {col} {sqltype}")
    conn.commit()


def insert_row(cursor: sqlite3.Cursor, table: str, cols: list, vals: list):
    """
    Insert a row into a table.
    
    Args:
        cursor: SQLite cursor.
        table: Table name.
        cols: List of column names.
        vals: List of values (must match cols order).
    """
    placeholders = ",".join(["?"] * len(vals))
    cursor.execute(f"INSERT INTO {table} ({','.join(cols)}) VALUES ({placeholders})", vals)


def get_connection(db_path: str) -> sqlite3.Connection:
    """
    Get a connection to the database with proper settings.
    
    Args:
        db_path: Path to database file.
    
    Returns:
        SQLite connection.
    """
    conn = sqlite3.connect(db_path, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout=5000;")
    return conn

