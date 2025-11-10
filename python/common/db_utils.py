"""
Database utility functions for UAIS.
Provides helpers for reading/writing DataFrames to SQL databases.
"""
import pandas as pd
from sqlalchemy import Engine, text
from typing import Optional


def read_table_as_df(engine: Engine, table_name: str, schema: Optional[str] = None) -> pd.DataFrame:
    """
    Read a database table into a pandas DataFrame.
    
    Args:
        engine: SQLAlchemy engine connected to the database.
        table_name: Name of the table to read.
        schema: Optional schema name (for Postgres).
    
    Returns:
        DataFrame containing all rows from the table.
    """
    if schema:
        full_table = f"{schema}.{table_name}"
    else:
        full_table = table_name
    
    return pd.read_sql_table(table_name, engine, schema=schema)


def write_df(df: pd.DataFrame, table_name: str, engine: Engine, 
             if_exists: str = 'append', schema: Optional[str] = None,
             index: bool = False) -> None:
    """
    Write a pandas DataFrame to a database table.
    
    Args:
        df: DataFrame to write.
        table_name: Target table name.
        engine: SQLAlchemy engine connected to the database.
        if_exists: Behavior if table exists ('fail', 'replace', 'append').
        schema: Optional schema name (for Postgres).
        index: Whether to write DataFrame index as a column.
    """
    df.to_sql(
        table_name,
        engine,
        schema=schema,
        if_exists=if_exists,
        index=index,
        method='multi'  # Faster for bulk inserts
    )


def execute_query(engine: Engine, query: str, params: Optional[dict] = None) -> pd.DataFrame:
    """
    Execute a SQL query and return results as a DataFrame.
    
    Args:
        engine: SQLAlchemy engine.
        query: SQL query string.
        params: Optional parameters for parameterized queries.
    
    Returns:
        DataFrame with query results.
    """
    with engine.connect() as conn:
        if params:
            result = conn.execute(text(query), params)
        else:
            result = conn.execute(text(query))
        return pd.DataFrame(result.fetchall(), columns=result.keys())


def table_exists(engine: Engine, table_name: str, schema: Optional[str] = None) -> bool:
    """
    Check if a table exists in the database.
    
    Args:
        engine: SQLAlchemy engine.
        table_name: Name of the table to check.
        schema: Optional schema name.
    
    Returns:
        True if table exists, False otherwise.
    """
    from sqlalchemy import inspect
    inspector = inspect(engine)
    if schema:
        return table_name in inspector.get_table_names(schema=schema)
    return table_name in inspector.get_table_names()


if __name__ == "__main__":
    # Example usage
    from common.config import get_warehouse_engine
    
    engine = get_warehouse_engine()
    print(f"Testing connection to: {engine.url}")
    
    # List tables
    from sqlalchemy import inspect
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    print(f"Available tables: {tables}")

