"""Database utilities for connecting to DuckDB databases"""
import duckdb
import logging
from config.database import DatabaseConfig

logger = logging.getLogger(__name__)


def get_connection(attach_clinisys=False, read_only=True):
    """
    Create and return a connection to the huntington_data_lake database.
    
    Args:
        attach_clinisys: If True, attach clinisys_all database
        read_only: If True, open database in read-only mode
    
    Returns:
        duckdb.DuckDBPyConnection: Database connection
    """
    conn = duckdb.connect(DatabaseConfig.HUNTINGTON_DB, read_only=read_only)
    logger.info(f"Connected to {DatabaseConfig.HUNTINGTON_DB} (read_only={read_only})")
    
    if attach_clinisys:
        conn.execute(f"ATTACH '{DatabaseConfig.CLINISYS_DB}' AS clinisys (READ_ONLY)")
        logger.info(f"Attached clinisys database: {DatabaseConfig.CLINISYS_DB}")
    
    return conn


def get_available_columns(conn, table_name="gold.embryoscope_clinisys_combined"):
    """
    Get list of available columns in a table.
    
    Args:
        conn: Database connection
        table_name: Name of the table to query
    
    Returns:
        list: List of column names
    """
    col_info = conn.execute(f"DESCRIBE {table_name}").df()
    columns = col_info['column_name'].tolist()
    logger.info(f"Found {len(columns)} columns in {table_name}")
    return columns
