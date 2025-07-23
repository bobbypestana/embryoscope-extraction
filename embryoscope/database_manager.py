"""
Database Manager for Embryoscope Data Extraction
Handles DuckDB operations and incremental extraction logic.
"""

import duckdb
import pandas as pd
import os
import logging
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
import hashlib
import json
from schema_config import get_table_schema, get_supported_data_types, validate_data_type


def check_db_lock(db_path):
    """Check if the DuckDB file can be opened (not locked by another process)."""
    import duckdb
    try:
        conn = duckdb.connect(db_path)
        conn.close()
        return True
    except Exception as e:
        print(f"ERROR: Cannot open database file '{db_path}'. It may be locked by another process.\nDetails: {e}")
        return False


class EmbryoscopeDatabaseManager:
    """Manages DuckDB database operations for embryoscope data."""
    
    def __init__(self, db_path: str, schema: str = 'embryoscope'):
        """
        Initialize the database manager.
        
        Args:
            db_path: Path to DuckDB database file
            schema: Database schema name
        """
        self.db_path = db_path
        self.schema = schema
        self.logger = logging.getLogger("embryoscope_database")
        
        # Ensure database directory exists
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        
        # Check for DB lock before proceeding
        if not check_db_lock(db_path):
            raise RuntimeError(f"Database file '{db_path}' is locked. Please close all other processes using this file and try again.")
        
        # Initialize database
        self._init_database()
    
    def _init_database(self):
        """Initialize database schema and tables."""
        try:
            with duckdb.connect(self.db_path) as conn:
                # Only create bronze schema
                conn.execute(f"CREATE SCHEMA IF NOT EXISTS bronze")
                # Create metadata tables (default schema)
                self._create_metadata_tables(conn)
                # Create data tables (default schema)
                self._create_data_tables(conn)
        except Exception as e:
            if "being used by another process" in str(e):
                self.logger.warning(f"Database file is locked, will retry: {e}")
                # Wait a bit and retry
                import time
                time.sleep(2)
                with duckdb.connect(self.db_path) as conn:
                    # Only create bronze schema
                    conn.execute(f"CREATE SCHEMA IF NOT EXISTS bronze")
                    # Create metadata tables (default schema)
                    self._create_metadata_tables(conn)
                    # Create data tables (default schema)
                    self._create_data_tables(conn)
            else:
                raise
    
    def _create_metadata_tables(self, conn):
        """Create metadata tables for tracking extractions."""
        # Remove schema from table names
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS view_metadata (
                view_name VARCHAR PRIMARY KEY,
                location VARCHAR,
                last_extraction_timestamp TIMESTAMP,
                last_row_count INTEGER,
                last_data_hash VARCHAR,
                change_detection_method VARCHAR,
                extraction_strategy VARCHAR,
                batch_size INTEGER DEFAULT 1000,
                parallel_processing BOOLEAN DEFAULT TRUE,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Incremental runs table
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS incremental_runs (
                run_id VARCHAR PRIMARY KEY,
                location VARCHAR,
                extraction_timestamp TIMESTAMP,
                total_views INTEGER,
                full_extractions INTEGER,
                incremental_extractions INTEGER,
                total_rows_processed INTEGER,
                processing_time_seconds REAL,
                status VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Row changes table
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS row_changes (
                view_name VARCHAR,
                location VARCHAR,
                row_hash VARCHAR,
                change_type VARCHAR,
                extraction_timestamp TIMESTAMP,
                run_id VARCHAR,
                PRIMARY KEY (view_name, location, row_hash, extraction_timestamp)
            )
        """)
    
    def _create_data_tables(self, conn):
        """Create data tables for storing embryoscope data."""
        for data_type in get_supported_data_types():
            schema = get_table_schema(data_type)
            if not schema:
                self.logger.warning(f"No schema found for data type: {data_type}")
                continue
            
            columns = schema['columns']
            primary_key = schema['primary_key']
            
            # Build CREATE TABLE statement
            columns_sql = ', '.join(columns)
            primary_key_sql = ', '.join(primary_key)
            
            create_sql = f"""
                CREATE TABLE IF NOT EXISTS data_{data_type} (
                    {columns_sql},
                    PRIMARY KEY ({primary_key_sql})
                )
            """
            
            conn.execute(create_sql)
            self.logger.debug(f"Created/verified table: data_{data_type}")
    
    def _create_bronze_tables(self, conn):
        """Create bronze (raw) tables for each data type."""
        bronze_schema = 'bronze'
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {bronze_schema}")
        # Table definitions for each data type
        bronze_defs = {
            'patients': ['PatientIDx VARCHAR', 'raw_json TEXT', '_extraction_timestamp TIMESTAMP', '_run_id VARCHAR', '_location VARCHAR', '_row_hash VARCHAR'],
            'treatments': ['PatientIDx VARCHAR', 'TreatmentName VARCHAR', 'raw_json TEXT', '_extraction_timestamp TIMESTAMP', '_run_id VARCHAR', '_location VARCHAR', '_row_hash VARCHAR'],
            'embryo_data': ['EmbryoID VARCHAR', 'PatientIDx VARCHAR', 'TreatmentName VARCHAR', 'raw_json TEXT', '_extraction_timestamp TIMESTAMP', '_run_id VARCHAR', '_location VARCHAR', '_row_hash VARCHAR'],
            'idascore': ['EmbryoID VARCHAR', 'raw_json TEXT', '_extraction_timestamp TIMESTAMP', '_run_id VARCHAR', '_location VARCHAR', '_row_hash VARCHAR'],
        }
        for data_type, columns in bronze_defs.items():
            table = f"{bronze_schema}.raw_{data_type}"
            columns_sql = ', '.join(columns)
            conn.execute(f"CREATE TABLE IF NOT EXISTS {table} ({columns_sql})")

    def save_bronze_raw(self, data_type: str, records: list, extraction_timestamp, run_id, location):
        """Save raw records to bronze layer, deduplicated by business key + hash."""
        bronze_schema = 'bronze'
        table = f"{bronze_schema}.raw_{data_type}"
        if not records:
            return 0
        # Determine business keys
        if data_type == 'patients':
            key_fields = ['PatientIDx']
        elif data_type == 'treatments':
            key_fields = ['PatientIDx', 'TreatmentName']
        elif data_type == 'embryo_data':
            key_fields = ['EmbryoID', 'PatientIDx', 'TreatmentName']
        elif data_type == 'idascore':
            key_fields = ['EmbryoID']
        else:
            raise ValueError(f"Unknown data_type: {data_type}")
        # Prepare DataFrame
        import hashlib, json
        rows = []
        for rec in records:
            # rec is a dict with all fields from API
            raw_json = json.dumps(rec, default=str)
            row_hash = hashlib.md5(raw_json.encode()).hexdigest()
            row = {k: rec.get(k, None) for k in key_fields}
            row['raw_json'] = raw_json
            row['_extraction_timestamp'] = extraction_timestamp
            row['_run_id'] = run_id
            row['_location'] = location
            row['_row_hash'] = row_hash
            rows.append(row)
        import pandas as pd
        df = pd.DataFrame(rows)
        # Deduplicate: only insert if (business key + hash) not already present
        with duckdb.connect(self.db_path) as conn:
            self._create_bronze_tables(conn)
            # Build where clause for deduplication
            if not df.empty:
                # Compose a unique key for deduplication
                key_cols = key_fields + ['_row_hash']
                # Find existing hashes
                where_clauses = [f"{k} = ?" for k in key_fields]
                select_sql = f"SELECT {', '.join(key_fields)}, _row_hash FROM {table} WHERE _location = ?"
                existing = conn.execute(select_sql, [location]).fetchall()
                existing_set = set(tuple(row[:len(key_fields)] + (row[-1],)) for row in existing)
                # Only keep new/changed
                def is_new(row):
                    return tuple(row[k] for k in key_fields) + (row['_row_hash'],) not in existing_set
                df_new = df[df.apply(is_new, axis=1)]
                if not df_new.empty:
                    conn.register('df_new', df_new)
                    conn.execute(f"INSERT INTO {table} SELECT * FROM df_new")
                    conn.unregister('df_new')
                return len(df_new)
            return 0
    
    def _get_table_name(self, data_type: str) -> str:
        """Get the table name for a data type."""
        return f"data_{data_type}"
    
    def _get_view_metadata(self, conn, view_name: str, location: str) -> Optional[Dict[str, Any]]:
        """Get metadata for a specific view and location."""
        result = conn.execute(f"""
            SELECT * FROM view_metadata 
            WHERE view_name = ? AND location = ?
        """, [view_name, location]).fetchone()
        
        if result:
            columns = [desc[0] for desc in conn.description]
            return dict(zip(columns, result))
        return None
    
    def _update_view_metadata(self, conn, view_name: str, location: str, metadata: Dict[str, Any]):
        """Update metadata for a specific view and location."""
        conn.execute(f"""
            INSERT OR REPLACE INTO view_metadata 
            (view_name, location, last_extraction_timestamp, last_row_count, last_data_hash, 
             change_detection_method, extraction_strategy, batch_size, parallel_processing, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """, [
            view_name, location, metadata.get('last_extraction_timestamp'),
            metadata.get('last_row_count'), metadata.get('last_data_hash'),
            metadata.get('change_detection_method'), metadata.get('extraction_strategy'),
            metadata.get('batch_size', 1000), metadata.get('parallel_processing', True)
        ])
    
    def _get_existing_hashes(self, conn, table_name: str, location: str) -> set:
        """Get existing row hashes for a table and location."""
        result = conn.execute(f"""
            SELECT DISTINCT _row_hash FROM {table_name}
            WHERE _location = ? AND _extraction_timestamp = (
                SELECT MAX(_extraction_timestamp) FROM {table_name} WHERE _location = ?
            )
        """, [location, location]).fetchall()
        
        return {row[0] for row in result}
    
    def _insert_data_incremental(self, conn, df: pd.DataFrame, table_name: str, location: str, run_id: str) -> int:
        """
        Insert data incrementally, only new/changed rows.
        
        Args:
            conn: Database connection
            df: Dataframe to insert
            table_name: Target table name
            location: Location identifier
            run_id: Run identifier
            
        Returns:
            Number of rows inserted
        """
        if df.empty:
            return 0
        
        # Get existing hashes
        existing_hashes = list(self._get_existing_hashes(conn, table_name, location))
        
        # Filter for new/changed rows
        new_rows = df[~df['_row_hash'].isin(existing_hashes)]
        
        if new_rows.empty:
            self.logger.info(f"No new/changed rows for {table_name} at {location}")
            return 0
        
        # Insert new rows using DuckDB's DataFrame insertion
        conn.register("new_rows", new_rows)
        conn.execute(f"INSERT INTO {table_name} SELECT * FROM new_rows")
        conn.unregister("new_rows")
        
        self.logger.info(f"Inserted {len(new_rows)} new/changed rows into {table_name} for {location}")
        return len(new_rows)
    
    def save_data(self, dataframes: Dict[str, pd.DataFrame], location: str, run_id: str, 
                  extraction_timestamp: datetime) -> Dict[str, int]:
        """
        Save processed data to database with incremental logic.
        
        Args:
            dataframes: Dictionary of dataframes by data type
            location: Location identifier
            run_id: Run identifier
            extraction_timestamp: Extraction timestamp
            
        Returns:
            Dictionary with row counts for each data type
        """
        row_counts = {}
        
        # Retry logic for database file locking
        max_retries = 3
        retry_delay = 2
        
        for attempt in range(max_retries):
            try:
                with duckdb.connect(self.db_path) as conn:
                    for data_type, df in dataframes.items():
                        if df.empty:
                            row_counts[data_type] = 0
                            continue
                        
                        table_name = self._get_table_name(data_type)
                        
                        # Insert data incrementally
                        inserted_rows = self._insert_data_incremental(conn, df, table_name, location, run_id)
                        row_counts[data_type] = inserted_rows
                        
                        # Update metadata
                        metadata = {
                            'last_extraction_timestamp': extraction_timestamp,
                            'last_row_count': len(df),
                            'last_data_hash': self._generate_data_hash(df),
                            'change_detection_method': 'hash_based',
                            'extraction_strategy': 'incremental'
                        }
                        self._update_view_metadata(conn, data_type, location, metadata)
                
                # If we get here, the operation was successful
                return row_counts
                
            except Exception as e:
                if "being used by another process" in str(e) and attempt < max_retries - 1:
                    self.logger.warning(f"Database file is locked (attempt {attempt + 1}/{max_retries}), retrying in {retry_delay} seconds: {e}")
                    import time
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                else:
                    self.logger.error(f"Failed to save data after {max_retries} attempts: {e}")
                    raise
        
        return row_counts
    
    def _generate_data_hash(self, df: pd.DataFrame) -> str:
        """Generate hash for entire dataframe."""
        if df.empty:
            return hashlib.md5(b"").hexdigest()
        
        # Sort by row hash for consistent hashing
        sorted_hashes = sorted(df['_row_hash'].tolist())
        data_str = json.dumps(sorted_hashes, sort_keys=True)
        return hashlib.md5(data_str.encode()).hexdigest()
    
    def get_latest_data(self, data_type: str, location: str) -> pd.DataFrame:
        """
        Get the latest data for a specific type and location.
        
        Args:
            data_type: Type of data (patients, treatments, embryo_data, idascore)
            location: Location identifier
            
        Returns:
            Dataframe with latest data
        """
        table_name = self._get_table_name(data_type)
        
        with duckdb.connect(self.db_path) as conn:
            query = f"""
                SELECT * FROM {table_name}
                WHERE _location = ? AND _extraction_timestamp = (
                    SELECT MAX(_extraction_timestamp) FROM {table_name} WHERE _location = ?
                )
            """
            return conn.execute(query, [location, location]).df()
    
    def get_all_existing_pairs(self, location: str) -> set:
        """
        Get all existing patient-treatment pairs from the database.
        This is used for incremental extraction to avoid re-fetching existing data.
        
        Args:
            location: Location identifier
            
        Returns:
            Set of tuples (PatientIDx, TreatmentName) representing existing pairs
        """
        with duckdb.connect(self.db_path) as conn:
            query = """
                SELECT DISTINCT PatientIDx, TreatmentName 
                FROM data_treatments 
                WHERE _location = ?
            """
            result = conn.execute(query, [location]).fetchall()
            return set((row[0], row[1]) for row in result)
    
    def get_data_summary(self, location: str = None) -> Dict[str, Any]:
        """
        Get summary of data in the database.
        
        Args:
            location: Optional location filter
            
        Returns:
            Dictionary with data summary
        """
        summary = {}
        
        with duckdb.connect(self.db_path) as conn:
            data_types = ['patients', 'treatments', 'embryo_data', 'idascore']
            
            for data_type in data_types:
                table_name = self._get_table_name(data_type)
                
                if location:
                    query = f"""
                        SELECT COUNT(*) as count, 
                               MAX(_extraction_timestamp) as last_extraction
                        FROM {table_name}
                        WHERE _location = ?
                    """
                    result = conn.execute(query, [location]).fetchone()
                else:
                    query = f"""
                        SELECT COUNT(*) as count, 
                               MAX(_extraction_timestamp) as last_extraction
                        FROM {table_name}
                    """
                    result = conn.execute(query).fetchone()
                
                summary[data_type] = {
                    'count': result[0] if result[0] else 0,
                    'last_extraction': result[1] if result[1] else None
                }
        
        return summary
    
    def get_extraction_history(self, location: str = None, limit: int = 10) -> pd.DataFrame:
        """
        Get extraction history.
        
        Args:
            location: Optional location filter
            limit: Number of records to return
            
        Returns:
            Dataframe with extraction history
        """
        with duckdb.connect(self.db_path) as conn:
            if location:
                query = f"""
                    SELECT * FROM incremental_runs
                    WHERE location = ?
                    ORDER BY extraction_timestamp DESC
                    LIMIT ?
                """
                return conn.execute(query, [location, limit]).df()
            else:
                query = f"""
                    SELECT * FROM incremental_runs
                    ORDER BY extraction_timestamp DESC
                    LIMIT ?
                """
                return conn.execute(query, [limit]).df()
    
    def cleanup_old_data(self, days_to_keep: int = 30):
        """
        Clean up old data, keeping only the specified number of days.
        
        Args:
            days_to_keep: Number of days of data to keep
        """
        with duckdb.connect(self.db_path) as conn:
            data_types = ['patients', 'treatments', 'embryo_data', 'idascore']
            
            for data_type in data_types:
                table_name = self._get_table_name(data_type)
                
                # Delete old data
                conn.execute(f"""
                    DELETE FROM {table_name}
                    WHERE _extraction_timestamp < DATE_SUB(CURRENT_TIMESTAMP, INTERVAL {days_to_keep} DAY)
                """)
                
                self.logger.info(f"Cleaned up old data from {table_name}")


if __name__ == "__main__":
    # Test the database manager
    db_manager = EmbryoscopeDatabaseManager("../database/embryoscope_test.duckdb")
    
    # Test data
    test_df = pd.DataFrame({
        'PatientIDx': ['TEST001', 'TEST002'],
        'Name': ['Test Patient 1', 'Test Patient 2'],
        '_location': ['Test Location', 'Test Location'],
        '_extraction_timestamp': [datetime.now(), datetime.now()],
        '_run_id': ['test_run', 'test_run'],
        '_row_hash': ['hash1', 'hash2']
    })
    
    # Save test data
    dataframes = {'patients': test_df}
    row_counts = db_manager.save_data(dataframes, 'Test Location', 'test_run', datetime.now())
    print(f"Row counts: {row_counts}")
    
    # Get summary
    summary = db_manager.get_data_summary('Test Location')
    print(f"Summary: {summary}") 