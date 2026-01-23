#!/usr/bin/env python3
"""
Planilha Embriologia Silver Transformation
Combines all bronze tables (one per Excel file) into a single silver table with:
- Standardized column names (normalized strings)
- Proper data type casting
- Data cleaning
"""

import logging
import pandas as pd
import numpy as np
import duckdb
from datetime import datetime
import os
import unicodedata
import re

# Setup logging
LOGS_DIR = os.path.join(os.path.dirname(__file__), 'logs')
os.makedirs(LOGS_DIR, exist_ok=True)
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
script_name = os.path.splitext(os.path.basename(__file__))[0]
LOG_PATH = os.path.join(LOGS_DIR, f'{script_name}_{timestamp}.log')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.FileHandler(LOG_PATH),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration
DUCKDB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'database', 'huntington_data_lake.duckdb')
BRONZE_PATTERN = 'planilha_%'  # Pattern to match all Planilha tables
SHEET_TYPES = ['fet', 'fresh']  # Process each sheet type separately

def get_duckdb_connection():
    """Create DuckDB connection"""
    try:
        logger.info(f"Attempting to connect to DuckDB at: {DUCKDB_PATH}")
        con = duckdb.connect(DUCKDB_PATH)
        logger.info("DuckDB connection successful")
        return con
    except Exception as e:
        logger.error(f"Failed to connect to DuckDB: {e}")
        raise

def normalize_column_name(col_name):
    """Normalize column name: remove accents, lowercase, trim spaces, handle special chars"""
    if pd.isna(col_name) or col_name is None:
        return None
    
    # Convert to string
    col_str = str(col_name).strip()
    
    # Remove accents/diacritics
    col_str = unicodedata.normalize('NFD', col_str)
    col_str = ''.join(c for c in col_str if unicodedata.category(c) != 'Mn')
    
    # Convert to lowercase
    col_str = col_str.lower()
    
    # Replace multiple spaces with single space
    col_str = re.sub(r'\s+', ' ', col_str)
    
    # Trim spaces
    col_str = col_str.strip()
    
    return col_str

def get_bronze_tables(con, sheet_type=None):
    """Get all bronze tables matching the pattern, optionally filtered by sheet type"""
    try:
        query = f"""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'bronze' 
        AND table_name LIKE '{BRONZE_PATTERN}'
        ORDER BY table_name
        """
        result = con.execute(query).fetchall()
        tables = [row[0] for row in result]
        
        # Filter by sheet type if specified
        if sheet_type:
            tables = [t for t in tables if t.endswith(f'_{sheet_type}')]
            logger.info(f"Found {len(tables)} bronze tables for sheet type '{sheet_type}': {tables}")
        else:
            logger.info(f"Found {len(tables)} bronze tables matching pattern '{BRONZE_PATTERN}': {tables}")
        
        return tables
    except Exception as e:
        logger.error(f"Error getting bronze tables: {e}")
        return []

def collect_all_columns_from_tables(con, bronze_tables):
    """Collect all unique columns from all bronze tables and create standardization mapping"""
    logger.info("Collecting columns from all bronze tables...")
    
    all_original_columns = {}  # normalized_name -> list of original names
    table_columns = {}  # table_name -> list of original columns
    
    for table_name in bronze_tables:
        try:
            # Get column names from table
            columns_info = con.execute(f"DESCRIBE bronze.{table_name}").fetchdf()
            # Exclude metadata columns
            original_cols = [col for col in columns_info['column_name'].tolist() 
                           if col not in ['line_number', 'extraction_timestamp', 'file_name', 'sheet_name']]
            table_columns[table_name] = original_cols
            
            # Normalize each column name
            for orig_col in original_cols:
                normalized = normalize_column_name(orig_col)
                if normalized:
                    if normalized not in all_original_columns:
                        all_original_columns[normalized] = []
                    if orig_col not in all_original_columns[normalized]:
                        all_original_columns[normalized].append(orig_col)
            
            logger.info(f"  {table_name}: {len(original_cols)} columns")
        except Exception as e:
            logger.warning(f"Could not get columns from {table_name}: {e}")
    
    # Create standardization mapping: choose the most common original name for each normalized name
    standardization_map = {}  # normalized -> standard_name (most common original)
    problematic_columns = []
    
    for normalized, original_list in all_original_columns.items():
        if len(original_list) == 1:
            # Only one variant - use it
            standardization_map[normalized] = original_list[0]
        else:
            # Multiple variants - choose the most common one
            # Count occurrences across all tables
            counts = {}
            for table_name, cols in table_columns.items():
                for orig in original_list:
                    if orig in cols:
                        counts[orig] = counts.get(orig, 0) + 1
            
            if counts:
                # Use the most common original name
                standard_name = max(counts.items(), key=lambda x: x[1])[0]
                standardization_map[normalized] = standard_name
                
                # Report if there are multiple variants
                if len(set(original_list)) > 1:
                    problematic_columns.append({
                        'normalized': normalized,
                        'variants': original_list,
                        'chosen': standard_name
                    })
            else:
                # Fallback: use first original
                standardization_map[normalized] = original_list[0]
    
    logger.info(f"Total unique normalized columns: {len(standardization_map)}")
    if problematic_columns:
        logger.warning(f"Found {len(problematic_columns)} columns with multiple variants")
    
    return standardization_map, problematic_columns, table_columns

def detect_column_types(df, sample_size=1000):
    """Detect column types by analyzing data patterns"""
    logger.info("Detecting column data types...")
    
    data_columns = [col for col in df.columns if col not in ['line_number', 'extraction_timestamp', 'file_name', 'sheet_name']]
    column_types = {}
    
    # Sample data for analysis (to speed up detection)
    sample_df = df[data_columns].head(min(sample_size, len(df)))
    
    for col in data_columns:
        # Get non-null values
        non_null_values = sample_df[col].dropna()
        
        if len(non_null_values) == 0:
            column_types[col] = 'VARCHAR'  # Default to VARCHAR if all null
            continue
        
        # Try to detect dates
        date_count = 0
        for val in non_null_values.head(100):  # Check first 100 non-null values
            val_str = str(val).strip()
            # Check if it looks like a date (contains date patterns)
            if val_str and (
                '202' in val_str or '201' in val_str or '200' in val_str or  # Years
                '/' in val_str or '-' in val_str or  # Date separators
                len(val_str) >= 8  # Date-like length
            ):
                try:
                    # Try parsing as date
                    pd.to_datetime(val_str, errors='raise')
                    date_count += 1
                except:
                    pass
        
        # If >50% of values are dates, treat as date column
        if date_count > len(non_null_values.head(100)) * 0.5:
            column_types[col] = 'TIMESTAMP'
            logger.debug(f"  {col}: detected as TIMESTAMP ({date_count}/{len(non_null_values.head(100))} date values)")
            continue
        
        # Try to detect numbers
        numeric_count = 0
        for val in non_null_values.head(100):
            val_str = str(val).strip()
            if val_str:
                # Remove common non-numeric characters but keep decimal point and minus
                cleaned = val_str.replace(',', '').replace(' ', '')
                try:
                    float(cleaned)
                    numeric_count += 1
                except:
                    pass
        
        # If >80% of values are numeric, treat as numeric column
        if numeric_count > len(non_null_values.head(100)) * 0.8:
            # Check if it's integer or float
            is_integer = True
            for val in non_null_values.head(50):
                val_str = str(val).strip().replace(',', '').replace(' ', '')
                try:
                    if '.' in val_str or 'e' in val_str.lower() or 'E' in val_str:
                        is_integer = False
                        break
                except:
                    pass
            
            if is_integer:
                column_types[col] = 'INTEGER'
                logger.debug(f"  {col}: detected as INTEGER ({numeric_count}/{len(non_null_values.head(100))} numeric values)")
            else:
                column_types[col] = 'DOUBLE'
                logger.debug(f"  {col}: detected as DOUBLE ({numeric_count}/{len(non_null_values.head(100))} numeric values)")
        else:
            column_types[col] = 'VARCHAR'
    
    logger.info(f"Column type detection completed: {sum(1 for t in column_types.values() if t == 'TIMESTAMP')} dates, "
                f"{sum(1 for t in column_types.values() if t in ['INTEGER', 'DOUBLE'])} numeric, "
                f"{sum(1 for t in column_types.values() if t == 'VARCHAR')} text")
    
    return column_types

def standardize_dataframe_columns(df, standardization_map):
    """Standardize column names in DataFrame using the mapping, ensuring unique column names"""
    column_mapping = {}
    used_standard_names = {}  # Track how many times we've used each standard name
    
    for orig_col in df.columns:
        if orig_col in ['line_number', 'extraction_timestamp', 'file_name', 'sheet_name']:
            # Keep metadata columns as-is
            column_mapping[orig_col] = orig_col
        else:
            # Normalize and map to standard name
            normalized = normalize_column_name(orig_col)
            if normalized and normalized in standardization_map:
                standard_name = standardization_map[normalized]
                
                # Handle duplicates: if we've already used this standard name, append counter
                if standard_name in used_standard_names:
                    used_standard_names[standard_name] += 1
                    unique_standard_name = f"{standard_name}_{used_standard_names[standard_name]}"
                    column_mapping[orig_col] = unique_standard_name
                    logger.debug(f"Renamed duplicate standard column '{standard_name}' to '{unique_standard_name}' for '{orig_col}'")
                else:
                    used_standard_names[standard_name] = 0
                    column_mapping[orig_col] = standard_name
            else:
                # If normalization fails or not in map, keep original
                logger.warning(f"Could not standardize column '{orig_col}' (normalized: {normalized})")
                column_mapping[orig_col] = orig_col
    
    # Rename columns
    df_renamed = df.rename(columns=column_mapping)
    
    # Verify no duplicates
    if len(df_renamed.columns) != len(set(df_renamed.columns)):
        duplicates = [col for col in df_renamed.columns if list(df_renamed.columns).count(col) > 1]
        logger.error(f"ERROR: Still have duplicate columns after standardization: {set(duplicates)}")
        raise ValueError(f"Duplicate columns found: {set(duplicates)}")
    
    return df_renamed

def clean_data(df, sheet_type):
    """Clean data by removing blank lines, rows with AUXILIAR = 0, and rows missing both PIN and procedure date"""
    logger.info("Cleaning data...")
    
    initial_count = len(df)
    
    # Get data columns (exclude metadata AND AUXILIAR)
    # AUXILIAR is excluded because rows with only AUXILIAR are considered blank
    metadata_cols = ['line_number', 'extraction_timestamp', 'file_name', 'sheet_name']
    auxiliar_cols = ['AUXILIAR', 'Auxiliar', 'auxiliar']  # Handle different casings
    exclude_cols = metadata_cols + auxiliar_cols
    data_cols = [col for col in df.columns if col not in exclude_cols]
    
    # Step 1: Remove rows where AUXILIAR = 0 or '0'
    auxiliar_col = None
    for col in auxiliar_cols:
        if col in df.columns:
            auxiliar_col = col
            break
    
    if auxiliar_col:
        # Remove rows where AUXILIAR is 0 or '0'
        mask_auxiliar = ~((df[auxiliar_col] == 0) | (df[auxiliar_col] == '0'))
        df = df[mask_auxiliar].copy()
        auxiliar_removed = initial_count - len(df)
        if auxiliar_removed > 0:
            logger.info(f"Removed {auxiliar_removed:,} rows with {auxiliar_col} = 0")
    
    # Step 2: Remove completely blank rows (excluding AUXILIAR from check)
    # Create a mask for rows where ALL data columns are blank (NaN or empty string)
    # A row is considered blank if all data columns are either NaN, None, or empty string
    def is_blank(val):
        if pd.isna(val):
            return True
        if isinstance(val, str) and val.strip() == '':
            return True
        return False
    
    # Check each row - keep only rows that have at least one non-blank data value
    mask = df[data_cols].apply(lambda row: not all(is_blank(val) for val in row), axis=1)
    df_clean = df[mask].copy()
    
    blank_removed = len(df) - len(df_clean)
    if blank_removed > 0:
        logger.info(f"Removed {blank_removed:,} completely blank rows")
    
    # Step 3: Remove rows where both PIN and procedure date are blank
    df = df_clean.copy()
    initial_step3_count = len(df)
    
    # Find PIN column
    pin_col = next((col for col in df.columns if normalize_column_name(col) == normalize_column_name('PIN')), 'PIN')
    
    # Determine procedure date column based on sheet type
    if sheet_type.upper() == 'FRESH':
        date_col = next((col for col in df.columns if normalize_column_name(col) == normalize_column_name('DATA DA PUNÇÃO')), 'DATA DA PUNÇÃO')
    else:  # FET
        date_col = next((col for col in df.columns if normalize_column_name(col) == normalize_column_name('DATA DA FET')), 'DATA DA FET')
    
    if pin_col in df.columns and date_col in df.columns:
        # A row is kept if either PIN or date is NOT blank
        mask_keys = ~(df[pin_col].apply(is_blank) & df[date_col].apply(is_blank))
        df_clean = df[mask_keys].copy()
        keys_removed = initial_step3_count - len(df_clean)
        if keys_removed > 0:
            logger.info(f"Removed {keys_removed:,} rows missing both {pin_col} and {date_col}")
    else:
        logger.warning(f"Could not find {pin_col} or {date_col} for key-based cleaning (Columns present: {pin_col in df.columns}, {date_col in df.columns})")
        df_clean = df

    total_removed = initial_count - len(df_clean)
    logger.info(f"Total rows removed: {total_removed:,}")
    logger.info(f"Total rows after cleaning: {len(df_clean):,}")
    
    return df_clean

def transform_data_types(df, column_types):
    """Transform DataFrame columns to proper data types"""
    logger.info("Transforming data types...")
    
    df_transformed = df.copy()
    data_columns = [col for col in df.columns if col not in ['line_number', 'extraction_timestamp', 'file_name', 'sheet_name']]
    
    for col in data_columns:
        col_type = column_types.get(col, 'VARCHAR')
        
        if col_type == 'TIMESTAMP':
            # Convert to datetime
            df_transformed[col] = pd.to_datetime(df_transformed[col], errors='coerce')
            logger.debug(f"  Converted {col} to TIMESTAMP")
        
        elif col_type == 'INTEGER':
            # Convert to integer (handle commas, spaces, etc.)
            df_transformed[col] = df_transformed[col].astype(str).str.replace(',', '').str.replace(' ', '')
            # Convert to float first, then round, then to nullable integer
            numeric_series = pd.to_numeric(df_transformed[col], errors='coerce')
            # Round to nearest integer (handles cases like 0.0, 1.0, etc.)
            numeric_series = numeric_series.round()
            # Convert float to int using numpy, then to nullable Int64
            # Handle NaN values properly by only converting non-NaN values
            mask = pd.isna(numeric_series)
            # Create array with NaN where mask is True, int values where mask is False
            int_values = np.full(len(numeric_series), np.nan, dtype=float)
            if not mask.all():
                int_values[~mask] = numeric_series[~mask].astype(int)
            df_transformed[col] = pd.array(int_values, dtype='Int64')
            logger.debug(f"  Converted {col} to INTEGER")
        
        elif col_type == 'DOUBLE':
            # Convert to float
            df_transformed[col] = df_transformed[col].astype(str).str.replace(',', '').str.replace(' ', '')
            df_transformed[col] = pd.to_numeric(df_transformed[col], errors='coerce')
            logger.debug(f"  Converted {col} to DOUBLE")
        
        # VARCHAR columns remain as strings (no conversion needed)
    
    logger.info("Data type transformation completed")
    return df_transformed

def add_prontuario_column(con, silver_table):
    """Add and populate prontuario column by matching PIN with view_pacientes"""
    logger.info(f"Matching PIN values with view_pacientes to populate prontuario column...")
    
    try:
        # Attach clinisys_all database
        clinisys_db_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'database', 'clinisys_all.duckdb')
        logger.info(f"Attaching clinisys_all database from: {clinisys_db_path}")
        con.execute(f"ATTACH '{clinisys_db_path}' AS clinisys_all (READ_ONLY)")
        
        # Update prontuario column using PIN matching
        # Try matching PIN against all prontuario fields in view_pacientes
        update_sql = f"""
        WITH pin_matches AS (
            SELECT DISTINCT
                p.PIN,
                COALESCE(
                    -- Try direct match with codigo (main prontuario)
                    (SELECT codigo FROM clinisys_all.silver.view_pacientes v 
                     WHERE CAST(p.PIN AS INTEGER) = v.codigo AND v.inativo = 0 LIMIT 1),
                    -- Try match with prontuario_esposa
                    (SELECT codigo FROM clinisys_all.silver.view_pacientes v 
                     WHERE CAST(p.PIN AS INTEGER) = v.prontuario_esposa AND v.inativo = 0 LIMIT 1),
                    -- Try match with prontuario_marido
                    (SELECT codigo FROM clinisys_all.silver.view_pacientes v 
                     WHERE CAST(p.PIN AS INTEGER) = v.prontuario_marido AND v.inativo = 0 LIMIT 1),
                    -- Try match with prontuario_responsavel1
                    (SELECT codigo FROM clinisys_all.silver.view_pacientes v 
                     WHERE CAST(p.PIN AS INTEGER) = v.prontuario_responsavel1 AND v.inativo = 0 LIMIT 1),
                    -- Try match with prontuario_responsavel2
                    (SELECT codigo FROM clinisys_all.silver.view_pacientes v 
                     WHERE CAST(p.PIN AS INTEGER) = v.prontuario_responsavel2 AND v.inativo = 0 LIMIT 1)
                ) as matched_prontuario
            FROM silver.{silver_table} p
            WHERE p.PIN IS NOT NULL
        )
        UPDATE silver.{silver_table}
        SET prontuario = m.matched_prontuario
        FROM pin_matches m
        WHERE silver.{silver_table}.PIN = m.PIN
          AND m.matched_prontuario IS NOT NULL
        """
        
        con.execute(update_sql)
        
        # Get statistics
        stats = con.execute(f"""
            SELECT 
                COUNT(*) as total,
                COUNT(CASE WHEN prontuario IS NOT NULL THEN 1 END) as matched,
                COUNT(CASE WHEN prontuario IS NULL THEN 1 END) as unmatched
            FROM silver.{silver_table}
            WHERE PIN IS NOT NULL
        """).fetchone()
        
        if stats[0] > 0:
            match_rate = (stats[1] / stats[0] * 100)
            logger.info(f"Prontuario matching results for {silver_table}:")
            logger.info(f"  Total rows with PIN: {stats[0]:,}")
            logger.info(f"  Matched: {stats[1]:,} ({match_rate:.2f}%)")
            logger.info(f"  Unmatched: {stats[2]:,} ({100-match_rate:.2f}%)")
        
        # Detach database
        con.execute("DETACH clinisys_all")
        
    except Exception as e:
        logger.error(f"Error adding prontuario column: {e}")
        raise

def create_silver_table(con, df, column_types, silver_table):
    """Create silver table with proper schema based on detected column types"""
    logger.info(f"Creating silver table: {silver_table}")
    
    # Create silver schema if it doesn't exist
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    
    # Drop existing table to ensure fresh data
    con.execute(f"DROP TABLE IF EXISTS silver.{silver_table}")
    logger.info(f"Dropped existing silver.{silver_table} table")
    
    # Get all columns from DataFrame (excluding metadata columns)
    data_columns = [col for col in df.columns if col not in ['line_number', 'extraction_timestamp', 'file_name', 'sheet_name']]
    
    # Create column definitions based on detected types
    column_definitions = []
    for col in data_columns:
        col_type = column_types.get(col, 'VARCHAR')
        # Map INTEGER to BIGINT for DuckDB (INTEGER in DuckDB is INT32, which is too small)
        if col_type == 'INTEGER':
            col_type = 'BIGINT'
        # Keep original column name in quotes for SQL
        column_definitions.append(f'"{col}" {col_type}')
    
    # Add metadata columns
    column_definitions.extend([
        'line_number INTEGER',
        'extraction_timestamp VARCHAR',
        'file_name VARCHAR',
        'sheet_name VARCHAR',
        'prontuario INTEGER'  # Add prontuario column
    ])
    
    create_table_sql = f"""
    CREATE TABLE silver.{silver_table} (
        {', '.join(column_definitions)}
    )
    """
    
    con.execute(create_table_sql)
    logger.info(f"Table silver.{silver_table} created successfully with {len(data_columns)} data columns")

def process_bronze_to_silver(con, sheet_type):
    """Process bronze tables for a specific sheet type and create corresponding silver table"""
    silver_table = f'planilha_embriologia_{sheet_type}'
    
    logger.info("=" * 50)
    logger.info(f"BRONZE TO SILVER TRANSFORMATION - {sheet_type.upper()}")
    logger.info("=" * 50)
    
    # Get all bronze tables
    bronze_tables = get_bronze_tables(con, sheet_type)
    
    if not bronze_tables:
        logger.warning("No bronze tables found matching pattern")
        return 0, []
    
    # Collect and standardize columns
    standardization_map, problematic_columns, table_columns = collect_all_columns_from_tables(con, bronze_tables)
    
    # Read and combine all bronze tables
    all_dataframes = []
    table_dataframe_map = {}  # Map table_name to dataframe
    
    for table_name in bronze_tables:
        try:
            logger.info(f"Reading data from bronze.{table_name}...")
            df = con.execute(f"SELECT * FROM bronze.{table_name}").df()
            logger.info(f"  Read {len(df)} rows from {table_name}")
            
            if len(df) == 0:
                logger.warning(f"  No data in {table_name}, skipping")
                continue
            
            # Standardize column names
            df_standardized = standardize_dataframe_columns(df, standardization_map)
            
            all_dataframes.append(df_standardized)
            table_dataframe_map[table_name] = df_standardized
            
        except Exception as e:
            logger.error(f"Error reading from {table_name}: {e}")
            continue
    
    if not all_dataframes:
        logger.warning("No data to process")
        return 0, problematic_columns
    
    # Combine all dataframes
    logger.info("Combining all dataframes...")
    
    # Get all unique columns from all dataframes
    all_columns_set = set()
    for df in all_dataframes:
        all_columns_set.update(df.columns)
    
    # Get column order from reference table
    # Since we changed table names, we'll try to find any 'planilha_..._fet' table as reference
    reference_table_found = False
    metadata_cols = ['line_number', 'extraction_timestamp', 'file_name', 'sheet_name']
    
    # Look for a reference table (prefer ..._ibi_fet as it usually has good columns)
    possible_references = [t for t in bronze_tables if 'ibi' in t.lower() and 'fet' in t.lower()]
    if not possible_references:
        # Fallback to any FET table
        possible_references = [t for t in bronze_tables if 'fet' in t.lower()]
    if not possible_references:
        # Fallback to any table
        possible_references = bronze_tables
        
    reference_cols_original = None
    
    if possible_references:
        reference_table_name = possible_references[0]
        try:
            # Get original columns from bronze table (before standardization)
            columns_info = con.execute(f"DESCRIBE bronze.{reference_table_name}").fetchdf()
            original_cols = [col for col in columns_info['column_name'].tolist() 
                           if col not in metadata_cols]
            reference_cols_original = original_cols
            reference_table_found = True
            logger.info(f"Using {reference_table_name} as reference for column order ({len(original_cols)} columns)")
        except Exception as e:
            logger.warning(f"Could not get columns from {reference_table_name}: {e}")
    
    # If we found the reference table, use its column order (after standardization)
    if reference_cols_original is not None:
        # Map original columns to standardized names
        reference_cols_standardized = []
        for orig_col in reference_cols_original:
            # Apply standardization mapping
            normalized = normalize_column_name(orig_col)
            if normalized and normalized in standardization_map:
                standard_name = standardization_map[normalized]
                if standard_name not in reference_cols_standardized:
                    reference_cols_standardized.append(standard_name)
            else:
                # If not in map, use original (shouldn't happen, but handle it)
                if orig_col not in reference_cols_standardized:
                    reference_cols_standardized.append(orig_col)
        
        # Move PIN to first position if it exists
        pin_cols = [col for col in reference_cols_standardized if normalize_column_name(col) == normalize_column_name('PIN')]
        if pin_cols:
            pin_col = pin_cols[0]
            reference_cols_standardized = [pin_col] + [col for col in reference_cols_standardized if col != pin_col]
            logger.info(f"Moving PIN column '{pin_col}' to first position")
        
        # Add any columns from other tables that aren't in the reference
        other_cols = [col for col in all_columns_set if col not in reference_cols_standardized and col not in metadata_cols]
        data_cols = reference_cols_standardized + sorted(other_cols)  # Add missing columns in alphabetical order
    else:
        # Fallback to alphabetical if reference table not found
        logger.warning("No reference table found, using alphabetical order")
        all_columns_ordered = sorted(list(all_columns_set))
        data_cols = [col for col in all_columns_ordered if col not in metadata_cols]
        # Move PIN to first if it exists
        pin_cols = [col for col in data_cols if normalize_column_name(col) == normalize_column_name('PIN')]
        if pin_cols:
            pin_col = pin_cols[0]
            data_cols = [pin_col] + [col for col in data_cols if col != pin_col]
    
    final_column_order = data_cols + [col for col in metadata_cols if col in all_columns_set]
    
    # Ensure all dataframes have the same columns (add missing as None)
    standardized_dfs = []
    for df in all_dataframes:
        df_aligned = df.copy()
        # Add missing columns
        for col in final_column_order:
            if col not in df_aligned.columns:
                df_aligned[col] = None
        # Reorder columns
        df_aligned = df_aligned[final_column_order]
        standardized_dfs.append(df_aligned)
    
    df_combined = pd.concat(standardized_dfs, ignore_index=True)
    logger.info(f"Combined {len(all_dataframes)} tables into {len(df_combined)} rows with {len(final_column_order)} columns")
    
    # Clean data
    df_clean = clean_data(df_combined, sheet_type)
    
    if len(df_clean) == 0:
        logger.warning("No data remaining after cleaning")
        return 0, problematic_columns
    
    # Detect column types
    column_types = detect_column_types(df_clean)
    
    # Transform data types
    df_transformed = transform_data_types(df_clean, column_types)
    
    # Create silver table
    create_silver_table(con, df_transformed, column_types, silver_table)
    
    logger.info(f"Inserting {len(df_transformed)} rows to silver layer")
    
    # Register DataFrame for SQL insertion
    con.register('temp_silver_data', df_transformed)
    
    # Build INSERT statement with proper type casting
    data_columns = [col for col in df_transformed.columns if col not in ['line_number', 'extraction_timestamp', 'file_name', 'sheet_name']]
    all_columns = data_columns + ['line_number', 'extraction_timestamp', 'file_name', 'sheet_name']
    
    # Build column list and select list with proper casting
    column_list = ', '.join([f'"{col}"' if col in data_columns else col for col in all_columns])
    select_parts = []
    for col in all_columns:
        if col in data_columns:
            col_type = column_types.get(col, 'VARCHAR')
            if col_type == 'TIMESTAMP':
                select_parts.append(f'CAST("{col}" AS TIMESTAMP) as "{col}"')
            elif col_type == 'INTEGER':
                # Use BIGINT for DuckDB (INTEGER in DuckDB is INT32, which is too small)
                select_parts.append(f'CAST("{col}" AS BIGINT) as "{col}"')
            elif col_type == 'DOUBLE':
                select_parts.append(f'CAST("{col}" AS DOUBLE) as "{col}"')
            else:
                select_parts.append(f'CAST("{col}" AS VARCHAR) as "{col}"')
        elif col == 'line_number':
            select_parts.append(f'CAST(line_number AS INTEGER) as line_number')
        else:
            select_parts.append(f'CAST({col} AS VARCHAR) as {col}')
    
    select_list = ', '.join(select_parts)
    
    insert_sql = f"""
    INSERT INTO silver.{silver_table} ({column_list})
    SELECT {select_list}
    FROM temp_silver_data
    """
    
    con.execute(insert_sql)
    
    # Clean up temporary table
    con.execute("DROP VIEW IF EXISTS temp_silver_data")
    
    logger.info(f"Successfully inserted {len(df_transformed)} rows to silver.{silver_table}")
    
    # Add prontuario column matching
    logger.info(f"Adding prontuario column to silver.{silver_table}...")
    add_prontuario_column(con, silver_table)
    
    logger.info(f"Successfully inserted {len(df_transformed)} rows to silver.{silver_table}")
    return len(df_transformed), problematic_columns

def main():
    """Main function to transform planilha_embriologia to silver"""
    logger.info("Starting Planilha Embriologia silver transformation")
    logger.info(f"DuckDB path: {DUCKDB_PATH}")
    logger.info(f"Processing sheet types: {SHEET_TYPES}")
    
    try:
        # Create DuckDB connection
        logger.info("Creating DuckDB connection...")
        con = get_duckdb_connection()
        logger.info("DuckDB connection created successfully")
        
        # Process each sheet type separately
        total_new_rows = 0
        all_problematic_columns = {}
        
        for sheet_type in SHEET_TYPES:
            logger.info("")
            logger.info("#" * 50)
            logger.info(f"Processing {sheet_type.upper()} tables")
            logger.info("#" * 50)
            
            new_rows, problematic_columns = process_bronze_to_silver(con, sheet_type)
            total_new_rows += new_rows
            
            if problematic_columns:
                all_problematic_columns[sheet_type] = problematic_columns
            
            # Get final table statistics for this sheet type
            silver_table = f'planilha_embriologia_{sheet_type}'
            result = con.execute(f'SELECT COUNT(*) FROM silver.{silver_table}').fetchone()
            total_rows = result[0] if result else 0
            
            logger.info(f"Rows inserted to silver.{silver_table}: {new_rows:,}")
            logger.info(f"Total rows in silver.{silver_table}: {total_rows:,}")
        
        # Final summary
        logger.info("")
        logger.info("=" * 50)
        logger.info("SILVER TRANSFORMATION SUMMARY")
        logger.info("=" * 50)
        logger.info(f"Total rows inserted across all tables: {total_new_rows:,}")
        
        # Report problematic columns for each sheet type
        if all_problematic_columns:
            logger.info("=" * 50)
            logger.info("COLUMNS WITH MULTIPLE VARIANTS (standardization issues):")
            logger.info("=" * 50)
            for sheet_type, problematic_columns in all_problematic_columns.items():
                logger.info(f"\n{sheet_type.upper()} sheet:")
                for item in problematic_columns:
                    logger.info(f"Normalized: '{item['normalized']}'")
                    logger.info(f"  Variants found: {item['variants']}")
                    logger.info(f"  Chosen standard: '{item['chosen']}'")
                    logger.info("")
        else:
            logger.info("All columns standardized successfully!")
        
        logger.info("=" * 50)
        
        # Close connection
        con.close()
        logger.info("Planilha Embriologia silver transformation completed")
        
    except Exception as e:
        logger.error(f"Error in main function: {e}")
        raise

if __name__ == "__main__":
    main()
