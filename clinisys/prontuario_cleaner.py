"""
Prontuario cleaning utilities
Handles conversion and validation of prontuario (patient ID) columns
"""

import logging
import pandas as pd

logger = logging.getLogger(__name__)


def convert_to_int(prontuario_val):
    """
    Convert prontuario value to integer, handling various formats
    
    Args:
        prontuario_val: Raw prontuario value
        
    Returns:
        Integer prontuario or None if invalid
    """
    if pd.isna(prontuario_val) or prontuario_val is None:
        return None
    
    # Handle float values (e.g., 875831.0 -> 875831)
    if isinstance(prontuario_val, float):
        # If it's a whole number float, convert to int
        if prontuario_val.is_integer():
            result = int(prontuario_val)
            return None if result == 0 else result
        # If it has decimal places, this might be an error - log and return None
        logger.warning(f"Unexpected decimal value in prontuario: {prontuario_val}")
        return None
    
    # Convert to string first
    prontuario_str = str(prontuario_val).strip()
    
    # Handle pure numeric strings first (most common case)
    if prontuario_str.isdigit():
        result = int(prontuario_str)
        # Discard if result is 0
        return None if result == 0 else result
    
    # Handle formatted numbers with dots (e.g., "520.124" -> 520124)
    # Only process if it's not a simple float representation
    if '.' in prontuario_str and not prontuario_str.endswith('.0'):
        # Remove all dots and convert to integer
        try:
            # Remove dots and check if the result is numeric
            cleaned_str = prontuario_str.replace('.', '')
            if cleaned_str.isdigit():
                result = int(cleaned_str)
                # Discard if result is 0
                return None if result == 0 else result
        except (ValueError, AttributeError):
            pass
    
    # If it's not a number, return None (will be discarded)
    return None


def clean_prontuario_columns(con, table_name):
    """
    Clean prontuario columns by converting to integer where possible.
    For view_pacientes: Keep all records, just clean prontuario values
    For other tables: Discard records that cannot be converted and log unique discarded values.
    
    Args:
        con: DuckDB connection
        table_name: Name of the table being processed
        
    Returns:
        None (modifies the silver table in place)
    """
    # Find all columns that contain 'prontuario' in their name
    columns = con.execute(f"DESCRIBE silver.{table_name}").df()
    prontuario_columns = [col for col in columns['column_name'] if 'prontuario' in col.lower()]
    
    if not prontuario_columns:
        logger.debug(f"No prontuario columns found in {table_name}")
        return
    
    logger.info(f"Found {len(prontuario_columns)} prontuario columns in {table_name}: {prontuario_columns}")
    
    # Special handling for view_pacientes - we cannot discard records
    is_patient_table = table_name == 'view_pacientes'
    
    if is_patient_table:
        # For view_pacientes: Keep all records, just clean the prontuario values
        # Get all data from the table
        all_data = con.execute(f"SELECT * FROM silver.{table_name}").df()
        
        # Apply the conversion to all prontuario columns
        for prontuario_col in prontuario_columns:
            logger.info(f"Cleaning prontuario column: {prontuario_col} in {table_name}")
            all_data[prontuario_col] = all_data[prontuario_col].apply(convert_to_int)
            logger.info(f"[{table_name}] {prontuario_col} cleaning completed (kept all records)")
        
        # Create a temporary table with the cleaned data
        temp_table = f"temp_{table_name}_cleaned"
        con.execute(f"DROP TABLE IF EXISTS {temp_table}")
        con.register(temp_table, all_data)
        con.execute(f"CREATE TABLE {temp_table} AS SELECT * FROM {temp_table}")
        con.unregister(temp_table)
        
        # Replace the original table
        con.execute(f"DROP TABLE silver.{table_name}")
        con.execute(f"CREATE TABLE silver.{table_name} AS SELECT * FROM {temp_table}")
        con.execute(f"DROP TABLE {temp_table}")
        
    else:
        # For other tables: Process all prontuario columns together
        # Get all data from the table
        all_data = con.execute(f"SELECT * FROM silver.{table_name}").df()
        original_count = len(all_data)
        
        # Apply the conversion to all prontuario columns
        for prontuario_col in prontuario_columns:
            logger.info(f"Cleaning prontuario column: {prontuario_col} in {table_name}")
            all_data[prontuario_col] = all_data[prontuario_col].apply(convert_to_int)
        
        # For non-patient tables, we want to keep records that have at least ONE valid prontuario
        # Create a mask for records that have at least one valid prontuario value
        valid_mask = all_data[prontuario_columns].notna().any(axis=1)
        valid_data = all_data[valid_mask]
        discarded_data = all_data[~valid_mask]
        
        valid_count = len(valid_data)
        discarded_count = len(discarded_data)
        
        logger.info(f"[{table_name}] Total records: {original_count} -> {valid_count} valid, {discarded_count} discarded")
        
        if discarded_count > 0:
            # Log some examples of discarded rows
            sample_discarded = discarded_data.head(5)
            logger.info(f"[{table_name}] Sample discarded rows (all prontuario columns invalid): {sample_discarded[prontuario_columns].to_dict('records')}")
        
        # Create a temporary table with only valid records
        temp_table = f"temp_{table_name}_cleaned"
        con.execute(f"DROP TABLE IF EXISTS {temp_table}")
        con.register(temp_table, valid_data)
        con.execute(f"CREATE TABLE {temp_table} AS SELECT * FROM {temp_table}")
        con.unregister(temp_table)
        
        # Replace the original table with only valid records
        con.execute(f"DROP TABLE silver.{table_name}")
        con.execute(f"CREATE TABLE silver.{table_name} AS SELECT * FROM {temp_table}")
        con.execute(f"DROP TABLE {temp_table}")
        
        logger.info(f"[{table_name}] prontuario cleaning completed (discarded records with all invalid prontuario values)")
