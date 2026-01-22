import logging
import pandas as pd

logger = logging.getLogger(__name__)

def clean_patient_id(df, table_name, db_name):
    """
    Clean PatientID column by converting to integer where possible.
    Discard records that cannot be converted and log unique discarded values.
    
    Args:
        df: DataFrame containing PatientID column
        table_name: Name of the table being processed
        db_name: Name of the database being processed
        
    Returns:
        DataFrame with cleaned PatientID (integer) and discarded records logged
    """
    if 'PatientID' not in df.columns:
        logger.debug(f"[{db_name}] No PatientID column found in {table_name}, skipping cleaning")
        return df
    
    original_count = len(df)
    logger.info(f"[{db_name}] Cleaning PatientID for {table_name}: {original_count} records")
    
    # Create a copy to avoid modifying original
    df_clean = df.copy()
    
    # Function to convert PatientID to integer
    def convert_to_int(patient_id):
        if pd.isna(patient_id) or patient_id is None:
            return None
        
        # Convert to string first
        patient_id_str = str(patient_id).strip()
        
        # Handle formatted numbers with dots (e.g., "520.124" -> 520124)
        if '.' in patient_id_str:
            # Remove all dots and convert to integer
            try:
                # Remove dots and check if the result is numeric
                cleaned_str = patient_id_str.replace('.', '')
                if cleaned_str.isdigit():
                    converted_id = int(cleaned_str)
                    # Discard if the result is 0
                    if converted_id == 0:
                        return None
                    return converted_id
            except (ValueError, AttributeError):
                pass
        
        # Handle pure numeric strings
        if patient_id_str.isdigit():
            converted_id = int(patient_id_str)
            # Discard if the result is 0
            if converted_id == 0:
                return None
            return converted_id
        
        # If it's not a number, return None (will be discarded)
        return None
    
    # Apply conversion
    df_clean['PatientID'] = df_clean['PatientID'].apply(convert_to_int)
    
    # Convert to integer type (not float)
    df_clean['PatientID'] = df_clean['PatientID'].astype('Int64')  # pandas nullable integer type
    
    # Identify records to keep (where PatientID is not None)
    valid_mask = df_clean['PatientID'].notna()
    df_valid = df_clean[valid_mask]
    df_discarded = df_clean[~valid_mask]
    
    # Log discarded records
    discarded_count = len(df_discarded)
    if discarded_count > 0:
        # Get unique discarded PatientID values from original data
        discarded_original_values = df.loc[~valid_mask, 'PatientID'].unique()
        logger.warning(f"[{db_name}] Discarded {discarded_count} records from {table_name} due to non-numeric PatientID")
        logger.warning(f"[{db_name}] Unique discarded PatientID values: {sorted(discarded_original_values)}")
        
        # Log some examples of discarded records
        if discarded_count <= 10:
            logger.warning(f"[{db_name}] All discarded records from {table_name}:")
            for idx, row in df_discarded.iterrows():
                logger.warning(f"[{db_name}]   - PatientIDx: {row.get('PatientIDx', 'N/A')}, PatientID: {row.get('PatientID', 'N/A')}, Name: {row.get('Name', 'N/A')}")
        else:
            logger.warning(f"[{db_name}] First 5 discarded records from {table_name}:")
            for idx, row in df_discarded.head().iterrows():
                logger.warning(f"[{db_name}]   - PatientIDx: {row.get('PatientIDx', 'N/A')}, PatientID: {row.get('PatientID', 'N/A')}, Name: {row.get('Name', 'N/A')}")
    else:
        logger.info(f"[{db_name}] No records discarded from {table_name}")
    
    valid_count = len(df_valid)
    logger.info(f"[{db_name}] PatientID cleaning complete for {table_name}: {valid_count} valid records, {discarded_count} discarded")
    
    return df_valid
