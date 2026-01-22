import os
import glob
import duckdb
import json
import logging
import pandas as pd
from datetime import datetime
import collections
import yaml

# Import new modules
from transformations import flatten_patients_json, flatten_embryo_json, flatten_idascore_json
from patient_id_cleaner import clean_patient_id
from patient_matching import update_prontuario_column
import feature_engineering

script_dir = os.path.dirname(os.path.abspath(__file__))
script_name = os.path.splitext(os.path.basename(__file__))[0]  # Get filename without extension
log_dir = os.path.join(script_dir, 'logs')
os.makedirs(log_dir, exist_ok=True)
log_ts = datetime.now().strftime('%Y%m%d_%H%M%S')
log_file = os.path.join(log_dir, f'{script_name}_{log_ts}.log')

# Load log level from params.yml
params_path = os.path.join(script_dir, 'params.yml')
with open(params_path, 'r') as f:
    params = yaml.safe_load(f)
log_level_str = params.get('extraction', {}).get('log_level', 'INFO').upper()
log_level = getattr(logging, log_level_str, logging.INFO)

# Setup logger with custom format
logger = logging.getLogger(script_name)
logger.setLevel(log_level)
formatter = logging.Formatter('%(asctime)s,%(msecs)03d - %(name)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
file_handler = logging.FileHandler(log_file)
file_handler.setFormatter(formatter)
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
if not logger.hasHandlers():
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

logger.info(f"Logging to: {log_file}")

# NOTE: calculate_column_filling_rates and filter_columns_by_null_rate have been removed.
# We now keep all columns to preserve data integrity.

def process_database(db_path):
    db_name = os.path.basename(db_path)
    logger.info(f"[{db_name}] Processing patients table.")
    try:
        con = duckdb.connect(db_path)
        con.execute('CREATE SCHEMA IF NOT EXISTS silver')
        read_query = 'SELECT raw_json, _extraction_timestamp, _location, _run_id, _row_hash FROM bronze.raw_patients'
        logger.debug(f"[{db_name}] Reading patients with query: {read_query}")
        logger.info(f"[{db_name}] Reading patients with query: {read_query}")
        df = con.execute(read_query).fetchdf()
        logger.info(f"[{db_name}] Read {len(df)} rows from bronze.raw_patients.")
        all_patients = []
        meta_cols = ['_extraction_timestamp', '_location', '_run_id', '_row_hash']
        for idx, row in df.iterrows():
            patients = flatten_patients_json(row['raw_json'])
            for p in patients:
                for col in meta_cols:
                    p[col] = row[col]
            all_patients.extend(patients)
        logger.info(f"[{db_name}] Flattened to {len(all_patients)} patient records.")
        if not all_patients:
            logger.warning(f"[{db_name}] No patients found.")
            return
        patients_df = pd.DataFrame(all_patients)
        if 'DateOfBirth' in patients_df.columns:
            patients_df['DateOfBirth'] = pd.to_datetime(patients_df['DateOfBirth'], errors='coerce')
        
        # Clean PatientID column
        patients_df = clean_patient_id(patients_df, 'patients', db_name)
        
        if patients_df.empty:
            logger.warning(f"[{db_name}] No patient records remain after cleaning")
            con.close()
            return
        
        # Initialize prontuario column with -1 (unmatched)
        logger.info(f"[{db_name}] Initializing prontuario column with -1 (unmatched)...")
        patients_df['prontuario'] = -1
        
        # Ensure all metadata columns exist
        for col in meta_cols:
            if col not in patients_df.columns:
                patients_df[col] = None
                
        logger.debug(f"[{db_name}] Saving patients to table: silver.patients in database: {db_path}")
        logger.info(f"[{db_name}] Saving patients to table: silver.patients.")
        con.execute('DROP TABLE IF EXISTS silver.patients')
        con.register('patients_df', patients_df)
        con.execute('CREATE TABLE silver.patients AS SELECT * FROM patients_df')
        con.unregister('patients_df')
        logger.info(f"[{db_name}] silver.patients creation complete.")
        
        # Update prontuario column using PatientID matching logic
        update_prontuario_column(con, db_name)
        
        # Apply feature engineering
        feature_engineering.feature_creation(con, 'patients')
        
        con.close()
        
    except Exception as e:
        logger.error(f"[{db_name}] Failed to process patients: {e}")

def process_treatments_database(db_path):
    db_name = os.path.basename(db_path)
    logger.info(f"[{db_name}] Processing treatments table.")
    try:
        con = duckdb.connect(db_path)
        con.execute('CREATE SCHEMA IF NOT EXISTS silver')
        read_query = 'SELECT raw_json, _extraction_timestamp, _location, _run_id, _row_hash FROM bronze.raw_treatments'
        logger.debug(f"[{db_name}] Reading treatments with query: {read_query}")
        logger.info(f"[{db_name}] Reading treatments with query: {read_query}")
        df = con.execute(read_query).fetchdf()
        logger.info(f"[{db_name}] Read {len(df)} rows from bronze.raw_treatments.")
        all_treatments = []
        meta_cols = ['_extraction_timestamp', '_location', '_run_id', '_row_hash']
        for idx, row in df.iterrows():
            try:
                treatment = json.loads(str(row['raw_json']))
                for col in meta_cols:
                    treatment[col] = row[col]
                all_treatments.append(treatment)
            except Exception as e:
                logger.error(f"[{db_name}] Error parsing treatment JSON at row {idx}: {e}")
        logger.info(f"[{db_name}] Parsed {len(all_treatments)} treatment records.")
        if not all_treatments:
            logger.warning(f"[{db_name}] No treatments found.")
            return
        treatments_df = pd.DataFrame(all_treatments)
        for col in treatments_df.columns:
            treatments_df[col] = treatments_df[col].astype(str)
        
        # Clean PatientID column
        treatments_df = clean_patient_id(treatments_df, 'treatments', db_name)
        
        if treatments_df.empty:
            logger.warning(f"[{db_name}] No treatment records remain after cleaning")
            con.close()
            return
        
        # Ensure all metadata columns exist
        for col in meta_cols:
            if col not in treatments_df.columns:
                treatments_df[col] = None
        logger.info(f"[{db_name}] Saving treatments to table: silver.treatments.")
        con.execute('DROP TABLE IF EXISTS silver.treatments')
        con.register('treatments_df', treatments_df)
        con.execute('CREATE TABLE silver.treatments AS SELECT * FROM treatments_df')
        con.unregister('treatments_df')
        
        # Apply feature engineering
        feature_engineering.feature_creation(con, 'treatments')
        
        con.close()
        logger.info(f"[{db_name}] silver.treatments creation complete.")
    except Exception as e:
        logger.error(f"[{db_name}] Failed to process treatments: {e}")

def process_idascore_database(db_path):
    db_name = os.path.basename(db_path)
    logger.info(f"[{db_name}] Processing idascore table.")
    try:
        con = duckdb.connect(db_path)
        con.execute('CREATE SCHEMA IF NOT EXISTS silver')
        read_query = 'SELECT raw_json, _extraction_timestamp, _location, _run_id, _row_hash FROM bronze.raw_idascore'
        logger.debug(f"[{db_name}] Reading idascore with query: {read_query}")
        logger.info(f"[{db_name}] Reading idascore with query: {read_query}")
        try:
            df = con.execute(read_query).fetchdf()
        except Exception as e:
            logger.warning(f"[{db_name}] Table bronze.raw_idascore does not exist: {e}")
            con.close()
            return
        logger.info(f"[{db_name}] Read {len(df)} rows from bronze.raw_idascore.")
        all_idascore = []
        meta_cols = ['_extraction_timestamp', '_location', '_run_id', '_row_hash']
        
        for idx, row in df.iterrows():
            mapped_record = flatten_idascore_json(row['raw_json'], meta_cols, row)
            if mapped_record:
                all_idascore.append(mapped_record)
                
        logger.info(f"[{db_name}] Parsed {len(all_idascore)} idascore records.")
        if not all_idascore:
            logger.warning(f"[{db_name}] No idascore records found.")
            # Create empty table with correct columns
            logger.info(f"[{db_name}] Creating empty silver.idascore table.")
            con.execute('DROP TABLE IF EXISTS silver.idascore')
            con.execute('CREATE TABLE silver.idascore (EmbryoID TEXT, IDAScore TEXT, IDATime TEXT, IDAVersion TEXT, IDATimestamp TEXT, _extraction_timestamp TIMESTAMP, _location TEXT, _run_id TEXT, _row_hash TEXT)')
            con.close()
            logger.info(f"[{db_name}] Empty silver.idascore table created.")
            return
        idascore_df = pd.DataFrame(all_idascore)
        for col in idascore_df.columns:
            idascore_df[col] = idascore_df[col].astype(str)
        
        if idascore_df.empty:
            logger.warning(f"[{db_name}] DataFrame empty for idascore table")
            con.close()
            return
        
        logger.info(f"[{db_name}] Saving idascore to table: silver.idascore.")
        con.execute('DROP TABLE IF EXISTS silver.idascore')
        con.register('idascore_df', idascore_df)
        con.execute('CREATE TABLE silver.idascore AS SELECT * FROM idascore_df')
        con.unregister('idascore_df')
        
        # Apply feature engineering
        feature_engineering.feature_creation(con, 'idascore')
        
        con.close()
        logger.info(f"[{db_name}] silver.idascore creation complete.")
    except Exception as e:
        logger.error(f"[{db_name}] Failed to process idascore: {e}")

def process_embryo_data_database(db_path):
    db_name = os.path.basename(db_path)
    logger.info(f"[{db_name}] Processing embryo_data table.")
    try:
        con = duckdb.connect(db_path)
        con.execute('CREATE SCHEMA IF NOT EXISTS silver')
        read_query = 'SELECT raw_json, _extraction_timestamp, _location, _run_id, _row_hash FROM bronze.raw_embryo_data'
        logger.debug(f"[{db_name}] Reading embryo_data with query: {read_query}")
        logger.info(f"[{db_name}] Reading embryo_data with query: {read_query}")
        try:
            df = con.execute(read_query).fetchdf()
        except Exception as e:
            logger.warning(f"[{db_name}] Table bronze.raw_embryo_data does not exist: {e}")
            con.close()
            return
        logger.info(f"[{db_name}] Read {len(df)} rows from bronze.raw_embryo_data.")
        # First pass: collect all annotation names
        annotation_names = set()
        for idx, row in df.iterrows():
            try:
                data = json.loads(str(row['raw_json']))
                for ann in data.get('AnnotationList', []):
                    if 'Name' in ann:
                        annotation_names.add(ann['Name'])
            except Exception as e:
                logger.error(f"[{db_name}] Error collecting annotation names at row {idx}: {e}")
        logger.debug(f"[{db_name}] Found annotation types: {sorted(annotation_names)}")
        logger.info(f"[{db_name}] Found annotation types: {sorted(annotation_names)}")
        
        # Second pass: flatten all rows
        all_embryos = []
        meta_cols = ['_extraction_timestamp', '_location', '_run_id', '_row_hash']
        for idx, row in df.iterrows():
            flat = flatten_embryo_json(row['raw_json'], annotation_names_set=annotation_names)
            for col in meta_cols:
                flat[col] = row[col]
            all_embryos.append(flat)
        logger.info(f"[{db_name}] Flattened {len(all_embryos)} embryo records.")
        if not all_embryos:
            logger.warning(f"[{db_name}] No embryo_data records found.")
            # Create empty table with all expected columns
            columns = [
                'EmbryoID', 'PatientIDx', 'TreatmentName',
                'EmbryoDetails_InstrumentNumber', 'EmbryoDetails_Position', 'EmbryoDetails_WellNumber',
                'EmbryoDetails_FertilizationTime', 'EmbryoDetails_FertilizationMethod', 'EmbryoDetails_EmbryoFate',
                'EmbryoDetails_Description', 'EmbryoDetails_EmbryoDescriptionID',
                'Evaluation_Model', 'Evaluation_User', 'Evaluation_EvaluationDate',
                '_extraction_timestamp', '_location', '_run_id', '_row_hash'
            ]
            for ann_name in sorted(annotation_names):
                columns.extend([
                    f'Name_{ann_name}', f'Time_{ann_name}', f'Value_{ann_name}', f'Timestamp_{ann_name}'
                ])
            col_defs = ', '.join([f'{col} TEXT' for col in columns])
            con.execute('DROP TABLE IF EXISTS silver.embryo_data')
            con.execute(f'CREATE TABLE silver.embryo_data ({col_defs})')
            con.close()
            logger.info(f"[{db_name}] Empty silver.embryo_data table created.")
            return
        embryo_df = pd.DataFrame(all_embryos)
        # Cast types - ensure proper DATE type for JOIN performance
        for col in embryo_df.columns:
            if col.startswith('Time_'):
                try:
                    embryo_df[col] = pd.to_numeric(embryo_df[col], errors='coerce')
                except Exception as e:
                    logger.error(f"[{db_name}] Error casting {col} to numeric: {e}")
            elif col.endswith('Date') or col.endswith('Time') or col.endswith('Timestamp'):
                try:
                    # Special handling for FertilizationTime - ensure it's properly typed for JOINs
                    if col == 'FertilizationTime':
                        embryo_df[col] = pd.to_datetime(embryo_df[col], errors='coerce')
                        logger.info(f"[{db_name}] Cast {col} to datetime for JOIN performance")
                    else:
                        embryo_df[col] = pd.to_datetime(embryo_df[col], errors='coerce')
                except Exception as e:
                    logger.error(f"[{db_name}] Error casting {col} to datetime: {e}")
                    
        # Map Evaluation_Evaluation -> KIDScore, Evaluation_EvaluationDate -> KIDDate
        if 'Evaluation_Evaluation' in embryo_df.columns:
            embryo_df['KIDScore'] = embryo_df['Evaluation_Evaluation']
            embryo_df = embryo_df.drop(columns=['Evaluation_Evaluation'])
        if 'Evaluation_EvaluationDate' in embryo_df.columns:
            embryo_df['KIDDate'] = embryo_df['Evaluation_EvaluationDate']
            embryo_df = embryo_df.drop(columns=['Evaluation_EvaluationDate'])
        if 'Evaluation_Model' in embryo_df.columns:
            embryo_df['KIDVersion'] = embryo_df['Evaluation_Model']
            embryo_df = embryo_df.drop(columns=['Evaluation_Model'])
        if 'Evaluation_User' in embryo_df.columns:
            embryo_df['KIDUser'] = embryo_df['Evaluation_User']
            embryo_df = embryo_df.drop(columns=['Evaluation_User'])
        # Remove EmbryoDetails_ prefix
        embryo_df = embryo_df.rename(columns={col: col.replace('EmbryoDetails_', '') for col in embryo_df.columns if col.startswith('EmbryoDetails_')})
        
        # Clean PatientID column (before processing)
        embryo_df = clean_patient_id(embryo_df, 'embryo_data', db_name)
        
        if embryo_df.empty:
            logger.warning(f"[{db_name}] No embryo records remain after cleaning")
            con.close()
            return
        
        # Order columns: EmbryoID, PatientIDx, TreatmentName, all KID*, all IDA*, then the rest
        cols = list(embryo_df.columns)
        main_cols = ['EmbryoID', 'PatientIDx', 'TreatmentName']
        kid_cols = sorted([c for c in cols if c.startswith('KID')])
        ida_cols = sorted([c for c in cols if c.startswith('IDA')])
        other_cols = sorted([c for c in cols if c not in main_cols + kid_cols + ida_cols])
        ordered_cols = main_cols + kid_cols + ida_cols + other_cols
        # Only reindex with columns that exist in the DataFrame
        ordered_cols = [c for c in ordered_cols if c in embryo_df.columns]
        embryo_df = embryo_df.reindex(columns=ordered_cols)
        
        # Ensure all metadata columns exist
        for col in meta_cols:
            if col not in embryo_df.columns:
                embryo_df[col] = None
                
        logger.info(f"[{db_name}] Saving embryo_data to table: silver.embryo_data.")
        con.execute('DROP TABLE IF EXISTS silver.embryo_data')
        con.register('embryo_df', embryo_df)
        con.execute('CREATE TABLE silver.embryo_data AS SELECT * FROM embryo_df')
        con.unregister('embryo_df')
        
        # Apply feature engineering
        feature_engineering.feature_creation(con, 'embryo_data')
        
        con.close()
        logger.info(f"[{db_name}] silver.embryo_data creation complete.")
    except Exception as e:
        logger.error(f"[{db_name}] Failed to process embryo_data: {e}")

def main():
    logger.info("Starting bronze to silver conversion...")
    
    # Process all embryoscope databases in the database folder
    db_folder = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'database')
    logger.info(f"Looking for databases in: {db_folder}")
    
    # Find all databases matching pattern
    db_files = glob.glob(os.path.join(db_folder, 'embryoscope_*.db'))
    logger.info(f"Found {len(db_files)} databases to process.")
    
    for db_path in db_files:
        # Skip huntington_data_lake.duckdb as it is the central sink
        if 'huntington_data_lake.duckdb' in db_path:
            continue
            
        logger.info(f"="*50)
        logger.info(f"Processing database: {os.path.basename(db_path)}")
        process_database(db_path)
        process_treatments_database(db_path)
        process_idascore_database(db_path)
        process_embryo_data_database(db_path)
        logger.info(f"Finished processing database: {os.path.basename(db_path)}")
        logger.info(f"="*50)

if __name__ == '__main__':
    main()
script_name = os.path.splitext(os.path.basename(__file__))[0]  # Get filename without extension
log_dir = os.path.join(script_dir, 'logs')
os.makedirs(log_dir, exist_ok=True)
log_ts = datetime.now().strftime('%Y%m%d_%H%M%S')
log_file = os.path.join(log_dir, f'{script_name}_{log_ts}.log')

# Load log level from params.yml
params_path = os.path.join(script_dir, 'params.yml')
with open(params_path, 'r') as f:
    params = yaml.safe_load(f)
log_level_str = params.get('extraction', {}).get('log_level', 'INFO').upper()
log_level = getattr(logging, log_level_str, logging.INFO)

# Setup logger with custom format
logger = logging.getLogger(script_name)
logger.setLevel(log_level)
formatter = logging.Formatter('%(asctime)s,%(msecs)03d - %(name)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
file_handler = logging.FileHandler(log_file)
file_handler.setFormatter(formatter)
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
if not logger.hasHandlers():
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

logger.info(f"Logging to: {log_file}")

def calculate_column_filling_rates(df):
    """
    Calculate filling rate (non-null percentage) for each column in a DataFrame.
    
    Args:
        df: pandas DataFrame
        
    Returns:
        Dictionary mapping column names to their filling rates (0-100)
    """
    filling_rates = {}
    
    if df.empty:
        logger.warning("DataFrame is empty, all columns will have 0% filling rate")
        return {col: 0.0 for col in df.columns}
    
    total_count = len(df)
    
    for column in df.columns:
        try:
            # Count non-null and non-empty values
            # Convert to string to handle different types, then check for empty strings
            non_null_count = df[column].notna().sum()
            
            # Also exclude empty strings and 'NULL'/'null' strings
            if df[column].dtype == 'object':
                non_empty_mask = df[column].notna() & (df[column].astype(str).str.strip() != '') & (~df[column].astype(str).str.strip().isin(['NULL', 'null', 'None', 'none']))
                non_null_count = non_empty_mask.sum()
            
            filling_rate = (non_null_count / total_count) * 100
            filling_rates[column] = filling_rate
            
        except Exception as e:
            logger.warning(f"Error calculating filling rate for column {column}: {e}")
            filling_rates[column] = 0.0
    
    return filling_rates

def filter_columns_by_null_rate(df, table_name, db_name, null_rate_threshold=90.0):
    """
    Filter DataFrame columns based on null rate threshold, excluding columns with null rate > threshold.
    
    Args:
        df: pandas DataFrame
        table_name: Name of the table being processed
        db_name: Name of the database being processed
        null_rate_threshold: Maximum null rate (percentage) allowed - columns with null rate > this will be excluded (default: 90.0)
        
    Returns:
        Tuple of (filtered_dataframe, excluded_columns_dict) where excluded_columns_dict maps
        column names to their null rates
    """
    if df.empty:
        logger.warning(f"[{db_name}] DataFrame for {table_name} is empty, no columns to filter")
        return df, {}
    
    logger.info(f"[{db_name}] Processing {len(df.columns)} columns for table {table_name}")
    
    # Calculate filling rates for all columns
    logger.info(f"[{db_name}] Calculating filling rates for columns in {table_name}...")
    filling_rates = calculate_column_filling_rates(df)
    
    # Filter columns based on null rate threshold
    # Exclude columns with >90% null rate (i.e., keep columns with >=10% filling rate)
    included_columns = []
    excluded_columns = {}
    
    for column in df.columns:
        filling_rate = filling_rates.get(column, 0.0)
        null_rate = 100.0 - filling_rate
        if null_rate > null_rate_threshold:
            excluded_columns[column] = null_rate
        else:
            included_columns.append(column)
    
    # Log excluded columns with their null rates
    if excluded_columns:
        logger.info(f"[{db_name}] [{table_name}] Excluded {len(excluded_columns)} columns with null rate > {null_rate_threshold}%:")
        for col, null_rate in sorted(excluded_columns.items(), key=lambda x: x[1], reverse=True):
            filling_rate = 100.0 - null_rate
            logger.info(f"[{db_name}]   - {col}: null_rate={null_rate:.2f}% (filling_rate={filling_rate:.2f}%)")
    else:
        logger.info(f"[{db_name}] [{table_name}] All columns have null rate <= {null_rate_threshold}%")
    
    logger.info(f"[{db_name}] [{table_name}] Including {len(included_columns)} columns (out of {len(df.columns)} total)")
    
    if not included_columns:
        logger.warning(f"[{db_name}] No columns meet the null rate threshold (all have null rate > {null_rate_threshold}%) for {table_name}")
        return pd.DataFrame(), excluded_columns
    
    # Return filtered DataFrame with only included columns
    filtered_df = df[included_columns].copy()
    return filtered_df, excluded_columns

def flatten_patients_json(raw_json_str):
    try:
        data = json.loads(raw_json_str)
        # If the data is a dict and not a list, treat as a single patient record
        if isinstance(data, dict):
            return [data]
        elif isinstance(data, list):
            return data
        else:
            return []
    except Exception as e:
        logger.error(f"Error parsing JSON: {e}")
        return []

def flatten_embryo_json(raw_json_str, annotation_names_set=None, log_errors=True):
    try:
        data = json.loads(raw_json_str)
        flat = {}
        # Flatten top-level fields
        for k, v in data.items():
            if isinstance(v, dict):
                for subk, subv in v.items():
                    flat[f"{k}_{subk}"] = subv
            elif k == 'AnnotationList' and isinstance(v, list):
                # We'll handle this below
                continue
            else:
                flat[k] = v
        # Pivot AnnotationList
        annotation_list = data.get('AnnotationList', [])
        if annotation_names_set is not None:
            # Use the provided set to ensure all columns are present
            for ann_name in annotation_names_set:
                ann = next((a for a in annotation_list if a.get('Name') == ann_name), None)
                if ann:
                    for annk, annv in ann.items():
                        flat[f"{annk}_{ann_name}"] = annv
                else:
                    # Fill with None for missing annotation
                    flat[f"Name_{ann_name}"] = None
                    flat[f"Time_{ann_name}"] = None
                    flat[f"Value_{ann_name}"] = None
                    flat[f"Timestamp_{ann_name}"] = None
        else:
            # If no set provided, just flatten what is present
            for ann in annotation_list:
                ann_name = ann.get('Name')
                for annk, annv in ann.items():
                    flat[f"{annk}_{ann_name}"] = annv
        return flat
    except Exception as e:
        if log_errors:
            logger.error(f"Error flattening embryo JSON: {e}")
        return {}

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

def process_database(db_path):
    db_name = os.path.basename(db_path)
    logger.info(f"[{db_name}] Processing patients table.")
    try:
        con = duckdb.connect(db_path)
        con.execute('CREATE SCHEMA IF NOT EXISTS silver')
        read_query = 'SELECT raw_json, _extraction_timestamp, _location, _run_id, _row_hash FROM bronze.raw_patients'
        logger.debug(f"[{db_name}] Reading patients with query: {read_query}")
        logger.info(f"[{db_name}] Reading patients with query: {read_query}")
        df = con.execute(read_query).fetchdf()
        logger.info(f"[{db_name}] Read {len(df)} rows from bronze.raw_patients.")
        all_patients = []
        meta_cols = ['_extraction_timestamp', '_location', '_run_id', '_row_hash']
        for idx, row in df.iterrows():
            patients = flatten_patients_json(row['raw_json'])
            for p in patients:
                for col in meta_cols:
                    p[col] = row[col]
            all_patients.extend(patients)
        logger.info(f"[{db_name}] Flattened to {len(all_patients)} patient records.")
        if not all_patients:
            logger.warning(f"[{db_name}] No patients found.")
            return
        patients_df = pd.DataFrame(all_patients)
        if 'DateOfBirth' in patients_df.columns:
            patients_df['DateOfBirth'] = pd.to_datetime(patients_df['DateOfBirth'], errors='coerce')
        
        # Clean PatientID column
        patients_df = clean_patient_id(patients_df, 'patients', db_name)
        
        # Filter columns by null rate (exclude columns with >90% null rate)
        patients_df, excluded_columns = filter_columns_by_null_rate(patients_df, 'patients', db_name, null_rate_threshold=90.0)
        
        if patients_df.empty:
            logger.warning(f"[{db_name}] No columns remain after filtering for patients table")
            con.close()
            return
        
        # Initialize prontuario column with -1 (unmatched)
        logger.info(f"[{db_name}] Initializing prontuario column with -1 (unmatched)...")
        patients_df['prontuario'] = -1
        
        # Ensure all metadata columns exist
        for col in meta_cols:
            if col not in patients_df.columns:
                patients_df[col] = None
        logger.debug(f"[{db_name}] Saving patients to table: silver.patients in database: {db_path}")
        logger.info(f"[{db_name}] Saving patients to table: silver.patients.")
        con.execute('DROP TABLE IF EXISTS silver.patients')
        con.register('patients_df', patients_df)
        con.execute('CREATE TABLE silver.patients AS SELECT * FROM patients_df')
        con.unregister('patients_df')
        logger.info(f"[{db_name}] silver.patients creation complete.")
        logger.debug(f"[{db_name}] silver.patients written for {db_path}")
        
        # Update prontuario column using PatientID matching logic
        update_prontuario_column(con, db_name)
        
        con.close()
        
    except Exception as e:
        logger.error(f"[{db_name}] Failed to process patients: {e}")

def process_treatments_database(db_path):
    db_name = os.path.basename(db_path)
    logger.info(f"[{db_name}] Processing treatments table.")
    try:
        con = duckdb.connect(db_path)
        con.execute('CREATE SCHEMA IF NOT EXISTS silver')
        read_query = 'SELECT raw_json, _extraction_timestamp, _location, _run_id, _row_hash FROM bronze.raw_treatments'
        logger.debug(f"[{db_name}] Reading treatments with query: {read_query}")
        logger.info(f"[{db_name}] Reading treatments with query: {read_query}")
        df = con.execute(read_query).fetchdf()
        logger.info(f"[{db_name}] Read {len(df)} rows from bronze.raw_treatments.")
        all_treatments = []
        meta_cols = ['_extraction_timestamp', '_location', '_run_id', '_row_hash']
        for idx, row in df.iterrows():
            try:
                treatment = json.loads(str(row['raw_json']))
                for col in meta_cols:
                    treatment[col] = row[col]
                all_treatments.append(treatment)
            except Exception as e:
                logger.error(f"[{db_name}] Error parsing treatment JSON at row {idx}: {e}")
        logger.info(f"[{db_name}] Parsed {len(all_treatments)} treatment records.")
        if not all_treatments:
            logger.warning(f"[{db_name}] No treatments found.")
            return
        treatments_df = pd.DataFrame(all_treatments)
        for col in treatments_df.columns:
            treatments_df[col] = treatments_df[col].astype(str)
        
        # Clean PatientID column
        treatments_df = clean_patient_id(treatments_df, 'treatments', db_name)
        
        # Filter columns by null rate (exclude columns with >90% null rate)
        treatments_df, excluded_columns = filter_columns_by_null_rate(treatments_df, 'treatments', db_name, null_rate_threshold=90.0)
        
        if treatments_df.empty:
            logger.warning(f"[{db_name}] No columns remain after filtering for treatments table")
            con.close()
            return
        
        # Ensure all metadata columns exist
        for col in meta_cols:
            if col not in treatments_df.columns:
                treatments_df[col] = None
        logger.info(f"[{db_name}] Saving treatments to table: silver.treatments.")
        con.execute('DROP TABLE IF EXISTS silver.treatments')
        con.register('treatments_df', treatments_df)
        con.execute('CREATE TABLE silver.treatments AS SELECT * FROM treatments_df')
        con.unregister('treatments_df')
        con.close()
        logger.info(f"[{db_name}] silver.treatments creation complete.")
    except Exception as e:
        logger.error(f"[{db_name}] Failed to process treatments: {e}")

def process_idascore_database(db_path):
    db_name = os.path.basename(db_path)
    logger.info(f"[{db_name}] Processing idascore table.")
    try:
        con = duckdb.connect(db_path)
        con.execute('CREATE SCHEMA IF NOT EXISTS silver')
        read_query = 'SELECT raw_json, _extraction_timestamp, _location, _run_id, _row_hash FROM bronze.raw_idascore'
        logger.debug(f"[{db_name}] Reading idascore with query: {read_query}")
        logger.info(f"[{db_name}] Reading idascore with query: {read_query}")
        try:
            df = con.execute(read_query).fetchdf()
        except Exception as e:
            logger.warning(f"[{db_name}] Table bronze.raw_idascore does not exist: {e}")
            con.close()
            return
        logger.info(f"[{db_name}] Read {len(df)} rows from bronze.raw_idascore.")
        all_idascore = []
        for idx, row in df.iterrows():
            try:
                record = json.loads(str(row['raw_json']))
                # Map fields as requested
                mapped_record = {}
                for k, v in record.items():
                    if k == 'Viability':
                        mapped_record['IDAScore'] = v
                    elif k == 'Time':
                        mapped_record['IDATime'] = v
                    elif k == 'Version':
                        mapped_record['IDAVersion'] = v
                    elif k == 'Timestamp':
                        mapped_record['IDATimestamp'] = v
                    else:
                        mapped_record[k] = v
                # Add meta columns
                mapped_record['_extraction_timestamp'] = row['_extraction_timestamp']
                mapped_record['_location'] = row['_location']
                mapped_record['_run_id'] = row['_run_id']
                mapped_record['_row_hash'] = row['_row_hash']
                all_idascore.append(mapped_record)
            except Exception as e:
                logger.error(f"[{db_name}] Error parsing idascore JSON at row {idx}: {e}")
        logger.info(f"[{db_name}] Parsed {len(all_idascore)} idascore records.")
        if not all_idascore:
            logger.warning(f"[{db_name}] No idascore records found.")
            # Create empty table with correct columns
            logger.info(f"[{db_name}] Creating empty silver.idascore table.")
            con.execute('DROP TABLE IF EXISTS silver.idascore')
            con.execute('CREATE TABLE silver.idascore (EmbryoID TEXT, IDAScore TEXT, IDATime TEXT, IDAVersion TEXT, IDATimestamp TEXT, _extraction_timestamp TIMESTAMP, _location TEXT, _run_id TEXT, _row_hash TEXT)')
            con.close()
            logger.info(f"[{db_name}] Empty silver.idascore table created.")
            return
        idascore_df = pd.DataFrame(all_idascore)
        for col in idascore_df.columns:
            idascore_df[col] = idascore_df[col].astype(str)
        
        # Filter columns by null rate (exclude columns with >90% null rate)
        idascore_df, excluded_columns = filter_columns_by_null_rate(idascore_df, 'idascore', db_name, null_rate_threshold=90.0)
        
        if idascore_df.empty:
            logger.warning(f"[{db_name}] No columns remain after filtering for idascore table")
            con.close()
            return
        
        logger.info(f"[{db_name}] Saving idascore to table: silver.idascore.")
        con.execute('DROP TABLE IF EXISTS silver.idascore')
        con.register('idascore_df', idascore_df)
        con.execute('CREATE TABLE silver.idascore AS SELECT * FROM idascore_df')
        con.unregister('idascore_df')
        con.close()
        logger.info(f"[{db_name}] silver.idascore creation complete.")
    except Exception as e:
        logger.error(f"[{db_name}] Failed to process idascore: {e}")

def process_embryo_data_database(db_path):
    db_name = os.path.basename(db_path)
    logger.info(f"[{db_name}] Processing embryo_data table.")
    try:
        con = duckdb.connect(db_path)
        con.execute('CREATE SCHEMA IF NOT EXISTS silver')
        read_query = 'SELECT raw_json, _extraction_timestamp, _location, _run_id, _row_hash FROM bronze.raw_embryo_data'
        logger.debug(f"[{db_name}] Reading embryo_data with query: {read_query}")
        logger.info(f"[{db_name}] Reading embryo_data with query: {read_query}")
        try:
            df = con.execute(read_query).fetchdf()
        except Exception as e:
            logger.warning(f"[{db_name}] Table bronze.raw_embryo_data does not exist: {e}")
            con.close()
            return
        logger.info(f"[{db_name}] Read {len(df)} rows from bronze.raw_embryo_data.")
        # First pass: collect all annotation names
        annotation_names = set()
        for idx, row in df.iterrows():
            try:
                data = json.loads(str(row['raw_json']))
                for ann in data.get('AnnotationList', []):
                    if 'Name' in ann:
                        annotation_names.add(ann['Name'])
            except Exception as e:
                logger.error(f"[{db_name}] Error collecting annotation names at row {idx}: {e}")
        logger.debug(f"[{db_name}] Found annotation types: {sorted(annotation_names)}")
        logger.info(f"[{db_name}] Found annotation types: {sorted(annotation_names)}")
        # Second pass: flatten all rows
        all_embryos = []
        meta_cols = ['_extraction_timestamp', '_location', '_run_id', '_row_hash']
        for idx, row in df.iterrows():
            flat = flatten_embryo_json(row['raw_json'], annotation_names_set=annotation_names)
            for col in meta_cols:
                flat[col] = row[col]
            all_embryos.append(flat)
        logger.info(f"[{db_name}] Flattened {len(all_embryos)} embryo records.")
        if not all_embryos:
            logger.warning(f"[{db_name}] No embryo_data records found.")
            # Create empty table with all expected columns
            columns = [
                'EmbryoID', 'PatientIDx', 'TreatmentName',
                'EmbryoDetails_InstrumentNumber', 'EmbryoDetails_Position', 'EmbryoDetails_WellNumber',
                'EmbryoDetails_FertilizationTime', 'EmbryoDetails_FertilizationMethod', 'EmbryoDetails_EmbryoFate',
                'EmbryoDetails_Description', 'EmbryoDetails_EmbryoDescriptionID',
                'Evaluation_Model', 'Evaluation_User', 'Evaluation_EvaluationDate',
                '_extraction_timestamp', '_location', '_run_id', '_row_hash'
            ]
            for ann_name in sorted(annotation_names):
                columns.extend([
                    f'Name_{ann_name}', f'Time_{ann_name}', f'Value_{ann_name}', f'Timestamp_{ann_name}'
                ])
            col_defs = ', '.join([f'{col} TEXT' for col in columns])
            con.execute('DROP TABLE IF EXISTS silver.embryo_data')
            con.execute(f'CREATE TABLE silver.embryo_data ({col_defs})')
            con.close()
            logger.info(f"[{db_name}] Empty silver.embryo_data table created.")
            return
        embryo_df = pd.DataFrame(all_embryos)
        # Cast types - ensure proper DATE type for JOIN performance
        for col in embryo_df.columns:
            if col.startswith('Time_'):
                try:
                    embryo_df[col] = pd.to_numeric(embryo_df[col], errors='coerce')
                except Exception as e:
                    logger.error(f"[{db_name}] Error casting {col} to numeric: {e}")
            elif col.endswith('Date') or col.endswith('Time') or col.endswith('Timestamp'):
                try:
                    # Special handling for FertilizationTime - ensure it's properly typed for JOINs
                    if col == 'FertilizationTime':
                        embryo_df[col] = pd.to_datetime(embryo_df[col], errors='coerce')
                        logger.info(f"[{db_name}] Cast {col} to datetime for JOIN performance")
                    else:
                        embryo_df[col] = pd.to_datetime(embryo_df[col], errors='coerce')
                except Exception as e:
                    logger.error(f"[{db_name}] Error casting {col} to datetime: {e}")
        # Map Evaluation_Evaluation -> KIDScore, Evaluation_EvaluationDate -> KIDDate
        if 'Evaluation_Evaluation' in embryo_df.columns:
            embryo_df['KIDScore'] = embryo_df['Evaluation_Evaluation']
            embryo_df = embryo_df.drop(columns=['Evaluation_Evaluation'])
        if 'Evaluation_EvaluationDate' in embryo_df.columns:
            embryo_df['KIDDate'] = embryo_df['Evaluation_EvaluationDate']
            embryo_df = embryo_df.drop(columns=['Evaluation_EvaluationDate'])
        if 'Evaluation_Model' in embryo_df.columns:
            embryo_df['KIDVersion'] = embryo_df['Evaluation_Model']
            embryo_df = embryo_df.drop(columns=['Evaluation_Model'])
        if 'Evaluation_User' in embryo_df.columns:
            embryo_df['KIDUser'] = embryo_df['Evaluation_User']
            embryo_df = embryo_df.drop(columns=['Evaluation_User'])
        # Remove EmbryoDetails_ prefix
        embryo_df = embryo_df.rename(columns={col: col.replace('EmbryoDetails_', '') for col in embryo_df.columns if col.startswith('EmbryoDetails_')})
        
        # Clean PatientID column (before filtering to ensure PatientID is properly cleaned)
        embryo_df = clean_patient_id(embryo_df, 'embryo_data', db_name)
        
        # Filter columns by null rate (exclude columns with >90% null rate)
        embryo_df, excluded_columns = filter_columns_by_null_rate(embryo_df, 'embryo_data', db_name, null_rate_threshold=90.0)
        
        if embryo_df.empty:
            logger.warning(f"[{db_name}] No columns remain after filtering for embryo_data table")
            con.close()
            return
        
        # Order columns: EmbryoID, PatientIDx, TreatmentName, all KID*, all IDA*, then the rest
        cols = list(embryo_df.columns)
        main_cols = ['EmbryoID', 'PatientIDx', 'TreatmentName']
        kid_cols = sorted([c for c in cols if c.startswith('KID')])
        ida_cols = sorted([c for c in cols if c.startswith('IDA')])
        other_cols = sorted([c for c in cols if c not in main_cols + kid_cols + ida_cols])
        ordered_cols = main_cols + kid_cols + ida_cols + other_cols
        # Only reindex with columns that exist in the filtered DataFrame
        ordered_cols = [c for c in ordered_cols if c in embryo_df.columns]
        embryo_df = embryo_df.reindex(columns=ordered_cols)
        
        # Ensure all metadata columns exist
        for col in meta_cols:
            if col not in embryo_df.columns:
                embryo_df[col] = None
        logger.info(f"[{db_name}] Saving embryo_data to table: silver.embryo_data.")
        con.execute('DROP TABLE IF EXISTS silver.embryo_data')
        con.register('embryo_df', embryo_df)
        con.execute('CREATE TABLE silver.embryo_data AS SELECT * FROM embryo_df')
        con.unregister('embryo_df')
        
        # NOTE: embryo_number is now calculated after deduplication in consolidation step
        # (removed create_embryo_number_feature call)
        
        con.close()
        logger.info(f"[{db_name}] silver.embryo_data creation complete.")
    except Exception as e:
        logger.error(f"[{db_name}] Failed to process embryo_data: {e}")

# NOTE: create_embryo_number_feature function has been moved to 02_03_consolidate_embryoscope_dbs.py
# to ensure embryo_number is calculated AFTER deduplication, preventing gaps in numbering

def update_prontuario_column(con, db_name):
    """Update prontuario column using PatientID matching logic with clinisys_all.silver.view_pacientes"""
    logger.info(f"[{db_name}] Updating prontuario column using PatientID matching logic...")
    
    try:
        # Attach clinisys_all database
        clinisys_db_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'database', 'clinisys_all.duckdb')
        logger.info(f"[{db_name}] Attaching clinisys_all database from: {clinisys_db_path}")
        con.execute(f"ATTACH '{clinisys_db_path}' AS clinisys_all")
        logger.info(f"[{db_name}] clinisys_all database attached successfully")
        
        # First pass: Active patients only (inativo = 0)
        logger.info(f"[{db_name}] === FIRST PASS: Active patients only (inativo = 0) ===")
        update_prontuario_with_inativo(con, db_name, include_inactive=False)
        
        # Second pass: Inactive patients only (inativo = 1) - only for unmatched records
        logger.info(f"[{db_name}] === SECOND PASS: Inactive patients only (inativo = 1) ===")
        update_prontuario_with_inativo(con, db_name, include_inactive=True)
        
        # Third pass: LastName condition (when LastName doesn't contain date pattern)
        logger.info(f"[{db_name}] === THIRD PASS: LastName condition (when FirstName is in LastName) ===")
        update_prontuario_with_lastname(con, db_name)
        
        # Final quality summary after all passes
        logger.info(f"[{db_name}] === FINAL PRONTUARIO MATCHING QUALITY SUMMARY ===")
        result = con.execute("""
            SELECT 
                COUNT(*) as total_rows,
                COUNT(CASE WHEN prontuario != -1 THEN 1 END) as matched_rows,
                COUNT(CASE WHEN prontuario = -1 THEN 1 END) as unmatched_rows
            FROM silver.patients
        """).fetchone()
        
        if result[0] > 0:
            match_rate = (result[1]/result[0]*100)
            logger.info(f"[{db_name}] Final prontuario matching results:")
            logger.info(f"[{db_name}]   Total rows: {result[0]:,}")
            logger.info(f"[{db_name}]   Matched rows: {result[1]:,}")
            logger.info(f"[{db_name}]   Unmatched rows: {result[2]:,}")
            logger.info(f"[{db_name}]   Match rate: {match_rate:.2f}%")
            
            # Quality assessment
            if match_rate >= 95:
                logger.info(f"[{db_name}]   Quality: EXCELLENT (>=95%)")
            elif match_rate >= 85:
                logger.info(f"[{db_name}]   Quality: GOOD (>=85%)")
            elif match_rate >= 70:
                logger.info(f"[{db_name}]   Quality: ACCEPTABLE (>=70%)")
            else:
                logger.warning(f"[{db_name}]   Quality: NEEDS ATTENTION (<70%)")
        else:
            logger.warning(f"[{db_name}] No rows found in silver.patients table")
        
        logger.info(f"[{db_name}] === END PRONTUARIO MATCHING QUALITY SUMMARY ===")
        
    except Exception as e:
        logger.error(f"[{db_name}] Error updating prontuario column: {e}")
        raise

def update_prontuario_with_inativo(con, db_name, include_inactive=False):
    """Update prontuario column using PatientID matching logic with specific inativo filter"""
    # Determine inactive filter condition
    inactive_condition = "inativo = 1" if include_inactive else "inativo = 0"
    patient_type = "inactive" if include_inactive else "active"
    logger.info(f"[{db_name}] Running PatientID-based matching logic ({patient_type} patients)...")
    
    # Build the SQL query for PatientID matching
    update_sql = f"""
    WITH 
    -- CTE 1: Extract name_first from patients (original logic - FirstName only)
    patient_name_extract AS (
        SELECT 
            "PatientID",
            prontuario,
            "FirstName",
            "LastName",
            CASE 
                -- Use FirstName as normal
                WHEN "FirstName" IS NOT NULL THEN 
                    CASE 
                        -- Handle "LastName, FirstName Middle Names" format (e.g., "VALADARES, FLAVIA.F.N." or "GIANNINI, LIVIA.")
                        WHEN POSITION(',' IN "FirstName") > 0 THEN 
                            -- Extract part after comma, normalize, then extract first sequence of letters only
                            -- Handles: "VALADARES, FLAVIA.F.N." -> "flavia", "GIANNINI, LIVIA." -> "livia"
                            REGEXP_REPLACE(
                                strip_accents(LOWER(TRIM(SPLIT_PART("FirstName", ',', 2)))),
                                '^[^a-z]*([a-z]+).*',
                                '\\1'
                            )
                        -- Handle "FirstName Middle Names" format
                        ELSE 
                            -- Extract first sequence of letters, handling cases with dots (e.g., "FLAVIA.F.N.")
                            REGEXP_REPLACE(
                                strip_accents(LOWER(TRIM("FirstName"))),
                                '^[^a-z]*([a-z]+).*',
                                '\\1'
                            )
                    END
                ELSE NULL 
            END as name_first
        FROM silver.patients
    ),
    
    -- CTE 2: Extract unmatched records using PatientID
    embryoscope_extract AS (
        SELECT DISTINCT 
            "PatientID" as patient_id,
            name_first
        FROM patient_name_extract
        WHERE prontuario = -1 
          AND "PatientID" IS NOT NULL
    ),

    -- CTE 2: Pre-process clinisys data with all transformations and accent normalization
    clinisys_processed AS (
        SELECT 
            codigo,
            prontuario_esposa,
            prontuario_marido,
            prontuario_responsavel1,
            prontuario_responsavel2,
            prontuario_esposa_pel,
            prontuario_marido_pel,
            prontuario_esposa_pc,
            prontuario_marido_pc,
            prontuario_responsavel1_pc,
            prontuario_responsavel2_pc,
            prontuario_esposa_fc,
            prontuario_marido_fc,
            prontuario_esposa_ba,
            prontuario_marido_ba,
            strip_accents(LOWER(SPLIT_PART(TRIM(esposa_nome), ' ', 1))) as esposa_nome,
            strip_accents(LOWER(SPLIT_PART(TRIM(marido_nome), ' ', 1))) as marido_nome,
            unidade_origem
        FROM clinisys_all.silver.view_pacientes
        WHERE {inactive_condition}
    ),

        -- CTE 3: PatientID ↔ prontuario (main/codigo)
        matches_1 AS (
            SELECT d.*,
                   p.codigo as prontuario, p.esposa_nome, p.marido_nome, p.unidade_origem,
                   'patientid_main' as match_type
            FROM embryoscope_extract d
            INNER JOIN clinisys_processed p 
                ON d.patient_id = p.codigo
        ),

        -- CTE 4: PatientID ↔ prontuario_esposa
        matches_2 AS (
            SELECT d.*, 
                   p.codigo as prontuario, p.esposa_nome, p.marido_nome, p.unidade_origem,
                   'patientid_esposa' as match_type
            FROM embryoscope_extract d
            INNER JOIN clinisys_processed p 
                ON d.patient_id = p.prontuario_esposa
        ),

        -- CTE 5: PatientID ↔ prontuario_marido
        matches_3 AS (
            SELECT d.*,
                  p.codigo as prontuario, p.esposa_nome, p.marido_nome, p.unidade_origem,
                   'patientid_marido' as match_type
            FROM embryoscope_extract d
            INNER JOIN clinisys_processed p 
                ON d.patient_id = p.prontuario_marido
        ),

        -- CTE 6: PatientID ↔ prontuario_responsavel1
        matches_4 AS (
            SELECT d.*,
                  p.codigo as prontuario, p.esposa_nome, p.marido_nome, p.unidade_origem,
                   'patientid_responsavel1' as match_type
            FROM embryoscope_extract d
            INNER JOIN clinisys_processed p 
                ON d.patient_id = p.prontuario_responsavel1
        ),

        -- CTE 7: PatientID ↔ prontuario_responsavel2
        matches_5 AS (
            SELECT d.*,
                   p.codigo as prontuario, p.esposa_nome, p.marido_nome, p.unidade_origem,
                   'patientid_responsavel2' as match_type
            FROM embryoscope_extract d
            INNER JOIN clinisys_processed p 
                ON d.patient_id = p.prontuario_responsavel2
        ),

        -- CTE 8: PatientID ↔ prontuario_esposa_pel
        matches_6 AS (
            SELECT d.*,
                   p.codigo as prontuario, p.esposa_nome, p.marido_nome, p.unidade_origem,
                   'patientid_esposa_pel' as match_type
            FROM embryoscope_extract d
            INNER JOIN clinisys_processed p 
                ON d.patient_id = p.prontuario_esposa_pel
        ),

        -- CTE 9: PatientID ↔ prontuario_marido_pel
        matches_7 AS (
            SELECT d.*,
                   p.codigo as prontuario, p.esposa_nome, p.marido_nome, p.unidade_origem,
                   'patientid_marido_pel' as match_type
            FROM embryoscope_extract d
            INNER JOIN clinisys_processed p 
                ON d.patient_id = p.prontuario_marido_pel
        ),

        -- CTE 10: PatientID ↔ prontuario_esposa_pc
        matches_8 AS (
            SELECT d.*,
                   p.codigo as prontuario, p.esposa_nome, p.marido_nome, p.unidade_origem,
                   'patientid_esposa_pc' as match_type
            FROM embryoscope_extract d
            INNER JOIN clinisys_processed p 
                ON d.patient_id = p.prontuario_esposa_pc
        ),

        -- CTE 11: PatientID ↔ prontuario_marido_pc
        matches_9 AS (
            SELECT d.*,
                   p.codigo as prontuario, p.esposa_nome, p.marido_nome, p.unidade_origem,
                   'patientid_marido_pc' as match_type
            FROM embryoscope_extract d
            INNER JOIN clinisys_processed p 
                ON d.patient_id = p.prontuario_marido_pc
        ),

        -- CTE 12: PatientID ↔ prontuario_responsavel1_pc
        matches_10 AS (
            SELECT d.*,
                   p.codigo as prontuario, p.esposa_nome, p.marido_nome, p.unidade_origem,
                   'patientid_responsavel1_pc' as match_type
            FROM embryoscope_extract d
            INNER JOIN clinisys_processed p 
                ON d.patient_id = p.prontuario_responsavel1_pc
        ),

        -- CTE 13: PatientID ↔ prontuario_responsavel2_pc
        matches_11 AS (
            SELECT d.*,
                   p.codigo as prontuario, p.esposa_nome, p.marido_nome, p.unidade_origem,
                   'patientid_responsavel2_pc' as match_type
            FROM embryoscope_extract d
            INNER JOIN clinisys_processed p 
                ON d.patient_id = p.prontuario_responsavel2_pc
        ),

        -- CTE 14: PatientID ↔ prontuario_esposa_fc
        matches_12 AS (
            SELECT d.*,
                   p.codigo as prontuario, p.esposa_nome, p.marido_nome, p.unidade_origem,
                   'patientid_esposa_fc' as match_type
            FROM embryoscope_extract d
            INNER JOIN clinisys_processed p 
                ON d.patient_id = p.prontuario_esposa_fc
        ),

        -- CTE 15: PatientID ↔ prontuario_marido_fc
        matches_13 AS (
            SELECT d.*,
                   p.codigo as prontuario, p.esposa_nome, p.marido_nome, p.unidade_origem,
                   'patientid_marido_fc' as match_type
            FROM embryoscope_extract d
            INNER JOIN clinisys_processed p 
                ON d.patient_id = p.prontuario_marido_fc
        ),

        -- CTE 16: PatientID ↔ prontuario_esposa_ba
        matches_14 AS (
            SELECT d.*,
                   p.codigo as prontuario, p.esposa_nome, p.marido_nome, p.unidade_origem,
                   'patientid_esposa_ba' as match_type
            FROM embryoscope_extract d
            INNER JOIN clinisys_processed p 
                ON d.patient_id = p.prontuario_esposa_ba
        ),

        -- CTE 17: PatientID ↔ prontuario_marido_ba
        matches_15 AS (
            SELECT d.*,
                   p.codigo as prontuario, p.esposa_nome, p.marido_nome, p.unidade_origem,
                   'patientid_marido_ba' as match_type
            FROM embryoscope_extract d
            INNER JOIN clinisys_processed p 
                ON d.patient_id = p.prontuario_marido_ba
        ),

        -- CTE 18: UNION matches
        all_matches AS (
            SELECT * FROM matches_1
            UNION
            SELECT * FROM matches_2
            UNION
            SELECT * FROM matches_3
            UNION 
            SELECT * FROM matches_4
            UNION
            SELECT * FROM matches_5
            UNION
            SELECT * FROM matches_6
            UNION
            SELECT * FROM matches_7
            UNION
            SELECT * FROM matches_8
            UNION
            SELECT * FROM matches_9
            UNION
            SELECT * FROM matches_10
            UNION
            SELECT * FROM matches_11
            UNION
            SELECT * FROM matches_12
            UNION
            SELECT * FROM matches_13
            UNION
            SELECT * FROM matches_14
            UNION
            SELECT * FROM matches_15
        ),

        -- CTE 19: Calculate scores for ranking
        scored_matches AS (
            SELECT *,
                   -- Calculate name match score
                   CASE 
                       WHEN name_first = esposa_nome OR name_first = marido_nome THEN 0
                       ELSE 4
                   END as name_match_score,
                   -- Calculate match type score (odd numbers)
                   CASE 
                       WHEN match_type = 'patientid_main' THEN 1
                       WHEN match_type = 'patientid_esposa' THEN 3
                       WHEN match_type = 'patientid_marido' THEN 5
                       WHEN match_type = 'patientid_responsavel1' THEN 7
                       WHEN match_type = 'patientid_responsavel2' THEN 9
                       WHEN match_type = 'patientid_esposa_pel' THEN 11
                       WHEN match_type = 'patientid_marido_pel' THEN 13
                       WHEN match_type = 'patientid_esposa_pc' THEN 15
                       WHEN match_type = 'patientid_marido_pc' THEN 17
                       WHEN match_type = 'patientid_responsavel1_pc' THEN 19
                       WHEN match_type = 'patientid_responsavel2_pc' THEN 21
                       WHEN match_type = 'patientid_esposa_fc' THEN 23
                       WHEN match_type = 'patientid_marido_fc' THEN 25
                       WHEN match_type = 'patientid_esposa_ba' THEN 27
                       WHEN match_type = 'patientid_marido_ba' THEN 29
                       ELSE 31
                   END as match_type_score
            FROM all_matches
            WHERE name_first = esposa_nome OR name_first = marido_nome
        ),

        -- CTE 20: Apply ranking based on combined scores
        ranked_matches AS (
            SELECT *,
                   (name_match_score + match_type_score) as combined_score,
                   ROW_NUMBER() OVER (
                       PARTITION BY patient_id, prontuario 
                       ORDER BY (name_match_score + match_type_score)
                   ) 
                   as rn
            FROM scored_matches
        ),

        -- CTE 21: Select best match per PatientID (lowest rn value)
        best_matches AS (
            SELECT * 
            FROM ranked_matches rm1
            WHERE rn = (
                SELECT MIN(rn) 
                FROM ranked_matches rm2 
                WHERE rm2.patient_id = rm1.patient_id
            )
        ),
        
        -- CTE 22: Join best_matches with patient_name_extract for final update
        update_matches AS (
            SELECT 
                pne."PatientID",
                COALESCE(bm.prontuario, -1) as new_prontuario
            FROM patient_name_extract pne
            INNER JOIN best_matches bm 
                ON pne."PatientID" = bm.patient_id
                AND pne.name_first = bm.name_first
            WHERE pne.prontuario = -1
        )

        -- Update prontuario column for unmatched records using PatientID
        UPDATE silver.patients 
        SET prontuario = um.new_prontuario
        FROM update_matches um
        WHERE silver.patients."PatientID" = um."PatientID"
            AND silver.patients.prontuario = -1
        """
        
    try:
        con.execute(update_sql)
        logger.info(f"[{db_name}] Prontuario column updated successfully with PatientID matching logic ({patient_type} patients)")
        
        # Get statistics on the update
        result = con.execute("""
            SELECT 
                COUNT(*) as total_rows,
                COUNT(CASE WHEN prontuario != -1 THEN 1 END) as matched_rows,
                COUNT(CASE WHEN prontuario = -1 THEN 1 END) as unmatched_rows
            FROM silver.patients
        """).fetchone()
        
        if result[0] > 0:
            match_rate = (result[1]/result[0]*100)
            logger.info(f"[{db_name}] Prontuario matching results after {patient_type} patients pass:")
            logger.info(f"[{db_name}]   Total rows: {result[0]:,}")
            logger.info(f"[{db_name}]   Matched rows: {result[1]:,}")
            logger.info(f"[{db_name}]   Unmatched rows: {result[2]:,}")
            logger.info(f"[{db_name}]   Match rate: {match_rate:.2f}%")
        else:
            logger.warning(f"[{db_name}] No rows found in silver.patients table")
            
    except Exception as e:
        logger.error(f"[{db_name}] Error updating prontuario column with {patient_type} patients: {e}")
        raise

def update_prontuario_with_lastname(con, db_name):
    """Update prontuario column using PatientID matching logic when FirstName is in LastName (no date pattern)"""
    logger.info(f"[{db_name}] Running PatientID-based matching logic (LastName condition - FirstName in LastName)...")
    
    # Build the SQL query for PatientID matching with LastName condition
    update_sql = f"""
    WITH 
    -- CTE 1: Extract name_first from LastName when it doesn't contain date pattern
    patient_name_extract_lastname AS (
        SELECT 
            "PatientID",
            prontuario,
            "FirstName",
            "LastName",
            CASE 
                -- Check if LastName contains a date pattern (dd/mm/yyyy, dd/mm/yy, or similar)
                -- If LastName doesn't contain a date pattern, FirstName is actually in LastName
                WHEN "LastName" IS NOT NULL 
                     AND NOT (
                         -- Check for date patterns using regex:
                         -- dd/mm/yyyy or dd/mm/yy (with optional leading zeros)
                         -- yyyy-mm-dd (ISO format)
                         -- dd-mm-yyyy or dd-mm-yy
                         REGEXP_MATCHES("LastName", '.*[0-9]{1,2}/[0-9]{1,2}/[0-9]{2,4}.*')
                         OR REGEXP_MATCHES("LastName", '.*[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}.*')
                         OR REGEXP_MATCHES("LastName", '.*[0-9]{1,2}-[0-9]{1,2}-[0-9]{2,4}.*')
                         -- Fallback: if it has 2+ slashes or dashes and looks like a date structure
                         OR ((LENGTH("LastName") - LENGTH(REPLACE("LastName", '/', '')) >= 2) 
                              AND LENGTH("LastName") >= 8 
                              AND REGEXP_MATCHES("LastName", '.*[0-9].*/.*[0-9].*/.*[0-9].*'))
                         OR ((LENGTH("LastName") - LENGTH(REPLACE("LastName", '-', '')) >= 2) 
                              AND LENGTH("LastName") >= 8 
                              AND REGEXP_MATCHES("LastName", '.*[0-9].*-.*[0-9].*-.*[0-9].*'))
                     ) THEN
                    -- LastName doesn't contain date pattern, so it's actually the first name
                    CASE 
                        -- Handle "LastName, FirstName Middle Names" format (e.g., "VALADARES, FLAVIA.F.N." or "GIANNINI, LIVIA.")
                        WHEN POSITION(',' IN "LastName") > 0 THEN 
                            -- Extract part after comma, normalize, then extract first sequence of letters only
                            -- Handles: "VALADARES, FLAVIA.F.N." -> "flavia", "GIANNINI, LIVIA." -> "livia"
                            REGEXP_REPLACE(
                                strip_accents(LOWER(TRIM(SPLIT_PART("LastName", ',', 2)))),
                                '^[^a-z]*([a-z]+).*',
                                '\\1'
                            )
                        -- Handle "FirstName Middle Names" format
                        ELSE 
                            -- Extract first sequence of letters, handling cases with dots (e.g., "FLAVIA.F.N.")
                            REGEXP_REPLACE(
                                strip_accents(LOWER(TRIM("LastName"))),
                                '^[^a-z]*([a-z]+).*',
                                '\\1'
                            )
                    END
                ELSE NULL 
            END as name_first
        FROM silver.patients
        WHERE prontuario = -1
          AND "PatientID" IS NOT NULL
          AND "LastName" IS NOT NULL
          AND NOT (
              -- Only process records where LastName doesn't contain date pattern
              -- Check for date patterns: dd/mm/yyyy, dd/mm/yy, yyyy-mm-dd, dd-mm-yyyy, dd-mm-yy
              REGEXP_MATCHES("LastName", '.*[0-9]{1,2}/[0-9]{1,2}/[0-9]{2,4}.*')
              OR REGEXP_MATCHES("LastName", '.*[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}.*')
              OR REGEXP_MATCHES("LastName", '.*[0-9]{1,2}-[0-9]{1,2}-[0-9]{2,4}.*')
              -- Fallback: if it has 2+ slashes or dashes and looks like a date structure
              OR ((LENGTH("LastName") - LENGTH(REPLACE("LastName", '/', '')) >= 2) 
                   AND LENGTH("LastName") >= 8 
                   AND REGEXP_MATCHES("LastName", '.*[0-9].*/.*[0-9].*/.*[0-9].*'))
              OR ((LENGTH("LastName") - LENGTH(REPLACE("LastName", '-', '')) >= 2) 
                   AND LENGTH("LastName") >= 8 
                   AND REGEXP_MATCHES("LastName", '.*[0-9].*-.*[0-9].*-.*[0-9].*'))
          )
    ),
    
    -- CTE 2: Extract unmatched records using PatientID
    embryoscope_extract AS (
        SELECT DISTINCT 
            "PatientID" as patient_id,
            name_first
        FROM patient_name_extract_lastname
        WHERE name_first IS NOT NULL
    ),

    -- CTE 3: Pre-process clinisys data with all transformations and accent normalization (active patients)
    clinisys_processed AS (
        SELECT 
            codigo,
            prontuario_esposa,
            prontuario_marido,
            prontuario_responsavel1,
            prontuario_responsavel2,
            prontuario_esposa_pel,
            prontuario_marido_pel,
            prontuario_esposa_pc,
            prontuario_marido_pc,
            prontuario_responsavel1_pc,
            prontuario_responsavel2_pc,
            prontuario_esposa_fc,
            prontuario_marido_fc,
            prontuario_esposa_ba,
            prontuario_marido_ba,
            strip_accents(LOWER(SPLIT_PART(TRIM(esposa_nome), ' ', 1))) as esposa_nome,
            strip_accents(LOWER(SPLIT_PART(TRIM(marido_nome), ' ', 1))) as marido_nome,
            unidade_origem
        FROM clinisys_all.silver.view_pacientes
        WHERE inativo = 0
    ),

    -- CTE 4-18: PatientID matching CTEs (same as before)
    matches_1 AS (
        SELECT d.*,
               p.codigo as prontuario, p.esposa_nome, p.marido_nome, p.unidade_origem,
               'patientid_main' as match_type
        FROM embryoscope_extract d
        INNER JOIN clinisys_processed p 
            ON d.patient_id = p.codigo
    ),

    matches_2 AS (
        SELECT d.*, 
               p.codigo as prontuario, p.esposa_nome, p.marido_nome, p.unidade_origem,
               'patientid_esposa' as match_type
        FROM embryoscope_extract d
        INNER JOIN clinisys_processed p 
            ON d.patient_id = p.prontuario_esposa
    ),

    matches_3 AS (
        SELECT d.*,
              p.codigo as prontuario, p.esposa_nome, p.marido_nome, p.unidade_origem,
               'patientid_marido' as match_type
        FROM embryoscope_extract d
        INNER JOIN clinisys_processed p 
            ON d.patient_id = p.prontuario_marido
    ),

    matches_4 AS (
        SELECT d.*,
              p.codigo as prontuario, p.esposa_nome, p.marido_nome, p.unidade_origem,
               'patientid_responsavel1' as match_type
        FROM embryoscope_extract d
        INNER JOIN clinisys_processed p 
            ON d.patient_id = p.prontuario_responsavel1
    ),

    matches_5 AS (
        SELECT d.*,
               p.codigo as prontuario, p.esposa_nome, p.marido_nome, p.unidade_origem,
               'patientid_responsavel2' as match_type
        FROM embryoscope_extract d
        INNER JOIN clinisys_processed p 
            ON d.patient_id = p.prontuario_responsavel2
    ),

    matches_6 AS (
        SELECT d.*,
               p.codigo as prontuario, p.esposa_nome, p.marido_nome, p.unidade_origem,
               'patientid_esposa_pel' as match_type
        FROM embryoscope_extract d
        INNER JOIN clinisys_processed p 
            ON d.patient_id = p.prontuario_esposa_pel
    ),

    matches_7 AS (
        SELECT d.*,
               p.codigo as prontuario, p.esposa_nome, p.marido_nome, p.unidade_origem,
               'patientid_marido_pel' as match_type
        FROM embryoscope_extract d
        INNER JOIN clinisys_processed p 
            ON d.patient_id = p.prontuario_marido_pel
    ),

    matches_8 AS (
        SELECT d.*,
               p.codigo as prontuario, p.esposa_nome, p.marido_nome, p.unidade_origem,
               'patientid_esposa_pc' as match_type
        FROM embryoscope_extract d
        INNER JOIN clinisys_processed p 
            ON d.patient_id = p.prontuario_esposa_pc
    ),

    matches_9 AS (
        SELECT d.*,
               p.codigo as prontuario, p.esposa_nome, p.marido_nome, p.unidade_origem,
               'patientid_marido_pc' as match_type
        FROM embryoscope_extract d
        INNER JOIN clinisys_processed p 
            ON d.patient_id = p.prontuario_marido_pc
    ),

    matches_10 AS (
        SELECT d.*,
               p.codigo as prontuario, p.esposa_nome, p.marido_nome, p.unidade_origem,
               'patientid_responsavel1_pc' as match_type
        FROM embryoscope_extract d
        INNER JOIN clinisys_processed p 
            ON d.patient_id = p.prontuario_responsavel1_pc
    ),

    matches_11 AS (
        SELECT d.*,
               p.codigo as prontuario, p.esposa_nome, p.marido_nome, p.unidade_origem,
               'patientid_responsavel2_pc' as match_type
        FROM embryoscope_extract d
        INNER JOIN clinisys_processed p 
            ON d.patient_id = p.prontuario_responsavel2_pc
    ),

    matches_12 AS (
        SELECT d.*,
               p.codigo as prontuario, p.esposa_nome, p.marido_nome, p.unidade_origem,
               'patientid_esposa_fc' as match_type
        FROM embryoscope_extract d
        INNER JOIN clinisys_processed p 
            ON d.patient_id = p.prontuario_esposa_fc
    ),

    matches_13 AS (
        SELECT d.*,
               p.codigo as prontuario, p.esposa_nome, p.marido_nome, p.unidade_origem,
               'patientid_marido_fc' as match_type
        FROM embryoscope_extract d
        INNER JOIN clinisys_processed p 
            ON d.patient_id = p.prontuario_marido_fc
    ),

    matches_14 AS (
        SELECT d.*,
               p.codigo as prontuario, p.esposa_nome, p.marido_nome, p.unidade_origem,
               'patientid_esposa_ba' as match_type
        FROM embryoscope_extract d
        INNER JOIN clinisys_processed p 
            ON d.patient_id = p.prontuario_esposa_ba
    ),

    matches_15 AS (
        SELECT d.*,
               p.codigo as prontuario, p.esposa_nome, p.marido_nome, p.unidade_origem,
               'patientid_marido_ba' as match_type
        FROM embryoscope_extract d
        INNER JOIN clinisys_processed p 
            ON d.patient_id = p.prontuario_marido_ba
    ),

    -- CTE 19: UNION matches
    all_matches AS (
        SELECT * FROM matches_1
        UNION
        SELECT * FROM matches_2
        UNION
        SELECT * FROM matches_3
        UNION 
        SELECT * FROM matches_4
        UNION
        SELECT * FROM matches_5
        UNION
        SELECT * FROM matches_6
        UNION
        SELECT * FROM matches_7
        UNION
        SELECT * FROM matches_8
        UNION
        SELECT * FROM matches_9
        UNION
        SELECT * FROM matches_10
        UNION
        SELECT * FROM matches_11
        UNION
        SELECT * FROM matches_12
        UNION
        SELECT * FROM matches_13
        UNION
        SELECT * FROM matches_14
        UNION
        SELECT * FROM matches_15
    ),

    -- CTE 20: Calculate scores for ranking
    scored_matches AS (
        SELECT *,
               -- Calculate name match score
               CASE 
                   WHEN name_first = esposa_nome OR name_first = marido_nome THEN 0
                   ELSE 4
               END as name_match_score,
               -- Calculate match type score (odd numbers)
               CASE 
                   WHEN match_type = 'patientid_main' THEN 1
                   WHEN match_type = 'patientid_esposa' THEN 3
                   WHEN match_type = 'patientid_marido' THEN 5
                   WHEN match_type = 'patientid_responsavel1' THEN 7
                   WHEN match_type = 'patientid_responsavel2' THEN 9
                   WHEN match_type = 'patientid_esposa_pel' THEN 11
                   WHEN match_type = 'patientid_marido_pel' THEN 13
                   WHEN match_type = 'patientid_esposa_pc' THEN 15
                   WHEN match_type = 'patientid_marido_pc' THEN 17
                   WHEN match_type = 'patientid_responsavel1_pc' THEN 19
                   WHEN match_type = 'patientid_responsavel2_pc' THEN 21
                   WHEN match_type = 'patientid_esposa_fc' THEN 23
                   WHEN match_type = 'patientid_marido_fc' THEN 25
                   WHEN match_type = 'patientid_esposa_ba' THEN 27
                   WHEN match_type = 'patientid_marido_ba' THEN 29
                   ELSE 31
               END as match_type_score
        FROM all_matches
        WHERE name_first = esposa_nome OR name_first = marido_nome
    ),

    -- CTE 21: Apply ranking based on combined scores
    ranked_matches AS (
        SELECT *,
               (name_match_score + match_type_score) as combined_score,
               ROW_NUMBER() OVER (
                   PARTITION BY patient_id, prontuario 
                   ORDER BY (name_match_score + match_type_score)
               ) 
               as rn
        FROM scored_matches
    ),

    -- CTE 22: Select best match per PatientID (lowest rn value)
    best_matches AS (
        SELECT * 
        FROM ranked_matches rm1
        WHERE rn = (
            SELECT MIN(rn) 
            FROM ranked_matches rm2 
            WHERE rm2.patient_id = rm1.patient_id
        )
    ),
    
    -- CTE 23: Join best_matches with patient_name_extract_lastname for final update
    update_matches AS (
        SELECT 
            pne."PatientID",
            COALESCE(bm.prontuario, -1) as new_prontuario
        FROM patient_name_extract_lastname pne
        INNER JOIN best_matches bm 
            ON pne."PatientID" = bm.patient_id
            AND pne.name_first = bm.name_first
        WHERE pne.prontuario = -1
    )

    -- Update prontuario column for unmatched records using PatientID
    UPDATE silver.patients 
    SET prontuario = um.new_prontuario
    FROM update_matches um
    WHERE silver.patients."PatientID" = um."PatientID"
        AND silver.patients.prontuario = -1
    """
    
    try:
        con.execute(update_sql)
        logger.info(f"[{db_name}] Prontuario column updated successfully with PatientID matching logic (LastName condition)")
        
        # Get statistics on the update
        result = con.execute("""
            SELECT 
                COUNT(*) as total_rows,
                COUNT(CASE WHEN prontuario != -1 THEN 1 END) as matched_rows,
                COUNT(CASE WHEN prontuario = -1 THEN 1 END) as unmatched_rows
            FROM silver.patients
        """).fetchone()
        
        if result[0] > 0:
            match_rate = (result[1]/result[0]*100)
            logger.info(f"[{db_name}] Prontuario matching results after LastName condition pass:")
            logger.info(f"[{db_name}]   Total rows: {result[0]:,}")
            logger.info(f"[{db_name}]   Matched rows: {result[1]:,}")
            logger.info(f"[{db_name}]   Unmatched rows: {result[2]:,}")
            logger.info(f"[{db_name}]   Match rate: {match_rate:.2f}%")
        else:
            logger.warning(f"[{db_name}] No rows found in silver.patients table")
            
    except Exception as e:
        logger.error(f"[{db_name}] Error updating prontuario column with LastName condition: {e}")
        raise

def main():
    db_dir = r'G:/My Drive/projetos_individuais/Huntington/database'
    logger.info('Starting embryoscope bronze to silver loader (EXCLUDING COLUMNS WITH >90% NULL RATE)')
    logger.info(f"Looking for DuckDB files in: {db_dir}")
    logger.debug(f"Looking for DuckDB files in: {db_dir}")
    db_paths = glob.glob(os.path.join(db_dir, '*.db'))
    logger.info(f"Found DB files: {db_paths}")
    logger.debug(f"Found DB files: {db_paths}")
    if not db_paths:
        logger.warning("No DuckDB databases found in database/ directory.")
        return
    for db_path in db_paths:
        db_name = os.path.basename(db_path)
        logger.info(f"Starting processing for {db_path}")
        logger.debug(f"Starting processing for {db_path}")
        process_database(db_path)
        process_treatments_database(db_path)
        process_idascore_database(db_path)
        process_embryo_data_database(db_path)
        logger.info(f"Finished processing for {db_path}")
        logger.debug(f"Finished processing for {db_path}")
    
    # Create indexes for better JOIN performance in gold layer
    logger.info('Creating indexes for better JOIN performance...')
    for db_path in db_paths:
        db_name = os.path.basename(db_path)
        try:
            con = duckdb.connect(db_path)
            index_queries = [
                "CREATE INDEX IF NOT EXISTS idx_embryo_fertilization_time ON silver.embryo_data(FertilizationTime)",
                "CREATE INDEX IF NOT EXISTS idx_embryo_patient_id ON silver.embryo_data(PatientIDx)",
                "CREATE INDEX IF NOT EXISTS idx_patients_patient_id ON silver.patients(PatientID)"
            ]
            
            for idx_query in index_queries:
                try:
                    con.execute(idx_query)
                except Exception as e:
                    logger.warning(f'[{db_name}] Index creation failed (may already exist): {e}')
            
            con.close()
            logger.info(f'[{db_name}] Indexes created successfully')
        except Exception as e:
            logger.error(f'[{db_name}] Error creating indexes: {e}')
    
    logger.info('All embryoscope databases processed successfully')

if __name__ == '__main__':
    main() 