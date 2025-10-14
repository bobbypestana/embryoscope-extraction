import os
import glob
import duckdb
import json
import logging
import pandas as pd
from datetime import datetime
import collections
import yaml

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
        # Cast types
        for col in embryo_df.columns:
            if col.startswith('Time_'):
                try:
                    embryo_df[col] = pd.to_numeric(embryo_df[col], errors='coerce')
                except Exception as e:
                    logger.error(f"[{db_name}] Error casting {col} to numeric: {e}")
            elif col.endswith('Date') or col.endswith('Time') or col.endswith('Timestamp'):
                try:
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
        # Order columns: EmbryoID, PatientIDx, TreatmentName, all KID*, all IDA*, then the rest
        cols = list(embryo_df.columns)
        main_cols = ['EmbryoID', 'PatientIDx', 'TreatmentName']
        kid_cols = sorted([c for c in cols if c.startswith('KID')])
        ida_cols = sorted([c for c in cols if c.startswith('IDA')])
        other_cols = sorted([c for c in cols if c not in main_cols + kid_cols + ida_cols])
        ordered_cols = main_cols + kid_cols + ida_cols + other_cols
        embryo_df = embryo_df.reindex(columns=ordered_cols)
        
        # Clean PatientID column
        embryo_df = clean_patient_id(embryo_df, 'embryo_data', db_name)
        
        # Ensure all metadata columns exist
        for col in meta_cols:
            if col not in embryo_df.columns:
                embryo_df[col] = None
        logger.info(f"[{db_name}] Saving embryo_data to table: silver.embryo_data.")
        con.execute('DROP TABLE IF EXISTS silver.embryo_data')
        con.register('embryo_df', embryo_df)
        con.execute('CREATE TABLE silver.embryo_data AS SELECT * FROM embryo_df')
        con.unregister('embryo_df')
        
        # Add embryo_number feature
        create_embryo_number_feature(con, db_name)
        
        con.close()
        logger.info(f"[{db_name}] silver.embryo_data creation complete.")
    except Exception as e:
        logger.error(f"[{db_name}] Failed to process embryo_data: {e}")

def create_embryo_number_feature(con, db_name):
    """Add embryo_number column to silver.embryo_data with proper sorting logic for EmbryoDescriptionID.
    
    The embryo_number is assigned sequentially within each patient-treatment combination,
    ensuring unique numbering per patient even when multiple patients share the same treatment name.
    """
    logger.info(f"[{db_name}] Adding embryo_number feature to silver.embryo_data")
    
    try:
        # Add embryo_number directly using a window function with proper sorting
        # Transform EmbryoDescriptionID inline for sorting: AA1 -> AA01, AA2 -> AA02, etc.
        # FIXED: Partition by both PatientIDx and TreatmentName to ensure unique numbering per patient-treatment
        con.execute("""
            CREATE OR REPLACE TABLE silver.embryo_data AS
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY PatientIDx, TreatmentName 
                       ORDER BY 
                           CASE 
                               WHEN EmbryoDescriptionID IS NULL THEN NULL
                               WHEN regexp_matches(EmbryoDescriptionID, '^[A-Z]+[0-9]+$') THEN
                                   regexp_replace(EmbryoDescriptionID, '^([A-Z]+)([0-9]+)$', 
                                                 '\\1' || lpad(regexp_extract(EmbryoDescriptionID, '([0-9]+)$', 1), 2, '0'))
                               ELSE EmbryoDescriptionID
                           END
                   ) AS embryo_number
            FROM silver.embryo_data
        """)
        
        logger.info(f"[{db_name}] embryo_number feature added successfully")
        
    except Exception as e:
        logger.error(f"[{db_name}] Error adding embryo_number feature: {e}")
        raise

def update_prontuario_column(con, db_name):
    """Update prontuario column using PatientID matching logic with clinisys_all.silver.view_pacientes"""
    logger.info(f"[{db_name}] Updating prontuario column using PatientID matching logic...")
    
    try:
        # Attach clinisys_all database
        clinisys_db_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'database', 'clinisys_all.duckdb')
        logger.info(f"[{db_name}] Attaching clinisys_all database from: {clinisys_db_path}")
        con.execute(f"ATTACH '{clinisys_db_path}' AS clinisys_all")
        logger.info(f"[{db_name}] clinisys_all database attached successfully")
        
        # Build the SQL query for PatientID matching
        update_sql = f"""
        WITH 
        -- CTE 1: Extract unmatched records using PatientID
        embryoscope_extract AS (
            SELECT DISTINCT 
                "PatientID" as patient_id,
                CASE 
                    WHEN "FirstName" IS NOT NULL THEN 
                        CASE 
                            -- Handle "LastName, FirstName Middle Names" format
                            WHEN POSITION(',' IN "FirstName") > 0 THEN 
                                strip_accents(LOWER(SPLIT_PART(TRIM(SPLIT_PART("FirstName", ',', 2)), ' ', 1)))
                            -- Handle "FirstName Middle Names" format
                            ELSE strip_accents(LOWER(SPLIT_PART(TRIM("FirstName"), ' ', 1)))
                        END
                    ELSE NULL 
                END as name_first
            FROM silver.patients
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
            WHERE inativo = 0
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
        )

        -- Update prontuario column for unmatched records using PatientID
        UPDATE silver.patients 
        SET prontuario = COALESCE(bm.prontuario, -1)
        FROM best_matches bm 
        WHERE silver.patients."PatientID" = bm.patient_id
            AND silver.patients.prontuario = -1
            AND (
                CASE 
                    -- Handle "LastName, FirstName Middle Names" format
                    WHEN POSITION(',' IN silver.patients."FirstName") > 0 THEN 
                        strip_accents(LOWER(SPLIT_PART(TRIM(SPLIT_PART(silver.patients."FirstName", ',', 2)), ' ', 1)))
                    -- Handle "FirstName Middle Names" format
                    ELSE strip_accents(LOWER(SPLIT_PART(TRIM(silver.patients."FirstName"), ' ', 1)))
                END
            ) = bm.name_first
        """
        
        con.execute(update_sql)
        logger.info(f"[{db_name}] Prontuario column updated successfully with PatientID matching logic")
        
        # Get statistics on the update
        result = con.execute("""
            SELECT 
                COUNT(*) as total_rows,
                COUNT(CASE WHEN prontuario != -1 THEN 1 END) as matched_rows,
                COUNT(CASE WHEN prontuario = -1 THEN 1 END) as unmatched_rows
            FROM silver.patients
        """).fetchone()
        
        logger.info(f"[{db_name}] Prontuario matching results:")
        logger.info(f"[{db_name}]   Total rows: {result[0]:,}")
        logger.info(f"[{db_name}]   Matched rows: {result[1]:,}")
        logger.info(f"[{db_name}]   Unmatched rows: {result[2]:,}")
        logger.info(f"[{db_name}]   Match rate: {(result[1]/result[0]*100):.2f}%")
        
    except Exception as e:
        logger.error(f"[{db_name}] Error updating prontuario column: {e}")
        raise

def main():
    db_dir = r'G:/My Drive/projetos_individuais/Huntington/database'
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

if __name__ == '__main__':
    main() 