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
project_root = os.path.abspath(os.path.join(script_dir, '..'))
log_dir = os.path.join(project_root, 'logs')
os.makedirs(log_dir, exist_ok=True)
log_ts = datetime.now().strftime('%Y%m%d_%H%M%S')
log_file = os.path.join(log_dir, f'create_silver_from_bronze_{log_ts}.log')

# Load log level from params.yml
params_path = os.path.join(project_root, 'params.yml')
with open(params_path, 'r') as f:
    params = yaml.safe_load(f)
log_level_str = params.get('extraction', {}).get('log_level', 'INFO').upper()
log_level = getattr(logging, log_level_str, logging.INFO)

# Setup logger with custom format
logger = logging.getLogger('create_silver_from_bronze')
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
        con.close()
        logger.info(f"[{db_name}] silver.patients creation complete.")
        logger.debug(f"[{db_name}] silver.patients written for {db_path}")
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
    """Add embryo_number column to silver.embryo_data with proper sorting logic for EmbryoDescriptionID."""
    logger.info(f"[{db_name}] Adding embryo_number feature to silver.embryo_data")
    
    try:
        # Add embryo_number directly using a window function with proper sorting
        # Transform EmbryoDescriptionID inline for sorting: AA1 -> AA01, AA2 -> AA02, etc.
        con.execute("""
            CREATE OR REPLACE TABLE silver.embryo_data AS
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY TreatmentName 
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