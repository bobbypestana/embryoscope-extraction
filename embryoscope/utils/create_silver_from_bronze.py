import os
import glob
import duckdb
import json
import logging
import pandas as pd
from datetime import datetime
import collections

# Ensure logs directory exists
log_dir = os.path.abspath(os.path.join(os.getcwd(), 'embryoscope', 'logs'))
os.makedirs(log_dir, exist_ok=True)
log_ts = datetime.now().strftime('%Y%m%d_%H%M%S')
log_file = os.path.join(log_dir, f'create_silver_from_bronze_{log_ts}.log')

# Setup logger with custom format
logger = logging.getLogger('create_silver_from_bronze')
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s,%(msecs)03d - %(name)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
file_handler = logging.FileHandler(log_file)
file_handler.setFormatter(formatter)
if not logger.hasHandlers():
    logger.addHandler(file_handler)

print(f"[INFO] Logging to: {log_file}")

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
    print(f"[INFO] Processing patients for database: {db_path}")
    try:
        con = duckdb.connect(db_path)
        read_query = 'SELECT raw_json FROM bronze.raw_patients'
        print(f"[DEBUG] Reading patients with query: {read_query}")
        logger.info(f"[{db_name}] Reading patients with query: {read_query}")
        df = con.execute(read_query).fetchdf()
        logger.info(f"[{db_name}] Read {len(df)} rows from bronze.raw_patients.")
        all_patients = []
        for idx, row in df.iterrows():
            patients = flatten_patients_json(row['raw_json'])
            all_patients.extend(patients)
        logger.info(f"[{db_name}] Flattened to {len(all_patients)} patient records.")
        if not all_patients:
            logger.warning(f"[{db_name}] No patients found.")
            print(f"[WARNING] No patients found in {db_path}")
            return
        patients_df = pd.DataFrame(all_patients)
        if 'DateOfBirth' in patients_df.columns:
            patients_df['DateOfBirth'] = pd.to_datetime(patients_df['DateOfBirth'], errors='coerce')
        print(f"[DEBUG] Saving patients to table: silver.patients in database: {db_path}")
        logger.info(f"[{db_name}] Saving patients to table: silver.patients.")
        con.execute('DROP TABLE IF EXISTS silver.patients')
        con.register('patients_df', patients_df)
        con.execute('CREATE TABLE silver.patients AS SELECT * FROM patients_df')
        con.unregister('patients_df')
        con.close()
        logger.info(f"[{db_name}] silver.patients creation complete.")
        print(f"[SUCCESS] silver.patients written for {db_path}")
    except Exception as e:
        logger.error(f"[{db_name}] Failed to process patients: {e}")
        print(f"[ERROR] Failed to process patients for {db_path}: {e}")

def process_treatments_database(db_path):
    db_name = os.path.basename(db_path)
    logger.info(f"[{db_name}] Processing treatments table.")
    print(f"[INFO] Processing treatments for database: {db_path}")
    try:
        con = duckdb.connect(db_path)
        con.execute('CREATE SCHEMA IF NOT EXISTS silver')
        read_query = 'SELECT raw_json FROM bronze.raw_treatments'
        print(f"[DEBUG] Reading treatments with query: {read_query}")
        logger.info(f"[{db_name}] Reading treatments with query: {read_query}")
        df = con.execute(read_query).fetchdf()
        logger.info(f"[{db_name}] Read {len(df)} rows from bronze.raw_treatments.")
        all_treatments = []
        for idx, row in df.iterrows():
            try:
                treatment = json.loads(str(row['raw_json']))
                all_treatments.append(treatment)
            except Exception as e:
                logger.error(f"[{db_name}] Error parsing treatment JSON at row {idx}: {e}")
                print(f"[ERROR] Error parsing treatment JSON in {db_path} at row {idx}: {e}")
        logger.info(f"[{db_name}] Parsed {len(all_treatments)} treatment records.")
        if not all_treatments:
            logger.warning(f"[{db_name}] No treatments found.")
            print(f"[WARNING] No treatments found in {db_path}")
            return
        treatments_df = pd.DataFrame(all_treatments)
        for col in treatments_df.columns:
            treatments_df[col] = treatments_df[col].astype(str)
        print(f"[DEBUG] Saving treatments to table: silver.treatments in database: {db_path}")
        logger.info(f"[{db_name}] Saving treatments to table: silver.treatments.")
        con.execute('DROP TABLE IF EXISTS silver.treatments')
        con.register('treatments_df', treatments_df)
        con.execute('CREATE TABLE silver.treatments AS SELECT * FROM treatments_df')
        con.unregister('treatments_df')
        con.close()
        logger.info(f"[{db_name}] silver.treatments creation complete.")
        print(f"[SUCCESS] silver.treatments written for {db_path}")
    except Exception as e:
        logger.error(f"[{db_name}] Failed to process treatments: {e}")
        print(f"[ERROR] Failed to process treatments for {db_path}: {e}")

def process_idascore_database(db_path):
    db_name = os.path.basename(db_path)
    logger.info(f"[{db_name}] Processing idascore table.")
    print(f"[INFO] Processing idascore for database: {db_path}")
    try:
        con = duckdb.connect(db_path)
        con.execute('CREATE SCHEMA IF NOT EXISTS silver')
        read_query = 'SELECT raw_json FROM bronze.raw_idascore'
        print(f"[DEBUG] Reading idascore with query: {read_query}")
        logger.info(f"[{db_name}] Reading idascore with query: {read_query}")
        try:
            df = con.execute(read_query).fetchdf()
        except Exception as e:
            logger.warning(f"[{db_name}] Table bronze.raw_idascore does not exist: {e}")
            print(f"[WARNING] Table bronze.raw_idascore does not exist in {db_path}")
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
                all_idascore.append(mapped_record)
            except Exception as e:
                logger.error(f"[{db_name}] Error parsing idascore JSON at row {idx}: {e}")
                print(f"[ERROR] Error parsing idascore JSON in {db_path} at row {idx}: {e}")
        logger.info(f"[{db_name}] Parsed {len(all_idascore)} idascore records.")
        if not all_idascore:
            logger.warning(f"[{db_name}] No idascore records found.")
            print(f"[WARNING] No idascore records found in {db_path}")
            # Create empty table with correct columns
            print(f"[DEBUG] Creating empty silver.idascore table in database: {db_path}")
            logger.info(f"[{db_name}] Creating empty silver.idascore table.")
            con.execute('DROP TABLE IF EXISTS silver.idascore')
            con.execute('CREATE TABLE silver.idascore (EmbryoID TEXT, IDAScore TEXT, IDATime TEXT, IDAVersion TEXT, IDATimestamp TEXT)')
            con.close()
            logger.info(f"[{db_name}] Empty silver.idascore table created.")
            print(f"[SUCCESS] Empty silver.idascore table created for {db_path}")
            return
        idascore_df = pd.DataFrame(all_idascore)
        for col in idascore_df.columns:
            idascore_df[col] = idascore_df[col].astype(str)
        print(f"[DEBUG] Saving idascore to table: silver.idascore in database: {db_path}")
        logger.info(f"[{db_name}] Saving idascore to table: silver.idascore.")
        con.execute('DROP TABLE IF EXISTS silver.idascore')
        con.register('idascore_df', idascore_df)
        con.execute('CREATE TABLE silver.idascore AS SELECT * FROM idascore_df')
        con.unregister('idascore_df')
        con.close()
        logger.info(f"[{db_name}] silver.idascore creation complete.")
        print(f"[SUCCESS] silver.idascore written for {db_path}")
    except Exception as e:
        logger.error(f"[{db_name}] Failed to process idascore: {e}")
        print(f"[ERROR] Failed to process idascore for {db_path}: {e}")

def process_embryo_data_database(db_path):
    db_name = os.path.basename(db_path)
    logger.info(f"[{db_name}] Processing embryo_data table.")
    print(f"[INFO] Processing embryo_data for database: {db_path}")
    try:
        con = duckdb.connect(db_path)
        con.execute('CREATE SCHEMA IF NOT EXISTS silver')
        read_query = 'SELECT raw_json FROM bronze.raw_embryo_data'
        print(f"[DEBUG] Reading embryo_data with query: {read_query}")
        logger.info(f"[{db_name}] Reading embryo_data with query: {read_query}")
        try:
            df = con.execute(read_query).fetchdf()
        except Exception as e:
            logger.warning(f"[{db_name}] Table bronze.raw_embryo_data does not exist: {e}")
            print(f"[WARNING] Table bronze.raw_embryo_data does not exist in {db_path}")
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
        logger.info(f"[{db_name}] Found annotation types: {sorted(annotation_names)}")
        # Second pass: flatten all rows
        all_embryos = []
        for idx, row in df.iterrows():
            flat = flatten_embryo_json(row['raw_json'], annotation_names_set=annotation_names)
            all_embryos.append(flat)
        logger.info(f"[{db_name}] Flattened {len(all_embryos)} embryo records.")
        if not all_embryos:
            logger.warning(f"[{db_name}] No embryo_data records found.")
            print(f"[WARNING] No embryo_data records found in {db_path}")
            # Create empty table with all expected columns
            columns = [
                'EmbryoID', 'PatientIDx', 'TreatmentName',
                'EmbryoDetails_InstrumentNumber', 'EmbryoDetails_Position', 'EmbryoDetails_WellNumber',
                'EmbryoDetails_FertilizationTime', 'EmbryoDetails_FertilizationMethod', 'EmbryoDetails_EmbryoFate',
                'EmbryoDetails_Description', 'EmbryoDetails_EmbryoDescriptionID',
                'Evaluation_Model', 'Evaluation_User', 'Evaluation_EvaluationDate'
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
            print(f"[SUCCESS] Empty silver.embryo_data table created for {db_path}")
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
        print(f"[DEBUG] Saving embryo_data to table: silver.embryo_data in database: {db_path}")
        logger.info(f"[{db_name}] Saving embryo_data to table: silver.embryo_data.")
        con.execute('DROP TABLE IF EXISTS silver.embryo_data')
        con.register('embryo_df', embryo_df)
        con.execute('CREATE TABLE silver.embryo_data AS SELECT * FROM embryo_df')
        con.unregister('embryo_df')
        con.close()
        logger.info(f"[{db_name}] silver.embryo_data creation complete.")
        print(f"[SUCCESS] silver.embryo_data written for {db_path}")
    except Exception as e:
        logger.error(f"[{db_name}] Failed to process embryo_data: {e}")
        print(f"[ERROR] Failed to process embryo_data for {db_path}: {e}")

def main():
    db_dir = os.path.abspath(os.path.join(os.getcwd(), 'database'))
    print(f"[DEBUG] Looking for DuckDB files in: {db_dir}")
    db_paths = glob.glob(os.path.join(db_dir, '*.db'))
    print(f"[DEBUG] Found DB files: {db_paths}")
    if not db_paths:
        logger.warning("No DuckDB databases found in database/ directory.")
        print("[WARNING] No DuckDB databases found in database/ directory.")
        return
    for db_path in db_paths:
        db_name = os.path.basename(db_path)
        print(f"[INFO] Starting processing for {db_path}")
        logger.info(f"[{db_name}] Starting processing.")
        process_database(db_path)
        process_treatments_database(db_path)
        process_idascore_database(db_path)
        process_embryo_data_database(db_path)
        print(f"[INFO] Finished processing for {db_path}")
        logger.info(f"[{db_name}] Finished processing.")

if __name__ == '__main__':
    main() 