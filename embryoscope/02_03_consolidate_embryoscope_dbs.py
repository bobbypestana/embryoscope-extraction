import os
import duckdb
import pandas as pd
from glob import glob
from pathlib import Path
import logging
from datetime import datetime
import yaml

# Configuration
script_dir = Path(__file__).resolve().parent
project_root = script_dir  # Use current directory since params.yml is here
params_path = project_root / 'params.yml'
with open(params_path, 'r') as f:
    params = yaml.safe_load(f)
log_level_str = params.get('extraction', {}).get('log_level', 'INFO').upper()
log_level = getattr(logging, log_level_str, logging.INFO)

DATABASE_DIR = project_root.parent / 'database'
LOGS_DIR = project_root / 'logs'
LOGS_DIR.mkdir(exist_ok=True)
log_filename = LOGS_DIR / f"consolidate_embryoscope_dbs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logger = logging.getLogger('consolidate_embryoscope_dbs')
logger.setLevel(log_level)
formatter = logging.Formatter('%(asctime)s,%(msecs)03d - %(name)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
file_handler = logging.FileHandler(log_filename, encoding='utf-8')
file_handler.setFormatter(formatter)
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
if not logger.hasHandlers():
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
logger.info(f"Logging to: {log_filename}")

# Instead of a single SCHEMA, use a schema per system under 'silver'
SYSTEM = 'embryoscope'
CENTRAL_SCHEMA = f'silver_{SYSTEM}'
READ_SCHEMA = 'silver'
TABLES = {
    'patients': {
        'table': 'patients',
        'business_keys': ['PatientIDx'],
    },
    'treatments': {
        'table': 'treatments',
        'business_keys': ['PatientIDx', 'TreatmentName'],
    },
    'embryo_data': {
        'table': 'embryo_data',
        'business_keys': ['EmbryoID'],
    },
    'idascore': {
        'table': 'idascore',
        'business_keys': ['EmbryoID'],
    },
}

# Helper to get clinic name from DB filename
def get_clinic_name(db_path):
    name = Path(db_path).stem.replace('embryoscope_', '').replace('.db', '').replace('.duckdb', '')
    return name.replace('_', ' ').title()

# Find all per-clinic DBs (exclude test and central)
def find_clinic_dbs():
    dbs = []
    logger.debug(f"Looking for clinic DBs in: {DATABASE_DIR}")
    for db_path in glob(str(DATABASE_DIR / 'embryoscope_*.db')):
        logger.debug(f"Found DB candidate: {db_path}")
        if 'test' in db_path or 'huntington_data_lake' in db_path:
            logger.debug(f"Skipping DB (test or central): {db_path}")
            continue
        dbs.append(db_path)
    logger.debug(f"Clinic DBs to use: {dbs}")
    return dbs

def read_table_from_db(db_path, table, clinic_name):
    logger.debug(f"Reading table {table} from {db_path} for clinic {clinic_name}")
    con = duckdb.connect(db_path, read_only=True)
    try:
        query = f"SELECT * FROM {READ_SCHEMA}.{table}"
        logger.debug(f"Executing query: {query}")
        df = con.execute(query).df()
        logger.debug(f"Read {len(df)} rows from {table} in {db_path}")
        df['unit_huntington'] = clinic_name
        return df
    except Exception as e:
        logger.warning(f"Could not read {table} from {db_path}: {e}")
        return pd.DataFrame()
    finally:
        con.close()

def consolidate_table(table_key, table_info, db_paths):
    logger.debug(f"Consolidating table {table_info['table']} from DBs: {db_paths}")
    all_dfs = []
    for db_path in db_paths:
        clinic_name = get_clinic_name(db_path)
        df = read_table_from_db(db_path, table_info['table'], clinic_name)
        if not df.empty:
            logger.debug(f"Adding {len(df)} rows from {db_path} to consolidated DataFrame")
            all_dfs.append(df)
        else:
            logger.debug(f"No data found in {db_path} for table {table_info['table']}")
    if not all_dfs:
        logger.debug(f"No data found for table {table_info['table']} in any DB.")
        return pd.DataFrame()
    df_all = pd.concat(all_dfs, ignore_index=True, sort=False)
    logger.debug(f"Concatenated DataFrame shape for {table_info['table']}: {df_all.shape}")
    sort_cols = table_info['business_keys'] + ['_extraction_timestamp']
    logger.debug(f"Sorting by columns: {sort_cols}")
    df_all = df_all.sort_values(sort_cols, ascending=[True]*len(table_info['business_keys']) + [False])
    df_dedup = df_all.drop_duplicates(subset=table_info['business_keys'], keep='first')
    logger.debug(f"Deduplicated DataFrame shape for {table_info['table']}: {df_dedup.shape}")
    return df_dedup

def write_table_to_central_db(df, table, schema, central_db):
    logger.debug(f"Writing table {table} to central DB: {central_db}, schema: {schema}")
    if df.empty:
        logger.info(f"No data to write for {table}")
        return
    con = duckdb.connect(central_db)
    try:
        logger.debug(f"Creating schema if not exists: {schema}")
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        logger.debug(f"Dropping table if exists: {schema}.{table}")
        con.execute(f"DROP TABLE IF EXISTS {schema}.{table}")
        logger.debug(f"Creating table: {schema}.{table} from DataFrame")
        con.execute(f"CREATE TABLE {schema}.{table} AS SELECT * FROM df")
        logger.info(f"Wrote {len(df)} rows and {len(df.columns)} columns to {schema}.{table}")
    except Exception as e:
        logger.error(f"Failed to write {table} to central DB: {e}")
    finally:
        con.close()

CENTRAL_DB = DATABASE_DIR / 'huntington_data_lake.duckdb'

def main():
    logger.info("Starting embryoscope DB consolidation process...")
    db_paths = find_clinic_dbs()
    logger.info(f"Found {len(db_paths)} clinic DBs: {db_paths}")
    for table_key, table_info in TABLES.items():
        logger.info(f"Consolidating table: {table_info['table']}")
        logger.debug(f"Table info: {table_info}")
        df = consolidate_table(table_key, table_info, db_paths)
        write_table_to_central_db(df, table_info['table'], CENTRAL_SCHEMA, CENTRAL_DB)
    logger.info(f"Consolidation complete. Central DB: {CENTRAL_DB}")
    logger.info(f"Log file saved to: {log_filename}")

if __name__ == "__main__":
    main() 