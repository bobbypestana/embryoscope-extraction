import os
import duckdb
import pandas as pd
from glob import glob
from pathlib import Path

# Configuration
DATABASE_DIR = Path(__file__).resolve().parent.parent.parent / 'database'
CENTRAL_DB = DATABASE_DIR / 'huntington_data_lake.duckdb'
SCHEMA = 'embryoscope'
TABLES = {
    'patients': {
        'table': 'data_patients',
        'business_keys': ['PatientIDx', '_location'],
    },
    'treatments': {
        'table': 'data_treatments',
        'business_keys': ['PatientIDx', 'TreatmentName', '_location'],
    },
    'embryo_data': {
        'table': 'data_embryo_data',
        'business_keys': ['EmbryoID', '_location'],
    },
    'idascore': {
        'table': 'data_idascore',
        'business_keys': ['EmbryoID', '_location'],
    },
}

# Helper to get clinic name from DB filename
def get_clinic_name(db_path):
    name = Path(db_path).stem.replace('embryoscope_', '').replace('.db', '').replace('.duckdb', '')
    return name.replace('_', ' ').title()

# Find all per-clinic DBs (exclude test and central)
def find_clinic_dbs():
    dbs = []
    for db_path in glob(str(DATABASE_DIR / 'embryoscope_*.db')):
        if 'test' in db_path or 'huntington_data_lake' in db_path:
            continue
        dbs.append(db_path)
    return dbs

def read_table_from_db(db_path, table, clinic_name):
    con = duckdb.connect(db_path, read_only=True)
    try:
        df = con.execute(f"SELECT * FROM {SCHEMA}.{table}").df()
        df['unit_huntington'] = clinic_name
        return df
    except Exception as e:
        print(f"Warning: Could not read {table} from {db_path}: {e}")
        return pd.DataFrame()
    finally:
        con.close()

def consolidate_table(table_key, table_info, db_paths):
    all_dfs = []
    for db_path in db_paths:
        clinic_name = get_clinic_name(db_path)
        df = read_table_from_db(db_path, table_info['table'], clinic_name)
        if not df.empty:
            all_dfs.append(df)
    if not all_dfs:
        return pd.DataFrame()
    df_all = pd.concat(all_dfs, ignore_index=True)
    # Keep only the most recent record per business key
    sort_cols = table_info['business_keys'] + ['_extraction_timestamp']
    df_all = df_all.sort_values(sort_cols, ascending=[True]*len(table_info['business_keys']) + [False])
    df_dedup = df_all.drop_duplicates(subset=table_info['business_keys'], keep='first')
    return df_dedup

def write_table_to_central_db(df, table, schema, central_db):
    if df.empty:
        print(f"No data to write for {table}")
        return
    con = duckdb.connect(central_db)
    try:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        # Drop and recreate table for clean consolidation
        con.execute(f"DROP TABLE IF EXISTS {schema}.{table}")
        con.execute(f"CREATE TABLE {schema}.{table} AS SELECT * FROM df")
        print(f"Wrote {len(df)} rows to {schema}.{table}")
    finally:
        con.close()

def main():
    db_paths = find_clinic_dbs()
    print(f"Found {len(db_paths)} clinic DBs: {db_paths}")
    for table_key, table_info in TABLES.items():
        print(f"Consolidating table: {table_info['table']}")
        df = consolidate_table(table_key, table_info, db_paths)
        write_table_to_central_db(df, table_info['table'], SCHEMA, CENTRAL_DB)
    print(f"Consolidation complete. Central DB: {CENTRAL_DB}")

if __name__ == "__main__":
    main() 