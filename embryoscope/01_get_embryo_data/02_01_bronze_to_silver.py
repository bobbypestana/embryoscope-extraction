"""
02_01_bronze_to_silver.py
=========================
Converts EmbryoScope bronze (raw JSON) tables into structured silver tables
for each per-clinic DuckDB database.

Patient-prontuario linking is performed by
commons.prontuario_matching_v1.find_prontuarios (Strategy L):
  Tier 0 -- Direct ID match against clinisys codigo
  Tier 1 -- CPF exact match
  Tier 2 -- ID + birthdate match
  Tier 3 -- Spousal / partner link columns
"""
import os
import sys
import glob
import json
import logging
import duckdb
import pandas as pd
from datetime import datetime
import yaml

# -- Path setup ------------------------------------------------------------------
script_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(script_dir)           # embryoscope/
root_dir   = os.path.dirname(parent_dir)            # Huntington/
sys.path.insert(0, root_dir)

from transformations import flatten_patients_json, flatten_embryo_json
from patient_id_cleaner import clean_patient_id
from commons.prontuario_matching_v1 import find_prontuarios
import feature_engineering

# -- Logging setup ---------------------------------------------------------------
script_name = os.path.splitext(os.path.basename(__file__))[0]
log_dir  = os.path.join(parent_dir, 'logs')
os.makedirs(log_dir, exist_ok=True)
log_ts   = datetime.now().strftime('%Y%m%d_%H%M%S')
log_file = os.path.join(log_dir, f'{script_name}_{log_ts}.log')

params_path = os.path.join(parent_dir, 'params.yml')
with open(params_path, 'r') as f:
    params = yaml.safe_load(f)
log_level_str = params.get('extraction', {}).get('log_level', 'INFO').upper()
log_level = getattr(logging, log_level_str, logging.INFO)

logger = logging.getLogger(script_name)
logger.setLevel(log_level)
_fmt = logging.Formatter(
    '%(asctime)s,%(msecs)03d - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
if not logger.hasHandlers():
    for _h in [logging.FileHandler(log_file), logging.StreamHandler()]:
        _h.setFormatter(_fmt)
        logger.addHandler(_h)

logger.info(f"Logging to: {log_file}")


# -- Column quality helpers ------------------------------------------------------

def calculate_column_filling_rates(df):
    """Return {column: filling_rate%} for each column in *df*."""
    if df.empty:
        logger.warning("DataFrame is empty -- all columns will have 0% filling rate")
        return {col: 0.0 for col in df.columns}
    total = len(df)
    rates = {}
    for col in df.columns:
        try:
            if df[col].dtype == 'object':
                mask = (
                    df[col].notna()
                    & (df[col].astype(str).str.strip() != '')
                    & (~df[col].astype(str).str.strip().isin(['NULL', 'null', 'None', 'none']))
                )
                rates[col] = mask.sum() / total * 100
            else:
                rates[col] = df[col].notna().sum() / total * 100
        except Exception as e:
            logger.warning(f"Error calculating filling rate for column {col}: {e}")
            rates[col] = 0.0
    return rates


def filter_columns_by_null_rate(df, table_name, db_name, null_rate_threshold=90.0):
    """Drop columns whose null rate exceeds *null_rate_threshold* (default 90%)."""
    if df.empty:
        logger.warning(f"[{db_name}] DataFrame for {table_name} is empty, no columns to filter")
        return df, {}
    filling_rates = calculate_column_filling_rates(df)
    included, excluded = [], {}
    for col in df.columns:
        null_rate = 100.0 - filling_rates.get(col, 0.0)
        if null_rate > null_rate_threshold:
            excluded[col] = null_rate
        else:
            included.append(col)
    if excluded:
        logger.info(
            f"[{db_name}] [{table_name}] Excluded {len(excluded)} columns "
            f"with null rate > {null_rate_threshold}%:"
        )
        for col, nr in sorted(excluded.items(), key=lambda x: x[1], reverse=True):
            logger.info(f"[{db_name}]   - {col}: null_rate={nr:.2f}% (filling_rate={100-nr:.2f}%)")
    else:
        logger.info(f"[{db_name}] [{table_name}] All columns pass the null rate threshold")
    logger.info(f"[{db_name}] [{table_name}] Keeping {len(included)} of {len(df.columns)} columns")
    if not included:
        logger.warning(f"[{db_name}] No columns meet the null rate threshold for {table_name}")
        return pd.DataFrame(), excluded
    return df[included].copy(), excluded


# -- Prontuario matching ---------------------------------------------------------

_TIER_LABELS = {
    0: 'Tier 0 (Direct ID)',
    1: 'Tier 1 (CPF)',
    2: 'Tier 2 (ID + Birthdate)',
    3: 'Tier 3 (Spousal link)',
}


def _run_prontuario_matching(con, db_name):
    """
    Call find_prontuarios against silver.patients and log a tier-breakdown summary.
    Updates the prontuario column in-place (suffix='').
    """
    clinisys_db_path = os.path.join(root_dir, 'database', 'clinisys_all.duckdb')
    logger.info(f"[{db_name}] Running prontuario matching (find_prontuarios / Strategy L) ...")

    df_matches = find_prontuarios(
        source_con=con,
        clinisys_db_path=clinisys_db_path,
        source_schema='silver',
        source_table='patients',
        id_col='PatientID',
        name_col='FirstName',
        birthdate_col='DateOfBirth',
        cpf_col=None,
        label=db_name,
        suffix='',
    )

    total   = len(df_matches)
    matched = int((df_matches['prontuario'] != -1).sum())
    rate    = matched / total * 100 if total else 0.0

    logger.info(f"[{db_name}] === PRONTUARIO MATCHING SUMMARY ===")
    logger.info(f"[{db_name}]   Total    : {total:,}")
    logger.info(f"[{db_name}]   Matched  : {matched:,}  ({rate:.2f}%)")
    logger.info(f"[{db_name}]   Unmatched: {total - matched:,}")
    if total > 0:
        tier_counts = (
            df_matches[df_matches['prontuario'] != -1]
            .groupby('match_tier')['source_id']
            .count()
            .sort_index()
        )
        for tier, cnt in tier_counts.items():
            logger.info(f"[{db_name}]     {_TIER_LABELS.get(tier, f'Tier {tier}')}: {cnt:,}")
    if rate >= 95:
        logger.info(f"[{db_name}]   Quality: EXCELLENT (>=95%)")
    elif rate >= 85:
        logger.info(f"[{db_name}]   Quality: GOOD (>=85%)")
    elif rate >= 70:
        logger.info(f"[{db_name}]   Quality: ACCEPTABLE (>=70%)")
    else:
        logger.warning(f"[{db_name}]   Quality: NEEDS ATTENTION (<70%)")
    logger.info(f"[{db_name}] === END PRONTUARIO MATCHING SUMMARY ===")


# -- Table processors ------------------------------------------------------------

def process_database(db_path):
    """Bronze -> silver: raw_patients -> silver.patients (with prontuario matching)."""
    db_name = os.path.basename(db_path)
    logger.info(f"[{db_name}] Processing patients table.")
    try:
        con = duckdb.connect(db_path)
        con.execute('CREATE SCHEMA IF NOT EXISTS silver')

        read_query = (
            'SELECT raw_json, _extraction_timestamp, _location, _run_id, _row_hash '
            'FROM bronze.raw_patients'
        )
        logger.info(f"[{db_name}] Reading patients with query: {read_query}")
        logger.debug(f"[{db_name}] Reading patients with query: {read_query}")
        df = con.execute(read_query).fetchdf()
        logger.info(f"[{db_name}] Read {len(df)} rows from bronze.raw_patients.")

        meta_cols = ['_extraction_timestamp', '_location', '_run_id', '_row_hash']
        all_patients = []
        for _, row in df.iterrows():
            patients = flatten_patients_json(row['raw_json'])
            for p in patients:
                for col in meta_cols:
                    p[col] = row[col]
            all_patients.extend(patients)
        logger.info(f"[{db_name}] Flattened to {len(all_patients)} patient records.")

        if not all_patients:
            logger.warning(f"[{db_name}] No patients found.")
            con.close()
            return

        patients_df = pd.DataFrame(all_patients)
        if 'DateOfBirth' in patients_df.columns:
            patients_df['DateOfBirth'] = pd.to_datetime(patients_df['DateOfBirth'], errors='coerce')

        patients_df = clean_patient_id(patients_df, 'patients', db_name)
        patients_df, _ = filter_columns_by_null_rate(
            patients_df, 'patients', db_name, null_rate_threshold=90.0
        )

        if patients_df.empty:
            logger.warning(f"[{db_name}] No patient records remain after cleaning")
            con.close()
            return

        patients_df['prontuario'] = -1
        for col in meta_cols:
            if col not in patients_df.columns:
                patients_df[col] = None

        logger.info(f"[{db_name}] Saving patients to table: silver.patients.")
        logger.debug(f"[{db_name}] Saving patients to table: silver.patients in database: {db_path}")
        con.execute('DROP TABLE IF EXISTS silver.patients')
        con.register('patients_df', patients_df)
        con.execute('CREATE TABLE silver.patients AS SELECT * FROM patients_df')
        con.unregister('patients_df')
        logger.info(f"[{db_name}] silver.patients creation complete.")

        _run_prontuario_matching(con, db_name)

        con.close()
    except Exception as e:
        logger.error(f"[{db_name}] Failed to process patients: {e}")


def process_treatments_database(db_path):
    """Bronze -> silver: raw_treatments -> silver.treatments."""
    db_name = os.path.basename(db_path)
    logger.info(f"[{db_name}] Processing treatments table.")
    try:
        con = duckdb.connect(db_path)
        con.execute('CREATE SCHEMA IF NOT EXISTS silver')

        read_query = (
            'SELECT raw_json, _extraction_timestamp, _location, _run_id, _row_hash '
            'FROM bronze.raw_treatments'
        )
        logger.info(f"[{db_name}] Reading treatments with query: {read_query}")
        logger.debug(f"[{db_name}] Reading treatments with query: {read_query}")
        df = con.execute(read_query).fetchdf()
        logger.info(f"[{db_name}] Read {len(df)} rows from bronze.raw_treatments.")

        meta_cols = ['_extraction_timestamp', '_location', '_run_id', '_row_hash']
        all_treatments = []
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
            con.close()
            return

        treatments_df = pd.DataFrame(all_treatments)
        for col in treatments_df.columns:
            treatments_df[col] = treatments_df[col].astype(str)

        treatments_df = clean_patient_id(treatments_df, 'treatments', db_name)
        treatments_df, _ = filter_columns_by_null_rate(
            treatments_df, 'treatments', db_name, null_rate_threshold=90.0
        )

        if treatments_df.empty:
            logger.warning(f"[{db_name}] No treatment records remain after cleaning")
            con.close()
            return

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
    """Bronze -> silver: raw_idascore -> silver.idascore."""
    db_name = os.path.basename(db_path)
    logger.info(f"[{db_name}] Processing idascore table.")

    _FIELD_MAP = {
        'Viability': 'IDAScore',
        'Time':      'IDATime',
        'Version':   'IDAVersion',
        'Timestamp': 'IDATimestamp',
    }

    try:
        con = duckdb.connect(db_path)
        con.execute('CREATE SCHEMA IF NOT EXISTS silver')

        read_query = (
            'SELECT raw_json, _extraction_timestamp, _location, _run_id, _row_hash '
            'FROM bronze.raw_idascore'
        )
        logger.info(f"[{db_name}] Reading idascore with query: {read_query}")
        logger.debug(f"[{db_name}] Reading idascore with query: {read_query}")
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
                mapped = {_FIELD_MAP.get(k, k): v for k, v in record.items()}
                mapped['_extraction_timestamp'] = row['_extraction_timestamp']
                mapped['_location']             = row['_location']
                mapped['_run_id']               = row['_run_id']
                mapped['_row_hash']             = row['_row_hash']
                all_idascore.append(mapped)
            except Exception as e:
                logger.error(f"[{db_name}] Error parsing idascore JSON at row {idx}: {e}")
        logger.info(f"[{db_name}] Parsed {len(all_idascore)} idascore records.")

        if not all_idascore:
            logger.warning(f"[{db_name}] No idascore records found.")
            logger.info(f"[{db_name}] Creating empty silver.idascore table.")
            con.execute('DROP TABLE IF EXISTS silver.idascore')
            con.execute(
                'CREATE TABLE silver.idascore ('
                'EmbryoID TEXT, IDAScore TEXT, IDATime TEXT, IDAVersion TEXT, '
                'IDATimestamp TEXT, _extraction_timestamp TIMESTAMP, '
                '_location TEXT, _run_id TEXT, _row_hash TEXT)'
            )
            con.close()
            logger.info(f"[{db_name}] Empty silver.idascore table created.")
            return

        idascore_df = pd.DataFrame(all_idascore)
        for col in idascore_df.columns:
            idascore_df[col] = idascore_df[col].astype(str)
        idascore_df, _ = filter_columns_by_null_rate(
            idascore_df, 'idascore', db_name, null_rate_threshold=90.0
        )

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
    """Bronze -> silver: raw_embryo_data -> silver.embryo_data."""
    db_name = os.path.basename(db_path)
    logger.info(f"[{db_name}] Processing embryo_data table.")

    _EVAL_RENAMES = {
        'Evaluation_Evaluation':   'KIDScore',
        'Evaluation_EvaluationDate': 'KIDDate',
        'Evaluation_Model':        'KIDVersion',
        'Evaluation_User':         'KIDUser',
    }

    try:
        con = duckdb.connect(db_path)
        con.execute('CREATE SCHEMA IF NOT EXISTS silver')

        read_query = (
            'SELECT raw_json, _extraction_timestamp, _location, _run_id, _row_hash '
            'FROM bronze.raw_embryo_data'
        )
        logger.info(f"[{db_name}] Reading embryo_data with query: {read_query}")
        logger.debug(f"[{db_name}] Reading embryo_data with query: {read_query}")
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
        logger.info(f"[{db_name}] Found annotation types: {sorted(annotation_names)}")
        logger.debug(f"[{db_name}] Found annotation types: {sorted(annotation_names)}")

        # Second pass: flatten all rows
        meta_cols = ['_extraction_timestamp', '_location', '_run_id', '_row_hash']
        all_embryos = []
        for _, row in df.iterrows():
            flat = flatten_embryo_json(row['raw_json'], annotation_names_set=annotation_names)
            for col in meta_cols:
                flat[col] = row[col]
            all_embryos.append(flat)
        logger.info(f"[{db_name}] Flattened {len(all_embryos)} embryo records.")

        if not all_embryos:
            logger.warning(f"[{db_name}] No embryo_data records found.")
            empty_cols = [
                'EmbryoID', 'PatientIDx', 'TreatmentName',
                'EmbryoDetails_InstrumentNumber', 'EmbryoDetails_Position',
                'EmbryoDetails_WellNumber', 'EmbryoDetails_FertilizationTime',
                'EmbryoDetails_FertilizationMethod', 'EmbryoDetails_EmbryoFate',
                'EmbryoDetails_Description', 'EmbryoDetails_EmbryoDescriptionID',
                'Evaluation_Model', 'Evaluation_User', 'Evaluation_EvaluationDate',
                '_extraction_timestamp', '_location', '_run_id', '_row_hash',
            ]
            for ann_name in sorted(annotation_names):
                empty_cols.extend([
                    f'Name_{ann_name}', f'Time_{ann_name}',
                    f'Value_{ann_name}', f'Timestamp_{ann_name}',
                ])
            col_defs = ', '.join(f'{c} TEXT' for c in empty_cols)
            con.execute('DROP TABLE IF EXISTS silver.embryo_data')
            con.execute(f'CREATE TABLE silver.embryo_data ({col_defs})')
            con.close()
            logger.info(f"[{db_name}] Empty silver.embryo_data table created.")
            return

        embryo_df = pd.DataFrame(all_embryos)

        # Cast datetime / numeric columns
        for col in embryo_df.columns:
            if col.startswith('Time_'):
                try:
                    embryo_df[col] = pd.to_numeric(embryo_df[col], errors='coerce')
                except Exception as e:
                    logger.error(f"[{db_name}] Error casting {col} to numeric: {e}")
            elif col.endswith(('Date', 'Time', 'Timestamp')):
                try:
                    embryo_df[col] = pd.to_datetime(embryo_df[col], errors='coerce')
                    if col == 'FertilizationTime':
                        logger.info(f"[{db_name}] Cast {col} to datetime for JOIN performance")
                except Exception as e:
                    logger.error(f"[{db_name}] Error casting {col} to datetime: {e}")

        # Rename Evaluation_* columns
        for old, new in _EVAL_RENAMES.items():
            if old in embryo_df.columns:
                embryo_df[new] = embryo_df[old]
                embryo_df = embryo_df.drop(columns=[old])

        # Strip EmbryoDetails_ prefix
        embryo_df = embryo_df.rename(
            columns={c: c.replace('EmbryoDetails_', '')
                     for c in embryo_df.columns if c.startswith('EmbryoDetails_')}
        )

        embryo_df = clean_patient_id(embryo_df, 'embryo_data', db_name)
        embryo_df, _ = filter_columns_by_null_rate(
            embryo_df, 'embryo_data', db_name, null_rate_threshold=90.0
        )

        if embryo_df.empty:
            logger.warning(f"[{db_name}] No embryo records remain after cleaning")
            con.close()
            return

        # Order columns: EmbryoID, PatientIDx, TreatmentName -> KID* -> IDA* -> rest
        cols = list(embryo_df.columns)
        main_cols = [c for c in ['EmbryoID', 'PatientIDx', 'TreatmentName'] if c in cols]
        kid_cols  = sorted(c for c in cols if c.startswith('KID'))
        ida_cols  = sorted(c for c in cols if c.startswith('IDA'))
        rest_cols = sorted(c for c in cols if c not in main_cols + kid_cols + ida_cols)
        embryo_df = embryo_df.reindex(columns=main_cols + kid_cols + ida_cols + rest_cols)

        for col in meta_cols:
            if col not in embryo_df.columns:
                embryo_df[col] = None

        logger.info(f"[{db_name}] Saving embryo_data to table: silver.embryo_data.")
        con.execute('DROP TABLE IF EXISTS silver.embryo_data')
        con.register('embryo_df', embryo_df)
        con.execute('CREATE TABLE silver.embryo_data AS SELECT * FROM embryo_df')
        con.unregister('embryo_df')

        # NOTE: embryo_number is calculated after deduplication in
        #       02_03_consolidate_embryoscope_dbs.py, preventing gaps in numbering.
        con.close()
        logger.info(f"[{db_name}] silver.embryo_data creation complete.")
    except Exception as e:
        logger.error(f"[{db_name}] Failed to process embryo_data: {e}")


# -- Entry point -----------------------------------------------------------------

def main():
    db_dir = os.path.join(root_dir, 'database')
    logger.info("Starting embryoscope bronze->silver conversion.")
    logger.info(f"Looking for databases in: {db_dir}")

    db_paths = glob.glob(os.path.join(db_dir, 'embryoscope_*.db'))
    logger.info(f"Found {len(db_paths)} databases to process.")
    if not db_paths:
        logger.warning("No embryoscope_*.db databases found in database/ directory.")
        return

    for db_path in db_paths:
        logger.info("=" * 60)
        logger.info(f"Processing database: {os.path.basename(db_path)}")
        process_database(db_path)
        process_treatments_database(db_path)
        process_idascore_database(db_path)
        process_embryo_data_database(db_path)
        logger.info(f"Finished: {os.path.basename(db_path)}")
        logger.info("=" * 60)

    # Create indexes for JOIN performance in the gold layer
    logger.info('Creating indexes for JOIN performance ...')
    for db_path in db_paths:
        db_name = os.path.basename(db_path)
        try:
            con = duckdb.connect(db_path)
            for idx_sql in [
                "CREATE INDEX IF NOT EXISTS idx_embryo_fertilization_time ON silver.embryo_data(FertilizationTime)",
                "CREATE INDEX IF NOT EXISTS idx_embryo_patient_id ON silver.embryo_data(PatientIDx)",
                "CREATE INDEX IF NOT EXISTS idx_patients_patient_id ON silver.patients(PatientID)",
            ]:
                try:
                    con.execute(idx_sql)
                except Exception as e:
                    logger.warning(f'[{db_name}] Index creation skipped (may already exist): {e}')
            con.close()
            logger.info(f'[{db_name}] Indexes created successfully')
        except Exception as e:
            logger.error(f'[{db_name}] Error creating indexes: {e}')

    logger.info('All embryoscope databases processed successfully.')


if __name__ == '__main__':
    main()