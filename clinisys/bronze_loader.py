import os
import sys
import logging
import duckdb
import pandas as pd
from glob import glob
from datetime import datetime
import io
import yaml
import hashlib
from collections import defaultdict
import re

# Debug mode
DEBUG_MODE = True

# Load config
CONFIG_PATH = os.path.join(os.path.dirname(__file__), 'params.yml')
with open(CONFIG_PATH, 'r') as f:
    config = yaml.safe_load(f)

# Paths
DATA_OUTPUT_DIR = os.path.join(os.path.dirname(__file__), 'data_output')
DATA_HISTORY_DIR = os.path.join(os.path.dirname(__file__), 'data_history')
DUCKDB_PATH = config.get('duckdb_path', r'G:\My Drive\projetos_individuais\Huntington\database\clinisys_all.duckdb')
LOGS_DIR = os.path.join(os.path.dirname(__file__), 'logs')
os.makedirs(LOGS_DIR, exist_ok=True)
os.makedirs(os.path.dirname(DUCKDB_PATH), exist_ok=True)

# Timestamped log file
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
LOG_PATH = os.path.join(LOGS_DIR, f'bronze_loader_{timestamp}.log')

# Setup logging to both file and terminal
log_level = logging.DEBUG if DEBUG_MODE else logging.INFO
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
file_handler = logging.FileHandler(LOG_PATH)
file_handler.setLevel(log_level)
file_handler.setFormatter(formatter)
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setLevel(log_level)
stream_handler.setFormatter(formatter)
logger = logging.getLogger(__name__)
logger.setLevel(log_level)
logger.handlers = []
logger.addHandler(file_handler)
logger.addHandler(stream_handler)

# Meta columns
META_COLUMNS = ['hash', 'extraction_timestamp']

def compute_row_hash(row, columns):
    row_str = '||'.join(str(row[col]) for col in columns)
    return hashlib.sha256(row_str.encode('utf-8')).hexdigest()

def extract_from_csv(table_name, latest_file, extraction_timestamp):
    skipped_lines = []
    def on_bad_line(line):
        skipped_lines.append(line)
        return None
    df = pd.read_csv(
        latest_file,
        delimiter=';',
        quotechar='"',
        encoding='utf-8',
        on_bad_lines=on_bad_line,
        engine='python'
    )
    logger.debug(f'Read {len(df)} rows from {latest_file}')
    return df, skipped_lines

def extract_from_db(table_name, extraction_timestamp, db_conf):
    import sqlalchemy
    engine = sqlalchemy.create_engine(db_conf['connection_string'])
    query = db_conf['tables'][table_name]['query']
    df = pd.read_sql(query, engine)
    logger.debug(f'Read {len(df)} rows from DB table {table_name}')
    skipped_lines = []
    return df, skipped_lines

def main():
    logger.info('Starting bronze loader')
    logger.info(f'DuckDB path: {DUCKDB_PATH}')
    logger.debug('Bronze loader started in debug mode')
    try:
        source_type = config.get('source', 'csv')
        logger.info(f'Extraction source: {source_type}')
        if source_type == 'csv':
            # Find all snapshots in data_history
            history_files = glob(os.path.join(DATA_HISTORY_DIR, '*.csv'))
            logger.debug(f'Found {len(history_files)} CSV files in {DATA_HISTORY_DIR}')
            if not history_files:
                logger.warning(f'No CSV files found in {DATA_HISTORY_DIR}')
                return
            table_files = defaultdict(list)
            pattern = re.compile(r'^(.*)_([0-9]{8}_[0-9]{6})\.csv$')
            for f in history_files:
                base = os.path.basename(f)
                m = pattern.match(base)
                if m:
                    table_files[m.group(1)].append((f, m.group(2)))
            logger.debug(f'Identified tables: {list(table_files.keys())}')
        elif source_type == 'db':
            db_conf = config['db']
            table_files = {tbl: [(None, datetime.now().strftime('%Y%m%d_%H%M%S'))] for tbl in db_conf['tables']}
        else:
            logger.error(f'Unknown source type: {source_type}')
            return
        with duckdb.connect(DUCKDB_PATH) as con:
            con.execute('CREATE SCHEMA IF NOT EXISTS bronze;')
            logger.info(f'Connected to DuckDB at {DUCKDB_PATH} (file will be created if it does not exist)')
            for table_name, files in table_files.items():
                files.sort(key=lambda x: x[1], reverse=True)
                if len(files) == 0:
                    logger.warning(f'No files found for table {table_name}')
                    continue
                latest_file, extraction_timestamp = files[0]
                logger.info(f'Processing table {table_name} from {latest_file if latest_file else "DB"}')
                try:
                    if source_type == 'csv':
                        df, skipped_lines = extract_from_csv(table_name, latest_file, extraction_timestamp)
                    elif source_type == 'db':
                        df, skipped_lines = extract_from_db(table_name, extraction_timestamp, db_conf)
                    else:
                        continue
                    original_columns = [col for col in df.columns if col not in META_COLUMNS]
                    df['hash'] = df.apply(lambda row: compute_row_hash(row, original_columns), axis=1)
                    df['extraction_timestamp'] = extraction_timestamp
                    result = con.execute(f"SELECT COUNT(*) FROM information_schema.tables WHERE lower(table_name)=lower('{table_name}') AND table_schema='bronze'").fetchone()
                    table_exists = result is not None and result[0] > 0
                    if table_exists:
                        col_info = con.execute(f"PRAGMA table_info('bronze.{table_name}')").fetchall()
                        col_names = [c[1] for c in col_info]
                        alter_needed = False
                        for meta_col in META_COLUMNS:
                            if meta_col not in col_names:
                                alter_needed = True
                                con.execute(f'ALTER TABLE bronze."{table_name}" ADD COLUMN {meta_col} VARCHAR')
                        if alter_needed:
                            logger.info(f'Altered table bronze.{table_name} to add meta columns: {META_COLUMNS}')
                        col_info = con.execute(f"PRAGMA table_info('bronze.{table_name}')").fetchall()
                        col_names = [c[1] for c in col_info]
                        if 'hash' in col_names:
                            existing_hashes = [r[0] for r in con.execute(f'SELECT hash FROM bronze."{table_name}"').fetchall()]
                        else:
                            existing_hashes = []
                        new_df = df[~df['hash'].isin(existing_hashes)]
                        logger.info(f'Appending {len(new_df)} new rows to bronze.{table_name}')
                        if not new_df.empty:
                            insert_cols = [col for col in df.columns if col in col_names]
                            col_str = ', '.join(insert_cols)
                            con.execute(f'INSERT INTO bronze."{table_name}" ({col_str}) SELECT {col_str} FROM new_df')
                    else:
                        logger.info(f'Creating new table bronze.{table_name} with {len(df)} rows')
                        con.execute(f'CREATE TABLE bronze."{table_name}" AS SELECT * FROM df')
                    if skipped_lines:
                        logger.warning(f'Skipped {len(skipped_lines)} bad lines in {latest_file}')
                        badlines_filename = os.path.join(LOGS_DIR, f'badlines_{table_name}_{timestamp}.log')
                        with open(badlines_filename, 'w', encoding='utf-8') as f:
                            for bad_line in skipped_lines:
                                f.write(str(bad_line) + '\n')
                except Exception as e:
                    logger.error(f'Error loading {latest_file if latest_file else table_name}: {e}', exc_info=True)
                logger.debug(f'--- Finished processing {latest_file if latest_file else table_name} as table {table_name} ---')
            logger.debug('Closed DuckDB connection')
            logger.info('Bronze loader finished successfully')
    except Exception as e:
        logger.error(f'Failed to connect to DuckDB at {DUCKDB_PATH}: {e}', exc_info=True)
        sys.exit(1)

if __name__ == '__main__':
    main() 