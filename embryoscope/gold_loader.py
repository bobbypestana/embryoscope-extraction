import duckdb
import os
import logging
from datetime import datetime
import yaml

# Load config and logging level
CONFIG_PATH = os.path.join(os.path.dirname(__file__), 'params.yml')
with open(CONFIG_PATH, 'r') as f:
    config = yaml.safe_load(f)
logging_level_str = config.get('logging_level', 'INFO').upper()
logging_level = getattr(logging, logging_level_str, logging.INFO)

# Setup logging
LOGS_DIR = os.path.join(os.path.dirname(__file__), 'logs')
os.makedirs(LOGS_DIR, exist_ok=True)
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
LOG_PATH = os.path.join(LOGS_DIR, f'gold_loader_{timestamp}.log')
logging.basicConfig(
    level=logging_level,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.FileHandler(LOG_PATH),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
logger.info(f'Loaded logging level: {logging_level_str}')

# Database paths - read from huntington_data_lake, write to huntington_data_lake
source_db_path = os.path.join('..', 'database', 'huntington_data_lake.duckdb')
target_db_path = os.path.join('..', 'database', 'huntington_data_lake.duckdb')

def main():
    logger.info('Starting embryoscope gold loader for embryoscope_embrioes')
    logger.info(f'Source database path: {source_db_path}')
    logger.info(f'Target database path: {target_db_path}')
    
    # Verify source database exists
    if not os.path.exists(source_db_path):
        logger.error(f'Source database not found: {source_db_path}')
        return
    
    query = '''
    SELECT *
    FROM silver_embryoscope.patients p 
    LEFT JOIN silver_embryoscope.treatments t 
        ON p.PatientIDx = t.PatientIDx 
    LEFT JOIN silver_embryoscope.embryo_data ed 
        ON ed.PatientIDx = p.PatientIDx and ed.TreatmentName = t.TreatmentName
    ORDER BY p.PatientID DESC
    '''
    
    try:
        # Read data from source database
        logger.info('Connecting to source database to read data')
        with duckdb.connect(source_db_path) as source_con:
            logger.info('Connected to source DuckDB')
            # Execute query to get data
            df = source_con.execute(query).df()
            logger.info(f'Read {len(df)} rows from source database')
            
            # Validate data
            if df.empty:
                logger.warning('No data found in source database')
                return
                
            logger.info(f'Data columns: {list(df.columns)}')
            
            # Column filtering and prefixing
            logger.info('Applying column filtering and prefixing...')
            
            # Remove all hash and extraction_timestamp columns
            columns_to_remove = [col for col in df.columns if 'hash' in col.lower() or 'extraction_timestamp' in col.lower()]
            df = df.drop(columns=columns_to_remove)
            logger.info(f'Removed {len(columns_to_remove)} hash/extraction_timestamp columns: {columns_to_remove}')
            
            # Define prefixes for each table
            table_prefixes = {
                'p': 'patient_',
                't': 'treatment_',
                'ed': 'embryo_'
            }
            
            # Get original column names from each table to determine prefixes
            with duckdb.connect(source_db_path) as temp_con:
                p_columns = set(temp_con.execute("SELECT column_name FROM information_schema.columns WHERE table_schema = 'silver_embryoscope' AND table_name = 'patients'").df()['column_name'].tolist())
                t_columns = set(temp_con.execute("SELECT column_name FROM information_schema.columns WHERE table_schema = 'silver_embryoscope' AND table_name = 'treatments'").df()['column_name'].tolist())
                ed_columns = set(temp_con.execute("SELECT column_name FROM information_schema.columns WHERE table_schema = 'silver_embryoscope' AND table_name = 'embryo_data'").df()['column_name'].tolist())
            
            # Create mapping of original column names to prefixed names
            column_mapping = {}
            
            # Map columns from each table to their prefixed versions
            for col in df.columns:
                if col in p_columns:
                    column_mapping[col] = f"patient_{col}"
                elif col in t_columns:
                    column_mapping[col] = f"treatment_{col}"
                elif col in ed_columns:
                    column_mapping[col] = f"embryo_{col}"
                else:
                    # Keep original name for columns that don't match any table
                    column_mapping[col] = col
            
            # Rename columns
            df = df.rename(columns=column_mapping)
            logger.info(f'Applied prefixes to columns. New column count: {len(df.columns)}')
            logger.info(f'New columns: {list(df.columns)}')
            
        # Write data to target database
        logger.info('Connecting to target database to write data')
        with duckdb.connect(target_db_path) as target_con:
            logger.info('Connected to target DuckDB')
            
            # Create schema if not exists
            target_con.execute('CREATE SCHEMA IF NOT EXISTS gold;')
            logger.info('Ensured gold schema exists')
            
            # Drop existing table if it exists
            target_con.execute('DROP TABLE IF EXISTS gold.embryoscope_embrioes;')
            logger.info('Dropped existing gold.embryoscope_embrioes table if it existed')
            
            # Create table from DataFrame
            target_con.execute('CREATE TABLE gold.embryoscope_embrioes AS SELECT * FROM df')
            logger.info('Created gold.embryoscope_embrioes table')
            
            # Validate schema
            schema = target_con.execute("DESCRIBE gold.embryoscope_embrioes").fetchdf()
            logger.info(f'gold.embryoscope_embrioes schema:\n{schema}')
            
            # Validate row count
            row_count = target_con.execute("SELECT COUNT(*) FROM gold.embryoscope_embrioes").fetchone()[0]
            logger.info(f'gold.embryoscope_embrioes row count: {row_count}')
            
    except Exception as e:
        logger.error(f'Error in embryoscope gold loader: {e}', exc_info=True)
        raise
        
    logger.info('Embryoscope gold loader finished successfully')

if __name__ == '__main__':
    main() 