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
LOG_PATH = os.path.join(LOGS_DIR, f'combined_gold_loader_{timestamp}.log')
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

# Database path
db_path = os.path.join('..', 'database', 'huntington_data_lake.duckdb')

def main():
    logger.info('Starting combined gold loader for embryoscope_clinisys_combined')
    logger.info(f'Database path: {db_path}')
    
    # Verify database exists
    if not os.path.exists(db_path):
        logger.error(f'Database not found: {db_path}')
        return
    
    query = '''
    SELECT 
        -- Clinisys columns
        c.oocito_id,
        c.oocito_id_micromanipulacao,
        c.oocito_Maturidade,
        c.oocito_TCD,
        c.micro_prontuario,
        c.micro_Data_DL,
        c.emb_cong_embriao,
        c.oocito_embryo_number,
        
        -- Embryoscope columns
        e.embryo_EmbryoID,
        e.embryo_FertilizationTime,
        e.treatment_TreatmentName,
        e.embryo_EmbryoFate,
        e.embryo_EmbryoDescriptionID,
        e.embryo_WellNumber,
        e.embryo_embryo_number,
        e.patient_PatientID,
        e.patient_PatientID_clean
        
    FROM gold.clinisys_embrioes c
    LEFT JOIN (
        SELECT *,
            CASE 
                WHEN patient_PatientID IS NULL OR patient_PatientID = 0 THEN NULL
                WHEN patient_PatientID = 0 THEN NULL
                ELSE patient_PatientID
            END as patient_PatientID_clean
        FROM gold.embryoscope_embrioes
    ) e
        ON CAST(c.micro_Data_DL AS DATE) = CAST(e.embryo_FertilizationTime AS DATE)
        AND c.oocito_embryo_number = e.embryo_embryo_number
        AND c.micro_prontuario = e.patient_PatientID_clean
        AND e.patient_PatientID_clean IS NOT NULL
    ORDER BY c.oocito_id
    '''
    
    try:
        # Read data from database
        logger.info('Connecting to database to read data')
        with duckdb.connect(db_path) as con:
            logger.info('Connected to DuckDB')
            # Execute query to get data
            df = con.execute(query).df()
            logger.info(f'Read {len(df)} rows from database')
            
            # Validate data
            if df.empty:
                logger.warning('No data found in database')
                return
                
            logger.info(f'Data columns: {list(df.columns)}')
            
        # Write data to target database
        logger.info('Connecting to database to write data')
        with duckdb.connect(db_path) as con:
            logger.info('Connected to DuckDB')
            
            # Create schema if not exists
            con.execute('CREATE SCHEMA IF NOT EXISTS gold;')
            logger.info('Ensured gold schema exists')
            
            # Drop existing table if it exists
            con.execute('DROP TABLE IF EXISTS gold.embryoscope_clinisys_combined;')
            logger.info('Dropped existing gold.embryoscope_clinisys_combined table if it existed')
            
            # Create table from DataFrame
            con.execute('CREATE TABLE gold.embryoscope_clinisys_combined AS SELECT * FROM df')
            logger.info('Created gold.embryoscope_clinisys_combined table')
            
            # Validate schema
            schema = con.execute("DESCRIBE gold.embryoscope_clinisys_combined").fetchdf()
            logger.info(f'gold.embryoscope_clinisys_combined schema:\n{schema}')
            
            # Validate row count
            row_count = con.execute("SELECT COUNT(*) FROM gold.embryoscope_clinisys_combined").fetchone()[0]
            logger.info(f'gold.embryoscope_clinisys_combined row count: {row_count}')
            
            # Check join statistics
            clinisys_count = con.execute("SELECT COUNT(*) FROM gold.clinisys_embrioes").fetchone()[0]
            embryoscope_count = con.execute("SELECT COUNT(*) FROM gold.embryoscope_embrioes").fetchone()[0]
            matched_count = con.execute("SELECT COUNT(*) FROM gold.embryoscope_clinisys_combined WHERE embryo_EmbryoID IS NOT NULL").fetchone()[0]
            
            logger.info(f'Join statistics:')
            logger.info(f'  - Clinisys records: {clinisys_count}')
            logger.info(f'  - Embryoscope records: {embryoscope_count}')
            logger.info(f'  - Combined records: {row_count}')
            logger.info(f'  - Matched records: {matched_count}')
            logger.info(f'  - Unmatched clinisys: {row_count - matched_count}')
            
    except Exception as e:
        logger.error(f'Error in combined gold loader: {e}', exc_info=True)
        raise
        
    logger.info('Combined gold loader finished successfully')

if __name__ == '__main__':
    main() 