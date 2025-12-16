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
script_name = os.path.splitext(os.path.basename(__file__))[0]
LOG_PATH = os.path.join(LOGS_DIR, f'{script_name}_{timestamp}.log')
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

def test_join_strategies(con):
    """Test exact day matching strategy."""
    logger.info("Testing exact day matching strategy...")
    
    strategy = {
        'name': 'Exact DATE match with prontuario and embryo_number (embryos only)',
        'query': '''
            SELECT COUNT(*) as matches
            FROM gold.clinisys_embrioes c
            LEFT JOIN gold.embryoscope_embrioes e
                ON (c.micro_Data_DL = DATE(e.embryo_FertilizationTime) )
                    -- OR c.micro_Data_DL = TRY_CAST(strptime(substring(e.embryo_EmbryoID, 2, 10), '%Y.%m.%d') AS DATE))
                AND c.micro_prontuario = e.prontuario
                AND e.embryo_embryo_number = c.oocito_embryo_number
                AND e.prontuario IS NOT NULL
                AND c.oocito_flag_embryoscope = 1
            WHERE e.embryo_EmbryoID IS NOT NULL
        '''
    }
    
    try:
        result = con.execute(strategy['query']).fetchone()[0]
        logger.info(f"{strategy['name']}: {result} matches")
        return {strategy['name']: result}
    except Exception as e:
        logger.warning(f"Strategy '{strategy['name']}' failed: {e}")
        return {strategy['name']: 0}

def main():
    logger.info('Starting combined gold loader (fixed) for embryoscope_clinisys_combined')
    logger.info(f'Database path: {db_path}')
    
    # Verify database exists
    if not os.path.exists(db_path):
        logger.error(f'Database not found: {db_path}')
        return
    
    try:
        # Read data from database
        logger.info('Connecting to database to test join strategies')
        with duckdb.connect(db_path) as con:
            logger.info('Connected to DuckDB')
            
            # Indexes should be created in silver layer, not here
            logger.info('Using pre-created indexes from silver layer for better JOIN performance')
            
            # Set DuckDB configuration for better performance
            logger.info('Setting DuckDB configuration for better performance...')
            con.execute("SET memory_limit='8GB'")
            con.execute("SET threads=16")
            con.execute("SET enable_progress_bar=true")
            logger.info('DuckDB configuration set successfully')
            
            # Test exact day matching strategy
            join_results = test_join_strategies(con)
            
            # Use exact day matching with properly typed DATE columns for better performance
            # Cast embryo_FertilizationTime (TIMESTAMP) to DATE for comparison with micro_Data_DL (DATE)
            # Cast embryo_FertilizationTime (TIMESTAMP) to DATE for comparison with micro_Data_DL (DATE)
            # OR Extract date from embryo_EmbryoID (format DYYYY.MM.DD...)
            date_condition = "(c.micro_Data_DL = DATE(e.embryo_FertilizationTime) OR c.micro_Data_DL = TRY_CAST(strptime(substring(e.embryo_EmbryoID, 2, 10), '%Y.%m.%d') AS DATE))"
            logger.info(f"Using exact day matching strategy with OR condition (FertilizationTime OR EmbryoID)")
            
            # CURRENT LOGIC (COMMENTED OUT) - Selective columns only
            # query = f'''
            # SELECT 
            #     -- Clinisys columns
            #     c.oocito_id,
            #     c.oocito_id_micromanipulacao,
            #     c.oocito_Maturidade,
            #     c.oocito_TCD,
            #     c.micro_prontuario,
            #     c.micro_Data_DL,
            #     c.emb_cong_embriao,
            #     c.oocito_embryo_number,
            #     c.oocito_flag_embryoscope,
            #     
            #     -- Embryoscope columns
            #     e.embryo_EmbryoID,
            #     e.embryo_FertilizationTime,
            #     e.treatment_TreatmentName,
            #     e.embryo_EmbryoFate,
            #     e.embryo_EmbryoDescriptionID,
            #     e.embryo_WellNumber,
            #     e.embryo_embryo_number,
            #     e.patient_PatientID,
            #     e.patient_PatientID_clean
            #     
            # FROM gold.clinisys_embrioes c
            
            # DYNAMIC LOGIC - Select only columns that actually exist
            # Get all available columns from the gold table
            available_columns = con.execute("DESCRIBE gold.clinisys_embrioes").df()['column_name'].tolist()
            logger.info(f'Available columns in gold.clinisys_embrioes: {len(available_columns)}')
            
            # Build the SELECT clause dynamically
            select_columns = []
            for col in available_columns:
                select_columns.append(f"c.{col}")
            
            # Add all embryoscope columns
            select_columns.append("e.*")
            
            query = f'''
            SELECT 
                {', '.join(select_columns)}
            FROM gold.clinisys_embrioes c
            FULL OUTER JOIN (
                SELECT *
                FROM gold.embryoscope_embrioes
                WHERE prontuario IS NOT NULL
            ) e
                ON {date_condition}
                AND c.micro_prontuario = e.prontuario
                AND e.embryo_embryo_number = c.oocito_embryo_number
            ORDER BY COALESCE(CAST(c.oocito_id AS VARCHAR), e.embryo_EmbryoID)
            '''
            
            logger.info(f"Using FULL OUTER JOIN with exact day matching: {date_condition} + prontuario + embryo_number")
            
            # Execute query to get data
            df = con.execute(query).df()
            logger.info(f'Read {len(df)} rows from database')
            
            # Validate data
            if df.empty:
                logger.warning('No data found in database')
                return
                
            logger.info(f'Data columns: {list(df.columns)}')
            
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
            clinisys_embryos_count = con.execute("SELECT COUNT(*) FROM gold.clinisys_embrioes WHERE oocito_flag_embryoscope = 1").fetchone()[0]
            embryoscope_count = con.execute("SELECT COUNT(*) FROM gold.embryoscope_embrioes").fetchone()[0]
            matched_count = con.execute("SELECT COUNT(*) FROM gold.embryoscope_clinisys_combined WHERE embryo_EmbryoID IS NOT NULL AND oocito_id IS NOT NULL").fetchone()[0]
            clinisys_only_count = con.execute("SELECT COUNT(*) FROM gold.embryoscope_clinisys_combined WHERE embryo_EmbryoID IS NULL AND oocito_id IS NOT NULL").fetchone()[0]
            embryoscope_only_count = con.execute("SELECT COUNT(*) FROM gold.embryoscope_clinisys_combined WHERE embryo_EmbryoID IS NOT NULL AND oocito_id IS NULL").fetchone()[0]
            
            logger.info(f'FULL OUTER JOIN statistics:')
            logger.info(f'  - Total clinisys records: {clinisys_count}')
            logger.info(f'  - Clinisys embryos (flag_embryoscope=1): {clinisys_embryos_count}')
            logger.info(f'  - Embryoscope records: {embryoscope_count}')
            logger.info(f'  - Combined records: {row_count}')
            logger.info(f'  - Matched records (both sides): {matched_count}')
            logger.info(f'  - Clinisys only records: {clinisys_only_count}')
            logger.info(f'  - Embryoscope only records: {embryoscope_only_count}')
            logger.info(f'  - Match rate (vs total clinisys): {(matched_count/clinisys_count*100):.2f}%')
            logger.info(f'  - Match rate (vs clinisys embryos): {(matched_count/clinisys_embryos_count*100):.2f}%')
            logger.info(f'  - Match rate (vs embryoscope): {(matched_count/embryoscope_count*100):.2f}%')
            
    except Exception as e:
        logger.error(f'Error in combined gold loader: {e}', exc_info=True)
        raise
        
    logger.info('Combined gold loader (fixed) finished successfully')

if __name__ == '__main__':
    main() 