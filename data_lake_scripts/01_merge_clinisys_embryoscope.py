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
        'name': 'Exact DATE match with clean PatientID (embryos only)',
        'query': '''
            SELECT COUNT(*) as matches
            FROM gold.clinisys_embrioes c
            LEFT JOIN (
                SELECT *,
                    CASE 
                        WHEN patient_PatientID IS NULL THEN NULL
                        ELSE patient_PatientID
                    END as patient_PatientID_clean
                FROM gold.embryoscope_embrioes
            ) e
                                     ON CAST(c.micro_Data_DL AS DATE) = CAST(e.embryo_FertilizationTime AS DATE)
                 AND c.oocito_embryo_number = e.embryo_embryo_number
                 AND c.micro_prontuario = e.patient_PatientID_clean
                 AND e.patient_PatientID_clean IS NOT NULL
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
            
            # Test exact day matching strategy
            join_results = test_join_strategies(con)
            
            # Use exact day matching
            date_condition = "CAST(c.micro_Data_DL AS DATE) = CAST(e.embryo_FertilizationTime AS DATE)"
            logger.info(f"Using exact day matching strategy")
            
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
            
            # NEW LOGIC - Optimized column selection
            query = f'''
            SELECT 
                -- Selected Clinisys columns:
                -- 1. All oocito_ columns
                c.oocito_id,
                c.oocito_id_micromanipulacao,
                c.oocito_diaseguinte,
                c.oocito_Maturidade,
                c.oocito_RC,
                c.oocito_ComentariosAntes,
                c.oocito_Embriologista,
                c.oocito_PI,
                c.oocito_TCD,
                c.oocito_AH,
                c.oocito_PGD,
                c.oocito_ResultadoPGD,
                c.oocito_IdentificacaoPGD,
                c.oocito_Fertilizacao,
                c.oocito_CorpusculoPolar,
                c.oocito_ComentariosDepois,
                c.oocito_GD1,
                c.oocito_OocitoDoado,
                c.oocito_ICSI,
                c.oocito_OrigemOocito,
                c.oocito_InseminacaoOocito,
                c.oocito_NClivou_D2,
                c.oocito_NCelulas_D2,
                c.oocito_Frag_D2,
                c.oocito_Blastomero_D2,
                c.oocito_NClivou_D3,
                c.oocito_NCelulas_D3,
                c.oocito_Frag_D3,
                c.oocito_Blastomero_D3,
                c.oocito_GD2,
                c.oocito_NClivou_D4,
                c.oocito_NCelulas_D4,
                c.oocito_Compactando_D4,
                c.oocito_MassaInterna_D4,
                c.oocito_Trofoblasto_D4,
                c.oocito_NClivou_D5,
                c.oocito_NCelulas_D5,
                c.oocito_Compactando_D5,
                c.oocito_MassaInterna_D5,
                c.oocito_Trofoblasto_D5,
                c.oocito_NClivou_D6,
                c.oocito_NCelulas_D6,
                c.oocito_Compactando_D6,
                c.oocito_MassaInterna_D6,
                c.oocito_Trofoblasto_D6,
                c.oocito_NClivou_D7,
                c.oocito_NCelulas_D7,
                c.oocito_Compactando_D7,
                c.oocito_MassaInterna_D7,
                c.oocito_Trofoblasto_D7,
                c.oocito_score_maia,
                c.oocito_relatorio_ia,
                c.oocito_flag_embryoscope,
                c.oocito_embryo_number,
                
                -- 2. Specific micro columns
                c.micro_prontuario,
                c.micro_Data_DL,
                c.micro_numero_caso,
                
                -- ALL Embryoscope columns (prefixed with 'e.')
                e.*
                
            FROM gold.clinisys_embrioes c
            FULL OUTER JOIN (
                SELECT *,
                    CASE 
                        WHEN patient_PatientID IS NULL THEN NULL
                        ELSE patient_PatientID
                    END as patient_PatientID_clean
                FROM gold.embryoscope_embrioes
            ) e
                ON {date_condition}
                AND c.oocito_embryo_number = e.embryo_embryo_number
                AND c.micro_prontuario = e.patient_PatientID_clean
                AND e.patient_PatientID_clean IS NOT NULL
                AND c.oocito_flag_embryoscope = 1
            ORDER BY COALESCE(CAST(c.oocito_id AS VARCHAR), e.embryo_EmbryoID)
            '''
            
            logger.info(f"Using FULL OUTER JOIN with exact day matching: {date_condition} + PatientID_clean")
            
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