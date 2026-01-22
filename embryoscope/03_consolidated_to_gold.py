import duckdb
import os
import logging
from datetime import datetime
import yaml

# Load config and logging level
script_dir = os.path.dirname(os.path.abspath(__file__))
params_path = os.path.join(script_dir, 'params.yml')
with open(params_path, 'r') as f:
    config = yaml.safe_load(f)
logging_level_str = config.get('logging_level', 'INFO').upper()
logging_level = getattr(logging, logging_level_str, logging.INFO)

# Load gold column config
gold_config_path = os.path.join(script_dir, 'gold_column_config.yml')
with open(gold_config_path, 'r') as f:
    gold_config = yaml.safe_load(f)

# Setup logging
log_dir = os.path.join(script_dir, 'logs')
os.makedirs(log_dir, exist_ok=True)
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
script_name = os.path.splitext(os.path.basename(__file__))[0]
log_path = os.path.join(log_dir, f'{script_name}_{timestamp}.log')
logging.basicConfig(
    level=logging_level,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.FileHandler(log_path),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Database paths
source_db_path = r'G:\My Drive\projetos_individuais\Huntington\database\huntington_data_lake.duckdb'
target_db_path = r'G:\My Drive\projetos_individuais\Huntington\database\huntington_data_lake.duckdb'

def build_query():
    """Build SELECT query based on configured columns"""
    logger.info('Building query based on config...')
    
    select_parts = []
    
    # 1. Patients - calculate YearOfBirth directly in query
    for col in gold_config.get('patients', []):
        if col == 'prontuario':
            select_parts.append(f'p.prontuario')
        elif col == 'YearOfBirth':
            # Calculate from DateOfBirth
            select_parts.append('EXTRACT(YEAR FROM CAST(p.DateOfBirth AS DATE)) as patient_YearOfBirth')
        else:
            select_parts.append(f'p.{col} as patient_{col}')
            
    # 2. Treatments
    for col in gold_config.get('treatments', []):
        select_parts.append(f't.{col} as treatment_{col}')
        
    # 3. Embryo Data - calculate EmbryoDate directly in query
    for col in gold_config.get('embryo_data', []):
        standard_cols = ['EmbryoID', 'PatientIDx', 'TreatmentName', 'InstrumentNumber', 'Position', 'WellNumber', 
                        'FertilizationTime', 'FertilizationMethod', 'EmbryoFate', 
                        'Description', 'EmbryoDescriptionID', 'KIDScore', 'KIDDate', 
                        'KIDVersion', 'KIDUser', 'embryo_number']
        
        if col == 'EmbryoDate':
            # Extract date from EmbryoID (format: D2022.12.05_...) and convert to proper DATE format
            select_parts.append("""
                CASE
                    WHEN regexp_matches(ed.EmbryoID, 'D20[0-9]{2}\\.[0-9]{2}\\.[0-9]{2}') THEN
                        TRY_CAST(
                            replace(regexp_extract(ed.EmbryoID, 'D(20[0-9]{2}\\.[0-9]{2}\\.[0-9]{2})', 1), '.', '-')
                            AS DATE
                        )
                    ELSE NULL
                END as embryo_EmbryoDate
            """)
        elif col in standard_cols:
            select_parts.append(f'ed.{col} as embryo_{col}')
        else:
            # Time annotations - include all 4 fields: Name, Time, Value, Timestamp
            for prefix in ['Name', 'Time', 'Value', 'Timestamp']:
                select_parts.append(f'ed."{prefix}_{col}" as embryo_{prefix}_{col}')


    # 4. IDAScore
    for col in gold_config.get('idascore', []):
        select_parts.append(f'ida.{col} as idascore_{col}')
        
    # 5. Calculated Features
    # Age At Fertilization: (FertilizationTime - DateOfBirth) / 365.25
    select_parts.append("""
        round(date_diff('day', CAST(p.DateOfBirth AS DATE), CAST(ed.FertilizationTime AS DATE)) / 365.25, 2) as AgeAtFertilization
    """)

    select_clause = ',\n        '.join(select_parts)
    
    query = f'''
    SELECT 
        {select_clause}
    FROM silver_embryoscope.patients p 
    LEFT JOIN silver_embryoscope.treatments t 
        ON p.PatientIDx = t.PatientIDx 
    LEFT JOIN silver_embryoscope.embryo_data ed 
        ON ed.PatientIDx = p.PatientIDx and ed.TreatmentName = t.TreatmentName
    LEFT JOIN silver_embryoscope.idascore ida
        ON ed.EmbryoID = ida.EmbryoID
    WHERE ed.EmbryoID IS NOT NULL
    ORDER BY p.PatientIDx DESC
    '''
    
    return query

def main():
    logger.info('Starting embryoscope gold loader')
    
    if not os.path.exists(source_db_path):
        logger.error(f'Source database not found: {source_db_path}')
        return
        
    try:
        query = build_query()
        
        logger.info('Connecting to database...')
        with duckdb.connect(source_db_path) as con:
            logger.info('Connected to DuckDB')
            
            # Execute query
            logger.info('Executing gold layer transformation...')
            df = con.execute(query).df()
            logger.info(f'Read {len(df)} rows')
            
            if df.empty:
                logger.warning('No data found')
                return

            # Write to gold schema
            logger.info('Writing to gold.embryoscope_embrioes...')
            con.execute('CREATE SCHEMA IF NOT EXISTS gold')
            con.execute('DROP TABLE IF EXISTS gold.embryoscope_embrioes')
            con.register('df_gold', df)
            con.execute('CREATE TABLE gold.embryoscope_embrioes AS SELECT * FROM df_gold')
            con.unregister('df_gold')
            
            # Create indexes
            logger.info('Creating indexes...')
            con.execute("CREATE INDEX IF NOT EXISTS idx_es_fert_time ON gold.embryoscope_embrioes(embryo_FertilizationTime)")
            con.execute("CREATE INDEX IF NOT EXISTS idx_es_patient_id ON gold.embryoscope_embrioes(patient_PatientID)")
            con.execute("CREATE INDEX IF NOT EXISTS idx_es_embryo_id ON gold.embryoscope_embrioes(embryo_EmbryoID)")
            
            count = con.execute("SELECT COUNT(*) FROM gold.embryoscope_embrioes").fetchone()[0]
            logger.info(f'Success! gold.embryoscope_embrioes created with {count} rows')
            
    except Exception as e:
        logger.error(f'Error in gold loader: {e}', exc_info=True)
        raise

if __name__ == '__main__':
    main()