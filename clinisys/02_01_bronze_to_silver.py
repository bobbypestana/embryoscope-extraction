import duckdb
import os
import logging
from datetime import datetime
import yaml

# Import our new modules
from transformations import (
    generate_complete_cast_sql,
    deduplicate_and_format_sql
)
from prontuario_cleaner import clean_prontuario_columns
from feature_engineering import feature_creation

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

db_path = os.path.join('..', 'database', 'clinisys_all.duckdb') if not os.path.exists('database/clinisys_all.duckdb') else 'database/clinisys_all.duckdb'


def main():
    logger.info('Starting bronze to silver transformation pipeline')
    logger.info(f'Database path: {db_path}')
    
    with duckdb.connect(db_path) as con:
        # Get list of bronze tables
        bronze_tables = con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'bronze'").df()['table_name'].tolist()
        logger.info(f'Found {len(bronze_tables)} bronze tables: {bronze_tables}')
        
        for table in bronze_tables:
            logger.info(f'Processing {table}')
            try:
                # Generate complete CAST SQL for all columns (no filtering)
                cast_sql = generate_complete_cast_sql(con, table)
                if cast_sql is None:
                    logger.error(f'Failed to generate CAST SQL for {table}')
                    continue
                
                # Generate SQL with deduplication
                sql = deduplicate_and_format_sql(table, cast_sql)
                logger.debug(f'Generated SQL for {table} (first 500 chars): {sql[:500]}...')
                
                # Execute transformation
                con.execute(sql)
                logger.info(f'Created/updated silver.{table}')
                
                # Apply table-specific feature engineering
                feature_creation(con, table)
                
                # Clean prontuario columns (convert to integer, discard invalid values)
                clean_prontuario_columns(con, table)
                
                # Verify column count
                bronze_cols = con.execute(f"SELECT COUNT(*) as col_count FROM information_schema.columns WHERE table_schema = 'bronze' AND table_name = '{table}'").df()['col_count'].iloc[0]
                silver_cols = con.execute(f"SELECT COUNT(*) as col_count FROM information_schema.columns WHERE table_schema = 'silver' AND table_name = '{table}'").df()['col_count'].iloc[0]
                logger.info(f'Column count: Bronze={bronze_cols}, Silver={silver_cols}')
                
            except Exception as e:
                logger.error(f'Error processing {table}: {e}', exc_info=True)
    
    # Create indexes for better JOIN performance in gold layer
    logger.info('Creating indexes for better JOIN performance...')
    with duckdb.connect(db_path) as con:
        index_queries = [
            "CREATE INDEX IF NOT EXISTS idx_oocitos_id_micromanipulacao ON silver.view_micromanipulacao_oocitos(id_micromanipulacao)",
            "CREATE INDEX IF NOT EXISTS idx_micromanipulacao_codigo_ficha ON silver.view_micromanipulacao(codigo_ficha)",
            "CREATE INDEX IF NOT EXISTS idx_congelamentos_ovulos_ciclo_prontuario ON silver.view_congelamentos_ovulos(Ciclo, prontuario)",
            "CREATE INDEX IF NOT EXISTS idx_descongelamentos_ovulos_data_prontuario ON silver.view_descongelamentos_ovulos(DataDescongelamento, prontuario)",
            "CREATE INDEX IF NOT EXISTS idx_congelamentos_embrioes_ciclo_prontuario ON silver.view_congelamentos_embrioes(Ciclo, prontuario)",
            "CREATE INDEX IF NOT EXISTS idx_descongelamentos_embrioes_ciclo_codcongelamento ON silver.view_descongelamentos_embrioes(Ciclo, CodCongelamento)",
            "CREATE INDEX IF NOT EXISTS idx_embrioes_congelados_id_oocito ON silver.view_embrioes_congelados(id_oocito)"
        ]
        
        for idx_query in index_queries:
            try:
                con.execute(idx_query)
            except Exception as e:
                logger.warning(f'Index creation failed (may already exist): {e}')
        
        logger.info('Indexes created successfully')
    
    logger.info('Bronze to silver transformation pipeline finished successfully')


if __name__ == '__main__':
    main()