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

# Load gold column configuration
GOLD_COLUMN_CONFIG_PATH = os.path.join(os.path.dirname(__file__), 'gold_column_config.yml')
with open(GOLD_COLUMN_CONFIG_PATH, 'r') as f:
    gold_column_config = yaml.safe_load(f)

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

# Database paths - read from clinisys_all, write to huntington_data_lake
source_db_path = os.path.join('..', 'database', 'clinisys_all.duckdb')
target_db_path = os.path.join('..', 'database', 'huntington_data_lake.duckdb')


def build_select_columns():
    """
    Build SELECT clause with only specified columns from gold_column_config.yml
    Handles view_tratamentos being joined twice with different aliases (trat1 and trat2)
    
    Returns:
        List of select column strings
    """
    # Table name to alias mapping (alias is used for both SQL alias and column prefix)
    table_aliases = {
        'view_micromanipulacao_oocitos': 'oocito',
        'view_micromanipulacao': 'micro',
        'view_congelamentos_embrioes': 'cong_em',
        'view_descongelamentos_embrioes': 'descong_em',
        'view_embrioes_congelados': 'emb_cong',
        'view_tratamentos': 'trat1',
        'view_tratamentos_transfer': 'trat2'
    }
    
    select_columns = []
    
    for table_name, columns in gold_column_config.items():
        if table_name not in table_aliases:
            logger.warning(f"Table {table_name} not found in table aliases, skipping")
            continue
            
        alias = table_aliases[table_name]
        
        logger.info(f"Adding {len(columns)} columns from {table_name} (alias: {alias})")
        
        for column in columns:
            # Use alias for both SQL alias and column prefix
            select_columns.append(f"{alias}.{column} AS {alias}_{column}")
    
    logger.info(f"Total columns in SELECT clause: {len(select_columns)}")
    
    return select_columns


def main():
    logger.info('Starting gold loader for clinisys_embrioes (selective columns)')
    logger.info(f'Source database path: {source_db_path}')
    logger.info(f'Target database path: {target_db_path}')
    
    # Verify source database exists
    if not os.path.exists(source_db_path):
        logger.error(f'Source database not found: {source_db_path}')
        return
    
    # Build SELECT clause with only specified columns
    select_columns = build_select_columns()
    
    # New join logic with view_tratamentos joined twice:
    # Use CTE with deduplication hierarchy to select best tratamento per (prontuario, data_procedimento)
    # Hierarchy: 1) non-null resultado_tratamento, 2) non-null tipo_procedimento, 3) higher tentativa, 4) lower id
    query = f'''
    WITH trat_clean AS (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY prontuario, data_procedimento 
                   ORDER BY 
                       CASE WHEN resultado_tratamento IS NOT NULL THEN 0 ELSE 1 END,
                       CASE WHEN tipo_procedimento IS NOT NULL THEN 0 ELSE 1 END,
                       tentativa DESC NULLS LAST,
                       id ASC
               ) as rn
        FROM silver.view_tratamentos
        WHERE prontuario IS NOT NULL AND data_procedimento IS NOT NULL
    )
    SELECT
        {', '.join(select_columns)}
    FROM silver.view_micromanipulacao_oocitos oocito
    
    LEFT JOIN silver.view_micromanipulacao micro 
        ON oocito.id_micromanipulacao = micro.codigo_ficha
    
    LEFT JOIN trat_clean trat1 
        ON micro.prontuario = trat1.prontuario 
        AND trat1.data_procedimento = micro.Data_DL
        AND trat1.rn = 1
    
    LEFT JOIN silver.view_embrioes_congelados emb_cong 
        ON emb_cong.id_oocito = oocito.id
        
    LEFT JOIN silver.view_congelamentos_embrioes cong_em 
        ON emb_cong.id_congelamento = cong_em.id 
        AND emb_cong.prontuario = cong_em.prontuario
        AND cong_em.id IS NOT NULL
        AND emb_cong.id_congelamento IS NOT NULL
    
    LEFT JOIN silver.view_descongelamentos_embrioes descong_em 
        ON emb_cong.id_descongelamento = descong_em.id 
        AND emb_cong.prontuario = descong_em.prontuario
        AND descong_em.id IS NOT NULL
        AND emb_cong.id_descongelamento IS NOT NULL
    
    LEFT JOIN trat_clean trat2
        ON micro.prontuario = trat2.prontuario 
        AND trat2.data_procedimento = descong_em.DataTransferencia
        AND trat2.rn = 1
        
    ORDER BY oocito.id_micromanipulacao, oocito.id 
    '''
    
    try:
        # Read data from source database
        logger.info('Connecting to source database to read data')
        with duckdb.connect(source_db_path) as source_con:
            logger.info('Connected to source DuckDB')
            
            # Indexes should be created in silver layer, not here
            logger.info('Using pre-created indexes from silver layer for better JOIN performance')
            
            # Execute query to get data
            df = source_con.execute(query).df()
            logger.info(f'Read {len(df)} rows from source database')
            
            # Validate data
            if df.empty:
                logger.warning('No data found in source database')
                return
            
            logger.info(f'Final column count: {len(df.columns)}')
            
        # Write data to target database
        logger.info('Connecting to target database to write data')
        with duckdb.connect(target_db_path) as target_con:
            logger.info('Connected to target DuckDB')
            
            # Create schema if not exists
            target_con.execute('CREATE SCHEMA IF NOT EXISTS gold;')
            logger.info('Ensured gold schema exists')
            
            # Drop existing table if it exists
            target_con.execute('DROP TABLE IF EXISTS gold.clinisys_embrioes;')
            logger.info('Dropped existing gold.clinisys_embrioes table if it existed')
            
            # Create table from DataFrame
            target_con.execute('CREATE TABLE gold.clinisys_embrioes AS SELECT * FROM df')
            logger.info('Created gold.clinisys_embrioes table')
            
            # Create indexes for better JOIN performance
            logger.info('Creating indexes for better JOIN performance...')
            index_queries = [
                "CREATE INDEX IF NOT EXISTS idx_clinisys_micro_data_dl ON gold.clinisys_embrioes(micro_Data_DL)",
                "CREATE INDEX IF NOT EXISTS idx_clinisys_micro_prontuario ON gold.clinisys_embrioes(micro_prontuario)",
                "CREATE INDEX IF NOT EXISTS idx_clinisys_trat1_id ON gold.clinisys_embrioes(trat1_id)",
                "CREATE INDEX IF NOT EXISTS idx_clinisys_trat2_id ON gold.clinisys_embrioes(trat2_id)"
            ]
            
            for idx_query in index_queries:
                try:
                    target_con.execute(idx_query)
                except Exception as e:
                    logger.warning(f'Index creation failed (may already exist): {e}')
            
            logger.info('Indexes created successfully')
            
            # Validate row count
            row_count = target_con.execute("SELECT COUNT(*) FROM gold.clinisys_embrioes").fetchone()[0]
            logger.info(f'gold.clinisys_embrioes row count: {row_count}')
            
            # ============================================================
            # MATCHING QUALITY REPORT
            # ============================================================
            logger.info('=' * 80)
            logger.info('MATCHING QUALITY REPORT')
            logger.info('=' * 80)
            
            # 1. Base table validation
            with duckdb.connect(source_db_path) as source_con:
                base_table_count = source_con.execute("SELECT COUNT(*) FROM silver.view_micromanipulacao_oocitos").fetchone()[0]
            
            logger.info(f'\n1. BASE TABLE VALIDATION:')
            logger.info(f'   Base table (view_micromanipulacao_oocitos): {base_table_count:,} rows')
            logger.info(f'   Result table (gold.clinisys_embrioes): {row_count:,} rows')
            
            if base_table_count == row_count:
                logger.info(f'   [PASS] Row count matches - No rows lost in joins')
            else:
                logger.warning(f'   [!] Row count mismatch - {abs(base_table_count - row_count):,} rows difference')
            
            # 2. Join match statistics - query the created table
            logger.info(f'\n2. JOIN MATCH STATISTICS:')
            
            # Join 1: view_micromanipulacao (mic)
            mic_matched = target_con.execute("""
                SELECT COUNT(*) FROM gold.clinisys_embrioes 
                WHERE micro_numero_caso IS NOT NULL OR micro_prontuario IS NOT NULL OR micro_Data_DL IS NOT NULL OR micro_data_procedimento IS NOT NULL
            """).fetchone()[0]
            mic_match_rate = (mic_matched / row_count * 100) if row_count > 0 else 0
            logger.info(f'\n   Join 1: view_micromanipulacao (mic)')
            logger.info(f'   - Key: ooc.id_micromanipulacao = mic.codigo_ficha')
            logger.info(f'   - Matched: {mic_matched:,} / {row_count:,} ({mic_match_rate:.2f}%)')
            logger.info(f'   - Unmatched: {row_count - mic_matched:,}')
            
            # Join 2: view_tratamentos (trat1)
            trat1_matched = target_con.execute("""
                SELECT COUNT(*) FROM gold.clinisys_embrioes 
                WHERE trat1_tentativa IS NOT NULL OR trat1_tipo_procedimento IS NOT NULL
            """).fetchone()[0]
            trat1_match_rate = (trat1_matched / row_count * 100) if row_count > 0 else 0
            logger.info(f'\n   Join 2: view_tratamentos (trat1) - micromanipulation date')
            logger.info(f'   - Keys: mic.prontuario = trat1.prontuario AND trat1.data_procedimento = mic.data_procedimento')
            logger.info(f'   - Matched: {trat1_matched:,} / {row_count:,} ({trat1_match_rate:.2f}%)')
            logger.info(f'   - Unmatched: {row_count - trat1_matched:,}')
            
            # Join 3: view_embrioes_congelados (vec)
            vec_matched = target_con.execute("""
                SELECT COUNT(*) FROM gold.clinisys_embrioes 
                WHERE emb_cong_qualidade IS NOT NULL OR emb_cong_transferidos IS NOT NULL
            """).fetchone()[0]
            vec_match_rate = (vec_matched / row_count * 100) if row_count > 0 else 0
            logger.info(f'\n   Join 3: view_embrioes_congelados (vec)')
            logger.info(f'   - Key: vec.id_oocito = ooc.id')
            logger.info(f'   - Matched: {vec_matched:,} / {row_count:,} ({vec_match_rate:.2f}%)')
            logger.info(f'   - Unmatched: {row_count - vec_matched:,}')
            
            # Join 4: view_congelamentos_embrioes (vce)
            vce_matched = target_con.execute("""
                SELECT COUNT(*) FROM gold.clinisys_embrioes 
                WHERE cong_em_CodCongelamento IS NOT NULL
            """).fetchone()[0]
            vce_match_rate = (vce_matched / row_count * 100) if row_count > 0 else 0
            logger.info(f'\n   Join 4: view_congelamentos_embrioes (vce)')
            logger.info(f'   - Keys: vec.id_congelamento = vce.id AND vec.prontuario = vce.prontuario')
            logger.info(f'   - Matched: {vce_matched:,} / {row_count:,} ({vce_match_rate:.2f}%)')
            logger.info(f'   - Unmatched: {row_count - vce_matched:,}')
            
            # Join 5: view_descongelamentos_embrioes (vde)
            vde_matched = target_con.execute("""
                SELECT COUNT(*) FROM gold.clinisys_embrioes 
                WHERE descong_em_CodDescongelamento IS NOT NULL OR descong_em_DataTransferencia IS NOT NULL
            """).fetchone()[0]
            vde_match_rate = (vde_matched / row_count * 100) if row_count > 0 else 0
            logger.info(f'\n   Join 5: view_descongelamentos_embrioes (vde)')
            logger.info(f'   - Keys: vec.id_descongelamento = vde.id AND vec.prontuario = vde.prontuario')
            logger.info(f'   - Matched: {vde_matched:,} / {row_count:,} ({vde_match_rate:.2f}%)')
            logger.info(f'   - Unmatched: {row_count - vde_matched:,}')
            
            # Join 6: view_tratamentos (trat2)
            trat2_matched = target_con.execute("""
                SELECT COUNT(*) FROM gold.clinisys_embrioes 
                WHERE trat2_tentativa IS NOT NULL OR trat2_tipo_procedimento IS NOT NULL
            """).fetchone()[0]
            trat2_match_rate = (trat2_matched / row_count * 100) if row_count > 0 else 0
            logger.info(f'\n   Join 6: view_tratamentos (trat2) - transfer date')
            logger.info(f'   - Keys: mic.prontuario = trat2.prontuario AND trat2.data_procedimento = vde.DataTransferencia')
            logger.info(f'   - Matched: {trat2_matched:,} / {row_count:,} ({trat2_match_rate:.2f}%)')
            logger.info(f'   - Unmatched: {row_count - trat2_matched:,}')
            
            # Summary
            logger.info(f'\n3. SUMMARY:')
            logger.info(f'   - Total rows: {row_count:,}')
            logger.info(f'   - Base table match: {"[PASS]" if base_table_count == row_count else "[FAIL]"}')
            
            logger.info('=' * 80)
            
    except Exception as e:
        logger.error(f'Error in gold loader: {e}', exc_info=True)
        raise
        
    logger.info('Gold loader finished successfully')


if __name__ == '__main__':
    main()