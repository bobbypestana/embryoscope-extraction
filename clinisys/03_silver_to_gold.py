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

# Database paths - read from clinisys_all, write to huntington_data_lake
source_db_path = os.path.join('..', 'database', 'clinisys_all.duckdb')
target_db_path = os.path.join('..', 'database', 'huntington_data_lake.duckdb')

def main():
    logger.info('Starting gold loader for clinisys_embrioes')
    logger.info(f'Source database path: {source_db_path}')
    logger.info(f'Target database path: {target_db_path}')
    
    # Verify source database exists
    if not os.path.exists(source_db_path):
        logger.error(f'Source database not found: {source_db_path}')
        return
    
    # Low-fill column filtering is done in bronze-to-silver, so we use all columns here
    # # First, identify columns with low fill rates from each table
    # logger.info('Analyzing column fill rates in source tables...')
    # 
    # def get_low_fill_columns(table_name, schema='silver', min_fill_rate=10.0):
    #     """Get columns with fill rate below threshold from a specific table"""
    #     with duckdb.connect(source_db_path) as temp_con:
    #         # Get all columns from the table
    #         columns_query = f"""
    #         SELECT column_name 
    #         FROM information_schema.columns 
    #         WHERE table_schema = '{schema}' 
    #         AND table_name = '{table_name}'
    #         ORDER BY ordinal_position
    #         """
    #         columns_df = temp_con.execute(columns_query).df()
    #         
    #         if columns_df.empty:
    #             return []
    #         
    #         # Get total row count
    #         total_rows_query = f"SELECT COUNT(*) FROM {schema}.{table_name}"
    #         total_rows = temp_con.execute(total_rows_query).fetchone()[0]
    #         
    #         low_fill_columns = []
    #         for _, row in columns_df.iterrows():
    #             column_name = row['column_name']
    #             
    #             # Count non-null values
    #             non_null_query = f"SELECT COUNT({column_name}) FROM {schema}.{table_name}"
    #             non_null_count = temp_con.execute(non_null_query).fetchone()[0]
    #             
    #             # Calculate fill rate
    #             fill_rate = (non_null_count / total_rows) * 100 if total_rows > 0 else 0
    #             
    #             if fill_rate < min_fill_rate:
    #                 low_fill_columns.append(column_name)
    #         
    #         return low_fill_columns
    # 
    # # Get low-fill columns from each table (only tables we're using in the new JOIN logic)
    # ooc_low_fill = get_low_fill_columns('view_micromanipulacao_oocitos')
    # mic_low_fill = get_low_fill_columns('view_micromanipulacao')
    # vce_low_fill = get_low_fill_columns('view_congelamentos_embrioes')
    # vde_low_fill = get_low_fill_columns('view_descongelamentos_embrioes')
    # vec_low_fill = get_low_fill_columns('view_embrioes_congelados')
    # tr_low_fill = get_low_fill_columns('view_tratamentos')
    # 
    # logger.info(f'Low-fill columns found:')
    # logger.info(f'  - view_micromanipulacao_oocitos: {len(ooc_low_fill)} columns')
    # logger.info(f'  - view_micromanipulacao: {len(mic_low_fill)} columns')
    # logger.info(f'  - view_congelamentos_embrioes: {len(vce_low_fill)} columns')
    # logger.info(f'  - view_descongelamentos_embrioes: {len(vde_low_fill)} columns')
    # logger.info(f'  - view_embrioes_congelados: {len(vec_low_fill)} columns')
    # logger.info(f'  - view_tratamentos: {len(tr_low_fill)} columns')
    # 
    # # Build column lists excluding low-fill columns
    # def build_column_list(table_alias, low_fill_columns):
    #     with duckdb.connect(source_db_path) as temp_con:
    #         # Map table aliases to actual table names
    #         table_name_map = {
    #             'ooc': 'view_micromanipulacao_oocitos',
    #             'mic': 'view_micromanipulacao',
    #             'vce': 'view_congelamentos_embrioes',
    #             'vde': 'view_descongelamentos_embrioes',
    #             'vec': 'view_embrioes_congelados',
    #             'tr': 'view_tratamentos'
    #         }
    #         
    #         table_name = table_name_map.get(table_alias, table_alias)
    #         
    #         all_columns_query = f"""
    #         SELECT column_name 
    #         FROM information_schema.columns 
    #         WHERE table_schema = 'silver' 
    #         AND table_name = '{table_name}'
    #         ORDER BY ordinal_position
    #         """
    #         all_columns = temp_con.execute(all_columns_query).df()['column_name'].tolist()
    #         
    #         # Filter out low-fill columns
    #         filtered_columns = [col for col in all_columns if col not in low_fill_columns]
    #         logger.info(f'Table {table_alias}: {len(all_columns)} total columns, {len(filtered_columns)} after filtering')
    #         return filtered_columns
    # 
    # # Get filtered column lists
    # ooc_columns = build_column_list('ooc', ooc_low_fill)
    # mic_columns = build_column_list('mic', mic_low_fill)
    # vce_columns = build_column_list('vce', vce_low_fill)
    # vde_columns = build_column_list('vde', vde_low_fill)
    # vec_columns = build_column_list('vec', vec_low_fill)
    # tr_columns = build_column_list('tr', tr_low_fill)
    # 
    # # Build the SELECT clause with only high-fill columns
    # select_columns = []
    # if ooc_columns:
    #     select_columns.extend([f"ooc.{col}" for col in ooc_columns])
    # if mic_columns:
    #     select_columns.extend([f"mic.{col}" for col in mic_columns])
    # if vce_columns:
    #     select_columns.extend([f"vce.{col}" for col in vce_columns])
    # if vde_columns:
    #     select_columns.extend([f"vde.{col}" for col in vde_columns])
    # if vec_columns:
    #     select_columns.extend([f"vec.{col}" for col in vec_columns])
    # if tr_columns:
    #     select_columns.extend([f"tr.{col}" for col in tr_columns])
    # 
    # logger.info(f'Selected {len(select_columns)} columns for join (excluding low-fill columns)')
    
    # Build column lists (all columns, no filtering - filtering done in bronze-to-silver)
    def build_column_list(table_alias):
        with duckdb.connect(source_db_path) as temp_con:
            # Map table aliases to actual table names
            table_name_map = {
                'ooc': 'view_micromanipulacao_oocitos',
                'mic': 'view_micromanipulacao',
                'vce': 'view_congelamentos_embrioes',
                'vde': 'view_descongelamentos_embrioes',
                'vec': 'view_embrioes_congelados',
                'tr': 'view_tratamentos'
            }
            
            table_name = table_name_map.get(table_alias, table_alias)
            
            all_columns_query = f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = 'silver' 
            AND table_name = '{table_name}'
            ORDER BY ordinal_position
            """
            all_columns = temp_con.execute(all_columns_query).df()['column_name'].tolist()
            logger.info(f'Table {table_alias}: {len(all_columns)} total columns')
            return all_columns
    
    # Get all column lists (no filtering)
    ooc_columns = build_column_list('ooc')
    mic_columns = build_column_list('mic')
    vce_columns = build_column_list('vce')
    vde_columns = build_column_list('vde')
    vec_columns = build_column_list('vec')
    tr_columns = build_column_list('tr')
    
    # Build the SELECT clause with all columns
    select_columns = []
    if ooc_columns:
        select_columns.extend([f"ooc.{col}" for col in ooc_columns])
    if mic_columns:
        select_columns.extend([f"mic.{col}" for col in mic_columns])
    if vce_columns:
        select_columns.extend([f"vce.{col}" for col in vce_columns])
    if vde_columns:
        select_columns.extend([f"vde.{col}" for col in vde_columns])
    if vec_columns:
        select_columns.extend([f"vec.{col}" for col in vec_columns])
    if tr_columns:
        select_columns.extend([f"tr.{col}" for col in tr_columns])
    
    logger.info(f'Selected {len(select_columns)} columns for join (all columns from silver layer)')
    
    # New JOIN logic as specified:
    # 1. Start from ooc
    # 2. LEFT JOIN vec on ooc.id = vec.id_oocito
    # 3. LEFT JOIN vce on vec.id_congelamento = vce.id and vec.prontuario = vce.prontuario
    # 4. LEFT JOIN vde on vec.id_descongelamento = vde.id and vec.prontuario = vde.prontuario
    # 5. LEFT JOIN mic on ooc.id_micromanipulacao = mic.codigo_ficha and mic.prontuario = vec.prontuario
    query = f'''
    SELECT
        {', '.join(select_columns)}
    FROM silver.view_micromanipulacao_oocitos ooc
    LEFT JOIN silver.view_embrioes_congelados vec 
        ON vec.id_oocito = ooc.id
    LEFT JOIN silver.view_congelamentos_embrioes vce 
        ON vec.id_congelamento = vce.id 
        AND vec.prontuario = vce.prontuario
    LEFT JOIN silver.view_descongelamentos_embrioes vde 
        ON vec.id_descongelamento = vde.id 
        AND vec.prontuario = vde.prontuario
    LEFT JOIN silver.view_micromanipulacao mic 
        ON ooc.id_micromanipulacao = mic.codigo_ficha
        AND (vec.prontuario IS NULL OR mic.prontuario = vec.prontuario)
    LEFT JOIN silver.view_tratamentos tr 
        ON vde.prontuario = tr.prontuario AND tr.data_procedimento = vde.DataTransferencia
    ORDER BY ooc.id_micromanipulacao, ooc.id 
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
                
            # Column filtering and prefixing
            logger.info('Applying column filtering and prefixing...')
            
            # Remove all hash and extraction_timestamp columns (these weren't filtered in the query)
            columns_to_remove = [col for col in df.columns if 'hash' in col.lower() or 'extraction_timestamp' in col.lower()]
            if columns_to_remove:
                df = df.drop(columns=columns_to_remove)
                logger.info(f'Removed {len(columns_to_remove)} hash/extraction_timestamp columns: {columns_to_remove}')
            else:
                logger.info('No hash/extraction_timestamp columns found')
            
            # Define prefixes for each table
            table_prefixes = {
                'ooc': 'oocito_',
                'mic': 'micro_',
                'vce': 'cong_em_',
                'vde': 'descong_em_',
                'vec': 'emb_cong_',
                'tr': 'trat_',
            }
            
            # Get original column names from each table to determine prefixes
            with duckdb.connect(source_db_path) as temp_con:
                ooc_columns = set(temp_con.execute("SELECT column_name FROM information_schema.columns WHERE table_schema = 'silver' AND table_name = 'view_micromanipulacao_oocitos'").df()['column_name'].tolist())
                mic_columns = set(temp_con.execute("SELECT column_name FROM information_schema.columns WHERE table_schema = 'silver' AND table_name = 'view_micromanipulacao'").df()['column_name'].tolist())
                vce_columns = set(temp_con.execute("SELECT column_name FROM information_schema.columns WHERE table_schema = 'silver' AND table_name = 'view_congelamentos_embrioes'").df()['column_name'].tolist())
                vde_columns = set(temp_con.execute("SELECT column_name FROM information_schema.columns WHERE table_schema = 'silver' AND table_name = 'view_descongelamentos_embrioes'").df()['column_name'].tolist())
                vec_columns = set(temp_con.execute("SELECT column_name FROM information_schema.columns WHERE table_schema = 'silver' AND table_name = 'view_embrioes_congelados'").df()['column_name'].tolist())
                tr_columns = set(temp_con.execute("SELECT column_name FROM information_schema.columns WHERE table_schema = 'silver' AND table_name = 'view_tratamentos'").df()['column_name'].tolist())
            
            # Create mapping of original column names to prefixed names
            column_mapping = {}
            
            # Map columns from each table to their prefixed versions
            for col in df.columns:
                if col in ooc_columns:
                    column_mapping[col] = f"oocito_{col}"
                elif col in mic_columns:
                    column_mapping[col] = f"micro_{col}"
                elif col in vce_columns:
                    column_mapping[col] = f"cong_em_{col}"
                elif col in vde_columns:
                    column_mapping[col] = f"descong_em_{col}"
                elif col in vec_columns:
                    column_mapping[col] = f"emb_cong_{col}"
                elif col in tr_columns:
                    column_mapping[col] = f"trat_{col}"
                else:
                    # Keep original name for columns that don't match any table
                    column_mapping[col] = col
            
            # Rename columns
            df = df.rename(columns=column_mapping)
            logger.info(f'Applied prefixes to columns. New column count: {len(df.columns)}')
            
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
                "CREATE INDEX IF NOT EXISTS idx_clinisys_oocito_flag_embryoscope ON gold.clinisys_embrioes(oocito_flag_embryoscope)"
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
            
    except Exception as e:
        logger.error(f'Error in gold loader: {e}', exc_info=True)
        raise
        
    logger.info('Gold loader finished successfully')

if __name__ == '__main__':
    main() 