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
    
    query = '''
    SELECT
        -- ooc.id AS oocito_id,
        -- ooc.id_micromanipulacao AS micromanipulacao_id,
        -- ooc.TCD AS TCD,
        -- ooc.Maturidade AS oocito_maturidade,
        -- ooc.PGD as PGD,
        -- ooc.ResultadoPGD ,
        -- ooc.IdentificacaoPGD ,
        -- ooc.Fertilizacao ,
        -- ooc.CorpusculoPolar ,
        -- mic.prontuario AS paciente_prontuario,
        -- mic.numero_caso AS caso_numero,
        -- mic.Data_DL AS Data_DL,
        -- mic.oocitos AS oocitos,
        -- mic.data_procedimento as data_procedimento,
        -- vco.id as id_congelamento_ovulo,
        -- vco.CodCongelamento as CodCongelamento, 
        -- vco.Data as data_congelamento_ovulo,
        -- vco.NOvulos as ovulos_congelados,
        -- vco.NPailletes ,
        -- vco.Identificacao as identificacao_congelamento_ovulo,
        -- vco.Tambor as tambor_congelanmento_ovulo,
        -- vdo.id as id_descongelamento_ovulo,
        -- vdo.CodDescongelamento as CodDescongelamento,
        -- vdo.doadora as doadora,
        -- vdo.DataDescongelamento as data_descongelamento_ovulo,
        -- vdo.Identificacao as idenficacao_descongelamento_ovulo,
        -- vce.id as id_congelamento_embriao,
        -- vce.CodCongelamento as codigo_congelamento_embriao,
        -- vce.NEmbrioes as embrioes_congelados,
        -- vce.Identificacao as identificacao_embriao_congelado,
        -- vde.id as id_descongelamento_embrioes,
        -- vde.CodDescongelamento as codigo_descongelamento_embriao,
        -- vec.id as id_embriao_congelado,
        -- vec.pailletes,
        -- vec.pailletes_id,
        -- vec.embriao,
        -- vec.score_maia     
        ooc.*,
        mic.*,
        vco.*,
        vdo.*,
        vce.*,
        vde.*,
        vec.*,
        -- vt.*
    FROM silver.view_micromanipulacao_oocitos ooc
    LEFT JOIN silver.view_micromanipulacao mic
        ON ooc.id_micromanipulacao = mic.codigo_ficha
    LEFT JOIN silver.view_congelamentos_ovulos vco 
        ON vco.Ciclo = mic.numero_caso and vco.prontuario = mic.prontuario and ooc.TCD = 'Criopreservado' and vco.Ciclo IS NOT NULL
    LEFT JOIN silver.view_descongelamentos_ovulos vdo 
        ON vdo.DataDescongelamento = mic.Data_DL and vdo.prontuario = mic.prontuario
    LEFT JOIN silver.view_congelamentos_embrioes vce 
        ON vce.Ciclo = mic.numero_caso and vce.prontuario = mic.prontuario and ooc.TCD = 'Criopreservado'
    LEFT JOIN silver.view_descongelamentos_embrioes vde 
        ON vde.Ciclo = mic.numero_caso and vde.CodCongelamento = vce.CodCongelamento 
    LEFT JOIN silver.view_embrioes_congelados vec 
        ON vec.id_oocito = ooc.id
    -- LEFT JOIN silver.view_tratamentos vt 
    --     ON vt.prontuario = mic.prontuario
    -- where mic.prontuario = 157654 
    order by ooc.id_micromanipulacao, ooc.id 
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
                'ooc': 'oocito_',
                'mic': 'micro_',
                'vco': 'cong_ov_',
                'vdo': 'descong_ov_',
                'vce': 'cong_em_',
                'vde': 'descong_em_',
                'vec': 'emb_cong_',
                # 'vt': 'trat_'
            }
            
            # Get original column names from each table to determine prefixes
            with duckdb.connect(source_db_path) as temp_con:
                ooc_columns = set(temp_con.execute("SELECT column_name FROM information_schema.columns WHERE table_schema = 'silver' AND table_name = 'view_micromanipulacao_oocitos'").df()['column_name'].tolist())
                mic_columns = set(temp_con.execute("SELECT column_name FROM information_schema.columns WHERE table_schema = 'silver' AND table_name = 'view_micromanipulacao'").df()['column_name'].tolist())
                vco_columns = set(temp_con.execute("SELECT column_name FROM information_schema.columns WHERE table_schema = 'silver' AND table_name = 'view_congelamentos_ovulos'").df()['column_name'].tolist())
                vdo_columns = set(temp_con.execute("SELECT column_name FROM information_schema.columns WHERE table_schema = 'silver' AND table_name = 'view_descongelamentos_ovulos'").df()['column_name'].tolist())
                vce_columns = set(temp_con.execute("SELECT column_name FROM information_schema.columns WHERE table_schema = 'silver' AND table_name = 'view_congelamentos_embrioes'").df()['column_name'].tolist())
                vde_columns = set(temp_con.execute("SELECT column_name FROM information_schema.columns WHERE table_schema = 'silver' AND table_name = 'view_descongelamentos_embrioes'").df()['column_name'].tolist())
                vec_columns = set(temp_con.execute("SELECT column_name FROM information_schema.columns WHERE table_schema = 'silver' AND table_name = 'view_embrioes_congelados'").df()['column_name'].tolist())
                # vt_columns = set(temp_con.execute("SELECT column_name FROM information_schema.columns WHERE table_schema = 'silver' AND table_name = 'view_tratamentos'").df()['column_name'].tolist())
            
            # Create mapping of original column names to prefixed names
            column_mapping = {}
            
            # Map columns from each table to their prefixed versions
            for col in df.columns:
                if col in ooc_columns:
                    column_mapping[col] = f"oocito_{col}"
                elif col in mic_columns:
                    column_mapping[col] = f"micro_{col}"
                elif col in vco_columns:
                    column_mapping[col] = f"cong_ov_{col}"
                elif col in vdo_columns:
                    column_mapping[col] = f"descong_ov_{col}"
                elif col in vce_columns:
                    column_mapping[col] = f"cong_em_{col}"
                elif col in vde_columns:
                    column_mapping[col] = f"descong_em_{col}"
                elif col in vec_columns:
                    column_mapping[col] = f"emb_cong_{col}"
                # elif col in vt_columns:
                #     column_mapping[col] = f"trat_{col}"
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
            target_con.execute('DROP TABLE IF EXISTS gold.clinisys_embrioes;')
            logger.info('Dropped existing gold.clinisys_embrioes table if it existed')
            
            # Create table from DataFrame
            target_con.execute('CREATE TABLE gold.clinisys_embrioes AS SELECT * FROM df')
            logger.info('Created gold.clinisys_embrioes table')
            
            # Validate schema
            schema = target_con.execute("DESCRIBE gold.clinisys_embrioes").fetchdf()
            logger.info(f'gold.clinisys_embrioes schema:\n{schema}')
            
            # Validate row count
            row_count = target_con.execute("SELECT COUNT(*) FROM gold.clinisys_embrioes").fetchone()[0]
            logger.info(f'gold.clinisys_embrioes row count: {row_count}')
            
    except Exception as e:
        logger.error(f'Error in gold loader: {e}', exc_info=True)
        raise
        
    logger.info('Gold loader finished successfully')

if __name__ == '__main__':
    main() 