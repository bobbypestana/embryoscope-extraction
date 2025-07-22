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

db_path = os.path.join('..', 'database', 'clinisys_all.duckdb') if not os.path.exists('database/clinisys_all.duckdb') else 'database/clinisys_all.duckdb'

def main():
    logger.info('Starting gold loader for clinisys_embrioes')
    logger.info(f'Database path: {db_path}')
    query = '''
    SELECT
        ooc.id AS oocito_id,
        ooc.id_micromanipulacao AS micromanipulacao_id,
        ooc.TCD AS TCD,
        ooc.Maturidade AS oocito_maturidade,
        ooc.PGD as PGD,
        ooc.ResultadoPGD ,
        ooc.IdentificacaoPGD ,
        ooc.Fertilizacao ,
        ooc.CorpusculoPolar ,
        mic.prontuario AS paciente_prontuario,
        mic.numero_caso AS caso_numero,
        mic.Data_DL AS Data_DL,
        mic.oocitos AS oocitos,
        mic.data_procedimento as data_procedimento,
        vco.id as id_congelamento_ovulo,
        vco.CodCongelamento as CodCongelamento, 
        vco.Data as data_congelamento_ovulo,
        vco.NOvulos as ovulos_congelados,
        vco.NPailletes ,
        vco.Identificacao as identificacao_congelamento_ovulo,
        vco.Tambor as tambor_congelanmento_ovulo,
        vdo.id as id_descongelamento_ovulo,
        vdo.CodDescongelamento as CodDescongelamento,
        vdo.doadora as doadora,
        vdo.DataDescongelamento as data_descongelamento_ovulo,
        vdo.Identificacao as idenficacao_descongelamento_ovulo,
        vce.id as id_congelamento_embriao,
        vce.CodCongelamento as codigo_congelamento_embriao,
        vce.NEmbrioes as embrioes_congelados,
        vce.Identificacao as identificacao_embriao_congelado,
        vde.id as id_descongelamento_embrioes,
        vde.CodDescongelamento as codigo_descongelamento_embriao,
        vec.id as id_embriao_congelado,
        vec.pailletes,
        vec.pailletes_id,
        vec.embriao,
        vec.score_maia     
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
    -- where mic.prontuario = 157654 
    order by micromanipulacao_id , oocito_id 
    '''
    try:
        with duckdb.connect(db_path) as con:
            logger.info('Connected to DuckDB')
            con.execute('CREATE SCHEMA IF NOT EXISTS gold;')
            logger.info('Ensured gold schema exists')
            con.execute('DROP TABLE IF EXISTS gold.clinisys_embrioes;')
            logger.info('Dropped existing gold.clinisys_embrioes table if it existed')
            create_sql = f"""
            CREATE TABLE gold.clinisys_embrioes AS {query}
            """
            logger.debug(f'Executing CREATE TABLE AS SELECT for gold.clinisys_embrioes')
            con.execute(create_sql)
            logger.info('Created gold.clinisys_embrioes table')
            # Validate schema
            schema = con.execute("DESCRIBE gold.clinisys_embrioes").fetchdf()
            logger.info(f'gold.clinisys_embrioes schema:\n{schema}')
            # Validate row count
            row_count = con.execute("SELECT COUNT(*) FROM gold.clinisys_embrioes").fetchone()[0]
            logger.info(f'gold.clinisys_embrioes row count: {row_count}')
    except Exception as e:
        logger.error(f'Error in gold loader: {e}', exc_info=True)
    logger.info('Gold loader finished successfully')

if __name__ == '__main__':
    main() 