import duckdb
import os
import logging
from datetime import datetime

# Setup logging
LOGS_DIR = os.path.join(os.path.dirname(__file__), 'logs')
os.makedirs(LOGS_DIR, exist_ok=True)
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
LOG_PATH = os.path.join(LOGS_DIR, f'silver_loader_try_strptime_complete_{timestamp}.log')
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.FileHandler(LOG_PATH),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

db_path = os.path.join('..', 'database', 'clinisys_all.duckdb') if not os.path.exists('database/clinisys_all.duckdb') else 'database/clinisys_all.duckdb'

def get_column_transformation(column_name, column_type, sample_data=None):
    """Determine the appropriate transformation for a column based on its name, type, and sample data"""
    
    # Special handling for known date columns
    date_columns = [
        'data', 'data_inicial', 'data_final', 'Data', 'DataCongelamento', 'DataDescongelamento',
        'DataTransferencia', 'data_entrega', 'data_pagamento', 'data_entrega_orcamento',
        'data_ultima_modificacao', 'data_agendamento_original', 'responsavel_recebimento_data',
        'responsavel_armazenamento_data'
    ]
    
    # Special handling for known time columns
    time_columns = ['hora', 'Hora', 'inicio']
    
    # Special handling for known numeric columns that should be integers
    int_columns = [
        'id', 'codigo', 'prontuario', 'ficha_id', 'medicamento', 'quantidade', 'duracao',
        'registro', 'unidade_origem', 'unidade', 'idade_esposa', 'Unidade', 'NEmbrioes',
        'NOvulos', 'doadora', 'Transferencia', 'Prateleira', 'responsavel_recebimento',
        'responsavel_armazenamento', 'BiologoResponsavel', 'responsavel_congelamento_d5',
        'responsavel_checagem_d5', 'responsavel_congelamento_d6', 'responsavel_checagem_d6',
        'responsavel_congelamento_d7', 'responsavel_checagem_d7', 'id_oocito', 'id_congelamento',
        'id_descongelamento', 'agendamento_id', 'medico', 'medico2', 'evento', 'centro_custos',
        'agenda', 'confirmado', 'paciente_codigo', 'codigo_ficha', 'IdadeEsposa_DG',
        'id_micromanipulacao', 'agendamento_id'
    ]
    
    # Special handling for known float columns
    float_columns = [
        'rqe', 'intervalo', 'ovulo', 'd2', 'd3', 'd4', 'd5', 'd6', 'd7', 'rack2', 'rack3', 'rack4',
        'responsavel_transferencia', 'valor', 'IdadeEsposa_DG'
    ]
    
    # Special handling for extraction_timestamp
    if column_name == 'extraction_timestamp':
        return f"try_strptime({column_name}, '%Y%m%d_%H%M%S') AS {column_name}"
    
    # Date columns
    if column_name.lower() in [col.lower() for col in date_columns]:
        return f"try_strptime({column_name}, '%d/%m/%Y') AS {column_name}"
    
    # Time columns
    if column_name.lower() in [col.lower() for col in time_columns]:
        return f"try_strptime({column_name}, '%H:%M') AS {column_name}"
    
    # Integer columns
    if column_name.lower() in [col.lower() for col in int_columns]:
        return f"try_cast({column_name} AS INTEGER) AS {column_name}"
    
    # Float columns
    if column_name.lower() in [col.lower() for col in float_columns]:
        return f"try_cast({column_name} AS DOUBLE) AS {column_name}"
    
    # Special handling for valor column (currency)
    if column_name.lower() == 'valor':
        return f"try_cast(REPLACE(REPLACE({column_name}, '.', ''), ',', '.') AS DOUBLE) AS {column_name}"
    
    # For all other columns, keep as VARCHAR (safe default)
    return f"CAST({column_name} AS VARCHAR) AS {column_name}"

def get_all_columns_from_bronze(con, table_name):
    """Get all column names from a bronze table"""
    try:
        result = con.execute(f"DESCRIBE bronze.{table_name}").df()
        return result['column_name'].tolist()
    except Exception as e:
        logger.error(f"Error getting columns for {table_name}: {e}")
        return []

def generate_complete_cast_sql(con, table_name):
    """Generate complete CAST SQL for all columns in a table"""
    columns = get_all_columns_from_bronze(con, table_name)
    if not columns:
        logger.error(f"No columns found for table {table_name}")
        return None
    
    logger.info(f"Processing {len(columns)} columns for table {table_name}")
    
    cast_clauses = []
    for column in columns:
        try:
            # Get sample data to help determine type
            sample_result = con.execute(f"SELECT {column} FROM bronze.{table_name} WHERE {column} IS NOT NULL LIMIT 1").df()
            sample_data = sample_result[column].iloc[0] if not sample_result.empty else None
            
            # Get column type from bronze
            desc_result = con.execute(f"DESCRIBE bronze.{table_name}").df()
            column_info = desc_result[desc_result['column_name'] == column]
            column_type = column_info['column_type'].iloc[0] if not column_info.empty else 'VARCHAR'
            
            cast_clause = get_column_transformation(column, column_type, sample_data)
            cast_clauses.append(cast_clause)
            
        except Exception as e:
            logger.warning(f"Error processing column {column} in {table_name}: {e}")
            # Fallback to VARCHAR
            cast_clauses.append(f"CAST({column} AS VARCHAR) AS {column}")
    
    return ',\n        '.join(cast_clauses)

def deduplicate_and_format_sql(table, cast_sql):
    return f'''
    CREATE SCHEMA IF NOT EXISTS silver;
    CREATE OR REPLACE TABLE silver.{table} AS
    SELECT {cast_sql}
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY hash ORDER BY extraction_timestamp DESC) AS rn
        FROM bronze.{table}
    )
    WHERE rn = 1;
    '''

def main():
    logger.info('Starting complete try_strptime silver loader (ALL COLUMNS)')
    logger.info(f'Database path: {db_path}')
    
    with duckdb.connect(db_path) as con:
        # Get list of bronze tables
        bronze_tables = con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'bronze'").df()['table_name'].tolist()
        logger.info(f'Found {len(bronze_tables)} bronze tables: {bronze_tables}')
        
        for table in bronze_tables:
            logger.info(f'Processing {table}')
            try:
                # Generate complete CAST SQL for all columns
                cast_sql = generate_complete_cast_sql(con, table)
                if cast_sql is None:
                    logger.error(f'Failed to generate CAST SQL for {table}')
                    continue
                
                sql = deduplicate_and_format_sql(table, cast_sql)
                logger.debug(f'Generated SQL for {table} (first 500 chars): {sql[:500]}...')
                
                con.execute(sql)
                logger.info(f'Created/updated silver.{table}')
                
                # Verify column count
                bronze_cols = len(get_all_columns_from_bronze(con, table))
                silver_cols = con.execute(f"SELECT COUNT(*) as col_count FROM information_schema.columns WHERE table_schema = 'silver' AND table_name = '{table}'").df()['col_count'].iloc[0]
                logger.info(f'Column count: Bronze={bronze_cols}, Silver={silver_cols}')
                
                if bronze_cols != silver_cols:
                    logger.warning(f'Column count mismatch for {table}: Bronze={bronze_cols}, Silver={silver_cols}')
                
            except Exception as e:
                logger.error(f'Error processing {table}: {e}', exc_info=True)
    
    logger.info('Complete try_strptime silver loader finished successfully')

if __name__ == '__main__':
    main() 