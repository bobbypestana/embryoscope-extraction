import duckdb
import os
import logging
from datetime import datetime
import yaml
import pandas as pd

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

def get_column_transformation(column_name, column_type, sample_data=None):
    """Determine the appropriate transformation for a column based on its name, type, and sample data"""
    
    # Special handling for known date columns
    date_columns = [
        'data', 'data_inicial', 'data_final', 'Data', 'DataCongelamento', 'DataDescongelamento',
        'DataTransferencia', 'data_entrega', 'data_pagamento', 'data_entrega_orcamento',
        'data_ultima_modificacao', 'data_agendamento_original', 'responsavel_recebimento_data',
        'responsavel_armazenamento_data', 'Data_DL', 'data_procedimento', 'data_dum', 'data_inicio_inducao',
        'data_transferencia', 'data_congelamento', 'data_descongelamento'
    ]
    
    # Critical date columns that need proper DATE casting for JOIN performance
    critical_date_columns = ['Data_DL', 'DataDescongelamento', 'DataTransferencia']
    
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
        return f"""
        CASE 
            WHEN {column_name} IS NULL THEN NULL
            WHEN try_strptime({column_name}, '%Y%m%d_%H%M%S') IS NULL THEN NULL
            WHEN try_strptime({column_name}, '%Y%m%d_%H%M%S') > CURRENT_TIMESTAMP THEN NULL
            ELSE try_strptime({column_name}, '%Y%m%d_%H%M%S')
        END AS {column_name}"""
    
    # Date columns - ensure proper DATE type for JOIN performance
    if column_name.lower() in [col.lower() for col in date_columns]:
        return f"""
        CAST(
            CASE 
                WHEN {column_name} IS NULL THEN NULL
                WHEN {column_name} IN ('00/00/0000', '0000-00-00', '00/00/00', '0000/00/00', 'NULL', 'null', '') THEN NULL
                WHEN try_strptime({column_name}, '%d/%m/%Y') IS NULL THEN NULL
                WHEN try_strptime({column_name}, '%d/%m/%Y') > CURRENT_DATE THEN NULL
                WHEN year(try_strptime({column_name}, '%d/%m/%Y')) < 1900 OR year(try_strptime({column_name}, '%d/%m/%Y')) > 2030 THEN NULL
                ELSE try_strptime({column_name}, '%d/%m/%Y')
            END AS DATE
        ) AS {column_name}"""
    
    # Time columns
    if column_name.lower() in [col.lower() for col in time_columns]:
        return f"""
        CASE 
            WHEN {column_name} IS NULL THEN NULL
            WHEN {column_name} IN ('NULL', 'null', '') THEN NULL
            WHEN try_strptime({column_name}, '%H:%M') IS NULL THEN NULL
            ELSE try_strptime({column_name}, '%H:%M')
        END AS {column_name}"""
    
    # Integer columns (except prontuario which needs special handling)
    if column_name.lower() in [col.lower() for col in int_columns]:
        # Special handling for prontuario columns - keep as VARCHAR initially
        if column_name.lower() == 'prontuario':
            return f"CAST({column_name} AS VARCHAR) AS {column_name}"
        else:
            return f"try_cast({column_name} AS INTEGER) AS {column_name}"
    
    # Float columns
    if column_name.lower() in [col.lower() for col in float_columns]:
        return f"try_cast({column_name} AS DOUBLE) AS {column_name}"
    
    # Special handling for valor column (currency)
    if column_name.lower() == 'valor':
        return f"try_cast(REPLACE(REPLACE({column_name}, '.', ''), ',', '.') AS DOUBLE) AS {column_name}"
    
    # Special handling for peso_paciente, altura_paciente, peso_conjuge, altura_conjuge, peso, and altura (may have alpha chars, comma as decimal)
    if column_name.lower() in ['peso_paciente', 'altura_paciente', 'peso_conjuge', 'altura_conjuge', 'peso', 'altura']:
        return f"""
        CASE 
            WHEN {column_name} IS NULL THEN NULL
            WHEN TRIM(CAST({column_name} AS VARCHAR)) IN ('', 'NULL', 'null') THEN NULL
            ELSE 
                CASE 
                    WHEN array_length(regexp_extract_all(TRIM(CAST({column_name} AS VARCHAR)), '[0-9,.]')) = 0 THEN NULL
                    ELSE try_cast(
                        REPLACE(
                            array_to_string(regexp_extract_all(TRIM(CAST({column_name} AS VARCHAR)), '[0-9,.]'), ''),
                            ',',
                            '.'
                        ) AS DOUBLE
                    )
                END
        END AS {column_name}"""
    
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

def calculate_column_filling_rates(con, table_name, columns):
    """
    Calculate filling rate (non-null percentage) for each column in a bronze table.
    
    Args:
        con: DuckDB connection
        table_name: Name of the bronze table
        columns: List of column names to check
        
    Returns:
        Dictionary mapping column names to their filling rates (0-100)
    """
    filling_rates = {}
    
    try:
        # Get total row count
        total_count_result = con.execute(f"SELECT COUNT(*) as total FROM bronze.{table_name}").df()
        total_count = total_count_result['total'].iloc[0]
        
        if total_count == 0:
            logger.warning(f"Table {table_name} is empty, all columns will have 0% filling rate")
            return {col: 0.0 for col in columns}
        
        # Calculate filling rate for each column
        for column in columns:
            try:
                # Count non-null and non-empty values
                # Use try_cast to safely handle different data types
                non_null_result = con.execute(f"""
                    SELECT COUNT(*) as non_null_count 
                    FROM bronze.{table_name} 
                    WHERE {column} IS NOT NULL 
                    AND COALESCE(TRIM(try_cast({column} AS VARCHAR)), '') NOT IN ('NULL', 'null', '')
                """).df()
                non_null_count = non_null_result['non_null_count'].iloc[0]
                
                filling_rate = (non_null_count / total_count) * 100
                filling_rates[column] = filling_rate
                
            except Exception as e:
                logger.warning(f"Error calculating filling rate for column {column} in {table_name}: {e}")
                filling_rates[column] = 0.0
                
    except Exception as e:
        logger.error(f"Error calculating filling rates for {table_name}: {e}")
        return {col: 0.0 for col in columns}
    
    return filling_rates

def clean_prontuario_columns(con, table_name):
    """
    Clean prontuario columns by converting to integer where possible.
    For view_pacientes: Keep all records, just clean prontuario values
    For other tables: Discard records that cannot be converted and log unique discarded values.
    
    Args:
        con: DuckDB connection
        table_name: Name of the table being processed
        
    Returns:
        None (modifies the silver table in place)
    """
    # Find all columns that contain 'prontuario' in their name
    columns = con.execute(f"DESCRIBE silver.{table_name}").df()
    prontuario_columns = [col for col in columns['column_name'] if 'prontuario' in col.lower()]
    
    if not prontuario_columns:
        logger.debug(f"No prontuario columns found in {table_name}")
        return
    
    logger.info(f"Found {len(prontuario_columns)} prontuario columns in {table_name}: {prontuario_columns}")
    
    # Special handling for view_pacientes - we cannot discard records
    is_patient_table = table_name == 'view_pacientes'
    
    # Function to convert prontuario to integer
    def convert_to_int(prontuario_val):
        if pd.isna(prontuario_val) or prontuario_val is None:
            return None
        
        # Handle float values (e.g., 875831.0 -> 875831)
        if isinstance(prontuario_val, float):
            # If it's a whole number float, convert to int
            if prontuario_val.is_integer():
                result = int(prontuario_val)
                return None if result == 0 else result
            # If it has decimal places, this might be an error - log and return None
            logger.warning(f"Unexpected decimal value in prontuario: {prontuario_val}")
            return None
        
        # Convert to string first
        prontuario_str = str(prontuario_val).strip()
        
        # Handle pure numeric strings first (most common case)
        if prontuario_str.isdigit():
            result = int(prontuario_str)
            # Discard if result is 0
            return None if result == 0 else result
        
        # Handle formatted numbers with dots (e.g., "520.124" -> 520124)
        # Only process if it's not a simple float representation
        if '.' in prontuario_str and not prontuario_str.endswith('.0'):
            # Remove all dots and convert to integer
            try:
                # Remove dots and check if the result is numeric
                cleaned_str = prontuario_str.replace('.', '')
                if cleaned_str.isdigit():
                    result = int(cleaned_str)
                    # Discard if result is 0
                    return None if result == 0 else result
            except (ValueError, AttributeError):
                pass
        
        # If it's not a number, return None (will be discarded)
        return None
    
    if is_patient_table:
        # For view_pacientes: Keep all records, just clean the prontuario values
        # Get all data from the table
        all_data = con.execute(f"SELECT * FROM silver.{table_name}").df()
        
        # Apply the conversion to all prontuario columns
        for prontuario_col in prontuario_columns:
            logger.info(f"Cleaning prontuario column: {prontuario_col} in {table_name}")
            all_data[prontuario_col] = all_data[prontuario_col].apply(convert_to_int)
            logger.info(f"[{table_name}] {prontuario_col} cleaning completed (kept all records)")
        
        # Create a temporary table with the cleaned data
        temp_table = f"temp_{table_name}_cleaned"
        con.execute(f"DROP TABLE IF EXISTS {temp_table}")
        con.register(temp_table, all_data)
        con.execute(f"CREATE TABLE {temp_table} AS SELECT * FROM {temp_table}")
        con.unregister(temp_table)
        
        # Replace the original table
        con.execute(f"DROP TABLE silver.{table_name}")
        con.execute(f"CREATE TABLE silver.{table_name} AS SELECT * FROM {temp_table}")
        con.execute(f"DROP TABLE {temp_table}")
        
    else:
        # For other tables: Process all prontuario columns together
        # Get all data from the table
        all_data = con.execute(f"SELECT * FROM silver.{table_name}").df()
        original_count = len(all_data)
        
        # Apply the conversion to all prontuario columns
        for prontuario_col in prontuario_columns:
            logger.info(f"Cleaning prontuario column: {prontuario_col} in {table_name}")
            all_data[prontuario_col] = all_data[prontuario_col].apply(convert_to_int)
        
        # For non-patient tables, we want to keep records that have at least ONE valid prontuario
        # Create a mask for records that have at least one valid prontuario value
        valid_mask = all_data[prontuario_columns].notna().any(axis=1)
        valid_data = all_data[valid_mask]
        discarded_data = all_data[~valid_mask]
        
        valid_count = len(valid_data)
        discarded_count = len(discarded_data)
        
        logger.info(f"[{table_name}] Total records: {original_count} -> {valid_count} valid, {discarded_count} discarded")
        
        if discarded_count > 0:
            # Log some examples of discarded rows
            sample_discarded = discarded_data.head(5)
            logger.info(f"[{table_name}] Sample discarded rows (all prontuario columns invalid): {sample_discarded[prontuario_columns].to_dict('records')}")
        
        # Create a temporary table with only valid records
        temp_table = f"temp_{table_name}_cleaned"
        con.execute(f"DROP TABLE IF EXISTS {temp_table}")
        con.register(temp_table, valid_data)
        con.execute(f"CREATE TABLE {temp_table} AS SELECT * FROM {temp_table}")
        con.unregister(temp_table)
        
        # Replace the original table with only valid records
        con.execute(f"DROP TABLE silver.{table_name}")
        con.execute(f"CREATE TABLE silver.{table_name} AS SELECT * FROM {temp_table}")
        con.execute(f"DROP TABLE {temp_table}")
        
        logger.info(f"[{table_name}] prontuario cleaning completed (discarded records with all invalid prontuario values)")

def get_exception_columns(table_name):
    """
    Get list of columns that must be kept even if they have low filling rate.
    
    Args:
        table_name: Name of the table
        
    Returns:
        List of column names that are exceptions
    """
    exceptions = {
        'view_tratamentos': ['prontuario_doadora', 'prontuario_genitores'],
        'view_micromanipulacao_oocitos': ['ResultadoPGD']
    }
    return exceptions.get(table_name, [])

def generate_complete_cast_sql(con, table_name, null_rate_threshold=90.0):
    """
    Generate complete CAST SQL for columns in a table, excluding columns with null rate above threshold.
    Columns in the exception list are always kept regardless of filling rate.
    
    Args:
        con: DuckDB connection
        table_name: Name of the table
        null_rate_threshold: Maximum null rate (percentage) allowed - columns with null rate > this will be excluded (default: 90.0)
        
    Returns:
        Tuple of (cast_sql_string, excluded_columns_dict) where excluded_columns_dict maps
        column names to their null rates
    """
    columns = get_all_columns_from_bronze(con, table_name)
    if not columns:
        logger.error(f"No columns found for table {table_name}")
        return None, {}
    
    logger.info(f"Processing {len(columns)} columns for table {table_name}")
    
    # Get exception columns for this table
    exception_columns = get_exception_columns(table_name)
    if exception_columns:
        logger.info(f"[{table_name}] Exception columns (always kept): {exception_columns}")
    
    # Calculate filling rates for all columns
    logger.info(f"Calculating filling rates for columns in {table_name}...")
    filling_rates = calculate_column_filling_rates(con, table_name, columns)
    
    # Filter columns based on null rate threshold
    # Exclude columns with >90% null rate (i.e., keep columns with >=10% filling rate)
    # Exception: columns in exception list are always kept
    included_columns = []
    excluded_columns = {}
    exception_kept_columns = []
    
    for column in columns:
        filling_rate = filling_rates.get(column, 0.0)
        null_rate = 100.0 - filling_rate
        
        # Check if column is in exception list
        if column in exception_columns:
            included_columns.append(column)
            if null_rate > null_rate_threshold:
                exception_kept_columns.append((column, null_rate))
        elif null_rate > null_rate_threshold:
            excluded_columns[column] = null_rate
        else:
            included_columns.append(column)
    
    # Log exception columns kept despite low filling rate
    if exception_kept_columns:
        logger.info(f"[{table_name}] Kept {len(exception_kept_columns)} exception columns despite null rate > {null_rate_threshold}%:")
        for col, null_rate in sorted(exception_kept_columns, key=lambda x: x[1], reverse=True):
            filling_rate = 100.0 - null_rate
            logger.info(f"  - {col}: null_rate={null_rate:.2f}% (filling_rate={filling_rate:.2f}%) [EXCEPTION - always kept]")
    
    # Log excluded columns with their null rates
    if excluded_columns:
        logger.info(f"[{table_name}] Excluded {len(excluded_columns)} columns with null rate > {null_rate_threshold}%:")
        for col, null_rate in sorted(excluded_columns.items(), key=lambda x: x[1], reverse=True):
            filling_rate = 100.0 - null_rate
            logger.info(f"  - {col}: null_rate={null_rate:.2f}% (filling_rate={filling_rate:.2f}%)")
    else:
        logger.info(f"[{table_name}] All columns have null rate <= {null_rate_threshold}%")
    
    logger.info(f"[{table_name}] Including {len(included_columns)} columns (out of {len(columns)} total)")
    
    if not included_columns:
        logger.warning(f"No columns meet the null rate threshold (all have null rate > {null_rate_threshold}%) for {table_name}")
        return None, excluded_columns
    
    # Generate CAST clauses for included columns only
    cast_clauses = []
    for column in included_columns:
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
    
    return ',\n        '.join(cast_clauses), excluded_columns

def get_primary_key_for_table(table_name):
    """Get the primary key column for a given table"""
    primary_keys = {
        'view_micromanipulacao': 'codigo_ficha',
        'view_micromanipulacao_oocitos': 'id',
        'view_tratamentos': 'id',
        'view_medicamentos_prescricoes': 'id',
        'view_pacientes': 'codigo',
        'view_medicos': 'id',
        'view_unidades': 'id',
        'view_medicamentos': 'id',
        'view_procedimentos_financas': 'id',
        'view_orcamentos': 'id',
        'view_extrato_atendimentos_central': 'agendamento_id',
        'view_congelamentos_embrioes': 'id',
        'view_congelamentos_ovulos': 'id',
        'view_descongelamentos_embrioes': 'id',
        'view_descongelamentos_ovulos': 'id',
        'view_embrioes_congelados': 'id'
    }
    return primary_keys.get(table_name, 'id')  # Default to 'id' if not found

def deduplicate_and_format_sql(table, cast_sql):
    """Generate SQL with proper primary key-based deduplication"""
    primary_key = get_primary_key_for_table(table)
    logger.info(f'Using primary key "{primary_key}" for deduplication of table {table}')
    
    return f'''
    CREATE SCHEMA IF NOT EXISTS silver;
    CREATE OR REPLACE TABLE silver.{table} AS
    SELECT {cast_sql}
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY {primary_key} ORDER BY extraction_timestamp DESC) AS rn
        FROM bronze.{table}
    )
    WHERE rn = 1;
    '''

def feature_creation(con, table):
    """Apply table-specific feature engineering to the silver table."""
    if table == 'view_micromanipulacao_oocitos':
        # Add flag_embryoscope: 1 if InseminacaoOocito is not null, 0 otherwise
        logger.info(f"Adding flag_embryoscope to silver.{table}")
        con.execute(f"""
            CREATE OR REPLACE TABLE silver.{table} AS
            SELECT *,
                   CASE WHEN InseminacaoOocito IS NOT NULL THEN 1 ELSE 0 END AS flag_embryoscope
            FROM silver.{table}
        """)
        logger.info(f"flag_embryoscope added to silver.{table}")
        
        # Add embryo_number: row_number per id_micromanipulacao ordered by id (for all rows)
        logger.info(f"Adding embryo_number to silver.{table} (for all rows)")
        con.execute(f"""
            CREATE OR REPLACE TABLE silver.{table} AS
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY id_micromanipulacao ORDER BY id) AS embryo_number
            FROM silver.{table}
        """)
        logger.info(f"embryo_number added to silver.{table}")
    
    elif table == 'view_tratamentos':
        # Fix gaps in tentativa column by creating sequential numbers per patient
        logger.info(f"Fixing gaps in tentativa column for silver.{table}")
        
        # Get all column names with their ordinal positions
        columns_result = con.execute(f"""
            SELECT column_name, ordinal_position
            FROM information_schema.columns 
            WHERE table_schema = 'silver' AND table_name = '{table}'
            ORDER BY ordinal_position
        """).df()
        
        # Find the position of tentativa column
        tentativa_position = columns_result[columns_result['column_name'] == 'tentativa']['ordinal_position'].iloc[0]
        
        # Build the SELECT clause with tentativa in its original position
        select_parts = []
        for _, row in columns_result.iterrows():
            if row['column_name'] == 'tentativa':
                # Insert the fixed tentativa value in its original position
                select_parts.append(f"""
                    CASE 
                        WHEN prontuario IS NOT NULL THEN 
                            CAST(ROW_NUMBER() OVER (PARTITION BY prontuario ORDER BY id) AS VARCHAR)
                        ELSE tentativa 
                    END AS tentativa""")
            else:
                select_parts.append(row['column_name'])
        
        columns_sql = ',\n                '.join(select_parts)
        
        # Update the tentativa column with the fixed sequential values
        logger.info(f"Updating tentativa column with fixed sequential values for silver.{table}")
        con.execute(f"""
            CREATE OR REPLACE TABLE silver.{table} AS
            SELECT 
                {columns_sql}
            FROM silver.{table}
        """)
        logger.info(f"tentativa column updated with fixed sequential values for silver.{table}")
    
    # Add more table-specific features here as needed

def main():
    logger.info('Starting complete try_strptime silver loader (EXCLUDING COLUMNS WITH >90% NULL RATE)')
    logger.info(f'Database path: {db_path}')
    
    with duckdb.connect(db_path) as con:
        # Get list of bronze tables
        bronze_tables = con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'bronze'").df()['table_name'].tolist()
        logger.info(f'Found {len(bronze_tables)} bronze tables: {bronze_tables}')
        
        for table in bronze_tables:
            logger.info(f'Processing {table}')
            try:
                # Generate complete CAST SQL, excluding columns with >90% null rate
                cast_sql, excluded_columns = generate_complete_cast_sql(con, table, null_rate_threshold=90.0)
                if cast_sql is None:
                    logger.error(f'Failed to generate CAST SQL for {table}')
                    continue
                
                sql = deduplicate_and_format_sql(table, cast_sql)
                logger.debug(f'Generated SQL for {table} (first 500 chars): {sql[:500]}...')
                
                con.execute(sql)
                logger.info(f'Created/updated silver.{table}')
                
                # Feature creation session (table-specific)
                feature_creation(con, table)
                
                # Clean prontuario columns (convert to integer, discard invalid values)
                clean_prontuario_columns(con, table)
                
                # Verify column count
                bronze_cols = len(get_all_columns_from_bronze(con, table))
                silver_cols = con.execute(f"SELECT COUNT(*) as col_count FROM information_schema.columns WHERE table_schema = 'silver' AND table_name = '{table}'").df()['col_count'].iloc[0]
                logger.info(f'Column count: Bronze={bronze_cols}, Silver={silver_cols}')
                
                if bronze_cols != silver_cols:
                    logger.info(f'Column count difference for {table}: Bronze={bronze_cols}, Silver={silver_cols} (expected due to null rate filtering)')
                
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
    
    logger.info('Complete try_strptime silver loader finished successfully')

if __name__ == '__main__':
    main() 