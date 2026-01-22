"""
Transformation utilities for Bronze to Silver layer
Handles column type transformations and SQL generation
"""

import logging
import yaml
import os

logger = logging.getLogger(__name__)

# Load column configuration
CONFIG_PATH = os.path.join(os.path.dirname(__file__), 'column_config.yml')
with open(CONFIG_PATH, 'r') as f:
    COLUMN_CONFIG = yaml.safe_load(f)


def get_column_transformation(column_name, column_type, sample_data=None):
    """
    Determine the appropriate transformation for a column based on its name, type, and sample data
    
    Args:
        column_name: Name of the column
        column_type: Type of the column in bronze layer
        sample_data: Optional sample data for type inference
        
    Returns:
        SQL transformation string
    """
    
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
    if column_name.lower() in [col.lower() for col in COLUMN_CONFIG['date_columns']]:
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
    if column_name.lower() in [col.lower() for col in COLUMN_CONFIG['time_columns']]:
        return f"""
        CASE 
            WHEN {column_name} IS NULL THEN NULL
            WHEN {column_name} IN ('NULL', 'null', '') THEN NULL
            WHEN try_strptime({column_name}, '%H:%M') IS NULL THEN NULL
            ELSE try_strptime({column_name}, '%H:%M')
        END AS {column_name}"""
    
    # Integer columns (except prontuario which needs special handling)
    if column_name.lower() in [col.lower() for col in COLUMN_CONFIG['int_columns']]:
        # Special handling for prontuario columns - keep as VARCHAR initially
        if column_name.lower() == 'prontuario':
            return f"CAST({column_name} AS VARCHAR) AS {column_name}"
        else:
            return f"try_cast({column_name} AS INTEGER) AS {column_name}"
    
    # Float columns
    if column_name.lower() in [col.lower() for col in COLUMN_CONFIG['float_columns']]:
        return f"try_cast({column_name} AS DOUBLE) AS {column_name}"
    
    # Special handling for valor column (currency)
    if column_name.lower() == 'valor':
        return f"try_cast(REPLACE(REPLACE({column_name}, '.', ''), ',', '.') AS DOUBLE) AS {column_name}"
    
    # Special handling for peso_paciente, altura_paciente, peso_conjuge, altura_conjuge, peso, and altura
    if column_name.lower() in [col.lower() for col in COLUMN_CONFIG['special_columns']['peso_altura']]:
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
    """
    Get all column names from a bronze table
    
    Args:
        con: DuckDB connection
        table_name: Name of the bronze table
        
    Returns:
        List of column names
    """
    try:
        result = con.execute(f"DESCRIBE bronze.{table_name}").df()
        return result['column_name'].tolist()
    except Exception as e:
        logger.error(f"Error getting columns for {table_name}: {e}")
        return []


def generate_complete_cast_sql(con, table_name):
    """
    Generate complete CAST SQL for all columns in a table.
    No filtering by fill rate - all columns are processed.
    
    Args:
        con: DuckDB connection
        table_name: Name of the table
        
    Returns:
        SQL string with all column transformations
    """
    columns = get_all_columns_from_bronze(con, table_name)
    if not columns:
        logger.error(f"No columns found for table {table_name}")
        return None
    
    logger.info(f"Processing {len(columns)} columns for table {table_name}")
    
    # Generate CAST clauses for all columns
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


def get_primary_key_for_table(table_name):
    """
    Get the primary key column for a given table
    
    Args:
        table_name: Name of the table
        
    Returns:
        Primary key column name
    """
    return COLUMN_CONFIG['primary_keys'].get(table_name, 'id')


def deduplicate_and_format_sql(table, cast_sql):
    """
    Generate SQL with proper primary key-based deduplication
    
    Args:
        table: Table name
        cast_sql: SQL string with column transformations
        
    Returns:
        Complete SQL for creating silver table
    """
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
