#!/usr/bin/env python3
"""
Diario Vendas Silver Transformation - Simplified Version
Transforms bronze.diario_vendas to silver layer with:
- Proper data type casting
- Remove completely blank lines
- Complex prontuario matching logic (using clinisys_all.silver.view_pacientes)
"""

import logging
import pandas as pd
import duckdb
from datetime import datetime
import os

# Setup logging
LOGS_DIR = os.path.join(os.path.dirname(__file__), 'logs')
os.makedirs(LOGS_DIR, exist_ok=True)
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
script_name = os.path.splitext(os.path.basename(__file__))[0]
LOG_PATH = os.path.join(LOGS_DIR, f'{script_name}_{timestamp}.log')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.FileHandler(LOG_PATH),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration
DUCKDB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'database', 'huntington_data_lake.duckdb')
BRONZE_TABLE = 'diario_vendas'
SILVER_TABLE = 'diario_vendas'

def get_duckdb_connection():
    """Create DuckDB connection"""
    try:
        logger.info(f"Attempting to connect to DuckDB at: {DUCKDB_PATH}")
        con = duckdb.connect(DUCKDB_PATH)
        logger.info("DuckDB connection successful")
        return con
    except Exception as e:
        logger.error(f"Failed to connect to DuckDB: {e}")
        raise

def transform_data_types(df):
    """Transform data types for silver layer using vectorized operations"""
    logger.info("Transforming data types...")
    
    # Create a copy to avoid modifying original
    df_transformed = df.copy()
    
    # Date columns - use pandas to_datetime for vectorized operation
    df_transformed['DT Emissao'] = pd.to_datetime(df_transformed['DT Emissao'], errors='coerce')
    
    # Numeric columns - use pandas to_numeric for vectorized operations
    # For nullable integers (Cliente)
    df_transformed['Cliente'] = pd.to_numeric(df_transformed['Cliente'], errors='coerce').astype('Int64')
    
    # For non-nullable integers
    df_transformed['Loja'] = pd.to_numeric(df_transformed['Loja'], errors='coerce').fillna(0).astype(int)
    df_transformed['Numero'] = pd.to_numeric(df_transformed['Numero'], errors='coerce').fillna(0).astype(int)
    df_transformed['Ciclos'] = pd.to_numeric(df_transformed['Ciclos'], errors='coerce').fillna(0).astype(int)
    df_transformed['Qnt Cons.'] = pd.to_numeric(df_transformed['Qnt Cons.'], errors='coerce').fillna(0).astype(int)
    df_transformed['Mês'] = pd.to_numeric(df_transformed['Mês'], errors='coerce').fillna(0).astype(int)
    df_transformed['Ano'] = pd.to_numeric(df_transformed['Ano'], errors='coerce').fillna(0).astype(int)
    
    # For decimal columns
    df_transformed['Qntd.'] = pd.to_numeric(df_transformed['Qntd.'], errors='coerce').fillna(0.0)
    df_transformed['Valor Unitario'] = pd.to_numeric(df_transformed['Valor Unitario'], errors='coerce').fillna(0.0)
    df_transformed['Valor Mercadoria'] = pd.to_numeric(df_transformed['Valor Mercadoria'], errors='coerce').fillna(0.0)
    df_transformed['Total'] = pd.to_numeric(df_transformed['Total'], errors='coerce').fillna(0.0)
    df_transformed['Custo'] = pd.to_numeric(df_transformed['Custo'], errors='coerce').fillna(0.0)
    df_transformed['Custo Unit'] = pd.to_numeric(df_transformed['Custo Unit'], errors='coerce').fillna(0.0)
    df_transformed['Desconto'] = pd.to_numeric(df_transformed['Desconto'], errors='coerce').fillna(0.0)
    
    logger.info("Data type transformation completed")
    return df_transformed

def create_silver_table(con, df):
    """Create silver table with proper schema"""
    logger.info(f"Creating silver table: {SILVER_TABLE}")
    
    # Create silver schema if it doesn't exist
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    
    # Define column types based on transformed data
    column_definitions = [
        '"Cliente" INTEGER',
        '"Loja" INTEGER',
        '"Nome" VARCHAR',
        '"Nom Paciente" VARCHAR',
        '"DT Emissao" TIMESTAMP',
        '"Tipo da nota" VARCHAR',
        '"Numero" INTEGER',
        '"Serie Docto." VARCHAR',
        '"NF Eletr." VARCHAR',
        '"Vend. 1" VARCHAR',
        '"Médico" VARCHAR',
        '"Cliente.1" VARCHAR',
        '"Opr " VARCHAR',
        '"Operador" VARCHAR',
        '"Produto" VARCHAR',
        '"Descricao" VARCHAR',
        '"Qntd." DOUBLE',
        '"Valor Unitario" DOUBLE',
        '"Valor Mercadoria" DOUBLE',
        '"Total" DOUBLE',
        '"Custo" DOUBLE',
        '"Custo Unit" DOUBLE',
        '"Desconto" DOUBLE',
        '"Unidade" VARCHAR',
        '"Mês" INTEGER',
        '"Ano" INTEGER',
        '"Cta-Ctbl" VARCHAR',
        '"Cta-Ctbl Eugin" VARCHAR',
        '"Interno/Externo" VARCHAR',
        '"Descrição Gerencial" VARCHAR',
        '"Descrição Mapping Actividad" VARCHAR',
        '"Ciclos" INTEGER',
        '"Qnt Cons." INTEGER',
        'prontuario INTEGER',
        'line_number INTEGER',
        'extraction_timestamp VARCHAR',
        'file_name VARCHAR'
    ]
    
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS silver.{SILVER_TABLE} (
        {', '.join(column_definitions)}
    )
    """
    
    con.execute(create_table_sql)
    logger.info(f"Table silver.{SILVER_TABLE} created/verified")

def update_prontuario_column(con):
    """Update prontuario column using complex matching logic with clinisys_all.silver.view_pacientes"""
    logger.info("Updating prontuario column using complex matching logic...")
    
    # Attach clinisys_all database
    clinisys_db_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'database', 'clinisys_all.duckdb')
    logger.info(f"Attaching clinisys_all database from: {clinisys_db_path}")
    con.execute(f"ATTACH '{clinisys_db_path}' AS clinisys_all")
    logger.info("clinisys_all database attached successfully")
    
    # Complex matching SQL query
    update_sql = """
    WITH 
    -- CTE 1: Extract and process diario_vendas data
    diario_extract AS (
        SELECT DISTINCT 
            "Cliente", 
            CASE 
                WHEN "Nome" IS NOT NULL THEN TRIM(LOWER(SPLIT_PART("Nome", ' ', 1)))
                ELSE NULL 
            END as nome_first,
            CASE 
                WHEN "Nom Paciente" IS NOT NULL THEN TRIM(LOWER(SPLIT_PART("Nom Paciente", ' ', 1)))
                ELSE NULL 
            END as nom_paciente_first
        FROM silver.diario_vendas
        WHERE "Cliente" IS NOT NULL
    ),

    -- CTE 2: Cliente ↔ prontuario (main/codigo)
    matches_1 AS (
        SELECT d.*,
               p.codigo as prontuario, lower(trim(p.esposa_nome)) esposa_nome, LOWER(TRIM(p.marido_nome)) marido_nome, p.unidade_origem,
               'prontuario_main' as match_type
        FROM diario_extract d
        INNER JOIN clinisys_all.silver.view_pacientes p 
            ON d."Cliente" = p.codigo
    ),

    -- CTE 3: Cliente ↔ prontuario_esposa
    matches_2 AS (
        SELECT d.*, 
               p.codigo as prontuario, lower(trim(p.esposa_nome)) esposa_nome, LOWER(TRIM(p.marido_nome)) marido_nome, p.unidade_origem,
               'prontuario_esposa' as match_type
        FROM diario_extract d
        INNER JOIN clinisys_all.silver.view_pacientes p 
            ON d."Cliente" = p.prontuario_esposa
    ),

    -- CTE 4: Cliente ↔ prontuario_marido
    matches_3 AS (
        SELECT d.*,
              p.codigo as prontuario, lower(trim(p.esposa_nome)) esposa_nome, LOWER(TRIM(p.marido_nome)) marido_nome, p.unidade_origem,
               'prontuario_marido' as match_type
        FROM diario_extract d
        INNER JOIN clinisys_all.silver.view_pacientes p 
            ON d."Cliente" = p.prontuario_marido
    ),

    -- CTE 5: Cliente ↔ prontuario_responsavel1
    matches_4 AS (
        SELECT d.*,
              p.codigo as prontuario, lower(trim(p.esposa_nome)) esposa_nome, LOWER(TRIM(p.marido_nome)) marido_nome, p.unidade_origem,
               'prontuario_responsavel1' as match_type
        FROM diario_extract d
        INNER JOIN clinisys_all.silver.view_pacientes p 
            ON d."Cliente" = p.prontuario_responsavel1
    ),

    -- CTE 6: Cliente ↔ prontuario_responsavel2
    matches_5 AS (
        SELECT d.*,
               p.codigo as prontuario, lower(trim(p.esposa_nome)) esposa_nome, LOWER(TRIM(p.marido_nome)) marido_nome, p.unidade_origem,
               'prontuario_responsavel2' as match_type
        FROM diario_extract d
        INNER JOIN clinisys_all.silver.view_pacientes p 
            ON d."Cliente" = p.prontuario_responsavel2
    ),

    -- CTE 7: Cliente ↔ prontuario_esposa_pel
    matches_6 AS (
        SELECT d.*,
               p.codigo as prontuario, lower(trim(p.esposa_nome)) esposa_nome, LOWER(TRIM(p.marido_nome)) marido_nome, p.unidade_origem,
               'prontuario_esposa_pel' as match_type
        FROM diario_extract d
        INNER JOIN clinisys_all.silver.view_pacientes p 
            ON d."Cliente" = p.prontuario_esposa_pel
    ),

    -- CTE 8: Cliente ↔ prontuario_marido_pel
    matches_7 AS (
        SELECT d.*,
               p.codigo as prontuario, lower(trim(p.esposa_nome)) esposa_nome, LOWER(TRIM(p.marido_nome)) marido_nome, p.unidade_origem,
               'prontuario_marido_pel' as match_type
        FROM diario_extract d
        INNER JOIN clinisys_all.silver.view_pacientes p 
            ON d."Cliente" = p.prontuario_marido_pel
    ),
    -- CTE 9: Cliente ↔ prontuario_esposa_pc
    matches_8 AS (
        SELECT d.*,
               p.codigo as prontuario, lower(trim(p.esposa_nome)) esposa_nome, LOWER(TRIM(p.marido_nome)) marido_nome, p.unidade_origem,
               'prontuario_esposa_pc' as match_type
        FROM diario_extract d
        INNER JOIN clinisys_all.silver.view_pacientes p 
            ON d."Cliente" = p.prontuario_esposa_pc
    ),
    -- CTE 10: Cliente ↔ prontuario_marido_pc
    matches_9 AS (
        SELECT d.*,
               p.codigo as prontuario, lower(trim(p.esposa_nome)) esposa_nome, LOWER(TRIM(p.marido_nome)) marido_nome, p.unidade_origem,
               'prontuario_marido_pc' as match_type
        FROM diario_extract d
        INNER JOIN clinisys_all.silver.view_pacientes p 
            ON d."Cliente" = p.prontuario_marido_pc
    ),
    -- CTE 11: Cliente ↔ prontuario_responsavel1_pc
    matches_10 AS (
        SELECT d.*,
               p.codigo as prontuario, lower(trim(p.esposa_nome)) esposa_nome, LOWER(TRIM(p.marido_nome)) marido_nome, p.unidade_origem,
               'prontuario_responsavel1_pc' as match_type
        FROM diario_extract d
        INNER JOIN clinisys_all.silver.view_pacientes p 
            ON d."Cliente" = p.prontuario_responsavel1_pc
    ),
    -- CTE 12: Cliente ↔ prontuario_responsavel2_pc
    matches_11 AS (
        SELECT d.*,
               p.codigo as prontuario, lower(trim(p.esposa_nome)) esposa_nome, LOWER(TRIM(p.marido_nome)) marido_nome, p.unidade_origem,
               'prontuario_responsavel2_pc' as match_type
        FROM diario_extract d
        INNER JOIN clinisys_all.silver.view_pacientes p 
            ON d."Cliente" = p.prontuario_responsavel2_pc
    ),
    -- CTE 13: Cliente ↔ prontuario_esposa_fc
    matches_12 AS (
        SELECT d.*,
               p.codigo as prontuario, lower(trim(p.esposa_nome)) esposa_nome, LOWER(TRIM(p.marido_nome)) marido_nome, p.unidade_origem,
               'prontuario_esposa_fc' as match_type
        FROM diario_extract d
        INNER JOIN clinisys_all.silver.view_pacientes p 
            ON d."Cliente" = p.prontuario_esposa_fc
    ),
    -- CTE 14: Cliente ↔ prontuario_marido_fc
    matches_13 AS (
        SELECT d.*,
               p.codigo as prontuario, lower(trim(p.esposa_nome)) esposa_nome, LOWER(TRIM(p.marido_nome)) marido_nome, p.unidade_origem,
               'prontuario_marido_fc' as match_type
        FROM diario_extract d
        INNER JOIN clinisys_all.silver.view_pacientes p 
            ON d."Cliente" = p.prontuario_marido_fc
    ),
    -- CTE 15: Cliente ↔ prontuario_esposa_ba
    matches_14 AS (
        SELECT d.*,
               p.codigo as prontuario, lower(trim(p.esposa_nome)) esposa_nome, LOWER(TRIM(p.marido_nome)) marido_nome, p.unidade_origem,
               'prontuario_esposa_ba' as match_type
        FROM diario_extract d
        INNER JOIN clinisys_all.silver.view_pacientes p 
            ON d."Cliente" = p.prontuario_esposa_ba
    ),
    -- CTE 16: Cliente ↔ prontuario_marido_ba
    matches_15 AS (
        SELECT d.*,
               p.codigo as prontuario, lower(trim(p.esposa_nome)) esposa_nome, LOWER(TRIM(p.marido_nome)) marido_nome, p.unidade_origem,
               'prontuario_marido_ba' as match_type
        FROM diario_extract d
        INNER JOIN clinisys_all.silver.view_pacientes p 
            ON d."Cliente" = p.prontuario_marido_ba
    ),

    -- CTE 17: UNION matches
    all_matches AS (
        SELECT * FROM matches_1
        UNION
        SELECT * FROM matches_2
        UNION
        SELECT * FROM matches_3
        UNION 
        SELECT * FROM matches_4
        UNION
        SELECT * FROM matches_5
        UNION
        SELECT * FROM matches_6
        UNION
        SELECT * FROM matches_7
        UNION
        SELECT * FROM matches_8
        UNION
        SELECT * FROM matches_9
        UNION
        SELECT * FROM matches_10
        UNION
        SELECT * FROM matches_11
        UNION
        SELECT * FROM matches_12
        UNION
        SELECT * FROM matches_13
        UNION
        SELECT * FROM matches_14
        UNION
        SELECT * FROM matches_15
    ),
    -- CTE 18: Apply name matching criteria and rank
    ranked_matches AS (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY "Cliente" 
                   ORDER BY 
                       CASE 
                           WHEN match_type = 'prontuario_main' THEN 1
                           WHEN match_type = 'prontuario_esposa' AND nome_first = TRIM(LOWER(esposa_nome)) THEN 2
                           WHEN match_type = 'prontuario_marido' AND nome_first = TRIM(LOWER(marido_nome)) THEN 3
                           WHEN match_type = 'prontuario_esposa' AND nom_paciente_first = TRIM(LOWER(esposa_nome)) THEN 4
                           WHEN match_type = 'prontuario_marido' AND nom_paciente_first = TRIM(LOWER(marido_nome)) THEN 5
                           ELSE 6
                       END
               ) 
               as rn
        FROM all_matches
        WHERE nome_first = TRIM(LOWER(esposa_nome)) OR nome_first = TRIM(LOWER(marido_nome)) OR nom_paciente_first = TRIM(LOWER(esposa_nome))  OR nom_paciente_first = TRIM(LOWER(marido_nome))
    ),

    -- CTE 19: Select best match per Cliente (lowest rn value)
    best_matches AS (
        SELECT * 
        FROM ranked_matches rm1
        WHERE rn = (
            SELECT MIN(rn) 
            FROM ranked_matches rm2 
            WHERE rm2."Cliente" = rm1."Cliente"
        )
    )

    -- Update prontuario column
    UPDATE silver.diario_vendas 
    SET prontuario = COALESCE(bm.prontuario, -1)
    FROM best_matches bm 
    WHERE silver.diario_vendas."Cliente" = bm."Cliente"
    """
    
    try:
        con.execute(update_sql)
        logger.info("Prontuario column updated successfully with complex matching logic")
        
        # Get statistics on the update
        result = con.execute("""
            SELECT 
                COUNT(*) as total_rows,
                COUNT(CASE WHEN prontuario != -1 THEN 1 END) as matched_rows,
                COUNT(CASE WHEN prontuario = -1 THEN 1 END) as unmatched_rows
            FROM silver.diario_vendas
        """).fetchone()
        
        logger.info(f"Prontuario matching results:")
        logger.info(f"  Total rows: {result[0]:,}")
        logger.info(f"  Matched rows: {result[1]:,}")
        logger.info(f"  Unmatched rows: {result[2]:,}")
        logger.info(f"  Match rate: {(result[1]/result[0]*100):.2f}%")
        
    except Exception as e:
        logger.error(f"Error updating prontuario column: {e}")
        raise

def process_bronze_to_silver(con):
    """Process bronze table and transform to silver"""
    logger.info(f"Processing {BRONZE_TABLE} to {SILVER_TABLE}")
    
    try:
        # Get data from bronze (full table)
        logger.info("Reading full data from bronze layer...")
        df_bronze = con.execute(f"SELECT * FROM bronze.{BRONZE_TABLE}").df()
        logger.info(f"Read {len(df_bronze)} rows from bronze.{BRONZE_TABLE}")
        
        if len(df_bronze) == 0:
            logger.warning("No data found in bronze table")
            return 0
        
        # Remove completely blank lines and rows with majority of columns blank
        logger.info("Removing completely blank lines and rows with majority of columns blank...")
        
        # First remove completely blank lines
        df_bronze_clean = df_bronze.dropna(how='all')
        logger.info(f"Removed {len(df_bronze) - len(df_bronze_clean)} completely blank lines")
        
        # Then remove rows with majority of columns blank (>50% nulls)
        # Count nulls per row (excluding metadata columns)
        business_columns = [col for col in df_bronze_clean.columns 
                          if col not in ['line_number', 'extraction_timestamp', 'file_name']]
        
        # Calculate null percentage per row
        null_counts = df_bronze_clean[business_columns].isnull().sum(axis=1)
        null_percentages = (null_counts / len(business_columns)) * 100
        
        # Remove rows with >50% nulls
        df_bronze_clean = df_bronze_clean[null_percentages <= 50]
        logger.info(f"Removed {len(df_bronze.dropna(how='all')) - len(df_bronze_clean)} rows with majority of columns blank")
        logger.info(f"Total rows after cleaning: {len(df_bronze_clean):,}")
        
        # Transform data types
        df_transformed = transform_data_types(df_bronze_clean)
        
        # Initialize prontuario column with -1 (unmatched)
        logger.info("Initializing prontuario column with -1 (unmatched)...")
        df_transformed['prontuario'] = -1
        
        # Drop existing silver table and recreate
        logger.info("Dropping existing silver table...")
        con.execute(f"DROP TABLE IF EXISTS silver.{SILVER_TABLE}")
        
        logger.info(f"Inserting {len(df_transformed)} rows to silver layer")
        
        # Create silver table
        create_silver_table(con, df_transformed)
        
        # Register DataFrame as temporary table
        con.register('temp_new_rows', df_transformed)
        
        # Insert new rows with explicit column casting
        insert_sql = f"""
        INSERT INTO silver.{SILVER_TABLE} 
        SELECT 
            CASE WHEN "Cliente" IS NULL THEN NULL ELSE CAST("Cliente" AS INTEGER) END as "Cliente",
            CAST("Loja" AS INTEGER) as "Loja",
            CAST("Nome" AS VARCHAR) as "Nome",
            CAST("Nom Paciente" AS VARCHAR) as "Nom Paciente",
            CAST("DT Emissao" AS TIMESTAMP) as "DT Emissao",
            CAST("Tipo da nota" AS VARCHAR) as "Tipo da nota",
            CAST("Numero" AS INTEGER) as "Numero",
            CAST("Serie Docto." AS VARCHAR) as "Serie Docto.",
            CAST("NF Eletr." AS VARCHAR) as "NF Eletr.",
            CAST("Vend. 1" AS VARCHAR) as "Vend. 1",
            CAST("Médico" AS VARCHAR) as "Médico",
            CAST("Cliente.1" AS VARCHAR) as "Cliente.1",
            CAST("Opr " AS VARCHAR) as "Opr ",
            CAST("Operador" AS VARCHAR) as "Operador",
            CAST("Produto" AS VARCHAR) as "Produto",
            CAST("Descricao" AS VARCHAR) as "Descricao",
            CAST("Qntd." AS DOUBLE) as "Qntd.",
            CAST("Valor Unitario" AS DOUBLE) as "Valor Unitario",
            CAST("Valor Mercadoria" AS DOUBLE) as "Valor Mercadoria",
            CAST("Total" AS DOUBLE) as "Total",
            CAST("Custo" AS DOUBLE) as "Custo",
            CAST("Custo Unit" AS DOUBLE) as "Custo Unit",
            CAST("Desconto" AS DOUBLE) as "Desconto",
            CAST("Unidade" AS VARCHAR) as "Unidade",
            CAST("Mês" AS INTEGER) as "Mês",
            CAST("Ano" AS INTEGER) as "Ano",
            CAST("Cta-Ctbl" AS VARCHAR) as "Cta-Ctbl",
            CAST("Cta-Ctbl Eugin" AS VARCHAR) as "Cta-Ctbl Eugin",
            CAST("Interno/Externo" AS VARCHAR) as "Interno/Externo",
            CAST("Descrição Gerencial" AS VARCHAR) as "Descrição Gerencial",
            CAST("Descrição Mapping Actividad" AS VARCHAR) as "Descrição Mapping Actividad",
            CAST("Ciclos" AS INTEGER) as "Ciclos",
            CAST("Qnt Cons." AS INTEGER) as "Qnt Cons.",
            CAST(prontuario AS INTEGER) as prontuario,
            CAST(line_number AS INTEGER) as line_number,
            CAST(extraction_timestamp AS VARCHAR) as extraction_timestamp,
            CAST(file_name AS VARCHAR) as file_name
        FROM temp_new_rows
        """
        
        con.execute(insert_sql)
        
        # Clean up temporary table
        con.execute("DROP VIEW IF EXISTS temp_new_rows")
        
        logger.info(f"Successfully inserted {len(df_transformed)} rows to silver.{SILVER_TABLE}")
        return len(df_transformed)
        
    except Exception as e:
        logger.error(f"Error processing bronze to silver: {e}")
        raise

def main():
    """Main function to transform diario_vendas to silver"""
    logger.info("Starting Diario Vendas silver transformation (with complex prontuario matching)")
    logger.info(f"DuckDB path: {DUCKDB_PATH}")
    
    try:
        # Create DuckDB connection
        logger.info("Creating DuckDB connection...")
        con = get_duckdb_connection()
        logger.info("DuckDB connection created successfully")
        
        # Process bronze to silver
        new_rows = process_bronze_to_silver(con)
        
        # Update prontuario column with complex matching logic
        update_prontuario_column(con)
        
        # Get final table statistics
        result = con.execute(f'SELECT COUNT(*) FROM silver.{SILVER_TABLE}').fetchone()
        total_rows = result[0] if result else 0
        
        # Final summary
        logger.info("=" * 50)
        logger.info("SILVER TRANSFORMATION SUMMARY")
        logger.info("=" * 50)
        logger.info(f"New rows inserted: {new_rows}")
        logger.info(f"Total rows in silver.{SILVER_TABLE}: {total_rows:,}")
        logger.info("=" * 50)
        
        # Close connection
        con.close()
        logger.info("Diario Vendas silver transformation completed")
        
    except Exception as e:
        logger.error(f"Error in main function: {e}")
        raise

if __name__ == "__main__":
    main() 