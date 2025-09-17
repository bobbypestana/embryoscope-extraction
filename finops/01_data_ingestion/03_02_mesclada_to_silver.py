#!/usr/bin/env python3
"""
Mesclada Vendas Silver Transformation - Simplified Version
Transforms bronze.mesclada_vendas to silver layer with:
- Column mapping from mesclada format to diario format
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
BRONZE_TABLE = 'mesclada_vendas'
SILVER_TABLE = 'mesclada_vendas'

# Column mapping from mesclada to diario format
COLUMN_MAPPING = {
    # Mesclada column -> Diario column
    'Cod Cli': 'Cliente.1',
    'Filial': 'Loja', 
    'Nom Cliente': 'Nome',
    'Paciente': 'Nom Paciente',
    'Data': 'DT Emissao',
    'TES': 'Tipo da nota',
    'Doc': 'Numero',
    'Serie': 'Serie Docto.',
    'NFSe': 'NF Eletr.',
    'Medico': 'Cod. Medicco',
    'Nom Medico': 'Médico',
    'MedSof Cli': 'Cliente',
    'MedSof Pac.': 'Paciente',
    'Produto': 'Produto',
    'Descr.Prod.': 'Descricao',
    'Qtde': 'Qntd.',
    'Vlr Venda': 'Total',
    'Vlr Desconto': 'Desconto',
    'Cta.Contab.': 'Cta-Ctbl',
    'Descrição Mapping Actividad': 'Descrição Mapping Actividad',
    'Descrição Gerencial': 'Descrição Gerencial',
    'Ciclos': 'Ciclos',
    # Additional mesclada columns (keeping their current names)
    'Grp': 'Grp',
    'Descr.TES': 'Descr.TES',
    'Lead Time': 'Lead Time',
    'Data do Ciclo': 'Data do Ciclo',
    'Fez Ciclo?': 'Fez Ciclo?'
}

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

def map_columns(df):
    """Map mesclada columns to diario format"""
    logger.info("Mapping mesclada columns to diario format...")
    
    # Create a copy to avoid modifying original
    df_mapped = df.copy()
    
    # Rename columns according to mapping
    df_mapped = df_mapped.rename(columns=COLUMN_MAPPING)
    
    # Add missing columns that exist in diario but not in mesclada
    missing_columns = {
        'Valor Mercadoria': 0.0,
        'Total': 0.0,
        'Custo': 0.0,
        'Custo Unit': 0.0,
        'Unidade': '',
        'Mês': 0,
        'Ano': 0,
        'Cta-Ctbl Eugin': '',
        'Interno/Externo': '',
        'Qnt Cons.': 0,
        'Operador': ''
    }
    
    for col, default_value in missing_columns.items():
        if col not in df_mapped.columns:
            df_mapped[col] = default_value
            logger.info(f"Added missing column '{col}' with default value: {default_value}")
    
    logger.info(f"Column mapping completed. Final columns: {list(df_mapped.columns)}")
    return df_mapped

def map_unidade(df):
    """Map Grp and Filial to Unidade using the unidade_map"""
    logger.info("Mapping Grp and Filial to Unidade...")
    
    # Unidade mapping based on Grp and Filial - using actual data format
    unidade_map = {
        ('1', '10101'): 'Ibirapuera',
        ('1', '10150'): 'Ibirapuera',
        ('1', '10155'): 'Vila Mariana', 
        ('1', '10104'): 'Vila Mariana',
        ('1', '10106'): 'Vila Mariana',
        ('3', '30101'): 'Campinas',
        ('6', '60101'): 'Santa Joana',
        ('5', '101'): 'Belo Horizonte',
        ('7', '10101'): 'Salvador - Cenafert',
        ('7', '20101'): 'Salvador - Cenafert',
        ('7', '30101'): 'FIV Brasilia'
    }
    
    df['Unidade'] = df.apply(lambda row: unidade_map.get((row['Grp'], row['Loja']), ''), axis=1)
    
    # Log mapping statistics
    unidade_counts = df['Unidade'].value_counts()
    logger.info(f"Unidade mapping completed. Distribution:")
    for unidade, count in unidade_counts.items():
        if unidade:  # Only log non-empty values
            logger.info(f"  {unidade}: {count:,} records")
    
    # Log unmapped records
    unmapped_count = (df['Unidade'] == '').sum()
    if unmapped_count > 0:
        logger.warning(f"  Unmapped records: {unmapped_count:,}")
        # Show some examples of unmapped records
        unmapped_examples = df[df['Unidade'] == ''][['Grp', 'Loja']].drop_duplicates().head(5)
        logger.warning(f"  Examples of unmapped Grp/Loja combinations:")
        for _, row in unmapped_examples.iterrows():
            logger.warning(f"    Grp: {row['Grp']}, Loja: {row['Loja']}")
    
    return df

def transform_data_types(df):
    """Transform data types for silver layer using vectorized operations"""
    logger.info("Transforming data types...")
    
    # Create a copy to avoid modifying original
    df_transformed = df.copy()
    
    # Date columns - use pandas to_datetime for vectorized operation
    df_transformed['DT Emissao'] = pd.to_datetime(df_transformed['DT Emissao'], errors='coerce')
    df_transformed['Data do Ciclo'] = pd.to_datetime(df_transformed['Data do Ciclo'], errors='coerce')
    
    # Numeric columns - use pandas to_numeric for vectorized operations
    # For nullable integers (Cliente)
    df_transformed['Cliente'] = pd.to_numeric(df_transformed['Cliente'], errors='coerce').astype('Int64')
    
    # String columns for unidade mapping (Grp and Loja need to be strings)
    # Keep as strings to preserve leading zeros for unidade mapping
    df_transformed['Grp'] = df_transformed['Grp'].astype(str)
    df_transformed['Loja'] = df_transformed['Loja'].astype(str)
    
    # For non-nullable integers
    df_transformed['Numero'] = pd.to_numeric(df_transformed['Numero'], errors='coerce').fillna(0).astype(int)
    df_transformed['Ciclos'] = pd.to_numeric(df_transformed['Ciclos'], errors='coerce').fillna(0).astype(int)
    df_transformed['Qnt Cons.'] = pd.to_numeric(df_transformed['Qnt Cons.'], errors='coerce').fillna(0).astype(int)
    df_transformed['Mês'] = pd.to_numeric(df_transformed['Mês'], errors='coerce').fillna(0).astype(int)
    df_transformed['Ano'] = pd.to_numeric(df_transformed['Ano'], errors='coerce').fillna(0).astype(int)
    
    # For decimal columns
    df_transformed['Qntd.'] = pd.to_numeric(df_transformed['Qntd.'], errors='coerce').fillna(0.0)
    df_transformed['Total'] = pd.to_numeric(df_transformed['Total'], errors='coerce').fillna(0.0)
    df_transformed['Valor Mercadoria'] = pd.to_numeric(df_transformed['Valor Mercadoria'], errors='coerce').fillna(0.0)
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
    
    # Define column types based on transformed data (ordered as requested)
    column_definitions = [
        '"Cliente" INTEGER',
        '"Nome" VARCHAR',
        '"Paciente" VARCHAR',
        '"Nom Paciente" VARCHAR',
        'prontuario INTEGER',
        '"DT Emissao" TIMESTAMP',
        '"Descricao" VARCHAR',
        '"Qntd." DOUBLE',
        '"Total" DOUBLE',
        '"Descrição Gerencial" VARCHAR',
        '"Loja" VARCHAR',
        '"Tipo da nota" VARCHAR',
        '"Numero" INTEGER',
        '"Serie Docto." VARCHAR',
        '"NF Eletr." VARCHAR',
        '"Vend. 1" VARCHAR',
        '"Médico" VARCHAR',
        '"Cliente.1" VARCHAR',
        '"Operador" VARCHAR',
        '"Produto" VARCHAR',
        '"Valor Mercadoria" DOUBLE',
        '"Custo" DOUBLE',
        '"Custo Unit" DOUBLE',
        '"Desconto" DOUBLE',
        '"Unidade" VARCHAR',
        '"Mês" INTEGER',
        '"Ano" INTEGER',
        '"Cta-Ctbl" VARCHAR',
        '"Cta-Ctbl Eugin" VARCHAR',
        '"Interno/Externo" VARCHAR',
        '"Descrição Mapping Actividad" VARCHAR',
        '"Ciclos" INTEGER',
        '"Qnt Cons." INTEGER',
        '"Grp" VARCHAR',
        '"Descr.TES" VARCHAR',
        '"Lead Time" VARCHAR',
        '"Data do Ciclo" TIMESTAMP',
        '"Fez Ciclo?" VARCHAR',
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
    
    # Complex matching SQL query (same as diario version)
    update_sql = """
    WITH 
    -- CTE 1: Extract and process mesclada_vendas data with accent normalization
    mesclada_extract AS (
        SELECT DISTINCT 
            "Paciente", 
            CASE 
                WHEN "Nome" IS NOT NULL THEN strip_accents(TRIM(LOWER(SPLIT_PART("Nome", ' ', 1))))
                ELSE NULL 
            END as nome_first,
            CASE 
                WHEN "Nom Paciente" IS NOT NULL THEN strip_accents(TRIM(LOWER(SPLIT_PART("Nom Paciente", ' ', 1))))
                ELSE NULL 
            END as nom_paciente_first
        FROM silver.mesclada_vendas
        WHERE "Paciente" IS NOT NULL
        
    ),

    -- CTE 1B: Pre-process clinisys data with all transformations and accent normalization
    clinisys_processed AS (
        SELECT 
            codigo,
            prontuario_esposa,
            prontuario_marido,
            prontuario_responsavel1,
            prontuario_responsavel2,
            prontuario_esposa_pel,
            prontuario_marido_pel,
            prontuario_esposa_pc,
            prontuario_marido_pc,
            prontuario_responsavel1_pc,
            prontuario_responsavel2_pc,
            prontuario_esposa_fc,
            prontuario_marido_fc,
            prontuario_esposa_ba,
            prontuario_marido_ba,
            strip_accents(LOWER(TRIM(SPLIT_PART(esposa_nome, ' ', 1)))) as esposa_nome,
    		strip_accents(LOWER(TRIM(SPLIT_PART(marido_nome, ' ', 1)))) as marido_nome,
            unidade_origem
        FROM clinisys_all.silver.view_pacientes
        where inativo = 0
    ),

    -- CTE 2: Paciente ↔ prontuario (main/codigo)
    matches_1 AS (
        SELECT d.*,
               p.codigo as prontuario, p.esposa_nome, p.marido_nome, p.unidade_origem,
               'prontuario_main' as match_type
        FROM mesclada_extract d
        INNER JOIN clinisys_processed p 
            ON d."Paciente" = p.codigo
    ),

    -- CTE 3: Paciente ↔ prontuario_esposa
    matches_2 AS (
        SELECT d.*, 
               p.codigo as prontuario, p.esposa_nome, p.marido_nome, p.unidade_origem,
               'prontuario_esposa' as match_type
        FROM mesclada_extract d
        INNER JOIN clinisys_processed p 
            ON d."Paciente" = p.prontuario_esposa
    ),

    -- CTE 4: Paciente ↔ prontuario_marido
    matches_3 AS (
        SELECT d.*,
              p.codigo as prontuario, p.esposa_nome, p.marido_nome, p.unidade_origem,
               'prontuario_marido' as match_type
        FROM mesclada_extract d
        INNER JOIN clinisys_processed p 
            ON d."Paciente" = p.prontuario_marido
    ),

    -- CTE 5: Paciente ↔ prontuario_responsavel1
    matches_4 AS (
        SELECT d.*,
              p.codigo as prontuario, p.esposa_nome, p.marido_nome, p.unidade_origem,
               'prontuario_responsavel1' as match_type
        FROM mesclada_extract d
        INNER JOIN clinisys_processed p 
            ON d."Paciente" = p.prontuario_responsavel1
    ),

    -- CTE 6: Paciente ↔ prontuario_responsavel2
    matches_5 AS (
        SELECT d.*,
               p.codigo as prontuario, p.esposa_nome, p.marido_nome, p.unidade_origem,
               'prontuario_responsavel2' as match_type
        FROM mesclada_extract d
        INNER JOIN clinisys_processed p 
            ON d."Paciente" = p.prontuario_responsavel2
    ),

    -- CTE 7: Paciente ↔ prontuario_esposa_pel
    matches_6 AS (
        SELECT d.*,
               p.codigo as prontuario, p.esposa_nome, p.marido_nome, p.unidade_origem,
               'prontuario_esposa_pel' as match_type
        FROM mesclada_extract d
        INNER JOIN clinisys_processed p 
            ON d."Paciente" = p.prontuario_esposa_pel
    ),

    -- CTE 8: Paciente ↔ prontuario_marido_pel
    matches_7 AS (
        SELECT d.*,
               p.codigo as prontuario, p.esposa_nome, p.marido_nome, p.unidade_origem,
               'prontuario_marido_pel' as match_type
        FROM mesclada_extract d
        INNER JOIN clinisys_processed p 
            ON d."Paciente" = p.prontuario_marido_pel
    ),
    -- CTE 9: Paciente ↔ prontuario_esposa_pc
    matches_8 AS (
        SELECT d.*,
               p.codigo as prontuario, p.esposa_nome, p.marido_nome, p.unidade_origem,
               'prontuario_esposa_pc' as match_type
        FROM mesclada_extract d
        INNER JOIN clinisys_processed p 
            ON d."Paciente" = p.prontuario_esposa_pc
    ),
    -- CTE 10: Paciente ↔ prontuario_marido_pc
    matches_9 AS (
        SELECT d.*,
               p.codigo as prontuario, p.esposa_nome, p.marido_nome, p.unidade_origem,
               'prontuario_marido_pc' as match_type
        FROM mesclada_extract d
        INNER JOIN clinisys_processed p 
            ON d."Paciente" = p.prontuario_marido_pc
    ),
    -- CTE 11: Paciente ↔ prontuario_responsavel1_pc
    matches_10 AS (
        SELECT d.*,
               p.codigo as prontuario, p.esposa_nome, p.marido_nome, p.unidade_origem,
               'prontuario_responsavel1_pc' as match_type
        FROM mesclada_extract d
        INNER JOIN clinisys_processed p 
            ON d."Paciente" = p.prontuario_responsavel1_pc
    ),
    -- CTE 12: Paciente ↔ prontuario_responsavel2_pc
    matches_11 AS (
        SELECT d.*,
               p.codigo as prontuario, p.esposa_nome, p.marido_nome, p.unidade_origem,
               'prontuario_responsavel2_pc' as match_type
        FROM mesclada_extract d
        INNER JOIN clinisys_processed p 
            ON d."Paciente" = p.prontuario_responsavel2_pc
    ),
    -- CTE 13: Paciente ↔ prontuario_esposa_fc
    matches_12 AS (
        SELECT d.*,
               p.codigo as prontuario, p.esposa_nome, p.marido_nome, p.unidade_origem,
               'prontuario_esposa_fc' as match_type
        FROM mesclada_extract d
        INNER JOIN clinisys_processed p 
            ON d."Paciente" = p.prontuario_esposa_fc
    ),
    -- CTE 14: Paciente ↔ prontuario_marido_fc
    matches_13 AS (
        SELECT d.*,
               p.codigo as prontuario, p.esposa_nome, p.marido_nome, p.unidade_origem,
               'prontuario_marido_fc' as match_type
        FROM mesclada_extract d
        INNER JOIN clinisys_processed p 
            ON d."Paciente" = p.prontuario_marido_fc
    ),
    -- CTE 15: Paciente ↔ prontuario_esposa_ba
    matches_14 AS (
        SELECT d.*,
               p.codigo as prontuario, p.esposa_nome, p.marido_nome, p.unidade_origem,
               'prontuario_esposa_ba' as match_type
        FROM mesclada_extract d
        INNER JOIN clinisys_processed p 
            ON d."Paciente" = p.prontuario_esposa_ba
    ),
    -- CTE 16: Paciente ↔ prontuario_marido_ba
    matches_15 AS (
        SELECT d.*,
               p.codigo as prontuario, p.esposa_nome, p.marido_nome, p.unidade_origem,
               'prontuario_marido_ba' as match_type
        FROM mesclada_extract d
        INNER JOIN clinisys_processed p 
            ON d."Paciente" = p.prontuario_marido_ba
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
        -- CTE 18: Calculate scores for ranking
    scored_matches AS (
        SELECT *,
               -- Calculate name match score
               CASE 
                   WHEN (nome_first = esposa_nome AND nom_paciente_first = marido_nome) 
                        OR (nom_paciente_first = esposa_nome AND nome_first = marido_nome) THEN 0
                   WHEN (nome_first = esposa_nome OR nom_paciente_first = marido_nome) 
                        OR (nom_paciente_first = esposa_nome OR nome_first = marido_nome) THEN 2
                   ELSE 4
               END as name_match_score,
               -- Calculate match type score (odd numbers)
               CASE 
                   WHEN match_type = 'prontuario_main' THEN 1
                   WHEN match_type = 'prontuario_esposa' THEN 3
                   WHEN match_type = 'prontuario_marido' THEN 5
                   WHEN match_type = 'prontuario_responsavel1' THEN 7
                   WHEN match_type = 'prontuario_responsavel2' THEN 9
                   WHEN match_type = 'prontuario_esposa_pel' THEN 11
                   WHEN match_type = 'prontuario_marido_pel' THEN 13
                   WHEN match_type = 'prontuario_esposa_pc' THEN 15
                   WHEN match_type = 'prontuario_marido_pc' THEN 17
                   WHEN match_type = 'prontuario_responsavel1_pc' THEN 19
                   WHEN match_type = 'prontuario_responsavel2_pc' THEN 21
                   WHEN match_type = 'prontuario_esposa_fc' THEN 23
                   WHEN match_type = 'prontuario_marido_fc' THEN 25
                   WHEN match_type = 'prontuario_esposa_ba' THEN 27
                   WHEN match_type = 'prontuario_marido_ba' THEN 29
                   ELSE 31
               END as match_type_score
        FROM all_matches
        WHERE nome_first =esposa_nome OR nome_first = marido_nome OR nom_paciente_first = esposa_nome  OR nom_paciente_first = marido_nome 
    ),

    -- CTE 19: Apply ranking based on combined scores
    ranked_matches AS (
        SELECT *,
               (name_match_score + match_type_score) as combined_score,
               ROW_NUMBER() OVER (
                   PARTITION BY "Paciente" 
                   ORDER BY (name_match_score + match_type_score)
               ) 
               as rn
        FROM scored_matches
    ),

    -- CTE 20: Select best match per Cliente (lowest rn value)
    best_matches AS (
        SELECT * 
        FROM ranked_matches rm1
        WHERE rn = (
            SELECT MIN(rn) 
            FROM ranked_matches rm2 
            WHERE rm2."Paciente" = rm1."Paciente"
        )
    )

    -- Update prontuario column
    UPDATE silver.mesclada_vendas 
    SET prontuario = COALESCE(bm.prontuario, -1)
    FROM best_matches bm 
    WHERE silver.mesclada_vendas."Paciente" = bm."Paciente"
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
            FROM silver.mesclada_vendas
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
        
        # Map columns from mesclada to diario format
        df_mapped = map_columns(df_bronze_clean)
        
        # Map Grp and Filial to Unidade
        df_mapped = map_unidade(df_mapped)
        
        # Transform data types
        df_transformed = transform_data_types(df_mapped)
        
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
        
        # Insert new rows with explicit column casting (ordered as requested)
        insert_sql = f"""
        INSERT INTO silver.{SILVER_TABLE} 
        SELECT 
            CASE WHEN "Cliente" IS NULL THEN NULL ELSE CAST("Cliente" AS INTEGER) END as "Cliente",
            CAST("Nome" AS VARCHAR) as "Nome",
            CAST("Paciente" AS VARCHAR) as "Paciente",
            CAST("Nom Paciente" AS VARCHAR) as "Nom Paciente",
            CAST(prontuario AS INTEGER) as prontuario,
            CAST("DT Emissao" AS TIMESTAMP) as "DT Emissao",
            CAST("Descricao" AS VARCHAR) as "Descricao",
            CAST("Qntd." AS DOUBLE) as "Qntd.",
            CAST("Total" AS DOUBLE) as "Total",
            CAST("Descrição Gerencial" AS VARCHAR) as "Descrição Gerencial",
            CAST("Loja" AS INTEGER) as "Loja",
            CAST("Tipo da nota" AS VARCHAR) as "Tipo da nota",
            CAST("Numero" AS INTEGER) as "Numero",
            CAST("Serie Docto." AS VARCHAR) as "Serie Docto.",
            CAST("NF Eletr." AS VARCHAR) as "NF Eletr.",
            CAST("Cod. Medicco" AS VARCHAR) as "Vend. 1",
            CAST("Médico" AS VARCHAR) as "Médico",
            CAST("Cliente.1" AS VARCHAR) as "Cliente.1",
            CAST("Operador" AS VARCHAR) as "Operador",
            CAST("Produto" AS VARCHAR) as "Produto",
            CAST("Valor Mercadoria" AS DOUBLE) as "Valor Mercadoria",
            CAST("Custo" AS DOUBLE) as "Custo",
            CAST("Custo Unit" AS DOUBLE) as "Custo Unit",
            CAST("Desconto" AS DOUBLE) as "Desconto",
            CAST("Unidade" AS VARCHAR) as "Unidade",
            CAST("Mês" AS INTEGER) as "Mês",
            CAST("Ano" AS INTEGER) as "Ano",
            CAST("Cta-Ctbl" AS VARCHAR) as "Cta-Ctbl",
            CAST("Cta-Ctbl Eugin" AS VARCHAR) as "Cta-Ctbl Eugin",
            CAST("Interno/Externo" AS VARCHAR) as "Interno/Externo",
            CAST("Descrição Mapping Actividad" AS VARCHAR) as "Descrição Mapping Actividad",
            CAST("Ciclos" AS INTEGER) as "Ciclos",
            CAST("Qnt Cons." AS INTEGER) as "Qnt Cons.",
            CAST("Grp" AS VARCHAR) as "Grp",
            CAST("Descr.TES" AS VARCHAR) as "Descr.TES",
            CAST("Lead Time" AS VARCHAR) as "Lead Time",
            CAST("Data do Ciclo" AS TIMESTAMP) as "Data do Ciclo",
            CAST("Fez Ciclo?" AS VARCHAR) as "Fez Ciclo?",
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
    """Main function to transform mesclada_vendas to silver"""
    logger.info("Starting Mesclada Vendas silver transformation (with column mapping and complex prontuario matching)")
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
        logger.info("Mesclada Vendas silver transformation completed")
        
    except Exception as e:
        logger.error(f"Error in main function: {e}")
        raise

if __name__ == "__main__":
    main()
