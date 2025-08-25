#!/usr/bin/env python3
"""
Diario Vendas Bronze to Silver Transformation
Simplified version with direct prontuario mapping
"""

import os
import sys
import logging
import pandas as pd
import duckdb as db
from datetime import datetime
from pathlib import Path

# Add the project root to the Python path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/02_02_diario_to_silver_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Constants
DUCKDB_PATH = os.path.join(project_root, 'database', 'huntington_data_lake.duckdb')
BRONZE_TABLE = 'diario_vendas'
SILVER_TABLE = 'diario_vendas'

def get_duckdb_connection():
    """Create and return a DuckDB connection"""
    try:
        con = db.connect(DUCKDB_PATH)
        logger.info(f"Connected to DuckDB: {DUCKDB_PATH}")
        return con
    except Exception as e:
        logger.error(f"Error connecting to DuckDB: {e}")
        raise

def create_silver_table(con, df):
    """Create the silver table with proper schema"""
    # Get column types from DataFrame
    columns = []
    for col, dtype in df.dtypes.items():
        if dtype == 'object':
            columns.append(f'"{col}" VARCHAR')
        elif dtype == 'int64':
            columns.append(f'"{col}" INTEGER')
        elif dtype == 'float64':
            columns.append(f'"{col}" DOUBLE')
        elif dtype == 'datetime64[ns]':
            columns.append(f'"{col}" TIMESTAMP')
        else:
            columns.append(f'"{col}" VARCHAR')
    
    # Add prontuario column
    columns.append('prontuario INTEGER')
    
    create_sql = f"""
    CREATE TABLE silver.{SILVER_TABLE} (
        {', '.join(columns)}
    )
    """
    
    con.execute(create_sql)
    logger.info(f"Created silver table: silver.{SILVER_TABLE}")

def transform_data_types(df):
    """Transform data types for silver layer"""
    logger.info("Transforming data types...")
    
    # Create a copy to avoid modifying the original
    df_transformed = df.copy()
    
    # Transform numeric columns
    numeric_columns = ['Cliente', 'Loja', 'Numero', 'Mês', 'Ano', 'Ciclos', 'Qnt Cons.']
    for col in numeric_columns:
        if col in df_transformed.columns:
            df_transformed[col] = pd.to_numeric(df_transformed[col], errors='coerce').astype('Int64')
    
    # Transform decimal columns
    decimal_columns = ['Qntd.', 'Valor Unitario', 'Valor Mercadoria', 'Total', 'Custo', 'Custo Unit', 'Desconto']
    for col in decimal_columns:
        if col in df_transformed.columns:
            df_transformed[col] = pd.to_numeric(df_transformed[col], errors='coerce')
    
    # Transform date columns
    date_columns = ['DT Emissao']
    for col in date_columns:
        if col in df_transformed.columns:
            df_transformed[col] = pd.to_datetime(df_transformed[col], errors='coerce')
    
    # Transform string columns
    string_columns = ['Nome', 'Nom Paciente', 'Tipo da nota', 'Serie Docto.', 'NF Eletr.', 
                     'Vend. 1', 'Médico', 'Cliente.1', 'Opr ', 'Operador', 'Produto', 
                     'Descricao', 'Unidade', 'Cta-Ctbl', 'Cta-Ctbl Eugin', 'Interno/Externo',
                     'Descrição Gerencial', 'Descrição Mapping Actividad', 'line_number', 
                     'extraction_timestamp', 'file_name']
    
    for col in string_columns:
        if col in df_transformed.columns:
            df_transformed[col] = df_transformed[col].astype(str)
    
    logger.info("Data type transformation completed")
    return df_transformed

def process_bronze_to_silver(con):
    """Process bronze to silver transformation with simple prontuario mapping"""
    try:
        logger.info("Starting bronze to silver transformation...")
        
        # Read bronze data
        logger.info(f"Reading from bronze.{BRONZE_TABLE}...")
        df_bronze = con.execute(f"SELECT * FROM bronze.{BRONZE_TABLE}").df()
        logger.info(f"Read {len(df_bronze):,} rows from bronze layer")
        
        # Basic data cleaning
        logger.info("Performing basic data cleaning...")
        df_bronze_clean = df_bronze.copy()
        
        # Remove completely empty rows
        initial_rows = len(df_bronze_clean)
        df_bronze_clean = df_bronze_clean.dropna(how='all')
        logger.info(f"Removed {initial_rows - len(df_bronze_clean)} completely empty rows")
        
        # Remove rows with >50% nulls
        null_percentages = df_bronze_clean.isnull().sum(axis=1) / len(df_bronze_clean.columns) * 100
        df_bronze_clean = df_bronze_clean[null_percentages <= 50]
        logger.info(f"Removed {len(df_bronze.dropna(how='all')) - len(df_bronze_clean)} rows with majority of columns blank")
        logger.info(f"Total rows after cleaning: {len(df_bronze_clean):,}")
        
        # Transform data types
        df_transformed = transform_data_types(df_bronze_clean)
        
        # Connect to clinisys database and attach it for prontuario mapping
        clinisys_db_path = os.path.join(project_root, 'database', 'clinisys_all.duckdb')
        
        if os.path.exists(clinisys_db_path):
            try:
                # Attach clinisys database to the main connection
                con.execute(f"ATTACH '{clinisys_db_path}' AS clinisys_all")
                logger.info("Attached clinisys_all database")
                
                # Drop existing silver table
                logger.info("Dropping existing silver table...")
                con.execute(f"DROP TABLE IF EXISTS silver.{SILVER_TABLE}")
                
                # Create silver table normally first
                logger.info("Creating silver table...")
                
                # Add a temporary prontuario column with -1
                df_transformed['prontuario'] = -1
                
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
                con.execute("DROP VIEW temp_new_rows")
                
                logger.info(f"Successfully inserted {len(df_transformed)} rows to silver.{SILVER_TABLE}")
                
                # Now update the prontuario column with simple join
                logger.info("Updating prontuario column with simple join...")
                
                update_sql = f"""
                UPDATE silver.{SILVER_TABLE} 
                SET prontuario = p.codigo
                FROM clinisys_all.silver.view_pacientes p 
                WHERE silver.{SILVER_TABLE}."Cliente" = p.codigo
                """
                
                con.execute(update_sql)
                
                # Log mapping statistics
                result = con.execute(f"SELECT COUNT(*) as count FROM silver.{SILVER_TABLE} WHERE prontuario != -1").fetchone()
                matched_count = result[0] if result else 0
                
                result = con.execute(f"SELECT COUNT(*) as count FROM silver.{SILVER_TABLE} WHERE prontuario = -1").fetchone()
                unmatched_count = result[0] if result else 0
                
                logger.info(f"Prontuario mapping completed: {matched_count:,} matched, {unmatched_count:,} unmatched")
                
            except Exception as e:
                logger.warning(f"Error executing prontuario mapping query: {e}")
                logger.info("Creating silver table without prontuario mapping")
                
                # Fallback: create silver table without prontuario mapping
                df_transformed['prontuario'] = -1
                
                # Drop existing silver table and recreate
                con.execute(f"DROP TABLE IF EXISTS silver.{SILVER_TABLE}")
                
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
                con.execute("DROP VIEW temp_new_rows")
                
                logger.info(f"Successfully inserted {len(df_transformed)} rows to silver.{SILVER_TABLE}")
        else:
            logger.warning(f"Clinisys database not found at {clinisys_db_path}")
            logger.info("Creating silver table without prontuario mapping")
            
            # Create silver table without prontuario mapping
            df_transformed['prontuario'] = -1
            
            # Drop existing silver table and recreate
            con.execute(f"DROP TABLE IF EXISTS silver.{SILVER_TABLE}")
            
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
            con.execute("DROP VIEW temp_new_rows")
            
            logger.info(f"Successfully inserted {len(df_transformed)} rows to silver.{SILVER_TABLE}")
        
        return len(df_transformed)
        
    except Exception as e:
        logger.error(f"Error processing bronze to silver: {e}")
        raise

def main():
    """Main function to transform diario_vendas to silver"""
    logger.info("Starting Diario Vendas silver transformation (simplified)")
    logger.info(f"DuckDB path: {DUCKDB_PATH}")
    
    try:
        # Create DuckDB connection
        logger.info("Creating DuckDB connection...")
        con = get_duckdb_connection()
        logger.info("DuckDB connection created successfully")
        
        # Process bronze to silver
        new_rows = process_bronze_to_silver(con)
        
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
