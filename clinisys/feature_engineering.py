"""
Feature engineering utilities for Silver layer
Handles table-specific feature creation and transformations
"""

import logging

logger = logging.getLogger(__name__)


def feature_creation(con, table):
    """
    Apply table-specific feature engineering to the silver table.
    
    Args:
        con: DuckDB connection
        table: Table name to apply features to
        
    Returns:
        None (modifies the silver table in place)
    """
    if table == 'view_micromanipulacao_oocitos':
        
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
        # Add calculated columns: BMI, previous_et, previous_et_od
        logger.info(f"Adding calculated columns to silver.{table}")
        
        con.execute(f"""
            CREATE OR REPLACE TABLE silver.{table} AS
            SELECT *,
                   -- BMI: weight(kg) / height(m)^2
                   CASE 
                       WHEN peso_paciente IS NOT NULL 
                            AND altura_paciente IS NOT NULL 
                            AND altura_paciente > 0 
                       THEN ROUND(CAST(peso_paciente AS DOUBLE) / POWER(CAST(altura_paciente AS DOUBLE), 2), 2)
                       ELSE NULL 
                   END AS bmi,
                   
                   -- Previous ET: count of previous embryo transfers for this patient
                   -- Excludes: 'No transfer', 'Cancelado', 'Congelamento de Óvulos'
                   COALESCE((
                       SELECT COUNT(*)
                       FROM silver.{table} AS prev
                       WHERE prev.prontuario = {table}.prontuario
                         AND COALESCE(prev.data_transferencia, prev.data_procedimento, prev.data_dum) 
                             < COALESCE({table}.data_transferencia, {table}.data_procedimento, {table}.data_dum)
                         AND (prev.resultado_tratamento IS NULL 
                              OR prev.resultado_tratamento NOT IN ('No transfer', 'Cancelado', 'Congelamento de Óvulos'))
                   ), 0) AS previous_et,
                   
                   -- Previous OD ET: count of previous egg donation embryo transfers
                   COALESCE((
                       SELECT COUNT(*)
                       FROM silver.{table} AS prev
                       WHERE prev.prontuario = {table}.prontuario
                         AND COALESCE(prev.data_transferencia, prev.data_procedimento, prev.data_dum) 
                             < COALESCE({table}.data_transferencia, {table}.data_procedimento, {table}.data_dum)
                         AND prev.doacao_ovulos = 'Sim'
                         AND (prev.resultado_tratamento IS NULL 
                              OR prev.resultado_tratamento NOT IN ('No transfer', 'Cancelado', 'Congelamento de Óvulos'))
                   ), 0) AS previous_et_od
                   
            FROM silver.{table}
        """)
        
        logger.info(f"Added bmi, previous_et, and previous_et_od to silver.{table}")
    
    # Add more table-specific features here as needed

