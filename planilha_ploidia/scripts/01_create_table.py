#!/usr/bin/env python3
"""
Refactored script to create gold.data_ploidia table using new modular structure.

This script demonstrates how to use the new config/ and utils/ modules
while maintaining the same functionality as the original script.
"""
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.columns import TARGET_COLUMNS, COLUMN_MAPPING, FILTER_PATIENT_ID, FILTER_EMBRYO_IDS
from utils.db_utils import get_connection, get_available_columns
from utils.logging_utils import setup_logger

# Import build_select_clause from original file (complex 140+ line function)
# This avoids duplicating special-case logic for Age, BMI, Video ID, etc.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from importlib.util import spec_from_file_location, module_from_spec
spec = spec_from_file_location("original", os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "01_create_data_ploidia_table.py"))
original_module = module_from_spec(spec)
spec.loader.exec_module(original_module)
build_select_clause = original_module.build_select_clause

logger = setup_logger('create_table')


def create_table():
    """Create gold.data_ploidia table with all transformations"""
    logger.info("=" * 80)
    logger.info("CREATING gold.data_ploidia TABLE")
    logger.info("=" * 80)
    
    # Connect to database (using new utility!)
    conn = get_connection(attach_clinisys=True, read_only=False)
    
    try:
        # Get available columns (using new utility!)
        available_columns = get_available_columns(conn)
        
        # Build SELECT clause (using imported function)
        logger.info("Building SELECT clause with column mappings...")
        select_clause, unmapped, missing = build_select_clause(
            TARGET_COLUMNS,
            COLUMN_MAPPING,
            available_columns
        )
        
        # Build WHERE clause
        where_conditions = []
        where_conditions.append('"micro_prontuario" IS NOT NULL')
        where_conditions.append('"embryo_EmbryoID" IS NOT NULL')
        where_conditions.append('"embryo_Description" IS NOT NULL')  # Filter out rows without embryo description
        
        if FILTER_PATIENT_ID:
            where_conditions.append(f'"micro_prontuario" = {FILTER_PATIENT_ID}')
        
        if FILTER_EMBRYO_IDS:
            embryo_ids_str = ', '.join([f"'{eid}'" for eid in FILTER_EMBRYO_IDS])
            where_conditions.append(f'"embryo_EmbryoID" IN ({embryo_ids_str})')
        
        
        where_clause = " WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        
        # Use exact query from original script to ensure identical results
        create_query = f"""
        CREATE TABLE gold.data_ploidia AS
        WITH base_data AS (
            SELECT DISTINCT
                {select_clause}
            FROM gold.embryoscope_clinisys_combined
            {where_clause}
        ),
        embryo_ref_dates AS (
            SELECT 
                *,
                STRPTIME(SPLIT_PART(SPLIT_PART("Slide ID", '_', 1), 'D', 2), '%Y.%m.%d') as ref_date
            FROM base_data
        ),
        treatment_counts AS (
            SELECT 
                e."Patient ID",
                e."Slide ID",
                e.ref_date,
                COUNT(t.prontuario) as prev_et_count,
                COUNT(CASE WHEN t.doacao_ovulos = 'Sim' THEN 1 END) as prev_od_et_count
            FROM embryo_ref_dates e
            LEFT JOIN clinisys.silver.view_tratamentos t 
                ON e."Patient ID" = t.prontuario 
                AND t.data_transferencia < e.ref_date
            GROUP BY e."Patient ID", e."Slide ID", e.ref_date
        ),
        most_recent_treatment AS (
            SELECT 
                e."Patient ID",
                e."Slide ID",
                e.ref_date,
                t.peso_paciente,
                t.altura_paciente,
                ROUND(t.peso_paciente / POWER(t.altura_paciente, 2), 2) as calc_bmi,
                t.fator_infertilidade1 as calc_diagnosis,
                t.origem_material as calc_oocyte_source,
                ROW_NUMBER() OVER (
                    PARTITION BY e."Patient ID", e."Slide ID"
                    ORDER BY t.data_transferencia DESC
                ) as rn
            FROM embryo_ref_dates e
            LEFT JOIN clinisys.silver.view_tratamentos t 
                ON e."Patient ID" = t.prontuario 
                AND t.peso_paciente IS NOT NULL
                AND t.altura_paciente IS NOT NULL
                AND t.altura_paciente > 0
        ),
        fallback_treatment AS (
            SELECT 
                e."Patient ID",
                e."Slide ID",
                t.fator_infertilidade1 as fallback_diagnosis,
                t.origem_material as fallback_oocyte_source,
                ROW_NUMBER() OVER (
                    PARTITION BY e."Patient ID", e."Slide ID"
                    ORDER BY t.data_transferencia DESC
                ) as rn
            FROM embryo_ref_dates e
            LEFT JOIN clinisys.silver.view_tratamentos t 
                ON e."Patient ID" = t.prontuario
        )
        SELECT 
            e."Unidade",
            e."Video ID",
            e."Age",
            COALESCE(mrt.calc_bmi, e."BMI") as "BMI",
            e."Birth Year",
            COALESCE(mrt.calc_diagnosis, ft.fallback_diagnosis, e."Diagnosis") as "Diagnosis",
            e."Patient Comments",
            e."Patient ID",
            COALESCE(tc.prev_et_count, 0) as "Previus ET",
            COALESCE(tc.prev_od_et_count, 0) as "Previus OD ET",
            e."Oocyte History",
            COALESCE(mrt.calc_oocyte_source, ft.fallback_oocyte_source, e."Oocyte Source") as "Oocyte Source",
            e."Oocytes Aspirated",
            e."Slide ID",
            e."Well",
            e."Embryo ID",
            e."t2",
            e."t3",
            e."t4",
            e."t5",
            e."t8",
            e."tB",
            e."tEB",
            e."tHB",
            e."tM",
            e."tPNa",
            e."tPNf",
            e."tSB",
            e."tSC",
            e."Frag-2 Cat. - Value",
            e."Fragmentation - Value",
            e."ICM - Value",
            e."MN-2 Type - Value",
            e."MN-2 Cells - Value",
            e."PN - Value",
            e."Pulsing - Value",
            e."Re-exp Count - Value",
            e."TE - Value",
            e."Embryo Description"
        FROM embryo_ref_dates e
        LEFT JOIN treatment_counts tc 
            ON e."Patient ID" = tc."Patient ID" 
            AND e."Slide ID" = tc."Slide ID"
        LEFT JOIN most_recent_treatment mrt 
            ON e."Patient ID" = mrt."Patient ID" 
            AND e."Slide ID" = mrt."Slide ID"
            AND mrt.rn = 1
        LEFT JOIN fallback_treatment ft
            ON e."Patient ID" = ft."Patient ID"
            AND e."Slide ID" = ft."Slide ID"
            AND ft.rn = 1
        """
        
        # Execute
        conn.execute("CREATE SCHEMA IF NOT EXISTS gold")
        conn.execute("DROP TABLE IF EXISTS gold.data_ploidia_new")
        logger.info("Executing CREATE TABLE query...")
        conn.execute(create_query.replace("CREATE TABLE gold.data_ploidia", "CREATE TABLE gold.data_ploidia_new"))
        
        logger.info("")
        logger.info("Successfully created gold.data_ploidia_new table")
        
    finally:
        conn.close()


if __name__ == "__main__":
    create_table()
