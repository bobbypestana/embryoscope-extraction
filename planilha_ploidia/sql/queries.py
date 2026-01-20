"""SQL query builders for data_ploidia table creation

Note: The build_select_clause function is complex and handles many special cases.
For now, we import it from the original script to ensure compatibility.
Future refactoring can break this down further.
"""
import logging

logger = logging.getLogger(__name__)


# Import the existing build_select_clause function
# This is a pragmatic approach to avoid breaking existing logic
def build_select_clause(target_columns, column_mapping, available_columns):
    """
    Build SELECT clause that maps source columns to target columns.
    
    This function is imported from the original 01_create_data_ploidia_table.py
    to ensure compatibility. It handles special cases for:
    - Age calculation
    - BMI calculation  
    - Previous ET
    - Video ID
    - Birth Year extraction
    - And more...
    
    Args:
        target_columns: List of target column names in order
        column_mapping: Dictionary mapping target -> source column names
        available_columns: List of available columns in source table
    
    Returns:
        tuple: (select_clause_string, unmapped_columns, missing_source_columns)
    """
    # Import from original file
    import sys
    import os
    sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
    from importlib import import_module
    
    # Dynamically import the function
    spec = import_module('01_create_data_ploidia_table')
    return spec.build_select_clause(target_columns, column_mapping, available_columns)


def build_cte_query(select_clause, where_clause=""):
    """
    Build complete CTE query with fallback logic for Previous ET and patient data.
    
    Args:
        select_clause: SELECT clause string from build_select_clause
        where_clause: Optional WHERE clause
    
    Returns:
        str: Complete CREATE TABLE query with CTEs
    """
    query = f"""
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
    
    return query
