#!/usr/bin/env python3
"""
Create gold.data_ploidia table from gold.planilha_embryoscope_combined

This script:
1. Reads the column mapping configuration
2. Creates a SELECT query that maps source columns to target columns
3. Leaves NULL for unmapped columns
4. Creates the table gold.data_ploidia with columns in the specified order
"""

import duckdb as db
import pandas as pd
from datetime import datetime
import os
import logging
import sys

# Import column mapping configuration
sys.path.insert(0, os.path.dirname(__file__))
import importlib.util
spec = importlib.util.spec_from_file_location("column_mapping", os.path.join(os.path.dirname(__file__), "00_02_column_mapping.py"))
column_mapping_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(column_mapping_module)
TARGET_COLUMNS = column_mapping_module.TARGET_COLUMNS
COLUMN_MAPPING = column_mapping_module.COLUMN_MAPPING

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

def get_database_connection(read_only=False):
    """Create and return a connection to the huntington_data_lake database and attach clinisys_all"""
    repo_root = os.path.dirname(os.path.dirname(__file__))
    path_to_db = os.path.join(repo_root, 'database', 'huntington_data_lake.duckdb')
    conn = db.connect(path_to_db, read_only=read_only)
    logger.info(f"Connected to database: {path_to_db} (read_only={read_only})")
    
    # Attach clinisys_all database for access to view_tratamentos
    clinisys_db = os.path.join(repo_root, 'database', 'clinisys_all.duckdb')
    conn.execute(f"ATTACH '{clinisys_db}' AS clinisys (READ_ONLY)")
    logger.info(f"Attached clinisys database: {clinisys_db}")
    
    return conn

def get_available_columns(conn):
    """Get list of available columns in pesquisa_embrioes_com_tratamento_morfocinetica_desfechos"""
    col_info = conn.execute("DESCRIBE gold.pesquisa_embrioes_com_tratamento_morfocinetica_desfechos").df()
    available_columns = col_info['column_name'].tolist()
    logger.info(f"Found {len(available_columns)} columns in gold.pesquisa_embrioes_com_tratamento_morfocinetica_desfechos")
    return available_columns

def build_select_clause(target_columns, column_mapping, available_columns):
    """
    Build SELECT clause that maps source columns to target columns
    
    Args:
        target_columns: List of target column names in order
        column_mapping: Dictionary mapping target -> source column names (can be None)
        available_columns: List of available columns in source table
    
    Returns:
        SELECT clause string, unmapped columns list, missing source columns list
    """
    select_parts = []
    unmapped_columns = []
    missing_source_columns = []
    
    for target_col in target_columns:
        # Special handling for Video ID - concatenate Patient ID and Slide ID
        if target_col == "Video ID":
            # Video ID = Patient ID (micro_prontuario) + "_" + Slide ID (full embryo_EmbryoID)
            if "micro_prontuario" in available_columns and "embryo_EmbryoID" in available_columns:
                select_parts.append(
                    'CAST("micro_prontuario" AS VARCHAR) || \'_\' || "embryo_EmbryoID" as "' + target_col + '"'
                )
            else:
                logger.warning("micro_prontuario or embryo_EmbryoID not found for Video ID. Using NULL.")
                select_parts.append(f'NULL as "{target_col}"')
                missing_source_columns.append((target_col, "micro_prontuario, embryo_EmbryoID"))
        
        elif target_col in column_mapping:
            source_col = column_mapping[target_col]
            
            # Handle None mapping (explicitly unmapped)
            if source_col is None:
                select_parts.append(f'NULL as "{target_col}"')
                unmapped_columns.append(target_col)
            # Check if source column exists
            elif source_col in available_columns:
                # Escape column name properly (handle special characters)
                select_parts.append(f'"{source_col}" as "{target_col}"')
            else:
                # Source column doesn't exist, use NULL
                logger.warning(f"Source column '{source_col}' not found for target '{target_col}'. Using NULL.")
                select_parts.append(f'NULL as "{target_col}"')
                missing_source_columns.append((target_col, source_col))
        else:
            # No mapping, use NULL
            select_parts.append(f'NULL as "{target_col}"')
            unmapped_columns.append(target_col)
    
    if unmapped_columns:
        logger.info(f"Unmapped columns (will be NULL): {unmapped_columns}")
    if missing_source_columns:
        logger.warning(f"Missing source columns (will be NULL): {missing_source_columns}")
    
    return ', '.join(select_parts), unmapped_columns, missing_source_columns

def create_data_ploidia_table(conn):
    """Create gold.data_ploidia table with mapped columns"""
    
    logger.info("=" * 80)
    logger.info("CREATING gold.data_ploidia TABLE")
    logger.info("=" * 80)
    
    # Get available columns from source table
    logger.info("Checking available columns in source table...")
    available_columns = get_available_columns(conn)
    
    # Build SELECT clause
    logger.info("Building SELECT clause with column mappings...")
    select_clause, unmapped, missing = build_select_clause(
        TARGET_COLUMNS, 
        COLUMN_MAPPING, 
        available_columns
    )
    
    # Create the table
    logger.info("Creating gold.data_ploidia table...")
    
    # Ensure gold schema exists
    conn.execute("CREATE SCHEMA IF NOT EXISTS gold")
    
    # Drop table/view if it exists
    for obj_name in ["gold.pesquisa_dados_para_ia", "gold.pesquisa_dados_para_ia_initial_mapping", "gold.data_ploidia_initial_mapping"]:
        for obj_type in ['VIEW', 'TABLE']:
            try:
                conn.execute(f"DROP {obj_type} IF EXISTS {obj_name};")
            except Exception:
                pass
    logger.info("Dropped existing gold.pesquisa_dados_para_ia objects if they existed")
    
    # Build WHERE clause
    where_conditions = []
    
    # Always exclude rows where Patient ID is NULL
    where_conditions.append('"micro_prontuario" IS NOT NULL')
    logger.info("Filter: Patient ID (micro_prontuario) IS NOT NULL")
    
    # Always exclude rows where Embryo ID is NULL
    where_conditions.append('"embryo_EmbryoID" IS NOT NULL')
    logger.info("Filter: Embryo ID (embryo_EmbryoID) IS NOT NULL")
    
    # Only include embryos with a biopsy OR a valid clinical outcome
    where_conditions.append('("has_biopsy" = True OR "has_valid_outcome" = True)')
    logger.info("Filter: has_biopsy = True OR has_valid_outcome = True")
    
    where_clause = " WHERE " + " AND ".join(where_conditions) if where_conditions else ""
    
    # Create table with CTE for Previous ET calculation when NULL
    # Most columns come directly from mapping, but Previous ET needs calculation fallback
    create_query = f"""
    CREATE TABLE gold.pesquisa_dados_para_ia AS
    WITH base_data AS (
        SELECT DISTINCT
            {select_clause}
        FROM gold.pesquisa_embrioes_com_tratamento_morfocinetica_desfechos
        {where_clause}
    ),
    deduped_base_data AS (
        SELECT * EXCLUDE (rn)
        FROM (
            SELECT 
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY "Slide ID"
                    ORDER BY 
                        CASE WHEN "Embryo Description Clinisys" IS NOT NULL AND "Embryo Description Clinisys" != 'None' THEN 0 ELSE 1 END,
                        CASE WHEN "Embryo Description Clinisys Detalhes" IS NOT NULL AND "Embryo Description Clinisys Detalhes" != 'None' THEN 0 ELSE 1 END,
                        CASE WHEN "has_valid_outcome" = True THEN 0 ELSE 1 END,
                        CASE WHEN "outcome_type" IS NOT NULL AND "outcome_type" != 'None' THEN 0 ELSE 1 END
                ) as rn
            FROM base_data
        )
        WHERE rn = 1
    ),
    embryo_ref_dates AS (
        SELECT 
            *,
            STRPTIME(SPLIT_PART(SPLIT_PART("Slide ID", '_', 1), 'D', 2), '%Y.%m.%d') as ref_date
        FROM deduped_base_data
    ),
    treatment_counts AS (
        SELECT 
            e."Patient ID",
            e."Slide ID",
            e.ref_date,
            COUNT(CASE 
                WHEN t.prontuario IS NOT NULL 
                     AND (t.resultado_tratamento == 'None' 
                          OR t.resultado_tratamento NOT IN ('No transfer', 'Cancelado', 'Congelamento de Óvulos'))
                THEN 1 
            END) as prev_et_count,
            COUNT(CASE 
                WHEN t.prontuario IS NOT NULL
                     AND t.doacao_ovulos = 'Sim' 
                     AND (t.resultado_tratamento == 'None' 
                          OR t.resultado_tratamento NOT IN ('No transfer', 'Cancelado', 'Congelamento de Óvulos'))
                THEN 1 
            END) as prev_od_et_count
        FROM embryo_ref_dates e
        LEFT JOIN clinisys.silver.view_tratamentos t 
            ON e."Patient ID" = t.prontuario 
            AND COALESCE(t.data_transferencia, t.data_procedimento, t.data_dum) < e.ref_date
        GROUP BY e."Patient ID", e."Slide ID", e.ref_date
    )
    SELECT 
        e."Unidade",
        e."Video ID",
        e."Age",
        e."BMI",
        e."Birth Year",
        e."Diagnosis",
        e."Patient Comments",
        e."Patient ID",
        COALESCE(e."Previus ET", tc.prev_et_count, 0) as "Previus ET",
        COALESCE(e."Previus OD ET", tc.prev_od_et_count, 0) as "Previus OD ET",
        e."Oocyte History",
        CASE 
            WHEN e."Oocyte Source" IS NOT NULL THEN e."Oocyte Source"
            WHEN e."Oocyte History" LIKE '%OR%' THEN 'Heterólogo'  -- Egg donation (OR = Ovodoação Recepção)
            ELSE 'Homólogo'  -- Default to Homólogo (Autologous) for NULL values
        END as "Oocyte Source",
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
        e."Embryo Description",
        e."Embryo Description Clinisys",
        e."Embryo Description Clinisys Detalhes",
        e."outcome_type",
        e."tipo_resultado",
        e."merged_numero_de_nascidos",
        e."gravidez_clinica",
        e."delivery_occurred",
        e."redlara_source_sheet",
        e."crosscheck_status",
        e."has_conflict",
        e."fet_gravidez_clinica",
        e."trat2_resultado_tratamento",
        e."trat1_resultado_tratamento",
        e."fet_tipo_resultado",
        e."has_biopsy",
        e."has_valid_outcome"
    FROM embryo_ref_dates e
    LEFT JOIN treatment_counts tc 
        ON e."Patient ID" = tc."Patient ID" 
        AND e."Slide ID" = tc."Slide ID"
    """
    
    logger.info("Executing CREATE TABLE query...")
    logger.info(f"Creating table with {len(TARGET_COLUMNS)} columns")
    if where_clause:
        logger.info(f"Applying filters: {where_clause}")
    
    conn.execute(create_query)
    logger.info("Table gold.pesquisa_dados_para_ia created successfully")
    
    # Get statistics
    row_count = conn.execute("SELECT COUNT(*) FROM gold.pesquisa_dados_para_ia").fetchone()[0]
    col_info = conn.execute("DESCRIBE gold.pesquisa_dados_para_ia").df()
    col_count = len(col_info)
    
    logger.info("=" * 80)
    logger.info("TABLE STATISTICS")
    logger.info("=" * 80)
    logger.info(f"Rows: {row_count:,}")
    logger.info(f"Columns: {col_count}")
    
    logger.info("")
    logger.info("Successfully created gold.pesquisa_dados_para_ia base table")

def fill_missing_values(conn):
    """Fill missing BMI and Diagnosis values using vectorized bulk mode imputation"""
    logger.info("=" * 80)
    logger.info("STEP 2: FILLING MISSING VALUES (VECTORIZED BULK IMPUTATION)")
    logger.info("=" * 80)
    
    # BMI Imputation
    bmi_nulls_before = conn.execute('SELECT COUNT(*) FROM gold.pesquisa_dados_para_ia WHERE "BMI" IS NULL').fetchone()[0]
    conn.execute("""
    UPDATE gold.pesquisa_dados_para_ia
    SET "BMI" = b.calculated_bmi
    FROM (
        WITH patient_bmi AS (
            SELECT 
                prontuario,
                peso_paciente,
                altura_paciente,
                ROUND(peso_paciente / POWER(altura_paciente, 2), 2) as calculated_bmi,
                COUNT(*) as frequency,
                ROW_NUMBER() OVER (
                    PARTITION BY prontuario 
                    ORDER BY COUNT(*) DESC, peso_paciente DESC
                ) as rn
            FROM clinisys.silver.view_tratamentos
            WHERE peso_paciente IS NOT NULL 
              AND altura_paciente IS NOT NULL
              AND altura_paciente > 0
            GROUP BY prontuario, peso_paciente, altura_paciente
        )
        SELECT prontuario, calculated_bmi
        FROM patient_bmi
        WHERE rn = 1
    ) b
    WHERE gold.pesquisa_dados_para_ia."Patient ID" = b.prontuario
      AND gold.pesquisa_dados_para_ia."BMI" IS NULL;
    """)
    bmi_nulls_after = conn.execute('SELECT COUNT(*) FROM gold.pesquisa_dados_para_ia WHERE "BMI" IS NULL').fetchone()[0]
    logger.info(f"BMI Imputation: Filled {bmi_nulls_before - bmi_nulls_after:,} missing values ({bmi_nulls_after:,} remaining)")
    
    # Diagnosis Imputation
    diag_nulls_before = conn.execute('SELECT COUNT(*) FROM gold.pesquisa_dados_para_ia WHERE "Diagnosis" IS NULL').fetchone()[0]
    conn.execute("""
    UPDATE gold.pesquisa_dados_para_ia
    SET "Diagnosis" = d.diagnosis
    FROM (
        WITH patient_diagnosis AS (
            SELECT 
                prontuario,
                fator_infertilidade1 as diagnosis,
                COUNT(*) as frequency,
                ROW_NUMBER() OVER (
                    PARTITION BY prontuario 
                    ORDER BY COUNT(*) DESC
                ) as rn
            FROM clinisys.silver.view_tratamentos
            WHERE fator_infertilidade1 IS NOT NULL
            GROUP BY prontuario, fator_infertilidade1
        )
        SELECT prontuario, diagnosis
        FROM patient_diagnosis
        WHERE rn = 1
    ) d
    WHERE gold.pesquisa_dados_para_ia."Patient ID" = d.prontuario
      AND gold.pesquisa_dados_para_ia."Diagnosis" IS NULL;
    """)
    diag_nulls_after = conn.execute('SELECT COUNT(*) FROM gold.pesquisa_dados_para_ia WHERE "Diagnosis" IS NULL').fetchone()[0]
    logger.info(f"Diagnosis Imputation: Filled {diag_nulls_before - diag_nulls_after:,} missing values ({diag_nulls_after:,} remaining)")

def join_image_availability(conn):
    """Enrich with server image availability and create all backward-compatible views"""
    logger.info("=" * 80)
    logger.info("STEP 3: JOINING IMAGE AVAILABILITY & CREATING VIEWS")
    logger.info("=" * 80)
    
    cols = [c[0] for c in conn.execute("DESCRIBE gold.pesquisa_dados_para_ia").fetchall()]
    if "api_response_code" not in cols:
        conn.execute("ALTER TABLE gold.pesquisa_dados_para_ia ADD COLUMN api_response_code INTEGER")
    if "api_error_message" not in cols:
        conn.execute("ALTER TABLE gold.pesquisa_dados_para_ia ADD COLUMN api_error_message VARCHAR")
        
    conn.execute("""
    UPDATE gold.pesquisa_dados_para_ia
    SET 
        api_response_code = s.api_response_code,
        api_error_message = s.error_message
    FROM silver.embryo_image_availability_latest s
    WHERE gold.pesquisa_dados_para_ia."Slide ID" = s."embryo_EmbryoID";
    """)
    
    # Image metrics
    matched = conn.execute('SELECT COUNT(*) FROM gold.pesquisa_dados_para_ia WHERE api_response_code = 200').fetchone()[0]
    total = conn.execute('SELECT COUNT(*) FROM gold.pesquisa_dados_para_ia').fetchone()[0]
    logger.info(f"Image Availability: {matched:,} / {total:,} videos confirmed available (HTTP 200: {matched/total*100:.1f}%)")
    
    # Safe drop of legacy aliases if they exist
    for obj_name in ["gold.data_ploidia", "gold.data_ploidia_metrics_enriched", "gold.data_ploidia_initial_mapping", "gold.pesquisa_dados_para_ia_initial_mapping"]:
        for obj_type in ['VIEW', 'TABLE']:
            try:
                conn.execute(f"DROP {obj_type} IF EXISTS {obj_name};")
            except Exception:
                pass
    logger.info("Ensured all legacy aliases are dropped (no views in production)")

def main():
    """Main function - Consolidated End-to-End Build for pesquisa_dados_para_ia"""
    logger.info("=" * 80)
    logger.info("BUILDING PESQUISA_DADOS_PARA_IA (CONSOLIDATED PIPELINE)")
    logger.info(f"Timestamp: {datetime.now()}")
    logger.info("=" * 80)
    
    try:
        logger.info("Connecting to database...")
        conn = get_database_connection(read_only=False)
        
        # Step 1: Create base table with strict 1:1 grain and deduplication on Slide ID
        create_data_ploidia_table(conn)
        
        # Step 2: Vectorized bulk mode imputation for BMI & Diagnosis
        fill_missing_values(conn)
        
        # Step 3: Enrich with video image availability and create backward-compatible views
        join_image_availability(conn)
        
        logger.info("")
        logger.info("CONSOLIDATED PIPELINE COMPLETED SUCCESSFULLY")
        
    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()


