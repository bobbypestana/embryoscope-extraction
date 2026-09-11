#!/usr/bin/env python3
"""
REDLARA Silver Transformation Pipeline
Combines bronze tables into 6 dedicated procedure-level Silver tables:
  1. silver.redlara_fresh
  2. silver.redlara_fet
  3. silver.redlara_fot
  4. silver.redlara_recep
  5. silver.redlara_fp
  6. silver.redlara_iui

Features:
- Exhaustive schema: Preserves all raw columns mapped and coalesced into standardized English snake_case.
- Robust data type casting (dates, integers, floats, clean strings).
- Derived clinical pregnancy & biochemical pregnancy binary indicators ('1' / '0' / NULL) according to ICMART / REDLARA standards.
- Strategy L Prontuario Matching via commons.prontuario_matching_v1 against clinisys_all.duckdb.
"""

import os
import sys
import re
import logging
import unicodedata
from datetime import datetime
from pathlib import Path
from collections import defaultdict
import duckdb
import pandas as pd

# Setup paths
SCRIPT_DIR = Path(__file__).resolve().parent
REDLARA_DIR = SCRIPT_DIR.parent
PROJECT_ROOT = REDLARA_DIR.parent
DB_PATH = PROJECT_ROOT / "database" / "huntington_data_lake.duckdb"
CLINISYS_PATH = PROJECT_ROOT / "database" / "clinisys_all.duckdb"
LOGS_DIR = SCRIPT_DIR / "logs"
LOGS_DIR.mkdir(parents=True, exist_ok=True)

# Add Huntington root to sys.path for commons
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from commons.prontuario_matching_v1 import find_prontuarios

# Setup logging
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_file = LOGS_DIR / f"02_redlara_to_silver_{timestamp}.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(str(log_file), encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("redlara_silver")

# Streams to process
STREAMS = ['fresh', 'fet', 'fot', 'recep', 'fp', 'iui']

def normalize_col_name(raw: str) -> str:
    """Normalizes raw header into clean snake_case string."""
    s = unicodedata.normalize('NFKD', str(raw))
    s = re.sub(r'[\u0300-\u036f]', '', s)
    s = re.sub(r'[^\w\s]', ' ', s)
    s = re.sub(r'\s+', '_', s.strip().lower())
    return s

def get_canonical_name(raw_name: str) -> str:
    """Maps normalized raw column name variations to canonical English snake_case names."""
    s = normalize_col_name(raw_name)
    
    mapping_rules = [
        # Patient Identification & Demographics
        (r'^(chart_of_pin|chart_or_pin.*|pin)$', 'chart_or_pin'),
        (r'^(nome.*|patient_name|paciente)$', 'patient_name'),
        (r'^(woman_s_date_of_birth|date_of_birth|data_de_nasc|birth_date)$', 'date_of_birth'),
        (r'^(m_dico.*|medico.*|doctor)$', 'doctor'),
        (r'^(relationship_status|marital_status)$', 'relationship_status'),
        (r'^(source_of_funding|funding)$', 'source_of_funding'),
        (r'^(weight.*|recipient_weight.*|peso_kg|peso)$', 'weight_kg'),
        (r'^(height.*|recipient_height.*|altura)$', 'height_cm'),
        (r'^(age_of_male_partner|man_s_age|idade_espermatozoide)$', 'male_partner_age'),
        (r'^(man_s_date_of_birth)$', 'male_partner_dob'),
        (r'^(partner_s_relationship)$', 'partner_relationship'),
        
        # Clinical Diagnosis & History
        (r'^(diagnosis_1|diagnose_1|diagnostico_1)$', 'diagnosis_1'),
        (r'^(diagnosis_2|diagnose_2|diagnostico_2)$', 'diagnosis_2'),
        (r'^(type_of_endometriosis)$', 'type_of_endometriosis'),
        (r'^(date_of_surgery.*|data_da_puncao)$', 'date_of_surgery'),
        (r'^(type_of_surgery)$', 'type_of_surgery'),
        (r'^(reason.*|motivo.*)$', 'reason_for_cryopreservation'),
        (r'^(if_cancer.*)$', 'if_cancer_related'),
        (r'^(lh_block)$', 'lh_block'),
        
        # Protocol & Stimulation
        (r'^(initial_date|data_do_procedimento)$', 'initial_date'),
        (r'^(date_when_embryos_were_cryopreserved.*|data_crio.*)$', 'date_cryopreserved'),
        (r'^(date_of_embryo_transfer.*|data_da_fet.*|data_et.*)$', 'date_of_embryo_transfer'),
        (r'^(day_of_embryo_transfer.*|dia_et.*)$', 'day_of_transfer'),
        (r'^(stage_at_embryo_transfer.*|stage_at_transfer.*)$', 'stage_at_transfer'),
        (r'^(number_of_embryos_transferred.*|no_et.*|n_et.*)$', 'number_of_embryos_transferred'),
        (r'^(preparation_for_embryo_transfer|preparo.*)$', 'preparation_for_transfer'),
        (r'^(condition.*)$', 'condition_at_transfer'),
        (r'^(fsh_total.*|total_fsh)$', 'fsh_total_dose'),
        (r'^(fsh)$', 'fsh'),
        (r'^(lh_total.*)$', 'lh_total_dose'),
        (r'^(lh)$', 'lh'),
        (r'^(gnrh.*)$', 'gnrh'),
        (r'^(oral)$', 'oral'),
        (r'^(follicular_phase)$', 'follicular_phase'),
        (r'^(luteal_phase|lutheal_fase)$', 'luteal_phase'),
        
        # Laboratory & Embryology
        (r'^(number_of_oocytes_retrieved|opu)$', 'oocytes_retrieved'),
        (r'^(number_of_mature_oocytes_mii|total_de_mii|mii)$', 'mature_oocytes_mii'),
        (r'^(number_of_immature_oocytes_mi)$', 'immature_oocytes_mi'),
        (r'^(number_of_germinal_vesicle_gv)$', 'germinal_vesicle_gv'),
        (r'^(fertilization_method|insemination.*|tipo_de_inseminacao)$', 'fertilization_method'),
        (r'^(number_of_fertilized_oocytes_2pn|fert_2pn|emb_fert)$', 'fertilized_2pn'),
        (r'^(number_of_cleaved_embryos)$', 'cleaved_embryos'),
        (r'^(number_of_blastocysts|qtd_blasto)$', 'blastocysts_total'),
        (r'^(number_of_embryos_biopsied.*|n_of_biopsied|no_biopsiados)$', 'pgt_biopsied'),
        (r'^(number_of_normal_embryos.*|n_of_normal|qtd_normais)$', 'pgt_normal'),
        (r'^(number_of_oocytes_thawed|o_c_descong)$', 'oocytes_thawed'),
        (r'^(number_of_oocytes_surviving.*|o_c_sobrev)$', 'oocytes_survived'),
        (r'^(number_of_embryos_thawed)$', 'embryos_thawed'),
        (r'^(number_of_embryos_surviving.*)$', 'embryos_survived'),
        (r'^(number_of_straws.*|no_de_palhetas.*|palheta)$', 'straws_vials_cryopreserved'),
        (r'^(sperm_source|origem)$', 'sperm_source'),
        (r'^(cryopreservation_method|meio_crio)$', 'cryopreservation_method'),
        (r'^(stage_at_cryopreservation.*|stage_of_embryo_development_at_freezing.*)$', 'stage_at_cryopreservation'),
        (r'^(number_of_fet_after_originally_frozen)$', 'number_of_fet_after_originally_frozen'),
        (r'^(pin_doadora|donor_pin)$', 'donor_pin'),
        (r'^(donor_age|idade_mulher)$', 'donor_age'),
        
        # Outcomes & Clinical Pregnancy (CRITICAL)
        (r'^(outcome_type|tipo_do_resultado)$', 'outcome_type'),
        (r'^(outcome|resultado|result)$', 'outcome'),
        (r'^(number_of_gestational_sac.*first.*|sg)$', 'gestational_sacs_first_us'),
        (r'^(number_of_gestational_sac.*second.*)$', 'gestational_sacs_second_us'),
        (r'^(number_of_newborn.*|no_nascidos)$', 'number_of_newborns'),
        (r'^(date_of_delivery|data_parto)$', 'date_of_delivery'),
        (r'^(gestat.*age_at_delivery|idd_gestacional)$', 'gestational_age_at_delivery'),
        (r'^(type_of_delivery|tipo_de_parto)$', 'type_of_delivery'),
        (r'^(baby_1.*weight|baby1_weight|peso_1)$', 'baby_1_weight'),
        (r'^(baby_1.*viability|baby1_viability)$', 'baby_1_viability'),
        (r'^(baby_1.*abnormality|baby1.*abnormality)$', 'baby_1_congenital_abnormality'),
        (r'^(baby_1.*citogenetic.*|baby1.*citogenetic.*|baby_1.*cytogenetic.*)$', 'baby_1_cytogenetic_study'),
        (r'^(baby_2.*weight|baby2_weight|peso_2)$', 'baby_2_weight'),
        (r'^(baby_2.*viability|baby2_viability)$', 'baby_2_viability'),
        (r'^(baby_2.*abnormality|baby2.*abnormality)$', 'baby_2_congenital_abnormality'),
        (r'^(baby_2.*citogenetic.*|baby2.*citogenetic.*|baby_2.*cytogenetic.*)$', 'baby_2_cytogenetic_study'),
        (r'^(baby_3.*weight|baby3_weight|peso_3)$', 'baby_3_weight'),
        (r'^(baby_3.*viability|baby3_viability)$', 'baby_3_viability'),
        (r'^(baby_3.*abnormality|baby3.*abnormality)$', 'baby_3_congenital_abnormality'),
        (r'^(baby_3.*citogenetic.*|baby3.*citogenetic.*|baby_3.*cytogenetic.*)$', 'baby_3_cytogenetic_study'),
        (r'^(baby_4.*weight|baby4_weight|peso_4)$', 'baby_4_weight'),
        (r'^(baby_4.*viability|baby4_viability)$', 'baby_4_viability'),
        (r'^(baby_4.*abnormality|baby4.*abnormality)$', 'baby_4_congenital_abnormality'),
        (r'^(baby_4.*citogenetic.*|baby4.*citogenetic.*|baby_4.*cytogenetic.*)$', 'baby_4_cytogenetic_study'),
        (r'^(complications_of_pregnancy_specify|complications.*)$', 'pregnancy_complications'),
        (r'^(obs.*)$', 'notes')
    ]
    
    for pat, target in mapping_rules:
        if re.match(pat, s):
            return target
            
    # Ensure identifier starts with a letter or underscore, not a digit
    if s and s[0].isdigit():
        s = f"col_{s}"
    if not s:
        s = "col"
        
    return s

def build_date_cast_sql(col_expr: str) -> str:
    """Generates robust DuckDB SQL expression to cast strings/serials/datetimes to DATE."""
    s_expr = f"CAST({col_expr} AS VARCHAR)"
    return f"""
    CASE 
        WHEN {col_expr} IS NULL OR TRIM({s_expr}) IN ('', '\\\\', '-', 'nan', 'None', '<NA>', 'null', 'NULL') THEN NULL
        WHEN {s_expr} ~ '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}' THEN TRY_CAST(SUBSTRING(TRIM({s_expr}), 1, 10) AS DATE)
        WHEN {s_expr} ~ '^[0-9]{{1,2}}/[0-9]{{1,2}}/[0-9]{{4}}' THEN TRY_STRPTIME(TRIM({s_expr}), '%d/%m/%Y')
        WHEN TRY_CAST({col_expr} AS DOUBLE) IS NOT NULL AND TRY_CAST({col_expr} AS DOUBLE) BETWEEN 30000 AND 60000 
        THEN CAST('1899-12-30' AS DATE) + to_days(CAST(TRY_CAST({col_expr} AS DOUBLE) AS INTEGER))
        ELSE TRY_CAST({s_expr} AS DATE)
    END
    """

def build_numeric_cast_sql(col_expr: str, is_integer: bool = False) -> str:
    """Generates DuckDB SQL expression to cast strings with commas/decimals to BIGINT or DOUBLE."""
    s_expr = f"CAST({col_expr} AS VARCHAR)"
    cleaned = f"NULLIF(TRIM(REPLACE(REPLACE({s_expr}, ',', '.'), ' ', '')), '')"
    target_type = "BIGINT" if is_integer else "DOUBLE"
    return f"TRY_CAST({cleaned} AS {target_type})"

def build_string_cast_sql(col_expr: str) -> str:
    """Cleans string values, converting null artifacts to NULL."""
    s_expr = f"CAST({col_expr} AS VARCHAR)"
    return f"""
    CASE 
        WHEN {col_expr} IS NULL OR TRIM({s_expr}) IN ('', '\\\\', '-', 'nan', 'None', '<NA>', 'null', 'NULL') THEN NULL
        ELSE TRIM({s_expr})
    END
    """

def get_column_data_type(canonical_col: str) -> str:
    """Determines target data type category for a canonical column."""
    if canonical_col in [
        'date_of_birth', 'initial_date', 'date_of_surgery', 'date_cryopreserved',
        'date_of_embryo_transfer', 'date_of_delivery', 'male_partner_dob'
    ] or canonical_col.startswith('date_'):
        return 'DATE'
        
    if canonical_col in [
        'oocytes_retrieved', 'mature_oocytes_mii', 'immature_oocytes_mi', 'germinal_vesicle_gv',
        'fertilized_2pn', 'cleaved_embryos', 'blastocysts_total', 'pgt_biopsied', 'pgt_normal',
        'oocytes_thawed', 'oocytes_survived', 'embryos_thawed', 'embryos_survived',
        'straws_vials_cryopreserved', 'number_of_embryos_transferred', 'day_of_transfer',
        'gestational_sacs_first_us', 'gestational_sacs_second_us', 'number_of_newborns',
        'male_partner_age', 'donor_age', 'gestational_age_at_delivery', 'line_number', 'year'
    ] or canonical_col.endswith('_count'):
        return 'BIGINT'
        
    if canonical_col in [
        'weight_kg', 'height_cm', 'fsh_total_dose', 'lh_total_dose',
        'baby_1_weight', 'baby_2_weight', 'baby_3_weight', 'baby_4_weight'
    ]:
        return 'DOUBLE'
        
    return 'VARCHAR'

def transform_stream(conn: duckdb.DuckDBPyConnection, stream: str):
    """Transforms all bronze tables for a procedure stream into a unified Silver table."""
    logger.info("-" * 60)
    logger.info(f"Transforming procedure stream: {stream.upper()}")
    
    # 1. Discover all bronze tables for this stream
    tables = [
        r[0] for r in conn.execute(
            f"SELECT table_name FROM information_schema.tables WHERE table_schema='bronze' AND table_name LIKE 'redlara_%_{stream}'"
        ).fetchall()
    ]
    
    if not tables:
        logger.warning(f"No bronze tables found for stream '{stream}'. Skipping.")
        return 0
        
    logger.info(f"Found {len(tables)} bronze tables for stream '{stream}'.")
    
    # 2. Collect all raw columns across all tables and determine canonical column inventory
    table_raw_cols = {}
    canonical_to_raw_by_table = defaultdict(lambda: defaultdict(list))
    all_canonical_cols = set()
    
    for t in tables:
        cols = [c[0] for c in conn.execute(f"SELECT * FROM bronze.{t} LIMIT 0").description]
        table_raw_cols[t] = cols
        for c in cols:
            if c in ['line_number', 'extraction_timestamp', 'file_name', 'sheet_name', 'unidade', 'year', 'procedure_stream']:
                continue
            canonical = get_canonical_name(c)
            canonical_to_raw_by_table[t][canonical].append(c)
            all_canonical_cols.add(canonical)
            
    # Priority ordered list of canonical columns
    primary_columns = [
        'chart_or_pin', 'patient_name', 'date_of_birth', 'doctor', 'relationship_status',
        'source_of_funding', 'weight_kg', 'height_cm', 'male_partner_age', 'male_partner_dob',
        'diagnosis_1', 'diagnosis_2', 'type_of_endometriosis', 'date_of_surgery', 'type_of_surgery',
        'reason_for_cryopreservation', 'if_cancer_related', 'initial_date', 'date_cryopreserved',
        'date_of_embryo_transfer', 'number_of_embryos_transferred', 'day_of_transfer',
        'stage_at_transfer', 'preparation_for_transfer', 'condition_at_transfer', 'fertilization_method',
        'oocytes_retrieved', 'mature_oocytes_mii', 'fertilized_2pn', 'blastocysts_total',
        'pgt_biopsied', 'pgt_normal', 'oocytes_thawed', 'oocytes_survived', 'embryos_thawed',
        'embryos_survived', 'straws_vials_cryopreserved', 'outcome', 'outcome_type',
        'gestational_sacs_first_us', 'gestational_sacs_second_us', 'number_of_newborns',
        'date_of_delivery', 'gestational_age_at_delivery', 'type_of_delivery',
        'baby_1_weight', 'baby_1_viability', 'baby_1_congenital_abnormality', 'baby_1_cytogenetic_study',
        'baby_2_weight', 'baby_2_viability', 'baby_2_congenital_abnormality', 'baby_2_cytogenetic_study',
        'baby_3_weight', 'baby_3_viability', 'baby_4_weight', 'baby_4_viability',
        'pregnancy_complications', 'notes'
    ]
    
    # Order: primary columns that exist in this stream, then any additional canonical columns alphabetically
    ordered_canonical = [c for c in primary_columns if c in all_canonical_cols]
    remaining_canonical = sorted(list(all_canonical_cols - set(ordered_canonical)))
    ordered_canonical.extend(remaining_canonical)
    
    logger.info(f"Mapped {len(ordered_canonical)} canonical columns across {len(tables)} tables for {stream.upper()}.")
    
    # 3. Construct SELECT query for each table
    select_queries = []
    for t in tables:
        raw_cols = table_raw_cols[t]
        t_canonical_map = canonical_to_raw_by_table[t]
        
        select_exprs = [
            f"md5(file_name || '_' || sheet_name || '_' || CAST(line_number AS VARCHAR)) AS row_id",
            f"file_name",
            f"sheet_name",
            f"line_number",
            f"unidade",
            f"year",
            f"'{t}' AS bronze_source_table"
        ]
        
        for col_name in ordered_canonical:
            raw_sources = t_canonical_map.get(col_name, [])
            col_type = get_column_data_type(col_name)
            
            if not raw_sources:
                select_exprs.append(f"CAST(NULL AS {col_type}) AS \"{col_name}\"")
            elif len(raw_sources) == 1:
                src_col = raw_sources[0]
                if col_type == 'DATE':
                    cast_sql = build_date_cast_sql(f'"{src_col}"')
                elif col_type == 'BIGINT':
                    cast_sql = build_numeric_cast_sql(f'"{src_col}"', is_integer=True)
                elif col_type == 'DOUBLE':
                    cast_sql = build_numeric_cast_sql(f'"{src_col}"', is_integer=False)
                else:
                    cast_sql = build_string_cast_sql(f'"{src_col}"')
                select_exprs.append(f"{cast_sql} AS \"{col_name}\"")
            else:
                # Coalesce multiple raw sources matching the same canonical column
                coalesce_terms = []
                for src_col in raw_sources:
                    if col_type == 'DATE':
                        cast_sql = build_date_cast_sql(f'"{src_col}"')
                    elif col_type == 'BIGINT':
                        cast_sql = build_numeric_cast_sql(f'"{src_col}"', is_integer=True)
                    elif col_type == 'DOUBLE':
                        cast_sql = build_numeric_cast_sql(f'"{src_col}"', is_integer=False)
                    else:
                        cast_sql = build_string_cast_sql(f'"{src_col}"')
                    coalesce_terms.append(cast_sql)
                select_exprs.append(f"COALESCE({', '.join(coalesce_terms)}) AS \"{col_name}\"")
                
        q = f"SELECT\n  " + ",\n  ".join(select_exprs) + f"\nFROM bronze.{t}"
        select_queries.append(q)
        
    union_query = "\nUNION ALL\n".join(select_queries)
    
    # 4. Wrap with derived pregnancy outcome indicators and create temporary view
    outcome_expr = '"outcome"' if "outcome" in ordered_canonical else "CAST(NULL AS VARCHAR)"
    outcome_type_expr = '"outcome_type"' if "outcome_type" in ordered_canonical else "CAST(NULL AS VARCHAR)"
    sacs_first_expr = '"gestational_sacs_first_us"' if "gestational_sacs_first_us" in ordered_canonical else "CAST(NULL AS BIGINT)"
    sacs_second_expr = '"gestational_sacs_second_us"' if "gestational_sacs_second_us" in ordered_canonical else "CAST(NULL AS BIGINT)"
    newborns_expr = '"number_of_newborns"' if "number_of_newborns" in ordered_canonical else "CAST(NULL AS BIGINT)"
    deliv_date_expr = '"date_of_delivery"' if "date_of_delivery" in ordered_canonical else "CAST(NULL AS DATE)"
    et_date_expr = '"date_of_embryo_transfer"' if "date_of_embryo_transfer" in ordered_canonical else "CAST(NULL AS DATE)"
    et_num_expr = '"number_of_embryos_transferred"' if "number_of_embryos_transferred" in ordered_canonical else "CAST(NULL AS BIGINT)"
    
    cte_sql = f"""
    WITH unified_raw AS (
        {union_query}
    ),
    with_derivations AS (
        SELECT 
            *,
            -- 1. Standard Clinical Pregnancy Derivation
            CASE
                WHEN COALESCE({sacs_first_expr}, 0) >= 1
                     OR COALESCE({sacs_second_expr}, 0) >= 1
                     OR COALESCE({newborns_expr}, 0) >= 1
                     OR {deliv_date_expr} IS NOT NULL
                     OR UPPER(TRIM(COALESCE({outcome_type_expr}, ''))) LIKE '%DELIVERY%'
                     OR UPPER(TRIM(COALESCE({outcome_expr}, ''))) LIKE '%DELIVERY%'
                     OR UPPER(TRIM(COALESCE({outcome_type_expr}, ''))) LIKE '%MISCARRIAGE%'
                     OR UPPER(TRIM(COALESCE({outcome_expr}, ''))) LIKE '%MISCARRIAGE%'
                     OR UPPER(TRIM(COALESCE({outcome_type_expr}, ''))) LIKE '%MISCARRIED%'
                     OR UPPER(TRIM(COALESCE({outcome_expr}, ''))) LIKE '%MISCARRIED%'
                     OR UPPER(TRIM(COALESCE({outcome_type_expr}, ''))) LIKE '%CLINICAL PREGNANCY%'
                     OR UPPER(TRIM(COALESCE({outcome_expr}, ''))) LIKE '%CLINICAL PREGNANCY%'
                     OR UPPER(TRIM(COALESCE({outcome_type_expr}, ''))) LIKE '%ECTOPIC%'
                     OR UPPER(TRIM(COALESCE({outcome_expr}, ''))) LIKE '%ECTOPIC%'
                     OR UPPER(TRIM(COALESCE({outcome_expr}, ''))) IN ('POSITIVO')
                THEN '1'
                
                WHEN (
                        UPPER(TRIM(COALESCE({outcome_expr}, ''))) LIKE '%TRANSFER%'
                        OR {et_date_expr} IS NOT NULL
                        OR COALESCE({et_num_expr}, 0) >= 1
                        OR UPPER(TRIM(COALESCE({outcome_expr}, ''))) IN ('NO PREGNANCY', 'NEGATIVO')
                     )
                     AND (
                        UPPER(TRIM(COALESCE({outcome_type_expr}, ''))) LIKE '%NO PREGNANCY%'
                        OR UPPER(TRIM(COALESCE({outcome_type_expr}, ''))) LIKE '%BIOCHEMICAL%'
                        OR UPPER(TRIM(COALESCE({outcome_expr}, ''))) LIKE '%BIOCHEMICAL%'
                        OR UPPER(TRIM(COALESCE({outcome_expr}, ''))) IN ('NO PREGNANCY', 'NEGATIVO')
                        OR UPPER(TRIM(COALESCE({outcome_type_expr}, ''))) IN ('SEM CONTATO', 'SEM RESPOSTA', 'RECUSA')
                     )
                THEN '0'
                
                ELSE NULL
            END AS clinical_pregnancy,
            
            -- 2. Biochemical Pregnancy Derivation
            CASE
                WHEN UPPER(TRIM(COALESCE({outcome_type_expr}, ''))) LIKE '%BIOCHEMICAL%'
                     OR UPPER(TRIM(COALESCE({outcome_expr}, ''))) LIKE '%BIOCHEMICAL%'
                THEN '1'
                WHEN (
                        UPPER(TRIM(COALESCE({outcome_expr}, ''))) LIKE '%TRANSFER%'
                        OR {et_date_expr} IS NOT NULL
                        OR COALESCE({et_num_expr}, 0) >= 1
                        OR UPPER(TRIM(COALESCE({outcome_expr}, ''))) IN ('NO PREGNANCY', 'NEGATIVO')
                     )
                THEN '0'
                ELSE NULL
            END AS biochemical_pregnancy,
            
            -- 3. Delivery Occurred Derivation
            CASE
                WHEN {deliv_date_expr} IS NOT NULL
                     OR COALESCE({newborns_expr}, 0) >= 1
                     OR UPPER(TRIM(COALESCE({outcome_type_expr}, ''))) LIKE '%DELIVERY%'
                     OR UPPER(TRIM(COALESCE({outcome_expr}, ''))) LIKE '%DELIVERY%'
                THEN '1'
                WHEN (
                        UPPER(TRIM(COALESCE({outcome_expr}, ''))) LIKE '%TRANSFER%'
                        OR {et_date_expr} IS NOT NULL
                        OR COALESCE({et_num_expr}, 0) >= 1
                     )
                THEN '0'
                ELSE NULL
            END AS delivery_occurred,
            
            -- 4. Initial placeholder for prontuario (Strategy L resolution)
            CAST(NULL AS BIGINT) AS prontuario
            
        FROM unified_raw
    )
    SELECT * FROM with_derivations
    """
    
    target_table = f"silver.redlara_{stream}"
    logger.info(f"Creating table {target_table}...")
    conn.execute(f"CREATE OR REPLACE TABLE {target_table} AS {cte_sql}")
    
    row_count = conn.execute(f"SELECT COUNT(*) FROM {target_table}").fetchone()[0]
    col_count = len(conn.execute(f"SELECT * FROM {target_table} LIMIT 0").description)
    logger.info(f"Created {target_table}: {row_count:,} rows, {col_count} columns.")
    
    # 5. Run Strategy L Prontuario Matching
    if CLINISYS_PATH.exists():
        logger.info(f"Running Strategy L Prontuario matching on {target_table}...")
        try:
            df_matches = find_prontuarios(
                source_con=conn,
                clinisys_db_path=str(CLINISYS_PATH),
                source_schema='silver',
                source_table=f'redlara_{stream}',
                id_col='chart_or_pin',
                name_col='patient_name' if 'patient_name' in ordered_canonical else None,
                birthdate_col='date_of_birth' if 'date_of_birth' in ordered_canonical else None,
                cpf_col=None,
                label=f'redlara_{stream}',
                suffix=''
            )
            total = len(df_matches)
            matched = int((df_matches['prontuario'] != -1).sum()) if 'prontuario' in df_matches.columns else 0
            rate = matched / total * 100 if total else 0.0
            logger.info(f"  Prontuario matching for {target_table}: {matched:,} / {total:,} ({rate:.2f}%)")
        except Exception as e:
            logger.error(f"  Error in prontuario matching for {target_table}: {e}", exc_info=True)
    else:
        logger.warning(f"Clinisys DB not found at {CLINISYS_PATH}. Skipping prontuario matching.")
        
    return row_count

def get_duckdb_connection(db_path: Path, max_retries: int = 8, retry_delay: float = 2.0):
    import time
    for attempt in range(1, max_retries + 1):
        try:
            return duckdb.connect(str(db_path))
        except Exception as e:
            if attempt < max_retries:
                logger.warning(f"DuckDB connection attempt {attempt} failed ({e}). Retrying in {retry_delay}s...")
                time.sleep(retry_delay)
            else:
                logger.error(f"DuckDB connection failed after {max_retries} attempts: {e}")
                raise

def run_silver_pipeline():
    """Main Silver transformation orchestrator."""
    logger.info("=" * 70)
    logger.info("Starting REDLARA Silver Transformation Pipeline")
    logger.info(f"Database: {DB_PATH}")
    logger.info("=" * 70)
    
    conn = get_duckdb_connection(DB_PATH)
    conn.execute("CREATE SCHEMA IF NOT EXISTS silver")
    
    total_silver_rows = 0
    stream_results = {}
    
    for stream in STREAMS:
        rows = transform_stream(conn, stream)
        stream_results[stream] = rows
        total_silver_rows += rows
        
    conn.close()
    
    logger.info("=" * 70)
    logger.info("REDLARA Silver Transformation Completed Successfully")
    logger.info(f"Total Silver Rows Created across 6 tables: {total_silver_rows:,}")
    for stream, count in stream_results.items():
        logger.info(f"  silver.redlara_{stream:<8}: {count:>6,} rows")
    logger.info("=" * 70)

if __name__ == "__main__":
    run_silver_pipeline()
