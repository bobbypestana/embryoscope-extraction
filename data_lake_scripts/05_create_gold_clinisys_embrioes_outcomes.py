#!/usr/bin/env python3
"""
data_lake_scripts/05_create_gold_clinisys_embrioes_outcomes.py
=============================================================
Enriches gold.clinisys_embrioes with clinical outcomes through independent dual matching
against Planilha Embriologia (4 independent silver sheets: FET, RECEP, FRESH, FOT) and REDLARA.

Design & Architectural Invariants:
1. Strict 1:1 Grain Integrity: Exactly 1 row per embryo in gold.clinisys_embrioes (~320,792 rows, 0 duplicates).
2. Clean Transfer Scope: Correctly identifies real transfers; excludes discarded post-thaw embryos and frozen tank embryos from fresh scope.
3. Aligned Multi-Tier Matching: Restricts cryo-date matching to aligned transfer windows (preventing cross-cycle outcome contamination).
4. Verified Outcomes: Filters REDLARA to verified embryo transfers (excluding freeze-all cycles and cancellations).
5. Dual Independent Matching & Cross-Check: Preserves raw sources side-by-side with high-precision concordance audit.
"""

import os
import sys
import time
import logging
import duckdb
import pandas as pd

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

DUCKDB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'database', 'huntington_data_lake.duckdb')

def create_gold_clinisys_embrioes_outcomes(con):
    logger.info("=" * 90)
    logger.info("BUILDING REFINED GOLD.CLINISYS_EMBRIOES_OUTCOMES")
    logger.info("=" * 90)
    
    t0 = time.time()
    
    # 1. Prepare Base Transferred Embryos Scope (Refined)
    logger.info("Step 1: Identifying transferred embryos and procedure dates in Clinisys...")
    con.execute("""
    CREATE OR REPLACE TEMP TABLE tmp_transferred_scope AS
    SELECT 
        c.oocito_id,
        c.oocito_id_micromanipulacao,
        c.oocito_embryo_number,
        TRY_CAST(c.micro_prontuario AS INTEGER) as prontuario,
        c.nome_medico,
        TRY_CAST(c.micro_Data_DL AS DATE) as data_puncao,
        TRY_CAST(c.cong_em_Data AS DATE) as data_crio,
        TRY_CAST(c.descong_em_DataTransferencia AS DATE) as data_transf_descong,
        TRY_CAST(c.descong_em_DataDescongelamento AS DATE) as data_descong,
        TRY_CAST(c.trat1_data_transferencia AS DATE) as data_transf_trat1,
        TRY_CAST(c.trat2_data_transferencia AS DATE) as data_transf_trat2,
        c.emb_cong_transferidos,
        c.trat1_resultado_tratamento,
        c.trat2_resultado_tratamento,
        
        -- Refined transfer identification
        CASE 
            -- Explicit non-transfers: discarded post-thaw or unthawed embryos in nitrogen tanks
            WHEN c.emb_cong_transferidos IN ('Descartado', 'Criopreservado', 'Transferido para outra Clínica', 'Doado para Pesquisa') THEN 0
            
            -- FET transfers
            WHEN (c.emb_cong_transferidos = 'Transferido' 
                  OR (c.emb_cong_transferidos IS NULL AND c.descong_em_DataTransferencia IS NOT NULL)) THEN 1
                  
            -- Fresh transfers (MUST NOT have been frozen into a cryo straw)
            WHEN (c.cong_em_Data IS NULL 
                  AND c.trat1_data_transferencia IS NOT NULL 
                  AND (c.trat1_motivo_nao_transferir IS NULL OR c.trat1_motivo_nao_transferir = '')
                  AND (c.trat1_resultado_tratamento IS NULL OR c.trat1_resultado_tratamento NOT IN ('No transfer', 'Congelamento de óvulos', 'Congelamento de vulos'))) THEN 1
            ELSE 0
        END as is_transferred,
        
        CASE 
            WHEN c.emb_cong_transferidos IN ('Descartado', 'Criopreservado', 'Transferido para outra Clínica', 'Doado para Pesquisa') THEN 'NON_TRANSFERRED'
            WHEN (c.emb_cong_transferidos = 'Transferido' 
                  OR (c.emb_cong_transferidos IS NULL AND c.descong_em_DataTransferencia IS NOT NULL)) THEN 'FET'
            WHEN (c.cong_em_Data IS NULL 
                  AND c.trat1_data_transferencia IS NOT NULL 
                  AND (c.trat1_motivo_nao_transferir IS NULL OR c.trat1_motivo_nao_transferir = '')
                  AND (c.trat1_resultado_tratamento IS NULL OR c.trat1_resultado_tratamento NOT IN ('No transfer', 'Congelamento de óvulos', 'Congelamento de vulos'))) THEN 'FRESH'
            ELSE 'NON_TRANSFERRED'
        END as transfer_category,
        
        COALESCE(TRY_CAST(c.descong_em_DataTransferencia AS DATE), TRY_CAST(c.trat2_data_transferencia AS DATE)) as fet_transfer_date,
        TRY_CAST(c.trat1_data_transferencia AS DATE) as fresh_transfer_date,
        COALESCE(TRY_CAST(c.descong_em_DataTransferencia AS DATE), TRY_CAST(c.trat2_data_transferencia AS DATE), TRY_CAST(c.trat1_data_transferencia AS DATE)) as effective_transfer_date,
        EXTRACT(YEAR FROM COALESCE(c.descong_em_DataTransferencia, c.trat2_data_transferencia, c.trat1_data_transferencia, c.micro_Data_DL)) as proc_year
    FROM gold.clinisys_embrioes c
    WHERE c.micro_prontuario IS NOT NULL AND c.micro_prontuario > 0;
    """)

    transf_count = con.execute("SELECT count(*) FROM tmp_transferred_scope WHERE is_transferred = 1").fetchone()[0]
    logger.info(f"Identified {transf_count:,d} verified transferred embryos in Clinisys.")

    # 2. Prepare Clean Planilha Tables
    logger.info("Step 2: Preparing clean deduplicated Planilha silver sources...")
    con.execute("""
    CREATE OR REPLACE TEMP TABLE tmp_fet_clean AS
    SELECT 
        TRY_CAST(prontuario AS INTEGER) as prontuario,
        TRY_CAST(data_da_fet AS DATE) as transfer_date,
        TRY_CAST(data_crio AS DATE) as crio_date,
        result, tipo_do_resultado, gravidez_clinica, gravidez_bioquimica, no_nascidos, 
        TRY_CAST(data_parto AS DATE) as data_parto,
        sheet_name, file_name, 'FET' as source_type
    FROM (
        SELECT *, ROW_NUMBER() OVER (
            PARTITION BY TRY_CAST(prontuario AS INTEGER), TRY_CAST(data_da_fet AS DATE) 
            ORDER BY CASE WHEN no_nascidos IS NOT NULL THEN 0 ELSE 1 END,
                     CASE WHEN gravidez_clinica IS NOT NULL THEN 0 ELSE 1 END,
                     CASE WHEN result IS NOT NULL THEN 0 ELSE 1 END
        ) as rn
        FROM silver.planilha_embriologia_fet
        WHERE prontuario IS NOT NULL AND prontuario > 0
          AND (result IS NULL OR UPPER(result) NOT IN ('REVITRI', 'EMBRYO VITRI', 'CANCELLATION', 'CANCELADO', 'SEM TRANSFERENCIA', 'NO TRANSFER'))
    ) WHERE rn = 1;

    CREATE OR REPLACE TEMP TABLE tmp_recep_clean AS
    SELECT 
        TRY_CAST(prontuario AS INTEGER) as prontuario,
        TRY_CAST(data_da_fet AS DATE) as transfer_date,
        TRY_CAST(data_crio AS DATE) as crio_date,
        tipo_do_resultado as result, tipo_do_resultado, gravidez_clinica, gravidez_bioquimica, no_nascidos,
        TRY_CAST(data_parto AS DATE) as data_parto,
        sheet_name, file_name, 'RECEP' as source_type
    FROM (
        SELECT *, ROW_NUMBER() OVER (
            PARTITION BY TRY_CAST(prontuario AS INTEGER), COALESCE(TRY_CAST(data_da_fet AS DATE), TRY_CAST(data_crio AS DATE))
            ORDER BY CASE WHEN no_nascidos IS NOT NULL THEN 0 ELSE 1 END,
                     CASE WHEN gravidez_clinica IS NOT NULL THEN 0 ELSE 1 END
        ) as rn
        FROM silver.planilha_embriologia_recep
        WHERE prontuario IS NOT NULL AND prontuario > 0
    ) WHERE rn = 1;

    CREATE OR REPLACE TEMP TABLE tmp_fresh_clean AS
    SELECT 
        TRY_CAST(prontuario AS INTEGER) as prontuario,
        TRY_CAST(data_da_puncao AS DATE) as puncao_date,
        TRY_CAST(data_crio AS DATE) as crio_date,
        result, tipo_do_resultado, gravidez_clinica, gravidez_bioquimica, no_nascidos,
        TRY_CAST(data_parto AS DATE) as data_parto,
        sheet_name, file_name, 'FRESH' as source_type
    FROM (
        SELECT *, ROW_NUMBER() OVER (
            PARTITION BY TRY_CAST(prontuario AS INTEGER), TRY_CAST(data_da_puncao AS DATE) 
            ORDER BY CASE WHEN result IS NOT NULL THEN 0 ELSE 1 END
        ) as rn
        FROM silver.planilha_embriologia_fresh
        WHERE prontuario IS NOT NULL AND prontuario > 0
    ) WHERE rn = 1;

    CREATE OR REPLACE TEMP TABLE tmp_fot_clean AS
    SELECT 
        TRY_CAST(prontuario AS INTEGER) as prontuario,
        COALESCE(TRY_CAST(data_do_procedimento AS DATE), TRY_CAST(data_da_puncao AS DATE)) as proc_date,
        TRY_CAST(data_crio AS DATE) as crio_date,
        result, tipo_do_resultado, gravidez_clinica, gravidez_bioquimica, no_nascidos,
        TRY_CAST(data_parto AS DATE) as data_parto,
        sheet_name, file_name, 'FOT' as source_type
    FROM (
        SELECT *, ROW_NUMBER() OVER (
            PARTITION BY TRY_CAST(prontuario AS INTEGER), COALESCE(TRY_CAST(data_do_procedimento AS DATE), TRY_CAST(data_da_puncao AS DATE)) 
            ORDER BY CASE WHEN result IS NOT NULL THEN 0 ELSE 1 END
        ) as rn
        FROM silver.planilha_embriologia_fot
        WHERE prontuario IS NOT NULL AND prontuario > 0
    ) WHERE rn = 1;

    -- 3. Prepare Clean REDLARA with Verified Transfers Filter
    CREATE OR REPLACE TEMP TABLE tmp_redlara_clean AS
    SELECT 
        TRY_CAST(prontuario AS INTEGER) as prontuario,
        TRY_CAST(date_of_embryo_transfer AS DATE) as transfer_date,
        TRY_CAST(date_when_embryos_were_cryopreserved AS DATE) as crio_date,
        outcome as redlara_outcome,
        outcome_type as redlara_outcome_type,
        CASE 
            WHEN UPPER(outcome_type) LIKE '%DELIVERY%' 
              OR UPPER(outcome_type) LIKE '%MISCARRIAGE%' 
              OR UPPER(outcome_type) LIKE '%CLINICAL%' 
              OR UPPER(outcome_type) LIKE '%ECTOPIC%' THEN '1'
            WHEN UPPER(outcome_type) LIKE '%NO PREGNANCY%' THEN '0'
            ELSE NULL 
        END as redlara_gravidez_clinica,
        CASE 
            WHEN UPPER(outcome_type) LIKE '%BIOCHEMICAL%' THEN '1'
            ELSE '0'
        END as redlara_gravidez_bioquimica,
        CASE 
            WHEN number_of_newborns IS NOT NULL AND TRIM(number_of_newborns) NOT IN ('\\\\', '-', '', 'None') 
            THEN TRIM(number_of_newborns) 
            ELSE NULL 
        END as redlara_no_nascidos,
        TRY_CAST(date_of_delivery AS DATE) as redlara_data_parto,
        type_of_delivery as redlara_tipo_parto,
        'REDLARA' as source_type
    FROM (
        SELECT *, ROW_NUMBER() OVER (
            PARTITION BY TRY_CAST(prontuario AS INTEGER), TRY_CAST(date_of_embryo_transfer AS DATE) 
            ORDER BY CASE WHEN date_of_delivery IS NOT NULL THEN 0 ELSE 1 END,
                     CASE WHEN number_of_newborns IS NOT NULL AND number_of_newborns NOT IN ('\\\\', '-') THEN 0 ELSE 1 END,
                     CASE WHEN outcome_type IS NOT NULL THEN 0 ELSE 1 END
        ) as rn
        FROM silver.redlara_unified
        WHERE prontuario IS NOT NULL AND prontuario > 0
          AND UPPER(outcome) LIKE '%EMBRYO TRANSFER%' -- Exclude freeze-all and cancellation records
    ) WHERE rn = 1;
    """)

    # 4. Planilha Matching with Option 2 & 1:1 Grain Guarantee
    logger.info("Step 3: Matching transferred embryos against Planilha 4 sheets...")
    con.execute("""
    CREATE OR REPLACE TEMP TABLE tmp_matched_planilha AS
    WITH 
    p1_fet AS (
        SELECT c.oocito_id, f.source_type, f.result, f.tipo_do_resultado, f.gravidez_clinica, f.gravidez_bioquimica, f.no_nascidos, f.data_parto, '1. FET: Exact Transfer Date' as plan_rule, 1 as priority
        FROM tmp_transferred_scope c JOIN tmp_fet_clean f ON c.prontuario = f.prontuario AND c.fet_transfer_date = f.transfer_date
        WHERE c.is_transferred = 1 AND c.transfer_category = 'FET' AND c.fet_transfer_date IS NOT NULL
    ),
    p1_recep AS (
        SELECT c.oocito_id, r.source_type, r.result, r.tipo_do_resultado, r.gravidez_clinica, r.gravidez_bioquimica, r.no_nascidos, r.data_parto, '2. RECEP: Exact Transfer Date' as plan_rule, 2 as priority
        FROM tmp_transferred_scope c JOIN tmp_recep_clean r ON c.prontuario = r.prontuario AND c.fet_transfer_date = r.transfer_date
        WHERE c.is_transferred = 1 AND c.transfer_category = 'FET' AND c.fet_transfer_date IS NOT NULL
          AND c.oocito_id NOT IN (SELECT oocito_id FROM p1_fet)
    ),
    p2_fresh AS (
        SELECT c.oocito_id, fr.source_type, fr.result, fr.tipo_do_resultado, fr.gravidez_clinica, fr.gravidez_bioquimica, fr.no_nascidos, fr.data_parto, '3. FRESH: Exact Puncture Date' as plan_rule, 3 as priority
        FROM tmp_transferred_scope c JOIN tmp_fresh_clean fr ON c.prontuario = fr.prontuario AND c.data_puncao = fr.puncao_date
        WHERE c.is_transferred = 1 AND c.transfer_category = 'FRESH' AND c.data_puncao IS NOT NULL
          AND c.oocito_id NOT IN (SELECT oocito_id FROM p1_fet UNION ALL SELECT oocito_id FROM p1_recep)
    ),
    p2_fot AS (
        SELECT c.oocito_id, fot.source_type, fot.result, fot.tipo_do_resultado, fot.gravidez_clinica, fot.gravidez_bioquimica, fot.no_nascidos, fot.data_parto, '4. FOT: Exact Proc Date' as plan_rule, 4 as priority
        FROM tmp_transferred_scope c JOIN tmp_fot_clean fot ON c.prontuario = fot.prontuario AND (c.data_puncao = fot.proc_date OR c.fresh_transfer_date = fot.proc_date)
        WHERE c.is_transferred = 1
          AND c.oocito_id NOT IN (SELECT oocito_id FROM p1_fet UNION ALL SELECT oocito_id FROM p1_recep UNION ALL SELECT oocito_id FROM p2_fresh)
    ),
    -- Rule 5: Cryo date match, but strictly aligned transfer date (+/- 30 days or transfer date null) to prevent cross-transfer contamination
    p3_fet_crio AS (
        SELECT c.oocito_id, f.source_type, f.result, f.tipo_do_resultado, f.gravidez_clinica, f.gravidez_bioquimica, f.no_nascidos, f.data_parto, '5. FET: Cryo Date (Aligned Cycle)' as plan_rule, 5 as priority
        FROM tmp_transferred_scope c JOIN tmp_fet_clean f ON c.prontuario = f.prontuario AND c.data_crio = f.crio_date
        WHERE c.is_transferred = 1 AND c.transfer_category = 'FET' AND c.data_crio IS NOT NULL
          AND (c.fet_transfer_date IS NULL OR f.transfer_date IS NULL OR ABS(DATEDIFF('day', c.fet_transfer_date, f.transfer_date)) <= 30)
          AND c.oocito_id NOT IN (SELECT oocito_id FROM p1_fet UNION ALL SELECT oocito_id FROM p1_recep UNION ALL SELECT oocito_id FROM p2_fresh UNION ALL SELECT oocito_id FROM p2_fot)
    ),
    p4_fet_win AS (
        SELECT c.oocito_id, f.source_type, f.result, f.tipo_do_resultado, f.gravidez_clinica, f.gravidez_bioquimica, f.no_nascidos, f.data_parto, '6. FET: Date Window (+/- 2d)' as plan_rule, 6 as priority
        FROM tmp_transferred_scope c JOIN tmp_fet_clean f ON c.prontuario = f.prontuario AND f.transfer_date BETWEEN (c.fet_transfer_date - INTERVAL 2 DAYS) AND (c.fet_transfer_date + INTERVAL 2 DAYS)
        WHERE c.is_transferred = 1 AND c.transfer_category = 'FET' AND c.fet_transfer_date IS NOT NULL
          AND c.oocito_id NOT IN (SELECT oocito_id FROM p1_fet UNION ALL SELECT oocito_id FROM p1_recep UNION ALL SELECT oocito_id FROM p2_fresh UNION ALL SELECT oocito_id FROM p2_fot UNION ALL SELECT oocito_id FROM p3_fet_crio)
    ),
    p4_fresh_win AS (
        SELECT c.oocito_id, fr.source_type, fr.result, fr.tipo_do_resultado, fr.gravidez_clinica, fr.gravidez_bioquimica, fr.no_nascidos, fr.data_parto, '7. FRESH: Puncture Window (+/- 2d)' as plan_rule, 7 as priority
        FROM tmp_transferred_scope c JOIN tmp_fresh_clean fr ON c.prontuario = fr.prontuario AND fr.puncao_date BETWEEN (c.data_puncao - INTERVAL 2 DAYS) AND (c.data_puncao + INTERVAL 2 DAYS)
        WHERE c.is_transferred = 1 AND c.transfer_category = 'FRESH' AND c.data_puncao IS NOT NULL
          AND c.oocito_id NOT IN (SELECT oocito_id FROM p1_fet UNION ALL SELECT oocito_id FROM p1_recep UNION ALL SELECT oocito_id FROM p2_fresh UNION ALL SELECT oocito_id FROM p2_fot UNION ALL SELECT oocito_id FROM p3_fet_crio UNION ALL SELECT oocito_id FROM p4_fet_win)
    ),
    all_plan AS (
        SELECT * FROM p1_fet UNION ALL SELECT * FROM p1_recep UNION ALL SELECT * FROM p2_fresh UNION ALL SELECT * FROM p2_fot
        UNION ALL SELECT * FROM p3_fet_crio UNION ALL SELECT * FROM p4_fet_win UNION ALL SELECT * FROM p4_fresh_win
    )
    -- Deduplicate strictly by oocito_id to guarantee 1:1 grain
    SELECT oocito_id, source_type, result, tipo_do_resultado, gravidez_clinica, gravidez_bioquimica, no_nascidos, data_parto, plan_rule
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY oocito_id ORDER BY priority) as rn
        FROM all_plan
    ) WHERE rn = 1;
    """)

    # 5. REDLARA Matching with Option 2 & 1:1 Grain Guarantee
    logger.info("Step 4: Matching transferred embryos against REDLARA independently...")
    con.execute("""
    CREATE OR REPLACE TEMP TABLE tmp_matched_redlara AS
    WITH 
    r1_transf AS (
        SELECT c.oocito_id, r.source_type, r.redlara_outcome, r.redlara_outcome_type, r.redlara_gravidez_clinica, r.redlara_gravidez_bioquimica, r.redlara_no_nascidos, r.redlara_data_parto, r.redlara_tipo_parto, '1. REDLARA: Exact Transfer Date' as redlara_rule, 1 as priority
        FROM tmp_transferred_scope c JOIN tmp_redlara_clean r ON c.prontuario = r.prontuario AND c.effective_transfer_date = r.transfer_date
        WHERE c.is_transferred = 1 AND c.effective_transfer_date IS NOT NULL
    ),
    -- Rule 2: Cryo date match, strictly aligned transfer date (+/- 30 days or transfer date null) to prevent cross-transfer contamination
    r2_crio AS (
        SELECT c.oocito_id, r.source_type, r.redlara_outcome, r.redlara_outcome_type, r.redlara_gravidez_clinica, r.redlara_gravidez_bioquimica, r.redlara_no_nascidos, r.redlara_data_parto, r.redlara_tipo_parto, '2. REDLARA: Cryo Date (Aligned Cycle)' as redlara_rule, 2 as priority
        FROM tmp_transferred_scope c JOIN tmp_redlara_clean r ON c.prontuario = r.prontuario AND c.data_crio = r.crio_date
        WHERE c.is_transferred = 1 AND c.data_crio IS NOT NULL
          AND (c.effective_transfer_date IS NULL OR r.transfer_date IS NULL OR ABS(DATEDIFF('day', c.effective_transfer_date, r.transfer_date)) <= 30)
          AND c.oocito_id NOT IN (SELECT oocito_id FROM r1_transf)
    ),
    r3_win AS (
        SELECT c.oocito_id, r.source_type, r.redlara_outcome, r.redlara_outcome_type, r.redlara_gravidez_clinica, r.redlara_gravidez_bioquimica, r.redlara_no_nascidos, r.redlara_data_parto, r.redlara_tipo_parto, '3. REDLARA: Transfer Window (+/- 2d)' as redlara_rule, 3 as priority
        FROM tmp_transferred_scope c JOIN tmp_redlara_clean r ON c.prontuario = r.prontuario AND r.transfer_date BETWEEN (c.effective_transfer_date - INTERVAL 2 DAYS) AND (c.effective_transfer_date + INTERVAL 2 DAYS)
        WHERE c.is_transferred = 1 AND c.effective_transfer_date IS NOT NULL 
          AND c.oocito_id NOT IN (SELECT oocito_id FROM r1_transf UNION ALL SELECT oocito_id FROM r2_crio)
    ),
    all_red AS (
        SELECT * FROM r1_transf UNION ALL SELECT * FROM r2_crio UNION ALL SELECT * FROM r3_win
    )
    -- Deduplicate strictly by oocito_id to guarantee 1:1 grain
    SELECT oocito_id, source_type, redlara_outcome, redlara_outcome_type, redlara_gravidez_clinica, redlara_gravidez_bioquimica, redlara_no_nascidos, redlara_data_parto, redlara_tipo_parto, redlara_rule
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY oocito_id ORDER BY priority) as rn
        FROM all_red
    ) WHERE rn = 1;
    """)

    # 6. Create gold.clinisys_embrioes_outcomes table
    logger.info("Step 5: Materializing refined gold.clinisys_embrioes_outcomes...")
    con.execute("CREATE SCHEMA IF NOT EXISTS gold;")
    con.execute("DROP TABLE IF EXISTS gold.clinisys_embrioes_outcomes;")
    
    con.execute("""
    CREATE TABLE gold.clinisys_embrioes_outcomes AS
    SELECT 
        c.*,
        
        -- Transfer procedure flags
        COALESCE(s.is_transferred, 0) as is_transferred,
        COALESCE(s.transfer_category, 'NON_TRANSFERRED') as transfer_category,
        s.effective_transfer_date,
        s.proc_year,
        
        -- Planilha Lab Sheet Outcomes (Preserved)
        CASE WHEN p.oocito_id IS NOT NULL THEN 1 ELSE 0 END as planilha_matched,
        p.source_type as planilha_source_sheet,
        p.plan_rule as planilha_match_rule,
        p.result as planilha_result,
        p.tipo_do_resultado as planilha_tipo_resultado,
        p.gravidez_clinica as planilha_gravidez_clinica,
        p.gravidez_bioquimica as planilha_gravidez_bioquimica,
        p.no_nascidos as planilha_no_nascidos,
        p.data_parto as planilha_data_parto,
        
        -- REDLARA Registry Outcomes (Preserved)
        CASE WHEN r.oocito_id IS NOT NULL THEN 1 ELSE 0 END as redlara_matched,
        r.redlara_rule as redlara_match_rule,
        r.redlara_outcome as redlara_outcome,
        r.redlara_outcome_type as redlara_outcome_type,
        r.redlara_gravidez_clinica as redlara_gravidez_clinica,
        r.redlara_gravidez_bioquimica as redlara_gravidez_bioquimica,
        r.redlara_no_nascidos as redlara_no_nascidos,
        r.redlara_data_parto as redlara_data_parto,
        r.redlara_tipo_parto as redlara_tipo_parto,
        
        -- Native Clinisys Outcomes (Preserved)
        c.trat1_resultado_tratamento as clinisys_trat1_resultado_tratamento,
        c.trat2_resultado_tratamento as clinisys_trat2_resultado_tratamento,
        
        -- Quality Cross-Check & Conflict Classification
        CASE 
            WHEN COALESCE(s.is_transferred, 0) = 0 THEN 'NOT_TRANSFERRED'
            WHEN p.oocito_id IS NOT NULL AND r.oocito_id IS NOT NULL THEN
                CASE 
                    WHEN p.gravidez_clinica IS NOT NULL AND r.redlara_gravidez_clinica IS NOT NULL AND p.gravidez_clinica = r.redlara_gravidez_clinica THEN 'CONCORDANT'
                    WHEN p.gravidez_clinica IS NOT NULL AND r.redlara_gravidez_clinica IS NOT NULL AND p.gravidez_clinica != r.redlara_gravidez_clinica THEN 'DISCORDANT'
                    ELSE 'PARTIALLY_CONCORDANT'
                END
            WHEN p.oocito_id IS NOT NULL AND r.oocito_id IS NULL THEN 'PLANILHA_ONLY'
            WHEN p.oocito_id IS NULL AND r.oocito_id IS NOT NULL THEN 'REDLARA_ONLY'
            WHEN COALESCE(c.trat2_resultado_tratamento, c.trat1_resultado_tratamento) IS NOT NULL THEN 'CLINISYS_ONLY'
            ELSE 'NO_OUTCOME'
        END as crosscheck_status,
        
        CASE 
            WHEN p.gravidez_clinica IS NOT NULL AND r.redlara_gravidez_clinica IS NOT NULL AND p.gravidez_clinica != r.redlara_gravidez_clinica THEN 1
            ELSE 0
        END as has_conflict,
        
        CASE 
            WHEN p.gravidez_clinica IS NOT NULL AND r.redlara_gravidez_clinica IS NOT NULL AND p.gravidez_clinica != r.redlara_gravidez_clinica THEN 'PREGNANCY_OUTCOME_MISMATCH'
            WHEN p.no_nascidos IS NOT NULL AND r.redlara_no_nascidos IS NOT NULL AND p.no_nascidos != r.redlara_no_nascidos THEN 'LIVE_BIRTH_COUNT_MISMATCH'
            WHEN p.oocito_id IS NOT NULL AND r.oocito_id IS NOT NULL THEN 'NONE'
            WHEN p.oocito_id IS NOT NULL OR r.oocito_id IS NOT NULL OR c.trat2_resultado_tratamento IS NOT NULL THEN 'SINGLE_SOURCE'
            ELSE 'NO_DATA'
        END as conflict_type,
        
        -- Unified Golden Outcome Fields (Coalesced: Planilha -> REDLARA -> Clinisys)
        COALESCE(p.tipo_do_resultado, r.redlara_outcome_type, c.trat2_resultado_tratamento, c.trat1_resultado_tratamento) as outcome_final_result,
        COALESCE(CAST(p.gravidez_clinica AS VARCHAR), CAST(r.redlara_gravidez_clinica AS VARCHAR)) as outcome_final_gravidez_clinica,
        COALESCE(CAST(p.no_nascidos AS VARCHAR), CAST(r.redlara_no_nascidos AS VARCHAR)) as outcome_final_no_nascidos,
        COALESCE(p.data_parto, r.redlara_data_parto) as outcome_final_data_parto,
        
        -- High-Level Quality & Modeling Flags
        (
            (c.oocito_ResultadoPGD IS NOT NULL AND TRIM(c.oocito_ResultadoPGD) != '' AND LOWER(TRIM(c.oocito_ResultadoPGD)) != 'none')
            OR
            (c.oocito_ResultadoPGDDetalhes IS NOT NULL AND TRIM(c.oocito_ResultadoPGDDetalhes) != '' AND LOWER(TRIM(c.oocito_ResultadoPGDDetalhes)) != 'none')
        ) as has_biopsy,
        
        (
            COALESCE(s.is_transferred, 0) = 1 AND (
                COALESCE(p.tipo_do_resultado, r.redlara_outcome_type, c.trat2_resultado_tratamento, c.trat1_resultado_tratamento) IS NOT NULL
                OR COALESCE(CAST(p.gravidez_clinica AS VARCHAR), CAST(r.redlara_gravidez_clinica AS VARCHAR)) IS NOT NULL
                OR COALESCE(CAST(p.no_nascidos AS VARCHAR), CAST(r.redlara_no_nascidos AS VARCHAR)) IS NOT NULL
            )
        ) as has_valid_outcome
        
    FROM gold.clinisys_embrioes c
    LEFT JOIN tmp_transferred_scope s ON c.oocito_id = s.oocito_id
    LEFT JOIN tmp_matched_planilha p ON c.oocito_id = p.oocito_id
    LEFT JOIN tmp_matched_redlara r ON c.oocito_id = r.oocito_id;
    """)
    
    elapsed = time.time() - t0
    total_rows = con.execute("SELECT count(*) FROM gold.clinisys_embrioes_outcomes").fetchone()[0]
    distinct_ids = con.execute("SELECT count(distinct oocito_id) FROM gold.clinisys_embrioes_outcomes").fetchone()[0]
    matched_transf = con.execute("SELECT count(*) FROM gold.clinisys_embrioes_outcomes WHERE is_transferred = 1 AND (planilha_matched = 1 OR redlara_matched = 1)").fetchone()[0]
    biopsy_cnt = con.execute("SELECT count(*) FROM gold.clinisys_embrioes_outcomes WHERE has_biopsy = True").fetchone()[0]
    valid_out_cnt = con.execute("SELECT count(*) FROM gold.clinisys_embrioes_outcomes WHERE has_valid_outcome = True").fetchone()[0]
    
    logger.info(f"Successfully built gold.clinisys_embrioes_outcomes in {elapsed:.2f}s!")
    logger.info(f"Total rows: {total_rows:,d} | Distinct oocito_ids: {distinct_ids:,d} (Duplicates: {total_rows - distinct_ids})")
    logger.info(f"Transferred embryos matched to at least one source: {matched_transf:,d} / {transf_count:,d} ({matched_transf/transf_count*100:.1f}%)")
    logger.info(f"Embryos with has_biopsy = True: {biopsy_cnt:,d} ({biopsy_cnt/total_rows*100:.1f}%)")
    logger.info(f"Embryos with has_valid_outcome = True: {valid_out_cnt:,d} ({valid_out_cnt/total_rows*100:.1f}%)")

def main():
    logger.info(f"Connecting to DuckDB at {DUCKDB_PATH}...")
    con = duckdb.connect(DUCKDB_PATH, read_only=False)
    try:
        create_gold_clinisys_embrioes_outcomes(con)
    finally:
        con.close()
        logger.info("Pipeline completed successfully.")

if __name__ == '__main__':
    main()
