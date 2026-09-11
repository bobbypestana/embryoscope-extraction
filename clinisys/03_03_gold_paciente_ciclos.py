"""
03_03_gold_paciente_ciclos.py
Production Pipeline for Clinisys Patient Cycles & Outcomes

Builds:
1. gold.clinisys_paciente_ciclos_detalhado: Granular cycle-level table
   (1 row per patient cycle attempt, with sequence numbers, reference dates,
    cycle classifications, lab metrics, and harmonized outcomes)
2. gold.clinisys_paciente_resumo_ciclos: Patient-level summary table
   (1 row per patient, answering 'how many cycles a patient has had',
    with counts of total cycles, aspirations, transfers, pregnancies, live births,
    and patient journey status)

Adheres to:
- No DROP TABLE statements (strictly adheres to SQL Safety Rule)
- Robust multi-tiered matching between view_tratamentos and laboratory tables
- Inclusion of unlinked laboratory procedures to prevent missing cycles
- Logging of all execution steps
"""

import os
import sys
import time
import logging
from datetime import datetime
import duckdb

# Setup logging
BASE_DIR = os.path.dirname(__file__)
LOGS_DIR = os.path.join(BASE_DIR, 'logs')
os.makedirs(LOGS_DIR, exist_ok=True)
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
script_name = os.path.splitext(os.path.basename(__file__))[0]
LOG_PATH = os.path.join(LOGS_DIR, f'{script_name}_{timestamp}.log')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(LOG_PATH, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Database paths
source_db_path = os.path.abspath(os.path.join(BASE_DIR, '..', 'database', 'clinisys_all.duckdb'))
lake_db_path = os.path.abspath(os.path.join(BASE_DIR, '..', 'database', 'huntington_data_lake.duckdb'))


def build_detailed_cycles_query():
    """Build the SQL query to generate the detailed cycle-level table."""
    return """
    CREATE OR REPLACE TABLE gold.clinisys_paciente_ciclos_detalhado AS
    WITH patient_primary AS (
        SELECT 
            TRY_CAST(prontuario_esposa AS BIGINT) as prontuario,
            TRY_CAST(medico AS INT) as medico_paciente,
            TRY_CAST(unidade_origem AS INT) as unidade_paciente
        FROM silver.view_pacientes
        WHERE prontuario_esposa IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY TRY_CAST(prontuario_esposa AS BIGINT) 
            ORDER BY medico IS NOT NULL DESC, unidade_origem IS NOT NULL DESC
        ) = 1
    ),
    micro_agg AS (
        SELECT 
            m.codigo_ficha,
            m.prontuario,
            m.Data_DL,
            m.data_procedimento as data_proc_micro,
            TRY_CAST(m.transferidos AS INT) as micro_transferidos,
            m.ICSIDescongelados,
            TRY_CAST(m.oocitos AS INT) as micro_oocitos,
            TRY_CAST(m.unidade_id AS INT) as unidade_micro,
            TRY_CAST(m.medico_id AS INT) as medico_id_micro,
            COUNT(o.id) as oocitos_count_total,
            COUNT(CASE WHEN o.maturidade = 'MII' THEN 1 END) as oocitos_mii,
            COUNT(CASE WHEN o.fertilizacao = '2PN' THEN 1 END) as oocitos_2pn
        FROM silver.view_micromanipulacao m
        LEFT JOIN silver.view_micromanipulacao_oocitos o
            ON m.codigo_ficha = o.id_micromanipulacao
        GROUP BY 
            m.codigo_ficha, m.prontuario, m.Data_DL, m.data_procedimento,
            m.transferidos, m.ICSIDescongelados, m.oocitos, m.unidade_id, m.medico_id
    ),
    descong_emb_agg AS (
        SELECT 
            d.id as descong_id,
            d.CodDescongelamento,
            d.prontuario,
            d.DataDescongelamento,
            d.DataTransferencia,
            TRY_CAST(d.transferidos_transferencia AS INT) as transferidos_descong,
            TRY_CAST(d.Unidade AS INT) as unidade_descong,
            COUNT(e.id) as embrioes_descongelados_count
        FROM silver.view_descongelamentos_embrioes d
        LEFT JOIN silver.view_embrioes_congelados e
            ON d.id = e.id_descongelamento
        GROUP BY 
            d.id, d.CodDescongelamento, d.prontuario, d.DataDescongelamento,
            d.DataTransferencia, d.transferidos_transferencia, d.Unidade
    ),
    descong_ov_agg AS (
        SELECT 
            d.id as descong_ov_id,
            d.prontuario,
            d.DataDescongelamento,
            COUNT(o.id) as ovulos_descongelados_count
        FROM silver.view_descongelamentos_ovulos d
        LEFT JOIN silver.view_ovulos_congelados o
            ON d.id = o.id_descongelamento
        GROUP BY d.id, d.prontuario, d.DataDescongelamento
    ),
    trat_micro_ranked AS (
        SELECT 
            t.id as trat_id,
            m.codigo_ficha,
            m.Data_DL,
            m.micro_transferidos,
            m.ICSIDescongelados,
            m.micro_oocitos,
            m.unidade_micro,
            m.medico_id_micro,
            m.oocitos_count_total,
            m.oocitos_mii,
            m.oocitos_2pn,
            ROW_NUMBER() OVER (
                PARTITION BY t.id
                ORDER BY 
                    CASE 
                        WHEN t.data_procedimento IS NOT NULL AND ABS(DATEDIFF('day', t.data_procedimento::DATE, m.Data_DL::DATE)) <= 5 THEN 1
                        WHEN t.data_procedimento IS NULL AND t.data_inicio_inducao IS NOT NULL AND DATEDIFF('day', t.data_inicio_inducao::DATE, m.Data_DL::DATE) BETWEEN 8 AND 18 THEN 2
                        ELSE 3
                    END ASC,
                    ABS(DATEDIFF('day', COALESCE(t.data_procedimento, t.data_inicio_inducao + INTERVAL 12 DAYS)::DATE, m.Data_DL::DATE)) ASC,
                    m.codigo_ficha ASC
            ) as rn
        FROM silver.view_tratamentos t
        JOIN micro_agg m
            ON t.prontuario = m.prontuario
            AND (
                (t.data_procedimento IS NOT NULL AND ABS(DATEDIFF('day', t.data_procedimento::DATE, m.Data_DL::DATE)) <= 5)
                OR (t.data_procedimento IS NULL AND t.data_inicio_inducao IS NOT NULL AND DATEDIFF('day', t.data_inicio_inducao::DATE, m.Data_DL::DATE) BETWEEN 8 AND 18)
            )
        WHERE t.tipo_procedimento IN ('Ciclo a Fresco FIV', 'Ciclo a Fresco Vitrificao')
    ),
    trat_descong_ranked AS (
        SELECT 
            t.id as trat_id,
            d.descong_id,
            d.CodDescongelamento,
            d.DataTransferencia,
            d.DataDescongelamento,
            d.transferidos_descong,
            d.unidade_descong,
            d.embrioes_descongelados_count,
            ROW_NUMBER() OVER (
                PARTITION BY t.id
                ORDER BY 
                    CASE 
                        WHEN t.data_transferencia IS NOT NULL AND ABS(DATEDIFF('day', t.data_transferencia::DATE, d.DataTransferencia::DATE)) <= 5 THEN 1
                        WHEN t.data_procedimento IS NOT NULL AND ABS(DATEDIFF('day', t.data_procedimento::DATE, COALESCE(d.DataTransferencia, d.DataDescongelamento)::DATE)) <= 5 THEN 2
                        WHEN t.data_transferencia IS NULL AND t.data_procedimento IS NULL AND t.data_inicio_inducao IS NOT NULL 
                             AND DATEDIFF('day', t.data_inicio_inducao::DATE, COALESCE(d.DataTransferencia, d.DataDescongelamento)::DATE) BETWEEN 10 AND 30 THEN 3
                        ELSE 4
                    END ASC,
                    ABS(DATEDIFF('day', COALESCE(t.data_transferencia, t.data_procedimento, t.data_inicio_inducao + INTERVAL 19 DAYS)::DATE, COALESCE(d.DataTransferencia, d.DataDescongelamento)::DATE)) ASC,
                    d.descong_id ASC
            ) as rn
        FROM silver.view_tratamentos t
        JOIN descong_emb_agg d
            ON t.prontuario = d.prontuario
            AND (
                (t.data_transferencia IS NOT NULL AND ABS(DATEDIFF('day', t.data_transferencia::DATE, d.DataTransferencia::DATE)) <= 5)
                OR (t.data_procedimento IS NOT NULL AND ABS(DATEDIFF('day', t.data_procedimento::DATE, COALESCE(d.DataTransferencia, d.DataDescongelamento)::DATE)) <= 5)
                OR (t.data_transferencia IS NULL AND t.data_procedimento IS NULL AND t.data_inicio_inducao IS NOT NULL 
                    AND DATEDIFF('day', t.data_inicio_inducao::DATE, COALESCE(d.DataTransferencia, d.DataDescongelamento)::DATE) BETWEEN 10 AND 30)
            )
        WHERE t.tipo_procedimento = 'Ciclo de Congelados'
    ),
    trat_descong_ov_ranked AS (
        SELECT 
            t.id as trat_id,
            dov.descong_ov_id,
            ROW_NUMBER() OVER (
                PARTITION BY t.id
                ORDER BY ABS(DATEDIFF('day', COALESCE(t.data_procedimento, t.data_inicio_inducao)::DATE, dov.DataDescongelamento::DATE)) ASC,
                         dov.descong_ov_id ASC
            ) as rn
        FROM silver.view_tratamentos t
        JOIN descong_ov_agg dov
            ON t.prontuario = dov.prontuario
            AND ABS(DATEDIFF('day', COALESCE(t.data_procedimento, t.data_inicio_inducao)::DATE, dov.DataDescongelamento::DATE)) <= 5
        WHERE t.tipo_procedimento = 'Ciclo a Fresco FIV' AND (t.status_ovulo = 'Criopreservado' OR t.status_ovulo = 'Ambos')
    ),
    trat_cycles AS (
        SELECT 
            t.prontuario,
            CAST(t.id AS VARCHAR) as treatment_id,
            'view_tratamentos' as source_system,
            COALESCE(TRY_CAST(t.unidade AS INT), tm.unidade_micro, td.unidade_descong, p.unidade_paciente) as id_unidade,
            COALESCE(TRY_CAST(t.responsavel_informacoes AS INT), tm.medico_id_micro, p.medico_paciente) as id_medico,
            t.tentativa as tentativa_raw,
            t.tipo_procedimento as tipo_procedimento_raw,
            t.status_ovulo,
            t.tipo_ciclo_congelado,
            t.resultado_tratamento as resultado_tratamento_raw,
            t.motivo_nao_transferir,
            t.motivo_cancelamento_tratamento,
            t.data_inicio_inducao,
            COALESCE(t.data_procedimento, tm.Data_DL) as data_procedimento,
            t.data_transferencia,
            t.data_congelamento,
            t.data_nascimento_bebes,
            t.via_parto_bebe1,
            t.motivo_perda_aborto,
            
            -- Linked lab IDs
            tm.codigo_ficha as id_micromanipulacao,
            td.descong_id as id_descongelamento_embrioes,
            tdov.descong_ov_id as id_descongelamento_ovulos,
            
            -- Lab metrics
            COALESCE(tm.micro_oocitos, tm.oocitos_count_total, TRY_CAST(t.foliculos_antrais AS INT)) as num_oocitos_aspirados,
            tm.oocitos_mii as num_oocitos_mii,
            tm.oocitos_2pn as num_fertilizados_2pn,
            TRY_CAST(t.num_congelados AS INT) as num_congelados,
            COALESCE(
                TRY_CAST(t.num_transferir AS INT),
                tm.micro_transferidos,
                td.transferidos_descong,
                CASE WHEN t.data_transferencia IS NOT NULL THEN 1 ELSE 0 END
            ) as num_transferidos,
            
            -- Reference Date Logic
            COALESCE(
                CASE 
                    WHEN t.tipo_procedimento = 'Ciclo de Congelados' THEN COALESCE(t.data_transferencia, t.data_procedimento, td.DataTransferencia, td.DataDescongelamento, t.data_inicio_inducao)
                    WHEN t.tipo_procedimento ILIKE '%Fresco%' OR t.tipo_procedimento ILIKE '%Vitrifica%' THEN COALESCE(t.data_procedimento, tm.Data_DL, t.data_congelamento, t.data_transferencia, t.data_inicio_inducao)
                    ELSE COALESCE(t.data_procedimento, t.data_transferencia, t.data_inicio_inducao, t.data_dum)
                END,
                t.data_dum,
                t.extraction_timestamp
            ) as reference_date,
            
            -- Detailed Cycle Type
            CASE 
                WHEN t.tipo_procedimento ILIKE '%Vitrifica%' THEN 'OOCYTE_FREEZING'
                WHEN t.tipo_procedimento = 'Ciclo a Fresco FIV' AND (t.status_ovulo = 'Criopreservado' OR t.status_ovulo = 'Ambos' OR tm.ICSIDescongelados = 'Sim') THEN
                    CASE 
                        WHEN t.data_transferencia IS NOT NULL OR (t.num_transferir IS NOT NULL AND TRY_CAST(t.num_transferir AS INT) > 0) OR COALESCE(tm.micro_transferidos, 0) > 0 THEN 'FOT_TRANSFER'
                        WHEN t.motivo_nao_transferir ILIKE '%congelamento%' OR t.motivo_nao_transferir ILIKE '%criopreserv%' THEN 'FOT_FREEZE_EMBRYO'
                        ELSE 'FOT_NO_TRANSFER_OTHER'
                    END
                WHEN t.tipo_procedimento = 'Ciclo a Fresco FIV' THEN
                    CASE 
                        WHEN t.data_transferencia IS NOT NULL OR (t.num_transferir IS NOT NULL AND TRY_CAST(t.num_transferir AS INT) > 0) OR COALESCE(tm.micro_transferidos, 0) > 0 THEN 'FRESH_IVF_TRANSFER'
                        WHEN t.motivo_nao_transferir ILIKE '%congelamento%' OR t.motivo_nao_transferir ILIKE '%criopreserv%' THEN 'FRESH_IVF_FREEZE_ALL'
                        WHEN t.motivo_nao_transferir ILIKE '%aus%ncia de embri%es%' OR t.motivo_nao_transferir ILIKE '%aneuploide%' THEN 'FRESH_IVF_NO_EUPLOID'
                        WHEN t.motivo_nao_transferir ILIKE '%desenvolvimento%' OR t.motivo_nao_transferir ILIKE '%fertiliza%' THEN 'FRESH_IVF_ARREST'
                        WHEN t.motivo_nao_transferir ILIKE '%ocitos%' OR t.motivo_nao_transferir ILIKE '%resposta%' THEN 'FRESH_IVF_POOR_RESPONSE'
                        WHEN t.motivo_cancelamento_tratamento IS NOT NULL OR t.resultado_tratamento = 'Cancelado' THEN 'FRESH_IVF_CANCELLED'
                        WHEN t.resultado_tratamento = 'No transfer' THEN 'FRESH_IVF_NO_TRANSFER_UNSPECIFIED'
                        WHEN t.data_procedimento IS NULL AND tm.codigo_ficha IS NULL AND t.resultado_tratamento IS NULL THEN 'FRESH_IVF_CANCELLED_OR_ABANDONED'
                        ELSE 'FRESH_IVF_OTHER'
                    END
                WHEN t.tipo_procedimento = 'Ciclo de Congelados' THEN
                    CASE 
                        WHEN t.data_transferencia IS NOT NULL 
                             OR (t.data_procedimento IS NOT NULL AND (t.resultado_tratamento ILIKE '%Gesta%' OR t.resultado_tratamento = 'Negativo'))
                             OR td.DataTransferencia IS NOT NULL
                             OR COALESCE(td.transferidos_descong, 0) > 0 THEN 'FET_TRANSFER'
                        WHEN t.resultado_tratamento = 'No transfer' OR t.motivo_nao_transferir IS NOT NULL THEN 'FET_CANCELLED_NO_TRANSFER'
                        WHEN t.data_transferencia IS NULL AND t.data_procedimento IS NULL AND td.descong_id IS NULL THEN 'FET_CANCELLED_DURING_PREP'
                        ELSE 'FET_OTHER'
                    END
                WHEN t.tipo_procedimento = 'Ciclo de IUI' THEN 'IUI'
                WHEN t.tipo_procedimento = 'Coito Programado' THEN 'COITO_PROGRAMADO'
                WHEN t.tipo_procedimento = 'ERA' THEN 'ERA_DIAGNOSTIC'
                WHEN t.tipo_procedimento ILIKE '%Bipsia%' OR t.tipo_procedimento ILIKE '%Bi%psia%' THEN 'THAW_FOR_BIOPSY'
                WHEN t.tipo_procedimento = 'Ciclo Natural' THEN 'CICLO_NATURAL'
                ELSE 'OTHER'
            END as cycle_type,
            
            -- High Level Category
            CASE 
                WHEN t.tipo_procedimento ILIKE '%Vitrifica%' THEN 'OOCYTE_BANKING'
                WHEN t.tipo_procedimento = 'Ciclo a Fresco FIV' AND (t.status_ovulo = 'Criopreservado' OR t.status_ovulo = 'Ambos' OR tm.ICSIDescongelados = 'Sim') THEN 'TRANSFER_FOT'
                WHEN t.tipo_procedimento = 'Ciclo a Fresco FIV' THEN 'ASPIRATION_OPU'
                WHEN t.tipo_procedimento = 'Ciclo de Congelados' THEN 'TRANSFER_FET'
                WHEN t.tipo_procedimento IN ('Ciclo de IUI', 'Coito Programado') THEN 'LOW_COMPLEXITY'
                WHEN t.tipo_procedimento = 'ERA' THEN 'DIAGNOSTIC'
                ELSE 'OTHER'
            END as cycle_category,
            
            -- Boolean flags
            CASE 
                WHEN t.tipo_procedimento IN ('Ciclo de IUI', 'Coito Programado', 'ERA') THEN FALSE
                WHEN t.tipo_procedimento ILIKE '%Vitrifica%' THEN FALSE
                WHEN t.resultado_tratamento = 'No transfer' AND t.data_transferencia IS NULL THEN FALSE
                WHEN t.motivo_nao_transferir IS NOT NULL AND t.data_transferencia IS NULL THEN FALSE
                WHEN t.data_transferencia IS NOT NULL 
                     OR COALESCE(tm.micro_transferidos, 0) > 0
                     OR COALESCE(td.transferidos_descong, 0) > 0
                     OR (t.tipo_procedimento = 'Ciclo de Congelados' AND (t.resultado_tratamento ILIKE '%Gesta%' OR t.resultado_tratamento = 'Negativo')) THEN TRUE
                ELSE FALSE
            END as is_transfer_cycle,
            
            CASE 
                WHEN t.tipo_procedimento ILIKE '%Fresco%' OR t.tipo_procedimento ILIKE '%Vitrifica%' THEN TRUE
                ELSE FALSE
            END as is_aspiration_cycle,
            
            -- Harmonized Outcome
            CASE 
                WHEN (t.data_nascimento_bebes IS NOT NULL AND t.data_nascimento_bebes != '') 
                     OR (t.via_parto_bebe1 IS NOT NULL AND t.via_parto_bebe1 NOT IN ('0', 'No', 'None', 'Não')) THEN 'LIVE_BIRTH'
                WHEN t.motivo_perda_aborto IS NOT NULL AND t.motivo_perda_aborto != '' THEN 'MISCARRIAGE'
                WHEN t.resultado_tratamento ILIKE '%Gesta%Cl%nica%' THEN 'CLINICAL_PREGNANCY'
                WHEN t.resultado_tratamento ILIKE '%Gesta%Qu%mica%Confirmada%' THEN 'BIOCHEMICAL_PREGNANCY_CONFIRMED'
                WHEN t.resultado_tratamento ILIKE '%Gesta%Qu%mica%' THEN 'BIOCHEMICAL_PREGNANCY'
                WHEN t.resultado_tratamento = 'Negativo' THEN 'NEGATIVE'
                WHEN t.resultado_tratamento ILIKE '%\%vulos%' 
                     OR t.tipo_procedimento ILIKE '%Vitrifica%' 
                     OR t.motivo_nao_transferir ILIKE '%congelamento de %ocitos%' THEN 'OOCYTES_FROZEN'
                WHEN t.motivo_nao_transferir ILIKE '%congelamento%' OR t.motivo_nao_transferir ILIKE '%criopreserv%' THEN 'EMBRYOS_FROZEN'
                WHEN t.motivo_nao_transferir ILIKE '%aus%ncia de embri%es%' OR t.motivo_nao_transferir ILIKE '%aneuploide%' THEN 'NO_EUPLOID_EMBRYOS'
                WHEN t.motivo_nao_transferir ILIKE '%desenvolvimento%' OR t.motivo_nao_transferir ILIKE '%fertiliza%' THEN 'DEVELOPMENT_FAILURE'
                WHEN t.motivo_nao_transferir ILIKE '%ocitos%' OR t.motivo_nao_transferir ILIKE '%resposta%' THEN 'RETRIEVAL_FAILURE'
                WHEN t.motivo_cancelamento_tratamento IS NOT NULL OR t.resultado_tratamento = 'Cancelado' THEN 'CANCELLED'
                WHEN t.resultado_tratamento = 'No transfer' THEN 'NO_TRANSFER_UNSPECIFIED'
                WHEN t.resultado_tratamento ILIKE '%Bi%psia%' THEN 'BIOPSY_PERFORMED'
                WHEN t.resultado_tratamento IS NULL THEN 'UNKNOWN_OR_IN_PROGRESS'
                ELSE 'OTHER'
            END as outcome
            
        FROM silver.view_tratamentos t
        LEFT JOIN trat_micro_ranked tm ON t.id = tm.trat_id AND tm.rn = 1
        LEFT JOIN trat_descong_ranked td ON t.id = td.trat_id AND td.rn = 1
        LEFT JOIN trat_descong_ov_ranked tdov ON t.id = tdov.trat_id AND tdov.rn = 1
        LEFT JOIN patient_primary p ON t.prontuario = p.prontuario
        WHERE t.prontuario IS NOT NULL
    ),
    orphan_micro_cycles AS (
        SELECT 
            m.prontuario,
            'micro_' || CAST(m.codigo_ficha AS VARCHAR) as treatment_id,
            'view_micromanipulacao' as source_system,
            COALESCE(m.unidade_micro, p.unidade_paciente) as id_unidade,
            COALESCE(m.medico_id_micro, p.medico_paciente) as id_medico,
            NULL as tentativa_raw,
            'Micromanipulao Lab' as tipo_procedimento_raw,
            NULL as status_ovulo,
            NULL as tipo_ciclo_congelado,
            NULL as resultado_tratamento_raw,
            NULL as motivo_nao_transferir,
            NULL as motivo_cancelamento_tratamento,
            NULL::TIMESTAMP as data_inicio_inducao,
            m.Data_DL as data_procedimento,
            NULL::TIMESTAMP as data_transferencia,
            NULL::TIMESTAMP as data_congelamento,
            NULL as data_nascimento_bebes,
            NULL as via_parto_bebe1,
            NULL as motivo_perda_aborto,
            m.codigo_ficha as id_micromanipulacao,
            NULL::BIGINT as id_descongelamento_embrioes,
            NULL::BIGINT as id_descongelamento_ovulos,
            COALESCE(m.micro_oocitos, m.oocitos_count_total) as num_oocitos_aspirados,
            m.oocitos_mii as num_oocitos_mii,
            m.oocitos_2pn as num_fertilizados_2pn,
            NULL::INT as num_congelados,
            COALESCE(m.micro_transferidos, 0) as num_transferidos,
            m.Data_DL as reference_date,
            'LAB_OPU_UNLINKED' as cycle_type,
            'ASPIRATION_OPU' as cycle_category,
            CASE WHEN COALESCE(m.micro_transferidos, 0) > 0 THEN TRUE ELSE FALSE END as is_transfer_cycle,
            TRUE as is_aspiration_cycle,
            CASE 
                WHEN COALESCE(m.micro_transferidos, 0) > 0 THEN 'UNKNOWN_TRANSFER_PERFORMED'
                ELSE 'LAB_PROCEDURE_COMPLETED'
            END as outcome
        FROM micro_agg m
        LEFT JOIN patient_primary p ON m.prontuario = p.prontuario
        WHERE m.codigo_ficha NOT IN (SELECT id_micromanipulacao FROM trat_cycles WHERE id_micromanipulacao IS NOT NULL)
          AND m.prontuario IS NOT NULL
    ),
    orphan_descong_cycles AS (
        SELECT 
            d.prontuario,
            'descong_emb_' || CAST(d.descong_id AS VARCHAR) as treatment_id,
            'view_descongelamentos_embrioes' as source_system,
            COALESCE(d.unidade_descong, p.unidade_paciente) as id_unidade,
            p.medico_paciente as id_medico,
            NULL as tentativa_raw,
            'Descongelamento Embries Lab' as tipo_procedimento_raw,
            NULL as status_ovulo,
            NULL as tipo_ciclo_congelado,
            NULL as resultado_tratamento_raw,
            NULL as motivo_nao_transferir,
            NULL as motivo_cancelamento_tratamento,
            NULL::TIMESTAMP as data_inicio_inducao,
            NULL::TIMESTAMP as data_procedimento,
            d.DataTransferencia as data_transferencia,
            d.DataDescongelamento as data_congelamento,
            NULL as data_nascimento_bebes,
            NULL as via_parto_bebe1,
            NULL as motivo_perda_aborto,
            NULL::BIGINT as id_micromanipulacao,
            d.descong_id as id_descongelamento_embrioes,
            NULL::BIGINT as id_descongelamento_ovulos,
            NULL::INT as num_oocitos_aspirados,
            NULL::INT as num_oocitos_mii,
            NULL::INT as num_fertilizados_2pn,
            NULL::INT as num_congelados,
            COALESCE(d.transferidos_descong, CASE WHEN d.DataTransferencia IS NOT NULL THEN 1 ELSE 0 END) as num_transferidos,
            COALESCE(d.DataTransferencia, d.DataDescongelamento) as reference_date,
            'LAB_FET_UNLINKED' as cycle_type,
            'TRANSFER_FET' as cycle_category,
            CASE WHEN COALESCE(d.transferidos_descong, 0) > 0 OR d.DataTransferencia IS NOT NULL THEN TRUE ELSE FALSE END as is_transfer_cycle,
            FALSE as is_aspiration_cycle,
            CASE 
                WHEN COALESCE(d.transferidos_descong, 0) > 0 OR d.DataTransferencia IS NOT NULL THEN 'UNKNOWN_TRANSFER_PERFORMED'
                ELSE 'THAW_NO_TRANSFER'
            END as outcome
        FROM descong_emb_agg d
        LEFT JOIN patient_primary p ON d.prontuario = p.prontuario
        WHERE d.descong_id NOT IN (SELECT id_descongelamento_embrioes FROM trat_cycles WHERE id_descongelamento_embrioes IS NOT NULL)
          AND d.prontuario IS NOT NULL
    ),
    all_cycles_union AS (
        SELECT * FROM trat_cycles
        UNION ALL
        SELECT * FROM orphan_micro_cycles
        UNION ALL
        SELECT * FROM orphan_descong_cycles
    )
    SELECT 
        c.prontuario,
        c.treatment_id,
        c.source_system,
        c.reference_date,
        ROW_NUMBER() OVER (
            PARTITION BY c.prontuario 
            ORDER BY c.reference_date ASC NULLS LAST, c.treatment_id ASC
        ) as cycle_seq_patient,
        CASE 
            WHEN c.is_aspiration_cycle THEN
                ROW_NUMBER() OVER (
                    PARTITION BY c.prontuario, c.is_aspiration_cycle 
                    ORDER BY c.reference_date ASC NULLS LAST, c.treatment_id ASC
                )
            ELSE NULL 
        END as cycle_seq_aspiration,
        CASE 
            WHEN c.is_transfer_cycle THEN
                ROW_NUMBER() OVER (
                    PARTITION BY c.prontuario, c.is_transfer_cycle 
                    ORDER BY c.reference_date ASC NULLS LAST, c.treatment_id ASC
                )
            ELSE NULL 
        END as cycle_seq_transfer,
        c.cycle_type,
        c.cycle_category,
        c.is_transfer_cycle,
        c.is_aspiration_cycle,
        c.outcome,
        c.resultado_tratamento_raw as outcome_raw,
        c.motivo_nao_transferir,
        c.motivo_cancelamento_tratamento,
        c.tipo_procedimento_raw,
        c.status_ovulo,
        c.tipo_ciclo_congelado,
        c.data_inicio_inducao,
        c.data_procedimento,
        c.data_transferencia,
        c.data_congelamento,
        c.data_nascimento_bebes,
        c.via_parto_bebe1,
        c.motivo_perda_aborto,
        c.num_oocitos_aspirados,
        c.num_oocitos_mii,
        c.num_fertilizados_2pn,
        c.num_congelados,
        c.num_transferidos,
        c.id_micromanipulacao,
        c.id_descongelamento_embrioes,
        c.id_descongelamento_ovulos,
        -- Clinic unit
        c.id_unidade,
        u.nome as nome_unidade,
        -- Doctor
        c.id_medico,
        med.nome as nome_medico,
        med.tipo_medico,
        med.registro as crm_medico,
        c.tentativa_raw
    FROM all_cycles_union c
    LEFT JOIN silver.view_unidades u ON c.id_unidade = u.id
    LEFT JOIN silver.view_medicos med ON c.id_medico = med.id
    ORDER BY c.prontuario, cycle_seq_patient;
    """


def build_summary_table_query():
    """Build the SQL query to generate the patient-level summary table."""
    return """
    CREATE OR REPLACE TABLE gold.clinisys_paciente_resumo_ciclos AS
    SELECT 
        prontuario,
        COUNT(*) as total_cycles,
        COUNT(CASE WHEN source_system = 'view_tratamentos' THEN 1 END) as total_treatments_clinisys,
        COUNT(CASE WHEN source_system != 'view_tratamentos' THEN 1 END) as total_lab_only_cycles,
        COUNT(CASE WHEN is_aspiration_cycle THEN 1 END) as total_aspiration_cycles,
        COUNT(CASE WHEN cycle_type = 'OOCYTE_FREEZING' THEN 1 END) as total_oocyte_freezing_cycles,
        COUNT(CASE WHEN is_transfer_cycle THEN 1 END) as total_transfer_cycles,
        COUNT(CASE WHEN cycle_type = 'FRESH_IVF_TRANSFER' THEN 1 END) as total_fresh_transfers,
        COUNT(CASE WHEN cycle_type IN ('FET_TRANSFER', 'LAB_FET_UNLINKED') THEN 1 END) as total_fet_transfers,
        COUNT(CASE WHEN cycle_type = 'FOT_TRANSFER' THEN 1 END) as total_fot_transfers,
        COUNT(CASE WHEN cycle_type IN ('FRESH_IVF_FREEZE_ALL', 'FOT_FREEZE_EMBRYO') THEN 1 END) as total_freeze_all_cycles,
        COUNT(CASE WHEN cycle_type LIKE '%CANCELLED%' THEN 1 END) as total_cancelled_cycles,
        COUNT(CASE WHEN outcome = 'LIVE_BIRTH' THEN 1 END) as total_live_births,
        COUNT(CASE WHEN outcome = 'CLINICAL_PREGNANCY' THEN 1 END) as total_clinical_pregnancies,
        COUNT(CASE WHEN outcome IN ('BIOCHEMICAL_PREGNANCY', 'BIOCHEMICAL_PREGNANCY_CONFIRMED') THEN 1 END) as total_biochemical_pregnancies,
        COUNT(CASE WHEN outcome = 'NEGATIVE' THEN 1 END) as total_negative_transfers,
        COUNT(CASE WHEN outcome = 'MISCARRIAGE' THEN 1 END) as total_miscarriages,
        MIN(reference_date) as first_cycle_date,
        MAX(reference_date) as latest_cycle_date,
        CASE 
            WHEN COUNT(*) > 1 THEN DATEDIFF('day', MIN(reference_date)::DATE, MAX(reference_date)::DATE)
            ELSE 0
        END as days_between_first_and_latest_cycle,
        -- Most frequent clinic unit and doctor across the patient's cycles
        MODE(nome_unidade) as unidade_principal,
        MODE(nome_medico) as medico_principal,
        CASE 
            WHEN COUNT(CASE WHEN outcome = 'LIVE_BIRTH' THEN 1 END) > 0 THEN 'LIVE_BIRTH_ACHIEVED'
            WHEN COUNT(CASE WHEN outcome = 'CLINICAL_PREGNANCY' THEN 1 END) > 0 THEN 'CLINICAL_PREGNANCY_ONGOING'
            WHEN COUNT(CASE WHEN is_transfer_cycle THEN 1 END) > 0 
                 AND COUNT(CASE WHEN outcome IN ('LIVE_BIRTH', 'CLINICAL_PREGNANCY') THEN 1 END) = 0 THEN 'TRANSFERS_FAILED'
            WHEN COUNT(CASE WHEN outcome IN ('EMBRYOS_FROZEN', 'OOCYTES_FROZEN') THEN 1 END) > 0 THEN 'EGGS_OR_EMBRYOS_BANKED'
            WHEN COUNT(CASE WHEN cycle_type LIKE '%CANCELLED%' OR outcome LIKE '%FAILURE%' THEN 1 END) > 0 THEN 'CANCELLED_OR_FAILED_EARLY'
            ELSE 'UNKNOWN_OR_IN_PROGRESS'
        END as patient_journey_status
    FROM gold.clinisys_paciente_ciclos_detalhado
    GROUP BY prontuario
    ORDER BY total_cycles DESC, prontuario ASC;
    """


def run_pipeline():
    """Main execution function."""
    logger.info("=" * 60)
    logger.info("STARTING PIPELINE: 03_03_gold_paciente_ciclos")
    logger.info("=" * 60)
    start_time = time.time()
    
    if not os.path.exists(source_db_path):
        logger.error(f"Source database not found at: {source_db_path}")
        return False
        
    logger.info(f"Target DB (clinisys_all): {source_db_path}")
    
    # 1. Connect to clinisys_all.duckdb
    try:
        con = duckdb.connect(source_db_path, read_only=False)
        con.execute("CREATE SCHEMA IF NOT EXISTS gold;")
        
        logger.info("Creating table gold.clinisys_paciente_ciclos_detalhado...")
        t0 = time.time()
        con.execute(build_detailed_cycles_query())
        detailed_count = con.execute("SELECT COUNT(*) FROM gold.clinisys_paciente_ciclos_detalhado").fetchone()[0]
        logger.info(f"Successfully created gold.clinisys_paciente_ciclos_detalhado with {detailed_count:,} cycles in {time.time() - t0:.2f}s.")
        
        logger.info("Creating table gold.clinisys_paciente_resumo_ciclos...")
        t1 = time.time()
        con.execute(build_summary_table_query())
        summary_count = con.execute("SELECT COUNT(*) FROM gold.clinisys_paciente_resumo_ciclos").fetchone()[0]
        logger.info(f"Successfully created gold.clinisys_paciente_resumo_ciclos with {summary_count:,} patients in {time.time() - t1:.2f}s.")
        
        # Validation checks
        logger.info("Running integrity and sanity checks...")
        dup_check = con.execute("""
            SELECT prontuario, treatment_id, COUNT(*) 
            FROM gold.clinisys_paciente_ciclos_detalhado 
            GROUP BY prontuario, treatment_id 
            HAVING COUNT(*) > 1
        """).fetchall()
        if len(dup_check) == 0:
            logger.info("PASSED: Exact 1:1 grain on (prontuario, treatment_id) - 0 duplicates!")
        else:
            logger.warning(f"WARNING: Found {len(dup_check)} duplicate (prontuario, treatment_id) pairs!")
            
        # Outcome breakdown
        outcomes = con.execute("""
            SELECT outcome, COUNT(*) as count 
            FROM gold.clinisys_paciente_ciclos_detalhado 
            GROUP BY outcome 
            ORDER BY count DESC
        """).fetchdf()
        logger.info("Outcome Breakdown:\n" + outcomes.to_string())
        
        # Unidade / Medico coverage
        coverage = con.execute("""
            SELECT 
                source_system,
                COUNT(*) as total,
                COUNT(nome_unidade) as with_unidade,
                ROUND(COUNT(nome_unidade) * 100.0 / COUNT(*), 2) as pct_unidade,
                COUNT(nome_medico) as with_medico,
                ROUND(COUNT(nome_medico) * 100.0 / COUNT(*), 2) as pct_medico
            FROM gold.clinisys_paciente_ciclos_detalhado
            GROUP BY source_system
        """).fetchdf()
        logger.info("Unidade/Medico Coverage:\n" + coverage.to_string())
        
        # Journey breakdown
        journeys = con.execute("""
            SELECT patient_journey_status, COUNT(*) as count 
            FROM gold.clinisys_paciente_resumo_ciclos 
            GROUP BY patient_journey_status 
            ORDER BY count DESC
        """).fetchdf()
        logger.info("Patient Journey Status Breakdown:\n" + journeys.to_string())
        
        con.close()
        logger.info(f"All operations in clinisys_all completed in {time.time() - start_time:.2f}s.")
        
    except Exception as e:
        logger.error(f"Failed to create tables in clinisys_all: {e}", exc_info=True)
        return False
        
    # 2. Also replicate/attach to huntington_data_lake.duckdb if available
    if os.path.exists(lake_db_path):
        try:
            logger.info(f"Synchronizing new tables to {lake_db_path}...")
            t_lake = time.time()
            lake_con = duckdb.connect(lake_db_path, read_only=False)
            lake_con.execute(f"ATTACH '{source_db_path}' AS source_clinisys (READ_ONLY);")
            lake_con.execute("CREATE SCHEMA IF NOT EXISTS gold;")
            lake_con.execute("CREATE OR REPLACE TABLE gold.clinisys_paciente_ciclos_detalhado AS SELECT * FROM source_clinisys.gold.clinisys_paciente_ciclos_detalhado;")
            lake_con.execute("CREATE OR REPLACE TABLE gold.clinisys_paciente_resumo_ciclos AS SELECT * FROM source_clinisys.gold.clinisys_paciente_resumo_ciclos;")
            lake_con.execute("DETACH source_clinisys;")
            lake_con.close()
            logger.info(f"Successfully synchronized to huntington_data_lake.duckdb in {time.time() - t_lake:.2f}s.")
        except Exception as e:
            logger.warning(f"Could not synchronize to huntington_data_lake.duckdb (non-blocking): {e}")

    logger.info("=" * 60)
    logger.info(f"PIPELINE COMPLETED SUCCESSFULLY IN {time.time() - start_time:.2f}s!")
    logger.info("=" * 60)
    return True


if __name__ == "__main__":
    success = run_pipeline()
    sys.exit(0 if success else 1)
