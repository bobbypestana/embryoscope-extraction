#!/usr/bin/env python3
"""
RD Station and Clinisys Funnel Integration Script — v3 (Production Lead Granularity)
Features:
  - Identity Resolution: Cascading unification (Email > Phone Mapped to Email > Phone > Contact > Deal)
  - Anti-Double-Counting: Primary patient lead attribution per couple/prontuário
  - Dual Financial Perspective: 2-year campaign window + Full Lifetime LTV
  - Inactivity Lifecycle: 2-year cutoff flag (closed)
  - High-performance priority matching with Clinisys patients
  - Retrocompatibility view for downstream dashboarding
"""

import os
import yaml
import logging
import duckdb
from datetime import datetime
import time

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LOGS_DIR = os.path.join(SCRIPT_DIR, 'logs')
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

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
PARAMS_PATH = os.path.join(SCRIPT_DIR, 'params.yml')
with open(PARAMS_PATH, 'r') as f:
    config = yaml.safe_load(f)

DUCKDB_PATH = config['duckdb_path']
CLINISYS_PATH = os.path.join(os.path.dirname(DUCKDB_PATH), "clinisys_all.duckdb")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _step(con, label: str, sql: str) -> int:
    """Execute sql, log elapsed time, return rowcount if possible."""
    t0 = time.time()
    con.execute(sql)
    elapsed = time.time() - t0
    try:
        n = con.execute("SELECT changes()").fetchone()[0]
    except Exception:
        n = None
    count_str = f" ({n:,} rows)" if n else ""
    logger.info(f"  [{elapsed:.1f}s] {label}{count_str}")
    return elapsed


def _count(con, table: str) -> int:
    return con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


# ---------------------------------------------------------------------------
# Main builder
# ---------------------------------------------------------------------------
def create_leads_funil(con):

    # -----------------------------------------------------------------------
    # 0. Attach Clinisys + define inline macros
    # -----------------------------------------------------------------------
    logger.info("Step 0 — Attach Clinisys and define macros")
    con.execute(f"ATTACH '{CLINISYS_PATH}' AS clinisys (READ_ONLY)")

    con.execute("""
        CREATE OR REPLACE TEMPORARY MACRO clean_phone_sql(p) AS (
            CASE
                WHEN p IS NULL THEN NULL
                ELSE
                    CASE
                        WHEN len(regexp_replace(p, '[^0-9]', '', 'g')) IN (12, 13)
                             AND regexp_replace(p, '[^0-9]', '', 'g') LIKE '55%'
                            THEN regexp_replace(regexp_replace(p, '[^0-9]', '', 'g'), '^55', '')
                        WHEN regexp_replace(p, '[^0-9]', '', 'g') LIKE '0%'
                            THEN regexp_replace(regexp_replace(p, '[^0-9]', '', 'g'), '^0', '')
                        ELSE regexp_replace(p, '[^0-9]', '', 'g')
                    END
            END
        );
    """)

    con.execute("""
        CREATE OR REPLACE TEMPORARY MACRO is_valid_phone(p) AS (
            p IS NOT NULL
            AND len(p) >= 8
            AND p NOT LIKE '%00000%'
            AND p NOT LIKE '%99999%'
            AND p NOT LIKE '%11111%'
            AND p NOT LIKE '%88888%'
        );
    """)

    con.execute("""
        CREATE OR REPLACE TEMPORARY MACRO is_valid_email(e) AS (
            e IS NOT NULL
            AND e LIKE '%@%'
            AND lower(e) NOT LIKE '%naotem%'
            AND lower(e) NOT LIKE '%nao_tem%'
            AND lower(e) NOT LIKE '%não_tem%'
            AND lower(e) NOT LIKE '%reproducaohumana%'
            AND lower(e) NOT LIKE '%teste%'
        );
    """)

    # -----------------------------------------------------------------------
    # 1. Identity Resolution across RD deals
    #    Cascading priority: Email > Phone mapped to known Email > Phone > Contact > Deal
    # -----------------------------------------------------------------------
    logger.info("Step 1 — Materialise RD raw deals with cascading Identity Resolution")
    _step(con, "tmp_rd_deals_raw", """
        CREATE OR REPLACE TEMP TABLE tmp_rd_deals_raw AS
        WITH best_contact_per_deal AS (
            SELECT 
                dc."Deal ID",
                c."Contact ID",
                c."Nome Contato",
                c."E-mail",
                c."Telefone",
                c."CPF",
                ROW_NUMBER() OVER (
                    PARTITION BY dc."Deal ID"
                    ORDER BY 
                        CASE 
                            WHEN (c."E-mail" IS NOT NULL AND c."E-mail" != '') AND (c."Telefone" IS NOT NULL AND c."Telefone" != '') THEN 1
                            WHEN (c."E-mail" IS NOT NULL AND c."E-mail" != '') THEN 2
                            WHEN (c."Telefone" IS NOT NULL AND c."Telefone" != '') THEN 3
                            ELSE 4
                        END ASC,
                        c."Data Atualização Contato" DESC NULLS LAST,
                        c."Data Criação Contato" DESC NULLS LAST
                ) AS rn
            FROM gold.rdstation_deal_contacts dc
            JOIN gold.rdstation_contacts c ON dc."Contact ID" = c."Contact ID"
        ),
        deals_base AS (
            SELECT
                d."Deal ID" AS deal_id,
                d."Negócio" AS deal_name,
                d."Status"  AS deal_status,
                d."Fonte"   AS fonte,
                d."Campanha ID" AS campanha_id,
                d."Campanha Nome" AS campanha_nome,
                d."Unidade" AS unidade,
                d."Funil"   AS funil,
                d."Data Criação" AS deal_date,
                bc."Contact ID" AS contact_id,
                bc."Nome Contato" AS contact_name,
                CASE 
                    WHEN bc."E-mail" IS NOT NULL AND trim(bc."E-mail") != '' AND bc."E-mail" LIKE '%@%'
                         THEN lower(trim(bc."E-mail"))
                    ELSE NULL
                END AS contact_email,
                CASE 
                    WHEN bc."Telefone" IS NOT NULL AND is_valid_phone(clean_phone_sql(bc."Telefone"))
                         THEN clean_phone_sql(bc."Telefone")
                    ELSE NULL
                END AS clean_phone,
                bc."Telefone" AS contact_phone,
                bc."CPF" AS contact_cpf
            FROM gold.rdstation_deals d
            LEFT JOIN best_contact_per_deal bc ON d."Deal ID" = bc."Deal ID" AND bc.rn = 1
        ),
        phone_to_canonical_email AS (
            SELECT clean_phone, arg_min(contact_email, deal_date) AS canonical_email
            FROM deals_base
            WHERE contact_email IS NOT NULL AND clean_phone IS NOT NULL
            GROUP BY clean_phone
        )
        SELECT
            d.*,
            CASE 
                WHEN d.contact_email IS NOT NULL THEN d.contact_email
                WHEN p.canonical_email IS NOT NULL THEN p.canonical_email
                WHEN d.clean_phone IS NOT NULL THEN 'phone_' || d.clean_phone
                WHEN d.contact_id IS NOT NULL THEN 'contact_' || d.contact_id
                ELSE 'deal_' || d.deal_id
            END AS lead_id
        FROM deals_base d
        LEFT JOIN phone_to_canonical_email p ON d.clean_phone = p.clean_phone
    """)
    logger.info(f"    Raw deals: {_count(con, 'tmp_rd_deals_raw'):,}")

    # -----------------------------------------------------------------------
    # 1b. Aggregate deals to Lead level (1 row per unique lead)
    # -----------------------------------------------------------------------
    logger.info("Step 1b — Aggregate deals to unique Lead level")
    _step(con, "tmp_leads_base", """
        CREATE OR REPLACE TEMP TABLE tmp_leads_base AS
        SELECT
            lead_id,
            COUNT(DISTINCT deal_id)                                            AS quantidade_de_contatos,
            MIN(deal_date)                                                     AS primeiro_contato,
            MAX(deal_date)                                                     AS ultimo_contato,
            arg_min(deal_name, deal_date)                                      AS deal_name,
            COALESCE(arg_min(fonte, deal_date), 'Não informada')               AS primeira_fonte,
            COALESCE(arg_max(fonte, deal_date), 'Não informada')               AS ultima_fonte,
            COALESCE(arg_min(unidade, deal_date), 'Não informada')             AS primeira_unidade,
            COALESCE(arg_max(unidade, deal_date), 'Não informada')             AS ultima_unidade,
            COALESCE(arg_min(funil, deal_date), '')                            AS primeiro_funil,
            COALESCE(arg_max(funil, deal_date), '')                            AS ultimo_funil,
            COUNT(DISTINCT campanha_id)                                        AS quantidade_de_campanhas,
            COALESCE(
                arg_min(campanha_nome, CASE WHEN campanha_nome IS NOT NULL AND campanha_nome != '' AND campanha_nome != 'Não informada' THEN deal_date END),
                'Não informada'
            )                                                                  AS primeira_campanha_nome,
            COALESCE(
                arg_max(campanha_nome, CASE WHEN campanha_nome IS NOT NULL AND campanha_nome != '' AND campanha_nome != 'Não informada' THEN deal_date END),
                'Não informada'
            )                                                                  AS ultima_campanha_nome,
            MAX(contact_email)                                                 AS lead_email,
            MAX(clean_phone)                                                   AS clean_phone
        FROM tmp_rd_deals_raw
        GROUP BY lead_id
    """)
    logger.info(f"    Unique leads: {_count(con, 'tmp_leads_base'):,}")

    # -----------------------------------------------------------------------
    # 2. Materialise RD valid emails per Lead (split multi-email fields)
    # -----------------------------------------------------------------------
    logger.info("Step 2 — Materialise RD valid emails per lead")
    _step(con, "tmp_rd_emails", """
        CREATE OR REPLACE TEMP TABLE tmp_rd_emails AS
        SELECT DISTINCT
            d.lead_id,
            lower(trim(e.email))                           AS clean_email,
            split_part(lower(trim(e.email)), '@', 1)       AS email_local,
            split_part(lower(trim(e.email)), '@', 2)       AS email_domain
        FROM tmp_rd_deals_raw d,
             LATERAL (
                 SELECT UNNEST(
                     regexp_split_to_array(d.contact_email, '[,;\\s]+')
                 ) AS email
             ) e
        WHERE d.contact_email IS NOT NULL
          AND d.contact_email != ''
          AND is_valid_email(lower(trim(e.email)))
    """)
    logger.info(f"    RD valid emails: {_count(con, 'tmp_rd_emails'):,}")

    # -----------------------------------------------------------------------
    # 2b. Materialise RD valid phones per Lead (pre-computed suffix)
    # -----------------------------------------------------------------------
    logger.info("Step 2b — Materialise RD phone suffixes per lead")
    _step(con, "tmp_rd_phones", """
        CREATE OR REPLACE TEMP TABLE tmp_rd_phones AS
        SELECT DISTINCT
            d.lead_id,
            d.clean_phone,
            right(d.clean_phone, 8) AS phone_suffix8
        FROM tmp_rd_deals_raw d
        WHERE d.clean_phone IS NOT NULL
          AND is_valid_phone(d.clean_phone)
    """)
    logger.info(f"    RD valid phones: {_count(con, 'tmp_rd_phones'):,}")

    # -----------------------------------------------------------------------
    # 3. Materialise Clinisys patients with clean fields pre-computed
    # -----------------------------------------------------------------------
    logger.info("Step 3 — Materialise Clinisys patients")
    _step(con, "tmp_clin", """
        CREATE OR REPLACE TEMP TABLE tmp_clin AS
        SELECT
            codigo,
            CASE WHEN is_valid_email(lower(trim(esposa_email)))
                 THEN lower(trim(esposa_email)) ELSE NULL
            END                                                     AS esposa_email,
            CASE WHEN is_valid_email(lower(trim(marido_email)))
                 THEN lower(trim(marido_email)) ELSE NULL
            END                                                     AS marido_email,
            split_part(lower(trim(esposa_email)), '@', 1)           AS esposa_email_local,
            split_part(lower(trim(esposa_email)), '@', 2)           AS esposa_email_domain,
            split_part(lower(trim(marido_email)), '@', 1)           AS marido_email_local,
            split_part(lower(trim(marido_email)), '@', 2)           AS marido_email_domain,
            CASE WHEN is_valid_phone(clean_phone_sql(esposa_celular))
                 THEN clean_phone_sql(esposa_celular) ELSE NULL
            END                                                     AS esposa_phone,
            CASE WHEN is_valid_phone(clean_phone_sql(marido_celular))
                 THEN clean_phone_sql(marido_celular) ELSE NULL
            END                                                     AS marido_phone,
            CASE WHEN is_valid_phone(clean_phone_sql(telefone))
                 THEN clean_phone_sql(telefone) ELSE NULL
            END                                                     AS general_phone,
            right(
                CASE WHEN is_valid_phone(clean_phone_sql(esposa_celular))
                     THEN clean_phone_sql(esposa_celular) ELSE NULL
                END, 8
            )                                                       AS esposa_phone_suffix8,
            right(
                CASE WHEN is_valid_phone(clean_phone_sql(marido_celular))
                     THEN clean_phone_sql(marido_celular) ELSE NULL
                END, 8
            )                                                       AS marido_phone_suffix8,
            right(
                CASE WHEN is_valid_phone(clean_phone_sql(telefone))
                     THEN clean_phone_sql(telefone) ELSE NULL
                END, 8
            )                                                       AS general_phone_suffix8
        FROM clinisys.silver.view_pacientes
    """)
    logger.info(f"    Clinisys patients: {_count(con, 'tmp_clin'):,}")

    # -----------------------------------------------------------------------
    # 4. Run all exact matches (priority 1-5) and materialise
    # -----------------------------------------------------------------------
    logger.info("Step 4 — Exact matches (email + phone exact + phone suffix)")
    _step(con, "tmp_matches_exact", """
        CREATE OR REPLACE TEMP TABLE tmp_matches_exact AS

        -- Match 1: Exact email - wife (priority 1)
        SELECT r.lead_id, c.codigo AS prontuario, c.esposa_email AS matched_email,
               'email_esposa_exact' AS matched_flag, 1 AS match_priority
        FROM tmp_rd_emails r
        JOIN tmp_clin c ON r.clean_email = c.esposa_email

        UNION ALL

        -- Match 2: Exact email - husband (priority 1)
        SELECT r.lead_id, c.codigo, c.marido_email,
               'email_marido_exact', 1
        FROM tmp_rd_emails r
        JOIN tmp_clin c ON r.clean_email = c.marido_email

        UNION ALL

        -- Match 3: Exact phone - wife (priority 2)
        SELECT r.lead_id, c.codigo, NULL,
               'phone_esposa_exact', 2
        FROM tmp_rd_phones r
        JOIN tmp_clin c ON r.clean_phone = c.esposa_phone
        WHERE c.esposa_phone IS NOT NULL

        UNION ALL

        -- Match 4: Exact phone - husband (priority 2)
        SELECT r.lead_id, c.codigo, NULL,
               'phone_marido_exact', 2
        FROM tmp_rd_phones r
        JOIN tmp_clin c ON r.clean_phone = c.marido_phone
        WHERE c.marido_phone IS NOT NULL

        UNION ALL

        -- Match 5: Exact phone - general (priority 3)
        SELECT r.lead_id, c.codigo, NULL,
               'phone_general_exact', 3
        FROM tmp_rd_phones r
        JOIN tmp_clin c ON r.clean_phone = c.general_phone
        WHERE c.general_phone IS NOT NULL

        UNION ALL

        -- Match 6: Phone suffix 8 - wife (priority 4)
        SELECT r.lead_id, c.codigo, NULL,
               'phone_esposa_suffix8', 4
        FROM tmp_rd_phones r
        JOIN tmp_clin c ON r.phone_suffix8 = c.esposa_phone_suffix8
        WHERE c.esposa_phone IS NOT NULL
          AND r.clean_phone != c.esposa_phone

        UNION ALL

        -- Match 7: Phone suffix 8 - husband (priority 4)
        SELECT r.lead_id, c.codigo, NULL,
               'phone_marido_suffix8', 4
        FROM tmp_rd_phones r
        JOIN tmp_clin c ON r.phone_suffix8 = c.marido_phone_suffix8
        WHERE c.marido_phone IS NOT NULL
          AND r.clean_phone != c.marido_phone

        UNION ALL

        -- Match 8: Phone suffix 8 - general (priority 5)
        SELECT r.lead_id, c.codigo, NULL,
               'phone_general_suffix8', 5
        FROM tmp_rd_phones r
        JOIN tmp_clin c ON r.phone_suffix8 = c.general_phone_suffix8
        WHERE c.general_phone IS NOT NULL
          AND r.clean_phone != c.general_phone
    """)
    logger.info(f"    Exact+suffix match candidates: {_count(con, 'tmp_matches_exact'):,}")

    # -----------------------------------------------------------------------
    # 5. Typo email matching — only for leads with NO exact match yet
    # -----------------------------------------------------------------------
    logger.info("Step 5 — Typo email matching (unmatched leads only)")
    _step(con, "tmp_unmatched_emails", """
        CREATE OR REPLACE TEMP TABLE tmp_unmatched_emails AS
        SELECT e.*
        FROM tmp_rd_emails e
        WHERE NOT EXISTS (
            SELECT 1 FROM tmp_matches_exact m WHERE m.lead_id = e.lead_id
        )
    """)
    unmatched_email_n = _count(con, 'tmp_unmatched_emails')
    logger.info(f"    Leads still unmatched with valid email: {unmatched_email_n:,}")

    _step(con, "tmp_matches_typo", f"""
        CREATE OR REPLACE TEMP TABLE tmp_matches_typo AS
        {'-- No unmatched leads with emails; skip typo join' if unmatched_email_n == 0 else ''}
        -- Match 9: Email typo - wife (priority 6)
        SELECT r.lead_id, c.codigo AS prontuario, c.esposa_email AS matched_email,
               'email_esposa_typo' AS matched_flag, 6 AS match_priority
        FROM tmp_unmatched_emails r
        JOIN tmp_clin c
          ON r.email_local = c.esposa_email_local
         AND r.clean_email != c.esposa_email
         AND c.esposa_email IS NOT NULL
         AND levenshtein(r.email_domain, c.esposa_email_domain) <= 2

        UNION ALL

        -- Match 10: Email typo - husband (priority 6)
        SELECT r.lead_id, c.codigo, c.marido_email,
               'email_marido_typo', 6
        FROM tmp_unmatched_emails r
        JOIN tmp_clin c
          ON r.email_local = c.marido_email_local
         AND r.clean_email != c.marido_email
         AND c.marido_email IS NOT NULL
         AND levenshtein(r.email_domain, c.marido_email_domain) <= 2
        {'-- (zero-row placeholder when no unmatched emails)' if unmatched_email_n == 0 else ''}
    """)
    logger.info(f"    Typo match candidates: {_count(con, 'tmp_matches_typo'):,}")

    # -----------------------------------------------------------------------
    # 6. Rank all matches and pick winner per lead
    # -----------------------------------------------------------------------
    logger.info("Step 6 — Rank matches and pick best per lead")
    _step(con, "tmp_best_match", """
        CREATE OR REPLACE TEMP TABLE tmp_best_match AS
        WITH all_matches AS (
            SELECT * FROM tmp_matches_exact
            UNION ALL
            SELECT * FROM tmp_matches_typo
        ),
        ranked AS (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY lead_id
                       ORDER BY match_priority ASC, prontuario DESC
                   ) AS rn
            FROM all_matches
        )
        SELECT lead_id, prontuario, matched_email, matched_flag
        FROM ranked
        WHERE rn = 1
    """)
    logger.info(f"    Leads with at least one match: {_count(con, 'tmp_best_match'):,}")

    # -----------------------------------------------------------------------
    # 7. Join matches back to unique Leads & resolve primary lead per prontuário
    # -----------------------------------------------------------------------
    logger.info("Step 7 — Join matches back to unique leads & resolve primary lead per prontuário")
    _step(con, "tmp_leads_matched", """
        CREATE OR REPLACE TEMP TABLE tmp_leads_matched AS
        WITH matched AS (
            SELECT 
                b.*,
                m.prontuario,
                m.matched_email,
                COALESCE(m.matched_flag, 'unmatched') AS matched_flag
            FROM tmp_leads_base b
            LEFT JOIN tmp_best_match m ON b.lead_id = m.lead_id
        )
        SELECT 
            m.*,
            CASE 
                WHEN m.prontuario IS NULL THEN TRUE
                WHEN ROW_NUMBER() OVER (
                    PARTITION BY m.prontuario 
                    ORDER BY m.primeiro_contato ASC, m.lead_id ASC
                ) = 1 THEN TRUE
                ELSE FALSE
            END AS is_lead_primario_paciente
        FROM matched m
    """)

    # -----------------------------------------------------------------------
    # 8. Consultation join
    # -----------------------------------------------------------------------
    logger.info("Step 8 — Consultation join")
    _step(con, "tmp_consultas", """
        CREATE OR REPLACE TEMP TABLE tmp_consultas AS
        WITH ranked AS (
            SELECT
                l.lead_id,
                c.procedimento_nome  AS consulta_type,
                c.data               AS consulta_date,
                CASE
                    WHEN c.data > CURRENT_DATE THEN 'Agendada'
                    ELSE c.chegou
                END                  AS consulta_status,
                ROW_NUMBER() OVER (
                    PARTITION BY l.lead_id
                    ORDER BY c.data ASC
                )                    AS rn
            FROM tmp_leads_matched l
            JOIN gold.extrato_atendimento_central c
              ON l.prontuario = c.paciente_codigo
             AND c.data >= l.primeiro_contato
             AND (
                 lower(strip_accents(c.procedimento_nome)) LIKE '%consulta%reprodu%'
                 OR lower(strip_accents(c.procedimento_nome)) LIKE '%consulta%preserva%'
             )
        )
        SELECT lead_id, consulta_type, consulta_date, consulta_status
        FROM ranked
        WHERE rn = 1
    """)
    logger.info(f"    Leads with qualifying consulta: {_count(con, 'tmp_consultas'):,}")

    # -----------------------------------------------------------------------
    # 8b. Financial sales aggregation (2-year window + Lifetime LTV)
    # -----------------------------------------------------------------------
    logger.info("Step 8b — Financial sales join (Protheus, 2-year window + Lifetime LTV)")
    _step(con, "tmp_vendas", """
        CREATE OR REPLACE TEMP TABLE tmp_vendas AS
        SELECT
            l.lead_id,
            -- Janela de 2 anos pós-lead/consulta
            COALESCE(SUM(CASE 
                WHEN p.dt_emissao <= (COALESCE(c.consulta_date, l.primeiro_contato) + INTERVAL 2 YEAR) 
                THEN p.valor_total 
                ELSE 0.0 
            END), 0.0) AS total_gasto_janela_2anos,
            COUNT(CASE 
                WHEN p.dt_emissao <= (COALESCE(c.consulta_date, l.primeiro_contato) + INTERVAL 2 YEAR) 
                THEN 1 
            END) AS qtd_itens_vendidos_janela_2anos,
            COUNT(DISTINCT CASE 
                WHEN p.dt_emissao <= (COALESCE(c.consulta_date, l.primeiro_contato) + INTERVAL 2 YEAR) 
                THEN p.num_nota 
            END) AS qtd_pedidos_janela_2anos,

            -- Vitalício pós-lead (LTV Total)
            COALESCE(SUM(p.valor_total), 0.0) AS total_gasto_vitalicio,
            COUNT(*)                          AS qtd_itens_vendidos_vitalicio,
            COUNT(DISTINCT p.num_nota)        AS qtd_pedidos_vitalicio,

            -- Primeira venda e indicador de reativação tardia
            MIN(p.dt_emissao)                 AS primeira_venda_data,
            CASE 
                WHEN MAX(p.dt_emissao) > (COALESCE(c.consulta_date, l.primeiro_contato) + INTERVAL 2 YEAR) 
                THEN TRUE 
                ELSE FALSE 
            END AS vendeu_apos_janela_2anos
        FROM tmp_leads_matched l
        LEFT JOIN tmp_consultas c ON l.lead_id = c.lead_id
        JOIN gold.protheus_vendas_consolidadas p
          ON l.prontuario = p.prontuario
         AND p.dt_emissao >= l.primeiro_contato
        GROUP BY l.lead_id, c.consulta_date, l.primeiro_contato
    """)
    logger.info(f"    Leads with post-lead sales: {_count(con, 'tmp_vendas'):,}")

    # -----------------------------------------------------------------------
    # 9. Build final gold table (gold.rd_station_leads_funil)
    # -----------------------------------------------------------------------
    logger.info("Step 9 — Write gold.rd_station_leads_funil (Lead-level granularity)")
    query = """
        CREATE OR REPLACE TABLE gold.rd_station_leads_funil AS
        SELECT
            l.lead_id,
            l.prontuario,
            l.deal_name,
            l.quantidade_de_contatos,
            l.primeiro_contato,
            l.ultimo_contato,
            l.primeira_fonte,
            l.ultima_fonte,
            l.quantidade_de_campanhas,
            l.primeira_campanha_nome,
            l.ultima_campanha_nome,
            l.primeira_unidade,
            l.ultima_unidade,
            l.primeiro_funil,
            l.ultimo_funil,
            l.matched_email,
            l.matched_flag,
            -- Clean category for downstream BI
            CASE
                WHEN l.matched_flag LIKE 'email_esposa%'  THEN 'email_esposa'
                WHEN l.matched_flag LIKE 'email_marido%'  THEN 'email_marido'
                WHEN l.matched_flag LIKE 'phone_esposa%'  THEN 'celular_esposa'
                WHEN l.matched_flag LIKE 'phone_marido%'  THEN 'celular_marido'
                WHEN l.matched_flag LIKE 'phone_general%' THEN 'celular_geral'
                ELSE 'unmatched'
            END AS match_category,
            l.is_lead_primario_paciente,
            c.consulta_type,
            c.consulta_date,
            c.consulta_status,
            date_diff('day', l.primeiro_contato, c.consulta_date) AS dias_ate_primeira_consulta,
            
            -- Receita Atribuída (Zero para secundários do casal para eliminar inflação em SUM())
            CASE 
                WHEN l.is_lead_primario_paciente THEN COALESCE(v.total_gasto_janela_2anos, 0.0) 
                ELSE 0.0 
            END AS total_gasto_pos_lead,
            
            -- Métrica Janela 2 anos
            CASE 
                WHEN l.is_lead_primario_paciente THEN COALESCE(v.total_gasto_janela_2anos, 0.0) 
                ELSE 0.0 
            END AS total_gasto_janela_2anos,
            CASE 
                WHEN l.is_lead_primario_paciente THEN COALESCE(v.qtd_itens_vendidos_janela_2anos, 0) 
                ELSE 0 
            END AS qtd_itens_vendidos_janela_2anos,
            CASE 
                WHEN l.is_lead_primario_paciente THEN COALESCE(v.qtd_pedidos_janela_2anos, 0) 
                ELSE 0 
            END AS qtd_pedidos_janela_2anos,

            -- Métrica Vitalícia (LTV Total)
            CASE 
                WHEN l.is_lead_primario_paciente THEN COALESCE(v.total_gasto_vitalicio, 0.0) 
                ELSE 0.0 
            END AS total_gasto_vitalicio,
            CASE 
                WHEN l.is_lead_primario_paciente THEN COALESCE(v.qtd_itens_vendidos_vitalicio, 0) 
                ELSE 0 
            END AS qtd_itens_vendidos_vitalicio,
            CASE 
                WHEN l.is_lead_primario_paciente THEN COALESCE(v.qtd_pedidos_vitalicio, 0) 
                ELSE 0 
            END AS qtd_pedidos_vitalicio,

            -- Receita Total do Prontuário (sempre preenchida, para conferência de casal)
            COALESCE(v.total_gasto_vitalicio, 0.0) AS total_gasto_compartilhado_familia,

            v.primeira_venda_data,
            date_diff('day', l.primeiro_contato, v.primeira_venda_data) AS dias_ate_primeira_venda,
            COALESCE(v.vendeu_apos_janela_2anos, FALSE) AS vendeu_apos_janela_2anos,

            -- Status do Ciclo de Inatividade
            COALESCE(c.consulta_date, l.primeiro_contato) + INTERVAL 2 YEAR AS data_inatividade_limite,
            CASE 
                WHEN CURRENT_DATE >= (COALESCE(c.consulta_date, l.primeiro_contato) + INTERVAL 2 YEAR) THEN TRUE
                ELSE FALSE
            END AS closed
        FROM tmp_leads_matched l
        LEFT JOIN tmp_consultas c ON l.lead_id = c.lead_id
        LEFT JOIN tmp_vendas v    ON l.lead_id = v.lead_id
    """
    _step(con, "gold.rd_station_leads_funil", query)

    # Backward compatibility view with legacy aliases
    con.execute("""
        CREATE OR REPLACE VIEW gold.leads_funil AS 
        SELECT 
            *,
            primeiro_contato AS lead_from,
            ultimo_contato AS lead_to,
            '' AS deal_status,
            lead_id AS lead_email
        FROM gold.rd_station_leads_funil
    """)

    # -----------------------------------------------------------------------
    # 10. Audit log
    # -----------------------------------------------------------------------
    total          = _count(con, 'gold.rd_station_leads_funil')
    matched        = con.execute("SELECT COUNT(*) FROM gold.rd_station_leads_funil WHERE prontuario IS NOT NULL").fetchone()[0]
    typos          = con.execute("SELECT COUNT(*) FROM gold.rd_station_leads_funil WHERE matched_flag LIKE '%typo%'").fetchone()[0]
    consultas      = con.execute("SELECT COUNT(*) FROM gold.rd_station_leads_funil WHERE consulta_date IS NOT NULL").fetchone()[0]
    with_vendas    = con.execute("SELECT COUNT(*) FROM gold.rd_station_leads_funil WHERE total_gasto_pos_lead > 0").fetchone()[0]
    rev_2anos      = con.execute("SELECT COALESCE(SUM(total_gasto_janela_2anos), 0.0) FROM gold.rd_station_leads_funil").fetchone()[0]
    rev_vitalicia  = con.execute("SELECT COALESCE(SUM(total_gasto_vitalicio), 0.0) FROM gold.rd_station_leads_funil").fetchone()[0]
    closed_leads   = con.execute("SELECT COUNT(*) FROM gold.rd_station_leads_funil WHERE closed = TRUE").fetchone()[0]
    multi_contatos = con.execute("SELECT COUNT(*) FROM gold.rd_station_leads_funil WHERE quantidade_de_contatos > 1").fetchone()[0]
    secondary_c    = con.execute("SELECT COUNT(*) FROM gold.rd_station_leads_funil WHERE is_lead_primario_paciente = FALSE").fetchone()[0]
    unmatched      = total - matched

    logger.info("=== AUDIT ===")
    logger.info(f"  Total unique leads  : {total:,}")
    logger.info(f"  Leads with >1 deal  : {multi_contatos:,} ({multi_contatos/total*100:.1f}%)" if total else "  Multi deals: 0")
    logger.info(f"  Secondary leads/casal: {secondary_c:,} (deduplicated from financial sum)")
    logger.info(f"  Closed (inactive)   : {closed_leads:,} ({closed_leads/total*100:.1f}%)" if total else "  Closed: 0")
    logger.info(f"  Matched to patient  : {matched:,} ({matched/total*100:.1f}%)" if total else "  Matched: 0")
    logger.info(f"  Typos resolved      : {typos:,}")
    logger.info(f"  Unmatched           : {unmatched:,} ({unmatched/total*100:.1f}%)" if total else "  Unmatched: 0")
    logger.info(f"  With consulta       : {consultas:,} ({consultas/matched*100:.1f}% of matched)" if matched else "  With consulta: 0")
    logger.info(f"  With sales (>0)     : {with_vendas:,} ({with_vendas/matched*100:.1f}% of matched)" if matched else "  With sales: 0")
    logger.info(f"  Revenue (2-yr cap)  : R$ {rev_2anos:,.2f}")
    logger.info(f"  Revenue (Full LTV)  : R$ {rev_vitalicia:,.2f}")

    con.execute("DETACH clinisys")


def main():
    logger.info("=== LEADS FUNIL CONSOLIDATION STARTED ===")
    logger.info(f"Target Database : {DUCKDB_PATH}")
    logger.info(f"Clinisys Database: {CLINISYS_PATH}")

    t0 = time.time()
    try:
        with duckdb.connect(DUCKDB_PATH) as con:
            con.execute("CREATE SCHEMA IF NOT EXISTS gold")
            create_leads_funil(con)
            logger.info(f"Total elapsed: {time.time() - t0:.1f}s")
            logger.info("=== LEADS FUNIL CONSOLIDATION FINISHED SUCCESSFUL ===")
    except Exception as e:
        logger.error(f"Consolidation Failed: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
