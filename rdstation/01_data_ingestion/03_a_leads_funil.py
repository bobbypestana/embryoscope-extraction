#!/usr/bin/env python3
"""
RD Station and Clinisys Funnel Integration Script
Creates the gold.leads_funil table by matching RD Station deals
with Clinisys patients and checking for subsequent consultations.

Matching pipeline (in priority order):
  1. Exact email - wife       (priority 1)
  2. Exact email - husband    (priority 1)
  3. Exact phone - wife       (priority 2)
  4. Exact phone - husband    (priority 2)
  5. Exact phone - general    (priority 3)
  6. Phone 8-digit suffix - wife     (priority 4)
  7. Phone 8-digit suffix - husband  (priority 4)
  8. Phone 8-digit suffix - general  (priority 5)
  9. Email typo (domain levenshtein ≤2) - wife   (priority 6, only for still-unmatched leads)
 10. Email typo (domain levenshtein ≤2) - husband (priority 6, only for still-unmatched leads)

Optimizations vs v1:
  - Pre-materialised temp tables for RD emails/phones and Clinisys patients
    so DuckDB can build hash indexes on clean columns.
  - Phone OR join split into two separate equi-joins (exact + suffix UNION)
    to guarantee hash-join path for each branch.
  - Typo matching runs only on leads that had no exact match (much smaller set).
  - SIMILAR TO replaced by two LIKE conditions.
  - Monolithic CTE broken into explicit Python steps with per-step timing.
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
        # Try to get rowcount from the last temp table touched
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
    # 1. Materialise RD deals (with window for next_lead_date)
    # -----------------------------------------------------------------------
    logger.info("Step 1 — Materialise RD deals base")
    con.execute("DROP TABLE IF EXISTS tmp_rd_deals")
    _step(con, "tmp_rd_deals", """
        CREATE TEMP TABLE tmp_rd_deals AS
        SELECT
            d."Deal ID"                                                          AS lead_id,
            d."Data Criação"                                                     AS lead_date,
            c."Contact ID"                                                       AS contact_id,
            c."E-mail"                                                           AS lead_email,
            CASE WHEN is_valid_phone(clean_phone_sql(c."Telefone"))
                 THEN clean_phone_sql(c."Telefone")
                 ELSE NULL
            END                                                                  AS clean_phone,
            -- Next deal date for same contact — defines attribution window upper bound
            LEAD(d."Data Criação") OVER (
                PARTITION BY c."Contact ID"
                ORDER BY d."Data Criação" ASC
            )                                                                    AS next_lead_date
        FROM gold.rdstation_deals d
        LEFT JOIN gold.rdstation_deal_contacts dc ON d."Deal ID" = dc."Deal ID"
        LEFT JOIN gold.rdstation_contacts       c  ON dc."Contact ID" = c."Contact ID"
    """)
    logger.info(f"    RD deals: {_count(con, 'tmp_rd_deals'):,}")

    # -----------------------------------------------------------------------
    # 2. Materialise RD valid emails (split multi-email fields)
    # -----------------------------------------------------------------------
    logger.info("Step 2 — Materialise RD valid emails")
    con.execute("DROP TABLE IF EXISTS tmp_rd_emails")
    _step(con, "tmp_rd_emails", """
        CREATE TEMP TABLE tmp_rd_emails AS
        SELECT DISTINCT
            d.lead_id,
            lower(trim(e.email))                           AS clean_email,
            split_part(lower(trim(e.email)), '@', 1)       AS email_local,
            split_part(lower(trim(e.email)), '@', 2)       AS email_domain
        FROM tmp_rd_deals d,
             LATERAL (
                 SELECT UNNEST(
                     regexp_split_to_array(d.lead_email, '[,;\\s]+')
                 ) AS email
             ) e
        WHERE d.lead_email IS NOT NULL
          AND d.lead_email != ''
          AND is_valid_email(lower(trim(e.email)))
    """)
    logger.info(f"    RD valid emails: {_count(con, 'tmp_rd_emails'):,}")

    # Phone 8-digit suffix pre-computed
    logger.info("Step 2b — Materialise RD phone suffixes")
    con.execute("DROP TABLE IF EXISTS tmp_rd_phones")
    _step(con, "tmp_rd_phones", """
        CREATE TEMP TABLE tmp_rd_phones AS
        SELECT
            lead_id,
            clean_phone,
            right(clean_phone, 8) AS phone_suffix8
        FROM tmp_rd_deals
        WHERE clean_phone IS NOT NULL
    """)
    logger.info(f"    RD valid phones: {_count(con, 'tmp_rd_phones'):,}")

    # -----------------------------------------------------------------------
    # 3. Materialise Clinisys patients with clean fields pre-computed
    # -----------------------------------------------------------------------
    logger.info("Step 3 — Materialise Clinisys patients")
    con.execute("DROP TABLE IF EXISTS tmp_clin")
    _step(con, "tmp_clin", """
        CREATE TEMP TABLE tmp_clin AS
        SELECT
            codigo,
            -- Emails (clean + decomposed for typo matching)
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
            -- Phones (clean + suffix)
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
    con.execute("DROP TABLE IF EXISTS tmp_matches_exact")
    _step(con, "tmp_matches_exact", """
        CREATE TEMP TABLE tmp_matches_exact AS

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
        -- Equi-join on suffix; filter out exact (already captured above)
        SELECT r.lead_id, c.codigo, NULL,
               'phone_esposa_suffix8', 4
        FROM tmp_rd_phones r
        JOIN tmp_clin c ON r.phone_suffix8 = c.esposa_phone_suffix8
        WHERE c.esposa_phone IS NOT NULL
          AND r.clean_phone != c.esposa_phone   -- exclude exact (already priority 2)

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
    con.execute("DROP TABLE IF EXISTS tmp_unmatched_emails")
    # Identify leads that had no exact/suffix match
    _step(con, "tmp_unmatched_emails", """
        CREATE TEMP TABLE tmp_unmatched_emails AS
        SELECT e.*
        FROM tmp_rd_emails e
        WHERE NOT EXISTS (
            SELECT 1 FROM tmp_matches_exact m WHERE m.lead_id = e.lead_id
        )
    """)
    unmatched_email_n = _count(con, 'tmp_unmatched_emails')
    logger.info(f"    Leads still unmatched with valid email: {unmatched_email_n:,}")

    con.execute("DROP TABLE IF EXISTS tmp_matches_typo")
    _step(con, "tmp_matches_typo", f"""
        CREATE TEMP TABLE tmp_matches_typo AS
        {'-- No unmatched leads with emails; skip typo join' if unmatched_email_n == 0 else ''}
        -- Match 9: Email typo - wife (priority 6)
        SELECT r.lead_id, c.codigo AS prontuario, c.esposa_email AS matched_email,
               'email_esposa_typo' AS matched_flag, 6 AS match_priority
        FROM tmp_unmatched_emails r
        JOIN tmp_clin c
          ON r.email_local = c.esposa_email_local          -- equi-join on local part
         AND r.clean_email != c.esposa_email               -- skip exact (shouldn't exist, but guard)
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
    con.execute("DROP TABLE IF EXISTS tmp_best_match")
    _step(con, "tmp_best_match", """
        CREATE TEMP TABLE tmp_best_match AS
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
    # 7. Join matches back to all deals
    # -----------------------------------------------------------------------
    logger.info("Step 7 — Join matches back to all deals")
    con.execute("DROP TABLE IF EXISTS tmp_leads_matched")
    _step(con, "tmp_leads_matched", """
        CREATE TEMP TABLE tmp_leads_matched AS
        SELECT
            d.lead_id,
            d.lead_date,
            d.next_lead_date,
            d.lead_email,
            m.prontuario,
            m.matched_email,
            COALESCE(m.matched_flag, 'unmatched') AS matched_flag
        FROM tmp_rd_deals d
        LEFT JOIN tmp_best_match m ON d.lead_id = m.lead_id
    """)

    # -----------------------------------------------------------------------
    # 8. Consultation join (post-lead, within attribution window)
    #    SIMILAR TO replaced by two LIKE conditions for performance
    # -----------------------------------------------------------------------
    logger.info("Step 8 — Consultation join")
    con.execute("DROP TABLE IF EXISTS tmp_consultas")
    _step(con, "tmp_consultas", """
        CREATE TEMP TABLE tmp_consultas AS
        WITH ranked AS (
            SELECT
                l.lead_id,
                c.procedimento_nome  AS consulta_type,
                c.data               AS consulta_date,
                -- Future consultations are marked as 'Agendada'
                CASE
                    WHEN c.data > CURRENT_DATE THEN 'Agendada'
                    ELSE c.chegou
                END                  AS consulta_status,
                ROW_NUMBER() OVER (
                    PARTITION BY l.lead_id
                    -- ASC = first consultation after the lead (not most recent)
                    ORDER BY c.data ASC
                )                    AS rn
            FROM tmp_leads_matched l
            JOIN gold.extrato_atendimento_central c
              ON l.prontuario = c.paciente_codigo
             AND c.data >= l.lead_date
             AND (l.next_lead_date IS NULL OR c.data < l.next_lead_date)
             -- SIMILAR TO replaced: two LIKE checks are index-friendlier
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
    # 8b. Financial sales aggregation (post-lead, within attribution window)
    # -----------------------------------------------------------------------
    logger.info("Step 8b — Financial sales join (Protheus)")
    con.execute("DROP TABLE IF EXISTS tmp_vendas")
    _step(con, "tmp_vendas", """
        CREATE TEMP TABLE tmp_vendas AS
        SELECT
            l.lead_id,
            COALESCE(SUM(p.valor_total), 0.0) AS total_gasto_pos_lead,
            COUNT(*)                          AS qtd_itens_vendidos_pos_lead,
            COUNT(DISTINCT p.pedido)          AS qtd_pedidos_pos_lead,
            MIN(p.dt_emissao)                 AS primeira_venda_data
        FROM tmp_leads_matched l
        JOIN gold.protheus_vendas_consolidadas p
          ON l.prontuario = p.prontuario
         AND p.dt_emissao >= l.lead_date
         AND (l.next_lead_date IS NULL OR p.dt_emissao < l.next_lead_date)
        GROUP BY l.lead_id
    """)
    logger.info(f"    Leads with post-lead sales: {_count(con, 'tmp_vendas'):,}")

    # -----------------------------------------------------------------------
    # 9. Build final gold table
    # -----------------------------------------------------------------------
    logger.info("Step 9 — Write gold.leads_funil")
    con.execute("DROP TABLE IF EXISTS gold.leads_funil")
    _step(con, "gold.leads_funil", """
        CREATE TABLE gold.leads_funil AS
        SELECT
            l.lead_id,
            l.lead_date,
            l.lead_email,
            l.prontuario,
            l.matched_email,
            l.matched_flag,
            -- Clean category for dashboarding
            CASE
                WHEN l.matched_flag LIKE 'email_esposa%'  THEN 'email_esposa'
                WHEN l.matched_flag LIKE 'email_marido%'  THEN 'email_marido'
                WHEN l.matched_flag LIKE 'phone_esposa%'  THEN 'celular_esposa'
                WHEN l.matched_flag LIKE 'phone_marido%'  THEN 'celular_marido'
                WHEN l.matched_flag LIKE 'phone_general%' THEN 'celular_geral'
                ELSE 'unmatched'
            END AS match_category,
            c.consulta_type,
            c.consulta_date,
            c.consulta_status,
            COALESCE(v.total_gasto_pos_lead, 0.0) AS total_gasto_pos_lead,
            COALESCE(v.qtd_itens_vendidos_pos_lead, 0) AS qtd_itens_vendidos_pos_lead,
            COALESCE(v.qtd_pedidos_pos_lead, 0) AS qtd_pedidos_pos_lead,
            v.primeira_venda_data
        FROM tmp_leads_matched l
        LEFT JOIN tmp_consultas c ON l.lead_id = c.lead_id
        LEFT JOIN tmp_vendas v    ON l.lead_id = v.lead_id
    """)

    # -----------------------------------------------------------------------
    # 10. Audit log
    # -----------------------------------------------------------------------
    total       = _count(con, 'gold.leads_funil')
    matched     = con.execute("SELECT COUNT(*) FROM gold.leads_funil WHERE prontuario IS NOT NULL").fetchone()[0]
    typos       = con.execute("SELECT COUNT(*) FROM gold.leads_funil WHERE matched_flag LIKE '%typo%'").fetchone()[0]
    consultas   = con.execute("SELECT COUNT(*) FROM gold.leads_funil WHERE consulta_date IS NOT NULL").fetchone()[0]
    with_vendas = con.execute("SELECT COUNT(*) FROM gold.leads_funil WHERE total_gasto_pos_lead > 0").fetchone()[0]
    total_val   = con.execute("SELECT COALESCE(SUM(total_gasto_pos_lead), 0.0) FROM gold.leads_funil").fetchone()[0]
    unmatched   = total - matched

    logger.info("=== AUDIT ===")
    logger.info(f"  Total deals      : {total:,}")
    logger.info(f"  Matched          : {matched:,} ({matched/total*100:.1f}%)" if total else "  Matched: 0")
    logger.info(f"  Typos resolved   : {typos:,}")
    logger.info(f"  Unmatched        : {unmatched:,} ({unmatched/total*100:.1f}%)" if total else "  Unmatched: 0")
    logger.info(f"  With consulta    : {consultas:,} ({consultas/matched*100:.1f}% of matched)" if matched else "  With consulta: 0")
    logger.info(f"  With sales (>0)  : {with_vendas:,} ({with_vendas/matched*100:.1f}% of matched)" if matched else "  With sales: 0")
    logger.info(f"  Total revenue    : R$ {total_val:,.2f}")

    # -----------------------------------------------------------------------
    # Cleanup temp tables
    # -----------------------------------------------------------------------
    for t in [
        "tmp_rd_deals", "tmp_rd_emails", "tmp_rd_phones",
        "tmp_clin", "tmp_matches_exact", "tmp_unmatched_emails",
        "tmp_matches_typo", "tmp_best_match", "tmp_leads_matched", "tmp_consultas",
        "tmp_vendas"
    ]:
        con.execute(f"DROP TABLE IF EXISTS {t}")

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
