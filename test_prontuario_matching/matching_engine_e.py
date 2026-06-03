"""
matching_engine_e.py — Strategy E: Controlled and Optimized Patient Matching
===========================================================================
Prioritizes matching based on:
1. ID matching type group:
   - Group 1: id matches codigo
   - Group 2: id matches any esposa column
   - Group 3: id matches any marido column
   - Group 4: id matches any responsavel column

2. Within each group, name similarity:
   - Rank 1: Full name match (p_words = c_words)
   - Rank 2: First name and last name match (first and last words present)
   - Rank 3: First name and some name match (first word + at least one other word present)
   - Rank 4: First name matches only (first word present, other words absent)

Cases where first name doesn't match are flagged as non-match (Score 5) and discarded.
Uses early filtering on source IDs to optimize Clinisys unpivoting and name parsing.
"""

import os
import sys
import logging
import time
import duckdb
import pandas as pd

logger = logging.getLogger(__name__)

def _t(label: str, start: float) -> None:
    logger.info("  [T] %-40s %6.0f ms", label, (time.perf_counter() - start) * 1000)

def run_strategy_e(
    source_con: duckdb.DuckDBPyConnection,
    clinisys_db_path: str,
    source_schema: str,
    source_table: str,
    id_col: str,
    name_col: str,
    label: str = "",
) -> pd.DataFrame:
    """
    Runs Strategy E patient matching on the source table.
    Does NOT modify the target table directly; returns a pd.DataFrame with results.
    """
    tag = label or f"{source_schema}.{source_table}"
    t_start = time.perf_counter()
    logger.info("[%s] Starting Strategy E matching", tag)

    # 1. Attach clinisys DB
    try:
        source_con.execute(f"ATTACH '{clinisys_db_path}' AS clinisys_all (READ_ONLY)")
        logger.info("[%s] Attached clinisys_all", tag)
    except Exception as e:
        if "already attached" in str(e).lower():
            logger.info("[%s] clinisys_all already attached", tag)
        else:
            raise

    # 1b. Check if original prontuario column exists for tie-breaker matching
    cols_df = source_con.execute(f"PRAGMA table_info('{source_schema}.\"{source_table}\"')").df()
    col_names = {c.lower() for c in cols_df["name"]}
    
    if "prontuario_old" in col_names:
        orig_pront_col = "prontuario_old"
    elif "prontuario" in col_names:
        orig_pront_col = "prontuario"
    else:
        orig_pront_col = "NULL"
        
    orig_pront_select = f'TRY_CAST("{orig_pront_col}" AS BIGINT)' if orig_pront_col != "NULL" else "CAST(NULL AS BIGINT)"

    # 2. Extract source IDs and parse names
    # Name cleaning splits on commas (for LASTNAME, FIRSTNAME format) and removes accents/symbols
    t0 = time.perf_counter()
    source_con.execute(f"""
        CREATE OR REPLACE TEMP TABLE __source_extract AS
        SELECT DISTINCT
            CAST("{id_col}" AS VARCHAR) AS source_id,
            TRY_CAST("{id_col}" AS BIGINT) AS source_id_numeric,
            "{name_col}" AS patient_name,
            {orig_pront_select} AS original_prontuario,
            list_filter(
                regexp_split_to_array(
                    regexp_replace(
                        strip_accents(
                            lower(
                                trim(
                                    CASE
                                        WHEN "{name_col}" IS NOT NULL THEN
                                            CASE
                                                WHEN POSITION(',' IN "{name_col}") > 0 THEN
                                                    SPLIT_PART("{name_col}", ',', 2) || ' ' || SPLIT_PART("{name_col}", ',', 1)
                                                ELSE "{name_col}"
                                            END
                                        ELSE NULL
                                    END
                                )
                            )
                        ),
                        '[^a-z]', ' ', 'g'
                    ),
                    '\\s+'
                ),
                w -> length(w) > 0
            ) AS p_words
        FROM {source_schema}."{source_table}"
        WHERE "{id_col}" IS NOT NULL AND TRIM(CAST("{id_col}" AS VARCHAR)) != '';
    """)
    _t("Extract and clean source names", t0)

    # 3. Create name scoring macro in session
    source_con.execute("""
        CREATE OR REPLACE TEMPORARY MACRO score_name(p_w, c_w) AS (
            CASE
                WHEN p_w IS NULL OR len(p_w) = 0 THEN 1
                WHEN c_w IS NULL OR len(c_w) = 0 THEN 5
                WHEN p_w = c_w THEN 1
                WHEN NOT list_contains(c_w, p_w[1]) THEN 5
                WHEN list_contains(c_w, p_w[-1]) THEN 2
                WHEN len(list_filter(p_w, w -> list_contains(c_w, w))) > 1 THEN 3
                ELSE 4
            END
        );
    """)

    # 4. Unpivot Clinisys database only for rows matching source_id_numeric
    t0 = time.perf_counter()
    source_con.execute("""
        CREATE OR REPLACE TEMP TABLE __clinisys_unpivoted AS
        -- Group 1: Main (codigo)
        SELECT
            codigo AS prontuario,
            codigo AS link_id,
            1 AS priority_group,
            esposa_nome,
            marido_nome,
            responsavel_nome,
            inativo
        FROM clinisys_all.silver.view_pacientes
        WHERE codigo IN (SELECT source_id_numeric FROM __source_extract)

        UNION ALL

        -- Group 2: Esposa columns
        SELECT
            codigo AS prontuario,
            TRY_CAST(prontuario_esposa AS BIGINT) AS link_id,
            2 AS priority_group,
            esposa_nome,
            NULL AS marido_nome,
            NULL AS responsavel_nome,
            inativo
        FROM clinisys_all.silver.view_pacientes
        WHERE TRY_CAST(prontuario_esposa AS BIGINT) IN (SELECT source_id_numeric FROM __source_extract)
        UNION ALL
        SELECT
            codigo AS prontuario,
            TRY_CAST(prontuario_esposa_pel AS BIGINT) AS link_id,
            2 AS priority_group,
            esposa_nome,
            NULL AS marido_nome,
            NULL AS responsavel_nome,
            inativo
        FROM clinisys_all.silver.view_pacientes
        WHERE TRY_CAST(prontuario_esposa_pel AS BIGINT) IN (SELECT source_id_numeric FROM __source_extract)
        UNION ALL
        SELECT
            codigo AS prontuario,
            TRY_CAST(prontuario_esposa_pc AS BIGINT) AS link_id,
            2 AS priority_group,
            esposa_nome,
            NULL AS marido_nome,
            NULL AS responsavel_nome,
            inativo
        FROM clinisys_all.silver.view_pacientes
        WHERE TRY_CAST(prontuario_esposa_pc AS BIGINT) IN (SELECT source_id_numeric FROM __source_extract)
        UNION ALL
        SELECT
            codigo AS prontuario,
            TRY_CAST(prontuario_esposa_fc AS BIGINT) AS link_id,
            2 AS priority_group,
            esposa_nome,
            NULL AS marido_nome,
            NULL AS responsavel_nome,
            inativo
        FROM clinisys_all.silver.view_pacientes
        WHERE TRY_CAST(prontuario_esposa_fc AS BIGINT) IN (SELECT source_id_numeric FROM __source_extract)
        UNION ALL
        SELECT
            codigo AS prontuario,
            TRY_CAST(prontuario_esposa_ba AS BIGINT) AS link_id,
            2 AS priority_group,
            esposa_nome,
            NULL AS marido_nome,
            NULL AS responsavel_nome,
            inativo
        FROM clinisys_all.silver.view_pacientes
        WHERE TRY_CAST(prontuario_esposa_ba AS BIGINT) IN (SELECT source_id_numeric FROM __source_extract)

        UNION ALL

        -- Group 3: Marido columns
        SELECT
            codigo AS prontuario,
            TRY_CAST(prontuario_marido AS BIGINT) AS link_id,
            3 AS priority_group,
            NULL AS esposa_nome,
            marido_nome,
            NULL AS responsavel_nome,
            inativo
        FROM clinisys_all.silver.view_pacientes
        WHERE TRY_CAST(prontuario_marido AS BIGINT) IN (SELECT source_id_numeric FROM __source_extract)
        UNION ALL
        SELECT
            codigo AS prontuario,
            TRY_CAST(prontuario_marido_pel AS BIGINT) AS link_id,
            3 AS priority_group,
            NULL AS esposa_nome,
            marido_nome,
            NULL AS responsavel_nome,
            inativo
        FROM clinisys_all.silver.view_pacientes
        WHERE TRY_CAST(prontuario_marido_pel AS BIGINT) IN (SELECT source_id_numeric FROM __source_extract)
        UNION ALL
        SELECT
            codigo AS prontuario,
            TRY_CAST(prontuario_marido_pc AS BIGINT) AS link_id,
            3 AS priority_group,
            NULL AS esposa_nome,
            marido_nome,
            NULL AS responsavel_nome,
            inativo
        FROM clinisys_all.silver.view_pacientes
        WHERE TRY_CAST(prontuario_marido_pc AS BIGINT) IN (SELECT source_id_numeric FROM __source_extract)
        UNION ALL
        SELECT
            codigo AS prontuario,
            TRY_CAST(prontuario_marido_fc AS BIGINT) AS link_id,
            3 AS priority_group,
            NULL AS esposa_nome,
            marido_nome,
            NULL AS responsavel_nome,
            inativo
        FROM clinisys_all.silver.view_pacientes
        WHERE TRY_CAST(prontuario_marido_fc AS BIGINT) IN (SELECT source_id_numeric FROM __source_extract)
        UNION ALL
        SELECT
            codigo AS prontuario,
            TRY_CAST(prontuario_marido_ba AS BIGINT) AS link_id,
            3 AS priority_group,
            NULL AS esposa_nome,
            marido_nome,
            NULL AS responsavel_nome,
            inativo
        FROM clinisys_all.silver.view_pacientes
        WHERE TRY_CAST(prontuario_marido_ba AS BIGINT) IN (SELECT source_id_numeric FROM __source_extract)

        UNION ALL

        -- Group 4: Responsavel columns
        SELECT
            codigo AS prontuario,
            TRY_CAST(prontuario_responsavel1 AS BIGINT) AS link_id,
            4 AS priority_group,
            NULL AS esposa_nome,
            NULL AS marido_nome,
            responsavel_nome,
            inativo
        FROM clinisys_all.silver.view_pacientes
        WHERE TRY_CAST(prontuario_responsavel1 AS BIGINT) IN (SELECT source_id_numeric FROM __source_extract)
        UNION ALL
        SELECT
            codigo AS prontuario,
            TRY_CAST(prontuario_responsavel2 AS BIGINT) AS link_id,
            4 AS priority_group,
            NULL AS esposa_nome,
            NULL AS marido_nome,
            responsavel_nome,
            inativo
        FROM clinisys_all.silver.view_pacientes
        WHERE TRY_CAST(prontuario_responsavel2 AS BIGINT) IN (SELECT source_id_numeric FROM __source_extract)
        UNION ALL
        SELECT
            codigo AS prontuario,
            TRY_CAST(prontuario_responsavel1_pc AS BIGINT) AS link_id,
            4 AS priority_group,
            NULL AS esposa_nome,
            NULL AS marido_nome,
            responsavel_nome,
            inativo
        FROM clinisys_all.silver.view_pacientes
        WHERE TRY_CAST(prontuario_responsavel1_pc AS BIGINT) IN (SELECT source_id_numeric FROM __source_extract)
        UNION ALL
        SELECT
            codigo AS prontuario,
            TRY_CAST(prontuario_responsavel2_pc AS BIGINT) AS link_id,
            4 AS priority_group,
            NULL AS esposa_nome,
            NULL AS marido_nome,
            responsavel_nome,
            inativo
        FROM clinisys_all.silver.view_pacientes
        WHERE TRY_CAST(prontuario_responsavel2_pc AS BIGINT) IN (SELECT source_id_numeric FROM __source_extract);
    """)
    _t("Unpivot and filter Clinisys view", t0)

    # 5. Parse Clinisys names
    t0 = time.perf_counter()
    source_con.execute("""
        CREATE OR REPLACE TEMP TABLE __clinisys_parsed AS
        SELECT
            prontuario,
            link_id,
            priority_group,
            inativo,
            esposa_nome,
            marido_nome,
            responsavel_nome,
            list_filter(regexp_split_to_array(regexp_replace(strip_accents(lower(trim(esposa_nome))), '[^a-z]', ' ', 'g'), '\\s+'), w -> length(w) > 0) AS e_words,
            list_filter(regexp_split_to_array(regexp_replace(strip_accents(lower(trim(marido_nome))), '[^a-z]', ' ', 'g'), '\\s+'), w -> length(w) > 0) AS m_words,
            list_filter(regexp_split_to_array(regexp_replace(strip_accents(lower(trim(responsavel_nome))), '[^a-z]', ' ', 'g'), '\\s+'), w -> length(w) > 0) AS r_words
        FROM __clinisys_unpivoted;
    """)
    _t("Parse Clinisys names", t0)

    # 6. Score name matches and select best matched name
    t0 = time.perf_counter()
    source_con.execute("""
        CREATE OR REPLACE TEMP TABLE __matches_scored AS
        SELECT
            s.source_id,
            s.patient_name,
            s.original_prontuario,
            c.prontuario,
            c.priority_group,
            c.inativo,
            
            -- Name Score
            CASE
                WHEN c.priority_group = 1 THEN
                    LEAST(
                        score_name(s.p_words, c.e_words),
                        score_name(s.p_words, c.m_words),
                        score_name(s.p_words, c.r_words)
                    )
                WHEN c.priority_group = 2 THEN score_name(s.p_words, c.e_words)
                WHEN c.priority_group = 3 THEN score_name(s.p_words, c.m_words)
                ELSE score_name(s.p_words, c.r_words)
            END AS name_score,
            
            -- Matched Name
            CASE
                WHEN c.priority_group = 1 THEN
                    CASE
                        WHEN score_name(s.p_words, c.e_words) <= score_name(s.p_words, c.m_words)
                         AND score_name(s.p_words, c.e_words) <= score_name(s.p_words, c.r_words)
                            THEN c.esposa_nome
                        WHEN score_name(s.p_words, c.m_words) <= score_name(s.p_words, c.e_words)
                         AND score_name(s.p_words, c.m_words) <= score_name(s.p_words, c.r_words)
                            THEN c.marido_nome
                        ELSE c.responsavel_nome
                    END
                WHEN c.priority_group = 2 THEN c.esposa_nome
                WHEN c.priority_group = 3 THEN c.marido_nome
                ELSE c.responsavel_nome
            END AS matched_name
        FROM __source_extract s
        INNER JOIN __clinisys_parsed c ON s.source_id_numeric = c.link_id;
    """)
    _t("Compute name match scores", t0)

    # 7. Rank matches and select top candidate per (source_id, patient_name)
    t0 = time.perf_counter()
    source_con.execute("""
        CREATE OR REPLACE TEMP TABLE __matches_ranked AS
        SELECT
            source_id,
            patient_name,
            prontuario,
            matched_name,
            priority_group,
            name_score,
            ROW_NUMBER() OVER (
                PARTITION BY source_id, patient_name
                ORDER BY
                    priority_group ASC,
                    name_score ASC,
                    CASE WHEN prontuario = original_prontuario THEN 0 ELSE 1 END ASC,
                    CASE WHEN inativo = '0' THEN 0 ELSE 1 END ASC,
                    prontuario DESC
            ) AS rn
        FROM __matches_scored
        WHERE name_score < 5;
    """)
    
    df_matched = source_con.execute("""
        SELECT
            source_id,
            patient_name,
            prontuario,
            matched_name,
            priority_group,
            name_score
        FROM __matches_ranked
        WHERE rn = 1;
    """).df()
    _t("Rank matches and select winners", t0)

    # 8. Add unmatched source ID/name pairs to construct the full denominator universe
    all_pairs_df = source_con.execute("SELECT DISTINCT source_id, patient_name FROM __source_extract").df()
    
    matched_pairs = set(zip(df_matched["source_id"].astype(str), df_matched["patient_name"].fillna("").astype(str)))
    unmatched_rows = []
    
    for _, row in all_pairs_df.iterrows():
        sid = str(row["source_id"])
        pname = str(row["patient_name"]) if not pd.isna(row["patient_name"]) else ""
        if (sid, pname) not in matched_pairs:
            unmatched_rows.append({
                "source_id": row["source_id"],
                "patient_name": row["patient_name"],
                "prontuario": -1,
                "matched_name": None,
                "priority_group": None,
                "name_score": None,
            })
            
    df_unmatched = pd.DataFrame(unmatched_rows)
    if df_unmatched.empty:
        df_unmatched = pd.DataFrame(columns=["source_id", "patient_name", "prontuario", "matched_name", "priority_group", "name_score"])

    final_df = pd.concat([df_matched, df_unmatched], ignore_index=True)
    
    n_matched = len(final_df[final_df["prontuario"] != -1])
    total = len(final_df)
    logger.info("[%s] Strategy E: %d/%d matched (%.2f%%) in %.1f seconds",
                tag, n_matched, total, (n_matched/total*100) if total else 0.0, time.perf_counter() - t_start)
    
    # Cleanup session tables and macro
    try:
        source_con.execute("DROP TABLE IF EXISTS __source_extract")
        source_con.execute("DROP TABLE IF EXISTS __clinisys_unpivoted")
        source_con.execute("DROP TABLE IF EXISTS __clinisys_parsed")
        source_con.execute("DROP TABLE IF EXISTS __matches_scored")
        source_con.execute("DROP TABLE IF EXISTS __matches_ranked")
        source_con.execute("DROP TEMPORARY MACRO score_name")
    except Exception:
        pass

    return final_df
