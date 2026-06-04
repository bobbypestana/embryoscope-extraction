"""
matching_engine_g.py — Strategy G: Unified Hierarchical Patient Matching in SQL
==============================================================================
A generalized patient matching engine that runs entirely in DuckDB SQL.
Takes as input any table and its ID, name, birthdate, and CPF columns.

Matching Hierarchy:
0. Tier 0: Source ID = codigo directly (highest confidence — direct record ownership)
1. Tier 1: CPF match (national ID, high confidence)
2. Tier 2: ID + Birthdate via link columns — exact match, OR year+month-only when source day=01
   (handles Embryoscope truncation). Applied when CPF absent or CPF failed to match.
3. Tier 3: ID via link columns + name fallback (lowest, universal fallback).
Priority ordering within each tier: name_score ASC, priority_group ASC, original prontuario match, active record.

Updates the target table in-place and returns a pandas DataFrame.
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

def run_strategy_g(
    source_con: duckdb.DuckDBPyConnection,
    clinisys_db_path: str,
    source_schema: str,
    source_table: str,
    id_col: str,
    name_col: str = None,
    birthdate_col: str = None,
    cpf_col: str = None,
    label: str = "",
) -> pd.DataFrame:
    """
    Runs Strategy G patient matching on the source table.
    Updates the table in-place by adding/updating:
      - prontuario (BIGINT)
      - clinisys_name (VARCHAR)
      - clinisys_matched_name (VARCHAR)
    And returns a pd.DataFrame with results.
    """
    tag = label or f"{source_schema}.{source_table}"
    t_start = time.perf_counter()
    logger.info("[%s] Starting Strategy G matching", tag)

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

    # Formulate inputs select
    name_select = f'"{name_col}"' if name_col else "CAST(NULL AS VARCHAR)"
    birthdate_select = f'TRY_CAST("{birthdate_col}" AS DATE)' if birthdate_col else "CAST(NULL AS DATE)"
    if cpf_col:
        cpf_select = f"""
            CASE
                WHEN length(regexp_replace(CAST("{cpf_col}" AS VARCHAR), '[^0-9]', '', 'g')) < 9 THEN NULL
                WHEN lpad(regexp_replace(CAST("{cpf_col}" AS VARCHAR), '[^0-9]', '', 'g'), 11, '0') IN (
                    '00000000000', '11111111111', '22222222222', '33333333333', '44444444444',
                    '55555555555', '66666666666', '77777777777', '88888888888', '99999999999'
                ) THEN NULL
                ELSE lpad(regexp_replace(CAST("{cpf_col}" AS VARCHAR), '[^0-9]', '', 'g'), 11, '0')
            END
        """
    else:
        cpf_select = "CAST(NULL AS VARCHAR)"
    
    if name_col:
        p_words_select = f"""
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
            )
        """
    else:
        p_words_select = "CAST([] AS VARCHAR[])"

    # 2. Extract source IDs and parse names
    t0 = time.perf_counter()
    source_con.execute(f"""
        CREATE OR REPLACE TEMP TABLE __source_extract AS
        SELECT DISTINCT
            CAST("{id_col}" AS VARCHAR) AS source_id,
            TRY_CAST("{id_col}" AS BIGINT) AS source_id_numeric,
            {name_select} AS patient_name,
            {birthdate_select} AS src_birthdate,
            {cpf_select} AS src_cpf,
            {orig_pront_select} AS original_prontuario,
            {p_words_select} AS p_words
        FROM {source_schema}."{source_table}"
        WHERE "{id_col}" IS NOT NULL AND TRIM(CAST("{id_col}" AS VARCHAR)) != '';
    """)
    _t("Extract and clean source records", t0)

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

    # 4. Extract candidates and unpivot them
    t0 = time.perf_counter()
    source_con.execute("""
        CREATE OR REPLACE TEMP TABLE __clinisys_candidate_codigos AS
        SELECT codigo FROM clinisys_all.silver.view_pacientes
        WHERE codigo IN (SELECT source_id_numeric FROM __source_extract WHERE source_id_numeric IS NOT NULL)
           OR TRY_CAST(prontuario_esposa AS BIGINT) IN (SELECT source_id_numeric FROM __source_extract WHERE source_id_numeric IS NOT NULL)
           OR TRY_CAST(prontuario_esposa_pel AS BIGINT) IN (SELECT source_id_numeric FROM __source_extract WHERE source_id_numeric IS NOT NULL)
           OR TRY_CAST(prontuario_esposa_pc AS BIGINT) IN (SELECT source_id_numeric FROM __source_extract WHERE source_id_numeric IS NOT NULL)
           OR TRY_CAST(prontuario_esposa_fc AS BIGINT) IN (SELECT source_id_numeric FROM __source_extract WHERE source_id_numeric IS NOT NULL)
           OR TRY_CAST(prontuario_esposa_ba AS BIGINT) IN (SELECT source_id_numeric FROM __source_extract WHERE source_id_numeric IS NOT NULL)
           OR TRY_CAST(prontuario_marido AS BIGINT) IN (SELECT source_id_numeric FROM __source_extract WHERE source_id_numeric IS NOT NULL)
           OR TRY_CAST(prontuario_marido_pel AS BIGINT) IN (SELECT source_id_numeric FROM __source_extract WHERE source_id_numeric IS NOT NULL)
           OR TRY_CAST(prontuario_marido_pc AS BIGINT) IN (SELECT source_id_numeric FROM __source_extract WHERE source_id_numeric IS NOT NULL)
           OR TRY_CAST(prontuario_marido_fc AS BIGINT) IN (SELECT source_id_numeric FROM __source_extract WHERE source_id_numeric IS NOT NULL)
           OR TRY_CAST(prontuario_marido_ba AS BIGINT) IN (SELECT source_id_numeric FROM __source_extract WHERE source_id_numeric IS NOT NULL)
           OR TRY_CAST(prontuario_responsavel1 AS BIGINT) IN (SELECT source_id_numeric FROM __source_extract WHERE source_id_numeric IS NOT NULL)
           OR TRY_CAST(prontuario_responsavel2 AS BIGINT) IN (SELECT source_id_numeric FROM __source_extract WHERE source_id_numeric IS NOT NULL)
           OR TRY_CAST(prontuario_responsavel1_pc AS BIGINT) IN (SELECT source_id_numeric FROM __source_extract WHERE source_id_numeric IS NOT NULL)
           OR TRY_CAST(prontuario_responsavel2_pc AS BIGINT) IN (SELECT source_id_numeric FROM __source_extract WHERE source_id_numeric IS NOT NULL)
           OR lpad(regexp_replace(esposa_cpf, '[^0-9]', '', 'g'), 11, '0') IN (SELECT src_cpf FROM __source_extract WHERE src_cpf IS NOT NULL AND length(src_cpf) = 11)
           OR lpad(regexp_replace(marido_cpf, '[^0-9]', '', 'g'), 11, '0') IN (SELECT src_cpf FROM __source_extract WHERE src_cpf IS NOT NULL AND length(src_cpf) = 11)
           OR lpad(regexp_replace(responsavel_cpf, '[^0-9]', '', 'g'), 11, '0') IN (SELECT src_cpf FROM __source_extract WHERE src_cpf IS NOT NULL AND length(src_cpf) = 11);

        CREATE OR REPLACE TEMP TABLE __clinisys_candidates AS
        SELECT * FROM clinisys_all.silver.view_pacientes
        WHERE codigo IN (SELECT codigo FROM __clinisys_candidate_codigos);

        CREATE OR REPLACE TEMP TABLE __clinisys_unpivoted AS
        -- Group 1: Main (codigo)
        SELECT
            codigo AS prontuario,
            codigo AS link_id,
            1 AS priority_group,
            'esposa' AS matched_role,
            lpad(regexp_replace(esposa_cpf, '[^0-9]', '', 'g'), 11, '0') AS matched_cpf,
            CAST(try_strptime(esposa_nascimento, '%d/%m/%Y') AS DATE) AS matched_birthdate,
            esposa_nome AS matched_name,
            esposa_nome,
            inativo
        FROM __clinisys_candidates
        UNION ALL
        SELECT
            codigo AS prontuario,
            codigo AS link_id,
            1 AS priority_group,
            'marido' AS matched_role,
            lpad(regexp_replace(marido_cpf, '[^0-9]', '', 'g'), 11, '0') AS matched_cpf,
            CAST(try_strptime(marido_nascimento, '%d/%m/%Y') AS DATE) AS matched_birthdate,
            marido_nome AS matched_name,
            esposa_nome,
            inativo
        FROM __clinisys_candidates
        UNION ALL
        SELECT
            codigo AS prontuario,
            codigo AS link_id,
            1 AS priority_group,
            'responsavel' AS matched_role,
            lpad(regexp_replace(responsavel_cpf, '[^0-9]', '', 'g'), 11, '0') AS matched_cpf,
            CAST(try_strptime(responsavel_nascimento, '%d/%m/%Y') AS DATE) AS matched_birthdate,
            responsavel_nome AS matched_name,
            esposa_nome,
            inativo
        FROM __clinisys_candidates

        UNION ALL

        -- Group 2: Esposa columns
        SELECT
            codigo AS prontuario,
            TRY_CAST(prontuario_esposa AS BIGINT) AS link_id,
            2 AS priority_group,
            'esposa' AS matched_role,
            lpad(regexp_replace(esposa_cpf, '[^0-9]', '', 'g'), 11, '0') AS matched_cpf,
            CAST(try_strptime(esposa_nascimento, '%d/%m/%Y') AS DATE) AS matched_birthdate,
            esposa_nome AS matched_name,
            esposa_nome,
            inativo
        FROM __clinisys_candidates WHERE prontuario_esposa IS NOT NULL
        UNION ALL
        SELECT
            codigo AS prontuario,
            TRY_CAST(prontuario_esposa_pel AS BIGINT) AS link_id,
            2 AS priority_group,
            'esposa' AS matched_role,
            lpad(regexp_replace(esposa_cpf, '[^0-9]', '', 'g'), 11, '0') AS matched_cpf,
            CAST(try_strptime(esposa_nascimento, '%d/%m/%Y') AS DATE) AS matched_birthdate,
            esposa_nome AS matched_name,
            esposa_nome,
            inativo
        FROM __clinisys_candidates WHERE prontuario_esposa_pel IS NOT NULL
        UNION ALL
        SELECT
            codigo AS prontuario,
            TRY_CAST(prontuario_esposa_pc AS BIGINT) AS link_id,
            2 AS priority_group,
            'esposa' AS matched_role,
            lpad(regexp_replace(esposa_cpf, '[^0-9]', '', 'g'), 11, '0') AS matched_cpf,
            CAST(try_strptime(esposa_nascimento, '%d/%m/%Y') AS DATE) AS matched_birthdate,
            esposa_nome AS matched_name,
            esposa_nome,
            inativo
        FROM __clinisys_candidates WHERE prontuario_esposa_pc IS NOT NULL
        UNION ALL
        SELECT
            codigo AS prontuario,
            TRY_CAST(prontuario_esposa_fc AS BIGINT) AS link_id,
            2 AS priority_group,
            'esposa' AS matched_role,
            lpad(regexp_replace(esposa_cpf, '[^0-9]', '', 'g'), 11, '0') AS matched_cpf,
            CAST(try_strptime(esposa_nascimento, '%d/%m/%Y') AS DATE) AS matched_birthdate,
            esposa_nome AS matched_name,
            esposa_nome,
            inativo
        FROM __clinisys_candidates WHERE prontuario_esposa_fc IS NOT NULL
        UNION ALL
        SELECT
            codigo AS prontuario,
            TRY_CAST(prontuario_esposa_ba AS BIGINT) AS link_id,
            2 AS priority_group,
            'esposa' AS matched_role,
            lpad(regexp_replace(esposa_cpf, '[^0-9]', '', 'g'), 11, '0') AS matched_cpf,
            CAST(try_strptime(esposa_nascimento, '%d/%m/%Y') AS DATE) AS matched_birthdate,
            esposa_nome AS matched_name,
            esposa_nome,
            inativo
        FROM __clinisys_candidates WHERE prontuario_esposa_ba IS NOT NULL

        UNION ALL

        -- Group 3: Marido columns
        SELECT
            codigo AS prontuario,
            TRY_CAST(prontuario_marido AS BIGINT) AS link_id,
            3 AS priority_group,
            'marido' AS matched_role,
            lpad(regexp_replace(marido_cpf, '[^0-9]', '', 'g'), 11, '0') AS matched_cpf,
            CAST(try_strptime(marido_nascimento, '%d/%m/%Y') AS DATE) AS matched_birthdate,
            marido_nome AS matched_name,
            esposa_nome,
            inativo
        FROM __clinisys_candidates WHERE prontuario_marido IS NOT NULL
        UNION ALL
        SELECT
            codigo AS prontuario,
            TRY_CAST(prontuario_marido_pel AS BIGINT) AS link_id,
            3 AS priority_group,
            'marido' AS matched_role,
            lpad(regexp_replace(marido_cpf, '[^0-9]', '', 'g'), 11, '0') AS matched_cpf,
            CAST(try_strptime(marido_nascimento, '%d/%m/%Y') AS DATE) AS matched_birthdate,
            marido_nome AS matched_name,
            esposa_nome,
            inativo
        FROM __clinisys_candidates WHERE prontuario_marido_pel IS NOT NULL
        UNION ALL
        SELECT
            codigo AS prontuario,
            TRY_CAST(prontuario_marido_pc AS BIGINT) AS link_id,
            3 AS priority_group,
            'marido' AS matched_role,
            lpad(regexp_replace(marido_cpf, '[^0-9]', '', 'g'), 11, '0') AS matched_cpf,
            CAST(try_strptime(marido_nascimento, '%d/%m/%Y') AS DATE) AS matched_birthdate,
            marido_nome AS matched_name,
            esposa_nome,
            inativo
        FROM __clinisys_candidates WHERE prontuario_marido_pc IS NOT NULL
        UNION ALL
        SELECT
            codigo AS prontuario,
            TRY_CAST(prontuario_marido_fc AS BIGINT) AS link_id,
            3 AS priority_group,
            'marido' AS matched_role,
            lpad(regexp_replace(marido_cpf, '[^0-9]', '', 'g'), 11, '0') AS matched_cpf,
            CAST(try_strptime(marido_nascimento, '%d/%m/%Y') AS DATE) AS matched_birthdate,
            marido_nome AS matched_name,
            esposa_nome,
            inativo
        FROM __clinisys_candidates WHERE prontuario_marido_fc IS NOT NULL
        UNION ALL
        SELECT
            codigo AS prontuario,
            TRY_CAST(prontuario_marido_ba AS BIGINT) AS link_id,
            3 AS priority_group,
            'marido' AS matched_role,
            lpad(regexp_replace(marido_cpf, '[^0-9]', '', 'g'), 11, '0') AS matched_cpf,
            CAST(try_strptime(marido_nascimento, '%d/%m/%Y') AS DATE) AS matched_birthdate,
            marido_nome AS matched_name,
            esposa_nome,
            inativo
        FROM __clinisys_candidates WHERE prontuario_marido_ba IS NOT NULL

        UNION ALL

        -- Group 4: Responsavel columns
        SELECT
            codigo AS prontuario,
            TRY_CAST(prontuario_responsavel1 AS BIGINT) AS link_id,
            4 AS priority_group,
            'responsavel' AS matched_role,
            lpad(regexp_replace(responsavel_cpf, '[^0-9]', '', 'g'), 11, '0') AS matched_cpf,
            CAST(try_strptime(responsavel_nascimento, '%d/%m/%Y') AS DATE) AS matched_birthdate,
            responsavel_nome AS matched_name,
            esposa_nome,
            inativo
        FROM __clinisys_candidates WHERE prontuario_responsavel1 IS NOT NULL
        UNION ALL
        SELECT
            codigo AS prontuario,
            TRY_CAST(prontuario_responsavel2 AS BIGINT) AS link_id,
            4 AS priority_group,
            'responsavel' AS matched_role,
            lpad(regexp_replace(responsavel_cpf, '[^0-9]', '', 'g'), 11, '0') AS matched_cpf,
            CAST(try_strptime(responsavel_nascimento, '%d/%m/%Y') AS DATE) AS matched_birthdate,
            responsavel_nome AS matched_name,
            esposa_nome,
            inativo
        FROM __clinisys_candidates WHERE prontuario_responsavel2 IS NOT NULL
        UNION ALL
        SELECT
            codigo AS prontuario,
            TRY_CAST(prontuario_responsavel1_pc AS BIGINT) AS link_id,
            4 AS priority_group,
            'responsavel' AS matched_role,
            lpad(regexp_replace(responsavel_cpf, '[^0-9]', '', 'g'), 11, '0') AS matched_cpf,
            CAST(try_strptime(responsavel_nascimento, '%d/%m/%Y') AS DATE) AS matched_birthdate,
            responsavel_nome AS matched_name,
            esposa_nome,
            inativo
        FROM __clinisys_candidates WHERE prontuario_responsavel1_pc IS NOT NULL
        UNION ALL
        SELECT
            codigo AS prontuario,
            TRY_CAST(prontuario_responsavel2_pc AS BIGINT) AS link_id,
            4 AS priority_group,
            'responsavel' AS matched_role,
            lpad(regexp_replace(responsavel_cpf, '[^0-9]', '', 'g'), 11, '0') AS matched_cpf,
            CAST(try_strptime(responsavel_nascimento, '%d/%m/%Y') AS DATE) AS matched_birthdate,
            responsavel_nome AS matched_name,
            esposa_nome,
            inativo
        FROM __clinisys_candidates WHERE prontuario_responsavel2_pc IS NOT NULL;

        CREATE OR REPLACE TEMP TABLE __clinisys_parsed AS
        SELECT
            prontuario,
            link_id,
            priority_group,
            matched_role,
            CASE
                WHEN length(regexp_replace(matched_cpf, '[^0-9]', '', 'g')) < 9 THEN NULL
                WHEN matched_cpf IN (
                    '00000000000', '11111111111', '22222222222', '33333333333', '44444444444',
                    '55555555555', '66666666666', '77777777777', '88888888888', '99999999999'
                ) THEN NULL
                ELSE matched_cpf
            END AS matched_cpf,
            matched_birthdate,
            matched_name,
            esposa_nome,
            inativo,
            list_filter(regexp_split_to_array(regexp_replace(strip_accents(lower(trim(matched_name))), '[^a-z]', ' ', 'g'), '\\s+'), w -> length(w) > 0) AS c_words,
            list_filter(regexp_split_to_array(regexp_replace(strip_accents(lower(trim(esposa_nome))), '[^a-z]', ' ', 'g'), '\\s+'), w -> length(w) > 0) AS e_words
        FROM __clinisys_unpivoted;
    """)
    _t("Extract, unpivot, and parse candidates", t0)

    # 7. Join tables based on matching rules and score name matches
    t0 = time.perf_counter()
    source_con.execute("""
        CREATE OR REPLACE TEMP TABLE __matches_all AS
        -- Tier 1: CPF join
        SELECT
            s.source_id,
            s.patient_name,
            s.original_prontuario,
            c.prontuario,
            c.esposa_nome,
            c.matched_name,
            c.matched_role,
            c.inativo,
            c.priority_group,
            c.link_id,
            1 AS match_tier,
            score_name(s.p_words, c.c_words) AS name_score,
            score_name(s.p_words, c.e_words) AS esposa_score
        FROM __source_extract s
        INNER JOIN __clinisys_parsed c ON s.src_cpf = c.matched_cpf
        WHERE s.src_cpf IS NOT NULL AND length(s.src_cpf) = 11

        UNION ALL

        -- Tier 2: ID + Birthdate join
        -- Exact date match, OR year+month-only when source day=01 (Embryoscope truncation).
        -- Only applies when CPF is absent or invalid in source.
        SELECT
            s.source_id,
            s.patient_name,
            s.original_prontuario,
            c.prontuario,
            c.esposa_nome,
            c.matched_name,
            c.matched_role,
            c.inativo,
            c.priority_group,
            c.link_id,
            2 AS match_tier,
            score_name(s.p_words, c.c_words) AS name_score,
            score_name(s.p_words, c.e_words) AS esposa_score
        FROM __source_extract s
        INNER JOIN __clinisys_parsed c ON s.source_id_numeric = c.link_id
        WHERE (s.src_cpf IS NULL OR length(s.src_cpf) < 11)
          AND s.src_birthdate IS NOT NULL
          AND c.matched_birthdate IS NOT NULL
          AND (
            s.src_birthdate = c.matched_birthdate
            OR (
                DAY(s.src_birthdate) = 1
                AND DATE_TRUNC('month', s.src_birthdate) = DATE_TRUNC('month', c.matched_birthdate)
            )
          )

        UNION ALL

        -- Tier 0: Direct ID = codigo (priority_group=1) — highest confidence.
        -- Source ID IS the Clinisys prontuario, so this beats CPF.
        SELECT
            s.source_id,
            s.patient_name,
            s.original_prontuario,
            c.prontuario,
            c.esposa_nome,
            c.matched_name,
            c.matched_role,
            c.inativo,
            c.priority_group,
            c.link_id,
            0 AS match_tier,
            score_name(s.p_words, c.c_words) AS name_score,
            score_name(s.p_words, c.e_words) AS esposa_score
        FROM __source_extract s
        INNER JOIN __clinisys_parsed c ON s.source_id_numeric = c.link_id
        WHERE c.priority_group = 1

        UNION ALL

        -- Tier 3: ID via link columns (priority_group>=2) — universal fallback.
        -- Handles esposa/marido/responsavel cross-links when no better match found.
        SELECT
            s.source_id,
            s.patient_name,
            s.original_prontuario,
            c.prontuario,
            c.esposa_nome,
            c.matched_name,
            c.matched_role,
            c.inativo,
            c.priority_group,
            c.link_id,
            3 AS match_tier,
            score_name(s.p_words, c.c_words) AS name_score,
            score_name(s.p_words, c.e_words) AS esposa_score
        FROM __source_extract s
        INNER JOIN __clinisys_parsed c ON s.source_id_numeric = c.link_id
        WHERE c.priority_group >= 2;
    """)
    _t("Apply hierarchical joins & score names", t0)

    # 8. Rank matches and select top candidate per (source_id, patient_name)
    t0 = time.perf_counter()
    source_con.execute("""
        CREATE OR REPLACE TEMP TABLE __matches_ranked AS
        SELECT
            source_id,
            patient_name,
            prontuario,
            esposa_nome,
            matched_name,
            match_tier,
            name_score,
            esposa_score,
            ROW_NUMBER() OVER (
                PARTITION BY source_id, patient_name
                ORDER BY
                    match_tier ASC,
                    CASE WHEN COALESCE(inativo, '0') = '0' THEN 0 ELSE 1 END ASC,
                    CASE WHEN link_id = TRY_CAST(source_id AS BIGINT) THEN 0 ELSE 1 END ASC,
                    esposa_score ASC,
                    name_score ASC,
                    priority_group ASC,
                    CASE WHEN prontuario = original_prontuario THEN 0 ELSE 1 END ASC,
                    prontuario DESC
            ) AS rn
        FROM __matches_all
        WHERE name_score < 5;
    """)
    
    # 9. Build final matches DataFrame with all denominator records
    df_matched = source_con.execute("""
        SELECT
            s.source_id,
            s.patient_name,
            COALESCE(m.prontuario, -1) AS prontuario,
            m.esposa_nome AS clinisys_name,
            m.matched_name AS clinisys_matched_name,
            m.match_tier,
            m.name_score
        FROM (SELECT DISTINCT source_id, patient_name FROM __source_extract) s
        LEFT JOIN __matches_ranked m
               ON s.source_id = m.source_id
              AND s.patient_name IS NOT DISTINCT FROM m.patient_name
              AND m.rn = 1;
    """).df()
    _t("Rank matches and select winners", t0)

    # Ensure target columns exist in the source table
    columns_to_add = [
        ("prontuario_G", "BIGINT"),
        ("clinisys_name_G", "VARCHAR"),
        ("clinisys_matched_name_G", "VARCHAR"),
    ]
    for col_name, col_type in columns_to_add:
        if col_name.lower() not in col_names:
            logger.info("[%s] Adding column %s (%s) to target table...", tag, col_name, col_type)
            source_con.execute(f"ALTER TABLE {source_schema}.\"{source_table}\" ADD COLUMN {col_name} {col_type}")

    # Register temporary view of df_matched to update
    source_con.register("df_g_temp", df_matched)
    
    # Update target table in-place using indexed join
    source_con.execute(f"""
        UPDATE {source_schema}.\"{source_table}\" AS target
        SET prontuario_G = m.prontuario,
            clinisys_name_G = m.clinisys_name,
            clinisys_matched_name_G = m.clinisys_matched_name
        FROM df_g_temp m
        WHERE target."{id_col}" = m.source_id
          AND target."{name_col if name_col else 'id'}" IS NOT DISTINCT FROM m.patient_name;
    """)
    logger.info("[%s] Strategy G in-place updates applied to %s.%s", tag, source_schema, source_table)

    # Clean up temp tables
    try:
        source_con.execute("DROP TABLE IF EXISTS __source_extract")
        source_con.execute("DROP TABLE IF EXISTS __clinisys_id_unpivoted")
        source_con.execute("DROP TABLE IF EXISTS __clinisys_cpf_candidates")
        source_con.execute("DROP TABLE IF EXISTS __clinisys_cpf_unpivoted")
        source_con.execute("DROP TABLE IF EXISTS __clinisys_unpivoted")
        source_con.execute("DROP TABLE IF EXISTS __clinisys_parsed")
        source_con.execute("DROP TABLE IF EXISTS __matches_all")
        source_con.execute("DROP TABLE IF EXISTS __matches_ranked")
        source_con.execute("DROP TEMPORARY MACRO score_name")
        source_con.execute("DROP TABLE IF EXISTS df_g_temp")
    except Exception:
        pass

    n_matched = len(df_matched[df_matched["prontuario"] != -1])
    total = len(df_matched)
    logger.info("[%s] Strategy G: %d/%d matched (%.2f%%) in %.1f seconds",
                tag, n_matched, total, (n_matched/total*100) if total else 0.0, time.perf_counter() - t_start)

    return df_matched
