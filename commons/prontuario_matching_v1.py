"""
prontuario_matching_v1.py — Strategy L: Standalone Production Matching Engine
==============================================================================
Self-contained production matching engine (copy of Strategy L logic) that runs
entirely in DuckDB SQL, with dynamic discovery of Clinisys silver.view_pacientes
columns at runtime.

Strategy L matching rules:
  1. CPF exact match (Tier 1).
  2. ID + birthdate match (Tier 2).
  3. Direct ID match against codigo (Tier 0) — with full Strategy L name scoring.
  4. ID match against spouse/partner link columns (Tier 3) — with Strategy L name scoring.

Strategy L name scoring:
  - Exact first-name match.
  - First-name spelling tolerance (Levenshtein <= 1) with gender safeguards.
  - First-name spelling tolerance (Levenshtein = 2) when last name matches exactly.
  - Couple/spousal folder fallback: direct ID match + Jaro-Winkler >= 0.72 + >= 2
    shared non-preposition words.

Public API
----------
  run_strategy_l(source_con, clinisys_db_path, source_schema, source_table,
                 id_col, name_col, birthdate_col, cpf_col, label, suffix)

  suffix=""   → production mode: only updates the bare `prontuario` column.
  suffix="_L" → benchmark mode: also adds `clinisys_name_L` and
                `clinisys_matched_name_L` columns.
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

def run_strategy_l(
    source_con: duckdb.DuckDBPyConnection,
    clinisys_db_path: str,
    source_schema: str,
    source_table: str,
    id_col: str,
    name_col: str = None,
    birthdate_col: str = None,
    cpf_col: str = None,
    label: str = "",
    suffix: str = "",
) -> pd.DataFrame:
    """
    Runs Strategy L patient matching on the source table.
    Updates the table in-place by adding/updating target columns.

    Parameters
    ----------
    suffix : str
        Column suffix.  "" (empty) for production — only `prontuario` is written.
        Use e.g. "_L" for benchmarking to also write clinisys_name and
        clinisys_matched_name columns.

    Returns
    -------
    pd.DataFrame with columns: source_id, patient_name, prontuario,
        clinisys_name, clinisys_matched_name, match_tier, name_score.
    """
    tag = label or f"{source_schema}.{source_table}"
    t_start = time.perf_counter()
    logger.info("[%s] Starting Strategy L matching", tag)

    target_pront_col = f"prontuario{suffix}"
    if suffix:
        target_name_col = f"clinisys_name{suffix}"
        target_matched_name_col = f"clinisys_matched_name{suffix}"
    else:
        target_name_col = None
        target_matched_name_col = None

    # ── 1. Attach Clinisys DB ──────────────────────────────────────────────────
    try:
        source_con.execute(f"ATTACH '{clinisys_db_path}' AS clinisys_all (READ_ONLY)")
        logger.info("[%s] Attached clinisys_all", tag)
    except Exception as e:
        if "already attached" in str(e).lower():
            logger.info("[%s] clinisys_all already attached", tag)
        else:
            raise

    # 1b. Detect original prontuario column for tie-breaker
    cols_df = source_con.execute(f"PRAGMA table_info('{source_schema}.\"{source_table}\"')").df()
    col_names = {c.lower() for c in cols_df["name"]}

    if "prontuario_old" in col_names:
        orig_pront_col = "prontuario_old"
    elif "prontuario" in col_names:
        orig_pront_col = "prontuario"
    else:
        orig_pront_col = "NULL"

    orig_pront_select = (
        f'TRY_CAST("{orig_pront_col}" AS BIGINT)'
        if orig_pront_col != "NULL"
        else "CAST(NULL AS BIGINT)"
    )

    # ── 2. Build input SELECT expressions ────────────────────────────────────
    name_select = f'"{name_col}"' if name_col else "CAST(NULL AS VARCHAR)"
    birthdate_select = (
        f'TRY_CAST("{birthdate_col}" AS DATE)' if birthdate_col else "CAST(NULL AS DATE)"
    )
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
        p_full_select = f"""
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
            )
        """
    else:
        p_words_select = "CAST([] AS VARCHAR[])"
        p_full_select = "CAST(NULL AS VARCHAR)"

    # ── 3. Phonetic normalisation macro ──────────────────────────────────────
    source_con.execute("""
        CREATE OR REPLACE TEMPORARY MACRO norm_name(w) AS (
            replace(
                replace(
                    replace(
                        replace(
                            replace(
                                replace(
                                    replace(
                                        replace(
                                            replace(
                                                replace(
                                                    replace(
                                                        replace(
                                                            replace(
                                                                replace(
                                                                    replace(w, 'ph', 'f'),
                                                                    'y', 'i'
                                                                ),
                                                                'w', 'v'
                                                                ),
                                                            'z', 's'
                                                        ),
                                                        'k', 'c'
                                                    ),
                                                    'ss', 's'
                                                ),
                                                'll', 'l'
                                            ),
                                            'rr', 'r'
                                        ),
                                        'nn', 'n'
                                    ),
                                    'tt', 't'
                                ),
                                'cc', 'c'
                            ),
                            'pp', 'p'
                        ),
                        'ff', 'f'
                    ),
                    'mm', 'm'
                ),
                'h', ''
            )
        );
    """)

    # ── 4. Extract & pre-normalise source records ─────────────────────────────
    t0 = time.perf_counter()
    source_con.execute(f"""
        CREATE OR REPLACE TEMP TABLE __source_extract AS
        WITH raw_extract AS (
            SELECT DISTINCT
                CAST("{id_col}" AS VARCHAR) AS source_id,
                TRY_CAST("{id_col}" AS BIGINT) AS source_id_numeric,
                {name_select} AS patient_name,
                {birthdate_select} AS src_birthdate,
                {cpf_select} AS src_cpf,
                {orig_pront_select} AS original_prontuario,
                {p_words_select} AS p_words,
                {p_full_select} AS p_full
            FROM {source_schema}."{source_table}"
            WHERE "{id_col}" IS NOT NULL AND TRIM(CAST("{id_col}" AS VARCHAR)) != ''
        )
        SELECT *,
               CASE WHEN len(p_words) > 0 THEN norm_name(p_words[1]) ELSE NULL END AS norm_p_w1
        FROM raw_extract;
    """)
    _t("Extract and clean source records", t0)

    # ── 5. Name scoring macro (Strategy L rules) ──────────────────────────────
    source_con.execute("""
        CREATE OR REPLACE TEMPORARY MACRO score_name(p_w, c_w, norm_p_w1, norm_c_w1, p_full, c_full, is_direct_id) AS (
            CASE
                WHEN p_w IS NULL OR len(p_w) = 0 THEN 1
                WHEN c_w IS NULL OR len(c_w) = 0 THEN 5
                WHEN p_w = c_w THEN 1
                WHEN NOT (
                    -- Rule 1: Exact first name match
                    list_contains(c_w, p_w[1]) OR

                    -- Rule 2: First name spelling tolerance (Levenshtein <= 1) with gender safeguards
                    (
                        norm_p_w1 IS NOT NULL AND norm_c_w1 IS NOT NULL AND
                        levenshtein(norm_p_w1, norm_c_w1) <= 1 AND
                        NOT (
                            (right(norm_p_w1, 1) = 'o' AND right(norm_c_w1, 1) = 'a') OR
                            (right(norm_p_w1, 1) = 'a' AND right(norm_c_w1, 1) = 'o') OR
                            (right(norm_p_w1, 1) = 'o' AND right(norm_c_w1, 1) = 'e') OR
                            (right(norm_p_w1, 1) = 'e' AND right(norm_c_w1, 1) = 'o') OR
                            (right(norm_p_w1, 2) = 'an' AND right(norm_c_w1, 2) = 'ne') OR
                            (right(norm_p_w1, 2) = 'ne' AND right(norm_c_w1, 2) = 'an') OR
                            (right(norm_p_w1, 3) = 'dre' AND right(norm_c_w1, 3) = 'dra') OR
                            (right(norm_p_w1, 3) = 'dra' AND right(norm_c_w1, 3) = 'dre') OR
                            norm_c_w1 = norm_p_w1 || 'a' OR
                            norm_p_w1 = norm_c_w1 || 'a'
                        )
                    ) OR

                    -- Rule 3: First-name spelling tolerance (Levenshtein = 2) if last name matches exactly
                    (
                        norm_p_w1 IS NOT NULL AND norm_c_w1 IS NOT NULL AND
                        levenshtein(norm_p_w1, norm_c_w1) = 2 AND
                        list_contains(c_w, p_w[-1]) AND
                        NOT (
                            (right(norm_p_w1, 1) = 'o' AND right(norm_c_w1, 1) = 'a') OR
                            (right(norm_p_w1, 1) = 'a' AND right(norm_c_w1, 1) = 'o') OR
                            (right(norm_p_w1, 1) = 'o' AND right(norm_c_w1, 1) = 'e') OR
                            (right(norm_p_w1, 1) = 'e' AND right(norm_c_w1, 1) = 'o')
                        )
                    ) OR

                    -- Rule 4: Couple/spousal folder fallback (direct ID only)
                    (
                        is_direct_id AND
                        p_full IS NOT NULL AND c_full IS NOT NULL AND
                        jaro_winkler_similarity(p_full, c_full) >= 0.72 AND
                        len(list_filter(p_w, w -> w NOT IN ('de', 'da', 'do', 'dos', 'das', 'e', 'a', 'o') AND list_contains(c_w, w))) >= 2
                    )
                ) THEN 5
                WHEN list_contains(c_w, p_w[-1]) THEN 2
                WHEN len(list_filter(p_w, w -> list_contains(c_w, w))) > 1 THEN 3
                ELSE 4
            END
        );
    """)

    # ── 6. Discover Clinisys schema ───────────────────────────────────────────
    t0 = time.perf_counter()
    cols_df = source_con.execute("PRAGMA table_info('clinisys_all.silver.view_pacientes')").df()
    clinisys_cols = {c.lower() for c in cols_df["name"]}
    logger.info("[%s] Discovered %d columns in clinisys_all.silver.view_pacientes", tag, len(clinisys_cols))

    discovered_prefixes = set()
    for col in clinisys_cols:
        if col.endswith("_nome"):
            prefix = col[:-5]
            if prefix:
                if f"{prefix}_cpf" in clinisys_cols or f"{prefix}_nascimento" in clinisys_cols:
                    discovered_prefixes.add(prefix)

    logger.info("[%s] Discovered patient roles: %s", tag, sorted(list(discovered_prefixes)))

    KNOWN_ROLES = {"esposa": 2, "marido": 3, "responsavel": 4}
    ROLES_CONFIG = {}
    next_priority = 5
    for prefix in sorted(list(discovered_prefixes)):
        priority = KNOWN_ROLES.get(prefix, next_priority)
        if prefix not in KNOWN_ROLES:
            next_priority += 1
        ROLES_CONFIG[prefix] = {"role_name": prefix, "prefix": prefix, "priority_group": priority}

    pront_cols = sorted([col for col in clinisys_cols if col.startswith("prontuario")])
    logger.info("[%s] Found prontuario link columns: %s", tag, pront_cols)

    # ── 7. Build candidate filter & unpivot ───────────────────────────────────
    cand_conditions = [
        "codigo IN (SELECT source_id_numeric FROM __source_extract WHERE source_id_numeric IS NOT NULL)"
    ]
    for col in pront_cols:
        if col == "prontuario":
            continue
        cand_conditions.append(
            f"TRY_CAST(\"{col}\" AS BIGINT) IN (SELECT source_id_numeric FROM __source_extract WHERE source_id_numeric IS NOT NULL)"
        )
    for role_key, config in ROLES_CONFIG.items():
        prefix = config["prefix"]
        cpf_col_name = f"{prefix}_cpf"
        if cpf_col_name in clinisys_cols:
            cand_conditions.append(
                f"lpad(regexp_replace(\"{cpf_col_name}\", '[^0-9]', '', 'g'), 11, '0') IN (SELECT src_cpf FROM __source_extract WHERE src_cpf IS NOT NULL AND length(src_cpf) = 11)"
            )

    cand_where = "\n           OR ".join(cand_conditions)
    prefixes_list = sorted(list(discovered_prefixes))

    def get_role_select_exprs(prefix, link_id_col, priority_group, matched_role):
        cpf_col_name = f"{prefix}_cpf"
        matched_cpf_expr = (
            f"lpad(regexp_replace(\"{cpf_col_name}\", '[^0-9]', '', 'g'), 11, '0')"
            if cpf_col_name in clinisys_cols
            else "CAST(NULL AS VARCHAR)"
        )
        birth_col = f"{prefix}_nascimento"
        matched_birthdate_expr = (
            f"CAST(try_strptime(\"{birth_col}\", '%d/%m/%Y') AS DATE)"
            if birth_col in clinisys_cols
            else "CAST(NULL AS DATE)"
        )
        name_col_name = f"{prefix}_nome"
        matched_name_expr = (
            f"\"{name_col_name}\""
            if name_col_name in clinisys_cols
            else "CAST(NULL AS VARCHAR)"
        )
        role_name_exprs = []
        for pref in prefixes_list:
            col = f"{pref}_nome"
            if col in clinisys_cols:
                role_name_exprs.append(f'"{col}" AS {col}')
            else:
                role_name_exprs.append(f"CAST(NULL AS VARCHAR) AS {col}")
        inativo_expr = "\"inativo\"" if "inativo" in clinisys_cols else "CAST(NULL AS VARCHAR)"
        return f"""
        SELECT
            codigo AS prontuario,
            {link_id_col} AS link_id,
            {priority_group} AS priority_group,
            '{matched_role}' AS matched_role,
            {matched_cpf_expr} AS matched_cpf,
            {matched_birthdate_expr} AS matched_birthdate,
            {matched_name_expr} AS matched_name,
            {', '.join(role_name_exprs)},
            {inativo_expr} AS inativo
        """

    union_parts = []
    for role_key, config in ROLES_CONFIG.items():
        prefix = config["prefix"]
        if f"{prefix}_nome" in clinisys_cols:
            union_parts.append(
                get_role_select_exprs(prefix, "codigo", 1, config["role_name"])
                + "FROM __clinisys_candidates"
            )
    for col in pront_cols:
        if col == "prontuario":
            continue
        matched_config = None
        for role_key, config in ROLES_CONFIG.items():
            if config["prefix"] in col:
                matched_config = config
                break
        if matched_config:
            prefix = matched_config["prefix"]
            pg = matched_config["priority_group"]
            role = matched_config["role_name"]
            union_parts.append(
                get_role_select_exprs(prefix, f"TRY_CAST(\"{col}\" AS BIGINT)", pg, role)
                + f"FROM __clinisys_candidates WHERE \"{col}\" IS NOT NULL"
            )
        else:
            logger.warning("[%s] Prontuario link column %s did not match any known role. Skipping.", tag, col)

    union_all_join = "\n        UNION ALL\n        "
    unpivoted_union_sql = union_all_join.join(union_parts)

    parsed_cols = []
    for pref in prefixes_list:
        col = f"{pref}_nome"
        parsed_cols.append(col)
        w_expr = f"list_filter(regexp_split_to_array(regexp_replace(strip_accents(lower(trim({col}))), '[^a-z]', ' ', 'g'), '\\s+'), w -> length(w) > 0)"
        parsed_cols.append(f"{w_expr} AS {pref}_words")
        parsed_cols.append(f"CASE WHEN len({w_expr}) > 0 THEN norm_name(({w_expr})[1]) ELSE NULL END AS norm_{pref}_w1")
        f_expr = f"regexp_replace(strip_accents(lower(trim({col}))), '[^a-z]', ' ', 'g')"
        parsed_cols.append(f"{f_expr} AS {pref}_full")

    parsed_cols_sql = ", ".join(parsed_cols)

    source_con.execute(f"""
        CREATE OR REPLACE TEMP TABLE __clinisys_candidate_codigos AS
        SELECT codigo FROM clinisys_all.silver.view_pacientes
        WHERE {cand_where};

        CREATE OR REPLACE TEMP TABLE __clinisys_candidates AS
        SELECT * FROM clinisys_all.silver.view_pacientes
        WHERE codigo IN (SELECT codigo FROM __clinisys_candidate_codigos);

        CREATE OR REPLACE TEMP TABLE __clinisys_unpivoted AS
        {unpivoted_union_sql};

        CREATE OR REPLACE TEMP TABLE __clinisys_parsed AS
        WITH raw_parsed AS (
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
                inativo,
                list_filter(regexp_split_to_array(regexp_replace(strip_accents(lower(trim(matched_name))), '[^a-z]', ' ', 'g'), '\\s+'), w -> length(w) > 0) AS c_words,
                {parsed_cols_sql}
            FROM __clinisys_unpivoted
        )
        SELECT *,
               CASE WHEN len(c_words) > 0 THEN norm_name(c_words[1]) ELSE NULL END AS norm_c_w1,
               regexp_replace(strip_accents(lower(trim(matched_name))), '[^a-z]', ' ', 'g') AS c_full
        FROM raw_parsed;
    """)
    _t("Extract, unpivot, and parse candidates dynamically (with pre-normalization)", t0)

    # ── 8. Hierarchical joins & scoring ──────────────────────────────────────
    t0 = time.perf_counter()
    role_names_select = ", ".join([f"c.{pref}_nome" for pref in prefixes_list])

    def get_tier_select(tier_num, join_clause, where_clause, is_direct_id: bool):
        is_direct_str = "true" if is_direct_id else "false"
        score_exprs = [
            f"score_name(s.p_words, c.c_words, s.norm_p_w1, c.norm_c_w1, s.p_full, c.c_full, {is_direct_str}) AS name_score"
        ]
        for pref in prefixes_list:
            score_exprs.append(
                f"score_name(s.p_words, c.{pref}_words, s.norm_p_w1, c.norm_{pref}_w1, s.p_full, c.{pref}_full, {is_direct_str}) AS {pref}_score"
            )
        score_exprs_sql = ", ".join(score_exprs)
        return f"""
        SELECT
            s.source_id,
            s.patient_name,
            s.original_prontuario,
            c.prontuario,
            {role_names_select},
            c.matched_name,
            c.matched_role,
            c.inativo,
            c.priority_group,
            c.link_id,
            {tier_num} AS match_tier,
            {score_exprs_sql}
        FROM __source_extract s
        {join_clause}
        {where_clause}
        """

    tier_1_sql = get_tier_select(
        1, "INNER JOIN __clinisys_parsed c ON s.src_cpf = c.matched_cpf",
        "WHERE s.src_cpf IS NOT NULL AND length(s.src_cpf) = 11", False
    )
    tier_2_sql = get_tier_select(
        2, "INNER JOIN __clinisys_parsed c ON s.source_id_numeric = c.link_id",
        """
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
        """, False
    )
    tier_0_sql = get_tier_select(
        0, "INNER JOIN __clinisys_parsed c ON s.source_id_numeric = c.link_id",
        "WHERE c.priority_group = 1", True
    )
    tier_3_sql = get_tier_select(
        3, "INNER JOIN __clinisys_parsed c ON s.source_id_numeric = c.link_id",
        "WHERE c.priority_group >= 2", True
    )

    source_con.execute(f"""
        CREATE OR REPLACE TEMP TABLE __matches_all AS
        {tier_1_sql}
        UNION ALL
        {tier_2_sql}
        UNION ALL
        {tier_0_sql}
        UNION ALL
        {tier_3_sql};
    """)
    _t("Apply hierarchical joins & score names", t0)

    # ── 9. Rank & select best match per source row ────────────────────────────
    t0 = time.perf_counter()
    least_args = ["COALESCE(name_score, 5)"]
    for pref in prefixes_list:
        least_args.append(f"COALESCE({pref}_score, 5)")
    least_expr = f"LEAST({', '.join(least_args)})"

    name_cases = ["WHEN name_score < 5 THEN matched_name"]
    for pref in prefixes_list:
        name_cases.append(f"WHEN {pref}_score < 5 THEN {pref}_nome")
    matched_name_select = f"CASE {' '.join(name_cases)} ELSE matched_name END"

    source_con.execute(f"""
        CREATE OR REPLACE TEMP TABLE __matches_ranked AS
        SELECT
            source_id,
            patient_name,
            prontuario,
            esposa_nome,
            {matched_name_select} AS matched_name,
            match_tier,
            {least_expr} AS best_name_score,
            ROW_NUMBER() OVER (
                PARTITION BY source_id, patient_name
                ORDER BY
                    match_tier ASC,
                    CASE WHEN COALESCE(inativo, '0') = '0' THEN 0 ELSE 1 END ASC,
                    CASE WHEN link_id = TRY_CAST(source_id AS BIGINT) THEN 0 ELSE 1 END ASC,
                    {least_expr} ASC,
                    priority_group ASC,
                    CASE WHEN prontuario = original_prontuario THEN 0 ELSE 1 END ASC,
                    prontuario DESC
            ) AS rn
        FROM __matches_all
        WHERE {least_expr} < 5;
    """)

    df_matched = source_con.execute(f"""
        SELECT
            s.source_id,
            s.patient_name,
            COALESCE(m.prontuario, -1) AS prontuario,
            m.esposa_nome AS clinisys_name,
            m.matched_name AS clinisys_matched_name,
            m.match_tier,
            m.best_name_score AS name_score
        FROM (SELECT DISTINCT source_id, patient_name FROM __source_extract) s
        LEFT JOIN __matches_ranked m
               ON s.source_id = m.source_id
              AND s.patient_name IS NOT DISTINCT FROM m.patient_name
              AND m.rn = 1;
    """).df()
    _t("Rank matches and select winners", t0)

    # ── 10. Ensure target columns exist & write results ───────────────────────
    pront_col_exists = target_pront_col.lower() in col_names

    columns_to_add = [(target_pront_col, "BIGINT")]
    if suffix:
        columns_to_add.extend([
            (target_name_col, "VARCHAR"),
            (target_matched_name_col, "VARCHAR"),
        ])
    for col_name, col_type in columns_to_add:
        if col_name.lower() not in col_names:
            logger.info("[%s] Adding column %s (%s) to target table...", tag, col_name, col_type)
            source_con.execute(
                f"ALTER TABLE {source_schema}.\"{source_table}\" ADD COLUMN {col_name} {col_type}"
            )

    source_con.register("__df_l_temp", df_matched)

    where_clause = f"""
        WHERE target."{id_col}" = m.source_id
          AND target."{name_col if name_col else 'id'}" IS NOT DISTINCT FROM m.patient_name
    """
    if pront_col_exists:
        where_clause += f'          AND target."{target_pront_col}" = -1'

    if suffix:
        source_con.execute(f"""
            UPDATE {source_schema}."{source_table}" AS target
            SET {target_pront_col} = m.prontuario,
                {target_name_col} = m.clinisys_name,
                {target_matched_name_col} = m.clinisys_matched_name
            FROM __df_l_temp m
            {where_clause};
        """)
    else:
        source_con.execute(f"""
            UPDATE {source_schema}."{source_table}" AS target
            SET {target_pront_col} = m.prontuario
            FROM __df_l_temp m
            {where_clause};
        """)
    logger.info("[%s] Strategy L in-place updates applied to %s.%s", tag, source_schema, source_table)

    # ── 11. Cleanup temp objects ──────────────────────────────────────────────
    try:
        for tbl in [
            "__source_extract", "__clinisys_candidate_codigos", "__clinisys_candidates",
            "__clinisys_unpivoted", "__clinisys_parsed", "__matches_all", "__matches_ranked",
            "__df_l_temp",
        ]:
            source_con.execute(f"DROP TABLE IF EXISTS {tbl}")
        source_con.execute("DROP TEMPORARY MACRO IF EXISTS score_name")
        source_con.execute("DROP TEMPORARY MACRO IF EXISTS norm_name")
    except Exception:
        pass

    n_matched = len(df_matched[df_matched["prontuario"] != -1])
    total = len(df_matched)
    logger.info(
        "[%s] Strategy L: %d/%d matched (%.2f%%) in %.1f seconds",
        tag, n_matched, total,
        (n_matched / total * 100) if total else 0.0,
        time.perf_counter() - t_start,
    )
    return df_matched
