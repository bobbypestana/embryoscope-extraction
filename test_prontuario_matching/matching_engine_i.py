"""
matching_engine_i.py — Strategy I: Patient Matching in SQL with Levenshtein Spelling Safeguards
=============================================================================================
A patient matching engine that extends Strategy H to support relaxed first-name comparison.
Uses DuckDB's built-in levenshtein() with explicit gender safeguards (detecting and rejecting
-o/-a swaps and gender suffix extensions like Gabriel/Gabriela) to prevent false positive matches
for spouses and family members.

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

def run_strategy_i(
    source_con: duckdb.DuckDBPyConnection,
    clinisys_db_path: str,
    source_schema: str,
    source_table: str,
    id_col: str,
    name_col: str = None,
    birthdate_col: str = None,
    cpf_col: str = None,
    label: str = "",
    target_pront_col: str = "prontuario_I",
    target_name_col: str = "clinisys_name_I",
    target_matched_name_col: str = "clinisys_matched_name_I",
) -> pd.DataFrame:
    """
    Runs Strategy I patient matching on the source table.
    Updates the table in-place by adding/updating target columns.
    Returns a pd.DataFrame with results.
    """
    tag = label or f"{source_schema}.{source_table}"
    t_start = time.perf_counter()
    logger.info("[%s] Starting Strategy I matching", tag)

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

    # 3. Create name scoring macro with Levenshtein & spelling safeguards
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

        CREATE OR REPLACE TEMPORARY MACRO score_name(p_w, c_w) AS (
            CASE
                WHEN p_w IS NULL OR len(p_w) = 0 THEN 1
                WHEN c_w IS NULL OR len(c_w) = 0 THEN 5
                WHEN p_w = c_w THEN 1
                WHEN NOT (
                    list_contains(c_w, p_w[1]) OR (
                        len(p_w) > 0 AND len(c_w) > 0 AND
                        levenshtein(norm_name(p_w[1]), norm_name(c_w[1])) <= 1 AND
                        NOT (
                            (right(norm_name(p_w[1]), 1) = 'o' AND right(norm_name(c_w[1]), 1) = 'a') OR
                            (right(norm_name(p_w[1]), 1) = 'a' AND right(norm_name(c_w[1]), 1) = 'o') OR
                            (right(norm_name(p_w[1]), 1) = 'o' AND right(norm_name(c_w[1]), 1) = 'e') OR
                            (right(norm_name(p_w[1]), 1) = 'e' AND right(norm_name(c_w[1]), 1) = 'o') OR
                            (right(norm_name(p_w[1]), 2) = 'an' AND right(norm_name(c_w[1]), 2) = 'ne') OR
                            (right(norm_name(p_w[1]), 2) = 'ne' AND right(norm_name(c_w[1]), 2) = 'an') OR
                            (right(norm_name(p_w[1]), 3) = 'dre' AND right(norm_name(c_w[1]), 3) = 'dra') OR
                            (right(norm_name(p_w[1]), 3) = 'dra' AND right(norm_name(c_w[1]), 3) = 'dre') OR
                            norm_name(c_w[1]) = norm_name(p_w[1]) || 'a' OR
                            norm_name(p_w[1]) = norm_name(c_w[1]) || 'a'
                        )
                    )
                ) THEN 5
                WHEN list_contains(c_w, p_w[-1]) THEN 2
                WHEN len(list_filter(p_w, w -> list_contains(c_w, w))) > 1 THEN 3
                ELSE 4
            END
        );
    """)

    # 3b. Dynamically discover Clinisys view_pacientes columns
    t0 = time.perf_counter()
    cols_df = source_con.execute("PRAGMA table_info('clinisys_all.silver.view_pacientes')").df()
    clinisys_cols = {c.lower() for c in cols_df["name"]}
    logger.info("[%s] Discovered %d columns in clinisys_all.silver.view_pacientes", tag, len(clinisys_cols))
    
    # 3c. Dynamically discover patient roles based on column suffix patterns
    discovered_prefixes = set()
    for col in clinisys_cols:
        if col.endswith("_nome"):
            prefix = col[:-5]
            if prefix:
                if f"{prefix}_cpf" in clinisys_cols or f"{prefix}_nascimento" in clinisys_cols:
                    discovered_prefixes.add(prefix)
                    
    logger.info("[%s] Discovered patient roles: %s", tag, sorted(list(discovered_prefixes)))
    
    # Core roles config with pre-defined priorities
    KNOWN_ROLES = {
        "esposa": 2,
        "marido": 3,
        "responsavel": 4,
    }
    
    ROLES_CONFIG = {}
    next_priority = 5
    for prefix in sorted(list(discovered_prefixes)):
        if prefix in KNOWN_ROLES:
            priority = KNOWN_ROLES[prefix]
        else:
            priority = next_priority
            next_priority += 1
            
        ROLES_CONFIG[prefix] = {
            "role_name": prefix,
            "prefix": prefix,
            "priority_group": priority
        }

    # Identify all columns starting with 'prontuario' in clinisys view
    pront_cols = sorted([col for col in clinisys_cols if col.startswith("prontuario")])
    logger.info("[%s] Found prontuario link columns: %s", tag, pront_cols)

    # 4. Extract candidates and unpivot them
    # Build candidate select WHERE filter dynamically
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
        cpf_col = f"{prefix}_cpf"
        if cpf_col in clinisys_cols:
            cand_conditions.append(
                f"lpad(regexp_replace(\"{cpf_col}\", '[^0-9]', '', 'g'), 11, '0') IN (SELECT src_cpf FROM __source_extract WHERE src_cpf IS NOT NULL AND length(src_cpf) = 11)"
            )
            
    cand_where = "\n           OR ".join(cand_conditions)

    # Discover prefixes list for role names
    prefixes_list = sorted(list(discovered_prefixes))

    # Helper function to generate SELECT expressions for a given role prefix
    def get_role_select_exprs(prefix, link_id_col, priority_group, matched_role):
        # Matched CPF expression
        cpf_col = f"{prefix}_cpf"
        if cpf_col in clinisys_cols:
            matched_cpf_expr = f"lpad(regexp_replace(\"{cpf_col}\", '[^0-9]', '', 'g'), 11, '0')"
        else:
            matched_cpf_expr = "CAST(NULL AS VARCHAR)"
            
        # Matched birthdate expression
        birth_col = f"{prefix}_nascimento"
        if birth_col in clinisys_cols:
            matched_birthdate_expr = f"CAST(try_strptime(\"{birth_col}\", '%d/%m/%Y') AS DATE)"
        else:
            matched_birthdate_expr = "CAST(NULL AS DATE)"
            
        # Matched name expression
        name_col = f"{prefix}_nome"
        if name_col in clinisys_cols:
            matched_name_expr = f"\"{name_col}\""
        else:
            matched_name_expr = "CAST(NULL AS VARCHAR)"
            
        # Dynamic spouse names select
        role_name_exprs = []
        for pref in prefixes_list:
            col = f"{pref}_nome"
            if col in clinisys_cols:
                role_name_exprs.append(f'"{col}" AS {col}')
            else:
                role_name_exprs.append(f"CAST(NULL AS VARCHAR) AS {col}")
        
        # inativo expression
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

    # Assemble union parts for unpivot
    union_parts = []
    
    # Main group (codigo, priority_group = 1)
    for role_key, config in ROLES_CONFIG.items():
        prefix = config["prefix"]
        if f"{prefix}_nome" in clinisys_cols:
            union_parts.append(
                get_role_select_exprs(prefix, "codigo", 1, config["role_name"]) + "FROM __clinisys_candidates"
            )
            
    # Link groups (prontuario_* columns, priority_group based on role prefix match)
    for col in pront_cols:
        if col == "prontuario":
            continue
        # Find which role this column matches via prefix substring
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
                get_role_select_exprs(prefix, f"TRY_CAST(\"{col}\" AS BIGINT)", pg, role) + f"FROM __clinisys_candidates WHERE \"{col}\" IS NOT NULL"
            )
        else:
            logger.warning("[%s] Prontuario link column %s did not match any known role. Skipping.", tag, col)

    union_all_join = "\n        UNION ALL\n        "
    unpivoted_union_sql = union_all_join.join(union_parts)

    # Build parsed columns dynamically for all roles
    parsed_cols = []
    for pref in prefixes_list:
        col = f"{pref}_nome"
        parsed_cols.append(col)
        parsed_cols.append(
            f"list_filter(regexp_split_to_array(regexp_replace(strip_accents(lower(trim({col}))), '[^a-z]', ' ', 'g'), '\\s+'), w -> length(w) > 0) AS {pref}_words"
        )
    parsed_cols_sql = ", ".join(parsed_cols)

    # Execute dynamic queries
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
        FROM __clinisys_unpivoted;
    """)
    _t("Extract, unpivot, and parse candidates dynamically", t0)

    # 7. Join tables based on matching rules and score name matches
    t0 = time.perf_counter()
    
    role_names_select = ", ".join([f"c.{pref}_nome" for pref in prefixes_list])
    score_exprs_sql = "score_name(s.p_words, c.c_words) AS name_score"
    for pref in prefixes_list:
        score_exprs_sql += f", score_name(s.p_words, c.{pref}_words) AS {pref}_score"

    def get_tier_select(tier_num, join_clause, where_clause):
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

    tier_1_sql = get_tier_select(1, "INNER JOIN __clinisys_parsed c ON s.src_cpf = c.matched_cpf", "WHERE s.src_cpf IS NOT NULL AND length(s.src_cpf) = 11")
    tier_2_sql = get_tier_select(2, "INNER JOIN __clinisys_parsed c ON s.source_id_numeric = c.link_id", """
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
    """)
    tier_0_sql = get_tier_select(0, "INNER JOIN __clinisys_parsed c ON s.source_id_numeric = c.link_id", "WHERE c.priority_group = 1")
    tier_3_sql = get_tier_select(3, "INNER JOIN __clinisys_parsed c ON s.source_id_numeric = c.link_id", "WHERE c.priority_group >= 2")

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

    # 8. Rank matches and select top candidate per (source_id, patient_name)
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
    
    # 9. Build final matches DataFrame with all denominator records
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

    # Ensure target columns exist in the source table
    columns_to_add = [
        (target_pront_col, "BIGINT"),
        (target_name_col, "VARCHAR"),
        (target_matched_name_col, "VARCHAR"),
    ]
    # Check if target prontuario column exists initially
    pront_col_exists = target_pront_col.lower() in col_names

    for col_name, col_type in columns_to_add:
        if col_name.lower() not in col_names:
            logger.info("[%s] Adding column %s (%s) to target table...", tag, col_name, col_type)
            source_con.execute(f"ALTER TABLE {source_schema}.\"{source_table}\" ADD COLUMN {col_name} {col_type}")

    # Register temporary view of df_matched to update
    source_con.register("df_i_temp", df_matched)
    
    # Update target table in-place using indexed join
    where_clause = f"""
        WHERE target."{id_col}" = m.source_id
          AND target."{name_col if name_col else 'id'}" IS NOT DISTINCT FROM m.patient_name
    """
    if pront_col_exists:
        where_clause += f'          AND target."{target_pront_col}" = -1'

    source_con.execute(f"""
        UPDATE {source_schema}.\"{source_table}\" AS target
        SET {target_pront_col} = m.prontuario,
            {target_name_col} = m.clinisys_name,
            {target_matched_name_col} = m.clinisys_matched_name
        FROM df_i_temp m
        {where_clause};
    """)
    logger.info("[%s] Strategy I in-place updates applied to %s.%s", tag, source_schema, source_table)

    # Clean up temp tables
    try:
        source_con.execute("DROP TABLE IF EXISTS __source_extract")
        source_con.execute("DROP TABLE IF EXISTS __clinisys_candidate_codigos")
        source_con.execute("DROP TABLE IF EXISTS __clinisys_candidates")
        source_con.execute("DROP TABLE IF EXISTS __clinisys_unpivoted")
        source_con.execute("DROP TABLE IF EXISTS __clinisys_parsed")
        source_con.execute("DROP TABLE IF EXISTS __matches_all")
        source_con.execute("DROP TABLE IF EXISTS __matches_ranked")
        source_con.execute("DROP TEMPORARY MACRO score_name")
        source_con.execute("DROP TEMPORARY MACRO norm_name")
        source_con.execute("DROP TABLE IF EXISTS df_i_temp")
    except Exception:
        pass

    n_matched = len(df_matched[df_matched["prontuario"] != -1])
    total = len(df_matched)
    logger.info("[%s] Strategy I: %d/%d matched (%.2f%%) in %.1f seconds",
                tag, n_matched, total, (n_matched/total*100) if total else 0.0, time.perf_counter() - t_start)

    return df_matched
