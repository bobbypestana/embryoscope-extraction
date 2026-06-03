"""
matching_engine_b.py — Strategy B: Scoring-based matching
Character-by-character port of update_prontuario_with_column (03_02_mesclada_to_silver.py).

Key differences from Strategy A:
- Name match is REQUIRED (WHERE clause, not just a filter)
- Uses a scoring system: name_match_score (0/2/4) + match_type_score (odd 1-31)
- Supports multiple ID columns per table (step_config list)
- UNION (dedup) instead of UNION ALL

Name extraction fix (v2):
- Now uses the same REGEXP_REPLACE + comma-detection as Strategy A
  to handle "LASTNAME, FIRSTNAME" format used in embryoscope

Metric fix (v2):
- Fetches ALL distinct source IDs upfront so the denominator is the
  true universe of IDs, not just those that hit at least one join row

Returns:
    pd.DataFrame with columns:
        source_id       — value from the source ID column (as VARCHAR)
        prontuario      — matched codigo (or -1 if unmatched)
        match_type      — e.g. 'paciente_main', 'paciente_esposa', ...
        pass_number     — 1 (first active step), 2 (first inactive step), etc.
        step_name       — name of the ID column step that matched
        combined_score  — lower is better
"""

import logging
import duckdb
import pandas as pd
from typing import Optional, Set, List

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helper: safe NOT IN clause — uses temp table for large sets
# ---------------------------------------------------------------------------

LARGE_EXCLUDE_THRESHOLD = 500


def _normalize_id(i) -> str:
    s = str(i)
    return s[:-2] if s.endswith(".0") else s


def _register_exclude_temp(con, exclude_ids: Set, temp_name: str = "__exclude_ids") -> None:
    """Materialize exclude IDs into a DuckDB temp table (in-memory, safe on read_only)."""
    ids = [{"id": _normalize_id(i)} for i in exclude_ids]
    df = pd.DataFrame(ids)
    try:
        con.execute(f"DROP TABLE IF EXISTS {temp_name}")
    except Exception:
        pass
    con.register(f"__df_{temp_name}", df)
    con.execute(f"CREATE TEMP TABLE {temp_name} AS SELECT id FROM __df_{temp_name}")
    con.unregister(f"__df_{temp_name}")


def _build_exclude_clause(id_col: str, exclude_ids: Optional[Set],
                          con=None, temp_name: str = "__exclude_ids") -> str:
    """Safe NOT IN clause. Uses inline list for small sets, temp table for large sets."""
    if not exclude_ids:
        return ""
    if len(exclude_ids) <= LARGE_EXCLUDE_THRESHOLD:
        quoted = [f"'{_normalize_id(i).replace(chr(39), chr(39)*2)}'" for i in exclude_ids]
        return f'AND CAST("{id_col}" AS VARCHAR) NOT IN ({", ".join(quoted)})'
    if con is None:
        raise ValueError("con required for large exclude sets")
    _register_exclude_temp(con, exclude_ids, temp_name)
    return f'AND CAST("{id_col}" AS VARCHAR) NOT IN (SELECT id FROM {temp_name})'


# ---------------------------------------------------------------------------
# Name extraction expression — same logic as Strategy A (v2 fix)
# ---------------------------------------------------------------------------

def _first_name_expr(col: Optional[str]) -> str:
    """
    Build SQL expression to extract first name using REGEXP_REPLACE + comma detection.
    Handles:
      - "LASTNAME, FIRSTNAME.X.Y." → splits on comma, extracts first alpha sequence
      - "FIRSTNAME LASTNAME"       → extracts first alpha sequence directly
    Returns 'NULL' if col is None.
    """
    if not col:
        return "NULL"
    return f"""
        CASE
            WHEN "{col}" IS NOT NULL THEN
                CASE
                    WHEN POSITION(',' IN "{col}") > 0 THEN
                        REGEXP_REPLACE(
                            strip_accents(LOWER(TRIM(SPLIT_PART("{col}", ',', 2)))),
                            '^[^a-z]*([a-z]+).*', '\\1'
                        )
                    ELSE
                        REGEXP_REPLACE(
                            strip_accents(LOWER(TRIM("{col}"))),
                            '^[^a-z]*([a-z]+).*', '\\1'
                        )
                END
            ELSE NULL
        END
    """


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run_strategy_b(
    source_con: duckdb.DuckDBPyConnection,
    clinisys_db_path: str,
    source_schema: str,
    source_table: str,
    step_configs: List[dict],
    label: str = "",
) -> pd.DataFrame:
    """
    Run Strategy B against a generic source table.

    Parameters
    ----------
    source_con      Read-only connection to the DB holding the source table.
    clinisys_db_path Path to clinisys_all.duckdb.
    source_schema   Schema (e.g. 'silver').
    source_table    Table name (e.g. 'mesclada_vendas').
    step_configs    List of dicts, each defining one ID column to match:
                    {
                        "name":             label for logging and match_type prefix,
                        "id_col":           column in source table used as ID,
                        "name_col_a":       first name column (e.g. "Nome"),
                        "name_col_b":       second name column (e.g. "Nom Paciente"),
                        "filter_condition": extra SQL WHERE clause (optional),
                        "is_numeric_id":    bool — if True skip empty-string check,
                        "numeric_filter":   bool — if True filter to numeric-only values
                    }
    label           Short label for log messages.

    Returns
    -------
    pd.DataFrame: one row per distinct non-null source_id in the FIRST step's id_col.
    """
    tag = f"[StrategyB][{label or f'{source_schema}.{source_table}'}]"
    logger.info(f"{tag} Starting Strategy B with {len(step_configs)} step(s)")

    # Attach clinisys read-only
    try:
        source_con.execute(f"ATTACH '{clinisys_db_path}' AS clinisys_all (READ_ONLY)")
        logger.info(f"{tag} Attached clinisys_all")
    except Exception as e:
        if "already attached" in str(e).lower():
            logger.info(f"{tag} clinisys_all already attached")
        else:
            raise

    # -----------------------------------------------------------------------
    # STEP 0: Fetch ALL distinct source IDs across ALL id_cols in step_configs
    # This is the true denominator — union of all ID columns
    # -----------------------------------------------------------------------
    union_parts = []
    for step in step_configs:
        id_col = step["id_col"]
        filt = step.get("filter_condition", "")
        union_parts.append(f"""
            SELECT DISTINCT CAST("{id_col}" AS VARCHAR) AS source_id
            FROM {source_schema}.{source_table}
            WHERE "{id_col}" IS NOT NULL
              AND CAST("{id_col}" AS VARCHAR) != ''
              {filt}
        """)
    all_ids_sql = " UNION ".join(union_parts)
    all_source_ids_df = source_con.execute(all_ids_sql).df()
    all_source_ids = set(all_source_ids_df["source_id"].tolist())
    logger.info(f"{tag} Total distinct source IDs (denominator): {len(all_source_ids):,}")

    all_results: List[pd.DataFrame] = []
    already_matched: Set = set()
    pass_number = 0

    # Active pass, then inactive pass (mirrors original loop structure)
    for include_inactive in [False, True]:
        inactive_label = "inactive" if include_inactive else "active"
        logger.info(f"{tag} === PASS ({inactive_label} patients) ===")

        for step in step_configs:
            pass_number += 1
            logger.info(f"{tag} Step '{step['name']}' ({inactive_label})...")
            df = _run_step(
                con=source_con,
                source_schema=source_schema,
                source_table=source_table,
                step=step,
                include_inactive=include_inactive,
                pass_number=pass_number,
                tag=tag,
                exclude_ids=already_matched,
            )
            if len(df) > 0:
                already_matched |= set(df["source_id"].tolist())
                all_results.append(df)
                logger.info(f"{tag} Step '{step['name']}' ({inactive_label}): {len(df)} new matches")
            else:
                logger.info(f"{tag} Step '{step['name']}' ({inactive_label}): 0 matches")

    # -----------------------------------------------------------------------
    # Build final: matched rows + all unmatched IDs from the full universe
    # -----------------------------------------------------------------------
    if all_results:
        matched_df = pd.concat(all_results, ignore_index=True).drop_duplicates(
            subset=["source_id"], keep="first"
        )
    else:
        matched_df = pd.DataFrame(columns=["source_id", "prontuario", "match_type",
                                            "pass_number", "step_name", "combined_score"])

    unmatched_ids = all_source_ids - set(matched_df["source_id"].tolist())
    unmatched_rows = pd.DataFrame({
        "source_id": list(unmatched_ids),
        "prontuario": -1,
        "match_type": "unmatched",
        "pass_number": 0,
        "step_name": "unmatched",
        "combined_score": None,
    })

    final = pd.concat([matched_df, unmatched_rows], ignore_index=True)
    n_matched = len(final[final["prontuario"] != -1])
    total = len(final)
    logger.info(f"{tag} === FINAL: {n_matched}/{total} matched ({n_matched/total*100:.2f}%) ===")
    return final


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _run_step(
    con, source_schema, source_table, step: dict,
    include_inactive: bool, pass_number: int, tag: str,
    exclude_ids: Optional[Set] = None,
) -> pd.DataFrame:
    """
    Run one step (one ID column, one active/inactive pass).
    Mirrors update_prontuario_with_column logic exactly.
    Returns only matched rows (prontuario != -1).
    """
    column_name = step["name"]
    id_col      = step["id_col"]
    name_col_a  = step.get("name_col_a")
    name_col_b  = step.get("name_col_b")
    filter_cond = step.get("filter_condition", "")
    numeric_flt = step.get("numeric_filter", False)
    is_numeric  = step.get("is_numeric_id", False)

    inactive_condition = "inativo = 1" if include_inactive else "inativo = 0"
    match_prefix       = column_name.lower()

    # Name extraction — now uses same REGEXP_REPLACE logic as Strategy A
    name_a_expr = _first_name_expr(name_col_a)
    name_b_expr = _first_name_expr(name_col_b)

    # Empty-string check: skip for integer-typed columns
    not_empty_clause = "" if is_numeric else f"AND CAST(\"{id_col}\" AS VARCHAR) != ''"

    # Exclude clause — safe VARCHAR comparison
    exclude_clause = _build_exclude_clause(id_col, exclude_ids, con=con)

    numeric_clause = ""
    if numeric_flt:
        numeric_clause = f'AND TRY_CAST("{id_col}" AS INTEGER) IS NOT NULL'

    sql = f"""
    WITH
    source_extract AS (
        SELECT DISTINCT
            CAST("{id_col}" AS VARCHAR) AS source_id,
            {name_a_expr} AS nome_first,
            {name_b_expr} AS nom_paciente_first
        FROM {source_schema}.{source_table}
        WHERE "{id_col}" IS NOT NULL
          {not_empty_clause}
          {filter_cond}
          {exclude_clause}
          {numeric_clause}
    ),

    clinisys_processed AS (
        SELECT
            codigo,
            prontuario_esposa,
            prontuario_marido,
            prontuario_responsavel1,
            prontuario_responsavel2,
            prontuario_esposa_pel,
            prontuario_marido_pel,
            prontuario_esposa_pc,
            prontuario_marido_pc,
            prontuario_responsavel1_pc,
            prontuario_responsavel2_pc,
            prontuario_esposa_fc,
            prontuario_marido_fc,
            prontuario_esposa_ba,
            prontuario_marido_ba,
            strip_accents(LOWER(SPLIT_PART(TRIM(esposa_nome), ' ', 1))) AS esposa_nome,
            strip_accents(LOWER(SPLIT_PART(TRIM(marido_nome), ' ', 1))) AS marido_nome,
            unidade_origem
        FROM clinisys_all.silver.view_pacientes
        WHERE {inactive_condition}
    ),

    matches_1  AS (SELECT d.source_id, d.nome_first, d.nom_paciente_first, p.codigo AS prontuario, p.esposa_nome, p.marido_nome, '{match_prefix}_main'            AS match_type FROM source_extract d INNER JOIN clinisys_processed p ON TRY_CAST(d.source_id AS BIGINT) = p.codigo),
    matches_2  AS (SELECT d.source_id, d.nome_first, d.nom_paciente_first, p.codigo AS prontuario, p.esposa_nome, p.marido_nome, '{match_prefix}_esposa'          AS match_type FROM source_extract d INNER JOIN clinisys_processed p ON TRY_CAST(d.source_id AS BIGINT) = p.prontuario_esposa),
    matches_3  AS (SELECT d.source_id, d.nome_first, d.nom_paciente_first, p.codigo AS prontuario, p.esposa_nome, p.marido_nome, '{match_prefix}_marido'          AS match_type FROM source_extract d INNER JOIN clinisys_processed p ON TRY_CAST(d.source_id AS BIGINT) = p.prontuario_marido),
    matches_4  AS (SELECT d.source_id, d.nome_first, d.nom_paciente_first, p.codigo AS prontuario, p.esposa_nome, p.marido_nome, '{match_prefix}_responsavel1'    AS match_type FROM source_extract d INNER JOIN clinisys_processed p ON TRY_CAST(d.source_id AS BIGINT) = p.prontuario_responsavel1),
    matches_5  AS (SELECT d.source_id, d.nome_first, d.nom_paciente_first, p.codigo AS prontuario, p.esposa_nome, p.marido_nome, '{match_prefix}_responsavel2'    AS match_type FROM source_extract d INNER JOIN clinisys_processed p ON TRY_CAST(d.source_id AS BIGINT) = p.prontuario_responsavel2),
    matches_6  AS (SELECT d.source_id, d.nome_first, d.nom_paciente_first, p.codigo AS prontuario, p.esposa_nome, p.marido_nome, '{match_prefix}_esposa_pel'      AS match_type FROM source_extract d INNER JOIN clinisys_processed p ON TRY_CAST(d.source_id AS BIGINT) = p.prontuario_esposa_pel),
    matches_7  AS (SELECT d.source_id, d.nome_first, d.nom_paciente_first, p.codigo AS prontuario, p.esposa_nome, p.marido_nome, '{match_prefix}_marido_pel'      AS match_type FROM source_extract d INNER JOIN clinisys_processed p ON TRY_CAST(d.source_id AS BIGINT) = p.prontuario_marido_pel),
    matches_8  AS (SELECT d.source_id, d.nome_first, d.nom_paciente_first, p.codigo AS prontuario, p.esposa_nome, p.marido_nome, '{match_prefix}_esposa_pc'       AS match_type FROM source_extract d INNER JOIN clinisys_processed p ON TRY_CAST(d.source_id AS BIGINT) = p.prontuario_esposa_pc),
    matches_9  AS (SELECT d.source_id, d.nome_first, d.nom_paciente_first, p.codigo AS prontuario, p.esposa_nome, p.marido_nome, '{match_prefix}_marido_pc'       AS match_type FROM source_extract d INNER JOIN clinisys_processed p ON TRY_CAST(d.source_id AS BIGINT) = p.prontuario_marido_pc),
    matches_10 AS (SELECT d.source_id, d.nome_first, d.nom_paciente_first, p.codigo AS prontuario, p.esposa_nome, p.marido_nome, '{match_prefix}_responsavel1_pc' AS match_type FROM source_extract d INNER JOIN clinisys_processed p ON TRY_CAST(d.source_id AS BIGINT) = p.prontuario_responsavel1_pc),
    matches_11 AS (SELECT d.source_id, d.nome_first, d.nom_paciente_first, p.codigo AS prontuario, p.esposa_nome, p.marido_nome, '{match_prefix}_responsavel2_pc' AS match_type FROM source_extract d INNER JOIN clinisys_processed p ON TRY_CAST(d.source_id AS BIGINT) = p.prontuario_responsavel2_pc),
    matches_12 AS (SELECT d.source_id, d.nome_first, d.nom_paciente_first, p.codigo AS prontuario, p.esposa_nome, p.marido_nome, '{match_prefix}_esposa_fc'       AS match_type FROM source_extract d INNER JOIN clinisys_processed p ON TRY_CAST(d.source_id AS BIGINT) = p.prontuario_esposa_fc),
    matches_13 AS (SELECT d.source_id, d.nome_first, d.nom_paciente_first, p.codigo AS prontuario, p.esposa_nome, p.marido_nome, '{match_prefix}_marido_fc'       AS match_type FROM source_extract d INNER JOIN clinisys_processed p ON TRY_CAST(d.source_id AS BIGINT) = p.prontuario_marido_fc),
    matches_14 AS (SELECT d.source_id, d.nome_first, d.nom_paciente_first, p.codigo AS prontuario, p.esposa_nome, p.marido_nome, '{match_prefix}_esposa_ba'       AS match_type FROM source_extract d INNER JOIN clinisys_processed p ON TRY_CAST(d.source_id AS BIGINT) = p.prontuario_esposa_ba),
    matches_15 AS (SELECT d.source_id, d.nome_first, d.nom_paciente_first, p.codigo AS prontuario, p.esposa_nome, p.marido_nome, '{match_prefix}_marido_ba'       AS match_type FROM source_extract d INNER JOIN clinisys_processed p ON TRY_CAST(d.source_id AS BIGINT) = p.prontuario_marido_ba),

    all_matches AS (
        SELECT * FROM matches_1  UNION SELECT * FROM matches_2  UNION
        SELECT * FROM matches_3  UNION SELECT * FROM matches_4  UNION
        SELECT * FROM matches_5  UNION SELECT * FROM matches_6  UNION
        SELECT * FROM matches_7  UNION SELECT * FROM matches_8  UNION
        SELECT * FROM matches_9  UNION SELECT * FROM matches_10 UNION
        SELECT * FROM matches_11 UNION SELECT * FROM matches_12 UNION
        SELECT * FROM matches_13 UNION SELECT * FROM matches_14 UNION
        SELECT * FROM matches_15
    ),

    scored_matches AS (
        SELECT *,
            -- name_match_score: when no name cols, assign 4 (worst) but still allow match
            -- Use CASE WHEN NULL literal comparison: force VARCHAR cast to avoid INT inference
            CASE
                WHEN nome_first IS NULL AND nom_paciente_first IS NULL THEN 4
                WHEN (CAST(nome_first AS VARCHAR) = CAST(esposa_nome AS VARCHAR) AND CAST(nom_paciente_first AS VARCHAR) = CAST(marido_nome AS VARCHAR))
                     OR (CAST(nom_paciente_first AS VARCHAR) = CAST(esposa_nome AS VARCHAR) AND CAST(nome_first AS VARCHAR) = CAST(marido_nome AS VARCHAR)) THEN 0
                WHEN (CAST(nome_first AS VARCHAR) = CAST(esposa_nome AS VARCHAR) OR CAST(nom_paciente_first AS VARCHAR) = CAST(marido_nome AS VARCHAR))
                     OR (CAST(nom_paciente_first AS VARCHAR) = CAST(esposa_nome AS VARCHAR) OR CAST(nome_first AS VARCHAR) = CAST(marido_nome AS VARCHAR)) THEN 2
                ELSE 4
            END AS name_match_score,
            CASE
                WHEN match_type = '{match_prefix}_main'            THEN 1
                WHEN match_type = '{match_prefix}_esposa'          THEN 3
                WHEN match_type = '{match_prefix}_marido'          THEN 5
                WHEN match_type = '{match_prefix}_responsavel1'    THEN 7
                WHEN match_type = '{match_prefix}_responsavel2'    THEN 9
                WHEN match_type = '{match_prefix}_esposa_pel'      THEN 11
                WHEN match_type = '{match_prefix}_marido_pel'      THEN 13
                WHEN match_type = '{match_prefix}_esposa_pc'       THEN 15
                WHEN match_type = '{match_prefix}_marido_pc'       THEN 17
                WHEN match_type = '{match_prefix}_responsavel1_pc' THEN 19
                WHEN match_type = '{match_prefix}_responsavel2_pc' THEN 21
                WHEN match_type = '{match_prefix}_esposa_fc'       THEN 23
                WHEN match_type = '{match_prefix}_marido_fc'       THEN 25
                WHEN match_type = '{match_prefix}_esposa_ba'       THEN 27
                WHEN match_type = '{match_prefix}_marido_ba'       THEN 29
                ELSE 31
            END AS match_type_score
        FROM all_matches
        -- NAME MATCH IS REQUIRED when name columns exist (key difference from Strategy A)
        -- When both name exprs are NULL (no name col), allow all ID matches through
        WHERE (nome_first IS NULL AND nom_paciente_first IS NULL)
           OR CAST(nome_first AS VARCHAR) = CAST(esposa_nome AS VARCHAR)
           OR CAST(nome_first AS VARCHAR) = CAST(marido_nome AS VARCHAR)
           OR CAST(nom_paciente_first AS VARCHAR) = CAST(esposa_nome AS VARCHAR)
           OR CAST(nom_paciente_first AS VARCHAR) = CAST(marido_nome AS VARCHAR)
    ),

    ranked_matches AS (
        SELECT *,
            (name_match_score + match_type_score) AS combined_score,
            ROW_NUMBER() OVER (
                PARTITION BY source_id
                ORDER BY (name_match_score + match_type_score)
            ) AS rn
        FROM scored_matches
    )

    SELECT source_id, prontuario, match_type, combined_score
    FROM ranked_matches
    WHERE rn = 1
    """


    logger.info(f"{tag} Running SQL for step '{column_name}' ({('inactive' if include_inactive else 'active')})...")
    try:
        df = con.execute(sql).df()
        df["source_id"] = df["source_id"].astype(str)
        df["pass_number"] = pass_number
        df["step_name"] = column_name
        logger.info(f"{tag} Step '{column_name}': {len(df)} matches")
        return df
    except Exception as e:
        logger.error(f"{tag} Error in step '{column_name}': {e}")
        raise
