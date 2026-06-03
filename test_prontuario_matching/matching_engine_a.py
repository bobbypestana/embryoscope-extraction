"""
matching_engine_a.py — Strategy A: PatientID-first matching
Character-by-character port of update_prontuario_with_inativo (patient_matching.py)
and update_prontuario_with_lastname.

This engine is generalized to accept any source table + ID column instead of being
hard-coded to silver.patients / PatientID.

Returns:
    pd.DataFrame with columns:
        source_id       — value from the source ID column
        prontuario      — matched codigo (or -1 if unmatched)
        match_type      — 'patientid_main', 'patientid_esposa', ..., 'lastname', 'unmatched'
        pass_number     — 1 (active), 2 (inactive), 3 (lastname)

Metric guarantee:
    The returned DataFrame always contains ONE row per distinct non-null source_id
    in the source table. prontuario=-1 means no match found. This ensures the
    denominator in match_rate is the true universe of IDs, not just those that
    happened to hit at least one join row.
"""

import logging
import duckdb
import pandas as pd
from typing import Optional, Set

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helper: safe NOT IN clause — uses temp table for large sets
# ---------------------------------------------------------------------------

LARGE_EXCLUDE_THRESHOLD = 500  # above this, use temp table instead of literal list


def _normalize_id(i) -> str:
    """Normalize an ID to a clean string (strip trailing .0 from floats)."""
    s = str(i)
    return s[:-2] if s.endswith(".0") else s


def _register_exclude_temp(con, exclude_ids: Set, temp_name: str = "__exclude_ids") -> None:
    """
    Create (or replace) a DuckDB temp table holding the exclude ID strings.
    Safe to call on read_only connections — temp tables are in-memory only.
    """
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
    """
    Build a safe NOT IN clause comparing as VARCHAR.
    - Small sets (<=LARGE_EXCLUDE_THRESHOLD): inline literal list.
    - Large sets: register a temp table and use NOT IN (SELECT id FROM temp).
    Requires `con` to be passed for large sets.
    """
    if not exclude_ids:
        return ""
    if len(exclude_ids) <= LARGE_EXCLUDE_THRESHOLD:
        quoted = [f"'{_normalize_id(i).replace(chr(39), chr(39)*2)}'" for i in exclude_ids]
        return f'AND CAST("{id_col}" AS VARCHAR) NOT IN ({", ".join(quoted)})'
    # Large set — use temp table
    if con is None:
        raise ValueError("con required for large exclude sets")
    _register_exclude_temp(con, exclude_ids, temp_name)
    return f'AND CAST("{id_col}" AS VARCHAR) NOT IN (SELECT id FROM {temp_name})'


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run_strategy_a(
    source_con: duckdb.DuckDBPyConnection,
    clinisys_db_path: str,
    source_schema: str,
    source_table: str,
    id_col: str,
    name_col: Optional[str],
    label: str = "",
) -> pd.DataFrame:
    """
    Run Strategy A against a generic source table.

    Parameters
    ----------
    source_con      Read-only connection to the DB that holds the source table.
    clinisys_db_path Path to clinisys_all.duckdb (will be attached read-only).
    source_schema   Schema of the source table (e.g. 'silver').
    source_table    Table name (e.g. 'patients').
    id_col          Column whose values will be matched against clinisys prontuarios.
    name_col        Column used for name-validation filter (FirstName equivalent).
                    Pass None to skip name validation (trust ID match).
    label           Short label for log messages (e.g. db filename).

    Returns
    -------
    pd.DataFrame with columns: source_id, prontuario, match_type, pass_number
    One row per distinct non-null source_id in the source table.
    """
    tag = f"[StrategyA][{label or f'{source_schema}.{source_table}'}]"
    logger.info(f"{tag} Starting Strategy A")

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
    # STEP 0: Fetch ALL distinct source IDs — this is the true denominator
    # -----------------------------------------------------------------------
    all_source_ids_df = source_con.execute(f"""
        SELECT DISTINCT CAST("{id_col}" AS VARCHAR) AS source_id
        FROM {source_schema}.{source_table}
        WHERE "{id_col}" IS NOT NULL
    """).df()
    all_source_ids = set(all_source_ids_df["source_id"].tolist())
    logger.info(f"{tag} Total distinct source IDs (denominator): {len(all_source_ids):,}")

    matched_ids: Set = set()
    all_matched_rows = []

    # Pass 1: active patients (inativo = 0)
    logger.info(f"{tag} === PASS 1: active patients (inativo=0) ===")
    df1 = _run_pass(source_con, source_schema, source_table, id_col, name_col,
                    include_inactive=False, pass_number=1, tag=tag)
    p1_matched = set(df1["source_id"].tolist())
    matched_ids |= p1_matched
    all_matched_rows.append(df1)
    logger.info(f"{tag} Pass 1: {len(p1_matched)} matched")

    # Pass 2: inactive patients (inativo = 1) — only for IDs not yet matched
    logger.info(f"{tag} === PASS 2: inactive patients (inativo=1) ===")
    df2 = _run_pass(source_con, source_schema, source_table, id_col, name_col,
                    include_inactive=True, pass_number=2, tag=tag,
                    exclude_ids=matched_ids)
    p2_matched = set(df2["source_id"].tolist())
    matched_ids |= p2_matched
    all_matched_rows.append(df2)
    logger.info(f"{tag} Pass 2: {len(p2_matched)} newly matched")

    # Pass 3: LastName fallback (only if name_col provided)
    if name_col is not None:
        logger.info(f"{tag} === PASS 3: LastName fallback ===")
        df3 = _run_lastname_pass(source_con, source_schema, source_table, id_col, name_col,
                                 pass_number=3, tag=tag, exclude_ids=matched_ids)
        p3_matched = set(df3["source_id"].tolist())
        matched_ids |= p3_matched
        all_matched_rows.append(df3)
        logger.info(f"{tag} Pass 3: {len(p3_matched)} newly matched")
    else:
        logger.info(f"{tag} Skipping Pass 3 (no name_col provided)")

    # -----------------------------------------------------------------------
    # Merge: keep best match per source_id (pass order = priority)
    # -----------------------------------------------------------------------
    combined = pd.concat(all_matched_rows, ignore_index=True)
    matched_df = combined.drop_duplicates(subset=["source_id"], keep="first")

    # -----------------------------------------------------------------------
    # Add ALL unmatched IDs so the denominator is the full source universe
    # -----------------------------------------------------------------------
    unmatched_ids = all_source_ids - set(matched_df["source_id"].tolist())
    unmatched_rows = pd.DataFrame({
        "source_id": list(unmatched_ids),
        "prontuario": -1,
        "match_type": "unmatched",
        "pass_number": 0,
    })

    final = pd.concat([matched_df, unmatched_rows], ignore_index=True)
    n_matched = len(final[final["prontuario"] != -1])
    total = len(final)
    logger.info(f"{tag} === FINAL: {n_matched}/{total} matched ({n_matched/total*100:.2f}%) ===")
    return final


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _run_pass(
    con, source_schema, source_table, id_col, name_col,
    include_inactive: bool, pass_number: int, tag: str,
    exclude_ids: Optional[Set] = None,
) -> pd.DataFrame:
    """
    Run one active/inactive pass. Mirrors update_prontuario_with_inativo logic exactly.
    Returns DataFrame(source_id, prontuario, match_type, pass_number) — only matched rows.
    """
    inactive_condition = "inativo = 1" if include_inactive else "inativo = 0"
    patient_type = "inactive" if include_inactive else "active"

    # Name extraction — same REGEXP_REPLACE + comma-detection as original
    if name_col:
        name_extract_expr = f"""
            CASE
                WHEN "{name_col}" IS NOT NULL THEN
                    CASE
                        WHEN POSITION(',' IN "{name_col}") > 0 THEN
                            REGEXP_REPLACE(
                                strip_accents(LOWER(TRIM(SPLIT_PART("{name_col}", ',', 2)))),
                                '^[^a-z]*([a-z]+).*', '\\1'
                            )
                        ELSE
                            REGEXP_REPLACE(
                                strip_accents(LOWER(TRIM("{name_col}"))),
                                '^[^a-z]*([a-z]+).*', '\\1'
                            )
                    END
                ELSE NULL
            END
        """
    else:
        name_extract_expr = "NULL"

    exclude_clause = _build_exclude_clause(id_col, exclude_ids, con=con)

    sql = f"""
    WITH
    source_name_extract AS (
        SELECT
            CAST("{id_col}" AS VARCHAR) AS source_id,
            {name_extract_expr} AS name_first
        FROM {source_schema}.{source_table}
        WHERE "{id_col}" IS NOT NULL
          {exclude_clause}
    ),

    source_extract AS (
        SELECT DISTINCT source_id, name_first
        FROM source_name_extract
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

    matches_1  AS (SELECT d.source_id, d.name_first, p.codigo AS prontuario, p.esposa_nome, p.marido_nome, 'patientid_main'            AS match_type FROM source_extract d INNER JOIN clinisys_processed p ON TRY_CAST(d.source_id AS BIGINT) = p.codigo),
    matches_2  AS (SELECT d.source_id, d.name_first, p.codigo AS prontuario, p.esposa_nome, p.marido_nome, 'patientid_esposa'          AS match_type FROM source_extract d INNER JOIN clinisys_processed p ON TRY_CAST(d.source_id AS BIGINT) = p.prontuario_esposa),
    matches_3  AS (SELECT d.source_id, d.name_first, p.codigo AS prontuario, p.esposa_nome, p.marido_nome, 'patientid_marido'          AS match_type FROM source_extract d INNER JOIN clinisys_processed p ON TRY_CAST(d.source_id AS BIGINT) = p.prontuario_marido),
    matches_4  AS (SELECT d.source_id, d.name_first, p.codigo AS prontuario, p.esposa_nome, p.marido_nome, 'patientid_responsavel1'    AS match_type FROM source_extract d INNER JOIN clinisys_processed p ON TRY_CAST(d.source_id AS BIGINT) = p.prontuario_responsavel1),
    matches_5  AS (SELECT d.source_id, d.name_first, p.codigo AS prontuario, p.esposa_nome, p.marido_nome, 'patientid_responsavel2'    AS match_type FROM source_extract d INNER JOIN clinisys_processed p ON TRY_CAST(d.source_id AS BIGINT) = p.prontuario_responsavel2),
    matches_6  AS (SELECT d.source_id, d.name_first, p.codigo AS prontuario, p.esposa_nome, p.marido_nome, 'patientid_esposa_pel'      AS match_type FROM source_extract d INNER JOIN clinisys_processed p ON TRY_CAST(d.source_id AS BIGINT) = p.prontuario_esposa_pel),
    matches_7  AS (SELECT d.source_id, d.name_first, p.codigo AS prontuario, p.esposa_nome, p.marido_nome, 'patientid_marido_pel'      AS match_type FROM source_extract d INNER JOIN clinisys_processed p ON TRY_CAST(d.source_id AS BIGINT) = p.prontuario_marido_pel),
    matches_8  AS (SELECT d.source_id, d.name_first, p.codigo AS prontuario, p.esposa_nome, p.marido_nome, 'patientid_esposa_pc'       AS match_type FROM source_extract d INNER JOIN clinisys_processed p ON TRY_CAST(d.source_id AS BIGINT) = p.prontuario_esposa_pc),
    matches_9  AS (SELECT d.source_id, d.name_first, p.codigo AS prontuario, p.esposa_nome, p.marido_nome, 'patientid_marido_pc'       AS match_type FROM source_extract d INNER JOIN clinisys_processed p ON TRY_CAST(d.source_id AS BIGINT) = p.prontuario_marido_pc),
    matches_10 AS (SELECT d.source_id, d.name_first, p.codigo AS prontuario, p.esposa_nome, p.marido_nome, 'patientid_responsavel1_pc' AS match_type FROM source_extract d INNER JOIN clinisys_processed p ON TRY_CAST(d.source_id AS BIGINT) = p.prontuario_responsavel1_pc),
    matches_11 AS (SELECT d.source_id, d.name_first, p.codigo AS prontuario, p.esposa_nome, p.marido_nome, 'patientid_responsavel2_pc' AS match_type FROM source_extract d INNER JOIN clinisys_processed p ON TRY_CAST(d.source_id AS BIGINT) = p.prontuario_responsavel2_pc),
    matches_12 AS (SELECT d.source_id, d.name_first, p.codigo AS prontuario, p.esposa_nome, p.marido_nome, 'patientid_esposa_fc'       AS match_type FROM source_extract d INNER JOIN clinisys_processed p ON TRY_CAST(d.source_id AS BIGINT) = p.prontuario_esposa_fc),
    matches_13 AS (SELECT d.source_id, d.name_first, p.codigo AS prontuario, p.esposa_nome, p.marido_nome, 'patientid_marido_fc'       AS match_type FROM source_extract d INNER JOIN clinisys_processed p ON TRY_CAST(d.source_id AS BIGINT) = p.prontuario_marido_fc),
    matches_14 AS (SELECT d.source_id, d.name_first, p.codigo AS prontuario, p.esposa_nome, p.marido_nome, 'patientid_esposa_ba'       AS match_type FROM source_extract d INNER JOIN clinisys_processed p ON TRY_CAST(d.source_id AS BIGINT) = p.prontuario_esposa_ba),
    matches_15 AS (SELECT d.source_id, d.name_first, p.codigo AS prontuario, p.esposa_nome, p.marido_nome, 'patientid_marido_ba'       AS match_type FROM source_extract d INNER JOIN clinisys_processed p ON TRY_CAST(d.source_id AS BIGINT) = p.prontuario_marido_ba),

    all_matches AS (
        SELECT * FROM matches_1  UNION ALL SELECT * FROM matches_2  UNION ALL
        SELECT * FROM matches_3  UNION ALL SELECT * FROM matches_4  UNION ALL
        SELECT * FROM matches_5  UNION ALL SELECT * FROM matches_6  UNION ALL
        SELECT * FROM matches_7  UNION ALL SELECT * FROM matches_8  UNION ALL
        SELECT * FROM matches_9  UNION ALL SELECT * FROM matches_10 UNION ALL
        SELECT * FROM matches_11 UNION ALL SELECT * FROM matches_12 UNION ALL
        SELECT * FROM matches_13 UNION ALL SELECT * FROM matches_14 UNION ALL
        SELECT * FROM matches_15
    ),

    filtered_matches AS (
        SELECT
            source_id,
            prontuario,
            match_type,
            ROW_NUMBER() OVER (
                PARTITION BY source_id
                ORDER BY
                    CASE
                        WHEN match_type = 'patientid_main'                             THEN 1
                        WHEN match_type IN ('patientid_esposa', 'patientid_marido')    THEN 2
                        ELSE 3
                    END,
                    prontuario DESC
            ) AS rn
        FROM all_matches
        WHERE
            name_first IS NULL
            OR esposa_nome LIKE '%' || name_first || '%'
            OR marido_nome LIKE '%' || name_first || '%'
    )

    SELECT source_id, prontuario, match_type
    FROM filtered_matches
    WHERE rn = 1
    """

    logger.info(f"{tag} Running {patient_type} pass SQL...")
    try:
        df = con.execute(sql).df()
        # Ensure source_id stays as string
        df["source_id"] = df["source_id"].astype(str)
        df["pass_number"] = pass_number
        logger.info(f"{tag} {patient_type} pass: {len(df)} matches found")
        return df
    except Exception as e:
        logger.error(f"{tag} Error in {patient_type} pass: {e}")
        raise


def _run_lastname_pass(
    con, source_schema, source_table, id_col, name_col,
    pass_number: int, tag: str, exclude_ids: Optional[Set] = None,
) -> pd.DataFrame:
    """
    LastName fallback pass. Mirrors update_prontuario_with_lastname logic exactly.
    Only runs when name_col is provided and a 'LastName' column exists alongside it.
    """
    try:
        cols = con.execute(f"""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema='{source_schema}' AND table_name='{source_table}'
        """).fetchall()
        col_names = [c[0] for c in cols]
    except Exception:
        col_names = []

    lastname_col = "LastName" if "LastName" in col_names else None
    if lastname_col is None:
        logger.info(f"{tag} No LastName column found, skipping LastName pass")
        return pd.DataFrame(columns=["source_id", "prontuario", "match_type", "pass_number"])

    exclude_clause = _build_exclude_clause(id_col, exclude_ids, con=con)

    sql = f"""
    WITH
    target_patients AS (
        SELECT
            CAST("{id_col}" AS VARCHAR) AS source_id,
            "{name_col}",
            "{lastname_col}",
            strip_accents(LOWER(TRIM(
                REPLACE("{name_col}", '.', '') || ' ' || "{lastname_col}"
            ))) AS full_name_search
        FROM {source_schema}.{source_table}
        WHERE "{id_col}" IS NOT NULL
          AND "{lastname_col}" IS NOT NULL
          AND (POSITION('.' IN "{name_col}") > 0 OR LENGTH("{name_col}") < 4)
          {exclude_clause}
    ),

    clinisys_active AS (
        SELECT
            codigo AS prontuario,
            strip_accents(LOWER(esposa_nome)) AS esposa_nome_full,
            strip_accents(LOWER(marido_nome)) AS marido_nome_full
        FROM clinisys_all.silver.view_pacientes
        WHERE inativo = 0
    ),

    matches AS (
        SELECT
            t.source_id,
            c.prontuario,
            'lastname' AS match_type,
            ROW_NUMBER() OVER (PARTITION BY t.source_id ORDER BY c.prontuario DESC) AS rn
        FROM target_patients t
        INNER JOIN clinisys_active c
            ON (c.esposa_nome_full LIKE '%' || t.full_name_search || '%'
                OR c.marido_nome_full LIKE '%' || t.full_name_search || '%')
    )

    SELECT source_id, prontuario, match_type
    FROM matches
    WHERE rn = 1
    """

    logger.info(f"{tag} Running LastName pass SQL...")
    try:
        df = con.execute(sql).df()
        df["source_id"] = df["source_id"].astype(str)
        df["pass_number"] = pass_number
        logger.info(f"{tag} LastName pass: {len(df)} matches found")
        return df
    except Exception as e:
        logger.error(f"{tag} Error in LastName pass: {e}")
        raise
