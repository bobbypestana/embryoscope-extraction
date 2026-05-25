"""
patient_matching_generic.py

Generic patient matching utility.
Given any DataFrame, matches a PIN column (and optionally a name column)
against clinisys_all.duckdb and returns the enriched DataFrame with
`prontuario` and `patient_name` columns.

Usage:
    from utils.patient_matching_generic import match_prontuario

    df = match_prontuario(df, pin_col='PIN', name_col='NOME DA RECEPTORA')
"""
import os
import logging
from typing import Optional
import pandas as pd
import duckdb

logger = logging.getLogger(__name__)

# Default DB path: <project_root>/database/clinisys_all.duckdb
_DEFAULT_DB = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    'database', 'clinisys_all.duckdb'
)


# ---------------------------------------------------------------------------
# PIN cleaning (copied from patient_id_cleaner.py)
# ---------------------------------------------------------------------------

def _clean_pin(raw):
    """Convert a raw PIN to int, removing thousand-separator dots."""
    if pd.isna(raw) or raw is None:
        return None
    s = str(raw).strip()
    if '.' in s:
        cleaned = s.replace('.', '')
        if cleaned.isdigit():
            v = int(cleaned)
            return v if v != 0 else None
    if s.isdigit():
        v = int(s)
        return v if v != 0 else None
    try:
        v = int(float(s))
        return v if v != 0 else None
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Matching passes (adapted from patient_matching.py)
# ---------------------------------------------------------------------------

def _matching_sql(inactive_condition: str, has_name: bool) -> str:
    """
    Build the matching SQL for one pass (active or inactive patients).

    The SQL mirrors the logic in patient_matching.py exactly:
    - CTE1: extracts name_first from the "FirstName" column (handles both
            "LAST, FIRST MIDDLE" and "FIRST MIDDLE" formats via comma detection)
    - CTE2: extracts unmatched rows
    - CTE3-17: joins PIN against each of the 15 prontuario columns in clinisys
    - filtered_matches: applies name validation if a name column was provided,
                        then picks the highest-priority match per PIN
    strip_accents() is a native DuckDB function — no macro needed.
    """
    # If no name column was provided, all matches pass the name filter
    name_filter = (
        "name_first IS NULL "
        "OR esposa_nome LIKE name_first || '%' "
        "OR marido_nome LIKE name_first || '%'"
        if has_name else "TRUE"
    )

    return f"""
    WITH
    patient_name_extract AS (
        SELECT
            "PatientID", prontuario, "FirstName",
            CASE
                WHEN "FirstName" IS NOT NULL THEN
                    CASE
                        WHEN POSITION(',' IN "FirstName") > 0 THEN
                            REGEXP_REPLACE(
                                strip_accents(LOWER(TRIM(SPLIT_PART("FirstName", ',', 2)))),
                                '^[^a-z]*([a-z]+).*', '\\1'
                            )
                        ELSE
                            REGEXP_REPLACE(
                                strip_accents(LOWER(TRIM("FirstName"))),
                                '^[^a-z]*([a-z]+).*', '\\1'
                            )
                    END
                ELSE NULL
            END AS name_first
        FROM silver.patients
    ),
    unmatched AS (
        SELECT DISTINCT "PatientID" AS patient_id, name_first
        FROM patient_name_extract
        WHERE prontuario = -1 AND "PatientID" IS NOT NULL
    ),
    clinisys AS (
        SELECT
            codigo,
            prontuario_esposa, prontuario_marido,
            prontuario_responsavel1, prontuario_responsavel2,
            prontuario_esposa_pel,  prontuario_marido_pel,
            prontuario_esposa_pc,   prontuario_marido_pc,
            prontuario_responsavel1_pc, prontuario_responsavel2_pc,
            prontuario_esposa_fc,   prontuario_marido_fc,
            prontuario_esposa_ba,   prontuario_marido_ba,
            strip_accents(LOWER(SPLIT_PART(TRIM(esposa_nome), ' ', 1))) AS esposa_nome,
            strip_accents(LOWER(SPLIT_PART(TRIM(marido_nome), ' ', 1))) AS marido_nome
        FROM clinisys_all.silver.view_pacientes
        WHERE {inactive_condition}
    ),
    all_matches AS (
        SELECT u.*, c.codigo AS prontuario, c.esposa_nome, c.marido_nome, 1 AS priority FROM unmatched u JOIN clinisys c ON u.patient_id = c.codigo
        UNION ALL SELECT u.*, c.codigo, c.esposa_nome, c.marido_nome, 2 FROM unmatched u JOIN clinisys c ON u.patient_id = c.prontuario_esposa
        UNION ALL SELECT u.*, c.codigo, c.esposa_nome, c.marido_nome, 2 FROM unmatched u JOIN clinisys c ON u.patient_id = c.prontuario_marido
        UNION ALL SELECT u.*, c.codigo, c.esposa_nome, c.marido_nome, 3 FROM unmatched u JOIN clinisys c ON u.patient_id = c.prontuario_responsavel1
        UNION ALL SELECT u.*, c.codigo, c.esposa_nome, c.marido_nome, 3 FROM unmatched u JOIN clinisys c ON u.patient_id = c.prontuario_responsavel2
        UNION ALL SELECT u.*, c.codigo, c.esposa_nome, c.marido_nome, 3 FROM unmatched u JOIN clinisys c ON u.patient_id = c.prontuario_esposa_pel
        UNION ALL SELECT u.*, c.codigo, c.esposa_nome, c.marido_nome, 3 FROM unmatched u JOIN clinisys c ON u.patient_id = c.prontuario_marido_pel
        UNION ALL SELECT u.*, c.codigo, c.esposa_nome, c.marido_nome, 3 FROM unmatched u JOIN clinisys c ON u.patient_id = c.prontuario_esposa_pc
        UNION ALL SELECT u.*, c.codigo, c.esposa_nome, c.marido_nome, 3 FROM unmatched u JOIN clinisys c ON u.patient_id = c.prontuario_marido_pc
        UNION ALL SELECT u.*, c.codigo, c.esposa_nome, c.marido_nome, 3 FROM unmatched u JOIN clinisys c ON u.patient_id = c.prontuario_responsavel1_pc
        UNION ALL SELECT u.*, c.codigo, c.esposa_nome, c.marido_nome, 3 FROM unmatched u JOIN clinisys c ON u.patient_id = c.prontuario_responsavel2_pc
        UNION ALL SELECT u.*, c.codigo, c.esposa_nome, c.marido_nome, 3 FROM unmatched u JOIN clinisys c ON u.patient_id = c.prontuario_esposa_fc
        UNION ALL SELECT u.*, c.codigo, c.esposa_nome, c.marido_nome, 3 FROM unmatched u JOIN clinisys c ON u.patient_id = c.prontuario_marido_fc
        UNION ALL SELECT u.*, c.codigo, c.esposa_nome, c.marido_nome, 3 FROM unmatched u JOIN clinisys c ON u.patient_id = c.prontuario_esposa_ba
        UNION ALL SELECT u.*, c.codigo, c.esposa_nome, c.marido_nome, 3 FROM unmatched u JOIN clinisys c ON u.patient_id = c.prontuario_marido_ba
    ),
    filtered_matches AS (
        SELECT
            patient_id, prontuario,
            ROW_NUMBER() OVER (
                PARTITION BY patient_id
                ORDER BY priority, prontuario DESC
            ) AS rn
        FROM all_matches
        WHERE {name_filter}
    )
    SELECT patient_id, prontuario FROM filtered_matches WHERE rn = 1
    """


def _lastname_sql() -> str:
    """
    LastName-based fallback pass (active patients only).
    Mirrors update_prontuario_with_lastname() from patient_matching.py.
    Only runs for rows where FirstName contains a dot or is very short.
    """
    return """
    WITH target AS (
        SELECT
            "PatientID",
            "FirstName",
            "LastName",
            strip_accents(LOWER(TRIM(REPLACE("FirstName", '.', '') || ' ' || "LastName")))
                AS full_name_search
        FROM silver.patients
        WHERE prontuario = -1
          AND "PatientID" IS NOT NULL
          AND "LastName" IS NOT NULL
          AND (POSITION('.' IN "FirstName") > 0 OR LENGTH("FirstName") < 4)
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
            t."PatientID" AS patient_id,
            c.prontuario,
            ROW_NUMBER() OVER (PARTITION BY t."PatientID" ORDER BY c.prontuario DESC) AS rn
        FROM target t
        JOIN clinisys_active c
          ON c.esposa_nome_full LIKE '%' || t.full_name_search || '%'
          OR c.marido_nome_full LIKE '%' || t.full_name_search || '%'
    )
    SELECT patient_id, prontuario FROM matches WHERE rn = 1
    """


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def match_prontuario(
    df: pd.DataFrame,
    pin_col: str,
    name_col: Optional[str] = None,
    db_path: Optional[str] = None,
) -> pd.DataFrame:
    """
    Enrich a DataFrame with `prontuario` and `patient_name` from clinisys_all.

    Args:
        df       : Input DataFrame (not modified in place)
        pin_col  : Column containing the patient PIN
        name_col : Optional column with the patient name. Two formats accepted:
                     "LAST, FIRST MIDDLE"  (comma-separated)
                     "FIRST MIDDLE"        (space-separated)
                   When None, PIN match alone determines the prontuario.
        db_path  : Explicit path to clinisys_all.duckdb. Defaults to
                   <project_root>/database/clinisys_all.duckdb.

    Returns:
        A copy of df with two new columns:
          prontuario   (int)  — -1 if unmatched
          patient_name (str)  — None if unmatched
    """
    db_path = db_path or _DEFAULT_DB
    has_name = name_col is not None

    result = df.copy()

    # -- Clean PINs ----------------------------------------------------------
    result['_PatientID'] = result[pin_col].apply(_clean_pin)
    n_valid = result['_PatientID'].notna().sum()
    logger.info(f"Valid PINs: {n_valid} / {len(result)}")

    # -- Build silver.patients -----------------------------------------------
    silver = pd.DataFrame({
        'PatientID': result['_PatientID'],
        'FirstName': result[name_col] if has_name else None,
        'LastName':  None,          # not used in this context
        'prontuario': -1,
    })

    con = duckdb.connect()
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    con.register('_silver', silver)
    con.execute("""
        CREATE TABLE silver.patients AS
        SELECT
            TRY_CAST("PatientID" AS BIGINT) AS "PatientID",
            "FirstName"::VARCHAR             AS "FirstName",
            "LastName"::VARCHAR              AS "LastName",
            prontuario::INTEGER              AS prontuario
        FROM _silver
    """)
    con.unregister('_silver')

    con.execute(f"ATTACH '{db_path}' AS clinisys_all (READ_ONLY)")

    # -- Run the three passes (mirrors patient_matching.py) ------------------
    for label, inactive_cond in [
        ("active",   "inativo = 0"),
        ("inactive", "inativo = 1"),
    ]:
        sql = _matching_sql(inactive_cond, has_name)
        con.execute(f"CREATE OR REPLACE TEMP TABLE _matches AS {sql}")
        n = con.execute("SELECT COUNT(*) FROM _matches").fetchone()[0]
        logger.info(f"  Pass {label}: {n} matches found")
        con.execute("""
            UPDATE silver.patients SET prontuario = m.prontuario
            FROM _matches m
            WHERE silver.patients."PatientID" = m.patient_id
              AND silver.patients.prontuario = -1
        """)

    if has_name:
        sql = _lastname_sql()
        con.execute(f"CREATE OR REPLACE TEMP TABLE _matches AS {sql}")
        n = con.execute("SELECT COUNT(*) FROM _matches").fetchone()[0]
        logger.info(f"  Pass lastname: {n} matches found")
        con.execute("""
            UPDATE silver.patients SET prontuario = m.prontuario
            FROM _matches m
            WHERE silver.patients."PatientID" = m.patient_id
              AND silver.patients.prontuario = -1
        """)

    # -- Read results --------------------------------------------------------
    matched = con.execute(
        'SELECT "PatientID", prontuario FROM silver.patients'
    ).df()

    total   = len(matched)
    n_match = (matched['prontuario'] != -1).sum()
    logger.info(f"Final match rate: {n_match}/{total} ({100*n_match/total:.1f}%)")

    # -- Fetch patient names -------------------------------------------------
    names = con.execute("""
        SELECT codigo AS prontuario,
               COALESCE(esposa_nome, marido_nome) AS patient_name
        FROM clinisys_all.silver.view_pacientes
    """).df().drop_duplicates('prontuario').set_index('prontuario')

    con.close()

    # -- Join back -----------------------------------------------------------
    pron_map = (
        matched
        .dropna(subset=['PatientID'])
        .drop_duplicates('PatientID')
        .set_index('PatientID')['prontuario']
    )
    result['prontuario']   = result['_PatientID'].map(pron_map).fillna(-1).astype(int)
    result['patient_name'] = result['prontuario'].map(names['patient_name'])
    result.drop(columns=['_PatientID'], inplace=True)

    return result
