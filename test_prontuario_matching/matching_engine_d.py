"""
matching_engine_d.py — Strategy D: Hybrid Fast SQL Join + Python Priority Name Match
Combines index-friendly single-column hash joins in DuckDB with accurate 
priority name-matching and tie-breaking in Python.

Priority Grouping:
1. main codigo match (Group 1)
2. esposa columns match (Group 2)
3. marido columns match (Group 3)
4. responsavel columns match (Group 4)

Tie-Breaking Hierarchies (in order):
1. Name Match Score (Level 1 to 4):
   - Level 1: Full name match (patient name matches Clinisys name exactly, ignoring case/accents)
   - Level 2: First name and last name both match Clinisys name
   - Level 3: First name and some middle name match
   - Level 4: First name only matches
   - Level 5: Unmatched (first name doesn't match at all -> rejected as non-match)
2. Source mapping match:
   - Prefer candidate code that matches the original/existing prontuario column in the source table.
3. Active vs Inactive record check:
   - Prefer active records (inativo = '0') over inactive ones (inativo = '1').
4. Prontuario tie-breaker:
   - Arbitrary fallback to highest prontuario ID.
"""

import logging
import re
import unicodedata
from functools import lru_cache
import duckdb
import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Caching Name Normalization and Matching Functions
# ---------------------------------------------------------------------------

@lru_cache(maxsize=20000)
def normalize_name(name):
    """Normalize a name: strip accents, lowercase, keep only letters and spaces, return word tuple."""
    if not name or name is None or (isinstance(name, float) and pd.isna(name)):
        return tuple()
    # Strip accents
    name = ''.join(c for c in unicodedata.normalize('NFD', str(name)) if unicodedata.category(c) != 'Mn')
    # Lowercase and keep only letters and spaces
    name = re.sub(r'[^a-zA-Z\s]', ' ', name).lower()
    return tuple(w for w in name.split() if w)


@lru_cache(maxsize=20000)
def parse_patient_name(name):
    """Parse name: reverse "LASTNAME, FIRSTNAME" format if comma is present, then normalize."""
    if not name or name is None or (isinstance(name, float) and pd.isna(name)):
        return tuple()
    name_str = str(name)
    if ',' in name_str:
        parts = name_str.split(',', 1)
        # Reverse to standard First Last order
        name_str = parts[1] + ' ' + parts[0]
    return normalize_name(name_str)


@lru_cache(maxsize=100000)
def compute_name_score(patient_words, target_words):
    """
    Computes name match score:
    1 - Full name match
    2 - First name and last name match
    3 - First name and some other name match
    4 - First name only matches
    5 - Unmatched (rejected)
    """
    if not patient_words or not target_words:
        return 5  # Unmatched
        
    first_name = patient_words[0]
    if first_name not in target_words:
        return 5  # Unmatched (rejected)
        
    if patient_words == target_words:
        return 1  # Full name match
        
    if len(patient_words) == 1:
        return 4  # First name only (since not equal)
        
    last_name = patient_words[-1]
    if last_name in target_words:
        return 2  # First name and last name match
        
    other_words = patient_words[1:]
    if any(w in target_words for w in other_words):
        return 3  # First name and some other name match
        
    return 4  # First name only


# ---------------------------------------------------------------------------
# Public Entry Point
# ---------------------------------------------------------------------------

def run_strategy_d(
    source_con: duckdb.DuckDBPyConnection,
    clinisys_db_path: str,
    source_schema: str,
    source_table: str,
    id_col: str,
    name_col: str = None,
    label: str = "",
) -> pd.DataFrame:
    """
    Run Strategy D against a source table.
    Returns a DataFrame with columns: source_id, patient_name, prontuario, match_type
    One row per distinct non-null source_id in the source table.
    """
    tag = f"[StrategyD][{label or f'{source_schema}.{source_table}'}]"
    logger.info(f"{tag} Starting Strategy D")

    # 1. Attach clinisys DB read-only
    try:
        source_con.execute(f"ATTACH '{clinisys_db_path}' AS clinisys_all (READ_ONLY)")
        logger.info(f"{tag} Attached clinisys_all")
    except Exception as e:
        if "already attached" in str(e).lower():
            logger.info(f"{tag} clinisys_all already attached")
        else:
            raise

    # 2. Get distinct source table records (denominator)
    cols_df = source_con.execute(f"PRAGMA table_info('{source_schema}.{source_table}')").df()
    col_names = {c.lower() for c in cols_df["name"]}
    
    name_select = f'"{name_col}"' if name_col else "CAST(NULL AS VARCHAR)"
    
    if "prontuario_old" in col_names:
        orig_pront_col = "prontuario_old"
    elif "prontuario" in col_names:
        orig_pront_col = "prontuario"
    else:
        orig_pront_col = "NULL"
        
    orig_pront_select = f'TRY_CAST("{orig_pront_col}" AS BIGINT)' if orig_pront_col != "NULL" else "CAST(NULL AS BIGINT)"

    source_df = source_con.execute(f"""
        SELECT DISTINCT 
            CAST("{id_col}" AS VARCHAR) AS source_id, 
            {name_select} AS patient_name,
            {orig_pront_select} AS original_prontuario
        FROM {source_schema}.{source_table}
        WHERE "{id_col}" IS NOT NULL AND CAST("{id_col}" AS VARCHAR) != ''
    """).df()
    
    logger.info(f"{tag} Distinct source_id/patient_name rows: {len(source_df):,}")
    if source_df.empty:
        return pd.DataFrame(columns=["source_id", "patient_name", "prontuario", "match_type"])

    # 3. Query all candidates using index-friendly joins
    sql_candidates = f"""
    WITH
    source_extract AS (
        SELECT DISTINCT
            CAST("{id_col}" AS VARCHAR) AS source_id,
            {name_select} AS patient_name,
            {orig_pront_select} AS original_prontuario
        FROM {source_schema}.{source_table}
        WHERE "{id_col}" IS NOT NULL AND CAST("{id_col}" AS VARCHAR) != ''
    ),
    clinisys_processed AS (
        SELECT
            codigo,
            inativo,
            esposa_nome,
            marido_nome,
            responsavel_nome,
            prontuario_esposa,
            prontuario_esposa_pel,
            prontuario_esposa_pc,
            prontuario_esposa_fc,
            prontuario_esposa_ba,
            prontuario_marido,
            prontuario_marido_pel,
            prontuario_marido_pc,
            prontuario_marido_fc,
            prontuario_marido_ba,
            prontuario_responsavel1,
            prontuario_responsavel2,
            prontuario_responsavel1_pc,
            prontuario_responsavel2_pc
        FROM clinisys_all.silver.view_pacientes
    ),
    all_candidates AS (
        -- Group 1: Main codigo match
        SELECT s.source_id, s.patient_name, s.original_prontuario, c.codigo AS prontuario, c.inativo, c.esposa_nome, c.marido_nome, c.responsavel_nome, 1 AS priority_group, 'codigo_main' AS match_type, 'codigo' AS matched_column FROM source_extract s INNER JOIN clinisys_processed c ON TRY_CAST(s.source_id AS BIGINT) = c.codigo
        UNION ALL
        -- Group 2: Esposa columns match
        SELECT s.source_id, s.patient_name, s.original_prontuario, c.codigo AS prontuario, c.inativo, c.esposa_nome, c.marido_nome, c.responsavel_nome, 2 AS priority_group, 'esposa_match' AS match_type, 'prontuario_esposa' AS matched_column FROM source_extract s INNER JOIN clinisys_processed c ON TRY_CAST(s.source_id AS BIGINT) = TRY_CAST(c.prontuario_esposa AS BIGINT)
        UNION ALL
        SELECT s.source_id, s.patient_name, s.original_prontuario, c.codigo AS prontuario, c.inativo, c.esposa_nome, c.marido_nome, c.responsavel_nome, 2 AS priority_group, 'esposa_match' AS match_type, 'prontuario_esposa_pel' AS matched_column FROM source_extract s INNER JOIN clinisys_processed c ON TRY_CAST(s.source_id AS BIGINT) = TRY_CAST(c.prontuario_esposa_pel AS BIGINT)
        UNION ALL
        SELECT s.source_id, s.patient_name, s.original_prontuario, c.codigo AS prontuario, c.inativo, c.esposa_nome, c.marido_nome, c.responsavel_nome, 2 AS priority_group, 'esposa_match' AS match_type, 'prontuario_esposa_pc' AS matched_column FROM source_extract s INNER JOIN clinisys_processed c ON TRY_CAST(s.source_id AS BIGINT) = TRY_CAST(c.prontuario_esposa_pc AS BIGINT)
        UNION ALL
        SELECT s.source_id, s.patient_name, s.original_prontuario, c.codigo AS prontuario, c.inativo, c.esposa_nome, c.marido_nome, c.responsavel_nome, 2 AS priority_group, 'esposa_match' AS match_type, 'prontuario_esposa_fc' AS matched_column FROM source_extract s INNER JOIN clinisys_processed c ON TRY_CAST(s.source_id AS BIGINT) = TRY_CAST(c.prontuario_esposa_fc AS BIGINT)
        UNION ALL
        SELECT s.source_id, s.patient_name, s.original_prontuario, c.codigo AS prontuario, c.inativo, c.esposa_nome, c.marido_nome, c.responsavel_nome, 2 AS priority_group, 'esposa_match' AS match_type, 'prontuario_esposa_ba' AS matched_column FROM source_extract s INNER JOIN clinisys_processed c ON TRY_CAST(s.source_id AS BIGINT) = TRY_CAST(c.prontuario_esposa_ba AS BIGINT)
        UNION ALL
        -- Group 3: Marido columns match
        SELECT s.source_id, s.patient_name, s.original_prontuario, c.codigo AS prontuario, c.inativo, c.esposa_nome, c.marido_nome, c.responsavel_nome, 3 AS priority_group, 'marido_match' AS match_type, 'prontuario_marido' AS matched_column FROM source_extract s INNER JOIN clinisys_processed c ON TRY_CAST(s.source_id AS BIGINT) = TRY_CAST(c.prontuario_marido AS BIGINT)
        UNION ALL
        SELECT s.source_id, s.patient_name, s.original_prontuario, c.codigo AS prontuario, c.inativo, c.esposa_nome, c.marido_nome, c.responsavel_nome, 3 AS priority_group, 'marido_match' AS match_type, 'prontuario_marido_pel' AS matched_column FROM source_extract s INNER JOIN clinisys_processed c ON TRY_CAST(s.source_id AS BIGINT) = TRY_CAST(c.prontuario_marido_pel AS BIGINT)
        UNION ALL
        SELECT s.source_id, s.patient_name, s.original_prontuario, c.codigo AS prontuario, c.inativo, c.esposa_nome, c.marido_nome, c.responsavel_nome, 3 AS priority_group, 'marido_match' AS match_type, 'prontuario_marido_pc' AS matched_column FROM source_extract s INNER JOIN clinisys_processed c ON TRY_CAST(s.source_id AS BIGINT) = TRY_CAST(c.prontuario_marido_pc AS BIGINT)
        UNION ALL
        SELECT s.source_id, s.patient_name, s.original_prontuario, c.codigo AS prontuario, c.inativo, c.esposa_nome, c.marido_nome, c.responsavel_nome, 3 AS priority_group, 'marido_match' AS match_type, 'prontuario_marido_fc' AS matched_column FROM source_extract s INNER JOIN clinisys_processed c ON TRY_CAST(s.source_id AS BIGINT) = TRY_CAST(c.prontuario_marido_fc AS BIGINT)
        UNION ALL
        SELECT s.source_id, s.patient_name, s.original_prontuario, c.codigo AS prontuario, c.inativo, c.esposa_nome, c.marido_nome, c.responsavel_nome, 3 AS priority_group, 'marido_match' AS match_type, 'prontuario_marido_ba' AS matched_column FROM source_extract s INNER JOIN clinisys_processed c ON TRY_CAST(s.source_id AS BIGINT) = TRY_CAST(c.prontuario_marido_ba AS BIGINT)
        UNION ALL
        -- Group 4: Responsavel columns match
        SELECT s.source_id, s.patient_name, s.original_prontuario, c.codigo AS prontuario, c.inativo, c.esposa_nome, c.marido_nome, c.responsavel_nome, 4 AS priority_group, 'responsavel_match' AS match_type, 'prontuario_responsavel1' AS matched_column FROM source_extract s INNER JOIN clinisys_processed c ON TRY_CAST(s.source_id AS BIGINT) = TRY_CAST(c.prontuario_responsavel1 AS BIGINT)
        UNION ALL
        SELECT s.source_id, s.patient_name, s.original_prontuario, c.codigo AS prontuario, c.inativo, c.esposa_nome, c.marido_nome, c.responsavel_nome, 4 AS priority_group, 'responsavel_match' AS match_type, 'prontuario_responsavel2' AS matched_column FROM source_extract s INNER JOIN clinisys_processed c ON TRY_CAST(s.source_id AS BIGINT) = TRY_CAST(c.prontuario_responsavel2 AS BIGINT)
        UNION ALL
        SELECT s.source_id, s.patient_name, s.original_prontuario, c.codigo AS prontuario, c.inativo, c.esposa_nome, c.marido_nome, c.responsavel_nome, 4 AS priority_group, 'responsavel_match' AS match_type, 'prontuario_responsavel1_pc' AS matched_column FROM source_extract s INNER JOIN clinisys_processed c ON TRY_CAST(s.source_id AS BIGINT) = TRY_CAST(c.prontuario_responsavel1_pc AS BIGINT)
        UNION ALL
        SELECT s.source_id, s.patient_name, s.original_prontuario, c.codigo AS prontuario, c.inativo, c.esposa_nome, c.marido_nome, c.responsavel_nome, 4 AS priority_group, 'responsavel_match' AS match_type, 'prontuario_responsavel2_pc' AS matched_column FROM source_extract s INNER JOIN clinisys_processed c ON TRY_CAST(s.source_id AS BIGINT) = TRY_CAST(c.prontuario_responsavel2_pc AS BIGINT)
    )
    SELECT DISTINCT * FROM all_candidates
    """

    logger.info(f"{tag} Running SQL query to fetch candidates...")
    candidates_df = source_con.execute(sql_candidates).df()
    logger.info(f"{tag} Total candidates fetched: {len(candidates_df):,}")

    if candidates_df.empty:
        # Return all as unmatched
        results_df = source_df.copy()
        results_df["prontuario"] = -1
        results_df["match_type"] = "unmatched"
        return results_df[["source_id", "patient_name", "prontuario", "match_type"]]

    # 4. Perform Name Match Strength Scoring in Python
    name_scores = []
    
    # Pre-parse names of source patients
    # We do a memoized mapping for speed
    parsed_source_names = {}
    for pname in candidates_df["patient_name"].unique():
        parsed_source_names[pname] = parse_patient_name(pname)

    logger.info(f"{tag} Scoring candidates...")
    for row in candidates_df.itertuples():
        pname = row.patient_name
        p_words = parsed_source_names.get(pname, tuple())
        priority = row.priority_group
        
        if not name_col or not p_words:
            # If name validation not requested or source name is empty, score is 1 (always matches)
            score = 1
        else:
            # Score against appropriate target columns
            if priority == 1:
                # Group 1 (codigo_main) can match any of the three Clinisys names
                score_esposa = compute_name_score(p_words, normalize_name(row.esposa_nome))
                score_marido = compute_name_score(p_words, normalize_name(row.marido_nome))
                score_resp = compute_name_score(p_words, normalize_name(row.responsavel_nome))
                score = min(score_esposa, score_marido, score_resp)
            elif priority == 2:
                score = compute_name_score(p_words, normalize_name(row.esposa_nome))
            elif priority == 3:
                score = compute_name_score(p_words, normalize_name(row.marido_nome))
            elif priority == 4:
                score = compute_name_score(p_words, normalize_name(row.responsavel_nome))
            else:
                score = 5
        
        name_scores.append(score)

    candidates_df["name_score"] = name_scores
    
    # 5. Filter out non-matches (name_score == 5 means first name didn't match / rejected)
    valid_candidates = candidates_df[candidates_df["name_score"] < 5].copy()
    logger.info(f"{tag} Candidates after name filtering: {len(valid_candidates):,}")

    if valid_candidates.empty:
        results_df = source_df.copy()
        results_df["prontuario"] = -1
        results_df["match_type"] = "unmatched"
        return results_df[["source_id", "patient_name", "prontuario", "match_type"]]

    # 6. Apply sorting & tie-breakers:
    #   a. priority_group (1 to 4) ASC
    #   b. name_score (1 to 4) ASC
    #   c. orig_match (0 for match, 1 for mismatch) ASC
    #   d. inativo_score (0 for active, 1 for inactive) ASC
    #   e. prontuario DESC
    
    # Pre-calculate tie-breaker columns
    valid_candidates["orig_match"] = (valid_candidates["prontuario"] != valid_candidates["original_prontuario"].fillna(-2)).astype(int)
    valid_candidates["inativo_score"] = (valid_candidates["inativo"].fillna("1") != "0").astype(int)
    
    # Sort
    valid_candidates = valid_candidates.sort_values(
        by=["priority_group", "name_score", "orig_match", "inativo_score", "prontuario"],
        ascending=[True, True, True, True, False]
    )
    
    # Deduplicate: keep first for each source_id and patient_name
    best_matches = valid_candidates.drop_duplicates(subset=["source_id", "patient_name"], keep="first")
    
    # 7. Merge back to the full source_df to ensure all rows are accounted for (denominator match)
    final_df = pd.merge(
        source_df,
        best_matches[["source_id", "patient_name", "prontuario", "match_type"]],
        on=["source_id", "patient_name"],
        how="left"
    )
    
    final_df["prontuario"] = final_df["prontuario"].fillna(-1).astype(int)
    final_df["match_type"] = final_df["match_type"].fillna("unmatched")
    
    n_matched = len(final_df[final_df["prontuario"] != -1])
    total = len(final_df)
    logger.info(f"{tag} Completed Strategy D: {n_matched}/{total} matched ({n_matched/total*100:.2f}%)")
    
    return final_df[["source_id", "patient_name", "prontuario", "match_type"]]
