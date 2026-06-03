"""
matching_engine_c.py — Strategy C: Hybrid Priority & Name Match Strength Scoring
Implements the hybrid Strategy C logic for patient prontuario matching.

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

import os
import re
import unicodedata
import logging
import pandas as pd
import duckdb

logger = logging.getLogger(__name__)

def normalize_name(name):
    if not name or pd.isna(name):
        return []
    # Strip accents
    name = ''.join(c for c in unicodedata.normalize('NFD', str(name)) if unicodedata.category(c) != 'Mn')
    # Lowercase and keep only letters and spaces
    name = re.sub(r'[^a-zA-Z\s]', ' ', name).lower()
    return [w for w in name.split() if w]

def parse_patient_name(name):
    if not name or pd.isna(name):
        return []
    name_str = str(name)
    if ',' in name_str:
        parts = name_str.split(',', 1)
        # Reverse to standard First Last order
        name_str = parts[1] + ' ' + parts[0]
    return normalize_name(name_str)

def compute_name_score(patient_words, clinisys_name):
    if not patient_words or not clinisys_name or pd.isna(clinisys_name):
        return 5  # Unmatched
        
    c_words = normalize_name(clinisys_name)
    if not c_words:
        return 5
        
    # Check 1: Full name match
    if patient_words == c_words:
        return 1
        
    first_name = patient_words[0]
    if first_name not in c_words:
        return 5  # Unmatched (rejected)
        
    if len(patient_words) == 1:
        return 4  # First name only (since not equal)
        
    # Check 2: First and Last name match
    last_name = patient_words[-1]
    if last_name in c_words:
        return 2
        
    # Check 3: First and any other name match
    other_words = patient_words[1:]
    if any(w in c_words for w in other_words):
        return 3
        
    # Check 4: First name only
    return 4

def run_strategy_c(
    source_con: duckdb.DuckDBPyConnection,
    clinisys_db_path: str,
    source_schema: str,
    source_table: str,
    id_col: str,
    name_col: str = None,
    label: str = "",
) -> pd.DataFrame:
    """
    Run Strategy C against a source table.
    Returns a DataFrame with columns: source_id, prontuario, match_type
    One row per distinct non-null source_id in the source table.
    """
    tag = f"[StrategyC][{label or f'{source_schema}.{source_table}'}]"
    logger.info(f"{tag} Starting Strategy C")

    # Attach clinisys DB
    try:
        source_con.execute(f"ATTACH '{clinisys_db_path}' AS clinisys_all (READ_ONLY)")
        logger.info(f"{tag} Attached clinisys_all")
    except Exception as e:
        if "already attached" in str(e).lower():
            logger.info(f"{tag} clinisys_all already attached")
        else:
            raise

    # Step 1: Fetch source table records
    name_select = f'"{name_col}"' if name_col else "CAST(NULL AS VARCHAR)"
    source_df = source_con.execute(f"""
        SELECT DISTINCT 
            CAST("{id_col}" AS VARCHAR) as source_id, 
            {name_select} as patient_name,
            prontuario as original_prontuario
        FROM {source_schema}.{source_table}
        WHERE "{id_col}" IS NOT NULL
    """).df()
    
    # Filter out empty source_ids
    source_df = source_df[source_df["source_id"].str.strip() != ""]
    logger.info(f"{tag} Total distinct source IDs: {len(source_df):,}")
    
    if source_df.empty:
        return pd.DataFrame(columns=["source_id", "prontuario", "match_type"])
        
    source_df["original_prontuario"] = source_df["original_prontuario"].fillna(-1).astype(int)
    
    # Prepare IDs for Clinisys query
    numeric_ids = []
    for sid in source_df["source_id"].unique():
        try:
            if sid.endswith(".0"):
                sid = sid[:-2]
            numeric_ids.append(int(sid))
        except ValueError:
            pass
            
    if not numeric_ids:
        logger.warning(f"{tag} No numeric IDs found to match.")
        source_df["prontuario"] = -1
        source_df["match_type"] = "unmatched"
        return source_df[["source_id", "prontuario", "match_type"]]
        
    sid_set = set(numeric_ids)
    
    # Step 2: Materialize view_pacientes into a temp table for fast execution
    logger.info(f"{tag} Materializing view_pacientes...")
    source_con.execute("CREATE OR REPLACE TEMP TABLE temp_view_pacientes AS SELECT * FROM clinisys_all.silver.view_pacientes")
    
    # Step 3: Load clinisys columns in Pandas
    logger.info(f"{tag} Loading Clinisys table...")
    clinisys_df = source_con.execute("""
        SELECT 
            codigo,
            inativo,
            esposa_nome,
            marido_nome,
            responsavel_nome,
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
            prontuario_marido_ba
        FROM temp_view_pacientes
    """).df()
    
    # Filter in Pandas using isin (100x faster than SQL OR-tree)
    codigo_series = clinisys_df["codigo"].fillna(-1).astype(int)
    
    esposa_cols = [
        "prontuario_esposa", "prontuario_esposa_pel", "prontuario_esposa_pc", 
        "prontuario_esposa_fc", "prontuario_esposa_ba"
    ]
    marido_cols = [
        "prontuario_marido", "prontuario_marido_pel", "prontuario_marido_pc", 
        "prontuario_marido_fc", "prontuario_marido_ba"
    ]
    responsavel_cols = [
        "prontuario_responsavel1", "prontuario_responsavel2", 
        "prontuario_responsavel1_pc", "prontuario_responsavel2_pc"
    ]
    
    mask = codigo_series.isin(sid_set)
    for col in esposa_cols + marido_cols + responsavel_cols:
        mask = mask | clinisys_df[col].fillna(-1).astype(int).isin(sid_set)
        
    clinisys_filtered = clinisys_df[mask].copy()
    
    # Convert match fields to int
    clinisys_filtered["codigo"] = clinisys_filtered["codigo"].fillna(-1).astype(int)
    for col in esposa_cols + marido_cols + responsavel_cols:
        clinisys_filtered[col] = clinisys_filtered[col].fillna(-1).astype(int)
        
    # Convert to dicts for fast lookups
    clinisys_list = clinisys_filtered.to_dict('records')
    
    codigo_map = {}
    esposa_map = {}
    marido_map = {}
    resp_map = {}
    
    for c_row in clinisys_list:
        cod = c_row["codigo"]
        if cod != -1:
            codigo_map.setdefault(cod, []).append(c_row)
            
        for col in esposa_cols:
            val = c_row[col]
            if val != -1:
                esposa_map.setdefault(val, []).append(c_row)
                
        for col in marido_cols:
            val = c_row[col]
            if val != -1:
                marido_map.setdefault(val, []).append(c_row)
                
        for col in responsavel_cols:
            val = c_row[col]
            if val != -1:
                resp_map.setdefault(val, []).append(c_row)
                
    results = []
    
    # Run matching logic
    logger.info(f"{tag} Running matching algorithms...")
    for _, row in source_df.iterrows():
        sid = row["source_id"]
        pname = row["patient_name"]
        orig_p = row["original_prontuario"]
        
        try:
            if sid.endswith(".0"):
                sid_val = int(sid[:-2])
            else:
                sid_val = int(sid)
        except ValueError:
            results.append({"source_id": str(sid), "prontuario": -1, "match_type": "unmatched"})
            continue
            
        p_words = parse_patient_name(pname)
        candidates = []
        
        # Check 1: Main (Group 1)
        if sid_val in codigo_map:
            for c_row in codigo_map[sid_val]:
                codigo = c_row["codigo"]
                inativo_str = str(c_row["inativo"]).strip()
                inativo_score = 0 if inativo_str == '0' else 1
                if name_col:
                    score_esposa = compute_name_score(p_words, c_row["esposa_nome"])
                    score_marido = compute_name_score(p_words, c_row["marido_nome"])
                    score_resp = compute_name_score(p_words, c_row["responsavel_nome"])
                    score = min(score_esposa, score_marido, score_resp)
                else:
                    score = 1
                    
                if not name_col or score < 5:
                    candidates.append({
                        "prontuario": codigo, "group": 1, "name_score": score, 
                        "inativo_score": inativo_score, "match_type": "codigo_main"
                    })
                    
        # Check 2: Esposa (Group 2)
        if sid_val in esposa_map:
            for c_row in esposa_map[sid_val]:
                codigo = c_row["codigo"]
                inativo_str = str(c_row["inativo"]).strip()
                inativo_score = 0 if inativo_str == '0' else 1
                if name_col:
                    score = compute_name_score(p_words, c_row["esposa_nome"])
                else:
                    score = 1
                if not name_col or score < 5:
                    candidates.append({
                        "prontuario": codigo, "group": 2, "name_score": score, 
                        "inativo_score": inativo_score, "match_type": "esposa_match"
                    })
                    
        # Check 3: Marido (Group 3)
        if sid_val in marido_map:
            for c_row in marido_map[sid_val]:
                codigo = c_row["codigo"]
                inativo_str = str(c_row["inativo"]).strip()
                inativo_score = 0 if inativo_str == '0' else 1
                if name_col:
                    score = compute_name_score(p_words, c_row["marido_nome"])
                else:
                    score = 1
                if not name_col or score < 5:
                    candidates.append({
                        "prontuario": codigo, "group": 3, "name_score": score, 
                        "inativo_score": inativo_score, "match_type": "marido_match"
                    })
                    
        # Check 4: Responsavel (Group 4)
        if sid_val in resp_map:
            for c_row in resp_map[sid_val]:
                codigo = c_row["codigo"]
                inativo_str = str(c_row["inativo"]).strip()
                inativo_score = 0 if inativo_str == '0' else 1
                if name_col:
                    score = compute_name_score(p_words, c_row["responsavel_nome"])
                else:
                    score = 1
                if not name_col or score < 5:
                    candidates.append({
                        "prontuario": codigo, "group": 4, "name_score": score, 
                        "inativo_score": inativo_score, "match_type": "responsavel_match"
                    })
                    
        if not candidates:
            results.append({"source_id": str(sid), "patient_name": pname, "prontuario": -1, "match_type": "unmatched"})
        else:
            for cand in candidates:
                cand["orig_match"] = 0 if cand["prontuario"] == orig_p else 1
                
            # Tie-breakers:
            # 1. Priority Group (1 to 4)
            # 2. Name matching score hierarchy (1 to 4)
            # 3. Source mapping match (original_prontuario matches candidate codigo)
            # 4. Clinisys inativo value (active preferred over inactive)
            # 5. Prontuario DESC fallback
            candidates.sort(key=lambda x: (x["group"], x["name_score"], x["orig_match"], x["inativo_score"], -x["prontuario"]))
            best = candidates[0]
            results.append({"source_id": str(sid), "patient_name": pname, "prontuario": best["prontuario"], "match_type": best["match_type"]})
            
    # Clean up temp table
    source_con.execute("DROP TABLE IF EXISTS temp_view_pacientes")
    
    final_df = pd.DataFrame(results)
    n_matched = len(final_df[final_df["prontuario"] != -1])
    logger.info(f"{tag} Completed Strategy C: {n_matched}/{len(final_df)} matched ({n_matched/len(final_df)*100:.2f}%)")
    return final_df
