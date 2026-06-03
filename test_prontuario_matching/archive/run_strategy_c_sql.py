import os
import sys
import duckdb
import pandas as pd

DB_DIR = r"g:\My Drive\projetos_individuais\Huntington\database"
CLINISYS_DB = os.path.join(DB_DIR, "clinisys_all.duckdb")
DATALAKE_DB = os.path.join(DB_DIR, "huntington_data_lake.duckdb")

def run_strategy_c_sql(con, clinisys_db_path, source_schema, source_table, id_col, name_col=None):
    """
    Builds and executes the pure SQL query for Strategy C.
    Runs on the entire table.
    """
    try:
        con.execute(f"ATTACH '{clinisys_db_path}' AS clinisys_all (READ_ONLY)")
    except Exception as e:
        if "already attached" not in str(e).lower():
            raise

    # Dynamic columns extraction expressions
    if name_col:
        pname_sql = f"""
        CASE 
            WHEN s."{name_col}" IS NOT NULL THEN 
                CASE 
                    WHEN POSITION(',' IN s."{name_col}") > 0 THEN 
                        SPLIT_PART(s."{name_col}", ',', 2) || ' ' || SPLIT_PART(s."{name_col}", ',', 1)
                    ELSE s."{name_col}" 
                END
            ELSE NULL 
        END
        """
        p_words_sql = f"""
        list_filter(
            regexp_split_to_array(
                regexp_replace(strip_accents(lower(trim({pname_sql}))), '[^a-z]', ' ', 'g'),
                '\\s+'
            ),
            w -> length(w) > 0
        )
        """
    else:
        p_words_sql = "CAST(NULL AS VARCHAR[])"

    c_words_sql = lambda col: f"""
    list_filter(
        regexp_split_to_array(
            regexp_replace(strip_accents(lower(trim({col}))), '[^a-z]', ' ', 'g'),
            '\\s+'
        ),
        w -> length(w) > 0
    )
    """

    if name_col:
        score_sql = lambda col: f"""
        CASE
            WHEN s.p_words IS NULL OR {c_words_sql(col)} IS NULL OR len(s.p_words) = 0 OR len({c_words_sql(col)}) = 0 THEN 5
            WHEN s.p_words = {c_words_sql(col)} THEN 1
            WHEN NOT list_contains({c_words_sql(col)}, s.p_words[1]) THEN 5
            WHEN list_contains({c_words_sql(col)}, s.p_words[-1]) THEN 2
            WHEN len(list_filter(s.p_words, w -> list_contains({c_words_sql(col)}, w))) > 1 THEN 3
            ELSE 4
        END
        """
    else:
        score_sql = lambda col: "1"

    name_select = f's."{name_col}"' if name_col else "CAST(NULL AS VARCHAR)"
    query = f"""
    WITH
    source_extract AS (
        SELECT DISTINCT
            CAST(s."{id_col}" AS VARCHAR) as source_id,
            {name_select} as patient_name,
            {p_words_sql} as p_words,
            s.prontuario as original_prontuario
        FROM source_db.{source_schema}.{source_table} s
        WHERE s."{id_col}" IS NOT NULL AND CAST(s."{id_col}" AS VARCHAR) != ''
    ),
    clinisys_processed AS (
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
            prontuario_marido_ba,
            list_filter(regexp_split_to_array(regexp_replace(strip_accents(lower(trim(esposa_nome))), '[^a-z]', ' ', 'g'), '\\s+'), w -> length(w) > 0) as esposa_words,
            list_filter(regexp_split_to_array(regexp_replace(strip_accents(lower(trim(marido_nome))), '[^a-z]', ' ', 'g'), '\\s+'), w -> length(w) > 0) as marido_words,
            list_filter(regexp_split_to_array(regexp_replace(strip_accents(lower(trim(responsavel_nome))), '[^a-z]', ' ', 'g'), '\\s+'), w -> length(w) > 0) as responsavel_words
        FROM silver.view_pacientes
    ),
    matches_1 AS (
        SELECT 
            s.source_id,
            s.patient_name,
            s.p_words,
            s.original_prontuario,
            c.codigo as prontuario,
            1 as priority_group,
            c.inativo,
            c.esposa_nome,
            c.marido_nome,
            'codigo_main' as match_type,
            LEAST({score_sql('c.esposa_nome')}, {score_sql('c.marido_nome')}, {score_sql('c.responsavel_nome')}) as name_score
        FROM source_extract s
        INNER JOIN clinisys_processed c ON TRY_CAST(s.source_id AS BIGINT) = c.codigo
    ),
    matches_2 AS (
        SELECT 
            s.source_id,
            s.patient_name,
            s.p_words,
            s.original_prontuario,
            c.codigo as prontuario,
            2 as priority_group,
            c.inativo,
            c.esposa_nome,
            c.marido_nome,
            'esposa_match' as match_type,
            {score_sql('c.esposa_nome')} as name_score
        FROM source_extract s
        INNER JOIN clinisys_processed c ON 
            TRY_CAST(s.source_id AS BIGINT) IN (
                c.prontuario_esposa, c.prontuario_esposa_pel, c.prontuario_esposa_pc, 
                c.prontuario_esposa_fc, c.prontuario_esposa_ba
            )
    ),
    matches_3 AS (
        SELECT 
            s.source_id,
            s.patient_name,
            s.p_words,
            s.original_prontuario,
            c.codigo as prontuario,
            3 as priority_group,
            c.inativo,
            c.esposa_nome,
            c.marido_nome,
            'marido_match' as match_type,
            {score_sql('c.marido_nome')} as name_score
        FROM source_extract s
        INNER JOIN clinisys_processed c ON 
            TRY_CAST(s.source_id AS BIGINT) IN (
                c.prontuario_marido, c.prontuario_marido_pel, c.prontuario_marido_pc, 
                c.prontuario_marido_fc, c.prontuario_marido_ba
            )
    ),
    matches_4 AS (
        SELECT 
            s.source_id,
            s.patient_name,
            s.p_words,
            s.original_prontuario,
            c.codigo as prontuario,
            4 as priority_group,
            c.inativo,
            c.esposa_nome,
            c.marido_nome,
            'responsavel_match' as match_type,
            {score_sql('c.responsavel_nome')} as name_score
        FROM source_extract s
        INNER JOIN clinisys_processed c ON 
            TRY_CAST(s.source_id AS BIGINT) IN (
                c.prontuario_responsavel1, c.prontuario_responsavel2, 
                c.prontuario_responsavel1_pc, c.prontuario_responsavel2_pc
            )
    ),
    all_matches AS (
        SELECT * FROM matches_1
        UNION ALL SELECT * FROM matches_2
        UNION ALL SELECT * FROM matches_3
        UNION ALL SELECT * FROM matches_4
    ),
    ranked_matches AS (
        SELECT 
            source_id,
            p_words,
            prontuario,
            match_type,
            ROW_NUMBER() OVER (
                PARTITION BY source_id, p_words
                ORDER BY 
                    priority_group ASC,
                    name_score ASC,
                    CASE WHEN prontuario = original_prontuario THEN 0 ELSE 1 END ASC,
                    CASE WHEN inativo = '0' THEN 0 ELSE 1 END ASC,
                    prontuario DESC
            ) as rn
        FROM all_matches
        WHERE name_score < 5
    )
    SELECT 
        s.source_id,
        s.patient_name,
        COALESCE(r.prontuario, -1) as prontuario,
        COALESCE(r.match_type, 'unmatched') as match_type
    FROM source_extract s
    LEFT JOIN ranked_matches r ON s.source_id = r.source_id AND s.p_words IS NOT DISTINCT FROM r.p_words AND r.rn = 1
    """
    df = con.execute(query).df()
    return df

def main():
    results = []
    
    # 1. Embryoscopes
    embryo_files = ["embryoscope_belo_horizonte.db", "embryoscope_brasilia.db", "embryoscope_ibirapuera.db", "embryoscope_vila_mariana.db"]
    for filename in embryo_files:
        label = filename.replace(".db", "")
        db_path = os.path.join(DB_DIR, filename)
        
        con = duckdb.connect(db_path, read_only=True)
        con.execute(f"ATTACH '{db_path}' AS source_db (READ_ONLY)")
        res_df = run_strategy_c_sql(con, CLINISYS_DB, "silver", "patients", "PatientID", "FirstName")
        con.execute("DETACH source_db")
        con.close()
        
        total = len(res_df)
        matched = len(res_df[res_df["prontuario"] != -1])
        rate = (matched / total * 100) if total > 0 else 0.0
        results.append({"Dataset": label, "Total IDs": total, "Matched": matched, "Match Rate": f"{rate:.2f}%"})
        
    # 2. Redlara Unified
    con = duckdb.connect(DATALAKE_DB, read_only=True)
    con.execute(f"ATTACH '{DATALAKE_DB}' AS source_db (READ_ONLY)")
    res_df = run_strategy_c_sql(con, CLINISYS_DB, "silver", "redlara_unified", "chart_or_pin", name_col=None)
    total = len(res_df)
    matched = len(res_df[res_df["prontuario"] != -1])
    rate = (matched / total * 100) if total > 0 else 0.0
    results.append({"Dataset": "redlara_unified", "Total IDs": total, "Matched": matched, "Match Rate": f"{rate:.2f}%"})
    
    # 3. Mesclada Vendas
    res_df = run_strategy_c_sql(con, CLINISYS_DB, "gold", "protheus_mesclada_vendas", "Paciente", name_col="Nom Paciente")
    con.execute("DETACH source_db")
    con.close()
    
    total = len(res_df)
    matched = len(res_df[res_df["prontuario"] != -1])
    rate = (matched / total * 100) if total > 0 else 0.0
    results.append({"Dataset": "protheus_mesclada_vendas", "Total IDs": total, "Matched": matched, "Match Rate": f"{rate:.2f}%"})
    
    summary_df = pd.DataFrame(results)
    print("\n=== STRATEGY C PURE SQL BENCHMARK RUN SUMMARY ===")
    print(summary_df.to_string(index=False))

if __name__ == "__main__":
    main()
