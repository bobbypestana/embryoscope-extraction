"""
patient_matching_strategy_c.py

Generic patient prontuario matching utility based on Strategy C (hybrid priority & name match strength scoring).
Enriches a given table in-place with `prontuario` and `clinisys_name` columns.
Only matches and updates rows where `prontuario` is NULL or -1 (preserving existing matches).
"""

import os
import logging
import duckdb

logger = logging.getLogger(__name__)

def add_columns_if_missing(con, source_schema: str, source_table: str) -> bool:
    """
    Checks if `prontuario` and `clinisys_name` columns exist in the target table.
    Adds them if missing.
    Returns True if `prontuario` already existed in the table.
    """
    columns = con.execute(f"PRAGMA table_info('{source_schema}.{source_table}')").df()
    col_names = [c.lower() for c in columns['name'].tolist()]
    
    prontuario_existed = 'prontuario' in col_names
    clinisys_name_existed = 'clinisys_name' in col_names
    
    if not prontuario_existed:
        logger.info(f"Adding 'prontuario' column to {source_schema}.{source_table}")
        con.execute(f"ALTER TABLE {source_schema}.{source_table} ADD COLUMN prontuario BIGINT")
        con.execute(f"UPDATE {source_schema}.{source_table} SET prontuario = -1")
    else:
        logger.info(f"'prontuario' column already exists in {source_schema}.{source_table}")
        
    if not clinisys_name_existed:
        logger.info(f"Adding 'clinisys_name' column to {source_schema}.{source_table}")
        con.execute(f"ALTER TABLE {source_schema}.{source_table} ADD COLUMN clinisys_name VARCHAR")
    else:
        logger.info(f"'clinisys_name' column already exists in {source_schema}.{source_table}")
        
    return prontuario_existed

def match_patients_generic(
    source_con,
    db_path: str,
    source_schema: str,
    source_table: str,
    id_col: str,
    name_col: str = None,
    label: str = "",
):
    """
    Runs Strategy C patient matching against a given table.
    Updates the target table in-place with matched `prontuario` and `clinisys_name` columns.
    Only matches and updates records where `prontuario` is NULL or -1.
    """
    tag = f"[StrategyC][{label or f'{source_schema}.{source_table}'}]"
    logger.info(f"{tag} Starting generic matching")
    
    # 1. Detect engine type
    is_duckdb = "duckdb" in type(source_con).__name__.lower()
    
    if is_duckdb:
        # Attach clinisys DB
        try:
            source_con.execute(f"ATTACH '{db_path}' AS clinisys_all (READ_ONLY)")
            logger.info(f"{tag} Attached clinisys_all database")
        except Exception as e:
            if "already attached" in str(e).lower():
                logger.info(f"{tag} clinisys_all already attached")
            else:
                raise
        clinisys_table = "clinisys_all.silver.view_pacientes"
    else:
        # For Athena, use the specified path directly without ATTACH
        clinisys_table = "clinisys_all.silver_view_pacientes"
        
    # 2. Add columns if missing (only for DuckDB; Athena schemas are usually managed via DDL)
    if is_duckdb:
        prontuario_existed = add_columns_if_missing(source_con, source_schema, source_table)
    else:
        prontuario_existed = True  # In production, columns are assumed to be predefined in schema
        
    # 3. Build dynamic SQL components
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
        
        # Determine best matching entity name
        matched_name_1 = f"""
        CASE
            WHEN {score_sql('c.esposa_nome')} <= {score_sql('c.marido_nome')} AND {score_sql('c.esposa_nome')} <= {score_sql('c.responsavel_nome')} THEN c.esposa_nome
            WHEN {score_sql('c.marido_nome')} <= {score_sql('c.esposa_nome')} AND {score_sql('c.marido_nome')} <= {score_sql('c.responsavel_nome')} THEN c.marido_nome
            ELSE c.responsavel_nome
        END
        """
    else:
        score_sql = lambda col: "1"
        matched_name_1 = "COALESCE(c.esposa_nome, c.marido_nome, c.responsavel_nome)"
        
    name_select = f's."{name_col}"' if name_col else "CAST(NULL AS VARCHAR)"
    
    # 4. Generate Strategy C query
    matching_query = f"""
    WITH
    source_extract AS (
        SELECT DISTINCT
            CAST(s."{id_col}" AS VARCHAR) as source_id,
            {name_select} as patient_name,
            {p_words_sql} as p_words,
            s.prontuario as original_prontuario
        FROM {source_schema}.{source_table} s
        WHERE s."{id_col}" IS NOT NULL AND CAST(s."{id_col}" AS VARCHAR) != ''
          AND (s.prontuario IS NULL OR s.prontuario = -1)
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
        FROM {clinisys_table}
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
            'codigo_main' as match_type,
            LEAST({score_sql('c.esposa_nome')}, {score_sql('c.marido_nome')}, {score_sql('c.responsavel_nome')}) as name_score,
            {matched_name_1} as matched_name
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
            'esposa_match' as match_type,
            {score_sql('c.esposa_nome')} as name_score,
            c.esposa_nome as matched_name
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
            'marido_match' as match_type,
            {score_sql('c.marido_nome')} as name_score,
            c.marido_nome as matched_name
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
            'responsavel_match' as match_type,
            {score_sql('c.responsavel_nome')} as name_score,
            c.responsavel_nome as matched_name
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
            matched_name,
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
        s.p_words,
        r.prontuario,
        r.matched_name
    FROM source_extract s
    INNER JOIN ranked_matches r ON s.source_id = r.source_id AND s.p_words IS NOT DISTINCT FROM r.p_words AND r.rn = 1
    """
    
    # 5. Run update query
    # Register matching view or execute update
    logger.info(f"{tag} Executing in-place update...")
    
    update_sql = f"""
    UPDATE {source_schema}.{source_table} AS target
    SET 
        prontuario = match_results.prontuario,
        clinisys_name = match_results.matched_name
    FROM ({matching_query}) AS match_results
    WHERE CAST(target."{id_col}" AS VARCHAR) = match_results.source_id
      AND (target.prontuario IS NULL OR target.prontuario = -1)
    """
    
    # If using DuckDB, we can also extract words for matching name filtering
    # Since we filter on (target.prontuario IS NULL OR target.prontuario = -1), we only update unmatched records.
    source_con.execute(update_sql)
    
    # 6. Logging stats
    if is_duckdb:
        stats = source_con.execute(f"""
            SELECT 
                COUNT(*) as total_rows,
                COUNT(CASE WHEN prontuario != -1 AND prontuario IS NOT NULL THEN 1 END) as matched_rows,
                COUNT(CASE WHEN prontuario = -1 OR prontuario IS NULL THEN 1 END) as unmatched_rows
            FROM {source_schema}.{source_table}
        """).fetchone()
        
        total = stats[0]
        matched = stats[1]
        unmatched = stats[2]
        rate = (matched / total * 100) if total > 0 else 0.0
        logger.info(f"{tag} Done. Stats - Total: {total:,} | Matched: {matched:,} | Unmatched: {unmatched:,} | Rate: {rate:.2f}%")
        
def main():
    import traceback
    import sys
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s', stream=sys.stdout)
    try:
        # Simple benchmark validation run
        DB_DIR = r"g:\My Drive\projetos_individuais\Huntington\database"
        CLINISYS_DB = os.path.join(DB_DIR, "clinisys_all.duckdb")
        DATALAKE_DB = os.path.join(DB_DIR, "huntington_data_lake.duckdb")
        
        if not os.path.exists(CLINISYS_DB):
            logger.error(f"Clinisys database not found at {CLINISYS_DB}. Skipping main testing block.")
            return
            
        # We will test on a temporary duplicate table of one of the databases to avoid modifying actual tables
        logger.info("Setting up a test database to verify the generic in-place matching engine...")
        con = duckdb.connect() # In-memory db
        
        # Attach actual databases to import data
        con.execute(f"ATTACH '{DATALAKE_DB}' AS source_db (READ_ONLY)")
        
        # Create copy of protheus_mesclada_vendas in our main memory db (under 'main' schema)
        logger.info("Copying mesclada_vendas table to in-memory DB...")
        con.execute("CREATE TABLE main.test_vendas AS SELECT * FROM source_db.gold.protheus_mesclada_vendas")
        
        # Detach source_db as we don't want to write to it
        con.execute("DETACH source_db")
        
        # Verify the table schema
        cols = con.execute("PRAGMA table_info('main.test_vendas')").df()
        logger.info(f"Original test table column names: {cols['name'].tolist()}")
        
        # Reset prontuario to -1 or drop it to test adding column
        # Let's drop it to test full addition
        con.execute("ALTER TABLE main.test_vendas DROP COLUMN prontuario")
        
        # Run generic strategy C matcher!
        logger.info("Running match_patients_generic on the test table...")
        match_patients_generic(
            source_con=con,
            db_path=CLINISYS_DB,
            source_schema="main",
            source_table="test_vendas",
            id_col="Paciente",
            name_col="Nom Paciente",
            label="Test Mesclada Vendas"
        )
        
        # Query sample updated rows to check matched names
        sample = con.execute("""
            SELECT "Paciente", "Nom Paciente", prontuario, clinisys_name 
            FROM main.test_vendas 
            WHERE prontuario != -1 
            LIMIT 10
        """).df()
        logger.info("Sample matched rows from updated table:")
        print(sample.to_string(index=False))
        
        con.close()
    except Exception as e:
        logger.error("An error occurred during verification:")
        traceback.print_exc(file=sys.stdout)
        raise e

if __name__ == "__main__":
    main()
