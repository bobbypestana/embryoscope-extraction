import logging
import os
import duckdb

logger = logging.getLogger(__name__)

def update_prontuario_column(con, db_name):
    """Update prontuario column using PatientID matching logic with clinisys_all.silver.view_pacientes"""
    logger.info(f"[{db_name}] Updating prontuario column using PatientID matching logic...")
    
    try:
        # Attach clinisys_all database
        clinisys_db_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'database', 'clinisys_all.duckdb')
        logger.info(f"[{db_name}] Attaching clinisys_all database from: {clinisys_db_path}")
        con.execute(f"ATTACH '{clinisys_db_path}' AS clinisys_all")
        logger.info(f"[{db_name}] clinisys_all database attached successfully")
        
        # First pass: Active patients only (inativo = 0)
        logger.info(f"[{db_name}] === FIRST PASS: Active patients only (inativo = 0) ===")
        update_prontuario_with_inativo(con, db_name, include_inactive=False)
        
        # Second pass: Inactive patients only (inativo = 1) - only for unmatched records
        logger.info(f"[{db_name}] === SECOND PASS: Inactive patients only (inativo = 1) ===")
        update_prontuario_with_inativo(con, db_name, include_inactive=True)
        
        # Third pass: LastName condition (when LastName doesn't contain date pattern)
        logger.info(f"[{db_name}] === THIRD PASS: LastName condition (when FirstName is in LastName) ===")
        update_prontuario_with_lastname(con, db_name)
        
        # Final quality summary after all passes
        logger.info(f"[{db_name}] === FINAL PRONTUARIO MATCHING QUALITY SUMMARY ===")
        result = con.execute("""
            SELECT 
                COUNT(*) as total_rows,
                COUNT(CASE WHEN prontuario != -1 THEN 1 END) as matched_rows,
                COUNT(CASE WHEN prontuario = -1 THEN 1 END) as unmatched_rows
            FROM silver.patients
        """).fetchone()
        
        if result[0] > 0:
            match_rate = (result[1]/result[0]*100)
            logger.info(f"[{db_name}] Final prontuario matching results:")
            logger.info(f"[{db_name}]   Total rows: {result[0]:,}")
            logger.info(f"[{db_name}]   Matched rows: {result[1]:,}")
            logger.info(f"[{db_name}]   Unmatched rows: {result[2]:,}")
            logger.info(f"[{db_name}]   Match rate: {match_rate:.2f}%")
            
            # Quality assessment
            if match_rate >= 95:
                logger.info(f"[{db_name}]   Quality: EXCELLENT (>=95%)")
            elif match_rate >= 85:
                logger.info(f"[{db_name}]   Quality: GOOD (>=85%)")
            elif match_rate >= 70:
                logger.info(f"[{db_name}]   Quality: ACCEPTABLE (>=70%)")
            else:
                logger.warning(f"[{db_name}]   Quality: NEEDS ATTENTION (<70%)")
        else:
            logger.warning(f"[{db_name}] No rows found in silver.patients table")
        
        logger.info(f"[{db_name}] === END PRONTUARIO MATCHING QUALITY SUMMARY ===")
        
    except Exception as e:
        logger.error(f"[{db_name}] Error updating prontuario column: {e}")
        raise

def update_prontuario_with_inativo(con, db_name, include_inactive=False):
    """Update prontuario column using PatientID matching logic with specific inativo filter"""
    # Determine inactive filter condition
    inactive_condition = "inativo = 1" if include_inactive else "inativo = 0"
    patient_type = "inactive" if include_inactive else "active"
    logger.info(f"[{db_name}] Running PatientID-based matching logic ({patient_type} patients)...")
    
    # Build the SQL query for PatientID matching
    update_sql = f"""
    WITH 
    -- CTE 1: Extract name_first from patients (original logic - FirstName only)
    patient_name_extract AS (
        SELECT 
            "PatientID",
            prontuario,
            "FirstName",
            "LastName",
            CASE 
                -- Use FirstName as normal
                WHEN "FirstName" IS NOT NULL THEN 
                    CASE 
                        -- Handle "LastName, FirstName Middle Names" format (e.g., "VALADARES, FLAVIA.F.N." or "GIANNINI, LIVIA.")
                        WHEN POSITION(',' IN "FirstName") > 0 THEN 
                            -- Extract part after comma, normalize, then extract first sequence of letters only
                            -- Handles: "VALADARES, FLAVIA.F.N." -> "flavia", "GIANNINI, LIVIA." -> "livia"
                            REGEXP_REPLACE(
                                strip_accents(LOWER(TRIM(SPLIT_PART("FirstName", ',', 2)))),
                                '^[^a-z]*([a-z]+).*',
                                '\\1'
                            )
                        -- Handle "FirstName Middle Names" format
                        ELSE 
                            -- Extract first sequence of letters, handling cases with dots (e.g., "FLAVIA.F.N.")
                            REGEXP_REPLACE(
                                strip_accents(LOWER(TRIM("FirstName"))),
                                '^[^a-z]*([a-z]+).*',
                                '\\1'
                            )
                    END
                ELSE NULL 
            END as name_first
        FROM silver.patients
    ),
    
    -- CTE 2: Extract unmatched records using PatientID
    embryoscope_extract AS (
        SELECT DISTINCT 
            "PatientID" as patient_id,
            name_first
        FROM patient_name_extract
        WHERE prontuario = -1 
          AND "PatientID" IS NOT NULL
    ),

    -- CTE 2: Pre-process clinisys data with all transformations and accent normalization
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
            strip_accents(LOWER(SPLIT_PART(TRIM(esposa_nome), ' ', 1))) as esposa_nome,
            strip_accents(LOWER(SPLIT_PART(TRIM(marido_nome), ' ', 1))) as marido_nome,
            unidade_origem
        FROM clinisys_all.silver.view_pacientes
        WHERE {inactive_condition}
    ),

        -- CTE 3: PatientID ↔ prontuario (main/codigo)
        matches_1 AS (
            SELECT d.*,
                   p.codigo as prontuario, p.esposa_nome, p.marido_nome, p.unidade_origem,
                   'patientid_main' as match_type
            FROM embryoscope_extract d
            INNER JOIN clinisys_processed p 
                ON d.patient_id = p.codigo
        ),

        -- CTE 4: PatientID ↔ prontuario_esposa
        matches_2 AS (
            SELECT d.*, 
                   p.codigo as prontuario, p.esposa_nome, p.marido_nome, p.unidade_origem,
                   'patientid_esposa' as match_type
            FROM embryoscope_extract d
            INNER JOIN clinisys_processed p 
                ON d.patient_id = p.prontuario_esposa
        ),

        -- CTE 5: PatientID ↔ prontuario_marido
        matches_3 AS (
            SELECT d.*,
                  p.codigo as prontuario, p.esposa_nome, p.marido_nome, p.unidade_origem,
                   'patientid_marido' as match_type
            FROM embryoscope_extract d
            INNER JOIN clinisys_processed p 
                ON d.patient_id = p.prontuario_marido
        ),

        -- CTE 6: PatientID ↔ prontuario_responsavel1
        matches_4 AS (
            SELECT d.*,
                  p.codigo as prontuario, p.esposa_nome, p.marido_nome, p.unidade_origem,
                   'patientid_responsavel1' as match_type
            FROM embryoscope_extract d
            INNER JOIN clinisys_processed p 
                ON d.patient_id = p.prontuario_responsavel1
        ),

        -- CTE 7: PatientID ↔ prontuario_responsavel2
        matches_5 AS (
            SELECT d.*,
                   p.codigo as prontuario, p.esposa_nome, p.marido_nome, p.unidade_origem,
                   'patientid_responsavel2' as match_type
            FROM embryoscope_extract d
            INNER JOIN clinisys_processed p 
                ON d.patient_id = p.prontuario_responsavel2
        ),

        -- CTE 8: PatientID ↔ prontuario_esposa_pel
        matches_6 AS (
            SELECT d.*,
                   p.codigo as prontuario, p.esposa_nome, p.marido_nome, p.unidade_origem,
                   'patientid_esposa_pel' as match_type
            FROM embryoscope_extract d
            INNER JOIN clinisys_processed p 
                ON d.patient_id = p.prontuario_esposa_pel
        ),

        -- CTE 9: PatientID ↔ prontuario_marido_pel
        matches_7 AS (
            SELECT d.*,
                   p.codigo as prontuario, p.esposa_nome, p.marido_nome, p.unidade_origem,
                   'patientid_marido_pel' as match_type
            FROM embryoscope_extract d
            INNER JOIN clinisys_processed p 
                ON d.patient_id = p.prontuario_marido_pel
        ),

        -- CTE 10: PatientID ↔ prontuario_esposa_pc
        matches_8 AS (
            SELECT d.*,
                   p.codigo as prontuario, p.esposa_nome, p.marido_nome, p.unidade_origem,
                   'patientid_esposa_pc' as match_type
            FROM embryoscope_extract d
            INNER JOIN clinisys_processed p 
                ON d.patient_id = p.prontuario_esposa_pc
        ),

        -- CTE 11: PatientID ↔ prontuario_marido_pc
        matches_9 AS (
            SELECT d.*,
                   p.codigo as prontuario, p.esposa_nome, p.marido_nome, p.unidade_origem,
                   'patientid_marido_pc' as match_type
            FROM embryoscope_extract d
            INNER JOIN clinisys_processed p 
                ON d.patient_id = p.prontuario_marido_pc
        ),

        -- CTE 12: PatientID ↔ prontuario_responsavel1_pc
        matches_10 AS (
            SELECT d.*,
                   p.codigo as prontuario, p.esposa_nome, p.marido_nome, p.unidade_origem,
                   'patientid_responsavel1_pc' as match_type
            FROM embryoscope_extract d
            INNER JOIN clinisys_processed p 
                ON d.patient_id = p.prontuario_responsavel1_pc
        ),

        -- CTE 13: PatientID ↔ prontuario_responsavel2_pc
        matches_11 AS (
            SELECT d.*,
                   p.codigo as prontuario, p.esposa_nome, p.marido_nome, p.unidade_origem,
                   'patientid_responsavel2_pc' as match_type
            FROM embryoscope_extract d
            INNER JOIN clinisys_processed p 
                ON d.patient_id = p.prontuario_responsavel2_pc
        ),

        -- CTE 14: PatientID ↔ prontuario_esposa_fc
        matches_12 AS (
            SELECT d.*,
                   p.codigo as prontuario, p.esposa_nome, p.marido_nome, p.unidade_origem,
                   'patientid_esposa_fc' as match_type
            FROM embryoscope_extract d
            INNER JOIN clinisys_processed p 
                ON d.patient_id = p.prontuario_esposa_fc
        ),

        -- CTE 15: PatientID ↔ prontuario_marido_fc
        matches_13 AS (
            SELECT d.*,
                   p.codigo as prontuario, p.esposa_nome, p.marido_nome, p.unidade_origem,
                   'patientid_marido_fc' as match_type
            FROM embryoscope_extract d
            INNER JOIN clinisys_processed p 
                ON d.patient_id = p.prontuario_marido_fc
        ),

        -- CTE 16: PatientID ↔ prontuario_esposa_ba
        matches_14 AS (
            SELECT d.*,
                   p.codigo as prontuario, p.esposa_nome, p.marido_nome, p.unidade_origem,
                   'patientid_esposa_ba' as match_type
            FROM embryoscope_extract d
            INNER JOIN clinisys_processed p 
                ON d.patient_id = p.prontuario_esposa_ba
        ),

        -- CTE 17: PatientID ↔ prontuario_marido_ba
        matches_15 AS (
            SELECT d.*,
                   p.codigo as prontuario, p.esposa_nome, p.marido_nome, p.unidade_origem,
                   'patientid_marido_ba' as match_type
            FROM embryoscope_extract d
            INNER JOIN clinisys_processed p 
                ON d.patient_id = p.prontuario_marido_ba
        ),

        -- Combine all PatientID based matches
        all_matches AS (
            SELECT * FROM matches_1
            UNION ALL SELECT * FROM matches_2
            UNION ALL SELECT * FROM matches_3
            UNION ALL SELECT * FROM matches_4
            UNION ALL SELECT * FROM matches_5
            UNION ALL SELECT * FROM matches_6
            UNION ALL SELECT * FROM matches_7
            UNION ALL SELECT * FROM matches_8
            UNION ALL SELECT * FROM matches_9
            UNION ALL SELECT * FROM matches_10
            UNION ALL SELECT * FROM matches_11
            UNION ALL SELECT * FROM matches_12
            UNION ALL SELECT * FROM matches_13
            UNION ALL SELECT * FROM matches_14
            UNION ALL SELECT * FROM matches_15
        ),

        -- Filter logic: Name matching validation
        -- Only if FirstName is NOT NULL, check if it appears in esposa_nome or marido_nome
        filtered_matches AS (
            SELECT 
                patient_id,
                prontuario,
                match_type,
                -- Priority logic: patientid_main gets highest priority
                CASE 
                    WHEN match_type = 'patientid_main' THEN 1
                    WHEN match_type IN ('patientid_esposa', 'patientid_marido') THEN 2
                    ELSE 3
                END as priority,
                ROW_NUMBER() OVER (PARTITION BY patient_id ORDER BY 
                    CASE 
                        WHEN match_type = 'patientid_main' THEN 1
                        WHEN match_type IN ('patientid_esposa', 'patientid_marido') THEN 2
                        ELSE 3
                    END,
                    prontuario DESC -- Tie breaker
                ) as rn
            FROM all_matches
            WHERE 
                name_first IS NULL -- If no first name extracted, trust the ID match
                OR (
                    esposa_nome LIKE '%' || name_first || '%' 
                    OR marido_nome LIKE '%' || name_first || '%'
                )
        )

        SELECT 
            patient_id,
            prontuario
        FROM filtered_matches
        WHERE rn = 1
    """
    
    # Execute the update using the CTE
    # We use a temporary table to store the matches first to avoid complex update join issues
    con.execute(f"CREATE OR REPLACE TEMP TABLE matched_prontuarios AS {update_sql}")
    
    matched_count = con.execute("SELECT COUNT(*) FROM matched_prontuarios").fetchone()[0]
    logger.info(f"[{db_name}] Found {matched_count} matches using PatientID logic ({patient_type})")
    
    if matched_count > 0:
        con.execute("""
            UPDATE silver.patients
            SET prontuario = m.prontuario
            FROM matched_prontuarios m
            WHERE silver.patients."PatientID" = m.patient_id
              AND silver.patients.prontuario = -1
        """)
        logger.info(f"[{db_name}] Updated prontuario for matched records")
    else:
        logger.info(f"[{db_name}] No new matches found in this pass")
    
    # Drop temp table
    con.execute("DROP TABLE IF EXISTS matched_prontuarios")

def update_prontuario_with_lastname(con, db_name):
    """
    Update prontuario column using LastName matching logic when FirstName is part of LastName
    e.g. FirstName='LIVIA.', LastName='GIANNINI' -> match against 'LIVIA GIANNINI'
    This handles cases where the normal FirstName extraction fails or name format is unusual
    """
    logger.info(f"[{db_name}] Running LastName-based matching logic...")
    
    # Check if there are still unmatched records
    unmatched_count = con.execute("SELECT COUNT(*) FROM silver.patients WHERE prontuario = -1").fetchone()[0]
    if unmatched_count == 0:
        logger.info(f"[{db_name}] No unmatched records left, skipping LastName matching")
        return

    update_sql = """
    WITH 
    -- CTE 1: Extract records where FirstName logic might have failed but we can use LastName + FirstName
    target_patients AS (
        SELECT 
            "PatientID",
            "FirstName",
            "LastName",
            -- Construct full name for matching
            strip_accents(LOWER(TRIM(REPLACE("FirstName", '.', '') || ' ' || "LastName"))) as full_name_search
        FROM silver.patients
        WHERE prontuario = -1 
          AND "PatientID" IS NOT NULL
          AND "LastName" IS NOT NULL
          -- Only process if FirstName looks like it might be part of a full name structure not handled
          AND (POSITION('.' IN "FirstName") > 0 OR LENGTH("FirstName") < 4)
    ),
    
    -- CTE 2: Clinisys patients (active only for safety)
    clinisys_active AS (
        SELECT 
            codigo as prontuario,
            strip_accents(LOWER(esposa_nome)) as esposa_nome_full,
            strip_accents(LOWER(marido_nome)) as marido_nome_full
        FROM clinisys_all.silver.view_pacientes
        WHERE inativo = 0
    ),
    
    -- CTE 3: Match based on name containment
    matches AS (
        SELECT 
            t."PatientID" as patient_id,
            c.prontuario,
            ROW_NUMBER() OVER (PARTITION BY t."PatientID" ORDER BY c.prontuario DESC) as rn
        FROM target_patients t
        INNER JOIN clinisys_active c
            ON (c.esposa_nome_full LIKE '%' || t.full_name_search || '%' 
                OR c.marido_nome_full LIKE '%' || t.full_name_search || '%')
    )
    
    SELECT patient_id, prontuario 
    FROM matches 
    WHERE rn = 1
    """
    
    # Execute the update
    con.execute(f"CREATE OR REPLACE TEMP TABLE matched_lastname_prontuarios AS {update_sql}")
    
    matched_count = con.execute("SELECT COUNT(*) FROM matched_lastname_prontuarios").fetchone()[0]
    logger.info(f"[{db_name}] Found {matched_count} matches using LastName logic")
    
    if matched_count > 0:
        con.execute("""
            UPDATE silver.patients
            SET prontuario = m.prontuario
            FROM matched_lastname_prontuarios m
            WHERE silver.patients."PatientID" = m.patient_id
              AND silver.patients.prontuario = -1
        """)
        logger.info(f"[{db_name}] Updated prontuario for matched records")
    
    # Drop temp table
    con.execute("DROP TABLE IF EXISTS matched_lastname_prontuarios")
