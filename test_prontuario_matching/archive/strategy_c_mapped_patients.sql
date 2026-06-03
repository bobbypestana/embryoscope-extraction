-- Strategy C adapted for test_mapped_patients.duckdb / mapped_patients table
-- Column mapping vs original strategy_c.sql:
--   source_db.silver.patients  -> main.mapped_patients
--   s.PatientID                -> s.id
--   s.FirstName                -> s.name
--   s.prontuario               -> s.prontuario_old
--   silver.view_pacientes      -> clinisys_all.silver.view_pacientes
--
-- Run from Python:
--   con.execute("ATTACH 'database/clinisys_all.duckdb' AS clinisys_all (READ_ONLY)")
--   con.execute("ATTACH 'database/test_mapped_patients.duckdb' AS src")
--   result = con.execute(open('test_prontuario_matching/strategy_c_mapped_patients.sql').read()).df()

-- Step 1: Pre-process and normalize source patient names
WITH source_extract AS (
    SELECT DISTINCT
        CAST(s.id AS VARCHAR) as source_id,
        s.name as patient_name,
        s.prontuario_old as original_prontuario,
        -- Parse name: reverse "LASTNAME, FIRSTNAME" format to "FIRSTNAME LASTNAME"
        CASE
            WHEN s.name IS NOT NULL THEN
                CASE
                    WHEN POSITION(',' IN s.name) > 0 THEN
                        SPLIT_PART(s.name, ',', 2) || ' ' || SPLIT_PART(s.name, ',', 1)
                    ELSE s.name
                END
            ELSE NULL
        END as parsed_name_raw,
        -- Split name into a normalized array of words (lowercase, accents stripped, letters only)
        list_filter(
            regexp_split_to_array(
                regexp_replace(strip_accents(lower(trim(
                    CASE
                        WHEN s.name IS NOT NULL THEN
                            CASE
                                WHEN POSITION(',' IN s.name) > 0 THEN
                                    SPLIT_PART(s.name, ',', 2) || ' ' || SPLIT_PART(s.name, ',', 1)
                                ELSE s.name
                            END
                        ELSE NULL
                    END
                ))), '[^a-z]', ' ', 'g'),
                '\s+'
            ),
            w -> length(w) > 0
        ) as p_words
    FROM main.mapped_patients s
    WHERE s.id IS NOT NULL AND CAST(s.id AS VARCHAR) != ''
),

-- Step 2: Query clinisys view
clinisys_processed AS (
    SELECT
        codigo,
        inativo,
        esposa_nome,
        marido_nome,
        responsavel_nome,
        -- Normalized lists of words for Clinisys names
        list_filter(regexp_split_to_array(regexp_replace(strip_accents(lower(trim(esposa_nome))), '[^a-z]', ' ', 'g'), '\s+'), w -> length(w) > 0) as esposa_words,
        list_filter(regexp_split_to_array(regexp_replace(strip_accents(lower(trim(marido_nome))), '[^a-z]', ' ', 'g'), '\s+'), w -> length(w) > 0) as marido_words,
        list_filter(regexp_split_to_array(regexp_replace(strip_accents(lower(trim(responsavel_nome))), '[^a-z]', ' ', 'g'), '\s+'), w -> length(w) > 0) as responsavel_words,
        -- List of all linked prontuarios
        prontuario_esposa, prontuario_esposa_pel, prontuario_esposa_pc, prontuario_esposa_fc, prontuario_esposa_ba,
        prontuario_marido, prontuario_marido_pel, prontuario_marido_pc, prontuario_marido_fc, prontuario_marido_ba,
        prontuario_responsavel1, prontuario_responsavel2, prontuario_responsavel1_pc, prontuario_responsavel2_pc
    FROM clinisys_all.silver.view_pacientes
),

-- Step 3: Match priority Group 1 (Main Codigo)
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
        c.responsavel_nome,
        'codigo_main' as match_type,
        LEAST(
            CASE
                WHEN s.p_words IS NULL OR c.esposa_words IS NULL OR len(s.p_words) = 0 OR len(c.esposa_words) = 0 THEN 5
                WHEN s.p_words = c.esposa_words THEN 1
                WHEN NOT list_contains(c.esposa_words, s.p_words[1]) THEN 5
                WHEN list_contains(c.esposa_words, s.p_words[-1]) THEN 2
                WHEN len(list_filter(s.p_words, w -> list_contains(c.esposa_words, w))) > 1 THEN 3
                ELSE 4
            END,
            CASE
                WHEN s.p_words IS NULL OR c.marido_words IS NULL OR len(s.p_words) = 0 OR len(c.marido_words) = 0 THEN 5
                WHEN s.p_words = c.marido_words THEN 1
                WHEN NOT list_contains(c.marido_words, s.p_words[1]) THEN 5
                WHEN list_contains(c.marido_words, s.p_words[-1]) THEN 2
                WHEN len(list_filter(s.p_words, w -> list_contains(c.marido_words, w))) > 1 THEN 3
                ELSE 4
            END,
            CASE
                WHEN s.p_words IS NULL OR c.responsavel_words IS NULL OR len(s.p_words) = 0 OR len(c.responsavel_words) = 0 THEN 5
                WHEN s.p_words = c.responsavel_words THEN 1
                WHEN NOT list_contains(c.responsavel_words, s.p_words[1]) THEN 5
                WHEN list_contains(c.responsavel_words, s.p_words[-1]) THEN 2
                WHEN len(list_filter(s.p_words, w -> list_contains(c.responsavel_words, w))) > 1 THEN 3
                ELSE 4
            END
        ) as name_score
    FROM source_extract s
    INNER JOIN clinisys_processed c ON TRY_CAST(s.source_id AS BIGINT) = c.codigo
),

-- Step 4: Match priority Group 2 (Esposa columns)
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
        c.responsavel_nome,
        'esposa_match' as match_type,
        CASE
            WHEN s.p_words IS NULL OR c.esposa_words IS NULL OR len(s.p_words) = 0 OR len(c.esposa_words) = 0 THEN 5
            WHEN s.p_words = c.esposa_words THEN 1
            WHEN NOT list_contains(c.esposa_words, s.p_words[1]) THEN 5
            WHEN list_contains(c.esposa_words, s.p_words[-1]) THEN 2
            WHEN len(list_filter(s.p_words, w -> list_contains(c.esposa_words, w))) > 1 THEN 3
            ELSE 4
        END as name_score
    FROM source_extract s
    INNER JOIN clinisys_processed c ON
        TRY_CAST(s.source_id AS BIGINT) IN (
            c.prontuario_esposa, c.prontuario_esposa_pel, c.prontuario_esposa_pc,
            c.prontuario_esposa_fc, c.prontuario_esposa_ba
        )
),

-- Step 5: Match priority Group 3 (Marido columns)
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
        c.responsavel_nome,
        'marido_match' as match_type,
        CASE
            WHEN s.p_words IS NULL OR c.marido_words IS NULL OR len(s.p_words) = 0 OR len(c.marido_words) = 0 THEN 5
            WHEN s.p_words = c.marido_words THEN 1
            WHEN NOT list_contains(c.marido_words, s.p_words[1]) THEN 5
            WHEN list_contains(c.marido_words, s.p_words[-1]) THEN 2
            WHEN len(list_filter(s.p_words, w -> list_contains(c.marido_words, w))) > 1 THEN 3
            ELSE 4
        END as name_score
    FROM source_extract s
    INNER JOIN clinisys_processed c ON
        TRY_CAST(s.source_id AS BIGINT) IN (
            c.prontuario_marido, c.prontuario_marido_pel, c.prontuario_marido_pc,
            c.prontuario_marido_fc, c.prontuario_marido_ba
        )
),

-- Step 6: Match priority Group 4 (Responsavel columns)
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
        c.responsavel_nome,
        'responsavel_match' as match_type,
        CASE
            WHEN s.p_words IS NULL OR c.responsavel_words IS NULL OR len(s.p_words) = 0 OR len(c.responsavel_words) = 0 THEN 5
            WHEN s.p_words = c.responsavel_words THEN 1
            WHEN NOT list_contains(c.responsavel_words, s.p_words[1]) THEN 5
            WHEN list_contains(c.responsavel_words, s.p_words[-1]) THEN 2
            WHEN len(list_filter(s.p_words, w -> list_contains(c.responsavel_words, w))) > 1 THEN 3
            ELSE 4
        END as name_score
    FROM source_extract s
    INNER JOIN clinisys_processed c ON
        TRY_CAST(s.source_id AS BIGINT) IN (
            c.prontuario_responsavel1, c.prontuario_responsavel2,
            c.prontuario_responsavel1_pc, c.prontuario_responsavel2_pc
        )
),

-- Step 7: Combine and apply sorting/tie-breaker hierarchy
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

-- Step 8: Left join back to source_extract to keep all rows (unmatched -> -1)
SELECT
    s.source_id,
    s.patient_name,
    COALESCE(r.prontuario, -1) as prontuario,
    COALESCE(r.match_type, 'unmatched') as match_type
FROM source_extract s
LEFT JOIN ranked_matches r
       ON s.source_id = r.source_id
      AND s.p_words IS NOT DISTINCT FROM r.p_words
      AND r.rn = 1;
