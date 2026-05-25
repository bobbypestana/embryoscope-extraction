-- ==============================================================================
-- ATHENA SILVER LAYER DDL & TRANSFORMATION QUERIES WITH LOCAL FILTERING LOGIC
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- 1. PATIENTS TRANSFORMATION (Silver)
-- Replicates:
--   - Deduplication: Keep the latest record for each PatientIDx based on extraction timestamp
--   - Patient ID cleaning: Keep only numeric/valid PatientIDs, discarding nulls, zero, and strings
-- ------------------------------------------------------------------------------

CREATE OR REPLACE VIEW silver_embryoscope_staging.patients_filtered AS
WITH deduplicated_patients AS (
    SELECT 
        raw_json,
        _extraction_timestamp,
        _location,
        _run_id,
        _row_hash,
        -- Flatten the key columns needed for filtering and sorting
        json_extract_scalar(raw_json, '$.PatientIDx') as patient_id_x,
        json_extract_scalar(raw_json, '$.PatientID') as patient_id,
        json_extract_scalar(raw_json, '$.FirstName') as first_name,
        json_extract_scalar(raw_json, '$.LastName') as last_name,
        json_extract_scalar(raw_json, '$.DateOfBirth') as date_of_birth,
        ROW_NUMBER() OVER (
            PARTITION BY json_extract_scalar(raw_json, '$.PatientIDx') 
            ORDER BY _extraction_timestamp DESC
        ) as rn
    FROM bronze_embryoscope_staging.patients
)
SELECT 
    patient_id_x,
    -- Apply numeric cleaning rules:
    -- Remove '.' (dots), verify digit-only structure, cast to BIGINT, ensure it is non-zero
    CAST(REPLACE(patient_id, '.', '') AS BIGINT) as patient_id,
    first_name,
    last_name,
    date_of_birth,
    _extraction_timestamp,
    _location as source_server,
    _run_id,
    _row_hash
FROM deduplicated_patients
WHERE rn = 1
  AND patient_id IS NOT NULL 
  -- Remove '.' and verify only digits exist in the cleaned patient_id
  AND regexp_like(REPLACE(patient_id, '.', ''), '^\d+$')
  -- Cast and ensure it's not a zero record
  AND CAST(REPLACE(patient_id, '.', '') AS BIGINT) > 0
  -- Exclude common test/administrative records
  AND LOWER(first_name) NOT LIKE 'overnight%'
  AND LOWER(patient_id) NOT LIKE '%test%';


-- ------------------------------------------------------------------------------
-- 2. TREATMENTS TRANSFORMATION (Silver)
-- Replicates:
--   - Deduplication: Keep the latest record for each (PatientIDx, TreatmentName)
--   - Cascading Delete: Keep only treatments belonging to valid, cleaned patients
-- ------------------------------------------------------------------------------

CREATE OR REPLACE VIEW silver_embryoscope_staging.treatments_filtered AS
WITH deduplicated_treatments AS (
    SELECT 
        _extraction_timestamp,
        _location,
        _run_id,
        _row_hash,
        -- Flatten columns
        json_extract_scalar(raw_json, '$.PatientIDx') as patient_id_x,
        json_extract_scalar(raw_json, '$.TreatmentName') as treatment_name,
        ROW_NUMBER() OVER (
            PARTITION BY json_extract_scalar(raw_json, '$.PatientIDx'), json_extract_scalar(raw_json, '$.TreatmentName') 
            ORDER BY _extraction_timestamp DESC
        ) as rn
    FROM bronze_embryoscope_staging.treatments
)
SELECT 
    patient_id_x,
    treatment_name,
    _extraction_timestamp,
    _location as source_server,
    _run_id,
    _row_hash
FROM deduplicated_treatments
WHERE rn = 1
  -- Cascading Filter: Only keep treatments associated with valid patients
  AND patient_id_x IN (SELECT patient_id_x FROM silver_embryoscope_staging.patients_filtered);


-- ------------------------------------------------------------------------------
-- 3. EMBRYO DATA TRANSFORMATION (Silver)
-- Replicates:
--   - Deduplication: Keep the latest record for each EmbryoID
--   - Cascading Delete: Keep only embryos belonging to valid patients
--   - Morphological Null Filtering: Remove embryos where all annotation/morphokinetic columns are NULL
-- ------------------------------------------------------------------------------

CREATE OR REPLACE VIEW silver_embryoscope_staging.embryo_data_filtered AS
WITH deduplicated_embryos AS (
    SELECT 
        _extraction_timestamp,
        _location,
        _run_id,
        _row_hash,
        -- Business & Core Identification Keys
        json_extract_scalar(raw_json, '$.EmbryoID') as embryo_id,
        json_extract_scalar(raw_json, '$.PatientIDx') as patient_id_x,
        json_extract_scalar(raw_json, '$.TreatmentName') as treatment_name,
        json_extract_scalar(raw_json, '$.EmbryoDetails.EmbryoFate') as embryo_fate,
        json_extract_scalar(raw_json, '$.EmbryoDetails.WellNumber') as well_number,
        json_extract_scalar(raw_json, '$.Evaluation.Evaluation') as kid_score,
        json_extract_scalar(raw_json, '$.Evaluation.EvaluationDate') as kid_date,
        
        -- Sample of Morphokinetics columns (extend as needed for other annotation lists)
        -- Since the annotation list is JSON arrays, map out the key annotation values
        -- Here we extract from the flattened table structure or JSON arrays
        -- (This acts as the mock data check for empty annotations)
        -- Assuming silver table has these columns:
        time_t2,
        time_t3,
        time_t4,
        time_t5,
        time_t8,
        time_t_blast_expand_last,
        value_morphological_grade,
        
        ROW_NUMBER() OVER (
            PARTITION BY json_extract_scalar(raw_json, '$.EmbryoID') 
            ORDER BY _extraction_timestamp DESC
        ) as rn
    FROM bronze_embryoscope_staging.embryo_data
)
SELECT 
    *
FROM deduplicated_embryos
WHERE rn = 1
  -- Cascading Filter: Only keep embryos associated with valid patients
  AND patient_id_x IN (SELECT patient_id_x FROM silver_embryoscope_staging.patients_filtered)
  
  -- Empty Annotations Filter:
  -- Drop records where all morphological/data columns are NULL (Keep only those with at least one non-null data point)
  AND COALESCE(
      time_t2, time_t3, time_t4, time_t5, time_t8, 
      time_t_blast_expand_last, value_morphological_grade, kid_score
  ) IS NOT NULL;
