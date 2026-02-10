import duckdb
import os

db_path = r"g:\My Drive\projetos_individuais\Huntington\database\huntington_data_lake.duckdb"
conn = duckdb.connect(db_path)

limit = 5
query = '''
    WITH RankedEmbryos AS (
        SELECT 
            dp."Slide ID" as embryo_id,
            ee.patient_unit_huntington as location,
            ee.patient_PatientID as prontuario,
            ee.embryo_EmbryoDescriptionID as embryo_description_id,
            ROW_NUMBER() OVER (PARTITION BY ee.patient_PatientID ORDER BY dp."Slide ID") as rn
        FROM gold.data_ploidia dp
        LEFT JOIN gold.embryoscope_embrioes ee 
            ON dp."Slide ID" = ee.embryo_EmbryoID
        LEFT JOIN gold.embryo_images_metadata eim
            ON dp."Slide ID" = eim.embryo_id
        WHERE dp."BMI" IS NOT NULL
          AND (eim.status IS NULL OR eim.status != 'success')
    )
    SELECT 
        embryo_id,
        location,
        prontuario,
        embryo_description_id
    FROM RankedEmbryos
    WHERE rn = 1
    ORDER BY prontuario DESC
    LIMIT ?
'''

print("--- Running Full Query ---")
df = conn.execute(query, [limit]).df()
print(f"Result count: {len(df)}")
if len(df) > 0:
    print(df)
else:
    print("Zero results found.")
    
    print("\nChecking why...")
    print("Counting with BMI NOT NULL and status != success:")
    count_q = '''
        SELECT COUNT(*)
        FROM gold.data_ploidia dp
        LEFT JOIN gold.embryo_images_metadata eim ON dp."Slide ID" = eim.embryo_id
        WHERE dp."BMI" IS NOT NULL AND (eim.status IS NULL OR eim.status != 'success')
    '''
    print(f"Total potential candidates: {conn.execute(count_q).fetchone()[0]}")
    
    print("\nChecking for NULL prontuario in those candidates:")
    count_null_p = '''
        SELECT COUNT(*)
        FROM gold.data_ploidia dp
        LEFT JOIN gold.embryoscope_embrioes ee ON dp."Slide ID" = ee.embryo_EmbryoID
        WHERE dp."BMI" IS NOT NULL AND ee.patient_PatientID IS NULL
    '''
    print(f"Candidates with NULL prontuario: {conn.execute(count_null_p).fetchone()[0]}")

conn.close()
