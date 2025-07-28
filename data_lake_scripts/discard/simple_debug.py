import duckdb

# Connect to database
db_path = "../database/huntington_data_lake.duckdb"
conn = duckdb.connect(db_path)

print("SIMPLE DEBUG - EMBRYO_NUMBER ISSUE")
print("=" * 50)

# Check silver layer for patient 874649
print("1. Silver layer data for patient 874649:")
query = """
SELECT 
    PatientIDx,
    TreatmentName,
    EmbryoDescriptionID,
    embryo_number
FROM silver_embryoscope.embryo_data
WHERE PatientIDx LIKE '%874649%'
ORDER BY TreatmentName, EmbryoDescriptionID
LIMIT 10
"""

result = conn.execute(query).df()
print(result.to_string(index=False))

# Check if there are multiple patients with same treatment
print("\n2. Multiple patients with same treatment:")
query2 = """
SELECT 
    TreatmentName,
    COUNT(DISTINCT PatientIDx) as unique_patients,
    COUNT(*) as total_embryos
FROM silver_embryoscope.embryo_data
WHERE TreatmentName = '2025 - 1233'
GROUP BY TreatmentName
"""

result2 = conn.execute(query2).df()
print(result2.to_string(index=False))

# Check all patients with this treatment
print("\n3. All patients with treatment '2025 - 1233':")
query3 = """
SELECT 
    PatientIDx,
    TreatmentName,
    COUNT(*) as embryo_count,
    MIN(embryo_number) as min_num,
    MAX(embryo_number) as max_num
FROM silver_embryoscope.embryo_data
WHERE TreatmentName = '2025 - 1233'
GROUP BY PatientIDx, TreatmentName
ORDER BY PatientIDx
"""

result3 = conn.execute(query3).df()
print(result3.to_string(index=False))

conn.close() 