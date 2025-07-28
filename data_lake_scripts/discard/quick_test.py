import duckdb

print("Quick test - embryo_number issue")
print("=" * 40)

try:
    conn = duckdb.connect("../database/huntington_data_lake.duckdb")
    
    # Check silver layer for patient 874649
    result = conn.execute("""
        SELECT PatientIDx, TreatmentName, EmbryoDescriptionID, embryo_number
        FROM silver_embryoscope.embryo_data
        WHERE PatientIDx LIKE '%874649%'
        ORDER BY TreatmentName, EmbryoDescriptionID
        LIMIT 5
    """).df()
    
    print("Silver layer data for patient 874649:")
    print(result.to_string(index=False))
    
    # Check if multiple patients have same treatment
    result2 = conn.execute("""
        SELECT COUNT(DISTINCT PatientIDx) as unique_patients
        FROM silver_embryoscope.embryo_data
        WHERE TreatmentName = '2025 - 1233'
    """).df()
    
    print(f"\nUnique patients with treatment '2025 - 1233': {result2.iloc[0,0]}")
    
    conn.close()
    
except Exception as e:
    print(f"Error: {e}") 