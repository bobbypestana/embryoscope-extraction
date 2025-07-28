import duckdb

# Connect to database
db_path = "../database/huntington_data_lake.duckdb"
conn = duckdb.connect(db_path)

print("CHECKING EMBRYO_NUMBER FOR PATIENT 874649")
print("=" * 80)

# Check embryoscope data for patient 874649
print("1. EMBRYOSCOPE DATA FOR PATIENT 874649:")
print("-" * 50)

query_embryoscope = """
SELECT 
    e.patient_PatientIDx,
    e.patient_PatientID,
    e.treatment_TreatmentName,
    e.embryo_EmbryoID,
    e.embryo_EmbryoDescriptionID,
    e.embryo_embryo_number,
    e.embryo_FertilizationTime,
    e.embryo_EmbryoFate,
    e.embryo_WellNumber
FROM gold.embryoscope_embrioes e
WHERE e.patient_PatientID = 874649
ORDER BY e.treatment_TreatmentName, e.embryo_embryo_number
"""

result_embryoscope = conn.execute(query_embryoscope).df()

if result_embryoscope.empty:
    print("❌ No data found for patient 874649 in embryoscope gold layer")
else:
    print(f"Found {len(result_embryoscope)} records for patient 874649")
    print("\nEmbryoscope data:")
    print(result_embryoscope.to_string(index=False))
    
    # Check if embryo_number is sequential per treatment
    treatments = result_embryoscope['treatment_TreatmentName'].unique()
    print(f"\n2. EMBRYO_NUMBER ANALYSIS BY TREATMENT:")
    print("-" * 50)
    
    for treatment in treatments:
        treatment_data = result_embryoscope[result_embryoscope['treatment_TreatmentName'] == treatment]
        embryo_nums = treatment_data['embryo_embryo_number'].dropna().tolist()
        expected_range = list(range(1, len(embryo_nums) + 1))
        
        print(f"\nTreatment: {treatment}")
        print(f"  - Embryo numbers: {embryo_nums}")
        print(f"  - Expected sequential: {expected_range}")
        
        if embryo_nums == expected_range:
            print(f"  ✅ Sequential")
        else:
            print(f"  ❌ Non-sequential")

print("\n" + "=" * 80)

# Check silver layer data for comparison
print("3. SILVER LAYER DATA FOR PATIENT 874649:")
print("-" * 50)

query_silver = """
SELECT 
    PatientIDx,
    TreatmentName,
    EmbryoID,
    EmbryoDescriptionID,
    embryo_number,
    FertilizationTime,
    EmbryoFate,
    WellNumber
FROM silver_embryoscope.embryo_data
WHERE PatientIDx LIKE '%874649%'
ORDER BY TreatmentName, embryo_number
"""

result_silver = conn.execute(query_silver).df()

if result_silver.empty:
    print("❌ No data found for patient 874649 in embryoscope silver layer")
else:
    print(f"Found {len(result_silver)} records for patient 874649 in silver layer")
    print("\nSilver layer data:")
    print(result_silver.to_string(index=False))
    
    # Check if embryo_number is sequential per treatment in silver
    treatments_silver = result_silver['TreatmentName'].unique()
    print(f"\n4. SILVER LAYER EMBRYO_NUMBER ANALYSIS BY TREATMENT:")
    print("-" * 50)
    
    for treatment in treatments_silver:
        treatment_data = result_silver[result_silver['TreatmentName'] == treatment]
        embryo_nums = treatment_data['embryo_number'].dropna().tolist()
        expected_range = list(range(1, len(embryo_nums) + 1))
        
        print(f"\nTreatment: {treatment}")
        print(f"  - Embryo numbers: {embryo_nums}")
        print(f"  - Expected sequential: {expected_range}")
        
        if embryo_nums == expected_range:
            print(f"  ✅ Sequential")
        else:
            print(f"  ❌ Non-sequential")

print("\n" + "=" * 80)

# Check if there are multiple patients with the same TreatmentName
print("5. CHECKING FOR MULTIPLE PATIENTS WITH SAME TREATMENT NAME:")
print("-" * 50)

if not result_silver.empty:
    treatments_to_check = result_silver['TreatmentName'].unique()
    
    for treatment in treatments_to_check:
        query_multiple = f"""
        SELECT 
            PatientID,
            TreatmentName,
            COUNT(*) as embryo_count,
            MIN(embryo_number) as min_embryo_num,
            MAX(embryo_number) as max_embryo_num
        FROM silver_embryoscope.embryo_data
        WHERE TreatmentName = '{treatment}'
        GROUP BY PatientID, TreatmentName
        ORDER BY PatientID
        """
        
        multiple_result = conn.execute(query_multiple).df()
        
        if len(multiple_result) > 1:
            print(f"\nTreatment '{treatment}' has multiple patients:")
            print(multiple_result.to_string(index=False))
        else:
            print(f"\nTreatment '{treatment}' has only one patient")

print("\n" + "=" * 80)
print("CONCLUSION:")
print("-" * 50)

if not result_embryoscope.empty:
    all_sequential = True
    for treatment in result_embryoscope['treatment_TreatmentName'].unique():
        treatment_data = result_embryoscope[result_embryoscope['treatment_TreatmentName'] == treatment]
        embryo_nums = treatment_data['embryo_embryo_number'].dropna().tolist()
        expected_range = list(range(1, len(embryo_nums) + 1))
        if embryo_nums != expected_range:
            all_sequential = False
            break
    
    if all_sequential:
        print("✅ FIX WORKING: embryo_number is sequential for patient 874649")
    else:
        print("❌ ISSUE: embryo_number is still not sequential for patient 874649")
        print("   The fix may not have been applied correctly or there's another issue")
else:
    print("⚠️  No data found for patient 874649 to verify the fix")

conn.close() 