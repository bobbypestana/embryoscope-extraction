import duckdb

# Connect to database
db_path = "../database/huntington_data_lake.duckdb"
conn = duckdb.connect(db_path)

print("INVESTIGATING EMBRYO_NUMBER ISSUE FOR PATIENT 874649")
print("=" * 80)

# 1. Check silver layer data for patient 874649
print("1. SILVER LAYER DATA FOR PATIENT 874649:")
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
ORDER BY TreatmentName, EmbryoDescriptionID
"""

result_silver = conn.execute(query_silver).df()

if result_silver.empty:
    print("❌ No data found for patient 874649 in silver layer")
else:
    print(f"Found {len(result_silver)} records for patient 874649 in silver layer")
    print("\nSilver layer data:")
    print(result_silver.to_string(index=False))
    
    # Check if embryo_number is sequential per treatment in silver
    treatments_silver = result_silver['TreatmentName'].unique()
    print(f"\n2. SILVER LAYER EMBRYO_NUMBER ANALYSIS BY TREATMENT:")
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

# 2. Check if there are multiple patients with the same TreatmentName
print("3. CHECKING FOR MULTIPLE PATIENTS WITH SAME TREATMENT NAME:")
print("-" * 50)

if not result_silver.empty:
    treatments_to_check = result_silver['TreatmentName'].unique()
    
    for treatment in treatments_to_check:
        query_multiple = f"""
        SELECT 
            PatientIDx,
            TreatmentName,
            COUNT(*) as embryo_count,
            MIN(embryo_number) as min_embryo_num,
            MAX(embryo_number) as max_embryo_num
        FROM silver_embryoscope.embryo_data
        WHERE TreatmentName = '{treatment}'
        GROUP BY PatientIDx, TreatmentName
        ORDER BY PatientIDx
        """
        
        multiple_result = conn.execute(query_multiple).df()
        
        if len(multiple_result) > 1:
            print(f"\nTreatment '{treatment}' has multiple patients:")
            print(multiple_result.to_string(index=False))
        else:
            print(f"\nTreatment '{treatment}' has only one patient")

print("\n" + "=" * 80)

# 3. Check the specific EmbryoDescriptionID transformation logic
print("4. CHECKING EMBRYODESCRIPTIONID TRANSFORMATION:")
print("-" * 50)

if not result_silver.empty:
    print("Sample EmbryoDescriptionID values and their expected sorting:")
    sample_data = result_silver[['EmbryoDescriptionID', 'embryo_number']].head(10)
    print(sample_data.to_string(index=False))
    
    # Test the transformation logic manually
    print("\nTesting transformation logic:")
    for _, row in sample_data.iterrows():
        embryo_desc = row['EmbryoDescriptionID']
        embryo_num = row['embryo_number']
        
        # Simulate the transformation logic
        if embryo_desc and embryo_desc.startswith('AA'):
            # Extract number part
            num_part = embryo_desc[2:]  # Remove 'AA'
            if num_part.isdigit():
                transformed = f"AA{num_part.zfill(2)}"  # Pad with zeros
                print(f"  {embryo_desc} -> {transformed} (embryo_number: {embryo_num})")
            else:
                print(f"  {embryo_desc} -> {embryo_desc} (embryo_number: {embryo_num})")
        else:
            print(f"  {embryo_desc} -> {embryo_desc} (embryo_number: {embryo_num})")

print("\n" + "=" * 80)

# 4. Check if the issue is in the gold layer transformation
print("5. CHECKING GOLD LAYER TRANSFORMATION:")
print("-" * 50)

query_gold = """
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
ORDER BY e.treatment_TreatmentName, e.embryo_EmbryoDescriptionID
"""

result_gold = conn.execute(query_gold).df()

if not result_gold.empty:
    print("Gold layer data:")
    print(result_gold.to_string(index=False))
    
    # Check if embryo_number is sequential per treatment in gold
    treatments_gold = result_gold['treatment_TreatmentName'].unique()
    print(f"\n6. GOLD LAYER EMBRYO_NUMBER ANALYSIS BY TREATMENT:")
    print("-" * 50)
    
    for treatment in treatments_gold:
        treatment_data = result_gold[result_gold['treatment_TreatmentName'] == treatment]
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
print("CONCLUSION:")
print("-" * 50)

if not result_silver.empty and not result_gold.empty:
    silver_sequential = True
    gold_sequential = True
    
    # Check silver layer
    for treatment in result_silver['TreatmentName'].unique():
        treatment_data = result_silver[result_silver['TreatmentName'] == treatment]
        embryo_nums = treatment_data['embryo_number'].dropna().tolist()
        expected_range = list(range(1, len(embryo_nums) + 1))
        if embryo_nums != expected_range:
            silver_sequential = False
            break
    
    # Check gold layer
    for treatment in result_gold['treatment_TreatmentName'].unique():
        treatment_data = result_gold[result_gold['treatment_TreatmentName'] == treatment]
        embryo_nums = treatment_data['embryo_embryo_number'].dropna().tolist()
        expected_range = list(range(1, len(embryo_nums) + 1))
        if embryo_nums != expected_range:
            gold_sequential = False
            break
    
    if silver_sequential and gold_sequential:
        print("✅ FIX WORKING: embryo_number is sequential in both layers")
    elif silver_sequential and not gold_sequential:
        print("❌ ISSUE: Silver layer is correct but gold layer is not")
        print("   The gold layer transformation is not working correctly")
    elif not silver_sequential and gold_sequential:
        print("❌ ISSUE: Gold layer is correct but silver layer is not")
        print("   The silver layer fix was not applied correctly")
    else:
        print("❌ ISSUE: Both layers have non-sequential embryo_number")
        print("   The fix was not applied correctly in either layer")
else:
    print("⚠️  No data found for patient 874649 to verify the fix")

conn.close() 