import duckdb

print("VERIFYING GOLD LAYER FIX")
print("=" * 40)

try:
    conn = duckdb.connect("../database/huntington_data_lake.duckdb")
    
    # Check the gold layer for patient 874649
    print("1. Checking gold layer for patient 874649:")
    result = conn.execute("""
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
    """).df()
    
    if result.empty:
        print("❌ No data found for patient 874649 in embryoscope gold layer")
    else:
        print(f"Found {len(result)} records for patient 874649")
        print("\nGold layer data:")
        print(result.to_string(index=False))
        
        # Check if embryo_number is sequential per treatment
        treatments = result['treatment_TreatmentName'].unique()
        print(f"\n2. EMBRYO_NUMBER ANALYSIS BY TREATMENT:")
        print("-" * 50)
        
        for treatment in treatments:
            treatment_data = result[result['treatment_TreatmentName'] == treatment]
            embryo_nums = treatment_data['embryo_embryo_number'].dropna().tolist()
            expected_range = list(range(1, len(embryo_nums) + 1))
            
            print(f"\nTreatment: {treatment}")
            print(f"  - Embryo numbers: {embryo_nums}")
            print(f"  - Expected sequential: {expected_range}")
            
            if embryo_nums == expected_range:
                print(f"  ✅ Sequential")
            else:
                print(f"  ❌ Non-sequential")
    
    # Also check the combined table
    print("\n" + "=" * 80)
    print("3. CHECKING COMBINED TABLE:")
    print("-" * 50)
    
    result2 = conn.execute("""
        SELECT 
            e.patient_PatientIDx,
            e.patient_PatientID,
            e.treatment_TreatmentName,
            e.embryo_EmbryoDescriptionID,
            e.embryo_embryo_number
        FROM gold.embryoscope_clinisys_combined e
        WHERE e.patient_PatientID = 874649
        ORDER BY e.treatment_TreatmentName, e.embryo_embryo_number
    """).df()
    
    if result2.empty:
        print("❌ No data found for patient 874649 in combined table")
    else:
        print(f"Found {len(result2)} records for patient 874649 in combined table")
        print("\nCombined table data:")
        print(result2.to_string(index=False))
        
        # Check if embryo_number is sequential
        embryo_nums = result2['embryo_embryo_number'].dropna().tolist()
        expected_range = list(range(1, len(embryo_nums) + 1))
        
        print(f"\nEmbryo numbers: {embryo_nums}")
        print(f"Expected sequential: {expected_range}")
        
        if embryo_nums == expected_range:
            print("✅ Combined table embryo_number is sequential!")
        else:
            print("❌ Combined table embryo_number is NOT sequential!")
    
    print("\n" + "=" * 80)
    print("CONCLUSION:")
    print("-" * 50)
    
    if not result.empty:
        all_sequential = True
        for treatment in result['treatment_TreatmentName'].unique():
            treatment_data = result[result['treatment_TreatmentName'] == treatment]
            embryo_nums = treatment_data['embryo_embryo_number'].dropna().tolist()
            expected_range = list(range(1, len(embryo_nums) + 1))
            if embryo_nums != expected_range:
                all_sequential = False
                break
        
        if all_sequential:
            print("✅ FIX SUCCESSFUL: embryo_number is sequential for patient 874649 in gold layer")
        else:
            print("❌ ISSUE: embryo_number is still not sequential for patient 874649 in gold layer")
    else:
        print("⚠️  No data found for patient 874649 to verify the fix")
    
    conn.close()
    
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc() 