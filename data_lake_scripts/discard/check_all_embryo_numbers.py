import duckdb

print("CHECKING ALL PATIENTS FOR NON-SEQUENTIAL EMBRYO_NUMBER")
print("=" * 60)

try:
    conn = duckdb.connect("../database/huntington_data_lake.duckdb")
    
    # Get all unique PatientIDx and TreatmentName combinations
    print("1. Analyzing all PatientIDx+TreatmentName combinations:")
    result = conn.execute("""
        SELECT 
            PatientIDx,
            TreatmentName,
            COUNT(*) as embryo_count,
            MIN(embryo_number) as min_embryo_num,
            MAX(embryo_number) as max_embryo_num
        FROM silver_embryoscope.embryo_data
        GROUP BY PatientIDx, TreatmentName
        HAVING COUNT(*) > 1
        ORDER BY PatientIDx, TreatmentName
    """).df()
    
    print(f"Found {len(result)} PatientIDx+TreatmentName combinations with multiple embryos")
    
    # Check for non-sequential embryo numbers
    print("\n2. Checking for non-sequential embryo numbers:")
    non_sequential_patients = []
    
    for _, row in result.iterrows():
        patientidx = row['PatientIDx']
        treatment = row['TreatmentName']
        embryo_count = row['embryo_count']
        min_num = row['min_embryo_num']
        max_num = row['max_embryo_num']
        
        # Check if the range is sequential
        expected_max = embryo_count
        if max_num != expected_max or min_num != 1:
            non_sequential_patients.append({
                'PatientIDx': patientidx,
                'TreatmentName': treatment,
                'EmbryoCount': embryo_count,
                'MinEmbryoNum': min_num,
                'MaxEmbryoNum': max_num,
                'ExpectedMax': expected_max,
                'IsSequential': False
            })
    
    if non_sequential_patients:
        print(f"❌ Found {len(non_sequential_patients)} patients with non-sequential embryo numbers:")
        print("\nNon-sequential patients:")
        for patient in non_sequential_patients:
            print(f"  - PatientIDx: {patient['PatientIDx']}")
            print(f"    Treatment: {patient['TreatmentName']}")
            print(f"    Embryo count: {patient['EmbryoCount']}")
            print(f"    Embryo number range: {patient['MinEmbryoNum']} to {patient['MaxEmbryoNum']}")
            print(f"    Expected range: 1 to {patient['ExpectedMax']}")
            print()
    else:
        print("✅ All patients have sequential embryo numbers!")
    
    # Show some examples of sequential patients for comparison
    print("\n3. Examples of patients with sequential embryo numbers:")
    sequential_examples = result.head(5)
    for _, row in sequential_examples.iterrows():
        patientidx = row['PatientIDx']
        treatment = row['TreatmentName']
        embryo_count = row['embryo_count']
        min_num = row['min_embryo_num']
        max_num = row['max_embryo_num']
        
        print(f"  - PatientIDx: {patientidx}")
        print(f"    Treatment: {treatment}")
        print(f"    Embryo count: {embryo_count}")
        print(f"    Embryo number range: {min_num} to {max_num}")
        print()
    
    # Detailed analysis of a few non-sequential cases (if any)
    if non_sequential_patients:
        print("\n4. Detailed analysis of non-sequential cases:")
        for i, patient in enumerate(non_sequential_patients[:3]):  # Show first 3 cases
            print(f"\nCase {i+1}: {patient['PatientIDx']} - {patient['TreatmentName']}")
            
            # Get the actual embryo numbers
            detail_result = conn.execute(f"""
                SELECT 
                    EmbryoDescriptionID,
                    embryo_number
                FROM silver_embryoscope.embryo_data
                WHERE PatientIDx = '{patient['PatientIDx']}'
                AND TreatmentName = '{patient['TreatmentName']}'
                ORDER BY 
                    CASE 
                        WHEN EmbryoDescriptionID IS NULL THEN NULL
                        WHEN regexp_matches(EmbryoDescriptionID, '^[A-Z]+[0-9]+$') THEN
                            regexp_replace(EmbryoDescriptionID, '^([A-Z]+)([0-9]+)$', 
                                          '\\1' || lpad(regexp_extract(EmbryoDescriptionID, '([0-9]+)$', 1), 2, '0'))
                        ELSE EmbryoDescriptionID
                    END
            """).df()
            
            embryo_nums = detail_result['embryo_number'].tolist()
            expected_range = list(range(1, len(embryo_nums) + 1))
            
            print(f"  Actual embryo numbers: {embryo_nums}")
            print(f"  Expected sequential: {expected_range}")
            print(f"  Embryo descriptions: {detail_result['EmbryoDescriptionID'].tolist()}")
    
    print("\n" + "=" * 60)
    print("SUMMARY:")
    print("-" * 30)
    
    total_combinations = len(result)
    non_sequential_count = len(non_sequential_patients)
    sequential_count = total_combinations - non_sequential_count
    
    print(f"Total PatientIDx+TreatmentName combinations: {total_combinations}")
    print(f"Sequential embryo numbers: {sequential_count}")
    print(f"Non-sequential embryo numbers: {non_sequential_count}")
    
    if non_sequential_count > 0:
        percentage = (non_sequential_count / total_combinations) * 100
        print(f"Percentage with issues: {percentage:.2f}%")
    else:
        print("✅ All embryo numbers are sequential!")
    
    conn.close()
    
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc() 