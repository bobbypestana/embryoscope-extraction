import duckdb

print("SIMPLE CHECK FOR NON-SEQUENTIAL EMBRYO_NUMBER")
print("=" * 50)

try:
    conn = duckdb.connect("../database/huntington_data_lake.duckdb")
    
    # Quick check for non-sequential embryo numbers
    print("Checking for non-sequential embryo numbers...")
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
    
    # Check for non-sequential
    non_sequential = []
    for _, row in result.iterrows():
        embryo_count = row['embryo_count']
        min_num = row['min_embryo_num']
        max_num = row['max_embryo_num']
        
        if max_num != embryo_count or min_num != 1:
            non_sequential.append(row)
    
    print(f"Found {len(non_sequential)} combinations with non-sequential embryo numbers")
    
    if non_sequential:
        print("\nNon-sequential cases:")
        for i, row in enumerate(non_sequential[:5]):  # Show first 5
            print(f"  {i+1}. PatientIDx: {row['PatientIDx']}")
            print(f"     Treatment: {row['TreatmentName']}")
            print(f"     Embryo count: {row['embryo_count']}")
            print(f"     Range: {row['min_embryo_num']} to {row['max_embryo_num']}")
            print(f"     Expected: 1 to {row['embryo_count']}")
            print()
    else:
        print("✅ All embryo numbers are sequential!")
    
    conn.close()
    
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc() 