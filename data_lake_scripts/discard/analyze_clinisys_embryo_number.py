import duckdb

# Connect to database
db_path = "../database/huntington_data_lake.duckdb"
conn = duckdb.connect(db_path)

print("ANALYZING CLINISYS EMBRYO_NUMBER LOGIC")
print("=" * 80)

# Check the current clinisys embryo_number logic
print("1. CURRENT CLINISYS EMBRYO_NUMBER LOGIC:")
print("-" * 50)
print("""
Current logic in clinisys silver_loader_try_strptime_complete.py:
```sql
ROW_NUMBER() OVER (PARTITION BY id_micromanipulacao ORDER BY id) AS embryo_number
```

This partitions by id_micromanipulacao (micro-manipulation ID) and orders by id.
""")

# Check if there are multiple patients per oocito_id_micromanipulacao
print("\n2. CHECKING FOR MULTIPLE PATIENTS PER MICROMANIPULATION:")
print("-" * 50)

query_multiple_patients = """
SELECT 
    oocito_id_micromanipulacao,
    COUNT(DISTINCT micro_prontuario) as unique_patients,
    COUNT(*) as total_records
FROM gold.clinisys_embrioes
WHERE micro_prontuario IS NOT NULL
GROUP BY oocito_id_micromanipulacao
HAVING COUNT(DISTINCT micro_prontuario) > 1
ORDER BY unique_patients DESC, total_records DESC
LIMIT 10
"""

result_multiple = conn.execute(query_multiple_patients).df()

if result_multiple.empty:
    print("✅ No multiple patients per micromanipulation found.")
    print("   The current clinisys embryo_number logic is correct.")
else:
    print("❌ Found multiple patients per micromanipulation:")
    print(result_multiple.to_string(index=False))
    print("\n   This indicates the clinisys embryo_number logic needs adjustment!")

print("\n" + "=" * 80)

# Check the data structure for a specific example
print("3. DETAILED ANALYSIS OF MICROMANIPULATION STRUCTURE:")
print("-" * 50)

# Get a sample micromanipulation with multiple patients (if any)
if not result_multiple.empty:
    sample_micromanipulacao = result_multiple.iloc[0]['oocito_id_micromanipulacao']
    
    query_sample = f"""
    SELECT 
        oocito_id_micromanipulacao,
        micro_prontuario,
        oocito_id,
        oocito_embryo_number,
        micro_Data_DL
    FROM gold.clinisys_embrioes
    WHERE oocito_id_micromanipulacao = {sample_micromanipulacao}
    ORDER BY micro_prontuario, oocito_embryo_number
    """
    
    sample_result = conn.execute(query_sample).df()
    print(f"Sample micromanipulation {sample_micromanipulacao}:")
    print(sample_result.to_string(index=False))
    
    # Check if embryo_number is sequential per patient
    patients = sample_result['micro_prontuario'].unique()
    print(f"\nAnalysis for micromanipulation {sample_micromanipulacao}:")
    for patient in patients:
        patient_data = sample_result[sample_result['micro_prontuario'] == patient]
        embryo_nums = patient_data['oocito_embryo_number'].tolist()
        expected_range = list(range(1, len(embryo_nums) + 1))
        
        print(f"  Patient {patient}: embryo_numbers {embryo_nums}")
        if embryo_nums == expected_range:
            print(f"    ✅ Sequential")
        else:
            print(f"    ❌ Non-sequential (expected: {expected_range})")

else:
    # Check a normal case
    query_normal = """
    SELECT 
        oocito_id_micromanipulacao,
        micro_prontuario,
        oocito_id,
        oocito_embryo_number,
        micro_Data_DL
    FROM gold.clinisys_embrioes
    WHERE micro_prontuario IS NOT NULL
    ORDER BY oocito_id_micromanipulacao, oocito_embryo_number
    LIMIT 10
    """
    
    normal_result = conn.execute(query_normal).df()
    print("Sample of normal micromanipulation structure:")
    print(normal_result.to_string(index=False))

print("\n" + "=" * 80)

# Compare with embryoscope logic
print("4. COMPARISON WITH EMBRYOSCOPE LOGIC:")
print("-" * 50)

print("""
EMBRYOSCOPE (FIXED):
```sql
ROW_NUMBER() OVER (
    PARTITION BY PatientIDx, TreatmentName 
    ORDER BY EmbryoDescriptionID (with transformation)
) AS embryo_number
```

CLINISYS (CURRENT):
```sql
ROW_NUMBER() OVER (
    PARTITION BY id_micromanipulacao 
    ORDER BY id
) AS embryo_number
```

The key difference is:
- Embryoscope: Partitions by PatientIDx + TreatmentName (patient-specific)
- Clinisys: Partitions by id_micromanipulacao (procedure-specific)
""")

print("\n" + "=" * 80)

# Check if oocito_id_micromanipulacao is unique per patient
print("5. CHECKING IF MICROMANIPULATION IS UNIQUE PER PATIENT:")
print("-" * 50)

query_unique_check = """
SELECT 
    micro_prontuario,
    COUNT(DISTINCT oocito_id_micromanipulacao) as unique_micromanipulations,
    COUNT(*) as total_records
FROM gold.clinisys_embrioes
WHERE micro_prontuario IS NOT NULL
GROUP BY micro_prontuario
HAVING COUNT(DISTINCT oocito_id_micromanipulacao) > 1
ORDER BY unique_micromanipulations DESC, total_records DESC
LIMIT 5
"""

unique_result = conn.execute(query_unique_check).df()

if unique_result.empty:
    print("✅ Each patient has only one micromanipulation ID.")
    print("   The current clinisys logic is appropriate.")
else:
    print("❌ Found patients with multiple micromanipulation IDs:")
    print(unique_result.to_string(index=False))
    print("\n   This suggests the current logic might be correct.")

print("\n" + "=" * 80)
print("CONCLUSION:")
print("-" * 50)

if result_multiple.empty:
    print("✅ CLINISYS EMBRYO_NUMBER LOGIC IS CORRECT")
    print("   - No multiple patients per micromanipulation found")
    print("   - Each micromanipulation ID is unique per patient")
    print("   - Current partitioning by id_micromanipulacao is appropriate")
else:
    print("❌ CLINISYS EMBRYO_NUMBER LOGIC NEEDS ADJUSTMENT")
    print("   - Multiple patients share the same micromanipulation ID")
    print("   - Should partition by micro_prontuario + oocito_id_micromanipulacao")
    print("   - Similar to the embryoscope fix we just applied")

conn.close() 