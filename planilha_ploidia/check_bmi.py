#!/usr/bin/env python3
"""Check BMI calculation for patient 823589"""

import duckdb as db
import os
import pandas as pd

repo_root = os.path.dirname(os.path.dirname(__file__))
path_to_db = os.path.join(repo_root, 'database', 'huntington_data_lake.duckdb')
conn = db.connect(path_to_db, read_only=True)

print("=" * 80)
print("CHECKING BMI IN gold.data_ploidia")
print("=" * 80)
result = conn.execute('''
    SELECT "Patient ID", "BMI", "Age", "Slide ID" 
    FROM gold.data_ploidia 
    WHERE "Patient ID" = 823589 
    LIMIT 10
''').df()
print(result.to_string())
print(f"\nBMI NULL count: {result['BMI'].isna().sum()} out of {len(result)} rows")
print(f"BMI non-NULL count: {result['BMI'].notna().sum()} out of {len(result)} rows")
if result['BMI'].notna().sum() > 0:
    print("\nRows with BMI calculated:")
    print(result[result['BMI'].notna()].to_string())

print("\n" + "=" * 80)
print("CHECKING SOURCE COLUMNS IN gold.planilha_embryoscope_combined")
print("=" * 80)
source = conn.execute('''
    SELECT 
        "micro_prontuario", 
        "trat_peso_paciente", 
        "trat_altura_paciente",
        "embryo_EmbryoID"
    FROM gold.planilha_embryoscope_combined 
    WHERE "micro_prontuario" = 823589 
        AND "embryo_EmbryoID" IS NOT NULL 
    LIMIT 10
''').df()
print(source.to_string())
print(f"\nWeight NULL count: {source['trat_peso_paciente'].isna().sum()} out of {len(source)} rows")
print(f"Height NULL count: {source['trat_altura_paciente'].isna().sum()} out of {len(source)} rows")

print("\n" + "=" * 80)
print("TESTING BMI CALCULATION MANUALLY")
print("=" * 80)
test = conn.execute('''
    SELECT 
        "micro_prontuario",
        "trat_peso_paciente",
        "trat_altura_paciente",
        ROUND(CAST("trat_peso_paciente" AS DOUBLE) / POWER(CAST("trat_altura_paciente" AS DOUBLE), 2), 2) as calculated_bmi
    FROM gold.planilha_embryoscope_combined 
    WHERE "micro_prontuario" = 823589 
        AND "embryo_EmbryoID" IS NOT NULL 
    LIMIT 10
''').df()
print(test.to_string())

print("\n" + "=" * 80)
print("SEARCHING FOR ALTERNATIVE WEIGHT/HEIGHT COLUMNS")
print("=" * 80)
all_cols = conn.execute('DESCRIBE gold.planilha_embryoscope_combined').df()['column_name'].tolist()
peso_cols = [c for c in all_cols if 'peso' in c.lower()]
altura_cols = [c for c in all_cols if 'altura' in c.lower()]
weight_cols = [c for c in all_cols if 'weight' in c.lower()]
height_cols = [c for c in all_cols if 'height' in c.lower()]

print(f"Weight columns (peso/weight): {peso_cols + weight_cols}")
print(f"Height columns (altura/height): {altura_cols + height_cols}")

# Check if there are any non-NULL values in these columns for this patient
if peso_cols or weight_cols:
    weight_col = (peso_cols + weight_cols)[0]
    print(f"\nChecking {weight_col} for patient 823589:")
    weight_check = conn.execute(f'''
        SELECT DISTINCT "{weight_col}" 
        FROM gold.planilha_embryoscope_combined 
        WHERE "micro_prontuario" = 823589 
        LIMIT 5
    ''').df()
    print(weight_check.to_string())

if altura_cols or height_cols:
    height_col = (altura_cols + height_cols)[0]
    print(f"\nChecking {height_col} for patient 823589:")
    height_check = conn.execute(f'''
        SELECT DISTINCT "{height_col}" 
        FROM gold.planilha_embryoscope_combined 
        WHERE "micro_prontuario" = 823589 
        LIMIT 5
    ''').df()
    print(height_check.to_string())

print("\n" + "=" * 80)
print("ROWS WHERE BOTH WEIGHT AND HEIGHT ARE NOT NULL")
print("=" * 80)
both_not_null = conn.execute('''
    SELECT 
        "micro_prontuario",
        "trat_peso_paciente",
        "trat_altura_paciente",
        "embryo_EmbryoID",
        ROUND(CAST("trat_peso_paciente" AS DOUBLE) / POWER(CAST("trat_altura_paciente" AS DOUBLE), 2), 2) as calculated_bmi
    FROM gold.planilha_embryoscope_combined 
    WHERE "micro_prontuario" = 823589 
        AND "embryo_EmbryoID" IS NOT NULL
        AND "trat_peso_paciente" IS NOT NULL
        AND "trat_altura_paciente" IS NOT NULL
''').df()
print(both_not_null.to_string())
print(f"\nFound {len(both_not_null)} rows with both weight and height")

# Check ALL rows for this patient (not just filtered)
print("\n" + "=" * 80)
print("ALL ROWS FOR PATIENT 823589 (checking all, not just with embryo_EmbryoID)")
print("=" * 80)
all_rows = conn.execute('''
    SELECT 
        "micro_prontuario",
        "trat_peso_paciente",
        "trat_altura_paciente",
        "embryo_EmbryoID"
    FROM gold.planilha_embryoscope_combined 
    WHERE "micro_prontuario" = 823589
        AND "trat_peso_paciente" IS NOT NULL
        AND "trat_altura_paciente" IS NOT NULL
''').df()
print(all_rows.to_string())
print(f"\nFound {len(all_rows)} total rows with both weight and height (including NULL embryo_EmbryoID)")

# Also check planilha columns
print("\n" + "=" * 80)
print("CHECKING planilha_PESO AND planilha_ALTURA")
print("=" * 80)
planilha_check = conn.execute('''
    SELECT 
        "micro_prontuario",
        "planilha_PESO",
        "planilha_ALTURA",
        "planilha_ALTURA_cm",
        "embryo_EmbryoID"
    FROM gold.planilha_embryoscope_combined 
    WHERE "micro_prontuario" = 823589 
        AND "embryo_EmbryoID" IS NOT NULL
    LIMIT 5
''').df()
print(planilha_check.to_string())

# Check distinct patient-level values
print("\n" + "=" * 80)
print("DISTINCT PATIENT-LEVEL VALUES (weight/height should be same for all embryos)")
print("=" * 80)
distinct = conn.execute('''
    SELECT DISTINCT 
        "micro_prontuario", 
        "trat_peso_paciente", 
        "trat_altura_paciente"
    FROM gold.planilha_embryoscope_combined 
    WHERE "micro_prontuario" = 823589
    ORDER BY "trat_peso_paciente" NULLS LAST, "trat_altura_paciente" NULLS LAST
''').df()
print(distinct.to_string())

# Check if we can use MAX to get non-NULL values
print("\n" + "=" * 80)
print("USING MAX TO GET NON-NULL VALUES (patient-level aggregation)")
print("=" * 80)
max_vals = conn.execute('''
    SELECT 
        "micro_prontuario",
        MAX("trat_peso_paciente") as max_weight,
        MAX("trat_altura_paciente") as max_height
    FROM gold.planilha_embryoscope_combined 
    WHERE "micro_prontuario" = 823589
    GROUP BY "micro_prontuario"
''').df()
print(max_vals.to_string())
if len(max_vals) > 0 and max_vals.iloc[0]['max_weight'] is not None and max_vals.iloc[0]['max_height'] is not None:
    weight = max_vals.iloc[0]['max_weight']
    height = max_vals.iloc[0]['max_height']
    bmi = round(weight / (height ** 2), 2)
    print(f"\nCalculated BMI: {weight} / ({height})^2 = {bmi}")

# Check embryoscope_clinisys_combined table
print("\n" + "=" * 80)
print("CHECKING gold.embryoscope_clinisys_combined TABLE")
print("=" * 80)
try:
    clinisys_cols = conn.execute('DESCRIBE gold.embryoscope_clinisys_combined').df()['column_name'].tolist()
    peso_clinisys = [c for c in clinisys_cols if 'peso' in c.lower()]
    altura_clinisys = [c for c in clinisys_cols if 'altura' in c.lower()]
    print(f"Weight columns in embryoscope_clinisys_combined: {peso_clinisys}")
    print(f"Height columns in embryoscope_clinisys_combined: {altura_clinisys}")
    
    if 'trat_peso_paciente' in clinisys_cols and 'trat_altura_paciente' in clinisys_cols:
        clinisys_vals = conn.execute('''
            SELECT DISTINCT 
                "micro_prontuario", 
                "trat_peso_paciente", 
                "trat_altura_paciente"
            FROM gold.embryoscope_clinisys_combined 
            WHERE "micro_prontuario" = 823589
        ''').df()
        print("\nValues in embryoscope_clinisys_combined:")
        print(clinisys_vals.to_string())
        
        # Check for rows with both not NULL
        both_not_null_clinisys = conn.execute('''
            SELECT 
                "micro_prontuario",
                "trat_peso_paciente",
                "trat_altura_paciente",
                ROUND(CAST("trat_peso_paciente" AS DOUBLE) / POWER(CAST("trat_altura_paciente" AS DOUBLE), 2), 2) as calculated_bmi
            FROM gold.embryoscope_clinisys_combined 
            WHERE "micro_prontuario" = 823589
                AND "trat_peso_paciente" IS NOT NULL
                AND "trat_altura_paciente" IS NOT NULL
        ''').df()
        print(f"\nRows in embryoscope_clinisys_combined with both weight and height: {len(both_not_null_clinisys)}")
        if len(both_not_null_clinisys) > 0:
            print(both_not_null_clinisys.to_string())
except Exception as e:
    print(f"Error checking embryoscope_clinisys_combined: {e}")

conn.close()

