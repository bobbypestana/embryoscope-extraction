#!/usr/bin/env python3
import duckdb as db

print("Debugging simple query...")

# Connect to the database
con = db.connect('G:/My Drive/projetos_individuais/Huntington/database/huntington_data_lake.duckdb')

# Attach clinisys database
clinisys_db_path = 'G:/My Drive/projetos_individuais/Huntington/database/clinisys_all.duckdb'
con.execute(f"ATTACH '{clinisys_db_path}' AS clinisys_all")
print("Attached clinisys_all database")

# Step 1: Test basic INNER JOIN
print("\nStep 1: Basic INNER JOIN")
basic_join = con.execute("""
SELECT 
    d."Cliente",
    p.codigo as prontuario
FROM silver.diario_vendas d
INNER JOIN clinisys_all.silver.view_pacientes p ON d."Cliente" = p.codigo
WHERE d."Cliente" IN (838483, 838547, 838756)
LIMIT 5
""").fetchdf()
print(basic_join)

# Step 2: Test the subquery structure
print("\nStep 2: Subquery structure")
subquery = con.execute("""
SELECT "Cliente", prontuario, 'prontuario_main' as match_type
FROM silver.diario_vendas d
INNER JOIN clinisys_all.silver.view_pacientes p ON d."Cliente" = p.codigo
WHERE d."Cliente" IN (838483, 838547, 838756)
LIMIT 5
""").fetchdf()
print(subquery)

# Step 3: Test the ROW_NUMBER part
print("\nStep 3: ROW_NUMBER part")
row_number_query = con.execute("""
SELECT "Cliente", prontuario, ROW_NUMBER() OVER (PARTITION BY "Cliente" ORDER BY match_type) as rn
FROM (
    SELECT "Cliente", prontuario, 'prontuario_main' as match_type
    FROM silver.diario_vendas d
    INNER JOIN clinisys_all.silver.view_pacientes p ON d."Cliente" = p.codigo
    WHERE d."Cliente" IN (838483, 838547, 838756)
) t
LIMIT 10
""").fetchdf()
print(row_number_query)

# Step 4: Test the ranked part
print("\nStep 4: Ranked part")
ranked_query = con.execute("""
SELECT "Cliente", prontuario FROM (
    SELECT "Cliente", prontuario, ROW_NUMBER() OVER (PARTITION BY "Cliente" ORDER BY match_type) as rn
    FROM (
        SELECT "Cliente", prontuario, 'prontuario_main' as match_type
        FROM silver.diario_vendas d
        INNER JOIN clinisys_all.silver.view_pacientes p ON d."Cliente" = p.codigo
        WHERE d."Cliente" IN (838483, 838547, 838756)
    ) t
) ranked WHERE rn = 1
""").fetchdf()
print(ranked_query)

con.close()
print("\nDebug completed.")
