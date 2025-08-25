#!/usr/bin/env python3
import duckdb as db

print("Fixing column selection...")

# Connect to the database
con = db.connect('G:/My Drive/projetos_individuais/Huntington/database/huntington_data_lake.duckdb')

# Attach clinisys database
clinisys_db_path = 'G:/My Drive/projetos_individuais/Huntington/database/clinisys_all.duckdb'
con.execute(f"ATTACH '{clinisys_db_path}' AS clinisys_all")
print("Attached clinisys_all database")

# Test with explicit column selection
print("\nStep 1: Explicit column selection")
explicit_query = con.execute("""
SELECT 
    d."Cliente", 
    p.codigo as prontuario, 
    'prontuario_main' as match_type
FROM silver.diario_vendas d
INNER JOIN clinisys_all.silver.view_pacientes p ON d."Cliente" = p.codigo
WHERE d."Cliente" IN (838483, 838547, 838756)
LIMIT 5
""").fetchdf()
print(explicit_query)

# Test the full query with explicit column selection
print("\nStep 2: Full query with explicit columns")
full_query = con.execute("""
SELECT 
    d."Cliente",
    COALESCE(bm.prontuario, -1) as prontuario
FROM silver.diario_vendas d
LEFT JOIN (
    SELECT "Cliente", prontuario FROM (
        SELECT "Cliente", prontuario, ROW_NUMBER() OVER (PARTITION BY "Cliente" ORDER BY match_type) as rn
        FROM (
            SELECT 
                d."Cliente", 
                p.codigo as prontuario, 
                'prontuario_main' as match_type
            FROM silver.diario_vendas d
            INNER JOIN clinisys_all.silver.view_pacientes p ON d."Cliente" = p.codigo
            WHERE d."Cliente" IN (838483, 838547, 838756)
        ) t
    ) ranked WHERE rn = 1
) bm ON d."Cliente" = bm."Cliente"
WHERE d."Cliente" IN (838483, 838547, 838756)
ORDER BY d."Cliente"
LIMIT 10
""").fetchdf()
print(full_query)

con.close()
print("\nTest completed.")
