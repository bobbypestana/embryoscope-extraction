#!/usr/bin/env python3
import duckdb as db

print("Testing final query execution...")

# Connect to the database
con = db.connect('G:/My Drive/projetos_individuais/Huntington/database/huntington_data_lake.duckdb')

# Attach clinisys database
clinisys_db_path = 'G:/My Drive/projetos_individuais/Huntington/database/clinisys_all.duckdb'
con.execute(f"ATTACH '{clinisys_db_path}' AS clinisys_all")
print("Attached clinisys_all database")

# Test the final query with LIMIT to see what we get
test_query = """
SELECT 
    d."Cliente",
    COALESCE(bm.prontuario, -1) as prontuario
FROM silver.diario_vendas d
LEFT JOIN (
    SELECT "Cliente", prontuario FROM (
        SELECT "Cliente", prontuario, ROW_NUMBER() OVER (PARTITION BY "Cliente" ORDER BY match_type) as rn
        FROM (
            SELECT "Cliente", prontuario, 'prontuario_main' as match_type
            FROM silver.diario_vendas d
            INNER JOIN clinisys_all.silver.view_pacientes p ON d."Cliente" = p.codigo
            LIMIT 10
        ) t
    ) ranked WHERE rn = 1
) bm ON d."Cliente" = bm."Cliente"
WHERE d."Cliente" IN (77785, 77536)
ORDER BY d."Cliente"
"""

result = con.execute(test_query).fetchdf()
print("\nTest query result:")
print(result)

con.close()
print("\nTest completed.")
