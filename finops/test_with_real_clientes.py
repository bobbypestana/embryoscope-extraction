#!/usr/bin/env python3
import duckdb as db

print("Testing with real matching clientes...")

# Connect to the database
con = db.connect('G:/My Drive/projetos_individuais/Huntington/database/huntington_data_lake.duckdb')

# Attach clinisys database
clinisys_db_path = 'G:/My Drive/projetos_individuais/Huntington/database/clinisys_all.duckdb'
con.execute(f"ATTACH '{clinisys_db_path}' AS clinisys_all")
print("Attached clinisys_all database")

# Test with actual matching clientes
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
            WHERE d."Cliente" IN (838483, 838547, 838756)
        ) t
    ) ranked WHERE rn = 1
) bm ON d."Cliente" = bm."Cliente"
WHERE d."Cliente" IN (838483, 838547, 838756)
ORDER BY d."Cliente"
LIMIT 20
"""

result = con.execute(test_query).fetchdf()
print("\nTest query result with real clientes:")
print(result)

# Check the distribution of prontuario values
prontuario_dist = con.execute("""
SELECT 
    CASE 
        WHEN prontuario IS NULL THEN 'NULL'
        WHEN prontuario = -1 THEN '-1'
        ELSE 'Valid'
    END as prontuario_type,
    COUNT(*) as count
FROM (
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
            ) t
        ) ranked WHERE rn = 1
    ) bm ON d."Cliente" = bm."Cliente"
) final
GROUP BY prontuario_type
""").fetchdf()

print("\nProntuario distribution:")
print(prontuario_dist)

con.close()
print("\nTest completed.")
