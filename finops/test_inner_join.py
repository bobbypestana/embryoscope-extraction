#!/usr/bin/env python3
import duckdb as db

print("Testing basic INNER JOIN...")

# Connect to the database
con = db.connect('G:/My Drive/projetos_individuais/Huntington/database/huntington_data_lake.duckdb')

# Attach clinisys database
clinisys_db_path = 'G:/My Drive/projetos_individuais/Huntington/database/clinisys_all.duckdb'
con.execute(f"ATTACH '{clinisys_db_path}' AS clinisys_all")
print("Attached clinisys_all database")

# Test basic INNER JOIN
test_query = """
SELECT 
    d."Cliente",
    p.codigo as prontuario
FROM silver.diario_vendas d
INNER JOIN clinisys_all.silver.view_pacientes p ON d."Cliente" = p.codigo
WHERE d."Cliente" IN (77785, 77536)
ORDER BY d."Cliente"
LIMIT 10
"""

result = con.execute(test_query).fetchdf()
print("\nINNER JOIN result:")
print(result)

# Check if these clientes exist in diario_vendas
check_diario = con.execute("""
SELECT "Cliente", COUNT(*) as count
FROM silver.diario_vendas 
WHERE "Cliente" IN (77785, 77536)
GROUP BY "Cliente"
""").fetchdf()
print("\nClientes in diario_vendas:")
print(check_diario)

# Check if these clientes exist in view_pacientes
check_pacientes = con.execute("""
SELECT codigo, COUNT(*) as count
FROM clinisys_all.silver.view_pacientes 
WHERE codigo IN (77785, 77536)
GROUP BY codigo
""").fetchdf()
print("\nClientes in view_pacientes:")
print(check_pacientes)

con.close()
print("\nTest completed.")
