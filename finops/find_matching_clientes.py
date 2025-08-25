#!/usr/bin/env python3
import duckdb as db

print("Finding matching clientes...")

# Connect to the database
con = db.connect('G:/My Drive/projetos_individuais/Huntington/database/huntington_data_lake.duckdb')

# Attach clinisys database
clinisys_db_path = 'G:/My Drive/projetos_individuais/Huntington/database/clinisys_all.duckdb'
con.execute(f"ATTACH '{clinisys_db_path}' AS clinisys_all")
print("Attached clinisys_all database")

# Find clientes that exist in both tables
matching_query = """
SELECT 
    d."Cliente",
    p.codigo as prontuario,
    p.esposa_nome,
    p.marido_nome
FROM silver.diario_vendas d
INNER JOIN clinisys_all.silver.view_pacientes p ON d."Cliente" = p.codigo
LIMIT 10
"""

result = con.execute(matching_query).fetchdf()
print("\nSample matching clientes:")
print(result)

# Count total matches
count_query = """
SELECT COUNT(*) as total_matches
FROM silver.diario_vendas d
INNER JOIN clinisys_all.silver.view_pacientes p ON d."Cliente" = p.codigo
"""

count_result = con.execute(count_query).fetchdf()
print(f"\nTotal matching clientes: {count_result.iloc[0,0]:,}")

# Get some sample clientes from diario_vendas
diario_sample = con.execute("""
SELECT "Cliente", COUNT(*) as count
FROM silver.diario_vendas 
GROUP BY "Cliente"
ORDER BY count DESC
LIMIT 10
""").fetchdf()
print("\nTop 10 clientes in diario_vendas:")
print(diario_sample)

con.close()
print("\nCheck completed.")
