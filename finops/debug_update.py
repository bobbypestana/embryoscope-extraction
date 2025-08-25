#!/usr/bin/env python3
import duckdb as db

print("Debugging UPDATE query...")

# Connect to the database
con = db.connect('G:/My Drive/projetos_individuais/Huntington/database/huntington_data_lake.duckdb')

# Attach clinisys database
clinisys_db_path = 'G:/My Drive/projetos_individuais/Huntington/database/clinisys_all.duckdb'
con.execute(f"ATTACH '{clinisys_db_path}' AS clinisys_all")
print("Attached clinisys_all database")

# Test the prontuario query to see what it returns
prontuario_query = """
WITH 
-- CTE 1: Extract and process diario_vendas data
diario_extract AS (
    SELECT DISTINCT 
        "Cliente", 
        CASE 
            WHEN "Nome" IS NOT NULL THEN TRIM(LOWER(SPLIT_PART("Nome", ' ', 1)))
            ELSE NULL 
        END as nome_first,
        CASE 
            WHEN "Nom Paciente" IS NOT NULL THEN TRIM(LOWER(SPLIT_PART("Nom Paciente", ' ', 1)))
            ELSE NULL 
        END as nom_paciente_first
    FROM silver.diario_vendas
    WHERE "Cliente" IS NOT NULL
),

-- CTE 2: Cliente ↔ prontuario (main/codigo)
matches_1 AS (
    SELECT d.*,
           p.codigo as prontuario, lower(trim(p.esposa_nome)) esposa_nome, LOWER(TRIM(p.marido_nome)) marido_nome, p.unidade_origem,
           'prontuario_main' as match_type
    FROM diario_extract d
    INNER JOIN clinisys_all.silver.view_pacientes p 
        ON d."Cliente" = p.codigo
)

-- Final result: Join with original diario_vendas and handle unmatched
SELECT 
    d.*,
    COALESCE(bm.prontuario, -1) as prontuario
FROM diario_extract d
LEFT JOIN matches_1 bm ON d."Cliente" = bm."Cliente"
ORDER BY d."Cliente"
LIMIT 10
"""

result = con.execute(prontuario_query).fetchdf()
print("\nSample result from prontuario query:")
print(result)
print(f"\nColumns: {list(result.columns)}")

# Check if there are any matches
matches = con.execute("""
SELECT COUNT(*) as count
FROM silver.diario_vendas d
INNER JOIN clinisys_all.silver.view_pacientes p ON d."Cliente" = p.codigo
""").fetchone()
print(f"\nDirect matches: {matches[0]:,}")

con.close()
print("\nDebug completed.")
