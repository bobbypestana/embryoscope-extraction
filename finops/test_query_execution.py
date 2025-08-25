#!/usr/bin/env python3
import duckdb as db
import os

print("Testing query execution step by step...")

# Connect to the database
con = db.connect('G:/My Drive/projetos_individuais/Huntington/database/huntington_data_lake.duckdb')

# Attach clinisys database
clinisys_db_path = 'G:/My Drive/projetos_individuais/Huntington/database/clinisys_all.duckdb'
con.execute(f"ATTACH '{clinisys_db_path}' AS clinisys_all")
print("Attached clinisys_all database")

# Test a simple query first
test_result = con.execute("SELECT COUNT(*) FROM clinisys_all.silver.view_pacientes").fetchdf()
print(f"Records in clinisys_all.silver.view_pacientes: {test_result.iloc[0,0]:,}")

# Test the diario_extract CTE
diario_extract_result = con.execute("""
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
    LIMIT 5
""").fetchdf()

print("\nSample from diario_extract:")
print(diario_extract_result)

# Test matches_1 CTE
matches_1_result = con.execute("""
    WITH diario_extract AS (
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
    )
    SELECT d.*,
           p.codigo as prontuario, lower(trim(p.esposa_nome)) esposa_nome, LOWER(TRIM(p.marido_nome)) marido_nome, p.unidade_origem,
           'prontuario_main' as match_type
    FROM diario_extract d
    INNER JOIN clinisys_all.silver.view_pacientes p 
        ON d."Cliente" = p.codigo
    LIMIT 5
""").fetchdf()

print("\nSample from matches_1:")
print(matches_1_result)

con.close()
print("\nTest completed.")
