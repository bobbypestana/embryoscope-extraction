#!/usr/bin/env python3
import duckdb as db

print("Checking database attachment...")

# Connect to the database
con = db.connect('G:/My Drive/projetos_individuais/Huntington/database/huntington_data_lake.duckdb')

# Attach clinisys database
clinisys_db_path = 'G:/My Drive/projetos_individuais/Huntington/database/clinisys_all.duckdb'
con.execute(f"ATTACH '{clinisys_db_path}' AS clinisys_all")
print("Attached clinisys_all database")

# Check if clinisys_all.silver.view_pacientes exists
try:
    table_info = con.execute("DESCRIBE clinisys_all.silver.view_pacientes").fetchdf()
    print("\nTable structure of clinisys_all.silver.view_pacientes:")
    print(table_info)
except Exception as e:
    print(f"\nError describing table: {e}")

# Check if the table has any data
try:
    count = con.execute("SELECT COUNT(*) FROM clinisys_all.silver.view_pacientes").fetchdf()
    print(f"\nTotal records in clinisys_all.silver.view_pacientes: {count.iloc[0,0]:,}")
except Exception as e:
    print(f"\nError counting records: {e}")

# Check if codigo column exists and has data
try:
    codigo_sample = con.execute("SELECT codigo FROM clinisys_all.silver.view_pacientes LIMIT 5").fetchdf()
    print("\nSample codigo values:")
    print(codigo_sample)
except Exception as e:
    print(f"\nError querying codigo: {e}")

# Check if our test clientes exist in the table
try:
    test_clientes = con.execute("""
        SELECT codigo, COUNT(*) as count
        FROM clinisys_all.silver.view_pacientes 
        WHERE codigo IN (77785, 77536)
        GROUP BY codigo
    """).fetchdf()
    print("\nTest clientes in view_pacientes:")
    print(test_clientes)
except Exception as e:
    print(f"\nError checking test clientes: {e}")

con.close()
print("\nCheck completed.")
