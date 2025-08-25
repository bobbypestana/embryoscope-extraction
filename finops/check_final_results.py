#!/usr/bin/env python3
import duckdb as db

print("Checking final results...")

# Connect to the database
con = db.connect('G:/My Drive/projetos_individuais/Huntington/database/huntington_data_lake.duckdb')

# Check the distribution of prontuario values
prontuario_dist = con.execute("""
SELECT 
    CASE 
        WHEN prontuario IS NULL THEN 'NULL'
        WHEN prontuario = -1 THEN '-1'
        ELSE 'Valid'
    END as prontuario_type,
    COUNT(*) as count
FROM silver.diario_vendas
GROUP BY prontuario_type
ORDER BY prontuario_type
""").fetchdf()

print("\nProntuario distribution:")
print(prontuario_dist)

# Check some sample valid prontuario values
valid_samples = con.execute("""
SELECT "Cliente", prontuario
FROM silver.diario_vendas
WHERE prontuario IS NOT NULL AND prontuario != -1
ORDER BY "Cliente"
LIMIT 10
""").fetchdf()

print("\nSample valid prontuario values:")
print(valid_samples)

# Check some sample -1 values
minus_one_samples = con.execute("""
SELECT "Cliente", prontuario
FROM silver.diario_vendas
WHERE prontuario = -1
ORDER BY "Cliente"
LIMIT 10
""").fetchdf()

print("\nSample -1 prontuario values:")
print(minus_one_samples)

# Count total rows
total_count = con.execute("SELECT COUNT(*) FROM silver.diario_vendas").fetchdf()
print(f"\nTotal rows in silver.diario_vendas: {total_count.iloc[0,0]:,}")

con.close()
print("\nCheck completed.")
