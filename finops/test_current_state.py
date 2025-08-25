#!/usr/bin/env python3
import duckdb as db
import pandas as pd

print("Testing current prontuario mapping state...")

# Connect to the database
con = db.connect('G:/My Drive/projetos_individuais/Huntington/database/huntington_data_lake.duckdb', read_only=True)

# Check total rows
total_rows = con.execute('SELECT COUNT(*) FROM silver.diario_vendas').fetchdf()
print(f"Total rows in silver.diario_vendas: {total_rows.iloc[0,0]:,}")

# Check specific Cliente values
result = con.execute('''
    SELECT "Cliente", prontuario, COUNT(*) as count
    FROM silver.diario_vendas 
    WHERE "Cliente" IN (77785, 77536)
    GROUP BY "Cliente", prontuario
    ORDER BY "Cliente"
''').fetchdf()

print('\nCliente 77785 and 77536:')
print(result)

# Check prontuario distribution
prontuario_dist = con.execute('''
    SELECT prontuario, COUNT(*) as count
    FROM silver.diario_vendas 
    GROUP BY prontuario
    ORDER BY count DESC
    LIMIT 10
''').fetchdf()

print('\nTop 10 prontuario values by count:')
print(prontuario_dist)

# Check -1 values (unmatched)
minus_one_count = con.execute('''
    SELECT COUNT(*) as count
    FROM silver.diario_vendas 
    WHERE prontuario = -1
''').fetchdf()

print(f'\nRecords with prontuario = -1 (unmatched): {minus_one_count.iloc[0,0]:,}')

con.close()
print("\nTest completed.")
