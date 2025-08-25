#!/usr/bin/env python3
import duckdb as db

# Connect to the database
con = db.connect('G:/My Drive/projetos_individuais/Huntington/database/huntington_data_lake.duckdb', read_only=True)

try:
    # Check for records with prontuario = -1
    result = con.execute('''
        SELECT "Cliente", prontuario, COUNT(*) as count 
        FROM silver.diario_vendas 
        WHERE prontuario = -1 
        GROUP BY "Cliente", prontuario 
        ORDER BY count DESC 
        LIMIT 10
    ''').fetchdf()
    
    print('Records with prontuario = -1 (unmatched):')
    print(result)
    
    # Count total records with prontuario = -1
    total = con.execute('''
        SELECT COUNT(*) as total 
        FROM silver.diario_vendas 
        WHERE prontuario = -1
    ''').fetchone()[0]
    
    print(f'\nTotal records with prontuario = -1: {total:,}')
    
    # Count unique Cliente values with prontuario = -1
    unique_clientes = con.execute('''
        SELECT COUNT(DISTINCT "Cliente") as unique_clientes 
        FROM silver.diario_vendas 
        WHERE prontuario = -1
    ''').fetchone()[0]
    
    print(f'Unique Cliente values with prontuario = -1: {unique_clientes:,}')

finally:
    con.close()
