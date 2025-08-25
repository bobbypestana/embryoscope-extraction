#!/usr/bin/env python3
"""
Check silver.diario_vendas table
"""

import duckdb as db
import pandas as pd

def check_diario_vendas():
    """Check the silver.diario_vendas table"""
    
    # Connect to the database
    con = db.connect('../../database/huntington_data_lake.duckdb', read_only=True)
    
    try:
        print('=== SILVER.DIARIO_VENDAS TABLE INFO ===')
        print()
        
        # Get table info
        table_info = con.execute('DESCRIBE silver.diario_vendas').fetchdf()
        print('Table structure:')
        print(table_info)
        print()
        
        # Get row count
        count = con.execute('SELECT COUNT(*) FROM silver.diario_vendas').fetchone()[0]
        print(f'Total rows: {count:,}')
        print()
        
        # Sample data
        sample = con.execute('SELECT * FROM silver.diario_vendas LIMIT 5').fetchdf()
        print('Sample data (first 5 rows):')
        print(sample[['Cliente', 'prontuario', 'Nome', 'DT Emissao']].to_string())
        print()
        
        # Check unique Cliente and prontuario combinations
        unique_mappings = con.execute('''
            SELECT "Cliente", prontuario, COUNT(*) as count
            FROM silver.diario_vendas 
            GROUP BY "Cliente", prontuario
            ORDER BY count DESC
            LIMIT 10
        ''').fetchdf()
        print('Top 10 Cliente-prontuario mappings:')
        print(unique_mappings)
        print()
        
        # Check specific Cliente values if they exist
        specific_clientes = con.execute('''
            SELECT "Cliente", prontuario, COUNT(*) as count
            FROM silver.diario_vendas 
            WHERE "Cliente" IN (77785, 777536)
            GROUP BY "Cliente", prontuario
            ORDER BY "Cliente"
        ''').fetchdf()
        print('Specific Cliente values (77785, 777536):')
        print(specific_clientes)
        print()
        
        # Check prontuario 175583
        prontuario_175583 = con.execute('''
            SELECT "Cliente", prontuario, COUNT(*) as count
            FROM silver.diario_vendas 
            WHERE prontuario = 175583
            GROUP BY "Cliente", prontuario
            ORDER BY "Cliente"
        ''').fetchdf()
        print('Records with prontuario = 175583:')
        print(prontuario_175583)
        print()
        
    finally:
        con.close()

if __name__ == "__main__":
    check_diario_vendas()
