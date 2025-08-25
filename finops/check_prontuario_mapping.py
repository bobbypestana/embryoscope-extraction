#!/usr/bin/env python3
"""
Check prontuario mapping for specific Cliente values
"""

import duckdb as db
import pandas as pd

def check_prontuario_mapping():
    """Check if Cliente 77785 and 777536 both have prontuario = 175583"""
    
    # Connect to the database
    con = db.connect('../../database/huntington_data_lake.duckdb', read_only=True)
    
    try:
        # Check the specific Cliente values
        query = '''
        SELECT "Cliente", prontuario, COUNT(*) as count
        FROM silver.diario_vendas 
        WHERE "Cliente" IN (77785, 777536)
        GROUP BY "Cliente", prontuario
        ORDER BY "Cliente"
        '''
        
        result = con.execute(query).fetchdf()
        print('Results for Cliente 77785 and 777536:')
        print(result)
        
        # Also check if prontuario 175583 exists
        query2 = '''
        SELECT "Cliente", prontuario, COUNT(*) as count
        FROM silver.diario_vendas 
        WHERE prontuario = 175583
        GROUP BY "Cliente", prontuario
        ORDER BY "Cliente"
        '''
        
        result2 = con.execute(query2).fetchdf()
        print('\nAll records with prontuario = 175583:')
        print(result2)
        
        # Check if both Cliente values have the expected prontuario
        expected_prontuario = 175583
        cliente_77785 = result[result['Cliente'] == 77785]
        cliente_777536 = result[result['Cliente'] == 777536]
        
        print(f'\nVerification:')
        print(f'Cliente 77785 has prontuario = {cliente_77785["prontuario"].iloc[0] if not cliente_77785.empty else "NOT FOUND"} (expected: {expected_prontuario})')
        print(f'Cliente 777536 has prontuario = {cliente_777536["prontuario"].iloc[0] if not cliente_777536.empty else "NOT FOUND"} (expected: {expected_prontuario})')
        
        # Check if both match the expected value
        both_correct = (
            not cliente_77785.empty and cliente_77785['prontuario'].iloc[0] == expected_prontuario and
            not cliente_777536.empty and cliente_777536['prontuario'].iloc[0] == expected_prontuario
        )
        
        print(f'\nBoth Cliente values have prontuario = {expected_prontuario}: {"✅ YES" if both_correct else "❌ NO"}')
        
    finally:
        con.close()

if __name__ == "__main__":
    check_prontuario_mapping()
