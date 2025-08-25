#!/usr/bin/env python3
import duckdb as db

# Connect to the database
con = db.connect('G:/My Drive/projetos_individuais/Huntington/database/huntington_data_lake.duckdb', read_only=True)

# Check the specific Cliente values
result = con.execute('''
    SELECT "Cliente", prontuario, COUNT(*) as count
    FROM silver.diario_vendas 
    WHERE "Cliente" IN (77785, 77536)
    GROUP BY "Cliente", prontuario
    ORDER BY "Cliente"
''').fetchdf()

print('Cliente 77785 and 77536:')
print(result)

# Check if prontuario 175583 exists
result2 = con.execute('''
    SELECT "Cliente", prontuario, COUNT(*) as count
    FROM silver.diario_vendas 
    WHERE prontuario = 175583
    GROUP BY "Cliente", prontuario
    ORDER BY "Cliente"
''').fetchdf()

print('\nAll records with prontuario = 175583:')
print(result2)

# Check if both Cliente values have the expected prontuario
expected_prontuario = 175583
cliente_77785 = result[result['Cliente'] == 77785]
cliente_77536 = result[result['Cliente'] == 77536]

print(f'\nVerification:')
print(f'Cliente 77785 has prontuario = {cliente_77785["prontuario"].iloc[0] if not cliente_77785.empty else "NOT FOUND"} (expected: {expected_prontuario})')
print(f'Cliente 77536 has prontuario = {cliente_77536["prontuario"].iloc[0] if not cliente_77536.empty else "NOT FOUND"} (expected: {expected_prontuario})')

# Check if both match the expected value
both_correct = (
    not cliente_77785.empty and cliente_77785['prontuario'].iloc[0] == expected_prontuario and
    not cliente_77536.empty and cliente_77536['prontuario'].iloc[0] == expected_prontuario
)

print(f'\nBoth Cliente values have prontuario = {expected_prontuario}: {"✅ YES" if both_correct else "❌ NO"}')

con.close()
