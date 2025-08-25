#!/usr/bin/env python3
import duckdb as db
import pandas as pd

print("Analyzing NULL prontuario values...")

# Connect to the database
con = db.connect('G:/My Drive/projetos_individuais/Huntington/database/huntington_data_lake.duckdb', read_only=True)

# Check NULL prontuario records
null_records = con.execute('''
    SELECT "Cliente", prontuario, "Nome", "Nom Paciente", COUNT(*) as count
    FROM silver.diario_vendas 
    WHERE prontuario IS NULL
    GROUP BY "Cliente", prontuario, "Nome", "Nom Paciente"
    ORDER BY count DESC
    LIMIT 10
''').fetchdf()

print('\nTop 10 NULL prontuario records:')
print(null_records)

# Check Cliente values that have NULL prontuario
null_clientes = con.execute('''
    SELECT "Cliente", COUNT(*) as count
    FROM silver.diario_vendas 
    WHERE prontuario IS NULL
    GROUP BY "Cliente"
    ORDER BY count DESC
    LIMIT 10
''').fetchdf()

print('\nTop 10 Cliente values with NULL prontuario:')
print(null_clientes)

# Check if these Cliente values exist in clinisys
if not null_clientes.empty:
    top_cliente = null_clientes.iloc[0]['Cliente']
    print(f'\nChecking if Cliente {top_cliente} exists in clinisys...')
    
    try:
        clinisys_con = db.connect('G:/My Drive/projetos_individuais/Huntington/database/clinisys_all.duckdb', read_only=True)
        
        # Check in view_pacientes
        clinisys_check = clinisys_con.execute(f'''
            SELECT codigo, prontuario_esposa, prontuario_marido, prontuario_responsavel1
            FROM silver.view_pacientes 
            WHERE codigo = {top_cliente} 
               OR prontuario_esposa = {top_cliente}
               OR prontuario_marido = {top_cliente}
               OR prontuario_responsavel1 = {top_cliente}
        ''').fetchdf()
        
        print(f'Found in clinisys: {len(clinisys_check)} records')
        if not clinisys_check.empty:
            print(clinisys_check)
        
        clinisys_con.close()
        
    except Exception as e:
        print(f"Error checking clinisys: {e}")

# Check data types
schema_info = con.execute('''
    DESCRIBE silver.diario_vendas
''').fetchdf()

print('\nTable schema:')
print(schema_info)

con.close()
print("\nAnalysis completed.")
