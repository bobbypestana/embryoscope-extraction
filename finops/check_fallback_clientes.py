#!/usr/bin/env python3
import duckdb as db

# Connect to the database
con = db.connect('G:/My Drive/projetos_individuais/Huntington/database/huntington_data_lake.duckdb', read_only=True)

try:
    # Find examples where Cliente = prontuario (fallback cases)
    result = con.execute('''
        SELECT "Cliente", prontuario, COUNT(*) as count
        FROM silver.diario_vendas 
        WHERE "Cliente" = prontuario
        GROUP BY "Cliente", prontuario
        ORDER BY count DESC
        LIMIT 10
    ''').fetchdf()
    
    print('Examples of Cliente values that use themselves as prontuario (fallback cases):')
    print(result)
    print()
    
    # Count total fallback cases
    total_fallback = con.execute('''
        SELECT COUNT(DISTINCT "Cliente") as total_fallback_clientes
        FROM silver.diario_vendas 
        WHERE "Cliente" = prontuario
    ''').fetchone()[0]
    
    print(f'Total Cliente values using fallback (Cliente = prontuario): {total_fallback}')
    
    # Count total unique Cliente values
    total_clientes = con.execute('''
        SELECT COUNT(DISTINCT "Cliente") as total_clientes
        FROM silver.diario_vendas
    ''').fetchone()[0]
    
    print(f'Total unique Cliente values: {total_clientes}')
    print(f'Percentage using fallback: {(total_fallback/total_clientes)*100:.2f}%')
    
    # Check if any of these fallback cases exist in clinisys
    clinisys_con = db.connect('G:/My Drive/projetos_individuais/Huntington/database/clinisys_all.duckdb', read_only=True)
    
    # Get some fallback Cliente values to check
    fallback_clientes = con.execute('''
        SELECT DISTINCT "Cliente"
        FROM silver.diario_vendas 
        WHERE "Cliente" = prontuario
        LIMIT 5
    ''').fetchdf()
    
    print(f'\nChecking if some fallback Cliente values exist in clinisys:')
    for cliente in fallback_clientes['Cliente']:
        exists = clinisys_con.execute(f'''
            SELECT COUNT(*) as count 
            FROM silver.view_pacientes 
            WHERE codigo = {cliente}
        ''').fetchone()[0]
        print(f'Cliente {cliente}: {"✅ EXISTS" if exists > 0 else "❌ NOT FOUND"} in clinisys')
    
    clinisys_con.close()

finally:
    con.close()
