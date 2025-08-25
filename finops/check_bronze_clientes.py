#!/usr/bin/env python3
import duckdb as db

# Connect to the database
con = db.connect('G:/My Drive/projetos_individuais/Huntington/database/huntington_data_lake.duckdb', read_only=True)

try:
    # Check if Cliente 777536 exists in bronze
    result_bronze = con.execute('''
        SELECT "Cliente", COUNT(*) as count 
        FROM bronze.diario_vendas 
        WHERE "Cliente" = 777536 
        GROUP BY "Cliente"
    ''').fetchdf()
    
    print('Cliente 777536 in bronze:')
    print(result_bronze)
    
    # Check if Cliente 777536 exists in silver
    result_silver = con.execute('''
        SELECT "Cliente", prontuario, COUNT(*) as count 
        FROM silver.diario_vendas 
        WHERE "Cliente" = 777536 
        GROUP BY "Cliente", prontuario
    ''').fetchdf()
    
    print('\nCliente 777536 in silver:')
    print(result_silver)
    
    # Check if Cliente 777536 exists in clinisys (using separate connection)
    clinisys_con = db.connect('G:/My Drive/projetos_individuais/Huntington/database/clinisys_all.duckdb', read_only=True)
    result_clinisys = clinisys_con.execute('''
        SELECT codigo, prontuario_esposa, prontuario_marido 
        FROM silver.view_pacientes 
        WHERE codigo = 777536
    ''').fetchdf()
    clinisys_con.close()
    
    print('\nCliente 777536 in clinisys:')
    print(result_clinisys)

finally:
    con.close()
