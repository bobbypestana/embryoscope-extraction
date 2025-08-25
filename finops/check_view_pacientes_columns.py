#!/usr/bin/env python3
import duckdb as db

print("Checking view_pacientes column names...")

try:
    # Connect to clinisys database
    clinisys_con = db.connect('G:/My Drive/projetos_individuais/Huntington/database/clinisys_all.duckdb', read_only=True)
    
    # Get column information
    columns_info = clinisys_con.execute('''
        DESCRIBE silver.view_pacientes
    ''').fetchdf()
    
    print("Columns in silver.view_pacientes:")
    print(columns_info)
    
    # Get sample data to see actual values
    sample_data = clinisys_con.execute('''
        SELECT codigo, esposa_nome, marido_nome, unidade_origem
        FROM silver.view_pacientes 
        LIMIT 5
    ''').fetchdf()
    
    print("\nSample data:")
    print(sample_data)
    
    clinisys_con.close()
    
except Exception as e:
    print(f"Error: {e}")

print("Check completed.")
