#!/usr/bin/env python3
import duckdb as db
import pandas as pd

# Connect to clinisys database
con = db.connect('G:/My Drive/projetos_individuais/Huntington/database/clinisys_all.duckdb', read_only=True)

try:
    # Check table structure
    print("=== CLINISYS DATABASE STRUCTURE ===")
    
    # Get column info
    columns_info = con.execute("DESCRIBE silver.view_pacientes").fetchdf()
    print("\nColumns in silver.view_pacientes:")
    print(columns_info)
    
    # Get sample data
    sample = con.execute("SELECT * FROM silver.view_pacientes LIMIT 3").fetchdf()
    print("\nSample data (first 3 rows):")
    print(sample)
    
    # Check if codigo column exists
    if 'codigo' in sample.columns:
        print(f"\n✅ 'codigo' column exists")
        print(f"Sample codigo values: {sample['codigo'].tolist()}")
    else:
        print(f"\n❌ 'codigo' column does not exist")
        print(f"Available columns: {list(sample.columns)}")
    
    # Check prontuario-related columns
    prontuario_cols = [col for col in sample.columns if 'prontuario' in col.lower()]
    print(f"\nProntuario-related columns: {prontuario_cols}")
    
    # Test the specific Cliente values
    print(f"\n=== TESTING SPECIFIC CLIENTE VALUES ===")
    
    # Check if Cliente 77785 exists
    result_77785 = con.execute("""
        SELECT * FROM silver.view_pacientes 
        WHERE codigo = 77785
    """).fetchdf()
    
    print(f"Cliente 77785 found: {len(result_77785) > 0}")
    if len(result_77785) > 0:
        print(f"77785 data: {result_77785.iloc[0].to_dict()}")
    
    # Check if Cliente 777536 exists
    result_777536 = con.execute("""
        SELECT * FROM silver.view_pacientes 
        WHERE codigo = 777536
    """).fetchdf()
    
    print(f"Cliente 777536 found: {len(result_777536) > 0}")
    if len(result_777536) > 0:
        print(f"777536 data: {result_777536.iloc[0].to_dict()}")
    
    # Check if prontuario 175583 exists
    result_175583 = con.execute("""
        SELECT * FROM silver.view_pacientes 
        WHERE codigo = 175583
    """).fetchdf()
    
    print(f"Prontuario 175583 found: {len(result_175583) > 0}")
    if len(result_175583) > 0:
        print(f"175583 data: {result_175583.iloc[0].to_dict()}")

finally:
    con.close()
