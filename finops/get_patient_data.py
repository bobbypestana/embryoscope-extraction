#!/usr/bin/env python3
"""
Function to retrieve all data for a given prontuario from silver.diario_vendas
Searches in both prontuario and prontuario_1 columns
"""

import duckdb
import pandas as pd
import os
from typing import Optional

# Configuration
DUCKDB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'database', 'huntington_data_lake.duckdb')

def get_patient_data(prontuario: int, include_metadata: bool = False) -> pd.DataFrame:
    """
    Retrieve all data for a given prontuario from silver.diario_vendas table.
    
    Args:
        prontuario (int): The patient ID to search for
        include_metadata (bool): Whether to include metadata columns (line_number, extraction_timestamp, file_name)
    
    Returns:
        pd.DataFrame: DataFrame containing all records for the patient
        
    Example:
        # Get all data for patient 123456
        df = get_patient_data(123456)
        
        # Get data with metadata columns
        df = get_patient_data(123456, include_metadata=True)
    """
    
    try:
        # Connect to DuckDB
        con = duckdb.connect(DUCKDB_PATH)
        
        # Build the query
        if include_metadata:
            select_columns = "*"
        else:
            select_columns = """
                "Cliente", "Loja", "Nome", "Nom Paciente", "DT Emissao",
                "Tipo da nota", "Numero", "Serie Docto.", "NF Eletr.", "Vend. 1",
                "Médico", "Cliente.1", "Opr ", "Operador", "Produto", "Descricao",
                "Qntd.", "Valor Unitario", "Valor Mercadoria", "Total", "Custo",
                "Custo Unit", "Desconto", "Unidade", "Mês", "Ano", "Cta-Ctbl",
                "Cta-Ctbl Eugin", "Interno/Externo", "Descrição Gerencial",
                "Descrição Mapping Actividad", "Ciclos", "Qnt Cons.",
                prontuario, prontuario_1
            """
        
        query = f"""
        SELECT {select_columns}
        FROM silver.diario_vendas
        WHERE prontuario = {prontuario} OR prontuario_1 = {prontuario}
        ORDER BY "DT Emissao" DESC, "Numero" DESC
        """
        
        # Execute query
        df = con.execute(query).df()
        
        # Close connection
        con.close()
        
        # Print summary
        print(f"Found {len(df)} records for prontuario {prontuario}")
        
        if len(df) > 0:
            print(f"Date range: {df['DT Emissao'].min()} to {df['DT Emissao'].max()}")
            print(f"Total value: R$ {df['Total'].sum():,.2f}")
            print(f"Number of unique documents: {df['Numero'].nunique()}")
        
        return df
        
    except Exception as e:
        print(f"Error retrieving data for prontuario {prontuario}: {e}")
        return pd.DataFrame()

def get_patient_summary(prontuario: int) -> dict:
    """
    Get a summary of patient data without returning the full DataFrame.
    
    Args:
        prontuario (int): The patient ID to search for
    
    Returns:
        dict: Summary statistics for the patient
    """
    
    try:
        # Connect to DuckDB
        con = duckdb.connect(DUCKDB_PATH)
        
        query = f"""
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT "Numero") as unique_documents,
            MIN("DT Emissao") as first_date,
            MAX("DT Emissao") as last_date,
            SUM("Total") as total_value,
            AVG("Total") as avg_value,
            COUNT(DISTINCT "Produto") as unique_products,
            COUNT(DISTINCT "Médico") as unique_doctors,
            COUNT(CASE WHEN prontuario = {prontuario} THEN 1 END) as records_in_prontuario,
            COUNT(CASE WHEN prontuario_1 = {prontuario} THEN 1 END) as records_in_prontuario_1
        FROM silver.diario_vendas
        WHERE prontuario = {prontuario} OR prontuario_1 = {prontuario}
        """
        
        # Execute query
        result = con.execute(query).df()
        
        # Close connection
        con.close()
        
        if len(result) > 0:
            summary = result.iloc[0].to_dict()
            summary['prontuario'] = prontuario
            return summary
        else:
            return {'prontuario': prontuario, 'total_records': 0}
            
    except Exception as e:
        print(f"Error retrieving summary for prontuario {prontuario}: {e}")
        return {'prontuario': prontuario, 'error': str(e)}

def search_patients_by_name(name_pattern: str, limit: int = 10) -> pd.DataFrame:
    """
    Search for patients by name pattern in the silver.diario_vendas table.
    
    Args:
        name_pattern (str): Name pattern to search for (supports partial matches)
        limit (int): Maximum number of results to return
    
    Returns:
        pd.DataFrame: DataFrame with patient information
    """
    
    try:
        # Connect to DuckDB
        con = duckdb.connect(DUCKDB_PATH)
        
        query = f"""
        SELECT DISTINCT
            prontuario,
            prontuario_1,
            "Nome",
            "Nom Paciente",
            COUNT(*) as total_records,
            SUM("Total") as total_value,
            MIN("DT Emissao") as first_date,
            MAX("DT Emissao") as last_date
        FROM silver.diario_vendas
        WHERE "Nome" ILIKE '%{name_pattern}%' 
           OR "Nom Paciente" ILIKE '%{name_pattern}%'
        GROUP BY prontuario, prontuario_1, "Nome", "Nom Paciente"
        ORDER BY total_value DESC
        LIMIT {limit}
        """
        
        # Execute query
        df = con.execute(query).df()
        
        # Close connection
        con.close()
        
        print(f"Found {len(df)} patients matching '{name_pattern}'")
        return df
        
    except Exception as e:
        print(f"Error searching for patients with pattern '{name_pattern}': {e}")
        return pd.DataFrame()

# Example usage
if __name__ == "__main__":
    # Example 1: Get all data for a specific prontuario
    print("=== Example 1: Get patient data ===")
    df = get_patient_data(123456)
    if len(df) > 0:
        print(df.head())
    
    # Example 2: Get patient summary
    print("\n=== Example 2: Get patient summary ===")
    summary = get_patient_summary(123456)
    print(summary)
    
    # Example 3: Search patients by name
    print("\n=== Example 3: Search patients by name ===")
    patients = search_patients_by_name("Silva", limit=5)
    if len(patients) > 0:
        print(patients) 