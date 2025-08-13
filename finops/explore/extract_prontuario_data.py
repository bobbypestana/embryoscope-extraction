#!/usr/bin/env python3
"""
Extract Prontuario Data - Modular Functions
Extract data for a specific prontuario from finops tables with individual functions for each table.
"""

import pandas as pd
import duckdb as db
import os

def get_database_connection():
    """
    Create and return a connection to the clinisys_all database
    
    Returns:
        duckdb.DuckDBPyConnection: Database connection
    """
    path_to_db = '../../database/clinisys_all.duckdb'
    conn = db.connect(path_to_db, read_only=True)
    
    print(f"Connected to database: {path_to_db}")
    print(f"Database file exists: {os.path.exists(path_to_db)}")
    
    return conn

def extract_orcamentos_data(conn, target_prontuario):
    """
    Extract orcamentos data for a specific prontuario
    
    Args:
        conn: Database connection
        target_prontuario (int): The prontuario number to extract data for
        
    Returns:
        pd.DataFrame: DataFrame containing orcamentos data
    """
    print(f"\n2. EXTRACTING VIEW_ORCAMENTOS DATA")
    print("-" * 50)
    
    try:
        # Check schema first
        schema_orc = conn.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_schema = 'silver' AND table_name = 'view_orcamentos'
            ORDER BY ordinal_position
        """).fetchdf()
        print(f"view_orcamentos columns: {list(schema_orc['column_name'])}")
        
        # Extract data
        orcamentos_df = conn.execute(f"""
            SELECT * FROM silver.view_orcamentos 
            WHERE prontuario = {target_prontuario}
            ORDER BY id DESC
        """).fetchdf()
        
        print(f"Found {len(orcamentos_df)} records in view_orcamentos")
        if len(orcamentos_df) > 0:
            print("Sample data:")
            print(orcamentos_df.head())
            print(f"Columns: {list(orcamentos_df.columns)}")
        else:
            print("No data found for this prontuario")
            
    except Exception as e:
        print(f"Error extracting orcamentos data: {str(e)}")
        orcamentos_df = pd.DataFrame()
    
    return orcamentos_df

def extract_extrato_atendimento_central_data(conn, target_prontuario):
    """
    Extract extrato_atendimento_central data for a specific prontuario
    
    Args:
        conn: Database connection
        target_prontuario (int): The prontuario number to extract data for
        
    Returns:
        pd.DataFrame: DataFrame containing extrato_atendimento_central data
    """
    print(f"\n3. EXTRACTING VIEW_EXTRATO_ATENDIMENTO_CENTRAL DATA")
    print("-" * 50)
    
    try:
        # Check schema first
        schema_eac = conn.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_schema = 'silver' AND table_name = 'view_extrato_atendimentos_central'
            ORDER BY ordinal_position
        """).fetchdf()
        print(f"view_extrato_atendimento_central columns: {list(schema_eac['column_name'])}")
        
        # Extract data
        extrato_atendimento_central_df = conn.execute(f"""
            SELECT * FROM silver.view_extrato_atendimentos_central 
            WHERE prontuario = {target_prontuario}
            ORDER BY data DESC, inicio DESC
        """).fetchdf()
        
        print(f"Found {len(extrato_atendimento_central_df)} records in view_extrato_atendimento_central")
        if len(extrato_atendimento_central_df) > 0:
            print("Sample data:")
            print(extrato_atendimento_central_df.head())
            print(f"Columns: {list(extrato_atendimento_central_df.columns)}")
        else:
            print("No data found for this prontuario")
            
    except Exception as e:
        print(f"Error extracting extrato_atendimento_central data: {str(e)}")
        extrato_atendimento_central_df = pd.DataFrame()
    
    return extrato_atendimento_central_df

def extract_tratamentos_data(conn, target_prontuario):
    """
    Extract tratamentos data for a specific prontuario
    
    Args:
        conn: Database connection
        target_prontuario (int): The prontuario number to extract data for
        
    Returns:
        pd.DataFrame: DataFrame containing tratamentos data
    """
    print(f"\n4. EXTRACTING VIEW_TRATAMENTOS DATA")
    print("-" * 50)
    
    try:
        # Check schema first
        schema_trat = conn.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_schema = 'silver' AND table_name = 'view_tratamentos'
            ORDER BY ordinal_position
        """).fetchdf()
        print(f"view_tratamentos columns: {list(schema_trat['column_name'])}")
        
        # Extract data
        tratamentos_df = conn.execute(f"""
            SELECT * FROM silver.view_tratamentos 
            WHERE prontuario = {target_prontuario}
            ORDER BY id DESC
        """).fetchdf()
        
        print(f"Found {len(tratamentos_df)} records in view_tratamentos")
        if len(tratamentos_df) > 0:
            print("Sample data:")
            print(tratamentos_df.head())
            print(f"Columns: {list(tratamentos_df.columns)}")
        else:
            print("No data found for this prontuario")
            
    except Exception as e:
        print(f"Error extracting tratamentos data: {str(e)}")
        tratamentos_df = pd.DataFrame()
    
    return tratamentos_df

def extract_congelamentos_embrioes_data(conn, target_prontuario):
    """
    Extract congelamentos_embrioes data for a specific prontuario
    
    Args:
        conn: Database connection
        target_prontuario (int): The prontuario number to extract data for
        
    Returns:
        pd.DataFrame: DataFrame containing congelamentos_embrioes data
    """
    print(f"\n5. EXTRACTING VIEW_CONGELAMENTOS_EMBRIOES DATA")
    print("-" * 50)
    
    try:
        # Check schema first
        schema_ce = conn.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_schema = 'silver' AND table_name = 'view_congelamentos_embrioes'
            ORDER BY ordinal_position
        """).fetchdf()
        print(f"view_congelamentos_embrioes columns: {list(schema_ce['column_name'])}")
        
        # Extract data
        congelamentos_embrioes_df = conn.execute(f"""
            SELECT * FROM silver.view_congelamentos_embrioes 
            WHERE prontuario = {target_prontuario}
            ORDER BY id DESC
        """).fetchdf()
        
        print(f"Found {len(congelamentos_embrioes_df)} records in view_congelamentos_embrioes")
        if len(congelamentos_embrioes_df) > 0:
            print("Sample data:")
            print(congelamentos_embrioes_df.head())
            print(f"Columns: {list(congelamentos_embrioes_df.columns)}")
        else:
            print("No data found for this prontuario")
            
    except Exception as e:
        print(f"Error extracting congelamentos_embrioes data: {str(e)}")
        congelamentos_embrioes_df = pd.DataFrame()
    
    return congelamentos_embrioes_df

def extract_congelamentos_ovulos_data(conn, target_prontuario):
    """
    Extract congelamentos_ovulos data for a specific prontuario
    
    Args:
        conn: Database connection
        target_prontuario (int): The prontuario number to extract data for
        
    Returns:
        pd.DataFrame: DataFrame containing congelamentos_ovulos data
    """
    print(f"\n6. EXTRACTING VIEW_CONGELAMENTOS_OVULOS DATA")
    print("-" * 50)
    
    try:
        # Check schema first
        schema_co = conn.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_schema = 'silver' AND table_name = 'view_congelamentos_ovulos'
            ORDER BY ordinal_position
        """).fetchdf()
        print(f"view_congelamentos_ovulos columns: {list(schema_co['column_name'])}")
        
        # Extract data
        congelamentos_ovulos_df = conn.execute(f"""
            SELECT * FROM silver.view_congelamentos_ovulos 
            WHERE prontuario = {target_prontuario}
            ORDER BY id DESC
        """).fetchdf()
        
        print(f"Found {len(congelamentos_ovulos_df)} records in view_congelamentos_ovulos")
        if len(congelamentos_ovulos_df) > 0:
            print("Sample data:")
            print(congelamentos_ovulos_df.head())
            print(f"Columns: {list(congelamentos_ovulos_df.columns)}")
        else:
            print("No data found for this prontuario")
            
    except Exception as e:
        print(f"Error extracting congelamentos_ovulos data: {str(e)}")
        congelamentos_ovulos_df = pd.DataFrame()
    
    return congelamentos_ovulos_df

def extract_descongelamentos_embrioes_data(conn, target_prontuario):
    """
    Extract descongelamentos_embrioes data for a specific prontuario
    
    Args:
        conn: Database connection
        target_prontuario (int): The prontuario number to extract data for
        
    Returns:
        pd.DataFrame: DataFrame containing descongelamentos_embrioes data
    """
    print(f"\n7. EXTRACTING VIEW_DESCONGELAMENTOS_EMBRIOES DATA")
    print("-" * 50)
    
    try:
        # Check schema first
        schema_de = conn.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_schema = 'silver' AND table_name = 'view_descongelamentos_embrioes'
            ORDER BY ordinal_position
        """).fetchdf()
        print(f"view_descongelamentos_embrioes columns: {list(schema_de['column_name'])}")
        
        # Extract data
        descongelamentos_embrioes_df = conn.execute(f"""
            SELECT * FROM silver.view_descongelamentos_embrioes 
            WHERE prontuario = {target_prontuario}
            ORDER BY id DESC
        """).fetchdf()
        
        print(f"Found {len(descongelamentos_embrioes_df)} records in view_descongelamentos_embrioes")
        if len(descongelamentos_embrioes_df) > 0:
            print("Sample data:")
            print(descongelamentos_embrioes_df.head())
            print(f"Columns: {list(descongelamentos_embrioes_df.columns)}")
        else:
            print("No data found for this prontuario")
            
    except Exception as e:
        print(f"Error extracting descongelamentos_embrioes data: {str(e)}")
        descongelamentos_embrioes_df = pd.DataFrame()
    
    return descongelamentos_embrioes_df

def extract_descongelamentos_ovulos_data(conn, target_prontuario):
    """
    Extract descongelamentos_ovulos data for a specific prontuario
    
    Args:
        conn: Database connection
        target_prontuario (int): The prontuario number to extract data for
        
    Returns:
        pd.DataFrame: DataFrame containing descongelamentos_ovulos data
    """
    print(f"\n8. EXTRACTING VIEW_DESCONGELAMENTOS_OVULOS DATA")
    print("-" * 50)
    
    try:
        # Check schema first
        schema_do = conn.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_schema = 'silver' AND table_name = 'view_descongelamentos_ovulos'
            ORDER BY ordinal_position
        """).fetchdf()
        print(f"view_descongelamentos_ovulos columns: {list(schema_do['column_name'])}")
        
        # Extract data
        descongelamentos_ovulos_df = conn.execute(f"""
            SELECT * FROM silver.view_descongelamentos_ovulos 
            WHERE prontuario = {target_prontuario}
            ORDER BY id DESC
        """).fetchdf()
        
        print(f"Found {len(descongelamentos_ovulos_df)} records in view_descongelamentos_ovulos")
        if len(descongelamentos_ovulos_df) > 0:
            print("Sample data:")
            print(descongelamentos_ovulos_df.head())
            print(f"Columns: {list(descongelamentos_ovulos_df.columns)}")
        else:
            print("No data found for this prontuario")
            
    except Exception as e:
        print(f"Error extracting descongelamentos_ovulos data: {str(e)}")
        descongelamentos_ovulos_df = pd.DataFrame()
    
    return descongelamentos_ovulos_df

def extract_embrioes_congelados_data(conn, target_prontuario):
    """
    Extract embrioes_congelados data for a specific prontuario
    
    Args:
        conn: Database connection
        target_prontuario (int): The prontuario number to extract data for
        
    Returns:
        pd.DataFrame: DataFrame containing embrioes_congelados data
    """
    print(f"\n9. EXTRACTING VIEW_EMBRIOES_CONGELADOS DATA")
    print("-" * 50)
    
    try:
        # Check schema first
        schema_ec = conn.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_schema = 'silver' AND table_name = 'view_embrioes_congelados'
            ORDER BY ordinal_position
        """).fetchdf()
        print(f"view_embrioes_congelados columns: {list(schema_ec['column_name'])}")
        
        # Extract data
        embrioes_congelados_df = conn.execute(f"""
            SELECT * FROM silver.view_embrioes_congelados 
            WHERE prontuario = {target_prontuario}
            ORDER BY id DESC
        """).fetchdf()
        
        print(f"Found {len(embrioes_congelados_df)} records in view_embrioes_congelados")
        if len(embrioes_congelados_df) > 0:
            print("Sample data:")
            print(embrioes_congelados_df.head())
            print(f"Columns: {list(embrioes_congelados_df.columns)}")
        else:
            print("No data found for this prontuario")
            
    except Exception as e:
        print(f"Error extracting embrioes_congelados data: {str(e)}")
        embrioes_congelados_df = pd.DataFrame()
    
    return embrioes_congelados_df

def extract_prontuario_data(target_prontuario=876950):
    """
    Extract all data for a specific prontuario from the finops tables:
    - view_orcamentos  
    - view_extrato_atendimento_central
    - view_tratamentos
    - view_congelamentos_embrioes
    - view_congelamentos_ovulos
    - view_descongelamentos_embrioes
    - view_descongelamentos_ovulos
    - view_embrioes_congelados
    
    Args:
        target_prontuario (int): The prontuario number to extract data for
        
    Returns:
        tuple: Eight DataFrames containing the extracted data
    """
    
    print(f"\n{'='*80}")
    print(f"EXTRACTING DATA FOR PRONTUARIO: {target_prontuario}")
    print(f"{'='*80}")
    
    # Get database connection
    conn = get_database_connection()
    
    try:
        # Extract data from each table using individual functions
        orcamentos_df = extract_orcamentos_data(conn, target_prontuario)
        extrato_atendimento_central_df = extract_extrato_atendimento_central_data(conn, target_prontuario)
        tratamentos_df = extract_tratamentos_data(conn, target_prontuario)
        congelamentos_embrioes_df = extract_congelamentos_embrioes_data(conn, target_prontuario)
        congelamentos_ovulos_df = extract_congelamentos_ovulos_data(conn, target_prontuario)
        descongelamentos_embrioes_df = extract_descongelamentos_embrioes_data(conn, target_prontuario)
        descongelamentos_ovulos_df = extract_descongelamentos_ovulos_data(conn, target_prontuario)
        embrioes_congelados_df = extract_embrioes_congelados_data(conn, target_prontuario)
        
        # Summary
        print(f"\n{'='*80}")
        print(f"SUMMARY FOR PRONTUARIO {target_prontuario}")
        print(f"{'='*80}")
        print(f"view_orcamentos: {len(orcamentos_df)} records")
        print(f"view_extrato_atendimento_central: {len(extrato_atendimento_central_df)} records")
        print(f"view_tratamentos: {len(tratamentos_df)} records")
        print(f"view_congelamentos_embrioes: {len(congelamentos_embrioes_df)} records")
        print(f"view_congelamentos_ovulos: {len(congelamentos_ovulos_df)} records")
        print(f"view_descongelamentos_embrioes: {len(descongelamentos_embrioes_df)} records")
        print(f"view_descongelamentos_ovulos: {len(descongelamentos_ovulos_df)} records")
        print(f"view_embrioes_congelados: {len(embrioes_congelados_df)} records")
        
        print(f"\n{'='*80}")
        print("DATA EXTRACTION COMPLETE")
        print(f"{'='*80}")
        print("DataFrames available:")
        print("- orcamentos_df") 
        print("- extrato_atendimento_central_df")
        print("- tratamentos_df")
        print("- congelamentos_embrioes_df")
        print("- congelamentos_ovulos_df")
        print("- descongelamentos_embrioes_df")
        print("- descongelamentos_ovulos_df")
        print("- embrioes_congelados_df")
        
        return (orcamentos_df, extrato_atendimento_central_df, tratamentos_df, 
                congelamentos_embrioes_df, congelamentos_ovulos_df, 
                descongelamentos_embrioes_df, descongelamentos_ovulos_df, 
                embrioes_congelados_df)
        
    finally:
        # Close connection
        conn.close()

def find_in_diario_by_all_ids(prontuario: int) -> pd.DataFrame:
    """
    Find all records in diario_vendas by all ids in the patient_id list.
    
    Args:
        patient_id (int): The patient's id
        
    Returns:
        pd.DataFrame: DataFrame containing all diario_vendas records for the patient
    """
    CLINISYS_DB_PATH = '../../database/clinisys_all.duckdb'
    HUNTINGTON_DB_PATH = '../../database/huntington_data_lake.duckdb'
    
    try:
        # First, get all records from silver.view_pacientes where codigo=prontuario
        with duckdb.connect(CLINISYS_DB_PATH, read_only=True) as con:
            query_pacientes = f"""
            SELECT *
            FROM silver.view_pacientes
            WHERE codigo = {prontuario}
            """
            
            df_pacientes = con.execute(query_pacientes).df()
            
            if df_pacientes.empty:
                print(f"No records found in silver.view_pacientes for prontuario {prontuario}")
                return []
            
            print(f"Found {len(df_pacientes)} records in silver.view_pacientes for prontuario {prontuario}")
            print("Patient information:")            
            prontuario_columns = [col for col in df_pacientes.columns if 'prontuario' in col.lower()]
            print(f"Found prontuario columns: {prontuario_columns}")
            
            all_prontuarios = []
            for column in prontuario_columns:
                value = df_pacientes[column].iloc[0]
                # Check if value is not NaN and not None and not 0
                if pd.notna(value) and value is not None and value != 0:
                    all_prontuarios.append(int(value))
            
            # Remove duplicates
            codigos = list(set(all_prontuarios))
            print(f"Valid prontuario codes (no NaNs): {codigos}")


            # Then, get all records from diario_vendas for these codigos
            with duckdb.connect(HUNTINGTON_DB_PATH, read_only=True) as con:
                # Create the IN clause for the query
                codigos_str = ','.join(map(str, codigos))
                
                query_diario = f"""
                SELECT *
                FROM silver.diario_vendas
                WHERE "Cliente" IN ({codigos_str})
                ORDER BY "DT Emissao" DESC
                """
                
                df_diario = con.execute(query_diario).df()
                
                if df_diario.empty:
                    print(f"No records found in silver.diario_vendas for codigos {codigos}")
                    return df_diario
                
                print(f"Found {len(df_diario)} records in silver.diario_vendas for codigos {codigos}")
                return df_diario
            

    except Exception as e:
        print(f"Error retrieving diario_vendas data for prontuario {prontuario}: {e}")
        return pd.DataFrame()

if __name__ == "__main__":
    # Example usage
    (orcamentos_df, extrato_atendimento_central_df, tratamentos_df, 
     congelamentos_embrioes_df, congelamentos_ovulos_df, 
     descongelamentos_embrioes_df, descongelamentos_ovulos_df, 
     embrioes_congelados_df) = extract_prontuario_data(876950)
    
    # You can now work with the DataFrames
    print("\n DataFrames loaded successfully!")
    print(f"orcamentos_df shape: {orcamentos_df.shape}")
    print(f"extrato_atendimento_central_df shape: {extrato_atendimento_central_df.shape}")
    print(f"tratamentos_df shape: {tratamentos_df.shape}")
    print(f"congelamentos_embrioes_df shape: {congelamentos_embrioes_df.shape}")
    print(f"congelamentos_ovulos_df shape: {congelamentos_ovulos_df.shape}")
    print(f"descongelamentos_embrioes_df shape: {descongelamentos_embrioes_df.shape}")
    print(f"descongelamentos_ovulos_df shape: {descongelamentos_ovulos_df.shape}")
    print(f"embrioes_congelados_df shape: {embrioes_congelados_df.shape}")
    
    # Example of using the diario function
    diario_df = find_in_diario_by_all_ids(876950)
    print(f"diario_df shape: {diario_df.shape}") 