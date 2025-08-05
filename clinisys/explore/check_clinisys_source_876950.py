import duckdb
import pandas as pd

def check_clinisys_source_876950():
    """Check if PatientID 876950 exists in the clinisys source database."""
    
    print("=== Checking Clinisys Source Database for PatientID 876950 ===")
    
    try:
        db = duckdb.connect('../../database/clinisys_all.duckdb')
        
        print("Connected to clinisys_all.duckdb")
        
        # 1. Check what tables exist
        print(f"\n1. TABLES IN CLINISYS DATABASE")
        print("-" * 50)
        
        tables = db.execute("SHOW TABLES").fetchall()
        for table in tables:
            print(f"  {table[0]}")
        
        # 2. Check silver tables
        print(f"\n2. SILVER TABLES")
        print("-" * 50)
        
        silver_tables = [table for table in tables if table[0].startswith('view_')]
        for table in silver_tables:
            print(f"  {table[0]}")
        
        # 3. Check for PatientID 876950 in view_micromanipulacao
        print(f"\n3. CHECKING view_micromanipulacao FOR PRONTUARIO 876950")
        print("-" * 50)
        
        try:
            micromanip_query = """
            SELECT * FROM view_micromanipulacao 
            WHERE prontuario = 876950
            """
            
            micromanip_df = db.execute(micromanip_query).df()
            
            if len(micromanip_df) > 0:
                print(f"✅ Found {len(micromanip_df)} records in view_micromanipulacao")
                print(f"Columns: {list(micromanip_df.columns)}")
                print(f"\nSample data:")
                print(micromanip_df.head())
            else:
                print("❌ No records found in view_micromanipulacao")
        except Exception as e:
            print(f"Error querying view_micromanipulacao: {e}")
        
        # 4. Check for PatientID 876950 in view_pacientes
        print(f"\n4. CHECKING view_pacientes FOR PRONTUARIO 876950")
        print("-" * 50)
        
        try:
            pacientes_query = """
            SELECT * FROM view_pacientes 
            WHERE prontuario = 876950
            """
            
            pacientes_df = db.execute(pacientes_query).df()
            
            if len(pacientes_df) > 0:
                print(f"✅ Found {len(pacientes_df)} records in view_pacientes")
                print(f"Columns: {list(pacientes_df.columns)}")
                print(f"\nSample data:")
                print(pacientes_df.head())
            else:
                print("❌ No records found in view_pacientes")
        except Exception as e:
            print(f"Error querying view_pacientes: {e}")
        
        # 5. Check all silver tables for prontuario 876950
        print(f"\n5. CHECKING ALL SILVER TABLES FOR PRONTUARIO 876950")
        print("-" * 50)
        
        for table in silver_tables:
            table_name = table[0]
            print(f"\nChecking {table_name}...")
            
            try:
                # First check if the table has a prontuario column
                schema_query = f"DESCRIBE {table_name}"
                schema_df = db.execute(schema_query).df()
                
                has_prontuario = any('prontuario' in col.lower() for col in schema_df['column_name'])
                
                if has_prontuario:
                    query = f"SELECT COUNT(*) as count FROM {table_name} WHERE prontuario = 876950"
                    result = db.execute(query).fetchone()
                    count = result[0] if result else 0
                    
                    if count > 0:
                        print(f"  ✅ Found {count} records in {table_name}")
                        
                        # Get sample data
                        sample_query = f"SELECT * FROM {table_name} WHERE prontuario = 876950 LIMIT 3"
                        sample_df = db.execute(sample_query).df()
                        print(f"  Sample columns: {list(sample_df.columns)}")
                    else:
                        print(f"  ❌ No records found in {table_name}")
                else:
                    print(f"  ⚠️ No prontuario column in {table_name}")
                    
            except Exception as e:
                print(f"  Error checking {table_name}: {e}")
        
        # 6. Check if there are any records with similar prontuario
        print(f"\n6. CHECKING FOR SIMILAR PRONTUARIOS")
        print("-" * 50)
        
        try:
            similar_query = """
            SELECT DISTINCT prontuario 
            FROM view_micromanipulacao 
            WHERE CAST(prontuario AS VARCHAR) LIKE '%876%'
            ORDER BY prontuario
            LIMIT 10
            """
            
            similar_df = db.execute(similar_query).df()
            
            if len(similar_df) > 0:
                print(f"Found {len(similar_df)} similar prontuarios:")
                for _, row in similar_df.iterrows():
                    print(f"  {row['prontuario']}")
            else:
                print("No similar prontuarios found")
        except Exception as e:
            print(f"Error checking similar prontuarios: {e}")
        
        db.close()
        
        print(f"\n=== Check completed ===")
        
    except Exception as e:
        print(f"Error: {e}")
        raise

if __name__ == "__main__":
    check_clinisys_source_876950() 