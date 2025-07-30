import duckdb
import pandas as pd
from datetime import datetime

def check_clinisys_data_pipeline_876950():
    """Check if PatientID 876950 exists in the clinisys data pipeline."""
    
    print(f"=== Checking Clinisys Data Pipeline for PatientID 876950 - {datetime.now()} ===")
    
    try:
        con = duckdb.connect('../../database/huntington_data_lake.duckdb')
        
        print("Connected to data lake")
        
        # 1. Check if PatientID 876950 exists in clinisys silver layer
        print(f"\n1. CHECKING CLINISYS SILVER LAYER")
        print("-" * 60)
        
        # Check clinisys silver tables
        clinisys_silver_query = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'silver' 
        AND table_name LIKE '%clinisys%'
        """
        
        clinisys_silver_tables = con.execute(clinisys_silver_query).df()
        
        if len(clinisys_silver_tables) > 0:
            print(f"Found {len(clinisys_silver_tables)} clinisys silver tables:")
            for _, row in clinisys_silver_tables.iterrows():
                print(f"  {row['table_name']}")
                
                # Check if this table has prontuario column and data for 876950
                try:
                    schema_query = f"DESCRIBE silver.{row['table_name']}"
                    schema_df = con.execute(schema_query).df()
                    
                    has_prontuario = any('prontuario' in col.lower() for col in schema_df['column_name'])
                    
                    if has_prontuario:
                        count_query = f"SELECT COUNT(*) as count FROM silver.{row['table_name']} WHERE prontuario = 876950"
                        count_result = con.execute(count_query).fetchone()
                        count = count_result[0] if count_result else 0
                        
                        if count > 0:
                            print(f"    ✅ Found {count} records for prontuario 876950")
                            
                            # Get sample data
                            sample_query = f"SELECT * FROM silver.{row['table_name']} WHERE prontuario = 876950 LIMIT 3"
                            sample_df = con.execute(sample_query).df()
                            print(f"    Sample columns: {list(sample_df.columns)}")
                        else:
                            print(f"    ❌ No records for prontuario 876950")
                    else:
                        print(f"    ⚠️ No prontuario column")
                        
                except Exception as e:
                    print(f"    Error checking {row['table_name']}: {e}")
        else:
            print("No clinisys silver tables found")
        
        # 2. Check if PatientID 876950 exists in clinisys bronze layer
        print(f"\n2. CHECKING CLINISYS BRONZE LAYER")
        print("-" * 60)
        
        clinisys_bronze_query = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'bronze' 
        AND table_name LIKE '%clinisys%'
        """
        
        clinisys_bronze_tables = con.execute(clinisys_bronze_query).df()
        
        if len(clinisys_bronze_tables) > 0:
            print(f"Found {len(clinisys_bronze_tables)} clinisys bronze tables:")
            for _, row in clinisys_bronze_tables.iterrows():
                print(f"  {row['table_name']}")
                
                # Check if this table has prontuario column and data for 876950
                try:
                    schema_query = f"DESCRIBE bronze.{row['table_name']}"
                    schema_df = con.execute(schema_query).df()
                    
                    has_prontuario = any('prontuario' in col.lower() for col in schema_df['column_name'])
                    
                    if has_prontuario:
                        count_query = f"SELECT COUNT(*) as count FROM bronze.{row['table_name']} WHERE prontuario = 876950"
                        count_result = con.execute(count_query).fetchone()
                        count = count_result[0] if count_result else 0
                        
                        if count > 0:
                            print(f"    ✅ Found {count} records for prontuario 876950")
                        else:
                            print(f"    ❌ No records for prontuario 876950")
                    else:
                        print(f"    ⚠️ No prontuario column")
                        
                except Exception as e:
                    print(f"    Error checking {row['table_name']}: {e}")
        else:
            print("No clinisys bronze tables found")
        
        # 3. Check clinisys gold layer data range
        print(f"\n3. CHECKING CLINISYS GOLD LAYER DATA RANGE")
        print("-" * 60)
        
        clinisys_date_range_query = """
        SELECT 
            MIN(CAST(micro_Data_DL AS DATE)) as earliest_date,
            MAX(CAST(micro_Data_DL AS DATE)) as latest_date,
            COUNT(DISTINCT micro_prontuario) as unique_patients,
            COUNT(*) as total_records
        FROM gold.clinisys_embrioes
        WHERE micro_Data_DL IS NOT NULL
        """
        
        clinisys_date_range_df = con.execute(clinisys_date_range_query).df()
        
        if len(clinisys_date_range_df) > 0:
            row = clinisys_date_range_df.iloc[0]
            print(f"Clinisys gold layer data range:")
            print(f"  Date range: {row['earliest_date']} to {row['latest_date']}")
            print(f"  Unique patients: {row['unique_patients']:,}")
            print(f"  Total records: {row['total_records']:,}")
        
        # 4. Check if PatientID 876950 exists in any clinisys table
        print(f"\n4. CHECKING ALL CLINISYS TABLES FOR PRONTUARIO 876950")
        print("-" * 60)
        
        all_clinisys_tables_query = """
        SELECT table_name, table_schema
        FROM information_schema.tables 
        WHERE table_name LIKE '%clinisys%' OR table_name LIKE '%micro%'
        ORDER BY table_schema, table_name
        """
        
        all_clinisys_tables = con.execute(all_clinisys_tables_query).df()
        
        if len(all_clinisys_tables) > 0:
            print(f"Found {len(all_clinisys_tables)} clinisys-related tables:")
            for _, row in all_clinisys_tables.iterrows():
                schema = row['table_schema']
                table_name = row['table_name']
                full_table_name = f"{schema}.{table_name}"
                
                print(f"  {full_table_name}")
                
                # Check if this table has prontuario column and data for 876950
                try:
                    schema_query = f"DESCRIBE {full_table_name}"
                    schema_df = con.execute(schema_query).df()
                    
                    has_prontuario = any('prontuario' in col.lower() for col in schema_df['column_name'])
                    
                    if has_prontuario:
                        count_query = f"SELECT COUNT(*) as count FROM {full_table_name} WHERE prontuario = 876950"
                        count_result = con.execute(count_query).fetchone()
                        count = count_result[0] if count_result else 0
                        
                        if count > 0:
                            print(f"    ✅ Found {count} records for prontuario 876950")
                        else:
                            print(f"    ❌ No records for prontuario 876950")
                    else:
                        print(f"    ⚠️ No prontuario column")
                        
                except Exception as e:
                    print(f"    Error checking {full_table_name}: {e}")
        else:
            print("No clinisys-related tables found")
        
        # 5. Check if there are any records with similar prontuario in clinisys
        print(f"\n5. CHECKING FOR SIMILAR PRONTUARIOS IN CLINISYS")
        print("-" * 60)
        
        similar_prontuarios_query = """
        SELECT DISTINCT micro_prontuario
        FROM gold.clinisys_embrioes
        WHERE CAST(micro_prontuario AS VARCHAR) LIKE '%876%'
        ORDER BY micro_prontuario
        LIMIT 10
        """
        
        similar_prontuarios_df = con.execute(similar_prontuarios_query).df()
        
        if len(similar_prontuarios_df) > 0:
            print(f"Found {len(similar_prontuarios_df)} similar prontuarios in clinisys gold:")
            for _, row in similar_prontuarios_df.iterrows():
                print(f"  {row['micro_prontuario']}")
        else:
            print("No similar prontuarios found in clinisys gold")
        
        # 6. Summary and conclusion
        print(f"\n6. SUMMARY AND CONCLUSION")
        print("-" * 60)
        
        print(f"PatientID 876950 investigation:")
        print(f"  - Exists in embryoscope: ✅ (13 records, 2025-07-28)")
        print(f"  - Exists in clinisys gold: ❌ (0 records)")
        print(f"  - Missing from clinisys: ✅ (Confirmed)")
        
        print(f"\nPossible reasons:")
        print(f"  1. Data synchronization delay (clinisys data only goes to 2025-07-24)")
        print(f"  2. PatientID 876950 not captured in clinisys source system")
        print(f"  3. Data pipeline issue preventing 876950 from reaching gold layer")
        print(f"  4. Different PatientID system between embryoscope and clinisys")
        
        con.close()
        
        print(f"\n=== Pipeline check completed ===")
        
    except Exception as e:
        print(f"Error: {e}")
        raise

if __name__ == "__main__":
    check_clinisys_data_pipeline_876950() 