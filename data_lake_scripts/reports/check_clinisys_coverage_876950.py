import duckdb
import pandas as pd
from datetime import datetime

def check_clinisys_coverage_876950():
    """Check if PatientID 876950 should have clinisys records based on date and location."""
    
    print(f"=== Checking Clinisys Coverage for PatientID 876950 - {datetime.now()} ===")
    
    try:
        con = duckdb.connect('../../database/huntington_data_lake.duckdb')
        
        print("Connected to data lake")
        
        # 1. Check clinisys records for the same date (2025-07-28)
        print(f"\n1. CLINISYS RECORDS FOR DATE 2025-07-28")
        print("-" * 60)
        
        clinisys_date_query = """
        SELECT 
            micro_prontuario,
            micro_Data_DL,
            oocito_id,
            oocito_embryo_number,
            oocito_flag_embryoscope
        FROM gold.clinisys_embrioes
        WHERE CAST(micro_Data_DL AS DATE) = '2025-07-28'
        ORDER BY micro_prontuario
        LIMIT 20
        """
        
        clinisys_date_df = con.execute(clinisys_date_query).df()
        
        if len(clinisys_date_df) > 0:
            print(f"Found {len(clinisys_date_df)} clinisys records for 2025-07-28")
            print(f"Sample records:")
            print(f"{'Prontuario':<12} {'Data_DL':<25} {'OocitoID':<12} {'EmbryoNumber':<12} {'Flag':<8}")
            print("-" * 80)
            for _, row in clinisys_date_df.head(10).iterrows():
                prontuario = row['micro_prontuario']
                data_dl = str(row['micro_Data_DL'])[:24] if pd.notna(row['micro_Data_DL']) else 'NULL'
                oocito_id = row['oocito_id'] if pd.notna(row['oocito_id']) else 'NULL'
                embryo_num = row['oocito_embryo_number'] if pd.notna(row['oocito_embryo_number']) else 'NULL'
                flag = row['oocito_flag_embryoscope'] if pd.notna(row['oocito_flag_embryoscope']) else 'NULL'
                print(f"{prontuario:<12} {data_dl:<25} {oocito_id:<12} {embryo_num:<12} {flag:<8}")
        else:
            print("No clinisys records found for 2025-07-28")
        
        # 2. Check clinisys records for Ibirapuera location
        print(f"\n2. CLINISYS RECORDS FOR IBIRAPUERA LOCATION")
        print("-" * 60)
        
        # First, let's check if there's a location field in clinisys
        clinisys_schema_query = """
        DESCRIBE gold.clinisys_embrioes
        """
        
        clinisys_schema_df = con.execute(clinisys_schema_query).df()
        print("Clinisys table schema:")
        for _, row in clinisys_schema_df.iterrows():
            print(f"  {row['column_name']}: {row['column_type']}")
        
        # 3. Check if there are any clinisys records with similar PatientID patterns
        print(f"\n3. CLINISYS RECORDS WITH SIMILAR PATIENTID PATTERNS")
        print("-" * 60)
        
        similar_patterns_query = """
        SELECT DISTINCT 
            micro_prontuario,
            COUNT(*) as record_count
        FROM gold.clinisys_embrioes
        WHERE CAST(micro_prontuario AS VARCHAR) LIKE '%876%'
        GROUP BY micro_prontuario
        ORDER BY micro_prontuario
        LIMIT 20
        """
        
        similar_patterns_df = con.execute(similar_patterns_query).df()
        
        if len(similar_patterns_df) > 0:
            print(f"Found {len(similar_patterns_df)} clinisys PatientIDs with '876' pattern:")
            print(f"{'Prontuario':<12} {'Record Count':<12}")
            print("-" * 30)
            for _, row in similar_patterns_df.iterrows():
                prontuario = row['micro_prontuario']
                count = row['record_count']
                print(f"{prontuario:<12} {count:<12}")
        else:
            print("No clinisys PatientIDs found with '876' pattern")
        
        # 4. Check if this is a recent data issue
        print(f"\n4. CHECKING RECENT DATA AVAILABILITY")
        print("-" * 60)
        
        recent_clinisys_query = """
        SELECT 
            MIN(CAST(micro_Data_DL AS DATE)) as earliest_date,
            MAX(CAST(micro_Data_DL AS DATE)) as latest_date,
            COUNT(DISTINCT micro_prontuario) as unique_patients,
            COUNT(*) as total_records
        FROM gold.clinisys_embrioes
        WHERE CAST(micro_Data_DL AS DATE) >= '2025-07-01'
        """
        
        recent_clinisys_df = con.execute(recent_clinisys_query).df()
        
        if len(recent_clinisys_df) > 0:
            row = recent_clinisys_df.iloc[0]
            print(f"Recent clinisys data (July 2025+):")
            print(f"  Date range: {row['earliest_date']} to {row['latest_date']}")
            print(f"  Unique patients: {row['unique_patients']:,}")
            print(f"  Total records: {row['total_records']:,}")
        else:
            print("No recent clinisys data found")
        
        # 5. Check embryoscope data for the same date
        print(f"\n5. EMBRYOSCOPE RECORDS FOR DATE 2025-07-28")
        print("-" * 60)
        
        embryoscope_date_query = """
        SELECT 
            patient_PatientID,
            patient__location,
            embryo_EmbryoID,
            embryo_FertilizationTime,
            embryo_embryo_number
        FROM gold.embryoscope_embrioes
        WHERE CAST(embryo_FertilizationTime AS DATE) = '2025-07-28'
        ORDER BY patient_PatientID
        LIMIT 20
        """
        
        embryoscope_date_df = con.execute(embryoscope_date_query).df()
        
        if len(embryoscope_date_df) > 0:
            print(f"Found {len(embryoscope_date_df)} embryoscope records for 2025-07-28")
            print(f"Unique patients: {embryoscope_date_df['patient_PatientID'].nunique()}")
            print(f"Locations: {embryoscope_date_df['patient__location'].unique()}")
            
            print(f"\nSample embryoscope records:")
            print(f"{'PatientID':<12} {'Location':<15} {'EmbryoID':<20} {'EmbryoNumber':<12}")
            print("-" * 70)
            for _, row in embryoscope_date_df.head(10).iterrows():
                patient_id = row['patient_PatientID']
                location = row['patient__location'] if pd.notna(row['patient__location']) else 'NULL'
                embryo_id = row['embryo_EmbryoID']
                embryo_num = row['embryo_embryo_number'] if pd.notna(row['embryo_embryo_number']) else 'NULL'
                print(f"{patient_id:<12} {str(location)[:14]:<15} {embryo_id:<20} {embryo_num:<12}")
        else:
            print("No embryoscope records found for 2025-07-28")
        
        # 6. Summary and conclusion
        print(f"\n6. SUMMARY AND CONCLUSION")
        print("-" * 60)
        
        embryoscope_count = len(embryoscope_date_df)
        clinisys_count = len(clinisys_date_df)
        
        print(f"Embryoscope records for 2025-07-28: {embryoscope_count}")
        print(f"Clinisys records for 2025-07-28: {clinisys_count}")
        
        if embryoscope_count > 0 and clinisys_count == 0:
            print(f"\n❌ CONCLUSION: No clinisys data available for 2025-07-28")
            print(f"This explains why PatientID 876950 is missing from clinisys")
            print(f"The issue is likely a data synchronization delay or missing clinisys data for this date")
        elif embryoscope_count > 0 and clinisys_count > 0:
            print(f"\n⚠️ CONCLUSION: Clinisys data exists for 2025-07-28 but PatientID 876950 is missing")
            print(f"This suggests a specific data integration issue for this patient")
        else:
            print(f"\n❓ CONCLUSION: No data available for 2025-07-28 in either system")
            print(f"This might be a weekend/holiday or data collection issue")
        
        con.close()
        
        print(f"\n=== Coverage check completed ===")
        
    except Exception as e:
        print(f"Error: {e}")
        raise

if __name__ == "__main__":
    check_clinisys_coverage_876950() 