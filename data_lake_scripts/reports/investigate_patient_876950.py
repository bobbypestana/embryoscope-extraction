import duckdb
import pandas as pd
from datetime import datetime

def investigate_patient_876950():
    """Investigate why PatientID 876950 is not matching between embryoscope and clinisys."""
    
    print(f"=== Investigating PatientID 876950 - {datetime.now()} ===")
    
    try:
        con = duckdb.connect('../../database/huntington_data_lake.duckdb')
        
        print("Connected to data lake")
        
        # 1. Check embryoscope data for PatientID 876950
        print(f"\n1. EMBRYOSCOPE DATA FOR PATIENTID 876950")
        print("-" * 60)
        
        embryoscope_query = """
        SELECT 
            patient_PatientID,
            patient_PatientIDx,
            patient_FirstName,
            patient_LastName,
            patient__location,
            embryo_EmbryoID,
            embryo_FertilizationTime,
            embryo_EmbryoFate,
            embryo_embryo_number,
            treatment_TreatmentName
        FROM gold.embryoscope_embrioes
        WHERE patient_PatientID = 876950
        ORDER BY embryo_FertilizationTime
        LIMIT 20
        """
        
        embryoscope_df = con.execute(embryoscope_query).df()
        
        if len(embryoscope_df) > 0:
            print(f"Found {len(embryoscope_df)} embryoscope records for PatientID 876950")
            print(f"Patient details:")
            print(f"  PatientIDx: {embryoscope_df.iloc[0]['patient_PatientIDx']}")
            print(f"  Name: {embryoscope_df.iloc[0]['patient_FirstName']} {embryoscope_df.iloc[0]['patient_LastName']}")
            print(f"  Location: {embryoscope_df.iloc[0]['patient__location']}")
            print(f"  Date range: {embryoscope_df['embryo_FertilizationTime'].min()} to {embryoscope_df['embryo_FertilizationTime'].max()}")
            print(f"  Total embryos: {len(embryoscope_df)}")
            
            print(f"\nSample embryoscope records:")
            print(f"{'EmbryoID':<20} {'FertilizationTime':<25} {'EmbryoNumber':<12} {'Fate':<15}")
            print("-" * 80)
            for _, row in embryoscope_df.head(10).iterrows():
                embryo_id = row['embryo_EmbryoID']
                fert_time = str(row['embryo_FertilizationTime'])[:24] if pd.notna(row['embryo_FertilizationTime']) else 'NULL'
                embryo_num = row['embryo_embryo_number'] if pd.notna(row['embryo_embryo_number']) else 'NULL'
                fate = row['embryo_EmbryoFate'] if pd.notna(row['embryo_EmbryoFate']) else 'NULL'
                print(f"{embryo_id:<20} {fert_time:<25} {embryo_num:<12} {fate:<15}")
        else:
            print("No embryoscope records found for PatientID 876950")
        
        # 2. Check clinisys data for PatientID 876950
        print(f"\n2. CLINISYS DATA FOR PATIENTID 876950")
        print("-" * 60)
        
        clinisys_query = """
        SELECT 
            micro_prontuario,
            micro_Data_DL,
            oocito_id,
            oocito_embryo_number,
            oocito_flag_embryoscope,
            emb_cong_embriao
        FROM gold.clinisys_embrioes
        WHERE micro_prontuario = 876950
        ORDER BY micro_Data_DL
        LIMIT 20
        """
        
        clinisys_df = con.execute(clinisys_query).df()
        
        if len(clinisys_df) > 0:
            print(f"Found {len(clinisys_df)} clinisys records for PatientID 876950")
            print(f"Date range: {clinisys_df['micro_Data_DL'].min()} to {clinisys_df['micro_Data_DL'].max()}")
            print(f"Embryoscope flag count: {clinisys_df['oocito_flag_embryoscope'].sum()}")
            
            print(f"\nSample clinisys records:")
            print(f"{'Prontuario':<12} {'Data_DL':<25} {'OocitoID':<12} {'EmbryoNumber':<12} {'Flag':<8}")
            print("-" * 80)
            for _, row in clinisys_df.head(10).iterrows():
                prontuario = row['micro_prontuario']
                data_dl = str(row['micro_Data_DL'])[:24] if pd.notna(row['micro_Data_DL']) else 'NULL'
                oocito_id = row['oocito_id'] if pd.notna(row['oocito_id']) else 'NULL'
                embryo_num = row['oocito_embryo_number'] if pd.notna(row['oocito_embryo_number']) else 'NULL'
                flag = row['oocito_flag_embryoscope'] if pd.notna(row['oocito_flag_embryoscope']) else 'NULL'
                print(f"{prontuario:<12} {data_dl:<25} {oocito_id:<12} {embryo_num:<12} {flag:<8}")
        else:
            print("No clinisys records found for PatientID 876950")
        
        # 3. Check combined gold table
        print(f"\n3. COMBINED GOLD TABLE FOR PATIENTID 876950")
        print("-" * 60)
        
        combined_query = """
        SELECT 
            patient_PatientID,
            micro_prontuario,
            patient_PatientID_clean,
            embryo_EmbryoID,
            embryo_FertilizationTime,
            micro_Data_DL,
            oocito_flag_embryoscope
        FROM gold.embryoscope_clinisys_combined
        WHERE patient_PatientID = 876950 OR micro_prontuario = 876950
        ORDER BY embryo_FertilizationTime
        LIMIT 20
        """
        
        combined_df = con.execute(combined_query).df()
        
        if len(combined_df) > 0:
            print(f"Found {len(combined_df)} combined records for PatientID 876950")
            print(f"\nSample combined records:")
            print(f"{'PatientID':<12} {'Prontuario':<12} {'EmbryoID':<20} {'FertTime':<25} {'DataDL':<25}")
            print("-" * 100)
            for _, row in combined_df.head(10).iterrows():
                patient_id = row['patient_PatientID'] if pd.notna(row['patient_PatientID']) else 'NULL'
                prontuario = row['micro_prontuario'] if pd.notna(row['micro_prontuario']) else 'NULL'
                embryo_id = row['embryo_EmbryoID'] if pd.notna(row['embryo_EmbryoID']) else 'NULL'
                fert_time = str(row['embryo_FertilizationTime'])[:24] if pd.notna(row['embryo_FertilizationTime']) else 'NULL'
                data_dl = str(row['micro_Data_DL'])[:24] if pd.notna(row['micro_Data_DL']) else 'NULL'
                print(f"{patient_id:<12} {prontuario:<12} {embryo_id:<20} {fert_time:<25} {data_dl:<25}")
        else:
            print("No combined records found for PatientID 876950")
        
        # 4. Check if PatientID 876950 exists in clinisys at all
        print(f"\n4. CHECKING IF PATIENTID 876950 EXISTS IN CLINISYS")
        print("-" * 60)
        
        clinisys_exists_query = """
        SELECT DISTINCT micro_prontuario
        FROM gold.clinisys_embrioes
        WHERE micro_prontuario = 876950
        """
        
        clinisys_exists_df = con.execute(clinisys_exists_query).df()
        
        if len(clinisys_exists_df) > 0:
            print(f"✅ PatientID 876950 EXISTS in clinisys data")
        else:
            print(f"❌ PatientID 876950 DOES NOT EXIST in clinisys data")
        
        # 5. Check for similar PatientIDs in clinisys
        print(f"\n5. CHECKING FOR SIMILAR PATIENTIDS IN CLINISYS")
        print("-" * 60)
        
        similar_query = """
        SELECT DISTINCT micro_prontuario
        FROM gold.clinisys_embrioes
        WHERE CAST(micro_prontuario AS VARCHAR) LIKE '%876950%' 
           OR CAST(micro_prontuario AS VARCHAR) LIKE '%87695%'
           OR CAST(micro_prontuario AS VARCHAR) LIKE '%8769%'
        ORDER BY micro_prontuario
        LIMIT 20
        """
        
        similar_df = con.execute(similar_query).df()
        
        if len(similar_df) > 0:
            print(f"Found {len(similar_df)} similar PatientIDs in clinisys:")
            for _, row in similar_df.iterrows():
                print(f"  {row['micro_prontuario']}")
        else:
            print("No similar PatientIDs found in clinisys")
        
        # 6. Check embryoscope silver layer
        print(f"\n6. CHECKING EMBRYOSCOPE SILVER LAYER")
        print("-" * 60)
        
        silver_query = """
        SELECT 
            PatientID,
            PatientIDx,
            FirstName,
            LastName,
            _location
        FROM silver_embryoscope.patients
        WHERE PatientID = 876950
        """
        
        silver_df = con.execute(silver_query).df()
        
        if len(silver_df) > 0:
            print(f"Found {len(silver_df)} records in embryoscope silver layer:")
            for _, row in silver_df.iterrows():
                print(f"  PatientID: {row['PatientID']}")
                print(f"  PatientIDx: {row['PatientIDx']}")
                print(f"  Name: {row['FirstName']} {row['LastName']}")
                print(f"  Location: {row['_location']}")
        else:
            print("No records found in embryoscope silver layer")
        
        # 7. Summary
        print(f"\n7. SUMMARY")
        print("-" * 60)
        
        embryoscope_count = len(embryoscope_df)
        clinisys_count = len(clinisys_df)
        combined_count = len(combined_df)
        
        print(f"Embryoscope records: {embryoscope_count}")
        print(f"Clinisys records: {clinisys_count}")
        print(f"Combined records: {combined_count}")
        
        if embryoscope_count > 0 and clinisys_count == 0:
            print(f"\n❌ ISSUE IDENTIFIED: PatientID 876950 exists in embryoscope but NOT in clinisys")
            print(f"This explains why it's in the missing PatientIDs list")
        elif embryoscope_count > 0 and clinisys_count > 0 and combined_count == 0:
            print(f"\n⚠️ ISSUE IDENTIFIED: PatientID 876950 exists in both systems but failed to match")
            print(f"This suggests a join condition problem")
        elif embryoscope_count > 0 and clinisys_count > 0 and combined_count > 0:
            print(f"\n✅ PatientID 876950 is properly matched in combined table")
        else:
            print(f"\n❓ UNEXPECTED: PatientID 876950 not found in embryoscope data")
        
        con.close()
        
        print(f"\n=== Investigation completed ===")
        
    except Exception as e:
        print(f"Error: {e}")
        raise

if __name__ == "__main__":
    investigate_patient_876950() 