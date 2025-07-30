import pandas as pd
import duckdb
from datetime import datetime

def check_876950_status():
    """Check if PatientID 876950 is still missing after pipeline runs."""
    
    print(f"=== Checking PatientID 876950 Status - {datetime.now()} ===")
    
    try:
        # 1. Check if 876950 is in the missing list
        print(f"\n1. CHECKING MISSING PATIENTIDS LIST")
        print("-" * 50)
        
        missing_df = pd.read_csv('exports/missing_clinisys_complete_20250730_133753.csv')
        print(f"Total missing PatientIDs: {len(missing_df):,}")
        
        is_missing = 876950 in missing_df['patient_PatientID'].values
        print(f"PatientID 876950 in missing list: {'❌ YES' if is_missing else '✅ NO'}")
        
        if is_missing:
            patient_row = missing_df[missing_df['patient_PatientID'] == 876950].iloc[0]
            print(f"  Location: {patient_row['patient__location']}")
            print(f"  Last date: {patient_row['last_date']}")
            print(f"  Embryo count: {patient_row['embryo_count']}")
        
        # 2. Check if 876950 is now in clinisys gold
        print(f"\n2. CHECKING CLINISYS GOLD LAYER")
        print("-" * 50)
        
        con = duckdb.connect('../../database/huntington_data_lake.duckdb')
        
        clinisys_check_query = """
        SELECT COUNT(*) as count
        FROM gold.clinisys_embrioes
        WHERE micro_prontuario = 876950
        """
        
        clinisys_count = con.execute(clinisys_check_query).fetchone()[0]
        print(f"PatientID 876950 in clinisys gold: {'✅ YES' if clinisys_count > 0 else '❌ NO'}")
        
        if clinisys_count > 0:
            print(f"  Record count: {clinisys_count}")
            
            # Get sample data
            sample_query = """
            SELECT micro_prontuario, micro_Data_DL, oocito_id, oocito_embryo_number
            FROM gold.clinisys_embrioes
            WHERE micro_prontuario = 876950
            LIMIT 3
            """
            sample_df = con.execute(sample_query).df()
            print(f"  Sample data:")
            print(sample_df)
        
        # 3. Check if 876950 is now in combined gold
        print(f"\n3. CHECKING COMBINED GOLD LAYER")
        print("-" * 50)
        
        combined_check_query = """
        SELECT COUNT(*) as count
        FROM gold.embryoscope_clinisys_combined
        WHERE patient_PatientID = 876950 OR micro_prontuario = 876950
        """
        
        combined_count = con.execute(combined_check_query).fetchone()[0]
        print(f"PatientID 876950 in combined gold: {'✅ YES' if combined_count > 0 else '❌ NO'}")
        
        if combined_count > 0:
            print(f"  Record count: {combined_count}")
            
            # Get sample data
            sample_query = """
            SELECT patient_PatientID, micro_prontuario, embryo_FertilizationTime, micro_Data_DL
            FROM gold.embryoscope_clinisys_combined
            WHERE patient_PatientID = 876950 OR micro_prontuario = 876950
            LIMIT 3
            """
            sample_df = con.execute(sample_query).df()
            print(f"  Sample data:")
            print(sample_df)
        
        # 4. Summary
        print(f"\n4. SUMMARY")
        print("-" * 50)
        
        print(f"PatientID 876950 status:")
        print(f"  - In missing list: {'❌ YES' if is_missing else '✅ NO'}")
        print(f"  - In clinisys gold: {'✅ YES' if clinisys_count > 0 else '❌ NO'}")
        print(f"  - In combined gold: {'✅ YES' if combined_count > 0 else '❌ NO'}")
        
        if clinisys_count > 0 and combined_count > 0:
            print(f"\n🎉 SUCCESS: PatientID 876950 is now properly matched!")
        elif clinisys_count > 0 and combined_count == 0:
            print(f"\n⚠️ PARTIAL: PatientID 876950 is in clinisys but not matched")
        else:
            print(f"\n❌ ISSUE: PatientID 876950 is still missing from clinisys")
        
        con.close()
        
        print(f"\n=== Status check completed ===")
        
    except Exception as e:
        print(f"Error: {e}")
        raise

if __name__ == "__main__":
    check_876950_status() 