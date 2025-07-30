import duckdb
import pandas as pd
from datetime import datetime
import os

def analyze_missing_patientids_by_date():
    """Analyze missing PatientIDs by activity date and split into before/after 2021-06."""
    
    print(f"=== Analyzing Missing PatientIDs by Date - {datetime.now()} ===")
    
    try:
        con = duckdb.connect('../../database/huntington_data_lake.duckdb')
        
        print("Connected to data lake")
        
        # 1. Get embryoscope PatientIDs not in clinisys
        print(f"\n1. EXTRACTING MISSING PATIENTIDS")
        print("-" * 50)
        
        # Get embryoscope PatientIDs
        embryoscope_query = """
        SELECT DISTINCT 
            patient_PatientID,
            COUNT(DISTINCT embryo_EmbryoID) as embryo_count
        FROM gold.embryoscope_embrioes
        WHERE patient_PatientID IS NOT NULL
        GROUP BY patient_PatientID
        """
        
        embryoscope_df = con.execute(embryoscope_query).df()
        embryoscope_patientids_set = set(embryoscope_df['patient_PatientID'].tolist())
        
        # Get clinisys PatientIDs
        clinisys_query = """
        SELECT DISTINCT micro_prontuario
        FROM gold.clinisys_embrioes
        WHERE micro_prontuario IS NOT NULL
        """
        
        clinisys_df = con.execute(clinisys_query).df()
        clinisys_patientids_set = set(clinisys_df['micro_prontuario'].tolist())
        
        # Find missing PatientIDs
        not_in_clinisys = embryoscope_patientids_set - clinisys_patientids_set
        missing_patientids_list = list(not_in_clinisys)
        
        print(f"Total embryoscope PatientIDs: {len(embryoscope_patientids_set):,}")
        print(f"Total clinisys PatientIDs: {len(clinisys_patientids_set):,}")
        print(f"PatientIDs missing from clinisys: {len(not_in_clinisys):,}")
        
        # 2. Get detailed information with dates
        print(f"\n2. EXTRACTING DETAILED INFORMATION WITH DATES")
        print("-" * 50)
        
        detailed_query = """
        SELECT DISTINCT 
            patient_PatientID,
            patient_PatientIDx,
            patient_FirstName,
            patient_LastName,
            patient__location,
            COUNT(DISTINCT embryo_EmbryoID) as embryo_count,
            MIN(CAST(embryo_FertilizationTime AS DATE)) as first_date,
            MAX(CAST(embryo_FertilizationTime AS DATE)) as last_date
        FROM gold.embryoscope_embrioes
        WHERE patient_PatientID IN ({})
        GROUP BY patient_PatientID, patient_PatientIDx, patient_FirstName, patient_LastName, patient__location
        ORDER BY last_date DESC
        """.format(','.join(map(str, missing_patientids_list)))
        
        detailed_df = con.execute(detailed_query).df()
        
        print(f"Extracted detailed information for {len(detailed_df):,} PatientIDs")
        
        # 3. Split by date
        print(f"\n3. SPLITTING BY ACTIVITY DATE")
        print("-" * 50)
        
        # Convert dates to datetime for comparison
        detailed_df['last_date'] = pd.to_datetime(detailed_df['last_date'])
        cutoff_date = pd.to_datetime('2021-06-01')
        
        # Split into before and after 2021-06
        before_2021_06 = detailed_df[detailed_df['last_date'] < cutoff_date].copy()
        after_2021_06 = detailed_df[detailed_df['last_date'] >= cutoff_date].copy()
        
        print(f"PatientIDs with last activity BEFORE 2021-06: {len(before_2021_06):,}")
        print(f"PatientIDs with last activity AFTER 2021-06: {len(after_2021_06):,}")
        
        # 4. Create exports directory if it doesn't exist
        exports_dir = "exports"
        if not os.path.exists(exports_dir):
            os.makedirs(exports_dir)
            print(f"Created exports directory: {exports_dir}")
        
        # 5. Save files
        print(f"\n4. SAVING FILES")
        print("-" * 50)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Save before 2021-06
        before_filename = f"{exports_dir}/missing_clinisys_before_2021_06_{timestamp}.csv"
        before_2021_06.to_csv(before_filename, index=False)
        print(f"Saved BEFORE 2021-06: {before_filename}")
        
        # Save after 2021-06
        after_filename = f"{exports_dir}/missing_clinisys_after_2021_06_{timestamp}.csv"
        after_2021_06.to_csv(after_filename, index=False)
        print(f"Saved AFTER 2021-06: {after_filename}")
        
        # Save complete list
        complete_filename = f"{exports_dir}/missing_clinisys_complete_{timestamp}.csv"
        detailed_df.to_csv(complete_filename, index=False)
        print(f"Saved COMPLETE LIST: {complete_filename}")
        
        # 6. Show statistics
        print(f"\n5. STATISTICS BY DATE RANGE")
        print("-" * 50)
        
        # Before 2021-06 statistics
        if len(before_2021_06) > 0:
            print(f"\nBEFORE 2021-06 ({len(before_2021_06):,} PatientIDs):")
            print(f"  Mean embryo count: {before_2021_06['embryo_count'].mean():.1f}")
            print(f"  Median embryo count: {before_2021_06['embryo_count'].median():.1f}")
            print(f"  Date range: {before_2021_06['last_date'].min()} to {before_2021_06['last_date'].max()}")
            
            location_dist_before = before_2021_06['patient__location'].value_counts()
            print(f"  Distribution by location:")
            for location, count in location_dist_before.head(5).items():
                print(f"    {location}: {count:,}")
        
        # After 2021-06 statistics
        if len(after_2021_06) > 0:
            print(f"\nAFTER 2021-06 ({len(after_2021_06):,} PatientIDs):")
            print(f"  Mean embryo count: {after_2021_06['embryo_count'].mean():.1f}")
            print(f"  Median embryo count: {after_2021_06['embryo_count'].median():.1f}")
            print(f"  Date range: {after_2021_06['last_date'].min()} to {after_2021_06['last_date'].max()}")
            
            location_dist_after = after_2021_06['patient__location'].value_counts()
            print(f"  Distribution by location:")
            for location, count in location_dist_after.head(5).items():
                print(f"    {location}: {count:,}")
        
        # 7. Show sample data
        print(f"\n6. SAMPLE DATA")
        print("-" * 50)
        
        print(f"\nBEFORE 2021-06 - Sample (first 10):")
        print(f"{'PatientID':<12} {'Location':<15} {'Embryo Count':<12} {'Last Date':<12}")
        print("-" * 60)
        for _, row in before_2021_06.head(10).iterrows():
            patientid = row['patient_PatientID']
            location = row['patient__location'] if pd.notna(row['patient__location']) else 'NULL'
            embryo_count = row['embryo_count']
            last_date = row['last_date'].strftime('%Y-%m-%d') if pd.notna(row['last_date']) else 'NULL'
            print(f"{patientid:<12} {str(location)[:14]:<15} {embryo_count:<12} {last_date:<12}")
        
        print(f"\nAFTER 2021-06 - Sample (first 10):")
        print(f"{'PatientID':<12} {'Location':<15} {'Embryo Count':<12} {'Last Date':<12}")
        print("-" * 60)
        for _, row in after_2021_06.head(10).iterrows():
            patientid = row['patient_PatientID']
            location = row['patient__location'] if pd.notna(row['patient__location']) else 'NULL'
            embryo_count = row['embryo_count']
            last_date = row['last_date'].strftime('%Y-%m-%d') if pd.notna(row['last_date']) else 'NULL'
            print(f"{patientid:<12} {str(location)[:14]:<15} {embryo_count:<12} {last_date:<12}")
        
        # 8. Summary
        print(f"\n7. SUMMARY")
        print("-" * 50)
        print(f"Total missing PatientIDs: {len(detailed_df):,}")
        print(f"  - Before 2021-06: {len(before_2021_06):,} ({len(before_2021_06)/len(detailed_df)*100:.1f}%)")
        print(f"  - After 2021-06: {len(after_2021_06):,} ({len(after_2021_06)/len(detailed_df)*100:.1f}%)")
        print(f"\nFiles saved:")
        print(f"  - {before_filename}")
        print(f"  - {after_filename}")
        print(f"  - {complete_filename}")
        
        con.close()
        
        print(f"\n=== Analysis completed successfully ===")
        
    except Exception as e:
        print(f"Error: {e}")
        raise

if __name__ == "__main__":
    analyze_missing_patientids_by_date() 