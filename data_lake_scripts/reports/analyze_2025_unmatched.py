import duckdb
import pandas as pd
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

def analyze_2025_unmatched():
    """Analyze 2025 unmatched embryos to see how many exist in Clinisys table."""
    
    logger.info("Starting 2025 unmatched embryos analysis")
    
    try:
        con = duckdb.connect('../database/huntington_data_lake.duckdb')
        logger.info("Connected to data lake")
        
        print("=" * 80)
        print("2025 UNMATCHED EMBRYOS ANALYSIS")
        print("=" * 80)
        
        # Get 2025 unmatched Embryoscope embryos
        logger.info("Fetching 2025 unmatched Embryoscope embryos...")
        unmatched_2025_query = """
        SELECT DISTINCT
            embryo_EmbryoID,
            embryo_FertilizationTime,
            embryo_embryo_number,
            patient_PatientID,
            treatment_TreatmentName,
            DATE_TRUNC('month', CAST(embryo_FertilizationTime AS DATE)) as year_month
        FROM gold.embryoscope_embrioes
        WHERE embryo_EmbryoID NOT IN (
            SELECT DISTINCT embryo_EmbryoID 
            FROM gold.embryoscope_clinisys_combined 
            WHERE embryo_EmbryoID IS NOT NULL
        )
        AND YEAR(CAST(embryo_FertilizationTime AS DATE)) = 2025
        ORDER BY embryo_FertilizationTime DESC
        """
        
        unmatched_2025_df = con.execute(unmatched_2025_query).df()
        logger.info(f"Found {len(unmatched_2025_df):,} unmatched 2025 Embryoscope embryos")
        
        # Get unique PatientIDs from Clinisys
        logger.info("Fetching unique PatientIDs from Clinisys...")
        clinisys_patientids = con.execute("""
            SELECT DISTINCT micro_prontuario
            FROM gold.clinisys_embrioes
            WHERE micro_prontuario IS NOT NULL
        """).fetchdf()['micro_prontuario'].tolist()
        
        logger.info(f"Found {len(clinisys_patientids):,} unique PatientIDs in Clinisys")
        
        # Analyze PatientID presence for 2025 unmatched embryos
        print(f"\n1. 2025 UNMATCHED EMBRYOS ANALYSIS")
        print("-" * 50)
        
        # Count embryos with valid PatientIDs
        valid_patientid_embryos = unmatched_2025_df[unmatched_2025_df['patient_PatientID'].notna()]
        invalid_patientid_embryos = unmatched_2025_df[unmatched_2025_df['patient_PatientID'].isna()]
        
        print(f"Total 2025 unmatched embryos: {len(unmatched_2025_df):,}")
        print(f"Embryos with valid PatientID: {len(valid_patientid_embryos):,}")
        print(f"Embryos with null PatientID: {len(invalid_patientid_embryos):,}")
        
        # Check which valid PatientIDs are present in Clinisys
        valid_patientids = valid_patientid_embryos['patient_PatientID'].unique()
        found_in_clinisys = [pid for pid in valid_patientids if pid in clinisys_patientids]
        not_found_in_clinisys = [pid for pid in valid_patientids if pid not in clinisys_patientids]
        
        print(f"\nValid PatientIDs found in Clinisys: {len(found_in_clinisys):,}")
        print(f"Valid PatientIDs NOT found in Clinisys: {len(not_found_in_clinisys):,}")
        
        # Count embryos by PatientID presence
        embryos_with_found_patientid = valid_patientid_embryos[
            valid_patientid_embryos['patient_PatientID'].isin(found_in_clinisys)
        ]
        embryos_with_notfound_patientid = valid_patientid_embryos[
            valid_patientid_embryos['patient_PatientID'].isin(not_found_in_clinisys)
        ]
        
        print(f"Embryos with PatientID found in Clinisys: {len(embryos_with_found_patientid):,}")
        print(f"Embryos with PatientID NOT found in Clinisys: {len(embryos_with_notfound_patientid):,}")
        
        # 2. Monthly breakdown for 2025
        print(f"\n2. 2025 MONTHLY BREAKDOWN")
        print("-" * 50)
        
        # Count by month
        monthly_counts = unmatched_2025_df.groupby('year_month').size().reset_index(name='unmatched_count')
        monthly_counts = monthly_counts.sort_values('year_month', ascending=False)
        monthly_counts['year_month_display'] = monthly_counts['year_month'].dt.strftime('%Y-%m')
        
        print("2025 unmatched embryos by month:")
        print(f"{'Month':<10} {'Unmatched':<10}")
        print("-" * 25)
        
        for _, row in monthly_counts.iterrows():
            print(f"{row['year_month_display']:<10} {row['unmatched_count']:<10}")
        
        # 3. Examples of PatientIDs found in Clinisys
        print(f"\n3. EXAMPLES: PATIENTIDS FOUND IN CLINISYS")
        print("-" * 50)
        
        if len(found_in_clinisys) > 0:
            # Get sample embryos with PatientIDs found in Clinisys
            sample_found = embryos_with_found_patientid.head(10)
            print("Sample 2025 embryos with PatientID found in Clinisys:")
            print(sample_found[['embryo_EmbryoID', 'embryo_FertilizationTime', 'embryo_embryo_number', 'patient_PatientID']].to_string(index=False))
            
            # Check Clinisys records for these PatientIDs
            print(f"\nClinisys records for these PatientIDs:")
            for _, row in sample_found.iterrows():
                patientid = row['patient_PatientID']
                date = row['embryo_FertilizationTime'].date()
                embryo_number = row['embryo_embryo_number']
                
                # Count total Clinisys records for this PatientID
                total_clinisys = con.execute("""
                    SELECT COUNT(*) 
                    FROM gold.clinisys_embrioes 
                    WHERE micro_prontuario = ?
                """, [patientid]).fetchone()[0]
                
                # Count Clinisys records for this PatientID on the same date
                same_date_clinisys = con.execute("""
                    SELECT COUNT(*) 
                    FROM gold.clinisys_embrioes 
                    WHERE micro_prontuario = ?
                    AND CAST(micro_Data_DL AS DATE) = ?
                """, [patientid, date]).fetchone()[0]
                
                # Count Clinisys records for this PatientID with same embryo number
                same_embryo_clinisys = con.execute("""
                    SELECT COUNT(*) 
                    FROM gold.clinisys_embrioes 
                    WHERE micro_prontuario = ?
                    AND oocito_embryo_number = ?
                """, [patientid, embryo_number]).fetchone()[0]
                
                print(f"  PatientID {patientid} ({row['embryo_EmbryoID']}):")
                print(f"    - Total Clinisys records: {total_clinisys}")
                print(f"    - Same date records: {same_date_clinisys}")
                print(f"    - Same embryo number records: {same_embryo_clinisys}")
        else:
            print("No PatientIDs found in Clinisys")
        
        # 4. Examples of PatientIDs NOT found in Clinisys
        print(f"\n4. EXAMPLES: PATIENTIDS NOT FOUND IN CLINISYS")
        print("-" * 50)
        
        if len(not_found_in_clinisys) > 0:
            # Get sample embryos with PatientIDs not found in Clinisys
            sample_notfound = embryos_with_notfound_patientid.head(10)
            print("Sample 2025 embryos with PatientID NOT found in Clinisys:")
            print(sample_notfound[['embryo_EmbryoID', 'embryo_FertilizationTime', 'embryo_embryo_number', 'patient_PatientID']].to_string(index=False))
            
            # Show some of the PatientIDs that are not in Clinisys
            print(f"\nSample PatientIDs not found in Clinisys:")
            for pid in not_found_in_clinisys[:10]:
                print(f"  {pid}")
        else:
            print("All PatientIDs are found in Clinisys")
        
        # 5. Summary statistics for 2025
        print(f"\n5. 2025 SUMMARY STATISTICS")
        print("-" * 50)
        
        total_2025_unmatched = len(unmatched_2025_df)
        valid_2025_embryos = len(valid_patientid_embryos)
        found_2025_embryos = len(embryos_with_found_patientid)
        notfound_2025_embryos = len(embryos_with_notfound_patientid)
        invalid_2025_embryos = len(invalid_patientid_embryos)
        
        print(f"Total 2025 unmatched embryos: {total_2025_unmatched:,} (100%)")
        print(f"  - Valid PatientID: {valid_2025_embryos:,} ({valid_2025_embryos/total_2025_unmatched*100:.1f}%)")
        print(f"    - Found in Clinisys: {found_2025_embryos:,} ({found_2025_embryos/total_2025_unmatched*100:.1f}%)")
        print(f"    - NOT found in Clinisys: {notfound_2025_embryos:,} ({notfound_2025_embryos/total_2025_unmatched*100:.1f}%)")
        print(f"  - Null PatientID: {invalid_2025_embryos:,} ({invalid_2025_embryos/total_2025_unmatched*100:.1f}%)")
        
        # 6. Potential matches analysis
        print(f"\n6. POTENTIAL MATCHES ANALYSIS")
        print("-" * 50)
        
        if len(embryos_with_found_patientid) > 0:
            print(f"Found {len(embryos_with_found_patientid):,} embryos with PatientIDs that exist in Clinisys")
            print("These embryos could potentially be matched if the join conditions are adjusted.")
            
            # Check if any of these have matching dates and embryo numbers
            potential_matches = 0
            for _, row in embryos_with_found_patientid.iterrows():
                patientid = row['patient_PatientID']
                date = row['embryo_FertilizationTime'].date()
                embryo_number = row['embryo_embryo_number']
                
                # Check for exact matches
                exact_match = con.execute("""
                    SELECT COUNT(*) 
                    FROM gold.clinisys_embrioes 
                    WHERE micro_prontuario = ?
                    AND CAST(micro_Data_DL AS DATE) = ?
                    AND oocito_embryo_number = ?
                """, [patientid, date, embryo_number]).fetchone()[0]
                
                if exact_match > 0:
                    potential_matches += 1
            
            print(f"Exact matches (same PatientID, date, and embryo number): {potential_matches:,}")
            print(f"Potential matches with date tolerance: {len(embryos_with_found_patientid):,}")
        else:
            print("No embryos with PatientIDs found in Clinisys")
        
        con.close()
        logger.info("2025 unmatched embryos analysis completed")
        
    except Exception as e:
        logger.error(f"Error in analysis: {e}")
        raise

if __name__ == '__main__':
    analyze_2025_unmatched() 