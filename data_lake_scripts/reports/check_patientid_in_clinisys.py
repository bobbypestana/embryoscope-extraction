import duckdb
import pandas as pd
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

def check_patientid_in_clinisys():
    """Check if PatientIDs from unmatched Embryoscope embryos are present in Clinisys."""
    
    logger.info("Starting PatientID presence analysis in Clinisys")
    
    try:
        con = duckdb.connect('../database/huntington_data_lake.duckdb')
        logger.info("Connected to data lake")
        
        print("=" * 80)
        print("PATIENTID PRESENCE ANALYSIS IN CLINISYS")
        print("=" * 80)
        
        # Get all unmatched Embryoscope embryos with their PatientIDs
        logger.info("Fetching unmatched Embryoscope embryos...")
        unmatched_query = """
        SELECT DISTINCT
            embryo_EmbryoID,
            embryo_FertilizationTime,
            embryo_embryo_number,
            patient_PatientID,
            treatment_TreatmentName,
            CASE 
                WHEN patient_PatientID IS NULL THEN NULL
                ELSE patient_PatientID
            END as patient_PatientID_clean
        FROM gold.embryoscope_embrioes
        WHERE embryo_EmbryoID NOT IN (
            SELECT DISTINCT embryo_EmbryoID 
            FROM gold.embryoscope_clinisys_combined 
            WHERE embryo_EmbryoID IS NOT NULL
        )
        ORDER BY embryo_FertilizationTime DESC
        """
        
        unmatched_df = con.execute(unmatched_query).df()
        logger.info(f"Found {len(unmatched_df):,} unmatched Embryoscope embryos")
        
        # Get unique PatientIDs from Clinisys (both micro_prontuario and prontuario_esposa)
        logger.info("Fetching unique PatientIDs from Clinisys...")
        clinisys_patientids = con.execute("""
            SELECT DISTINCT micro_prontuario
            FROM gold.clinisys_embrioes
            WHERE micro_prontuario IS NOT NULL
        """).fetchdf()['micro_prontuario'].tolist()
        
        # Get PatientIDs from all prontuario fields
        clinisys_prontuario_1 = con.execute("""
            SELECT DISTINCT prontuario_1
            FROM gold.clinisys_embrioes
            WHERE prontuario_1 IS NOT NULL
        """).fetchdf()['prontuario_1'].tolist()
        
        clinisys_prontuario_2 = con.execute("""
            SELECT DISTINCT prontuario_2
            FROM gold.clinisys_embrioes
            WHERE prontuario_2 IS NOT NULL
        """).fetchdf()['prontuario_2'].tolist()
        
        clinisys_prontuario_3 = con.execute("""
            SELECT DISTINCT prontuario_3
            FROM gold.clinisys_embrioes
            WHERE prontuario_3 IS NOT NULL
        """).fetchdf()['prontuario_3'].tolist()
        
        clinisys_prontuario_4 = con.execute("""
            SELECT DISTINCT prontuario_4
            FROM gold.clinisys_embrioes
            WHERE prontuario_4 IS NOT NULL
        """).fetchdf()['prontuario_4'].tolist()
        
        clinisys_prontuario_5 = con.execute("""
            SELECT DISTINCT prontuario_5
            FROM gold.clinisys_embrioes
            WHERE prontuario_5 IS NOT NULL
        """).fetchdf()['prontuario_5'].tolist()
        
        logger.info(f"Found {len(clinisys_patientids):,} unique PatientIDs in micro_prontuario")
        logger.info(f"Found {len(clinisys_prontuario_1):,} unique PatientIDs in prontuario_1")
        logger.info(f"Found {len(clinisys_prontuario_2):,} unique PatientIDs in prontuario_2")
        logger.info(f"Found {len(clinisys_prontuario_3):,} unique PatientIDs in prontuario_3")
        logger.info(f"Found {len(clinisys_prontuario_4):,} unique PatientIDs in prontuario_4")
        logger.info(f"Found {len(clinisys_prontuario_5):,} unique PatientIDs in prontuario_5")
        
        # Analyze PatientID presence
        print(f"\n1. OVERALL ANALYSIS")
        print("-" * 50)
        
        # Count embryos with valid PatientIDs
        valid_patientid_embryos = unmatched_df[unmatched_df['patient_PatientID_clean'].notna()]
        invalid_patientid_embryos = unmatched_df[unmatched_df['patient_PatientID_clean'].isna()]
        
        print(f"Total unmatched embryos: {len(unmatched_df):,}")
        print(f"Embryos with valid PatientID: {len(valid_patientid_embryos):,}")
        print(f"Embryos with invalid PatientID: {len(invalid_patientid_embryos):,}")
        
        # Check which valid PatientIDs are present in Clinisys
        valid_patientids = valid_patientid_embryos['patient_PatientID_clean'].unique()
        found_in_micro_prontuario = [pid for pid in valid_patientids if pid in clinisys_patientids]
        found_in_prontuario_1 = [pid for pid in valid_patientids if pid in clinisys_prontuario_1]
        found_in_prontuario_2 = [pid for pid in valid_patientids if pid in clinisys_prontuario_2]
        found_in_prontuario_3 = [pid for pid in valid_patientids if pid in clinisys_prontuario_3]
        found_in_prontuario_4 = [pid for pid in valid_patientids if pid in clinisys_prontuario_4]
        found_in_prontuario_5 = [pid for pid in valid_patientids if pid in clinisys_prontuario_5]
        
        # Combine all found PatientIDs
        all_found = found_in_micro_prontuario + found_in_prontuario_1 + found_in_prontuario_2 + found_in_prontuario_3 + found_in_prontuario_4 + found_in_prontuario_5
        found_in_either = list(set(all_found))
        not_found_in_clinisys = [pid for pid in valid_patientids if pid not in found_in_either]
        
        print(f"\nValid PatientIDs found in micro_prontuario: {len(found_in_micro_prontuario):,}")
        print(f"Valid PatientIDs found in prontuario_1: {len(found_in_prontuario_1):,}")
        print(f"Valid PatientIDs found in prontuario_2: {len(found_in_prontuario_2):,}")
        print(f"Valid PatientIDs found in prontuario_3: {len(found_in_prontuario_3):,}")
        print(f"Valid PatientIDs found in prontuario_4: {len(found_in_prontuario_4):,}")
        print(f"Valid PatientIDs found in prontuario_5: {len(found_in_prontuario_5):,}")
        print(f"Valid PatientIDs found in any prontuario field: {len(found_in_either):,}")
        print(f"Valid PatientIDs NOT found in Clinisys: {len(not_found_in_clinisys):,}")
        
        # Count embryos by PatientID presence
        embryos_with_found_patientid = valid_patientid_embryos[
            valid_patientid_embryos['patient_PatientID_clean'].isin(found_in_either)
        ]
        embryos_with_notfound_patientid = valid_patientid_embryos[
            valid_patientid_embryos['patient_PatientID_clean'].isin(not_found_in_clinisys)
        ]
        
        print(f"Embryos with PatientID found in Clinisys: {len(embryos_with_found_patientid):,}")
        print(f"Embryos with PatientID NOT found in Clinisys: {len(embryos_with_notfound_patientid):,}")
        
        # 2. Examples of PatientIDs found in Clinisys
        print(f"\n2. EXAMPLES: PATIENTIDS FOUND IN CLINISYS")
        print("-" * 50)
        
        if len(found_in_either) > 0:
            # Get sample embryos with PatientIDs found in Clinisys
            sample_found = embryos_with_found_patientid.head(10)
            print("Sample embryos with PatientID found in Clinisys:")
            print(sample_found[['embryo_EmbryoID', 'embryo_FertilizationTime', 'embryo_embryo_number', 'patient_PatientID', 'patient_PatientID_clean']].to_string(index=False))
            
            # Check Clinisys records for these PatientIDs
            print(f"\nClinisys records for these PatientIDs:")
            for _, row in sample_found.iterrows():
                patientid_clean = row['patient_PatientID_clean']
                date = row['embryo_FertilizationTime'].date()
                embryo_number = row['embryo_embryo_number']
                
                # Count total Clinisys records for this PatientID (all prontuario fields)
                total_clinisys = con.execute("""
                    SELECT COUNT(*) 
                    FROM gold.clinisys_embrioes 
                    WHERE micro_prontuario = ? 
                       OR prontuario_1 = ? 
                       OR prontuario_2 = ? 
                       OR prontuario_3 = ? 
                       OR prontuario_4 = ? 
                       OR prontuario_5 = ?
                """, [patientid_clean, patientid_clean, patientid_clean, patientid_clean, patientid_clean, patientid_clean]).fetchone()[0]
                
                # Count Clinisys records for this PatientID on the same date (all prontuario fields)
                same_date_clinisys = con.execute("""
                    SELECT COUNT(*) 
                    FROM gold.clinisys_embrioes 
                    WHERE (micro_prontuario = ? 
                           OR prontuario_1 = ? 
                           OR prontuario_2 = ? 
                           OR prontuario_3 = ? 
                           OR prontuario_4 = ? 
                           OR prontuario_5 = ?)
                    AND CAST(micro_Data_DL AS DATE) = ?
                """, [patientid_clean, patientid_clean, patientid_clean, patientid_clean, patientid_clean, patientid_clean, date]).fetchone()[0]
                
                # Count Clinisys records for this PatientID with same embryo number (all prontuario fields)
                same_embryo_clinisys = con.execute("""
                    SELECT COUNT(*) 
                    FROM gold.clinisys_embrioes 
                    WHERE (micro_prontuario = ? 
                           OR prontuario_1 = ? 
                           OR prontuario_2 = ? 
                           OR prontuario_3 = ? 
                           OR prontuario_4 = ? 
                           OR prontuario_5 = ?)
                    AND oocito_embryo_number = ?
                """, [patientid_clean, patientid_clean, patientid_clean, patientid_clean, patientid_clean, patientid_clean, embryo_number]).fetchone()[0]
                
                # Check which field the PatientID was found in
                in_micro = patientid_clean in found_in_micro_prontuario
                in_prontuario_1 = patientid_clean in found_in_prontuario_1
                in_prontuario_2 = patientid_clean in found_in_prontuario_2
                in_prontuario_3 = patientid_clean in found_in_prontuario_3
                in_prontuario_4 = patientid_clean in found_in_prontuario_4
                in_prontuario_5 = patientid_clean in found_in_prontuario_5
                
                print(f"  PatientID {patientid_clean} ({row['embryo_EmbryoID']}):")
                print(f"    - Found in micro_prontuario: {in_micro}")
                print(f"    - Found in prontuario_1: {in_prontuario_1}")
                print(f"    - Found in prontuario_2: {in_prontuario_2}")
                print(f"    - Found in prontuario_3: {in_prontuario_3}")
                print(f"    - Found in prontuario_4: {in_prontuario_4}")
                print(f"    - Found in prontuario_5: {in_prontuario_5}")
                print(f"    - Total Clinisys records: {total_clinisys}")
                print(f"    - Same date records: {same_date_clinisys}")
                print(f"    - Same embryo number records: {same_embryo_clinisys}")
        else:
            print("No PatientIDs found in Clinisys")
        
        # 3. Examples of PatientIDs NOT found in Clinisys
        print(f"\n3. EXAMPLES: PATIENTIDS NOT FOUND IN CLINISYS")
        print("-" * 50)
        
        if len(not_found_in_clinisys) > 0:
            # Get sample embryos with PatientIDs not found in Clinisys
            sample_notfound = embryos_with_notfound_patientid.head(10)
            print("Sample embryos with PatientID NOT found in Clinisys:")
            print(sample_notfound[['embryo_EmbryoID', 'embryo_FertilizationTime', 'embryo_embryo_number', 'patient_PatientID', 'patient_PatientID_clean']].to_string(index=False))
            
            # Show some of the PatientIDs that are not in Clinisys
            print(f"\nSample PatientIDs not found in Clinisys:")
            for pid in not_found_in_clinisys[:10]:
                print(f"  {pid}")
        else:
            print("All PatientIDs are found in Clinisys")
        
        # 4. Null PatientID examples
        print(f"\n4. EXAMPLES: NULL PATIENTIDS")
        print("-" * 50)
        
        if len(invalid_patientid_embryos) > 0:
            print("Sample embryos with null PatientIDs:")
            sample_invalid = invalid_patientid_embryos.head(10)
            print(sample_invalid[['embryo_EmbryoID', 'embryo_FertilizationTime', 'embryo_embryo_number', 'patient_PatientID']].to_string(index=False))
            
            # Show count of null PatientIDs
            null_count = len(invalid_patientid_embryos)
            print(f"\nTotal embryos with null PatientID: {null_count:,}")
        else:
            print("No null PatientIDs found")
        
        # 5. Summary statistics
        print(f"\n5. SUMMARY STATISTICS")
        print("-" * 50)
        
        total_embryos = len(unmatched_df)
        valid_embryos = len(valid_patientid_embryos)
        found_embryos = len(embryos_with_found_patientid)
        notfound_embryos = len(embryos_with_notfound_patientid)
        invalid_embryos = len(invalid_patientid_embryos)
        
        print(f"Total unmatched embryos: {total_embryos:,} (100%)")
        print(f"  - Valid PatientID: {valid_embryos:,} ({valid_embryos/total_embryos*100:.1f}%)")
        print(f"    - Found in Clinisys: {found_embryos:,} ({found_embryos/total_embryos*100:.1f}%)")
        print(f"    - NOT found in Clinisys: {notfound_embryos:,} ({notfound_embryos/total_embryos*100:.1f}%)")
        print(f"  - Null PatientID: {invalid_embryos:,} ({invalid_embryos/total_embryos*100:.1f}%)")
        
        con.close()
        logger.info("PatientID presence analysis completed")
        
    except Exception as e:
        logger.error(f"Error in analysis: {e}")
        raise

if __name__ == '__main__':
    check_patientid_in_clinisys() 