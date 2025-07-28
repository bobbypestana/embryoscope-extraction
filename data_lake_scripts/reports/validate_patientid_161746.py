import duckdb
import pandas as pd
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

def validate_patientid_161746():
    """Validate if all embryos from PatientID 161746 actually found matches."""
    
    logger.info("Starting validation for PatientID 161746")
    
    try:
        con = duckdb.connect('../database/huntington_data_lake.duckdb')
        logger.info("Connected to data lake")
        
        print("=" * 80)
        print("VALIDATION: PATIENTID 161746")
        print("=" * 80)
        
        # 1. Get all Embryoscope embryos for PatientID 161746
        logger.info("Fetching Embryoscope embryos for PatientID 161746...")
        embryoscope_query = """
        SELECT 
            embryo_EmbryoID,
            embryo_FertilizationTime,
            embryo_embryo_number,
            patient_PatientID
        FROM gold.embryoscope_embrioes 
        WHERE patient_PatientID = 161746
        ORDER BY embryo_FertilizationTime
        """
        
        embryoscope_df = con.execute(embryoscope_query).df()
        logger.info(f"Found {len(embryoscope_df):,} Embryoscope embryos for PatientID 161746")
        
        # 2. Get all Clinisys records for PatientID 161746
        logger.info("Fetching Clinisys records for PatientID 161746...")
        clinisys_query = """
        SELECT 
            micro_prontuario,
            micro_Data_DL,
            oocito_embryo_number
        FROM gold.clinisys_embrioes 
        WHERE micro_prontuario = 161746
        ORDER BY micro_Data_DL
        """
        
        clinisys_df = con.execute(clinisys_query).df()
        logger.info(f"Found {len(clinisys_df):,} Clinisys records for PatientID 161746")
        
        # 3. Get all combined matches for PatientID 161746
        logger.info("Fetching combined matches for PatientID 161746...")
        combined_query = """
        SELECT 
            embryo_EmbryoID,
            micro_prontuario,
            embryo_FertilizationTime,
            micro_Data_DL,
            embryo_embryo_number,
            oocito_embryo_number
        FROM gold.embryoscope_clinisys_combined 
        WHERE micro_prontuario = 161746
        ORDER BY embryo_FertilizationTime
        """
        
        combined_df = con.execute(combined_query).df()
        logger.info(f"Found {len(combined_df):,} combined matches for PatientID 161746")
        
        # 4. Analysis
        print(f"\n1. SUMMARY FOR PATIENTID 161746")
        print("-" * 50)
        print(f"Total Embryoscope embryos: {len(embryoscope_df):,}")
        print(f"Total Clinisys records: {len(clinisys_df):,}")
        print(f"Total combined matches: {len(combined_df):,}")
        
        # 5. Check if all Embryoscope embryos found matches
        embryoscope_embryo_ids = set(embryoscope_df['embryo_EmbryoID'].tolist())
        combined_embryo_ids = set(combined_df['embryo_EmbryoID'].tolist())
        
        matched_embryos = embryoscope_embryo_ids.intersection(combined_embryo_ids)
        unmatched_embryos = embryoscope_embryo_ids - combined_embryo_ids
        
        print(f"\n2. MATCH ANALYSIS")
        print("-" * 50)
        print(f"Embryoscope embryos that found matches: {len(matched_embryos):,}")
        print(f"Embryoscope embryos that did NOT find matches: {len(unmatched_embryos):,}")
        print(f"Match rate: {len(matched_embryos)/len(embryoscope_embryo_ids)*100:.1f}%")
        
        # 6. Show details
        print(f"\n3. EMBRYOSCOPE EMBRYOS DETAILS")
        print("-" * 50)
        print(embryoscope_df.to_string(index=False))
        
        print(f"\n4. CLINISYS RECORDS DETAILS")
        print("-" * 50)
        print(clinisys_df.to_string(index=False))
        
        print(f"\n5. COMBINED MATCHES DETAILS")
        print("-" * 50)
        print(combined_df.to_string(index=False))
        
        # 7. Show unmatched embryos if any
        if len(unmatched_embryos) > 0:
            print(f"\n6. UNMATCHED EMBRYOS")
            print("-" * 50)
            unmatched_df = embryoscope_df[embryoscope_df['embryo_EmbryoID'].isin(unmatched_embryos)]
            print(unmatched_df.to_string(index=False))
        else:
            print(f"\n6. UNMATCHED EMBRYOS")
            print("-" * 50)
            print("All embryos found matches! ✅")
        
        # 8. Check if this PatientID appears in the unmatched analysis
        print(f"\n7. VALIDATION AGAINST ANALYSIS SCRIPT")
        print("-" * 50)
        
        # Check if PatientID 161746 appears in the unmatched analysis
        unmatched_check_query = """
        SELECT COUNT(*) 
        FROM gold.embryoscope_embrioes
        WHERE embryo_EmbryoID NOT IN (
            SELECT DISTINCT embryo_EmbryoID 
            FROM gold.embryoscope_clinisys_combined 
            WHERE embryo_EmbryoID IS NOT NULL
        )
        AND patient_PatientID = 161746
        AND YEAR(CAST(embryo_FertilizationTime AS DATE)) = 2025
        """
        
        unmatched_count = con.execute(unmatched_check_query).fetchone()[0]
        print(f"PatientID 161746 embryos in 2025 unmatched analysis: {unmatched_count}")
        
        if unmatched_count > 0:
            print("❌ ISSUE FOUND: PatientID 161746 appears in unmatched analysis but should be matched!")
        else:
            print("✅ CORRECT: PatientID 161746 does not appear in unmatched analysis")
        
        con.close()
        logger.info("Validation completed")
        
    except Exception as e:
        logger.error(f"Error in validation: {e}")
        raise

if __name__ == '__main__':
    validate_patientid_161746() 