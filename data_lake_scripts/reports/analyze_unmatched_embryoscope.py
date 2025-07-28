import duckdb
import pandas as pd
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

def analyze_unmatched_embryoscope():
    """Analyze Embryoscope embryos that don't have matches in Clinisys."""
    
    logger.info("Starting analysis of unmatched Embryoscope embryos")
    
    try:
        # Connect to the data lake
        con = duckdb.connect('../database/huntington_data_lake.duckdb')
        logger.info("Connected to data lake")
        
        # Get all Embryoscope embryos
        logger.info("Fetching all Embryoscope embryos...")
        embryoscope_query = """
        SELECT 
            embryo_EmbryoID,
            embryo_FertilizationTime,
            embryo_embryo_number,
            patient_PatientID,
            treatment_TreatmentName,
            embryo_EmbryoFate,
            embryo_WellNumber
        FROM gold.embryoscope_embrioes
        ORDER BY embryo_FertilizationTime DESC
        """
        
        embryoscope_df = con.execute(embryoscope_query).df()
        logger.info(f"Found {len(embryoscope_df):,} Embryoscope embryos")
        
        # Get matched Embryoscope embryos from the combined table
        logger.info("Fetching matched Embryoscope embryos...")
        matched_query = """
        SELECT DISTINCT
            embryo_EmbryoID,
            embryo_FertilizationTime,
            embryo_embryo_number,
            patient_PatientID,
            treatment_TreatmentName,
            embryo_EmbryoFate,
            embryo_WellNumber
        FROM gold.embryoscope_clinisys_combined
        WHERE embryo_EmbryoID IS NOT NULL
        ORDER BY embryo_FertilizationTime DESC
        """
        
        matched_df = con.execute(matched_query).df()
        logger.info(f"Found {len(matched_df):,} matched Embryoscope embryos")
        
        # Find unmatched embryos
        unmatched_df = embryoscope_df[~embryoscope_df['embryo_EmbryoID'].isin(matched_df['embryo_EmbryoID'])]
        logger.info(f"Found {len(unmatched_df):,} unmatched Embryoscope embryos")
        
        # Analysis of unmatched embryos
        print("=" * 80)
        print("UNMATCHED EMBRYOSCOPE EMBRYOS ANALYSIS")
        print("=" * 80)
        print(f"Total Embryoscope embryos: {len(embryoscope_df):,}")
        print(f"Matched embryos: {len(matched_df):,}")
        print(f"Unmatched embryos: {len(unmatched_df):,}")
        print(f"Match rate: {(len(matched_df)/len(embryoscope_df)*100):.2f}%")
        
        if len(unmatched_df) > 0:
            print("\n" + "=" * 50)
            print("SAMPLE UNMATCHED EMBRYOS")
            print("=" * 50)
            print(unmatched_df.head(20).to_string(index=False))
            
            # Analyze reasons for no matches
            print("\n" + "=" * 50)
            print("ANALYSIS OF UNMATCHED EMBRYOS")
            print("=" * 50)
            
            # Check for null PatientID
            null_patientid = unmatched_df['patient_PatientID'].isnull().sum()
            print(f"Embryos with null PatientID: {null_patientid:,} ({(null_patientid/len(unmatched_df)*100):.2f}%)")
            
            # Check for null embryo_number
            null_embryo_number = unmatched_df['embryo_embryo_number'].isnull().sum()
            print(f"Embryos with null embryo_number: {null_embryo_number:,} ({(null_embryo_number/len(unmatched_df)*100):.2f}%)")
            
            # Check for null FertilizationTime
            null_fertilization = unmatched_df['embryo_FertilizationTime'].isnull().sum()
            print(f"Embryos with null FertilizationTime: {null_fertilization:,} ({(null_fertilization/len(unmatched_df)*100):.2f}%)")
            
            # Date range analysis
            print(f"\nDate range of unmatched embryos:")
            print(f"  Earliest: {unmatched_df['embryo_FertilizationTime'].min()}")
            print(f"  Latest: {unmatched_df['embryo_FertilizationTime'].max()}")
            
            # Check if there are Clinisys records for the same dates
            print(f"\nChecking Clinisys coverage for unmatched dates...")
            
            # Get unique dates from unmatched embryos
            unmatched_dates = unmatched_df['embryo_FertilizationTime'].dt.date.unique()
            logger.info(f"Found {len(unmatched_dates)} unique dates in unmatched embryos")
            
            # Check Clinisys coverage for these dates
            clinisys_coverage = []
            for date in unmatched_dates[:10]:  # Check first 10 dates
                clinisys_count = con.execute("""
                    SELECT COUNT(*) 
                    FROM gold.clinisys_embrioes 
                    WHERE CAST(micro_Data_DL AS DATE) = ?
                """, [date]).fetchone()[0]
                
                embryoscope_count = len(unmatched_df[unmatched_df['embryo_FertilizationTime'].dt.date == date])
                
                clinisys_coverage.append({
                    'date': date,
                    'clinisys_records': clinisys_count,
                    'unmatched_embryoscope': embryoscope_count
                })
            
            coverage_df = pd.DataFrame(clinisys_coverage)
            print("\nClinisys coverage for unmatched embryo dates (first 10):")
            print(coverage_df.to_string(index=False))
            
            # Check for potential matches with relaxed criteria
            print(f"\n" + "=" * 50)
            print("POTENTIAL MATCHES WITH RELAXED CRITERIA")
            print("=" * 50)
            
            # Try matching with 1-day tolerance
            relaxed_matches = con.execute("""
                SELECT COUNT(*) as potential_matches
                FROM (
                    SELECT *,
                        CASE 
                            WHEN patient_PatientID IS NULL OR TRIM(patient_PatientID) = '' THEN NULL
                            WHEN TRY_CAST(REPLACE(patient_PatientID, '.', '') AS INTEGER) IS NULL THEN NULL
                            ELSE TRY_CAST(REPLACE(patient_PatientID, '.', '') AS INTEGER)
                        END as patient_PatientID_clean
                    FROM gold.embryoscope_embrioes
                ) e
                LEFT JOIN gold.clinisys_embrioes c
                    ON ABS(DATE_DIFF('day', CAST(c.micro_Data_DL AS DATE), CAST(e.embryo_FertilizationTime AS DATE))) <= 1
                    AND c.oocito_embryo_number = e.embryo_embryo_number
                    AND c.micro_prontuario = e.patient_PatientID_clean
                    AND e.patient_PatientID_clean IS NOT NULL
                WHERE e.embryo_EmbryoID NOT IN (
                    SELECT DISTINCT embryo_EmbryoID 
                    FROM gold.embryoscope_clinisys_combined 
                    WHERE embryo_EmbryoID IS NOT NULL
                )
                AND c.oocito_id IS NOT NULL
            """).fetchone()[0]
            
            print(f"Potential matches with 1-day tolerance: {relaxed_matches:,}")
            
            # Try matching with 2-day tolerance
            relaxed_matches_2 = con.execute("""
                SELECT COUNT(*) as potential_matches
                FROM (
                    SELECT *,
                        CASE 
                            WHEN patient_PatientID IS NULL OR TRIM(patient_PatientID) = '' THEN NULL
                            WHEN TRY_CAST(REPLACE(patient_PatientID, '.', '') AS INTEGER) IS NULL THEN NULL
                            ELSE TRY_CAST(REPLACE(patient_PatientID, '.', '') AS INTEGER)
                        END as patient_PatientID_clean
                    FROM gold.embryoscope_embrioes
                ) e
                LEFT JOIN gold.clinisys_embrioes c
                    ON ABS(DATE_DIFF('day', CAST(c.micro_Data_DL AS DATE), CAST(e.embryo_FertilizationTime AS DATE))) <= 2
                    AND c.oocito_embryo_number = e.embryo_embryo_number
                    AND c.micro_prontuario = e.patient_PatientID_clean
                    AND e.patient_PatientID_clean IS NOT NULL
                WHERE e.embryo_EmbryoID NOT IN (
                    SELECT DISTINCT embryo_EmbryoID 
                    FROM gold.embryoscope_clinisys_combined 
                    WHERE embryo_EmbryoID IS NOT NULL
                )
                AND c.oocito_id IS NOT NULL
            """).fetchone()[0]
            
            print(f"Potential matches with 2-day tolerance: {relaxed_matches_2:,}")
            
            # Check for embryos that could match if we ignore PatientID
            date_only_matches = con.execute("""
                SELECT COUNT(*) as potential_matches
                FROM gold.embryoscope_embrioes e
                LEFT JOIN gold.clinisys_embrioes c
                    ON CAST(c.micro_Data_DL AS DATE) = CAST(e.embryo_FertilizationTime AS DATE)
                    AND c.oocito_embryo_number = e.embryo_embryo_number
                WHERE e.embryo_EmbryoID NOT IN (
                    SELECT DISTINCT embryo_EmbryoID 
                    FROM gold.embryoscope_clinisys_combined 
                    WHERE embryo_EmbryoID IS NOT NULL
                )
                AND c.oocito_id IS NOT NULL
            """).fetchone()[0]
            
            print(f"Potential matches with date + embryo_number only: {date_only_matches:,}")
        
        con.close()
        logger.info("Analysis completed")
        
    except Exception as e:
        logger.error(f"Error in analysis: {e}")
        raise

if __name__ == '__main__':
    analyze_unmatched_embryoscope() 