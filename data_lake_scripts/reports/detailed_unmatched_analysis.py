import duckdb
import pandas as pd
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

def detailed_unmatched_analysis():
    """Detailed analysis of why Embryoscope embryos are not matching."""
    
    logger.info("Starting detailed unmatched analysis")
    
    try:
        con = duckdb.connect('../database/huntington_data_lake.duckdb')
        logger.info("Connected to data lake")
        
        print("=" * 80)
        print("DETAILED UNMATCHED EMBRYOSCOPE ANALYSIS")
        print("=" * 80)
        
        # 1. Analyze PatientID issues
        print("\n1. PATIENT ID ANALYSIS")
        print("-" * 50)
        
        patientid_analysis = con.execute("""
            SELECT 
                CASE 
                    WHEN patient_PatientID IS NULL THEN 'NULL'
                    WHEN TRIM(patient_PatientID) = '' THEN 'EMPTY'
                    WHEN TRY_CAST(REPLACE(patient_PatientID, '.', '') AS INTEGER) IS NULL THEN 'INVALID_FORMAT'
                    ELSE 'VALID'
                END as patientid_status,
                COUNT(*) as count,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
            FROM gold.embryoscope_embrioes
            WHERE embryo_EmbryoID NOT IN (
                SELECT DISTINCT embryo_EmbryoID 
                FROM gold.embryoscope_clinisys_combined 
                WHERE embryo_EmbryoID IS NOT NULL
            )
            GROUP BY patientid_status
            ORDER BY count DESC
        """).df()
        
        print("PatientID status for unmatched embryos:")
        print(patientid_analysis.to_string(index=False))
        
        # Show sample invalid PatientIDs
        invalid_patientids = con.execute("""
            SELECT DISTINCT patient_PatientID, COUNT(*) as count
            FROM gold.embryoscope_embrioes
            WHERE embryo_EmbryoID NOT IN (
                SELECT DISTINCT embryo_EmbryoID 
                FROM gold.embryoscope_clinisys_combined 
                WHERE embryo_EmbryoID IS NOT NULL
            )
            AND (patient_PatientID IS NULL 
                 OR TRIM(patient_PatientID) = '' 
                 OR TRY_CAST(REPLACE(patient_PatientID, '.', '') AS INTEGER) IS NULL)
            GROUP BY patient_PatientID
            ORDER BY count DESC
            LIMIT 10
        """).df()
        
        print("\nTop 10 invalid PatientIDs in unmatched embryos:")
        print(invalid_patientids.to_string(index=False))
        
        # 2. Analyze date coverage
        print("\n2. DATE COVERAGE ANALYSIS")
        print("-" * 50)
        
        # Check if there are Clinisys records for the same dates as unmatched embryos
        date_coverage = con.execute("""
            SELECT 
                CAST(e.embryo_FertilizationTime AS DATE) as date,
                COUNT(DISTINCT e.embryo_EmbryoID) as unmatched_embryoscope,
                COUNT(DISTINCT c.oocito_id) as clinisys_records,
                CASE 
                    WHEN COUNT(DISTINCT c.oocito_id) > 0 THEN 'HAS_CLINISYS'
                    ELSE 'NO_CLINISYS'
                END as coverage_status
            FROM gold.embryoscope_embrioes e
            LEFT JOIN gold.clinisys_embrioes c
                ON CAST(c.micro_Data_DL AS DATE) = CAST(e.embryo_FertilizationTime AS DATE)
            WHERE e.embryo_EmbryoID NOT IN (
                SELECT DISTINCT embryo_EmbryoID 
                FROM gold.embryoscope_clinisys_combined 
                WHERE embryo_EmbryoID IS NOT NULL
            )
            GROUP BY CAST(e.embryo_FertilizationTime AS DATE)
            ORDER BY date DESC
            LIMIT 20
        """).df()
        
        print("Date coverage for unmatched embryos (last 20 dates):")
        print(date_coverage.to_string(index=False))
        
        # 3. Analyze embryo_number issues
        print("\n3. EMBRYO NUMBER ANALYSIS")
        print("-" * 50)
        
        embryo_number_analysis = con.execute("""
            SELECT 
                CASE 
                    WHEN embryo_embryo_number IS NULL THEN 'NULL'
                    ELSE 'HAS_NUMBER'
                END as embryo_number_status,
                COUNT(*) as count,
                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
            FROM gold.embryoscope_embrioes
            WHERE embryo_EmbryoID NOT IN (
                SELECT DISTINCT embryo_EmbryoID 
                FROM gold.embryoscope_clinisys_combined 
                WHERE embryo_EmbryoID IS NOT NULL
            )
            GROUP BY embryo_number_status
        """).df()
        
        print("Embryo number status for unmatched embryos:")
        print(embryo_number_analysis.to_string(index=False))
        
        # 4. Check for potential matches with different strategies
        print("\n4. POTENTIAL MATCH STRATEGIES")
        print("-" * 50)
        
        # Strategy 1: Date + embryo_number only (no PatientID)
        strategy1 = con.execute("""
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
        
        print(f"Strategy 1 - Date + embryo_number only: {strategy1:,} potential matches")
        
        # Strategy 2: Date + PatientID only (no embryo_number)
        strategy2 = con.execute("""
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
                ON CAST(c.micro_Data_DL AS DATE) = CAST(e.embryo_FertilizationTime AS DATE)
                AND c.micro_prontuario = e.patient_PatientID_clean
                AND e.patient_PatientID_clean IS NOT NULL
            WHERE e.embryo_EmbryoID NOT IN (
                SELECT DISTINCT embryo_EmbryoID 
                FROM gold.embryoscope_clinisys_combined 
                WHERE embryo_EmbryoID IS NOT NULL
            )
            AND c.oocito_id IS NOT NULL
        """).fetchone()[0]
        
        print(f"Strategy 2 - Date + PatientID only: {strategy2:,} potential matches")
        
        # Strategy 3: Date only
        strategy3 = con.execute("""
            SELECT COUNT(*) as potential_matches
            FROM gold.embryoscope_embrioes e
            LEFT JOIN gold.clinisys_embrioes c
                ON CAST(c.micro_Data_DL AS DATE) = CAST(e.embryo_FertilizationTime AS DATE)
            WHERE e.embryo_EmbryoID NOT IN (
                SELECT DISTINCT embryo_EmbryoID 
                FROM gold.embryoscope_clinisys_combined 
                WHERE embryo_EmbryoID IS NOT NULL
            )
            AND c.oocito_id IS NOT NULL
        """).fetchone()[0]
        
        print(f"Strategy 3 - Date only: {strategy3:,} potential matches")
        
        # 5. Sample cases for manual investigation
        print("\n5. SAMPLE CASES FOR MANUAL INVESTIGATION")
        print("-" * 50)
        
        # Sample unmatched embryos with valid PatientID
        valid_patientid_samples = con.execute("""
            SELECT 
                embryo_EmbryoID,
                embryo_FertilizationTime,
                embryo_embryo_number,
                patient_PatientID,
                treatment_TreatmentName
            FROM gold.embryoscope_embrioes
            WHERE embryo_EmbryoID NOT IN (
                SELECT DISTINCT embryo_EmbryoID 
                FROM gold.embryoscope_clinisys_combined 
                WHERE embryo_EmbryoID IS NOT NULL
            )
            AND patient_PatientID IS NOT NULL 
            AND TRIM(patient_PatientID) != ''
            AND TRY_CAST(REPLACE(patient_PatientID, '.', '') AS INTEGER) IS NOT NULL
            ORDER BY embryo_FertilizationTime DESC
            LIMIT 5
        """).df()
        
        print("Sample unmatched embryos with valid PatientID:")
        print(valid_patientid_samples.to_string(index=False))
        
        # Check if these specific cases have Clinisys records
        print("\nChecking Clinisys records for sample cases:")
        for _, row in valid_patientid_samples.iterrows():
            patientid_clean = int(str(row['patient_PatientID']).replace('.', ''))
            date = row['embryo_FertilizationTime'].date()
            embryo_number = row['embryo_embryo_number']
            
            clinisys_count = con.execute("""
                SELECT COUNT(*) 
                FROM gold.clinisys_embrioes 
                WHERE CAST(micro_Data_DL AS DATE) = ?
                AND micro_prontuario = ?
                AND oocito_embryo_number = ?
            """, [date, patientid_clean, embryo_number]).fetchone()[0]
            
            print(f"  {row['embryo_EmbryoID']}: {clinisys_count} Clinisys records found")
        
        con.close()
        logger.info("Detailed analysis completed")
        
    except Exception as e:
        logger.error(f"Error in detailed analysis: {e}")
        raise

if __name__ == '__main__':
    detailed_unmatched_analysis() 