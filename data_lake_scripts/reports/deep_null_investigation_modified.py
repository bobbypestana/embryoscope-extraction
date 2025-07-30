import duckdb
import pandas as pd
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

def deep_null_investigation():
    """Deep investigation of NULL PatientIDx values in the data pipeline."""
    
    logger.info("Starting deep NULL PatientIDx investigation")
    
    try:
        con = duckdb.connect('database/huntington_data_lake.duckdb')
        logger.info("Connected to data lake")
        
        print("=" * 80)
        print("DEEP NULL PATIENTIDX INVESTIGATION")
        print("=" * 80)
        
        # 1. Check the actual data we were working with in the export script
        logger.info("Checking the data from our export script...")
        
        # Get PatientIDs not in Clinisys (same as export script)
        embryoscope_gold_query = """
        SELECT DISTINCT 
            patient_PatientID,
            COUNT(DISTINCT embryo_EmbryoID) as embryo_count
        FROM gold.embryoscope_embrioes
        WHERE patient_PatientID IS NOT NULL
        GROUP BY patient_PatientID
        """
        
        embryoscope_gold_df = con.execute(embryoscope_gold_query).df()
        embryoscope_patientids_set = set(embryoscope_gold_df['patient_PatientID'].tolist())
        
        clinisys_patientids_query = """
        SELECT DISTINCT micro_prontuario
        FROM gold.clinisys_embrioes
        WHERE micro_prontuario IS NOT NULL
        """
        
        clinisys_patientids_df = con.execute(clinisys_patientids_query).df()
        clinisys_patientids_set = set(clinisys_patientids_df['micro_prontuario'].tolist())
        
        not_in_clinisys = embryoscope_patientids_set - clinisys_patientids_set
        
        print(f"\n1. PATIENTIDS NOT IN CLINISYS")
        print("-" * 50)
        print(f"Total PatientIDs not in Clinisys: {len(not_in_clinisys):,}")
        
        # 2. Check how many of these have silver layer mapping
        logger.info("Checking silver layer mapping for missing PatientIDs...")
        
        # Get silver mapping for first 1000 PatientIDs
        sample_not_in_clinisys = list(not_in_clinisys)[:1000]
        
        silver_mapping_query = """
        SELECT DISTINCT 
            p.PatientID as patient_PatientID,
            p.PatientIDx as patient_PatientIDx,
            p._location as clinic_name
        FROM silver_embryoscope.patients p
        WHERE p.PatientID IN ({})
        """.format(','.join(map(str, sample_not_in_clinisys)))
        
        silver_mapping_df = con.execute(silver_mapping_query).df()
        
        print(f"\n2. SILVER LAYER MAPPING ANALYSIS")
        print("-" * 50)
        print(f"Sample size: {len(sample_not_in_clinisys):,}")
        print(f"Found in silver: {len(silver_mapping_df):,}")
        print(f"Missing from silver: {len(sample_not_in_clinisys) - len(silver_mapping_df):,}")
        
        # 3. Check the actual PatientIDx values in silver
        if len(silver_mapping_df) > 0:
            null_patientidx_in_silver = silver_mapping_df['patient_PatientIDx'].isnull().sum()
            print(f"NULL PatientIDx in silver: {null_patientidx_in_silver:,}")
            print(f"NOT NULL PatientIDx in silver: {len(silver_mapping_df) - null_patientidx_in_silver:,}")
        
        # 4. Check gold layer PatientIDx for these specific PatientIDs
        logger.info("Checking gold layer PatientIDx for missing PatientIDs...")
        
        gold_check_query = """
        SELECT DISTINCT 
            patient_PatientID,
            patient_PatientIDx,
            COUNT(DISTINCT embryo_EmbryoID) as embryo_count
        FROM gold.embryoscope_embrioes
        WHERE patient_PatientID IN ({})
        GROUP BY patient_PatientID, patient_PatientIDx
        ORDER BY embryo_count DESC
        LIMIT 20
        """.format(','.join(map(str, sample_not_in_clinisys)))
        
        gold_check_df = con.execute(gold_check_query).df()
        
        print(f"\n3. GOLD LAYER PATIENTIDX FOR MISSING PATIENTIDS (SAMPLE)")
        print("-" * 80)
        print(f"{'PatientID':<12} {'PatientIDx':<20} {'Embryo Count':<12}")
        print("-" * 80)
        
        for _, row in gold_check_df.iterrows():
            patientid = row['patient_PatientID']
            patientidx = row['patient_PatientIDx'] if pd.notna(row['patient_PatientIDx']) else 'NULL'
            embryo_count = row['embryo_count']
            print(f"{patientid:<12} {str(patientidx):<20} {embryo_count:<12}")
        
        # 5. Check if there are any NULL PatientIDx in gold at all
        logger.info("Checking for any NULL PatientIDx in gold layer...")
        
        any_null_query = """
        SELECT 
            COUNT(*) as total_records,
            COUNT(CASE WHEN patient_PatientIDx IS NULL THEN 1 END) as null_count,
            COUNT(CASE WHEN patient_PatientIDx IS NOT NULL THEN 1 END) as not_null_count
        FROM gold.embryoscope_embrioes
        """
        
        any_null_df = con.execute(any_null_query).df()
        total_records = any_null_df.iloc[0]['total_records']
        null_count = any_null_df.iloc[0]['null_count']
        not_null_count = any_null_df.iloc[0]['not_null_count']
        
        print(f"\n4. GOLD LAYER NULL ANALYSIS (ALL RECORDS)")
        print("-" * 50)
        print(f"Total records: {total_records:,}")
        print(f"NULL PatientIDx: {null_count:,} ({null_count/total_records*100:.1f}%)")
        print(f"NOT NULL PatientIDx: {not_null_count:,} ({not_null_count/total_records*100:.1f}%)")
        
        # 6. Check the schema of gold.embryoscope_embrioes
        logger.info("Checking gold layer schema...")
        
        schema_query = """
        DESCRIBE gold.embryoscope_embrioes
        """
        
        schema_df = con.execute(schema_query).df()
        
        print(f"\n5. GOLD LAYER SCHEMA")
        print("-" * 50)
        print(f"{'Column':<25} {'Type':<15} {'Null':<8} {'Key':<8}")
        print("-" * 50)
        
        for _, row in schema_df.iterrows():
            column = row['column_name']
            col_type = row['column_type']
            null_ok = row['null']
            key = row['key']
            print(f"{column:<25} {col_type:<15} {str(null_ok):<8} {str(key):<8}")
        
        # 7. Check if the issue is in our export script logic
        logger.info("Checking export script logic...")
        
        # Simulate the exact logic from our export script
        silver_mapping_full_query = """
        SELECT DISTINCT 
            p.PatientID as patient_PatientID,
            p.PatientIDx as patient_PatientIDx,
            p._location as clinic_name,
            COUNT(DISTINCT e.EmbryoID) as embryo_count,
            MIN(CAST(e.FertilizationTime AS DATE)) as first_date,
            MAX(CAST(e.FertilizationTime AS DATE)) as last_date,
            COUNT(DISTINCT YEAR(CAST(e.FertilizationTime AS DATE))) as year_span
        FROM silver_embryoscope.embryo_data e
        JOIN silver_embryoscope.patients p ON e.PatientIDx = p.PatientIDx
        WHERE p.PatientID IN ({})
        GROUP BY p.PatientID, p.PatientIDx, p._location
        ORDER BY embryo_count DESC
        """.format(','.join(map(str, sample_not_in_clinisys)))
        
        silver_mapping_full_df = con.execute(silver_mapping_full_query).df()
        
        print(f"\n6. EXPORT SCRIPT LOGIC ANALYSIS")
        print("-" * 50)
        print(f"PatientIDs with silver mapping: {len(silver_mapping_full_df):,}")
        
        if len(silver_mapping_full_df) > 0:
            null_patientidx_in_export = silver_mapping_full_df['patient_PatientIDx'].isnull().sum()
            print(f"NULL PatientIDx in export data: {null_patientidx_in_export:,}")
            print(f"NOT NULL PatientIDx in export data: {len(silver_mapping_full_df) - null_patientidx_in_export:,}")
        
        # 8. Check if the issue is in the JOIN condition
        logger.info("Checking JOIN condition...")
        
        # Check if there are PatientIDs in silver.patients but not in silver.embryo_data
        orphan_patients_query = """
        SELECT 
            COUNT(DISTINCT p.PatientID) as orphan_patients
        FROM silver_embryoscope.patients p
        LEFT JOIN silver_embryoscope.embryo_data e ON p.PatientIDx = e.PatientIDx
        WHERE p.PatientID IN ({})
        AND e.PatientIDx IS NULL
        """.format(','.join(map(str, sample_not_in_clinisys)))
        
        orphan_patients_df = con.execute(orphan_patients_query).df()
        orphan_patients = orphan_patients_df.iloc[0]['orphan_patients']
        
        print(f"\n7. JOIN CONDITION ANALYSIS")
        print("-" * 50)
        print(f"PatientIDs in silver.patients but not in silver.embryo_data: {orphan_patients:,}")
        
        # 9. Summary and comparison with our findings
        print(f"\n8. SUMMARY AND COMPARISON")
        print("-" * 50)
        print(f"Total embryoscope PatientIDs in gold: {len(embryoscope_patientids_set):,}")
        print(f"Total clinisys PatientIDs in gold: {len(clinisys_patientids_set):,}")
        print(f"PatientIDs not in clinisys: {len(not_in_clinisys):,}")
        print(f"Sample checked in silver: {len(sample_not_in_clinisys):,}")
        print(f"Found in silver: {len(silver_mapping_df):,}")
        print(f"Missing from silver: {len(sample_not_in_clinisys) - len(silver_mapping_df):,}")
        
        # Calculate the percentage
        if len(sample_not_in_clinisys) > 0:
            missing_percentage = (len(sample_not_in_clinisys) - len(silver_mapping_df)) / len(sample_not_in_clinisys) * 100
            print(f"Missing percentage in sample: {missing_percentage:.1f}%")
            
            # Extrapolate to full dataset
            estimated_missing = int(len(not_in_clinisys) * missing_percentage / 100)
            print(f"Estimated missing in full dataset: {estimated_missing:,}")
        
        con.close()
        logger.info("Deep NULL investigation completed")
        
    except Exception as e:
        logger.error(f"Error in analysis: {e}")
        raise

if __name__ == '__main__':
    deep_null_investigation() 