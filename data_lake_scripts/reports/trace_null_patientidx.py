import duckdb
import pandas as pd
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

def trace_null_patientidx():
    """Trace the origin of NULL PatientIDx values in the data pipeline."""
    
    logger.info("Starting NULL PatientIDx tracing analysis")
    
    try:
        con = duckdb.connect('database/huntington_data_lake.duckdb')
        logger.info("Connected to data lake")
        
        print("=" * 80)
        print("TRACING NULL PATIENTIDX ORIGINS")
        print("=" * 80)
        
        # 1. Check gold layer - how many PatientIDs have NULL PatientIDx
        logger.info("Checking gold layer for NULL PatientIDx...")
        gold_null_query = """
        SELECT 
            COUNT(*) as total_patientids,
            COUNT(CASE WHEN patient_PatientIDx IS NULL THEN 1 END) as null_patientidx_count,
            COUNT(CASE WHEN patient_PatientIDx IS NOT NULL THEN 1 END) as not_null_patientidx_count
        FROM gold.embryoscope_embrioes
        WHERE patient_PatientID IS NOT NULL
        """
        
        gold_null_df = con.execute(gold_null_query).df()
        total_patientids = gold_null_df.iloc[0]['total_patientids']
        null_patientidx_count = gold_null_df.iloc[0]['null_patientidx_count']
        not_null_patientidx_count = gold_null_df.iloc[0]['not_null_patientidx_count']
        
        print(f"\n1. GOLD LAYER ANALYSIS")
        print("-" * 50)
        print(f"Total PatientIDs: {total_patientids:,}")
        print(f"NULL PatientIDx: {null_patientidx_count:,} ({null_patientidx_count/total_patientids*100:.1f}%)")
        print(f"NOT NULL PatientIDx: {not_null_patientidx_count:,} ({not_null_patientidx_count/total_patientids*100:.1f}%)")
        
        # 2. Check silver layer - how many PatientIDs exist in silver
        logger.info("Checking silver layer PatientID coverage...")
        silver_coverage_query = """
        SELECT 
            COUNT(DISTINCT p.PatientID) as silver_patientids,
            COUNT(DISTINCT e.PatientIDx) as silver_patientidx_count
        FROM silver_embryoscope.patients p
        LEFT JOIN silver_embryoscope.embryo_data e ON p.PatientIDx = e.PatientIDx
        """
        
        silver_coverage_df = con.execute(silver_coverage_query).df()
        silver_patientids = silver_coverage_df.iloc[0]['silver_patientids']
        silver_patientidx_count = silver_coverage_df.iloc[0]['silver_patientidx_count']
        
        print(f"\n2. SILVER LAYER ANALYSIS")
        print("-" * 50)
        print(f"Total PatientIDs in silver.patients: {silver_patientids:,}")
        print(f"PatientIDx with embryo data: {silver_patientidx_count:,}")
        
        # 3. Check which PatientIDs from gold are missing from silver
        logger.info("Finding PatientIDs in gold but missing from silver...")
        missing_from_silver_query = """
        SELECT 
            COUNT(DISTINCT g.patient_PatientID) as missing_from_silver_count
        FROM gold.embryoscope_embrioes g
        LEFT JOIN silver_embryoscope.patients p ON g.patient_PatientID = p.PatientID
        WHERE g.patient_PatientID IS NOT NULL
        AND p.PatientID IS NULL
        """
        
        missing_from_silver_df = con.execute(missing_from_silver_query).df()
        missing_from_silver_count = missing_from_silver_df.iloc[0]['missing_from_silver_count']
        
        print(f"\n3. MISSING FROM SILVER ANALYSIS")
        print("-" * 50)
        print(f"PatientIDs in gold but missing from silver: {missing_from_silver_count:,}")
        print(f"Coverage rate: {((total_patientids - missing_from_silver_count) / total_patientids * 100):.1f}%")
        
        # 4. Check individual silver databases
        logger.info("Checking individual silver databases...")
        silver_dbs_query = """
        SELECT 
            _location as database_name,
            COUNT(DISTINCT PatientID) as patient_count,
            COUNT(DISTINCT PatientIDx) as patientidx_count
        FROM silver_embryoscope.patients
        GROUP BY _location
        ORDER BY patient_count DESC
        """
        
        silver_dbs_df = con.execute(silver_dbs_query).df()
        
        print(f"\n4. INDIVIDUAL SILVER DATABASES")
        print("-" * 60)
        print(f"{'Database':<20} {'PatientIDs':<12} {'PatientIDx':<12}")
        print("-" * 60)
        
        for _, row in silver_dbs_df.iterrows():
            db_name = row['database_name']
            patient_count = row['patient_count']
            patientidx_count = row['patientidx_count']
            print(f"{str(db_name):<20} {patient_count:<12} {patientidx_count:<12}")
        
        # 5. Sample of PatientIDs with NULL PatientIDx in gold
        logger.info("Getting sample of PatientIDs with NULL PatientIDx...")
        sample_null_query = """
        SELECT DISTINCT 
            patient_PatientID,
            patient_PatientIDx,
            patient__location,
            COUNT(DISTINCT embryo_EmbryoID) as embryo_count,
            MIN(CAST(embryo_FertilizationTime AS DATE)) as first_date,
            MAX(CAST(embryo_FertilizationTime AS DATE)) as last_date
        FROM gold.embryoscope_embrioes
        WHERE patient_PatientID IS NOT NULL
        AND patient_PatientIDx IS NULL
        GROUP BY patient_PatientID, patient_PatientIDx, patient__location
        ORDER BY embryo_count DESC
        LIMIT 20
        """
        
        sample_null_df = con.execute(sample_null_query).df()
        
        print(f"\n5. SAMPLE OF PATIENTIDS WITH NULL PATIENTIDX (TOP 20 BY EMBRYO COUNT)")
        print("-" * 100)
        print(f"{'PatientID':<12} {'Location':<15} {'Embryo Count':<12} {'First Date':<12} {'Last Date':<12}")
        print("-" * 100)
        
        for _, row in sample_null_df.iterrows():
            patientid = row['patient_PatientID']
            location = row['patient__location'] if pd.notna(row['patient__location']) else 'NULL'
            embryo_count = row['embryo_count']
            first_date = row['first_date']
            last_date = row['last_date']
            print(f"{patientid:<12} {str(location):<15} {embryo_count:<12} {first_date:<12} {last_date:<12}")
        
        # 6. Check if these PatientIDs exist in any silver database
        logger.info("Checking if sample PatientIDs exist in silver databases...")
        sample_patientids = sample_null_df['patient_PatientID'].tolist()[:10]  # Check first 10
        
        if sample_patientids:
            check_silver_query = """
            SELECT 
                p.PatientID,
                p.PatientIDx,
                p._location,
                COUNT(DISTINCT e.EmbryoID) as embryo_count
            FROM silver_embryoscope.patients p
            LEFT JOIN silver_embryoscope.embryo_data e ON p.PatientIDx = e.PatientIDx
            WHERE p.PatientID IN ({})
            GROUP BY p.PatientID, p.PatientIDx, p._location
            ORDER BY embryo_count DESC
            """.format(','.join(map(str, sample_patientids)))
            
            check_silver_df = con.execute(check_silver_query).df()
            
            print(f"\n6. CHECKING IF SAMPLE PATIENTIDS EXIST IN SILVER")
            print("-" * 80)
            if len(check_silver_df) > 0:
                print(f"Found {len(check_silver_df)} PatientIDs in silver:")
                print(f"{'PatientID':<12} {'PatientIDx':<15} {'Location':<15} {'Embryo Count':<12}")
                print("-" * 80)
                for _, row in check_silver_df.iterrows():
                    patientid = row['PatientID']
                    patientidx = row['PatientIDx'] if pd.notna(row['PatientIDx']) else 'NULL'
                    location = row['_location'] if pd.notna(row['_location']) else 'NULL'
                    embryo_count = row['embryo_count']
                    print(f"{patientid:<12} {str(patientidx):<15} {str(location):<15} {embryo_count:<12}")
            else:
                print("None of the sample PatientIDs found in silver layer")
        
        # 7. Summary and recommendations
        print(f"\n7. SUMMARY AND RECOMMENDATIONS")
        print("-" * 50)
        print(f"• {null_patientidx_count:,} PatientIDs ({null_patientidx_count/total_patientids*100:.1f}%) have NULL PatientIDx in gold layer")
        print(f"• {missing_from_silver_count:,} PatientIDs are missing from silver layer entirely")
        print(f"• Silver layer coverage: {((total_patientids - missing_from_silver_count) / total_patientids * 100):.1f}%")
        print(f"\nPossible causes:")
        print(f"• Data quality issues during silver-to-gold transformation")
        print(f"• Missing PatientIDx mappings in silver layer")
        print(f"• Incomplete data extraction from individual databases")
        print(f"• PatientIDs added directly to gold without silver layer processing")
        
        con.close()
        logger.info("NULL PatientIDx tracing completed")
        
    except Exception as e:
        logger.error(f"Error in analysis: {e}")
        raise

if __name__ == '__main__':
    trace_null_patientidx() 