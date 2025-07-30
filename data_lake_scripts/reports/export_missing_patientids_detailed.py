import duckdb
import pandas as pd
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

def export_missing_patientids_detailed():
    """Export detailed list of PatientIDs not found in Clinisys with both PatientID and PatientIDx."""
    
    logger.info("Starting detailed PatientID export - finding PatientIDs not in Clinisys")
    
    try:
        con = duckdb.connect('database/huntington_data_lake.duckdb')
        logger.info("Connected to data lake")
        
        print("=" * 80)
        print("EXPORTING MISSING PATIENTIDS WITH DETAILED INFORMATION")
        print("=" * 80)
        
        # Get all unique PatientIDs from Embryoscope (gold layer)
        logger.info("Fetching unique PatientIDs from Embryoscope gold layer...")
        embryoscope_gold_query = """
        SELECT DISTINCT 
            patient_PatientID,
            COUNT(DISTINCT embryo_EmbryoID) as embryo_count,
            MIN(CAST(embryo_FertilizationTime AS DATE)) as first_date,
            MAX(CAST(embryo_FertilizationTime AS DATE)) as last_date,
            COUNT(DISTINCT YEAR(CAST(embryo_FertilizationTime AS DATE))) as year_span
        FROM gold.embryoscope_embrioes
        WHERE patient_PatientID IS NOT NULL
        GROUP BY patient_PatientID
        ORDER BY embryo_count DESC
        """
        
        embryoscope_gold_df = con.execute(embryoscope_gold_query).df()
        logger.info(f"Found {len(embryoscope_gold_df):,} unique PatientIDs in Embryoscope gold layer")
        
        # Get all unique PatientIDs from Clinisys
        logger.info("Fetching unique PatientIDs from Clinisys...")
        clinisys_patientids_query = """
        SELECT DISTINCT micro_prontuario
        FROM gold.clinisys_embrioes
        WHERE micro_prontuario IS NOT NULL
        """
        
        clinisys_patientids_df = con.execute(clinisys_patientids_query).df()
        clinisys_patientids_set = set(clinisys_patientids_df['micro_prontuario'].tolist())
        logger.info(f"Found {len(clinisys_patientids_set):,} unique PatientIDs in Clinisys")
        
        # Find PatientIDs in Embryoscope but not in Clinisys
        embryoscope_patientids_set = set(embryoscope_gold_df['patient_PatientID'].tolist())
        not_in_clinisys = embryoscope_patientids_set - clinisys_patientids_set
        
        logger.info(f"Found {len(not_in_clinisys):,} PatientIDs in Embryoscope but not in Clinisys")
        
        # Get detailed information from silver layer including PatientIDx
        logger.info("Fetching detailed information from silver layer...")
        
        # First, get the mapping from silver layer with clinic name
        silver_mapping_query = """
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
        """.format(','.join(map(str, list(not_in_clinisys)[:1000])))  # Limit to first 1000 for performance
        
        silver_mapping_df = con.execute(silver_mapping_query).df()
        logger.info(f"Found {len(silver_mapping_df):,} PatientIDs with silver layer mapping")
        
        # Only use PatientIDs with silver layer mapping (exclude NULL PatientIDx)
        combined_df = silver_mapping_df
        
        logger.info(f"Excluded {len(not_in_clinisys) - len(silver_mapping_df):,} PatientIDs without silver layer mapping")
        
        # Add additional analysis columns
        combined_df['is_recent'] = combined_df['last_date'] >= '2022-01-01'
        combined_df['is_historical'] = combined_df['last_date'] < '2022-01-01'
        combined_df['period'] = combined_df['is_recent'].map({True: 'Recent', False: 'Historical'})
        
        # Sort by embryo count descending
        combined_df = combined_df.sort_values('embryo_count', ascending=False)
        
        # Summary statistics
        print(f"\n1. SUMMARY STATISTICS")
        print("-" * 50)
        print(f"Total PatientIDs in Embryoscope: {len(embryoscope_patientids_set):,}")
        print(f"Total PatientIDs in Clinisys: {len(clinisys_patientids_set):,}")
        print(f"PatientIDs missing from Clinisys: {len(not_in_clinisys):,}")
        print(f"Coverage rate: {((len(embryoscope_patientids_set) - len(not_in_clinisys)) / len(embryoscope_patientids_set) * 100):.1f}%")
        
        # Recent vs Historical breakdown
        recent_missing = combined_df[combined_df['is_recent']]
        historical_missing = combined_df[combined_df['is_historical']]
        
        print(f"\n2. RECENT VS HISTORICAL BREAKDOWN")
        print("-" * 50)
        print(f"Recent PatientIDs (2022+): {len(recent_missing):,}")
        print(f"Historical PatientIDs (pre-2022): {len(historical_missing):,}")
        
        # Top PatientIDs by embryo count
        print(f"\n3. TOP 20 PATIENTIDS BY EMBRYO COUNT (NOT IN CLINISYS)")
        print("-" * 120)
        print(f"{'PatientID':<12} {'PatientIDx':<15} {'Clinic':<15} {'Embryo Count':<12} {'First Date':<12} {'Last Date':<12} {'Year Span':<10} {'Period':<10}")
        print("-" * 120)
        
        top_patientids = combined_df.head(20)
        for _, row in top_patientids.iterrows():
            patientid = row['patient_PatientID']
            patientidx = row['patient_PatientIDx'] if pd.notna(row['patient_PatientIDx']) else 'NULL'
            clinic_name = row['clinic_name'] if pd.notna(row['clinic_name']) else 'NULL'
            embryo_count = row['embryo_count']
            first_date = row['first_date']
            last_date = row['last_date']
            year_span = row['year_span']
            period = row['period']
            
            print(f"{patientid:<12} {str(patientidx):<15} {str(clinic_name):<15} {embryo_count:<12} {first_date:<12} {last_date:<12} {year_span:<10} {period:<10}")
        
        # Export to CSV with detailed information
        output_file = "data_lake_scripts/reports/exports/missing_patientids_detailed.csv"
        combined_df.to_csv(output_file, index=False)
        logger.info(f"Exported {len(combined_df):,} PatientIDs to {output_file}")
        
        # Export recent PatientIDs separately
        recent_output_file = "data_lake_scripts/reports/exports/missing_patientids_recent_2022plus.csv"
        recent_missing.to_csv(recent_output_file, index=False)
        logger.info(f"Exported {len(recent_missing):,} recent PatientIDs to {recent_output_file}")
        
        # Export historical PatientIDs separately
        historical_output_file = "data_lake_scripts/reports/exports/missing_patientids_historical_pre2022.csv"
        historical_missing.to_csv(historical_output_file, index=False)
        logger.info(f"Exported {len(historical_missing):,} historical PatientIDs to {historical_output_file}")
        
        # Summary by year
        print(f"\n4. SUMMARY BY YEAR (PATIENTIDS NOT IN CLINISYS)")
        print("-" * 60)
        
        year_analysis_query = """
        SELECT 
            YEAR(CAST(embryo_FertilizationTime AS DATE)) as year,
            COUNT(DISTINCT patient_PatientID) as unique_patientids,
            COUNT(DISTINCT embryo_EmbryoID) as total_embryos
        FROM gold.embryoscope_embrioes
        WHERE patient_PatientID IN ({})
        AND embryo_FertilizationTime IS NOT NULL
        GROUP BY YEAR(CAST(embryo_FertilizationTime AS DATE))
        ORDER BY year DESC
        """.format(','.join(map(str, list(not_in_clinisys)[:1000])))
        
        year_analysis_df = con.execute(year_analysis_query).df()
        
        if len(year_analysis_df) > 0:
            print(f"{'Year':<6} {'PatientIDs':<12} {'Embryos':<10}")
            print("-" * 30)
            for _, row in year_analysis_df.iterrows():
                year = row['year']
                patientids = row['unique_patientids']
                embryos = row['total_embryos']
                print(f"{year:<6} {patientids:<12} {embryos:<10}")
        
        # Final summary
        print(f"\n5. FINAL SUMMARY")
        print("-" * 50)
        print(f"• Total missing PatientIDs: {len(not_in_clinisys):,}")
        print(f"• Recent missing PatientIDs (2022+): {len(recent_missing):,}")
        print(f"• Historical missing PatientIDs (pre-2022): {len(historical_missing):,}")
        print(f"• Total embryos affected: {combined_df['embryo_count'].sum():,}")
        print(f"• Average embryos per missing PatientID: {combined_df['embryo_count'].mean():.1f}")
        print(f"• Data exported to:")
        print(f"  - {output_file} (complete list)")
        print(f"  - {recent_output_file} (recent only)")
        print(f"  - {historical_output_file} (historical only)")
        
        # Show sample of the exported data
        print(f"\n6. SAMPLE OF EXPORTED DATA")
        print("-" * 50)
        print("Columns in CSV files:")
        print("- patient_PatientID: PatientID from gold layer")
        print("- patient_PatientIDx: PatientIDx from silver layer (NULL if not found)")
        print("- clinic_name: Clinic name from silver layer (NULL if not found)")
        print("- embryo_count: Number of embryos for this PatientID")
        print("- first_date: First embryo date")
        print("- last_date: Last embryo date")
        print("- year_span: Number of years with data")
        print("- is_recent: Boolean for 2022+")
        print("- is_historical: Boolean for pre-2022")
        print("- period: 'Recent' or 'Historical'")
        
        con.close()
        logger.info("Detailed PatientID export completed")
        
    except Exception as e:
        logger.error(f"Error in analysis: {e}")
        raise

if __name__ == '__main__':
    export_missing_patientids_detailed() 