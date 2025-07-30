import duckdb
import pandas as pd
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

def list_patientids_not_in_clinisys():
    """List all PatientIDs from Embryoscope that were not found in Clinisys."""
    
    logger.info("Starting PatientID analysis - finding PatientIDs not in Clinisys")
    
    try:
        con = duckdb.connect('database/huntington_data_lake.duckdb')
        logger.info("Connected to data lake")
        
        print("=" * 80)
        print("PATIENTIDS NOT FOUND IN CLINISYS")
        print("=" * 80)
        
        # Get all unique PatientIDs from Embryoscope
        logger.info("Fetching unique PatientIDs from Embryoscope...")
        embryoscope_patientids_query = """
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
        
        embryoscope_patientids_df = con.execute(embryoscope_patientids_query).df()
        logger.info(f"Found {len(embryoscope_patientids_df):,} unique PatientIDs in Embryoscope")
        
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
        embryoscope_patientids_set = set(embryoscope_patientids_df['patient_PatientID'].tolist())
        not_in_clinisys = embryoscope_patientids_set - clinisys_patientids_set
        
        logger.info(f"Found {len(not_in_clinisys):,} PatientIDs in Embryoscope but not in Clinisys")
        
        # Filter the Embryoscope data to only include PatientIDs not in Clinisys
        missing_patientids_df = embryoscope_patientids_df[
            embryoscope_patientids_df['patient_PatientID'].isin(not_in_clinisys)
        ].copy()
        
        # Add additional analysis
        missing_patientids_df['is_recent'] = missing_patientids_df['last_date'] >= '2022-01-01'
        missing_patientids_df['is_historical'] = missing_patientids_df['last_date'] < '2022-01-01'
        
        # Summary statistics
        print(f"\n1. SUMMARY STATISTICS")
        print("-" * 50)
        print(f"Total PatientIDs in Embryoscope: {len(embryoscope_patientids_set):,}")
        print(f"Total PatientIDs in Clinisys: {len(clinisys_patientids_set):,}")
        print(f"PatientIDs missing from Clinisys: {len(not_in_clinisys):,}")
        print(f"Coverage rate: {((len(embryoscope_patientids_set) - len(not_in_clinisys)) / len(embryoscope_patientids_set) * 100):.1f}%")
        
        # Recent vs Historical breakdown
        recent_missing = missing_patientids_df[missing_patientids_df['is_recent']]
        historical_missing = missing_patientids_df[missing_patientids_df['is_historical']]
        
        print(f"\n2. RECENT VS HISTORICAL BREAKDOWN")
        print("-" * 50)
        print(f"Recent PatientIDs (2022+): {len(recent_missing):,}")
        print(f"Historical PatientIDs (pre-2022): {len(historical_missing):,}")
        
        # Embryo count distribution
        print(f"\n3. EMBRYO COUNT DISTRIBUTION")
        print("-" * 50)
        embryo_count_stats = missing_patientids_df['embryo_count'].describe()
        print(f"Mean embryos per PatientID: {embryo_count_stats['mean']:.1f}")
        print(f"Median embryos per PatientID: {embryo_count_stats['50%']:.1f}")
        print(f"Min embryos per PatientID: {embryo_count_stats['min']:.0f}")
        print(f"Max embryos per PatientID: {embryo_count_stats['max']:.0f}")
        
        # Top PatientIDs by embryo count
        print(f"\n4. TOP 20 PATIENTIDS BY EMBRYO COUNT (NOT IN CLINISYS)")
        print("-" * 80)
        print(f"{'PatientID':<12} {'Embryo Count':<12} {'First Date':<12} {'Last Date':<12} {'Year Span':<10} {'Period':<10}")
        print("-" * 80)
        
        top_patientids = missing_patientids_df.nlargest(20, 'embryo_count')
        for _, row in top_patientids.iterrows():
            patientid = row['patient_PatientID']
            embryo_count = row['embryo_count']
            first_date = row['first_date']
            last_date = row['last_date']
            year_span = row['year_span']
            period = "Recent" if row['is_recent'] else "Historical"
            
            print(f"{patientid:<12} {embryo_count:<12} {first_date:<12} {last_date:<12} {year_span:<10} {period:<10}")
        
        # Recent PatientIDs (2022+)
        if len(recent_missing) > 0:
            print(f"\n5. RECENT PATIENTIDS (2022+) - TOP 20")
            print("-" * 80)
            print(f"{'PatientID':<12} {'Embryo Count':<12} {'First Date':<12} {'Last Date':<12} {'Year Span':<10}")
            print("-" * 80)
            
            recent_top = recent_missing.nlargest(20, 'embryo_count')
            for _, row in recent_top.iterrows():
                patientid = row['patient_PatientID']
                embryo_count = row['embryo_count']
                first_date = row['first_date']
                last_date = row['last_date']
                year_span = row['year_span']
                
                print(f"{patientid:<12} {embryo_count:<12} {first_date:<12} {last_date:<12} {year_span:<10}")
        
        # Export to CSV
        output_file = "missing_patientids_in_clinisys.csv"
        missing_patientids_df.to_csv(output_file, index=False)
        logger.info(f"Exported {len(missing_patientids_df):,} PatientIDs to {output_file}")
        
        # Summary by year
        print(f"\n6. SUMMARY BY YEAR (PATIENTIDS NOT IN CLINISYS)")
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
        """.format(','.join(map(str, list(not_in_clinisys)[:1000])))  # Limit to first 1000 for performance
        
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
        print(f"\n7. FINAL SUMMARY")
        print("-" * 50)
        print(f"• Total missing PatientIDs: {len(not_in_clinisys):,}")
        print(f"• Recent missing PatientIDs (2022+): {len(recent_missing):,}")
        print(f"• Historical missing PatientIDs (pre-2022): {len(historical_missing):,}")
        print(f"• Total embryos affected: {missing_patientids_df['embryo_count'].sum():,}")
        print(f"• Average embryos per missing PatientID: {missing_patientids_df['embryo_count'].mean():.1f}")
        print(f"• Data exported to: {output_file}")
        
        con.close()
        logger.info("PatientID analysis completed")
        
    except Exception as e:
        logger.error(f"Error in analysis: {e}")
        raise

if __name__ == '__main__':
    list_patientids_not_in_clinisys() 