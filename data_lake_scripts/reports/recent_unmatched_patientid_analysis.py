import duckdb
import pandas as pd
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

def recent_unmatched_patientid_analysis():
    """Analyze PatientIDs missing matches in recent years (2022-2025) and categorize them."""
    
    logger.info("Starting recent unmatched PatientID analysis")
    
    try:
        con = duckdb.connect('../database/huntington_data_lake.duckdb')
        logger.info("Connected to data lake")
        
        print("=" * 80)
        print("RECENT UNMATCHED PATIENTID ANALYSIS (2022-2025)")
        print("=" * 80)
        
        # Get unmatched embryos from recent years
        logger.info("Fetching unmatched embryos from recent years...")
        unmatched_recent_query = """
        SELECT DISTINCT
            embryo_EmbryoID,
            embryo_FertilizationTime,
            embryo_embryo_number,
            patient_PatientID,
            treatment_TreatmentName,
            YEAR(CAST(embryo_FertilizationTime AS DATE)) as year
        FROM gold.embryoscope_embrioes
        WHERE embryo_EmbryoID NOT IN (
            SELECT DISTINCT embryo_EmbryoID 
            FROM gold.embryoscope_clinisys_combined 
            WHERE embryo_EmbryoID IS NOT NULL
        )
        AND YEAR(CAST(embryo_FertilizationTime AS DATE)) >= 2022
        AND embryo_FertilizationTime IS NOT NULL
        ORDER BY embryo_FertilizationTime DESC
        """
        
        unmatched_recent_df = con.execute(unmatched_recent_query).df()
        logger.info(f"Found {len(unmatched_recent_df):,} unmatched embryos from recent years")
        
        # Get unique PatientIDs from Clinisys
        logger.info("Fetching unique PatientIDs from Clinisys...")
        clinisys_patientids = con.execute("""
            SELECT DISTINCT micro_prontuario
            FROM gold.clinisys_embrioes
            WHERE micro_prontuario IS NOT NULL
        """).fetchdf()['micro_prontuario'].tolist()
        
        logger.info(f"Found {len(clinisys_patientids):,} unique PatientIDs in Clinisys")
        
        # Analyze PatientID presence
        print(f"\n1. OVERALL SUMMARY (2022-2025)")
        print("-" * 50)
        
        # Count embryos with valid PatientIDs
        valid_patientid_embryos = unmatched_recent_df[unmatched_recent_df['patient_PatientID'].notna()]
        invalid_patientid_embryos = unmatched_recent_df[unmatched_recent_df['patient_PatientID'].isna()]
        
        print(f"Total unmatched embryos (2022-2025): {len(unmatched_recent_df):,}")
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
        
        # 2. Categorize unmatched PatientIDs
        print(f"\n2. PATIENTID CATEGORIZATION")
        print("-" * 50)
        
        # Category 1: PatientID exists in Clinisys but no match found
        print(f"Category 1: PatientID exists in Clinisys but no match found")
        print(f"  Count: {len(embryos_with_found_patientid):,} embryos")
        print(f"  Unique PatientIDs: {len(found_in_clinisys):,}")
        
        # Category 2: PatientID does not exist in Clinisys
        print(f"\nCategory 2: PatientID does not exist in Clinisys")
        print(f"  Count: {len(embryos_with_notfound_patientid):,} embryos")
        print(f"  Unique PatientIDs: {len(not_found_in_clinisys):,}")
        
        # Category 3: Null PatientID
        print(f"\nCategory 3: Null PatientID")
        print(f"  Count: {len(invalid_patientid_embryos):,} embryos")
        
        # 3. Yearly breakdown
        print(f"\n3. YEARLY BREAKDOWN")
        print("-" * 50)
        
        yearly_summary = unmatched_recent_df.groupby('year').agg({
            'embryo_EmbryoID': 'count',
            'patient_PatientID': lambda x: x.notna().sum()
        }).rename(columns={'embryo_EmbryoID': 'total_unmatched', 'patient_PatientID': 'valid_patientid'})
        
        yearly_summary['null_patientid'] = yearly_summary['total_unmatched'] - yearly_summary['valid_patientid']
        
        print(f"{'Year':<6} {'Total':<8} {'Valid PID':<10} {'Null PID':<10}")
        print("-" * 40)
        
        for year, row in yearly_summary.iterrows():
            print(f"{year:<6} {row['total_unmatched']:<8,} {row['valid_patientid']:<10,} {row['null_patientid']:<10,}")
        
        # 4. Detailed analysis of Category 1 (PatientID exists in Clinisys)
        print(f"\n4. DETAILED ANALYSIS: CATEGORY 1 (PatientID exists in Clinisys)")
        print("-" * 70)
        
        if len(embryos_with_found_patientid) > 0:
            # Group by PatientID to see patterns
            patientid_groups = embryos_with_found_patientid.groupby('patient_PatientID').agg({
                'embryo_EmbryoID': 'count',
                'year': 'nunique',
                'embryo_embryo_number': ['min', 'max']
            }).round(2)
            
            patientid_groups.columns = ['embryo_count', 'year_count', 'min_embryo_num', 'max_embryo_num']
            patientid_groups = patientid_groups.sort_values('embryo_count', ascending=False)
            
            print(f"Top 20 PatientIDs with most unmatched embryos:")
            print(f"{'PatientID':<12} {'Embryos':<10} {'Years':<8} {'Embryo Range':<15}")
            print("-" * 50)
            
            for patientid, row in patientid_groups.head(20).iterrows():
                embryo_range = f"{row['min_embryo_num']:.0f}-{row['max_embryo_num']:.0f}"
                print(f"{patientid:<12} {row['embryo_count']:<10.0f} {row['year_count']:<8.0f} {embryo_range:<15}")
            
            # Analyze why these PatientIDs don't match
            print(f"\nAnalysis of why Category 1 PatientIDs don't match:")
            
            # Sample analysis for top PatientIDs
            sample_patientids = patientid_groups.head(10).index.tolist()
            
            for patientid in sample_patientids:
                patient_embryos = embryos_with_found_patientid[embryos_with_found_patientid['patient_PatientID'] == patientid]
                
                # Get Clinisys records for this PatientID
                clinisys_records = con.execute("""
                    SELECT 
                        COUNT(*) as total_records,
                        COUNT(DISTINCT CAST(micro_Data_DL AS DATE)) as unique_dates,
                        MIN(oocito_embryo_number) as min_embryo_num,
                        MAX(oocito_embryo_number) as max_embryo_num
                    FROM gold.clinisys_embrioes 
                    WHERE micro_prontuario = ?
                """, [patientid]).fetchone()
                
                total_clinisys = clinisys_records[0]
                unique_dates = clinisys_records[1]
                min_clinisys_embryo = clinisys_records[2]
                max_clinisys_embryo = clinisys_records[3]
                
                # Get Embryoscope embryo numbers for this PatientID
                embryoscope_embryos = patient_embryos['embryo_embryo_number'].tolist()
                min_embryo_embryo = min(embryoscope_embryos)
                max_embryo_embryo = max(embryoscope_embryos)
                
                print(f"\nPatientID {patientid}:")
                print(f"  Embryoscope: {len(patient_embryos)} embryos, numbers {min_embryo_embryo}-{max_embryo_embryo}")
                print(f"  Clinisys: {total_clinisys} records, {unique_dates} unique dates, numbers {min_clinisys_embryo}-{max_clinisys_embryo}")
                
                # Identify the issue
                if max_embryo_embryo > max_clinisys_embryo:
                    print(f"  Issue: Embryoscope has higher embryo numbers than Clinisys")
                elif min_embryo_embryo < min_clinisys_embryo:
                    print(f"  Issue: Embryoscope has lower embryo numbers than Clinisys")
                else:
                    print(f"  Issue: Embryo number overlap but no exact matches (likely date mismatch)")
        
        # 5. Detailed analysis of Category 2 (PatientID does not exist in Clinisys)
        print(f"\n5. DETAILED ANALYSIS: CATEGORY 2 (PatientID does not exist in Clinisys)")
        print("-" * 70)
        
        if len(embryos_with_notfound_patientid) > 0:
            # Group by PatientID
            notfound_patientid_groups = embryos_with_notfound_patientid.groupby('patient_PatientID').agg({
                'embryo_EmbryoID': 'count',
                'year': 'nunique'
            }).round(2)
            
            notfound_patientid_groups.columns = ['embryo_count', 'year_count']
            notfound_patientid_groups = notfound_patientid_groups.sort_values('embryo_count', ascending=False)
            
            print(f"Top 20 PatientIDs not found in Clinisys:")
            print(f"{'PatientID':<12} {'Embryos':<10} {'Years':<8}")
            print("-" * 35)
            
            for patientid, row in notfound_patientid_groups.head(20).iterrows():
                print(f"{patientid:<12} {row['embryo_count']:<10.0f} {row['year_count']:<8.0f}")
            
            # Check for data quality issues
            print(f"\nData quality analysis:")
            
            # Check for suspicious PatientIDs
            suspicious_patientids = []
            for patientid in not_found_in_clinisys:
                if patientid == 1 or patientid < 100 or patientid > 999999:
                    suspicious_patientids.append(patientid)
            
            if suspicious_patientids:
                print(f"  Suspicious PatientIDs found: {suspicious_patientids}")
                print(f"  These may be data quality issues or placeholder values")
        
        # 6. Summary by category
        print(f"\n6. SUMMARY BY CATEGORY")
        print("-" * 50)
        
        category1_count = len(embryos_with_found_patientid)
        category2_count = len(embryos_with_notfound_patientid)
        category3_count = len(invalid_patientid_embryos)
        total_unmatched = len(unmatched_recent_df)
        
        print(f"Category 1 - PatientID exists in Clinisys but no match:")
        print(f"  Count: {category1_count:,} embryos ({category1_count/total_unmatched*100:.1f}%)")
        print(f"  Reason: Date/embryo number mismatches or missing Clinisys records")
        
        print(f"\nCategory 2 - PatientID does not exist in Clinisys:")
        print(f"  Count: {category2_count:,} embryos ({category2_count/total_unmatched*100:.1f}%)")
        print(f"  Reason: Data coverage gap - PatientID not in Clinisys system")
        
        print(f"\nCategory 3 - Null PatientID:")
        print(f"  Count: {category3_count:,} embryos ({category3_count/total_unmatched*100:.1f}%)")
        print(f"  Reason: Data quality issue - missing PatientID")
        
        # 7. Recommendations
        print(f"\n7. RECOMMENDATIONS")
        print("-" * 50)
        
        print(f"• Category 1 ({category1_count:,} embryos):")
        print(f"  - Investigate date field differences between systems")
        print(f"  - Check embryo number mapping logic")
        print(f"  - Consider date tolerance in join conditions")
        
        print(f"\n• Category 2 ({category2_count:,} embryos):")
        print(f"  - Verify if these PatientIDs should exist in Clinisys")
        print(f"  - Check for data synchronization issues")
        print(f"  - Consider if these represent legitimate missing data")
        
        print(f"\n• Category 3 ({category3_count:,} embryos):")
        print(f"  - Fix data quality issues in Embryoscope system")
        print(f"  - Implement validation to prevent null PatientIDs")
        print(f"  - Investigate source of null values")
        
        con.close()
        logger.info("Recent unmatched PatientID analysis completed")
        
    except Exception as e:
        logger.error(f"Error in analysis: {e}")
        raise

if __name__ == '__main__':
    recent_unmatched_patientid_analysis() 