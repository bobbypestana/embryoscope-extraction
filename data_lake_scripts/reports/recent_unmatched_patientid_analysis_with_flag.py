import duckdb
import pandas as pd
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

def recent_unmatched_patientid_analysis_with_flag():
    """Analyze PatientIDs missing matches in recent years (2022-2025) with flag_embryoscope analysis."""
    
    logger.info("Starting recent unmatched PatientID analysis with flag_embryoscope")
    
    try:
        con = duckdb.connect('../database/huntington_data_lake.duckdb')
        logger.info("Connected to data lake")
        
        print("=" * 80)
        print("RECENT UNMATCHED PATIENTID ANALYSIS WITH FLAG_EMBRYOSCOPE (2022-2025)")
        print("=" * 80)
        
        # First, let's check the flag_embryoscope field in Clinisys
        logger.info("Checking flag_embryoscope field in Clinisys...")
        
        flag_analysis = con.execute("""
            SELECT 
                oocito_flag_embryoscope,
                COUNT(*) as record_count,
                COUNT(DISTINCT micro_prontuario) as unique_patients
            FROM gold.clinisys_embrioes
            WHERE oocito_flag_embryoscope IS NOT NULL
            GROUP BY oocito_flag_embryoscope
            ORDER BY oocito_flag_embryoscope
        """).fetchdf()
        
        print(f"\n1. OOCITO_FLAG_EMBRYOSCOPE ANALYSIS IN CLINISYS")
        print("-" * 50)
        print(flag_analysis.to_string(index=False))
        
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
        print(f"\n2. OVERALL SUMMARY (2022-2025)")
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
        
        # 3. Enhanced categorization with oocito_flag_embryoscope
        print(f"\n3. ENHANCED CATEGORIZATION WITH OOCITO_FLAG_EMBRYOSCOPE")
        print("-" * 70)
        
        # Category 1: PatientID exists in Clinisys but no match found
        print(f"Category 1: PatientID exists in Clinisys but no match found")
        print(f"  Count: {len(embryos_with_found_patientid):,} embryos")
        print(f"  Unique PatientIDs: {len(found_in_clinisys):,}")
        
        # Analyze oocito_flag_embryoscope for Category 1 PatientIDs
        if len(found_in_clinisys) > 0:
            flag_analysis_cat1 = con.execute("""
                SELECT 
                    oocito_flag_embryoscope,
                    COUNT(*) as record_count,
                    COUNT(DISTINCT micro_prontuario) as unique_patients
                FROM gold.clinisys_embrioes
                WHERE micro_prontuario IN ({})
                AND oocito_flag_embryoscope IS NOT NULL
                GROUP BY oocito_flag_embryoscope
                ORDER BY oocito_flag_embryoscope
            """.format(','.join(map(str, found_in_clinisys[:100])))).fetchdf()  # Limit to first 100 for performance
            
            print(f"  Oocito_flag_embryoscope breakdown for Category 1 PatientIDs:")
            print(flag_analysis_cat1.to_string(index=False))
        
        # Category 2: PatientID does not exist in Clinisys
        print(f"\nCategory 2: PatientID does not exist in Clinisys")
        print(f"  Count: {len(embryos_with_notfound_patientid):,} embryos")
        print(f"  Unique PatientIDs: {len(not_found_in_clinisys):,}")
        
        # Category 3: Null PatientID
        print(f"\nCategory 3: Null PatientID")
        print(f"  Count: {len(invalid_patientid_embryos):,} embryos")
        
        # 4. Detailed analysis of Category 1 with oocito_flag_embryoscope
        print(f"\n4. DETAILED ANALYSIS: CATEGORY 1 WITH OOCITO_FLAG_EMBRYOSCOPE")
        print("-" * 70)
        
        if len(embryos_with_found_patientid) > 0:
            # Sample analysis for top PatientIDs
            sample_patientids = embryos_with_found_patientid.groupby('patient_PatientID').size().sort_values(ascending=False).head(10).index.tolist()
            
            for patientid in sample_patientids:
                patient_embryos = embryos_with_found_patientid[embryos_with_found_patientid['patient_PatientID'] == patientid]
                
                # Get Clinisys records for this PatientID with oocito_flag_embryoscope
                clinisys_records = con.execute("""
                    SELECT 
                        COUNT(*) as total_records,
                        COUNT(DISTINCT CAST(micro_Data_DL AS DATE)) as unique_dates,
                        MIN(oocito_embryo_number) as min_embryo_num,
                        MAX(oocito_embryo_number) as max_embryo_num,
                        SUM(CASE WHEN oocito_flag_embryoscope = 1 THEN 1 ELSE 0 END) as flag_embryoscope_count,
                        SUM(CASE WHEN oocito_flag_embryoscope = 0 THEN 1 ELSE 0 END) as flag_not_embryoscope_count,
                        SUM(CASE WHEN oocito_flag_embryoscope IS NULL THEN 1 ELSE 0 END) as flag_null_count
                    FROM gold.clinisys_embrioes 
                    WHERE micro_prontuario = ?
                """, [patientid]).fetchone()
                
                total_clinisys = clinisys_records[0]
                unique_dates = clinisys_records[1]
                min_clinisys_embryo = clinisys_records[2]
                max_clinisys_embryo = clinisys_records[3]
                flag_embryoscope_count = clinisys_records[4]
                flag_not_embryoscope_count = clinisys_records[5]
                flag_null_count = clinisys_records[6]
                
                # Get Embryoscope embryo numbers for this PatientID
                embryoscope_embryos = patient_embryos['embryo_embryo_number'].tolist()
                min_embryo_embryo = min(embryoscope_embryos)
                max_embryo_embryo = max(embryoscope_embryos)
                
                print(f"\nPatientID {patientid}:")
                print(f"  Embryoscope: {len(patient_embryos)} embryos, numbers {min_embryo_embryo}-{max_embryo_embryo}")
                print(f"  Clinisys: {total_clinisys} records, {unique_dates} unique dates, numbers {min_clinisys_embryo}-{max_clinisys_embryo}")
                print(f"  Oocito_flag_embryoscope: {flag_embryoscope_count} (expected), {flag_not_embryoscope_count} (not expected), {flag_null_count} (null)")
                
                # Identify the issue
                if max_clinisys_embryo is None:
                    print(f"  Issue: Clinisys has NULL embryo numbers")
                elif max_embryo_embryo > max_clinisys_embryo:
                    print(f"  Issue: Embryoscope has higher embryo numbers than Clinisys")
                elif min_embryo_embryo < min_clinisys_embryo:
                    print(f"  Issue: Embryoscope has lower embryo numbers than Clinisys")
                else:
                    print(f"  Issue: Embryo number overlap but no exact matches (likely date mismatch)")
                
                # Check if the unmatched embryos should be expected based on flag
                if flag_embryoscope_count > 0:
                    print(f"  Note: {flag_embryoscope_count} Clinisys records are flagged as expected in Embryoscope")
        
        # 5. Summary by category with flag analysis
        print(f"\n5. SUMMARY BY CATEGORY WITH FLAG ANALYSIS")
        print("-" * 70)
        
        category1_count = len(embryos_with_found_patientid)
        category2_count = len(embryos_with_notfound_patientid)
        category3_count = len(invalid_patientid_embryos)
        total_unmatched = len(unmatched_recent_df)
        
        print(f"Category 1 - PatientID exists in Clinisys but no match:")
        print(f"  Count: {category1_count:,} embryos ({category1_count/total_unmatched*100:.1f}%)")
        print(f"  Reason: Date/embryo number mismatches or missing Clinisys records")
        print(f"  Flag Analysis: Check if these embryos are flagged as expected in Clinisys")
        
        print(f"\nCategory 2 - PatientID does not exist in Clinisys:")
        print(f"  Count: {category2_count:,} embryos ({category2_count/total_unmatched*100:.1f}%)")
        print(f"  Reason: Data coverage gap - PatientID not in Clinisys system")
        print(f"  Flag Analysis: These PatientIDs don't exist in Clinisys at all")
        
        print(f"\nCategory 3 - Null PatientID:")
        print(f"  Count: {category3_count:,} embryos ({category3_count/total_unmatched*100:.1f}%)")
        print(f"  Reason: Data quality issue - missing PatientID")
        print(f"  Flag Analysis: Data quality issue needs to be fixed first")
        
        # 6. Recommendations with flag consideration
        print(f"\n6. RECOMMENDATIONS WITH FLAG CONSIDERATION")
        print("-" * 70)
        
        print(f"• Category 1 ({category1_count:,} embryos):")
        print(f"  - Check oocito_flag_embryoscope to see if these embryos are expected in Embryoscope")
        print(f"  - If flagged as expected but not found, investigate date/embryo number mismatches")
        print(f"  - If not flagged as expected, these may be legitimate missing data")
        
        print(f"\n• Category 2 ({category2_count:,} embryos):")
        print(f"  - These PatientIDs don't exist in Clinisys, so flag_embryoscope is not applicable")
        print(f"  - Verify if these PatientIDs should exist in Clinisys")
        print(f"  - Check for data synchronization issues")
        
        print(f"\n• Category 3 ({category3_count:,} embryos):")
        print(f"  - Fix data quality issues in Embryoscope system")
        print(f"  - Implement validation to prevent null PatientIDs")
        print(f"  - Flag analysis not applicable until PatientID is fixed")
        
        con.close()
        logger.info("Recent unmatched PatientID analysis with flag_embryoscope completed")
        
    except Exception as e:
        logger.error(f"Error in analysis: {e}")
        raise

if __name__ == '__main__':
    recent_unmatched_patientid_analysis_with_flag() 