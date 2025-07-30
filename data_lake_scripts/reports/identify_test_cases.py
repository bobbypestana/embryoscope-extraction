import duckdb
import pandas as pd
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

def identify_test_cases():
    """Identify potential test cases from missing patients analysis."""
    
    logger.info("Starting test case identification from missing patients")
    
    try:
        con = duckdb.connect('database/huntington_data_lake.duckdb')
        logger.info("Connected to data lake")
        
        print("=" * 80)
        print("IDENTIFYING POTENTIAL TEST CASES FROM MISSING PATIENTS")
        print("=" * 80)
        
        # Get missing patients with detailed information
        missing_patients_query = """
        SELECT DISTINCT 
            p.PatientID as patient_PatientID,
            p.PatientIDx as patient_PatientIDx,
            p.FirstName as patient_FirstName,
            p.LastName as patient_LastName,
            p._location as clinic_name,
            COUNT(DISTINCT e.EmbryoID) as embryo_count,
            MIN(CAST(e.FertilizationTime AS DATE)) as first_date,
            MAX(CAST(e.FertilizationTime AS DATE)) as last_date,
            COUNT(DISTINCT YEAR(CAST(e.FertilizationTime AS DATE))) as year_span
        FROM silver_embryoscope.embryo_data e
        JOIN silver_embryoscope.patients p ON e.PatientIDx = p.PatientIDx
        WHERE p.PatientID NOT IN (
            SELECT DISTINCT micro_prontuario 
            FROM gold.clinisys_embrioes 
            WHERE micro_prontuario IS NOT NULL
        )
        GROUP BY p.PatientID, p.PatientIDx, p.FirstName, p.LastName, p._location
        ORDER BY embryo_count DESC
        """
        
        missing_patients_df = con.execute(missing_patients_query).df()
        
        print(f"\n1. OVERVIEW")
        print("-" * 50)
        print(f"Total missing patients: {len(missing_patients_df):,}")
        
        # Add period classification
        missing_patients_df['is_after_2021_05'] = missing_patients_df['last_date'] >= '2021-06-01'
        missing_patients_df['period'] = missing_patients_df['is_after_2021_05'].map({True: 'After 2021-05', False: 'Before 2021-06'})
        
        after_2021_05 = missing_patients_df[missing_patients_df['is_after_2021_05']]
        before_2021_06 = missing_patients_df[~missing_patients_df['is_after_2021_05']]
        
        print(f"Patients after 2021-05: {len(after_2021_05):,}")
        print(f"Patients before 2021-06: {len(before_2021_06):,}")
        
        # 2. Test Case 1: High Embryo Count Patients
        print(f"\n2. TEST CASE 1: HIGH EMBRYO COUNT PATIENTS")
        print("-" * 60)
        print("These patients have many embryos but are missing from Clinisys")
        print("Potential issues: Data volume handling, complex patient scenarios")
        
        high_embryo_patients = missing_patients_df[missing_patients_df['embryo_count'] >= 50].head(10)
        print(f"{'PatientID':<12} {'PatientIDx':<20} {'Clinic':<15} {'Embryos':<8} {'Period':<15}")
        print("-" * 80)
        for _, row in high_embryo_patients.iterrows():
            print(f"{row['patient_PatientID']:<12} {str(row['patient_PatientIDx']):<20} {str(row['clinic_name']):<15} {row['embryo_count']:<8} {row['period']:<15}")
        
        # 3. Test Case 2: Recent Patients (After 2021-05)
        print(f"\n3. TEST CASE 2: RECENT PATIENTS (AFTER 2021-05)")
        print("-" * 60)
        print("These patients should have been captured by the integration")
        print("Potential issues: Integration timing, data synchronization")
        
        recent_patients = after_2021_05.head(10)
        print(f"{'PatientID':<12} {'PatientIDx':<20} {'Clinic':<15} {'Embryos':<8} {'Last Date':<12}")
        print("-" * 80)
        for _, row in recent_patients.iterrows():
            print(f"{row['patient_PatientID']:<12} {str(row['patient_PatientIDx']):<20} {str(row['clinic_name']):<15} {row['embryo_count']:<8} {row['last_date']:<12}")
        
        # 4. Test Case 3: Multi-Year Patients
        print(f"\n4. TEST CASE 3: MULTI-YEAR PATIENTS")
        print("-" * 60)
        print("These patients have data spanning multiple years")
        print("Potential issues: Historical data integration, temporal consistency")
        
        multi_year_patients = missing_patients_df[missing_patients_df['year_span'] >= 2].head(10)
        print(f"{'PatientID':<12} {'PatientIDx':<20} {'Clinic':<15} {'Years':<6} {'Period':<15}")
        print("-" * 80)
        for _, row in multi_year_patients.iterrows():
            print(f"{row['patient_PatientID']:<12} {str(row['patient_PatientIDx']):<20} {str(row['clinic_name']):<15} {row['year_span']:<6} {row['period']:<15}")
        
        # 5. Test Case 4: Clinic-Specific Patterns
        print(f"\n5. TEST CASE 4: CLINIC-SPECIFIC PATTERNS")
        print("-" * 60)
        print("Analyzing missing patients by clinic")
        
        clinic_analysis = missing_patients_df.groupby('clinic_name').agg({
            'patient_PatientID': 'count',
            'embryo_count': 'sum',
            'is_after_2021_05': 'sum'
        }).rename(columns={
            'patient_PatientID': 'missing_patients',
            'embryo_count': 'total_embryos',
            'is_after_2021_05': 'recent_patients'
        }).sort_values('missing_patients', ascending=False)
        
        print(f"{'Clinic':<20} {'Missing Patients':<15} {'Total Embryos':<15} {'Recent Patients':<15}")
        print("-" * 80)
        for clinic, row in clinic_analysis.iterrows():
            print(f"{str(clinic):<20} {row['missing_patients']:<15} {row['total_embryos']:<15} {row['recent_patients']:<15}")
        
        # 6. Test Case 5: PatientID Patterns
        print(f"\n6. TEST CASE 5: PATIENTID PATTERNS")
        print("-" * 60)
        print("Analyzing PatientID patterns for potential data quality issues")
        
        # Check for suspicious PatientIDs
        suspicious_patterns = missing_patients_df[
            (missing_patients_df['patient_PatientID'].astype(str).str.len() <= 3) |
            (missing_patients_df['patient_PatientID'].astype(str).str.contains('^[A-Z]')) |
            (missing_patients_df['patient_PatientID'].astype(str).str.contains('TEST'))
        ].head(10)
        
        if len(suspicious_patterns) > 0:
            print("Suspicious PatientID patterns found:")
            print(f"{'PatientID':<12} {'PatientIDx':<20} {'Clinic':<15} {'Embryos':<8}")
            print("-" * 60)
            for _, row in suspicious_patterns.iterrows():
                print(f"{row['patient_PatientID']:<12} {str(row['patient_PatientIDx']):<20} {str(row['clinic_name']):<15} {row['embryo_count']:<8}")
        else:
            print("No obvious suspicious PatientID patterns found")
        
        # 7. Test Case 6: Data Completeness
        print(f"\n7. TEST CASE 6: DATA COMPLETENESS")
        print("-" * 60)
        print("Patients with missing or incomplete data")
        
        # Check for patients with missing names
        missing_names = missing_patients_df[
            (missing_patients_df['patient_FirstName'].isnull()) |
            (missing_patients_df['patient_LastName'].isnull()) |
            (missing_patients_df['patient_FirstName'] == '') |
            (missing_patients_df['patient_LastName'] == '')
        ].head(10)
        
        if len(missing_names) > 0:
            print("Patients with missing names:")
            print(f"{'PatientID':<12} {'PatientIDx':<20} {'FirstName':<15} {'LastName':<15}")
            print("-" * 70)
            for _, row in missing_names.iterrows():
                first_name = row['patient_FirstName'] if pd.notna(row['patient_FirstName']) else 'NULL'
                last_name = row['patient_LastName'] if pd.notna(row['patient_LastName']) else 'NULL'
                print(f"{row['patient_PatientID']:<12} {str(row['patient_PatientIDx']):<20} {str(first_name):<15} {str(last_name):<15}")
        else:
            print("All patients have complete name information")
        
        # 8. Test Case 7: Edge Cases
        print(f"\n8. TEST CASE 7: EDGE CASES")
        print("-" * 60)
        print("Identifying edge cases for testing")
        
        # Single embryo patients
        single_embryo = missing_patients_df[missing_patients_df['embryo_count'] == 1].head(5)
        print("Single embryo patients:")
        print(f"{'PatientID':<12} {'PatientIDx':<20} {'Clinic':<15} {'Date':<12}")
        print("-" * 65)
        for _, row in single_embryo.iterrows():
            print(f"{row['patient_PatientID']:<12} {str(row['patient_PatientIDx']):<20} {str(row['clinic_name']):<15} {row['first_date']:<12}")
        
        # Very old patients
        very_old = missing_patients_df[missing_patients_df['first_date'] < '2018-01-01'].head(5)
        print("\nVery old patients (before 2018):")
        print(f"{'PatientID':<12} {'PatientIDx':<20} {'Clinic':<15} {'First Date':<12}")
        print("-" * 65)
        for _, row in very_old.iterrows():
            print(f"{row['patient_PatientID']:<12} {str(row['patient_PatientIDx']):<20} {str(row['clinic_name']):<15} {row['first_date']:<12}")
        
        # 9. Summary of Test Cases
        print(f"\n9. SUMMARY OF TEST CASES")
        print("-" * 60)
        print("Recommended test scenarios:")
        print("1. High embryo count patients (>50 embryos)")
        print("2. Recent patients (after 2021-05) - integration timing")
        print("3. Multi-year patients (spanning 2+ years)")
        print("4. Clinic-specific integration issues")
        print("5. Data quality issues (suspicious PatientIDs)")
        print("6. Data completeness (missing names)")
        print("7. Edge cases (single embryos, very old data)")
        print("8. PatientIDx mapping validation")
        print("9. Temporal data consistency")
        print("10. Cross-system data synchronization")
        
        # Export test cases to CSV
        output_file = "data_lake_scripts/reports/exports/test_cases_missing_patients.csv"
        missing_patients_df.to_csv(output_file, index=False)
        logger.info(f"Exported {len(missing_patients_df):,} test cases to {output_file}")
        
        # Export specific test case categories
        high_embryo_file = "data_lake_scripts/reports/exports/test_cases_high_embryo_count.csv"
        high_embryo_patients.to_csv(high_embryo_file, index=False)
        logger.info(f"Exported {len(high_embryo_patients):,} high embryo count test cases")
        
        recent_file = "data_lake_scripts/reports/exports/test_cases_recent_patients.csv"
        after_2021_05.to_csv(recent_file, index=False)
        logger.info(f"Exported {len(after_2021_05):,} recent patient test cases")
        
        con.close()
        logger.info("Test case identification completed")
        
    except Exception as e:
        logger.error(f"Error in analysis: {e}")
        raise

if __name__ == '__main__':
    identify_test_cases() 