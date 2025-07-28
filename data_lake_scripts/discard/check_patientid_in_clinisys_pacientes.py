import duckdb
import pandas as pd
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

def check_patientid_in_clinisys_pacientes():
    """Check if PatientIDs from unmatched Embryoscope embryos are present in clinisys_all.view_pacientes."""
    
    logger.info("Starting PatientID presence analysis in clinisys_all.view_pacientes")
    
    try:
        con = duckdb.connect('../database/huntington_data_lake.duckdb')
        logger.info("Connected to data lake")
        
        print("=" * 80)
        print("PATIENTID PRESENCE ANALYSIS IN CLINISYS_ALL.VIEW_PACIENTES")
        print("=" * 80)
        
        # First, let's check what columns are available in the view_pacientes table
        logger.info("Checking schema of clinisys_all.view_pacientes...")
        schema_query = "DESCRIBE clinisys_all.view_pacientes"
        schema_df = con.execute(schema_query).df()
        
        print("\nAvailable columns in clinisys_all.view_pacientes:")
        print(schema_df.to_string(index=False))
        
        # Find all prontuario-related columns
        prontuario_columns = []
        for _, row in schema_df.iterrows():
            column_name = row['column_name']
            if 'prontuario' in column_name.lower():
                prontuario_columns.append(column_name)
        
        print(f"\nFound {len(prontuario_columns)} prontuario-related columns:")
        for col in prontuario_columns:
            print(f"  - {col}")
        
        # Get all unmatched Embryoscope embryos with their PatientIDs
        logger.info("Fetching unmatched Embryoscope embryos...")
        unmatched_query = """
        SELECT DISTINCT
            embryo_EmbryoID,
            embryo_FertilizationTime,
            embryo_embryo_number,
            patient_PatientID,
            treatment_TreatmentName,
            CASE 
                WHEN patient_PatientID IS NULL THEN NULL
                ELSE patient_PatientID
            END as patient_PatientID_clean
        FROM gold.embryoscope_embrioes
        WHERE embryo_EmbryoID NOT IN (
            SELECT DISTINCT embryo_EmbryoID 
            FROM gold.embryoscope_clinisys_combined 
            WHERE embryo_EmbryoID IS NOT NULL
        )
        ORDER BY embryo_FertilizationTime DESC
        """
        
        unmatched_df = con.execute(unmatched_query).df()
        logger.info(f"Found {len(unmatched_df):,} unmatched Embryoscope embryos")
        
        # Get unique PatientIDs from each prontuario column in view_pacientes
        logger.info("Fetching unique PatientIDs from clinisys_all.view_pacientes...")
        
        clinisys_patientids_by_column = {}
        for column in prontuario_columns:
            try:
                query = f"""
                SELECT DISTINCT {column}
                FROM clinisys_all.view_pacientes
                WHERE {column} IS NOT NULL
                """
                result = con.execute(query).fetchdf()
                patientids = result[column].tolist()
                clinisys_patientids_by_column[column] = patientids
                logger.info(f"Found {len(patientids):,} unique PatientIDs in {column}")
            except Exception as e:
                logger.warning(f"Error querying {column}: {e}")
                clinisys_patientids_by_column[column] = []
        
        # Analyze PatientID presence
        print(f"\n1. OVERALL ANALYSIS")
        print("-" * 50)
        
        # Count embryos with valid PatientIDs
        valid_patientid_embryos = unmatched_df[unmatched_df['patient_PatientID_clean'].notna()]
        invalid_patientid_embryos = unmatched_df[unmatched_df['patient_PatientID_clean'].isna()]
        
        print(f"Total unmatched embryos: {len(unmatched_df):,}")
        print(f"Embryos with valid PatientID: {len(valid_patientid_embryos):,}")
        print(f"Embryos with invalid PatientID: {len(invalid_patientid_embryos):,}")
        
        # Check which valid PatientIDs are present in each Clinisys column
        valid_patientids = valid_patientid_embryos['patient_PatientID_clean'].unique()
        
        found_by_column = {}
        for column in prontuario_columns:
            if column in clinisys_patientids_by_column:
                found_patientids = [pid for pid in valid_patientids if pid in clinisys_patientids_by_column[column]]
                found_by_column[column] = found_patientids
                print(f"Valid PatientIDs found in {column}: {len(found_patientids):,}")
        
        # Combine all found PatientIDs
        all_found = []
        for column, patientids in found_by_column.items():
            all_found.extend(patientids)
        found_in_any = list(set(all_found))
        not_found_in_clinisys = [pid for pid in valid_patientids if pid not in found_in_any]
        
        print(f"\nValid PatientIDs found in any prontuario field: {len(found_in_any):,}")
        print(f"Valid PatientIDs NOT found in Clinisys: {len(not_found_in_clinisys):,}")
        
        # Count embryos by PatientID presence
        embryos_with_found_patientid = valid_patientid_embryos[
            valid_patientid_embryos['patient_PatientID_clean'].isin(found_in_any)
        ]
        embryos_with_notfound_patientid = valid_patientid_embryos[
            valid_patientid_embryos['patient_PatientID_clean'].isin(not_found_in_clinisys)
        ]
        
        print(f"Embryos with PatientID found in Clinisys: {len(embryos_with_found_patientid):,}")
        print(f"Embryos with PatientID NOT found in Clinisys: {len(embryos_with_notfound_patientid):,}")
        
        # 2. Examples of PatientIDs found in Clinisys
        print(f"\n2. EXAMPLES: PATIENTIDS FOUND IN CLINISYS")
        print("-" * 50)
        
        if len(found_in_any) > 0:
            # Get sample embryos with PatientIDs found in Clinisys
            sample_found = embryos_with_found_patientid.head(10)
            print("Sample embryos with PatientID found in Clinisys:")
            print(sample_found[['embryo_EmbryoID', 'embryo_FertilizationTime', 'embryo_embryo_number', 'patient_PatientID', 'patient_PatientID_clean']].to_string(index=False))
            
            # Check Clinisys records for these PatientIDs
            print(f"\nClinisys records for these PatientIDs:")
            for _, row in sample_found.iterrows():
                patientid_clean = row['patient_PatientID_clean']
                
                # Check which columns contain this PatientID
                found_in_columns = []
                for column in prontuario_columns:
                    if column in found_by_column and patientid_clean in found_by_column[column]:
                        found_in_columns.append(column)
                
                # Count total Clinisys records for this PatientID (all prontuario fields)
                where_conditions = []
                params = []
                for column in prontuario_columns:
                    where_conditions.append(f"{column} = ?")
                    params.append(patientid_clean)
                
                where_clause = " OR ".join(where_conditions)
                total_clinisys_query = f"""
                    SELECT COUNT(*) 
                    FROM clinisys_all.view_pacientes 
                    WHERE {where_clause}
                """
                
                try:
                    total_clinisys = con.execute(total_clinisys_query, params).fetchone()[0]
                except Exception as e:
                    logger.warning(f"Error counting records for PatientID {patientid_clean}: {e}")
                    total_clinisys = 0
                
                print(f"  PatientID {patientid_clean} ({row['embryo_EmbryoID']}):")
                print(f"    - Found in columns: {', '.join(found_in_columns) if found_in_columns else 'None'}")
                print(f"    - Total Clinisys records: {total_clinisys}")
        else:
            print("No PatientIDs found in Clinisys")
        
        # 3. Examples of PatientIDs NOT found in Clinisys
        print(f"\n3. EXAMPLES: PATIENTIDS NOT FOUND IN CLINISYS")
        print("-" * 50)
        
        if len(not_found_in_clinisys) > 0:
            # Get sample embryos with PatientIDs not found in Clinisys
            sample_notfound = embryos_with_notfound_patientid.head(10)
            print("Sample embryos with PatientID NOT found in Clinisys:")
            print(sample_notfound[['embryo_EmbryoID', 'embryo_FertilizationTime', 'embryo_embryo_number', 'patient_PatientID', 'patient_PatientID_clean']].to_string(index=False))
            
            # Show some of the PatientIDs that are not in Clinisys
            print(f"\nSample PatientIDs not found in Clinisys:")
            for pid in not_found_in_clinisys[:10]:
                print(f"  {pid}")
        else:
            print("All PatientIDs are found in Clinisys")
        
        # 4. Null PatientID examples
        print(f"\n4. EXAMPLES: NULL PATIENTIDS")
        print("-" * 50)
        
        if len(invalid_patientid_embryos) > 0:
            print("Sample embryos with null PatientIDs:")
            sample_invalid = invalid_patientid_embryos.head(10)
            print(sample_invalid[['embryo_EmbryoID', 'embryo_FertilizationTime', 'embryo_embryo_number', 'patient_PatientID']].to_string(index=False))
            
            # Show count of null PatientIDs
            null_count = len(invalid_patientid_embryos)
            print(f"\nTotal embryos with null PatientID: {null_count:,}")
        else:
            print("No null PatientIDs found")
        
        # 5. Summary statistics
        print(f"\n5. SUMMARY STATISTICS")
        print("-" * 50)
        
        total_embryos = len(unmatched_df)
        valid_embryos = len(valid_patientid_embryos)
        found_embryos = len(embryos_with_found_patientid)
        notfound_embryos = len(embryos_with_notfound_patientid)
        invalid_embryos = len(invalid_patientid_embryos)
        
        print(f"Total unmatched embryos: {total_embryos:,} (100%)")
        print(f"  - Valid PatientID: {valid_embryos:,} ({valid_embryos/total_embryos*100:.1f}%)")
        print(f"    - Found in Clinisys: {found_embryos:,} ({found_embryos/total_embryos*100:.1f}%)")
        print(f"    - NOT found in Clinisys: {notfound_embryos:,} ({notfound_embryos/total_embryos*100:.1f}%)")
        print(f"  - Null PatientID: {invalid_embryos:,} ({invalid_embryos/total_embryos*100:.1f}%)")
        
        # 6. Column-by-column breakdown
        print(f"\n6. COLUMN-BY-COLUMN BREAKDOWN")
        print("-" * 50)
        
        for column in prontuario_columns:
            if column in found_by_column:
                found_count = len(found_by_column[column])
                print(f"{column}: {found_count:,} PatientIDs found")
        
        con.close()
        logger.info("PatientID presence analysis in clinisys_all.view_pacientes completed")
        
    except Exception as e:
        logger.error(f"Error in analysis: {e}")
        raise

if __name__ == '__main__':
    check_patientid_in_clinisys_pacientes() 