#!/usr/bin/env python3
"""
Test script for all embryoscope endpoints with generic processing.
Tests patients, treatments, embryo_data, and idascore extraction and save.
"""

import sys
import os
import logging
from datetime import datetime
import uuid
import pandas as pd
import duckdb

# Add the parent directory to the path so we can import our modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.config_manager import EmbryoscopeConfigManager
from utils.api_client import EmbryoscopeAPIClient
from utils.data_processor import EmbryoscopeDataProcessor
from utils.database_manager import EmbryoscopeDatabaseManager

def setup_logging():
    """Setup logging configuration."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )

def test_all_endpoints():
    """Test all endpoints with the first enabled embryoscope."""
    logger = logging.getLogger(__name__)
    
    logger.info("=" * 50)
    logger.info("Testing All Endpoints with Generic Processing")
    logger.info("=" * 50)
    
    try:
        # 1. Initialize components
        logger.info("1. Initializing components...")
        config = EmbryoscopeConfigManager('params.yml')
        embryoscopes = config.get_enabled_embryoscopes()
        
        if not embryoscopes:
            logger.error("No enabled embryoscopes found in configuration")
            return False
        
        # Use the first enabled embryoscope
        location, credentials = list(embryoscopes.items())[0]
        logger.info(f"2. Testing with embryoscope: {location}")
        logger.info(f"   Credentials type: {type(credentials)}")
        logger.info(f"   Credentials: {credentials}")
        
        # Initialize components
        api_client = EmbryoscopeAPIClient(location, credentials)
        data_processor = EmbryoscopeDataProcessor(location)
        db_manager = EmbryoscopeDatabaseManager('../database/embryoscope_vila_mariana.db')
        
        # 3. Test API connection
        logger.info("3. Testing API connection...")
        if not api_client.test_connection():
            logger.error("✗ API connection failed")
            return False
        logger.info("✓ API connection successful")
        
        # 4. Extract all data types
        logger.info("4. Extracting all data types...")
        extraction_timestamp = datetime.now()
        run_id = str(uuid.uuid4())
        
        all_data = {}
        
        # Extract patients
        logger.info("  - Extracting patients...")
        patients_data = api_client.get_patients()
        if patients_data:
            processed_patients = data_processor.process_patients(patients_data, extraction_timestamp, run_id)
            all_data['patients'] = processed_patients
            logger.info(f"    ✓ Extracted {len(processed_patients)} patients")
        else:
            logger.warning("    ⚠ No patients data")
            all_data['patients'] = pd.DataFrame()
        
        # Extract treatments (need patients first)
        logger.info("  - Extracting treatments...")
        if not all_data['patients'].empty:
            # Get first few patients for testing
            test_patients = all_data['patients']['PatientIDx'].head(3).tolist()
            all_treatments = []
            
            for patient_idx in test_patients:
                treatments_data = api_client.get_treatments(patient_idx)
                if treatments_data:
                    processed_treatments = data_processor.process_treatments(
                        treatments_data, patient_idx, extraction_timestamp, run_id
                    )
                    if not processed_treatments.empty:
                        all_treatments.append(processed_treatments)
            
            if all_treatments:
                all_data['treatments'] = pd.concat(all_treatments, ignore_index=True)
                logger.info(f"    ✓ Extracted {len(all_data['treatments'])} treatments")
            else:
                logger.warning("    ⚠ No treatments data")
                all_data['treatments'] = pd.DataFrame()
        else:
            logger.warning("    ⚠ No patients available for treatments")
            all_data['treatments'] = pd.DataFrame()
        
        # Extract embryo data (need patients and treatments)
        logger.info("  - Extracting embryo data...")
        if not all_data['patients'].empty and not all_data['treatments'].empty:
            all_embryo_data = []
            
            for _, treatment_row in all_data['treatments'].head(3).iterrows():
                patient_idx = str(treatment_row['PatientIDx'])
                treatment_name = str(treatment_row['TreatmentName'])
                
                embryo_data = api_client.get_embryo_data(patient_idx, treatment_name)
                if embryo_data:
                    processed_embryo = data_processor.process_embryo_data(
                        embryo_data, patient_idx, treatment_name, extraction_timestamp, run_id
                    )
                    if not processed_embryo.empty:
                        all_embryo_data.append(processed_embryo)
            
            if all_embryo_data:
                all_data['embryo_data'] = pd.concat(all_embryo_data, ignore_index=True)
                logger.info(f"    ✓ Extracted {len(all_data['embryo_data'])} embryo records")
            else:
                logger.warning("    ⚠ No embryo data")
                all_data['embryo_data'] = pd.DataFrame()
        else:
            logger.warning("    ⚠ No patients/treatments available for embryo data")
            all_data['embryo_data'] = pd.DataFrame()
        
        # Extract IDA scores
        logger.info("  - Extracting IDA scores...")
        idascore_data = api_client.get_idascore()
        if idascore_data:
            processed_idascore = data_processor.process_idascore(idascore_data, extraction_timestamp, run_id)
            all_data['idascore'] = processed_idascore
            logger.info(f"    ✓ Extracted {len(processed_idascore)} IDA scores")
        else:
            logger.warning("    ⚠ No IDA score data")
            all_data['idascore'] = pd.DataFrame()
        
        # 5. Save all data to database
        logger.info("5. Saving all data to database...")
        try:
            row_counts = db_manager.save_data(all_data, location, run_id, extraction_timestamp)
            logger.info("✓ Successfully saved all data to database")
            for data_type, count in row_counts.items():
                logger.info(f"  - {data_type}: {count} rows")
        except Exception as e:
            logger.error(f"✗ Database save failed: {e}")
            return False
        
        # 6. Verify saved data
        logger.info("6. Verifying saved data...")
        verification_results = {}
        
        for data_type in ['patients', 'treatments', 'embryo_data', 'idascore']:
            try:
                saved_data = db_manager.get_latest_data(data_type, location)
                record_count = len(saved_data)
                verification_results[data_type] = record_count
                logger.info(f"  ✓ {data_type}: {record_count} records retrieved")
                
                # Show sample data structure
                if not saved_data.empty:
                    logger.info(f"    Columns: {list(saved_data.columns)}")
                    logger.info(f"    Sample row: {saved_data.iloc[0].to_dict()}")
                else:
                    logger.info(f"    ⚠ No data found for {data_type}")
                    
            except Exception as e:
                logger.warning(f"  ⚠ {data_type}: Error retrieving data - {e}")
                verification_results[data_type] = 0
        
        # 7. Database integrity check
        logger.info("7. Database integrity check...")
        try:
            # Check if database file exists and has content
            import os
            db_file = "../database/embryoscope_vila_mariana.db"
            if os.path.exists(db_file):
                file_size = os.path.getsize(db_file)
                logger.info(f"  ✓ Database file exists: {file_size:,} bytes")
                
                # Test database connection and basic queries
                with duckdb.connect(db_file) as test_conn:
                    # Check schemas
                    schemas = test_conn.execute("SELECT schema_name FROM information_schema.schemata;").fetchall()
                    if schemas:
                        logger.info(f"  ✓ Schemas found: {[s[0] for s in schemas]}")
                    else:
                        logger.warning("  ⚠ No schemas found")
                    
                    # Check tables in embryoscope schema
                    tables = test_conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'embryoscope';").fetchall()
                    if tables:
                        logger.info(f"  ✓ Tables in embryoscope schema: {[t[0] for t in tables]}")
                    else:
                        logger.warning("  ⚠ No tables found in embryoscope schema")
                    
                    # Verify each table has data
                    for table in tables:
                        table_name = table[0]
                        count_result = test_conn.execute(f"SELECT COUNT(*) FROM embryoscope.{table_name}").fetchone()
                        count = count_result[0] if count_result else 0
                        logger.info(f"    - embryoscope.{table_name}: {count} rows")
                        
            else:
                logger.error(f"  ✗ Database file not found: {db_file}")
                return False
                
        except Exception as e:
            logger.error(f"  ✗ Database integrity check failed: {e}")
            return False
        
        # 8. Test summary
        logger.info("8. Test summary...")
        total_extracted = sum(len(df) for df in all_data.values() if not df.empty)
        total_saved = sum(verification_results.values())
        
        logger.info(f"  - Total records extracted: {total_extracted}")
        logger.info(f"  - Total records saved: {total_saved}")
        logger.info(f"  - Database file: ../database/embryoscope_vila_mariana.db")
        logger.info(f"  - Schema: embryoscope")
        
        # Show detailed results
        logger.info("  - Detailed results:")
        for data_type in ['patients', 'treatments', 'embryo_data', 'idascore']:
            extracted_count = len(all_data.get(data_type, pd.DataFrame()))
            saved_count = verification_results.get(data_type, 0)
            status = "✓" if saved_count > 0 else "⚠"
            logger.info(f"    {status} {data_type}: {extracted_count} extracted, {saved_count} saved")
        
        logger.info("=" * 50)
        logger.info("TEST RESULT")
        logger.info("=" * 50)
        logger.info("🎉 SUCCESS: All endpoints test passed!")
        logger.info("The generic processing system is working correctly.")
        logger.info("Database save verification completed successfully.")
        logger.info("=" * 50)
        
        return True
        
    except Exception as e:
        logger.error(f"❌ FAILED: All endpoints test failed!")
        logger.error(f"Error: {e}")
        logger.error(f"Error type: {type(e)}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        logger.error("Check the logs above for details.")
        return False

if __name__ == "__main__":
    setup_logging()
    success = test_all_endpoints()
    
    if success:
        print("\nTest completed successfully!")
    else:
        print("\nTest failed!")
        sys.exit(1) 