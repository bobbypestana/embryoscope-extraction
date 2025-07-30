#!/usr/bin/env python3
"""
Test script to extract and save the last N patients, their treatments, and embryo_data to the bronze layer.
This is for fast, repeatable testing of the incremental bronze logic.
"""

import os
import sys
import logging
from datetime import datetime

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.config_manager import EmbryoscopeConfigManager
from utils.api_client import EmbryoscopeAPIClient
from utils.data_processor import EmbryoscopeDataProcessor
from utils.database_manager import EmbryoscopeDatabaseManager

# Adjustable parameter: how many patients to test
TAIL_N = 10

def setup_logging():
    """Setup basic logging."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)

def test_patients_treatments_embryos_tail():
    """Test extracting tail N patients, their treatments, and embryo_data, saving to bronze."""
    logger = setup_logging()
    logger.info("=" * 50)
    logger.info(f"Testing Tail {TAIL_N} Patients, Treatments, and Embryo Data Save")
    logger.info("=" * 50)
    try:
        # Initialize components
        logger.info("1. Initializing components...")
        config_manager = EmbryoscopeConfigManager("embryoscope/params.yml")
        enabled_embryoscopes = config_manager.get_enabled_embryoscopes()
        if not enabled_embryoscopes:
            logger.error("No enabled embryoscopes found!")
            return False
        location = list(enabled_embryoscopes.keys())[0]
        config = enabled_embryoscopes[location]
        # Set test DB path robustly relative to project root
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
        db_path = os.path.join(project_root, 'database', f"embryoscope_{location.lower().replace(' ', '_')}_test.duckdb")
        print("Saving to DB path:", db_path)
        logger.info(f"2. Testing with embryoscope: {location} (DB: {db_path})")
        api_client = EmbryoscopeAPIClient(location, config, config_manager.get_rate_limit_delay())
        db_manager = EmbryoscopeDatabaseManager(db_path, config_manager.get_database_schema())
        logger.info("3. Testing API connection...")
        if not api_client.test_connection():
            logger.error(f"Failed to connect to {location}")
            return False
        logger.info("✓ API connection successful")
        # Extract patients
        logger.info("4. Extracting patients...")
        patients_data = api_client.get_patients()
        if not patients_data or 'Patients' not in patients_data:
            logger.error("No patients data received")
            return False
        patients_list = patients_data['Patients']
        logger.info(f"✓ Extracted {len(patients_list)} patients")
        # Take only the last N patients
        patients_list = patients_list[-TAIL_N:]
        patients_data['Patients'] = patients_list
        logger.info(f"✓ Using tail {TAIL_N} patients for test")
        # Get tail N patient IDs
        selected_patient_ids = [
            patient.get('PatientIDx') or patient.get('PatientIdx') or patient.get('PatientID')
            for patient in patients_list
            if patient.get('PatientIDx') or patient.get('PatientIdx') or patient.get('PatientID')
        ]
        # Use the real extractor logic for these patients
        extractor = EmbryoscopeExtractor("embryoscope/params.yml")
        success = extractor.extract_for_patients(location, selected_patient_ids, db_path=db_path)
        if success:
            logger.info(f"Test extraction for tail {TAIL_N} patients completed successfully.")
        else:
            logger.error(f"Test extraction for tail {TAIL_N} patients failed.")
        return success
    except Exception as e:
        logger.error(f"✗ Test failed with error: {e}")
        return False

def main():
    logger = setup_logging()
    success = test_patients_treatments_embryos_tail()
    logger.info("\n" + "=" * 50)
    logger.info("TEST RESULT")
    logger.info("=" * 50)
    if success:
        logger.info("🎉 SUCCESS: Tail N patients, treatments, and embryo_data extraction and save test passed!")
    else:
        logger.error("❌ FAILED: Tail N patients, treatments, and embryo_data extraction and save test failed!")
        logger.error("Check the logs above for details.")
    logger.info("\nTest completed.")

if __name__ == "__main__":
    main() 