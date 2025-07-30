#!/usr/bin/env python3
"""
Simple test script to load only patients and test saving them to the database.
This tests the database insertion fix without running the full extraction.
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

def setup_logging():
    """Setup basic logging."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)

def test_patients_extraction_and_save():
    """Test extracting patients and saving them to database."""
    logger = setup_logging()
    
    logger.info("=" * 50)
    logger.info("Testing Patients Extraction and Save")
    logger.info("=" * 50)
    
    try:
        # Initialize components
        logger.info("1. Initializing components...")
        config_manager = EmbryoscopeConfigManager("params.yml")
        db_manager = EmbryoscopeDatabaseManager(
            config_manager.get_database_path(),
            config_manager.get_database_schema()
        )
        
        # Get enabled embryoscopes
        enabled_embryoscopes = config_manager.get_enabled_embryoscopes()
        if not enabled_embryoscopes:
            logger.error("No enabled embryoscopes found!")
            return False
        
        # Test with the first enabled embryoscope
        location = list(enabled_embryoscopes.keys())[0]
        config = enabled_embryoscopes[location]
        
        logger.info(f"2. Testing with embryoscope: {location}")
        
        # Initialize API client
        api_client = EmbryoscopeAPIClient(location, config, config_manager.get_rate_limit_delay())
        
        # Test connection
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
        
        # Process patients data
        logger.info("5. Processing patients data...")
        processor = EmbryoscopeDataProcessor(location)
        
        # Create extraction timestamp and run ID
        extraction_timestamp = datetime.now()
        run_id = f"test_patients_{location}_{extraction_timestamp.strftime('%Y%m%d_%H%M%S')}"
        
        # Process patients data
        processed_data = processor.process_patients(patients_data, extraction_timestamp, run_id)
        
        if processed_data is None or processed_data.empty:
            logger.error("Failed to process patients data")
            return False
        
        logger.info(f"✓ Processed {len(processed_data)} patient records")
        logger.info(f"  Columns: {list(processed_data.columns)}")
        
        # Test database save
        logger.info("6. Testing database save...")
        dataframes = {'patients': processed_data}
        
        try:
            row_counts = db_manager.save_data(dataframes, location, run_id, extraction_timestamp)
            logger.info(f"✓ Successfully saved data to database")
            logger.info(f"  Row counts: {row_counts}")
            
            # Verify data was saved
            logger.info("7. Verifying saved data...")
            saved_data = db_manager.get_latest_data('patients', location)
            logger.info(f"✓ Retrieved {len(saved_data)} records from database")
            
            return True
            
        except Exception as e:
            logger.error(f"✗ Database save failed: {e}")
            return False
        
    except Exception as e:
        logger.error(f"✗ Test failed with error: {e}")
        return False

def main():
    """Run the test."""
    logger = setup_logging()
    
    success = test_patients_extraction_and_save()
    
    logger.info("\n" + "=" * 50)
    logger.info("TEST RESULT")
    logger.info("=" * 50)
    
    if success:
        logger.info("🎉 SUCCESS: Patients extraction and save test passed!")
        logger.info("The database insertion fix is working correctly.")
    else:
        logger.error("❌ FAILED: Patients extraction and save test failed!")
        logger.error("Check the logs above for details.")
    
    logger.info("\nTest completed.")

if __name__ == "__main__":
    main() 