"""
Test Script for Embryoscope Data Extraction System
Tests all components to ensure they work correctly.
"""

import sys
import os
from datetime import datetime

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def test_config_manager():
    """Test configuration manager."""
    print("Testing Configuration Manager...")
    try:
        from config_manager import EmbryoscopeConfigManager
        
        config_manager = EmbryoscopeConfigManager()
        config_manager.print_config_summary()
        
        # Test validation
        is_valid = config_manager.validate_config()
        print(f"Configuration valid: {is_valid}")
        
        # Test enabled embryoscopes
        enabled = config_manager.get_enabled_embryoscopes()
        print(f"Enabled embryoscopes: {list(enabled.keys())}")
        
        return True
    except Exception as e:
        print(f"Configuration Manager test failed: {e}")
        return False

def test_api_client():
    """Test API client with a single location."""
    print("\nTesting API Client...")
    try:
        from config_manager import EmbryoscopeConfigManager
        from api_client import EmbryoscopeAPIClient
        
        config_manager = EmbryoscopeConfigManager()
        enabled_embryoscopes = config_manager.get_enabled_embryoscopes()
        
        if not enabled_embryoscopes:
            print("No enabled embryoscopes found for testing")
            return False
        
        # Test with first enabled embryoscope
        location = list(enabled_embryoscopes.keys())[0]
        config = enabled_embryoscopes[location]
        
        print(f"Testing connection to {location}...")
        client = EmbryoscopeAPIClient(location, config)
        
        # Test connection
        connection_ok = client.test_connection()
        print(f"Connection test: {'SUCCESS' if connection_ok else 'FAILED'}")
        
        if connection_ok:
            # Test data summary
            summary = client.get_data_summary()
            print(f"Data summary: {summary}")
        
        return connection_ok
    except Exception as e:
        print(f"API Client test failed: {e}")
        return False

def test_data_processor():
    """Test data processor with sample data."""
    print("\nTesting Data Processor...")
    try:
        from data_processor import EmbryoscopeDataProcessor
        
        processor = EmbryoscopeDataProcessor("Test Location")
        
        # Test data
        test_patients = {
            "Patients": [
                {"PatientIDx": "TEST001", "Name": "Test Patient 1"},
                {"PatientIDx": "TEST002", "Name": "Test Patient 2"}
            ]
        }
        
        test_treatments = {
            "TreatmentList": ["Treatment 1", "Treatment 2"]
        }
        
        extraction_timestamp = datetime.now()
        run_id = "test_run_001"
        
        # Process test data
        df_patients = processor.process_patients(test_patients, extraction_timestamp, run_id)
        df_treatments = processor.process_treatments(test_treatments, "TEST001", extraction_timestamp, run_id)
        
        print(f"Processed {len(df_patients)} patients")
        print(f"Processed {len(df_treatments)} treatments")
        
        # Test summary
        test_data = {
            'patients': test_patients,
            'treatments': {'TEST001': test_treatments}
        }
        processed_data = processor.process_all_data(test_data, extraction_timestamp, run_id)
        summary = processor.get_data_summary(processed_data)
        print(f"Data summary: {summary}")
        
        return True
    except Exception as e:
        print(f"Data Processor test failed: {e}")
        return False

def test_database_manager():
    """Test database manager."""
    print("\nTesting Database Manager...")
    try:
        from database_manager import EmbryoscopeDatabaseManager
        
        # Use test database
        db_manager = EmbryoscopeDatabaseManager("../database/embryoscope_test.duckdb")
        
        # Test data
        import pandas as pd
        test_df = pd.DataFrame({
            'PatientIDx': ['TEST001', 'TEST002'],
            'Name': ['Test Patient 1', 'Test Patient 2'],
            '_location': ['Test Location', 'Test Location'],
            '_extraction_timestamp': [datetime.now(), datetime.now()],
            '_run_id': ['test_run', 'test_run'],
            '_row_hash': ['hash1', 'hash2']
        })
        
        # Save test data
        dataframes = {'patients': test_df}
        row_counts = db_manager.save_data(dataframes, 'Test Location', 'test_run', datetime.now())
        print(f"Saved data with row counts: {row_counts}")
        
        # Get summary
        summary = db_manager.get_data_summary('Test Location')
        print(f"Database summary: {summary}")
        
        return True
    except Exception as e:
        print(f"Database Manager test failed: {e}")
        return False

def test_query_interface():
    """Test query interface."""
    print("\nTesting Query Interface...")
    try:
        from exploration.query_interface import EmbryoscopeQueryInterface
        
        query_interface = EmbryoscopeQueryInterface()
        
        # Test summaries
        patient_summary = query_interface.get_patient_summary()
        print(f"Patient summary: {patient_summary}")
        
        embryo_summary = query_interface.get_embryo_summary()
        print(f"Embryo summary: {embryo_summary}")
        
        return True
    except Exception as e:
        print(f"Query Interface test failed: {e}")
        return False

def main():
    """Run all tests."""
    print("=== Embryoscope System Test ===")
    print(f"Test started at: {datetime.now()}")
    print()
    
    tests = [
        ("Configuration Manager", test_config_manager),
        ("API Client", test_api_client),
        ("Data Processor", test_data_processor),
        ("Database Manager", test_database_manager),
        ("Query Interface", test_query_interface)
    ]
    
    results = {}
    
    for test_name, test_func in tests:
        try:
            result = test_func()
            results[test_name] = result
            status = "PASS" if result else "FAIL"
            print(f"{test_name}: {status}")
        except Exception as e:
            results[test_name] = False
            print(f"{test_name}: FAIL - {e}")
    
    print("\n=== Test Summary ===")
    passed = sum(1 for result in results.values() if result)
    total = len(results)
    
    for test_name, result in results.items():
        status = "PASS" if result else "FAIL"
        print(f"{test_name}: {status}")
    
    print(f"\nOverall: {passed}/{total} tests passed")
    
    if passed == total:
        print("All tests passed! System is ready for use.")
        return 0
    else:
        print("Some tests failed. Check the output above for details.")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code) 