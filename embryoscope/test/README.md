# Test Files

This folder contains test scripts for the embryoscope data extraction system.

## Files

- `test_all_endpoints.py` - Comprehensive test of all API endpoints and data processing
- `test_api_retries.py` - Test API retry logic and error handling
- `test_connection_manager.py` - Test database connection management
- `test_patients_save.py` - Test patient data extraction and saving
- `test_system.py` - System-wide integration tests

## Running Tests

From the main embryoscope directory:

```bash
# Run all endpoints test
python test/test_all_endpoints.py

# Or use the batch file
test/test_all_endpoints.bat
```

## Note

All test files have been updated to import modules from the parent directory. The batch files will automatically set the correct working directory. 