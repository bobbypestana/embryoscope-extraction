# Embryoscope Data Extraction System

A comprehensive system for extracting, processing, and storing embryoscope data from multiple locations using DuckDB for efficient incremental extraction.

## Overview

This system extracts embryo data from multiple embryoscope locations via their APIs, processes the JSON data into structured formats, and stores it in a DuckDB database with incremental extraction capabilities. The system supports parallel processing and ensures only new or changed data is extracted in subsequent runs.

## Features

- **Multi-location Support**: Extract data from 6 embryoscope locations simultaneously
- **Incremental Extraction**: Only extract new or changed data using hash-based change detection
- **Parallel Processing**: Run extractions in parallel for faster processing
- **Rate Limiting**: Built-in rate limiting to avoid overwhelming APIs
- **Comprehensive Data Processing**: Flatten nested JSON structures into relational format
- **DuckDB Storage**: Efficient columnar storage with temporal tracking
- **Query Interface**: Easy access to extracted data with filtering and summarization
- **Logging**: Comprehensive logging for monitoring and debugging

## System Architecture

```
embryoscope/
├── config_manager.py          # Configuration management
├── api_client.py              # API client with authentication
├── schema_config.py           # Schema configuration and mappings
├── data_processor.py          # Generic JSON flattening and data processing
├── database_manager.py        # DuckDB operations and incremental logic
├── embryoscope_extractor.py   # Main extraction orchestrator
├── query_interface.py         # Data querying and export interface
├── close_db_connections.py    # Database connection manager utility
├── close_db_connections.bat   # Windows batch file for connection manager
├── test_connection_manager.py # Test script for connection manager
├── test_connection_manager.bat # Windows batch file for connection test
├── test_api_retries.py        # Test script for API retry logic
├── test_api_retries.bat       # Windows batch file for API retry test
├── test_patients_save.py      # Test script for patients extraction
├── test_patients_save.bat     # Windows batch file for patients test
├── test_all_endpoints.py      # Test script for all endpoints
├── test_all_endpoints.bat     # Windows batch file for all endpoints test
├── params.yml                 # Configuration file
├── requirements.txt           # Python dependencies
├── run_extraction.bat         # Windows batch file
├── README.md                  # This file
└── logs/                      # Extraction logs
```

### Generic Processing System

The system now uses a **configuration-driven approach** that makes it easy to add new endpoints:

- **Schema Configuration** (`schema_config.py`): All API-to-database mappings are defined centrally
- **Generic Processing**: A single `process_data_generic()` method handles all data types
- **Extensible**: Adding new endpoints only requires updating the schema configuration
- **Consistent**: All data types follow the same processing pattern

#### Supported Data Types

1. **patients**: Patient information with name and birth date
2. **treatments**: Treatment information linked to patients
3. **embryo_data**: Embryo data with flattened nested structures
4. **idascore**: IDA score evaluation data

#### Adding New Endpoints

To add a new endpoint, simply update `schema_config.py`:

```python
# Add to TABLE_SCHEMAS
'new_endpoint': {
    'columns': ['Field1 VARCHAR', 'Field2 INTEGER', ...],
    'primary_key': ['Field1', '_location', '_extraction_timestamp']
}

# Add to COLUMN_MAPPINGS
'new_endpoint': {
    'api_fields': ['ApiField1', 'ApiField2'],
    'db_columns': ['Field1', 'Field2'],
    'transformations': {
        'Field1': lambda row: row.get('ApiField1'),
        'Field2': lambda row: int(row.get('ApiField2', 0))
    }
}

# Add to API_STRUCTURES
'new_endpoint': {
    'root_key': 'DataList',
    'is_list': True,
    'requires_patient_context': False
}
```

## Data Flow

1. **Configuration**: Load embryoscope credentials and settings
2. **Authentication**: Authenticate with each embryoscope API
3. **Data Extraction**: Extract patients → treatments → embryo data → IDA scores
4. **Processing**: Flatten JSON structures and add metadata
5. **Storage**: Store in DuckDB with incremental logic
6. **Query**: Access data through query interface

## Database Schema

### Metadata Tables
- `view_metadata`: Tracks extraction metadata for each view/location
- `incremental_runs`: Records each extraction run with performance metrics
- `row_changes`: Tracks individual row-level changes

### Data Tables
- `data_patients`: Patient information
- `data_treatments`: Treatment information linked to patients
- `data_embryo_data`: Embryo data with flattened structures
- `data_idascore`: IDA score evaluation data

All tables include metadata columns:
- `_location`: Embryoscope location identifier
- `_extraction_timestamp`: When the data was extracted
- `_run_id`: Unique run identifier
- `_row_hash`: MD5 hash for change detection

## Configuration

Edit `params.yml` to configure:

```yaml
embryoscope_credentials:
  'Santa Joana':     
    ip: '10.250.113.97'
    login: 'WEB'
    password: 'web'
    port: 4000
    enabled: true
  # ... other locations

database:
  path: '../database/embryoscope_incremental.duckdb'
  schema: 'embryoscope'

extraction:
  rate_limit_delay: 0.1  # seconds between requests
  max_retries: 3
  timeout: 30
  batch_size: 1000
  parallel_processing: true
  max_workers: 3
```

## Installation

1. **Clone the Repository**:
   ```bash
   git clone <repository-url>
   cd embryoscope
   ```

2. **Create Python Environment** (Python 3.9+ required):
   ```bash
   conda create -n try_request python=3.9
   conda activate try_request
   ```

3. **Install Python Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

4. **Setup Configuration**:
   ```bash
   # Copy the template and edit with your credentials
   cp params_template.yml params.yml
   # Edit params.yml with your actual embryoscope credentials
   ```

5. **Create Database Directory**:
   ```bash
   mkdir -p ../database
   ```

## Security Note

⚠️ **Never commit sensitive information!** 
- The `params.yml` file contains real credentials and is excluded from git
- Use `params_template.yml` as a template for your configuration
- Keep your credentials secure and never share them in version control

## Testing

Before running the full extraction, you can test individual components:

### Test Database Connection Manager
```bash
# Windows
test_connection_manager.bat

# Or manually
python test_connection_manager.py
```

This test verifies:
- Database connection creation
- File locking detection
- Connection manager functionality
- Process identification

### Test API Retry Logic
```bash
# Windows
test_api_retries.bat

# Or manually
python test_api_retries.py
```

This test verifies:
- API request retry mechanism (3 attempts)
- Exponential backoff timing
- Authentication retry logic
- Error handling

### Test Patients Extraction and Save
```bash
# Windows
test_patients_save.bat

# Or manually
python test_patients_save.py
```

This test verifies:
- Patients data extraction
- Column mapping and transformations
- Database schema compatibility
- Incremental save functionality

### Test All Endpoints with Generic Processing
```bash
# Windows
test_all_endpoints.bat

# Or manually
python test_all_endpoints.py
```

This comprehensive test verifies:
- All data types (patients, treatments, embryo_data, idascore)
- Generic processing system
- Schema configuration
- Database operations for all endpoints
- End-to-end data flow

## Usage

### Running Extraction

**Windows (Batch File)**:
```bash
run_extraction.bat
```

**Python Script**:
```bash
python embryoscope_extractor.py
```

**Individual Location**:
```python
from embryoscope_extractor import EmbryoscopeExtractor

extractor = EmbryoscopeExtractor()
success = extractor.extract_single_location('Santa Joana')
```

### Querying Data

```python
from query_interface import EmbryoscopeQueryInterface

# Initialize query interface
query = EmbryoscopeQueryInterface()

# Get latest data
patients = query.get_latest_patients()
embryos = query.get_latest_embryo_data()
complete_data = query.get_complete_embryo_data()

# Filter embryos
high_score_embryos = query.get_embryos_by_criteria(
    min_score=0.8, 
    viability='Viable'
)

# Get summaries
summary = query.get_comprehensive_summary()
patient_summary = query.get_patient_summary('Santa Joana')

# Export to CSV
query.export_data_to_csv('data_output')
```

### Database Queries

Direct DuckDB queries:

```sql
-- Get latest embryo data for a location
SELECT * FROM embryoscope.data_embryo_data 
WHERE _location = 'Santa Joana' 
AND _extraction_timestamp = (
    SELECT MAX(_extraction_timestamp) 
    FROM embryoscope.data_embryo_data 
    WHERE _location = 'Santa Joana'
);

-- Get embryos with high IDA scores
SELECT e.*, i.Score, i.Viability
FROM embryoscope.data_embryo_data e
JOIN embryoscope.data_idascore i ON e.EmbryoID = i.EmbryoID
WHERE i.Score > 0.8 AND i.Viability = 'Viable'
AND e._location = 'Santa Joana';

-- Track extraction performance
SELECT * FROM embryoscope.incremental_runs
ORDER BY extraction_timestamp DESC
LIMIT 10;
```

## API Endpoints

The system extracts data from these embryoscope API endpoints:

- `GET/patients` - Patient information
- `GET/TREATMENT` - Treatments for a patient
- `GET/embryodata` - Embryo data for a patient-treatment
- `GET/IDASCORE` - IDA score evaluations
- `GET/embryoID` - Embryo identifiers
- `GET/fertilizationtime` - Fertilization timing
- `GET/imageruns` - Image run data
- `GET/evaluation` - Evaluation data
- `GET/embryofate` - Embryo fate information
- `GET/embryodetails` - Detailed embryo information
- `GET/transfers` - Transfer data

## Data Quality

The system includes data quality checks:

- **Connection Validation**: Tests API connectivity before extraction
- **Data Validation**: Validates JSON structure and required fields
- **Incremental Logic**: Only processes new/changed data
- **Error Handling**: Comprehensive error handling and logging
- **Retry Logic**: Automatic retries for failed requests

## Performance

- **Parallel Processing**: Up to 3 concurrent extractions (configurable)
- **Rate Limiting**: 100ms delay between requests (configurable)
- **Incremental Storage**: Only stores changed data
- **Efficient Queries**: Optimized DuckDB queries with indexes

## Monitoring

### Logs
- Extraction logs: `logs/embryoscope_extraction_YYYYMMDD_HHMMSS.log`
- Database logs: Integrated with extraction logs

### Metrics
- Extraction performance: Processing time, rows processed
- Data quality: Missing data, validation errors
- API performance: Response times, error rates

### Database Monitoring
```sql
-- Check extraction history
SELECT * FROM embryoscope.incremental_runs;

-- Monitor data growth
SELECT 
    _location,
    COUNT(*) as total_rows,
    MAX(_extraction_timestamp) as last_update
FROM embryoscope.data_embryo_data
GROUP BY _location;
```

## Troubleshooting

### Common Issues

1. **Connection Failures**:
   - Check network connectivity to embryoscope IPs
   - Verify credentials in `params.yml`
   - Check if embryoscope is enabled

2. **Authentication Errors**:
   - Verify login/password credentials
   - Check if API is accessible from your network

3. **Data Processing Errors**:
   - Check logs for specific error messages
   - Verify JSON structure from API responses

4. **Database Errors**:
   - Ensure database directory exists and is writable
   - Check disk space for database file
   - **File Locking**: If you get "file is being used by another process" error:
     ```bash
     # Run the connection manager
     close_db_connections.bat
     # Or manually
     python close_db_connections.py
     ```

### Debug Mode

Enable debug logging by modifying the logging level in `embryoscope_extractor.py`:

```python
logger.setLevel(logging.DEBUG)
```

## Development

### Adding New Locations

1. Add location to `params.yml`:
   ```yaml
   'New Location':
     ip: '192.168.1.100'
     login: 'WEB'
     password: 'web'
     port: 4000
     enabled: true
   ```

2. Test connection:
   ```python
   from api_client import EmbryoscopeAPIClient
   client = EmbryoscopeAPIClient('New Location', config)
   print(client.test_connection())
   ```

## System Requirements

- **Python**: 3.9 or higher
- **Conda Environment**: `try_request` (recommended)
- **Operating System**: Windows 10/11, Linux, macOS
- **Memory**: Minimum 4GB RAM (8GB+ recommended for large datasets)
- **Storage**: Sufficient space for DuckDB database and logs

### Extending Data Processing

Modify `data_processor.py` to add new data types or processing logic.

### Custom Queries

Extend `query_interface.py` with custom query methods for specific use cases.

## License

This system is developed for internal use at Huntington.

## Support

For issues or questions, check the logs and refer to this documentation. The system includes comprehensive error handling and logging to help diagnose problems. 