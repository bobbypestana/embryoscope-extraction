# Utility Files

This folder contains utility scripts for database management and system operations.

## Files

- `check_database.py` - Check database contents and structure
- `close_db_connections.py` - Close database connections (useful for troubleshooting)
- `launch_all_clinics.py` - Launch extraction for all enabled clinics in parallel

## Usage

From the main embryoscope directory:

```bash
# Check database contents
python utils/check_database.py

# Close database connections
python utils/close_db_connections.py

# Launch all clinics (legacy - use embryoscope_extractor.py instead)
python utils/launch_all_clinics.py
```

## Note

All utility files have been updated to work from their new location. Database paths have been adjusted to point to the correct location (`../../database/`). 