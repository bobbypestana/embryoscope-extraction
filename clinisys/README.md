# Clinisys Data Pipeline

This folder contains the data pipeline for extracting, transforming, and loading Clinisys data from bronze to silver layer.

## Setup

1. **Copy the template configuration:**
   ```bash
   cp params_template.yml params.yml
   ```

2. **Edit `params.yml` with your actual database credentials:**
   - Replace `username:password@host:port/database` with your actual connection string
   - Update the `con_params` section with your database details

## Files Structure

### Core Pipeline Files
- `bronze_loader.py` - Extracts data from CSV files or database to bronze layer
- `silver_loader_try_strptime_complete.py` - **Recommended** silver loader using try_strptime() for robust data transformation
- `check_tables.py` - Validates bronze and silver table structures and row counts

### Alternative Silver Loaders
- `silver_loader_try_strptime.py` - Basic try_strptime implementation
- `silver_loader_pandas.py` - Pandas-based transformation (alternative approach)
- `silver_loader_ultra_robust.py` - Complex SQL-based transformation

### Batch Files
- `run_bronze_and_silver.bat` - Runs both bronze and silver loaders
- `run_silver_loader_try_strptime.bat` - Runs the recommended silver loader
- `run_silver_loader_pandas.bat` - Runs pandas-based silver loader

## Usage

### Recommended Workflow
1. **Extract to Bronze:**
   ```bash
   python bronze_loader.py
   ```

2. **Transform to Silver:**
   ```bash
   python silver_loader_try_strptime_complete.py
   ```

3. **Validate Results:**
   ```bash
   python check_tables.py
   ```

## Key Features

- **Complete Column Preservation**: All columns from bronze are included in silver
- **Robust Data Handling**: Uses `try_strptime()` and `try_cast()` to handle bad data gracefully
- **Proper Data Types**: Dates, times, numbers, and strings are properly typed
- **Deduplication**: Removes duplicate records based on hash while keeping latest
- **Comprehensive Logging**: Detailed logs for troubleshooting

## Data Quality

The silver layer transformation:
- Casts invalid dates/times to NULL instead of failing
- Handles malformed numeric data gracefully
- Preserves all original columns
- Applies appropriate data type conversions
- Removes duplicate records

## Security

- `params.yml` contains sensitive database credentials and is excluded from git
- Use `params_template.yml` as a template for your configuration
- Never commit actual database credentials to version control 