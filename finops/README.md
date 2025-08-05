# FinOps Data Pipeline

This project handles the loading of financial/billing data from Excel files into the Huntington data lake.

## Project Structure

```
finops/
├── data_input/
│   └── matr550/                    # Excel files from MATR550 system
│       ├── 0401_FAT-xBH-*.xlsx    # Belo Horizonte
│       ├── 0401_FAT-xBR-*.xlsx    # Brasília
│       ├── 0401_FAT-xCA-*.xlsx    # Campinas
│       ├── 0401_FAT-xIB-*.xlsx    # Ibirapuera
│       ├── 0401_FAT-xIB2-*.xlsx   # Ibirapuera 2
│       ├── 0401_FAT-xSA-*.xlsx    # Salvador
│       ├── 0401_FAT-xSJ-*.xlsx    # Santa Joana
│       ├── 0401_FAT-xVM-*.xlsx    # Vila Mariana
│       └── 0401_FAT-xVM2-*.xlsx   # Vila Mariana 2
├── logs/                          # Execution logs
├── 01_load_to_bronze.py           # Main loader script
├── 00_run_finops_loader.bat       # Batch file to run loader
├── verify_tables.py               # Verification script
├── requirements.txt               # Python dependencies
└── README.md                      # This file
```

## File Naming Pattern

Excel files follow the pattern: `0401_FAT-x{CLINIC_CODE}-{DATE_RANGE}.xlsx`

- `0401_FAT-x` - Fixed prefix (report type: Faturamento)
- `{CLINIC_CODE}` - Clinic identifier (BH, BR, CA, IB, IB2, SA, SJ, VM, VM2)
- `{DATE_RANGE}` - Date range in format `000101a250710` (01/01/2000 to 10/07/2025)

## Clinic Codes

| Code | Location |
|------|----------|
| xBH  | Belo Horizonte |
| xBR  | Brasília |
| xCA  | Campinas |
| xIB  | Ibirapuera |
| xIB2 | Ibirapuera 2 |
| xSA  | Salvador |
| xSJ  | Santa Joana |
| xVM  | Vila Mariana |
| xVM2 | Vila Mariana 2 |

## Usage

### Prerequisites

1. Activate the conda environment:
   ```bash
   conda activate try_request
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

### Running the Loader

#### Option 1: Using the batch file (Windows)
```bash
00_run_finops_loader.bat
```

#### Option 2: Direct Python execution
```bash
python 01_load_to_bronze.py
```

### Verification

To verify the loaded data:
```bash
python verify_tables.py
```

## Data Pipeline

### Bronze Layer

- **Purpose**: Raw data storage with minimal processing
- **Location**: `huntington_data_lake.duckdb` in `bronze` schema
- **Table Naming**: Uses filename without date range (e.g., `0401_FAT-xBH`)
- **Data**: All columns from "3-Analitico - Itens" sheet, preserved as-is
- **Deduplication**: Hash-based logic to prevent duplicate rows
- **Metadata**: Includes `hash`, `extraction_timestamp`, and `file_name` columns

### Hash-Based Deduplication

The loader uses MD5 hashes of row data to prevent duplicate insertions:
- Hash is calculated from all data columns (excluding metadata)
- Only new rows (based on hash) are inserted
- Supports incremental loading of updated files

## Data Structure

Each table contains:
- All original columns from the Excel sheet (raw data)
- `hash` - MD5 hash of row data for deduplication
- `extraction_timestamp` - When the data was loaded
- `file_name` - Source Excel file name

## Logging

- Logs are stored in `logs/` directory
- Timestamped log files for each execution
- Both file and console output
- Detailed progress and error reporting

## Current Status

- ✅ Bronze layer loader implemented
- ✅ Hash-based deduplication working
- ✅ All 9 clinic files processed
- ✅ Total: 1,113,372 rows loaded across all tables

## Next Steps

- [ ] Silver layer transformation (data cleaning, standardization)
- [ ] Gold layer consolidation (business logic, aggregations)
- [ ] Data quality validation
- [ ] Automated scheduling 