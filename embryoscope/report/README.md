# Embryo Image Availability Pipeline

This directory contains a robust ETL pipeline for checking and tracking embryo image availability via the Embryoscope API.

## Architecture

The pipeline follows a **Medallion Architecture** with proper separation of concerns:

```
API → Logs (JSON) → Bronze → Silver → Gold
```

| Layer | Table | Purpose |
|:------|:------|:--------|
| **Bronze** | `bronze.embryo_image_availability_logs` | Append-only log of all API checks |
| **Silver** | `silver.embryo_image_availability_latest` | Latest status per embryo (deduplicated) |
| **Gold** | `gold.embryo_image_status_changes` | Audit trail of status changes |

## Scripts

### One-Time Setup

**`00_migrate_to_robust_schema.py`** - Creates Bronze/Silver/Gold tables and migrates existing data
```powershell
conda run -n try_request python "embryoscope/report/00_migrate_to_robust_schema.py"
```

### Regular Pipeline (Run in Order)

**Step 1: `01_check_image_availability.py`** - Query API and write JSON logs
```powershell
# Check only new embryos
conda run -n try_request python "embryoscope/report/01_check_image_availability.py" --mode new

# Check new + errors/no images
conda run -n try_request python "embryoscope/report/01_check_image_availability.py" --mode retry

# Full refresh
conda run -n try_request python "embryoscope/report/01_check_image_availability.py" --mode all
```

**Step 2: `02_logs_to_bronze.py`** - Ingest JSON results into Bronze
```powershell
conda run -n try_request python "embryoscope/report/02_logs_to_bronze.py" --input-dir "embryoscope/report/api_results/new_20260128_175702"
```

**Step 3: `03_bronze_to_silver.py`** - Update Silver with latest status
```powershell
conda run -n try_request python "embryoscope/report/03_bronze_to_silver.py"
```

**Step 4: `04_track_changes_to_gold.py`** - Track status changes in Gold
```powershell
conda run -n try_request python "embryoscope/report/04_track_changes_to_gold.py"
```

**Step 5: `05_cleanup_logs.py`** - Cleanup old logs and API results (optional)
```powershell
conda run -n try_request python "embryoscope/report/05_cleanup_logs.py"
```
- Keeps only the last 3 general log files
- Keeps only the last 3 API result directories
- Automatically runs at the end of the batch pipeline


### Quick Start: Run Complete Pipeline

**`00_run_image_availability_pipeline.bat`** - Runs all 4 steps automatically

```batch
REM Default: retry mode (new embryos + errors/no images)
00_run_image_availability_pipeline.bat

REM Explicit modes:
00_run_image_availability_pipeline.bat new    # Only new embryos
00_run_image_availability_pipeline.bat retry  # New + errors/no images
00_run_image_availability_pipeline.bat all    # Full refresh

REM Test with limited embryos
00_run_image_availability_pipeline.bat retry 5
```


## Benefits of This Architecture

1. **Separation of Concerns**: Each script has a single, clear responsibility
2. **Resumability**: If any step fails, you can re-run just that step
3. **Auditability**: Bronze keeps full history, Gold tracks changes
4. **Flexibility**: Can process logs from any source (not just live API)
5. **Testing**: Each layer can be tested independently

## Archive

Old scripts are in `archive/`:
- `archive/01_check_image_availability_old.py` - Original monolithic script
- `archive/02_extract_from_logs_old.py` - Original log recovery script
