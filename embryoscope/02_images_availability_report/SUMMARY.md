# Embryo Image Availability Report - Summary

## Overview

Created a complete system to check image availability for embryos in the database by querying the Embryoscope API servers.

## Files Created

### 📁 `embryoscope/report/`

#### Scripts:
1. **`00_test_setup.py`** - Test script to verify connections and API functionality
   - Tests authentication to all enabled servers
   - Runs sample checks on 3 embryos per server
   - Validates setup before running full extraction

2. **`01_check_image_availability.py`** - Main extraction script
   - Queries `gold.embryoscope_embrioes` for all embryos
   - Groups by server (`patient_unit_huntington`)
   - Checks image availability via API (`GET/imageruns` endpoint)
   - Saves results to `gold.embryo_image_availability_raw`

#### Documentation:
3. **`README.md`** - Complete documentation
   - Script descriptions
   - Usage instructions
   - Output table schema
   - Configuration details

#### Exploration:
4. **`explore/explore_image_availability.ipynb`** - Jupyter notebook for data analysis
   - Query and explore the raw table
   - Summary statistics by server
   - API response status breakdown
   - Sample data views
   - Error analysis
   - Temporal and distribution analysis

#### Logs:
5. **`logs/`** - Directory for execution logs

## Key Features

### ✅ Reuses Existing Infrastructure
- Uses `embryoscope/utils/api_client.py` for API calls
- Uses `embryoscope/utils/config_manager.py` for configuration
- Leverages existing authentication and retry logic

### ✅ Robust Error Handling
- **Rate limiting**: 10 requests/second (0.1s delay between requests)
- **Auto re-authentication**: Handles token expiration
- **Retry logic**: Up to 3 attempts per embryo
- **Token refresh**: Every 1000 embryos
- **Connection loss recovery**: Automatic re-authentication

### ✅ Progress Tracking
- Logs progress every 100 embryos
- Shows completion percentage
- Detailed logging to file and console

### ✅ Raw Data Preservation
- Saves all original columns from `gold.embryoscope_embrioes`
- Adds API check status fields:
  - `image_available` (BOOLEAN)
  - `image_runs_count` (INTEGER)
  - `api_response_status` (VARCHAR)
  - `error_message` (VARCHAR)
  - `checked_at` (TIMESTAMP)

## Output Table

**Table Name**: `gold.embryo_image_availability_raw`

**Original Columns** (from `gold.embryoscope_embrioes`):
- `prontuario`
- `patient_PatientID`
- `patient_PatientIDx`
- `patient_unit_huntington`
- `treatment_TreatmentName`
- `embryo_EmbryoID`
- `embryo_EmbryoDate`

**Status Columns** (added by script):
- `image_available` - Whether images are available
- `image_runs_count` - Number of image runs found
- `api_response_status` - API response status (success, no_response, error, etc.)
- `error_message` - Error message if any
- `checked_at` - When the check was performed

## Configuration

### Enabled Servers (in `embryoscope/params.yml`):
- ✅ Vila Mariana
- ✅ Ibirapuera
- ✅ Belo Horizonte
- ✅ Brasilia
- ❌ Salvador (disabled - connection timeout)

### Rate Limiting:
- **10 requests/second** (0.1s delay between requests)
- Respects server capacity limits

## Usage

### 1. Test Setup (Recommended First)
```bash
cd "G:/My Drive/projetos_individuais/Huntington"
conda activate try_request
python embryoscope/report/00_test_setup.py
```

This will:
- Test connections to all enabled servers
- Verify authentication works
- Test API calls on 3 sample embryos per server

### 2. Run Full Extraction
```bash
cd "G:/My Drive/projetos_individuais/Huntington"
conda activate try_request
python embryoscope/report/01_check_image_availability.py
```

This will:
- Process all ~115k embryos from 4 servers
- Save results to `gold.embryo_image_availability_raw`
- Generate summary statistics
- Create detailed logs in `embryoscope/report/logs/`

**Estimated time**: ~3-4 hours for 115k embryos at 10 req/s

### 3. Explore Results
Open the Jupyter notebook:
```bash
jupyter notebook embryoscope/report/explore/explore_image_availability.ipynb
```

## Data Statistics

**Total Embryos**: ~115,361
**Servers**: 4 (Belo Horizonte, Brasilia, Ibirapuera, Vila Mariana)

## Next Steps

1. ✅ **Test completed** - Connections verified for 4 servers
2. ⏳ **Ready to run** - Full extraction script is ready
3. 📊 **Analysis ready** - Exploration notebook created

## Notes

- Only processes embryos where `embryo_EmbryoID IS NOT NULL`
- Only processes servers that are enabled in `params.yml`
- Automatically handles authentication and token refresh
- Progress is logged every 100 embryos
- All logs saved to `embryoscope/report/logs/`
