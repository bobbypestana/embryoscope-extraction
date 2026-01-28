# Embryo Image Availability Report

This folder contains scripts to check image availability for embryos stored in the database.

## Overview

The script queries embryo data from `gold.embryoscope_embrioes` and verifies if images are available on each Embryoscope server via the API.

## Script: `01_check_image_availability.py`

### What it does:

1. **Queries embryo data** from `gold.embryoscope_embrioes` with the following columns:
   - `prontuario`
   - `patient_PatientID`
   - `patient_PatientIDx`
   - `patient_unit_huntington` (server identifier)
   - `treatment_TreatmentName`
   - `embryo_EmbryoID`
   - `embryo_EmbryoDate`

2. **Groups embryos by server** (`patient_unit_huntington`)

3. **Checks image availability** for each embryo by calling the API endpoint `GET/imageruns`
   - Returns response status (success, no_response, error, etc.)
   - Counts number of image runs available
   - Marks embryo as having images if `image_runs_count > 0`

4. **Handles connection issues**:
   - Automatic re-authentication if token expires
   - Retry logic (up to 3 attempts per embryo)
   - Token refresh every 1000 embryos

5. **Respects rate limits**: 10 requests/second (0.1s delay between requests)

6. **Saves results** to `gold.embryo_image_availability_raw` table (raw format with all original columns + status)

### Output Table: `gold.embryo_image_availability_raw`

This table preserves all original columns from `gold.embryoscope_embrioes` and adds API check status:

**Original Columns:**
- `prontuario` - Patient record number
- `patient_PatientID` - Patient ID
- `patient_PatientIDx` - Patient IDx
- `patient_unit_huntington` - Server/unit name
- `treatment_TreatmentName` - Treatment name
- `embryo_EmbryoID` - Embryo ID
- `embryo_EmbryoDate` - Embryo date

**Status Columns (added by this script):**
- `image_available` (BOOLEAN) - Whether images are available
- `image_runs_count` (INTEGER) - Number of image runs found
- `api_response_status` (VARCHAR) - API response status (success, no_response, error, etc.)
- `error_message` (VARCHAR) - Error message if any
- `checked_at` (TIMESTAMP) - When the check was performed


### Usage:

```bash
# From the embryoscope/ directory
conda activate try_request
python report/01_check_image_availability.py
```

### Configuration:

The script uses the existing `embryoscope/params.yml` configuration file for:
- Server credentials (IP, login, password)
- Enabled/disabled servers
- Rate limiting settings

### Logs:

Logs are saved to `report/logs/image_availability_YYYYMMDD_HHMMSS.log`

### Summary Output:

After completion, the script prints a summary table showing:
- Total embryos per server
- Embryos with images
- Embryos without images
- Percentage with images

## Notes:

- Only processes embryos where `embryo_EmbryoID IS NOT NULL`
- Only processes servers that are enabled in `params.yml`
- Automatically handles authentication and token refresh
- Progress is logged every 100 embryos
