# Missing PatientIDs Analysis by Date Report

**Date:** July 30, 2025  
**Analysis:** Missing PatientIDs split by activity date (before/after 2021-06)  
**Script:** `analyze_missing_patientids_by_date.py`

## Executive Summary

The analysis reveals that **5,736 PatientIDs** are missing from Clinisys, with a clear temporal distribution:

- **Before 2021-06:** 3,995 PatientIDs (69.6%)
- **After 2021-06:** 1,688 PatientIDs (29.4%)

## Key Findings

### Temporal Distribution
- **Cutoff Date:** June 1, 2021
- **Historical Patients:** 3,995 patients with last activity before 2021-06
- **Recent Patients:** 1,688 patients with activity after 2021-06
- **Date Range:** October 2017 to July 2025

### Before 2021-06 Analysis (3,995 PatientIDs)
- **Mean embryo count:** 10.0 embryos per patient
- **Median embryo count:** 8.0 embryos per patient
- **Date range:** 2017-10-24 to 2021-05-31
- **Location distribution:**
  - Ibirapuera: 2,266 (56.7%)
  - Vila Mariana: 666 (16.7%)
  - Belo Horizonte: 641 (16.0%)
  - Brasilia: 422 (10.6%)

### After 2021-06 Analysis (1,688 PatientIDs)
- **Mean embryo count:** 11.9 embryos per patient
- **Median embryo count:** 9.0 embryos per patient
- **Date range:** 2021-06-01 to 2025-07-29
- **Location distribution:**
  - Ibirapuera: 706 (41.8%)
  - Belo Horizonte: 407 (24.1%)
  - Vila Mariana: 348 (20.6%)
  - Brasilia: 227 (13.4%)

## Data Quality Insights

### Historical vs Recent Patterns
1. **Higher Activity in Recent Patients:** Recent patients (after 2021-06) have higher mean embryo counts (11.9 vs 10.0)
2. **Location Shift:** Ibirapuera dominance decreased from 56.7% to 41.8% in recent years
3. **Belo Horizonte Growth:** Increased from 16.0% to 24.1% in recent years
4. **Active Patients:** 1,688 patients with recent activity (up to July 2025) still lack clinisys records

### Business Implications
1. **Ongoing Data Integration Issue:** Recent patients (2021-2025) still missing clinisys records
2. **Location-Specific Problems:** Ibirapuera consistently has the highest missing rates
3. **Growing Problem:** 29.4% of missing patients are from recent years

## Files Generated

### Exports Directory (`data_lake_scripts/reports/exports/`)
1. **`missing_clinisys_before_2021_06_20250730_112352.csv`**
   - 3,995 PatientIDs with last activity before 2021-06
   - Columns: PatientID, PatientIDx, FirstName, LastName, Location, EmbryoCount, FirstDate, LastDate

2. **`missing_clinisys_after_2021_06_20250730_112352.csv`**
   - 1,688 PatientIDs with last activity after 2021-06
   - Columns: PatientID, PatientIDx, FirstName, LastName, Location, EmbryoCount, FirstDate, LastDate

3. **`missing_clinisys_complete_20250730_112352.csv`**
   - Complete list of 5,736 missing PatientIDs
   - Columns: PatientID, PatientIDx, FirstName, LastName, Location, EmbryoCount, FirstDate, LastDate

## Recommendations

### Immediate Actions
1. **Prioritize Recent Patients:** Focus on 1,688 patients with activity after 2021-06
2. **Investigate Ibirapuera Pipeline:** Address the highest missing rate location
3. **Review Data Integration:** Check why recent patients still lack clinisys records

### Long-term Solutions
1. **Implement Real-time Validation:** Prevent future missing records
2. **Cross-system Monitoring:** Automated alerts for missing clinisys records
3. **Data Quality Dashboard:** Track missing PatientIDs by location and time period

## Technical Details

### Comparison Columns Used
- **Embryoscope:** `patient_PatientID` (from `gold.embryoscope_embrioes`)
- **Clinisys:** `micro_prontuario` (from `gold.clinisys_embrioes`)

### Analysis Logic
1. Extract all embryoscope PatientIDs not present in clinisys
2. Get detailed information including last activity date
3. Split by cutoff date (2021-06-01)
4. Generate statistics and export files

---

**Analysis conducted by:** AI Assistant  
**Timestamp:** 2025-07-30 11:23:52  
**Data source:** Huntington Data Lake (gold layer)  
**Script location:** `data_lake_scripts/reports/analyze_missing_patientids_by_date.py` 