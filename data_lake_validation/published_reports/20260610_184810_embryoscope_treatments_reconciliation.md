# Data Lake Reconciliation Report: silver_embryoscope.treatments vs. Athena silver_embryoscope_staging.treatments

> [!NOTE]
> ### 📊 Data Lake Ingestion & Promotion Quality KPI Dashboard
> * **Row Count Match Rate**: **95.715%** (23,024 Local vs 22,078 Target - **+946 difference**)
> * **Local Key Overlap Rate**: **94.957%**
> * **Target Key Overlap Rate**: **99.026%**
> * **Overall Value Alignment**: **N/A**


This report presents a generalized reconciliation audit between **silver_embryoscope.treatments** and **Athena silver_embryoscope_staging.treatments** using automated keys verification.

---

## 1. Schema Comparison
* Local columns count: **7**
* Target columns count: **6**

### 1.1 Columns Only in Local
`PatientIDx`, `TreatmentName`, `_extraction_timestamp`, `_location`, `_row_hash`, `_run_id`, `unit_huntington`

### 1.2 Columns Only in Target
`_dlt_id`, `bronze_updated_at`, `patient_id_x`, `source_server`, `treatment_name`, `treatment_sk`

---

## 2. Row Count & Volume Validation (Full Datasets)

| Table | Local Count | Target Count | Difference | Pct Diff | Status |
| :--- | :---: | :---: | :---: | :---: | :--- |
| **treatments** | 23,024 | 22,078 | +946 | +4.28% | Variance Detected |

---

## 3. Key Overlap Summary (Joined on PatientIDx, TreatmentName)

* **Total Overlapping Keys**: **21,863**
* **Keys ONLY in Local**: **1,161**
* **Keys ONLY in Target**: **215**

---

## 4. Key Takeaways & Root Cause Analysis

### 4.1 Sample Records Only in Local (1,161 total)
| Patientidx | Treatmentname |
| :---: | :---: |
| NEXTGEN_[PHONE_REDACTED]0 | 2025-2153 |
| PC1P7BHG_[PHONE_REDACTED]6 |  |
| PC1P7BHG_[PHONE_REDACTED]6 | 2025-472 |
| PC1P7BHG_[PHONE_REDACTED]1 | 2025-1457 |
| PC1P7BHG_[PHONE_REDACTED]1 | 2025-273 |
| PC1P7BHG_[PHONE_REDACTED]5 |  |
| PC1P7BHG_[PHONE_REDACTED]5 | 2025-700 |
| PC1P7BHG_[PHONE_REDACTED]8 | 2025-2539 |
| PC1P7BHG_[PHONE_REDACTED]5 | 2025-1192 |
| PC1P7BHG_[PHONE_REDACTED]8 | 2025-819 |
| PC1P7BHG_[PHONE_REDACTED]6 |  |
| PC1P7BHG_[PHONE_REDACTED]6 | 2025-339 |
| PC1P7BHG_[PHONE_REDACTED]7 | 2025-1896 |
| PC1P7BHG_[PHONE_REDACTED]3 |  |
| PC1P7BHG_[PHONE_REDACTED]3 | 2025-381 |

### 4.2 Sample Records Only in Target (215 total)
| Patient_Id_X | Treatment_Name |
| :---: | :---: |
| S4JP1389_[PHONE_REDACTED]5 | 06/06/2026 |
| S4JP1389_[PHONE_REDACTED]5 | 29/05/2026 |
| S4JP1389_[PHONE_REDACTED]2 | 01/06/2026 |
| S4JP1389_[PHONE_REDACTED]7 | 03/06/2026 |
| S4JP1389_[PHONE_REDACTED]2 | 03/06/2026 |
| S4JP1389_[PHONE_REDACTED]7 | 04/06/2026 |
| S4JP1389_[PHONE_REDACTED]6 | 04/06/2026 |
| S4JP1389_[PHONE_REDACTED]9 | 05/06/2026 |
| S4JP1389_[PHONE_REDACTED]2 | 06/06/2026 |
| PC1PPEMH_[PHONE_REDACTED]4 | 29-05-2026 |
| PC1PPEMH_[PHONE_REDACTED]1 | 29/05/2026 |
| PC1PPEMH_[PHONE_REDACTED]9 | 30/05/2026 |
| PC1PPEMH_[PHONE_REDACTED]9 | 02/06/2026 |
| PC1PPEMH_[PHONE_REDACTED]5 | 03/06/2026 |
| PC1PPEMH_[PHONE_REDACTED]8 | 03/06/2026 |


---

## 5. Actionable Recommendations
1. **Deduplicate / Trace Gaps**: Inspect missing records in either local or remote target databases using the sample keys above.
2. **Handle Schema Drift**: If column mismatches exist, update downstream mapping logic or execute database alter statements.
