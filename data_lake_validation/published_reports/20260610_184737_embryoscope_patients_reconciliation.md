# Data Lake Reconciliation Report: silver_embryoscope.patients vs. Athena silver_embryoscope_staging.patients

> [!NOTE]
> ### 📊 Data Lake Ingestion & Promotion Quality KPI Dashboard
> * **Row Count Match Rate**: **95.939%** (13,555 Local vs 13,026 Target - **+529 difference**)
> * **Local Key Overlap Rate**: **95.160%**
> * **Target Key Overlap Rate**: **99.025%**
> * **Overall Value Alignment**: **N/A**


This report presents a generalized reconciliation audit between **silver_embryoscope.patients** and **Athena silver_embryoscope_staging.patients** using automated keys verification.

---

## 1. Schema Comparison
* Local columns count: **11**
* Target columns count: **9**

### 1.1 Columns Only in Local
`DateOfBirth`, `FirstName`, `LastName`, `PatientID`, `PatientIDx`, `_extraction_timestamp`, `_location`, `_row_hash`, `_run_id`, `prontuario`, `unit_huntington`

### 1.2 Columns Only in Target
`_dlt_id`, `bronze_updated_at`, `date_of_birth`, `first_name`, `last_name`, `patient_id`, `patient_id_x`, `patient_sk`, `source_server`

---

## 2. Row Count & Volume Validation (Full Datasets)

| Table | Local Count | Target Count | Difference | Pct Diff | Status |
| :--- | :---: | :---: | :---: | :---: | :--- |
| **patients** | 13,555 | 13,026 | +529 | +4.06% | Variance Detected |

---

## 3. Key Overlap Summary (Joined on PatientIDx)

* **Total Overlapping Keys**: **12,899**
* **Keys ONLY in Local**: **656**
* **Keys ONLY in Target**: **127**

---

## 4. Key Takeaways & Root Cause Analysis

### 4.1 Sample Records Only in Local (656 total)
| Patientidx |
| :---: |
| PC1P7BHG_[PHONE_REDACTED]3 |
| PC1P7BHG_[PHONE_REDACTED]0 |
| PC1P7BHG_[PHONE_REDACTED]0 |
| PC1P7BHG_[PHONE_REDACTED]4 |
| PC1P7BHG_[PHONE_REDACTED]5 |
| PC1P7BHG_[PHONE_REDACTED]2 |
| PC1P7BHG_[PHONE_REDACTED]1 |
| PC1P7BHG_[PHONE_REDACTED]4 |
| PC1P7BHG_[PHONE_REDACTED]1 |
| PC1P7BHG_[PHONE_REDACTED]6 |
| PC1P7BHG_[PHONE_REDACTED]7 |
| PC1P7BHG_[PHONE_REDACTED]0 |
| PC1P7BHG_[PHONE_REDACTED]4 |
| PC1P7BHG_[PHONE_REDACTED]2 |
| PC1P7BHG_[PHONE_REDACTED]7 |

### 4.2 Sample Records Only in Target (127 total)
| Patient_Id_X |
| :---: |
| PC1PPEMH_[PHONE_REDACTED]9 |
| PC1PPEMH_[PHONE_REDACTED]3 |
| PC1PPEMH_[PHONE_REDACTED]7 |
| PC1R85KM_[PHONE_REDACTED]9 |
| PC1P7BHG_[PHONE_REDACTED]9 |
| PC1P7BHG_[PHONE_REDACTED]6 |
| S4JP1389_[PHONE_REDACTED]1 |
| PC1PPEMH_[PHONE_REDACTED]4 |
| PC1PPEMH_[PHONE_REDACTED]9 |
| PC1PPEMH_[PHONE_REDACTED]1 |
| PC1PPEMH_[PHONE_REDACTED]2 |
| PC1R85KM_[PHONE_REDACTED]6 |
| PC1R85KM_[PHONE_REDACTED]3 |
| PC1R85KM_[PHONE_REDACTED]5 |
| PC1R85KM_[PHONE_REDACTED]9 |


---

## 5. Actionable Recommendations
1. **Deduplicate / Trace Gaps**: Inspect missing records in either local or remote target databases using the sample keys above.
2. **Handle Schema Drift**: If column mismatches exist, update downstream mapping logic or execute database alter statements.
