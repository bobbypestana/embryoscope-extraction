# Data Lake Reconciliation Report: silver.view_unidades vs. Athena silver_clinisys_staging.view_unidades

> [!NOTE]
> ### 📊 Data Lake Ingestion & Promotion Quality KPI Dashboard
> * **Row Count Match Rate**: **72.727%** (14 Local vs 11 Target - **+3 difference**)
> * **Local Key Overlap Rate**: **78.571%**
> * **Target Key Overlap Rate**: **100.000%**
> * **Overall Value Alignment**: **N/A**


This report presents a generalized reconciliation audit between **silver.view_unidades** and **Athena silver_clinisys_staging.view_unidades** using automated keys verification.

---

## 1. Schema Comparison
* Local columns count: **4**
* Target columns count: **4**

### 1.1 Columns Only in Local
`extraction_timestamp`, `hash`

### 1.2 Columns Only in Target
`_dlt_id`, `bronze_updated_at`

---

## 2. Row Count & Volume Validation (Full Datasets)

| Table | Local Count | Target Count | Difference | Pct Diff | Status |
| :--- | :---: | :---: | :---: | :---: | :--- |
| **view_unidades** | 14 | 11 | +3 | +27.27% | Variance Detected |

---

## 3. Key Overlap Summary (Joined on id)

* **Total Overlapping Keys**: **11**
* **Keys ONLY in Local**: **3**
* **Keys ONLY in Target**: **0**

---

## 4. Key Takeaways & Root Cause Analysis

### 4.1 Sample Records Only in Local (3 total)
| Id |
| :---: |
| 12 |
| 16 |
| 17 |


---

## 5. Actionable Recommendations
1. **Deduplicate / Trace Gaps**: Inspect missing records in either local or remote target databases using the sample keys above.
2. **Handle Schema Drift**: If column mismatches exist, update downstream mapping logic or execute database alter statements.
