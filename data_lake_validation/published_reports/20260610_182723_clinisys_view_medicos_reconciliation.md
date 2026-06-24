# Data Lake Reconciliation Report: silver.view_medicos vs. Athena silver_clinisys_staging.view_medicos

> [!NOTE]
> ### 📊 Data Lake Ingestion & Promotion Quality KPI Dashboard
> * **Row Count Match Rate**: **99.912%** (2,261 Local vs 2,263 Target - **-2 difference**)
> * **Local Key Overlap Rate**: **100.000%**
> * **Target Key Overlap Rate**: **99.912%**
> * **Overall Value Alignment**: **N/A**


This report presents a generalized reconciliation audit between **silver.view_medicos** and **Athena silver_clinisys_staging.view_medicos** using automated keys verification.

---

## 1. Schema Comparison
* Local columns count: **9**
* Target columns count: **9**

### 1.1 Columns Only in Local
`extraction_timestamp`, `hash`

### 1.2 Columns Only in Target
`_dlt_id`, `bronze_updated_at`

---

## 2. Row Count & Volume Validation (Full Datasets)

| Table | Local Count | Target Count | Difference | Pct Diff | Status |
| :--- | :---: | :---: | :---: | :---: | :--- |
| **view_medicos** | 2,261 | 2,263 | -2 | -0.09% | Variance Detected |

---

## 3. Key Overlap Summary (Joined on id)

* **Total Overlapping Keys**: **2,261**
* **Keys ONLY in Local**: **0**
* **Keys ONLY in Target**: **2**

---

## 4. Key Takeaways & Root Cause Analysis

### 4.2 Sample Records Only in Target (2 total)
| Id |
| :---: |
| 2432 |
| 2433 |


---

## 5. Actionable Recommendations
1. **Deduplicate / Trace Gaps**: Inspect missing records in either local or remote target databases using the sample keys above.
2. **Handle Schema Drift**: If column mismatches exist, update downstream mapping logic or execute database alter statements.
