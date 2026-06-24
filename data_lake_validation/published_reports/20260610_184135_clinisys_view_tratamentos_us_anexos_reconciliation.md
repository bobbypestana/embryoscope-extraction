# Data Lake Reconciliation Report: silver.view_tratamentos_us_anexos vs. Athena silver_clinisys_staging.view_tratamentos_us_anexos

> [!NOTE]
> ### 📊 Data Lake Ingestion & Promotion Quality KPI Dashboard
> * **Row Count Match Rate**: **98.475%** (345,858 Local vs 351,215 Target - **-5,357 difference**)
> * **Local Key Overlap Rate**: **100.000%**
> * **Target Key Overlap Rate**: **98.475%**
> * **Overall Value Alignment**: **N/A**


This report presents a generalized reconciliation audit between **silver.view_tratamentos_us_anexos** and **Athena silver_clinisys_staging.view_tratamentos_us_anexos** using automated keys verification.

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
| **view_tratamentos_us_anexos** | 345,858 | 351,215 | -5,357 | -1.53% | Variance Detected |

---

## 3. Key Overlap Summary (Joined on id)

* **Total Overlapping Keys**: **345,858**
* **Keys ONLY in Local**: **0**
* **Keys ONLY in Target**: **5,357**

---

## 4. Key Takeaways & Root Cause Analysis

### 4.2 Sample Records Only in Target (5,357 total)
| Id |
| :---: |
| 358391 |
| 358392 |
| 358393 |
| 358394 |
| 358395 |
| 358396 |
| 358397 |
| 358398 |
| 358399 |
| 358400 |
| 358401 |
| 358402 |
| 358403 |
| 358404 |
| 358405 |


---

## 5. Actionable Recommendations
1. **Deduplicate / Trace Gaps**: Inspect missing records in either local or remote target databases using the sample keys above.
2. **Handle Schema Drift**: If column mismatches exist, update downstream mapping logic or execute database alter statements.
