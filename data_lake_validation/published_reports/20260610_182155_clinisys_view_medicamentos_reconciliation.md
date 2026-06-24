# Data Lake Reconciliation Report: silver.view_medicamentos vs. Athena silver_clinisys_staging.view_medicamentos

> [!NOTE]
> ### 📊 Data Lake Ingestion & Promotion Quality KPI Dashboard
> * **Row Count Match Rate**: **100.000%** (244 Local vs 244 Target - **+0 difference**)
> * **Local Key Overlap Rate**: **100.000%**
> * **Target Key Overlap Rate**: **100.000%**
> * **Overall Value Alignment**: **N/A**


This report presents a generalized reconciliation audit between **silver.view_medicamentos** and **Athena silver_clinisys_staging.view_medicamentos** using automated keys verification.

---

## 1. Schema Comparison
* Local columns count: **7**
* Target columns count: **7**

### 1.1 Columns Only in Local
`extraction_timestamp`, `hash`

### 1.2 Columns Only in Target
`_dlt_id`, `bronze_updated_at`

---

## 2. Row Count & Volume Validation (Full Datasets)

| Table | Local Count | Target Count | Difference | Pct Diff | Status |
| :--- | :---: | :---: | :---: | :---: | :--- |
| **view_medicamentos** | 244 | 244 | +0 | +0.00% | Perfect Match |

---

## 3. Key Overlap Summary (Joined on id)

* **Total Overlapping Keys**: **244**
* **Keys ONLY in Local**: **0**
* **Keys ONLY in Target**: **0**

---

## 4. Key Takeaways & Root Cause Analysis


---

## 5. Actionable Recommendations
1. **Deduplicate / Trace Gaps**: Inspect missing records in either local or remote target databases using the sample keys above.
2. **Handle Schema Drift**: If column mismatches exist, update downstream mapping logic or execute database alter statements.
