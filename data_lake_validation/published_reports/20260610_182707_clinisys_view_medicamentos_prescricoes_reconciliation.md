# Data Lake Reconciliation Report: silver.view_medicamentos_prescricoes vs. Athena silver_clinisys_staging.view_medicamentos_prescricoes

> [!NOTE]
> ### 📊 Data Lake Ingestion & Promotion Quality KPI Dashboard
> * **Row Count Match Rate**: **49.973%** (203,323 Local vs 135,524 Target - **+67,799 difference**)
> * **Local Key Overlap Rate**: **66.389%**
> * **Target Key Overlap Rate**: **99.602%**
> * **Overall Value Alignment**: **N/A**


This report presents a generalized reconciliation audit between **silver.view_medicamentos_prescricoes** and **Athena silver_clinisys_staging.view_medicamentos_prescricoes** using automated keys verification.

---

## 1. Schema Comparison
* Local columns count: **25**
* Target columns count: **25**

### 1.1 Columns Only in Local
`extraction_timestamp`, `hash`

### 1.2 Columns Only in Target
`_dlt_id`, `bronze_updated_at`

---

## 2. Row Count & Volume Validation (Full Datasets)

| Table | Local Count | Target Count | Difference | Pct Diff | Status |
| :--- | :---: | :---: | :---: | :---: | :--- |
| **view_medicamentos_prescricoes** | 203,323 | 135,524 | +67,799 | +50.03% | Variance Detected |

---

## 3. Key Overlap Summary (Joined on id)

* **Total Overlapping Keys**: **134,984**
* **Keys ONLY in Local**: **68,339**
* **Keys ONLY in Target**: **540**

---

## 4. Key Takeaways & Root Cause Analysis

### 4.1 Sample Records Only in Local (68,339 total)
| Id |
| :---: |
| 811780 |
| 811832 |
| 811860 |
| 811948 |
| 811982 |
| 812016 |
| 812074 |
| 812137 |
| 812168 |
| 812222 |
| 812264 |
| 812335 |
| 812382 |
| 812427 |
| 812434 |

### 4.2 Sample Records Only in Target (540 total)
| Id |
| :---: |
| 1048022 |
| 1048023 |
| 1048024 |
| 1048025 |
| 1048026 |
| 1048027 |
| 1048028 |
| 1048029 |
| 1048030 |
| 1048031 |
| 1048032 |
| 1048033 |
| 1048034 |
| 1048043 |
| 1048044 |


---

## 5. Actionable Recommendations
1. **Deduplicate / Trace Gaps**: Inspect missing records in either local or remote target databases using the sample keys above.
2. **Handle Schema Drift**: If column mismatches exist, update downstream mapping logic or execute database alter statements.
