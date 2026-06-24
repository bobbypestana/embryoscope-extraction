# Data Lake Reconciliation Report: silver.view_procedimentos_financas vs. Athena silver_clinisys_staging.view_procedimentos_financas

> [!NOTE]
> ### 📊 Data Lake Ingestion & Promotion Quality KPI Dashboard
> * **Row Count Match Rate**: **87.113%** (507 Local vs 582 Target - **-75 difference**)
> * **Local Key Overlap Rate**: **100.000%**
> * **Target Key Overlap Rate**: **87.113%**
> * **Overall Value Alignment**: **0.000%**


This report presents a generalized reconciliation audit between **silver.view_procedimentos_financas** and **Athena silver_clinisys_staging.view_procedimentos_financas** using automated keys verification.

---

## 1. Schema Comparison
* Local columns count: **6**
* Target columns count: **9**

### 1.1 Columns Only in Local
`extraction_timestamp`, `hash`

### 1.2 Columns Only in Target
`_dlt_id`, `bronze_updated_at`, `categoria`, `descricao`, `exibir_indicacao`

---

## 2. Row Count & Volume Validation (Full Datasets)

| Table | Local Count | Target Count | Difference | Pct Diff | Status |
| :--- | :---: | :---: | :---: | :---: | :--- |
| **view_procedimentos_financas** | 507 | 582 | -75 | -12.89% | Variance Detected |

---

## 3. Key Overlap Summary (Joined on id)

* **Total Overlapping Keys**: **507**
* **Keys ONLY in Local**: **0**
* **Keys ONLY in Target**: **75**

### 3.1 Overlap Value Comparison
| Column Name | Local Sum | Target Sum | Difference | Alignment Rate |
| :--- | :---: | :---: | :---: | :---: |
| **valor** | 0.00 | 28607.00 | -28607.00 | 0.0000% |

---

## 4. Key Takeaways & Root Cause Analysis

### 4.2 Sample Records Only in Target (75 total)
| Id | Valor |
| :---: | :---: |
| 765377 | nan |
| 765378 | nan |
| 765379 | nan |
| 765380 | nan |
| 765381 | nan |
| 765382 | nan |
| 765383 | nan |
| 765384 | nan |
| 765385 | nan |
| 765386 | nan |
| 765388 | nan |
| 765389 | nan |
| 765390 | nan |
| 765391 | nan |
| 765392 | nan |


---

## 5. Actionable Recommendations
1. **Deduplicate / Trace Gaps**: Inspect missing records in either local or remote target databases using the sample keys above.
2. **Handle Schema Drift**: If column mismatches exist, update downstream mapping logic or execute database alter statements.
