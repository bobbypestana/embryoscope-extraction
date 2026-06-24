# Data Lake Reconciliation Report: silver.view_ovulos_congelados vs. Athena silver_clinisys_staging.view_ovulos_congelados

> [!NOTE]
> ### 📊 Data Lake Ingestion & Promotion Quality KPI Dashboard
> * **Row Count Match Rate**: **99.931%** (112,705 Local vs 112,627 Target - **+78 difference**)
> * **Local Key Overlap Rate**: **99.874%**
> * **Target Key Overlap Rate**: **99.943%**
> * **Overall Value Alignment**: **N/A**


This report presents a generalized reconciliation audit between **silver.view_ovulos_congelados** and **Athena silver_clinisys_staging.view_ovulos_congelados** using automated keys verification.

---

## 1. Schema Comparison
* Local columns count: **28**
* Target columns count: **28**

### 1.1 Columns Only in Local
`extraction_timestamp`, `hash`

### 1.2 Columns Only in Target
`_dlt_id`, `bronze_updated_at`

---

## 2. Row Count & Volume Validation (Full Datasets)

| Table | Local Count | Target Count | Difference | Pct Diff | Status |
| :--- | :---: | :---: | :---: | :---: | :--- |
| **view_ovulos_congelados** | 112,705 | 112,627 | +78 | +0.07% | Variance Detected |

---

## 3. Key Overlap Summary (Joined on id)

* **Total Overlapping Keys**: **112,563**
* **Keys ONLY in Local**: **142**
* **Keys ONLY in Target**: **64**

---

## 4. Key Takeaways & Root Cause Analysis

### 4.1 Sample Records Only in Local (142 total)
| Id |
| :---: |
| 100243 |
| 100254 |
| 100255 |
| 100256 |
| 112499 |
| 100234 |
| 100238 |
| 101091 |
| 103894 |
| 111499 |
| 111501 |
| 86654 |
| 90797 |
| 90799 |
| 91673 |

### 4.2 Sample Records Only in Target (64 total)
| Id |
| :---: |
| 119517 |
| 119518 |
| 119519 |
| 119520 |
| 119521 |
| 119522 |
| 119523 |
| 119524 |
| 119525 |
| 119526 |
| 119527 |
| 119528 |
| 119529 |
| 119530 |
| 119531 |


---

## 5. Actionable Recommendations
1. **Deduplicate / Trace Gaps**: Inspect missing records in either local or remote target databases using the sample keys above.
2. **Handle Schema Drift**: If column mismatches exist, update downstream mapping logic or execute database alter statements.
