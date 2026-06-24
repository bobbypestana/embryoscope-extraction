# Data Lake Reconciliation Report: silver.view_embrioes_congelados vs. Athena silver_clinisys_staging.view_embrioes_congelados

> [!NOTE]
> ### 📊 Data Lake Ingestion & Promotion Quality KPI Dashboard
> * **Row Count Match Rate**: **99.934%** (94,208 Local vs 94,146 Target - **+62 difference**)
> * **Local Key Overlap Rate**: **99.899%**
> * **Target Key Overlap Rate**: **99.965%**
> * **Overall Value Alignment**: **N/A**


This report presents a generalized reconciliation audit between **silver.view_embrioes_congelados** and **Athena silver_clinisys_staging.view_embrioes_congelados** using automated keys verification.

---

## 1. Schema Comparison
* Local columns count: **65**
* Target columns count: **65**

### 1.1 Columns Only in Local
`extraction_timestamp`, `hash`

### 1.2 Columns Only in Target
`_dlt_id`, `bronze_updated_at`

---

## 2. Row Count & Volume Validation (Full Datasets)

| Table | Local Count | Target Count | Difference | Pct Diff | Status |
| :--- | :---: | :---: | :---: | :---: | :--- |
| **view_embrioes_congelados** | 94,208 | 94,146 | +62 | +0.07% | Variance Detected |

---

## 3. Key Overlap Summary (Joined on id)

* **Total Overlapping Keys**: **94,113**
* **Keys ONLY in Local**: **95**
* **Keys ONLY in Target**: **33**

---

## 4. Key Takeaways & Root Cause Analysis

### 4.1 Sample Records Only in Local (95 total)
| Id |
| :---: |
| 76950 |
| 87517 |
| 90947 |
| 91820 |
| 91821 |
| 92713 |
| 97222 |
| 87519 |
| 87734 |
| 88840 |
| 90053 |
| 90887 |
| 91668 |
| 20729 |
| 77790 |

### 4.2 Sample Records Only in Target (33 total)
| Id |
| :---: |
| 99536 |
| 99537 |
| 99538 |
| 99539 |
| 99540 |
| 99541 |
| 99542 |
| 99543 |
| 99544 |
| 99545 |
| 99546 |
| 99547 |
| 99548 |
| 99549 |
| 99550 |


---

## 5. Actionable Recommendations
1. **Deduplicate / Trace Gaps**: Inspect missing records in either local or remote target databases using the sample keys above.
2. **Handle Schema Drift**: If column mismatches exist, update downstream mapping logic or execute database alter statements.
