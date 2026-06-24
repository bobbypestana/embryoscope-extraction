# Data Lake Reconciliation Report: silver.view_congelamentos_embrioes vs. Athena silver_clinisys_staging.view_congelamentos_embrioes

> [!NOTE]
> ### 📊 Data Lake Ingestion & Promotion Quality KPI Dashboard
> * **Row Count Match Rate**: **99.961%** (28,509 Local vs 28,498 Target - **+11 difference**)
> * **Local Key Overlap Rate**: **99.926%**
> * **Target Key Overlap Rate**: **99.965%**
> * **Overall Value Alignment**: **N/A**


This report presents a generalized reconciliation audit between **silver.view_congelamentos_embrioes** and **Athena silver_clinisys_staging.view_congelamentos_embrioes** using automated keys verification.

---

## 1. Schema Comparison
* Local columns count: **47**
* Target columns count: **47**

### 1.1 Columns Only in Local
`BiologoFIV`, `BiologoFIV2`, `BiologoResponsavel`, `CicloRecongelamento`, `CodCongelamento`, `NEmbrioes`, `NPailletes`, `extraction_timestamp`, `hash`

### 1.2 Columns Only in Target
`_dlt_id`, `biologo_fiv`, `biologo_fiv2`, `biologo_responsavel`, `bronze_updated_at`, `ciclo_recongelamento`, `cod_congelamento`, `n_embrioes`, `n_pailletes`

---

## 2. Row Count & Volume Validation (Full Datasets)

| Table | Local Count | Target Count | Difference | Pct Diff | Status |
| :--- | :---: | :---: | :---: | :---: | :--- |
| **view_congelamentos_embrioes** | 28,509 | 28,498 | +11 | +0.04% | Variance Detected |

---

## 3. Key Overlap Summary (Joined on id)

* **Total Overlapping Keys**: **28,488**
* **Keys ONLY in Local**: **21**
* **Keys ONLY in Target**: **10**

---

## 4. Key Takeaways & Root Cause Analysis

### 4.1 Sample Records Only in Local (21 total)
| Id |
| :---: |
| 26896 |
| 30399 |
| 29451 |
| 31763 |
| 32609 |
| 30012 |
| 30090 |
| 26631 |
| 30884 |
| 6850 |
| 30416 |
| 16033 |
| 32100 |
| 32140 |
| 29933 |

### 4.2 Sample Records Only in Target (10 total)
| Id |
| :---: |
| 32748 |
| 32749 |
| 32750 |
| 32751 |
| 32752 |
| 32753 |
| 32754 |
| 32755 |
| 32756 |
| 32757 |


---

## 5. Actionable Recommendations
1. **Deduplicate / Trace Gaps**: Inspect missing records in either local or remote target databases using the sample keys above.
2. **Handle Schema Drift**: If column mismatches exist, update downstream mapping logic or execute database alter statements.
