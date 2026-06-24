# Data Lake Reconciliation Report: silver.view_congelamentos_ovulos vs. Athena silver_clinisys_staging.view_congelamentos_ovulos

> [!NOTE]
> ### 📊 Data Lake Ingestion & Promotion Quality KPI Dashboard
> * **Row Count Match Rate**: **99.910%** (12,254 Local vs 12,243 Target - **+11 difference**)
> * **Local Key Overlap Rate**: **99.837%**
> * **Target Key Overlap Rate**: **99.926%**
> * **Overall Value Alignment**: **N/A**


This report presents a generalized reconciliation audit between **silver.view_congelamentos_ovulos** and **Athena silver_clinisys_staging.view_congelamentos_ovulos** using automated keys verification.

---

## 1. Schema Comparison
* Local columns count: **30**
* Target columns count: **30**

### 1.1 Columns Only in Local
`BiologoFIV`, `BiologoFIV2`, `BiologoResponsavel`, `CodCongelamento`, `NOvulos`, `NPailletes`, `extraction_timestamp`, `hash`

### 1.2 Columns Only in Target
`_dlt_id`, `biologo_fiv`, `biologo_fiv2`, `biologo_responsavel`, `bronze_updated_at`, `cod_congelamento`, `n_ovulos`, `n_pailletes`

---

## 2. Row Count & Volume Validation (Full Datasets)

| Table | Local Count | Target Count | Difference | Pct Diff | Status |
| :--- | :---: | :---: | :---: | :---: | :--- |
| **view_congelamentos_ovulos** | 12,254 | 12,243 | +11 | +0.09% | Variance Detected |

---

## 3. Key Overlap Summary (Joined on id)

* **Total Overlapping Keys**: **12,234**
* **Keys ONLY in Local**: **20**
* **Keys ONLY in Target**: **9**

---

## 4. Key Takeaways & Root Cause Analysis

### 4.1 Sample Records Only in Local (20 total)
| Id |
| :---: |
| 12160 |
| 13348 |
| 14371 |
| 14058 |
| 7719 |
| 13162 |
| 14344 |
| 11883 |
| 13995 |
| 15521 |
| 8476 |
| 7670 |
| 15218 |
| 13970 |
| 15302 |

### 4.2 Sample Records Only in Target (9 total)
| Id |
| :---: |
| 15946 |
| 15947 |
| 15948 |
| 15949 |
| 15950 |
| 15951 |
| 15952 |
| 15953 |
| 15954 |


---

## 5. Actionable Recommendations
1. **Deduplicate / Trace Gaps**: Inspect missing records in either local or remote target databases using the sample keys above.
2. **Handle Schema Drift**: If column mismatches exist, update downstream mapping logic or execute database alter statements.
