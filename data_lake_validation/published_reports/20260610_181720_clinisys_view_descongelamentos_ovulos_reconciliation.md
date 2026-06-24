# Data Lake Reconciliation Report: silver.view_descongelamentos_ovulos vs. Athena silver_clinisys_staging.view_descongelamentos_ovulos

> [!NOTE]
> ### 📊 Data Lake Ingestion & Promotion Quality KPI Dashboard
> * **Row Count Match Rate**: **99.949%** (3,935 Local vs 3,933 Target - **+2 difference**)
> * **Local Key Overlap Rate**: **99.924%**
> * **Target Key Overlap Rate**: **99.975%**
> * **Overall Value Alignment**: **N/A**


This report presents a generalized reconciliation audit between **silver.view_descongelamentos_ovulos** and **Athena silver_clinisys_staging.view_descongelamentos_ovulos** using automated keys verification.

---

## 1. Schema Comparison
* Local columns count: **19**
* Target columns count: **19**

### 1.1 Columns Only in Local
`BiologoFIV`, `BiologoFIV2`, `CodCongelamento`, `CodDescongelamento`, `DataCongelamento`, `DataDescongelamento`, `PailletesDescongeladas`, `extraction_timestamp`, `hash`

### 1.2 Columns Only in Target
`_dlt_id`, `biologo_fiv`, `biologo_fiv2`, `bronze_updated_at`, `cod_congelamento`, `cod_descongelamento`, `data_congelamento`, `data_descongelamento`, `pailletes_descongeladas`

---

## 2. Row Count & Volume Validation (Full Datasets)

| Table | Local Count | Target Count | Difference | Pct Diff | Status |
| :--- | :---: | :---: | :---: | :---: | :--- |
| **view_descongelamentos_ovulos** | 3,935 | 3,933 | +2 | +0.05% | Variance Detected |

---

## 3. Key Overlap Summary (Joined on id)

* **Total Overlapping Keys**: **3,932**
* **Keys ONLY in Local**: **3**
* **Keys ONLY in Target**: **1**

---

## 4. Key Takeaways & Root Cause Analysis

### 4.1 Sample Records Only in Local (3 total)
| Id |
| :---: |
| 4556 |
| 4929 |
| 4891 |

### 4.2 Sample Records Only in Target (1 total)
| Id |
| :---: |
| 5529 |


---

## 5. Actionable Recommendations
1. **Deduplicate / Trace Gaps**: Inspect missing records in either local or remote target databases using the sample keys above.
2. **Handle Schema Drift**: If column mismatches exist, update downstream mapping logic or execute database alter statements.
