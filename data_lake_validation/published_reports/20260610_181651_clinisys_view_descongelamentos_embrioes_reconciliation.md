# Data Lake Reconciliation Report: silver.view_descongelamentos_embrioes vs. Athena silver_clinisys_staging.view_descongelamentos_embrioes

> [!NOTE]
> ### 📊 Data Lake Ingestion & Promotion Quality KPI Dashboard
> * **Row Count Match Rate**: **99.949%** (17,506 Local vs 17,497 Target - **+9 difference**)
> * **Local Key Overlap Rate**: **99.914%**
> * **Target Key Overlap Rate**: **99.966%**
> * **Overall Value Alignment**: **N/A**


This report presents a generalized reconciliation audit between **silver.view_descongelamentos_embrioes** and **Athena silver_clinisys_staging.view_descongelamentos_embrioes** using automated keys verification.

---

## 1. Schema Comparison
* Local columns count: **34**
* Target columns count: **34**

### 1.1 Columns Only in Local
`BiologoFIV`, `BiologoFIV2`, `CodCongelamento`, `CodDescongelamento`, `DataCongelamento`, `DataDescongelamento`, `DataTransferencia`, `PailletesDescongeladas`, `extraction_timestamp`, `hash`

### 1.2 Columns Only in Target
`_dlt_id`, `biologo_fiv`, `biologo_fiv2`, `bronze_updated_at`, `cod_congelamento`, `cod_descongelamento`, `data_congelamento`, `data_descongelamento`, `data_transferencia`, `pailletes_descongeladas`

---

## 2. Row Count & Volume Validation (Full Datasets)

| Table | Local Count | Target Count | Difference | Pct Diff | Status |
| :--- | :---: | :---: | :---: | :---: | :--- |
| **view_descongelamentos_embrioes** | 17,506 | 17,497 | +9 | +0.05% | Variance Detected |

---

## 3. Key Overlap Summary (Joined on id)

* **Total Overlapping Keys**: **17,491**
* **Keys ONLY in Local**: **15**
* **Keys ONLY in Target**: **6**

---

## 4. Key Takeaways & Root Cause Analysis

### 4.1 Sample Records Only in Local (15 total)
| Id |
| :---: |
| 18291 |
| 19804 |
| 20341 |
| 19353 |
| 19418 |
| 16149 |
| 19202 |
| 20042 |
| 18480 |
| 18503 |
| 19412 |
| 19459 |
| 19494 |
| 17576 |
| 18373 |

### 4.2 Sample Records Only in Target (6 total)
| Id |
| :---: |
| 21253 |
| 21254 |
| 21255 |
| 21256 |
| 21257 |
| 21258 |


---

## 5. Actionable Recommendations
1. **Deduplicate / Trace Gaps**: Inspect missing records in either local or remote target databases using the sample keys above.
2. **Handle Schema Drift**: If column mismatches exist, update downstream mapping logic or execute database alter statements.
