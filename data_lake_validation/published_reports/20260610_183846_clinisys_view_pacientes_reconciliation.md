# Data Lake Reconciliation Report: silver.view_pacientes vs. Athena silver_clinisys_staging.view_pacientes

> [!NOTE]
> ### 📊 Data Lake Ingestion & Promotion Quality KPI Dashboard
> * **Row Count Match Rate**: **99.860%** (250,380 Local vs 250,730 Target - **-350 difference**)
> * **Local Key Overlap Rate**: **99.999%**
> * **Target Key Overlap Rate**: **99.859%**
> * **Overall Value Alignment**: **N/A**


This report presents a generalized reconciliation audit between **silver.view_pacientes** and **Athena silver_clinisys_staging.view_pacientes** using automated keys verification.

---

## 1. Schema Comparison
* Local columns count: **115**
* Target columns count: **115**

### 1.1 Columns Only in Local
`extraction_timestamp`, `hash`

### 1.2 Columns Only in Target
`_dlt_id`, `bronze_updated_at`

---

## 2. Row Count & Volume Validation (Full Datasets)

| Table | Local Count | Target Count | Difference | Pct Diff | Status |
| :--- | :---: | :---: | :---: | :---: | :--- |
| **view_pacientes** | 250,380 | 250,730 | -350 | -0.14% | Variance Detected |

---

## 3. Key Overlap Summary (Joined on codigo)

* **Total Overlapping Keys**: **250,377**
* **Keys ONLY in Local**: **3**
* **Keys ONLY in Target**: **353**

---

## 4. Key Takeaways & Root Cause Analysis

### 4.1 Sample Records Only in Local (3 total)
| Codigo |
| :---: |
| 195204 |
| 711585 |
| 114113 |

### 4.2 Sample Records Only in Target (353 total)
| Codigo |
| :---: |
| 922093 |
| 922129 |
| 922193 |
| 922225 |
| 922313 |
| 922341 |
| 922391 |
| 922399 |
| 922411 |
| 922495 |
| 922503 |
| 922575 |
| 922626 |
| 922630 |
| 922654 |


---

## 5. Actionable Recommendations
1. **Deduplicate / Trace Gaps**: Inspect missing records in either local or remote target databases using the sample keys above.
2. **Handle Schema Drift**: If column mismatches exist, update downstream mapping logic or execute database alter statements.
