# Data Lake Reconciliation Report: silver.view_tratamentos vs. Athena silver_clinisys_staging.view_tratamentos

> [!NOTE]
> ### 📊 Data Lake Ingestion & Promotion Quality KPI Dashboard
> * **Row Count Match Rate**: **99.235%** (42,927 Local vs 43,258 Target - **-331 difference**)
> * **Local Key Overlap Rate**: **99.916%**
> * **Target Key Overlap Rate**: **99.152%**
> * **Overall Value Alignment**: **N/A**


This report presents a generalized reconciliation audit between **silver.view_tratamentos** and **Athena silver_clinisys_staging.view_tratamentos** using automated keys verification.

---

## 1. Schema Comparison
* Local columns count: **196**
* Target columns count: **196**

### 1.1 Columns Only in Local
`bcf_embrião1_fonte`, `bcf_embrião1_status`, `extraction_timestamp`, `hash`

### 1.2 Columns Only in Target
`_dlt_id`, `bcf_embriao1_fonte`, `bcf_embriao1_status`, `bronze_updated_at`

---

## 2. Row Count & Volume Validation (Full Datasets)

| Table | Local Count | Target Count | Difference | Pct Diff | Status |
| :--- | :---: | :---: | :---: | :---: | :--- |
| **view_tratamentos** | 42,927 | 43,258 | -331 | -0.77% | Variance Detected |

---

## 3. Key Overlap Summary (Joined on id)

* **Total Overlapping Keys**: **42,891**
* **Keys ONLY in Local**: **36**
* **Keys ONLY in Target**: **367**

---

## 4. Key Takeaways & Root Cause Analysis

### 4.1 Sample Records Only in Local (36 total)
| Id |
| :---: |
| 37606 |
| 36241 |
| 35556 |
| 35827 |
| 36242 |
| 37205 |
| 36240 |
| 42957 |
| 39232 |
| 40877 |
| 34341 |
| 36844 |
| 40452 |
| 41193 |
| 36234 |

### 4.2 Sample Records Only in Target (367 total)
| Id |
| :---: |
| 44605 |
| 44506 |
| 44639 |
| 44450 |
| 44723 |
| 44745 |
| 44710 |
| 44662 |
| 44676 |
| 44744 |
| 44442 |
| 44579 |
| 44652 |
| 44656 |
| 44507 |


---

## 5. Actionable Recommendations
1. **Deduplicate / Trace Gaps**: Inspect missing records in either local or remote target databases using the sample keys above.
2. **Handle Schema Drift**: If column mismatches exist, update downstream mapping logic or execute database alter statements.
