# Data Lake Reconciliation Report: silver.view_orcamentos vs. Athena silver_clinisys_staging.view_orcamentos

> [!NOTE]
> ### 📊 Data Lake Ingestion & Promotion Quality KPI Dashboard
> * **Row Count Match Rate**: **91.300%** (51,209 Local vs 56,089 Target - **-4,880 difference**)
> * **Local Key Overlap Rate**: **99.447%**
> * **Target Key Overlap Rate**: **90.795%**
> * **Overall Value Alignment**: **100.000%**


This report presents a generalized reconciliation audit between **silver.view_orcamentos** and **Athena silver_clinisys_staging.view_orcamentos** using automated keys verification.

---

## 1. Schema Comparison
* Local columns count: **36**
* Target columns count: **36**

### 1.1 Columns Only in Local
`extraction_timestamp`, `hash`

### 1.2 Columns Only in Target
`_dlt_id`, `bronze_updated_at`

---

## 2. Row Count & Volume Validation (Full Datasets)

| Table | Local Count | Target Count | Difference | Pct Diff | Status |
| :--- | :---: | :---: | :---: | :---: | :--- |
| **view_orcamentos** | 51,209 | 56,089 | -4,880 | -8.70% | Variance Detected |

---

## 3. Key Overlap Summary (Joined on id)

* **Total Overlapping Keys**: **50,926**
* **Keys ONLY in Local**: **283**
* **Keys ONLY in Target**: **5,163**

### 3.1 Overlap Value Comparison
| Column Name | Local Sum | Target Sum | Difference | Alignment Rate |
| :--- | :---: | :---: | :---: | :---: |
| **valor** | 0.00 | 0.00 | +0.00 | 100.0000% |

---

## 4. Key Takeaways & Root Cause Analysis

### 4.1 Sample Records Only in Local (283 total)
| Id | Valor |
| :---: | :---: |
| 25089 | nan |
| 29618 | nan |
| 33213 | nan |
| 29219 | nan |
| 30422 | nan |
| 40601 | nan |
| 32562 | nan |
| 33217 | nan |
| 45395 | nan |
| 49576 | nan |
| 50312 | nan |
| 52266 | nan |
| 52305 | nan |
| 53371 | nan |
| 53574 | nan |

### 4.2 Sample Records Only in Target (5,163 total)
| Id | Valor |
| :---: | :---: |
| 56275 | None |
| 56276 | None |
| 56277 | None |
| 56278 | None |
| 56279 | None |
| 56280 | None |
| 56281 | None |
| 56282 | None |
| 56284 | None |
| 56285 | None |
| 56286 | None |
| 56287 | None |
| 56288 | None |
| 56289 | None |
| 56290 | None |


---

## 5. Actionable Recommendations
1. **Deduplicate / Trace Gaps**: Inspect missing records in either local or remote target databases using the sample keys above.
2. **Handle Schema Drift**: If column mismatches exist, update downstream mapping logic or execute database alter statements.
