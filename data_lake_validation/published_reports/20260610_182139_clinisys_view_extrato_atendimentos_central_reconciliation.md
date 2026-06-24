# Data Lake Reconciliation Report: silver.view_extrato_atendimentos_central vs. Athena silver_clinisys_staging.view_extrato_atendimentos_central

> [!NOTE]
> ### 📊 Data Lake Ingestion & Promotion Quality KPI Dashboard
> * **Row Count Match Rate**: **71.981%** (640,093 Local vs 500,000 Target - **+140,093 difference**)
> * **Local Key Overlap Rate**: **75.854%**
> * **Target Key Overlap Rate**: **97.107%**
> * **Overall Value Alignment**: **N/A**


This report presents a generalized reconciliation audit between **silver.view_extrato_atendimentos_central** and **Athena silver_clinisys_staging.view_extrato_atendimentos_central** using automated keys verification.

---

## 1. Schema Comparison
* Local columns count: **23**
* Target columns count: **23**

### 1.1 Columns Only in Local
`extraction_timestamp`, `hash`

### 1.2 Columns Only in Target
`_dlt_id`, `bronze_updated_at`

---

## 2. Row Count & Volume Validation (Full Datasets)

| Table | Local Count | Target Count | Difference | Pct Diff | Status |
| :--- | :---: | :---: | :---: | :---: | :--- |
| **view_extrato_atendimentos_central** | 640,093 | 500,000 | +140,093 | +28.02% | Variance Detected |

---

## 3. Key Overlap Summary (Joined on agendamento_id)

* **Total Overlapping Keys**: **485,534**
* **Keys ONLY in Local**: **154,559**
* **Keys ONLY in Target**: **14,466**

---

## 4. Key Takeaways & Root Cause Analysis

### 4.1 Sample Records Only in Local (154,559 total)
| Agendamento_Id |
| :---: |
| 856960 |
| 892509 |
| 898098 |
| 947654 |
| 953500 |
| 1006977 |
| 1006980 |
| 1011322 |
| 1017087 |
| 1020445 |
| 1042908 |
| 1045977 |
| 1046687 |
| 1046689 |
| 1058222 |

### 4.2 Sample Records Only in Target (14,466 total)
| Agendamento_Id |
| :---: |
| 1714667 |
| 1714666 |
| 1714665 |
| 1714664 |
| 1714663 |
| 1714662 |
| 1714661 |
| 1714660 |
| 1714659 |
| 1714658 |
| 1714657 |
| 1714656 |
| 1714655 |
| 1714654 |
| 1714653 |


---

## 5. Actionable Recommendations
1. **Deduplicate / Trace Gaps**: Inspect missing records in either local or remote target databases using the sample keys above.
2. **Handle Schema Drift**: If column mismatches exist, update downstream mapping logic or execute database alter statements.
