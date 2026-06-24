# Data Lake Reconciliation Report: gold.protheus_mesclada_vendas vs. silver.mesclada_vendas

> [!NOTE]
> ### 📊 Data Lake Ingestion & Promotion Quality KPI Dashboard
> * **Row Count Match Rate**: **89.963%** (592,401 Local vs 658,496 Target - **-66,095 difference**)
> * **Local Key Overlap Rate**: **86.451%**
> * **Target Key Overlap Rate**: **77.777%**
> * **Overall Value Alignment**: **100.000%**


This report presents a generalized reconciliation audit between **gold.protheus_mesclada_vendas** and **silver.mesclada_vendas** using automated keys verification.

---

## 1. Schema Comparison
* Local columns count: **42**
* Target columns count: **41**

### 1.1 Columns Only in Local
`CPF`

### 1.2 Columns Only in Target
*None*

---

## 2. Row Count & Volume Validation (Full Datasets)

| Table | Local Count | Target Count | Difference | Pct Diff | Status |
| :--- | :---: | :---: | :---: | :---: | :--- |
| **protheus_mesclada_vendas** | 592,401 | 658,496 | -66,095 | -10.04% | Variance Detected |

---

## 3. Key Overlap Summary (Joined on Loja, Numero, Serie Docto., DT Emissao)

* **Total Overlapping Keys**: **452,351**
* **Keys ONLY in Local**: **70,897**
* **Keys ONLY in Target**: **129,250**

### 3.1 Overlap Value Comparison
| Column Name | Local Sum | Target Sum | Difference | Alignment Rate |
| :--- | :---: | :---: | :---: | :---: |
| **total** | 509936487.45 | 509936487.45 | +0.00 | 100.0000% |

---

## 4. Key Takeaways & Root Cause Analysis

### 4.1 Sample Records Only in Local (70,897 total)
| Loja | Numero | Serie Docto. | Dt Emissao | Total |
| :---: | :---: | :---: | :---: | :---: |
| 060101 | 255041 | RPS | 2026-06-03 00:00:00 | 607.0 |
| 060101 | 255040 | RPS | 2026-06-03 00:00:00 | 607.0 |
| 060101 | 255039 | RPS | 2026-06-03 00:00:00 | 607.0 |
| 060101 | 255038 | RPS | 2026-06-03 00:00:00 | 1667.0 |
| 060101 | 255037 | RPS | 2026-06-03 00:00:00 | 607.0 |
| 060101 | 255036 | RPS | 2026-06-03 00:00:00 | 65.0 |
| 060101 | 255036 | RPS | 2026-06-03 00:00:00 | 65.0 |
| 060101 | 255035 | RPS | 2026-06-03 00:00:00 | 130.0 |
| 010101 | 156980 | RPS | 2026-06-03 00:00:00 | 230.0 |
| 010101 | 156979 | RPS | 2026-06-03 00:00:00 | 131.0 |
| 010101 | 156978 | RPS | 2026-06-03 00:00:00 | 250.0 |
| 010101 | 156977 | RPS | 2026-06-03 00:00:00 | 125.0 |
| 010101 | 156976 | RPS | 2026-06-03 00:00:00 | 83.33 |
| 010101 | 156975 | RPS | 2026-06-03 00:00:00 | 200.0 |
| 010101 | 156974 | RPS | 2026-06-03 00:00:00 | 166.66 |

### 4.2 Sample Records Only in Target (129,250 total)
| Loja | Numero | Serie Docto. | Dt Emissao | Total |
| :---: | :---: | :---: | :---: | :---: |
| 101 | 2 | 2 | 2025-12-03 00:00:00 | 7636.0 |
| 101 | 3 | 2 | 2025-12-03 00:00:00 | 1040.0 |
| 101 | 4 | 2 | 2025-12-03 00:00:00 | 400.0 |
| 101 | 6 | 2 | 2025-12-03 00:00:00 | 3300.0 |
| 101 | 7 | 2 | 2025-12-04 00:00:00 | 550.0 |
| 101 | 8 | 2 | 2025-12-04 00:00:00 | 450.0 |
| 101 | 9 | 2 | 2025-12-04 00:00:00 | 7636.0 |
| 101 | 10 | 2 | 2025-12-04 00:00:00 | 540.0 |
| 101 | 11 | 2 | 2025-12-04 00:00:00 | 450.0 |
| 101 | 14 | 2 | 2025-12-04 00:00:00 | 2877.0 |
| 101 | 14 | 2 | 2025-12-04 00:00:00 | 89.0 |
| 101 | 14 | 2 | 2025-12-04 00:00:00 | 2962.0 |
| 101 | 14 | 2 | 2025-12-04 00:00:00 | 2380.0 |
| 101 | 15 | 2 | 2025-12-04 00:00:00 | 4055.0 |
| 101 | 15 | 2 | 2025-12-04 00:00:00 | 1135.0 |


---

## 5. Actionable Recommendations
1. **Deduplicate / Trace Gaps**: Inspect missing records in either local or remote target databases using the sample keys above.
2. **Handle Schema Drift**: If column mismatches exist, update downstream mapping logic or execute database alter statements.
