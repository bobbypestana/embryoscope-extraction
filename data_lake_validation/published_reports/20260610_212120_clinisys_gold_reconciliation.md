# Data Lake Reconciliation Report: gold.clinisys_embrioes vs. silver.view_micromanipulacao_oocitos (Clinisys Gold vs Silver Reconciliation)

> [!NOTE]
> ### 📊 Data Lake Ingestion & Promotion Quality KPI Dashboard
> * **Row Count Match Rate**: **99.604%** (309,343 Local vs 308,123 Target - **+1,220 difference**)
> * **Key Overlap Alignment**: **100.000%** (308,123 Overlapping Keys)
> * **Overall Value Alignment**: **99.751%** (Value Sum: 2,950,688.00 vs 2,943,352.00 - **+7,336.00 difference**)
> * **Entity Match Rates (Overlap)**:
>   * **Local Key Match Rate**: **100.000%**
>   * **Target Key Match Rate**: **100.000%**

This validation report presents a reconciliation audit between the Gold layer table `gold.clinisys_embrioes` (local database: `database/huntington_data_lake.duckdb`) and the Silver layer table `silver.view_micromanipulacao_oocitos` (local database: `database/clinisys_all.duckdb`). The audit covers a comprehensive date range from **1981-04-15** to **2026-06-05**, evaluating row count volumes, schema alignment, join logic behavior, and numeric metric correctness. The analysis identifies key duplication in the Gold layer due to multi-event tracking for individual oocytes, which explains the row count discrepancies.

---

## 1. Schema Comparison
The Gold layer table `gold.clinisys_embrioes` contains **56** columns, which is identical to the column count of **56** in the Silver layer table `silver.view_micromanipulacao_oocitos`. However, the column sets differ substantially in scope and naming convention. Column names in the Gold layer are standardized using source table prefixes (e.g. `oocito_`, `micro_`, `cong_em_`, `descong_em_`, `emb_cong_`, `trat1_`, `trat2_`, `medico_`) to clearly trace the provenance of each attribute after joins. Furthermore, raw developmental stage columns (e.g., cell division counts at days 2-7) are excluded from the Gold layer to focus on clinical and demographic summary metrics, whereas freezing, thawing, treatment history, and physician details are incorporated into the Gold table.

### 1.1 Column differences
| Table | Columns in Source A (gold) Only | Columns in Source B (silver) Only | Datatype Shifts / Observations |
| :--- | :--- | :--- | :--- |
| `gold.clinisys_embrioes` vs `silver.view_micromanipulacao_oocitos` | 56 prefixed columns: `oocito_id`, `oocito_embryo_number`, `micro_data_procedimento`, `cong_em_id`, `descong_em_id`, `emb_cong_id`, `nome_medico`, `flag_is_external_medical`, etc. | 56 raw columns: `id`, `embryo_number`, `NCelulas_D2`, `Compactando_D4`, `MassaInterna_D5`, `AH`, `PI`, `RC`, `Trofoblasto_D4`, etc. | Standardized column names with aliases to trace provenance. Datatypes are preserved. Raw clinical observations are omitted from Gold, while lookup dimensions are added. |

---

## 2. Row Count & Volume Validation (Full Datasets)
The overall row counts reveal a small variance (+1,220 rows) between the local Gold table and the target Silver table. This discrepancy is not due to extra or missing source records, but rather is an artifact of the join logic used to build the Gold table.

### 2.1 overall Row Counts
| Table | Local (Gold) Count | Target (Silver) Count | Difference | Pct Diff | Status / Observations |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **clinisys_embrioes** vs **view_micromanipulacao_oocitos** | 309,343 | 308,123 | +1,220 | +0.40% | Variance Detected (due to duplicate key joins) |

---

## 3. Overlapping Keys Summary (Overlap Only)
*These tables include only records present in both sources, isolating mapping logic correctness.*

### 3.1 Overlap By Procedure Year
| Year/Category | Overlapping Records | Local Key Match Rate | Target Key Match Rate |
| :---: | :---: | :---: | :---: |
| 1981 | 3 | 100.0% | 100.0% |
| 2000 | 7 | 100.0% | 100.0% |
| 2010 | 29 | 100.0% | 100.0% |
| 2011 | 30 | 100.0% | 100.0% |
| 2012 | 9 | 100.0% | 100.0% |
| 2016 | 35 | 100.0% | 100.0% |
| 2017 | 22 | 100.0% | 100.0% |
| 2019 | 106 | 100.0% | 100.0% |
| 2020 | 1,738 | 100.0% | 100.0% |
| 2021 | 18,456 | 100.0% | 100.0% |
| 2022 | 31,102 | 100.0% | 100.0% |
| 2023 | 34,694 | 100.0% | 100.0% |
| 2024 | 34,129 | 100.0% | 100.0% |
| 2025 | 36,181 | 100.0% | 100.0% |
| 2026 | 13,095 | 100.0% | 100.0% |
| Unknown (Null) | 138,487 | 100.0% | 100.0% |

---

## 4. Key Takeaways & Root Cause Analysis

### 4.1. Key Duplication due to Left Join on Freezing Records
The primary source of row inflation in the Gold table `gold.clinisys_embrioes` is the `LEFT JOIN` on the table `silver.view_embrioes_congelados` (alias `emb_cong`) on the condition `emb_cong.id_oocito = oocito.id`.
A subset of 1,210 oocytes have multiple records in the frozen embryos table. This happens because the same embryo can have multiple recorded actions or states (such as being cryopreserved first and later thawed/discarded, or frozen and then transferred to another clinic). Since the join is one-to-many, these multiple entries duplicate the oocyte record in the final Gold table, resulting in exactly 1,220 extra rows.

To prove that no values are mutated and the data is equivalent, we can show that deduplicating the Gold table on `oocito_id` yields the exact same embryo number sum as the Silver table:

```sql
WITH deduped AS (
    SELECT oocito_id, oocito_embryo_number,
           ROW_NUMBER() OVER (PARTITION BY oocito_id ORDER BY emb_cong_id ASC) as rn
    FROM gold.clinisys_embrioes
)
SELECT SUM(oocito_embryo_number)
FROM deduped
WHERE rn = 1; -- Result: 2,943,352 (Matches Target Sum perfectly!)
```

### 4.2. Example Duplicated Cases (Anonymized)
The following table lists representative samples of oocytes that were duplicated. It demonstrates that the duplication is directly linked to multiple cryopreservation entries with different transfer/cryopreservation statuses.

#### **Anonymized Examples**
| Oocito ID | Mapped Value (Local) | Target Value (Target) | Record Status | Category / Impact |
| :---: | :---: | :---: | :---: | :---: |
| 3355 | 1 | 1 | Criopreservado | Multi-state event tracking |
| 3355 | 1 | 1 | Descartado | Multi-state event tracking |
| 265896 | 1 | 1 | Criopreservado | Multi-state event tracking |
| 265896 | 1 | 1 | NULL | Multi-state event tracking |
| 270755 | 2 | 2 | NULL | Multi-state event tracking |
| 270755 | 2 | 2 | Descartado | Multi-state event tracking |
| 276977 | 3 | 3 | Transferido para outra Clinica | Multi-state event tracking |
| 276977 | 3 | 3 | NULL | Multi-state event tracking |
| 298649 | 2 | 2 | Criopreservado | Multi-state event tracking |
| 298649 | 2 | 2 | Descartado | Multi-state event tracking |

---

## 5. Actionable Recommendations
1. **Clarify Grain of Gold Table** - Standardize the grain of the `gold.clinisys_embrioes` table. If the intended grain is "one row per oocyte/embryo," we should deduplicate the `silver.view_embrioes_congelados` join (e.g. using `ROW_NUMBER() OVER (PARTITION BY id_oocito ORDER BY id DESC)` to select only the most recent cryopreservation state).
2. **Handle Incomplete References** - Investigate the 38,263 records in `silver.view_embrioes_congelados` that have `id_oocito = 0`. This indicates a large volume of unlinked frozen embryo entries that do not propagate to the Gold table since they don't match any oocyte ID in `silver.view_micromanipulacao_oocitos`.
3. **Verify Upstream Quality Gates** - Since the row count discrepancy (+0.40%) is well within the acceptable threshold limit of +/- 2.0%, the quality gate for volume is passing. However, the value alignment rate of `99.75%` failed the strict threshold of `99.99%` due to duplication. Applying recommendation 1 will resolve this value mismatch and allow the quality gate to pass.
