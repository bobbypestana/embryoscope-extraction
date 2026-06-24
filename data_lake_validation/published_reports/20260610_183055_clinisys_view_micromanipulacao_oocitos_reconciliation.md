# Data Lake Reconciliation Report: silver.view_micromanipulacao_oocitos vs. Athena silver_clinisys_staging.view_micromanipulacao_oocitos

> [!NOTE]
> ### 📊 Data Lake Ingestion & Promotion Quality KPI Dashboard
> * **Row Count Match Rate**: **99.985%** (308,123 Local vs 308,170 Target - **-47 difference**)
> * **Local Key Overlap Rate**: **99.969%**
> * **Target Key Overlap Rate**: **99.953%**
> * **Overall Value Alignment**: **N/A**


This report presents a generalized reconciliation audit between **silver.view_micromanipulacao_oocitos** and **Athena silver_clinisys_staging.view_micromanipulacao_oocitos** using automated keys verification.

---

## 1. Schema Comparison
* Local columns count: **56**
* Target columns count: **56**

### 1.1 Columns Only in Local
`ComentariosAntes`, `ComentariosDepois`, `CorpusculoPolar`, `IdentificacaoPGD`, `InseminacaoOocito`, `MassaInterna_D4`, `MassaInterna_D5`, `MassaInterna_D6`, `MassaInterna_D7`, `NCelulas_D2`, `NCelulas_D3`, `NCelulas_D4`, `NCelulas_D5`, `NCelulas_D6`, `NCelulas_D7`, `NClivou_D2`, `NClivou_D3`, `NClivou_D4`, `NClivou_D5`, `NClivou_D6`, `NClivou_D7`, `OocitoDoado`, `OrigemOocito`, `ResultadoPGD`, `ResultadoPGDDetalhes`, `extraction_timestamp`, `hash`

### 1.2 Columns Only in Target
`_dlt_id`, `bronze_updated_at`, `comentarios_antes`, `comentarios_depois`, `corpusculo_polar`, `identificacao_pgd`, `inseminacao_oocito`, `massa_interna_d4`, `massa_interna_d5`, `massa_interna_d6`, `massa_interna_d7`, `n_celulas_d2`, `n_celulas_d3`, `n_celulas_d4`, `n_celulas_d5`, `n_celulas_d6`, `n_celulas_d7`, `n_clivou_d2`, `n_clivou_d3`, `n_clivou_d4`, `n_clivou_d5`, `n_clivou_d6`, `n_clivou_d7`, `oocito_doado`, `origem_oocito`, `resultado_pgd`, `resultado_pgd_detalhes`

---

## 2. Row Count & Volume Validation (Full Datasets)

| Table | Local Count | Target Count | Difference | Pct Diff | Status |
| :--- | :---: | :---: | :---: | :---: | :--- |
| **view_micromanipulacao_oocitos** | 308,123 | 308,170 | -47 | -0.02% | Variance Detected |

---

## 3. Key Overlap Summary (Joined on id)

* **Total Overlapping Keys**: **308,026**
* **Keys ONLY in Local**: **97**
* **Keys ONLY in Target**: **144**

---

## 4. Key Takeaways & Root Cause Analysis

### 4.1 Sample Records Only in Local (97 total)
| Id |
| :---: |
| 295973 |
| 289867 |
| 289868 |
| 289869 |
| 289870 |
| 289871 |
| 289872 |
| 289873 |
| 289874 |
| 289875 |
| 289876 |
| 289877 |
| 289878 |
| 289879 |
| 289880 |

### 4.2 Sample Records Only in Target (144 total)
| Id |
| :---: |
| 317491 |
| 317492 |
| 317493 |
| 317494 |
| 317495 |
| 317496 |
| 317497 |
| 317498 |
| 317499 |
| 317500 |
| 317501 |
| 317502 |
| 317503 |
| 317525 |
| 317526 |


---

## 5. Actionable Recommendations
1. **Deduplicate / Trace Gaps**: Inspect missing records in either local or remote target databases using the sample keys above.
2. **Handle Schema Drift**: If column mismatches exist, update downstream mapping logic or execute database alter statements.
