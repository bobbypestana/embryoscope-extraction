# Data Lake Reconciliation Report: silver.view_congelamentos_semen_doador vs. Athena silver_clinisys_staging.view_congelamentos_semen_doador

> [!NOTE]
> ### 📊 Data Lake Ingestion & Promotion Quality KPI Dashboard
> * **Row Count Match Rate**: **99.948%** (1,937 Local vs 1,938 Target - **-1 difference**)
> * **Local Key Overlap Rate**: **99.948%**
> * **Target Key Overlap Rate**: **99.897%**
> * **Overall Value Alignment**: **N/A**


This report presents a generalized reconciliation audit between **silver.view_congelamentos_semen_doador** and **Athena silver_clinisys_staging.view_congelamentos_semen_doador** using automated keys verification.

---

## 1. Schema Comparison
* Local columns count: **42**
* Target columns count: **42**

### 1.1 Columns Only in Local
`BiologoResponsavel`, `CodCongelamento`, `DataDescongelamento`, `DataFicha`, `DataTransferDescarte`, `PalhetaVial`, `QtdPalheta`, `QtdPalhetasDescongelamento`, `QtdVialsDescongelamento`, `ResponsavelDescarte`, `TransferClinica`, `UtilizacaoDescongelamento`, `extraction_timestamp`, `hash`

### 1.2 Columns Only in Target
`_dlt_id`, `biologo_responsavel`, `bronze_updated_at`, `cod_congelamento`, `data_descongelamento`, `data_ficha`, `data_transfer_descarte`, `palheta_vial`, `qtd_palheta`, `qtd_palhetas_descongelamento`, `qtd_vials_descongelamento`, `responsavel_descarte`, `transfer_clinica`, `utilizacao_descongelamento`

---

## 2. Row Count & Volume Validation (Full Datasets)

| Table | Local Count | Target Count | Difference | Pct Diff | Status |
| :--- | :---: | :---: | :---: | :---: | :--- |
| **view_congelamentos_semen_doador** | 1,937 | 1,938 | -1 | -0.05% | Variance Detected |

---

## 3. Key Overlap Summary (Joined on id)

* **Total Overlapping Keys**: **1,936**
* **Keys ONLY in Local**: **1**
* **Keys ONLY in Target**: **2**

---

## 4. Key Takeaways & Root Cause Analysis

### 4.1 Sample Records Only in Local (1 total)
| Id |
| :---: |
| 3102 |

### 4.2 Sample Records Only in Target (2 total)
| Id |
| :---: |
| 3277 |
| 3278 |


---

## 5. Actionable Recommendations
1. **Deduplicate / Trace Gaps**: Inspect missing records in either local or remote target databases using the sample keys above.
2. **Handle Schema Drift**: If column mismatches exist, update downstream mapping logic or execute database alter statements.
