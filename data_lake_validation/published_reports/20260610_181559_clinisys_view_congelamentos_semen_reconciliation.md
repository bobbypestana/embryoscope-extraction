# Data Lake Reconciliation Report: silver.view_congelamentos_semen vs. Athena silver_clinisys_staging.view_congelamentos_semen

> [!NOTE]
> ### 📊 Data Lake Ingestion & Promotion Quality KPI Dashboard
> * **Row Count Match Rate**: **99.560%** (5,658 Local vs 5,683 Target - **-25 difference**)
> * **Local Key Overlap Rate**: **100.000%**
> * **Target Key Overlap Rate**: **99.560%**
> * **Overall Value Alignment**: **N/A**


This report presents a generalized reconciliation audit between **silver.view_congelamentos_semen** and **Athena silver_clinisys_staging.view_congelamentos_semen** using automated keys verification.

---

## 1. Schema Comparison
* Local columns count: **44**
* Target columns count: **44**

### 1.1 Columns Only in Local
`BiologoResponsavel`, `CodCongelamento`, `DataDescongelamento`, `DataTransferDescarte`, `PalhetaVial`, `PalhetaVial2`, `QtdPalheta`, `QtdPalheta2`, `QtdPalhetasDescongelamento`, `QtdVialsDescongelamento`, `ResponsavelDescarte`, `TransferClinica`, `UtilizacaoDescongelamento`, `extraction_timestamp`, `hash`

### 1.2 Columns Only in Target
`_dlt_id`, `biologo_responsavel`, `bronze_updated_at`, `cod_congelamento`, `data_descongelamento`, `data_transfer_descarte`, `palheta_vial`, `palheta_vial2`, `qtd_palheta`, `qtd_palheta2`, `qtd_palhetas_descongelamento`, `qtd_vials_descongelamento`, `responsavel_descarte`, `transfer_clinica`, `utilizacao_descongelamento`

---

## 2. Row Count & Volume Validation (Full Datasets)

| Table | Local Count | Target Count | Difference | Pct Diff | Status |
| :--- | :---: | :---: | :---: | :---: | :--- |
| **view_congelamentos_semen** | 5,658 | 5,683 | -25 | -0.44% | Variance Detected |

---

## 3. Key Overlap Summary (Joined on id)

* **Total Overlapping Keys**: **5,658**
* **Keys ONLY in Local**: **0**
* **Keys ONLY in Target**: **25**

---

## 4. Key Takeaways & Root Cause Analysis

### 4.2 Sample Records Only in Target (25 total)
| Id |
| :---: |
| 10011 |
| 9987 |
| 9988 |
| 9989 |
| 9990 |
| 9991 |
| 9992 |
| 9993 |
| 9994 |
| 9995 |
| 9996 |
| 9997 |
| 9998 |
| 9999 |
| 10000 |


---

## 5. Actionable Recommendations
1. **Deduplicate / Trace Gaps**: Inspect missing records in either local or remote target databases using the sample keys above.
2. **Handle Schema Drift**: If column mismatches exist, update downstream mapping logic or execute database alter statements.
