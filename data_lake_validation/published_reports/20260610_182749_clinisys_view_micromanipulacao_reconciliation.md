# Data Lake Reconciliation Report: silver.view_micromanipulacao vs. Athena silver_clinisys_staging.view_micromanipulacao

> [!NOTE]
> ### 📊 Data Lake Ingestion & Promotion Quality KPI Dashboard
> * **Row Count Match Rate**: **99.409%** (27,909 Local vs 28,075 Target - **-166 difference**)
> * **Local Key Overlap Rate**: **99.910%**
> * **Target Key Overlap Rate**: **99.320%**
> * **Overall Value Alignment**: **N/A**


This report presents a generalized reconciliation audit between **silver.view_micromanipulacao** and **Athena silver_clinisys_staging.view_micromanipulacao** using automated keys verification.

---

## 1. Schema Comparison
* Local columns count: **211**
* Target columns count: **211**

### 1.1 Columns Only in Local
`CicloDescongelamento`, `CicloDoadora`, `CongelamentoOvulos`, `CongelamentoSemenHet`, `CongelamentoSemenHet2`, `CongelamentoSemenHom`, `CongelamentoSemenHom2`, `EstadoSptz`, `EstadoSptz2`, `ICSIDescongelados`, `IdadeDoadora`, `IdadeEsposa_DG`, `IdadeMarido_DG`, `MorfologiaAlterada`, `MotivoCongelamentoOvulos`, `VialsDescongeladasHet`, `VialsDescongeladasHet2`, `VialsDescongeladasHom`, `VialsDescongeladasHom2`, `extraction_timestamp`, `hash`

### 1.2 Columns Only in Target
`_dlt_id`, `bronze_updated_at`, `ciclo_descongelamento`, `ciclo_doadora`, `congelamento_ovulos`, `congelamento_semen_het`, `congelamento_semen_het2`, `congelamento_semen_hom`, `congelamento_semen_hom2`, `estado_sptz`, `estado_sptz2`, `icsi_descongelados`, `idade_doadora`, `idade_esposa_dg`, `idade_marido_dg`, `morfologia_alterada`, `motivo_congelamento_ovulos`, `vials_descongeladas_het`, `vials_descongeladas_het2`, `vials_descongeladas_hom`, `vials_descongeladas_hom2`

---

## 2. Row Count & Volume Validation (Full Datasets)

| Table | Local Count | Target Count | Difference | Pct Diff | Status |
| :--- | :---: | :---: | :---: | :---: | :--- |
| **view_micromanipulacao** | 27,909 | 28,075 | -166 | -0.59% | Variance Detected |

---

## 3. Key Overlap Summary (Joined on codigo_ficha)

* **Total Overlapping Keys**: **27,884**
* **Keys ONLY in Local**: **25**
* **Keys ONLY in Target**: **191**

---

## 4. Key Takeaways & Root Cause Analysis

### 4.1 Sample Records Only in Local (25 total)
| Codigo_Ficha |
| :---: |
| 27132 |
| 27463 |
| 28079 |
| 27353 |
| 28403 |
| 27318 |
| 27320 |
| 27995 |
| 27223 |
| 28385 |
| 29250 |
| 30221 |
| 30890 |
| 27319 |
| 27631 |

### 4.2 Sample Records Only in Target (191 total)
| Codigo_Ficha |
| :---: |
| 30983 |
| 30982 |
| 30991 |
| 30992 |
| 31008 |
| 31013 |
| 31014 |
| 31015 |
| 31019 |
| 31021 |
| 31023 |
| 31025 |
| 31028 |
| 31031 |
| 31032 |


---

## 5. Actionable Recommendations
1. **Deduplicate / Trace Gaps**: Inspect missing records in either local or remote target databases using the sample keys above.
2. **Handle Schema Drift**: If column mismatches exist, update downstream mapping logic or execute database alter statements.
