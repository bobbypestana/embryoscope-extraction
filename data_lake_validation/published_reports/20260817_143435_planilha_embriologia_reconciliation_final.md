# Data Lake Reconciliation Report: planilha_embriologia Local vs Production Athena (Finalized)

> [!NOTE]
> ### 📊 Global Reconciliation Dashboard (All Fixes Applied)
> * **Total Tables Audited**: **3**
> * **Perfect / High Value Alignment (>99% Match across all KPIs)**: **3** tables
> * **Audit Execution Failures**: **0** tables
> 
> | Table Name (Local) | Table Name (Athena) | Local Count | Athena Count | Delta | Match Rate | Status |
> | :--- | :--- | :---: | :---: | :---: | :---: | :--- |
> | planilha_embriologia_fresh | fresh | 13,467 | 13,523 | -56 | 99.58% | ✅ High Alignment |
> | planilha_embriologia_fet | fet | 11,776 | 11,850 | -74 | 99.37% | ✅ High Alignment |
> | planilha_embriologia_combined | planilha_embriologia_combined | 20,520 | 20,646 | -126 | 99.39% | ✅ High Alignment |

This report documents the final validation state between local DuckDB and AWS Athena Production following both the column mapping overwrite resolution and the biopsy variant normalization.

---

## 1. Outcome Values Reconciliation (Fresh & Combined)

| Outcome Metric | Local (DuckDB) | Athena (Prod) | Delta | Match Rate % | Status |
| :--- | :---: | :---: | :---: | :---: | :--- |
| **Sum of `qtd_blasto`** | 39,596 | 39,763 | -167 | **99.58%** | ✅ Aligned |
| **Sum of `qtd_blasto_tq`** | 29,672 | 29,797 | -125 | **99.58%** | ✅ Aligned |
| **Sum of `no_biopsiados`** | 24,165 | 24,292 | -127 | **99.47%** | ✅ Fully Resolved (from 32.77% -> 99.47%) |
| **Sum of `qtd_analisados`** | 21,471 | 21,606 | -135 | **99.37%** | ✅ Aligned |
| **Sum of `qtd_normais`** | 7,472 | 7,541 | -69 | **99.08%** | ✅ Aligned |

---

## 2. FET Outcome Distributions

| Outcome Category (`result`) | Local Count | Athena Count | Delta | Status |
| :--- | :---: | :---: | :---: | :--- |
| **EMBRYO TRANSFER** | 5,484 | 5,486 | -2 | ✅ Aligned |
| **POSITIVO** | 3,144 | 3,175 | -31 | ✅ Aligned |
| **NEGATIVO** | 2,376 | 2,396 | -20 | ✅ Aligned |
| **EMBRYO VITRI** | 332 | 336 | -4 | ✅ Aligned |
| **REVITRI** | 293 | 293 | 0 | ✅ Exact Match |
| **NO ET** | 80 | 81 | -1 | ✅ Aligned |
| **CANCELLATION** | 62 | 67 | -5 | ✅ Aligned |

---

## 3. Prontuario & Key Alignment

| Table | Local Unique Prontuarios | Athena Unique Prontuarios | Delta | Match Rate % |
| :--- | :---: | :---: | :---: | :---: |
| **Fresh** | 9,055 | 9,079 | -24 | **99.73%** |
| **FET** | 7,827 | 7,858 | -31 | **99.60%** |
| **Combined** | 11,632 | 11,654 | -22 | **99.81%** |
