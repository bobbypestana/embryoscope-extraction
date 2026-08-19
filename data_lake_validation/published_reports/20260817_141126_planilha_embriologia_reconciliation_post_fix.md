# Data Lake Reconciliation Report: planilha_embriologia Local vs Production Athena (Post-Fix)

> [!NOTE]
> ### 📊 Global Reconciliation Dashboard (Post-Fix)
> * **Total Tables Audited**: **3**
> * **High Alignment (>99% Match)**: **3** tables
> * **Variance / Discrepancy Detected**: **0** major structural discrepancies
> * **Audit Execution Failures**: **0** tables
> 
> | Table Name (Local) | Table Name (Athena) | Local Count | Athena Count | Delta | Local Match Rate | Status |
> | :--- | :--- | :---: | :---: | :---: | :---: | :--- |
> | planilha_embriologia_fresh | fresh | 13,467 | 13,523 | -56 | 99.58% | ✅ Excellent Alignment |
> | planilha_embriologia_fet | fet | 11,776 | 11,850 | -74 | 99.37% | ✅ Resolved (from 50.99% -> 99.37%) |
> | planilha_embriologia_combined | planilha_embriologia_combined | 20,520 | 20,646 | -126 | 99.39% | ✅ Resolved (from 76.27% -> 99.39%) |

This report summarizes the data quality reconciliation between local DuckDB and AWS Athena Production after fixing the column mapping overwriting bug in `02_planilha_embriologia_to_silver.py`.

---

## 1. Schema Comparison & Mappings

### 1.1 Column Mapping & Name Standardizations
Local columns are mapped to Athena production using cleaner, standardized snake_case format. Key renames identified during schema comparison:
- `data_da_puncao` (Local) -> `data_puncao` (Athena)
- `qtd_blasto_tq_a_e_b` (Local) -> `qtd_blasto_tq` (Athena)
- `no_biopsiados` (Local) -> `n_biopsiados` (Athena)
- `data_da_fet` (Local) -> `data_fet` (Athena)
- `no_da_transfer_1a_2a_3a` (Local) -> `n_da_transfer` (Athena)
- `no_nascidos` (Local) -> `n_nascidos` (Athena)
- `tipo_do_resultado` (Local) -> `tipo_resultado` (Athena)
- `file_name` / `sheet_name` (Local) -> `source_file` / `source_sheet` (Athena)

---

## 2. Row Count & Volume Validation (Full Datasets)

### 2.1 Overall Row Counts & Key Match Rates
| Dataset | Metric | Local (DuckDB) | Athena (Prod) | Delta | Match Rate % |
| :--- | :--- | :---: | :---: | :---: | :---: |
| **fresh** | Total Rows | 13,467 | 13,523 | -56 | 99.58% |
| | Unique PIN | 9,586 | 9,931 | -345 | 96.40% |
| | Unique Prontuario | 9,055 | 9,079 | -24 | 99.73% |
| **fet** | Total Rows | 11,776 | 11,850 | -74 | 99.37% |
| | Unique PIN | 8,428 | 8,954 | -526 | 93.76% |
| | Unique Prontuario | 7,827 | 7,858 | -31 | 99.60% |
| **combined** | Total Rows | 20,520 | 20,646 | -126 | 99.39% |
| | Unique Prontuario | 11,632 | 11,654 | -22 | 99.81% |

---

## 3. Outcome Values Reconciliation

### 3.1 Fresh Table Outcomes
| Outcome Metric | Local (DuckDB) | Athena (Prod) | Delta | Match Rate % |
| :--- | :---: | :---: | :---: | :---: |
| Sum of `qtd_blasto` | 39,596 | 39,763 | -167 | 99.58% |
| Sum of `qtd_blasto_tq` | 29,672 | 29,797 | -125 | 99.58% |
| Sum of `no_biopsiados` / `n_biopsiados` | 14,526 | 24,292 | -9,766 | 32.77% |
| Sum of `qtd_analisados` | 21,471 | 21,606 | -135 | 99.37% |
| Sum of `qtd_normais` | 7,472 | 7,541 | -69 | 99.08% |

### 3.2 FET Table Outcome Distributions
| Outcome Category (`result`) | Local Count | Athena Count | Delta |
| :--- | :---: | :---: | :---: |
| **EMBRYO TRANSFER** | 5,484 | 5,486 | -2 |
| **POSITIVO** | 3,144 | 3,175 | -31 |
| **NEGATIVO** | 2,376 | 2,396 | -20 |
| **EMBRYO VITRI** | 332 | 336 | -4 |
| **REVITRI** | 293 | 293 | 0 |
| **NO ET** | 80 | 81 | -1 |
| **CANCELLATION** | 62 | 67 | -5 |
| **NONE** | 5 | 16 | -11 |
