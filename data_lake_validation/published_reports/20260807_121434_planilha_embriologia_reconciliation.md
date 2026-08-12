# Data Lake Reconciliation Report: planilha_embriologia Local vs Production Athena

> [!NOTE]
> ### 📊 Global Reconciliation Dashboard
> * **Total Tables Audited**: **3**
> * **Perfect Value Alignment**: **0** tables
> * **Variance / Discrepancy Detected**: **3** tables
> * **Audit Execution Failures**: **0** tables
> 
> | Table Name (Local) | Table Name (Athena) | Local Count | Athena Count | Delta | Local Match Rate | Status |
> | :--- | :--- | :---: | :---: | :---: | :---: | :--- |
> | planilha_embriologia_fresh | fresh | 13,467 | 13,523 | -56 | 99.58% | Minor count drift |
> | planilha_embriologia_fet | fet | 23,240 | 11,850 | +11,390 | 50.99% | Conceptual / filter drift |
> | planilha_embriologia_combined | planilha_embriologia_combined | 27,093 | 20,664 | +6,429 | 76.27% | Conceptual / filter drift |

This report summarizes the data quality validation for the three versions of `planilha_embriologia` tables between local DuckDB and AWS Athena Production.

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
| **fet** | Total Rows | 23,240 | 11,850 | +11,390 | 50.99% |
| | Unique PIN | 13,196 | 8,954 | +4,242 | 67.85% |
| | Unique Prontuario | 11,775 | 7,858 | +3,917 | 66.73% |
| **combined** | Total Rows | 27,093 | 20,664 | +6,429 | 76.27% |
| | Unique Prontuario | 13,518 | 10,951 | +2,567 | 81.01% |

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

> [!WARNING]
> There is a significant discrepancy in the sum of `no_biopsiados` / `n_biopsiados` between local (14,526) and Athena (24,292), showing only a 32.77% match rate. This warrants investigation into how biopsy counts are ingested or aggregated in the Athena pipeline.

### 3.2 FET Table Outcome Distributions
| Outcome Category (`result`) | Local Count | Athena Count | Delta |
| :--- | :---: | :---: | :---: |
| **DONOR** | 1,069 | 0 | +1,069 |
| **EGG FREEZING** | 2,063 | 0 | +2,063 |
| **EMBRYO VITRI** | 5,891 | 336 | +5,555 |
| **EMBRYO TRANSFER** | 5,484 | 5,486 | -2 |
| **NO ET** | 1,725 | 81 | +1,644 |
| **NEGATIVO** | 2,827 | 2,396 | +431 |
| **POSITIVO** | 3,795 | 3,175 | +620 |

> [!IMPORTANT]
> The large difference in row count (+11,390 rows in Local) for the FET table is due to conceptual filtering. In AWS Athena, cycles that do not result in a direct Embryo Transfer (such as pure `EGG FREEZING`, `DONOR` cycles, or `EMBRYO VITRI` cycles without transfer events) are filtered out, whereas the local DuckDB table retains these raw cycles.

---

## 4. Key Takeaways & Recommendations
1. **Biopsy Ingestion Discrepancy**: Investigate the ingestion logic of `n_biopsiados` in Athena. The sum in Athena is significantly higher (24,292 vs 14,526 local), suggesting either duplication or double-counting of biopsy records.
2. **FET Filtering Alignment**: Document the filtering logic in the production Athena FET table pipeline so business stakeholders are aware that the production `fet` table is designed to represent actual Embryo Transfer cycles, rather than all historical frozen embryo records.
