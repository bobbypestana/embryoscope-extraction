# Data Lake Reconciliation Report: DuckDB Gold vs. AWS Athena Gold

> [!NOTE]
> ### 📊 Global Reconciliation Dashboard
> * **Total Tables Audited**: **17**
> * **Perfect Value Alignment**: **0** tables
> * **Variance / Discrepancy Detected**: **17** tables
> * **Audit Execution Failures**: **0** tables

This report presents a detailed reconciliation audit between the local DuckDB database (`gold` schema) and the AWS Athena database (`gold_huntington_staging`).

---

## 1. Table Volume and Key Alignment Summary

| Table Name | DuckDB Count | Athena Count | Overlap | DuckDB Only | Athena Only | DuckDB Match Rate | Athena Match Rate | Status |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :--- |
| `patient_info` | 51,107 | 50,284 | 0 | 51,107 | 50,284 | 0.00% | 0.00% | ⚠️ VARIANCE |
| `finops_summary` | 35,044 | 15,033 | 14,155 | 20,889 | 15,024 | 40.39% | 94.22% | ⚠️ VARIANCE |
| `biopsy_pgta_timeline` | 8,800 | 9,963 | 0 | 8,800 | 9,963 | 0.00% | 0.00% | ⚠️ VARIANCE |
| `consultas_timeline` | 50,862 | 44,517 | 0 | 50,862 | 44,517 | 0.00% | 0.00% | ⚠️ VARIANCE |
| `cryopreservation_events_timeline` | 46,550 | 42,804 | 0 | 46,550 | 42,804 | 0.00% | 0.00% | ⚠️ VARIANCE |
| `embryo_freeze_timeline` | 40,311 | 43,522 | 0 | 40,311 | 43,522 | 0.00% | 0.00% | ⚠️ VARIANCE |
| `embryoscope_timeline` | 18,091 | 6,119 | 0 | 18,091 | 6,119 | 0.00% | 0.00% | ⚠️ VARIANCE |
| `all_patients_timeline` | 403,305 | 388,727 | 0 | 403,305 | 388,727 | 0.00% | 0.00% | ⚠️ VARIANCE |
| `recent_patients_timeline` | 176,491 | 235,158 | 0 | 176,491 | 235,158 | 0.00% | 0.00% | ⚠️ VARIANCE |
| `embryoscope_embrioes` | 122,615 | 143,069 | 115,777 | 6,838 | 143,069 | 94.42% | 80.92% | ⚠️ VARIANCE |
| `clinisys_embrioes` | 311,534 | 314,171 | 254,954 | 56,580 | 314,171 | 81.84% | 81.15% | ⚠️ VARIANCE |
| `embryos_with_prescription_long` | 318,074 | 98,061 | 69,587 | 242,736 | 97,668 | 22.28% | 71.25% | ⚠️ VARIANCE |
| `embryos_with_prescription_wide` | 40,729 | 39,962 | 12,948 | 27,781 | 39,755 | 31.79% | 32.57% | ⚠️ VARIANCE |
| `embryoscope_clinisys_combined` | 312,165 | 314,194 | 213,028 | 99,137 | 314,194 | 68.24% | 67.80% | ⚠️ VARIANCE |
| `planilha_embryoscope_combined` | 312,165 | 314,194 | 213,028 | 99,137 | 314,194 | 68.24% | 67.80% | ⚠️ VARIANCE |
| `redlara_planilha_combined` | 27,498 | 21,683 | 3,507 | 22,663 | 19,814 | 13.40% | 17.70% | ⚠️ VARIANCE |
| `protheus_mesclada_vendas` | 601,866 | 754,886 | 601,866 | 0 | 754,886 | 100.00% | 79.73% | ⚠️ VARIANCE |

---

## 2. Table-by-Table Mismatch Diagnostics & Group Isolation

### `patient_info`
### `finops_summary`
#### **Numeric Metric Alignment**
| Column Name | DuckDB Sum | Athena Sum | Difference | Alignment Rate |
| :--- | :---: | :---: | :---: | :---: |
| `cycle_with_transfer` | 12348.00 | 14479.00 | -2131.00 | 85.2821% |
| `cycle_without_transfer` | 14228.00 | 14905.00 | -677.00 | 95.4579% |
| `cycle_total` | 26576.00 | 29384.00 | -2808.00 | 90.4438% |
| `treatment_paid_count` | 20990.00 | 0.00 | +20990.00 | 100.0000% |
| `treatment_paid_total` | 268823965.49 | 0.00 | +268823965.49 | 100.0000% |

#### **Discrepancy Group Isolation**
* **By Year/Date**:
  * Year **1982**: 1 discrepant records
  * Year **2012**: 1 discrepant records
  * Year **2013**: 1 discrepant records
  * Year **2016**: 1 discrepant records
  * Year **2017**: 2 discrepant records
  * Year **2018**: 236 discrepant records
  * Year **2019**: 774 discrepant records
  * Year **2020**: 608 discrepant records
  * Year **2021**: 1105 discrepant records
  * Year **2022**: 2294 discrepant records
  * Year **2023**: 2596 discrepant records
  * Year **2024**: 2455 discrepant records
  * Year **2025**: 3228 discrepant records
  * Year **N/A**: 853 discrepant records
* **By Dimension Group**:
  * Group **1. HTT SP - Ibirapuera**: 3590 discrepant records
  * Group **10. HTT SP - DOE**: 452 discrepant records
  * Group **11. HTT SP - Alphaville**: 32 discrepant records
  * Group **13. HTT SP - Bauru**: 17 discrepant records
  * Group **14. Unidade Salvador Camaçari**: 1 discrepant records
  * Group **15. Unidade Salvador Feira de Santana**: 3 discrepant records
  * Group **2. HTT SP - Vila Mariana**: 1660 discrepant records
  * Group **3. HTT SP - ProFIV**: 895 discrepant records
  * Group **4. HTT SP - Campinas**: 634 discrepant records
  * Group **5. HTT Belo Horizonte**: 4364 discrepant records
  * Group **6. HTT Brasília**: 884 discrepant records
  * Group **8. Unidade Salvador**: 996 discrepant records
  * Group **9. Unidade Salvador Insemina**: 245 discrepant records
  * Group **N/A**: 382 discrepant records

#### **Anonymized Column Discrepancy Samples**
| Composite Key | Column Name | DuckDB Value | Athena Value |
| :--- | :--- | :--- | :--- |
| prontuario=100742 | `billing_first_date` | `2022-12-08 00:00:00` | `None` |
| prontuario=105933 | `billing_first_date` | `2022-03-29 00:00:00` | `None` |
| prontuario=107805 | `billing_first_date` | `2024-07-30 00:00:00` | `None` |
| prontuario=107929 | `billing_first_date` | `2023-10-24 00:00:00` | `None` |
| prontuario=109603 | `billing_first_date` | `2022-10-14 00:00:00` | `None` |
| prontuario=110898 | `billing_first_date` | `2022-07-23 00:00:00` | `None` |
| prontuario=111345 | `billing_first_date` | `2022-09-22 00:00:00` | `None` |
| prontuario=111433 | `billing_first_date` | `2024-12-17 00:00:00` | `None` |
| prontuario=112660 | `billing_first_date` | `2022-04-19 00:00:00` | `None` |
| prontuario=115037 | `billing_first_date` | `2022-04-13 00:00:00` | `None` |
| prontuario=100742 | `billing_last_date` | `2022-12-08 00:00:00` | `None` |
| prontuario=105933 | `billing_last_date` | `2024-12-18 00:00:00` | `None` |
| prontuario=107805 | `billing_last_date` | `2025-07-25 00:00:00` | `None` |
| prontuario=107929 | `billing_last_date` | `2023-10-24 00:00:00` | `None` |
| prontuario=109603 | `billing_last_date` | `2022-10-14 00:00:00` | `None` |
| prontuario=110898 | `billing_last_date` | `2024-04-29 00:00:00` | `None` |
| prontuario=111345 | `billing_last_date` | `2025-10-31 00:00:00` | `None` |
| prontuario=111433 | `billing_last_date` | `2024-12-17 00:00:00` | `None` |
| prontuario=112660 | `billing_last_date` | `2022-04-19 00:00:00` | `None` |
| prontuario=115037 | `billing_last_date` | `2022-04-13 00:00:00` | `None` |
| prontuario=117719 | `cycle_total` | `3.0` | `4` |
| prontuario=119306 | `cycle_total` | `2.0` | `3` |
| prontuario=122879 | `cycle_total` | `1.0` | `2` |
| prontuario=123570 | `cycle_total` | `1.0` | `3` |
| prontuario=124805 | `cycle_total` | `0.0` | `1` |
| prontuario=132876 | `cycle_total` | `0.0` | `3` |
| prontuario=132960 | `cycle_total` | `0.0` | `1` |
| prontuario=134546 | `cycle_total` | `2.0` | `4` |
| prontuario=136384 | `cycle_total` | `3.0` | `4` |
| prontuario=136601 | `cycle_total` | `1.0` | `2` |
| prontuario=115426 | `cycle_with_transfer` | `0.0` | `1` |
| prontuario=119306 | `cycle_with_transfer` | `2.0` | `3` |
| prontuario=122879 | `cycle_with_transfer` | `1.0` | `2` |
| prontuario=123570 | `cycle_with_transfer` | `1.0` | `3` |
| prontuario=124805 | `cycle_with_transfer` | `0.0` | `1` |
| prontuario=129543 | `cycle_with_transfer` | `0.0` | `1` |
| prontuario=132876 | `cycle_with_transfer` | `0.0` | `3` |
| prontuario=132960 | `cycle_with_transfer` | `0.0` | `1` |
| prontuario=134546 | `cycle_with_transfer` | `2.0` | `4` |
| prontuario=135734 | `cycle_with_transfer` | `0.0` | `1` |
| prontuario=115426 | `cycle_without_transfer` | `1.0` | `0` |
| prontuario=117719 | `cycle_without_transfer` | `1.0` | `2` |
| prontuario=129543 | `cycle_without_transfer` | `2.0` | `1` |
| prontuario=135734 | `cycle_without_transfer` | `1.0` | `0` |
| prontuario=136229 | `cycle_without_transfer` | `1.0` | `0` |
| prontuario=136601 | `cycle_without_transfer` | `1.0` | `2` |
| prontuario=140410 | `cycle_without_transfer` | `2.0` | `4` |
| prontuario=141524 | `cycle_without_transfer` | `1.0` | `0` |
| prontuario=141712 | `cycle_without_transfer` | `1.0` | `0` |
| prontuario=143476 | `cycle_without_transfer` | `1.0` | `0` |
| prontuario=100742 | `flag_cycles_no_payments` | `0` | `None` |
| prontuario=104827 | `flag_cycles_no_payments` | `1` | `None` |
| prontuario=105933 | `flag_cycles_no_payments` | `0` | `None` |
| prontuario=106361 | `flag_cycles_no_payments` | `1` | `None` |
| prontuario=107805 | `flag_cycles_no_payments` | `0` | `None` |
| prontuario=107929 | `flag_cycles_no_payments` | `0` | `None` |
| prontuario=109603 | `flag_cycles_no_payments` | `0` | `None` |
| prontuario=110208 | `flag_cycles_no_payments` | `1` | `None` |
| prontuario=110898 | `flag_cycles_no_payments` | `0` | `None` |
| prontuario=111345 | `flag_cycles_no_payments` | `0` | `None` |
| prontuario=175437 | `flag_is_donor` | `0` | `1` |
| prontuario=611203 | `flag_is_donor` | `0` | `1` |
| prontuario=681656 | `flag_is_donor` | `0` | `1` |
| prontuario=692318 | `flag_is_donor` | `0` | `1` |
| prontuario=754808 | `flag_is_donor` | `0` | `1` |
| prontuario=779052 | `flag_is_donor` | `0` | `1` |
| prontuario=793563 | `flag_is_donor` | `0` | `1` |
| prontuario=796910 | `flag_is_donor` | `0` | `1` |
| prontuario=797788 | `flag_is_donor` | `0` | `1` |
| prontuario=809660 | `flag_is_donor` | `0` | `1` |
| prontuario=100742 | `flag_less_cycles_than_payments` | `0` | `None` |
| prontuario=104827 | `flag_less_cycles_than_payments` | `0` | `None` |
| prontuario=105933 | `flag_less_cycles_than_payments` | `0` | `None` |
| prontuario=106361 | `flag_less_cycles_than_payments` | `0` | `None` |
| prontuario=107805 | `flag_less_cycles_than_payments` | `1` | `None` |
| prontuario=107929 | `flag_less_cycles_than_payments` | `0` | `None` |
| prontuario=109603 | `flag_less_cycles_than_payments` | `0` | `None` |
| prontuario=110208 | `flag_less_cycles_than_payments` | `0` | `None` |
| prontuario=110898 | `flag_less_cycles_than_payments` | `0` | `None` |
| prontuario=111345 | `flag_less_cycles_than_payments` | `0` | `None` |
| prontuario=100742 | `flag_more_cycles_than_payments` | `0` | `None` |
| prontuario=104827 | `flag_more_cycles_than_payments` | `1` | `None` |
| prontuario=105933 | `flag_more_cycles_than_payments` | `1` | `None` |
| prontuario=106361 | `flag_more_cycles_than_payments` | `1` | `None` |
| prontuario=107805 | `flag_more_cycles_than_payments` | `0` | `None` |
| prontuario=107929 | `flag_more_cycles_than_payments` | `0` | `None` |
| prontuario=109603 | `flag_more_cycles_than_payments` | `0` | `None` |
| prontuario=110208 | `flag_more_cycles_than_payments` | `1` | `None` |
| prontuario=110898 | `flag_more_cycles_than_payments` | `0` | `None` |
| prontuario=111345 | `flag_more_cycles_than_payments` | `0` | `None` |
| prontuario=100742 | `flag_no_cycles_but_payments` | `0` | `None` |
| prontuario=104827 | `flag_no_cycles_but_payments` | `0` | `None` |
| prontuario=105933 | `flag_no_cycles_but_payments` | `0` | `None` |
| prontuario=106361 | `flag_no_cycles_but_payments` | `0` | `None` |
| prontuario=107805 | `flag_no_cycles_but_payments` | `0` | `None` |
| prontuario=107929 | `flag_no_cycles_but_payments` | `0` | `None` |
| prontuario=109603 | `flag_no_cycles_but_payments` | `0` | `None` |
| prontuario=110208 | `flag_no_cycles_but_payments` | `0` | `None` |
| prontuario=110898 | `flag_no_cycles_but_payments` | `0` | `None` |
| prontuario=111345 | `flag_no_cycles_but_payments` | `0` | `None` |
| prontuario=122879 | `medico_nome` | `Eduardo Leme Alves da Motta` | `Livia Munhoz` |
| prontuario=132876 | `medico_nome` | `nan` | `Claudia Gomes Padilla` |
| prontuario=138256 | `medico_nome` | `nan` | `Arnaldo Schizzi Cambiaghi` |
| prontuario=142170 | `medico_nome` | `nan` | `Claudia Gomes Padilla` |
| prontuario=144077 | `medico_nome` | `Michele Quaranta Panzan ` | `Paula Beatriz Tavares Fettback` |
| prontuario=144125 | `medico_nome` | `nan` | `Eduardo Leme Alves da Motta` |
| prontuario=144186 | `medico_nome` | `Mauricio Chehin` | `Arnaldo Schizzi Cambiaghi` |
| prontuario=147742 | `medico_nome` | `nan` | `Mauricio Chehin` |
| prontuario=150319 | `medico_nome` | `nan` | `Raphaela Menin Franco Martins` |
| prontuario=150326 | `medico_nome` | `nan` | `Arnaldo Schizzi Cambiaghi` |
| prontuario=512612 | `prontuario_genitores` | `184127.0` | `512612` |
| prontuario=653214 | `prontuario_genitores` | `653214.0` | `0` |
| prontuario=665890 | `prontuario_genitores` | `665890.0` | `0` |
| prontuario=780950 | `prontuario_genitores` | `780950.0` | `0` |
| prontuario=793423 | `prontuario_genitores` | `793423.0` | `0` |
| prontuario=796573 | `prontuario_genitores` | `775647.0` | `1` |
| prontuario=808245 | `prontuario_genitores` | `808245.0` | `0` |
| prontuario=836751 | `prontuario_genitores` | `827681.0` | `836751` |
| prontuario=883727 | `prontuario_genitores` | `883727.0` | `0` |
| prontuario=100742 | `timeline_first_date` | `2022-12-08 00:00:00` | `2022-12-08` |
| prontuario=104827 | `timeline_first_date` | `2022-06-11 00:00:00` | `2022-06-11` |
| prontuario=105933 | `timeline_first_date` | `2022-04-09 00:00:00` | `2022-04-09` |
| prontuario=106361 | `timeline_first_date` | `2021-11-10 00:00:00` | `2021-11-10` |
| prontuario=107805 | `timeline_first_date` | `2025-01-19 00:00:00` | `2025-01-19` |
| prontuario=107929 | `timeline_first_date` | `2023-10-27 00:00:00` | `2023-10-27` |
| prontuario=109603 | `timeline_first_date` | `2022-12-12 00:00:00` | `2022-12-12` |
| prontuario=110208 | `timeline_first_date` | `2022-07-08 00:00:00` | `2022-07-08` |
| prontuario=110898 | `timeline_first_date` | `2022-07-23 00:00:00` | `2022-07-23` |
| prontuario=111345 | `timeline_first_date` | `2022-10-13 00:00:00` | `2022-10-13` |
| prontuario=100742 | `timeline_last_date` | `2022-12-08 00:00:00` | `2022-12-08` |
| prontuario=104827 | `timeline_last_date` | `2023-08-15 00:00:00` | `2023-08-15` |
| prontuario=105933 | `timeline_last_date` | `2025-01-20 00:00:00` | `2025-01-20` |
| prontuario=106361 | `timeline_last_date` | `2021-11-10 00:00:00` | `2021-11-10` |
| prontuario=107805 | `timeline_last_date` | `2025-07-22 00:00:00` | `2025-07-22` |
| prontuario=107929 | `timeline_last_date` | `2023-10-27 00:00:00` | `2023-10-27` |
| prontuario=109603 | `timeline_last_date` | `2022-12-12 00:00:00` | `2022-12-12` |
| prontuario=110208 | `timeline_last_date` | `2022-07-08 00:00:00` | `2022-07-08` |
| prontuario=110898 | `timeline_last_date` | `2024-04-29 00:00:00` | `2024-04-29` |
| prontuario=111345 | `timeline_last_date` | `2025-11-08 00:00:00` | `2025-11-08` |
| prontuario=116302 | `timeline_unidade` | `5. HTT Belo Horizonte` | `6. HTT Belo Horizonte` |
| prontuario=117800 | `timeline_unidade` | `nan` | `Não informado` |
| prontuario=119865 | `timeline_unidade` | `6. HTT Brasília` | `7. HTT Brasília` |
| prontuario=120305 | `timeline_unidade` | `nan` | `Não informado` |
| prontuario=122879 | `timeline_unidade` | `1. HTT SP - Ibirapuera` | `2. HTT SP - Vila Mariana` |
| prontuario=123792 | `timeline_unidade` | `nan` | `Não informado` |
| prontuario=125806 | `timeline_unidade` | `6. HTT Brasília` | `7. HTT Brasília` |
| prontuario=126395 | `timeline_unidade` | `nan` | `Não informado` |
| prontuario=126802 | `timeline_unidade` | `nan` | `Não informado` |
| prontuario=127051 | `timeline_unidade` | `10. HTT SP - DOE` | `5. HTT SP - DOE` |
| prontuario=100742 | `treatment_paid_count` | `1.0` | `0` |
| prontuario=105933 | `treatment_paid_count` | `4.0` | `0` |
| prontuario=107805 | `treatment_paid_count` | `4.0` | `0` |
| prontuario=107929 | `treatment_paid_count` | `1.0` | `0` |
| prontuario=109603 | `treatment_paid_count` | `1.0` | `0` |
| prontuario=110898 | `treatment_paid_count` | `2.0` | `0` |
| prontuario=111345 | `treatment_paid_count` | `3.0` | `0` |
| prontuario=111433 | `treatment_paid_count` | `1.0` | `0` |
| prontuario=112660 | `treatment_paid_count` | `1.0` | `0` |
| prontuario=115037 | `treatment_paid_count` | `1.0` | `0` |
| prontuario=100742 | `treatment_paid_total` | `9327.6` | `0.0` |
| prontuario=105933 | `treatment_paid_total` | `72811.36` | `0.0` |
| prontuario=107805 | `treatment_paid_total` | `25275.0` | `0.0` |
| prontuario=107929 | `treatment_paid_total` | `16410.0` | `0.0` |
| prontuario=109603 | `treatment_paid_total` | `10364.0` | `0.0` |
| prontuario=110898 | `treatment_paid_total` | `45995.0` | `0.0` |
| prontuario=111345 | `treatment_paid_total` | `45788.0` | `0.0` |
| prontuario=111433 | `treatment_paid_total` | `4840.0` | `0.0` |
| prontuario=112660 | `treatment_paid_total` | `14115.0` | `0.0` |
| prontuario=115037 | `treatment_paid_total` | `6794.9` | `0.0` |

### `biopsy_pgta_timeline`
#### **Numeric Metric Alignment**
| Column Name | DuckDB Sum | Athena Sum | Difference | Alignment Rate |
| :--- | :---: | :---: | :---: | :---: |
| `biopsy_count` | 0.00 | 0.00 | +0.00 | 100.0000% |
| `pgta_test_count` | 0.00 | 0.00 | +0.00 | 100.0000% |
| `cumulative_biopsy_count` | 0.00 | 0.00 | +0.00 | 100.0000% |
| `cumulative_pgta_test_count` | 0.00 | 0.00 | +0.00 | 100.0000% |

### `consultas_timeline`
#### **Numeric Metric Alignment**
| Column Name | DuckDB Sum | Athena Sum | Difference | Alignment Rate |
| :--- | :---: | :---: | :---: | :---: |
| `consultas_events_count` | 0.00 | 0.00 | +0.00 | 100.0000% |
| `billing_events_count` | 0.00 | 0.00 | +0.00 | 100.0000% |
| `total_billing_amount` | 0.00 | 0.00 | +0.00 | 100.0000% |
| `total_quantity` | 0.00 | 0.00 | +0.00 | 100.0000% |

### `cryopreservation_events_timeline`
#### **Numeric Metric Alignment**
| Column Name | DuckDB Sum | Athena Sum | Difference | Alignment Rate |
| :--- | :---: | :---: | :---: | :---: |
| `freezing_events_count` | 0.00 | 0.00 | +0.00 | 100.0000% |
| `billing_events_count` | 0.00 | 0.00 | +0.00 | 100.0000% |
| `total_billing_amount` | 0.00 | 0.00 | +0.00 | 100.0000% |

### `embryo_freeze_timeline`
#### **Numeric Metric Alignment**
| Column Name | DuckDB Sum | Athena Sum | Difference | Alignment Rate |
| :--- | :---: | :---: | :---: | :---: |
| `embryos_frozen_count` | 0.00 | 0.00 | +0.00 | 100.0000% |
| `embryos_unfrozen_count` | 0.00 | 0.00 | +0.00 | 100.0000% |
| `embryos_discarded_count` | 0.00 | 0.00 | +0.00 | 100.0000% |
| `embryos_storage_balance` | 0.00 | 0.00 | +0.00 | 100.0000% |

### `embryoscope_timeline`
#### **Numeric Metric Alignment**
| Column Name | DuckDB Sum | Athena Sum | Difference | Alignment Rate |
| :--- | :---: | :---: | :---: | :---: |
| `embryoscope_usage_count` | 0.00 | 0.00 | +0.00 | 100.0000% |
| `billing_events_count` | 0.00 | 0.00 | +0.00 | 100.0000% |
| `total_billing_amount` | 0.00 | 0.00 | +0.00 | 100.0000% |

### `all_patients_timeline`
#### **Schema Mismatches**
* Columns in Athena Only: `table_order`
### `recent_patients_timeline`
#### **Schema Mismatches**
* Columns in Athena Only: `table_order`
### `embryoscope_embrioes`
#### **Schema Mismatches**
* Columns in DuckDB Only: `embryo_Name_BlastExpandLast`, `embryo_Name_BlastomereSize`, `embryo_Name_EVEN2`, `embryo_Name_EVEN4`, `embryo_Name_EVEN8`, `embryo_Name_FRAG2`, `embryo_Name_FRAG2CAT`, `embryo_Name_FRAG4`, `embryo_Name_FRAG8`, `embryo_Name_Fragmentation`, `embryo_Name_ICM`, `embryo_Name_Line`, `embryo_Name_MN2Type`, `embryo_Name_MorphologicalGrade`, `embryo_Name_MultiNucleation`, `embryo_Name_Nuclei2`, `embryo_Name_PN`, `embryo_Name_Pulsing`, `embryo_Name_ReexpansionCount`, `embryo_Name_Strings`, `embryo_Name_TE`, `embryo_Name_t2`, `embryo_Name_t3`, `embryo_Name_t4`, `embryo_Name_t5`, `embryo_Name_t6`, `embryo_Name_t7`, `embryo_Name_t8`, `embryo_Name_t9`, `embryo_Name_tB`, `embryo_Name_tEB`, `embryo_Name_tHB`, `embryo_Name_tM`, `embryo_Name_tPB2`, `embryo_Name_tPNa`, `embryo_Name_tPNf`, `embryo_Name_tSB`, `embryo_Name_tSC`, `embryo_Time_BlastExpandLast`, `embryo_Time_BlastomereSize`, `embryo_Time_EVEN2`, `embryo_Time_EVEN4`, `embryo_Time_EVEN8`, `embryo_Time_FRAG2`, `embryo_Time_FRAG2CAT`, `embryo_Time_FRAG4`, `embryo_Time_FRAG8`, `embryo_Time_ICM`, `embryo_Time_Line`, `embryo_Time_MN2Type`, `embryo_Time_MorphologicalGrade`, `embryo_Time_MultiNucleation`, `embryo_Time_Nuclei2`, `embryo_Time_PN`, `embryo_Time_Pulsing`, `embryo_Time_ReexpansionCount`, `embryo_Time_Strings`, `embryo_Time_TE`, `embryo_Time_t6`, `embryo_Time_t7`, `embryo_Time_t9`, `embryo_Time_tPB2`, `embryo_Timestamp_BlastExpandLast`, `embryo_Timestamp_BlastomereSize`, `embryo_Timestamp_EVEN2`, `embryo_Timestamp_EVEN4`, `embryo_Timestamp_EVEN8`, `embryo_Timestamp_FRAG2`, `embryo_Timestamp_FRAG2CAT`, `embryo_Timestamp_FRAG4`, `embryo_Timestamp_FRAG8`, `embryo_Timestamp_Fragmentation`, `embryo_Timestamp_ICM`, `embryo_Timestamp_Line`, `embryo_Timestamp_MN2Type`, `embryo_Timestamp_MorphologicalGrade`, `embryo_Timestamp_MultiNucleation`, `embryo_Timestamp_Nuclei2`, `embryo_Timestamp_PN`, `embryo_Timestamp_Pulsing`, `embryo_Timestamp_ReexpansionCount`, `embryo_Timestamp_Strings`, `embryo_Timestamp_TE`, `embryo_Timestamp_t2`, `embryo_Timestamp_t3`, `embryo_Timestamp_t4`, `embryo_Timestamp_t5`, `embryo_Timestamp_t6`, `embryo_Timestamp_t7`, `embryo_Timestamp_t8`, `embryo_Timestamp_t9`, `embryo_Timestamp_tB`, `embryo_Timestamp_tEB`, `embryo_Timestamp_tHB`, `embryo_Timestamp_tM`, `embryo_Timestamp_tPB2`, `embryo_Timestamp_tPNa`, `embryo_Timestamp_tPNf`, `embryo_Timestamp_tSB`, `embryo_Timestamp_tSC`, `embryo_Value_BlastExpandLast`, `embryo_Value_BlastomereSize`, `embryo_Value_EVEN2`, `embryo_Value_EVEN4`, `embryo_Value_EVEN8`, `embryo_Value_FRAG2`, `embryo_Value_FRAG4`, `embryo_Value_FRAG8`, `embryo_Value_Fragmentation`, `embryo_Value_Line`, `embryo_Value_MorphologicalGrade`, `embryo_Value_MultiNucleation`, `embryo_Value_Strings`, `embryo_Value_t2`, `embryo_Value_t3`, `embryo_Value_t4`, `embryo_Value_t5`, `embryo_Value_t6`, `embryo_Value_t7`, `embryo_Value_t8`, `embryo_Value_t9`, `embryo_Value_tB`, `embryo_Value_tEB`, `embryo_Value_tHB`, `embryo_Value_tM`, `embryo_Value_tPB2`, `embryo_Value_tPNa`, `embryo_Value_tPNf`, `embryo_Value_tSB`, `embryo_Value_tSC`, `idascore_IDAScore`, `idascore_IDATime`, `idascore_IDAVersion`, `patient_FirstName`, `patient_LastName`, `patient_unit_huntington`
* Columns in Athena Only: `first_name`, `last_name`, `patient_sk`, `unit_huntington`
#### **Discrepancy Group Isolation**
* **By Year/Date**:
  * Year **2017**: 362 discrepant records
  * Year **2018**: 2814 discrepant records
  * Year **2019**: 12339 discrepant records
  * Year **2020**: 14668 discrepant records
  * Year **2021**: 18120 discrepant records
  * Year **2022**: 16862 discrepant records
  * Year **2023**: 16948 discrepant records
  * Year **2024**: 15733 discrepant records
  * Year **2025**: 10869 discrepant records
  * Year **2026**: 7062 discrepant records
* **By Dimension Group**:
  * Group **N/A**: 115777 discrepant records

#### **Anonymized Column Discrepancy Samples**
| Composite Key | Column Name | DuckDB Value | Athena Value |
| :--- | :--- | :--- | :--- |
| embryo_embryo_id=d2018.10.21_s00592_i3027_p-4 | `age_at_fertilization` | `33.66` | `33.72` |
| embryo_embryo_id=d2018.10.21_s00592_i3027_p-6 | `age_at_fertilization` | `33.66` | `33.72` |
| embryo_embryo_id=d2018.10.21_s00592_i3027_p-16 | `age_at_fertilization` | `33.66` | `33.72` |
| embryo_embryo_id=d2018.10.21_s00592_i3027_p-7 | `age_at_fertilization` | `33.66` | `33.72` |
| embryo_embryo_id=d2021.04.03_s02041_i3027_p-6 | `age_at_fertilization` | `35.52` | `35.59` |
| embryo_embryo_id=d2021.04.03_s02041_i3027_p-7 | `age_at_fertilization` | `35.52` | `35.59` |
| embryo_embryo_id=d2021.04.03_s02041_i3027_p-1 | `age_at_fertilization` | `35.52` | `35.59` |
| embryo_embryo_id=d2021.04.03_s02041_i3027_p-2 | `age_at_fertilization` | `35.52` | `35.59` |
| embryo_embryo_id=d2021.04.03_s02041_i3027_p-3 | `age_at_fertilization` | `35.52` | `35.59` |
| embryo_embryo_id=d2021.04.03_s02041_i3027_p-8 | `age_at_fertilization` | `35.52` | `35.59` |
| embryo_embryo_id=d2023.08.03_s01887_i4120_p-8 | `embryo_description` | `nan` | `5BB` |
| embryo_embryo_id=d2023.08.03_s01887_i4120_p-3 | `embryo_description` | `nan` | `5AA` |
| embryo_embryo_id=d2023.08.03_s01887_i4120_p-4 | `embryo_description` | `nan` | `4AA` |
| embryo_embryo_id=d2023.08.03_s01887_i4120_p-7 | `embryo_description` | `nan` | `4BA` |
| embryo_embryo_id=d2023.08.03_s01887_i4120_p-5 | `embryo_description` | `nan` | `5BB` |
| embryo_embryo_id=d2023.08.03_s01887_i4120_p-2 | `embryo_description` | `nan` | `5BB` |
| embryo_embryo_id=d2023.03.11_s01804_i4120_p-6 | `embryo_description` | `nan` | `MOSAICO DE ALTO GRAU, MONOSSOMIA 22` |
| embryo_embryo_id=d2023.03.08_s01797_i4120_p-2 | `embryo_description` | `nan` | `MOSAICO ALTO GRAU XY` |
| embryo_embryo_id=d2023.03.08_s01798_i4120_p-4 | `embryo_description` | `nan` | `MOSAICO BAIXO GRAU XY` |
| embryo_embryo_id=d2023.03.01_s01790_i4120_p-8 | `embryo_description` | `nan` | `ANEUPLOIDE COMPLEXO MASCULINO` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-4 | `embryo_embryo_date` | `2026-06-15 00:00:00` | `2026-06-15` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-10 | `embryo_embryo_date` | `2026-06-15 00:00:00` | `2026-06-15` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-1 | `embryo_embryo_date` | `2026-06-15 00:00:00` | `2026-06-15` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-9 | `embryo_embryo_date` | `2026-06-15 00:00:00` | `2026-06-15` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-7 | `embryo_embryo_date` | `2026-06-15 00:00:00` | `2026-06-15` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-5 | `embryo_embryo_date` | `2026-06-15 00:00:00` | `2026-06-15` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-6 | `embryo_embryo_date` | `2026-06-15 00:00:00` | `2026-06-15` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-3 | `embryo_embryo_date` | `2026-06-15 00:00:00` | `2026-06-15` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-2 | `embryo_embryo_date` | `2026-06-15 00:00:00` | `2026-06-15` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-13 | `embryo_embryo_date` | `2026-06-15 00:00:00` | `2026-06-15` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-4 | `embryo_embryo_fate` | `Unknown` | `Freeze` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-10 | `embryo_embryo_fate` | `Unknown` | `Avoid` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-1 | `embryo_embryo_fate` | `Unknown` | `Freeze` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-9 | `embryo_embryo_fate` | `Unknown` | `Avoid` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-7 | `embryo_embryo_fate` | `Unknown` | `Avoid` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-5 | `embryo_embryo_fate` | `Unknown` | `Freeze` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-6 | `embryo_embryo_fate` | `Unknown` | `Avoid` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-3 | `embryo_embryo_fate` | `Unknown` | `Freeze` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-2 | `embryo_embryo_fate` | `Unknown` | `Freeze` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-13 | `embryo_embryo_fate` | `Unknown` | `Avoid` |
| embryo_embryo_id=d2023.03.22_s01822_i4120_p-5 | `embryo_fertilization_method` | `nan` | `ICSI` |
| embryo_embryo_id=d2023.03.22_s01822_i4120_p-6 | `embryo_fertilization_method` | `nan` | `ICSI` |
| embryo_embryo_id=d2023.03.22_s01822_i4120_p-8 | `embryo_fertilization_method` | `nan` | `ICSI` |
| embryo_embryo_id=d2023.03.22_s01822_i4120_p-2 | `embryo_fertilization_method` | `nan` | `ICSI` |
| embryo_embryo_id=d2023.03.22_s01822_i4120_p-7 | `embryo_fertilization_method` | `nan` | `ICSI` |
| embryo_embryo_id=d2023.03.22_s01822_i4120_p-9 | `embryo_fertilization_method` | `nan` | `ICSI` |
| embryo_embryo_id=d2023.03.22_s01822_i4120_p-3 | `embryo_fertilization_method` | `nan` | `ICSI` |
| embryo_embryo_id=d2023.03.22_s01822_i4120_p-10 | `embryo_fertilization_method` | `nan` | `ICSI` |
| embryo_embryo_id=d2023.03.22_s01822_i4120_p-4 | `embryo_fertilization_method` | `nan` | `ICSI` |
| embryo_embryo_id=d2023.03.22_s01822_i4120_p-1 | `embryo_fertilization_method` | `nan` | `ICSI` |
| embryo_embryo_id=d2026.05.13_s02626_i4120_p-9 | `embryo_kid_date` | `NaT` | `2026-05-19 00:00:00` |
| embryo_embryo_id=d2026.05.13_s02626_i4120_p-12 | `embryo_kid_date` | `NaT` | `2026-05-19 00:00:00` |
| embryo_embryo_id=d2026.05.13_s02626_i4120_p-1 | `embryo_kid_date` | `NaT` | `2026-05-19 00:00:00` |
| embryo_embryo_id=d2026.05.13_s02626_i4120_p-8 | `embryo_kid_date` | `NaT` | `2026-05-19 00:00:00` |
| embryo_embryo_id=d2026.05.13_s02626_i4120_p-7 | `embryo_kid_date` | `NaT` | `2026-05-19 00:00:00` |
| embryo_embryo_id=d2026.05.13_s02626_i4120_p-11 | `embryo_kid_date` | `NaT` | `2026-05-19 00:00:00` |
| embryo_embryo_id=d2026.05.13_s02626_i4120_p-10 | `embryo_kid_date` | `NaT` | `2026-05-19 00:00:00` |
| embryo_embryo_id=d2026.05.13_s02626_i4120_p-2 | `embryo_kid_date` | `NaT` | `2026-05-19 00:00:00` |
| embryo_embryo_id=d2026.05.13_s02626_i4120_p-3 | `embryo_kid_date` | `NaT` | `2026-05-19 00:00:00` |
| embryo_embryo_id=d2026.05.13_s02626_i4120_p-4 | `embryo_kid_date` | `NaT` | `2026-05-19 00:00:00` |
| embryo_embryo_id=d2026.05.13_s02626_i4120_p-9 | `embryo_kid_score` | `nan` | `1.6` |
| embryo_embryo_id=d2026.05.13_s02626_i4120_p-12 | `embryo_kid_score` | `nan` | `4.3` |
| embryo_embryo_id=d2026.05.13_s02626_i4120_p-1 | `embryo_kid_score` | `nan` | `5.2` |
| embryo_embryo_id=d2026.05.13_s02626_i4120_p-8 | `embryo_kid_score` | `nan` | `1.7` |
| embryo_embryo_id=d2026.05.13_s02626_i4120_p-7 | `embryo_kid_score` | `nan` | `1.6` |
| embryo_embryo_id=d2026.05.13_s02626_i4120_p-11 | `embryo_kid_score` | `nan` | `8.3` |
| embryo_embryo_id=d2026.05.13_s02626_i4120_p-10 | `embryo_kid_score` | `nan` | `0` |
| embryo_embryo_id=d2026.05.13_s02626_i4120_p-2 | `embryo_kid_score` | `nan` | `5.6` |
| embryo_embryo_id=d2026.05.13_s02626_i4120_p-3 | `embryo_kid_score` | `nan` | `8.9` |
| embryo_embryo_id=d2026.05.13_s02626_i4120_p-4 | `embryo_kid_score` | `nan` | `4.2` |
| embryo_embryo_id=d2026.05.13_s02626_i4120_p-9 | `embryo_kid_user` | `nan` | `ADMIN` |
| embryo_embryo_id=d2026.05.13_s02626_i4120_p-12 | `embryo_kid_user` | `nan` | `ADMIN` |
| embryo_embryo_id=d2026.05.13_s02626_i4120_p-1 | `embryo_kid_user` | `nan` | `ADMIN` |
| embryo_embryo_id=d2026.05.13_s02626_i4120_p-8 | `embryo_kid_user` | `nan` | `ADMIN` |
| embryo_embryo_id=d2026.05.13_s02626_i4120_p-7 | `embryo_kid_user` | `nan` | `ADMIN` |
| embryo_embryo_id=d2026.05.13_s02626_i4120_p-11 | `embryo_kid_user` | `nan` | `ADMIN` |
| embryo_embryo_id=d2026.05.13_s02626_i4120_p-10 | `embryo_kid_user` | `nan` | `ADMIN` |
| embryo_embryo_id=d2026.05.13_s02626_i4120_p-2 | `embryo_kid_user` | `nan` | `ADMIN` |
| embryo_embryo_id=d2026.05.13_s02626_i4120_p-3 | `embryo_kid_user` | `nan` | `ADMIN` |
| embryo_embryo_id=d2026.05.13_s02626_i4120_p-4 | `embryo_kid_user` | `nan` | `ADMIN` |
| embryo_embryo_id=d2026.05.13_s02626_i4120_p-9 | `embryo_kid_version` | `nan` | `KIDScoreD5 v3.3` |
| embryo_embryo_id=d2026.05.13_s02626_i4120_p-12 | `embryo_kid_version` | `nan` | `KIDScoreD5 v3.3` |
| embryo_embryo_id=d2026.05.13_s02626_i4120_p-1 | `embryo_kid_version` | `nan` | `KIDScoreD5 v3.3` |
| embryo_embryo_id=d2026.05.13_s02626_i4120_p-8 | `embryo_kid_version` | `nan` | `KIDScoreD5 v3.3` |
| embryo_embryo_id=d2026.05.13_s02626_i4120_p-7 | `embryo_kid_version` | `nan` | `KIDScoreD5 v3.3` |
| embryo_embryo_id=d2026.05.13_s02626_i4120_p-11 | `embryo_kid_version` | `nan` | `KIDScoreD5 v3.3` |
| embryo_embryo_id=d2026.05.13_s02626_i4120_p-10 | `embryo_kid_version` | `nan` | `KIDScoreD5 v3.3` |
| embryo_embryo_id=d2026.05.13_s02626_i4120_p-2 | `embryo_kid_version` | `nan` | `KIDScoreD5 v3.3` |
| embryo_embryo_id=d2026.05.13_s02626_i4120_p-3 | `embryo_kid_version` | `nan` | `KIDScoreD5 v3.3` |
| embryo_embryo_id=d2026.05.13_s02626_i4120_p-4 | `embryo_kid_version` | `nan` | `KIDScoreD5 v3.3` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-10 | `embryo_number` | `9` | `10` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-9 | `embryo_number` | `8` | `9` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-13 | `embryo_number` | `12` | `13` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-12 | `embryo_number` | `11` | `12` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-11 | `embryo_number` | `10` | `11` |
| embryo_embryo_id=d2026.06.10_s02646_i4120_p-8 | `embryo_number` | `7` | `8` |
| embryo_embryo_id=d2026.06.11_s02647_i4120_p-2 | `embryo_number` | `16` | `18` |
| embryo_embryo_id=d2026.06.11_s02647_i4120_p-3 | `embryo_number` | `17` | `19` |
| embryo_embryo_id=d2026.06.11_s02647_i4120_p-4 | `embryo_number` | `18` | `20` |
| embryo_embryo_id=d2026.06.10_s02646_i4120_p-6 | `embryo_number` | `5` | `6` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-4 | `embryo_position` | `12` | `7` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-10 | `embryo_position` | `12` | `7` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-1 | `embryo_position` | `12` | `7` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-9 | `embryo_position` | `12` | `7` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-7 | `embryo_position` | `12` | `7` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-5 | `embryo_position` | `12` | `7` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-6 | `embryo_position` | `12` | `7` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-3 | `embryo_position` | `12` | `7` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-2 | `embryo_position` | `12` | `7` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-13 | `embryo_position` | `12` | `7` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-4 | `embryo_time_fragmentation` | `25.2` | `62.6` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-10 | `embryo_time_fragmentation` | `22.6` | `59.7` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-1 | `embryo_time_fragmentation` | `22.1` | `63.8` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-9 | `embryo_time_fragmentation` | `23.9` | `53.4` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-7 | `embryo_time_fragmentation` | `28.3` | `70.5` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-5 | `embryo_time_fragmentation` | `25.0` | `58.1` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-6 | `embryo_time_fragmentation` | `26.4` | `63.6` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-3 | `embryo_time_fragmentation` | `22.2` | `63.4` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-2 | `embryo_time_fragmentation` | `24.2` | `68.0` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-13 | `embryo_time_fragmentation` | `23.4` | `71.6` |
| embryo_embryo_id=d2026.05.23_s02634_i4120_p-3 | `embryo_time_t2` | `nan` | `29.3` |
| embryo_embryo_id=d2026.05.23_s02634_i4120_p-2 | `embryo_time_t2` | `nan` | `28.3` |
| embryo_embryo_id=d2026.05.23_s02634_i4120_p-1 | `embryo_time_t2` | `nan` | `26.0` |
| embryo_embryo_id=d2026.05.23_s02632_i4120_p-8 | `embryo_time_t2` | `nan` | `29.7` |
| embryo_embryo_id=d2026.05.23_s02632_i4120_p-2 | `embryo_time_t2` | `nan` | `24.6` |
| embryo_embryo_id=d2026.05.23_s02632_i4120_p-9 | `embryo_time_t2` | `nan` | `34.1` |
| embryo_embryo_id=d2026.05.23_s02632_i4120_p-11 | `embryo_time_t2` | `nan` | `28.8` |
| embryo_embryo_id=d2026.05.23_s02632_i4120_p-10 | `embryo_time_t2` | `nan` | `27.8` |
| embryo_embryo_id=d2026.05.13_s02626_i4120_p-9 | `embryo_time_t2` | `nan` | `27.5` |
| embryo_embryo_id=d2026.05.13_s02626_i4120_p-12 | `embryo_time_t2` | `nan` | `22.3` |
| embryo_embryo_id=d2026.05.23_s02634_i4120_p-4 | `embryo_time_t3` | `nan` | `40.3` |
| embryo_embryo_id=d2026.05.23_s02634_i4120_p-3 | `embryo_time_t3` | `nan` | `39.6` |
| embryo_embryo_id=d2026.05.23_s02634_i4120_p-2 | `embryo_time_t3` | `nan` | `32.2` |
| embryo_embryo_id=d2026.05.23_s02634_i4120_p-1 | `embryo_time_t3` | `nan` | `36.5` |
| embryo_embryo_id=d2026.05.23_s02632_i4120_p-8 | `embryo_time_t3` | `nan` | `42.0` |
| embryo_embryo_id=d2026.05.23_s02632_i4120_p-13 | `embryo_time_t3` | `nan` | `24.7` |
| embryo_embryo_id=d2026.05.23_s02632_i4120_p-2 | `embryo_time_t3` | `nan` | `36.0` |
| embryo_embryo_id=d2026.05.23_s02632_i4120_p-9 | `embryo_time_t3` | `nan` | `37.5` |
| embryo_embryo_id=d2026.05.23_s02632_i4120_p-12 | `embryo_time_t3` | `nan` | `36.2` |
| embryo_embryo_id=d2026.05.23_s02632_i4120_p-11 | `embryo_time_t3` | `nan` | `39.0` |
| embryo_embryo_id=d2026.05.23_s02634_i4120_p-4 | `embryo_time_t4` | `nan` | `47.4` |
| embryo_embryo_id=d2026.05.23_s02634_i4120_p-3 | `embryo_time_t4` | `nan` | `42.5` |
| embryo_embryo_id=d2026.05.23_s02634_i4120_p-2 | `embryo_time_t4` | `nan` | `37.5` |
| embryo_embryo_id=d2026.05.23_s02634_i4120_p-1 | `embryo_time_t4` | `nan` | `36.9` |
| embryo_embryo_id=d2026.05.23_s02632_i4120_p-8 | `embryo_time_t4` | `nan` | `43.0` |
| embryo_embryo_id=d2026.05.23_s02632_i4120_p-13 | `embryo_time_t4` | `nan` | `35.1` |
| embryo_embryo_id=d2026.05.23_s02632_i4120_p-2 | `embryo_time_t4` | `nan` | `41.2` |
| embryo_embryo_id=d2026.05.23_s02632_i4120_p-9 | `embryo_time_t4` | `nan` | `39.6` |
| embryo_embryo_id=d2026.05.23_s02632_i4120_p-3 | `embryo_time_t4` | `nan` | `28.9` |
| embryo_embryo_id=d2026.05.23_s02632_i4120_p-12 | `embryo_time_t4` | `nan` | `46.1` |
| embryo_embryo_id=d2026.05.23_s02634_i4120_p-3 | `embryo_time_t5` | `nan` | `44.1` |
| embryo_embryo_id=d2026.05.23_s02634_i4120_p-2 | `embryo_time_t5` | `nan` | `38.3` |
| embryo_embryo_id=d2026.05.23_s02634_i4120_p-1 | `embryo_time_t5` | `nan` | `37.7` |
| embryo_embryo_id=d2026.05.23_s02632_i4120_p-8 | `embryo_time_t5` | `nan` | `59.0` |
| embryo_embryo_id=d2026.05.23_s02632_i4120_p-13 | `embryo_time_t5` | `nan` | `35.6` |
| embryo_embryo_id=d2026.05.23_s02632_i4120_p-2 | `embryo_time_t5` | `nan` | `46.9` |
| embryo_embryo_id=d2026.05.23_s02632_i4120_p-9 | `embryo_time_t5` | `nan` | `46.7` |
| embryo_embryo_id=d2026.05.23_s02632_i4120_p-3 | `embryo_time_t5` | `nan` | `41.9` |
| embryo_embryo_id=d2026.05.23_s02632_i4120_p-12 | `embryo_time_t5` | `nan` | `62.1` |
| embryo_embryo_id=d2026.05.23_s02632_i4120_p-5 | `embryo_time_t5` | `nan` | `40.1` |
| embryo_embryo_id=d2026.05.23_s02634_i4120_p-4 | `embryo_time_t8` | `nan` | `53.2` |
| embryo_embryo_id=d2026.05.23_s02634_i4120_p-3 | `embryo_time_t8` | `nan` | `58.8` |
| embryo_embryo_id=d2026.05.23_s02634_i4120_p-2 | `embryo_time_t8` | `nan` | `54.8` |
| embryo_embryo_id=d2026.05.23_s02634_i4120_p-1 | `embryo_time_t8` | `nan` | `66.0` |
| embryo_embryo_id=d2026.05.23_s02632_i4120_p-8 | `embryo_time_t8` | `nan` | `76.8` |
| embryo_embryo_id=d2026.05.23_s02632_i4120_p-13 | `embryo_time_t8` | `nan` | `51.1` |
| embryo_embryo_id=d2026.05.23_s02632_i4120_p-2 | `embryo_time_t8` | `nan` | `67.1` |
| embryo_embryo_id=d2026.05.23_s02632_i4120_p-9 | `embryo_time_t8` | `nan` | `60.9` |
| embryo_embryo_id=d2026.05.23_s02632_i4120_p-5 | `embryo_time_t8` | `nan` | `59.8` |
| embryo_embryo_id=d2026.05.23_s02632_i4120_p-11 | `embryo_time_t8` | `nan` | `67.9` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-4 | `embryo_time_tb` | `nan` | `94.8` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-1 | `embryo_time_tb` | `nan` | `108.8` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-5 | `embryo_time_tb` | `nan` | `116.0` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-3 | `embryo_time_tb` | `nan` | `94.6` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-2 | `embryo_time_tb` | `nan` | `108.3` |
| embryo_embryo_id=d2026.06.15_s02648_i4120_p-2 | `embryo_time_tb` | `nan` | `125.3` |
| embryo_embryo_id=d2026.06.06_s02643_i4120_p-2 | `embryo_time_tb` | `125.1` | `nan` |
| embryo_embryo_id=d2026.06.06_s02643_i4120_p-6 | `embryo_time_tb` | `133.3` | `nan` |
| embryo_embryo_id=d2026.06.04_s02641_i4120_p-1 | `embryo_time_tb` | `119.1` | `nan` |
| embryo_embryo_id=d2026.05.23_s02634_i4120_p-1 | `embryo_time_tb` | `nan` | `128.0` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-4 | `embryo_time_teb` | `nan` | `99.1` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-1 | `embryo_time_teb` | `nan` | `112.2` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-5 | `embryo_time_teb` | `nan` | `123.9` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-3 | `embryo_time_teb` | `nan` | `100.7` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-2 | `embryo_time_teb` | `nan` | `115.6` |
| embryo_embryo_id=d2026.06.15_s02648_i4120_p-2 | `embryo_time_teb` | `nan` | `128.9` |
| embryo_embryo_id=d2026.06.06_s02643_i4120_p-2 | `embryo_time_teb` | `126.7` | `nan` |
| embryo_embryo_id=d2026.06.06_s02643_i4120_p-4 | `embryo_time_teb` | `137.5` | `nan` |
| embryo_embryo_id=d2026.05.23_s02634_i4120_p-1 | `embryo_time_teb` | `nan` | `139.4` |
| embryo_embryo_id=d2026.05.23_s02632_i4120_p-8 | `embryo_time_teb` | `nan` | `125.9` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-4 | `embryo_time_thb` | `nan` | `100.8` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-1 | `embryo_time_thb` | `nan` | `113.2` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-5 | `embryo_time_thb` | `nan` | `131.7` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-3 | `embryo_time_thb` | `nan` | `102.5` |
| embryo_embryo_id=d2026.06.15_s02648_i4120_p-2 | `embryo_time_thb` | `nan` | `138.3` |
| embryo_embryo_id=d2026.06.06_s02643_i4120_p-2 | `embryo_time_thb` | `131.7` | `nan` |
| embryo_embryo_id=d2026.05.23_s02632_i4120_p-2 | `embryo_time_thb` | `nan` | `128.0` |
| embryo_embryo_id=d2026.05.23_s02632_i4120_p-9 | `embryo_time_thb` | `nan` | `115.7` |
| embryo_embryo_id=d2026.05.23_s02632_i4120_p-11 | `embryo_time_thb` | `nan` | `137.9` |
| embryo_embryo_id=d2026.05.13_s02626_i4120_p-11 | `embryo_time_thb` | `nan` | `107.7` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-4 | `embryo_time_tm` | `nan` | `76.6` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-1 | `embryo_time_tm` | `nan` | `72.1` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-5 | `embryo_time_tm` | `nan` | `92.3` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-6 | `embryo_time_tm` | `nan` | `80.2` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-3 | `embryo_time_tm` | `nan` | `82.6` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-2 | `embryo_time_tm` | `nan` | `88.8` |
| embryo_embryo_id=d2026.06.15_s02648_i4120_p-2 | `embryo_time_tm` | `nan` | `85.3` |
| embryo_embryo_id=d2026.06.04_s02641_i4120_p-1 | `embryo_time_tm` | `106.2` | `nan` |
| embryo_embryo_id=d2026.05.23_s02634_i4120_p-1 | `embryo_time_tm` | `nan` | `98.7` |
| embryo_embryo_id=d2026.05.23_s02632_i4120_p-8 | `embryo_time_tm` | `nan` | `107.5` |
| embryo_embryo_id=d2026.05.13_s02626_i4120_p-1 | `embryo_time_tpna` | `9.1` | `11.0` |
| embryo_embryo_id=d2026.02.23_s02574_i4120_p-6 | `embryo_time_tpna` | `nan` | `7.0` |
| embryo_embryo_id=d2025.09.29_s02477_i4120_p-1 | `embryo_time_tpna` | `nan` | `31.8` |
| embryo_embryo_id=d2025.08.14_s03082_i3254_p-9 | `embryo_time_tpna` | `nan` | `6.5` |
| embryo_embryo_id=d2026.05.23_s02634_i4120_p-4 | `embryo_time_tpnf` | `nan` | `28.9` |
| embryo_embryo_id=d2026.05.23_s02634_i4120_p-3 | `embryo_time_tpnf` | `nan` | `26.2` |
| embryo_embryo_id=d2026.05.23_s02634_i4120_p-2 | `embryo_time_tpnf` | `nan` | `26.4` |
| embryo_embryo_id=d2026.05.23_s02634_i4120_p-1 | `embryo_time_tpnf` | `nan` | `23.6` |
| embryo_embryo_id=d2026.05.23_s02632_i4120_p-8 | `embryo_time_tpnf` | `nan` | `27.5` |
| embryo_embryo_id=d2026.05.23_s02632_i4120_p-9 | `embryo_time_tpnf` | `nan` | `32.2` |
| embryo_embryo_id=d2026.05.23_s02632_i4120_p-3 | `embryo_time_tpnf` | `nan` | `27.2` |
| embryo_embryo_id=d2026.05.23_s02632_i4120_p-12 | `embryo_time_tpnf` | `nan` | `32.0` |
| embryo_embryo_id=d2026.05.23_s02632_i4120_p-5 | `embryo_time_tpnf` | `nan` | `38.1` |
| embryo_embryo_id=d2026.05.23_s02632_i4120_p-11 | `embryo_time_tpnf` | `nan` | `25.5` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-4 | `embryo_time_tsb` | `nan` | `86.1` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-1 | `embryo_time_tsb` | `nan` | `86.1` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-5 | `embryo_time_tsb` | `nan` | `98.1` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-3 | `embryo_time_tsb` | `nan` | `86.7` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-2 | `embryo_time_tsb` | `nan` | `91.3` |
| embryo_embryo_id=d2026.06.15_s02648_i4120_p-2 | `embryo_time_tsb` | `nan` | `99.4` |
| embryo_embryo_id=d2026.06.04_s02641_i4120_p-1 | `embryo_time_tsb` | `111.3` | `nan` |
| embryo_embryo_id=d2026.05.23_s02634_i4120_p-1 | `embryo_time_tsb` | `nan` | `102.4` |
| embryo_embryo_id=d2026.05.23_s02632_i4120_p-8 | `embryo_time_tsb` | `nan` | `109.3` |
| embryo_embryo_id=d2026.05.23_s02632_i4120_p-2 | `embryo_time_tsb` | `nan` | `99.2` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-4 | `embryo_time_tsc` | `nan` | `68.5` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-10 | `embryo_time_tsc` | `nan` | `93.3` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-1 | `embryo_time_tsc` | `nan` | `68.5` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-7 | `embryo_time_tsc` | `nan` | `88.5` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-5 | `embryo_time_tsc` | `nan` | `81.5` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-6 | `embryo_time_tsc` | `nan` | `73.4` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-3 | `embryo_time_tsc` | `nan` | `70.6` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-2 | `embryo_time_tsc` | `nan` | `77.8` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-13 | `embryo_time_tsc` | `nan` | `108.7` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-12 | `embryo_time_tsc` | `nan` | `77.9` |
| embryo_embryo_id=d2026.06.22_s04300_i3253_p-3 | `embryo_value_frag2cat` | `nan` | `10-20%` |
| embryo_embryo_id=d2026.06.22_s04300_i3253_p-1 | `embryo_value_frag2cat` | `nan` | `20-50%` |
| embryo_embryo_id=d2026.06.22_s04300_i3253_p-5 | `embryo_value_frag2cat` | `nan` | `10-20%` |
| embryo_embryo_id=d2026.06.22_s04300_i3253_p-6 | `embryo_value_frag2cat` | `nan` | `0-10%` |
| embryo_embryo_id=d2026.06.22_s04300_i3253_p-10 | `embryo_value_frag2cat` | `nan` | `10-20%` |
| embryo_embryo_id=d2026.06.22_s04300_i3253_p-9 | `embryo_value_frag2cat` | `nan` | `0-10%` |
| embryo_embryo_id=d2026.06.22_s04300_i3253_p-8 | `embryo_value_frag2cat` | `nan` | `0-10%` |
| embryo_embryo_id=d2026.06.22_s04299_i3253_p-1 | `embryo_value_frag2cat` | `nan` | `0-10%` |
| embryo_embryo_id=d2026.06.22_s04299_i3253_p-6 | `embryo_value_frag2cat` | `nan` | `0-10%` |
| embryo_embryo_id=d2026.06.22_s04299_i3253_p-7 | `embryo_value_frag2cat` | `nan` | `0-10%` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-4 | `embryo_value_icm` | `nan` | `A` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-1 | `embryo_value_icm` | `nan` | `A` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-5 | `embryo_value_icm` | `nan` | `B` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-3 | `embryo_value_icm` | `nan` | `A` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-2 | `embryo_value_icm` | `nan` | `A` |
| embryo_embryo_id=d2026.06.15_s02648_i4120_p-2 | `embryo_value_icm` | `nan` | `B` |
| embryo_embryo_id=d2026.06.06_s02643_i4120_p-2 | `embryo_value_icm` | `A` | `nan` |
| embryo_embryo_id=d2026.06.06_s02643_i4120_p-4 | `embryo_value_icm` | `B` | `nan` |
| embryo_embryo_id=d2026.06.06_s02643_i4120_p-6 | `embryo_value_icm` | `B` | `nan` |
| embryo_embryo_id=d2026.06.04_s02641_i4120_p-1 | `embryo_value_icm` | `A` | `nan` |
| embryo_embryo_id=d2026.06.22_s04300_i3253_p-3 | `embryo_value_mn2_type` | `nan` | `NA` |
| embryo_embryo_id=d2026.06.22_s04300_i3253_p-5 | `embryo_value_mn2_type` | `nan` | `Mono` |
| embryo_embryo_id=d2026.06.22_s04300_i3253_p-6 | `embryo_value_mn2_type` | `nan` | `Mono` |
| embryo_embryo_id=d2026.06.22_s04300_i3253_p-10 | `embryo_value_mn2_type` | `nan` | `NA` |
| embryo_embryo_id=d2026.06.22_s04300_i3253_p-9 | `embryo_value_mn2_type` | `nan` | `Mono` |
| embryo_embryo_id=d2026.06.22_s04300_i3253_p-8 | `embryo_value_mn2_type` | `nan` | `Mono` |
| embryo_embryo_id=d2026.06.22_s04299_i3253_p-1 | `embryo_value_mn2_type` | `nan` | `Multi` |
| embryo_embryo_id=d2026.06.22_s04299_i3253_p-6 | `embryo_value_mn2_type` | `nan` | `Mono` |
| embryo_embryo_id=d2026.06.22_s04299_i3253_p-7 | `embryo_value_mn2_type` | `nan` | `Mono` |
| embryo_embryo_id=d2026.06.22_s04299_i3253_p-5 | `embryo_value_mn2_type` | `nan` | `Mono` |
| embryo_embryo_id=d2026.06.22_s04300_i3253_p-3 | `embryo_value_nuclei2` | `nan` | `NA` |
| embryo_embryo_id=d2026.06.22_s04300_i3253_p-5 | `embryo_value_nuclei2` | `nan` | `2` |
| embryo_embryo_id=d2026.06.22_s04300_i3253_p-6 | `embryo_value_nuclei2` | `nan` | `0` |
| embryo_embryo_id=d2026.06.22_s04300_i3253_p-10 | `embryo_value_nuclei2` | `nan` | `NA` |
| embryo_embryo_id=d2026.06.22_s04300_i3253_p-9 | `embryo_value_nuclei2` | `nan` | `0` |
| embryo_embryo_id=d2026.06.22_s04300_i3253_p-8 | `embryo_value_nuclei2` | `nan` | `0` |
| embryo_embryo_id=d2026.06.22_s04299_i3253_p-1 | `embryo_value_nuclei2` | `nan` | `2` |
| embryo_embryo_id=d2026.06.22_s04299_i3253_p-6 | `embryo_value_nuclei2` | `nan` | `0` |
| embryo_embryo_id=d2026.06.22_s04299_i3253_p-7 | `embryo_value_nuclei2` | `nan` | `0` |
| embryo_embryo_id=d2026.06.22_s04299_i3253_p-5 | `embryo_value_nuclei2` | `nan` | `0` |
| embryo_embryo_id=d2026.05.23_s02632_i4120_p-3 | `embryo_value_pn` | `2` | `3` |
| embryo_embryo_id=d2026.04.24_s02612_i4120_p-6 | `embryo_value_pn` | `1` | `2` |
| embryo_embryo_id=d2025.10.20_s02499_i4120_p-3 | `embryo_value_pn` | `nan` | `2` |
| embryo_embryo_id=d2025.10.20_s02499_i4120_p-8 | `embryo_value_pn` | `2` | `3` |
| embryo_embryo_id=d2025.08.28_s02456_i4120_p-4 | `embryo_value_pn` | `2` | `3` |
| embryo_embryo_id=d2025.06.02_s02396_i4120_p-2 | `embryo_value_pn` | `2` | `3` |
| embryo_embryo_id=d2025.05.28_s02392_i4120_p-7 | `embryo_value_pn` | `1` | `2` |
| embryo_embryo_id=d2025.05.09_s02377_i4120_p-3 | `embryo_value_pn` | `0` | `2` |
| embryo_embryo_id=d2025.09.29_s02477_i4120_p-1 | `embryo_value_pn` | `0` | `2` |
| embryo_embryo_id=d2024.04.03_s02061_i4120_p-4 | `embryo_value_pn` | `2` | `4` |
| embryo_embryo_id=d2026.06.22_s04300_i3253_p-4 | `embryo_value_pulsing` | `nan` | `Yes` |
| embryo_embryo_id=d2026.06.22_s04300_i3253_p-6 | `embryo_value_pulsing` | `nan` | `No` |
| embryo_embryo_id=d2026.06.22_s04300_i3253_p-8 | `embryo_value_pulsing` | `nan` | `Yes` |
| embryo_embryo_id=d2026.06.22_s04300_i3253_p-7 | `embryo_value_pulsing` | `nan` | `Yes` |
| embryo_embryo_id=d2026.06.22_s04299_i3253_p-1 | `embryo_value_pulsing` | `nan` | `Yes` |
| embryo_embryo_id=d2026.06.22_s04299_i3253_p-6 | `embryo_value_pulsing` | `nan` | `Yes` |
| embryo_embryo_id=d2026.06.22_s04299_i3253_p-7 | `embryo_value_pulsing` | `nan` | `Yes` |
| embryo_embryo_id=d2026.06.22_s04299_i3253_p-5 | `embryo_value_pulsing` | `nan` | `Yes` |
| embryo_embryo_id=d2026.06.22_s04299_i3253_p-2 | `embryo_value_pulsing` | `nan` | `No` |
| embryo_embryo_id=d2026.06.22_s04299_i3253_p-3 | `embryo_value_pulsing` | `nan` | `Yes` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-4 | `embryo_value_te` | `nan` | `A` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-1 | `embryo_value_te` | `nan` | `B` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-5 | `embryo_value_te` | `nan` | `B` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-3 | `embryo_value_te` | `nan` | `A` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-2 | `embryo_value_te` | `nan` | `B` |
| embryo_embryo_id=d2026.06.15_s02648_i4120_p-2 | `embryo_value_te` | `nan` | `B` |
| embryo_embryo_id=d2026.06.06_s02643_i4120_p-2 | `embryo_value_te` | `A` | `nan` |
| embryo_embryo_id=d2026.06.06_s02643_i4120_p-4 | `embryo_value_te` | `B` | `nan` |
| embryo_embryo_id=d2026.06.06_s02643_i4120_p-6 | `embryo_value_te` | `B` | `nan` |
| embryo_embryo_id=d2026.06.04_s02641_i4120_p-1 | `embryo_value_te` | `B` | `nan` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-4 | `patient_date_of_birth` | `1986-07-22 00:00:00` | `1986-07-22` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-10 | `patient_date_of_birth` | `1986-07-22 00:00:00` | `1986-07-22` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-1 | `patient_date_of_birth` | `1986-07-22 00:00:00` | `1986-07-22` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-9 | `patient_date_of_birth` | `1986-07-22 00:00:00` | `1986-07-22` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-7 | `patient_date_of_birth` | `1986-07-22 00:00:00` | `1986-07-22` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-5 | `patient_date_of_birth` | `1986-07-22 00:00:00` | `1986-07-22` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-6 | `patient_date_of_birth` | `1986-07-22 00:00:00` | `1986-07-22` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-3 | `patient_date_of_birth` | `1986-07-22 00:00:00` | `1986-07-22` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-2 | `patient_date_of_birth` | `1986-07-22 00:00:00` | `1986-07-22` |
| embryo_embryo_id=d2026.06.15_s02649_i4120_p-13 | `patient_date_of_birth` | `1986-07-22 00:00:00` | `1986-07-22` |
| embryo_embryo_id=d2026.02.05_s02567_i4120_p-1 | `patient_id` | `736080` | `736.080` |
| embryo_embryo_id=d2026.02.05_s02567_i4120_p-3 | `patient_id` | `736080` | `736.080` |
| embryo_embryo_id=d2026.02.05_s02567_i4120_p-2 | `patient_id` | `736080` | `736.080` |
| embryo_embryo_id=d2023.05.05_s01847_i4120_p-2 | `patient_id` | `10` | `0.10` |
| embryo_embryo_id=d2023.05.05_s01847_i4120_p-1 | `patient_id` | `10` | `0.10` |
| embryo_embryo_id=d2023.05.05_s01847_i4120_p-3 | `patient_id` | `10` | `0.10` |
| embryo_embryo_id=d2023.05.05_s01847_i4120_p-4 | `patient_id` | `10` | `0.10` |
| embryo_embryo_id=d2023.05.05_s01847_i4120_p-7 | `patient_id` | `10` | `0.10` |
| embryo_embryo_id=d2023.05.05_s01847_i4120_p-8 | `patient_id` | `10` | `0.10` |
| embryo_embryo_id=d2023.05.10_s01851_i4120_p-6 | `patient_id` | `10` | `0.10` |
| embryo_embryo_id=d2026.04.02_s00064_i5521_p-3 | `patient_year_of_birth` | `<NA>` | `1989.0` |
| embryo_embryo_id=d2026.04.02_s00064_i5521_p-1 | `patient_year_of_birth` | `<NA>` | `1989.0` |
| embryo_embryo_id=d2026.04.02_s00063_i5521_p-15 | `patient_year_of_birth` | `<NA>` | `1989.0` |
| embryo_embryo_id=d2026.04.02_s00063_i5521_p-14 | `patient_year_of_birth` | `<NA>` | `1989.0` |
| embryo_embryo_id=d2026.04.02_s00063_i5521_p-13 | `patient_year_of_birth` | `<NA>` | `1989.0` |
| embryo_embryo_id=d2026.04.02_s00063_i5521_p-12 | `patient_year_of_birth` | `<NA>` | `1989.0` |
| embryo_embryo_id=d2026.04.02_s00063_i5521_p-11 | `patient_year_of_birth` | `<NA>` | `1989.0` |
| embryo_embryo_id=d2026.04.02_s00063_i5521_p-10 | `patient_year_of_birth` | `<NA>` | `1989.0` |
| embryo_embryo_id=d2026.04.02_s00063_i5521_p-8 | `patient_year_of_birth` | `<NA>` | `1989.0` |
| embryo_embryo_id=d2026.04.02_s00063_i5521_p-7 | `patient_year_of_birth` | `<NA>` | `1989.0` |
| embryo_embryo_id=d2026.02.05_s02567_i4120_p-1 | `prontuario` | `736080` | `-1` |
| embryo_embryo_id=d2026.02.05_s02567_i4120_p-3 | `prontuario` | `736080` | `-1` |
| embryo_embryo_id=d2026.02.05_s02567_i4120_p-2 | `prontuario` | `736080` | `-1` |
| embryo_embryo_id=d2025.10.31_s02509_i4120_p-5 | `prontuario` | `-1` | `896380` |
| embryo_embryo_id=d2025.10.31_s02509_i4120_p-6 | `prontuario` | `-1` | `896380` |
| embryo_embryo_id=d2025.10.31_s02509_i4120_p-8 | `prontuario` | `-1` | `896380` |
| embryo_embryo_id=d2025.10.31_s02509_i4120_p-9 | `prontuario` | `-1` | `896380` |
| embryo_embryo_id=d2025.10.31_s02509_i4120_p-10 | `prontuario` | `-1` | `896380` |
| embryo_embryo_id=d2025.10.31_s02509_i4120_p-7 | `prontuario` | `-1` | `896380` |
| embryo_embryo_id=d2025.10.31_s02509_i4120_p-11 | `prontuario` | `-1` | `896380` |
| embryo_embryo_id=d2022.03.25_s02557_i3027_p-2 | `treatment_name` | `2022-127` | `2022-127 | Merge` |
| embryo_embryo_id=d2022.08.06_s02804_i3027_p-1 | `treatment_name` | `2022-1063` | `2022-1063 | Merge` |
| embryo_embryo_id=d2022.08.06_s02804_i3027_p-2 | `treatment_name` | `2022-1063` | `2022-1063 | Merge` |
| embryo_embryo_id=d2022.03.25_s02556_i3027_p-1 | `treatment_name` | `2022-131` | `2022-131 | Merge` |
| embryo_embryo_id=d2022.03.25_s02556_i3027_p-2 | `treatment_name` | `2022-131` | `2022-131 | Merge` |
| embryo_embryo_id=d2022.03.25_s02556_i3027_p-3 | `treatment_name` | `2022-131` | `2022-131 | Merge` |
| embryo_embryo_id=d2022.03.25_s02556_i3027_p-6 | `treatment_name` | `2022-131` | `2022-131 | Merge` |
| embryo_embryo_id=d2022.03.25_s02556_i3027_p-7 | `treatment_name` | `2022-131` | `2022-131 | Merge` |
| embryo_embryo_id=d2022.03.25_s02556_i3027_p-8 | `treatment_name` | `2022-131` | `2022-131 | Merge` |
| embryo_embryo_id=d2022.03.25_s02556_i3027_p-5 | `treatment_name` | `2022-131` | `2022-131 | Merge` |

### `clinisys_embrioes`
#### **Schema Mismatches**
* Columns in DuckDB Only: `micro_CicloDoadora`, `micro_recepcao_ovulos`, `trat1_idade_esposa`, `trat1_idade_marido`, `trat1_prontuario_doadora`, `trat2_idade_esposa`, `trat2_idade_marido`, `trat2_prontuario_doadora`
#### **Discrepancy Group Isolation**
* **By Year/Date**:
  * Year **1981**: 3 discrepant records
  * Year **2000**: 7 discrepant records
  * Year **2010**: 23 discrepant records
  * Year **2011**: 22 discrepant records
  * Year **2012**: 7 discrepant records
  * Year **2016**: 32 discrepant records
  * Year **2017**: 12 discrepant records
  * Year **2019**: 75 discrepant records
  * Year **2020**: 1350 discrepant records
  * Year **2021**: 13864 discrepant records
  * Year **2022**: 22950 discrepant records
  * Year **2023**: 23920 discrepant records
  * Year **2024**: 23670 discrepant records
  * Year **2025**: 24630 discrepant records
  * Year **2026**: 10745 discrepant records
  * Year **N/A**: 133644 discrepant records
* **By Dimension Group**:
  * Group ** Adrianne Mary Leao Sette e Oliveira**: 10 discrepant records
  * Group **Alex  Sander Miguel**: 137 discrepant records
  * Group **Alex Sander Jose Miguel**: 8 discrepant records
  * Group **Alexander Kopelman**: 844 discrepant records
  * Group **Alice Campos de Pinho Tavares **: 16 discrepant records
  * Group **Aléssio Calil Mathias**: 7 discrepant records
  * Group **Amanda Matos **: 6 discrepant records
  * Group **Amanda Oliveira Cutalo Prates**: 422 discrepant records
  * Group **Ana Claudia Moura Trigo**: 599 discrepant records
  * Group **Ana Lucia Beltrame**: 459 discrepant records
  * Group **Ana Luiza Alvarenga Gomes**: 78 discrepant records
  * Group **Ana Luiza Mattos Tavares**: 1845 discrepant records
  * Group **Ana Luiza Nunes**: 2364 discrepant records
  * Group **Ana Paula Aquino**: 38749 discrepant records
  * Group **Ana Rita de Paiva Toledo**: 254 discrepant records
  * Group **Andre Giannini Rodrigues**: 11 discrepant records
  * Group **Andrea de Fatima Castro**: 456 discrepant records
  * Group **Anelisa Bueno Pereira**: 82 discrepant records
  * Group **Anna Luiza Moraes Souza**: 47 discrepant records
  * Group **Antonio Hochgreb De Freitas Junior**: 36 discrepant records
  * Group **Arnaldo S Cambiaghi**: 5332 discrepant records
  * Group **Arnaldo Schizzi Cambiaghi**: 9530 discrepant records
  * Group **Barbara Souza Melo**: 1358 discrepant records
  * Group **Beatriz Cabral Pires**: 26 discrepant records
  * Group **Beatriz Passaro Biscaro**: 976 discrepant records
  * Group **Beatriz Pavin de Toledo**: 136 discrepant records
  * Group **Benilson Eustaqio de Souza**: 43 discrepant records
  * Group **Bruna Barros Cavalcante**: 1022 discrepant records
  * Group **Bruna Costa Queiroz**: 698 discrepant records
  * Group **Bruna Cristina Lobo Santos**: 655 discrepant records
  * Group **Bruna Lobo Santos**: 171 discrepant records
  * Group **Camila Campos**: 401 discrepant records
  * Group **Camila Schizzi Cambiaghi**: 353 discrepant records
  * Group **Carla Maria Franco Dias **: 165 discrepant records
  * Group **Carla Martins**: 871 discrepant records
  * Group **Carlos Augusto Vieira de Moraes Filho**: 40 discrepant records
  * Group **Carolina Andrade Guedes dos Santos**: 24 discrepant records
  * Group **Carolina Zendron Machado Rudge**: 12 discrepant records
  * Group **Carolina de Andrade Melo e Souza**: 521 discrepant records
  * Group **Cesar Augusto Ferrari Teixeira**: 48 discrepant records
  * Group **Claudia Gomes Padilla**: 6696 discrepant records
  * Group **Cybele Lopes da Silva Lascala**: 11 discrepant records
  * Group **Daniel Suslik Zylbersztejn**: 61 discrepant records
  * Group **Daniela Boechat Gomide**: 1 discrepant records
  * Group **Daniela de Lima e Montes Castanho**: 97 discrepant records
  * Group **Daniella Spilborghs Castellotti**: 12056 discrepant records
  * Group **Davi Vischi Paluello**: 3 discrepant records
  * Group **Dayana Couto**: 1542 discrepant records
  * Group **Drauzio Oppenheimer**: 7 discrepant records
  * Group **Eduardo Leme Alves da Motta**: 11183 discrepant records
  * Group **Eduardo Yoneyama Mourthe**: 9 discrepant records
  * Group **Erica Becker de Sousa Xavier**: 8052 discrepant records
  * Group **Fabio Aiello Padilla**: 11 discrepant records
  * Group **Fabio Padilla**: 6 discrepant records
  * Group **Fabiola Cesconeto**: 119 discrepant records
  * Group **Fabyanne Mazutti da Silva **: 288 discrepant records
  * Group **Fernanda Marques Luz da Ressurreição**: 9 discrepant records
  * Group **Fernanda Peres Mastrocola**: 535 discrepant records
  * Group **Fernanda de Paula Rodrigues**: 3886 discrepant records
  * Group **Fernando Barboza De Lima **: 182 discrepant records
  * Group **Flavia Torelli**: 1101 discrepant records
  * Group **Flavio Ramirez Rosário**: 25 discrepant records
  * Group **Frederico Jose Silva Correa**: 786 discrepant records
  * Group **Fábio Costa Peixoto**: 5410 discrepant records
  * Group **Gabriela Franco Mourao de Oliveira**: 14 discrepant records
  * Group **Gabriela de Oliveira Ribeiro Lima**: 62 discrepant records
  * Group **Gabriella Dória Monteiro Cardoso**: 13 discrepant records
  * Group **Gabriella de Oliveira Ferreira**: 1129 discrepant records
  * Group **Geraldo Caldeira**: 11390 discrepant records
  * Group **Gersia Araujo Viana**: 2015 discrepant records
  * Group **Giuliana Gatto**: 309 discrepant records
  * Group **Guilherme Jacom Abdulmassih Wood**: 11 discrepant records
  * Group **Guilherme Leme de Souza**: 37 discrepant records
  * Group **Gustavo Comodo**: 273 discrepant records
  * Group **Gustavo Nardini Cecchino**: 2 discrepant records
  * Group **Gustavo Teles**: 1160 discrepant records
  * Group **Hanna Park**: 2231 discrepant records
  * Group **Helio Haddad Filho**: 388 discrepant records
  * Group **Herica Cristina Mendonça**: 7405 discrepant records
  * Group **Joao Frederico Luciano de Mello**: 11 discrepant records
  * Group **Joao Oscar Almeida Falcao Junior**: 22 discrepant records
  * Group **Joaquim Roberto Costa Lopes**: 1326 discrepant records
  * Group **Jorge Hallak**: 30 discrepant records
  * Group **Jose Geraldo Alves Caldeira**: 1694 discrepant records
  * Group **Josenice de Araujo SIlva Gomes**: 195 discrepant records
  * Group **João Pedro Junqueira Caetano**: 8401 discrepant records
  * Group **Juliana Halley Hatty**: 95 discrepant records
  * Group **Karen de Lima Souza Ortiz**: 48 discrepant records
  * Group **Karina de Sa Adami**: 35 discrepant records
  * Group **Karla Giusti Zacharias Fantin**: 5 discrepant records
  * Group **Keila Veludo Santos**: 18 discrepant records
  * Group **Laura Maria Almeida Maia**: 3606 discrepant records
  * Group **Lauriane G Schmidt De Abreu**: 330 discrepant records
  * Group **Layza Borges**: 3997 discrepant records
  * Group **Leci Veiga Caetano Amorim**: 6093 discrepant records
  * Group **Leonardo Araripe Dantas**: 111 discrepant records
  * Group **Leonardo Matheus Ribeiro Pereira**: 621 discrepant records
  * Group **Leticia Couto Motta Piccolo**: 18 discrepant records
  * Group **Lillian Silvestre Califre**: 11 discrepant records
  * Group **Livia Munhoz**: 4596 discrepant records
  * Group **Luana Lopes Toledo**: 854 discrepant records
  * Group **Luciana Campomizzi Calazans**: 5488 discrepant records
  * Group **Luciana Ferreira Potiguara Amador Sousa**: 859 discrepant records
  * Group **Luiz Fernando Bellintani **: 5 discrepant records
  * Group **Luiz Fernando Pina de Carvalho**: 50 discrepant records
  * Group **Manoela Porto Silva Resende**: 12 discrepant records
  * Group **Marcelo Afonso Goncalves**: 66 discrepant records
  * Group **Marcelo Afonso Gonçalves**: 429 discrepant records
  * Group **Marcelo Lopes Cancado**: 1870 discrepant records
  * Group **Marcos Eiji Shiroma**: 2512 discrepant records
  * Group **Marcos Eji Shiroma**: 208 discrepant records
  * Group **Maria Augusta Engler Tamm de Lima - VM**: 422 discrepant records
  * Group **Maria Juliana Albuquerque**: 2225 discrepant records
  * Group **Mariana Santana de A L Yoshida**: 14 discrepant records
  * Group **Mariana Santana de Almeida Liporoni Yoshida**: 8 discrepant records
  * Group **Marina de Melo Mendes**: 100 discrepant records
  * Group **Marjorie Fasolin **: 1076 discrepant records
  * Group **Matheus Teixeira Roque**: 2828 discrepant records
  * Group **Mauricio Barbour Chehin**: 603 discrepant records
  * Group **Mauricio Chehin**: 11072 discrepant records
  * Group **Melissa Cavagnoli**: 200 discrepant records
  * Group **Michele Quaranta Panzan **: 4369 discrepant records
  * Group **Médico Externo**: 86 discrepant records
  * Group **Médico TI Huntington**: 14 discrepant records
  * Group **Mônica de Oliveira Jorge**: 2 discrepant records
  * Group **N/A**: 4522 discrepant records
  * Group **Nina Rotsen Santos Ferreira**: 9 discrepant records
  * Group **Patricia Santos Marques**: 256 discrepant records
  * Group **Paula Beatriz (desativado)**: 16 discrepant records
  * Group **Paula Beatriz Tavares Fettback**: 10 discrepant records
  * Group **Paula Bortolai Martins Araujo**: 4547 discrepant records
  * Group **Paula Vieira Nunes Brito**: 563 discrepant records
  * Group **Paulo Homem de Mello Bianchi**: 15 discrepant records
  * Group **Pedro Ivan de Almeida Pretti**: 10 discrepant records
  * Group **Pedro Paulo Bastos Filho**: 96 discrepant records
  * Group **Priscila Morais Galvão Sousa**: 77 discrepant records
  * Group **Priscilla Lopes Caldeira**: 252 discrepant records
  * Group **Pró-FIV**: 1322 discrepant records
  * Group **RAIMUNDO  CESAR PINHEIRO**: 22 discrepant records
  * Group **Rafael Lacordia**: 2495 discrepant records
  * Group **Raimundo Cesar Pinheiro**: 11 discrepant records
  * Group **Raphaela Menin Franco Martins**: 1669 discrepant records
  * Group **Renata Fioravanti Schaal**: 204 discrepant records
  * Group **Renato Fraietta**: 182 discrepant records
  * Group **Ricardo Barini**: 11 discrepant records
  * Group **Ricardo Mello Marinho**: 2965 discrepant records
  * Group **Rodrigo da Rosa Filho**: 20 discrepant records
  * Group **Rogerio de Barros Ferreira Leao**: 6667 discrepant records
  * Group **Samara Laham**: 94 discrepant records
  * Group **Simone Portugal Silva Lima**: 487 discrepant records
  * Group **Sofia Andrade de Oliveira**: 3469 discrepant records
  * Group **Stephanie Majer Franceschini Cinquetti**: 5 discrepant records
  * Group **Tatiana Magalhães Aguiar **: 47 discrepant records
  * Group **Tatianna Quintas Furtado Ribeiro**: 53 discrepant records
  * Group **Thais Cambiaghi**: 43 discrepant records
  * Group **Thais Sanches Domingues**: 9733 discrepant records
  * Group **Thomas Gabriel Miklos**: 14 discrepant records
  * Group **Tomás Ribeiro Gonçalves Dias**: 4 discrepant records
  * Group **Valentina Nascimento Cotrim**: 1000 discrepant records
  * Group **Victoria Furquim Werneck Marinho**: 31 discrepant records
  * Group **Wendy Delmondes Galvão**: 29 discrepant records
  * Group **Zuleica Antunes Guimarães Cardoso **: 42 discrepant records

#### **Anonymized Column Discrepancy Samples**
| Composite Key | Column Name | DuckDB Value | Athena Value |
| :--- | :--- | :--- | :--- |
| oocito_id=3, emb_cong_id=null | `micro_data_dl` | `2021-06-04 00:00:00` | `2021-06-04` |
| oocito_id=4, emb_cong_id=null | `micro_data_dl` | `2021-06-04 00:00:00` | `2021-06-04` |
| oocito_id=5, emb_cong_id=null | `micro_data_dl` | `2021-06-04 00:00:00` | `2021-06-04` |
| oocito_id=6, emb_cong_id=null | `micro_data_dl` | `2021-06-04 00:00:00` | `2021-06-04` |
| oocito_id=7, emb_cong_id=null | `micro_data_dl` | `2021-06-04 00:00:00` | `2021-06-04` |
| oocito_id=8, emb_cong_id=null | `micro_data_dl` | `2021-06-04 00:00:00` | `2021-06-04` |
| oocito_id=9, emb_cong_id=null | `micro_data_dl` | `2021-06-04 00:00:00` | `2021-06-04` |
| oocito_id=10, emb_cong_id=null | `micro_data_dl` | `2021-06-04 00:00:00` | `2021-06-04` |
| oocito_id=11, emb_cong_id=null | `micro_data_dl` | `2021-06-04 00:00:00` | `2021-06-04` |
| oocito_id=14, emb_cong_id=null | `micro_data_dl` | `2021-06-04 00:00:00` | `2021-06-04` |
| oocito_id=3, emb_cong_id=null | `micro_data_procedimento` | `2021-06-04 00:00:00` | `2021-06-04` |
| oocito_id=4, emb_cong_id=null | `micro_data_procedimento` | `2021-06-04 00:00:00` | `2021-06-04` |
| oocito_id=5, emb_cong_id=null | `micro_data_procedimento` | `2021-06-04 00:00:00` | `2021-06-04` |
| oocito_id=6, emb_cong_id=null | `micro_data_procedimento` | `2021-06-04 00:00:00` | `2021-06-04` |
| oocito_id=7, emb_cong_id=null | `micro_data_procedimento` | `2021-06-04 00:00:00` | `2021-06-04` |
| oocito_id=8, emb_cong_id=null | `micro_data_procedimento` | `2021-06-04 00:00:00` | `2021-06-04` |
| oocito_id=9, emb_cong_id=null | `micro_data_procedimento` | `2021-06-04 00:00:00` | `2021-06-04` |
| oocito_id=10, emb_cong_id=null | `micro_data_procedimento` | `2021-06-04 00:00:00` | `2021-06-04` |
| oocito_id=11, emb_cong_id=null | `micro_data_procedimento` | `2021-06-04 00:00:00` | `2021-06-04` |
| oocito_id=15, emb_cong_id=null | `micro_data_procedimento` | `2021-06-02 00:00:00` | `2021-06-02` |
| oocito_id=23443, emb_cong_id=null | `micro_prontuario` | `175171` | `971029` |
| oocito_id=23444, emb_cong_id=null | `micro_prontuario` | `175171` | `971029` |
| oocito_id=23445, emb_cong_id=null | `micro_prontuario` | `175171` | `971029` |
| oocito_id=23446, emb_cong_id=null | `micro_prontuario` | `175171` | `971029` |
| oocito_id=23447, emb_cong_id=null | `micro_prontuario` | `175171` | `971029` |
| oocito_id=23448, emb_cong_id=null | `micro_prontuario` | `175171` | `971029` |
| oocito_id=23449, emb_cong_id=null | `micro_prontuario` | `175171` | `971029` |
| oocito_id=23450, emb_cong_id=null | `micro_prontuario` | `175171` | `971029` |
| oocito_id=23451, emb_cong_id=null | `micro_prontuario` | `175171` | `971029` |
| oocito_id=23452, emb_cong_id=null | `micro_prontuario` | `175171` | `971029` |
| oocito_id=315567, emb_cong_id=null | `oocito_embryo_number` | `10` | `1` |
| oocito_id=315568, emb_cong_id=null | `oocito_embryo_number` | `11` | `2` |
| oocito_id=315569, emb_cong_id=null | `oocito_embryo_number` | `12` | `3` |
| oocito_id=315570, emb_cong_id=null | `oocito_embryo_number` | `13` | `4` |
| oocito_id=315571, emb_cong_id=null | `oocito_embryo_number` | `14` | `5` |
| oocito_id=315572, emb_cong_id=null | `oocito_embryo_number` | `15` | `6` |
| oocito_id=315574, emb_cong_id=null | `oocito_embryo_number` | `17` | `8` |
| oocito_id=315575, emb_cong_id=null | `oocito_embryo_number` | `18` | `9` |
| oocito_id=87818, emb_cong_id=null | `oocito_origem_oocito` | `Descongelado` | `Descongelado OR` |
| oocito_id=87819, emb_cong_id=null | `oocito_origem_oocito` | `Descongelado` | `Descongelado OR` |
| oocito_id=87820, emb_cong_id=null | `oocito_origem_oocito` | `Descongelado` | `Descongelado OR` |
| oocito_id=87821, emb_cong_id=null | `oocito_origem_oocito` | `Descongelado` | `Descongelado OR` |
| oocito_id=87822, emb_cong_id=null | `oocito_origem_oocito` | `Descongelado` | `Descongelado OR` |
| oocito_id=87823, emb_cong_id=null | `oocito_origem_oocito` | `Descongelado` | `Descongelado OR` |
| oocito_id=87824, emb_cong_id=null | `oocito_origem_oocito` | `Descongelado` | `Descongelado OR` |
| oocito_id=87825, emb_cong_id=null | `oocito_origem_oocito` | `Descongelado` | `Descongelado OR` |
| oocito_id=100153, emb_cong_id=null | `oocito_origem_oocito` | `Fresco` | `Fresco OR` |
| oocito_id=100155, emb_cong_id=null | `oocito_origem_oocito` | `Fresco` | `Fresco OR` |
| oocito_id=317604, emb_cong_id=null | `oocito_resultado_pgd` | `nan` | `Aneuploide` |
| oocito_id=317605, emb_cong_id=null | `oocito_resultado_pgd` | `nan` | `Aneuploide` |
| oocito_id=318309, emb_cong_id=null | `oocito_resultado_pgd` | `nan` | `Euploide` |
| oocito_id=319246, emb_cong_id=null | `oocito_resultado_pgd` | `nan` | `Aneuploide` |
| oocito_id=319248, emb_cong_id=null | `oocito_resultado_pgd` | `nan` | `Aneuploide` |
| oocito_id=317604, emb_cong_id=null | `oocito_resultado_pgd_detalhes` | `nan` | `Feminino, Múltiplas aneuploidias detectadas` |
| oocito_id=317605, emb_cong_id=null | `oocito_resultado_pgd_detalhes` | `nan` | `Masculino, Múltiplas aneuploidias detectadas` |
| oocito_id=318309, emb_cong_id=null | `oocito_resultado_pgd_detalhes` | `nan` | `Euploide 46, XX ` |
| oocito_id=319246, emb_cong_id=null | `oocito_resultado_pgd_detalhes` | `nan` | `Aneuploidia detectada - Monossomia Chr20 - 20p13q13.33x1 - FEMININO` |
| oocito_id=319248, emb_cong_id=null | `oocito_resultado_pgd_detalhes` | `nan` | `Aneuploidia detectada - Trissomia Chr19 - 19p13.3q13.43x3 - MASCULINO` |
| oocito_id=318908, emb_cong_id=null | `oocito_tcd` | `nan` | `Descartado` |
| oocito_id=318894, emb_cong_id=null | `oocito_tcd` | `nan` | `Descartado` |
| oocito_id=318897, emb_cong_id=null | `oocito_tcd` | `nan` | `Descartado` |
| oocito_id=318772, emb_cong_id=null | `oocito_tcd` | `nan` | `Descartado` |
| oocito_id=318919, emb_cong_id=null | `oocito_tcd` | `nan` | `Descartado` |
| oocito_id=318884, emb_cong_id=null | `oocito_tcd` | `nan` | `Descartado` |
| oocito_id=318889, emb_cong_id=null | `oocito_tcd` | `nan` | `Descartado` |
| oocito_id=318755, emb_cong_id=null | `oocito_tcd` | `nan` | `Descartado` |
| oocito_id=318760, emb_cong_id=null | `oocito_tcd` | `nan` | `Descartado` |
| oocito_id=319000, emb_cong_id=null | `oocito_tcd` | `nan` | `Descartado` |
| oocito_id=11548, emb_cong_id=null | `trat1_bmi` | `23.29` | `0.0` |
| oocito_id=11549, emb_cong_id=null | `trat1_bmi` | `23.29` | `0.0` |
| oocito_id=11550, emb_cong_id=null | `trat1_bmi` | `23.29` | `0.0` |
| oocito_id=11558, emb_cong_id=null | `trat1_bmi` | `23.29` | `0.0` |
| oocito_id=11559, emb_cong_id=null | `trat1_bmi` | `23.29` | `0.0` |
| oocito_id=11560, emb_cong_id=null | `trat1_bmi` | `23.29` | `0.0` |
| oocito_id=11561, emb_cong_id=null | `trat1_bmi` | `23.29` | `0.0` |
| oocito_id=182949, emb_cong_id=null | `trat1_bmi` | `1.26` | `nan` |
| oocito_id=182950, emb_cong_id=null | `trat1_bmi` | `1.26` | `nan` |
| oocito_id=182951, emb_cong_id=null | `trat1_bmi` | `1.26` | `nan` |
| oocito_id=426, emb_cong_id=null | `trat1_data_inicio_inducao` | `2021-05-30 00:00:00` | `2021-05-30` |
| oocito_id=427, emb_cong_id=null | `trat1_data_inicio_inducao` | `2021-05-30 00:00:00` | `2021-05-30` |
| oocito_id=429, emb_cong_id=null | `trat1_data_inicio_inducao` | `2021-05-30 00:00:00` | `2021-05-30` |
| oocito_id=432, emb_cong_id=null | `trat1_data_inicio_inducao` | `2021-05-30 00:00:00` | `2021-05-30` |
| oocito_id=433, emb_cong_id=null | `trat1_data_inicio_inducao` | `2021-05-30 00:00:00` | `2021-05-30` |
| oocito_id=434, emb_cong_id=null | `trat1_data_inicio_inducao` | `2021-05-30 00:00:00` | `2021-05-30` |
| oocito_id=435, emb_cong_id=null | `trat1_data_inicio_inducao` | `2021-05-30 00:00:00` | `2021-05-30` |
| oocito_id=436, emb_cong_id=null | `trat1_data_inicio_inducao` | `2021-05-30 00:00:00` | `2021-05-30` |
| oocito_id=437, emb_cong_id=null | `trat1_data_inicio_inducao` | `2021-05-30 00:00:00` | `2021-05-30` |
| oocito_id=614, emb_cong_id=null | `trat1_data_inicio_inducao` | `2021-05-28 00:00:00` | `2021-05-28` |
| oocito_id=6301, emb_cong_id=null | `trat1_data_transferencia` | `2021-07-27 00:00:00` | `2021-07-27` |
| oocito_id=6302, emb_cong_id=null | `trat1_data_transferencia` | `2021-07-27 00:00:00` | `2021-07-27` |
| oocito_id=6303, emb_cong_id=null | `trat1_data_transferencia` | `2021-07-27 00:00:00` | `2021-07-27` |
| oocito_id=6304, emb_cong_id=null | `trat1_data_transferencia` | `2021-07-27 00:00:00` | `2021-07-27` |
| oocito_id=6305, emb_cong_id=null | `trat1_data_transferencia` | `2021-07-27 00:00:00` | `2021-07-27` |
| oocito_id=6306, emb_cong_id=null | `trat1_data_transferencia` | `2021-07-27 00:00:00` | `2021-07-27` |
| oocito_id=7547, emb_cong_id=null | `trat1_data_transferencia` | `2021-08-02 00:00:00` | `2021-08-02` |
| oocito_id=7551, emb_cong_id=null | `trat1_data_transferencia` | `2021-08-02 00:00:00` | `2021-08-02` |
| oocito_id=7552, emb_cong_id=null | `trat1_data_transferencia` | `2021-08-02 00:00:00` | `2021-08-02` |
| oocito_id=7553, emb_cong_id=null | `trat1_data_transferencia` | `2021-08-02 00:00:00` | `2021-08-02` |
| oocito_id=182949, emb_cong_id=null | `trat1_fator_infertilidade1` | `Outros` | `nan` |
| oocito_id=182950, emb_cong_id=null | `trat1_fator_infertilidade1` | `Outros` | `nan` |
| oocito_id=182951, emb_cong_id=null | `trat1_fator_infertilidade1` | `Outros` | `nan` |
| oocito_id=182952, emb_cong_id=null | `trat1_fator_infertilidade1` | `Outros` | `nan` |
| oocito_id=182953, emb_cong_id=null | `trat1_fator_infertilidade1` | `Outros` | `nan` |
| oocito_id=200186, emb_cong_id=null | `trat1_fator_infertilidade1` | `Insuficiência ovariana` | `nan` |
| oocito_id=200187, emb_cong_id=null | `trat1_fator_infertilidade1` | `Insuficiência ovariana` | `nan` |
| oocito_id=200188, emb_cong_id=null | `trat1_fator_infertilidade1` | `Insuficiência ovariana` | `nan` |
| oocito_id=200189, emb_cong_id=null | `trat1_fator_infertilidade1` | `Insuficiência ovariana` | `nan` |
| oocito_id=233231, emb_cong_id=null | `trat1_fator_infertilidade1` | `Insuficiência ovariana` | `nan` |
| oocito_id=11548, emb_cong_id=null | `trat1_id` | `1538` | `499.0` |
| oocito_id=11549, emb_cong_id=null | `trat1_id` | `1538` | `499.0` |
| oocito_id=11550, emb_cong_id=null | `trat1_id` | `1538` | `499.0` |
| oocito_id=11558, emb_cong_id=null | `trat1_id` | `1538` | `499.0` |
| oocito_id=11559, emb_cong_id=null | `trat1_id` | `1538` | `499.0` |
| oocito_id=11560, emb_cong_id=null | `trat1_id` | `1538` | `499.0` |
| oocito_id=11561, emb_cong_id=null | `trat1_id` | `1538` | `499.0` |
| oocito_id=84383, emb_cong_id=null | `trat1_id` | `12737` | `11498.0` |
| oocito_id=84384, emb_cong_id=null | `trat1_id` | `12737` | `11498.0` |
| oocito_id=84385, emb_cong_id=null | `trat1_id` | `12737` | `11498.0` |
| oocito_id=11548, emb_cong_id=null | `trat1_motivo_nao_transferir` | `nan` | `Criopreservação total` |
| oocito_id=11549, emb_cong_id=null | `trat1_motivo_nao_transferir` | `nan` | `Criopreservação total` |
| oocito_id=11550, emb_cong_id=null | `trat1_motivo_nao_transferir` | `nan` | `Criopreservação total` |
| oocito_id=11558, emb_cong_id=null | `trat1_motivo_nao_transferir` | `nan` | `Criopreservação total` |
| oocito_id=11559, emb_cong_id=null | `trat1_motivo_nao_transferir` | `nan` | `Criopreservação total` |
| oocito_id=11560, emb_cong_id=null | `trat1_motivo_nao_transferir` | `nan` | `Criopreservação total` |
| oocito_id=11561, emb_cong_id=null | `trat1_motivo_nao_transferir` | `nan` | `Criopreservação total` |
| oocito_id=84383, emb_cong_id=null | `trat1_motivo_nao_transferir` | `nan` | `Outros` |
| oocito_id=84384, emb_cong_id=null | `trat1_motivo_nao_transferir` | `nan` | `Outros` |
| oocito_id=84385, emb_cong_id=null | `trat1_motivo_nao_transferir` | `nan` | `Outros` |
| oocito_id=11548, emb_cong_id=null | `trat1_origem_ovulo` | `nan` | `Homólogo` |
| oocito_id=11549, emb_cong_id=null | `trat1_origem_ovulo` | `nan` | `Homólogo` |
| oocito_id=11550, emb_cong_id=null | `trat1_origem_ovulo` | `nan` | `Homólogo` |
| oocito_id=11558, emb_cong_id=null | `trat1_origem_ovulo` | `nan` | `Homólogo` |
| oocito_id=11559, emb_cong_id=null | `trat1_origem_ovulo` | `nan` | `Homólogo` |
| oocito_id=11560, emb_cong_id=null | `trat1_origem_ovulo` | `nan` | `Homólogo` |
| oocito_id=11561, emb_cong_id=null | `trat1_origem_ovulo` | `nan` | `Homólogo` |
| oocito_id=233231, emb_cong_id=null | `trat1_origem_ovulo` | `Homólogo` | `nan` |
| oocito_id=233232, emb_cong_id=null | `trat1_origem_ovulo` | `Homólogo` | `nan` |
| oocito_id=233233, emb_cong_id=null | `trat1_origem_ovulo` | `Homólogo` | `nan` |
| oocito_id=11548, emb_cong_id=null | `trat1_previous_et` | `0` | `1.0` |
| oocito_id=11549, emb_cong_id=null | `trat1_previous_et` | `0` | `1.0` |
| oocito_id=11550, emb_cong_id=null | `trat1_previous_et` | `0` | `1.0` |
| oocito_id=11558, emb_cong_id=null | `trat1_previous_et` | `0` | `1.0` |
| oocito_id=11559, emb_cong_id=null | `trat1_previous_et` | `0` | `1.0` |
| oocito_id=11560, emb_cong_id=null | `trat1_previous_et` | `0` | `1.0` |
| oocito_id=11561, emb_cong_id=null | `trat1_previous_et` | `0` | `1.0` |
| oocito_id=28052, emb_cong_id=null | `trat1_previous_et` | `0` | `1.0` |
| oocito_id=28053, emb_cong_id=null | `trat1_previous_et` | `0` | `1.0` |
| oocito_id=28056, emb_cong_id=null | `trat1_previous_et` | `0` | `1.0` |
| oocito_id=182949, emb_cong_id=null | `trat1_previous_et_od` | `0` | `nan` |
| oocito_id=182950, emb_cong_id=null | `trat1_previous_et_od` | `0` | `nan` |
| oocito_id=182951, emb_cong_id=null | `trat1_previous_et_od` | `0` | `nan` |
| oocito_id=182952, emb_cong_id=null | `trat1_previous_et_od` | `0` | `nan` |
| oocito_id=182953, emb_cong_id=null | `trat1_previous_et_od` | `0` | `nan` |
| oocito_id=200186, emb_cong_id=null | `trat1_previous_et_od` | `0` | `nan` |
| oocito_id=200187, emb_cong_id=null | `trat1_previous_et_od` | `0` | `nan` |
| oocito_id=200188, emb_cong_id=null | `trat1_previous_et_od` | `0` | `nan` |
| oocito_id=200189, emb_cong_id=null | `trat1_previous_et_od` | `0` | `nan` |
| oocito_id=233231, emb_cong_id=null | `trat1_previous_et_od` | `0` | `nan` |
| oocito_id=11548, emb_cong_id=null | `trat1_resultado_tratamento` | `Gestação Química Confirmada` | `` |
| oocito_id=11549, emb_cong_id=null | `trat1_resultado_tratamento` | `Gestação Química Confirmada` | `` |
| oocito_id=11550, emb_cong_id=null | `trat1_resultado_tratamento` | `Gestação Química Confirmada` | `` |
| oocito_id=11558, emb_cong_id=null | `trat1_resultado_tratamento` | `Gestação Química Confirmada` | `` |
| oocito_id=11559, emb_cong_id=null | `trat1_resultado_tratamento` | `Gestação Química Confirmada` | `` |
| oocito_id=11560, emb_cong_id=null | `trat1_resultado_tratamento` | `Gestação Química Confirmada` | `` |
| oocito_id=11561, emb_cong_id=null | `trat1_resultado_tratamento` | `Gestação Química Confirmada` | `` |
| oocito_id=41165, emb_cong_id=null | `trat1_resultado_tratamento` | `nan` | `No transfer` |
| oocito_id=41166, emb_cong_id=null | `trat1_resultado_tratamento` | `nan` | `No transfer` |
| oocito_id=41167, emb_cong_id=null | `trat1_resultado_tratamento` | `nan` | `No transfer` |
| oocito_id=182949, emb_cong_id=null | `trat1_tentativa` | `1` | `nan` |
| oocito_id=182950, emb_cong_id=null | `trat1_tentativa` | `1` | `nan` |
| oocito_id=182951, emb_cong_id=null | `trat1_tentativa` | `1` | `nan` |
| oocito_id=182952, emb_cong_id=null | `trat1_tentativa` | `1` | `nan` |
| oocito_id=182953, emb_cong_id=null | `trat1_tentativa` | `1` | `nan` |
| oocito_id=200186, emb_cong_id=null | `trat1_tentativa` | `1` | `nan` |
| oocito_id=200187, emb_cong_id=null | `trat1_tentativa` | `1` | `nan` |
| oocito_id=200188, emb_cong_id=null | `trat1_tentativa` | `1` | `nan` |
| oocito_id=200189, emb_cong_id=null | `trat1_tentativa` | `1` | `nan` |
| oocito_id=233231, emb_cong_id=null | `trat1_tentativa` | `4` | `nan` |
| oocito_id=11548, emb_cong_id=null | `trat1_tipo_procedimento` | `Ciclo de Congelados` | `Ciclo a Fresco FIV` |
| oocito_id=11549, emb_cong_id=null | `trat1_tipo_procedimento` | `Ciclo de Congelados` | `Ciclo a Fresco FIV` |
| oocito_id=11550, emb_cong_id=null | `trat1_tipo_procedimento` | `Ciclo de Congelados` | `Ciclo a Fresco FIV` |
| oocito_id=11558, emb_cong_id=null | `trat1_tipo_procedimento` | `Ciclo de Congelados` | `Ciclo a Fresco FIV` |
| oocito_id=11559, emb_cong_id=null | `trat1_tipo_procedimento` | `Ciclo de Congelados` | `Ciclo a Fresco FIV` |
| oocito_id=11560, emb_cong_id=null | `trat1_tipo_procedimento` | `Ciclo de Congelados` | `Ciclo a Fresco FIV` |
| oocito_id=11561, emb_cong_id=null | `trat1_tipo_procedimento` | `Ciclo de Congelados` | `Ciclo a Fresco FIV` |
| oocito_id=108487, emb_cong_id=null | `trat1_tipo_procedimento` | `Ciclo de Congelados` | `Ciclo a Fresco FIV` |
| oocito_id=108490, emb_cong_id=null | `trat1_tipo_procedimento` | `Ciclo de Congelados` | `Ciclo a Fresco FIV` |
| oocito_id=108494, emb_cong_id=null | `trat1_tipo_procedimento` | `Ciclo de Congelados` | `Ciclo a Fresco FIV` |

### `embryos_with_prescription_long`
#### **Schema Mismatches**
* Columns in DuckDB Only: `baby_1_weight`, `baby_2_weight`, `baby_3_weight`, `chart_or_pin`, `complications_of_pregnancy_specify`, `date_of_birth`, `embryo_Name_BlastExpandLast`, `embryo_Name_BlastomereSize`, `embryo_Name_EVEN2`, `embryo_Name_EVEN4`, `embryo_Name_EVEN8`, `embryo_Name_FRAG2`, `embryo_Name_FRAG2CAT`, `embryo_Name_FRAG4`, `embryo_Name_FRAG8`, `embryo_Name_Fragmentation`, `embryo_Name_ICM`, `embryo_Name_Line`, `embryo_Name_MN2Type`, `embryo_Name_MorphologicalGrade`, `embryo_Name_MultiNucleation`, `embryo_Name_Nuclei2`, `embryo_Name_PN`, `embryo_Name_Pulsing`, `embryo_Name_ReexpansionCount`, `embryo_Name_Strings`, `embryo_Name_TE`, `embryo_Name_t2`, `embryo_Name_t3`, `embryo_Name_t4`, `embryo_Name_t5`, `embryo_Name_t6`, `embryo_Name_t7`, `embryo_Name_t8`, `embryo_Name_t9`, `embryo_Name_tB`, `embryo_Name_tEB`, `embryo_Name_tHB`, `embryo_Name_tM`, `embryo_Name_tPB2`, `embryo_Name_tPNa`, `embryo_Name_tPNf`, `embryo_Name_tSB`, `embryo_Name_tSC`, `embryo_Time_BlastExpandLast`, `embryo_Time_BlastomereSize`, `embryo_Time_EVEN2`, `embryo_Time_EVEN4`, `embryo_Time_EVEN8`, `embryo_Time_FRAG2`, `embryo_Time_FRAG2CAT`, `embryo_Time_FRAG4`, `embryo_Time_FRAG8`, `embryo_Time_ICM`, `embryo_Time_Line`, `embryo_Time_MN2Type`, `embryo_Time_MorphologicalGrade`, `embryo_Time_MultiNucleation`, `embryo_Time_Nuclei2`, `embryo_Time_PN`, `embryo_Time_Pulsing`, `embryo_Time_ReexpansionCount`, `embryo_Time_Strings`, `embryo_Time_TE`, `embryo_Time_t6`, `embryo_Time_t7`, `embryo_Time_t9`, `embryo_Time_tPB2`, `embryo_Timestamp_BlastExpandLast`, `embryo_Timestamp_BlastomereSize`, `embryo_Timestamp_EVEN2`, `embryo_Timestamp_EVEN4`, `embryo_Timestamp_EVEN8`, `embryo_Timestamp_FRAG2`, `embryo_Timestamp_FRAG2CAT`, `embryo_Timestamp_FRAG4`, `embryo_Timestamp_FRAG8`, `embryo_Timestamp_Fragmentation`, `embryo_Timestamp_ICM`, `embryo_Timestamp_Line`, `embryo_Timestamp_MN2Type`, `embryo_Timestamp_MorphologicalGrade`, `embryo_Timestamp_MultiNucleation`, `embryo_Timestamp_Nuclei2`, `embryo_Timestamp_PN`, `embryo_Timestamp_Pulsing`, `embryo_Timestamp_ReexpansionCount`, `embryo_Timestamp_Strings`, `embryo_Timestamp_TE`, `embryo_Timestamp_t2`, `embryo_Timestamp_t3`, `embryo_Timestamp_t4`, `embryo_Timestamp_t5`, `embryo_Timestamp_t6`, `embryo_Timestamp_t7`, `embryo_Timestamp_t8`, `embryo_Timestamp_t9`, `embryo_Timestamp_tB`, `embryo_Timestamp_tEB`, `embryo_Timestamp_tHB`, `embryo_Timestamp_tM`, `embryo_Timestamp_tPB2`, `embryo_Timestamp_tPNa`, `embryo_Timestamp_tPNf`, `embryo_Timestamp_tSB`, `embryo_Timestamp_tSC`, `embryo_Value_BlastExpandLast`, `embryo_Value_BlastomereSize`, `embryo_Value_EVEN2`, `embryo_Value_EVEN4`, `embryo_Value_EVEN8`, `embryo_Value_FRAG2`, `embryo_Value_FRAG4`, `embryo_Value_FRAG8`, `embryo_Value_Fragmentation`, `embryo_Value_Line`, `embryo_Value_MorphologicalGrade`, `embryo_Value_MultiNucleation`, `embryo_Value_Strings`, `embryo_Value_t2`, `embryo_Value_t3`, `embryo_Value_t4`, `embryo_Value_t5`, `embryo_Value_t6`, `embryo_Value_t7`, `embryo_Value_t8`, `embryo_Value_t9`, `embryo_Value_tB`, `embryo_Value_tEB`, `embryo_Value_tHB`, `embryo_Value_tM`, `embryo_Value_tPB2`, `embryo_Value_tPNa`, `embryo_Value_tPNf`, `embryo_Value_tSB`, `embryo_Value_tSC`, `fet_data_crio`, `fet_dia_cryo`, `fet_dia_et`, `fet_file_name`, `fet_idade_do_cong_de_embriao`, `fet_idade_mulher`, `fet_no_da_transfer_1a_2a_3a`, `fet_no_et`, `fet_preparo_para_transferencia`, `fet_sheet_name`, `fet_tipo_1`, `fet_tipo_biopsia`, `fet_tipo_da_doacao`, `fet_tipo_de_fet`, `fet_tipo_de_tratamento`, `fresh_altura`, `fresh_data_crio`, `fresh_data_de_nasc`, `fresh_dia_cryo`, `fresh_fator_1`, `fresh_file_name`, `fresh_idade_espermatozoide`, `fresh_incubadora`, `fresh_no_biopsiados`, `fresh_opu`, `fresh_origem`, `fresh_peso`, `fresh_qtd_analisados`, `fresh_qtd_blasto`, `fresh_qtd_blasto_tq_a_e_b`, `fresh_qtd_normais`, `fresh_sheet_name`, `fresh_tipo`, `fresh_tipo_1`, `fresh_tipo_biopsia`, `fresh_tipo_de_inseminacao`, `fresh_total_de_mii`, `idascore_IDAScore`, `idascore_IDATime`, `idascore_IDAVersion`, `join_step_1`, `micro_CicloDoadora`, `micro_recepcao_ovulos`, `number_of_fet_after_originally_frozen`, `patient_DateOfBirth`, `patient_FirstName`, `patient_LastName`, `patient_PatientID`, `patient_PatientIDx`, `patient_YearOfBirth`, `patient_name`, `patient_unit_huntington`, `prontuario`, `trat1_idade_esposa`, `trat1_idade_marido`, `trat1_prontuario_doadora`, `trat2_idade_esposa`, `trat2_idade_marido`, `trat2_prontuario_doadora`, `treatment_TreatmentName`, `unidade`, `year`
* Columns in Athena Only: `date_of_embryo_transfer`, `date_when_embryos_were_cryopreserved`, `embryo_first_name`, `embryo_last_name`, `embryo_patient_date_of_birth`, `embryo_patient_id`, `embryo_patient_id_x`, `embryo_patient_sk`, `embryo_patient_year_of_birth`, `embryo_prontuario`, `embryo_treatment_name`, `embryo_unit_huntington`, `number_of_newborns`
#### **Numeric Metric Alignment**
| Column Name | DuckDB Sum | Athena Sum | Difference | Alignment Rate |
| :--- | :---: | :---: | :---: | :---: |
| `presc_dose` | 17489688.15 | 17489556.15 | +132.00 | 99.9992% |
| `presc_dose_total` | 43112908.59 | 43112789.59 | +119.00 | 99.9997% |
| `presc_numero_dias` | 311803.00 | 311816.00 | -13.00 | 99.9958% |

#### **Discrepancy Group Isolation**
* **By Year/Date**:
  * Year **2019**: 7 discrepant records
  * Year **2021**: 68 discrepant records
  * Year **2022**: 9189 discrepant records
  * Year **2023**: 16012 discrepant records
  * Year **2024**: 21067 discrepant records
  * Year **2025**: 18842 discrepant records
  * Year **2026**: 4284 discrepant records
  * Year **N/A**: 118 discrepant records
* **By Dimension Group**:
  * Group **AAS**: 127 discrepant records
  * Group **ANDROGEL**: 5 discrepant records
  * Group **AZITROMICINA**: 10 discrepant records
  * Group **BUSCOPAN**: 2 discrepant records
  * Group **CERAZETTE**: 2351 discrepant records
  * Group **CETROTIDE**: 641 discrepant records
  * Group **CHORIOMON**: 2545 discrepant records
  * Group **CLEXANE**: 291 discrepant records
  * Group **CLOMID**: 395 discrepant records
  * Group **CLOMIFENO**: 11 discrepant records
  * Group **COENZIMA**: 12 discrepant records
  * Group **CRINONE**: 68 discrepant records
  * Group **DELESTROGEN**: 54 discrepant records
  * Group **DOSTINEX**: 98 discrepant records
  * Group **DRAMIN**: 2 discrepant records
  * Group **DUPHASTON**: 9780 discrepant records
  * Group **ELONVA**: 2921 discrepant records
  * Group **FEMARA**: 64 discrepant records
  * Group **FILGRASTIM**: 4 discrepant records
  * Group **FOSTIMON**: 138 discrepant records
  * Group **GONAL**: 1271 discrepant records
  * Group **GONAPEPTYL**: 8412 discrepant records
  * Group **INDUX**: 56 discrepant records
  * Group **LACTOBACILLUS**: 70 discrepant records
  * Group **LECTRUM**: 380 discrepant records
  * Group **LETROZOL**: 1544 discrepant records
  * Group **LIPOFUNDIN**: 68 discrepant records
  * Group **MECLIN**: 2 discrepant records
  * Group **MENOPUR**: 11388 discrepant records
  * Group **MERIONAL**: 3124 discrepant records
  * Group **METICORTEN**: 127 discrepant records
  * Group **MYLANTA**: 2 discrepant records
  * Group **NATIFA**: 9 discrepant records
  * Group **NEO**: 287 discrepant records
  * Group **OESTROGEL**: 929 discrepant records
  * Group **ORGALUTRAN**: 1140 discrepant records
  * Group **OVIDREL**: 2719 discrepant records
  * Group **PERGOVERIS**: 9073 discrepant records
  * Group **PREDSIM**: 7 discrepant records
  * Group **PRIMOGYNA**: 1760 discrepant records
  * Group **PROGESTERONA**: 96 discrepant records
  * Group **PUREGON**: 4340 discrepant records
  * Group **REKOVELLE**: 1879 discrepant records
  * Group **SOMATROPINA**: 112 discrepant records
  * Group **TAMARINE**: 2 discrepant records
  * Group **TORAGESIC**: 13 discrepant records
  * Group **UTROGESTAN**: 1237 discrepant records
  * Group **VIAGRA**: 9 discrepant records
  * Group **VITERGAN**: 12 discrepant records

#### **Anonymized Column Discrepancy Samples**
| Composite Key | Column Name | DuckDB Value | Athena Value |
| :--- | :--- | :--- | :--- |
| oocito_id=191749, presc_id=451606 | `cong_em_cod_congelamento` | `E184/25` | `E183/25` |
| oocito_id=191749, presc_id=451608 | `cong_em_cod_congelamento` | `E184/25` | `E183/25` |
| oocito_id=191749, presc_id=451607 | `cong_em_cod_congelamento` | `E184/25` | `E183/25` |
| oocito_id=191749, presc_id=451602 | `cong_em_cod_congelamento` | `E184/25` | `E183/25` |
| oocito_id=191749, presc_id=451604 | `cong_em_cod_congelamento` | `E184/25` | `E183/25` |
| oocito_id=191749, presc_id=451603 | `cong_em_cod_congelamento` | `E184/25` | `E183/25` |
| oocito_id=191749, presc_id=451605 | `cong_em_cod_congelamento` | `E184/25` | `E183/25` |
| oocito_id=193802, presc_id=439235 | `cong_em_cod_congelamento` | `E1144/24` | `E1035/24` |
| oocito_id=193802, presc_id=439237 | `cong_em_cod_congelamento` | `E1144/24` | `E1035/24` |
| oocito_id=193802, presc_id=439234 | `cong_em_cod_congelamento` | `E1144/24` | `E1035/24` |
| oocito_id=188017, presc_id=426825 | `cong_em_data` | `2024-03-16 00:00:00` | `2024-03-16` |
| oocito_id=188019, presc_id=426825 | `cong_em_data` | `2024-03-16 00:00:00` | `2024-03-16` |
| oocito_id=188021, presc_id=426825 | `cong_em_data` | `2024-03-16 00:00:00` | `2024-03-16` |
| oocito_id=188023, presc_id=460012 | `cong_em_data` | `2024-03-16 00:00:00` | `2024-03-16` |
| oocito_id=188191, presc_id=424471 | `cong_em_data` | `2024-03-18 00:00:00` | `2024-03-18` |
| oocito_id=188192, presc_id=424471 | `cong_em_data` | `2024-03-18 00:00:00` | `2024-03-18` |
| oocito_id=188436, presc_id=414726 | `cong_em_data` | `2024-03-19 00:00:00` | `2024-03-19` |
| oocito_id=188437, presc_id=414726 | `cong_em_data` | `2024-03-19 00:00:00` | `2024-03-19` |
| oocito_id=188438, presc_id=414726 | `cong_em_data` | `2024-03-19 00:00:00` | `2024-03-19` |
| oocito_id=188442, presc_id=414726 | `cong_em_data` | `2024-03-19 00:00:00` | `2024-03-19` |
| oocito_id=191749, presc_id=451606 | `cong_em_id` | `27847` | `27846.0` |
| oocito_id=191749, presc_id=451608 | `cong_em_id` | `27847` | `27846.0` |
| oocito_id=191749, presc_id=451607 | `cong_em_id` | `27847` | `27846.0` |
| oocito_id=191749, presc_id=451602 | `cong_em_id` | `27847` | `27846.0` |
| oocito_id=191749, presc_id=451604 | `cong_em_id` | `27847` | `27846.0` |
| oocito_id=191749, presc_id=451603 | `cong_em_id` | `27847` | `27846.0` |
| oocito_id=191749, presc_id=451605 | `cong_em_id` | `27847` | `27846.0` |
| oocito_id=193802, presc_id=439235 | `cong_em_id` | `25372` | `25263.0` |
| oocito_id=193802, presc_id=439237 | `cong_em_id` | `25372` | `25263.0` |
| oocito_id=193802, presc_id=439234 | `cong_em_id` | `25372` | `25263.0` |
| oocito_id=196658, presc_id=450586 | `date_of_delivery` | `NaT` | `2025-05-27` |
| oocito_id=196658, presc_id=450582 | `date_of_delivery` | `NaT` | `2025-05-27` |
| oocito_id=196658, presc_id=450585 | `date_of_delivery` | `NaT` | `2025-05-27` |
| oocito_id=196658, presc_id=450584 | `date_of_delivery` | `NaT` | `2025-05-27` |
| oocito_id=196658, presc_id=450583 | `date_of_delivery` | `NaT` | `2025-05-27` |
| oocito_id=203626, presc_id=479181 | `date_of_delivery` | `NaT` | `2025-06-30` |
| oocito_id=203627, presc_id=479181 | `date_of_delivery` | `NaT` | `2025-06-30` |
| oocito_id=203626, presc_id=479184 | `date_of_delivery` | `NaT` | `2025-06-30` |
| oocito_id=203627, presc_id=479184 | `date_of_delivery` | `NaT` | `2025-06-30` |
| oocito_id=203626, presc_id=479183 | `date_of_delivery` | `NaT` | `2025-06-30` |
| oocito_id=191749, presc_id=451606 | `descong_em_cod_descongelamento` | `nan` | `852/24` |
| oocito_id=191749, presc_id=451608 | `descong_em_cod_descongelamento` | `nan` | `852/24` |
| oocito_id=191749, presc_id=451607 | `descong_em_cod_descongelamento` | `nan` | `852/24` |
| oocito_id=191749, presc_id=451602 | `descong_em_cod_descongelamento` | `nan` | `852/24` |
| oocito_id=191749, presc_id=451604 | `descong_em_cod_descongelamento` | `nan` | `852/24` |
| oocito_id=191749, presc_id=451603 | `descong_em_cod_descongelamento` | `nan` | `852/24` |
| oocito_id=191749, presc_id=451605 | `descong_em_cod_descongelamento` | `nan` | `852/24` |
| oocito_id=193802, presc_id=439235 | `descong_em_cod_descongelamento` | `1284/24` | `967/24` |
| oocito_id=193802, presc_id=439237 | `descong_em_cod_descongelamento` | `1284/24` | `967/24` |
| oocito_id=193802, presc_id=439234 | `descong_em_cod_descongelamento` | `1284/24` | `967/24` |
| oocito_id=188017, presc_id=426825 | `descong_em_data_congelamento` | `2024-03-16 00:00:00` | `2024-03-16` |
| oocito_id=188019, presc_id=426825 | `descong_em_data_congelamento` | `2024-03-16 00:00:00` | `2024-03-16` |
| oocito_id=188021, presc_id=426825 | `descong_em_data_congelamento` | `2024-03-16 00:00:00` | `2024-03-16` |
| oocito_id=188023, presc_id=460012 | `descong_em_data_congelamento` | `2024-03-16 00:00:00` | `2024-03-16` |
| oocito_id=188191, presc_id=424471 | `descong_em_data_congelamento` | `2024-03-18 00:00:00` | `2024-03-18` |
| oocito_id=188192, presc_id=424471 | `descong_em_data_congelamento` | `2024-03-18 00:00:00` | `2024-03-18` |
| oocito_id=188895, presc_id=419196 | `descong_em_data_congelamento` | `2024-03-23 00:00:00` | `2024-03-23` |
| oocito_id=188903, presc_id=419196 | `descong_em_data_congelamento` | `2024-03-23 00:00:00` | `2024-03-23` |
| oocito_id=188904, presc_id=419196 | `descong_em_data_congelamento` | `2024-03-23 00:00:00` | `2024-03-23` |
| oocito_id=188953, presc_id=460068 | `descong_em_data_congelamento` | `2024-03-23 00:00:00` | `2024-03-23` |
| oocito_id=188017, presc_id=426825 | `descong_em_data_descongelamento` | `2024-04-03 00:00:00` | `2024-04-03` |
| oocito_id=188019, presc_id=426825 | `descong_em_data_descongelamento` | `2025-10-30 00:00:00` | `2025-10-30` |
| oocito_id=188021, presc_id=426825 | `descong_em_data_descongelamento` | `2024-04-03 00:00:00` | `2024-04-03` |
| oocito_id=188023, presc_id=460012 | `descong_em_data_descongelamento` | `2024-05-20 00:00:00` | `2024-05-20` |
| oocito_id=188191, presc_id=424471 | `descong_em_data_descongelamento` | `2024-08-07 00:00:00` | `2024-08-07` |
| oocito_id=188192, presc_id=424471 | `descong_em_data_descongelamento` | `2024-04-08 00:00:00` | `2024-04-08` |
| oocito_id=188895, presc_id=419196 | `descong_em_data_descongelamento` | `2026-03-02 00:00:00` | `2026-03-02` |
| oocito_id=188903, presc_id=419196 | `descong_em_data_descongelamento` | `2024-04-11 00:00:00` | `2024-04-11` |
| oocito_id=188904, presc_id=419196 | `descong_em_data_descongelamento` | `2024-04-11 00:00:00` | `2024-04-11` |
| oocito_id=188953, presc_id=460068 | `descong_em_data_descongelamento` | `2024-05-23 00:00:00` | `2024-05-23` |
| oocito_id=188017, presc_id=426825 | `descong_em_data_transferencia` | `2024-04-03 00:00:00` | `2024-04-03` |
| oocito_id=188019, presc_id=426825 | `descong_em_data_transferencia` | `2025-10-30 00:00:00` | `2025-10-30` |
| oocito_id=188021, presc_id=426825 | `descong_em_data_transferencia` | `2024-04-03 00:00:00` | `2024-04-03` |
| oocito_id=188023, presc_id=460012 | `descong_em_data_transferencia` | `2024-05-20 00:00:00` | `2024-05-20` |
| oocito_id=188191, presc_id=424471 | `descong_em_data_transferencia` | `2024-08-07 00:00:00` | `2024-08-07` |
| oocito_id=188192, presc_id=424471 | `descong_em_data_transferencia` | `2024-04-10 00:00:00` | `2024-04-10` |
| oocito_id=188895, presc_id=419196 | `descong_em_data_transferencia` | `2026-03-02 00:00:00` | `2026-03-02` |
| oocito_id=188903, presc_id=419196 | `descong_em_data_transferencia` | `2024-04-11 00:00:00` | `2024-04-11` |
| oocito_id=188904, presc_id=419196 | `descong_em_data_transferencia` | `2024-04-11 00:00:00` | `2024-04-11` |
| oocito_id=188953, presc_id=460068 | `descong_em_data_transferencia` | `2024-05-23 00:00:00` | `2024-05-23` |
| oocito_id=191749, presc_id=451606 | `descong_em_id` | `<NA>` | `14628.0` |
| oocito_id=191749, presc_id=451608 | `descong_em_id` | `<NA>` | `14628.0` |
| oocito_id=191749, presc_id=451607 | `descong_em_id` | `<NA>` | `14628.0` |
| oocito_id=191749, presc_id=451602 | `descong_em_id` | `<NA>` | `14628.0` |
| oocito_id=191749, presc_id=451604 | `descong_em_id` | `<NA>` | `14628.0` |
| oocito_id=191749, presc_id=451603 | `descong_em_id` | `<NA>` | `14628.0` |
| oocito_id=191749, presc_id=451605 | `descong_em_id` | `<NA>` | `14628.0` |
| oocito_id=193802, presc_id=439235 | `descong_em_id` | `15088` | `14748.0` |
| oocito_id=193802, presc_id=439237 | `descong_em_id` | `15088` | `14748.0` |
| oocito_id=193802, presc_id=439234 | `descong_em_id` | `15088` | `14748.0` |
| oocito_id=191749, presc_id=451606 | `emb_cong_id` | `81450` | `81446.0` |
| oocito_id=191749, presc_id=451608 | `emb_cong_id` | `81450` | `81446.0` |
| oocito_id=191749, presc_id=451607 | `emb_cong_id` | `81450` | `81446.0` |
| oocito_id=191749, presc_id=451602 | `emb_cong_id` | `81450` | `81446.0` |
| oocito_id=191749, presc_id=451604 | `emb_cong_id` | `81450` | `81446.0` |
| oocito_id=191749, presc_id=451603 | `emb_cong_id` | `81450` | `81446.0` |
| oocito_id=191749, presc_id=451605 | `emb_cong_id` | `81450` | `81446.0` |
| oocito_id=193802, presc_id=439235 | `emb_cong_id` | `72454` | `72048.0` |
| oocito_id=193802, presc_id=439237 | `emb_cong_id` | `72454` | `72048.0` |
| oocito_id=193802, presc_id=439234 | `emb_cong_id` | `72454` | `72048.0` |
| oocito_id=236845, presc_id=618903 | `emb_cong_qualidade` | `Blastocisto Grau 5 - A - A` | `Blastocisto Grau 4 - A - A` |
| oocito_id=236845, presc_id=618900 | `emb_cong_qualidade` | `Blastocisto Grau 5 - A - A` | `Blastocisto Grau 4 - A - A` |
| oocito_id=236845, presc_id=618902 | `emb_cong_qualidade` | `Blastocisto Grau 5 - A - A` | `Blastocisto Grau 4 - A - A` |
| oocito_id=236845, presc_id=618901 | `emb_cong_qualidade` | `Blastocisto Grau 5 - A - A` | `Blastocisto Grau 4 - A - A` |
| oocito_id=105093, presc_id=277521 | `emb_cong_qualidade` | `Blastocisto Grau 5 - C - C` | `Blastocisto Grau 5 - B - C` |
| oocito_id=105093, presc_id=277523 | `emb_cong_qualidade` | `Blastocisto Grau 5 - C - C` | `Blastocisto Grau 5 - B - C` |
| oocito_id=105093, presc_id=277520 | `emb_cong_qualidade` | `Blastocisto Grau 5 - C - C` | `Blastocisto Grau 5 - B - C` |
| oocito_id=105093, presc_id=277522 | `emb_cong_qualidade` | `Blastocisto Grau 5 - C - C` | `Blastocisto Grau 5 - B - C` |
| oocito_id=156879, presc_id=286477 | `emb_cong_qualidade` | `Blastocisto Grau 5 - A - B` | `Blastocisto Grau 4 - A - B` |
| oocito_id=156881, presc_id=286477 | `emb_cong_qualidade` | `Blastocisto Grau 5 - A - B` | `Blastocisto Grau 5 - A - A` |
| oocito_id=191749, presc_id=451606 | `emb_cong_transferidos` | `nan` | `Criopreservado` |
| oocito_id=191754, presc_id=451606 | `emb_cong_transferidos` | `nan` | `Descartado` |
| oocito_id=191755, presc_id=451606 | `emb_cong_transferidos` | `nan` | `Descartado` |
| oocito_id=191751, presc_id=451606 | `emb_cong_transferidos` | `nan` | `Descartado` |
| oocito_id=191749, presc_id=451608 | `emb_cong_transferidos` | `nan` | `Criopreservado` |
| oocito_id=191754, presc_id=451608 | `emb_cong_transferidos` | `nan` | `Descartado` |
| oocito_id=191755, presc_id=451608 | `emb_cong_transferidos` | `nan` | `Descartado` |
| oocito_id=191751, presc_id=451608 | `emb_cong_transferidos` | `nan` | `Descartado` |
| oocito_id=191749, presc_id=451607 | `emb_cong_transferidos` | `nan` | `Criopreservado` |
| oocito_id=191754, presc_id=451607 | `emb_cong_transferidos` | `nan` | `Descartado` |
| oocito_id=121015, presc_id=161973 | `embryo_description` | `nan` | `Aneuploide - Trissomia 13, XX` |
| oocito_id=121015, presc_id=161971 | `embryo_description` | `nan` | `Aneuploide - Trissomia 13, XX` |
| oocito_id=121015, presc_id=161974 | `embryo_description` | `nan` | `Aneuploide - Trissomia 13, XX` |
| oocito_id=121015, presc_id=161975 | `embryo_description` | `nan` | `Aneuploide - Trissomia 13, XX` |
| oocito_id=121015, presc_id=161972 | `embryo_description` | `nan` | `Aneuploide - Trissomia 13, XX` |
| oocito_id=121015, presc_id=161970 | `embryo_description` | `nan` | `Aneuploide - Trissomia 13, XX` |
| oocito_id=122624, presc_id=177439 | `embryo_description` | `nan` | `NORMAL/EUPLOIDE, XY` |
| oocito_id=122625, presc_id=177439 | `embryo_description` | `nan` | `NORMAL/EUPLOIDE, XX` |
| oocito_id=122856, presc_id=179489 | `embryo_description` | `nan` | `EUPLOIDE, XY` |
| oocito_id=122624, presc_id=177436 | `embryo_description` | `nan` | `NORMAL/EUPLOIDE, XY` |
| oocito_id=187968, presc_id=459996 | `embryo_embryo_date` | `2024-03-11 00:00:00` | `2024-03-11` |
| oocito_id=188017, presc_id=426825 | `embryo_embryo_date` | `2024-03-11 00:00:00` | `2024-03-11` |
| oocito_id=188019, presc_id=426825 | `embryo_embryo_date` | `2024-03-11 00:00:00` | `2024-03-11` |
| oocito_id=188021, presc_id=426825 | `embryo_embryo_date` | `2024-03-11 00:00:00` | `2024-03-11` |
| oocito_id=188023, presc_id=460012 | `embryo_embryo_date` | `2024-03-11 00:00:00` | `2024-03-11` |
| oocito_id=188191, presc_id=424471 | `embryo_embryo_date` | `2024-03-13 00:00:00` | `2024-03-13` |
| oocito_id=188192, presc_id=424471 | `embryo_embryo_date` | `2024-03-13 00:00:00` | `2024-03-13` |
| oocito_id=188434, presc_id=430052 | `embryo_embryo_date` | `2024-03-14 00:00:00` | `2024-03-14` |
| oocito_id=188436, presc_id=414726 | `embryo_embryo_date` | `2024-03-14 00:00:00` | `2024-03-14` |
| oocito_id=188437, presc_id=414726 | `embryo_embryo_date` | `2024-03-14 00:00:00` | `2024-03-14` |
| oocito_id=189523, presc_id=466382 | `embryo_embryo_description_id` | `AA4` | `AA3` |
| oocito_id=189524, presc_id=466382 | `embryo_embryo_description_id` | `AA5` | `AA4` |
| oocito_id=189523, presc_id=466381 | `embryo_embryo_description_id` | `AA4` | `AA3` |
| oocito_id=189524, presc_id=466381 | `embryo_embryo_description_id` | `AA5` | `AA4` |
| oocito_id=189523, presc_id=466380 | `embryo_embryo_description_id` | `AA4` | `AA3` |
| oocito_id=189524, presc_id=466380 | `embryo_embryo_description_id` | `AA5` | `AA4` |
| oocito_id=189523, presc_id=466379 | `embryo_embryo_description_id` | `AA4` | `AA3` |
| oocito_id=189524, presc_id=466379 | `embryo_embryo_description_id` | `AA5` | `AA4` |
| oocito_id=189523, presc_id=466378 | `embryo_embryo_description_id` | `AA4` | `AA3` |
| oocito_id=189524, presc_id=466378 | `embryo_embryo_description_id` | `AA5` | `AA4` |
| oocito_id=188019, presc_id=426825 | `embryo_embryo_fate` | `Freeze` | `FrozenEmbryoTransfer` |
| oocito_id=188895, presc_id=419196 | `embryo_embryo_fate` | `Freeze` | `FrozenEmbryoTransfer` |
| oocito_id=188019, presc_id=426824 | `embryo_embryo_fate` | `Freeze` | `FrozenEmbryoTransfer` |
| oocito_id=188895, presc_id=419199 | `embryo_embryo_fate` | `Freeze` | `FrozenEmbryoTransfer` |
| oocito_id=188019, presc_id=426827 | `embryo_embryo_fate` | `Freeze` | `FrozenEmbryoTransfer` |
| oocito_id=188895, presc_id=419200 | `embryo_embryo_fate` | `Freeze` | `FrozenEmbryoTransfer` |
| oocito_id=188019, presc_id=426826 | `embryo_embryo_fate` | `Freeze` | `FrozenEmbryoTransfer` |
| oocito_id=188895, presc_id=419198 | `embryo_embryo_fate` | `Freeze` | `FrozenEmbryoTransfer` |
| oocito_id=188019, presc_id=426828 | `embryo_embryo_fate` | `Freeze` | `FrozenEmbryoTransfer` |
| oocito_id=188895, presc_id=419197 | `embryo_embryo_fate` | `Freeze` | `FrozenEmbryoTransfer` |
| oocito_id=189523, presc_id=466382 | `embryo_embryo_id` | `D2024.03.22_S02048_I4120_P-4` | `D2024.03.22_S02048_I4120_P-3` |
| oocito_id=189524, presc_id=466382 | `embryo_embryo_id` | `D2024.03.22_S02048_I4120_P-5` | `D2024.03.22_S02048_I4120_P-4` |
| oocito_id=189523, presc_id=466381 | `embryo_embryo_id` | `D2024.03.22_S02048_I4120_P-4` | `D2024.03.22_S02048_I4120_P-3` |
| oocito_id=189524, presc_id=466381 | `embryo_embryo_id` | `D2024.03.22_S02048_I4120_P-5` | `D2024.03.22_S02048_I4120_P-4` |
| oocito_id=189523, presc_id=466380 | `embryo_embryo_id` | `D2024.03.22_S02048_I4120_P-4` | `D2024.03.22_S02048_I4120_P-3` |
| oocito_id=189524, presc_id=466380 | `embryo_embryo_id` | `D2024.03.22_S02048_I4120_P-5` | `D2024.03.22_S02048_I4120_P-4` |
| oocito_id=189523, presc_id=466379 | `embryo_embryo_id` | `D2024.03.22_S02048_I4120_P-4` | `D2024.03.22_S02048_I4120_P-3` |
| oocito_id=189524, presc_id=466379 | `embryo_embryo_id` | `D2024.03.22_S02048_I4120_P-5` | `D2024.03.22_S02048_I4120_P-4` |
| oocito_id=189523, presc_id=466378 | `embryo_embryo_id` | `D2024.03.22_S02048_I4120_P-4` | `D2024.03.22_S02048_I4120_P-3` |
| oocito_id=189524, presc_id=466378 | `embryo_embryo_id` | `D2024.03.22_S02048_I4120_P-5` | `D2024.03.22_S02048_I4120_P-4` |
| oocito_id=120480, presc_id=160767 | `embryo_fertilization_method` | `nan` | `ICSI` |
| oocito_id=121018, presc_id=161636 | `embryo_fertilization_method` | `nan` | `ICSI` |
| oocito_id=121019, presc_id=161636 | `embryo_fertilization_method` | `nan` | `ICSI` |
| oocito_id=121020, presc_id=161636 | `embryo_fertilization_method` | `nan` | `ICSI` |
| oocito_id=121015, presc_id=161973 | `embryo_fertilization_method` | `nan` | `ICSI` |
| oocito_id=120480, presc_id=160768 | `embryo_fertilization_method` | `nan` | `ICSI` |
| oocito_id=121018, presc_id=161633 | `embryo_fertilization_method` | `nan` | `ICSI` |
| oocito_id=121019, presc_id=161633 | `embryo_fertilization_method` | `nan` | `ICSI` |
| oocito_id=121020, presc_id=161633 | `embryo_fertilization_method` | `nan` | `ICSI` |
| oocito_id=121015, presc_id=161971 | `embryo_fertilization_method` | `nan` | `ICSI` |
| oocito_id=210824, presc_id=509620 | `embryo_fertilization_time` | `2024-08-09 12:45:00` | `2024-08-09 12:25:00` |
| oocito_id=210825, presc_id=509620 | `embryo_fertilization_time` | `2024-08-09 12:45:00` | `2024-08-09 12:25:00` |
| oocito_id=210826, presc_id=509620 | `embryo_fertilization_time` | `2024-08-09 12:45:00` | `2024-08-09 12:25:00` |
| oocito_id=210827, presc_id=509620 | `embryo_fertilization_time` | `2024-08-09 12:45:00` | `2024-08-09 12:25:00` |
| oocito_id=210824, presc_id=509621 | `embryo_fertilization_time` | `2024-08-09 12:45:00` | `2024-08-09 12:25:00` |
| oocito_id=210825, presc_id=509621 | `embryo_fertilization_time` | `2024-08-09 12:45:00` | `2024-08-09 12:25:00` |
| oocito_id=210826, presc_id=509621 | `embryo_fertilization_time` | `2024-08-09 12:45:00` | `2024-08-09 12:25:00` |
| oocito_id=210827, presc_id=509621 | `embryo_fertilization_time` | `2024-08-09 12:45:00` | `2024-08-09 12:25:00` |
| oocito_id=210824, presc_id=509619 | `embryo_fertilization_time` | `2024-08-09 12:45:00` | `2024-08-09 12:25:00` |
| oocito_id=210825, presc_id=509619 | `embryo_fertilization_time` | `2024-08-09 12:45:00` | `2024-08-09 12:25:00` |
| oocito_id=200939, presc_id=486080 | `embryo_kid_date` | `2025-02-05 00:00:00` | `2025-08-18 00:00:00` |
| oocito_id=200940, presc_id=486080 | `embryo_kid_date` | `2025-02-05 00:00:00` | `2025-08-18 00:00:00` |
| oocito_id=200941, presc_id=486080 | `embryo_kid_date` | `2025-02-05 00:00:00` | `2025-08-18 00:00:00` |
| oocito_id=200942, presc_id=486080 | `embryo_kid_date` | `2025-02-05 00:00:00` | `2025-08-18 00:00:00` |
| oocito_id=200943, presc_id=486080 | `embryo_kid_date` | `2025-02-05 00:00:00` | `2025-08-18 00:00:00` |
| oocito_id=200944, presc_id=486080 | `embryo_kid_date` | `2025-02-05 00:00:00` | `2025-08-18 00:00:00` |
| oocito_id=200939, presc_id=486078 | `embryo_kid_date` | `2025-02-05 00:00:00` | `2025-08-18 00:00:00` |
| oocito_id=200940, presc_id=486078 | `embryo_kid_date` | `2025-02-05 00:00:00` | `2025-08-18 00:00:00` |
| oocito_id=200941, presc_id=486078 | `embryo_kid_date` | `2025-02-05 00:00:00` | `2025-08-18 00:00:00` |
| oocito_id=200942, presc_id=486078 | `embryo_kid_date` | `2025-02-05 00:00:00` | `2025-08-18 00:00:00` |
| oocito_id=251143, presc_id=721862 | `embryo_kid_score` | `nan` | `8.6` |
| oocito_id=251146, presc_id=721862 | `embryo_kid_score` | `nan` | `7` |
| oocito_id=251147, presc_id=721862 | `embryo_kid_score` | `nan` | `9.3` |
| oocito_id=251141, presc_id=721862 | `embryo_kid_score` | `nan` | `8.1` |
| oocito_id=251142, presc_id=721862 | `embryo_kid_score` | `nan` | `NA` |
| oocito_id=251144, presc_id=721862 | `embryo_kid_score` | `nan` | `1.6` |
| oocito_id=251145, presc_id=721862 | `embryo_kid_score` | `nan` | `9.3` |
| oocito_id=251148, presc_id=721862 | `embryo_kid_score` | `nan` | `NA` |
| oocito_id=251143, presc_id=721861 | `embryo_kid_score` | `nan` | `8.6` |
| oocito_id=251146, presc_id=721861 | `embryo_kid_score` | `nan` | `7` |
| oocito_id=200939, presc_id=486080 | `embryo_kid_user` | `BLENDA` | `TAYNARA` |
| oocito_id=200940, presc_id=486080 | `embryo_kid_user` | `BLENDA` | `TAYNARA` |
| oocito_id=200941, presc_id=486080 | `embryo_kid_user` | `BLENDA` | `TAYNARA` |
| oocito_id=200942, presc_id=486080 | `embryo_kid_user` | `BLENDA` | `TAYNARA` |
| oocito_id=200943, presc_id=486080 | `embryo_kid_user` | `BLENDA` | `TAYNARA` |
| oocito_id=200944, presc_id=486080 | `embryo_kid_user` | `BLENDA` | `TAYNARA` |
| oocito_id=200939, presc_id=486078 | `embryo_kid_user` | `BLENDA` | `TAYNARA` |
| oocito_id=200940, presc_id=486078 | `embryo_kid_user` | `BLENDA` | `TAYNARA` |
| oocito_id=200941, presc_id=486078 | `embryo_kid_user` | `BLENDA` | `TAYNARA` |
| oocito_id=200942, presc_id=486078 | `embryo_kid_user` | `BLENDA` | `TAYNARA` |
| oocito_id=251143, presc_id=721862 | `embryo_kid_version` | `nan` | `KIDScoreD5 v3.3` |
| oocito_id=251146, presc_id=721862 | `embryo_kid_version` | `nan` | `KIDScoreD5 v3.3` |
| oocito_id=251147, presc_id=721862 | `embryo_kid_version` | `nan` | `KIDScoreD5 v3.3` |
| oocito_id=251141, presc_id=721862 | `embryo_kid_version` | `nan` | `KIDScoreD5 v3.3` |
| oocito_id=251142, presc_id=721862 | `embryo_kid_version` | `nan` | `KIDScoreD5 v3.3` |
| oocito_id=251144, presc_id=721862 | `embryo_kid_version` | `nan` | `KIDScoreD5 v3.3` |
| oocito_id=251145, presc_id=721862 | `embryo_kid_version` | `nan` | `KIDScoreD5 v3.3` |
| oocito_id=251148, presc_id=721862 | `embryo_kid_version` | `nan` | `KIDScoreD5 v3.3` |
| oocito_id=251143, presc_id=721861 | `embryo_kid_version` | `nan` | `KIDScoreD5 v3.3` |
| oocito_id=251146, presc_id=721861 | `embryo_kid_version` | `nan` | `KIDScoreD5 v3.3` |
| oocito_id=210824, presc_id=509620 | `embryo_position` | `8` | `7` |
| oocito_id=210825, presc_id=509620 | `embryo_position` | `8` | `7` |
| oocito_id=210826, presc_id=509620 | `embryo_position` | `8` | `7` |
| oocito_id=210827, presc_id=509620 | `embryo_position` | `8` | `7` |
| oocito_id=210824, presc_id=509621 | `embryo_position` | `8` | `7` |
| oocito_id=210825, presc_id=509621 | `embryo_position` | `8` | `7` |
| oocito_id=210826, presc_id=509621 | `embryo_position` | `8` | `7` |
| oocito_id=210827, presc_id=509621 | `embryo_position` | `8` | `7` |
| oocito_id=210824, presc_id=509619 | `embryo_position` | `8` | `7` |
| oocito_id=210825, presc_id=509619 | `embryo_position` | `8` | `7` |
| oocito_id=188191, presc_id=424471 | `embryo_time_fragmentation` | `23.7` | `53.6` |
| oocito_id=188192, presc_id=424471 | `embryo_time_fragmentation` | `24.0` | `71.7` |
| oocito_id=188434, presc_id=430052 | `embryo_time_fragmentation` | `22.8` | `62.5` |
| oocito_id=188965, presc_id=513205 | `embryo_time_fragmentation` | `25.5` | `58.1` |
| oocito_id=189522, presc_id=466382 | `embryo_time_fragmentation` | `26.7` | `66.5` |
| oocito_id=187834, presc_id=559979 | `embryo_time_fragmentation` | `22.7` | `62.7` |
| oocito_id=187832, presc_id=559979 | `embryo_time_fragmentation` | `24.7` | `70.8` |
| oocito_id=187833, presc_id=559979 | `embryo_time_fragmentation` | `24.7` | `49.7` |
| oocito_id=187837, presc_id=559979 | `embryo_time_fragmentation` | `26.5` | `71.4` |
| oocito_id=187838, presc_id=559979 | `embryo_time_fragmentation` | `24.6` | `61.5` |
| oocito_id=190190, presc_id=434243 | `embryo_time_t2` | `23.3` | `27.6` |
| oocito_id=190191, presc_id=434243 | `embryo_time_t2` | `nan` | `23.3` |
| oocito_id=190189, presc_id=434243 | `embryo_time_t2` | `27.6` | `nan` |
| oocito_id=190190, presc_id=434244 | `embryo_time_t2` | `23.3` | `27.6` |
| oocito_id=190191, presc_id=434244 | `embryo_time_t2` | `nan` | `23.3` |
| oocito_id=190189, presc_id=434244 | `embryo_time_t2` | `27.6` | `nan` |
| oocito_id=190190, presc_id=434242 | `embryo_time_t2` | `23.3` | `27.6` |
| oocito_id=190191, presc_id=434242 | `embryo_time_t2` | `nan` | `23.3` |
| oocito_id=190189, presc_id=434242 | `embryo_time_t2` | `27.6` | `nan` |
| oocito_id=190190, presc_id=434245 | `embryo_time_t2` | `23.3` | `27.6` |
| oocito_id=189523, presc_id=466382 | `embryo_time_t3` | `36.4` | `nan` |
| oocito_id=189524, presc_id=466382 | `embryo_time_t3` | `nan` | `36.4` |
| oocito_id=189523, presc_id=466381 | `embryo_time_t3` | `36.4` | `nan` |
| oocito_id=189524, presc_id=466381 | `embryo_time_t3` | `nan` | `36.4` |
| oocito_id=189523, presc_id=466380 | `embryo_time_t3` | `36.4` | `nan` |
| oocito_id=189524, presc_id=466380 | `embryo_time_t3` | `nan` | `36.4` |
| oocito_id=189523, presc_id=466379 | `embryo_time_t3` | `36.4` | `nan` |
| oocito_id=189524, presc_id=466379 | `embryo_time_t3` | `nan` | `36.4` |
| oocito_id=189523, presc_id=466378 | `embryo_time_t3` | `36.4` | `nan` |
| oocito_id=189524, presc_id=466378 | `embryo_time_t3` | `nan` | `36.4` |
| oocito_id=189523, presc_id=466382 | `embryo_time_t4` | `52.0` | `nan` |
| oocito_id=189524, presc_id=466382 | `embryo_time_t4` | `nan` | `52.0` |
| oocito_id=189523, presc_id=466381 | `embryo_time_t4` | `52.0` | `nan` |
| oocito_id=189524, presc_id=466381 | `embryo_time_t4` | `nan` | `52.0` |
| oocito_id=189523, presc_id=466380 | `embryo_time_t4` | `52.0` | `nan` |
| oocito_id=189524, presc_id=466380 | `embryo_time_t4` | `nan` | `52.0` |
| oocito_id=189523, presc_id=466379 | `embryo_time_t4` | `52.0` | `nan` |
| oocito_id=189524, presc_id=466379 | `embryo_time_t4` | `nan` | `52.0` |
| oocito_id=189523, presc_id=466378 | `embryo_time_t4` | `52.0` | `nan` |
| oocito_id=189524, presc_id=466378 | `embryo_time_t4` | `nan` | `52.0` |
| oocito_id=189523, presc_id=466382 | `embryo_time_t5` | `52.8` | `nan` |
| oocito_id=189524, presc_id=466382 | `embryo_time_t5` | `nan` | `52.8` |
| oocito_id=189523, presc_id=466381 | `embryo_time_t5` | `52.8` | `nan` |
| oocito_id=189524, presc_id=466381 | `embryo_time_t5` | `nan` | `52.8` |
| oocito_id=189523, presc_id=466380 | `embryo_time_t5` | `52.8` | `nan` |
| oocito_id=189524, presc_id=466380 | `embryo_time_t5` | `nan` | `52.8` |
| oocito_id=189523, presc_id=466379 | `embryo_time_t5` | `52.8` | `nan` |
| oocito_id=189524, presc_id=466379 | `embryo_time_t5` | `nan` | `52.8` |
| oocito_id=189523, presc_id=466378 | `embryo_time_t5` | `52.8` | `nan` |
| oocito_id=189524, presc_id=466378 | `embryo_time_t5` | `nan` | `52.8` |
| oocito_id=189523, presc_id=466382 | `embryo_time_t8` | `87.5` | `nan` |
| oocito_id=189524, presc_id=466382 | `embryo_time_t8` | `nan` | `87.5` |
| oocito_id=189523, presc_id=466381 | `embryo_time_t8` | `87.5` | `nan` |
| oocito_id=189524, presc_id=466381 | `embryo_time_t8` | `nan` | `87.5` |
| oocito_id=189523, presc_id=466380 | `embryo_time_t8` | `87.5` | `nan` |
| oocito_id=189524, presc_id=466380 | `embryo_time_t8` | `nan` | `87.5` |
| oocito_id=189523, presc_id=466379 | `embryo_time_t8` | `87.5` | `nan` |
| oocito_id=189524, presc_id=466379 | `embryo_time_t8` | `nan` | `87.5` |
| oocito_id=189523, presc_id=466378 | `embryo_time_t8` | `87.5` | `nan` |
| oocito_id=189524, presc_id=466378 | `embryo_time_t8` | `nan` | `87.5` |
| oocito_id=190190, presc_id=434243 | `embryo_time_tb` | `96.4` | `nan` |
| oocito_id=190191, presc_id=434243 | `embryo_time_tb` | `nan` | `96.4` |
| oocito_id=190190, presc_id=434244 | `embryo_time_tb` | `96.4` | `nan` |
| oocito_id=190191, presc_id=434244 | `embryo_time_tb` | `nan` | `96.4` |
| oocito_id=190190, presc_id=434242 | `embryo_time_tb` | `96.4` | `nan` |
| oocito_id=190191, presc_id=434242 | `embryo_time_tb` | `nan` | `96.4` |
| oocito_id=190190, presc_id=434245 | `embryo_time_tb` | `96.4` | `nan` |
| oocito_id=190191, presc_id=434245 | `embryo_time_tb` | `nan` | `96.4` |
| oocito_id=193144, presc_id=880618 | `embryo_time_tb` | `93.2` | `nan` |
| oocito_id=195514, presc_id=459367 | `embryo_time_tb` | `98.2` | `nan` |
| oocito_id=190190, presc_id=434243 | `embryo_time_teb` | `104.9` | `nan` |
| oocito_id=190191, presc_id=434243 | `embryo_time_teb` | `nan` | `104.9` |
| oocito_id=190190, presc_id=434244 | `embryo_time_teb` | `104.9` | `nan` |
| oocito_id=190191, presc_id=434244 | `embryo_time_teb` | `nan` | `104.9` |
| oocito_id=190190, presc_id=434242 | `embryo_time_teb` | `104.9` | `nan` |
| oocito_id=190191, presc_id=434242 | `embryo_time_teb` | `nan` | `104.9` |
| oocito_id=190190, presc_id=434245 | `embryo_time_teb` | `104.9` | `nan` |
| oocito_id=190191, presc_id=434245 | `embryo_time_teb` | `nan` | `104.9` |
| oocito_id=193144, presc_id=880618 | `embryo_time_teb` | `99.9` | `nan` |
| oocito_id=198632, presc_id=484967 | `embryo_time_teb` | `101.8` | `nan` |
| oocito_id=188021, presc_id=426825 | `embryo_time_thb` | `nan` | `115.1` |
| oocito_id=188023, presc_id=460012 | `embryo_time_thb` | `nan` | `110.9` |
| oocito_id=188436, presc_id=414726 | `embryo_time_thb` | `nan` | `105.0` |
| oocito_id=188444, presc_id=414726 | `embryo_time_thb` | `nan` | `112.0` |
| oocito_id=188904, presc_id=419196 | `embryo_time_thb` | `nan` | `108.8` |
| oocito_id=187814, presc_id=493420 | `embryo_time_thb` | `nan` | `111.7` |
| oocito_id=188443, presc_id=414726 | `embryo_time_thb` | `nan` | `115.7` |
| oocito_id=189498, presc_id=435160 | `embryo_time_thb` | `nan` | `123.8` |
| oocito_id=189240, presc_id=609237 | `embryo_time_thb` | `nan` | `111.3` |
| oocito_id=189549, presc_id=442267 | `embryo_time_thb` | `nan` | `111.2` |
| oocito_id=190190, presc_id=434243 | `embryo_time_tm` | `91.4` | `nan` |
| oocito_id=190191, presc_id=434243 | `embryo_time_tm` | `nan` | `91.4` |
| oocito_id=190190, presc_id=434244 | `embryo_time_tm` | `91.4` | `nan` |
| oocito_id=190191, presc_id=434244 | `embryo_time_tm` | `nan` | `91.4` |
| oocito_id=190190, presc_id=434242 | `embryo_time_tm` | `91.4` | `nan` |
| oocito_id=190191, presc_id=434242 | `embryo_time_tm` | `nan` | `91.4` |
| oocito_id=190190, presc_id=434245 | `embryo_time_tm` | `91.4` | `nan` |
| oocito_id=190191, presc_id=434245 | `embryo_time_tm` | `nan` | `91.4` |
| oocito_id=193144, presc_id=880618 | `embryo_time_tm` | `77.3` | `nan` |
| oocito_id=195514, presc_id=459367 | `embryo_time_tm` | `92.0` | `nan` |
| oocito_id=189523, presc_id=466382 | `embryo_time_tpna` | `11.6` | `nan` |
| oocito_id=189524, presc_id=466382 | `embryo_time_tpna` | `8.2` | `11.6` |
| oocito_id=189523, presc_id=466381 | `embryo_time_tpna` | `11.6` | `nan` |
| oocito_id=189524, presc_id=466381 | `embryo_time_tpna` | `8.2` | `11.6` |
| oocito_id=189523, presc_id=466380 | `embryo_time_tpna` | `11.6` | `nan` |
| oocito_id=189524, presc_id=466380 | `embryo_time_tpna` | `8.2` | `11.6` |
| oocito_id=189523, presc_id=466379 | `embryo_time_tpna` | `11.6` | `nan` |
| oocito_id=189524, presc_id=466379 | `embryo_time_tpna` | `8.2` | `11.6` |
| oocito_id=189523, presc_id=466378 | `embryo_time_tpna` | `11.6` | `nan` |
| oocito_id=189524, presc_id=466378 | `embryo_time_tpna` | `8.2` | `11.6` |
| oocito_id=189523, presc_id=466382 | `embryo_time_tpnf` | `33.0` | `nan` |
| oocito_id=189524, presc_id=466382 | `embryo_time_tpnf` | `nan` | `33.0` |
| oocito_id=189523, presc_id=466381 | `embryo_time_tpnf` | `33.0` | `nan` |
| oocito_id=189524, presc_id=466381 | `embryo_time_tpnf` | `nan` | `33.0` |
| oocito_id=189523, presc_id=466380 | `embryo_time_tpnf` | `33.0` | `nan` |
| oocito_id=189524, presc_id=466380 | `embryo_time_tpnf` | `nan` | `33.0` |
| oocito_id=189523, presc_id=466379 | `embryo_time_tpnf` | `33.0` | `nan` |
| oocito_id=189524, presc_id=466379 | `embryo_time_tpnf` | `nan` | `33.0` |
| oocito_id=189523, presc_id=466378 | `embryo_time_tpnf` | `33.0` | `nan` |
| oocito_id=189524, presc_id=466378 | `embryo_time_tpnf` | `nan` | `33.0` |
| oocito_id=190190, presc_id=434243 | `embryo_time_tsb` | `92.9` | `nan` |
| oocito_id=190191, presc_id=434243 | `embryo_time_tsb` | `nan` | `92.9` |
| oocito_id=190190, presc_id=434244 | `embryo_time_tsb` | `92.9` | `nan` |
| oocito_id=190191, presc_id=434244 | `embryo_time_tsb` | `nan` | `92.9` |
| oocito_id=190190, presc_id=434242 | `embryo_time_tsb` | `92.9` | `nan` |
| oocito_id=190191, presc_id=434242 | `embryo_time_tsb` | `nan` | `92.9` |
| oocito_id=190190, presc_id=434245 | `embryo_time_tsb` | `92.9` | `nan` |
| oocito_id=190191, presc_id=434245 | `embryo_time_tsb` | `nan` | `92.9` |
| oocito_id=193144, presc_id=880618 | `embryo_time_tsb` | `87.9` | `nan` |
| oocito_id=195514, presc_id=459367 | `embryo_time_tsb` | `93.1` | `nan` |
| oocito_id=190190, presc_id=434243 | `embryo_time_tsc` | `82.5` | `128.2` |
| oocito_id=190191, presc_id=434243 | `embryo_time_tsc` | `nan` | `82.5` |
| oocito_id=190189, presc_id=434243 | `embryo_time_tsc` | `128.2` | `nan` |
| oocito_id=190190, presc_id=434244 | `embryo_time_tsc` | `82.5` | `128.2` |
| oocito_id=190191, presc_id=434244 | `embryo_time_tsc` | `nan` | `82.5` |
| oocito_id=190189, presc_id=434244 | `embryo_time_tsc` | `128.2` | `nan` |
| oocito_id=190190, presc_id=434242 | `embryo_time_tsc` | `82.5` | `128.2` |
| oocito_id=190191, presc_id=434242 | `embryo_time_tsc` | `nan` | `82.5` |
| oocito_id=190189, presc_id=434242 | `embryo_time_tsc` | `128.2` | `nan` |
| oocito_id=190190, presc_id=434245 | `embryo_time_tsc` | `82.5` | `128.2` |
| oocito_id=280644, presc_id=818242 | `embryo_value_frag2cat` | `0-10%` | `nan` |
| oocito_id=280644, presc_id=818248 | `embryo_value_frag2cat` | `0-10%` | `nan` |
| oocito_id=280644, presc_id=818240 | `embryo_value_frag2cat` | `0-10%` | `nan` |
| oocito_id=280644, presc_id=818250 | `embryo_value_frag2cat` | `0-10%` | `nan` |
| oocito_id=280644, presc_id=818243 | `embryo_value_frag2cat` | `0-10%` | `nan` |
| oocito_id=280644, presc_id=818249 | `embryo_value_frag2cat` | `0-10%` | `nan` |
| oocito_id=280644, presc_id=818245 | `embryo_value_frag2cat` | `0-10%` | `nan` |
| oocito_id=280644, presc_id=818244 | `embryo_value_frag2cat` | `0-10%` | `nan` |
| oocito_id=280644, presc_id=818246 | `embryo_value_frag2cat` | `0-10%` | `nan` |
| oocito_id=280644, presc_id=818241 | `embryo_value_frag2cat` | `0-10%` | `nan` |
| oocito_id=190190, presc_id=434243 | `embryo_value_icm` | `B` | `nan` |
| oocito_id=190191, presc_id=434243 | `embryo_value_icm` | `nan` | `B` |
| oocito_id=190190, presc_id=434244 | `embryo_value_icm` | `B` | `nan` |
| oocito_id=190191, presc_id=434244 | `embryo_value_icm` | `nan` | `B` |
| oocito_id=190190, presc_id=434242 | `embryo_value_icm` | `B` | `nan` |
| oocito_id=190191, presc_id=434242 | `embryo_value_icm` | `nan` | `B` |
| oocito_id=190190, presc_id=434245 | `embryo_value_icm` | `B` | `nan` |
| oocito_id=190191, presc_id=434245 | `embryo_value_icm` | `nan` | `B` |
| oocito_id=193144, presc_id=880618 | `embryo_value_icm` | `A` | `nan` |
| oocito_id=195514, presc_id=459367 | `embryo_value_icm` | `B` | `nan` |
| oocito_id=280644, presc_id=818242 | `embryo_value_mn2_type` | `Mono` | `nan` |
| oocito_id=280645, presc_id=818242 | `embryo_value_mn2_type` | `Multi` | `Mono` |
| oocito_id=280644, presc_id=818248 | `embryo_value_mn2_type` | `Mono` | `nan` |
| oocito_id=280645, presc_id=818248 | `embryo_value_mn2_type` | `Multi` | `Mono` |
| oocito_id=280644, presc_id=818240 | `embryo_value_mn2_type` | `Mono` | `nan` |
| oocito_id=280645, presc_id=818240 | `embryo_value_mn2_type` | `Multi` | `Mono` |
| oocito_id=280644, presc_id=818250 | `embryo_value_mn2_type` | `Mono` | `nan` |
| oocito_id=280645, presc_id=818250 | `embryo_value_mn2_type` | `Multi` | `Mono` |
| oocito_id=280644, presc_id=818243 | `embryo_value_mn2_type` | `Mono` | `nan` |
| oocito_id=280645, presc_id=818243 | `embryo_value_mn2_type` | `Multi` | `Mono` |
| oocito_id=280644, presc_id=818242 | `embryo_value_nuclei2` | `2` | `nan` |
| oocito_id=280644, presc_id=818248 | `embryo_value_nuclei2` | `2` | `nan` |
| oocito_id=280644, presc_id=818240 | `embryo_value_nuclei2` | `2` | `nan` |
| oocito_id=280644, presc_id=818250 | `embryo_value_nuclei2` | `2` | `nan` |
| oocito_id=280644, presc_id=818243 | `embryo_value_nuclei2` | `2` | `nan` |
| oocito_id=280644, presc_id=818249 | `embryo_value_nuclei2` | `2` | `nan` |
| oocito_id=280644, presc_id=818245 | `embryo_value_nuclei2` | `2` | `nan` |
| oocito_id=280644, presc_id=818244 | `embryo_value_nuclei2` | `2` | `nan` |
| oocito_id=280644, presc_id=818246 | `embryo_value_nuclei2` | `2` | `nan` |
| oocito_id=280644, presc_id=818241 | `embryo_value_nuclei2` | `2` | `nan` |
| oocito_id=189523, presc_id=466382 | `embryo_value_pn` | `2` | `nan` |
| oocito_id=189523, presc_id=466381 | `embryo_value_pn` | `2` | `nan` |
| oocito_id=189523, presc_id=466380 | `embryo_value_pn` | `2` | `nan` |
| oocito_id=189523, presc_id=466379 | `embryo_value_pn` | `2` | `nan` |
| oocito_id=189523, presc_id=466378 | `embryo_value_pn` | `2` | `nan` |
| oocito_id=190191, presc_id=434243 | `embryo_value_pn` | `nan` | `2` |
| oocito_id=190189, presc_id=434243 | `embryo_value_pn` | `2` | `nan` |
| oocito_id=190308, presc_id=513202 | `embryo_value_pn` | `0` | `4` |
| oocito_id=190191, presc_id=434244 | `embryo_value_pn` | `nan` | `2` |
| oocito_id=190189, presc_id=434244 | `embryo_value_pn` | `2` | `nan` |
| oocito_id=190190, presc_id=434243 | `embryo_value_te` | `B` | `nan` |
| oocito_id=190191, presc_id=434243 | `embryo_value_te` | `nan` | `B` |
| oocito_id=190190, presc_id=434244 | `embryo_value_te` | `B` | `nan` |
| oocito_id=190191, presc_id=434244 | `embryo_value_te` | `nan` | `B` |
| oocito_id=190190, presc_id=434242 | `embryo_value_te` | `B` | `nan` |
| oocito_id=190191, presc_id=434242 | `embryo_value_te` | `nan` | `B` |
| oocito_id=190190, presc_id=434245 | `embryo_value_te` | `B` | `nan` |
| oocito_id=190191, presc_id=434245 | `embryo_value_te` | `nan` | `B` |
| oocito_id=193144, presc_id=880618 | `embryo_value_te` | `B` | `nan` |
| oocito_id=195514, presc_id=459367 | `embryo_value_te` | `C` | `nan` |
| oocito_id=189523, presc_id=466382 | `embryo_well_number` | `4` | `3` |
| oocito_id=189524, presc_id=466382 | `embryo_well_number` | `5` | `4` |
| oocito_id=189523, presc_id=466381 | `embryo_well_number` | `4` | `3` |
| oocito_id=189524, presc_id=466381 | `embryo_well_number` | `5` | `4` |
| oocito_id=189523, presc_id=466380 | `embryo_well_number` | `4` | `3` |
| oocito_id=189524, presc_id=466380 | `embryo_well_number` | `5` | `4` |
| oocito_id=189523, presc_id=466379 | `embryo_well_number` | `4` | `3` |
| oocito_id=189524, presc_id=466379 | `embryo_well_number` | `5` | `4` |
| oocito_id=189523, presc_id=466378 | `embryo_well_number` | `4` | `3` |
| oocito_id=189524, presc_id=466378 | `embryo_well_number` | `5` | `4` |
| oocito_id=188017, presc_id=426825 | `fet_gravidez_bioquimica` | `nan` | `1.0` |
| oocito_id=188019, presc_id=426825 | `fet_gravidez_bioquimica` | `nan` | `1.0` |
| oocito_id=188021, presc_id=426825 | `fet_gravidez_bioquimica` | `nan` | `1.0` |
| oocito_id=188191, presc_id=424471 | `fet_gravidez_bioquimica` | `nan` | `0.0` |
| oocito_id=188192, presc_id=424471 | `fet_gravidez_bioquimica` | `nan` | `0.0` |
| oocito_id=188895, presc_id=419196 | `fet_gravidez_bioquimica` | `nan` | `1.0` |
| oocito_id=188903, presc_id=419196 | `fet_gravidez_bioquimica` | `nan` | `1.0` |
| oocito_id=188904, presc_id=419196 | `fet_gravidez_bioquimica` | `nan` | `1.0` |
| oocito_id=189522, presc_id=466382 | `fet_gravidez_bioquimica` | `nan` | `0` |
| oocito_id=189543, presc_id=442267 | `fet_gravidez_bioquimica` | `nan` | `0.0` |
| oocito_id=188017, presc_id=426825 | `fet_gravidez_clinica` | `1` | `0.0` |
| oocito_id=188019, presc_id=426825 | `fet_gravidez_clinica` | `1` | `0.0` |
| oocito_id=188021, presc_id=426825 | `fet_gravidez_clinica` | `1` | `0.0` |
| oocito_id=188953, presc_id=460068 | `fet_gravidez_clinica` | `1` | `nan` |
| oocito_id=189522, presc_id=466382 | `fet_gravidez_clinica` | `1` | `0` |
| oocito_id=189541, presc_id=442267 | `fet_gravidez_clinica` | `1` | `nan` |
| oocito_id=189542, presc_id=442267 | `fet_gravidez_clinica` | `0` | `nan` |
| oocito_id=189545, presc_id=442267 | `fet_gravidez_clinica` | `1` | `nan` |
| oocito_id=189550, presc_id=442267 | `fet_gravidez_clinica` | `0` | `nan` |
| oocito_id=187834, presc_id=559979 | `fet_gravidez_clinica` | `1` | `nan` |
| oocito_id=187968, presc_id=459996 | `fet_resultado` | `nan` | `EMBRYO TRANSFER` |
| oocito_id=188953, presc_id=460068 | `fet_resultado` | `EMBRYO TRANSFER` | `nan` |
| oocito_id=189541, presc_id=442267 | `fet_resultado` | `EMBRYO TRANSFER` | `nan` |
| oocito_id=189542, presc_id=442267 | `fet_resultado` | `EMBRYO TRANSFER` | `nan` |
| oocito_id=189545, presc_id=442267 | `fet_resultado` | `EMBRYO TRANSFER` | `nan` |
| oocito_id=189550, presc_id=442267 | `fet_resultado` | `EMBRYO TRANSFER` | `nan` |
| oocito_id=189499, presc_id=435160 | `fet_resultado` | `nan` | `EMBRYO TRANSFER` |
| oocito_id=189502, presc_id=435160 | `fet_resultado` | `nan` | `EMBRYO TRANSFER` |
| oocito_id=189503, presc_id=435160 | `fet_resultado` | `nan` | `EMBRYO TRANSFER` |
| oocito_id=189504, presc_id=435160 | `fet_resultado` | `nan` | `EMBRYO TRANSFER` |
| oocito_id=187968, presc_id=459996 | `fet_tipo_resultado` | `nan` | `NÃO ENGRAVIDOU` |
| oocito_id=188017, presc_id=426825 | `fet_tipo_resultado` | `ABORTO` | `POSITIVO` |
| oocito_id=188019, presc_id=426825 | `fet_tipo_resultado` | `ABORTO` | `POSITIVO` |
| oocito_id=188021, presc_id=426825 | `fet_tipo_resultado` | `ABORTO` | `POSITIVO` |
| oocito_id=188953, presc_id=460068 | `fet_tipo_resultado` | `NASCIMENTO` | `nan` |
| oocito_id=189522, presc_id=466382 | `fet_tipo_resultado` | `POSITIVO` | `NÃO ENGRAVIDOU` |
| oocito_id=189541, presc_id=442267 | `fet_tipo_resultado` | `POSITIVO` | `nan` |
| oocito_id=189542, presc_id=442267 | `fet_tipo_resultado` | `BIOQUIMICA` | `nan` |
| oocito_id=189543, presc_id=442267 | `fet_tipo_resultado` | `BIOQUIMICA` | `NÃO ENGRAVIDOU` |
| oocito_id=189544, presc_id=442267 | `fet_tipo_resultado` | `BIOQUIMICA` | `NÃO ENGRAVIDOU` |
| oocito_id=196658, presc_id=450586 | `gestational_age_at_delivery` | `nan` | `39.0` |
| oocito_id=196658, presc_id=450582 | `gestational_age_at_delivery` | `nan` | `39.0` |
| oocito_id=196658, presc_id=450585 | `gestational_age_at_delivery` | `nan` | `39.0` |
| oocito_id=196658, presc_id=450584 | `gestational_age_at_delivery` | `nan` | `39.0` |
| oocito_id=196658, presc_id=450583 | `gestational_age_at_delivery` | `nan` | `39.0` |
| oocito_id=203626, presc_id=479181 | `gestational_age_at_delivery` | `nan` | `40.0` |
| oocito_id=203627, presc_id=479181 | `gestational_age_at_delivery` | `nan` | `40.0` |
| oocito_id=203626, presc_id=479184 | `gestational_age_at_delivery` | `nan` | `40.0` |
| oocito_id=203627, presc_id=479184 | `gestational_age_at_delivery` | `nan` | `40.0` |
| oocito_id=203626, presc_id=479183 | `gestational_age_at_delivery` | `nan` | `40.0` |
| oocito_id=187968, presc_id=459996 | `has_biopsy` | `False` | `True` |
| oocito_id=188017, presc_id=426825 | `has_biopsy` | `False` | `True` |
| oocito_id=188019, presc_id=426825 | `has_biopsy` | `False` | `True` |
| oocito_id=188021, presc_id=426825 | `has_biopsy` | `False` | `True` |
| oocito_id=188023, presc_id=460012 | `has_biopsy` | `False` | `True` |
| oocito_id=188191, presc_id=424471 | `has_biopsy` | `False` | `True` |
| oocito_id=188192, presc_id=424471 | `has_biopsy` | `False` | `True` |
| oocito_id=188434, presc_id=430052 | `has_biopsy` | `False` | `True` |
| oocito_id=188436, presc_id=414726 | `has_biopsy` | `False` | `True` |
| oocito_id=188437, presc_id=414726 | `has_biopsy` | `False` | `True` |
| oocito_id=188436, presc_id=414726 | `has_transfer_logging_gap` | `False` | `True` |
| oocito_id=188437, presc_id=414726 | `has_transfer_logging_gap` | `False` | `True` |
| oocito_id=188438, presc_id=414726 | `has_transfer_logging_gap` | `False` | `True` |
| oocito_id=188439, presc_id=414726 | `has_transfer_logging_gap` | `False` | `True` |
| oocito_id=188442, presc_id=414726 | `has_transfer_logging_gap` | `False` | `True` |
| oocito_id=188444, presc_id=414726 | `has_transfer_logging_gap` | `False` | `True` |
| oocito_id=188445, presc_id=414726 | `has_transfer_logging_gap` | `False` | `True` |
| oocito_id=188440, presc_id=414726 | `has_transfer_logging_gap` | `False` | `True` |
| oocito_id=188441, presc_id=414726 | `has_transfer_logging_gap` | `False` | `True` |
| oocito_id=188443, presc_id=414726 | `has_transfer_logging_gap` | `False` | `True` |
| oocito_id=187968, presc_id=459996 | `has_valid_outcome` | `False` | `True` |
| oocito_id=189499, presc_id=435160 | `has_valid_outcome` | `False` | `True` |
| oocito_id=189502, presc_id=435160 | `has_valid_outcome` | `False` | `True` |
| oocito_id=189503, presc_id=435160 | `has_valid_outcome` | `False` | `True` |
| oocito_id=189504, presc_id=435160 | `has_valid_outcome` | `False` | `True` |
| oocito_id=189505, presc_id=435160 | `has_valid_outcome` | `False` | `True` |
| oocito_id=187814, presc_id=493420 | `has_valid_outcome` | `False` | `True` |
| oocito_id=187964, presc_id=459996 | `has_valid_outcome` | `False` | `True` |
| oocito_id=187965, presc_id=459996 | `has_valid_outcome` | `False` | `True` |
| oocito_id=187966, presc_id=459996 | `has_valid_outcome` | `False` | `True` |
| oocito_id=188434, presc_id=430052 | `incubadora_padronizada` | `Embryoscope` | `nan` |
| oocito_id=188965, presc_id=513205 | `incubadora_padronizada` | `THERMO` | `nan` |
| oocito_id=189522, presc_id=466382 | `incubadora_padronizada` | `Embryoscope` | `nan` |
| oocito_id=189542, presc_id=442267 | `incubadora_padronizada` | `nan` | `Embryoscope` |
| oocito_id=189550, presc_id=442267 | `incubadora_padronizada` | `nan` | `Embryoscope` |
| oocito_id=187834, presc_id=559979 | `incubadora_padronizada` | `Embryoscope` | `nan` |
| oocito_id=188434, presc_id=430057 | `incubadora_padronizada` | `Embryoscope` | `nan` |
| oocito_id=188965, presc_id=513206 | `incubadora_padronizada` | `THERMO` | `nan` |
| oocito_id=189522, presc_id=466381 | `incubadora_padronizada` | `Embryoscope` | `nan` |
| oocito_id=189542, presc_id=442266 | `incubadora_padronizada` | `nan` | `Embryoscope` |
| oocito_id=188442, presc_id=414726 | `is_transferred_combined` | `<NA>` | `False` |
| oocito_id=188445, presc_id=414726 | `is_transferred_combined` | `<NA>` | `False` |
| oocito_id=188965, presc_id=513205 | `is_transferred_combined` | `<NA>` | `False` |
| oocito_id=187832, presc_id=559979 | `is_transferred_combined` | `<NA>` | `False` |
| oocito_id=187833, presc_id=559979 | `is_transferred_combined` | `<NA>` | `False` |
| oocito_id=187837, presc_id=559979 | `is_transferred_combined` | `<NA>` | `False` |
| oocito_id=188603, presc_id=505144 | `is_transferred_combined` | `<NA>` | `False` |
| oocito_id=188605, presc_id=505144 | `is_transferred_combined` | `<NA>` | `False` |
| oocito_id=188608, presc_id=505144 | `is_transferred_combined` | `<NA>` | `False` |
| oocito_id=188609, presc_id=505144 | `is_transferred_combined` | `<NA>` | `False` |
| oocito_id=188434, presc_id=430052 | `join_step` | `1` | `nan` |
| oocito_id=188434, presc_id=430057 | `join_step` | `1` | `nan` |
| oocito_id=188434, presc_id=430055 | `join_step` | `1` | `nan` |
| oocito_id=188434, presc_id=430056 | `join_step` | `1` | `nan` |
| oocito_id=188434, presc_id=430053 | `join_step` | `1` | `nan` |
| oocito_id=188434, presc_id=430058 | `join_step` | `1` | `nan` |
| oocito_id=188434, presc_id=430054 | `join_step` | `1` | `nan` |
| oocito_id=188434, presc_id=430051 | `join_step` | `1` | `nan` |
| oocito_id=191648, presc_id=452623 | `join_step` | `1` | `nan` |
| oocito_id=191647, presc_id=452623 | `join_step` | `1` | `nan` |
| oocito_id=187968, presc_id=459996 | `matched_planilha_prontuario` | `817641` | `830671.0` |
| oocito_id=188434, presc_id=430052 | `matched_planilha_prontuario` | `825035` | `nan` |
| oocito_id=188953, presc_id=460068 | `matched_planilha_prontuario` | `824096` | `902601.0` |
| oocito_id=188965, presc_id=513205 | `matched_planilha_prontuario` | `814863` | `224419.0` |
| oocito_id=189522, presc_id=466382 | `matched_planilha_prontuario` | `814815` | `167811.0` |
| oocito_id=189541, presc_id=442267 | `matched_planilha_prontuario` | `646331` | `186357.0` |
| oocito_id=189542, presc_id=442267 | `matched_planilha_prontuario` | `646331` | `806303.0` |
| oocito_id=189543, presc_id=442267 | `matched_planilha_prontuario` | `646331` | `664482.0` |
| oocito_id=189544, presc_id=442267 | `matched_planilha_prontuario` | `646331` | `664482.0` |
| oocito_id=189545, presc_id=442267 | `matched_planilha_prontuario` | `646331` | `186357.0` |
| oocito_id=187968, presc_id=459996 | `matched_planilha_transfer_date` | `NaT` | `2024-10-27` |
| oocito_id=188017, presc_id=426825 | `matched_planilha_transfer_date` | `2024-04-03 00:00:00` | `2025-10-30` |
| oocito_id=188019, presc_id=426825 | `matched_planilha_transfer_date` | `2024-04-03 00:00:00` | `2025-10-30` |
| oocito_id=188021, presc_id=426825 | `matched_planilha_transfer_date` | `2024-04-03 00:00:00` | `2025-10-30` |
| oocito_id=188023, presc_id=460012 | `matched_planilha_transfer_date` | `2024-05-20 00:00:00` | `2024-05-20` |
| oocito_id=188191, presc_id=424471 | `matched_planilha_transfer_date` | `2024-04-10 00:00:00` | `2024-08-07` |
| oocito_id=188192, presc_id=424471 | `matched_planilha_transfer_date` | `2024-04-10 00:00:00` | `2024-08-07` |
| oocito_id=188895, presc_id=419196 | `matched_planilha_transfer_date` | `2024-04-11 00:00:00` | `2024-04-11` |
| oocito_id=188903, presc_id=419196 | `matched_planilha_transfer_date` | `2024-04-11 00:00:00` | `2024-04-11` |
| oocito_id=188904, presc_id=419196 | `matched_planilha_transfer_date` | `2024-04-11 00:00:00` | `2024-04-11` |
| oocito_id=187968, presc_id=459996 | `matched_src_row_id` | `23658` | `14689.0` |
| oocito_id=188017, presc_id=426825 | `matched_src_row_id` | `17387` | `9127.0` |
| oocito_id=188019, presc_id=426825 | `matched_src_row_id` | `17387` | `9127.0` |
| oocito_id=188021, presc_id=426825 | `matched_src_row_id` | `17387` | `9127.0` |
| oocito_id=188023, presc_id=460012 | `matched_src_row_id` | `17415` | `10030.0` |
| oocito_id=188191, presc_id=424471 | `matched_src_row_id` | `17583` | `10095.0` |
| oocito_id=188192, presc_id=424471 | `matched_src_row_id` | `17583` | `10095.0` |
| oocito_id=188434, presc_id=430052 | `matched_src_row_id` | `23856` | `nan` |
| oocito_id=188436, presc_id=414726 | `matched_src_row_id` | `23661` | `6482.0` |
| oocito_id=188437, presc_id=414726 | `matched_src_row_id` | `23661` | `6482.0` |
| oocito_id=188953, presc_id=460068 | `merged_numero_de_nascidos` | `1` | `nan` |
| oocito_id=189522, presc_id=466382 | `merged_numero_de_nascidos` | `1` | `nan` |
| oocito_id=187834, presc_id=559979 | `merged_numero_de_nascidos` | `1` | `nan` |
| oocito_id=188953, presc_id=460067 | `merged_numero_de_nascidos` | `1` | `nan` |
| oocito_id=189522, presc_id=466381 | `merged_numero_de_nascidos` | `1` | `nan` |
| oocito_id=187834, presc_id=559976 | `merged_numero_de_nascidos` | `1` | `nan` |
| oocito_id=188953, presc_id=460069 | `merged_numero_de_nascidos` | `1` | `nan` |
| oocito_id=189522, presc_id=466380 | `merged_numero_de_nascidos` | `1` | `nan` |
| oocito_id=187834, presc_id=559978 | `merged_numero_de_nascidos` | `1` | `nan` |
| oocito_id=188953, presc_id=460065 | `merged_numero_de_nascidos` | `1` | `nan` |
| oocito_id=187968, presc_id=459996 | `micro_data_dl` | `2024-03-11 00:00:00` | `2024-03-11` |
| oocito_id=188017, presc_id=426825 | `micro_data_dl` | `2024-03-11 00:00:00` | `2024-03-11` |
| oocito_id=188019, presc_id=426825 | `micro_data_dl` | `2024-03-11 00:00:00` | `2024-03-11` |
| oocito_id=188021, presc_id=426825 | `micro_data_dl` | `2024-03-11 00:00:00` | `2024-03-11` |
| oocito_id=188023, presc_id=460012 | `micro_data_dl` | `2024-03-11 00:00:00` | `2024-03-11` |
| oocito_id=188191, presc_id=424471 | `micro_data_dl` | `2024-03-13 00:00:00` | `2024-03-13` |
| oocito_id=188192, presc_id=424471 | `micro_data_dl` | `2024-03-13 00:00:00` | `2024-03-13` |
| oocito_id=188434, presc_id=430052 | `micro_data_dl` | `2024-03-14 00:00:00` | `2024-03-14` |
| oocito_id=188436, presc_id=414726 | `micro_data_dl` | `2024-03-14 00:00:00` | `2024-03-14` |
| oocito_id=188437, presc_id=414726 | `micro_data_dl` | `2024-03-14 00:00:00` | `2024-03-14` |
| oocito_id=187968, presc_id=459996 | `micro_data_procedimento` | `2024-03-11 00:00:00` | `2024-03-11` |
| oocito_id=188017, presc_id=426825 | `micro_data_procedimento` | `2024-03-11 00:00:00` | `2024-03-11` |
| oocito_id=188019, presc_id=426825 | `micro_data_procedimento` | `2024-03-11 00:00:00` | `2024-03-11` |
| oocito_id=188021, presc_id=426825 | `micro_data_procedimento` | `2024-03-11 00:00:00` | `2024-03-11` |
| oocito_id=188023, presc_id=460012 | `micro_data_procedimento` | `2024-03-11 00:00:00` | `2024-03-11` |
| oocito_id=188191, presc_id=424471 | `micro_data_procedimento` | `2024-03-13 00:00:00` | `2024-03-13` |
| oocito_id=188192, presc_id=424471 | `micro_data_procedimento` | `2024-03-13 00:00:00` | `2024-03-13` |
| oocito_id=188434, presc_id=430052 | `micro_data_procedimento` | `2024-03-14 00:00:00` | `2024-03-14` |
| oocito_id=188436, presc_id=414726 | `micro_data_procedimento` | `2024-03-14 00:00:00` | `2024-03-14` |
| oocito_id=188437, presc_id=414726 | `micro_data_procedimento` | `2024-03-14 00:00:00` | `2024-03-14` |
| oocito_id=187968, presc_id=459996 | `n_of_biopsied` | `nan` | `3.0` |
| oocito_id=187964, presc_id=459996 | `n_of_biopsied` | `nan` | `3.0` |
| oocito_id=187965, presc_id=459996 | `n_of_biopsied` | `nan` | `3.0` |
| oocito_id=187966, presc_id=459996 | `n_of_biopsied` | `nan` | `3.0` |
| oocito_id=187967, presc_id=459996 | `n_of_biopsied` | `nan` | `3.0` |
| oocito_id=187968, presc_id=459992 | `n_of_biopsied` | `nan` | `3.0` |
| oocito_id=187964, presc_id=459992 | `n_of_biopsied` | `nan` | `3.0` |
| oocito_id=187965, presc_id=459992 | `n_of_biopsied` | `nan` | `3.0` |
| oocito_id=187966, presc_id=459992 | `n_of_biopsied` | `nan` | `3.0` |
| oocito_id=187967, presc_id=459992 | `n_of_biopsied` | `nan` | `3.0` |
| oocito_id=187968, presc_id=459996 | `n_of_normal` | `nan` | `1.0` |
| oocito_id=187964, presc_id=459996 | `n_of_normal` | `nan` | `1.0` |
| oocito_id=187965, presc_id=459996 | `n_of_normal` | `nan` | `1.0` |
| oocito_id=187966, presc_id=459996 | `n_of_normal` | `nan` | `1.0` |
| oocito_id=187967, presc_id=459996 | `n_of_normal` | `nan` | `1.0` |
| oocito_id=187968, presc_id=459992 | `n_of_normal` | `nan` | `1.0` |
| oocito_id=187964, presc_id=459992 | `n_of_normal` | `nan` | `1.0` |
| oocito_id=187965, presc_id=459992 | `n_of_normal` | `nan` | `1.0` |
| oocito_id=187966, presc_id=459992 | `n_of_normal` | `nan` | `1.0` |
| oocito_id=187967, presc_id=459992 | `n_of_normal` | `nan` | `1.0` |
| oocito_id=187968, presc_id=459996 | `number_of_embryos_transferred` | `nan` | `1.0` |
| oocito_id=187964, presc_id=459996 | `number_of_embryos_transferred` | `nan` | `1.0` |
| oocito_id=187965, presc_id=459996 | `number_of_embryos_transferred` | `nan` | `1.0` |
| oocito_id=187966, presc_id=459996 | `number_of_embryos_transferred` | `nan` | `1.0` |
| oocito_id=187967, presc_id=459996 | `number_of_embryos_transferred` | `nan` | `1.0` |
| oocito_id=187968, presc_id=459992 | `number_of_embryos_transferred` | `nan` | `1.0` |
| oocito_id=187964, presc_id=459992 | `number_of_embryos_transferred` | `nan` | `1.0` |
| oocito_id=187965, presc_id=459992 | `number_of_embryos_transferred` | `nan` | `1.0` |
| oocito_id=187966, presc_id=459992 | `number_of_embryos_transferred` | `nan` | `1.0` |
| oocito_id=187967, presc_id=459992 | `number_of_embryos_transferred` | `nan` | `1.0` |
| oocito_id=315271, presc_id=1035121 | `oocito_resultado_pgd` | `nan` | `Aneuploide` |
| oocito_id=317239, presc_id=1043839 | `oocito_resultado_pgd` | `nan` | `Euploide` |
| oocito_id=317240, presc_id=1043839 | `oocito_resultado_pgd` | `nan` | `Aneuploide` |
| oocito_id=315271, presc_id=1035119 | `oocito_resultado_pgd` | `nan` | `Aneuploide` |
| oocito_id=317239, presc_id=1043837 | `oocito_resultado_pgd` | `nan` | `Euploide` |
| oocito_id=317240, presc_id=1043837 | `oocito_resultado_pgd` | `nan` | `Aneuploide` |
| oocito_id=316845, presc_id=1062274 | `oocito_resultado_pgd` | `Não analisado` | `Aneuploide complexo` |
| oocito_id=316846, presc_id=1062274 | `oocito_resultado_pgd` | `Não analisado` | `Euploide` |
| oocito_id=317239, presc_id=1043838 | `oocito_resultado_pgd` | `nan` | `Euploide` |
| oocito_id=317240, presc_id=1043838 | `oocito_resultado_pgd` | `nan` | `Aneuploide` |
| oocito_id=315271, presc_id=1035121 | `oocito_resultado_pgd_detalhes` | `nan` | `Múltiplas aneuplodias detectadas` |
| oocito_id=317239, presc_id=1043839 | `oocito_resultado_pgd_detalhes` | `nan` | `Euploide 46, XY ` |
| oocito_id=317240, presc_id=1043839 | `oocito_resultado_pgd_detalhes` | `nan` | `Múltiplas aneuploidias detectadas` |
| oocito_id=315271, presc_id=1035119 | `oocito_resultado_pgd_detalhes` | `nan` | `Múltiplas aneuplodias detectadas` |
| oocito_id=317239, presc_id=1043837 | `oocito_resultado_pgd_detalhes` | `nan` | `Euploide 46, XY ` |
| oocito_id=317240, presc_id=1043837 | `oocito_resultado_pgd_detalhes` | `nan` | `Múltiplas aneuploidias detectadas` |
| oocito_id=316845, presc_id=1062274 | `oocito_resultado_pgd_detalhes` | `nan` | `Múltiplas aneuploidias detectadas` |
| oocito_id=316846, presc_id=1062274 | `oocito_resultado_pgd_detalhes` | `nan` | `Euploide` |
| oocito_id=317239, presc_id=1043838 | `oocito_resultado_pgd_detalhes` | `nan` | `Euploide 46, XY ` |
| oocito_id=317240, presc_id=1043838 | `oocito_resultado_pgd_detalhes` | `nan` | `Múltiplas aneuploidias detectadas` |
| oocito_id=187968, presc_id=459996 | `outcome_type` | `nan` | `No pregnancy` |
| oocito_id=187964, presc_id=459996 | `outcome_type` | `nan` | `No pregnancy` |
| oocito_id=187965, presc_id=459996 | `outcome_type` | `nan` | `No pregnancy` |
| oocito_id=187966, presc_id=459996 | `outcome_type` | `nan` | `No pregnancy` |
| oocito_id=187967, presc_id=459996 | `outcome_type` | `nan` | `No pregnancy` |
| oocito_id=187968, presc_id=459992 | `outcome_type` | `nan` | `No pregnancy` |
| oocito_id=187964, presc_id=459992 | `outcome_type` | `nan` | `No pregnancy` |
| oocito_id=187965, presc_id=459992 | `outcome_type` | `nan` | `No pregnancy` |
| oocito_id=187966, presc_id=459992 | `outcome_type` | `nan` | `No pregnancy` |
| oocito_id=187967, presc_id=459992 | `outcome_type` | `nan` | `No pregnancy` |
| oocito_id=187968, presc_id=459996 | `presc_data_final` | `2024-03-09 00:00:00` | `2024-03-09` |
| oocito_id=188017, presc_id=426825 | `presc_data_final` | `2024-03-09 00:00:00` | `2024-03-09` |
| oocito_id=188019, presc_id=426825 | `presc_data_final` | `2024-03-09 00:00:00` | `2024-03-09` |
| oocito_id=188021, presc_id=426825 | `presc_data_final` | `2024-03-09 00:00:00` | `2024-03-09` |
| oocito_id=188023, presc_id=460012 | `presc_data_final` | `2024-03-05 00:00:00` | `2024-03-05` |
| oocito_id=188191, presc_id=424471 | `presc_data_final` | `2024-03-10 00:00:00` | `2024-03-10` |
| oocito_id=188192, presc_id=424471 | `presc_data_final` | `2024-03-10 00:00:00` | `2024-03-10` |
| oocito_id=188434, presc_id=430052 | `presc_data_final` | `2024-03-08 00:00:00` | `2024-03-08` |
| oocito_id=188436, presc_id=414726 | `presc_data_final` | `2024-03-01 00:00:00` | `2024-03-01` |
| oocito_id=188437, presc_id=414726 | `presc_data_final` | `2024-03-01 00:00:00` | `2024-03-01` |
| oocito_id=187968, presc_id=459996 | `presc_data_inicial` | `2024-03-09 00:00:00` | `2024-03-09` |
| oocito_id=188017, presc_id=426825 | `presc_data_inicial` | `2024-02-24 00:00:00` | `2024-02-24` |
| oocito_id=188019, presc_id=426825 | `presc_data_inicial` | `2024-02-24 00:00:00` | `2024-02-24` |
| oocito_id=188021, presc_id=426825 | `presc_data_inicial` | `2024-02-24 00:00:00` | `2024-02-24` |
| oocito_id=188023, presc_id=460012 | `presc_data_inicial` | `2024-03-04 00:00:00` | `2024-03-04` |
| oocito_id=188191, presc_id=424471 | `presc_data_inicial` | `2024-03-06 00:00:00` | `2024-03-06` |
| oocito_id=188192, presc_id=424471 | `presc_data_inicial` | `2024-03-06 00:00:00` | `2024-03-06` |
| oocito_id=188434, presc_id=430052 | `presc_data_inicial` | `2024-02-28 00:00:00` | `2024-02-28` |
| oocito_id=188436, presc_id=414726 | `presc_data_inicial` | `2024-02-29 00:00:00` | `2024-02-29` |
| oocito_id=188437, presc_id=414726 | `presc_data_inicial` | `2024-02-29 00:00:00` | `2024-02-29` |
| oocito_id=122276, presc_id=161732 | `presc_dose` | `12.0` | `1.0` |
| oocito_id=122278, presc_id=161732 | `presc_dose` | `12.0` | `1.0` |
| oocito_id=122280, presc_id=161732 | `presc_dose` | `12.0` | `1.0` |
| oocito_id=122281, presc_id=161732 | `presc_dose` | `12.0` | `1.0` |
| oocito_id=122274, presc_id=161732 | `presc_dose` | `12.0` | `1.0` |
| oocito_id=122275, presc_id=161732 | `presc_dose` | `12.0` | `1.0` |
| oocito_id=122277, presc_id=161732 | `presc_dose` | `12.0` | `1.0` |
| oocito_id=122279, presc_id=161732 | `presc_dose` | `12.0` | `1.0` |
| oocito_id=176666, presc_id=414229 | `presc_dose` | `12.0` | `1.0` |
| oocito_id=176667, presc_id=414229 | `presc_dose` | `12.0` | `1.0` |
| oocito_id=122276, presc_id=161732 | `presc_dose_total` | `12.0` | `1.0` |
| oocito_id=122278, presc_id=161732 | `presc_dose_total` | `12.0` | `1.0` |
| oocito_id=122280, presc_id=161732 | `presc_dose_total` | `12.0` | `1.0` |
| oocito_id=122281, presc_id=161732 | `presc_dose_total` | `12.0` | `1.0` |
| oocito_id=122274, presc_id=161732 | `presc_dose_total` | `12.0` | `1.0` |
| oocito_id=122275, presc_id=161732 | `presc_dose_total` | `12.0` | `1.0` |
| oocito_id=122277, presc_id=161732 | `presc_dose_total` | `12.0` | `1.0` |
| oocito_id=122279, presc_id=161732 | `presc_dose_total` | `12.0` | `1.0` |
| oocito_id=176666, presc_id=414229 | `presc_dose_total` | `12.0` | `1.0` |
| oocito_id=176667, presc_id=414229 | `presc_dose_total` | `12.0` | `1.0` |
| oocito_id=284222, presc_id=1028946 | `presc_numero_dias` | `<NA>` | `1.0` |
| oocito_id=284223, presc_id=1028946 | `presc_numero_dias` | `<NA>` | `1.0` |
| oocito_id=284224, presc_id=1028946 | `presc_numero_dias` | `<NA>` | `1.0` |
| oocito_id=284225, presc_id=1028946 | `presc_numero_dias` | `<NA>` | `1.0` |
| oocito_id=284226, presc_id=1028946 | `presc_numero_dias` | `<NA>` | `1.0` |
| oocito_id=284227, presc_id=1028946 | `presc_numero_dias` | `<NA>` | `1.0` |
| oocito_id=284228, presc_id=1028946 | `presc_numero_dias` | `<NA>` | `1.0` |
| oocito_id=284229, presc_id=1028946 | `presc_numero_dias` | `<NA>` | `1.0` |
| oocito_id=284230, presc_id=1028946 | `presc_numero_dias` | `<NA>` | `1.0` |
| oocito_id=284231, presc_id=1028946 | `presc_numero_dias` | `<NA>` | `1.0` |
| oocito_id=224807, presc_id=571120 | `presc_unidade_padronizada` | `NÃO DEFINIDO` | `mg` |
| oocito_id=224808, presc_id=571120 | `presc_unidade_padronizada` | `NÃO DEFINIDO` | `mg` |
| oocito_id=224809, presc_id=571120 | `presc_unidade_padronizada` | `NÃO DEFINIDO` | `mg` |
| oocito_id=224810, presc_id=571120 | `presc_unidade_padronizada` | `NÃO DEFINIDO` | `mg` |
| oocito_id=91519, presc_id=356306 | `presc_unidade_padronizada` | `NÃO DEFINIDO` | `mg` |
| oocito_id=91520, presc_id=356306 | `presc_unidade_padronizada` | `NÃO DEFINIDO` | `mg` |
| oocito_id=91521, presc_id=356306 | `presc_unidade_padronizada` | `NÃO DEFINIDO` | `mg` |
| oocito_id=91522, presc_id=356306 | `presc_unidade_padronizada` | `NÃO DEFINIDO` | `mg` |
| oocito_id=91523, presc_id=356306 | `presc_unidade_padronizada` | `NÃO DEFINIDO` | `mg` |
| oocito_id=91524, presc_id=356306 | `presc_unidade_padronizada` | `NÃO DEFINIDO` | `mg` |
| oocito_id=187968, presc_id=459996 | `redlara_outcome` | `nan` | `EMBRYO TRANSFER` |
| oocito_id=187964, presc_id=459996 | `redlara_outcome` | `nan` | `EMBRYO TRANSFER` |
| oocito_id=187965, presc_id=459996 | `redlara_outcome` | `nan` | `EMBRYO TRANSFER` |
| oocito_id=187966, presc_id=459996 | `redlara_outcome` | `nan` | `EMBRYO TRANSFER` |
| oocito_id=187967, presc_id=459996 | `redlara_outcome` | `nan` | `EMBRYO TRANSFER` |
| oocito_id=187968, presc_id=459992 | `redlara_outcome` | `nan` | `EMBRYO TRANSFER` |
| oocito_id=187964, presc_id=459992 | `redlara_outcome` | `nan` | `EMBRYO TRANSFER` |
| oocito_id=187965, presc_id=459992 | `redlara_outcome` | `nan` | `EMBRYO TRANSFER` |
| oocito_id=187966, presc_id=459992 | `redlara_outcome` | `nan` | `EMBRYO TRANSFER` |
| oocito_id=187967, presc_id=459992 | `redlara_outcome` | `nan` | `EMBRYO TRANSFER` |
| oocito_id=192065, presc_id=432406 | `trat1_bmi` | `1933.59` | `19.14` |
| oocito_id=192067, presc_id=432406 | `trat1_bmi` | `1933.59` | `19.14` |
| oocito_id=192066, presc_id=432406 | `trat1_bmi` | `1933.59` | `19.14` |
| oocito_id=192068, presc_id=432406 | `trat1_bmi` | `1933.59` | `19.14` |
| oocito_id=192065, presc_id=432404 | `trat1_bmi` | `1933.59` | `19.14` |
| oocito_id=192067, presc_id=432404 | `trat1_bmi` | `1933.59` | `19.14` |
| oocito_id=192066, presc_id=432404 | `trat1_bmi` | `1933.59` | `19.14` |
| oocito_id=192068, presc_id=432404 | `trat1_bmi` | `1933.59` | `19.14` |
| oocito_id=192065, presc_id=432409 | `trat1_bmi` | `1933.59` | `19.14` |
| oocito_id=192067, presc_id=432409 | `trat1_bmi` | `1933.59` | `19.14` |
| oocito_id=187968, presc_id=459996 | `trat1_data_inicio_inducao` | `2024-03-01 00:00:00` | `2024-03-01` |
| oocito_id=188017, presc_id=426825 | `trat1_data_inicio_inducao` | `2024-02-24 00:00:00` | `2024-02-24` |
| oocito_id=188019, presc_id=426825 | `trat1_data_inicio_inducao` | `2024-02-24 00:00:00` | `2024-02-24` |
| oocito_id=188021, presc_id=426825 | `trat1_data_inicio_inducao` | `2024-02-24 00:00:00` | `2024-02-24` |
| oocito_id=188023, presc_id=460012 | `trat1_data_inicio_inducao` | `2024-02-27 00:00:00` | `2024-02-27` |
| oocito_id=188191, presc_id=424471 | `trat1_data_inicio_inducao` | `2024-02-29 00:00:00` | `2024-02-29` |
| oocito_id=188192, presc_id=424471 | `trat1_data_inicio_inducao` | `2024-02-29 00:00:00` | `2024-02-29` |
| oocito_id=188434, presc_id=430052 | `trat1_data_inicio_inducao` | `2024-02-28 00:00:00` | `2024-02-28` |
| oocito_id=188436, presc_id=414726 | `trat1_data_inicio_inducao` | `2024-02-29 00:00:00` | `2024-02-29` |
| oocito_id=188437, presc_id=414726 | `trat1_data_inicio_inducao` | `2024-02-29 00:00:00` | `2024-02-29` |
| oocito_id=189541, presc_id=442267 | `trat1_data_transferencia` | `2024-03-27 00:00:00` | `2024-03-27` |
| oocito_id=189542, presc_id=442267 | `trat1_data_transferencia` | `2024-03-27 00:00:00` | `2024-03-27` |
| oocito_id=189543, presc_id=442267 | `trat1_data_transferencia` | `2024-03-27 00:00:00` | `2024-03-27` |
| oocito_id=189544, presc_id=442267 | `trat1_data_transferencia` | `2024-03-27 00:00:00` | `2024-03-27` |
| oocito_id=189545, presc_id=442267 | `trat1_data_transferencia` | `2024-03-27 00:00:00` | `2024-03-27` |
| oocito_id=189550, presc_id=442267 | `trat1_data_transferencia` | `2024-03-27 00:00:00` | `2024-03-27` |
| oocito_id=189239, presc_id=609237 | `trat1_data_transferencia` | `2024-03-25 00:00:00` | `2024-03-25` |
| oocito_id=189240, presc_id=609237 | `trat1_data_transferencia` | `2024-03-25 00:00:00` | `2024-03-25` |
| oocito_id=189241, presc_id=609237 | `trat1_data_transferencia` | `2024-03-25 00:00:00` | `2024-03-25` |
| oocito_id=189242, presc_id=609237 | `trat1_data_transferencia` | `2024-03-25 00:00:00` | `2024-03-25` |
| oocito_id=193802, presc_id=439235 | `trat2_bmi` | `22.31` | `nan` |
| oocito_id=192065, presc_id=432406 | `trat2_bmi` | `1933.59` | `19.14` |
| oocito_id=192067, presc_id=432406 | `trat2_bmi` | `1933.59` | `19.14` |
| oocito_id=193802, presc_id=439237 | `trat2_bmi` | `22.31` | `nan` |
| oocito_id=192065, presc_id=432404 | `trat2_bmi` | `1933.59` | `19.14` |
| oocito_id=192067, presc_id=432404 | `trat2_bmi` | `1933.59` | `19.14` |
| oocito_id=193802, presc_id=439234 | `trat2_bmi` | `22.31` | `nan` |
| oocito_id=192065, presc_id=432409 | `trat2_bmi` | `1933.59` | `19.14` |
| oocito_id=192067, presc_id=432409 | `trat2_bmi` | `1933.59` | `19.14` |
| oocito_id=193802, presc_id=439236 | `trat2_bmi` | `22.31` | `nan` |
| oocito_id=188017, presc_id=426825 | `trat2_data_inicio_inducao` | `2024-03-18 00:00:00` | `2024-03-18` |
| oocito_id=188019, presc_id=426825 | `trat2_data_inicio_inducao` | `2025-10-13 00:00:00` | `2025-10-13` |
| oocito_id=188021, presc_id=426825 | `trat2_data_inicio_inducao` | `2024-03-18 00:00:00` | `2024-03-18` |
| oocito_id=188191, presc_id=424471 | `trat2_data_inicio_inducao` | `2024-07-22 00:00:00` | `2024-07-22` |
| oocito_id=188192, presc_id=424471 | `trat2_data_inicio_inducao` | `2024-03-26 00:00:00` | `2024-03-26` |
| oocito_id=188895, presc_id=419196 | `trat2_data_inicio_inducao` | `2026-02-11 00:00:00` | `2026-02-11` |
| oocito_id=188903, presc_id=419196 | `trat2_data_inicio_inducao` | `2024-03-22 00:00:00` | `2024-03-22` |
| oocito_id=188904, presc_id=419196 | `trat2_data_inicio_inducao` | `2024-03-22 00:00:00` | `2024-03-22` |
| oocito_id=188953, presc_id=460068 | `trat2_data_inicio_inducao` | `2024-05-08 00:00:00` | `2024-05-08` |
| oocito_id=189541, presc_id=442267 | `trat2_data_inicio_inducao` | `2024-06-05 00:00:00` | `2024-06-05` |
| oocito_id=188017, presc_id=426825 | `trat2_data_transferencia` | `2024-04-03 00:00:00` | `2024-04-03` |
| oocito_id=188019, presc_id=426825 | `trat2_data_transferencia` | `2025-10-30 00:00:00` | `2025-10-30` |
| oocito_id=188021, presc_id=426825 | `trat2_data_transferencia` | `2024-04-03 00:00:00` | `2024-04-03` |
| oocito_id=188895, presc_id=419196 | `trat2_data_transferencia` | `2026-03-02 00:00:00` | `2026-03-02` |
| oocito_id=188903, presc_id=419196 | `trat2_data_transferencia` | `2024-04-11 00:00:00` | `2024-04-11` |
| oocito_id=188904, presc_id=419196 | `trat2_data_transferencia` | `2024-04-11 00:00:00` | `2024-04-11` |
| oocito_id=188953, presc_id=460068 | `trat2_data_transferencia` | `2024-05-23 00:00:00` | `2024-05-23` |
| oocito_id=189542, presc_id=442267 | `trat2_data_transferencia` | `2024-04-30 00:00:00` | `2024-04-30` |
| oocito_id=189550, presc_id=442267 | `trat2_data_transferencia` | `2024-04-30 00:00:00` | `2024-04-30` |
| oocito_id=187834, presc_id=559979 | `trat2_data_transferencia` | `2024-10-28 00:00:00` | `2024-10-28` |
| oocito_id=193802, presc_id=439235 | `trat2_id` | `27408` | `nan` |
| oocito_id=193802, presc_id=439237 | `trat2_id` | `27408` | `nan` |
| oocito_id=193802, presc_id=439234 | `trat2_id` | `27408` | `nan` |
| oocito_id=193802, presc_id=439236 | `trat2_id` | `27408` | `nan` |
| oocito_id=207999, presc_id=495358 | `trat2_id` | `29299` | `nan` |
| oocito_id=207999, presc_id=495361 | `trat2_id` | `29299` | `nan` |
| oocito_id=207999, presc_id=495357 | `trat2_id` | `29299` | `nan` |
| oocito_id=207999, presc_id=495362 | `trat2_id` | `29299` | `nan` |
| oocito_id=207999, presc_id=495359 | `trat2_id` | `29299` | `nan` |
| oocito_id=207999, presc_id=495363 | `trat2_id` | `29299` | `nan` |
| oocito_id=236838, presc_id=618903 | `trat2_motivo_nao_transferir` | `Outras complicações` | `` |
| oocito_id=236840, presc_id=618903 | `trat2_motivo_nao_transferir` | `nan` | `Outras complicações` |
| oocito_id=236842, presc_id=618903 | `trat2_motivo_nao_transferir` | `nan` | `Outras complicações` |
| oocito_id=236845, presc_id=618903 | `trat2_motivo_nao_transferir` | `nan` | `Outras complicações` |
| oocito_id=236838, presc_id=618900 | `trat2_motivo_nao_transferir` | `Outras complicações` | `` |
| oocito_id=236840, presc_id=618900 | `trat2_motivo_nao_transferir` | `nan` | `Outras complicações` |
| oocito_id=236842, presc_id=618900 | `trat2_motivo_nao_transferir` | `nan` | `Outras complicações` |
| oocito_id=236845, presc_id=618900 | `trat2_motivo_nao_transferir` | `nan` | `Outras complicações` |
| oocito_id=236838, presc_id=618902 | `trat2_motivo_nao_transferir` | `Outras complicações` | `` |
| oocito_id=236840, presc_id=618902 | `trat2_motivo_nao_transferir` | `nan` | `Outras complicações` |
| oocito_id=193802, presc_id=439235 | `trat2_previous_et` | `0` | `nan` |
| oocito_id=193802, presc_id=439237 | `trat2_previous_et` | `0` | `nan` |
| oocito_id=193802, presc_id=439234 | `trat2_previous_et` | `0` | `nan` |
| oocito_id=193802, presc_id=439236 | `trat2_previous_et` | `0` | `nan` |
| oocito_id=207999, presc_id=495358 | `trat2_previous_et` | `0` | `nan` |
| oocito_id=207999, presc_id=495361 | `trat2_previous_et` | `0` | `nan` |
| oocito_id=207999, presc_id=495357 | `trat2_previous_et` | `0` | `nan` |
| oocito_id=207999, presc_id=495362 | `trat2_previous_et` | `0` | `nan` |
| oocito_id=207999, presc_id=495359 | `trat2_previous_et` | `0` | `nan` |
| oocito_id=207999, presc_id=495363 | `trat2_previous_et` | `0` | `nan` |
| oocito_id=193802, presc_id=439235 | `trat2_previous_et_od` | `0` | `nan` |
| oocito_id=193802, presc_id=439237 | `trat2_previous_et_od` | `0` | `nan` |
| oocito_id=193802, presc_id=439234 | `trat2_previous_et_od` | `0` | `nan` |
| oocito_id=193802, presc_id=439236 | `trat2_previous_et_od` | `0` | `nan` |
| oocito_id=207999, presc_id=495358 | `trat2_previous_et_od` | `0` | `nan` |
| oocito_id=207999, presc_id=495361 | `trat2_previous_et_od` | `0` | `nan` |
| oocito_id=207999, presc_id=495357 | `trat2_previous_et_od` | `0` | `nan` |
| oocito_id=207999, presc_id=495362 | `trat2_previous_et_od` | `0` | `nan` |
| oocito_id=207999, presc_id=495359 | `trat2_previous_et_od` | `0` | `nan` |
| oocito_id=207999, presc_id=495363 | `trat2_previous_et_od` | `0` | `nan` |
| oocito_id=187814, presc_id=493420 | `trat2_resultado_tratamento` | `nan` | `Negativo` |
| oocito_id=187814, presc_id=493419 | `trat2_resultado_tratamento` | `nan` | `Negativo` |
| oocito_id=187814, presc_id=493417 | `trat2_resultado_tratamento` | `nan` | `Negativo` |
| oocito_id=187814, presc_id=493418 | `trat2_resultado_tratamento` | `nan` | `Negativo` |
| oocito_id=193802, presc_id=439235 | `trat2_resultado_tratamento` | `Negativo` | `nan` |
| oocito_id=193802, presc_id=439237 | `trat2_resultado_tratamento` | `Negativo` | `nan` |
| oocito_id=193802, presc_id=439234 | `trat2_resultado_tratamento` | `Negativo` | `nan` |
| oocito_id=193802, presc_id=439236 | `trat2_resultado_tratamento` | `Negativo` | `nan` |
| oocito_id=197030, presc_id=451429 | `trat2_resultado_tratamento` | `nan` | `Negativo` |
| oocito_id=197032, presc_id=451429 | `trat2_resultado_tratamento` | `nan` | `Negativo` |
| oocito_id=193802, presc_id=439235 | `trat2_tentativa` | `1` | `nan` |
| oocito_id=193802, presc_id=439237 | `trat2_tentativa` | `1` | `nan` |
| oocito_id=193802, presc_id=439234 | `trat2_tentativa` | `1` | `nan` |
| oocito_id=193802, presc_id=439236 | `trat2_tentativa` | `1` | `nan` |
| oocito_id=207999, presc_id=495358 | `trat2_tentativa` | `2` | `nan` |
| oocito_id=207999, presc_id=495361 | `trat2_tentativa` | `2` | `nan` |
| oocito_id=207999, presc_id=495357 | `trat2_tentativa` | `2` | `nan` |
| oocito_id=207999, presc_id=495362 | `trat2_tentativa` | `2` | `nan` |
| oocito_id=207999, presc_id=495359 | `trat2_tentativa` | `2` | `nan` |
| oocito_id=207999, presc_id=495363 | `trat2_tentativa` | `2` | `nan` |
| oocito_id=193802, presc_id=439235 | `trat2_tipo_procedimento` | `Ciclo de Congelados` | `nan` |
| oocito_id=193802, presc_id=439237 | `trat2_tipo_procedimento` | `Ciclo de Congelados` | `nan` |
| oocito_id=193802, presc_id=439234 | `trat2_tipo_procedimento` | `Ciclo de Congelados` | `nan` |
| oocito_id=193802, presc_id=439236 | `trat2_tipo_procedimento` | `Ciclo de Congelados` | `nan` |
| oocito_id=207999, presc_id=495358 | `trat2_tipo_procedimento` | `Ciclo de Congelados` | `nan` |
| oocito_id=207999, presc_id=495361 | `trat2_tipo_procedimento` | `Ciclo de Congelados` | `nan` |
| oocito_id=207999, presc_id=495357 | `trat2_tipo_procedimento` | `Ciclo de Congelados` | `nan` |
| oocito_id=207999, presc_id=495362 | `trat2_tipo_procedimento` | `Ciclo de Congelados` | `nan` |
| oocito_id=207999, presc_id=495359 | `trat2_tipo_procedimento` | `Ciclo de Congelados` | `nan` |
| oocito_id=207999, presc_id=495363 | `trat2_tipo_procedimento` | `Ciclo de Congelados` | `nan` |
| oocito_id=196658, presc_id=450586 | `type_of_delivery` | `nan` | `Cesarean section` |
| oocito_id=196658, presc_id=450582 | `type_of_delivery` | `nan` | `Cesarean section` |
| oocito_id=196658, presc_id=450585 | `type_of_delivery` | `nan` | `Cesarean section` |
| oocito_id=196658, presc_id=450584 | `type_of_delivery` | `nan` | `Cesarean section` |
| oocito_id=196658, presc_id=450583 | `type_of_delivery` | `nan` | `Cesarean section` |
| oocito_id=203626, presc_id=479181 | `type_of_delivery` | `nan` | `Cesarean section` |
| oocito_id=203627, presc_id=479181 | `type_of_delivery` | `nan` | `Cesarean section` |
| oocito_id=203626, presc_id=479184 | `type_of_delivery` | `nan` | `Cesarean section` |
| oocito_id=203627, presc_id=479184 | `type_of_delivery` | `nan` | `Cesarean section` |
| oocito_id=203626, presc_id=479183 | `type_of_delivery` | `nan` | `Cesarean section` |

### `embryos_with_prescription_wide`
#### **Schema Mismatches**
* Columns in DuckDB Only: `CICLOPRIMOGYNA_days`, `CICLOPRIMOGYNA_dose`, `CICLOPRIMOGYNA_end`, `CICLOPRIMOGYNA_interval`, `CICLOPRIMOGYNA_start`, `CICLOPRIMOGYNA_unit`, `CÁLCIO_days`, `CÁLCIO_dose`, `CÁLCIO_end`, `CÁLCIO_interval`, `CÁLCIO_start`, `CÁLCIO_unit`, `DOXICICLINA_days`, `DOXICICLINA_dose`, `DOXICICLINA_end`, `DOXICICLINA_interval`, `DOXICICLINA_start`, `DOXICICLINA_unit`, `ESTRADOT_days`, `ESTRADOT_dose`, `ESTRADOT_end`, `ESTRADOT_interval`, `ESTRADOT_start`, `ESTRADOT_unit`, `EVOCANIL_days`, `EVOCANIL_dose`, `EVOCANIL_end`, `EVOCANIL_interval`, `EVOCANIL_start`, `EVOCANIL_unit`, `LENZETTO_days`, `LENZETTO_dose`, `LENZETTO_end`, `LENZETTO_interval`, `LENZETTO_start`, `LENZETTO_unit`, `PRIMOLUT_days`, `PRIMOLUT_dose`, `PRIMOLUT_end`, `PRIMOLUT_interval`, `PRIMOLUT_start`, `PRIMOLUT_unit`, `PROFENID_days`, `PROFENID_dose`, `PROFENID_end`, `PROFENID_interval`, `PROFENID_start`, `PROFENID_unit`, `TESTOSTERONA_days`, `TESTOSTERONA_dose`, `TESTOSTERONA_end`, `TESTOSTERONA_interval`, `TESTOSTERONA_start`, `TESTOSTERONA_unit`, `TYLEX_days`, `TYLEX_dose`, `TYLEX_end`, `TYLEX_interval`, `TYLEX_start`, `TYLEX_unit`, `VERSA_days`, `VERSA_dose`, `VERSA_end`, `VERSA_interval`, `VERSA_start`, `VERSA_unit`, `VITAMINA_days`, `VITAMINA_dose`, `VITAMINA_end`, `VITAMINA_interval`, `VITAMINA_start`, `VITAMINA_unit`, `baby_1_weight`, `baby_2_weight`, `baby_3_weight`, `chart_or_pin`, `complications_of_pregnancy_specify`, `date_of_birth`, `embryo_Name_BlastExpandLast`, `embryo_Name_BlastomereSize`, `embryo_Name_EVEN2`, `embryo_Name_EVEN4`, `embryo_Name_EVEN8`, `embryo_Name_FRAG2`, `embryo_Name_FRAG2CAT`, `embryo_Name_FRAG4`, `embryo_Name_FRAG8`, `embryo_Name_Fragmentation`, `embryo_Name_ICM`, `embryo_Name_Line`, `embryo_Name_MN2Type`, `embryo_Name_MorphologicalGrade`, `embryo_Name_MultiNucleation`, `embryo_Name_Nuclei2`, `embryo_Name_PN`, `embryo_Name_Pulsing`, `embryo_Name_ReexpansionCount`, `embryo_Name_Strings`, `embryo_Name_TE`, `embryo_Name_t2`, `embryo_Name_t3`, `embryo_Name_t4`, `embryo_Name_t5`, `embryo_Name_t6`, `embryo_Name_t7`, `embryo_Name_t8`, `embryo_Name_t9`, `embryo_Name_tB`, `embryo_Name_tEB`, `embryo_Name_tHB`, `embryo_Name_tM`, `embryo_Name_tPB2`, `embryo_Name_tPNa`, `embryo_Name_tPNf`, `embryo_Name_tSB`, `embryo_Name_tSC`, `embryo_Time_BlastExpandLast`, `embryo_Time_BlastomereSize`, `embryo_Time_EVEN2`, `embryo_Time_EVEN4`, `embryo_Time_EVEN8`, `embryo_Time_FRAG2`, `embryo_Time_FRAG2CAT`, `embryo_Time_FRAG4`, `embryo_Time_FRAG8`, `embryo_Time_ICM`, `embryo_Time_Line`, `embryo_Time_MN2Type`, `embryo_Time_MorphologicalGrade`, `embryo_Time_MultiNucleation`, `embryo_Time_Nuclei2`, `embryo_Time_PN`, `embryo_Time_Pulsing`, `embryo_Time_ReexpansionCount`, `embryo_Time_Strings`, `embryo_Time_TE`, `embryo_Time_t6`, `embryo_Time_t7`, `embryo_Time_t9`, `embryo_Time_tPB2`, `embryo_Timestamp_BlastExpandLast`, `embryo_Timestamp_BlastomereSize`, `embryo_Timestamp_EVEN2`, `embryo_Timestamp_EVEN4`, `embryo_Timestamp_EVEN8`, `embryo_Timestamp_FRAG2`, `embryo_Timestamp_FRAG2CAT`, `embryo_Timestamp_FRAG4`, `embryo_Timestamp_FRAG8`, `embryo_Timestamp_Fragmentation`, `embryo_Timestamp_ICM`, `embryo_Timestamp_Line`, `embryo_Timestamp_MN2Type`, `embryo_Timestamp_MorphologicalGrade`, `embryo_Timestamp_MultiNucleation`, `embryo_Timestamp_Nuclei2`, `embryo_Timestamp_PN`, `embryo_Timestamp_Pulsing`, `embryo_Timestamp_ReexpansionCount`, `embryo_Timestamp_Strings`, `embryo_Timestamp_TE`, `embryo_Timestamp_t2`, `embryo_Timestamp_t3`, `embryo_Timestamp_t4`, `embryo_Timestamp_t5`, `embryo_Timestamp_t6`, `embryo_Timestamp_t7`, `embryo_Timestamp_t8`, `embryo_Timestamp_t9`, `embryo_Timestamp_tB`, `embryo_Timestamp_tEB`, `embryo_Timestamp_tHB`, `embryo_Timestamp_tM`, `embryo_Timestamp_tPB2`, `embryo_Timestamp_tPNa`, `embryo_Timestamp_tPNf`, `embryo_Timestamp_tSB`, `embryo_Timestamp_tSC`, `embryo_Value_BlastExpandLast`, `embryo_Value_BlastomereSize`, `embryo_Value_EVEN2`, `embryo_Value_EVEN4`, `embryo_Value_EVEN8`, `embryo_Value_FRAG2`, `embryo_Value_FRAG4`, `embryo_Value_FRAG8`, `embryo_Value_Fragmentation`, `embryo_Value_Line`, `embryo_Value_MorphologicalGrade`, `embryo_Value_MultiNucleation`, `embryo_Value_Strings`, `embryo_Value_t2`, `embryo_Value_t3`, `embryo_Value_t4`, `embryo_Value_t5`, `embryo_Value_t6`, `embryo_Value_t7`, `embryo_Value_t8`, `embryo_Value_t9`, `embryo_Value_tB`, `embryo_Value_tEB`, `embryo_Value_tHB`, `embryo_Value_tM`, `embryo_Value_tPB2`, `embryo_Value_tPNa`, `embryo_Value_tPNf`, `embryo_Value_tSB`, `embryo_Value_tSC`, `fet_data_crio`, `fet_dia_cryo`, `fet_dia_et`, `fet_file_name`, `fet_idade_do_cong_de_embriao`, `fet_idade_mulher`, `fet_no_da_transfer_1a_2a_3a`, `fet_no_et`, `fet_preparo_para_transferencia`, `fet_sheet_name`, `fet_tipo_1`, `fet_tipo_biopsia`, `fet_tipo_da_doacao`, `fet_tipo_de_fet`, `fet_tipo_de_tratamento`, `fresh_altura`, `fresh_data_crio`, `fresh_data_de_nasc`, `fresh_dia_cryo`, `fresh_fator_1`, `fresh_file_name`, `fresh_idade_espermatozoide`, `fresh_incubadora`, `fresh_no_biopsiados`, `fresh_opu`, `fresh_origem`, `fresh_peso`, `fresh_qtd_analisados`, `fresh_qtd_blasto`, `fresh_qtd_blasto_tq_a_e_b`, `fresh_qtd_normais`, `fresh_sheet_name`, `fresh_tipo`, `fresh_tipo_1`, `fresh_tipo_biopsia`, `fresh_tipo_de_inseminacao`, `fresh_total_de_mii`, `idascore_IDAScore`, `idascore_IDATime`, `idascore_IDAVersion`, `join_step_1`, `micro_CicloDoadora`, `micro_recepcao_ovulos`, `number_of_fet_after_originally_frozen`, `patient_DateOfBirth`, `patient_FirstName`, `patient_LastName`, `patient_PatientID`, `patient_PatientIDx`, `patient_YearOfBirth`, `patient_name`, `patient_unit_huntington`, `prontuario`, `trat1_idade_esposa`, `trat1_idade_marido`, `trat1_prontuario_doadora`, `trat2_idade_esposa`, `trat2_idade_marido`, `trat2_prontuario_doadora`, `treatment_TreatmentName`, `unidade`, `year`
* Columns in Athena Only: `date_of_embryo_transfer`, `date_when_embryos_were_cryopreserved`, `embryo_first_name`, `embryo_last_name`, `embryo_patient_date_of_birth`, `embryo_patient_id`, `embryo_patient_id_x`, `embryo_patient_sk`, `embryo_patient_year_of_birth`, `embryo_prontuario`, `embryo_treatment_name`, `embryo_unit_huntington`, `number_of_newborns`, `presc_oocito_id`
#### **Discrepancy Group Isolation**
* **By Year/Date**:
  * Year **1981**: 3 discrepant records
  * Year **2012**: 6 discrepant records
  * Year **2021**: 52 discrepant records
  * Year **2022**: 1843 discrepant records
  * Year **2023**: 2991 discrepant records
  * Year **2024**: 3958 discrepant records
  * Year **2025**: 3287 discrepant records
  * Year **2026**: 761 discrepant records
  * Year **N/A**: 47 discrepant records
* **By Dimension Group**:
  * Group **Ana Luiza Mattos Tavares**: 258 discrepant records
  * Group **Ana Luiza Nunes**: 41 discrepant records
  * Group **Ana Paula Aquino**: 31 discrepant records
  * Group **Ana Rita de Paiva Toledo**: 25 discrepant records
  * Group **Andrea de Fatima Castro**: 7 discrepant records
  * Group **Anna Luiza Moraes Souza**: 3 discrepant records
  * Group **Bruna Barros Cavalcante**: 165 discrepant records
  * Group **Bruna Costa Queiroz**: 149 discrepant records
  * Group **Carla Martins**: 371 discrepant records
  * Group **Claudia Gomes Padilla**: 20 discrepant records
  * Group **Eduardo Leme Alves da Motta**: 135 discrepant records
  * Group **Erica Becker de Sousa Xavier**: 1463 discrepant records
  * Group **Fabyanne Mazutti da Silva **: 70 discrepant records
  * Group **Fernanda de Paula Rodrigues**: 18 discrepant records
  * Group **Frederico Jose Silva Correa**: 205 discrepant records
  * Group **Fábio Costa Peixoto**: 543 discrepant records
  * Group **Gabriella de Oliveira Ferreira**: 535 discrepant records
  * Group **Geraldo Caldeira**: 12 discrepant records
  * Group **Gustavo Teles**: 49 discrepant records
  * Group **Helio Haddad Filho**: 9 discrepant records
  * Group **Herica Cristina Mendonça**: 1181 discrepant records
  * Group **Josenice de Araujo SIlva Gomes**: 30 discrepant records
  * Group **João Pedro Junqueira Caetano**: 1933 discrepant records
  * Group **Laura Maria Almeida Maia**: 691 discrepant records
  * Group **Leci Veiga Caetano Amorim**: 1585 discrepant records
  * Group **Leonardo Matheus Ribeiro Pereira**: 20 discrepant records
  * Group **Livia Munhoz**: 760 discrepant records
  * Group **Luana Lopes Toledo**: 240 discrepant records
  * Group **Luciana Campomizzi Calazans**: 779 discrepant records
  * Group **Luciana Ferreira Potiguara Amador Sousa**: 126 discrepant records
  * Group **Marcos Eiji Shiroma**: 3 discrepant records
  * Group **Marcos Eji Shiroma**: 4 discrepant records
  * Group **Maria Juliana Albuquerque**: 261 discrepant records
  * Group **Mariana Santana de Almeida Liporoni Yoshida**: 8 discrepant records
  * Group **Matheus Teixeira Roque**: 12 discrepant records
  * Group **Mauricio Chehin**: 25 discrepant records
  * Group **Médico Externo**: 6 discrepant records
  * Group **N/A**: 44 discrepant records
  * Group **Paula Vieira Nunes Brito**: 58 discrepant records
  * Group **Priscila Morais Galvão Sousa**: 13 discrepant records
  * Group **Pró-FIV**: 46 discrepant records
  * Group **Raimundo Cesar Pinheiro**: 3 discrepant records
  * Group **Raphaela Menin Franco Martins**: 362 discrepant records
  * Group **Ricardo Mello Marinho**: 513 discrepant records
  * Group **Tatianna Quintas Furtado Ribeiro**: 8 discrepant records
  * Group **Thais Sanches Domingues**: 128 discrepant records

#### **Anonymized Column Discrepancy Samples**
| Composite Key | Column Name | DuckDB Value | Athena Value |
| :--- | :--- | :--- | :--- |
| oocito_id=296529 | `aas_days` | `14.0` | `7.0` |
| oocito_id=296528 | `aas_days` | `14.0` | `7.0` |
| oocito_id=296527 | `aas_days` | `14.0` | `7.0` |
| oocito_id=296526 | `aas_days` | `14.0` | `7.0` |
| oocito_id=296525 | `aas_days` | `14.0` | `7.0` |
| oocito_id=283572 | `aas_days` | `208.0` | `33.0` |
| oocito_id=283571 | `aas_days` | `208.0` | `33.0` |
| oocito_id=283570 | `aas_days` | `208.0` | `33.0` |
| oocito_id=283569 | `aas_days` | `208.0` | `33.0` |
| oocito_id=283568 | `aas_days` | `208.0` | `33.0` |
| oocito_id=296529 | `aas_dose` | `1400.0` | `700.0` |
| oocito_id=296528 | `aas_dose` | `1400.0` | `700.0` |
| oocito_id=296527 | `aas_dose` | `1400.0` | `700.0` |
| oocito_id=296526 | `aas_dose` | `1400.0` | `700.0` |
| oocito_id=296525 | `aas_dose` | `1400.0` | `700.0` |
| oocito_id=283572 | `aas_dose` | `20800.0` | `3300.0` |
| oocito_id=283571 | `aas_dose` | `20800.0` | `3300.0` |
| oocito_id=283570 | `aas_dose` | `20800.0` | `3300.0` |
| oocito_id=283569 | `aas_dose` | `20800.0` | `3300.0` |
| oocito_id=283568 | `aas_dose` | `20800.0` | `3300.0` |
| oocito_id=296529 | `aas_end` | `2026-02-02 00:00:00` | `2026-02-02` |
| oocito_id=296528 | `aas_end` | `2026-02-02 00:00:00` | `2026-02-02` |
| oocito_id=296527 | `aas_end` | `2026-02-02 00:00:00` | `2026-02-02` |
| oocito_id=296526 | `aas_end` | `2026-02-02 00:00:00` | `2026-02-02` |
| oocito_id=296525 | `aas_end` | `2026-02-02 00:00:00` | `2026-02-02` |
| oocito_id=283572 | `aas_end` | `2025-11-15 00:00:00` | `2025-11-15` |
| oocito_id=283571 | `aas_end` | `2025-11-15 00:00:00` | `2025-11-15` |
| oocito_id=283570 | `aas_end` | `2025-11-15 00:00:00` | `2025-11-15` |
| oocito_id=283569 | `aas_end` | `2025-11-15 00:00:00` | `2025-11-15` |
| oocito_id=283568 | `aas_end` | `2025-11-15 00:00:00` | `2025-11-15` |
| oocito_id=296529 | `aas_start` | `2026-01-27 00:00:00` | `2026-01-27` |
| oocito_id=296528 | `aas_start` | `2026-01-27 00:00:00` | `2026-01-27` |
| oocito_id=296527 | `aas_start` | `2026-01-27 00:00:00` | `2026-01-27` |
| oocito_id=296526 | `aas_start` | `2026-01-27 00:00:00` | `2026-01-27` |
| oocito_id=296525 | `aas_start` | `2026-01-27 00:00:00` | `2026-01-27` |
| oocito_id=283572 | `aas_start` | `2025-10-14 00:00:00` | `2025-10-14` |
| oocito_id=283571 | `aas_start` | `2025-10-14 00:00:00` | `2025-10-14` |
| oocito_id=283570 | `aas_start` | `2025-10-14 00:00:00` | `2025-10-14` |
| oocito_id=283569 | `aas_start` | `2025-10-14 00:00:00` | `2025-10-14` |
| oocito_id=283568 | `aas_start` | `2025-10-14 00:00:00` | `2025-10-14` |
| oocito_id=288336 | `azitromicina_days` | `3.0` | `1.0` |
| oocito_id=288335 | `azitromicina_days` | `3.0` | `1.0` |
| oocito_id=288334 | `azitromicina_days` | `3.0` | `1.0` |
| oocito_id=288333 | `azitromicina_days` | `3.0` | `1.0` |
| oocito_id=288332 | `azitromicina_days` | `3.0` | `1.0` |
| oocito_id=288331 | `azitromicina_days` | `3.0` | `1.0` |
| oocito_id=288330 | `azitromicina_days` | `3.0` | `1.0` |
| oocito_id=288329 | `azitromicina_days` | `3.0` | `1.0` |
| oocito_id=288328 | `azitromicina_days` | `3.0` | `1.0` |
| oocito_id=288327 | `azitromicina_days` | `3.0` | `1.0` |
| oocito_id=288336 | `azitromicina_dose` | `3.0` | `1.0` |
| oocito_id=288335 | `azitromicina_dose` | `3.0` | `1.0` |
| oocito_id=288334 | `azitromicina_dose` | `3.0` | `1.0` |
| oocito_id=288333 | `azitromicina_dose` | `3.0` | `1.0` |
| oocito_id=288332 | `azitromicina_dose` | `3.0` | `1.0` |
| oocito_id=288331 | `azitromicina_dose` | `3.0` | `1.0` |
| oocito_id=288330 | `azitromicina_dose` | `3.0` | `1.0` |
| oocito_id=288329 | `azitromicina_dose` | `3.0` | `1.0` |
| oocito_id=288328 | `azitromicina_dose` | `3.0` | `1.0` |
| oocito_id=288327 | `azitromicina_dose` | `3.0` | `1.0` |
| oocito_id=288336 | `azitromicina_end` | `2025-11-24 00:00:00` | `2025-11-24` |
| oocito_id=288335 | `azitromicina_end` | `2025-11-24 00:00:00` | `2025-11-24` |
| oocito_id=288334 | `azitromicina_end` | `2025-11-24 00:00:00` | `2025-11-24` |
| oocito_id=288333 | `azitromicina_end` | `2025-11-24 00:00:00` | `2025-11-24` |
| oocito_id=288332 | `azitromicina_end` | `2025-11-24 00:00:00` | `2025-11-24` |
| oocito_id=288331 | `azitromicina_end` | `2025-11-24 00:00:00` | `2025-11-24` |
| oocito_id=288330 | `azitromicina_end` | `2025-11-24 00:00:00` | `2025-11-24` |
| oocito_id=288329 | `azitromicina_end` | `2025-11-24 00:00:00` | `2025-11-24` |
| oocito_id=288328 | `azitromicina_end` | `2025-11-24 00:00:00` | `2025-11-24` |
| oocito_id=288327 | `azitromicina_end` | `2025-11-24 00:00:00` | `2025-11-24` |
| oocito_id=288336 | `azitromicina_start` | `2025-11-24 00:00:00` | `2025-11-24` |
| oocito_id=288335 | `azitromicina_start` | `2025-11-24 00:00:00` | `2025-11-24` |
| oocito_id=288334 | `azitromicina_start` | `2025-11-24 00:00:00` | `2025-11-24` |
| oocito_id=288333 | `azitromicina_start` | `2025-11-24 00:00:00` | `2025-11-24` |
| oocito_id=288332 | `azitromicina_start` | `2025-11-24 00:00:00` | `2025-11-24` |
| oocito_id=288331 | `azitromicina_start` | `2025-11-24 00:00:00` | `2025-11-24` |
| oocito_id=288330 | `azitromicina_start` | `2025-11-24 00:00:00` | `2025-11-24` |
| oocito_id=288329 | `azitromicina_start` | `2025-11-24 00:00:00` | `2025-11-24` |
| oocito_id=288328 | `azitromicina_start` | `2025-11-24 00:00:00` | `2025-11-24` |
| oocito_id=288327 | `azitromicina_start` | `2025-11-24 00:00:00` | `2025-11-24` |
| oocito_id=304776 | `cerazette_days` | `22.0` | `15.0` |
| oocito_id=304775 | `cerazette_days` | `22.0` | `15.0` |
| oocito_id=304774 | `cerazette_days` | `22.0` | `15.0` |
| oocito_id=304773 | `cerazette_days` | `22.0` | `15.0` |
| oocito_id=304772 | `cerazette_days` | `22.0` | `15.0` |
| oocito_id=304771 | `cerazette_days` | `22.0` | `15.0` |
| oocito_id=304770 | `cerazette_days` | `22.0` | `15.0` |
| oocito_id=304769 | `cerazette_days` | `22.0` | `15.0` |
| oocito_id=304768 | `cerazette_days` | `22.0` | `15.0` |
| oocito_id=304767 | `cerazette_days` | `22.0` | `15.0` |
| oocito_id=304776 | `cerazette_dose` | `22.0` | `15.0` |
| oocito_id=304775 | `cerazette_dose` | `22.0` | `15.0` |
| oocito_id=304774 | `cerazette_dose` | `22.0` | `15.0` |
| oocito_id=304773 | `cerazette_dose` | `22.0` | `15.0` |
| oocito_id=304772 | `cerazette_dose` | `22.0` | `15.0` |
| oocito_id=304771 | `cerazette_dose` | `22.0` | `15.0` |
| oocito_id=304770 | `cerazette_dose` | `22.0` | `15.0` |
| oocito_id=304769 | `cerazette_dose` | `22.0` | `15.0` |
| oocito_id=304768 | `cerazette_dose` | `22.0` | `15.0` |
| oocito_id=304767 | `cerazette_dose` | `22.0` | `15.0` |
| oocito_id=311261 | `cerazette_end` | `2026-04-28 00:00:00` | `2026-04-28` |
| oocito_id=311260 | `cerazette_end` | `2026-04-28 00:00:00` | `2026-04-28` |
| oocito_id=311193 | `cerazette_end` | `2026-04-28 00:00:00` | `2026-04-28` |
| oocito_id=311192 | `cerazette_end` | `2026-04-28 00:00:00` | `2026-04-28` |
| oocito_id=311191 | `cerazette_end` | `2026-04-28 00:00:00` | `2026-04-28` |
| oocito_id=311190 | `cerazette_end` | `2026-04-28 00:00:00` | `2026-04-28` |
| oocito_id=311189 | `cerazette_end` | `2026-04-28 00:00:00` | `2026-04-28` |
| oocito_id=311188 | `cerazette_end` | `2026-04-28 00:00:00` | `2026-04-28` |
| oocito_id=306926 | `cerazette_end` | `2026-04-01 00:00:00` | `2026-04-01` |
| oocito_id=306925 | `cerazette_end` | `2026-04-01 00:00:00` | `2026-04-01` |
| oocito_id=271005 | `cerazette_interval` | `24.0` | `nan` |
| oocito_id=271004 | `cerazette_interval` | `24.0` | `nan` |
| oocito_id=271003 | `cerazette_interval` | `24.0` | `nan` |
| oocito_id=271002 | `cerazette_interval` | `24.0` | `nan` |
| oocito_id=311261 | `cerazette_start` | `2026-04-20 00:00:00` | `2026-04-20` |
| oocito_id=311260 | `cerazette_start` | `2026-04-20 00:00:00` | `2026-04-20` |
| oocito_id=311193 | `cerazette_start` | `2026-04-20 00:00:00` | `2026-04-20` |
| oocito_id=311192 | `cerazette_start` | `2026-04-20 00:00:00` | `2026-04-20` |
| oocito_id=311191 | `cerazette_start` | `2026-04-20 00:00:00` | `2026-04-20` |
| oocito_id=311190 | `cerazette_start` | `2026-04-20 00:00:00` | `2026-04-20` |
| oocito_id=311189 | `cerazette_start` | `2026-04-20 00:00:00` | `2026-04-20` |
| oocito_id=311188 | `cerazette_start` | `2026-04-20 00:00:00` | `2026-04-20` |
| oocito_id=306926 | `cerazette_start` | `2026-03-20 00:00:00` | `2026-03-20` |
| oocito_id=306925 | `cerazette_start` | `2026-03-20 00:00:00` | `2026-03-20` |
| oocito_id=271005 | `cerazette_unit` | `comp` | `nan` |
| oocito_id=271004 | `cerazette_unit` | `comp` | `nan` |
| oocito_id=271003 | `cerazette_unit` | `comp` | `nan` |
| oocito_id=271002 | `cerazette_unit` | `comp` | `nan` |
| oocito_id=315272 | `cetrotide_days` | `4.0` | `2.0` |
| oocito_id=315271 | `cetrotide_days` | `4.0` | `2.0` |
| oocito_id=315270 | `cetrotide_days` | `4.0` | `2.0` |
| oocito_id=315269 | `cetrotide_days` | `4.0` | `2.0` |
| oocito_id=315268 | `cetrotide_days` | `4.0` | `2.0` |
| oocito_id=315267 | `cetrotide_days` | `4.0` | `2.0` |
| oocito_id=315266 | `cetrotide_days` | `4.0` | `2.0` |
| oocito_id=315265 | `cetrotide_days` | `4.0` | `2.0` |
| oocito_id=315264 | `cetrotide_days` | `4.0` | `2.0` |
| oocito_id=315263 | `cetrotide_days` | `4.0` | `2.0` |
| oocito_id=315272 | `cetrotide_dose` | `1.0` | `0.5` |
| oocito_id=315271 | `cetrotide_dose` | `1.0` | `0.5` |
| oocito_id=315270 | `cetrotide_dose` | `1.0` | `0.5` |
| oocito_id=315269 | `cetrotide_dose` | `1.0` | `0.5` |
| oocito_id=315268 | `cetrotide_dose` | `1.0` | `0.5` |
| oocito_id=315267 | `cetrotide_dose` | `1.0` | `0.5` |
| oocito_id=315266 | `cetrotide_dose` | `1.0` | `0.5` |
| oocito_id=315265 | `cetrotide_dose` | `1.0` | `0.5` |
| oocito_id=315264 | `cetrotide_dose` | `1.0` | `0.5` |
| oocito_id=315263 | `cetrotide_dose` | `1.0` | `0.5` |
| oocito_id=315272 | `cetrotide_end` | `2026-05-24 00:00:00` | `2026-05-24` |
| oocito_id=315271 | `cetrotide_end` | `2026-05-24 00:00:00` | `2026-05-24` |
| oocito_id=315270 | `cetrotide_end` | `2026-05-24 00:00:00` | `2026-05-24` |
| oocito_id=315269 | `cetrotide_end` | `2026-05-24 00:00:00` | `2026-05-24` |
| oocito_id=315268 | `cetrotide_end` | `2026-05-24 00:00:00` | `2026-05-24` |
| oocito_id=315267 | `cetrotide_end` | `2026-05-24 00:00:00` | `2026-05-24` |
| oocito_id=315266 | `cetrotide_end` | `2026-05-24 00:00:00` | `2026-05-24` |
| oocito_id=315265 | `cetrotide_end` | `2026-05-24 00:00:00` | `2026-05-24` |
| oocito_id=315264 | `cetrotide_end` | `2026-05-24 00:00:00` | `2026-05-24` |
| oocito_id=315263 | `cetrotide_end` | `2026-05-24 00:00:00` | `2026-05-24` |
| oocito_id=315272 | `cetrotide_start` | `2026-05-23 00:00:00` | `2026-05-23` |
| oocito_id=315271 | `cetrotide_start` | `2026-05-23 00:00:00` | `2026-05-23` |
| oocito_id=315270 | `cetrotide_start` | `2026-05-23 00:00:00` | `2026-05-23` |
| oocito_id=315269 | `cetrotide_start` | `2026-05-23 00:00:00` | `2026-05-23` |
| oocito_id=315268 | `cetrotide_start` | `2026-05-23 00:00:00` | `2026-05-23` |
| oocito_id=315267 | `cetrotide_start` | `2026-05-23 00:00:00` | `2026-05-23` |
| oocito_id=315266 | `cetrotide_start` | `2026-05-23 00:00:00` | `2026-05-23` |
| oocito_id=315265 | `cetrotide_start` | `2026-05-23 00:00:00` | `2026-05-23` |
| oocito_id=315264 | `cetrotide_start` | `2026-05-23 00:00:00` | `2026-05-23` |
| oocito_id=315263 | `cetrotide_start` | `2026-05-23 00:00:00` | `2026-05-23` |
| oocito_id=286487 | `choriomon_days` | `2.0` | `1.0` |
| oocito_id=286486 | `choriomon_days` | `2.0` | `1.0` |
| oocito_id=286485 | `choriomon_days` | `2.0` | `1.0` |
| oocito_id=286484 | `choriomon_days` | `2.0` | `1.0` |
| oocito_id=286483 | `choriomon_days` | `2.0` | `1.0` |
| oocito_id=235619 | `choriomon_days` | `2.0` | `1.0` |
| oocito_id=235618 | `choriomon_days` | `2.0` | `1.0` |
| oocito_id=235617 | `choriomon_days` | `2.0` | `1.0` |
| oocito_id=227720 | `choriomon_days` | `2.0` | `1.0` |
| oocito_id=227719 | `choriomon_days` | `2.0` | `1.0` |
| oocito_id=286487 | `choriomon_dose` | `10000.0` | `5000.0` |
| oocito_id=286486 | `choriomon_dose` | `10000.0` | `5000.0` |
| oocito_id=286485 | `choriomon_dose` | `10000.0` | `5000.0` |
| oocito_id=286484 | `choriomon_dose` | `10000.0` | `5000.0` |
| oocito_id=286483 | `choriomon_dose` | `10000.0` | `5000.0` |
| oocito_id=235619 | `choriomon_dose` | `4.0` | `2.0` |
| oocito_id=235618 | `choriomon_dose` | `4.0` | `2.0` |
| oocito_id=235617 | `choriomon_dose` | `4.0` | `2.0` |
| oocito_id=227720 | `choriomon_dose` | `10000.0` | `5000.0` |
| oocito_id=227719 | `choriomon_dose` | `10000.0` | `5000.0` |
| oocito_id=286487 | `choriomon_end` | `2025-11-15 00:00:00` | `2025-11-15` |
| oocito_id=286486 | `choriomon_end` | `2025-11-15 00:00:00` | `2025-11-15` |
| oocito_id=286485 | `choriomon_end` | `2025-11-15 00:00:00` | `2025-11-15` |
| oocito_id=286484 | `choriomon_end` | `2025-11-15 00:00:00` | `2025-11-15` |
| oocito_id=286483 | `choriomon_end` | `2025-11-15 00:00:00` | `2025-11-15` |
| oocito_id=253538 | `choriomon_end` | `2025-05-08 00:00:00` | `2025-05-08` |
| oocito_id=253537 | `choriomon_end` | `2025-05-08 00:00:00` | `2025-05-08` |
| oocito_id=253536 | `choriomon_end` | `2025-05-08 00:00:00` | `2025-05-08` |
| oocito_id=253535 | `choriomon_end` | `2025-05-08 00:00:00` | `2025-05-08` |
| oocito_id=244680 | `choriomon_end` | `2025-03-13 00:00:00` | `2025-03-13` |
| oocito_id=286487 | `choriomon_start` | `2025-11-15 00:00:00` | `2025-11-15` |
| oocito_id=286486 | `choriomon_start` | `2025-11-15 00:00:00` | `2025-11-15` |
| oocito_id=286485 | `choriomon_start` | `2025-11-15 00:00:00` | `2025-11-15` |
| oocito_id=286484 | `choriomon_start` | `2025-11-15 00:00:00` | `2025-11-15` |
| oocito_id=286483 | `choriomon_start` | `2025-11-15 00:00:00` | `2025-11-15` |
| oocito_id=253538 | `choriomon_start` | `2025-05-08 00:00:00` | `2025-05-08` |
| oocito_id=253537 | `choriomon_start` | `2025-05-08 00:00:00` | `2025-05-08` |
| oocito_id=253536 | `choriomon_start` | `2025-05-08 00:00:00` | `2025-05-08` |
| oocito_id=253535 | `choriomon_start` | `2025-05-08 00:00:00` | `2025-05-08` |
| oocito_id=244680 | `choriomon_start` | `2025-03-13 00:00:00` | `2025-03-13` |
| oocito_id=116057 | `choriomon_unit` | `NÃO DEFINIDO` | `ampola` |
| oocito_id=116056 | `choriomon_unit` | `NÃO DEFINIDO` | `ampola` |
| oocito_id=116055 | `choriomon_unit` | `NÃO DEFINIDO` | `ampola` |
| oocito_id=116054 | `choriomon_unit` | `NÃO DEFINIDO` | `ampola` |
| oocito_id=99535 | `choriomon_unit` | `NÃO DEFINIDO` | `ampola` |
| oocito_id=99534 | `choriomon_unit` | `NÃO DEFINIDO` | `ampola` |
| oocito_id=99533 | `choriomon_unit` | `NÃO DEFINIDO` | `ampola` |
| oocito_id=99532 | `choriomon_unit` | `NÃO DEFINIDO` | `ampola` |
| oocito_id=99531 | `choriomon_unit` | `NÃO DEFINIDO` | `ampola` |
| oocito_id=99530 | `choriomon_unit` | `NÃO DEFINIDO` | `ampola` |
| oocito_id=315292 | `clexane_days` | `29.0` | `10.0` |
| oocito_id=315291 | `clexane_days` | `29.0` | `10.0` |
| oocito_id=315290 | `clexane_days` | `29.0` | `10.0` |
| oocito_id=315289 | `clexane_days` | `29.0` | `10.0` |
| oocito_id=313942 | `clexane_days` | `4.0` | `1.0` |
| oocito_id=313941 | `clexane_days` | `4.0` | `1.0` |
| oocito_id=313940 | `clexane_days` | `4.0` | `1.0` |
| oocito_id=313939 | `clexane_days` | `4.0` | `1.0` |
| oocito_id=308157 | `clexane_days` | `18.0` | `6.0` |
| oocito_id=308156 | `clexane_days` | `18.0` | `6.0` |
| oocito_id=315292 | `clexane_dose` | `1160.0` | `400.0` |
| oocito_id=315291 | `clexane_dose` | `1160.0` | `400.0` |
| oocito_id=315290 | `clexane_dose` | `1160.0` | `400.0` |
| oocito_id=315289 | `clexane_dose` | `1160.0` | `400.0` |
| oocito_id=313942 | `clexane_dose` | `80.0` | `20.0` |
| oocito_id=313941 | `clexane_dose` | `80.0` | `20.0` |
| oocito_id=313940 | `clexane_dose` | `80.0` | `20.0` |
| oocito_id=313939 | `clexane_dose` | `80.0` | `20.0` |
| oocito_id=308157 | `clexane_dose` | `720.0` | `240.0` |
| oocito_id=308156 | `clexane_dose` | `720.0` | `240.0` |
| oocito_id=315292 | `clexane_end` | `2026-05-20 00:00:00` | `2026-05-20` |
| oocito_id=315291 | `clexane_end` | `2026-05-20 00:00:00` | `2026-05-20` |
| oocito_id=315290 | `clexane_end` | `2026-05-20 00:00:00` | `2026-05-20` |
| oocito_id=315289 | `clexane_end` | `2026-05-20 00:00:00` | `2026-05-20` |
| oocito_id=313942 | `clexane_end` | `2026-05-22 00:00:00` | `2026-05-22` |
| oocito_id=313941 | `clexane_end` | `2026-05-22 00:00:00` | `2026-05-22` |
| oocito_id=313940 | `clexane_end` | `2026-05-22 00:00:00` | `2026-05-22` |
| oocito_id=313939 | `clexane_end` | `2026-05-22 00:00:00` | `2026-05-22` |
| oocito_id=308157 | `clexane_end` | `2026-04-08 00:00:00` | `2026-04-08` |
| oocito_id=308156 | `clexane_end` | `2026-04-08 00:00:00` | `2026-04-08` |
| oocito_id=315292 | `clexane_start` | `2026-05-11 00:00:00` | `2026-05-11` |
| oocito_id=315291 | `clexane_start` | `2026-05-11 00:00:00` | `2026-05-11` |
| oocito_id=315290 | `clexane_start` | `2026-05-11 00:00:00` | `2026-05-11` |
| oocito_id=315289 | `clexane_start` | `2026-05-11 00:00:00` | `2026-05-11` |
| oocito_id=313942 | `clexane_start` | `2026-05-22 00:00:00` | `2026-05-22` |
| oocito_id=313941 | `clexane_start` | `2026-05-22 00:00:00` | `2026-05-22` |
| oocito_id=313940 | `clexane_start` | `2026-05-22 00:00:00` | `2026-05-22` |
| oocito_id=313939 | `clexane_start` | `2026-05-22 00:00:00` | `2026-05-22` |
| oocito_id=308157 | `clexane_start` | `2026-04-03 00:00:00` | `2026-04-03` |
| oocito_id=308156 | `clexane_start` | `2026-04-03 00:00:00` | `2026-04-03` |
| oocito_id=316948 | `clomid_days` | `15.0` | `nan` |
| oocito_id=297474 | `clomid_days` | `10.0` | `5.0` |
| oocito_id=297473 | `clomid_days` | `10.0` | `5.0` |
| oocito_id=297472 | `clomid_days` | `10.0` | `5.0` |
| oocito_id=297471 | `clomid_days` | `10.0` | `5.0` |
| oocito_id=297470 | `clomid_days` | `10.0` | `5.0` |
| oocito_id=297469 | `clomid_days` | `10.0` | `5.0` |
| oocito_id=297468 | `clomid_days` | `10.0` | `5.0` |
| oocito_id=297467 | `clomid_days` | `10.0` | `5.0` |
| oocito_id=297466 | `clomid_days` | `10.0` | `5.0` |
| oocito_id=316948 | `clomid_dose` | `750.0` | `nan` |
| oocito_id=297474 | `clomid_dose` | `1000.0` | `500.0` |
| oocito_id=297473 | `clomid_dose` | `1000.0` | `500.0` |
| oocito_id=297472 | `clomid_dose` | `1000.0` | `500.0` |
| oocito_id=297471 | `clomid_dose` | `1000.0` | `500.0` |
| oocito_id=297470 | `clomid_dose` | `1000.0` | `500.0` |
| oocito_id=297469 | `clomid_dose` | `1000.0` | `500.0` |
| oocito_id=297468 | `clomid_dose` | `1000.0` | `500.0` |
| oocito_id=297467 | `clomid_dose` | `1000.0` | `500.0` |
| oocito_id=297466 | `clomid_dose` | `1000.0` | `500.0` |
| oocito_id=316948 | `clomid_end` | `2026-02-28 00:00:00` | `None` |
| oocito_id=314025 | `clomid_end` | `2026-05-11 00:00:00` | `2026-05-11` |
| oocito_id=314024 | `clomid_end` | `2026-05-11 00:00:00` | `2026-05-11` |
| oocito_id=314023 | `clomid_end` | `2026-05-11 00:00:00` | `2026-05-11` |
| oocito_id=297474 | `clomid_end` | `2026-01-25 00:00:00` | `2026-01-25` |
| oocito_id=297473 | `clomid_end` | `2026-01-25 00:00:00` | `2026-01-25` |
| oocito_id=297472 | `clomid_end` | `2026-01-25 00:00:00` | `2026-01-25` |
| oocito_id=297471 | `clomid_end` | `2026-01-25 00:00:00` | `2026-01-25` |
| oocito_id=297470 | `clomid_end` | `2026-01-25 00:00:00` | `2026-01-25` |
| oocito_id=297469 | `clomid_end` | `2026-01-25 00:00:00` | `2026-01-25` |
| oocito_id=316948 | `clomid_interval` | `24.0` | `nan` |
| oocito_id=282900 | `clomid_interval` | `12.0` | `nan` |
| oocito_id=282899 | `clomid_interval` | `12.0` | `nan` |
| oocito_id=282898 | `clomid_interval` | `12.0` | `nan` |
| oocito_id=282897 | `clomid_interval` | `12.0` | `nan` |
| oocito_id=282896 | `clomid_interval` | `12.0` | `nan` |
| oocito_id=282895 | `clomid_interval` | `12.0` | `nan` |
| oocito_id=316948 | `clomid_start` | `2026-02-24 00:00:00` | `None` |
| oocito_id=314025 | `clomid_start` | `2026-05-05 00:00:00` | `2026-05-05` |
| oocito_id=314024 | `clomid_start` | `2026-05-05 00:00:00` | `2026-05-05` |
| oocito_id=314023 | `clomid_start` | `2026-05-05 00:00:00` | `2026-05-05` |
| oocito_id=297474 | `clomid_start` | `2026-01-21 00:00:00` | `2026-01-21` |
| oocito_id=297473 | `clomid_start` | `2026-01-21 00:00:00` | `2026-01-21` |
| oocito_id=297472 | `clomid_start` | `2026-01-21 00:00:00` | `2026-01-21` |
| oocito_id=297471 | `clomid_start` | `2026-01-21 00:00:00` | `2026-01-21` |
| oocito_id=297470 | `clomid_start` | `2026-01-21 00:00:00` | `2026-01-21` |
| oocito_id=297469 | `clomid_start` | `2026-01-21 00:00:00` | `2026-01-21` |
| oocito_id=316948 | `clomid_unit` | `mg` | `nan` |
| oocito_id=282900 | `clomid_unit` | `mg` | `nan` |
| oocito_id=282899 | `clomid_unit` | `mg` | `nan` |
| oocito_id=282898 | `clomid_unit` | `mg` | `nan` |
| oocito_id=282897 | `clomid_unit` | `mg` | `nan` |
| oocito_id=282896 | `clomid_unit` | `mg` | `nan` |
| oocito_id=282895 | `clomid_unit` | `mg` | `nan` |
| oocito_id=215681 | `clomifeno_end` | `2024-09-03 00:00:00` | `2024-09-03` |
| oocito_id=215680 | `clomifeno_end` | `2024-09-03 00:00:00` | `2024-09-03` |
| oocito_id=215679 | `clomifeno_end` | `2024-09-03 00:00:00` | `2024-09-03` |
| oocito_id=215678 | `clomifeno_end` | `2024-09-03 00:00:00` | `2024-09-03` |
| oocito_id=215677 | `clomifeno_end` | `2024-09-03 00:00:00` | `2024-09-03` |
| oocito_id=215676 | `clomifeno_end` | `2024-09-03 00:00:00` | `2024-09-03` |
| oocito_id=215675 | `clomifeno_end` | `2024-09-03 00:00:00` | `2024-09-03` |
| oocito_id=215681 | `clomifeno_start` | `2024-08-24 00:00:00` | `2024-08-24` |
| oocito_id=215680 | `clomifeno_start` | `2024-08-24 00:00:00` | `2024-08-24` |
| oocito_id=215679 | `clomifeno_start` | `2024-08-24 00:00:00` | `2024-08-24` |
| oocito_id=215678 | `clomifeno_start` | `2024-08-24 00:00:00` | `2024-08-24` |
| oocito_id=215677 | `clomifeno_start` | `2024-08-24 00:00:00` | `2024-08-24` |
| oocito_id=215676 | `clomifeno_start` | `2024-08-24 00:00:00` | `2024-08-24` |
| oocito_id=215675 | `clomifeno_start` | `2024-08-24 00:00:00` | `2024-08-24` |
| oocito_id=319425 | `cong_em_cod_congelamento` | `nan` | `E1628/26` |
| oocito_id=319423 | `cong_em_cod_congelamento` | `nan` | `E1628/26` |
| oocito_id=294705 | `cong_em_cod_congelamento` | `E123/26` | `E134/26` |
| oocito_id=294702 | `cong_em_cod_congelamento` | `E123/26` | `E134/26` |
| oocito_id=294697 | `cong_em_cod_congelamento` | `E123/26` | `E134/26` |
| oocito_id=291980 | `cong_em_cod_congelamento` | `E1171/26` | `E3467/25` |
| oocito_id=287222 | `cong_em_cod_congelamento` | `E1171/26` | `E3203/25` |
| oocito_id=273413 | `cong_em_cod_congelamento` | `E2407/25` | `E1352/26` |
| oocito_id=273409 | `cong_em_cod_congelamento` | `E2407/25` | `E1352/26` |
| oocito_id=273407 | `cong_em_cod_congelamento` | `E2407/25` | `E1352/26` |
| oocito_id=319425 | `cong_em_data` | `NaT` | `2026-06-27` |
| oocito_id=319423 | `cong_em_data` | `NaT` | `2026-06-27` |
| oocito_id=318541 | `cong_em_data` | `2026-06-20 00:00:00` | `2026-06-20` |
| oocito_id=318540 | `cong_em_data` | `2026-06-20 00:00:00` | `2026-06-20` |
| oocito_id=318539 | `cong_em_data` | `2026-06-20 00:00:00` | `2026-06-20` |
| oocito_id=318538 | `cong_em_data` | `2026-06-20 00:00:00` | `2026-06-20` |
| oocito_id=318537 | `cong_em_data` | `2026-06-20 00:00:00` | `2026-06-20` |
| oocito_id=317715 | `cong_em_data` | `2026-06-15 00:00:00` | `2026-06-15` |
| oocito_id=317711 | `cong_em_data` | `2026-06-15 00:00:00` | `2026-06-15` |
| oocito_id=317710 | `cong_em_data` | `2026-06-15 00:00:00` | `2026-06-15` |
| oocito_id=319425 | `cong_em_id` | `<NA>` | `32892.0` |
| oocito_id=319423 | `cong_em_id` | `<NA>` | `32892.0` |
| oocito_id=294705 | `cong_em_id` | `31370` | `31381.0` |
| oocito_id=294702 | `cong_em_id` | `31370` | `31381.0` |
| oocito_id=294697 | `cong_em_id` | `31370` | `31381.0` |
| oocito_id=291980 | `cong_em_id` | `32429` | `31157.0` |
| oocito_id=287222 | `cong_em_id` | `32429` | `30889.0` |
| oocito_id=273413 | `cong_em_id` | `30090` | `32611.0` |
| oocito_id=273409 | `cong_em_id` | `30090` | `32611.0` |
| oocito_id=273407 | `cong_em_id` | `30090` | `32611.0` |
| oocito_id=296529 | `crinone_days` | `12.0` | `6.0` |
| oocito_id=296528 | `crinone_days` | `12.0` | `6.0` |
| oocito_id=296527 | `crinone_days` | `12.0` | `6.0` |
| oocito_id=296526 | `crinone_days` | `12.0` | `6.0` |
| oocito_id=296525 | `crinone_days` | `12.0` | `6.0` |
| oocito_id=294413 | `crinone_days` | `95.0` | `29.0` |
| oocito_id=294412 | `crinone_days` | `95.0` | `29.0` |
| oocito_id=294411 | `crinone_days` | `95.0` | `29.0` |
| oocito_id=294410 | `crinone_days` | `95.0` | `29.0` |
| oocito_id=294409 | `crinone_days` | `95.0` | `29.0` |
| oocito_id=296529 | `crinone_dose` | `1080.0` | `540.0` |
| oocito_id=296528 | `crinone_dose` | `1080.0` | `540.0` |
| oocito_id=296527 | `crinone_dose` | `1080.0` | `540.0` |
| oocito_id=296526 | `crinone_dose` | `1080.0` | `540.0` |
| oocito_id=296525 | `crinone_dose` | `1080.0` | `540.0` |
| oocito_id=294413 | `crinone_dose` | `8550.0` | `2610.0` |
| oocito_id=294412 | `crinone_dose` | `8550.0` | `2610.0` |
| oocito_id=294411 | `crinone_dose` | `8550.0` | `2610.0` |
| oocito_id=294410 | `crinone_dose` | `8550.0` | `2610.0` |
| oocito_id=294409 | `crinone_dose` | `8550.0` | `2610.0` |
| oocito_id=305809 | `crinone_end` | `2026-04-01 00:00:00` | `2026-04-01` |
| oocito_id=305808 | `crinone_end` | `2026-04-01 00:00:00` | `2026-04-01` |
| oocito_id=305807 | `crinone_end` | `2026-04-01 00:00:00` | `2026-04-01` |
| oocito_id=305806 | `crinone_end` | `2026-04-01 00:00:00` | `2026-04-01` |
| oocito_id=305805 | `crinone_end` | `2026-04-01 00:00:00` | `2026-04-01` |
| oocito_id=296529 | `crinone_end` | `2026-02-02 00:00:00` | `2026-02-02` |
| oocito_id=296528 | `crinone_end` | `2026-02-02 00:00:00` | `2026-02-02` |
| oocito_id=296527 | `crinone_end` | `2026-02-02 00:00:00` | `2026-02-02` |
| oocito_id=296526 | `crinone_end` | `2026-02-02 00:00:00` | `2026-02-02` |
| oocito_id=296525 | `crinone_end` | `2026-02-02 00:00:00` | `2026-02-02` |
| oocito_id=305809 | `crinone_start` | `2026-03-27 00:00:00` | `2026-03-27` |
| oocito_id=305808 | `crinone_start` | `2026-03-27 00:00:00` | `2026-03-27` |
| oocito_id=305807 | `crinone_start` | `2026-03-27 00:00:00` | `2026-03-27` |
| oocito_id=305806 | `crinone_start` | `2026-03-27 00:00:00` | `2026-03-27` |
| oocito_id=305805 | `crinone_start` | `2026-03-27 00:00:00` | `2026-03-27` |
| oocito_id=296529 | `crinone_start` | `2026-01-28 00:00:00` | `2026-01-28` |
| oocito_id=296528 | `crinone_start` | `2026-01-28 00:00:00` | `2026-01-28` |
| oocito_id=296527 | `crinone_start` | `2026-01-28 00:00:00` | `2026-01-28` |
| oocito_id=296526 | `crinone_start` | `2026-01-28 00:00:00` | `2026-01-28` |
| oocito_id=296525 | `crinone_start` | `2026-01-28 00:00:00` | `2026-01-28` |
| oocito_id=262449 | `date_of_delivery` | `NaT` | `2024-07-23` |
| oocito_id=251147 | `date_of_delivery` | `NaT` | `2023-09-03` |
| oocito_id=251141 | `date_of_delivery` | `NaT` | `2023-09-03` |
| oocito_id=246960 | `date_of_delivery` | `NaT` | `2024-12-18` |
| oocito_id=246959 | `date_of_delivery` | `NaT` | `2024-12-18` |
| oocito_id=246958 | `date_of_delivery` | `NaT` | `2024-12-18` |
| oocito_id=246957 | `date_of_delivery` | `NaT` | `2024-12-18` |
| oocito_id=246956 | `date_of_delivery` | `NaT` | `2024-12-18` |
| oocito_id=239383 | `date_of_delivery` | `NaT` | `2023-04-12` |
| oocito_id=209228 | `date_of_delivery` | `NaT` | `2023-06-04` |
| oocito_id=296529 | `delestrogen_days` | `28.0` | `14.0` |
| oocito_id=296528 | `delestrogen_days` | `28.0` | `14.0` |
| oocito_id=296527 | `delestrogen_days` | `28.0` | `14.0` |
| oocito_id=296526 | `delestrogen_days` | `28.0` | `14.0` |
| oocito_id=296525 | `delestrogen_days` | `28.0` | `14.0` |
| oocito_id=223099 | `delestrogen_days` | `52.0` | `26.0` |
| oocito_id=223098 | `delestrogen_days` | `52.0` | `26.0` |
| oocito_id=223097 | `delestrogen_days` | `52.0` | `26.0` |
| oocito_id=223096 | `delestrogen_days` | `52.0` | `26.0` |
| oocito_id=223095 | `delestrogen_days` | `52.0` | `26.0` |
| oocito_id=296529 | `delestrogen_dose` | `[PHONE_REDACTED]6667` | `[PHONE_REDACTED]3333` |
| oocito_id=296528 | `delestrogen_dose` | `[PHONE_REDACTED]6667` | `[PHONE_REDACTED]3333` |
| oocito_id=296527 | `delestrogen_dose` | `[PHONE_REDACTED]6667` | `[PHONE_REDACTED]3333` |
| oocito_id=296526 | `delestrogen_dose` | `[PHONE_REDACTED]6667` | `[PHONE_REDACTED]3333` |
| oocito_id=296525 | `delestrogen_dose` | `[PHONE_REDACTED]6667` | `[PHONE_REDACTED]3333` |
| oocito_id=223099 | `delestrogen_dose` | `[PHONE_REDACTED]667` | `[PHONE_REDACTED]3334` |
| oocito_id=223098 | `delestrogen_dose` | `[PHONE_REDACTED]667` | `[PHONE_REDACTED]3334` |
| oocito_id=223097 | `delestrogen_dose` | `[PHONE_REDACTED]667` | `[PHONE_REDACTED]3334` |
| oocito_id=223096 | `delestrogen_dose` | `[PHONE_REDACTED]667` | `[PHONE_REDACTED]3334` |
| oocito_id=223095 | `delestrogen_dose` | `[PHONE_REDACTED]667` | `[PHONE_REDACTED]3334` |
| oocito_id=296529 | `delestrogen_end` | `2026-02-02 00:00:00` | `2026-02-02` |
| oocito_id=296528 | `delestrogen_end` | `2026-02-02 00:00:00` | `2026-02-02` |
| oocito_id=296527 | `delestrogen_end` | `2026-02-02 00:00:00` | `2026-02-02` |
| oocito_id=296526 | `delestrogen_end` | `2026-02-02 00:00:00` | `2026-02-02` |
| oocito_id=296525 | `delestrogen_end` | `2026-02-02 00:00:00` | `2026-02-02` |
| oocito_id=247740 | `delestrogen_end` | `2025-04-02 00:00:00` | `2025-04-02` |
| oocito_id=247739 | `delestrogen_end` | `2025-04-02 00:00:00` | `2025-04-02` |
| oocito_id=247738 | `delestrogen_end` | `2025-04-02 00:00:00` | `2025-04-02` |
| oocito_id=247737 | `delestrogen_end` | `2025-04-02 00:00:00` | `2025-04-02` |
| oocito_id=247736 | `delestrogen_end` | `2025-04-02 00:00:00` | `2025-04-02` |
| oocito_id=296529 | `delestrogen_start` | `2026-01-20 00:00:00` | `2026-01-20` |
| oocito_id=296528 | `delestrogen_start` | `2026-01-20 00:00:00` | `2026-01-20` |
| oocito_id=296527 | `delestrogen_start` | `2026-01-20 00:00:00` | `2026-01-20` |
| oocito_id=296526 | `delestrogen_start` | `2026-01-20 00:00:00` | `2026-01-20` |
| oocito_id=296525 | `delestrogen_start` | `2026-01-20 00:00:00` | `2026-01-20` |
| oocito_id=247740 | `delestrogen_start` | `2025-03-14 00:00:00` | `2025-03-14` |
| oocito_id=247739 | `delestrogen_start` | `2025-03-14 00:00:00` | `2025-03-14` |
| oocito_id=247738 | `delestrogen_start` | `2025-03-14 00:00:00` | `2025-03-14` |
| oocito_id=247737 | `delestrogen_start` | `2025-03-14 00:00:00` | `2025-03-14` |
| oocito_id=247736 | `delestrogen_start` | `2025-03-14 00:00:00` | `2025-03-14` |
| oocito_id=311195 | `descong_em_cod_descongelamento` | `nan` | `1499/26` |
| oocito_id=308148 | `descong_em_cod_descongelamento` | `nan` | `1420/26` |
| oocito_id=305980 | `descong_em_cod_descongelamento` | `nan` | `1427/26` |
| oocito_id=301767 | `descong_em_cod_descongelamento` | `nan` | `1479/26` |
| oocito_id=296168 | `descong_em_cod_descongelamento` | `nan` | `1403/26` |
| oocito_id=294296 | `descong_em_cod_descongelamento` | `nan` | `1472/26` |
| oocito_id=291980 | `descong_em_cod_descongelamento` | `nan` | `1031/26` |
| oocito_id=287222 | `descong_em_cod_descongelamento` | `nan` | `1031/26` |
| oocito_id=285744 | `descong_em_cod_descongelamento` | `nan` | `1512/26` |
| oocito_id=274316 | `descong_em_cod_descongelamento` | `nan` | `1533/26` |
| oocito_id=311262 | `descong_em_data_congelamento` | `2026-05-05 00:00:00` | `2026-05-05` |
| oocito_id=311195 | `descong_em_data_congelamento` | `NaT` | `2026-05-05` |
| oocito_id=308148 | `descong_em_data_congelamento` | `NaT` | `2026-05-08` |
| oocito_id=305980 | `descong_em_data_congelamento` | `NaT` | `2026-04-01` |
| oocito_id=304771 | `descong_em_data_congelamento` | `2026-03-25 00:00:00` | `2026-03-25` |
| oocito_id=304767 | `descong_em_data_congelamento` | `2026-03-25 00:00:00` | `2026-03-25` |
| oocito_id=304766 | `descong_em_data_congelamento` | `2026-03-25 00:00:00` | `2026-03-25` |
| oocito_id=303974 | `descong_em_data_congelamento` | `2026-03-19 00:00:00` | `2026-03-19` |
| oocito_id=303876 | `descong_em_data_congelamento` | `2026-03-19 00:00:00` | `2026-03-19` |
| oocito_id=301767 | `descong_em_data_congelamento` | `NaT` | `2026-02-09` |
| oocito_id=311262 | `descong_em_data_descongelamento` | `2026-05-25 00:00:00` | `2026-05-25` |
| oocito_id=311195 | `descong_em_data_descongelamento` | `NaT` | `2026-07-08` |
| oocito_id=308148 | `descong_em_data_descongelamento` | `NaT` | `2026-06-26` |
| oocito_id=305980 | `descong_em_data_descongelamento` | `NaT` | `2026-06-29` |
| oocito_id=304771 | `descong_em_data_descongelamento` | `2026-05-04 00:00:00` | `2026-05-04` |
| oocito_id=304767 | `descong_em_data_descongelamento` | `2026-05-04 00:00:00` | `2026-05-04` |
| oocito_id=304766 | `descong_em_data_descongelamento` | `2026-05-04 00:00:00` | `2026-05-04` |
| oocito_id=303974 | `descong_em_data_descongelamento` | `2026-06-05 00:00:00` | `2026-06-05` |
| oocito_id=303876 | `descong_em_data_descongelamento` | `2026-06-08 00:00:00` | `2026-06-08` |
| oocito_id=301767 | `descong_em_data_descongelamento` | `NaT` | `2026-07-06` |
| oocito_id=311262 | `descong_em_data_transferencia` | `2026-05-25 00:00:00` | `2026-05-25` |
| oocito_id=311195 | `descong_em_data_transferencia` | `NaT` | `2026-07-08` |
| oocito_id=308148 | `descong_em_data_transferencia` | `NaT` | `2026-06-26` |
| oocito_id=305980 | `descong_em_data_transferencia` | `NaT` | `2026-06-29` |
| oocito_id=304771 | `descong_em_data_transferencia` | `2026-05-04 00:00:00` | `2026-05-04` |
| oocito_id=304767 | `descong_em_data_transferencia` | `2026-05-04 00:00:00` | `2026-05-04` |
| oocito_id=304766 | `descong_em_data_transferencia` | `2026-05-04 00:00:00` | `2026-05-04` |
| oocito_id=303974 | `descong_em_data_transferencia` | `2026-06-05 00:00:00` | `2026-06-05` |
| oocito_id=303876 | `descong_em_data_transferencia` | `2026-06-08 00:00:00` | `2026-06-08` |
| oocito_id=301767 | `descong_em_data_transferencia` | `NaT` | `2026-07-06` |
| oocito_id=311195 | `descong_em_id` | `<NA>` | `21469.0` |
| oocito_id=308148 | `descong_em_id` | `<NA>` | `21386.0` |
| oocito_id=305980 | `descong_em_id` | `<NA>` | `21393.0` |
| oocito_id=301767 | `descong_em_id` | `<NA>` | `21449.0` |
| oocito_id=296168 | `descong_em_id` | `<NA>` | `21369.0` |
| oocito_id=294296 | `descong_em_id` | `<NA>` | `21442.0` |
| oocito_id=291980 | `descong_em_id` | `<NA>` | `20968.0` |
| oocito_id=287222 | `descong_em_id` | `<NA>` | `20968.0` |
| oocito_id=285744 | `descong_em_id` | `<NA>` | `21483.0` |
| oocito_id=274316 | `descong_em_id` | `<NA>` | `21505.0` |
| oocito_id=273090 | `dostinex_days` | `24.0` | `8.0` |
| oocito_id=273089 | `dostinex_days` | `24.0` | `8.0` |
| oocito_id=273088 | `dostinex_days` | `24.0` | `8.0` |
| oocito_id=273087 | `dostinex_days` | `24.0` | `8.0` |
| oocito_id=273086 | `dostinex_days` | `24.0` | `8.0` |
| oocito_id=273085 | `dostinex_days` | `24.0` | `8.0` |
| oocito_id=273084 | `dostinex_days` | `24.0` | `8.0` |
| oocito_id=273083 | `dostinex_days` | `24.0` | `8.0` |
| oocito_id=273082 | `dostinex_days` | `24.0` | `8.0` |
| oocito_id=273081 | `dostinex_days` | `24.0` | `8.0` |
| oocito_id=273090 | `dostinex_dose` | `12.0` | `4.0` |
| oocito_id=273089 | `dostinex_dose` | `12.0` | `4.0` |
| oocito_id=273088 | `dostinex_dose` | `12.0` | `4.0` |
| oocito_id=273087 | `dostinex_dose` | `12.0` | `4.0` |
| oocito_id=273086 | `dostinex_dose` | `12.0` | `4.0` |
| oocito_id=273085 | `dostinex_dose` | `12.0` | `4.0` |
| oocito_id=273084 | `dostinex_dose` | `12.0` | `4.0` |
| oocito_id=273083 | `dostinex_dose` | `12.0` | `4.0` |
| oocito_id=273082 | `dostinex_dose` | `12.0` | `4.0` |
| oocito_id=273081 | `dostinex_dose` | `12.0` | `4.0` |
| oocito_id=278754 | `dostinex_end` | `2025-10-07 00:00:00` | `2025-10-07` |
| oocito_id=278753 | `dostinex_end` | `2025-10-07 00:00:00` | `2025-10-07` |
| oocito_id=278752 | `dostinex_end` | `2025-10-07 00:00:00` | `2025-10-07` |
| oocito_id=278751 | `dostinex_end` | `2025-10-07 00:00:00` | `2025-10-07` |
| oocito_id=278750 | `dostinex_end` | `2025-10-07 00:00:00` | `2025-10-07` |
| oocito_id=278749 | `dostinex_end` | `2025-10-07 00:00:00` | `2025-10-07` |
| oocito_id=278748 | `dostinex_end` | `2025-10-07 00:00:00` | `2025-10-07` |
| oocito_id=278747 | `dostinex_end` | `2025-10-07 00:00:00` | `2025-10-07` |
| oocito_id=278746 | `dostinex_end` | `2025-10-07 00:00:00` | `2025-10-07` |
| oocito_id=278745 | `dostinex_end` | `2025-10-07 00:00:00` | `2025-10-07` |
| oocito_id=278754 | `dostinex_start` | `2025-09-30 00:00:00` | `2025-09-30` |
| oocito_id=278753 | `dostinex_start` | `2025-09-30 00:00:00` | `2025-09-30` |
| oocito_id=278752 | `dostinex_start` | `2025-09-30 00:00:00` | `2025-09-30` |
| oocito_id=278751 | `dostinex_start` | `2025-09-30 00:00:00` | `2025-09-30` |
| oocito_id=278750 | `dostinex_start` | `2025-09-30 00:00:00` | `2025-09-30` |
| oocito_id=278749 | `dostinex_start` | `2025-09-30 00:00:00` | `2025-09-30` |
| oocito_id=278748 | `dostinex_start` | `2025-09-30 00:00:00` | `2025-09-30` |
| oocito_id=278747 | `dostinex_start` | `2025-09-30 00:00:00` | `2025-09-30` |
| oocito_id=278746 | `dostinex_start` | `2025-09-30 00:00:00` | `2025-09-30` |
| oocito_id=278745 | `dostinex_start` | `2025-09-30 00:00:00` | `2025-09-30` |
| oocito_id=319427 | `duphaston_days` | `23.0` | `12.0` |
| oocito_id=319426 | `duphaston_days` | `23.0` | `12.0` |
| oocito_id=319425 | `duphaston_days` | `23.0` | `12.0` |
| oocito_id=319424 | `duphaston_days` | `23.0` | `12.0` |
| oocito_id=319423 | `duphaston_days` | `23.0` | `12.0` |
| oocito_id=319422 | `duphaston_days` | `23.0` | `12.0` |
| oocito_id=319421 | `duphaston_days` | `23.0` | `12.0` |
| oocito_id=318548 | `duphaston_days` | `19.0` | `11.0` |
| oocito_id=318547 | `duphaston_days` | `19.0` | `11.0` |
| oocito_id=318546 | `duphaston_days` | `19.0` | `11.0` |
| oocito_id=319427 | `duphaston_dose` | `460.0` | `240.0` |
| oocito_id=319426 | `duphaston_dose` | `460.0` | `240.0` |
| oocito_id=319425 | `duphaston_dose` | `460.0` | `240.0` |
| oocito_id=319424 | `duphaston_dose` | `460.0` | `240.0` |
| oocito_id=319423 | `duphaston_dose` | `460.0` | `240.0` |
| oocito_id=319422 | `duphaston_dose` | `460.0` | `240.0` |
| oocito_id=319421 | `duphaston_dose` | `460.0` | `240.0` |
| oocito_id=318548 | `duphaston_dose` | `380.0` | `220.0` |
| oocito_id=318547 | `duphaston_dose` | `380.0` | `220.0` |
| oocito_id=318546 | `duphaston_dose` | `380.0` | `220.0` |
| oocito_id=319427 | `duphaston_end` | `2026-06-21 00:00:00` | `2026-06-21` |
| oocito_id=319426 | `duphaston_end` | `2026-06-21 00:00:00` | `2026-06-21` |
| oocito_id=319425 | `duphaston_end` | `2026-06-21 00:00:00` | `2026-06-21` |
| oocito_id=319424 | `duphaston_end` | `2026-06-21 00:00:00` | `2026-06-21` |
| oocito_id=319423 | `duphaston_end` | `2026-06-21 00:00:00` | `2026-06-21` |
| oocito_id=319422 | `duphaston_end` | `2026-06-21 00:00:00` | `2026-06-21` |
| oocito_id=319421 | `duphaston_end` | `2026-06-21 00:00:00` | `2026-06-21` |
| oocito_id=318548 | `duphaston_end` | `2026-06-13 00:00:00` | `2026-06-13` |
| oocito_id=318547 | `duphaston_end` | `2026-06-13 00:00:00` | `2026-06-13` |
| oocito_id=318546 | `duphaston_end` | `2026-06-13 00:00:00` | `2026-06-13` |
| oocito_id=316948 | `duphaston_interval` | `8.0` | `12.0` |
| oocito_id=315346 | `duphaston_interval` | `8.0` | `12.0` |
| oocito_id=315345 | `duphaston_interval` | `8.0` | `12.0` |
| oocito_id=315344 | `duphaston_interval` | `8.0` | `12.0` |
| oocito_id=315343 | `duphaston_interval` | `8.0` | `12.0` |
| oocito_id=299704 | `duphaston_interval` | `8.0` | `12.0` |
| oocito_id=299703 | `duphaston_interval` | `8.0` | `12.0` |
| oocito_id=299702 | `duphaston_interval` | `8.0` | `12.0` |
| oocito_id=299701 | `duphaston_interval` | `8.0` | `12.0` |
| oocito_id=299700 | `duphaston_interval` | `8.0` | `12.0` |
| oocito_id=319427 | `duphaston_start` | `2026-06-10 00:00:00` | `2026-06-10` |
| oocito_id=319426 | `duphaston_start` | `2026-06-10 00:00:00` | `2026-06-10` |
| oocito_id=319425 | `duphaston_start` | `2026-06-10 00:00:00` | `2026-06-10` |
| oocito_id=319424 | `duphaston_start` | `2026-06-10 00:00:00` | `2026-06-10` |
| oocito_id=319423 | `duphaston_start` | `2026-06-10 00:00:00` | `2026-06-10` |
| oocito_id=319422 | `duphaston_start` | `2026-06-10 00:00:00` | `2026-06-10` |
| oocito_id=319421 | `duphaston_start` | `2026-06-10 00:00:00` | `2026-06-10` |
| oocito_id=318548 | `duphaston_start` | `2026-06-03 00:00:00` | `2026-06-03` |
| oocito_id=318547 | `duphaston_start` | `2026-06-03 00:00:00` | `2026-06-03` |
| oocito_id=318546 | `duphaston_start` | `2026-06-03 00:00:00` | `2026-06-03` |
| oocito_id=228470 | `duphaston_unit` | `cap` | `mg` |
| oocito_id=228469 | `duphaston_unit` | `cap` | `mg` |
| oocito_id=228468 | `duphaston_unit` | `cap` | `mg` |
| oocito_id=228467 | `duphaston_unit` | `cap` | `mg` |
| oocito_id=228466 | `duphaston_unit` | `cap` | `mg` |
| oocito_id=228465 | `duphaston_unit` | `cap` | `mg` |
| oocito_id=228464 | `duphaston_unit` | `cap` | `mg` |
| oocito_id=198294 | `duphaston_unit` | `comp` | `mg` |
| oocito_id=198293 | `duphaston_unit` | `comp` | `mg` |
| oocito_id=198292 | `duphaston_unit` | `comp` | `mg` |
| oocito_id=317241 | `elonva_days` | `2.0` | `1.0` |
| oocito_id=317240 | `elonva_days` | `2.0` | `1.0` |
| oocito_id=317239 | `elonva_days` | `2.0` | `1.0` |
| oocito_id=311830 | `elonva_days` | `12.0` | `6.0` |
| oocito_id=311829 | `elonva_days` | `12.0` | `6.0` |
| oocito_id=311828 | `elonva_days` | `12.0` | `6.0` |
| oocito_id=311827 | `elonva_days` | `12.0` | `6.0` |
| oocito_id=304776 | `elonva_days` | `4.0` | `2.0` |
| oocito_id=304775 | `elonva_days` | `4.0` | `2.0` |
| oocito_id=304774 | `elonva_days` | `4.0` | `2.0` |
| oocito_id=317241 | `elonva_dose` | `300.0` | `150.0` |
| oocito_id=317240 | `elonva_dose` | `300.0` | `150.0` |
| oocito_id=317239 | `elonva_dose` | `300.0` | `150.0` |
| oocito_id=311830 | `elonva_dose` | `[PHONE_REDACTED]8571` | `[PHONE_REDACTED]42856` |
| oocito_id=311829 | `elonva_dose` | `[PHONE_REDACTED]8571` | `[PHONE_REDACTED]42856` |
| oocito_id=311828 | `elonva_dose` | `[PHONE_REDACTED]8571` | `[PHONE_REDACTED]42856` |
| oocito_id=311827 | `elonva_dose` | `[PHONE_REDACTED]8571` | `[PHONE_REDACTED]42856` |
| oocito_id=304776 | `elonva_dose` | `500.0` | `250.0` |
| oocito_id=304775 | `elonva_dose` | `500.0` | `250.0` |
| oocito_id=304774 | `elonva_dose` | `500.0` | `250.0` |
| oocito_id=317241 | `elonva_end` | `2026-05-24 00:00:00` | `2026-05-24` |
| oocito_id=317240 | `elonva_end` | `2026-05-24 00:00:00` | `2026-05-24` |
| oocito_id=317239 | `elonva_end` | `2026-05-24 00:00:00` | `2026-05-24` |
| oocito_id=314025 | `elonva_end` | `2026-05-05 00:00:00` | `2026-05-05` |
| oocito_id=314024 | `elonva_end` | `2026-05-05 00:00:00` | `2026-05-05` |
| oocito_id=314023 | `elonva_end` | `2026-05-05 00:00:00` | `2026-05-05` |
| oocito_id=311830 | `elonva_end` | `2026-04-26 00:00:00` | `2026-04-26` |
| oocito_id=311829 | `elonva_end` | `2026-04-26 00:00:00` | `2026-04-26` |
| oocito_id=311828 | `elonva_end` | `2026-04-26 00:00:00` | `2026-04-26` |
| oocito_id=311827 | `elonva_end` | `2026-04-26 00:00:00` | `2026-04-26` |
| oocito_id=317241 | `elonva_start` | `2026-05-24 00:00:00` | `2026-05-24` |
| oocito_id=317240 | `elonva_start` | `2026-05-24 00:00:00` | `2026-05-24` |
| oocito_id=317239 | `elonva_start` | `2026-05-24 00:00:00` | `2026-05-24` |
| oocito_id=314025 | `elonva_start` | `2026-05-05 00:00:00` | `2026-05-05` |
| oocito_id=314024 | `elonva_start` | `2026-05-05 00:00:00` | `2026-05-05` |
| oocito_id=314023 | `elonva_start` | `2026-05-05 00:00:00` | `2026-05-05` |
| oocito_id=311830 | `elonva_start` | `2026-04-21 00:00:00` | `2026-04-21` |
| oocito_id=311829 | `elonva_start` | `2026-04-21 00:00:00` | `2026-04-21` |
| oocito_id=311828 | `elonva_start` | `2026-04-21 00:00:00` | `2026-04-21` |
| oocito_id=311827 | `elonva_start` | `2026-04-21 00:00:00` | `2026-04-21` |
| oocito_id=164623 | `elonva_unit` | `UI` | `ampola` |
| oocito_id=164622 | `elonva_unit` | `UI` | `ampola` |
| oocito_id=164621 | `elonva_unit` | `UI` | `ampola` |
| oocito_id=164620 | `elonva_unit` | `UI` | `ampola` |
| oocito_id=164619 | `elonva_unit` | `UI` | `ampola` |
| oocito_id=164618 | `elonva_unit` | `UI` | `ampola` |
| oocito_id=164617 | `elonva_unit` | `UI` | `ampola` |
| oocito_id=164616 | `elonva_unit` | `UI` | `ampola` |
| oocito_id=164615 | `elonva_unit` | `UI` | `ampola` |
| oocito_id=164614 | `elonva_unit` | `UI` | `ampola` |
| oocito_id=319425 | `emb_cong_id` | `<NA>` | `100001.0` |
| oocito_id=319423 | `emb_cong_id` | `<NA>` | `100000.0` |
| oocito_id=294705 | `emb_cong_id` | `94547` | `94585.0` |
| oocito_id=294702 | `emb_cong_id` | `94545` | `94583.0` |
| oocito_id=294697 | `emb_cong_id` | `94544` | `94581.0` |
| oocito_id=291980 | `emb_cong_id` | `98443` | `93781.0` |
| oocito_id=287222 | `emb_cong_id` | `98440` | `92757.0` |
| oocito_id=273413 | `emb_cong_id` | `89774` | `99057.0` |
| oocito_id=273409 | `emb_cong_id` | `89773` | `99056.0` |
| oocito_id=273407 | `emb_cong_id` | `89772` | `99055.0` |
| oocito_id=319425 | `emb_cong_qualidade` | `nan` | `Blastocisto Grau 4 - A - A` |
| oocito_id=319423 | `emb_cong_qualidade` | `nan` | `Blastocisto Grau 4 - A - B` |
| oocito_id=236845 | `emb_cong_qualidade` | `Blastocisto Grau 5 - A - A` | `Blastocisto Grau 4 - A - A` |
| oocito_id=163710 | `emb_cong_qualidade` | `Blastocisto Grau 3 - B - B` | `Blastocisto Grau 5 - A - A` |
| oocito_id=156881 | `emb_cong_qualidade` | `Blastocisto Grau 5 - A - B` | `Blastocisto Grau 5 - A - A` |
| oocito_id=156879 | `emb_cong_qualidade` | `Blastocisto Grau 5 - A - B` | `Blastocisto Grau 4 - A - B` |
| oocito_id=105093 | `emb_cong_qualidade` | `Blastocisto Grau 5 - C - C` | `Blastocisto Grau 5 - B - C` |
| oocito_id=311195 | `emb_cong_transferidos` | `nan` | `Transferido` |
| oocito_id=308148 | `emb_cong_transferidos` | `nan` | `Transferido` |
| oocito_id=305980 | `emb_cong_transferidos` | `nan` | `Transferido` |
| oocito_id=301767 | `emb_cong_transferidos` | `nan` | `Transferido` |
| oocito_id=296168 | `emb_cong_transferidos` | `nan` | `Transferido` |
| oocito_id=294705 | `emb_cong_transferidos` | `nan` | `Descartado` |
| oocito_id=294702 | `emb_cong_transferidos` | `nan` | `Descartado` |
| oocito_id=294296 | `emb_cong_transferidos` | `nan` | `Transferido` |
| oocito_id=291980 | `emb_cong_transferidos` | `nan` | `Criopreservado` |
| oocito_id=287222 | `emb_cong_transferidos` | `Descartado` | `Criopreservado` |
| oocito_id=294314 | `embryo_description` | `nan` | `Low-level Aneuploid Mosaic -22, XX` |
| oocito_id=130381 | `embryo_description` | `nan` | `ANEUPLOIDE MONOSSOMIA PARCIAL 6q21q27 XX` |
| oocito_id=130028 | `embryo_description` | `nan` | `Alterado complexo, Feminino` |
| oocito_id=128939 | `embryo_description` | `nan` | `EUPLOIDE/ PLOIDIA NAO INFORMATIVA XY` |
| oocito_id=126605 | `embryo_description` | `nan` | `ANEUPLOIDE COMPLEXO TRISSOMIA 16 E 21 MASCULINO` |
| oocito_id=126604 | `embryo_description` | `nan` | `ANEUPLOIDE TRISSOMIA 16 FEMININO` |
| oocito_id=126602 | `embryo_description` | `nan` | `SURGIMENTO 3PN / ANEUPLOIDE MONOSSOMIA 9 FEMININO` |
| oocito_id=125796 | `embryo_description` | `nan` | `ANEUPLOIDE MONOSSOMIA 16 FEMININO` |
| oocito_id=122860 | `embryo_description` | `nan` | `ANEUPLOIDE - TRISSOMIA 21, XX` |
| oocito_id=122859 | `embryo_description` | `nan` | `ANEUPLOIDE - MONOSSOMIA 6, XY` |
| oocito_id=319427 | `embryo_embryo_date` | `2026-06-22 00:00:00` | `2026-06-22` |
| oocito_id=319426 | `embryo_embryo_date` | `2026-06-22 00:00:00` | `2026-06-22` |
| oocito_id=319425 | `embryo_embryo_date` | `2026-06-22 00:00:00` | `2026-06-22` |
| oocito_id=319424 | `embryo_embryo_date` | `2026-06-22 00:00:00` | `2026-06-22` |
| oocito_id=319423 | `embryo_embryo_date` | `2026-06-22 00:00:00` | `2026-06-22` |
| oocito_id=319422 | `embryo_embryo_date` | `2026-06-22 00:00:00` | `2026-06-22` |
| oocito_id=319421 | `embryo_embryo_date` | `2026-06-22 00:00:00` | `2026-06-22` |
| oocito_id=318548 | `embryo_embryo_date` | `2026-06-15 00:00:00` | `2026-06-15` |
| oocito_id=318547 | `embryo_embryo_date` | `2026-06-15 00:00:00` | `2026-06-15` |
| oocito_id=318546 | `embryo_embryo_date` | `2026-06-15 00:00:00` | `2026-06-15` |
| oocito_id=319427 | `embryo_embryo_description_id` | `AE8` | `AE7` |
| oocito_id=319426 | `embryo_embryo_description_id` | `AE7` | `AE6` |
| oocito_id=319425 | `embryo_embryo_description_id` | `AE6` | `AE5` |
| oocito_id=319424 | `embryo_embryo_description_id` | `AE5` | `AE4` |
| oocito_id=319423 | `embryo_embryo_description_id` | `AE4` | `AE3` |
| oocito_id=319422 | `embryo_embryo_description_id` | `AE3` | `AE2` |
| oocito_id=319421 | `embryo_embryo_description_id` | `AE2` | `AE1` |
| oocito_id=318548 | `embryo_embryo_description_id` | `AA13` | `AA12` |
| oocito_id=318547 | `embryo_embryo_description_id` | `AA12` | `AA11` |
| oocito_id=318546 | `embryo_embryo_description_id` | `AA11` | `AA10` |
| oocito_id=319425 | `embryo_embryo_fate` | `Unknown` | `Freeze` |
| oocito_id=319423 | `embryo_embryo_fate` | `Unknown` | `Freeze` |
| oocito_id=319421 | `embryo_embryo_fate` | `Unknown` | `Avoid` |
| oocito_id=318548 | `embryo_embryo_fate` | `Unknown` | `Avoid` |
| oocito_id=318547 | `embryo_embryo_fate` | `Unknown` | `Avoid` |
| oocito_id=318546 | `embryo_embryo_fate` | `Unknown` | `Avoid` |
| oocito_id=318545 | `embryo_embryo_fate` | `Unknown` | `Avoid` |
| oocito_id=318544 | `embryo_embryo_fate` | `Unknown` | `Avoid` |
| oocito_id=318543 | `embryo_embryo_fate` | `Unknown` | `Avoid` |
| oocito_id=318542 | `embryo_embryo_fate` | `Unknown` | `Avoid` |
| oocito_id=319427 | `embryo_embryo_id` | `D2026.06.22_S02652_I4120_P-8` | `D2026.06.22_S02652_I4120_P-7` |
| oocito_id=319426 | `embryo_embryo_id` | `D2026.06.22_S02652_I4120_P-7` | `D2026.06.22_S02652_I4120_P-6` |
| oocito_id=319425 | `embryo_embryo_id` | `D2026.06.22_S02652_I4120_P-6` | `D2026.06.22_S02652_I4120_P-5` |
| oocito_id=319424 | `embryo_embryo_id` | `D2026.06.22_S02652_I4120_P-5` | `D2026.06.22_S02652_I4120_P-4` |
| oocito_id=319423 | `embryo_embryo_id` | `D2026.06.22_S02652_I4120_P-4` | `D2026.06.22_S02652_I4120_P-3` |
| oocito_id=319422 | `embryo_embryo_id` | `D2026.06.22_S02652_I4120_P-3` | `D2026.06.22_S02652_I4120_P-2` |
| oocito_id=319421 | `embryo_embryo_id` | `D2026.06.22_S02652_I4120_P-2` | `D2026.06.22_S02652_I4120_P-1` |
| oocito_id=318548 | `embryo_embryo_id` | `D2026.06.15_S02649_I4120_P-13` | `D2026.06.15_S02649_I4120_P-12` |
| oocito_id=318547 | `embryo_embryo_id` | `D2026.06.15_S02649_I4120_P-12` | `D2026.06.15_S02649_I4120_P-11` |
| oocito_id=318546 | `embryo_embryo_id` | `D2026.06.15_S02649_I4120_P-11` | `D2026.06.15_S02649_I4120_P-10` |
| oocito_id=128939 | `embryo_fertilization_method` | `nan` | `ICSI` |
| oocito_id=128938 | `embryo_fertilization_method` | `nan` | `ICSI` |
| oocito_id=128937 | `embryo_fertilization_method` | `nan` | `ICSI` |
| oocito_id=128936 | `embryo_fertilization_method` | `nan` | `ICSI` |
| oocito_id=128935 | `embryo_fertilization_method` | `nan` | `ICSI` |
| oocito_id=126626 | `embryo_fertilization_method` | `nan` | `ICSI` |
| oocito_id=126625 | `embryo_fertilization_method` | `nan` | `ICSI` |
| oocito_id=126624 | `embryo_fertilization_method` | `nan` | `ICSI` |
| oocito_id=126623 | `embryo_fertilization_method` | `nan` | `ICSI` |
| oocito_id=126618 | `embryo_fertilization_method` | `nan` | `ICSI` |
| oocito_id=294329 | `embryo_fertilization_time` | `2026-01-17 10:50:00` | `2026-01-17 10:40:00` |
| oocito_id=288929 | `embryo_fertilization_time` | `2025-11-28 10:55:00` | `2025-11-28 10:50:00` |
| oocito_id=288928 | `embryo_fertilization_time` | `2025-11-28 10:55:00` | `2025-11-28 10:50:00` |
| oocito_id=288927 | `embryo_fertilization_time` | `2025-11-28 10:55:00` | `2025-11-28 10:50:00` |
| oocito_id=288926 | `embryo_fertilization_time` | `2025-11-28 10:55:00` | `2025-11-28 10:50:00` |
| oocito_id=288925 | `embryo_fertilization_time` | `2025-11-28 10:55:00` | `2025-11-28 10:50:00` |
| oocito_id=288923 | `embryo_fertilization_time` | `2025-11-28 10:55:00` | `2025-11-28 10:50:00` |
| oocito_id=288922 | `embryo_fertilization_time` | `2025-11-28 10:55:00` | `2025-11-28 10:50:00` |
| oocito_id=288921 | `embryo_fertilization_time` | `2025-11-28 10:55:00` | `2025-11-28 10:50:00` |
| oocito_id=288920 | `embryo_fertilization_time` | `2025-11-28 10:55:00` | `2025-11-28 10:50:00` |
| oocito_id=313434 | `embryo_kid_date` | `NaT` | `2026-05-19 00:00:00` |
| oocito_id=313433 | `embryo_kid_date` | `NaT` | `2026-05-19 00:00:00` |
| oocito_id=313432 | `embryo_kid_date` | `NaT` | `2026-05-19 00:00:00` |
| oocito_id=313431 | `embryo_kid_date` | `NaT` | `2026-05-19 00:00:00` |
| oocito_id=313430 | `embryo_kid_date` | `NaT` | `2026-05-19 00:00:00` |
| oocito_id=313429 | `embryo_kid_date` | `NaT` | `2026-05-19 00:00:00` |
| oocito_id=313428 | `embryo_kid_date` | `NaT` | `2026-05-19 00:00:00` |
| oocito_id=313427 | `embryo_kid_date` | `NaT` | `2026-05-19 00:00:00` |
| oocito_id=313426 | `embryo_kid_date` | `NaT` | `2026-05-19 00:00:00` |
| oocito_id=313425 | `embryo_kid_date` | `NaT` | `2026-05-19 00:00:00` |
| oocito_id=313434 | `embryo_kid_score` | `nan` | `8.3` |
| oocito_id=313433 | `embryo_kid_score` | `nan` | `0` |
| oocito_id=313432 | `embryo_kid_score` | `nan` | `1.6` |
| oocito_id=313431 | `embryo_kid_score` | `nan` | `1.7` |
| oocito_id=313430 | `embryo_kid_score` | `nan` | `1.6` |
| oocito_id=313429 | `embryo_kid_score` | `nan` | `NA` |
| oocito_id=313428 | `embryo_kid_score` | `nan` | `4.4` |
| oocito_id=313427 | `embryo_kid_score` | `nan` | `4.2` |
| oocito_id=313426 | `embryo_kid_score` | `nan` | `8.9` |
| oocito_id=313425 | `embryo_kid_score` | `nan` | `5.6` |
| oocito_id=313434 | `embryo_kid_user` | `nan` | `ADMIN` |
| oocito_id=313433 | `embryo_kid_user` | `nan` | `ADMIN` |
| oocito_id=313432 | `embryo_kid_user` | `nan` | `ADMIN` |
| oocito_id=313431 | `embryo_kid_user` | `nan` | `ADMIN` |
| oocito_id=313430 | `embryo_kid_user` | `nan` | `ADMIN` |
| oocito_id=313429 | `embryo_kid_user` | `nan` | `ADMIN` |
| oocito_id=313428 | `embryo_kid_user` | `nan` | `ADMIN` |
| oocito_id=313427 | `embryo_kid_user` | `nan` | `ADMIN` |
| oocito_id=313426 | `embryo_kid_user` | `nan` | `ADMIN` |
| oocito_id=313425 | `embryo_kid_user` | `nan` | `ADMIN` |
| oocito_id=313434 | `embryo_kid_version` | `nan` | `KIDScoreD5 v3.3` |
| oocito_id=313433 | `embryo_kid_version` | `nan` | `KIDScoreD5 v3.3` |
| oocito_id=313432 | `embryo_kid_version` | `nan` | `KIDScoreD5 v3.3` |
| oocito_id=313431 | `embryo_kid_version` | `nan` | `KIDScoreD5 v3.3` |
| oocito_id=313430 | `embryo_kid_version` | `nan` | `KIDScoreD5 v3.3` |
| oocito_id=313429 | `embryo_kid_version` | `nan` | `KIDScoreD5 v3.3` |
| oocito_id=313428 | `embryo_kid_version` | `nan` | `KIDScoreD5 v3.3` |
| oocito_id=313427 | `embryo_kid_version` | `nan` | `KIDScoreD5 v3.3` |
| oocito_id=313426 | `embryo_kid_version` | `nan` | `KIDScoreD5 v3.3` |
| oocito_id=313425 | `embryo_kid_version` | `nan` | `KIDScoreD5 v3.3` |
| oocito_id=319427 | `embryo_position` | `8` | `4` |
| oocito_id=319426 | `embryo_position` | `8` | `4` |
| oocito_id=319425 | `embryo_position` | `8` | `4` |
| oocito_id=319424 | `embryo_position` | `8` | `4` |
| oocito_id=319423 | `embryo_position` | `8` | `4` |
| oocito_id=319422 | `embryo_position` | `8` | `4` |
| oocito_id=319421 | `embryo_position` | `8` | `4` |
| oocito_id=318548 | `embryo_position` | `12` | `7` |
| oocito_id=318547 | `embryo_position` | `12` | `7` |
| oocito_id=318546 | `embryo_position` | `12` | `7` |
| oocito_id=319427 | `embryo_time_fragmentation` | `nan` | `87.8` |
| oocito_id=319426 | `embryo_time_fragmentation` | `nan` | `91.7` |
| oocito_id=319425 | `embryo_time_fragmentation` | `nan` | `64.7` |
| oocito_id=319424 | `embryo_time_fragmentation` | `nan` | `51.3` |
| oocito_id=319423 | `embryo_time_fragmentation` | `nan` | `77.6` |
| oocito_id=319422 | `embryo_time_fragmentation` | `nan` | `110.2` |
| oocito_id=318548 | `embryo_time_fragmentation` | `23.4` | `65.9` |
| oocito_id=318547 | `embryo_time_fragmentation` | `27.1` | `64.3` |
| oocito_id=318546 | `embryo_time_fragmentation` | `42.6` | `59.7` |
| oocito_id=318545 | `embryo_time_fragmentation` | `22.6` | `53.4` |
| oocito_id=319427 | `embryo_time_t2` | `nan` | `36.6` |
| oocito_id=319426 | `embryo_time_t2` | `nan` | `29.6` |
| oocito_id=319425 | `embryo_time_t2` | `nan` | `22.4` |
| oocito_id=319423 | `embryo_time_t2` | `nan` | `32.0` |
| oocito_id=319422 | `embryo_time_t2` | `nan` | `31.2` |
| oocito_id=318548 | `embryo_time_t2` | `23.4` | `27.1` |
| oocito_id=318547 | `embryo_time_t2` | `27.1` | `31.4` |
| oocito_id=318546 | `embryo_time_t2` | `31.4` | `22.4` |
| oocito_id=318545 | `embryo_time_t2` | `22.4` | `23.9` |
| oocito_id=318544 | `embryo_time_t2` | `23.9` | `nan` |
| oocito_id=319427 | `embryo_time_t3` | `nan` | `51.5` |
| oocito_id=319426 | `embryo_time_t3` | `nan` | `42.3` |
| oocito_id=319425 | `embryo_time_t3` | `nan` | `32.6` |
| oocito_id=319423 | `embryo_time_t3` | `nan` | `44.4` |
| oocito_id=319422 | `embryo_time_t3` | `nan` | `38.0` |
| oocito_id=318548 | `embryo_time_t3` | `34.0` | `38.3` |
| oocito_id=318547 | `embryo_time_t3` | `38.3` | `42.6` |
| oocito_id=318546 | `embryo_time_t3` | `42.6` | `34.2` |
| oocito_id=318545 | `embryo_time_t3` | `34.2` | `33.2` |
| oocito_id=318544 | `embryo_time_t3` | `33.2` | `nan` |
| oocito_id=319427 | `embryo_time_t4` | `nan` | `52.2` |
| oocito_id=319426 | `embryo_time_t4` | `nan` | `57.6` |
| oocito_id=319425 | `embryo_time_t4` | `nan` | `35.0` |
| oocito_id=319424 | `embryo_time_t4` | `nan` | `32.8` |
| oocito_id=319423 | `embryo_time_t4` | `nan` | `44.8` |
| oocito_id=319422 | `embryo_time_t4` | `nan` | `43.2` |
| oocito_id=318548 | `embryo_time_t4` | `34.7` | `42.6` |
| oocito_id=318547 | `embryo_time_t4` | `42.6` | `43.1` |
| oocito_id=318546 | `embryo_time_t4` | `43.1` | `41.8` |
| oocito_id=318545 | `embryo_time_t4` | `41.8` | `34.7` |
| oocito_id=319427 | `embryo_time_t5` | `nan` | `53.8` |
| oocito_id=319426 | `embryo_time_t5` | `nan` | `60.5` |
| oocito_id=319425 | `embryo_time_t5` | `nan` | `44.5` |
| oocito_id=319424 | `embryo_time_t5` | `nan` | `44.5` |
| oocito_id=319423 | `embryo_time_t5` | `nan` | `58.4` |
| oocito_id=319422 | `embryo_time_t5` | `nan` | `44.0` |
| oocito_id=318548 | `embryo_time_t5` | `48.2` | `43.4` |
| oocito_id=318547 | `embryo_time_t5` | `43.4` | `43.8` |
| oocito_id=318546 | `embryo_time_t5` | `43.8` | `42.1` |
| oocito_id=318545 | `embryo_time_t5` | `42.1` | `46.4` |
| oocito_id=319427 | `embryo_time_t8` | `nan` | `84.6` |
| oocito_id=319426 | `embryo_time_t8` | `nan` | `85.4` |
| oocito_id=319425 | `embryo_time_t8` | `nan` | `53.9` |
| oocito_id=319424 | `embryo_time_t8` | `nan` | `45.2` |
| oocito_id=319423 | `embryo_time_t8` | `nan` | `63.1` |
| oocito_id=319422 | `embryo_time_t8` | `nan` | `90.5` |
| oocito_id=318548 | `embryo_time_t8` | `52.7` | `56.5` |
| oocito_id=318547 | `embryo_time_t8` | `56.5` | `64.3` |
| oocito_id=318546 | `embryo_time_t8` | `64.3` | `58.6` |
| oocito_id=318545 | `embryo_time_t8` | `58.6` | `49.8` |
| oocito_id=319425 | `embryo_time_tb` | `nan` | `109.4` |
| oocito_id=319423 | `embryo_time_tb` | `nan` | `109.6` |
| oocito_id=318541 | `embryo_time_tb` | `nan` | `116.0` |
| oocito_id=318540 | `embryo_time_tb` | `nan` | `94.8` |
| oocito_id=318539 | `embryo_time_tb` | `nan` | `94.6` |
| oocito_id=318538 | `embryo_time_tb` | `nan` | `108.3` |
| oocito_id=318537 | `embryo_time_tb` | `nan` | `108.8` |
| oocito_id=317715 | `embryo_time_tb` | `nan` | `114.3` |
| oocito_id=317714 | `embryo_time_tb` | `114.3` | `nan` |
| oocito_id=317242 | `embryo_time_tb` | `104.6` | `nan` |
| oocito_id=319425 | `embryo_time_teb` | `nan` | `118.3` |
| oocito_id=319423 | `embryo_time_teb` | `nan` | `118.1` |
| oocito_id=318541 | `embryo_time_teb` | `nan` | `123.9` |
| oocito_id=318540 | `embryo_time_teb` | `nan` | `99.1` |
| oocito_id=318539 | `embryo_time_teb` | `nan` | `100.7` |
| oocito_id=318538 | `embryo_time_teb` | `nan` | `115.6` |
| oocito_id=318537 | `embryo_time_teb` | `nan` | `112.2` |
| oocito_id=317715 | `embryo_time_teb` | `nan` | `116.3` |
| oocito_id=317714 | `embryo_time_teb` | `116.3` | `nan` |
| oocito_id=317242 | `embryo_time_teb` | `113.5` | `nan` |
| oocito_id=318541 | `embryo_time_thb` | `nan` | `131.7` |
| oocito_id=318540 | `embryo_time_thb` | `nan` | `100.8` |
| oocito_id=318539 | `embryo_time_thb` | `nan` | `102.5` |
| oocito_id=318537 | `embryo_time_thb` | `nan` | `113.2` |
| oocito_id=316893 | `embryo_time_thb` | `139.7` | `nan` |
| oocito_id=316848 | `embryo_time_thb` | `nan` | `127.7` |
| oocito_id=316846 | `embryo_time_thb` | `127.7` | `nan` |
| oocito_id=315819 | `embryo_time_thb` | `nan` | `115.4` |
| oocito_id=315289 | `embryo_time_thb` | `nan` | `111.9` |
| oocito_id=315271 | `embryo_time_thb` | `nan` | `115.7` |
| oocito_id=319426 | `embryo_time_tm` | `nan` | `105.9` |
| oocito_id=319425 | `embryo_time_tm` | `nan` | `97.5` |
| oocito_id=319423 | `embryo_time_tm` | `nan` | `97.8` |
| oocito_id=318542 | `embryo_time_tm` | `nan` | `80.2` |
| oocito_id=318541 | `embryo_time_tm` | `nan` | `92.3` |
| oocito_id=318540 | `embryo_time_tm` | `nan` | `76.6` |
| oocito_id=318539 | `embryo_time_tm` | `nan` | `82.6` |
| oocito_id=318538 | `embryo_time_tm` | `nan` | `88.8` |
| oocito_id=318537 | `embryo_time_tm` | `nan` | `72.1` |
| oocito_id=317715 | `embryo_time_tm` | `nan` | `103.1` |
| oocito_id=319427 | `embryo_time_tpna` | `10.5` | `9.3` |
| oocito_id=319426 | `embryo_time_tpna` | `9.3` | `8.6` |
| oocito_id=319425 | `embryo_time_tpna` | `8.6` | `9.6` |
| oocito_id=319424 | `embryo_time_tpna` | `9.6` | `10.1` |
| oocito_id=319423 | `embryo_time_tpna` | `10.1` | `9.6` |
| oocito_id=319422 | `embryo_time_tpna` | `9.6` | `11.0` |
| oocito_id=319421 | `embryo_time_tpna` | `11.0` | `nan` |
| oocito_id=318548 | `embryo_time_tpna` | `9.4` | `12.0` |
| oocito_id=318547 | `embryo_time_tpna` | `12.0` | `11.1` |
| oocito_id=318546 | `embryo_time_tpna` | `11.1` | `7.4` |
| oocito_id=319427 | `embryo_time_tpnf` | `nan` | `33.7` |
| oocito_id=319426 | `embryo_time_tpnf` | `nan` | `26.3` |
| oocito_id=319425 | `embryo_time_tpnf` | `nan` | `20.8` |
| oocito_id=319424 | `embryo_time_tpnf` | `nan` | `29.9` |
| oocito_id=319423 | `embryo_time_tpnf` | `nan` | `28.5` |
| oocito_id=319422 | `embryo_time_tpnf` | `nan` | `24.6` |
| oocito_id=318548 | `embryo_time_tpnf` | `21.1` | `25.0` |
| oocito_id=318547 | `embryo_time_tpnf` | `25.0` | `28.8` |
| oocito_id=318546 | `embryo_time_tpnf` | `28.8` | `19.3` |
| oocito_id=318545 | `embryo_time_tpnf` | `19.3` | `21.4` |
| oocito_id=319425 | `embryo_time_tsb` | `nan` | `101.0` |
| oocito_id=319423 | `embryo_time_tsb` | `nan` | `102.6` |
| oocito_id=318541 | `embryo_time_tsb` | `nan` | `98.1` |
| oocito_id=318540 | `embryo_time_tsb` | `nan` | `86.1` |
| oocito_id=318539 | `embryo_time_tsb` | `nan` | `86.7` |
| oocito_id=318538 | `embryo_time_tsb` | `nan` | `91.3` |
| oocito_id=318537 | `embryo_time_tsb` | `nan` | `86.1` |
| oocito_id=317715 | `embryo_time_tsb` | `nan` | `109.5` |
| oocito_id=317714 | `embryo_time_tsb` | `109.5` | `nan` |
| oocito_id=317242 | `embryo_time_tsb` | `95.1` | `nan` |
| oocito_id=319427 | `embryo_time_tsc` | `nan` | `115.9` |
| oocito_id=319426 | `embryo_time_tsc` | `nan` | `100.5` |
| oocito_id=319425 | `embryo_time_tsc` | `nan` | `95.2` |
| oocito_id=319424 | `embryo_time_tsc` | `nan` | `98.0` |
| oocito_id=319423 | `embryo_time_tsc` | `nan` | `91.1` |
| oocito_id=319422 | `embryo_time_tsc` | `nan` | `113.1` |
| oocito_id=318548 | `embryo_time_tsc` | `nan` | `77.9` |
| oocito_id=318546 | `embryo_time_tsc` | `nan` | `93.3` |
| oocito_id=318543 | `embryo_time_tsc` | `nan` | `88.5` |
| oocito_id=318542 | `embryo_time_tsc` | `nan` | `73.4` |
| oocito_id=294327 | `embryo_value_frag2cat` | `10-20%` | `nan` |
| oocito_id=294325 | `embryo_value_frag2cat` | `0-10%` | `nan` |
| oocito_id=280644 | `embryo_value_frag2cat` | `0-10%` | `nan` |
| oocito_id=319425 | `embryo_value_icm` | `nan` | `A` |
| oocito_id=319423 | `embryo_value_icm` | `nan` | `A` |
| oocito_id=318541 | `embryo_value_icm` | `nan` | `B` |
| oocito_id=318540 | `embryo_value_icm` | `nan` | `A` |
| oocito_id=318539 | `embryo_value_icm` | `nan` | `A` |
| oocito_id=318538 | `embryo_value_icm` | `nan` | `A` |
| oocito_id=318537 | `embryo_value_icm` | `nan` | `A` |
| oocito_id=317715 | `embryo_value_icm` | `nan` | `A` |
| oocito_id=317714 | `embryo_value_icm` | `A` | `nan` |
| oocito_id=317242 | `embryo_value_icm` | `A` | `nan` |
| oocito_id=294327 | `embryo_value_mn2_type` | `Mono` | `nan` |
| oocito_id=294325 | `embryo_value_mn2_type` | `Bi` | `nan` |
| oocito_id=280645 | `embryo_value_mn2_type` | `Multi` | `Mono` |
| oocito_id=280644 | `embryo_value_mn2_type` | `Mono` | `nan` |
| oocito_id=294327 | `embryo_value_nuclei2` | `0` | `nan` |
| oocito_id=294325 | `embryo_value_nuclei2` | `2` | `nan` |
| oocito_id=280644 | `embryo_value_nuclei2` | `2` | `nan` |
| oocito_id=319424 | `embryo_value_pn` | `2` | `3` |
| oocito_id=319423 | `embryo_value_pn` | `3` | `2` |
| oocito_id=319421 | `embryo_value_pn` | `2` | `nan` |
| oocito_id=318544 | `embryo_value_pn` | `2` | `nan` |
| oocito_id=317716 | `embryo_value_pn` | `2` | `nan` |
| oocito_id=317715 | `embryo_value_pn` | `nan` | `2` |
| oocito_id=317714 | `embryo_value_pn` | `2` | `nan` |
| oocito_id=317242 | `embryo_value_pn` | `2` | `nan` |
| oocito_id=316893 | `embryo_value_pn` | `3` | `2` |
| oocito_id=316847 | `embryo_value_pn` | `2` | `nan` |
| oocito_id=319425 | `embryo_value_te` | `nan` | `A` |
| oocito_id=319423 | `embryo_value_te` | `nan` | `B` |
| oocito_id=318541 | `embryo_value_te` | `nan` | `B` |
| oocito_id=318540 | `embryo_value_te` | `nan` | `A` |
| oocito_id=318539 | `embryo_value_te` | `nan` | `A` |
| oocito_id=318538 | `embryo_value_te` | `nan` | `B` |
| oocito_id=318537 | `embryo_value_te` | `nan` | `B` |
| oocito_id=317715 | `embryo_value_te` | `nan` | `A` |
| oocito_id=317714 | `embryo_value_te` | `A` | `nan` |
| oocito_id=317242 | `embryo_value_te` | `A` | `nan` |
| oocito_id=319427 | `embryo_well_number` | `8` | `7` |
| oocito_id=319426 | `embryo_well_number` | `7` | `6` |
| oocito_id=319425 | `embryo_well_number` | `6` | `5` |
| oocito_id=319424 | `embryo_well_number` | `5` | `4` |
| oocito_id=319423 | `embryo_well_number` | `4` | `3` |
| oocito_id=319422 | `embryo_well_number` | `3` | `2` |
| oocito_id=319421 | `embryo_well_number` | `2` | `1` |
| oocito_id=318548 | `embryo_well_number` | `13` | `12` |
| oocito_id=318547 | `embryo_well_number` | `12` | `11` |
| oocito_id=318546 | `embryo_well_number` | `11` | `10` |
| oocito_id=245434 | `femara_end` | `2025-03-27 00:00:00` | `2025-03-27` |
| oocito_id=245433 | `femara_end` | `2025-03-27 00:00:00` | `2025-03-27` |
| oocito_id=245432 | `femara_end` | `2025-03-27 00:00:00` | `2025-03-27` |
| oocito_id=245431 | `femara_end` | `2025-03-27 00:00:00` | `2025-03-27` |
| oocito_id=245430 | `femara_end` | `2025-03-27 00:00:00` | `2025-03-27` |
| oocito_id=245429 | `femara_end` | `2025-03-27 00:00:00` | `2025-03-27` |
| oocito_id=245428 | `femara_end` | `2025-03-27 00:00:00` | `2025-03-27` |
| oocito_id=245427 | `femara_end` | `2025-03-27 00:00:00` | `2025-03-27` |
| oocito_id=245426 | `femara_end` | `2025-03-27 00:00:00` | `2025-03-27` |
| oocito_id=245425 | `femara_end` | `2025-03-27 00:00:00` | `2025-03-27` |
| oocito_id=245434 | `femara_start` | `2025-03-20 00:00:00` | `2025-03-20` |
| oocito_id=245433 | `femara_start` | `2025-03-20 00:00:00` | `2025-03-20` |
| oocito_id=245432 | `femara_start` | `2025-03-20 00:00:00` | `2025-03-20` |
| oocito_id=245431 | `femara_start` | `2025-03-20 00:00:00` | `2025-03-20` |
| oocito_id=245430 | `femara_start` | `2025-03-20 00:00:00` | `2025-03-20` |
| oocito_id=245429 | `femara_start` | `2025-03-20 00:00:00` | `2025-03-20` |
| oocito_id=245428 | `femara_start` | `2025-03-20 00:00:00` | `2025-03-20` |
| oocito_id=245427 | `femara_start` | `2025-03-20 00:00:00` | `2025-03-20` |
| oocito_id=245426 | `femara_start` | `2025-03-20 00:00:00` | `2025-03-20` |
| oocito_id=245425 | `femara_start` | `2025-03-20 00:00:00` | `2025-03-20` |
| oocito_id=293047 | `fet_gravidez_bioquimica` | `nan` | `1` |
| oocito_id=293045 | `fet_gravidez_bioquimica` | `nan` | `1` |
| oocito_id=288336 | `fet_gravidez_bioquimica` | `nan` | `0` |
| oocito_id=288335 | `fet_gravidez_bioquimica` | `nan` | `0` |
| oocito_id=288334 | `fet_gravidez_bioquimica` | `nan` | `0` |
| oocito_id=288333 | `fet_gravidez_bioquimica` | `nan` | `0` |
| oocito_id=288332 | `fet_gravidez_bioquimica` | `nan` | `0` |
| oocito_id=288331 | `fet_gravidez_bioquimica` | `nan` | `0` |
| oocito_id=288330 | `fet_gravidez_bioquimica` | `nan` | `0` |
| oocito_id=288329 | `fet_gravidez_bioquimica` | `nan` | `0` |
| oocito_id=293047 | `fet_gravidez_clinica` | `nan` | `1` |
| oocito_id=293045 | `fet_gravidez_clinica` | `nan` | `1` |
| oocito_id=288372 | `fet_gravidez_clinica` | `0` | `nan` |
| oocito_id=288370 | `fet_gravidez_clinica` | `0` | `nan` |
| oocito_id=288336 | `fet_gravidez_clinica` | `nan` | `0.0` |
| oocito_id=288335 | `fet_gravidez_clinica` | `nan` | `0.0` |
| oocito_id=288334 | `fet_gravidez_clinica` | `nan` | `0.0` |
| oocito_id=288333 | `fet_gravidez_clinica` | `nan` | `0.0` |
| oocito_id=288332 | `fet_gravidez_clinica` | `nan` | `0.0` |
| oocito_id=288331 | `fet_gravidez_clinica` | `nan` | `0.0` |
| oocito_id=293047 | `fet_resultado` | `nan` | `EMBRYO TRANSFER` |
| oocito_id=293045 | `fet_resultado` | `nan` | `EMBRYO TRANSFER` |
| oocito_id=292102 | `fet_resultado` | `nan` | `NEGATIVO` |
| oocito_id=292101 | `fet_resultado` | `nan` | `NEGATIVO` |
| oocito_id=292100 | `fet_resultado` | `nan` | `NEGATIVO` |
| oocito_id=292099 | `fet_resultado` | `nan` | `NEGATIVO` |
| oocito_id=292098 | `fet_resultado` | `nan` | `NEGATIVO` |
| oocito_id=292097 | `fet_resultado` | `nan` | `NEGATIVO` |
| oocito_id=292096 | `fet_resultado` | `nan` | `NEGATIVO` |
| oocito_id=288372 | `fet_resultado` | `EMBRYO TRANSFER` | `POSITIVO` |
| oocito_id=293047 | `fet_tipo_resultado` | `nan` | `POSITIVO` |
| oocito_id=293045 | `fet_tipo_resultado` | `nan` | `POSITIVO` |
| oocito_id=292102 | `fet_tipo_resultado` | `nan` | `TRANSFERÊNCIA DE CONGELADOS` |
| oocito_id=292101 | `fet_tipo_resultado` | `nan` | `TRANSFERÊNCIA DE CONGELADOS` |
| oocito_id=292100 | `fet_tipo_resultado` | `nan` | `TRANSFERÊNCIA DE CONGELADOS` |
| oocito_id=292099 | `fet_tipo_resultado` | `nan` | `TRANSFERÊNCIA DE CONGELADOS` |
| oocito_id=292098 | `fet_tipo_resultado` | `nan` | `TRANSFERÊNCIA DE CONGELADOS` |
| oocito_id=292097 | `fet_tipo_resultado` | `nan` | `TRANSFERÊNCIA DE CONGELADOS` |
| oocito_id=292096 | `fet_tipo_resultado` | `nan` | `TRANSFERÊNCIA DE CONGELADOS` |
| oocito_id=288372 | `fet_tipo_resultado` | `NÃO ENGRAVIDOU` | `TRANSFERÊNCIA CONGELADOS` |
| oocito_id=313942 | `filgrastim_days` | `4.0` | `1.0` |
| oocito_id=313941 | `filgrastim_days` | `4.0` | `1.0` |
| oocito_id=313940 | `filgrastim_days` | `4.0` | `1.0` |
| oocito_id=313939 | `filgrastim_days` | `4.0` | `1.0` |
| oocito_id=313942 | `filgrastim_dose` | `1.0` | `0.25` |
| oocito_id=313941 | `filgrastim_dose` | `1.0` | `0.25` |
| oocito_id=313940 | `filgrastim_dose` | `1.0` | `0.25` |
| oocito_id=313939 | `filgrastim_dose` | `1.0` | `0.25` |
| oocito_id=313942 | `filgrastim_end` | `2026-05-21 00:00:00` | `2026-05-21` |
| oocito_id=313941 | `filgrastim_end` | `2026-05-21 00:00:00` | `2026-05-21` |
| oocito_id=313940 | `filgrastim_end` | `2026-05-21 00:00:00` | `2026-05-21` |
| oocito_id=313939 | `filgrastim_end` | `2026-05-21 00:00:00` | `2026-05-21` |
| oocito_id=313942 | `filgrastim_start` | `2026-05-21 00:00:00` | `2026-05-21` |
| oocito_id=313941 | `filgrastim_start` | `2026-05-21 00:00:00` | `2026-05-21` |
| oocito_id=313940 | `filgrastim_start` | `2026-05-21 00:00:00` | `2026-05-21` |
| oocito_id=313939 | `filgrastim_start` | `2026-05-21 00:00:00` | `2026-05-21` |
| oocito_id=294329 | `fostimon_days` | `3.0` | `1.0` |
| oocito_id=294328 | `fostimon_days` | `3.0` | `1.0` |
| oocito_id=294327 | `fostimon_days` | `3.0` | `1.0` |
| oocito_id=294326 | `fostimon_days` | `3.0` | `1.0` |
| oocito_id=294325 | `fostimon_days` | `6.0` | `1.0` |
| oocito_id=294329 | `fostimon_dose` | `450.0` | `150.0` |
| oocito_id=294328 | `fostimon_dose` | `450.0` | `150.0` |
| oocito_id=294327 | `fostimon_dose` | `450.0` | `150.0` |
| oocito_id=294326 | `fostimon_dose` | `450.0` | `150.0` |
| oocito_id=294325 | `fostimon_dose` | `900.0` | `150.0` |
| oocito_id=294329 | `fostimon_end` | `2026-01-14 00:00:00` | `2026-01-14` |
| oocito_id=294328 | `fostimon_end` | `2026-01-14 00:00:00` | `2026-01-14` |
| oocito_id=294327 | `fostimon_end` | `2026-01-14 00:00:00` | `2026-01-14` |
| oocito_id=294326 | `fostimon_end` | `2026-01-14 00:00:00` | `2026-01-14` |
| oocito_id=294325 | `fostimon_end` | `2026-01-14 00:00:00` | `2026-01-14` |
| oocito_id=191649 | `fostimon_end` | `2024-03-31 00:00:00` | `2024-03-31` |
| oocito_id=191648 | `fostimon_end` | `2024-03-31 00:00:00` | `2024-03-31` |
| oocito_id=191647 | `fostimon_end` | `2024-03-31 00:00:00` | `2024-03-31` |
| oocito_id=165696 | `fostimon_end` | `2023-09-23 00:00:00` | `2023-09-23` |
| oocito_id=165695 | `fostimon_end` | `2023-09-23 00:00:00` | `2023-09-23` |
| oocito_id=294329 | `fostimon_start` | `2026-01-14 00:00:00` | `2026-01-14` |
| oocito_id=294328 | `fostimon_start` | `2026-01-14 00:00:00` | `2026-01-14` |
| oocito_id=294327 | `fostimon_start` | `2026-01-14 00:00:00` | `2026-01-14` |
| oocito_id=294326 | `fostimon_start` | `2026-01-14 00:00:00` | `2026-01-14` |
| oocito_id=294325 | `fostimon_start` | `2026-01-14 00:00:00` | `2026-01-14` |
| oocito_id=191649 | `fostimon_start` | `2024-03-25 00:00:00` | `2024-03-25` |
| oocito_id=191648 | `fostimon_start` | `2024-03-25 00:00:00` | `2024-03-25` |
| oocito_id=191647 | `fostimon_start` | `2024-03-25 00:00:00` | `2024-03-25` |
| oocito_id=165696 | `fostimon_start` | `2023-09-19 00:00:00` | `2023-09-19` |
| oocito_id=165695 | `fostimon_start` | `2023-09-19 00:00:00` | `2023-09-19` |
| oocito_id=262449 | `gestational_age_at_delivery` | `nan` | `38.0` |
| oocito_id=251147 | `gestational_age_at_delivery` | `nan` | `22.0` |
| oocito_id=251141 | `gestational_age_at_delivery` | `nan` | `22.0` |
| oocito_id=246960 | `gestational_age_at_delivery` | `nan` | `38.0` |
| oocito_id=246959 | `gestational_age_at_delivery` | `nan` | `38.0` |
| oocito_id=246958 | `gestational_age_at_delivery` | `nan` | `38.0` |
| oocito_id=246957 | `gestational_age_at_delivery` | `nan` | `38.0` |
| oocito_id=246956 | `gestational_age_at_delivery` | `nan` | `38.0` |
| oocito_id=239383 | `gestational_age_at_delivery` | `nan` | `39.0` |
| oocito_id=209228 | `gestational_age_at_delivery` | `nan` | `40.0` |
| oocito_id=316842 | `gonal_days` | `24.0` | `10.0` |
| oocito_id=316841 | `gonal_days` | `24.0` | `10.0` |
| oocito_id=316840 | `gonal_days` | `24.0` | `10.0` |
| oocito_id=316839 | `gonal_days` | `24.0` | `10.0` |
| oocito_id=316838 | `gonal_days` | `24.0` | `10.0` |
| oocito_id=308958 | `gonal_days` | `13.0` | `8.0` |
| oocito_id=308957 | `gonal_days` | `13.0` | `8.0` |
| oocito_id=308956 | `gonal_days` | `13.0` | `8.0` |
| oocito_id=308955 | `gonal_days` | `13.0` | `8.0` |
| oocito_id=308954 | `gonal_days` | `13.0` | `8.0` |
| oocito_id=316842 | `gonal_dose` | `7200.0` | `3000.0` |
| oocito_id=316841 | `gonal_dose` | `7200.0` | `3000.0` |
| oocito_id=316840 | `gonal_dose` | `7200.0` | `3000.0` |
| oocito_id=316839 | `gonal_dose` | `7200.0` | `3000.0` |
| oocito_id=316838 | `gonal_dose` | `7200.0` | `3000.0` |
| oocito_id=308958 | `gonal_dose` | `1950.0` | `1200.0` |
| oocito_id=308957 | `gonal_dose` | `1950.0` | `1200.0` |
| oocito_id=308956 | `gonal_dose` | `1950.0` | `1200.0` |
| oocito_id=308955 | `gonal_dose` | `1950.0` | `1200.0` |
| oocito_id=308954 | `gonal_dose` | `1950.0` | `1200.0` |
| oocito_id=316842 | `gonal_end` | `2026-05-31 00:00:00` | `2026-05-31` |
| oocito_id=316841 | `gonal_end` | `2026-05-31 00:00:00` | `2026-05-31` |
| oocito_id=316840 | `gonal_end` | `2026-05-31 00:00:00` | `2026-05-31` |
| oocito_id=316839 | `gonal_end` | `2026-05-31 00:00:00` | `2026-05-31` |
| oocito_id=316838 | `gonal_end` | `2026-05-31 00:00:00` | `2026-05-31` |
| oocito_id=308958 | `gonal_end` | `2026-04-11 00:00:00` | `2026-04-11` |
| oocito_id=308957 | `gonal_end` | `2026-04-11 00:00:00` | `2026-04-11` |
| oocito_id=308956 | `gonal_end` | `2026-04-11 00:00:00` | `2026-04-11` |
| oocito_id=308955 | `gonal_end` | `2026-04-11 00:00:00` | `2026-04-11` |
| oocito_id=308954 | `gonal_end` | `2026-04-11 00:00:00` | `2026-04-11` |
| oocito_id=316842 | `gonal_start` | `2026-05-22 00:00:00` | `2026-05-22` |
| oocito_id=316841 | `gonal_start` | `2026-05-22 00:00:00` | `2026-05-22` |
| oocito_id=316840 | `gonal_start` | `2026-05-22 00:00:00` | `2026-05-22` |
| oocito_id=316839 | `gonal_start` | `2026-05-22 00:00:00` | `2026-05-22` |
| oocito_id=316838 | `gonal_start` | `2026-05-22 00:00:00` | `2026-05-22` |
| oocito_id=308958 | `gonal_start` | `2026-04-04 00:00:00` | `2026-04-04` |
| oocito_id=308957 | `gonal_start` | `2026-04-04 00:00:00` | `2026-04-04` |
| oocito_id=308956 | `gonal_start` | `2026-04-04 00:00:00` | `2026-04-04` |
| oocito_id=308955 | `gonal_start` | `2026-04-04 00:00:00` | `2026-04-04` |
| oocito_id=308954 | `gonal_start` | `2026-04-04 00:00:00` | `2026-04-04` |
| oocito_id=37428 | `gonal_unit` | `NÃO DEFINIDO` | `UI` |
| oocito_id=316948 | `gonapeptyl_days` | `2.0` | `1.0` |
| oocito_id=316942 | `gonapeptyl_days` | `2.0` | `1.0` |
| oocito_id=316941 | `gonapeptyl_days` | `2.0` | `1.0` |
| oocito_id=316940 | `gonapeptyl_days` | `2.0` | `1.0` |
| oocito_id=316939 | `gonapeptyl_days` | `2.0` | `1.0` |
| oocito_id=316893 | `gonapeptyl_days` | `3.0` | `1.0` |
| oocito_id=316892 | `gonapeptyl_days` | `3.0` | `1.0` |
| oocito_id=316891 | `gonapeptyl_days` | `3.0` | `1.0` |
| oocito_id=316849 | `gonapeptyl_days` | `3.0` | `1.0` |
| oocito_id=316848 | `gonapeptyl_days` | `3.0` | `1.0` |
| oocito_id=316948 | `gonapeptyl_dose` | `6.0` | `3.0` |
| oocito_id=316942 | `gonapeptyl_dose` | `6.0` | `3.0` |
| oocito_id=316941 | `gonapeptyl_dose` | `6.0` | `3.0` |
| oocito_id=316940 | `gonapeptyl_dose` | `6.0` | `3.0` |
| oocito_id=316939 | `gonapeptyl_dose` | `6.0` | `3.0` |
| oocito_id=316893 | `gonapeptyl_dose` | `9.0` | `3.0` |
| oocito_id=316892 | `gonapeptyl_dose` | `9.0` | `3.0` |
| oocito_id=316891 | `gonapeptyl_dose` | `9.0` | `3.0` |
| oocito_id=316849 | `gonapeptyl_dose` | `9.0` | `3.0` |
| oocito_id=316848 | `gonapeptyl_dose` | `9.0` | `3.0` |
| oocito_id=319427 | `gonapeptyl_end` | `2026-06-20 00:00:00` | `2026-06-20` |
| oocito_id=319426 | `gonapeptyl_end` | `2026-06-20 00:00:00` | `2026-06-20` |
| oocito_id=319425 | `gonapeptyl_end` | `2026-06-20 00:00:00` | `2026-06-20` |
| oocito_id=319424 | `gonapeptyl_end` | `2026-06-20 00:00:00` | `2026-06-20` |
| oocito_id=319423 | `gonapeptyl_end` | `2026-06-20 00:00:00` | `2026-06-20` |
| oocito_id=319422 | `gonapeptyl_end` | `2026-06-20 00:00:00` | `2026-06-20` |
| oocito_id=319421 | `gonapeptyl_end` | `2026-06-20 00:00:00` | `2026-06-20` |
| oocito_id=318548 | `gonapeptyl_end` | `2026-06-13 00:00:00` | `2026-06-13` |
| oocito_id=318547 | `gonapeptyl_end` | `2026-06-13 00:00:00` | `2026-06-13` |
| oocito_id=318546 | `gonapeptyl_end` | `2026-06-13 00:00:00` | `2026-06-13` |
| oocito_id=108493 | `gonapeptyl_interval` | `nan` | `24.0` |
| oocito_id=108492 | `gonapeptyl_interval` | `nan` | `24.0` |
| oocito_id=108491 | `gonapeptyl_interval` | `nan` | `24.0` |
| oocito_id=108490 | `gonapeptyl_interval` | `nan` | `24.0` |
| oocito_id=108489 | `gonapeptyl_interval` | `nan` | `24.0` |
| oocito_id=108488 | `gonapeptyl_interval` | `nan` | `24.0` |
| oocito_id=108487 | `gonapeptyl_interval` | `nan` | `24.0` |
| oocito_id=319427 | `gonapeptyl_start` | `2026-06-20 00:00:00` | `2026-06-20` |
| oocito_id=319426 | `gonapeptyl_start` | `2026-06-20 00:00:00` | `2026-06-20` |
| oocito_id=319425 | `gonapeptyl_start` | `2026-06-20 00:00:00` | `2026-06-20` |
| oocito_id=319424 | `gonapeptyl_start` | `2026-06-20 00:00:00` | `2026-06-20` |
| oocito_id=319423 | `gonapeptyl_start` | `2026-06-20 00:00:00` | `2026-06-20` |
| oocito_id=319422 | `gonapeptyl_start` | `2026-06-20 00:00:00` | `2026-06-20` |
| oocito_id=319421 | `gonapeptyl_start` | `2026-06-20 00:00:00` | `2026-06-20` |
| oocito_id=318548 | `gonapeptyl_start` | `2026-06-13 00:00:00` | `2026-06-13` |
| oocito_id=318547 | `gonapeptyl_start` | `2026-06-13 00:00:00` | `2026-06-13` |
| oocito_id=318546 | `gonapeptyl_start` | `2026-06-13 00:00:00` | `2026-06-13` |
| oocito_id=224810 | `gonapeptyl_unit` | `NÃO DEFINIDO` | `mg` |
| oocito_id=224809 | `gonapeptyl_unit` | `NÃO DEFINIDO` | `mg` |
| oocito_id=224808 | `gonapeptyl_unit` | `NÃO DEFINIDO` | `mg` |
| oocito_id=224807 | `gonapeptyl_unit` | `NÃO DEFINIDO` | `mg` |
| oocito_id=110829 | `gonapeptyl_unit` | `NÃO DEFINIDO` | `seringa` |
| oocito_id=110828 | `gonapeptyl_unit` | `NÃO DEFINIDO` | `seringa` |
| oocito_id=110827 | `gonapeptyl_unit` | `NÃO DEFINIDO` | `seringa` |
| oocito_id=110826 | `gonapeptyl_unit` | `NÃO DEFINIDO` | `seringa` |
| oocito_id=110825 | `gonapeptyl_unit` | `NÃO DEFINIDO` | `seringa` |
| oocito_id=110824 | `gonapeptyl_unit` | `NÃO DEFINIDO` | `seringa` |
| oocito_id=319427 | `has_biopsy` | `False` | `True` |
| oocito_id=319426 | `has_biopsy` | `False` | `True` |
| oocito_id=319425 | `has_biopsy` | `False` | `True` |
| oocito_id=319424 | `has_biopsy` | `False` | `True` |
| oocito_id=319423 | `has_biopsy` | `False` | `True` |
| oocito_id=319422 | `has_biopsy` | `False` | `True` |
| oocito_id=319421 | `has_biopsy` | `False` | `True` |
| oocito_id=318548 | `has_biopsy` | `False` | `True` |
| oocito_id=318547 | `has_biopsy` | `False` | `True` |
| oocito_id=318546 | `has_biopsy` | `False` | `True` |
| oocito_id=292102 | `has_transfer_logging_gap` | `False` | `True` |
| oocito_id=292101 | `has_transfer_logging_gap` | `False` | `True` |
| oocito_id=292100 | `has_transfer_logging_gap` | `False` | `True` |
| oocito_id=292099 | `has_transfer_logging_gap` | `False` | `True` |
| oocito_id=292098 | `has_transfer_logging_gap` | `False` | `True` |
| oocito_id=292097 | `has_transfer_logging_gap` | `False` | `True` |
| oocito_id=292096 | `has_transfer_logging_gap` | `False` | `True` |
| oocito_id=291521 | `has_transfer_logging_gap` | `False` | `True` |
| oocito_id=291520 | `has_transfer_logging_gap` | `False` | `True` |
| oocito_id=291519 | `has_transfer_logging_gap` | `False` | `True` |
| oocito_id=313434 | `has_valid_outcome` | `False` | `True` |
| oocito_id=313433 | `has_valid_outcome` | `False` | `True` |
| oocito_id=313432 | `has_valid_outcome` | `False` | `True` |
| oocito_id=313431 | `has_valid_outcome` | `False` | `True` |
| oocito_id=313430 | `has_valid_outcome` | `False` | `True` |
| oocito_id=313429 | `has_valid_outcome` | `False` | `True` |
| oocito_id=313428 | `has_valid_outcome` | `False` | `True` |
| oocito_id=313427 | `has_valid_outcome` | `False` | `True` |
| oocito_id=313426 | `has_valid_outcome` | `False` | `True` |
| oocito_id=313425 | `has_valid_outcome` | `False` | `True` |
| oocito_id=291216 | `incubadora_padronizada` | `Embryoscope` | `K-SYSTEM` |
| oocito_id=291215 | `incubadora_padronizada` | `Embryoscope` | `K-SYSTEM` |
| oocito_id=291214 | `incubadora_padronizada` | `Embryoscope` | `K-SYSTEM` |
| oocito_id=291213 | `incubadora_padronizada` | `Embryoscope` | `K-SYSTEM` |
| oocito_id=291203 | `incubadora_padronizada` | `Embryoscope` | `nan` |
| oocito_id=291202 | `incubadora_padronizada` | `Embryoscope` | `nan` |
| oocito_id=291201 | `incubadora_padronizada` | `Embryoscope` | `nan` |
| oocito_id=291200 | `incubadora_padronizada` | `Embryoscope` | `nan` |
| oocito_id=290550 | `incubadora_padronizada` | `Embryoscope` | `nan` |
| oocito_id=290549 | `incubadora_padronizada` | `Embryoscope` | `nan` |
| oocito_id=308958 | `indux_days` | `15.0` | `10.0` |
| oocito_id=308957 | `indux_days` | `15.0` | `10.0` |
| oocito_id=308956 | `indux_days` | `15.0` | `10.0` |
| oocito_id=308955 | `indux_days` | `15.0` | `10.0` |
| oocito_id=308954 | `indux_days` | `15.0` | `10.0` |
| oocito_id=295603 | `indux_days` | `10.0` | `5.0` |
| oocito_id=295602 | `indux_days` | `10.0` | `5.0` |
| oocito_id=295601 | `indux_days` | `10.0` | `5.0` |
| oocito_id=295600 | `indux_days` | `10.0` | `5.0` |
| oocito_id=295599 | `indux_days` | `10.0` | `5.0` |
| oocito_id=308958 | `indux_dose` | `1500.0` | `1000.0` |
| oocito_id=308957 | `indux_dose` | `1500.0` | `1000.0` |
| oocito_id=308956 | `indux_dose` | `1500.0` | `1000.0` |
| oocito_id=308955 | `indux_dose` | `1500.0` | `1000.0` |
| oocito_id=308954 | `indux_dose` | `1500.0` | `1000.0` |
| oocito_id=295603 | `indux_dose` | `1000.0` | `500.0` |
| oocito_id=295602 | `indux_dose` | `1000.0` | `500.0` |
| oocito_id=295601 | `indux_dose` | `1000.0` | `500.0` |
| oocito_id=295600 | `indux_dose` | `1000.0` | `500.0` |
| oocito_id=295599 | `indux_dose` | `1000.0` | `500.0` |
| oocito_id=308958 | `indux_end` | `2026-04-13 00:00:00` | `2026-04-13` |
| oocito_id=308957 | `indux_end` | `2026-04-13 00:00:00` | `2026-04-13` |
| oocito_id=308956 | `indux_end` | `2026-04-13 00:00:00` | `2026-04-13` |
| oocito_id=308955 | `indux_end` | `2026-04-13 00:00:00` | `2026-04-13` |
| oocito_id=308954 | `indux_end` | `2026-04-13 00:00:00` | `2026-04-13` |
| oocito_id=295603 | `indux_end` | `2026-01-13 00:00:00` | `2026-01-13` |
| oocito_id=295602 | `indux_end` | `2026-01-13 00:00:00` | `2026-01-13` |
| oocito_id=295601 | `indux_end` | `2026-01-13 00:00:00` | `2026-01-13` |
| oocito_id=295600 | `indux_end` | `2026-01-13 00:00:00` | `2026-01-13` |
| oocito_id=295599 | `indux_end` | `2026-01-13 00:00:00` | `2026-01-13` |
| oocito_id=308958 | `indux_start` | `2026-04-04 00:00:00` | `2026-04-04` |
| oocito_id=308957 | `indux_start` | `2026-04-04 00:00:00` | `2026-04-04` |
| oocito_id=308956 | `indux_start` | `2026-04-04 00:00:00` | `2026-04-04` |
| oocito_id=308955 | `indux_start` | `2026-04-04 00:00:00` | `2026-04-04` |
| oocito_id=308954 | `indux_start` | `2026-04-04 00:00:00` | `2026-04-04` |
| oocito_id=295603 | `indux_start` | `2026-01-09 00:00:00` | `2026-01-09` |
| oocito_id=295602 | `indux_start` | `2026-01-09 00:00:00` | `2026-01-09` |
| oocito_id=295601 | `indux_start` | `2026-01-09 00:00:00` | `2026-01-09` |
| oocito_id=295600 | `indux_start` | `2026-01-09 00:00:00` | `2026-01-09` |
| oocito_id=295599 | `indux_start` | `2026-01-09 00:00:00` | `2026-01-09` |
| oocito_id=319425 | `is_transferred_combined` | `<NA>` | `False` |
| oocito_id=319423 | `is_transferred_combined` | `<NA>` | `False` |
| oocito_id=318541 | `is_transferred_combined` | `<NA>` | `False` |
| oocito_id=318540 | `is_transferred_combined` | `<NA>` | `False` |
| oocito_id=318539 | `is_transferred_combined` | `<NA>` | `False` |
| oocito_id=318538 | `is_transferred_combined` | `<NA>` | `False` |
| oocito_id=318537 | `is_transferred_combined` | `<NA>` | `False` |
| oocito_id=317715 | `is_transferred_combined` | `<NA>` | `False` |
| oocito_id=317711 | `is_transferred_combined` | `<NA>` | `False` |
| oocito_id=317710 | `is_transferred_combined` | `<NA>` | `False` |
| oocito_id=291203 | `join_step` | `1` | `nan` |
| oocito_id=291202 | `join_step` | `1` | `nan` |
| oocito_id=291201 | `join_step` | `1` | `nan` |
| oocito_id=291200 | `join_step` | `1` | `nan` |
| oocito_id=290550 | `join_step` | `1` | `nan` |
| oocito_id=290549 | `join_step` | `1` | `nan` |
| oocito_id=290548 | `join_step` | `1` | `nan` |
| oocito_id=290547 | `join_step` | `1` | `nan` |
| oocito_id=290546 | `join_step` | `1` | `nan` |
| oocito_id=274324 | `join_step` | `1` | `nan` |
| oocito_id=205253 | `lactobacillus_end` | `2024-06-19 00:00:00` | `2024-06-19` |
| oocito_id=205252 | `lactobacillus_end` | `2024-06-19 00:00:00` | `2024-06-19` |
| oocito_id=205251 | `lactobacillus_end` | `2024-06-19 00:00:00` | `2024-06-19` |
| oocito_id=205250 | `lactobacillus_end` | `2024-06-19 00:00:00` | `2024-06-19` |
| oocito_id=205249 | `lactobacillus_end` | `2024-06-19 00:00:00` | `2024-06-19` |
| oocito_id=205248 | `lactobacillus_end` | `2024-06-19 00:00:00` | `2024-06-19` |
| oocito_id=205247 | `lactobacillus_end` | `2024-06-19 00:00:00` | `2024-06-19` |
| oocito_id=205246 | `lactobacillus_end` | `2024-06-19 00:00:00` | `2024-06-19` |
| oocito_id=205245 | `lactobacillus_end` | `2024-06-19 00:00:00` | `2024-06-19` |
| oocito_id=205244 | `lactobacillus_end` | `2024-06-19 00:00:00` | `2024-06-19` |
| oocito_id=205253 | `lactobacillus_start` | `2024-06-19 00:00:00` | `2024-06-19` |
| oocito_id=205252 | `lactobacillus_start` | `2024-06-19 00:00:00` | `2024-06-19` |
| oocito_id=205251 | `lactobacillus_start` | `2024-06-19 00:00:00` | `2024-06-19` |
| oocito_id=205250 | `lactobacillus_start` | `2024-06-19 00:00:00` | `2024-06-19` |
| oocito_id=205249 | `lactobacillus_start` | `2024-06-19 00:00:00` | `2024-06-19` |
| oocito_id=205248 | `lactobacillus_start` | `2024-06-19 00:00:00` | `2024-06-19` |
| oocito_id=205247 | `lactobacillus_start` | `2024-06-19 00:00:00` | `2024-06-19` |
| oocito_id=205246 | `lactobacillus_start` | `2024-06-19 00:00:00` | `2024-06-19` |
| oocito_id=205245 | `lactobacillus_start` | `2024-06-19 00:00:00` | `2024-06-19` |
| oocito_id=205244 | `lactobacillus_start` | `2024-06-19 00:00:00` | `2024-06-19` |
| oocito_id=315822 | `lectrum_days` | `4.0` | `1.0` |
| oocito_id=315821 | `lectrum_days` | `4.0` | `1.0` |
| oocito_id=315820 | `lectrum_days` | `4.0` | `1.0` |
| oocito_id=315819 | `lectrum_days` | `4.0` | `1.0` |
| oocito_id=315818 | `lectrum_days` | `4.0` | `1.0` |
| oocito_id=312268 | `lectrum_days` | `4.0` | `1.0` |
| oocito_id=312267 | `lectrum_days` | `4.0` | `1.0` |
| oocito_id=312266 | `lectrum_days` | `4.0` | `1.0` |
| oocito_id=312265 | `lectrum_days` | `4.0` | `1.0` |
| oocito_id=312264 | `lectrum_days` | `4.0` | `1.0` |
| oocito_id=315822 | `lectrum_dose` | `15.0` | `3.75` |
| oocito_id=315821 | `lectrum_dose` | `15.0` | `3.75` |
| oocito_id=315820 | `lectrum_dose` | `15.0` | `3.75` |
| oocito_id=315819 | `lectrum_dose` | `15.0` | `3.75` |
| oocito_id=315818 | `lectrum_dose` | `15.0` | `3.75` |
| oocito_id=312268 | `lectrum_dose` | `4.0` | `1.0` |
| oocito_id=312267 | `lectrum_dose` | `4.0` | `1.0` |
| oocito_id=312266 | `lectrum_dose` | `4.0` | `1.0` |
| oocito_id=312265 | `lectrum_dose` | `4.0` | `1.0` |
| oocito_id=312264 | `lectrum_dose` | `4.0` | `1.0` |
| oocito_id=315822 | `lectrum_end` | `2026-04-30 00:00:00` | `2026-04-30` |
| oocito_id=315821 | `lectrum_end` | `2026-04-30 00:00:00` | `2026-04-30` |
| oocito_id=315820 | `lectrum_end` | `2026-04-30 00:00:00` | `2026-04-30` |
| oocito_id=315819 | `lectrum_end` | `2026-04-30 00:00:00` | `2026-04-30` |
| oocito_id=315818 | `lectrum_end` | `2026-04-30 00:00:00` | `2026-04-30` |
| oocito_id=312268 | `lectrum_end` | `2026-04-06 00:00:00` | `2026-04-06` |
| oocito_id=312267 | `lectrum_end` | `2026-04-06 00:00:00` | `2026-04-06` |
| oocito_id=312266 | `lectrum_end` | `2026-04-06 00:00:00` | `2026-04-06` |
| oocito_id=312265 | `lectrum_end` | `2026-04-06 00:00:00` | `2026-04-06` |
| oocito_id=312264 | `lectrum_end` | `2026-04-06 00:00:00` | `2026-04-06` |
| oocito_id=108493 | `lectrum_interval` | `24.0` | `nan` |
| oocito_id=108492 | `lectrum_interval` | `24.0` | `nan` |
| oocito_id=108491 | `lectrum_interval` | `24.0` | `nan` |
| oocito_id=108490 | `lectrum_interval` | `24.0` | `nan` |
| oocito_id=108489 | `lectrum_interval` | `24.0` | `nan` |
| oocito_id=108488 | `lectrum_interval` | `24.0` | `nan` |
| oocito_id=108487 | `lectrum_interval` | `24.0` | `nan` |
| oocito_id=315822 | `lectrum_start` | `2026-04-30 00:00:00` | `2026-04-30` |
| oocito_id=315821 | `lectrum_start` | `2026-04-30 00:00:00` | `2026-04-30` |
| oocito_id=315820 | `lectrum_start` | `2026-04-30 00:00:00` | `2026-04-30` |
| oocito_id=315819 | `lectrum_start` | `2026-04-30 00:00:00` | `2026-04-30` |
| oocito_id=315818 | `lectrum_start` | `2026-04-30 00:00:00` | `2026-04-30` |
| oocito_id=312268 | `lectrum_start` | `2026-04-06 00:00:00` | `2026-04-06` |
| oocito_id=312267 | `lectrum_start` | `2026-04-06 00:00:00` | `2026-04-06` |
| oocito_id=312266 | `lectrum_start` | `2026-04-06 00:00:00` | `2026-04-06` |
| oocito_id=312265 | `lectrum_start` | `2026-04-06 00:00:00` | `2026-04-06` |
| oocito_id=312264 | `lectrum_start` | `2026-04-06 00:00:00` | `2026-04-06` |
| oocito_id=108493 | `lectrum_unit` | `ampola` | `nan` |
| oocito_id=108492 | `lectrum_unit` | `ampola` | `nan` |
| oocito_id=108491 | `lectrum_unit` | `ampola` | `nan` |
| oocito_id=108490 | `lectrum_unit` | `ampola` | `nan` |
| oocito_id=108489 | `lectrum_unit` | `ampola` | `nan` |
| oocito_id=108488 | `lectrum_unit` | `ampola` | `nan` |
| oocito_id=108487 | `lectrum_unit` | `ampola` | `nan` |
| oocito_id=316942 | `letrozol_days` | `21.0` | `8.0` |
| oocito_id=316941 | `letrozol_days` | `21.0` | `8.0` |
| oocito_id=316940 | `letrozol_days` | `21.0` | `8.0` |
| oocito_id=316939 | `letrozol_days` | `21.0` | `8.0` |
| oocito_id=315272 | `letrozol_days` | `4.0` | `2.0` |
| oocito_id=315271 | `letrozol_days` | `4.0` | `2.0` |
| oocito_id=315270 | `letrozol_days` | `4.0` | `2.0` |
| oocito_id=315269 | `letrozol_days` | `4.0` | `2.0` |
| oocito_id=315268 | `letrozol_days` | `4.0` | `2.0` |
| oocito_id=315267 | `letrozol_days` | `4.0` | `2.0` |
| oocito_id=316942 | `letrozol_dose` | `42.0` | `16.0` |
| oocito_id=316941 | `letrozol_dose` | `42.0` | `16.0` |
| oocito_id=316940 | `letrozol_dose` | `42.0` | `16.0` |
| oocito_id=316939 | `letrozol_dose` | `42.0` | `16.0` |
| oocito_id=315272 | `letrozol_dose` | `8.0` | `4.0` |
| oocito_id=315271 | `letrozol_dose` | `8.0` | `4.0` |
| oocito_id=315270 | `letrozol_dose` | `8.0` | `4.0` |
| oocito_id=315269 | `letrozol_dose` | `8.0` | `4.0` |
| oocito_id=315268 | `letrozol_dose` | `8.0` | `4.0` |
| oocito_id=315267 | `letrozol_dose` | `8.0` | `4.0` |
| oocito_id=316942 | `letrozol_end` | `2026-05-30 00:00:00` | `2026-05-30` |
| oocito_id=316941 | `letrozol_end` | `2026-05-30 00:00:00` | `2026-05-30` |
| oocito_id=316940 | `letrozol_end` | `2026-05-30 00:00:00` | `2026-05-30` |
| oocito_id=316939 | `letrozol_end` | `2026-05-30 00:00:00` | `2026-05-30` |
| oocito_id=315272 | `letrozol_end` | `2026-05-24 00:00:00` | `2026-05-24` |
| oocito_id=315271 | `letrozol_end` | `2026-05-24 00:00:00` | `2026-05-24` |
| oocito_id=315270 | `letrozol_end` | `2026-05-24 00:00:00` | `2026-05-24` |
| oocito_id=315269 | `letrozol_end` | `2026-05-24 00:00:00` | `2026-05-24` |
| oocito_id=315268 | `letrozol_end` | `2026-05-24 00:00:00` | `2026-05-24` |
| oocito_id=315267 | `letrozol_end` | `2026-05-24 00:00:00` | `2026-05-24` |
| oocito_id=284396 | `letrozol_interval` | `12.0` | `24.0` |
| oocito_id=284395 | `letrozol_interval` | `12.0` | `24.0` |
| oocito_id=284394 | `letrozol_interval` | `12.0` | `24.0` |
| oocito_id=284393 | `letrozol_interval` | `12.0` | `24.0` |
| oocito_id=284392 | `letrozol_interval` | `12.0` | `24.0` |
| oocito_id=284391 | `letrozol_interval` | `12.0` | `24.0` |
| oocito_id=284390 | `letrozol_interval` | `12.0` | `24.0` |
| oocito_id=284389 | `letrozol_interval` | `12.0` | `24.0` |
| oocito_id=284388 | `letrozol_interval` | `12.0` | `24.0` |
| oocito_id=284387 | `letrozol_interval` | `12.0` | `24.0` |
| oocito_id=316942 | `letrozol_start` | `2026-05-23 00:00:00` | `2026-05-23` |
| oocito_id=316941 | `letrozol_start` | `2026-05-23 00:00:00` | `2026-05-23` |
| oocito_id=316940 | `letrozol_start` | `2026-05-23 00:00:00` | `2026-05-23` |
| oocito_id=316939 | `letrozol_start` | `2026-05-23 00:00:00` | `2026-05-23` |
| oocito_id=315272 | `letrozol_start` | `2026-05-23 00:00:00` | `2026-05-23` |
| oocito_id=315271 | `letrozol_start` | `2026-05-23 00:00:00` | `2026-05-23` |
| oocito_id=315270 | `letrozol_start` | `2026-05-23 00:00:00` | `2026-05-23` |
| oocito_id=315269 | `letrozol_start` | `2026-05-23 00:00:00` | `2026-05-23` |
| oocito_id=315268 | `letrozol_start` | `2026-05-23 00:00:00` | `2026-05-23` |
| oocito_id=315267 | `letrozol_start` | `2026-05-23 00:00:00` | `2026-05-23` |
| oocito_id=279765 | `letrozol_unit` | `comp` | `mg` |
| oocito_id=279764 | `letrozol_unit` | `comp` | `mg` |
| oocito_id=279763 | `letrozol_unit` | `comp` | `mg` |
| oocito_id=279762 | `letrozol_unit` | `comp` | `mg` |
| oocito_id=279761 | `letrozol_unit` | `comp` | `mg` |
| oocito_id=279760 | `letrozol_unit` | `comp` | `mg` |
| oocito_id=279759 | `letrozol_unit` | `comp` | `mg` |
| oocito_id=279758 | `letrozol_unit` | `comp` | `mg` |
| oocito_id=279757 | `letrozol_unit` | `comp` | `mg` |
| oocito_id=279756 | `letrozol_unit` | `comp` | `mg` |
| oocito_id=313942 | `lipofundin_days` | `4.0` | `1.0` |
| oocito_id=313941 | `lipofundin_days` | `4.0` | `1.0` |
| oocito_id=313940 | `lipofundin_days` | `4.0` | `1.0` |
| oocito_id=313939 | `lipofundin_days` | `4.0` | `1.0` |
| oocito_id=296529 | `lipofundin_days` | `2.0` | `1.0` |
| oocito_id=296528 | `lipofundin_days` | `2.0` | `1.0` |
| oocito_id=296527 | `lipofundin_days` | `2.0` | `1.0` |
| oocito_id=296526 | `lipofundin_days` | `2.0` | `1.0` |
| oocito_id=296525 | `lipofundin_days` | `2.0` | `1.0` |
| oocito_id=313942 | `lipofundin_dose` | `400.0` | `100.0` |
| oocito_id=313941 | `lipofundin_dose` | `400.0` | `100.0` |
| oocito_id=313940 | `lipofundin_dose` | `400.0` | `100.0` |
| oocito_id=313939 | `lipofundin_dose` | `400.0` | `100.0` |
| oocito_id=296529 | `lipofundin_dose` | `200.0` | `100.0` |
| oocito_id=296528 | `lipofundin_dose` | `200.0` | `100.0` |
| oocito_id=296527 | `lipofundin_dose` | `200.0` | `100.0` |
| oocito_id=296526 | `lipofundin_dose` | `200.0` | `100.0` |
| oocito_id=296525 | `lipofundin_dose` | `200.0` | `100.0` |
| oocito_id=313942 | `lipofundin_end` | `2026-05-21 00:00:00` | `2026-05-21` |
| oocito_id=313941 | `lipofundin_end` | `2026-05-21 00:00:00` | `2026-05-21` |
| oocito_id=313940 | `lipofundin_end` | `2026-05-21 00:00:00` | `2026-05-21` |
| oocito_id=313939 | `lipofundin_end` | `2026-05-21 00:00:00` | `2026-05-21` |
| oocito_id=296529 | `lipofundin_end` | `2026-01-30 00:00:00` | `2026-01-30` |
| oocito_id=296528 | `lipofundin_end` | `2026-01-30 00:00:00` | `2026-01-30` |
| oocito_id=296527 | `lipofundin_end` | `2026-01-30 00:00:00` | `2026-01-30` |
| oocito_id=296526 | `lipofundin_end` | `2026-01-30 00:00:00` | `2026-01-30` |
| oocito_id=296525 | `lipofundin_end` | `2026-01-30 00:00:00` | `2026-01-30` |
| oocito_id=254061 | `lipofundin_end` | `2025-05-16 00:00:00` | `2025-05-16` |
| oocito_id=313942 | `lipofundin_start` | `2026-05-21 00:00:00` | `2026-05-21` |
| oocito_id=313941 | `lipofundin_start` | `2026-05-21 00:00:00` | `2026-05-21` |
| oocito_id=313940 | `lipofundin_start` | `2026-05-21 00:00:00` | `2026-05-21` |
| oocito_id=313939 | `lipofundin_start` | `2026-05-21 00:00:00` | `2026-05-21` |
| oocito_id=296529 | `lipofundin_start` | `2026-01-30 00:00:00` | `2026-01-30` |
| oocito_id=296528 | `lipofundin_start` | `2026-01-30 00:00:00` | `2026-01-30` |
| oocito_id=296527 | `lipofundin_start` | `2026-01-30 00:00:00` | `2026-01-30` |
| oocito_id=296526 | `lipofundin_start` | `2026-01-30 00:00:00` | `2026-01-30` |
| oocito_id=296525 | `lipofundin_start` | `2026-01-30 00:00:00` | `2026-01-30` |
| oocito_id=254061 | `lipofundin_start` | `2025-05-16 00:00:00` | `2025-05-16` |
| oocito_id=293047 | `matched_planilha_prontuario` | `753579` | `179637.0` |
| oocito_id=293045 | `matched_planilha_prontuario` | `753579` | `179637.0` |
| oocito_id=292286 | `matched_planilha_prontuario` | `882118` | `175339.0` |
| oocito_id=292285 | `matched_planilha_prontuario` | `882118` | `175339.0` |
| oocito_id=292284 | `matched_planilha_prontuario` | `882118` | `175339.0` |
| oocito_id=292283 | `matched_planilha_prontuario` | `882118` | `175339.0` |
| oocito_id=292282 | `matched_planilha_prontuario` | `882118` | `175339.0` |
| oocito_id=292281 | `matched_planilha_prontuario` | `882118` | `175339.0` |
| oocito_id=292280 | `matched_planilha_prontuario` | `882118` | `175339.0` |
| oocito_id=292279 | `matched_planilha_prontuario` | `882118` | `175339.0` |
| oocito_id=293047 | `matched_planilha_transfer_date` | `NaT` | `2024-10-23` |
| oocito_id=293045 | `matched_planilha_transfer_date` | `NaT` | `2024-10-23` |
| oocito_id=288372 | `matched_planilha_transfer_date` | `2025-12-19 00:00:00` | `2021-11-09` |
| oocito_id=288370 | `matched_planilha_transfer_date` | `2025-12-19 00:00:00` | `2021-11-09` |
| oocito_id=288336 | `matched_planilha_transfer_date` | `NaT` | `2025-02-10` |
| oocito_id=288335 | `matched_planilha_transfer_date` | `NaT` | `2025-02-10` |
| oocito_id=288334 | `matched_planilha_transfer_date` | `NaT` | `2025-02-10` |
| oocito_id=288333 | `matched_planilha_transfer_date` | `NaT` | `2025-02-10` |
| oocito_id=288332 | `matched_planilha_transfer_date` | `NaT` | `2025-02-10` |
| oocito_id=288331 | `matched_planilha_transfer_date` | `NaT` | `2025-02-10` |
| oocito_id=293047 | `matched_src_row_id` | `25388` | `15342.0` |
| oocito_id=293045 | `matched_src_row_id` | `25388` | `15342.0` |
| oocito_id=292286 | `matched_src_row_id` | `25387` | `14857.0` |
| oocito_id=292285 | `matched_src_row_id` | `25387` | `14857.0` |
| oocito_id=292284 | `matched_src_row_id` | `25387` | `14857.0` |
| oocito_id=292283 | `matched_src_row_id` | `25387` | `14857.0` |
| oocito_id=292282 | `matched_src_row_id` | `25387` | `14857.0` |
| oocito_id=292281 | `matched_src_row_id` | `25387` | `14857.0` |
| oocito_id=292280 | `matched_src_row_id` | `25387` | `14857.0` |
| oocito_id=292279 | `matched_src_row_id` | `25387` | `14857.0` |
| oocito_id=319427 | `menopur_days` | `18.0` | `9.0` |
| oocito_id=319426 | `menopur_days` | `18.0` | `9.0` |
| oocito_id=319425 | `menopur_days` | `18.0` | `9.0` |
| oocito_id=319424 | `menopur_days` | `18.0` | `9.0` |
| oocito_id=319423 | `menopur_days` | `18.0` | `9.0` |
| oocito_id=319422 | `menopur_days` | `18.0` | `9.0` |
| oocito_id=319421 | `menopur_days` | `18.0` | `9.0` |
| oocito_id=316942 | `menopur_days` | `4.0` | `2.0` |
| oocito_id=316941 | `menopur_days` | `4.0` | `2.0` |
| oocito_id=316940 | `menopur_days` | `4.0` | `2.0` |
| oocito_id=319427 | `menopur_dose` | `5400.0` | `2700.0` |
| oocito_id=319426 | `menopur_dose` | `5400.0` | `2700.0` |
| oocito_id=319425 | `menopur_dose` | `5400.0` | `2700.0` |
| oocito_id=319424 | `menopur_dose` | `5400.0` | `2700.0` |
| oocito_id=319423 | `menopur_dose` | `5400.0` | `2700.0` |
| oocito_id=319422 | `menopur_dose` | `5400.0` | `2700.0` |
| oocito_id=319421 | `menopur_dose` | `5400.0` | `2700.0` |
| oocito_id=316942 | `menopur_dose` | `1200.0` | `600.0` |
| oocito_id=316941 | `menopur_dose` | `1200.0` | `600.0` |
| oocito_id=316940 | `menopur_dose` | `1200.0` | `600.0` |
| oocito_id=319427 | `menopur_end` | `2026-06-18 00:00:00` | `2026-06-18` |
| oocito_id=319426 | `menopur_end` | `2026-06-18 00:00:00` | `2026-06-18` |
| oocito_id=319425 | `menopur_end` | `2026-06-18 00:00:00` | `2026-06-18` |
| oocito_id=319424 | `menopur_end` | `2026-06-18 00:00:00` | `2026-06-18` |
| oocito_id=319423 | `menopur_end` | `2026-06-18 00:00:00` | `2026-06-18` |
| oocito_id=319422 | `menopur_end` | `2026-06-18 00:00:00` | `2026-06-18` |
| oocito_id=319421 | `menopur_end` | `2026-06-18 00:00:00` | `2026-06-18` |
| oocito_id=317241 | `menopur_end` | `2026-06-02 00:00:00` | `2026-06-02` |
| oocito_id=317240 | `menopur_end` | `2026-06-02 00:00:00` | `2026-06-02` |
| oocito_id=317239 | `menopur_end` | `2026-06-02 00:00:00` | `2026-06-02` |
| oocito_id=221825 | `menopur_interval` | `24.0` | `48.0` |
| oocito_id=221824 | `menopur_interval` | `24.0` | `48.0` |
| oocito_id=108493 | `menopur_interval` | `nan` | `24.0` |
| oocito_id=108492 | `menopur_interval` | `nan` | `24.0` |
| oocito_id=108491 | `menopur_interval` | `nan` | `24.0` |
| oocito_id=108490 | `menopur_interval` | `nan` | `24.0` |
| oocito_id=108489 | `menopur_interval` | `nan` | `24.0` |
| oocito_id=108488 | `menopur_interval` | `nan` | `24.0` |
| oocito_id=108487 | `menopur_interval` | `nan` | `24.0` |
| oocito_id=319427 | `menopur_start` | `2026-06-10 00:00:00` | `2026-06-10` |
| oocito_id=319426 | `menopur_start` | `2026-06-10 00:00:00` | `2026-06-10` |
| oocito_id=319425 | `menopur_start` | `2026-06-10 00:00:00` | `2026-06-10` |
| oocito_id=319424 | `menopur_start` | `2026-06-10 00:00:00` | `2026-06-10` |
| oocito_id=319423 | `menopur_start` | `2026-06-10 00:00:00` | `2026-06-10` |
| oocito_id=319422 | `menopur_start` | `2026-06-10 00:00:00` | `2026-06-10` |
| oocito_id=319421 | `menopur_start` | `2026-06-10 00:00:00` | `2026-06-10` |
| oocito_id=317241 | `menopur_start` | `2026-05-27 00:00:00` | `2026-05-27` |
| oocito_id=317240 | `menopur_start` | `2026-05-27 00:00:00` | `2026-05-27` |
| oocito_id=317239 | `menopur_start` | `2026-05-27 00:00:00` | `2026-05-27` |
| oocito_id=258947 | `menopur_unit` | `NÃO DEFINIDO` | `UI` |
| oocito_id=258946 | `menopur_unit` | `NÃO DEFINIDO` | `UI` |
| oocito_id=258945 | `menopur_unit` | `NÃO DEFINIDO` | `UI` |
| oocito_id=258944 | `menopur_unit` | `NÃO DEFINIDO` | `UI` |
| oocito_id=258943 | `menopur_unit` | `NÃO DEFINIDO` | `UI` |
| oocito_id=258942 | `menopur_unit` | `NÃO DEFINIDO` | `UI` |
| oocito_id=258941 | `menopur_unit` | `NÃO DEFINIDO` | `UI` |
| oocito_id=258940 | `menopur_unit` | `NÃO DEFINIDO` | `UI` |
| oocito_id=239207 | `menopur_unit` | `UI` | `mg` |
| oocito_id=239206 | `menopur_unit` | `UI` | `mg` |
| oocito_id=262449 | `merged_numero_de_nascidos` | `<NA>` | `1.0` |
| oocito_id=259138 | `merged_numero_de_nascidos` | `<NA>` | `1.0` |
| oocito_id=251147 | `merged_numero_de_nascidos` | `<NA>` | `2.0` |
| oocito_id=251141 | `merged_numero_de_nascidos` | `<NA>` | `2.0` |
| oocito_id=246960 | `merged_numero_de_nascidos` | `<NA>` | `1.0` |
| oocito_id=246959 | `merged_numero_de_nascidos` | `<NA>` | `1.0` |
| oocito_id=246958 | `merged_numero_de_nascidos` | `<NA>` | `1.0` |
| oocito_id=246957 | `merged_numero_de_nascidos` | `<NA>` | `1.0` |
| oocito_id=246956 | `merged_numero_de_nascidos` | `<NA>` | `1.0` |
| oocito_id=239383 | `merged_numero_de_nascidos` | `<NA>` | `1.0` |
| oocito_id=316948 | `merional_days` | `2.0` | `1.0` |
| oocito_id=316942 | `merional_days` | `2.0` | `1.0` |
| oocito_id=316941 | `merional_days` | `2.0` | `1.0` |
| oocito_id=316940 | `merional_days` | `2.0` | `1.0` |
| oocito_id=316939 | `merional_days` | `2.0` | `1.0` |
| oocito_id=315346 | `merional_days` | `8.0` | `3.0` |
| oocito_id=315345 | `merional_days` | `8.0` | `3.0` |
| oocito_id=315344 | `merional_days` | `8.0` | `3.0` |
| oocito_id=315343 | `merional_days` | `8.0` | `3.0` |
| oocito_id=313201 | `merional_days` | `2.0` | `1.0` |
| oocito_id=316948 | `merional_dose` | `300.0` | `150.0` |
| oocito_id=316942 | `merional_dose` | `600.0` | `300.0` |
| oocito_id=316941 | `merional_dose` | `600.0` | `300.0` |
| oocito_id=316940 | `merional_dose` | `600.0` | `300.0` |
| oocito_id=316939 | `merional_dose` | `600.0` | `300.0` |
| oocito_id=315346 | `merional_dose` | `1800.0` | `675.0` |
| oocito_id=315345 | `merional_dose` | `1800.0` | `675.0` |
| oocito_id=315344 | `merional_dose` | `1800.0` | `675.0` |
| oocito_id=315343 | `merional_dose` | `1800.0` | `675.0` |
| oocito_id=313201 | `merional_dose` | `150.0` | `75.0` |
| oocito_id=317716 | `merional_end` | `2026-06-08 00:00:00` | `2026-06-08` |
| oocito_id=317715 | `merional_end` | `2026-06-08 00:00:00` | `2026-06-08` |
| oocito_id=317714 | `merional_end` | `2026-06-08 00:00:00` | `2026-06-08` |
| oocito_id=317713 | `merional_end` | `2026-06-08 00:00:00` | `2026-06-08` |
| oocito_id=317712 | `merional_end` | `2026-06-08 00:00:00` | `2026-06-08` |
| oocito_id=317711 | `merional_end` | `2026-06-08 00:00:00` | `2026-06-08` |
| oocito_id=317710 | `merional_end` | `2026-06-08 00:00:00` | `2026-06-08` |
| oocito_id=317709 | `merional_end` | `2026-06-08 00:00:00` | `2026-06-08` |
| oocito_id=317241 | `merional_end` | `2026-06-03 00:00:00` | `2026-06-03` |
| oocito_id=317240 | `merional_end` | `2026-06-03 00:00:00` | `2026-06-03` |
| oocito_id=272654 | `merional_interval` | `24.0` | `nan` |
| oocito_id=272653 | `merional_interval` | `24.0` | `nan` |
| oocito_id=272652 | `merional_interval` | `24.0` | `nan` |
| oocito_id=272651 | `merional_interval` | `24.0` | `nan` |
| oocito_id=272650 | `merional_interval` | `24.0` | `nan` |
| oocito_id=272649 | `merional_interval` | `24.0` | `nan` |
| oocito_id=317716 | `merional_start` | `2026-06-08 00:00:00` | `2026-06-08` |
| oocito_id=317715 | `merional_start` | `2026-06-08 00:00:00` | `2026-06-08` |
| oocito_id=317714 | `merional_start` | `2026-06-08 00:00:00` | `2026-06-08` |
| oocito_id=317713 | `merional_start` | `2026-06-08 00:00:00` | `2026-06-08` |
| oocito_id=317712 | `merional_start` | `2026-06-08 00:00:00` | `2026-06-08` |
| oocito_id=317711 | `merional_start` | `2026-06-08 00:00:00` | `2026-06-08` |
| oocito_id=317710 | `merional_start` | `2026-06-08 00:00:00` | `2026-06-08` |
| oocito_id=317709 | `merional_start` | `2026-06-08 00:00:00` | `2026-06-08` |
| oocito_id=317241 | `merional_start` | `2026-06-02 00:00:00` | `2026-06-02` |
| oocito_id=317240 | `merional_start` | `2026-06-02 00:00:00` | `2026-06-02` |
| oocito_id=272654 | `merional_unit` | `UI` | `nan` |
| oocito_id=272653 | `merional_unit` | `UI` | `nan` |
| oocito_id=272652 | `merional_unit` | `UI` | `nan` |
| oocito_id=272651 | `merional_unit` | `UI` | `nan` |
| oocito_id=272650 | `merional_unit` | `UI` | `nan` |
| oocito_id=272649 | `merional_unit` | `UI` | `nan` |
| oocito_id=282676 | `meticorten_days` | `49.0` | `18.0` |
| oocito_id=282675 | `meticorten_days` | `49.0` | `18.0` |
| oocito_id=282674 | `meticorten_days` | `49.0` | `18.0` |
| oocito_id=282673 | `meticorten_days` | `49.0` | `18.0` |
| oocito_id=282672 | `meticorten_days` | `49.0` | `18.0` |
| oocito_id=282671 | `meticorten_days` | `49.0` | `18.0` |
| oocito_id=282670 | `meticorten_days` | `49.0` | `18.0` |
| oocito_id=276395 | `meticorten_days` | `105.0` | `15.0` |
| oocito_id=276394 | `meticorten_days` | `105.0` | `15.0` |
| oocito_id=276393 | `meticorten_days` | `105.0` | `15.0` |
| oocito_id=282676 | `meticorten_dose` | `49.0` | `18.0` |
| oocito_id=282675 | `meticorten_dose` | `49.0` | `18.0` |
| oocito_id=282674 | `meticorten_dose` | `49.0` | `18.0` |
| oocito_id=282673 | `meticorten_dose` | `49.0` | `18.0` |
| oocito_id=282672 | `meticorten_dose` | `49.0` | `18.0` |
| oocito_id=282671 | `meticorten_dose` | `49.0` | `18.0` |
| oocito_id=282670 | `meticorten_dose` | `49.0` | `18.0` |
| oocito_id=276395 | `meticorten_dose` | `525.0` | `75.0` |
| oocito_id=276394 | `meticorten_dose` | `525.0` | `75.0` |
| oocito_id=276393 | `meticorten_dose` | `525.0` | `75.0` |
| oocito_id=282676 | `meticorten_end` | `2025-10-28 00:00:00` | `2025-10-28` |
| oocito_id=282675 | `meticorten_end` | `2025-10-28 00:00:00` | `2025-10-28` |
| oocito_id=282674 | `meticorten_end` | `2025-10-28 00:00:00` | `2025-10-28` |
| oocito_id=282673 | `meticorten_end` | `2025-10-28 00:00:00` | `2025-10-28` |
| oocito_id=282672 | `meticorten_end` | `2025-10-28 00:00:00` | `2025-10-28` |
| oocito_id=282671 | `meticorten_end` | `2025-10-28 00:00:00` | `2025-10-28` |
| oocito_id=282670 | `meticorten_end` | `2025-10-28 00:00:00` | `2025-10-28` |
| oocito_id=276395 | `meticorten_end` | `2025-09-24 00:00:00` | `2025-09-24` |
| oocito_id=276394 | `meticorten_end` | `2025-09-24 00:00:00` | `2025-09-24` |
| oocito_id=276393 | `meticorten_end` | `2025-09-24 00:00:00` | `2025-09-24` |
| oocito_id=282676 | `meticorten_start` | `2025-10-11 00:00:00` | `2025-10-11` |
| oocito_id=282675 | `meticorten_start` | `2025-10-11 00:00:00` | `2025-10-11` |
| oocito_id=282674 | `meticorten_start` | `2025-10-11 00:00:00` | `2025-10-11` |
| oocito_id=282673 | `meticorten_start` | `2025-10-11 00:00:00` | `2025-10-11` |
| oocito_id=282672 | `meticorten_start` | `2025-10-11 00:00:00` | `2025-10-11` |
| oocito_id=282671 | `meticorten_start` | `2025-10-11 00:00:00` | `2025-10-11` |
| oocito_id=282670 | `meticorten_start` | `2025-10-11 00:00:00` | `2025-10-11` |
| oocito_id=276395 | `meticorten_start` | `2025-09-10 00:00:00` | `2025-09-10` |
| oocito_id=276394 | `meticorten_start` | `2025-09-10 00:00:00` | `2025-09-10` |
| oocito_id=276393 | `meticorten_start` | `2025-09-10 00:00:00` | `2025-09-10` |
| oocito_id=319427 | `micro_data_dl` | `2026-06-22 00:00:00` | `2026-06-22` |
| oocito_id=319426 | `micro_data_dl` | `2026-06-22 00:00:00` | `2026-06-22` |
| oocito_id=319425 | `micro_data_dl` | `2026-06-22 00:00:00` | `2026-06-22` |
| oocito_id=319424 | `micro_data_dl` | `2026-06-22 00:00:00` | `2026-06-22` |
| oocito_id=319423 | `micro_data_dl` | `2026-06-22 00:00:00` | `2026-06-22` |
| oocito_id=319422 | `micro_data_dl` | `2026-06-22 00:00:00` | `2026-06-22` |
| oocito_id=319421 | `micro_data_dl` | `2026-06-22 00:00:00` | `2026-06-22` |
| oocito_id=318548 | `micro_data_dl` | `2026-06-15 00:00:00` | `2026-06-15` |
| oocito_id=318547 | `micro_data_dl` | `2026-06-15 00:00:00` | `2026-06-15` |
| oocito_id=318546 | `micro_data_dl` | `2026-06-15 00:00:00` | `2026-06-15` |
| oocito_id=319427 | `micro_data_procedimento` | `2026-06-22 00:00:00` | `2026-06-22` |
| oocito_id=319426 | `micro_data_procedimento` | `2026-06-22 00:00:00` | `2026-06-22` |
| oocito_id=319425 | `micro_data_procedimento` | `2026-06-22 00:00:00` | `2026-06-22` |
| oocito_id=319424 | `micro_data_procedimento` | `2026-06-22 00:00:00` | `2026-06-22` |
| oocito_id=319423 | `micro_data_procedimento` | `2026-06-22 00:00:00` | `2026-06-22` |
| oocito_id=319422 | `micro_data_procedimento` | `2026-06-22 00:00:00` | `2026-06-22` |
| oocito_id=319421 | `micro_data_procedimento` | `2026-06-22 00:00:00` | `2026-06-22` |
| oocito_id=318548 | `micro_data_procedimento` | `2026-06-15 00:00:00` | `2026-06-15` |
| oocito_id=318547 | `micro_data_procedimento` | `2026-06-15 00:00:00` | `2026-06-15` |
| oocito_id=318546 | `micro_data_procedimento` | `2026-06-15 00:00:00` | `2026-06-15` |
| oocito_id=288316 | `n_of_biopsied` | `nan` | `3.0` |
| oocito_id=288315 | `n_of_biopsied` | `nan` | `3.0` |
| oocito_id=288314 | `n_of_biopsied` | `nan` | `3.0` |
| oocito_id=288313 | `n_of_biopsied` | `nan` | `3.0` |
| oocito_id=288312 | `n_of_biopsied` | `nan` | `3.0` |
| oocito_id=288311 | `n_of_biopsied` | `nan` | `3.0` |
| oocito_id=288310 | `n_of_biopsied` | `nan` | `3.0` |
| oocito_id=288309 | `n_of_biopsied` | `nan` | `3.0` |
| oocito_id=264829 | `n_of_biopsied` | `nan` | `1.0` |
| oocito_id=264828 | `n_of_biopsied` | `nan` | `1.0` |
| oocito_id=288316 | `n_of_normal` | `nan` | `0.0` |
| oocito_id=288315 | `n_of_normal` | `nan` | `0.0` |
| oocito_id=288314 | `n_of_normal` | `nan` | `0.0` |
| oocito_id=288313 | `n_of_normal` | `nan` | `0.0` |
| oocito_id=288312 | `n_of_normal` | `nan` | `0.0` |
| oocito_id=288311 | `n_of_normal` | `nan` | `0.0` |
| oocito_id=288310 | `n_of_normal` | `nan` | `0.0` |
| oocito_id=288309 | `n_of_normal` | `nan` | `0.0` |
| oocito_id=271823 | `n_of_normal` | `nan` | `0.0` |
| oocito_id=264829 | `n_of_normal` | `nan` | `1.0` |
| oocito_id=261343 | `natifa_days` | `20.0` | `10.0` |
| oocito_id=261342 | `natifa_days` | `20.0` | `10.0` |
| oocito_id=261341 | `natifa_days` | `20.0` | `10.0` |
| oocito_id=261340 | `natifa_days` | `20.0` | `10.0` |
| oocito_id=261339 | `natifa_days` | `20.0` | `10.0` |
| oocito_id=261338 | `natifa_days` | `20.0` | `10.0` |
| oocito_id=261337 | `natifa_days` | `20.0` | `10.0` |
| oocito_id=261336 | `natifa_days` | `20.0` | `10.0` |
| oocito_id=261335 | `natifa_days` | `20.0` | `10.0` |
| oocito_id=261343 | `natifa_dose` | `120.0` | `60.0` |
| oocito_id=261342 | `natifa_dose` | `120.0` | `60.0` |
| oocito_id=261341 | `natifa_dose` | `120.0` | `60.0` |
| oocito_id=261340 | `natifa_dose` | `120.0` | `60.0` |
| oocito_id=261339 | `natifa_dose` | `120.0` | `60.0` |
| oocito_id=261338 | `natifa_dose` | `120.0` | `60.0` |
| oocito_id=261337 | `natifa_dose` | `120.0` | `60.0` |
| oocito_id=261336 | `natifa_dose` | `120.0` | `60.0` |
| oocito_id=261335 | `natifa_dose` | `120.0` | `60.0` |
| oocito_id=261343 | `natifa_end` | `2025-06-18 00:00:00` | `2025-06-18` |
| oocito_id=261342 | `natifa_end` | `2025-06-18 00:00:00` | `2025-06-18` |
| oocito_id=261341 | `natifa_end` | `2025-06-18 00:00:00` | `2025-06-18` |
| oocito_id=261340 | `natifa_end` | `2025-06-18 00:00:00` | `2025-06-18` |
| oocito_id=261339 | `natifa_end` | `2025-06-18 00:00:00` | `2025-06-18` |
| oocito_id=261338 | `natifa_end` | `2025-06-18 00:00:00` | `2025-06-18` |
| oocito_id=261337 | `natifa_end` | `2025-06-18 00:00:00` | `2025-06-18` |
| oocito_id=261336 | `natifa_end` | `2025-06-18 00:00:00` | `2025-06-18` |
| oocito_id=261335 | `natifa_end` | `2025-06-18 00:00:00` | `2025-06-18` |
| oocito_id=261343 | `natifa_start` | `2025-06-09 00:00:00` | `2025-06-09` |
| oocito_id=261342 | `natifa_start` | `2025-06-09 00:00:00` | `2025-06-09` |
| oocito_id=261341 | `natifa_start` | `2025-06-09 00:00:00` | `2025-06-09` |
| oocito_id=261340 | `natifa_start` | `2025-06-09 00:00:00` | `2025-06-09` |
| oocito_id=261339 | `natifa_start` | `2025-06-09 00:00:00` | `2025-06-09` |
| oocito_id=261338 | `natifa_start` | `2025-06-09 00:00:00` | `2025-06-09` |
| oocito_id=261337 | `natifa_start` | `2025-06-09 00:00:00` | `2025-06-09` |
| oocito_id=261336 | `natifa_start` | `2025-06-09 00:00:00` | `2025-06-09` |
| oocito_id=261335 | `natifa_start` | `2025-06-09 00:00:00` | `2025-06-09` |
| oocito_id=313942 | `neo_days` | `14.0` | `3.0` |
| oocito_id=313941 | `neo_days` | `14.0` | `3.0` |
| oocito_id=313940 | `neo_days` | `14.0` | `3.0` |
| oocito_id=313939 | `neo_days` | `14.0` | `3.0` |
| oocito_id=295044 | `neo_days` | `6.0` | `2.0` |
| oocito_id=295043 | `neo_days` | `6.0` | `2.0` |
| oocito_id=295042 | `neo_days` | `6.0` | `2.0` |
| oocito_id=295041 | `neo_days` | `6.0` | `2.0` |
| oocito_id=295040 | `neo_days` | `6.0` | `2.0` |
| oocito_id=295039 | `neo_days` | `6.0` | `2.0` |
| oocito_id=313942 | `neo_dose` | `52.5` | `11.25` |
| oocito_id=313941 | `neo_dose` | `52.5` | `11.25` |
| oocito_id=313940 | `neo_dose` | `52.5` | `11.25` |
| oocito_id=313939 | `neo_dose` | `52.5` | `11.25` |
| oocito_id=295044 | `neo_dose` | `22.5` | `7.5` |
| oocito_id=295043 | `neo_dose` | `22.5` | `7.5` |
| oocito_id=295042 | `neo_dose` | `22.5` | `7.5` |
| oocito_id=295041 | `neo_dose` | `22.5` | `7.5` |
| oocito_id=295040 | `neo_dose` | `22.5` | `7.5` |
| oocito_id=295039 | `neo_dose` | `22.5` | `7.5` |
| oocito_id=313942 | `neo_end` | `2026-04-16 00:00:00` | `2026-04-16` |
| oocito_id=313941 | `neo_end` | `2026-04-16 00:00:00` | `2026-04-16` |
| oocito_id=313940 | `neo_end` | `2026-04-16 00:00:00` | `2026-04-16` |
| oocito_id=313939 | `neo_end` | `2026-04-16 00:00:00` | `2026-04-16` |
| oocito_id=295044 | `neo_end` | `2025-12-19 00:00:00` | `2025-12-19` |
| oocito_id=295043 | `neo_end` | `2025-12-19 00:00:00` | `2025-12-19` |
| oocito_id=295042 | `neo_end` | `2025-12-19 00:00:00` | `2025-12-19` |
| oocito_id=295041 | `neo_end` | `2025-12-19 00:00:00` | `2025-12-19` |
| oocito_id=295040 | `neo_end` | `2025-12-19 00:00:00` | `2025-12-19` |
| oocito_id=295039 | `neo_end` | `2025-12-19 00:00:00` | `2025-12-19` |
| oocito_id=313942 | `neo_start` | `2026-02-14 00:00:00` | `2026-02-14` |
| oocito_id=313941 | `neo_start` | `2026-02-14 00:00:00` | `2026-02-14` |
| oocito_id=313940 | `neo_start` | `2026-02-14 00:00:00` | `2026-02-14` |
| oocito_id=313939 | `neo_start` | `2026-02-14 00:00:00` | `2026-02-14` |
| oocito_id=295044 | `neo_start` | `2025-11-19 00:00:00` | `2025-11-19` |
| oocito_id=295043 | `neo_start` | `2025-11-19 00:00:00` | `2025-11-19` |
| oocito_id=295042 | `neo_start` | `2025-11-19 00:00:00` | `2025-11-19` |
| oocito_id=295041 | `neo_start` | `2025-11-19 00:00:00` | `2025-11-19` |
| oocito_id=295040 | `neo_start` | `2025-11-19 00:00:00` | `2025-11-19` |
| oocito_id=295039 | `neo_start` | `2025-11-19 00:00:00` | `2025-11-19` |
| oocito_id=119418 | `neo_unit` | `NÃO DEFINIDO` | `pump` |
| oocito_id=119417 | `neo_unit` | `NÃO DEFINIDO` | `pump` |
| oocito_id=271823 | `number_of_embryos_transferred` | `nan` | `1.0` |
| oocito_id=264829 | `number_of_embryos_transferred` | `nan` | `1.0` |
| oocito_id=264828 | `number_of_embryos_transferred` | `nan` | `1.0` |
| oocito_id=264827 | `number_of_embryos_transferred` | `nan` | `1.0` |
| oocito_id=264826 | `number_of_embryos_transferred` | `nan` | `1.0` |
| oocito_id=264825 | `number_of_embryos_transferred` | `nan` | `1.0` |
| oocito_id=264824 | `number_of_embryos_transferred` | `nan` | `1.0` |
| oocito_id=264823 | `number_of_embryos_transferred` | `nan` | `1.0` |
| oocito_id=262449 | `number_of_embryos_transferred` | `nan` | `1.0` |
| oocito_id=259138 | `number_of_embryos_transferred` | `nan` | `1.0` |
| oocito_id=313942 | `oestrogel_days` | `80.0` | `20.0` |
| oocito_id=313941 | `oestrogel_days` | `80.0` | `20.0` |
| oocito_id=313940 | `oestrogel_days` | `80.0` | `20.0` |
| oocito_id=313939 | `oestrogel_days` | `80.0` | `20.0` |
| oocito_id=312268 | `oestrogel_days` | `78.0` | `26.0` |
| oocito_id=312267 | `oestrogel_days` | `78.0` | `26.0` |
| oocito_id=312266 | `oestrogel_days` | `78.0` | `26.0` |
| oocito_id=312265 | `oestrogel_days` | `78.0` | `26.0` |
| oocito_id=312264 | `oestrogel_days` | `78.0` | `26.0` |
| oocito_id=304874 | `oestrogel_days` | `56.0` | `28.0` |
| oocito_id=313942 | `oestrogel_dose` | `320.0` | `80.0` |
| oocito_id=313941 | `oestrogel_dose` | `320.0` | `80.0` |
| oocito_id=313940 | `oestrogel_dose` | `320.0` | `80.0` |
| oocito_id=313939 | `oestrogel_dose` | `320.0` | `80.0` |
| oocito_id=312268 | `oestrogel_dose` | `156.0` | `52.0` |
| oocito_id=312267 | `oestrogel_dose` | `156.0` | `52.0` |
| oocito_id=312266 | `oestrogel_dose` | `156.0` | `52.0` |
| oocito_id=312265 | `oestrogel_dose` | `156.0` | `52.0` |
| oocito_id=312264 | `oestrogel_dose` | `156.0` | `52.0` |
| oocito_id=304874 | `oestrogel_dose` | `224.0` | `112.0` |
| oocito_id=313942 | `oestrogel_end` | `2026-05-21 00:00:00` | `2026-05-21` |
| oocito_id=313941 | `oestrogel_end` | `2026-05-21 00:00:00` | `2026-05-21` |
| oocito_id=313940 | `oestrogel_end` | `2026-05-21 00:00:00` | `2026-05-21` |
| oocito_id=313939 | `oestrogel_end` | `2026-05-21 00:00:00` | `2026-05-21` |
| oocito_id=312268 | `oestrogel_end` | `2026-05-22 00:00:00` | `2026-05-22` |
| oocito_id=312267 | `oestrogel_end` | `2026-05-22 00:00:00` | `2026-05-22` |
| oocito_id=312266 | `oestrogel_end` | `2026-05-22 00:00:00` | `2026-05-22` |
| oocito_id=312265 | `oestrogel_end` | `2026-05-22 00:00:00` | `2026-05-22` |
| oocito_id=312264 | `oestrogel_end` | `2026-05-22 00:00:00` | `2026-05-22` |
| oocito_id=311197 | `oestrogel_end` | `2026-05-05 00:00:00` | `2026-05-05` |
| oocito_id=111303 | `oestrogel_interval` | `12.0` | `24.0` |
| oocito_id=111302 | `oestrogel_interval` | `12.0` | `24.0` |
| oocito_id=111301 | `oestrogel_interval` | `12.0` | `24.0` |
| oocito_id=111300 | `oestrogel_interval` | `12.0` | `24.0` |
| oocito_id=111299 | `oestrogel_interval` | `12.0` | `24.0` |
| oocito_id=111298 | `oestrogel_interval` | `12.0` | `24.0` |
| oocito_id=111297 | `oestrogel_interval` | `12.0` | `24.0` |
| oocito_id=111296 | `oestrogel_interval` | `12.0` | `24.0` |
| oocito_id=41430 | `oestrogel_interval` | `12.0` | `24.0` |
| oocito_id=41429 | `oestrogel_interval` | `12.0` | `24.0` |
| oocito_id=313942 | `oestrogel_start` | `2026-05-02 00:00:00` | `2026-05-02` |
| oocito_id=313941 | `oestrogel_start` | `2026-05-02 00:00:00` | `2026-05-02` |
| oocito_id=313940 | `oestrogel_start` | `2026-05-02 00:00:00` | `2026-05-02` |
| oocito_id=313939 | `oestrogel_start` | `2026-05-02 00:00:00` | `2026-05-02` |
| oocito_id=312268 | `oestrogel_start` | `2026-04-27 00:00:00` | `2026-04-27` |
| oocito_id=312267 | `oestrogel_start` | `2026-04-27 00:00:00` | `2026-04-27` |
| oocito_id=312266 | `oestrogel_start` | `2026-04-27 00:00:00` | `2026-04-27` |
| oocito_id=312265 | `oestrogel_start` | `2026-04-27 00:00:00` | `2026-04-27` |
| oocito_id=312264 | `oestrogel_start` | `2026-04-27 00:00:00` | `2026-04-27` |
| oocito_id=311197 | `oestrogel_start` | `2026-04-18 00:00:00` | `2026-04-18` |
| oocito_id=133991 | `oestrogel_unit` | `comp` | `pump` |
| oocito_id=133990 | `oestrogel_unit` | `comp` | `pump` |
| oocito_id=133989 | `oestrogel_unit` | `comp` | `pump` |
| oocito_id=133988 | `oestrogel_unit` | `comp` | `pump` |
| oocito_id=133987 | `oestrogel_unit` | `comp` | `pump` |
| oocito_id=133986 | `oestrogel_unit` | `comp` | `pump` |
| oocito_id=133985 | `oestrogel_unit` | `comp` | `pump` |
| oocito_id=133984 | `oestrogel_unit` | `comp` | `pump` |
| oocito_id=133983 | `oestrogel_unit` | `comp` | `pump` |
| oocito_id=110274 | `oestrogel_unit` | `NÃO DEFINIDO` | `pump` |
| oocito_id=319425 | `oocito_resultado_pgd` | `nan` | `Aneuploide` |
| oocito_id=319423 | `oocito_resultado_pgd` | `nan` | `Aneuploide complexo` |
| oocito_id=318541 | `oocito_resultado_pgd` | `nan` | `Aneuploide complexo` |
| oocito_id=318540 | `oocito_resultado_pgd` | `nan` | `Aneuploide` |
| oocito_id=318539 | `oocito_resultado_pgd` | `nan` | `Euploide` |
| oocito_id=318538 | `oocito_resultado_pgd` | `nan` | `Aneuploide` |
| oocito_id=318537 | `oocito_resultado_pgd` | `nan` | `Aneuploide` |
| oocito_id=317240 | `oocito_resultado_pgd` | `nan` | `Aneuploide` |
| oocito_id=317239 | `oocito_resultado_pgd` | `nan` | `Euploide` |
| oocito_id=316846 | `oocito_resultado_pgd` | `Não analisado` | `Euploide` |
| oocito_id=319425 | `oocito_resultado_pgd_detalhes` | `nan` | `Aneuploidia detectada - Trissomia` |
| oocito_id=319423 | `oocito_resultado_pgd_detalhes` | `nan` | `Múltiplas aneuploidias detectadas` |
| oocito_id=318541 | `oocito_resultado_pgd_detalhes` | `nan` | `Múltiplas aneuploidias detectadas` |
| oocito_id=318538 | `oocito_resultado_pgd_detalhes` | `nan` | `Aneuploidia detectada - Monossomia` |
| oocito_id=317240 | `oocito_resultado_pgd_detalhes` | `nan` | `Múltiplas aneuploidias detectadas` |
| oocito_id=317239 | `oocito_resultado_pgd_detalhes` | `nan` | `Euploide 46, XY ` |
| oocito_id=316846 | `oocito_resultado_pgd_detalhes` | `nan` | `Euploide` |
| oocito_id=316845 | `oocito_resultado_pgd_detalhes` | `nan` | `Múltiplas aneuploidias detectadas` |
| oocito_id=315271 | `oocito_resultado_pgd_detalhes` | `nan` | `Múltiplas aneuplodias detectadas` |
| oocito_id=319427 | `oocito_tcd` | `nan` | `Descartado` |
| oocito_id=319426 | `oocito_tcd` | `nan` | `Descartado` |
| oocito_id=319425 | `oocito_tcd` | `nan` | `Criopreservado` |
| oocito_id=319424 | `oocito_tcd` | `nan` | `Descartado` |
| oocito_id=319423 | `oocito_tcd` | `nan` | `Criopreservado` |
| oocito_id=319422 | `oocito_tcd` | `nan` | `Descartado` |
| oocito_id=316842 | `orgalutran_days` | `2.0` | `1.0` |
| oocito_id=316841 | `orgalutran_days` | `2.0` | `1.0` |
| oocito_id=316840 | `orgalutran_days` | `2.0` | `1.0` |
| oocito_id=316839 | `orgalutran_days` | `2.0` | `1.0` |
| oocito_id=316838 | `orgalutran_days` | `2.0` | `1.0` |
| oocito_id=311830 | `orgalutran_days` | `8.0` | `4.0` |
| oocito_id=311829 | `orgalutran_days` | `8.0` | `4.0` |
| oocito_id=311828 | `orgalutran_days` | `8.0` | `4.0` |
| oocito_id=311827 | `orgalutran_days` | `8.0` | `4.0` |
| oocito_id=308189 | `orgalutran_days` | `17.0` | `6.0` |
| oocito_id=316842 | `orgalutran_dose` | `0.5` | `0.25` |
| oocito_id=316841 | `orgalutran_dose` | `0.5` | `0.25` |
| oocito_id=316840 | `orgalutran_dose` | `0.5` | `0.25` |
| oocito_id=316839 | `orgalutran_dose` | `0.5` | `0.25` |
| oocito_id=316838 | `orgalutran_dose` | `0.5` | `0.25` |
| oocito_id=311830 | `orgalutran_dose` | `2.0` | `1.0` |
| oocito_id=311829 | `orgalutran_dose` | `2.0` | `1.0` |
| oocito_id=311828 | `orgalutran_dose` | `2.0` | `1.0` |
| oocito_id=311827 | `orgalutran_dose` | `2.0` | `1.0` |
| oocito_id=308189 | `orgalutran_dose` | `4.25` | `1.5` |
| oocito_id=318548 | `orgalutran_end` | `2026-06-13 00:00:00` | `2026-06-13` |
| oocito_id=318547 | `orgalutran_end` | `2026-06-13 00:00:00` | `2026-06-13` |
| oocito_id=318546 | `orgalutran_end` | `2026-06-13 00:00:00` | `2026-06-13` |
| oocito_id=318545 | `orgalutran_end` | `2026-06-13 00:00:00` | `2026-06-13` |
| oocito_id=318544 | `orgalutran_end` | `2026-06-13 00:00:00` | `2026-06-13` |
| oocito_id=318543 | `orgalutran_end` | `2026-06-13 00:00:00` | `2026-06-13` |
| oocito_id=318542 | `orgalutran_end` | `2026-06-13 00:00:00` | `2026-06-13` |
| oocito_id=318541 | `orgalutran_end` | `2026-06-13 00:00:00` | `2026-06-13` |
| oocito_id=318540 | `orgalutran_end` | `2026-06-13 00:00:00` | `2026-06-13` |
| oocito_id=318539 | `orgalutran_end` | `2026-06-13 00:00:00` | `2026-06-13` |
| oocito_id=318548 | `orgalutran_start` | `2026-06-12 00:00:00` | `2026-06-12` |
| oocito_id=318547 | `orgalutran_start` | `2026-06-12 00:00:00` | `2026-06-12` |
| oocito_id=318546 | `orgalutran_start` | `2026-06-12 00:00:00` | `2026-06-12` |
| oocito_id=318545 | `orgalutran_start` | `2026-06-12 00:00:00` | `2026-06-12` |
| oocito_id=318544 | `orgalutran_start` | `2026-06-12 00:00:00` | `2026-06-12` |
| oocito_id=318543 | `orgalutran_start` | `2026-06-12 00:00:00` | `2026-06-12` |
| oocito_id=318542 | `orgalutran_start` | `2026-06-12 00:00:00` | `2026-06-12` |
| oocito_id=318541 | `orgalutran_start` | `2026-06-12 00:00:00` | `2026-06-12` |
| oocito_id=318540 | `orgalutran_start` | `2026-06-12 00:00:00` | `2026-06-12` |
| oocito_id=318539 | `orgalutran_start` | `2026-06-12 00:00:00` | `2026-06-12` |
| oocito_id=265418 | `orgalutran_unit` | `mg` | `seringa` |
| oocito_id=265417 | `orgalutran_unit` | `mg` | `seringa` |
| oocito_id=265416 | `orgalutran_unit` | `mg` | `seringa` |
| oocito_id=265415 | `orgalutran_unit` | `mg` | `seringa` |
| oocito_id=265414 | `orgalutran_unit` | `mg` | `seringa` |
| oocito_id=265413 | `orgalutran_unit` | `mg` | `seringa` |
| oocito_id=265412 | `orgalutran_unit` | `mg` | `seringa` |
| oocito_id=265411 | `orgalutran_unit` | `mg` | `seringa` |
| oocito_id=265410 | `orgalutran_unit` | `mg` | `seringa` |
| oocito_id=265409 | `orgalutran_unit` | `mg` | `seringa` |
| oocito_id=271823 | `outcome_type` | `nan` | `NO PREGNANCY` |
| oocito_id=264829 | `outcome_type` | `nan` | `No pregnancy` |
| oocito_id=264828 | `outcome_type` | `nan` | `No pregnancy` |
| oocito_id=264827 | `outcome_type` | `nan` | `No pregnancy` |
| oocito_id=264826 | `outcome_type` | `nan` | `No pregnancy` |
| oocito_id=264825 | `outcome_type` | `nan` | `No pregnancy` |
| oocito_id=264824 | `outcome_type` | `nan` | `No pregnancy` |
| oocito_id=264823 | `outcome_type` | `nan` | `No pregnancy` |
| oocito_id=262449 | `outcome_type` | `nan` | `Delivery (>21WA)` |
| oocito_id=259138 | `outcome_type` | `nan` | `Delivery (> 21 WA)` |
| oocito_id=317242 | `ovidrel_days` | `3.0` | `1.0` |
| oocito_id=316948 | `ovidrel_days` | `2.0` | `1.0` |
| oocito_id=316942 | `ovidrel_days` | `2.0` | `1.0` |
| oocito_id=316941 | `ovidrel_days` | `2.0` | `1.0` |
| oocito_id=316940 | `ovidrel_days` | `2.0` | `1.0` |
| oocito_id=316939 | `ovidrel_days` | `2.0` | `1.0` |
| oocito_id=316893 | `ovidrel_days` | `3.0` | `1.0` |
| oocito_id=316892 | `ovidrel_days` | `3.0` | `1.0` |
| oocito_id=316891 | `ovidrel_days` | `3.0` | `1.0` |
| oocito_id=316849 | `ovidrel_days` | `3.0` | `1.0` |
| oocito_id=317242 | `ovidrel_dose` | `3.0` | `1.0` |
| oocito_id=316948 | `ovidrel_dose` | `500.0` | `250.0` |
| oocito_id=316942 | `ovidrel_dose` | `500.0` | `250.0` |
| oocito_id=316941 | `ovidrel_dose` | `500.0` | `250.0` |
| oocito_id=316940 | `ovidrel_dose` | `500.0` | `250.0` |
| oocito_id=316939 | `ovidrel_dose` | `500.0` | `250.0` |
| oocito_id=316893 | `ovidrel_dose` | `750.0` | `250.0` |
| oocito_id=316892 | `ovidrel_dose` | `750.0` | `250.0` |
| oocito_id=316891 | `ovidrel_dose` | `750.0` | `250.0` |
| oocito_id=316849 | `ovidrel_dose` | `750.0` | `250.0` |
| oocito_id=319427 | `ovidrel_end` | `2026-06-20 00:00:00` | `2026-06-20` |
| oocito_id=319426 | `ovidrel_end` | `2026-06-20 00:00:00` | `2026-06-20` |
| oocito_id=319425 | `ovidrel_end` | `2026-06-20 00:00:00` | `2026-06-20` |
| oocito_id=319424 | `ovidrel_end` | `2026-06-20 00:00:00` | `2026-06-20` |
| oocito_id=319423 | `ovidrel_end` | `2026-06-20 00:00:00` | `2026-06-20` |
| oocito_id=319422 | `ovidrel_end` | `2026-06-20 00:00:00` | `2026-06-20` |
| oocito_id=319421 | `ovidrel_end` | `2026-06-20 00:00:00` | `2026-06-20` |
| oocito_id=318548 | `ovidrel_end` | `2026-06-13 00:00:00` | `2026-06-13` |
| oocito_id=318547 | `ovidrel_end` | `2026-06-13 00:00:00` | `2026-06-13` |
| oocito_id=318546 | `ovidrel_end` | `2026-06-13 00:00:00` | `2026-06-13` |
| oocito_id=319427 | `ovidrel_start` | `2026-06-20 00:00:00` | `2026-06-20` |
| oocito_id=319426 | `ovidrel_start` | `2026-06-20 00:00:00` | `2026-06-20` |
| oocito_id=319425 | `ovidrel_start` | `2026-06-20 00:00:00` | `2026-06-20` |
| oocito_id=319424 | `ovidrel_start` | `2026-06-20 00:00:00` | `2026-06-20` |
| oocito_id=319423 | `ovidrel_start` | `2026-06-20 00:00:00` | `2026-06-20` |
| oocito_id=319422 | `ovidrel_start` | `2026-06-20 00:00:00` | `2026-06-20` |
| oocito_id=319421 | `ovidrel_start` | `2026-06-20 00:00:00` | `2026-06-20` |
| oocito_id=318548 | `ovidrel_start` | `2026-06-13 00:00:00` | `2026-06-13` |
| oocito_id=318547 | `ovidrel_start` | `2026-06-13 00:00:00` | `2026-06-13` |
| oocito_id=318546 | `ovidrel_start` | `2026-06-13 00:00:00` | `2026-06-13` |
| oocito_id=107047 | `ovidrel_unit` | `NÃO DEFINIDO` | `ampola` |
| oocito_id=107046 | `ovidrel_unit` | `NÃO DEFINIDO` | `ampola` |
| oocito_id=107045 | `ovidrel_unit` | `NÃO DEFINIDO` | `ampola` |
| oocito_id=107044 | `ovidrel_unit` | `NÃO DEFINIDO` | `ampola` |
| oocito_id=107043 | `ovidrel_unit` | `NÃO DEFINIDO` | `ampola` |
| oocito_id=107042 | `ovidrel_unit` | `NÃO DEFINIDO` | `ampola` |
| oocito_id=107041 | `ovidrel_unit` | `NÃO DEFINIDO` | `ampola` |
| oocito_id=107040 | `ovidrel_unit` | `NÃO DEFINIDO` | `ampola` |
| oocito_id=107039 | `ovidrel_unit` | `NÃO DEFINIDO` | `ampola` |
| oocito_id=319427 | `pergoveris_days` | `2.0` | `1.0` |
| oocito_id=319426 | `pergoveris_days` | `2.0` | `1.0` |
| oocito_id=319425 | `pergoveris_days` | `2.0` | `1.0` |
| oocito_id=319424 | `pergoveris_days` | `2.0` | `1.0` |
| oocito_id=319423 | `pergoveris_days` | `2.0` | `1.0` |
| oocito_id=319422 | `pergoveris_days` | `2.0` | `1.0` |
| oocito_id=319421 | `pergoveris_days` | `2.0` | `1.0` |
| oocito_id=318548 | `pergoveris_days` | `18.0` | `11.0` |
| oocito_id=318547 | `pergoveris_days` | `18.0` | `11.0` |
| oocito_id=318546 | `pergoveris_days` | `18.0` | `11.0` |
| oocito_id=319427 | `pergoveris_dose` | `600.0` | `300.0` |
| oocito_id=319426 | `pergoveris_dose` | `600.0` | `300.0` |
| oocito_id=319425 | `pergoveris_dose` | `600.0` | `300.0` |
| oocito_id=319424 | `pergoveris_dose` | `600.0` | `300.0` |
| oocito_id=319423 | `pergoveris_dose` | `600.0` | `300.0` |
| oocito_id=319422 | `pergoveris_dose` | `600.0` | `300.0` |
| oocito_id=319421 | `pergoveris_dose` | `600.0` | `300.0` |
| oocito_id=318548 | `pergoveris_dose` | `3525.0` | `2100.0` |
| oocito_id=318547 | `pergoveris_dose` | `3525.0` | `2100.0` |
| oocito_id=318546 | `pergoveris_dose` | `3525.0` | `2100.0` |
| oocito_id=319427 | `pergoveris_end` | `2026-06-19 00:00:00` | `2026-06-19` |
| oocito_id=319426 | `pergoveris_end` | `2026-06-19 00:00:00` | `2026-06-19` |
| oocito_id=319425 | `pergoveris_end` | `2026-06-19 00:00:00` | `2026-06-19` |
| oocito_id=319424 | `pergoveris_end` | `2026-06-19 00:00:00` | `2026-06-19` |
| oocito_id=319423 | `pergoveris_end` | `2026-06-19 00:00:00` | `2026-06-19` |
| oocito_id=319422 | `pergoveris_end` | `2026-06-19 00:00:00` | `2026-06-19` |
| oocito_id=319421 | `pergoveris_end` | `2026-06-19 00:00:00` | `2026-06-19` |
| oocito_id=318548 | `pergoveris_end` | `2026-06-13 00:00:00` | `2026-06-13` |
| oocito_id=318547 | `pergoveris_end` | `2026-06-13 00:00:00` | `2026-06-13` |
| oocito_id=318546 | `pergoveris_end` | `2026-06-13 00:00:00` | `2026-06-13` |
| oocito_id=269981 | `pergoveris_interval` | `24.0` | `nan` |
| oocito_id=269980 | `pergoveris_interval` | `24.0` | `nan` |
| oocito_id=269979 | `pergoveris_interval` | `24.0` | `nan` |
| oocito_id=269978 | `pergoveris_interval` | `24.0` | `nan` |
| oocito_id=269977 | `pergoveris_interval` | `24.0` | `nan` |
| oocito_id=269976 | `pergoveris_interval` | `24.0` | `nan` |
| oocito_id=319427 | `pergoveris_start` | `2026-06-19 00:00:00` | `2026-06-19` |
| oocito_id=319426 | `pergoveris_start` | `2026-06-19 00:00:00` | `2026-06-19` |
| oocito_id=319425 | `pergoveris_start` | `2026-06-19 00:00:00` | `2026-06-19` |
| oocito_id=319424 | `pergoveris_start` | `2026-06-19 00:00:00` | `2026-06-19` |
| oocito_id=319423 | `pergoveris_start` | `2026-06-19 00:00:00` | `2026-06-19` |
| oocito_id=319422 | `pergoveris_start` | `2026-06-19 00:00:00` | `2026-06-19` |
| oocito_id=319421 | `pergoveris_start` | `2026-06-19 00:00:00` | `2026-06-19` |
| oocito_id=318548 | `pergoveris_start` | `2026-06-03 00:00:00` | `2026-06-03` |
| oocito_id=318547 | `pergoveris_start` | `2026-06-03 00:00:00` | `2026-06-03` |
| oocito_id=318546 | `pergoveris_start` | `2026-06-03 00:00:00` | `2026-06-03` |
| oocito_id=269981 | `pergoveris_unit` | `UI` | `nan` |
| oocito_id=269980 | `pergoveris_unit` | `UI` | `nan` |
| oocito_id=269979 | `pergoveris_unit` | `UI` | `nan` |
| oocito_id=269978 | `pergoveris_unit` | `UI` | `nan` |
| oocito_id=269977 | `pergoveris_unit` | `UI` | `nan` |
| oocito_id=269976 | `pergoveris_unit` | `UI` | `nan` |
| oocito_id=197323 | `pergoveris_unit` | `UI` | `mcg` |
| oocito_id=197322 | `pergoveris_unit` | `UI` | `mcg` |
| oocito_id=197321 | `pergoveris_unit` | `UI` | `mcg` |
| oocito_id=197320 | `pergoveris_unit` | `UI` | `mcg` |
| oocito_id=241641 | `predsim_end` | `2025-02-27 00:00:00` | `2025-02-27` |
| oocito_id=241640 | `predsim_end` | `2025-02-27 00:00:00` | `2025-02-27` |
| oocito_id=241639 | `predsim_end` | `2025-02-27 00:00:00` | `2025-02-27` |
| oocito_id=241638 | `predsim_end` | `2025-02-27 00:00:00` | `2025-02-27` |
| oocito_id=241637 | `predsim_end` | `2025-02-27 00:00:00` | `2025-02-27` |
| oocito_id=241636 | `predsim_end` | `2025-02-27 00:00:00` | `2025-02-27` |
| oocito_id=241635 | `predsim_end` | `2025-02-27 00:00:00` | `2025-02-27` |
| oocito_id=241641 | `predsim_start` | `2025-02-13 00:00:00` | `2025-02-13` |
| oocito_id=241640 | `predsim_start` | `2025-02-13 00:00:00` | `2025-02-13` |
| oocito_id=241639 | `predsim_start` | `2025-02-13 00:00:00` | `2025-02-13` |
| oocito_id=241638 | `predsim_start` | `2025-02-13 00:00:00` | `2025-02-13` |
| oocito_id=241637 | `predsim_start` | `2025-02-13 00:00:00` | `2025-02-13` |
| oocito_id=241636 | `predsim_start` | `2025-02-13 00:00:00` | `2025-02-13` |
| oocito_id=241635 | `predsim_start` | `2025-02-13 00:00:00` | `2025-02-13` |
| oocito_id=315822 | `primogyna_days` | `76.0` | `19.0` |
| oocito_id=315821 | `primogyna_days` | `76.0` | `19.0` |
| oocito_id=315820 | `primogyna_days` | `76.0` | `19.0` |
| oocito_id=315819 | `primogyna_days` | `76.0` | `19.0` |
| oocito_id=315818 | `primogyna_days` | `76.0` | `19.0` |
| oocito_id=313942 | `primogyna_days` | `80.0` | `20.0` |
| oocito_id=313941 | `primogyna_days` | `80.0` | `20.0` |
| oocito_id=313940 | `primogyna_days` | `80.0` | `20.0` |
| oocito_id=313939 | `primogyna_days` | `80.0` | `20.0` |
| oocito_id=313434 | `primogyna_days` | `60.0` | `30.0` |
| oocito_id=315822 | `primogyna_dose` | `228.0` | `57.0` |
| oocito_id=315821 | `primogyna_dose` | `228.0` | `57.0` |
| oocito_id=315820 | `primogyna_dose` | `228.0` | `57.0` |
| oocito_id=315819 | `primogyna_dose` | `228.0` | `57.0` |
| oocito_id=315818 | `primogyna_dose` | `228.0` | `57.0` |
| oocito_id=313942 | `primogyna_dose` | `480.0` | `120.0` |
| oocito_id=313941 | `primogyna_dose` | `480.0` | `120.0` |
| oocito_id=313940 | `primogyna_dose` | `480.0` | `120.0` |
| oocito_id=313939 | `primogyna_dose` | `480.0` | `120.0` |
| oocito_id=313434 | `primogyna_dose` | `444.0` | `222.0` |
| oocito_id=315822 | `primogyna_end` | `2026-06-01 00:00:00` | `2026-06-01` |
| oocito_id=315821 | `primogyna_end` | `2026-06-01 00:00:00` | `2026-06-01` |
| oocito_id=315820 | `primogyna_end` | `2026-06-01 00:00:00` | `2026-06-01` |
| oocito_id=315819 | `primogyna_end` | `2026-06-01 00:00:00` | `2026-06-01` |
| oocito_id=315818 | `primogyna_end` | `2026-06-01 00:00:00` | `2026-06-01` |
| oocito_id=313942 | `primogyna_end` | `2026-05-21 00:00:00` | `2026-05-21` |
| oocito_id=313941 | `primogyna_end` | `2026-05-21 00:00:00` | `2026-05-21` |
| oocito_id=313940 | `primogyna_end` | `2026-05-21 00:00:00` | `2026-05-21` |
| oocito_id=313939 | `primogyna_end` | `2026-05-21 00:00:00` | `2026-05-21` |
| oocito_id=313434 | `primogyna_end` | `2026-05-29 00:00:00` | `2026-05-29` |
| oocito_id=312268 | `primogyna_interval` | `8.0` | `12.0` |
| oocito_id=312267 | `primogyna_interval` | `8.0` | `12.0` |
| oocito_id=312266 | `primogyna_interval` | `8.0` | `12.0` |
| oocito_id=312265 | `primogyna_interval` | `8.0` | `12.0` |
| oocito_id=312264 | `primogyna_interval` | `8.0` | `12.0` |
| oocito_id=303457 | `primogyna_interval` | `8.0` | `24.0` |
| oocito_id=303456 | `primogyna_interval` | `8.0` | `24.0` |
| oocito_id=303455 | `primogyna_interval` | `8.0` | `24.0` |
| oocito_id=303454 | `primogyna_interval` | `8.0` | `24.0` |
| oocito_id=303453 | `primogyna_interval` | `8.0` | `24.0` |
| oocito_id=315822 | `primogyna_start` | `2026-05-14 00:00:00` | `2026-05-14` |
| oocito_id=315821 | `primogyna_start` | `2026-05-14 00:00:00` | `2026-05-14` |
| oocito_id=315820 | `primogyna_start` | `2026-05-14 00:00:00` | `2026-05-14` |
| oocito_id=315819 | `primogyna_start` | `2026-05-14 00:00:00` | `2026-05-14` |
| oocito_id=315818 | `primogyna_start` | `2026-05-14 00:00:00` | `2026-05-14` |
| oocito_id=313942 | `primogyna_start` | `2026-05-02 00:00:00` | `2026-05-02` |
| oocito_id=313941 | `primogyna_start` | `2026-05-02 00:00:00` | `2026-05-02` |
| oocito_id=313940 | `primogyna_start` | `2026-05-02 00:00:00` | `2026-05-02` |
| oocito_id=313939 | `primogyna_start` | `2026-05-02 00:00:00` | `2026-05-02` |
| oocito_id=313434 | `primogyna_start` | `2026-04-30 00:00:00` | `2026-04-30` |
| oocito_id=108493 | `primogyna_unit` | `comp` | `nan` |
| oocito_id=108492 | `primogyna_unit` | `comp` | `nan` |
| oocito_id=108491 | `primogyna_unit` | `comp` | `nan` |
| oocito_id=108490 | `primogyna_unit` | `comp` | `nan` |
| oocito_id=108489 | `primogyna_unit` | `comp` | `nan` |
| oocito_id=108488 | `primogyna_unit` | `comp` | `nan` |
| oocito_id=108487 | `primogyna_unit` | `comp` | `nan` |
| oocito_id=41430 | `primogyna_unit` | `cap` | `mg` |
| oocito_id=41429 | `primogyna_unit` | `cap` | `mg` |
| oocito_id=41428 | `primogyna_unit` | `cap` | `mg` |
| oocito_id=305809 | `progesterona_days` | `6.0` | `nan` |
| oocito_id=305808 | `progesterona_days` | `6.0` | `nan` |
| oocito_id=305807 | `progesterona_days` | `6.0` | `nan` |
| oocito_id=305806 | `progesterona_days` | `6.0` | `nan` |
| oocito_id=305805 | `progesterona_days` | `6.0` | `nan` |
| oocito_id=304874 | `progesterona_days` | `34.0` | `17.0` |
| oocito_id=304873 | `progesterona_days` | `34.0` | `17.0` |
| oocito_id=304872 | `progesterona_days` | `34.0` | `17.0` |
| oocito_id=304871 | `progesterona_days` | `34.0` | `17.0` |
| oocito_id=304870 | `progesterona_days` | `34.0` | `17.0` |
| oocito_id=305809 | `progesterona_dose` | `6.0` | `nan` |
| oocito_id=305808 | `progesterona_dose` | `6.0` | `nan` |
| oocito_id=305807 | `progesterona_dose` | `6.0` | `nan` |
| oocito_id=305806 | `progesterona_dose` | `6.0` | `nan` |
| oocito_id=305805 | `progesterona_dose` | `6.0` | `nan` |
| oocito_id=304874 | `progesterona_dose` | `34.0` | `17.0` |
| oocito_id=304873 | `progesterona_dose` | `34.0` | `17.0` |
| oocito_id=304872 | `progesterona_dose` | `34.0` | `17.0` |
| oocito_id=304871 | `progesterona_dose` | `34.0` | `17.0` |
| oocito_id=304870 | `progesterona_dose` | `34.0` | `17.0` |
| oocito_id=305809 | `progesterona_end` | `2026-04-01 00:00:00` | `None` |
| oocito_id=305808 | `progesterona_end` | `2026-04-01 00:00:00` | `None` |
| oocito_id=305807 | `progesterona_end` | `2026-04-01 00:00:00` | `None` |
| oocito_id=305806 | `progesterona_end` | `2026-04-01 00:00:00` | `None` |
| oocito_id=305805 | `progesterona_end` | `2026-04-01 00:00:00` | `None` |
| oocito_id=304874 | `progesterona_end` | `2026-04-05 00:00:00` | `2026-04-05` |
| oocito_id=304873 | `progesterona_end` | `2026-04-05 00:00:00` | `2026-04-05` |
| oocito_id=304872 | `progesterona_end` | `2026-04-05 00:00:00` | `2026-04-05` |
| oocito_id=304871 | `progesterona_end` | `2026-04-05 00:00:00` | `2026-04-05` |
| oocito_id=304870 | `progesterona_end` | `2026-04-05 00:00:00` | `2026-04-05` |
| oocito_id=305809 | `progesterona_interval` | `24.0` | `nan` |
| oocito_id=305808 | `progesterona_interval` | `24.0` | `nan` |
| oocito_id=305807 | `progesterona_interval` | `24.0` | `nan` |
| oocito_id=305806 | `progesterona_interval` | `24.0` | `nan` |
| oocito_id=305805 | `progesterona_interval` | `24.0` | `nan` |
| oocito_id=305809 | `progesterona_start` | `2026-03-27 00:00:00` | `None` |
| oocito_id=305808 | `progesterona_start` | `2026-03-27 00:00:00` | `None` |
| oocito_id=305807 | `progesterona_start` | `2026-03-27 00:00:00` | `None` |
| oocito_id=305806 | `progesterona_start` | `2026-03-27 00:00:00` | `None` |
| oocito_id=305805 | `progesterona_start` | `2026-03-27 00:00:00` | `None` |
| oocito_id=304874 | `progesterona_start` | `2026-03-20 00:00:00` | `2026-03-20` |
| oocito_id=304873 | `progesterona_start` | `2026-03-20 00:00:00` | `2026-03-20` |
| oocito_id=304872 | `progesterona_start` | `2026-03-20 00:00:00` | `2026-03-20` |
| oocito_id=304871 | `progesterona_start` | `2026-03-20 00:00:00` | `2026-03-20` |
| oocito_id=304870 | `progesterona_start` | `2026-03-20 00:00:00` | `2026-03-20` |
| oocito_id=305809 | `progesterona_unit` | `ampola` | `nan` |
| oocito_id=305808 | `progesterona_unit` | `ampola` | `nan` |
| oocito_id=305807 | `progesterona_unit` | `ampola` | `nan` |
| oocito_id=305806 | `progesterona_unit` | `ampola` | `nan` |
| oocito_id=305805 | `progesterona_unit` | `ampola` | `nan` |
| oocito_id=311558 | `puregon_days` | `26.0` | `11.0` |
| oocito_id=311557 | `puregon_days` | `26.0` | `11.0` |
| oocito_id=311556 | `puregon_days` | `26.0` | `11.0` |
| oocito_id=311555 | `puregon_days` | `26.0` | `11.0` |
| oocito_id=311554 | `puregon_days` | `26.0` | `11.0` |
| oocito_id=311553 | `puregon_days` | `26.0` | `11.0` |
| oocito_id=311552 | `puregon_days` | `26.0` | `11.0` |
| oocito_id=311551 | `puregon_days` | `26.0` | `11.0` |
| oocito_id=311550 | `puregon_days` | `26.0` | `11.0` |
| oocito_id=311549 | `puregon_days` | `26.0` | `11.0` |
| oocito_id=311558 | `puregon_dose` | `4700.0` | `1950.0` |
| oocito_id=311557 | `puregon_dose` | `4700.0` | `1950.0` |
| oocito_id=311556 | `puregon_dose` | `4700.0` | `1950.0` |
| oocito_id=311555 | `puregon_dose` | `4700.0` | `1950.0` |
| oocito_id=311554 | `puregon_dose` | `4700.0` | `1950.0` |
| oocito_id=311553 | `puregon_dose` | `4700.0` | `1950.0` |
| oocito_id=311552 | `puregon_dose` | `4700.0` | `1950.0` |
| oocito_id=311551 | `puregon_dose` | `4700.0` | `1950.0` |
| oocito_id=311550 | `puregon_dose` | `4700.0` | `1950.0` |
| oocito_id=311549 | `puregon_dose` | `4700.0` | `1950.0` |
| oocito_id=311558 | `puregon_end` | `2026-04-28 00:00:00` | `2026-04-28` |
| oocito_id=311557 | `puregon_end` | `2026-04-28 00:00:00` | `2026-04-28` |
| oocito_id=311556 | `puregon_end` | `2026-04-28 00:00:00` | `2026-04-28` |
| oocito_id=311555 | `puregon_end` | `2026-04-28 00:00:00` | `2026-04-28` |
| oocito_id=311554 | `puregon_end` | `2026-04-28 00:00:00` | `2026-04-28` |
| oocito_id=311553 | `puregon_end` | `2026-04-28 00:00:00` | `2026-04-28` |
| oocito_id=311552 | `puregon_end` | `2026-04-28 00:00:00` | `2026-04-28` |
| oocito_id=311551 | `puregon_end` | `2026-04-28 00:00:00` | `2026-04-28` |
| oocito_id=311550 | `puregon_end` | `2026-04-28 00:00:00` | `2026-04-28` |
| oocito_id=311549 | `puregon_end` | `2026-04-28 00:00:00` | `2026-04-28` |
| oocito_id=311558 | `puregon_start` | `2026-04-11 00:00:00` | `2026-04-18` |
| oocito_id=311557 | `puregon_start` | `2026-04-11 00:00:00` | `2026-04-18` |
| oocito_id=311556 | `puregon_start` | `2026-04-11 00:00:00` | `2026-04-18` |
| oocito_id=311555 | `puregon_start` | `2026-04-11 00:00:00` | `2026-04-18` |
| oocito_id=311554 | `puregon_start` | `2026-04-11 00:00:00` | `2026-04-18` |
| oocito_id=311553 | `puregon_start` | `2026-04-11 00:00:00` | `2026-04-18` |
| oocito_id=311552 | `puregon_start` | `2026-04-11 00:00:00` | `2026-04-18` |
| oocito_id=311551 | `puregon_start` | `2026-04-11 00:00:00` | `2026-04-18` |
| oocito_id=311550 | `puregon_start` | `2026-04-11 00:00:00` | `2026-04-18` |
| oocito_id=311549 | `puregon_start` | `2026-04-11 00:00:00` | `2026-04-18` |
| oocito_id=98948 | `puregon_unit` | `NÃO DEFINIDO` | `UI` |
| oocito_id=98947 | `puregon_unit` | `NÃO DEFINIDO` | `UI` |
| oocito_id=98946 | `puregon_unit` | `NÃO DEFINIDO` | `UI` |
| oocito_id=98945 | `puregon_unit` | `NÃO DEFINIDO` | `UI` |
| oocito_id=98944 | `puregon_unit` | `NÃO DEFINIDO` | `UI` |
| oocito_id=98943 | `puregon_unit` | `NÃO DEFINIDO` | `UI` |
| oocito_id=98942 | `puregon_unit` | `NÃO DEFINIDO` | `UI` |
| oocito_id=98941 | `puregon_unit` | `NÃO DEFINIDO` | `UI` |
| oocito_id=98940 | `puregon_unit` | `NÃO DEFINIDO` | `UI` |
| oocito_id=98939 | `puregon_unit` | `NÃO DEFINIDO` | `UI` |
| oocito_id=288316 | `redlara_outcome` | `nan` | `CANCELLATION` |
| oocito_id=288315 | `redlara_outcome` | `nan` | `CANCELLATION` |
| oocito_id=288314 | `redlara_outcome` | `nan` | `CANCELLATION` |
| oocito_id=288313 | `redlara_outcome` | `nan` | `CANCELLATION` |
| oocito_id=288312 | `redlara_outcome` | `nan` | `CANCELLATION` |
| oocito_id=288311 | `redlara_outcome` | `nan` | `CANCELLATION` |
| oocito_id=288310 | `redlara_outcome` | `nan` | `CANCELLATION` |
| oocito_id=288309 | `redlara_outcome` | `nan` | `CANCELLATION` |
| oocito_id=271823 | `redlara_outcome` | `nan` | `EMBRYO TRANSFER` |
| oocito_id=264829 | `redlara_outcome` | `nan` | `EMBRYO TRANSFER` |
| oocito_id=296653 | `rekovelle_days` | `25.0` | `10.0` |
| oocito_id=296652 | `rekovelle_days` | `25.0` | `10.0` |
| oocito_id=296651 | `rekovelle_days` | `25.0` | `10.0` |
| oocito_id=296650 | `rekovelle_days` | `25.0` | `10.0` |
| oocito_id=296649 | `rekovelle_days` | `25.0` | `10.0` |
| oocito_id=296648 | `rekovelle_days` | `25.0` | `10.0` |
| oocito_id=295832 | `rekovelle_days` | `23.0` | `14.0` |
| oocito_id=295831 | `rekovelle_days` | `23.0` | `14.0` |
| oocito_id=295830 | `rekovelle_days` | `23.0` | `14.0` |
| oocito_id=291521 | `rekovelle_days` | `26.0` | `13.0` |
| oocito_id=296653 | `rekovelle_dose` | `184.0` | `72.0` |
| oocito_id=296652 | `rekovelle_dose` | `184.0` | `72.0` |
| oocito_id=296651 | `rekovelle_dose` | `184.0` | `72.0` |
| oocito_id=296650 | `rekovelle_dose` | `184.0` | `72.0` |
| oocito_id=296649 | `rekovelle_dose` | `184.0` | `72.0` |
| oocito_id=296648 | `rekovelle_dose` | `184.0` | `72.0` |
| oocito_id=295832 | `rekovelle_dose` | `262.0` | `154.0` |
| oocito_id=295831 | `rekovelle_dose` | `262.0` | `154.0` |
| oocito_id=295830 | `rekovelle_dose` | `262.0` | `154.0` |
| oocito_id=291521 | `rekovelle_dose` | `232.0` | `116.0` |
| oocito_id=301474 | `rekovelle_end` | `2026-02-24 00:00:00` | `2026-02-24` |
| oocito_id=301473 | `rekovelle_end` | `2026-02-24 00:00:00` | `2026-02-24` |
| oocito_id=300489 | `rekovelle_end` | `2026-02-17 00:00:00` | `2026-02-17` |
| oocito_id=300488 | `rekovelle_end` | `2026-02-17 00:00:00` | `2026-02-17` |
| oocito_id=300487 | `rekovelle_end` | `2026-02-17 00:00:00` | `2026-02-17` |
| oocito_id=296653 | `rekovelle_end` | `2026-01-25 00:00:00` | `2026-01-25` |
| oocito_id=296652 | `rekovelle_end` | `2026-01-25 00:00:00` | `2026-01-25` |
| oocito_id=296651 | `rekovelle_end` | `2026-01-25 00:00:00` | `2026-01-25` |
| oocito_id=296650 | `rekovelle_end` | `2026-01-25 00:00:00` | `2026-01-25` |
| oocito_id=296649 | `rekovelle_end` | `2026-01-25 00:00:00` | `2026-01-25` |
| oocito_id=301474 | `rekovelle_start` | `2026-02-14 00:00:00` | `2026-02-14` |
| oocito_id=301473 | `rekovelle_start` | `2026-02-14 00:00:00` | `2026-02-14` |
| oocito_id=300489 | `rekovelle_start` | `2026-02-10 00:00:00` | `2026-02-10` |
| oocito_id=300488 | `rekovelle_start` | `2026-02-10 00:00:00` | `2026-02-10` |
| oocito_id=300487 | `rekovelle_start` | `2026-02-10 00:00:00` | `2026-02-10` |
| oocito_id=296653 | `rekovelle_start` | `2026-01-16 00:00:00` | `2026-01-16` |
| oocito_id=296652 | `rekovelle_start` | `2026-01-16 00:00:00` | `2026-01-16` |
| oocito_id=296651 | `rekovelle_start` | `2026-01-16 00:00:00` | `2026-01-16` |
| oocito_id=296650 | `rekovelle_start` | `2026-01-16 00:00:00` | `2026-01-16` |
| oocito_id=296649 | `rekovelle_start` | `2026-01-16 00:00:00` | `2026-01-16` |
| oocito_id=114191 | `rekovelle_unit` | `mcg` | `mg` |
| oocito_id=114190 | `rekovelle_unit` | `mcg` | `mg` |
| oocito_id=114189 | `rekovelle_unit` | `mcg` | `mg` |
| oocito_id=319427 | `somatropina_days` | `22.0` | `11.0` |
| oocito_id=319426 | `somatropina_days` | `22.0` | `11.0` |
| oocito_id=319425 | `somatropina_days` | `22.0` | `11.0` |
| oocito_id=319424 | `somatropina_days` | `22.0` | `11.0` |
| oocito_id=319423 | `somatropina_days` | `22.0` | `11.0` |
| oocito_id=319422 | `somatropina_days` | `22.0` | `11.0` |
| oocito_id=319421 | `somatropina_days` | `22.0` | `11.0` |
| oocito_id=313201 | `somatropina_days` | `22.0` | `11.0` |
| oocito_id=313200 | `somatropina_days` | `22.0` | `11.0` |
| oocito_id=313199 | `somatropina_days` | `22.0` | `11.0` |
| oocito_id=319427 | `somatropina_dose` | `26.4` | `13.2` |
| oocito_id=319426 | `somatropina_dose` | `26.4` | `13.2` |
| oocito_id=319425 | `somatropina_dose` | `26.4` | `13.2` |
| oocito_id=319424 | `somatropina_dose` | `26.4` | `13.2` |
| oocito_id=319423 | `somatropina_dose` | `26.4` | `13.2` |
| oocito_id=319422 | `somatropina_dose` | `26.4` | `13.2` |
| oocito_id=319421 | `somatropina_dose` | `26.4` | `13.2` |
| oocito_id=313201 | `somatropina_dose` | `26.4` | `13.2` |
| oocito_id=313200 | `somatropina_dose` | `26.4` | `13.2` |
| oocito_id=313199 | `somatropina_dose` | `26.4` | `13.2` |
| oocito_id=319427 | `somatropina_end` | `2026-06-20 00:00:00` | `2026-06-20` |
| oocito_id=319426 | `somatropina_end` | `2026-06-20 00:00:00` | `2026-06-20` |
| oocito_id=319425 | `somatropina_end` | `2026-06-20 00:00:00` | `2026-06-20` |
| oocito_id=319424 | `somatropina_end` | `2026-06-20 00:00:00` | `2026-06-20` |
| oocito_id=319423 | `somatropina_end` | `2026-06-20 00:00:00` | `2026-06-20` |
| oocito_id=319422 | `somatropina_end` | `2026-06-20 00:00:00` | `2026-06-20` |
| oocito_id=319421 | `somatropina_end` | `2026-06-20 00:00:00` | `2026-06-20` |
| oocito_id=313201 | `somatropina_end` | `2026-05-08 00:00:00` | `2026-05-08` |
| oocito_id=313200 | `somatropina_end` | `2026-05-08 00:00:00` | `2026-05-08` |
| oocito_id=313199 | `somatropina_end` | `2026-05-08 00:00:00` | `2026-05-08` |
| oocito_id=319427 | `somatropina_start` | `2026-06-10 00:00:00` | `2026-06-10` |
| oocito_id=319426 | `somatropina_start` | `2026-06-10 00:00:00` | `2026-06-10` |
| oocito_id=319425 | `somatropina_start` | `2026-06-10 00:00:00` | `2026-06-10` |
| oocito_id=319424 | `somatropina_start` | `2026-06-10 00:00:00` | `2026-06-10` |
| oocito_id=319423 | `somatropina_start` | `2026-06-10 00:00:00` | `2026-06-10` |
| oocito_id=319422 | `somatropina_start` | `2026-06-10 00:00:00` | `2026-06-10` |
| oocito_id=319421 | `somatropina_start` | `2026-06-10 00:00:00` | `2026-06-10` |
| oocito_id=313201 | `somatropina_start` | `2026-04-28 00:00:00` | `2026-04-28` |
| oocito_id=313200 | `somatropina_start` | `2026-04-28 00:00:00` | `2026-04-28` |
| oocito_id=313199 | `somatropina_start` | `2026-04-28 00:00:00` | `2026-04-28` |
| oocito_id=175721 | `toragesic_days` | `10.0` | `5.0` |
| oocito_id=175720 | `toragesic_days` | `10.0` | `5.0` |
| oocito_id=175721 | `toragesic_dose` | `300.0` | `150.0` |
| oocito_id=175720 | `toragesic_dose` | `300.0` | `150.0` |
| oocito_id=175732 | `toragesic_end` | `2023-12-06 00:00:00` | `2023-12-06` |
| oocito_id=175731 | `toragesic_end` | `2023-12-06 00:00:00` | `2023-12-06` |
| oocito_id=175730 | `toragesic_end` | `2023-12-06 00:00:00` | `2023-12-06` |
| oocito_id=175729 | `toragesic_end` | `2023-12-06 00:00:00` | `2023-12-06` |
| oocito_id=175728 | `toragesic_end` | `2023-12-06 00:00:00` | `2023-12-06` |
| oocito_id=175727 | `toragesic_end` | `2023-12-06 00:00:00` | `2023-12-06` |
| oocito_id=175726 | `toragesic_end` | `2023-12-06 00:00:00` | `2023-12-06` |
| oocito_id=175725 | `toragesic_end` | `2023-12-06 00:00:00` | `2023-12-06` |
| oocito_id=175724 | `toragesic_end` | `2023-12-06 00:00:00` | `2023-12-06` |
| oocito_id=175723 | `toragesic_end` | `2023-12-06 00:00:00` | `2023-12-06` |
| oocito_id=175732 | `toragesic_start` | `2023-12-02 00:00:00` | `2023-12-02` |
| oocito_id=175731 | `toragesic_start` | `2023-12-02 00:00:00` | `2023-12-02` |
| oocito_id=175730 | `toragesic_start` | `2023-12-02 00:00:00` | `2023-12-02` |
| oocito_id=175729 | `toragesic_start` | `2023-12-02 00:00:00` | `2023-12-02` |
| oocito_id=175728 | `toragesic_start` | `2023-12-02 00:00:00` | `2023-12-02` |
| oocito_id=175727 | `toragesic_start` | `2023-12-02 00:00:00` | `2023-12-02` |
| oocito_id=175726 | `toragesic_start` | `2023-12-02 00:00:00` | `2023-12-02` |
| oocito_id=175725 | `toragesic_start` | `2023-12-02 00:00:00` | `2023-12-02` |
| oocito_id=175724 | `toragesic_start` | `2023-12-02 00:00:00` | `2023-12-02` |
| oocito_id=175723 | `toragesic_start` | `2023-12-02 00:00:00` | `2023-12-02` |
| oocito_id=262688 | `trat1_bmi` | `1933.59` | `19.14` |
| oocito_id=262687 | `trat1_bmi` | `1933.59` | `19.14` |
| oocito_id=262686 | `trat1_bmi` | `1933.59` | `19.14` |
| oocito_id=262685 | `trat1_bmi` | `1933.59` | `19.14` |
| oocito_id=262684 | `trat1_bmi` | `1933.59` | `19.14` |
| oocito_id=262683 | `trat1_bmi` | `1933.59` | `19.14` |
| oocito_id=262682 | `trat1_bmi` | `1933.59` | `19.14` |
| oocito_id=262681 | `trat1_bmi` | `1933.59` | `19.14` |
| oocito_id=262680 | `trat1_bmi` | `1933.59` | `19.14` |
| oocito_id=262679 | `trat1_bmi` | `1933.59` | `19.14` |
| oocito_id=319427 | `trat1_data_inicio_inducao` | `2026-06-10 00:00:00` | `2026-06-10` |
| oocito_id=319426 | `trat1_data_inicio_inducao` | `2026-06-10 00:00:00` | `2026-06-10` |
| oocito_id=319425 | `trat1_data_inicio_inducao` | `2026-06-10 00:00:00` | `2026-06-10` |
| oocito_id=319424 | `trat1_data_inicio_inducao` | `2026-06-10 00:00:00` | `2026-06-10` |
| oocito_id=319423 | `trat1_data_inicio_inducao` | `2026-06-10 00:00:00` | `2026-06-10` |
| oocito_id=319422 | `trat1_data_inicio_inducao` | `2026-06-10 00:00:00` | `2026-06-10` |
| oocito_id=319421 | `trat1_data_inicio_inducao` | `2026-06-10 00:00:00` | `2026-06-10` |
| oocito_id=318548 | `trat1_data_inicio_inducao` | `2026-06-03 00:00:00` | `2026-06-03` |
| oocito_id=318547 | `trat1_data_inicio_inducao` | `2026-06-03 00:00:00` | `2026-06-03` |
| oocito_id=318546 | `trat1_data_inicio_inducao` | `2026-06-03 00:00:00` | `2026-06-03` |
| oocito_id=317242 | `trat1_data_transferencia` | `2026-06-11 00:00:00` | `2026-06-11` |
| oocito_id=315822 | `trat1_data_transferencia` | `2026-06-01 00:00:00` | `2026-06-01` |
| oocito_id=315821 | `trat1_data_transferencia` | `2026-06-01 00:00:00` | `2026-06-01` |
| oocito_id=315820 | `trat1_data_transferencia` | `2026-06-01 00:00:00` | `2026-06-01` |
| oocito_id=315819 | `trat1_data_transferencia` | `2026-06-01 00:00:00` | `2026-06-01` |
| oocito_id=315818 | `trat1_data_transferencia` | `2026-06-01 00:00:00` | `2026-06-01` |
| oocito_id=313942 | `trat1_data_transferencia` | `2026-05-21 00:00:00` | `2026-05-21` |
| oocito_id=313941 | `trat1_data_transferencia` | `2026-05-21 00:00:00` | `2026-05-21` |
| oocito_id=313940 | `trat1_data_transferencia` | `2026-05-21 00:00:00` | `2026-05-21` |
| oocito_id=313939 | `trat1_data_transferencia` | `2026-05-21 00:00:00` | `2026-05-21` |
| oocito_id=108493 | `trat1_id` | `15224` | `15145.0` |
| oocito_id=108492 | `trat1_id` | `15224` | `15145.0` |
| oocito_id=108491 | `trat1_id` | `15224` | `15145.0` |
| oocito_id=108490 | `trat1_id` | `15224` | `15145.0` |
| oocito_id=108489 | `trat1_id` | `15224` | `15145.0` |
| oocito_id=108488 | `trat1_id` | `15224` | `15145.0` |
| oocito_id=108487 | `trat1_id` | `15224` | `15145.0` |
| oocito_id=317716 | `trat1_motivo_nao_transferir` | `nan` | `Criopreservação total` |
| oocito_id=317715 | `trat1_motivo_nao_transferir` | `nan` | `Criopreservação total` |
| oocito_id=317714 | `trat1_motivo_nao_transferir` | `nan` | `Criopreservação total` |
| oocito_id=317713 | `trat1_motivo_nao_transferir` | `nan` | `Criopreservação total` |
| oocito_id=317712 | `trat1_motivo_nao_transferir` | `nan` | `Criopreservação total` |
| oocito_id=317711 | `trat1_motivo_nao_transferir` | `nan` | `Criopreservação total` |
| oocito_id=317710 | `trat1_motivo_nao_transferir` | `nan` | `Criopreservação total` |
| oocito_id=317709 | `trat1_motivo_nao_transferir` | `nan` | `Criopreservação total` |
| oocito_id=108493 | `trat1_motivo_nao_transferir` | `nan` | `Criopreservação total` |
| oocito_id=108492 | `trat1_motivo_nao_transferir` | `nan` | `Criopreservação total` |
| oocito_id=108493 | `trat1_previous_et` | `1` | `0.0` |
| oocito_id=108492 | `trat1_previous_et` | `1` | `0.0` |
| oocito_id=108491 | `trat1_previous_et` | `1` | `0.0` |
| oocito_id=108490 | `trat1_previous_et` | `1` | `0.0` |
| oocito_id=108489 | `trat1_previous_et` | `1` | `0.0` |
| oocito_id=108488 | `trat1_previous_et` | `1` | `0.0` |
| oocito_id=108487 | `trat1_previous_et` | `1` | `0.0` |
| oocito_id=317716 | `trat1_resultado_tratamento` | `nan` | `No transfer` |
| oocito_id=317715 | `trat1_resultado_tratamento` | `nan` | `No transfer` |
| oocito_id=317714 | `trat1_resultado_tratamento` | `nan` | `No transfer` |
| oocito_id=317713 | `trat1_resultado_tratamento` | `nan` | `No transfer` |
| oocito_id=317712 | `trat1_resultado_tratamento` | `nan` | `No transfer` |
| oocito_id=317711 | `trat1_resultado_tratamento` | `nan` | `No transfer` |
| oocito_id=317710 | `trat1_resultado_tratamento` | `nan` | `No transfer` |
| oocito_id=317709 | `trat1_resultado_tratamento` | `nan` | `No transfer` |
| oocito_id=315822 | `trat1_resultado_tratamento` | `Gestação Química` | `Gestação Clínica` |
| oocito_id=315821 | `trat1_resultado_tratamento` | `Gestação Química` | `Gestação Clínica` |
| oocito_id=108493 | `trat1_tipo_procedimento` | `Ciclo de Congelados` | `Ciclo a Fresco FIV` |
| oocito_id=108492 | `trat1_tipo_procedimento` | `Ciclo de Congelados` | `Ciclo a Fresco FIV` |
| oocito_id=108491 | `trat1_tipo_procedimento` | `Ciclo de Congelados` | `Ciclo a Fresco FIV` |
| oocito_id=108490 | `trat1_tipo_procedimento` | `Ciclo de Congelados` | `Ciclo a Fresco FIV` |
| oocito_id=108489 | `trat1_tipo_procedimento` | `Ciclo de Congelados` | `Ciclo a Fresco FIV` |
| oocito_id=108488 | `trat1_tipo_procedimento` | `Ciclo de Congelados` | `Ciclo a Fresco FIV` |
| oocito_id=108487 | `trat1_tipo_procedimento` | `Ciclo de Congelados` | `Ciclo a Fresco FIV` |
| oocito_id=308148 | `trat2_bmi` | `nan` | `21.11` |
| oocito_id=305980 | `trat2_bmi` | `nan` | `29.74` |
| oocito_id=301767 | `trat2_bmi` | `nan` | `27.34` |
| oocito_id=296168 | `trat2_bmi` | `nan` | `21.48` |
| oocito_id=294296 | `trat2_bmi` | `nan` | `21.3` |
| oocito_id=285744 | `trat2_bmi` | `nan` | `19.47` |
| oocito_id=274316 | `trat2_bmi` | `nan` | `23.05` |
| oocito_id=273413 | `trat2_bmi` | `nan` | `28.28` |
| oocito_id=273409 | `trat2_bmi` | `nan` | `28.28` |
| oocito_id=262686 | `trat2_bmi` | `1933.59` | `19.14` |
| oocito_id=311262 | `trat2_data_inicio_inducao` | `2026-05-12 00:00:00` | `2026-05-12` |
| oocito_id=308148 | `trat2_data_inicio_inducao` | `NaT` | `2026-06-10` |
| oocito_id=305980 | `trat2_data_inicio_inducao` | `NaT` | `2026-06-10` |
| oocito_id=304771 | `trat2_data_inicio_inducao` | `2026-04-18 00:00:00` | `2026-04-18` |
| oocito_id=304767 | `trat2_data_inicio_inducao` | `2026-04-18 00:00:00` | `2026-04-18` |
| oocito_id=304766 | `trat2_data_inicio_inducao` | `2026-04-18 00:00:00` | `2026-04-18` |
| oocito_id=303974 | `trat2_data_inicio_inducao` | `2026-05-18 00:00:00` | `2026-05-18` |
| oocito_id=303876 | `trat2_data_inicio_inducao` | `2026-05-25 00:00:00` | `2026-05-25` |
| oocito_id=301767 | `trat2_data_inicio_inducao` | `NaT` | `2026-06-15` |
| oocito_id=300788 | `trat2_data_inicio_inducao` | `2026-05-06 00:00:00` | `2026-05-06` |
| oocito_id=308148 | `trat2_data_transferencia` | `NaT` | `2026-06-26` |
| oocito_id=305980 | `trat2_data_transferencia` | `NaT` | `2026-06-29` |
| oocito_id=304771 | `trat2_data_transferencia` | `2026-05-04 00:00:00` | `2026-05-04` |
| oocito_id=304767 | `trat2_data_transferencia` | `2026-05-04 00:00:00` | `2026-05-04` |
| oocito_id=304766 | `trat2_data_transferencia` | `2026-05-04 00:00:00` | `2026-05-04` |
| oocito_id=303974 | `trat2_data_transferencia` | `2026-06-05 00:00:00` | `2026-06-05` |
| oocito_id=303876 | `trat2_data_transferencia` | `2026-06-08 00:00:00` | `2026-06-08` |
| oocito_id=301767 | `trat2_data_transferencia` | `NaT` | `2026-07-06` |
| oocito_id=300601 | `trat2_data_transferencia` | `2026-05-25 00:00:00` | `2026-05-25` |
| oocito_id=298207 | `trat2_data_transferencia` | `2026-03-23 00:00:00` | `2026-03-23` |
| oocito_id=308148 | `trat2_id` | `<NA>` | `44799.0` |
| oocito_id=305980 | `trat2_id` | `<NA>` | `44457.0` |
| oocito_id=301767 | `trat2_id` | `<NA>` | `44903.0` |
| oocito_id=296168 | `trat2_id` | `<NA>` | `43915.0` |
| oocito_id=294296 | `trat2_id` | `<NA>` | `45052.0` |
| oocito_id=285744 | `trat2_id` | `<NA>` | `45134.0` |
| oocito_id=274316 | `trat2_id` | `<NA>` | `43797.0` |
| oocito_id=273413 | `trat2_id` | `<NA>` | `43809.0` |
| oocito_id=273409 | `trat2_id` | `<NA>` | `43809.0` |
| oocito_id=252757 | `trat2_id` | `<NA>` | `44893.0` |
| oocito_id=236845 | `trat2_motivo_nao_transferir` | `nan` | `Outras complicações` |
| oocito_id=236842 | `trat2_motivo_nao_transferir` | `nan` | `Outras complicações` |
| oocito_id=236840 | `trat2_motivo_nao_transferir` | `nan` | `Outras complicações` |
| oocito_id=236838 | `trat2_motivo_nao_transferir` | `Outras complicações` | `` |
| oocito_id=156881 | `trat2_motivo_nao_transferir` | `nan` | `Outras complicações` |
| oocito_id=156879 | `trat2_motivo_nao_transferir` | `nan` | `Outras complicações` |
| oocito_id=105093 | `trat2_motivo_nao_transferir` | `nan` | `Ausência de embriões geneticamente normais` |
| oocito_id=308148 | `trat2_previous_et` | `<NA>` | `0.0` |
| oocito_id=305980 | `trat2_previous_et` | `<NA>` | `0.0` |
| oocito_id=301767 | `trat2_previous_et` | `<NA>` | `0.0` |
| oocito_id=296168 | `trat2_previous_et` | `<NA>` | `0.0` |
| oocito_id=294296 | `trat2_previous_et` | `<NA>` | `1.0` |
| oocito_id=285744 | `trat2_previous_et` | `<NA>` | `2.0` |
| oocito_id=274316 | `trat2_previous_et` | `<NA>` | `0.0` |
| oocito_id=273413 | `trat2_previous_et` | `<NA>` | `5.0` |
| oocito_id=273409 | `trat2_previous_et` | `<NA>` | `5.0` |
| oocito_id=258941 | `trat2_previous_et` | `6` | `5.0` |
| oocito_id=308148 | `trat2_previous_et_od` | `<NA>` | `0.0` |
| oocito_id=305980 | `trat2_previous_et_od` | `<NA>` | `0.0` |
| oocito_id=301767 | `trat2_previous_et_od` | `<NA>` | `0.0` |
| oocito_id=296168 | `trat2_previous_et_od` | `<NA>` | `0.0` |
| oocito_id=294296 | `trat2_previous_et_od` | `<NA>` | `0.0` |
| oocito_id=285744 | `trat2_previous_et_od` | `<NA>` | `0.0` |
| oocito_id=274316 | `trat2_previous_et_od` | `<NA>` | `0.0` |
| oocito_id=273413 | `trat2_previous_et_od` | `<NA>` | `0.0` |
| oocito_id=273409 | `trat2_previous_et_od` | `<NA>` | `0.0` |
| oocito_id=252757 | `trat2_previous_et_od` | `<NA>` | `0.0` |
| oocito_id=308148 | `trat2_resultado_tratamento` | `nan` | `Negativo` |
| oocito_id=305980 | `trat2_resultado_tratamento` | `nan` | `Negativo` |
| oocito_id=303974 | `trat2_resultado_tratamento` | `nan` | `Gestação Química` |
| oocito_id=300788 | `trat2_resultado_tratamento` | `nan` | `Negativo` |
| oocito_id=296168 | `trat2_resultado_tratamento` | `nan` | `Negativo` |
| oocito_id=294976 | `trat2_resultado_tratamento` | `nan` | `Gestação Química` |
| oocito_id=285387 | `trat2_resultado_tratamento` | `nan` | `Negativo` |
| oocito_id=283249 | `trat2_resultado_tratamento` | `nan` | `Gestação Química` |
| oocito_id=282898 | `trat2_resultado_tratamento` | `Gestação Química` | `Gestação Clínica` |
| oocito_id=273413 | `trat2_resultado_tratamento` | `nan` | `Gestação Clínica` |
| oocito_id=308148 | `trat2_tentativa` | `nan` | `3` |
| oocito_id=305980 | `trat2_tentativa` | `nan` | `3` |
| oocito_id=301767 | `trat2_tentativa` | `nan` | `3` |
| oocito_id=296168 | `trat2_tentativa` | `nan` | `2` |
| oocito_id=294296 | `trat2_tentativa` | `nan` | `2` |
| oocito_id=285744 | `trat2_tentativa` | `nan` | `3` |
| oocito_id=274316 | `trat2_tentativa` | `nan` | `5` |
| oocito_id=273413 | `trat2_tentativa` | `nan` | `8` |
| oocito_id=273409 | `trat2_tentativa` | `nan` | `8` |
| oocito_id=252757 | `trat2_tentativa` | `nan` | `6` |
| oocito_id=308148 | `trat2_tipo_procedimento` | `nan` | `Ciclo de Congelados` |
| oocito_id=305980 | `trat2_tipo_procedimento` | `nan` | `Ciclo de Congelados` |
| oocito_id=301767 | `trat2_tipo_procedimento` | `nan` | `Ciclo de Congelados` |
| oocito_id=296168 | `trat2_tipo_procedimento` | `nan` | `Ciclo de Congelados` |
| oocito_id=294296 | `trat2_tipo_procedimento` | `nan` | `Ciclo de Congelados` |
| oocito_id=285744 | `trat2_tipo_procedimento` | `nan` | `Ciclo de Congelados` |
| oocito_id=274316 | `trat2_tipo_procedimento` | `nan` | `Ciclo de Congelados` |
| oocito_id=273413 | `trat2_tipo_procedimento` | `nan` | `Ciclo de Congelados` |
| oocito_id=273409 | `trat2_tipo_procedimento` | `nan` | `Ciclo de Congelados` |
| oocito_id=252757 | `trat2_tipo_procedimento` | `nan` | `Ciclo de Congelados` |
| oocito_id=262449 | `type_of_delivery` | `nan` | `Cesarean section` |
| oocito_id=251147 | `type_of_delivery` | `nan` | `VAGINAL` |
| oocito_id=251141 | `type_of_delivery` | `nan` | `VAGINAL` |
| oocito_id=246960 | `type_of_delivery` | `nan` | `Cesarean section` |
| oocito_id=246959 | `type_of_delivery` | `nan` | `Cesarean section` |
| oocito_id=246958 | `type_of_delivery` | `nan` | `Cesarean section` |
| oocito_id=246957 | `type_of_delivery` | `nan` | `Cesarean section` |
| oocito_id=246956 | `type_of_delivery` | `nan` | `Cesarean section` |
| oocito_id=239383 | `type_of_delivery` | `nan` | `Cesarean section` |
| oocito_id=209228 | `type_of_delivery` | `nan` | `Cesarean section` |
| oocito_id=317242 | `utrogestan_days` | `18.0` | `6.0` |
| oocito_id=315822 | `utrogestan_days` | `18.0` | `6.0` |
| oocito_id=315821 | `utrogestan_days` | `18.0` | `6.0` |
| oocito_id=315820 | `utrogestan_days` | `18.0` | `6.0` |
| oocito_id=315819 | `utrogestan_days` | `18.0` | `6.0` |
| oocito_id=315818 | `utrogestan_days` | `18.0` | `6.0` |
| oocito_id=313942 | `utrogestan_days` | `24.0` | `6.0` |
| oocito_id=313941 | `utrogestan_days` | `24.0` | `6.0` |
| oocito_id=313940 | `utrogestan_days` | `24.0` | `6.0` |
| oocito_id=313939 | `utrogestan_days` | `24.0` | `6.0` |
| oocito_id=317242 | `utrogestan_dose` | `36.0` | `12.0` |
| oocito_id=315822 | `utrogestan_dose` | `7200.0` | `2400.0` |
| oocito_id=315821 | `utrogestan_dose` | `7200.0` | `2400.0` |
| oocito_id=315820 | `utrogestan_dose` | `7200.0` | `2400.0` |
| oocito_id=315819 | `utrogestan_dose` | `7200.0` | `2400.0` |
| oocito_id=315818 | `utrogestan_dose` | `7200.0` | `2400.0` |
| oocito_id=313942 | `utrogestan_dose` | `19200.0` | `4800.0` |
| oocito_id=313941 | `utrogestan_dose` | `19200.0` | `4800.0` |
| oocito_id=313940 | `utrogestan_dose` | `19200.0` | `4800.0` |
| oocito_id=313939 | `utrogestan_dose` | `19200.0` | `4800.0` |
| oocito_id=317242 | `utrogestan_end` | `2026-06-11 00:00:00` | `2026-06-11` |
| oocito_id=315822 | `utrogestan_end` | `2026-06-01 00:00:00` | `2026-06-01` |
| oocito_id=315821 | `utrogestan_end` | `2026-06-01 00:00:00` | `2026-06-01` |
| oocito_id=315820 | `utrogestan_end` | `2026-06-01 00:00:00` | `2026-06-01` |
| oocito_id=315819 | `utrogestan_end` | `2026-06-01 00:00:00` | `2026-06-01` |
| oocito_id=315818 | `utrogestan_end` | `2026-06-01 00:00:00` | `2026-06-01` |
| oocito_id=313942 | `utrogestan_end` | `2026-05-21 00:00:00` | `2026-05-21` |
| oocito_id=313941 | `utrogestan_end` | `2026-05-21 00:00:00` | `2026-05-21` |
| oocito_id=313940 | `utrogestan_end` | `2026-05-21 00:00:00` | `2026-05-21` |
| oocito_id=313939 | `utrogestan_end` | `2026-05-21 00:00:00` | `2026-05-21` |
| oocito_id=300604 | `utrogestan_interval` | `8.0` | `12.0` |
| oocito_id=300603 | `utrogestan_interval` | `8.0` | `12.0` |
| oocito_id=300602 | `utrogestan_interval` | `8.0` | `12.0` |
| oocito_id=300601 | `utrogestan_interval` | `8.0` | `12.0` |
| oocito_id=132186 | `utrogestan_interval` | `12.0` | `24.0` |
| oocito_id=132185 | `utrogestan_interval` | `12.0` | `24.0` |
| oocito_id=132184 | `utrogestan_interval` | `12.0` | `24.0` |
| oocito_id=132183 | `utrogestan_interval` | `12.0` | `24.0` |
| oocito_id=132182 | `utrogestan_interval` | `12.0` | `24.0` |
| oocito_id=132181 | `utrogestan_interval` | `12.0` | `24.0` |
| oocito_id=317242 | `utrogestan_start` | `2026-06-06 00:00:00` | `2026-06-06` |
| oocito_id=315822 | `utrogestan_start` | `2026-05-27 00:00:00` | `2026-05-27` |
| oocito_id=315821 | `utrogestan_start` | `2026-05-27 00:00:00` | `2026-05-27` |
| oocito_id=315820 | `utrogestan_start` | `2026-05-27 00:00:00` | `2026-05-27` |
| oocito_id=315819 | `utrogestan_start` | `2026-05-27 00:00:00` | `2026-05-27` |
| oocito_id=315818 | `utrogestan_start` | `2026-05-27 00:00:00` | `2026-05-27` |
| oocito_id=313942 | `utrogestan_start` | `2026-05-16 00:00:00` | `2026-05-16` |
| oocito_id=313941 | `utrogestan_start` | `2026-05-16 00:00:00` | `2026-05-16` |
| oocito_id=313940 | `utrogestan_start` | `2026-05-16 00:00:00` | `2026-05-16` |
| oocito_id=313939 | `utrogestan_start` | `2026-05-16 00:00:00` | `2026-05-16` |
| oocito_id=119418 | `utrogestan_unit` | `NÃO DEFINIDO` | `cap` |
| oocito_id=119417 | `utrogestan_unit` | `NÃO DEFINIDO` | `cap` |
| oocito_id=115046 | `utrogestan_unit` | `NÃO DEFINIDO` | `cap` |
| oocito_id=115045 | `utrogestan_unit` | `NÃO DEFINIDO` | `cap` |
| oocito_id=115044 | `utrogestan_unit` | `NÃO DEFINIDO` | `cap` |
| oocito_id=115043 | `utrogestan_unit` | `NÃO DEFINIDO` | `cap` |
| oocito_id=108493 | `utrogestan_unit` | `cap` | `nan` |
| oocito_id=108492 | `utrogestan_unit` | `cap` | `nan` |
| oocito_id=108491 | `utrogestan_unit` | `cap` | `nan` |
| oocito_id=108490 | `utrogestan_unit` | `cap` | `nan` |
| oocito_id=133991 | `viagra_end` | `2023-03-25 00:00:00` | `2023-03-25` |
| oocito_id=133990 | `viagra_end` | `2023-03-25 00:00:00` | `2023-03-25` |
| oocito_id=133989 | `viagra_end` | `2023-03-25 00:00:00` | `2023-03-25` |
| oocito_id=133988 | `viagra_end` | `2023-03-25 00:00:00` | `2023-03-25` |
| oocito_id=133987 | `viagra_end` | `2023-03-25 00:00:00` | `2023-03-25` |
| oocito_id=133986 | `viagra_end` | `2023-03-25 00:00:00` | `2023-03-25` |
| oocito_id=133985 | `viagra_end` | `2023-03-25 00:00:00` | `2023-03-25` |
| oocito_id=133984 | `viagra_end` | `2023-03-25 00:00:00` | `2023-03-25` |
| oocito_id=133983 | `viagra_end` | `2023-03-25 00:00:00` | `2023-03-25` |
| oocito_id=133991 | `viagra_start` | `2023-03-15 00:00:00` | `2023-03-15` |
| oocito_id=133990 | `viagra_start` | `2023-03-15 00:00:00` | `2023-03-15` |
| oocito_id=133989 | `viagra_start` | `2023-03-15 00:00:00` | `2023-03-15` |
| oocito_id=133988 | `viagra_start` | `2023-03-15 00:00:00` | `2023-03-15` |
| oocito_id=133987 | `viagra_start` | `2023-03-15 00:00:00` | `2023-03-15` |
| oocito_id=133986 | `viagra_start` | `2023-03-15 00:00:00` | `2023-03-15` |
| oocito_id=133985 | `viagra_start` | `2023-03-15 00:00:00` | `2023-03-15` |
| oocito_id=133984 | `viagra_start` | `2023-03-15 00:00:00` | `2023-03-15` |
| oocito_id=133983 | `viagra_start` | `2023-03-15 00:00:00` | `2023-03-15` |

### `embryoscope_clinisys_combined`
#### **Schema Mismatches**
* Columns in DuckDB Only: `embryo_Name_BlastExpandLast`, `embryo_Name_BlastomereSize`, `embryo_Name_EVEN2`, `embryo_Name_EVEN4`, `embryo_Name_EVEN8`, `embryo_Name_FRAG2`, `embryo_Name_FRAG2CAT`, `embryo_Name_FRAG4`, `embryo_Name_FRAG8`, `embryo_Name_Fragmentation`, `embryo_Name_ICM`, `embryo_Name_Line`, `embryo_Name_MN2Type`, `embryo_Name_MorphologicalGrade`, `embryo_Name_MultiNucleation`, `embryo_Name_Nuclei2`, `embryo_Name_PN`, `embryo_Name_Pulsing`, `embryo_Name_ReexpansionCount`, `embryo_Name_Strings`, `embryo_Name_TE`, `embryo_Name_t2`, `embryo_Name_t3`, `embryo_Name_t4`, `embryo_Name_t5`, `embryo_Name_t6`, `embryo_Name_t7`, `embryo_Name_t8`, `embryo_Name_t9`, `embryo_Name_tB`, `embryo_Name_tEB`, `embryo_Name_tHB`, `embryo_Name_tM`, `embryo_Name_tPB2`, `embryo_Name_tPNa`, `embryo_Name_tPNf`, `embryo_Name_tSB`, `embryo_Name_tSC`, `embryo_Time_BlastExpandLast`, `embryo_Time_BlastomereSize`, `embryo_Time_EVEN2`, `embryo_Time_EVEN4`, `embryo_Time_EVEN8`, `embryo_Time_FRAG2`, `embryo_Time_FRAG2CAT`, `embryo_Time_FRAG4`, `embryo_Time_FRAG8`, `embryo_Time_ICM`, `embryo_Time_Line`, `embryo_Time_MN2Type`, `embryo_Time_MorphologicalGrade`, `embryo_Time_MultiNucleation`, `embryo_Time_Nuclei2`, `embryo_Time_PN`, `embryo_Time_Pulsing`, `embryo_Time_ReexpansionCount`, `embryo_Time_Strings`, `embryo_Time_TE`, `embryo_Time_t6`, `embryo_Time_t7`, `embryo_Time_t9`, `embryo_Time_tPB2`, `embryo_Timestamp_BlastExpandLast`, `embryo_Timestamp_BlastomereSize`, `embryo_Timestamp_EVEN2`, `embryo_Timestamp_EVEN4`, `embryo_Timestamp_EVEN8`, `embryo_Timestamp_FRAG2`, `embryo_Timestamp_FRAG2CAT`, `embryo_Timestamp_FRAG4`, `embryo_Timestamp_FRAG8`, `embryo_Timestamp_Fragmentation`, `embryo_Timestamp_ICM`, `embryo_Timestamp_Line`, `embryo_Timestamp_MN2Type`, `embryo_Timestamp_MorphologicalGrade`, `embryo_Timestamp_MultiNucleation`, `embryo_Timestamp_Nuclei2`, `embryo_Timestamp_PN`, `embryo_Timestamp_Pulsing`, `embryo_Timestamp_ReexpansionCount`, `embryo_Timestamp_Strings`, `embryo_Timestamp_TE`, `embryo_Timestamp_t2`, `embryo_Timestamp_t3`, `embryo_Timestamp_t4`, `embryo_Timestamp_t5`, `embryo_Timestamp_t6`, `embryo_Timestamp_t7`, `embryo_Timestamp_t8`, `embryo_Timestamp_t9`, `embryo_Timestamp_tB`, `embryo_Timestamp_tEB`, `embryo_Timestamp_tHB`, `embryo_Timestamp_tM`, `embryo_Timestamp_tPB2`, `embryo_Timestamp_tPNa`, `embryo_Timestamp_tPNf`, `embryo_Timestamp_tSB`, `embryo_Timestamp_tSC`, `embryo_Value_BlastExpandLast`, `embryo_Value_BlastomereSize`, `embryo_Value_EVEN2`, `embryo_Value_EVEN4`, `embryo_Value_EVEN8`, `embryo_Value_FRAG2`, `embryo_Value_FRAG4`, `embryo_Value_FRAG8`, `embryo_Value_Fragmentation`, `embryo_Value_Line`, `embryo_Value_MorphologicalGrade`, `embryo_Value_MultiNucleation`, `embryo_Value_Strings`, `embryo_Value_t2`, `embryo_Value_t3`, `embryo_Value_t4`, `embryo_Value_t5`, `embryo_Value_t6`, `embryo_Value_t7`, `embryo_Value_t8`, `embryo_Value_t9`, `embryo_Value_tB`, `embryo_Value_tEB`, `embryo_Value_tHB`, `embryo_Value_tM`, `embryo_Value_tPB2`, `embryo_Value_tPNa`, `embryo_Value_tPNf`, `embryo_Value_tSB`, `embryo_Value_tSC`, `idascore_IDAScore`, `idascore_IDATime`, `idascore_IDAVersion`, `micro_CicloDoadora`, `micro_recepcao_ovulos`, `patient_DateOfBirth`, `patient_FirstName`, `patient_LastName`, `patient_PatientID`, `patient_PatientIDx`, `patient_YearOfBirth`, `patient_unit_huntington`, `prontuario`, `trat1_idade_esposa`, `trat1_idade_marido`, `trat1_prontuario_doadora`, `trat2_idade_esposa`, `trat2_idade_marido`, `trat2_prontuario_doadora`, `treatment_TreatmentName`
* Columns in Athena Only: `embryo_first_name`, `embryo_last_name`, `embryo_patient_date_of_birth`, `embryo_patient_id`, `embryo_patient_id_x`, `embryo_patient_sk`, `embryo_patient_year_of_birth`, `embryo_prontuario`, `embryo_treatment_name`, `embryo_unit_huntington`
#### **Discrepancy Group Isolation**
* **By Year/Date**:
  * Year **1981**: 3 discrepant records
  * Year **2000**: 2 discrepant records
  * Year **2010**: 23 discrepant records
  * Year **2011**: 22 discrepant records
  * Year **2012**: 6 discrepant records
  * Year **2016**: 32 discrepant records
  * Year **2017**: 12 discrepant records
  * Year **2019**: 52 discrepant records
  * Year **2020**: 778 discrepant records
  * Year **2021**: 9638 discrepant records
  * Year **2022**: 15693 discrepant records
  * Year **2023**: 15335 discrepant records
  * Year **2024**: 16986 discrepant records
  * Year **2025**: 16244 discrepant records
  * Year **2026**: 5948 discrepant records
  * Year **N/A**: 132254 discrepant records
* **By Dimension Group**:
  * Group ** Adrianne Mary Leao Sette e Oliveira**: 10 discrepant records
  * Group **Alex  Sander Miguel**: 137 discrepant records
  * Group **Alex Sander Jose Miguel**: 8 discrepant records
  * Group **Alexander Kopelman**: 759 discrepant records
  * Group **Alice Campos de Pinho Tavares **: 5 discrepant records
  * Group **Aléssio Calil Mathias**: 4 discrepant records
  * Group **Amanda Matos **: 6 discrepant records
  * Group **Amanda Oliveira Cutalo Prates**: 422 discrepant records
  * Group **Ana Claudia Moura Trigo**: 599 discrepant records
  * Group **Ana Lucia Beltrame**: 431 discrepant records
  * Group **Ana Luiza Alvarenga Gomes**: 72 discrepant records
  * Group **Ana Luiza Mattos Tavares**: 1598 discrepant records
  * Group **Ana Luiza Nunes**: 1619 discrepant records
  * Group **Ana Paula Aquino**: 36747 discrepant records
  * Group **Ana Rita de Paiva Toledo**: 242 discrepant records
  * Group **Andre Giannini Rodrigues**: 11 discrepant records
  * Group **Andrea de Fatima Castro**: 421 discrepant records
  * Group **Anelisa Bueno Pereira**: 82 discrepant records
  * Group **Anna Luiza Moraes Souza**: 43 discrepant records
  * Group **Antonio Hochgreb De Freitas Junior**: 36 discrepant records
  * Group **Arnaldo S Cambiaghi**: 4767 discrepant records
  * Group **Arnaldo Schizzi Cambiaghi**: 6396 discrepant records
  * Group **Barbara Souza Melo**: 1358 discrepant records
  * Group **Beatriz Cabral Pires**: 18 discrepant records
  * Group **Beatriz Passaro Biscaro**: 794 discrepant records
  * Group **Beatriz Pavin de Toledo**: 104 discrepant records
  * Group **Benilson Eustaqio de Souza**: 36 discrepant records
  * Group **Bruna Barros Cavalcante**: 924 discrepant records
  * Group **Bruna Costa Queiroz**: 617 discrepant records
  * Group **Bruna Cristina Lobo Santos**: 616 discrepant records
  * Group **Bruna Lobo Santos**: 168 discrepant records
  * Group **Camila Campos**: 269 discrepant records
  * Group **Camila Schizzi Cambiaghi**: 353 discrepant records
  * Group **Carla Maria Franco Dias **: 157 discrepant records
  * Group **Carla Martins**: 736 discrepant records
  * Group **Carlos Augusto Vieira de Moraes Filho**: 37 discrepant records
  * Group **Carolina Andrade Guedes dos Santos**: 6 discrepant records
  * Group **Carolina Zendron Machado Rudge**: 12 discrepant records
  * Group **Carolina de Andrade Melo e Souza**: 390 discrepant records
  * Group **Cesar Augusto Ferrari Teixeira**: 48 discrepant records
  * Group **Claudia Gomes Padilla**: 3590 discrepant records
  * Group **Cybele Lopes da Silva Lascala**: 11 discrepant records
  * Group **Daniel Suslik Zylbersztejn**: 61 discrepant records
  * Group **Daniela Boechat Gomide**: 1 discrepant records
  * Group **Daniela de Lima e Montes Castanho**: 97 discrepant records
  * Group **Daniella Spilborghs Castellotti**: 8594 discrepant records
  * Group **Davi Vischi Paluello**: 3 discrepant records
  * Group **Dayana Couto**: 1169 discrepant records
  * Group **Drauzio Oppenheimer**: 7 discrepant records
  * Group **Eduardo Leme Alves da Motta**: 6743 discrepant records
  * Group **Eduardo Yoneyama Mourthe**: 9 discrepant records
  * Group **Erica Becker de Sousa Xavier**: 7290 discrepant records
  * Group **Fabio Aiello Padilla**: 11 discrepant records
  * Group **Fabio Padilla**: 6 discrepant records
  * Group **Fabiola Cesconeto**: 111 discrepant records
  * Group **Fabyanne Mazutti da Silva **: 262 discrepant records
  * Group **Fernanda Marques Luz da Ressurreição**: 9 discrepant records
  * Group **Fernanda Peres Mastrocola**: 327 discrepant records
  * Group **Fernanda de Paula Rodrigues**: 2601 discrepant records
  * Group **Fernando Barboza De Lima **: 177 discrepant records
  * Group **Flavia Torelli**: 1059 discrepant records
  * Group **Flavio Ramirez Rosário**: 25 discrepant records
  * Group **Frederico Jose Silva Correa**: 700 discrepant records
  * Group **Fábio Costa Peixoto**: 4860 discrepant records
  * Group **Gabriela Franco Mourao de Oliveira**: 14 discrepant records
  * Group **Gabriela de Oliveira Ribeiro Lima**: 62 discrepant records
  * Group **Gabriella Dória Monteiro Cardoso**: 13 discrepant records
  * Group **Gabriella de Oliveira Ferreira**: 996 discrepant records
  * Group **Geraldo Caldeira**: 10830 discrepant records
  * Group **Gersia Araujo Viana**: 2015 discrepant records
  * Group **Giuliana Gatto**: 309 discrepant records
  * Group **Guilherme Jacom Abdulmassih Wood**: 11 discrepant records
  * Group **Guilherme Leme de Souza**: 37 discrepant records
  * Group **Gustavo Comodo**: 254 discrepant records
  * Group **Gustavo Nardini Cecchino**: 2 discrepant records
  * Group **Gustavo Teles**: 944 discrepant records
  * Group **Hanna Park**: 1628 discrepant records
  * Group **Helio Haddad Filho**: 359 discrepant records
  * Group **Herica Cristina Mendonça**: 6799 discrepant records
  * Group **Joao Frederico Luciano de Mello**: 9 discrepant records
  * Group **Joao Oscar Almeida Falcao Junior**: 12 discrepant records
  * Group **Joaquim Roberto Costa Lopes**: 1326 discrepant records
  * Group **Jorge Hallak**: 25 discrepant records
  * Group **Jose Geraldo Alves Caldeira**: 1609 discrepant records
  * Group **Josenice de Araujo SIlva Gomes**: 147 discrepant records
  * Group **João Pedro Junqueira Caetano**: 7431 discrepant records
  * Group **Juliana Halley Hatty**: 89 discrepant records
  * Group **Karen de Lima Souza Ortiz**: 28 discrepant records
  * Group **Karina de Sa Adami**: 35 discrepant records
  * Group **Karla Giusti Zacharias Fantin**: 1 discrepant records
  * Group **Keila Veludo Santos**: 18 discrepant records
  * Group **Laura Maria Almeida Maia**: 3303 discrepant records
  * Group **Lauriane G Schmidt De Abreu**: 163 discrepant records
  * Group **Layza Borges**: 3366 discrepant records
  * Group **Leci Veiga Caetano Amorim**: 5424 discrepant records
  * Group **Leonardo Araripe Dantas**: 108 discrepant records
  * Group **Leonardo Matheus Ribeiro Pereira**: 606 discrepant records
  * Group **Leticia Couto Motta Piccolo**: 18 discrepant records
  * Group **Lillian Silvestre Califre**: 11 discrepant records
  * Group **Livia Munhoz**: 3529 discrepant records
  * Group **Luana Lopes Toledo**: 730 discrepant records
  * Group **Luciana Campomizzi Calazans**: 5052 discrepant records
  * Group **Luciana Ferreira Potiguara Amador Sousa**: 786 discrepant records
  * Group **Luiz Fernando Bellintani **: 2 discrepant records
  * Group **Luiz Fernando Pina de Carvalho**: 50 discrepant records
  * Group **Manoela Porto Silva Resende**: 11 discrepant records
  * Group **Marcelo Afonso Goncalves**: 66 discrepant records
  * Group **Marcelo Afonso Gonçalves**: 414 discrepant records
  * Group **Marcelo Lopes Cancado**: 1827 discrepant records
  * Group **Marcos Eiji Shiroma**: 2206 discrepant records
  * Group **Marcos Eji Shiroma**: 208 discrepant records
  * Group **Maria Augusta Engler Tamm de Lima - VM**: 320 discrepant records
  * Group **Maria Juliana Albuquerque**: 2082 discrepant records
  * Group **Mariana Santana de A L Yoshida**: 14 discrepant records
  * Group **Mariana Santana de Almeida Liporoni Yoshida**: 7 discrepant records
  * Group **Marina de Melo Mendes**: 89 discrepant records
  * Group **Marjorie Fasolin **: 928 discrepant records
  * Group **Matheus Teixeira Roque**: 1493 discrepant records
  * Group **Mauricio Barbour Chehin**: 399 discrepant records
  * Group **Mauricio Chehin**: 6807 discrepant records
  * Group **Melissa Cavagnoli**: 159 discrepant records
  * Group **Michele Quaranta Panzan **: 3206 discrepant records
  * Group **Médico Externo**: 84 discrepant records
  * Group **Médico TI Huntington**: 5 discrepant records
  * Group **Mônica de Oliveira Jorge**: 2 discrepant records
  * Group **N/A**: 3976 discrepant records
  * Group **Nina Rotsen Santos Ferreira**: 9 discrepant records
  * Group **Patricia Santos Marques**: 256 discrepant records
  * Group **Paula Beatriz (desativado)**: 12 discrepant records
  * Group **Paula Beatriz Tavares Fettback**: 6 discrepant records
  * Group **Paula Bortolai Martins Araujo**: 4387 discrepant records
  * Group **Paula Vieira Nunes Brito**: 538 discrepant records
  * Group **Paulo Homem de Mello Bianchi**: 15 discrepant records
  * Group **Pedro Ivan de Almeida Pretti**: 10 discrepant records
  * Group **Pedro Paulo Bastos Filho**: 96 discrepant records
  * Group **Priscila Morais Galvão Sousa**: 68 discrepant records
  * Group **Priscilla Lopes Caldeira**: 236 discrepant records
  * Group **Pró-FIV**: 1239 discrepant records
  * Group **RAIMUNDO  CESAR PINHEIRO**: 22 discrepant records
  * Group **Rafael Lacordia**: 1700 discrepant records
  * Group **Raimundo Cesar Pinheiro**: 10 discrepant records
  * Group **Raphaela Menin Franco Martins**: 1526 discrepant records
  * Group **Renata Fioravanti Schaal**: 191 discrepant records
  * Group **Renato Fraietta**: 145 discrepant records
  * Group **Ricardo Barini**: 10 discrepant records
  * Group **Ricardo Mello Marinho**: 2697 discrepant records
  * Group **Rodrigo da Rosa Filho**: 20 discrepant records
  * Group **Rogerio de Barros Ferreira Leao**: 6347 discrepant records
  * Group **Samara Laham**: 94 discrepant records
  * Group **Simone Portugal Silva Lima**: 487 discrepant records
  * Group **Sofia Andrade de Oliveira**: 3468 discrepant records
  * Group **Stephanie Majer Franceschini Cinquetti**: 5 discrepant records
  * Group **Tatiana Magalhães Aguiar **: 47 discrepant records
  * Group **Tatianna Quintas Furtado Ribeiro**: 53 discrepant records
  * Group **Thais Cambiaghi**: 43 discrepant records
  * Group **Thais Sanches Domingues**: 6940 discrepant records
  * Group **Thomas Gabriel Miklos**: 14 discrepant records
  * Group **Valentina Nascimento Cotrim**: 1000 discrepant records
  * Group **Victoria Furquim Werneck Marinho**: 31 discrepant records
  * Group **Wendy Delmondes Galvão**: 29 discrepant records
  * Group **Zuleica Antunes Guimarães Cardoso **: 42 discrepant records

#### **Anonymized Column Discrepancy Samples**
| Composite Key | Column Name | DuckDB Value | Athena Value |
| :--- | :--- | :--- | :--- |
| oocito_id=10201, emb_cong_id=null, embryo_embryo_id=d2021.08.06_s00833_i4120_p-2 | `embryo_description` | `nan` | `NAO HOUVE EXTRUSAO DO 2CP` |
| oocito_id=62156, emb_cong_id=null, embryo_embryo_id=d2019.07.26_s00080_i3254_p-2 | `embryo_description` | `nan` | `NF` |
| oocito_id=62157, emb_cong_id=null, embryo_embryo_id=d2019.07.26_s00080_i3254_p-3 | `embryo_description` | `nan` | `DEG` |
| oocito_id=122944, emb_cong_id=null, embryo_embryo_id=d2023.01.20_s01733_i4120_p-1 | `embryo_description` | `nan` | `BAIXO RISCO - ANEUPLOIDE COMPLEXO` |
| oocito_id=129920, emb_cong_id=null, embryo_embryo_id=d2023.02.21_s01775_i4120_p-7 | `embryo_description` | `nan` | `ANEUPLOIDE COMPLEXO XX` |
| oocito_id=130028, emb_cong_id=null, embryo_embryo_id=d2023.02.22_s01780_i4120_p-3 | `embryo_description` | `nan` | `Alterado complexo, Feminino` |
| oocito_id=253535, emb_cong_id=null, embryo_embryo_id=d2025.05.10_s02919_i3254_p-1 | `embryo_embryo_date` | `2025-05-10 00:00:00` | `2025-05-10` |
| oocito_id=253537, emb_cong_id=null, embryo_embryo_id=d2025.05.10_s02919_i3254_p-3 | `embryo_embryo_date` | `2025-05-10 00:00:00` | `2025-05-10` |
| oocito_id=253562, emb_cong_id=null, embryo_embryo_id=d2025.05.10_s02920_i3254_p-1 | `embryo_embryo_date` | `2025-05-10 00:00:00` | `2025-05-10` |
| oocito_id=253567, emb_cong_id=null, embryo_embryo_id=d2025.05.10_s02920_i3254_p-6 | `embryo_embryo_date` | `2025-05-10 00:00:00` | `2025-05-10` |
| oocito_id=253521, emb_cong_id=null, embryo_embryo_id=d2025.05.10_s02921_i3254_p-2 | `embryo_embryo_date` | `2025-05-10 00:00:00` | `2025-05-10` |
| oocito_id=253522, emb_cong_id=null, embryo_embryo_id=d2025.05.10_s02921_i3254_p-3 | `embryo_embryo_date` | `2025-05-10 00:00:00` | `2025-05-10` |
| oocito_id=253523, emb_cong_id=null, embryo_embryo_id=d2025.05.10_s02921_i3254_p-4 | `embryo_embryo_date` | `2025-05-10 00:00:00` | `2025-05-10` |
| oocito_id=253525, emb_cong_id=null, embryo_embryo_id=d2025.05.10_s02921_i3254_p-6 | `embryo_embryo_date` | `2025-05-10 00:00:00` | `2025-05-10` |
| oocito_id=253911, emb_cong_id=null, embryo_embryo_id=d2025.05.13_s02922_i3254_p-1 | `embryo_embryo_date` | `2025-05-13 00:00:00` | `2025-05-13` |
| oocito_id=253912, emb_cong_id=null, embryo_embryo_id=d2025.05.13_s02922_i3254_p-2 | `embryo_embryo_date` | `2025-05-13 00:00:00` | `2025-05-13` |
| oocito_id=264194, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-1 | `embryo_embryo_fate` | `Unknown` | `Avoid` |
| oocito_id=264196, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-3 | `embryo_embryo_fate` | `Unknown` | `Avoid` |
| oocito_id=264197, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-4 | `embryo_embryo_fate` | `Unknown` | `Avoid` |
| oocito_id=264198, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-5 | `embryo_embryo_fate` | `Unknown` | `Avoid` |
| oocito_id=264202, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-9 | `embryo_embryo_fate` | `Unknown` | `Avoid` |
| oocito_id=264203, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-10 | `embryo_embryo_fate` | `Unknown` | `Avoid` |
| oocito_id=264204, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-11 | `embryo_embryo_fate` | `Unknown` | `Avoid` |
| oocito_id=264794, emb_cong_id=null, embryo_embryo_id=d2025.07.17_s03039_i3254_p-3 | `embryo_embryo_fate` | `Unknown` | `Avoid` |
| oocito_id=264814, emb_cong_id=null, embryo_embryo_id=d2025.07.17_s03043_i3254_p-1 | `embryo_embryo_fate` | `Unknown` | `Avoid` |
| oocito_id=264815, emb_cong_id=null, embryo_embryo_id=d2025.07.17_s03043_i3254_p-2 | `embryo_embryo_fate` | `Unknown` | `Avoid` |
| oocito_id=255661, emb_cong_id=null, embryo_embryo_id=d2021.06.01_s00709_i4120_p-1 | `embryo_fertilization_method` | `nan` | `ICSI` |
| oocito_id=255662, emb_cong_id=null, embryo_embryo_id=d2021.06.01_s00709_i4120_p-2 | `embryo_fertilization_method` | `nan` | `ICSI` |
| oocito_id=255663, emb_cong_id=null, embryo_embryo_id=d2021.06.01_s00709_i4120_p-3 | `embryo_fertilization_method` | `nan` | `ICSI` |
| oocito_id=255664, emb_cong_id=null, embryo_embryo_id=d2021.06.01_s00709_i4120_p-4 | `embryo_fertilization_method` | `nan` | `ICSI` |
| oocito_id=255665, emb_cong_id=null, embryo_embryo_id=d2021.06.01_s00709_i4120_p-5 | `embryo_fertilization_method` | `nan` | `ICSI` |
| oocito_id=255666, emb_cong_id=null, embryo_embryo_id=d2021.06.01_s00709_i4120_p-6 | `embryo_fertilization_method` | `nan` | `ICSI` |
| oocito_id=255667, emb_cong_id=null, embryo_embryo_id=d2021.06.01_s00709_i4120_p-7 | `embryo_fertilization_method` | `nan` | `ICSI` |
| oocito_id=255668, emb_cong_id=null, embryo_embryo_id=d2021.06.01_s00709_i4120_p-8 | `embryo_fertilization_method` | `nan` | `ICSI` |
| oocito_id=255669, emb_cong_id=null, embryo_embryo_id=d2021.06.01_s00709_i4120_p-9 | `embryo_fertilization_method` | `nan` | `ICSI` |
| oocito_id=255670, emb_cong_id=null, embryo_embryo_id=d2021.06.01_s00709_i4120_p-10 | `embryo_fertilization_method` | `nan` | `ICSI` |
| oocito_id=264181, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s03038_i3254_p-2 | `embryo_kid_date` | `NaT` | `2025-07-24 00:00:00` |
| oocito_id=264794, emb_cong_id=null, embryo_embryo_id=d2025.07.17_s03039_i3254_p-3 | `embryo_kid_date` | `NaT` | `2025-07-22 00:00:00` |
| oocito_id=264183, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s03038_i3254_p-4 | `embryo_kid_date` | `NaT` | `2025-07-24 00:00:00` |
| oocito_id=264184, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s03038_i3254_p-5 | `embryo_kid_date` | `NaT` | `2025-07-24 00:00:00` |
| oocito_id=264185, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s03038_i3254_p-6 | `embryo_kid_date` | `NaT` | `2025-07-24 00:00:00` |
| oocito_id=264828, emb_cong_id=null, embryo_embryo_id=d2025.07.17_s03040_i3254_p-6 | `embryo_kid_date` | `NaT` | `2025-07-29 00:00:00` |
| oocito_id=264829, emb_cong_id=null, embryo_embryo_id=d2025.07.17_s03040_i3254_p-7 | `embryo_kid_date` | `NaT` | `2025-07-29 00:00:00` |
| oocito_id=269376, emb_cong_id=null, embryo_embryo_id=d2025.08.14_s03082_i3254_p-2 | `embryo_kid_date` | `2025-08-19 00:00:00` | `2025-08-20 00:00:00` |
| oocito_id=269377, emb_cong_id=null, embryo_embryo_id=d2025.08.14_s03082_i3254_p-3 | `embryo_kid_date` | `2025-08-19 00:00:00` | `2025-08-20 00:00:00` |
| oocito_id=269375, emb_cong_id=null, embryo_embryo_id=d2025.08.14_s03082_i3254_p-1 | `embryo_kid_date` | `2025-08-19 00:00:00` | `2025-08-20 00:00:00` |
| oocito_id=264181, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s03038_i3254_p-2 | `embryo_kid_score` | `nan` | `1.7` |
| oocito_id=264794, emb_cong_id=null, embryo_embryo_id=d2025.07.17_s03039_i3254_p-3 | `embryo_kid_score` | `nan` | `NA` |
| oocito_id=264183, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s03038_i3254_p-4 | `embryo_kid_score` | `nan` | `1.7` |
| oocito_id=264184, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s03038_i3254_p-5 | `embryo_kid_score` | `nan` | `1.7` |
| oocito_id=264185, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s03038_i3254_p-6 | `embryo_kid_score` | `nan` | `1.7` |
| oocito_id=264828, emb_cong_id=null, embryo_embryo_id=d2025.07.17_s03040_i3254_p-6 | `embryo_kid_score` | `nan` | `NA` |
| oocito_id=264829, emb_cong_id=null, embryo_embryo_id=d2025.07.17_s03040_i3254_p-7 | `embryo_kid_score` | `nan` | `NA` |
| oocito_id=281558, emb_cong_id=null, embryo_embryo_id=d2025.10.17_s02497_i4120_p-2 | `embryo_kid_score` | `nan` | `1.7` |
| oocito_id=281560, emb_cong_id=null, embryo_embryo_id=d2025.10.17_s02497_i4120_p-4 | `embryo_kid_score` | `nan` | `NA` |
| oocito_id=281561, emb_cong_id=null, embryo_embryo_id=d2025.10.17_s02497_i4120_p-5 | `embryo_kid_score` | `nan` | `1.7` |
| oocito_id=264181, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s03038_i3254_p-2 | `embryo_kid_user` | `nan` | `BLENDA` |
| oocito_id=264794, emb_cong_id=null, embryo_embryo_id=d2025.07.17_s03039_i3254_p-3 | `embryo_kid_user` | `nan` | `BLENDA` |
| oocito_id=264183, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s03038_i3254_p-4 | `embryo_kid_user` | `nan` | `BLENDA` |
| oocito_id=264184, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s03038_i3254_p-5 | `embryo_kid_user` | `nan` | `BLENDA` |
| oocito_id=264185, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s03038_i3254_p-6 | `embryo_kid_user` | `nan` | `BLENDA` |
| oocito_id=264828, emb_cong_id=null, embryo_embryo_id=d2025.07.17_s03040_i3254_p-6 | `embryo_kid_user` | `nan` | `TAYNARA` |
| oocito_id=264829, emb_cong_id=null, embryo_embryo_id=d2025.07.17_s03040_i3254_p-7 | `embryo_kid_user` | `nan` | `TAYNARA` |
| oocito_id=281558, emb_cong_id=null, embryo_embryo_id=d2025.10.17_s02497_i4120_p-2 | `embryo_kid_user` | `nan` | `ADMIN` |
| oocito_id=281560, emb_cong_id=null, embryo_embryo_id=d2025.10.17_s02497_i4120_p-4 | `embryo_kid_user` | `nan` | `ADMIN` |
| oocito_id=281561, emb_cong_id=null, embryo_embryo_id=d2025.10.17_s02497_i4120_p-5 | `embryo_kid_user` | `nan` | `ADMIN` |
| oocito_id=264181, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s03038_i3254_p-2 | `embryo_kid_version` | `nan` | `KIDScoreD5 v3.3` |
| oocito_id=264794, emb_cong_id=null, embryo_embryo_id=d2025.07.17_s03039_i3254_p-3 | `embryo_kid_version` | `nan` | `KIDScoreD5 v3.3` |
| oocito_id=264183, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s03038_i3254_p-4 | `embryo_kid_version` | `nan` | `KIDScoreD5 v3.3` |
| oocito_id=264184, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s03038_i3254_p-5 | `embryo_kid_version` | `nan` | `KIDScoreD5 v3.3` |
| oocito_id=264185, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s03038_i3254_p-6 | `embryo_kid_version` | `nan` | `KIDScoreD5 v3.3` |
| oocito_id=264828, emb_cong_id=null, embryo_embryo_id=d2025.07.17_s03040_i3254_p-6 | `embryo_kid_version` | `nan` | `KIDScoreD5 v3.3` |
| oocito_id=264829, emb_cong_id=null, embryo_embryo_id=d2025.07.17_s03040_i3254_p-7 | `embryo_kid_version` | `nan` | `KIDScoreD5 v3.3` |
| oocito_id=281558, emb_cong_id=null, embryo_embryo_id=d2025.10.17_s02497_i4120_p-2 | `embryo_kid_version` | `nan` | `KIDScoreD5 v3.1` |
| oocito_id=281560, emb_cong_id=null, embryo_embryo_id=d2025.10.17_s02497_i4120_p-4 | `embryo_kid_version` | `nan` | `KIDScoreD5 v3.1` |
| oocito_id=281561, emb_cong_id=null, embryo_embryo_id=d2025.10.17_s02497_i4120_p-5 | `embryo_kid_version` | `nan` | `KIDScoreD5 v3.1` |
| oocito_id=268057, emb_cong_id=null, embryo_embryo_id=d2025.08.06_s02438_i4120_p-1 | `embryo_position` | `2` | `10.0` |
| oocito_id=268060, emb_cong_id=null, embryo_embryo_id=d2025.08.06_s02438_i4120_p-4 | `embryo_position` | `2` | `10.0` |
| oocito_id=264194, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-1 | `embryo_position` | `9` | `6.0` |
| oocito_id=264196, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-3 | `embryo_position` | `9` | `6.0` |
| oocito_id=264197, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-4 | `embryo_position` | `9` | `6.0` |
| oocito_id=264198, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-5 | `embryo_position` | `9` | `6.0` |
| oocito_id=264202, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-9 | `embryo_position` | `9` | `6.0` |
| oocito_id=264203, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-10 | `embryo_position` | `9` | `6.0` |
| oocito_id=264204, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-11 | `embryo_position` | `9` | `6.0` |
| oocito_id=264794, emb_cong_id=null, embryo_embryo_id=d2025.07.17_s03039_i3254_p-3 | `embryo_position` | `2` | `7.0` |
| oocito_id=254168, emb_cong_id=null, embryo_embryo_id=d2025.05.14_s02380_i4120_p-3 | `embryo_time_fragmentation` | `34.1` | `70.2` |
| oocito_id=254918, emb_cong_id=null, embryo_embryo_id=d2025.05.19_s02385_i4120_p-1 | `embryo_time_fragmentation` | `24.3` | `64.8` |
| oocito_id=254920, emb_cong_id=null, embryo_embryo_id=d2025.05.19_s02385_i4120_p-3 | `embryo_time_fragmentation` | `30.1` | `58.5` |
| oocito_id=254921, emb_cong_id=null, embryo_embryo_id=d2025.05.19_s02385_i4120_p-4 | `embryo_time_fragmentation` | `26.3` | `67.7` |
| oocito_id=254923, emb_cong_id=null, embryo_embryo_id=d2025.05.19_s02385_i4120_p-6 | `embryo_time_fragmentation` | `28.1` | `62.3` |
| oocito_id=254924, emb_cong_id=null, embryo_embryo_id=d2025.05.19_s02385_i4120_p-7 | `embryo_time_fragmentation` | `25.1` | `60.7` |
| oocito_id=254928, emb_cong_id=null, embryo_embryo_id=d2025.05.19_s02386_i4120_p-11 | `embryo_time_fragmentation` | `27.5` | `44.0` |
| oocito_id=255333, emb_cong_id=null, embryo_embryo_id=d2025.05.21_s02388_i4120_p-1 | `embryo_time_fragmentation` | `26.0` | `75.3` |
| oocito_id=255560, emb_cong_id=null, embryo_embryo_id=d2025.05.22_s02389_i4120_p-3 | `embryo_time_fragmentation` | `26.0` | `65.8` |
| oocito_id=254166, emb_cong_id=null, embryo_embryo_id=d2025.05.14_s02380_i4120_p-1 | `embryo_time_fragmentation` | `33.5` | `57.4` |
| oocito_id=264196, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-3 | `embryo_time_t2` | `nan` | `27.4` |
| oocito_id=264197, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-4 | `embryo_time_t2` | `nan` | `28.7` |
| oocito_id=264202, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-9 | `embryo_time_t2` | `nan` | `36.5` |
| oocito_id=268053, emb_cong_id=null, embryo_embryo_id=d2025.08.06_s02437_i4120_p-9 | `embryo_time_t2` | `nan` | `29.6` |
| oocito_id=269976, emb_cong_id=null, embryo_embryo_id=d2025.08.18_s02446_i4120_p-1 | `embryo_time_t2` | `nan` | `24.9` |
| oocito_id=269980, emb_cong_id=null, embryo_embryo_id=d2025.08.18_s02445_i4120_p-5 | `embryo_time_t2` | `nan` | `24.3` |
| oocito_id=269978, emb_cong_id=null, embryo_embryo_id=d2025.08.18_s02446_i4120_p-3 | `embryo_time_t2` | `nan` | `27.7` |
| oocito_id=278120, emb_cong_id=null, embryo_embryo_id=d2025.09.29_s02477_i4120_p-1 | `embryo_time_t2` | `nan` | `49.2` |
| oocito_id=282074, emb_cong_id=null, embryo_embryo_id=d2025.10.20_s02499_i4120_p-2 | `embryo_time_t2` | `nan` | `35.5` |
| oocito_id=282077, emb_cong_id=null, embryo_embryo_id=d2025.10.20_s02499_i4120_p-5 | `embryo_time_t2` | `nan` | `34.7` |
| oocito_id=264194, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-1 | `embryo_time_t3` | `nan` | `60.0` |
| oocito_id=264196, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-3 | `embryo_time_t3` | `nan` | `38.8` |
| oocito_id=264197, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-4 | `embryo_time_t3` | `nan` | `37.0` |
| oocito_id=264202, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-9 | `embryo_time_t3` | `nan` | `37.9` |
| oocito_id=268053, emb_cong_id=null, embryo_embryo_id=d2025.08.06_s02437_i4120_p-9 | `embryo_time_t3` | `nan` | `43.1` |
| oocito_id=269976, emb_cong_id=null, embryo_embryo_id=d2025.08.18_s02446_i4120_p-1 | `embryo_time_t3` | `nan` | `35.4` |
| oocito_id=269980, emb_cong_id=null, embryo_embryo_id=d2025.08.18_s02445_i4120_p-5 | `embryo_time_t3` | `nan` | `27.3` |
| oocito_id=269977, emb_cong_id=null, embryo_embryo_id=d2025.08.18_s02446_i4120_p-2 | `embryo_time_t3` | `nan` | `32.2` |
| oocito_id=269978, emb_cong_id=null, embryo_embryo_id=d2025.08.18_s02446_i4120_p-3 | `embryo_time_t3` | `nan` | `27.9` |
| oocito_id=278120, emb_cong_id=null, embryo_embryo_id=d2025.09.29_s02477_i4120_p-1 | `embryo_time_t3` | `nan` | `50.6` |
| oocito_id=264194, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-1 | `embryo_time_t4` | `nan` | `62.2` |
| oocito_id=264196, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-3 | `embryo_time_t4` | `nan` | `40.4` |
| oocito_id=264197, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-4 | `embryo_time_t4` | `nan` | `38.3` |
| oocito_id=264202, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-9 | `embryo_time_t4` | `nan` | `39.1` |
| oocito_id=268053, emb_cong_id=null, embryo_embryo_id=d2025.08.06_s02437_i4120_p-9 | `embryo_time_t4` | `nan` | `46.4` |
| oocito_id=269976, emb_cong_id=null, embryo_embryo_id=d2025.08.18_s02446_i4120_p-1 | `embryo_time_t4` | `nan` | `35.7` |
| oocito_id=269980, emb_cong_id=null, embryo_embryo_id=d2025.08.18_s02445_i4120_p-5 | `embryo_time_t4` | `nan` | `31.0` |
| oocito_id=269977, emb_cong_id=null, embryo_embryo_id=d2025.08.18_s02446_i4120_p-2 | `embryo_time_t4` | `nan` | `32.4` |
| oocito_id=269978, emb_cong_id=null, embryo_embryo_id=d2025.08.18_s02446_i4120_p-3 | `embryo_time_t4` | `nan` | `38.7` |
| oocito_id=278120, emb_cong_id=null, embryo_embryo_id=d2025.09.29_s02477_i4120_p-1 | `embryo_time_t4` | `nan` | `79.9` |
| oocito_id=264194, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-1 | `embryo_time_t5` | `nan` | `65.6` |
| oocito_id=264196, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-3 | `embryo_time_t5` | `nan` | `51.5` |
| oocito_id=264197, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-4 | `embryo_time_t5` | `nan` | `39.4` |
| oocito_id=264202, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-9 | `embryo_time_t5` | `nan` | `49.8` |
| oocito_id=268053, emb_cong_id=null, embryo_embryo_id=d2025.08.06_s02437_i4120_p-9 | `embryo_time_t5` | `nan` | `73.2` |
| oocito_id=269976, emb_cong_id=null, embryo_embryo_id=d2025.08.18_s02446_i4120_p-1 | `embryo_time_t5` | `nan` | `44.7` |
| oocito_id=269980, emb_cong_id=null, embryo_embryo_id=d2025.08.18_s02445_i4120_p-5 | `embryo_time_t5` | `nan` | `38.0` |
| oocito_id=269977, emb_cong_id=null, embryo_embryo_id=d2025.08.18_s02446_i4120_p-2 | `embryo_time_t5` | `nan` | `40.8` |
| oocito_id=269978, emb_cong_id=null, embryo_embryo_id=d2025.08.18_s02446_i4120_p-3 | `embryo_time_t5` | `nan` | `45.4` |
| oocito_id=278120, emb_cong_id=null, embryo_embryo_id=d2025.09.29_s02477_i4120_p-1 | `embryo_time_t5` | `nan` | `81.0` |
| oocito_id=264194, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-1 | `embryo_time_t8` | `nan` | `70.8` |
| oocito_id=264196, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-3 | `embryo_time_t8` | `nan` | `63.2` |
| oocito_id=264197, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-4 | `embryo_time_t8` | `nan` | `53.6` |
| oocito_id=264202, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-9 | `embryo_time_t8` | `nan` | `68.5` |
| oocito_id=268053, emb_cong_id=null, embryo_embryo_id=d2025.08.06_s02437_i4120_p-9 | `embryo_time_t8` | `nan` | `110.9` |
| oocito_id=269976, emb_cong_id=null, embryo_embryo_id=d2025.08.18_s02446_i4120_p-1 | `embryo_time_t8` | `nan` | `48.9` |
| oocito_id=269980, emb_cong_id=null, embryo_embryo_id=d2025.08.18_s02445_i4120_p-5 | `embryo_time_t8` | `nan` | `52.3` |
| oocito_id=269977, emb_cong_id=null, embryo_embryo_id=d2025.08.18_s02446_i4120_p-2 | `embryo_time_t8` | `nan` | `53.0` |
| oocito_id=269978, emb_cong_id=null, embryo_embryo_id=d2025.08.18_s02446_i4120_p-3 | `embryo_time_t8` | `nan` | `52.7` |
| oocito_id=282273, emb_cong_id=null, embryo_embryo_id=d2025.10.21_s02500_i4120_p-2 | `embryo_time_t8` | `nan` | `53.5` |
| oocito_id=269976, emb_cong_id=null, embryo_embryo_id=d2025.08.18_s02446_i4120_p-1 | `embryo_time_tb` | `nan` | `101.1` |
| oocito_id=269977, emb_cong_id=null, embryo_embryo_id=d2025.08.18_s02446_i4120_p-2 | `embryo_time_tb` | `nan` | `99.2` |
| oocito_id=277667, emb_cong_id=null, embryo_embryo_id=d2025.09.26_s02475_i4120_p-3 | `embryo_time_tb` | `nan` | `118.5` |
| oocito_id=284232, emb_cong_id=null, embryo_embryo_id=d2025.11.01_s02510_i4120_p-11 | `embryo_time_tb` | `nan` | `116.5` |
| oocito_id=284234, emb_cong_id=null, embryo_embryo_id=d2025.11.01_s02510_i4120_p-13 | `embryo_time_tb` | `nan` | `97.1` |
| oocito_id=287030, emb_cong_id=null, embryo_embryo_id=d2025.11.19_s02523_i4120_p-1 | `embryo_time_tb` | `nan` | `104.8` |
| oocito_id=298566, emb_cong_id=null, embryo_embryo_id=d2026.02.05_s02568_i4120_p-9 | `embryo_time_tb` | `nan` | `141.9` |
| oocito_id=313426, emb_cong_id=null, embryo_embryo_id=d2026.05.13_s02626_i4120_p-3 | `embryo_time_tb` | `nan` | `99.5` |
| oocito_id=269976, emb_cong_id=null, embryo_embryo_id=d2025.08.18_s02446_i4120_p-1 | `embryo_time_teb` | `nan` | `106.0` |
| oocito_id=269977, emb_cong_id=null, embryo_embryo_id=d2025.08.18_s02446_i4120_p-2 | `embryo_time_teb` | `nan` | `106.4` |
| oocito_id=277667, emb_cong_id=null, embryo_embryo_id=d2025.09.26_s02475_i4120_p-3 | `embryo_time_teb` | `nan` | `127.0` |
| oocito_id=287030, emb_cong_id=null, embryo_embryo_id=d2025.11.19_s02523_i4120_p-1 | `embryo_time_teb` | `nan` | `114.9` |
| oocito_id=313426, emb_cong_id=null, embryo_embryo_id=d2026.05.13_s02626_i4120_p-3 | `embryo_time_teb` | `nan` | `105.6` |
| oocito_id=253521, emb_cong_id=null, embryo_embryo_id=d2025.05.10_s02921_i3254_p-2 | `embryo_time_thb` | `nan` | `109.4` |
| oocito_id=255626, emb_cong_id=null, embryo_embryo_id=d2025.05.23_s02943_i3254_p-1 | `embryo_time_thb` | `nan` | `115.2` |
| oocito_id=256395, emb_cong_id=null, embryo_embryo_id=d2025.05.28_s02949_i3254_p-6 | `embryo_time_thb` | `nan` | `113.4` |
| oocito_id=261175, emb_cong_id=null, embryo_embryo_id=d2025.06.18_s02997_i3254_p-6 | `embryo_time_thb` | `nan` | `112.2` |
| oocito_id=261148, emb_cong_id=null, embryo_embryo_id=d2025.06.18_s03001_i3254_p-4 | `embryo_time_thb` | `nan` | `113.2` |
| oocito_id=261337, emb_cong_id=null, embryo_embryo_id=d2025.06.20_s03003_i3254_p-3 | `embryo_time_thb` | `nan` | `110.4` |
| oocito_id=261339, emb_cong_id=null, embryo_embryo_id=d2025.06.20_s03003_i3254_p-5 | `embryo_time_thb` | `nan` | `115.8` |
| oocito_id=262683, emb_cong_id=null, embryo_embryo_id=d2025.07.02_s03016_i3254_p-6 | `embryo_time_thb` | `nan` | `115.6` |
| oocito_id=262831, emb_cong_id=null, embryo_embryo_id=d2025.07.03_s03019_i3254_p-8 | `embryo_time_thb` | `nan` | `114.2` |
| oocito_id=263618, emb_cong_id=null, embryo_embryo_id=d2025.07.10_s03030_i3254_p-7 | `embryo_time_thb` | `nan` | `108.8` |
| oocito_id=264196, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-3 | `embryo_time_tm` | `nan` | `90.5` |
| oocito_id=264197, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-4 | `embryo_time_tm` | `nan` | `91.1` |
| oocito_id=268332, emb_cong_id=null, embryo_embryo_id=d2025.08.08_s02441_i4120_p-3 | `embryo_time_tm` | `nan` | `103.9` |
| oocito_id=268338, emb_cong_id=null, embryo_embryo_id=d2025.08.08_s02441_i4120_p-9 | `embryo_time_tm` | `nan` | `116.3` |
| oocito_id=269976, emb_cong_id=null, embryo_embryo_id=d2025.08.18_s02446_i4120_p-1 | `embryo_time_tm` | `nan` | `83.9` |
| oocito_id=269980, emb_cong_id=null, embryo_embryo_id=d2025.08.18_s02445_i4120_p-5 | `embryo_time_tm` | `nan` | `86.0` |
| oocito_id=269977, emb_cong_id=null, embryo_embryo_id=d2025.08.18_s02446_i4120_p-2 | `embryo_time_tm` | `nan` | `82.9` |
| oocito_id=277667, emb_cong_id=null, embryo_embryo_id=d2025.09.26_s02475_i4120_p-3 | `embryo_time_tm` | `nan` | `95.3` |
| oocito_id=280545, emb_cong_id=null, embryo_embryo_id=d2025.10.11_s02491_i4120_p-1 | `embryo_time_tm` | `nan` | `100.2` |
| oocito_id=280547, emb_cong_id=null, embryo_embryo_id=d2025.10.11_s02491_i4120_p-3 | `embryo_time_tm` | `nan` | `89.2` |
| oocito_id=278120, emb_cong_id=null, embryo_embryo_id=d2025.09.29_s02477_i4120_p-1 | `embryo_time_tpna` | `nan` | `31.8` |
| oocito_id=264194, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-1 | `embryo_time_tpnf` | `nan` | `57.4` |
| oocito_id=264196, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-3 | `embryo_time_tpnf` | `nan` | `24.9` |
| oocito_id=264202, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-9 | `embryo_time_tpnf` | `nan` | `34.2` |
| oocito_id=269978, emb_cong_id=null, embryo_embryo_id=d2025.08.18_s02446_i4120_p-3 | `embryo_time_tpnf` | `nan` | `26.1` |
| oocito_id=278120, emb_cong_id=null, embryo_embryo_id=d2025.09.29_s02477_i4120_p-1 | `embryo_time_tpnf` | `nan` | `46.0` |
| oocito_id=282073, emb_cong_id=null, embryo_embryo_id=d2025.10.20_s02499_i4120_p-1 | `embryo_time_tpnf` | `nan` | `33.8` |
| oocito_id=282074, emb_cong_id=null, embryo_embryo_id=d2025.10.20_s02499_i4120_p-2 | `embryo_time_tpnf` | `nan` | `32.9` |
| oocito_id=282077, emb_cong_id=null, embryo_embryo_id=d2025.10.20_s02499_i4120_p-5 | `embryo_time_tpnf` | `nan` | `32.5` |
| oocito_id=282080, emb_cong_id=null, embryo_embryo_id=d2025.10.20_s02499_i4120_p-8 | `embryo_time_tpnf` | `nan` | `31.4` |
| oocito_id=282273, emb_cong_id=null, embryo_embryo_id=d2025.10.21_s02500_i4120_p-2 | `embryo_time_tpnf` | `nan` | `31.0` |
| oocito_id=264196, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-3 | `embryo_time_tsb` | `nan` | `103.7` |
| oocito_id=268332, emb_cong_id=null, embryo_embryo_id=d2025.08.08_s02441_i4120_p-3 | `embryo_time_tsb` | `nan` | `110.9` |
| oocito_id=269976, emb_cong_id=null, embryo_embryo_id=d2025.08.18_s02446_i4120_p-1 | `embryo_time_tsb` | `nan` | `88.1` |
| oocito_id=269977, emb_cong_id=null, embryo_embryo_id=d2025.08.18_s02446_i4120_p-2 | `embryo_time_tsb` | `nan` | `90.1` |
| oocito_id=277667, emb_cong_id=null, embryo_embryo_id=d2025.09.26_s02475_i4120_p-3 | `embryo_time_tsb` | `nan` | `98.4` |
| oocito_id=280545, emb_cong_id=null, embryo_embryo_id=d2025.10.11_s02491_i4120_p-1 | `embryo_time_tsb` | `nan` | `105.0` |
| oocito_id=284232, emb_cong_id=null, embryo_embryo_id=d2025.11.01_s02510_i4120_p-11 | `embryo_time_tsb` | `nan` | `110.4` |
| oocito_id=284234, emb_cong_id=null, embryo_embryo_id=d2025.11.01_s02510_i4120_p-13 | `embryo_time_tsb` | `nan` | `93.9` |
| oocito_id=284379, emb_cong_id=null, embryo_embryo_id=d2025.11.03_s02512_i4120_p-2 | `embryo_time_tsb` | `nan` | `116.9` |
| oocito_id=284384, emb_cong_id=null, embryo_embryo_id=d2025.11.03_s02512_i4120_p-7 | `embryo_time_tsb` | `nan` | `111.3` |
| oocito_id=264194, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-1 | `embryo_time_tsc` | `nan` | `115.6` |
| oocito_id=264196, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-3 | `embryo_time_tsc` | `nan` | `76.7` |
| oocito_id=264197, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-4 | `embryo_time_tsc` | `nan` | `66.0` |
| oocito_id=264202, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-9 | `embryo_time_tsc` | `nan` | `90.2` |
| oocito_id=268332, emb_cong_id=null, embryo_embryo_id=d2025.08.08_s02441_i4120_p-3 | `embryo_time_tsc` | `nan` | `90.0` |
| oocito_id=268335, emb_cong_id=null, embryo_embryo_id=d2025.08.08_s02441_i4120_p-6 | `embryo_time_tsc` | `nan` | `83.1` |
| oocito_id=268338, emb_cong_id=null, embryo_embryo_id=d2025.08.08_s02441_i4120_p-9 | `embryo_time_tsc` | `nan` | `83.3` |
| oocito_id=269976, emb_cong_id=null, embryo_embryo_id=d2025.08.18_s02446_i4120_p-1 | `embryo_time_tsc` | `nan` | `79.4` |
| oocito_id=269980, emb_cong_id=null, embryo_embryo_id=d2025.08.18_s02445_i4120_p-5 | `embryo_time_tsc` | `nan` | `79.0` |
| oocito_id=269977, emb_cong_id=null, embryo_embryo_id=d2025.08.18_s02446_i4120_p-2 | `embryo_time_tsc` | `nan` | `78.6` |
| oocito_id=269976, emb_cong_id=null, embryo_embryo_id=d2025.08.18_s02446_i4120_p-1 | `embryo_value_icm` | `nan` | `B` |
| oocito_id=269977, emb_cong_id=null, embryo_embryo_id=d2025.08.18_s02446_i4120_p-2 | `embryo_value_icm` | `nan` | `B` |
| oocito_id=277667, emb_cong_id=null, embryo_embryo_id=d2025.09.26_s02475_i4120_p-3 | `embryo_value_icm` | `nan` | `C` |
| oocito_id=287030, emb_cong_id=null, embryo_embryo_id=d2025.11.19_s02523_i4120_p-1 | `embryo_value_icm` | `nan` | `A` |
| oocito_id=298566, emb_cong_id=null, embryo_embryo_id=d2026.02.05_s02568_i4120_p-9 | `embryo_value_icm` | `nan` | `B` |
| oocito_id=313426, emb_cong_id=null, embryo_embryo_id=d2026.05.13_s02626_i4120_p-3 | `embryo_value_icm` | `nan` | `A` |
| oocito_id=255664, emb_cong_id=null, embryo_embryo_id=d2021.06.01_s00709_i4120_p-4 | `embryo_value_pn` | `2` | `3` |
| oocito_id=257666, emb_cong_id=null, embryo_embryo_id=d2025.06.02_s02396_i4120_p-2 | `embryo_value_pn` | `2` | `3` |
| oocito_id=272035, emb_cong_id=null, embryo_embryo_id=d2025.08.28_s02456_i4120_p-4 | `embryo_value_pn` | `2` | `3` |
| oocito_id=278120, emb_cong_id=null, embryo_embryo_id=d2025.09.29_s02477_i4120_p-1 | `embryo_value_pn` | `0` | `2` |
| oocito_id=282080, emb_cong_id=null, embryo_embryo_id=d2025.10.20_s02499_i4120_p-8 | `embryo_value_pn` | `2` | `3` |
| oocito_id=3769, emb_cong_id=null, embryo_embryo_id=d2021.06.27_s00759_i4120_p-4 | `embryo_value_pn` | `2` | `3` |
| oocito_id=20560, emb_cong_id=null, embryo_embryo_id=d2021.09.22_s00916_i4120_p-6 | `embryo_value_pn` | `1` | `2` |
| oocito_id=20587, emb_cong_id=null, embryo_embryo_id=d2021.09.22_s00914_i4120_p-7 | `embryo_value_pn` | `2` | `3` |
| oocito_id=30441, emb_cong_id=null, embryo_embryo_id=d2021.12.02_s00990_i4120_p-9 | `embryo_value_pn` | `2` | `3` |
| oocito_id=31318, emb_cong_id=null, embryo_embryo_id=d2021.12.03_s00994_i4120_p-4 | `embryo_value_pn` | `2` | `3` |
| oocito_id=269976, emb_cong_id=null, embryo_embryo_id=d2025.08.18_s02446_i4120_p-1 | `embryo_value_te` | `nan` | `B` |
| oocito_id=269977, emb_cong_id=null, embryo_embryo_id=d2025.08.18_s02446_i4120_p-2 | `embryo_value_te` | `nan` | `B` |
| oocito_id=277667, emb_cong_id=null, embryo_embryo_id=d2025.09.26_s02475_i4120_p-3 | `embryo_value_te` | `nan` | `C` |
| oocito_id=287030, emb_cong_id=null, embryo_embryo_id=d2025.11.19_s02523_i4120_p-1 | `embryo_value_te` | `nan` | `B` |
| oocito_id=298566, emb_cong_id=null, embryo_embryo_id=d2026.02.05_s02568_i4120_p-9 | `embryo_value_te` | `nan` | `C` |
| oocito_id=313426, emb_cong_id=null, embryo_embryo_id=d2026.05.13_s02626_i4120_p-3 | `embryo_value_te` | `nan` | `A` |
| oocito_id=253535, emb_cong_id=null, embryo_embryo_id=d2025.05.10_s02919_i3254_p-1 | `micro_data_dl` | `2025-05-10 00:00:00` | `2025-05-10` |
| oocito_id=253537, emb_cong_id=null, embryo_embryo_id=d2025.05.10_s02919_i3254_p-3 | `micro_data_dl` | `2025-05-10 00:00:00` | `2025-05-10` |
| oocito_id=253562, emb_cong_id=null, embryo_embryo_id=d2025.05.10_s02920_i3254_p-1 | `micro_data_dl` | `2025-05-10 00:00:00` | `2025-05-10` |
| oocito_id=253567, emb_cong_id=null, embryo_embryo_id=d2025.05.10_s02920_i3254_p-6 | `micro_data_dl` | `2025-05-10 00:00:00` | `2025-05-10` |
| oocito_id=253521, emb_cong_id=null, embryo_embryo_id=d2025.05.10_s02921_i3254_p-2 | `micro_data_dl` | `2025-05-10 00:00:00` | `2025-05-10` |
| oocito_id=253522, emb_cong_id=null, embryo_embryo_id=d2025.05.10_s02921_i3254_p-3 | `micro_data_dl` | `2025-05-10 00:00:00` | `2025-05-10` |
| oocito_id=253523, emb_cong_id=null, embryo_embryo_id=d2025.05.10_s02921_i3254_p-4 | `micro_data_dl` | `2025-05-10 00:00:00` | `2025-05-10` |
| oocito_id=253525, emb_cong_id=null, embryo_embryo_id=d2025.05.10_s02921_i3254_p-6 | `micro_data_dl` | `2025-05-10 00:00:00` | `2025-05-10` |
| oocito_id=253911, emb_cong_id=null, embryo_embryo_id=d2025.05.13_s02922_i3254_p-1 | `micro_data_dl` | `2025-05-13 00:00:00` | `2025-05-13` |
| oocito_id=253912, emb_cong_id=null, embryo_embryo_id=d2025.05.13_s02922_i3254_p-2 | `micro_data_dl` | `2025-05-13 00:00:00` | `2025-05-13` |
| oocito_id=253535, emb_cong_id=null, embryo_embryo_id=d2025.05.10_s02919_i3254_p-1 | `micro_data_procedimento` | `2025-05-10 00:00:00` | `2025-05-10` |
| oocito_id=253537, emb_cong_id=null, embryo_embryo_id=d2025.05.10_s02919_i3254_p-3 | `micro_data_procedimento` | `2025-05-10 00:00:00` | `2025-05-10` |
| oocito_id=253562, emb_cong_id=null, embryo_embryo_id=d2025.05.10_s02920_i3254_p-1 | `micro_data_procedimento` | `2025-05-10 00:00:00` | `2025-05-10` |
| oocito_id=253567, emb_cong_id=null, embryo_embryo_id=d2025.05.10_s02920_i3254_p-6 | `micro_data_procedimento` | `2025-05-10 00:00:00` | `2025-05-10` |
| oocito_id=253521, emb_cong_id=null, embryo_embryo_id=d2025.05.10_s02921_i3254_p-2 | `micro_data_procedimento` | `2025-05-10 00:00:00` | `2025-05-10` |
| oocito_id=253522, emb_cong_id=null, embryo_embryo_id=d2025.05.10_s02921_i3254_p-3 | `micro_data_procedimento` | `2025-05-10 00:00:00` | `2025-05-10` |
| oocito_id=253523, emb_cong_id=null, embryo_embryo_id=d2025.05.10_s02921_i3254_p-4 | `micro_data_procedimento` | `2025-05-10 00:00:00` | `2025-05-10` |
| oocito_id=253525, emb_cong_id=null, embryo_embryo_id=d2025.05.10_s02921_i3254_p-6 | `micro_data_procedimento` | `2025-05-10 00:00:00` | `2025-05-10` |
| oocito_id=253911, emb_cong_id=null, embryo_embryo_id=d2025.05.13_s02922_i3254_p-1 | `micro_data_procedimento` | `2025-05-13 00:00:00` | `2025-05-13` |
| oocito_id=253912, emb_cong_id=null, embryo_embryo_id=d2025.05.13_s02922_i3254_p-2 | `micro_data_procedimento` | `2025-05-13 00:00:00` | `2025-05-13` |
| oocito_id=23443, emb_cong_id=null, embryo_embryo_id=null | `micro_prontuario` | `175171` | `971029` |
| oocito_id=23444, emb_cong_id=null, embryo_embryo_id=null | `micro_prontuario` | `175171` | `971029` |
| oocito_id=23445, emb_cong_id=null, embryo_embryo_id=null | `micro_prontuario` | `175171` | `971029` |
| oocito_id=23446, emb_cong_id=null, embryo_embryo_id=null | `micro_prontuario` | `175171` | `971029` |
| oocito_id=23447, emb_cong_id=null, embryo_embryo_id=null | `micro_prontuario` | `175171` | `971029` |
| oocito_id=23448, emb_cong_id=null, embryo_embryo_id=null | `micro_prontuario` | `175171` | `971029` |
| oocito_id=23449, emb_cong_id=null, embryo_embryo_id=null | `micro_prontuario` | `175171` | `971029` |
| oocito_id=23450, emb_cong_id=null, embryo_embryo_id=null | `micro_prontuario` | `175171` | `971029` |
| oocito_id=23451, emb_cong_id=null, embryo_embryo_id=null | `micro_prontuario` | `175171` | `971029` |
| oocito_id=23452, emb_cong_id=null, embryo_embryo_id=null | `micro_prontuario` | `175171` | `971029` |
| oocito_id=315567, emb_cong_id=null, embryo_embryo_id=null | `oocito_embryo_number` | `10` | `1` |
| oocito_id=315568, emb_cong_id=null, embryo_embryo_id=null | `oocito_embryo_number` | `11` | `2` |
| oocito_id=315569, emb_cong_id=null, embryo_embryo_id=null | `oocito_embryo_number` | `12` | `3` |
| oocito_id=315570, emb_cong_id=null, embryo_embryo_id=null | `oocito_embryo_number` | `13` | `4` |
| oocito_id=315571, emb_cong_id=null, embryo_embryo_id=null | `oocito_embryo_number` | `14` | `5` |
| oocito_id=315572, emb_cong_id=null, embryo_embryo_id=null | `oocito_embryo_number` | `15` | `6` |
| oocito_id=315574, emb_cong_id=null, embryo_embryo_id=null | `oocito_embryo_number` | `17` | `8` |
| oocito_id=315575, emb_cong_id=null, embryo_embryo_id=null | `oocito_embryo_number` | `18` | `9` |
| oocito_id=87818, emb_cong_id=null, embryo_embryo_id=null | `oocito_origem_oocito` | `Descongelado` | `Descongelado OR` |
| oocito_id=87819, emb_cong_id=null, embryo_embryo_id=null | `oocito_origem_oocito` | `Descongelado` | `Descongelado OR` |
| oocito_id=87820, emb_cong_id=null, embryo_embryo_id=null | `oocito_origem_oocito` | `Descongelado` | `Descongelado OR` |
| oocito_id=87821, emb_cong_id=null, embryo_embryo_id=null | `oocito_origem_oocito` | `Descongelado` | `Descongelado OR` |
| oocito_id=87822, emb_cong_id=null, embryo_embryo_id=null | `oocito_origem_oocito` | `Descongelado` | `Descongelado OR` |
| oocito_id=87823, emb_cong_id=null, embryo_embryo_id=null | `oocito_origem_oocito` | `Descongelado` | `Descongelado OR` |
| oocito_id=87824, emb_cong_id=null, embryo_embryo_id=null | `oocito_origem_oocito` | `Descongelado` | `Descongelado OR` |
| oocito_id=87825, emb_cong_id=null, embryo_embryo_id=null | `oocito_origem_oocito` | `Descongelado` | `Descongelado OR` |
| oocito_id=100153, emb_cong_id=null, embryo_embryo_id=d2022.08.26_s01502_i4120_p-5 | `oocito_origem_oocito` | `Fresco` | `Fresco OR` |
| oocito_id=100155, emb_cong_id=null, embryo_embryo_id=d2022.08.26_s01502_i4120_p-7 | `oocito_origem_oocito` | `Fresco` | `Fresco OR` |
| oocito_id=318772, emb_cong_id=null, embryo_embryo_id=null | `oocito_tcd` | `nan` | `Descartado` |
| oocito_id=318919, emb_cong_id=null, embryo_embryo_id=null | `oocito_tcd` | `nan` | `Descartado` |
| oocito_id=318968, emb_cong_id=null, embryo_embryo_id=null | `oocito_tcd` | `nan` | `Descartado` |
| oocito_id=318973, emb_cong_id=null, embryo_embryo_id=null | `oocito_tcd` | `nan` | `Descartado` |
| oocito_id=318987, emb_cong_id=null, embryo_embryo_id=null | `oocito_tcd` | `nan` | `Descartado` |
| oocito_id=318988, emb_cong_id=null, embryo_embryo_id=null | `oocito_tcd` | `nan` | `Descartado` |
| oocito_id=318990, emb_cong_id=null, embryo_embryo_id=null | `oocito_tcd` | `nan` | `Descartado` |
| oocito_id=318992, emb_cong_id=null, embryo_embryo_id=null | `oocito_tcd` | `nan` | `Descartado` |
| oocito_id=318996, emb_cong_id=null, embryo_embryo_id=null | `oocito_tcd` | `nan` | `Descartado` |
| oocito_id=318997, emb_cong_id=null, embryo_embryo_id=null | `oocito_tcd` | `nan` | `Descartado` |
| oocito_id=260468, emb_cong_id=null, embryo_embryo_id=null | `trat1_bmi` | `2254.82` | `22.32` |
| oocito_id=262680, emb_cong_id=null, embryo_embryo_id=d2025.07.02_s03016_i3254_p-3 | `trat1_bmi` | `1933.59` | `19.14` |
| oocito_id=262681, emb_cong_id=null, embryo_embryo_id=d2025.07.02_s03016_i3254_p-4 | `trat1_bmi` | `1933.59` | `19.14` |
| oocito_id=262682, emb_cong_id=null, embryo_embryo_id=d2025.07.02_s03016_i3254_p-5 | `trat1_bmi` | `1933.59` | `19.14` |
| oocito_id=262683, emb_cong_id=null, embryo_embryo_id=d2025.07.02_s03016_i3254_p-6 | `trat1_bmi` | `1933.59` | `19.14` |
| oocito_id=262684, emb_cong_id=null, embryo_embryo_id=d2025.07.02_s03016_i3254_p-7 | `trat1_bmi` | `1933.59` | `19.14` |
| oocito_id=262687, emb_cong_id=null, embryo_embryo_id=d2025.07.02_s03016_i3254_p-10 | `trat1_bmi` | `1933.59` | `19.14` |
| oocito_id=262688, emb_cong_id=null, embryo_embryo_id=d2025.07.02_s03016_i3254_p-11 | `trat1_bmi` | `1933.59` | `19.14` |
| oocito_id=263307, emb_cong_id=null, embryo_embryo_id=null | `trat1_bmi` | `1955.97` | `19.36` |
| oocito_id=270152, emb_cong_id=null, embryo_embryo_id=null | `trat1_bmi` | `1878.29` | `18.59` |
| oocito_id=253535, emb_cong_id=null, embryo_embryo_id=d2025.05.10_s02919_i3254_p-1 | `trat1_data_inicio_inducao` | `2025-04-23 00:00:00` | `2025-04-23` |
| oocito_id=253537, emb_cong_id=null, embryo_embryo_id=d2025.05.10_s02919_i3254_p-3 | `trat1_data_inicio_inducao` | `2025-04-23 00:00:00` | `2025-04-23` |
| oocito_id=253562, emb_cong_id=null, embryo_embryo_id=d2025.05.10_s02920_i3254_p-1 | `trat1_data_inicio_inducao` | `2025-04-26 00:00:00` | `2025-04-26` |
| oocito_id=253567, emb_cong_id=null, embryo_embryo_id=d2025.05.10_s02920_i3254_p-6 | `trat1_data_inicio_inducao` | `2025-04-26 00:00:00` | `2025-04-26` |
| oocito_id=253521, emb_cong_id=null, embryo_embryo_id=d2025.05.10_s02921_i3254_p-2 | `trat1_data_inicio_inducao` | `2025-05-03 00:00:00` | `2025-05-03` |
| oocito_id=253522, emb_cong_id=null, embryo_embryo_id=d2025.05.10_s02921_i3254_p-3 | `trat1_data_inicio_inducao` | `2025-05-03 00:00:00` | `2025-05-03` |
| oocito_id=253523, emb_cong_id=null, embryo_embryo_id=d2025.05.10_s02921_i3254_p-4 | `trat1_data_inicio_inducao` | `2025-05-03 00:00:00` | `2025-05-03` |
| oocito_id=253525, emb_cong_id=null, embryo_embryo_id=d2025.05.10_s02921_i3254_p-6 | `trat1_data_inicio_inducao` | `2025-05-03 00:00:00` | `2025-05-03` |
| oocito_id=253911, emb_cong_id=null, embryo_embryo_id=d2025.05.13_s02922_i3254_p-1 | `trat1_data_inicio_inducao` | `2025-05-02 00:00:00` | `2025-05-02` |
| oocito_id=253912, emb_cong_id=null, embryo_embryo_id=d2025.05.13_s02922_i3254_p-2 | `trat1_data_inicio_inducao` | `2025-05-02 00:00:00` | `2025-05-02` |
| oocito_id=253521, emb_cong_id=null, embryo_embryo_id=d2025.05.10_s02921_i3254_p-2 | `trat1_data_transferencia` | `2025-05-15 00:00:00` | `2025-05-15` |
| oocito_id=253522, emb_cong_id=null, embryo_embryo_id=d2025.05.10_s02921_i3254_p-3 | `trat1_data_transferencia` | `2025-05-15 00:00:00` | `2025-05-15` |
| oocito_id=253523, emb_cong_id=null, embryo_embryo_id=d2025.05.10_s02921_i3254_p-4 | `trat1_data_transferencia` | `2025-05-15 00:00:00` | `2025-05-15` |
| oocito_id=253525, emb_cong_id=null, embryo_embryo_id=d2025.05.10_s02921_i3254_p-6 | `trat1_data_transferencia` | `2025-05-15 00:00:00` | `2025-05-15` |
| oocito_id=254051, emb_cong_id=null, embryo_embryo_id=d2025.05.14_s02928_i3254_p-1 | `trat1_data_transferencia` | `2025-05-19 00:00:00` | `2025-05-19` |
| oocito_id=254052, emb_cong_id=null, embryo_embryo_id=d2025.05.14_s02928_i3254_p-2 | `trat1_data_transferencia` | `2025-05-19 00:00:00` | `2025-05-19` |
| oocito_id=254053, emb_cong_id=null, embryo_embryo_id=d2025.05.14_s02928_i3254_p-3 | `trat1_data_transferencia` | `2025-05-19 00:00:00` | `2025-05-19` |
| oocito_id=254054, emb_cong_id=null, embryo_embryo_id=d2025.05.14_s02928_i3254_p-4 | `trat1_data_transferencia` | `2025-05-19 00:00:00` | `2025-05-19` |
| oocito_id=254055, emb_cong_id=null, embryo_embryo_id=d2025.05.14_s02928_i3254_p-5 | `trat1_data_transferencia` | `2025-05-19 00:00:00` | `2025-05-19` |
| oocito_id=254056, emb_cong_id=null, embryo_embryo_id=d2025.05.14_s02928_i3254_p-6 | `trat1_data_transferencia` | `2025-05-19 00:00:00` | `2025-05-19` |
| oocito_id=313960, emb_cong_id=null, embryo_embryo_id=null | `trat1_fator_infertilidade1` | `nan` | `Outros` |
| oocito_id=314501, emb_cong_id=null, embryo_embryo_id=null | `trat1_fator_infertilidade1` | `nan` | `Outros` |
| oocito_id=314502, emb_cong_id=null, embryo_embryo_id=null | `trat1_fator_infertilidade1` | `nan` | `Outros` |
| oocito_id=314503, emb_cong_id=null, embryo_embryo_id=null | `trat1_fator_infertilidade1` | `nan` | `Outros` |
| oocito_id=314504, emb_cong_id=null, embryo_embryo_id=null | `trat1_fator_infertilidade1` | `nan` | `Outros` |
| oocito_id=314505, emb_cong_id=null, embryo_embryo_id=null | `trat1_fator_infertilidade1` | `nan` | `Outros` |
| oocito_id=314506, emb_cong_id=null, embryo_embryo_id=null | `trat1_fator_infertilidade1` | `nan` | `Outros` |
| oocito_id=314507, emb_cong_id=null, embryo_embryo_id=null | `trat1_fator_infertilidade1` | `nan` | `Outros` |
| oocito_id=314508, emb_cong_id=null, embryo_embryo_id=null | `trat1_fator_infertilidade1` | `nan` | `Outros` |
| oocito_id=314509, emb_cong_id=null, embryo_embryo_id=null | `trat1_fator_infertilidade1` | `nan` | `Outros` |
| oocito_id=313960, emb_cong_id=null, embryo_embryo_id=null | `trat1_id` | `<NA>` | `44374.0` |
| oocito_id=314501, emb_cong_id=null, embryo_embryo_id=null | `trat1_id` | `<NA>` | `44205.0` |
| oocito_id=314502, emb_cong_id=null, embryo_embryo_id=null | `trat1_id` | `<NA>` | `44205.0` |
| oocito_id=314503, emb_cong_id=null, embryo_embryo_id=null | `trat1_id` | `<NA>` | `44205.0` |
| oocito_id=314504, emb_cong_id=null, embryo_embryo_id=null | `trat1_id` | `<NA>` | `44205.0` |
| oocito_id=314505, emb_cong_id=null, embryo_embryo_id=null | `trat1_id` | `<NA>` | `44205.0` |
| oocito_id=314506, emb_cong_id=null, embryo_embryo_id=null | `trat1_id` | `<NA>` | `44205.0` |
| oocito_id=314507, emb_cong_id=null, embryo_embryo_id=null | `trat1_id` | `<NA>` | `44205.0` |
| oocito_id=314508, emb_cong_id=null, embryo_embryo_id=null | `trat1_id` | `<NA>` | `44205.0` |
| oocito_id=314509, emb_cong_id=null, embryo_embryo_id=null | `trat1_id` | `<NA>` | `44205.0` |
| oocito_id=289069, emb_cong_id=null, embryo_embryo_id=null | `trat1_motivo_nao_transferir` | `Criopreservação total` | `Ausência de embriões geneticamente normais` |
| oocito_id=306973, emb_cong_id=null, embryo_embryo_id=null | `trat1_motivo_nao_transferir` | `nan` | `Criopreservação total` |
| oocito_id=306974, emb_cong_id=null, embryo_embryo_id=null | `trat1_motivo_nao_transferir` | `nan` | `Criopreservação total` |
| oocito_id=306975, emb_cong_id=null, embryo_embryo_id=null | `trat1_motivo_nao_transferir` | `nan` | `Criopreservação total` |
| oocito_id=308967, emb_cong_id=null, embryo_embryo_id=null | `trat1_motivo_nao_transferir` | `nan` | `Ausência de embriões geneticamente normais` |
| oocito_id=308968, emb_cong_id=null, embryo_embryo_id=null | `trat1_motivo_nao_transferir` | `nan` | `Ausência de embriões geneticamente normais` |
| oocito_id=313960, emb_cong_id=null, embryo_embryo_id=null | `trat1_motivo_nao_transferir` | `nan` | `Criopreservação total` |
| oocito_id=314501, emb_cong_id=null, embryo_embryo_id=null | `trat1_motivo_nao_transferir` | `nan` | `Ausência de embriões geneticamente normais` |
| oocito_id=314502, emb_cong_id=null, embryo_embryo_id=null | `trat1_motivo_nao_transferir` | `nan` | `Ausência de embriões geneticamente normais` |
| oocito_id=314503, emb_cong_id=null, embryo_embryo_id=null | `trat1_motivo_nao_transferir` | `nan` | `Ausência de embriões geneticamente normais` |
| oocito_id=313960, emb_cong_id=null, embryo_embryo_id=null | `trat1_origem_ovulo` | `nan` | `Heterólogo` |
| oocito_id=314501, emb_cong_id=null, embryo_embryo_id=null | `trat1_origem_ovulo` | `nan` | `Homólogo` |
| oocito_id=314502, emb_cong_id=null, embryo_embryo_id=null | `trat1_origem_ovulo` | `nan` | `Homólogo` |
| oocito_id=314503, emb_cong_id=null, embryo_embryo_id=null | `trat1_origem_ovulo` | `nan` | `Homólogo` |
| oocito_id=314504, emb_cong_id=null, embryo_embryo_id=null | `trat1_origem_ovulo` | `nan` | `Homólogo` |
| oocito_id=314505, emb_cong_id=null, embryo_embryo_id=null | `trat1_origem_ovulo` | `nan` | `Homólogo` |
| oocito_id=314506, emb_cong_id=null, embryo_embryo_id=null | `trat1_origem_ovulo` | `nan` | `Homólogo` |
| oocito_id=314507, emb_cong_id=null, embryo_embryo_id=null | `trat1_origem_ovulo` | `nan` | `Homólogo` |
| oocito_id=314508, emb_cong_id=null, embryo_embryo_id=null | `trat1_origem_ovulo` | `nan` | `Homólogo` |
| oocito_id=314509, emb_cong_id=null, embryo_embryo_id=null | `trat1_origem_ovulo` | `nan` | `Homólogo` |
| oocito_id=267436, emb_cong_id=null, embryo_embryo_id=null | `trat1_previous_et` | `11` | `1.0` |
| oocito_id=267437, emb_cong_id=null, embryo_embryo_id=null | `trat1_previous_et` | `11` | `1.0` |
| oocito_id=267438, emb_cong_id=null, embryo_embryo_id=null | `trat1_previous_et` | `11` | `1.0` |
| oocito_id=267439, emb_cong_id=null, embryo_embryo_id=null | `trat1_previous_et` | `11` | `1.0` |
| oocito_id=267440, emb_cong_id=null, embryo_embryo_id=null | `trat1_previous_et` | `11` | `1.0` |
| oocito_id=267441, emb_cong_id=null, embryo_embryo_id=null | `trat1_previous_et` | `11` | `1.0` |
| oocito_id=267442, emb_cong_id=null, embryo_embryo_id=null | `trat1_previous_et` | `11` | `1.0` |
| oocito_id=267443, emb_cong_id=null, embryo_embryo_id=null | `trat1_previous_et` | `11` | `1.0` |
| oocito_id=267444, emb_cong_id=null, embryo_embryo_id=null | `trat1_previous_et` | `11` | `1.0` |
| oocito_id=267445, emb_cong_id=null, embryo_embryo_id=null | `trat1_previous_et` | `11` | `1.0` |
| oocito_id=313960, emb_cong_id=null, embryo_embryo_id=null | `trat1_previous_et_od` | `<NA>` | `0.0` |
| oocito_id=314501, emb_cong_id=null, embryo_embryo_id=null | `trat1_previous_et_od` | `<NA>` | `0.0` |
| oocito_id=314502, emb_cong_id=null, embryo_embryo_id=null | `trat1_previous_et_od` | `<NA>` | `0.0` |
| oocito_id=314503, emb_cong_id=null, embryo_embryo_id=null | `trat1_previous_et_od` | `<NA>` | `0.0` |
| oocito_id=314504, emb_cong_id=null, embryo_embryo_id=null | `trat1_previous_et_od` | `<NA>` | `0.0` |
| oocito_id=314505, emb_cong_id=null, embryo_embryo_id=null | `trat1_previous_et_od` | `<NA>` | `0.0` |
| oocito_id=314506, emb_cong_id=null, embryo_embryo_id=null | `trat1_previous_et_od` | `<NA>` | `0.0` |
| oocito_id=314507, emb_cong_id=null, embryo_embryo_id=null | `trat1_previous_et_od` | `<NA>` | `0.0` |
| oocito_id=314508, emb_cong_id=null, embryo_embryo_id=null | `trat1_previous_et_od` | `<NA>` | `0.0` |
| oocito_id=314509, emb_cong_id=null, embryo_embryo_id=null | `trat1_previous_et_od` | `<NA>` | `0.0` |
| oocito_id=309960, emb_cong_id=null, embryo_embryo_id=null | `trat1_resultado_tratamento` | `nan` | `No transfer` |
| oocito_id=309962, emb_cong_id=null, embryo_embryo_id=null | `trat1_resultado_tratamento` | `nan` | `No transfer` |
| oocito_id=309963, emb_cong_id=null, embryo_embryo_id=null | `trat1_resultado_tratamento` | `nan` | `No transfer` |
| oocito_id=309964, emb_cong_id=null, embryo_embryo_id=null | `trat1_resultado_tratamento` | `nan` | `No transfer` |
| oocito_id=309965, emb_cong_id=null, embryo_embryo_id=null | `trat1_resultado_tratamento` | `nan` | `No transfer` |
| oocito_id=309966, emb_cong_id=null, embryo_embryo_id=null | `trat1_resultado_tratamento` | `nan` | `No transfer` |
| oocito_id=309968, emb_cong_id=null, embryo_embryo_id=null | `trat1_resultado_tratamento` | `nan` | `No transfer` |
| oocito_id=309969, emb_cong_id=null, embryo_embryo_id=null | `trat1_resultado_tratamento` | `nan` | `No transfer` |
| oocito_id=309970, emb_cong_id=null, embryo_embryo_id=null | `trat1_resultado_tratamento` | `nan` | `No transfer` |
| oocito_id=309971, emb_cong_id=null, embryo_embryo_id=null | `trat1_resultado_tratamento` | `nan` | `No transfer` |
| oocito_id=313960, emb_cong_id=null, embryo_embryo_id=null | `trat1_tentativa` | `nan` | `2` |
| oocito_id=314501, emb_cong_id=null, embryo_embryo_id=null | `trat1_tentativa` | `nan` | `1` |
| oocito_id=314502, emb_cong_id=null, embryo_embryo_id=null | `trat1_tentativa` | `nan` | `1` |
| oocito_id=314503, emb_cong_id=null, embryo_embryo_id=null | `trat1_tentativa` | `nan` | `1` |
| oocito_id=314504, emb_cong_id=null, embryo_embryo_id=null | `trat1_tentativa` | `nan` | `1` |
| oocito_id=314505, emb_cong_id=null, embryo_embryo_id=null | `trat1_tentativa` | `nan` | `1` |
| oocito_id=314506, emb_cong_id=null, embryo_embryo_id=null | `trat1_tentativa` | `nan` | `1` |
| oocito_id=314507, emb_cong_id=null, embryo_embryo_id=null | `trat1_tentativa` | `nan` | `1` |
| oocito_id=314508, emb_cong_id=null, embryo_embryo_id=null | `trat1_tentativa` | `nan` | `1` |
| oocito_id=314509, emb_cong_id=null, embryo_embryo_id=null | `trat1_tentativa` | `nan` | `1` |
| oocito_id=313960, emb_cong_id=null, embryo_embryo_id=null | `trat1_tipo_procedimento` | `nan` | `Ciclo a Fresco FIV` |
| oocito_id=314501, emb_cong_id=null, embryo_embryo_id=null | `trat1_tipo_procedimento` | `nan` | `Ciclo a Fresco FIV` |
| oocito_id=314502, emb_cong_id=null, embryo_embryo_id=null | `trat1_tipo_procedimento` | `nan` | `Ciclo a Fresco FIV` |
| oocito_id=314503, emb_cong_id=null, embryo_embryo_id=null | `trat1_tipo_procedimento` | `nan` | `Ciclo a Fresco FIV` |
| oocito_id=314504, emb_cong_id=null, embryo_embryo_id=null | `trat1_tipo_procedimento` | `nan` | `Ciclo a Fresco FIV` |
| oocito_id=314505, emb_cong_id=null, embryo_embryo_id=null | `trat1_tipo_procedimento` | `nan` | `Ciclo a Fresco FIV` |
| oocito_id=314506, emb_cong_id=null, embryo_embryo_id=null | `trat1_tipo_procedimento` | `nan` | `Ciclo a Fresco FIV` |
| oocito_id=314507, emb_cong_id=null, embryo_embryo_id=null | `trat1_tipo_procedimento` | `nan` | `Ciclo a Fresco FIV` |
| oocito_id=314508, emb_cong_id=null, embryo_embryo_id=null | `trat1_tipo_procedimento` | `nan` | `Ciclo a Fresco FIV` |
| oocito_id=314509, emb_cong_id=null, embryo_embryo_id=null | `trat1_tipo_procedimento` | `nan` | `Ciclo a Fresco FIV` |

### `planilha_embryoscope_combined`
#### **Schema Mismatches**
* Columns in DuckDB Only: `baby_1_weight`, `baby_2_weight`, `baby_3_weight`, `chart_or_pin`, `complications_of_pregnancy_specify`, `date_of_birth`, `embryo_Name_BlastExpandLast`, `embryo_Name_BlastomereSize`, `embryo_Name_EVEN2`, `embryo_Name_EVEN4`, `embryo_Name_EVEN8`, `embryo_Name_FRAG2`, `embryo_Name_FRAG2CAT`, `embryo_Name_FRAG4`, `embryo_Name_FRAG8`, `embryo_Name_Fragmentation`, `embryo_Name_ICM`, `embryo_Name_Line`, `embryo_Name_MN2Type`, `embryo_Name_MorphologicalGrade`, `embryo_Name_MultiNucleation`, `embryo_Name_Nuclei2`, `embryo_Name_PN`, `embryo_Name_Pulsing`, `embryo_Name_ReexpansionCount`, `embryo_Name_Strings`, `embryo_Name_TE`, `embryo_Name_t2`, `embryo_Name_t3`, `embryo_Name_t4`, `embryo_Name_t5`, `embryo_Name_t6`, `embryo_Name_t7`, `embryo_Name_t8`, `embryo_Name_t9`, `embryo_Name_tB`, `embryo_Name_tEB`, `embryo_Name_tHB`, `embryo_Name_tM`, `embryo_Name_tPB2`, `embryo_Name_tPNa`, `embryo_Name_tPNf`, `embryo_Name_tSB`, `embryo_Name_tSC`, `embryo_Time_BlastExpandLast`, `embryo_Time_BlastomereSize`, `embryo_Time_EVEN2`, `embryo_Time_EVEN4`, `embryo_Time_EVEN8`, `embryo_Time_FRAG2`, `embryo_Time_FRAG2CAT`, `embryo_Time_FRAG4`, `embryo_Time_FRAG8`, `embryo_Time_ICM`, `embryo_Time_Line`, `embryo_Time_MN2Type`, `embryo_Time_MorphologicalGrade`, `embryo_Time_MultiNucleation`, `embryo_Time_Nuclei2`, `embryo_Time_PN`, `embryo_Time_Pulsing`, `embryo_Time_ReexpansionCount`, `embryo_Time_Strings`, `embryo_Time_TE`, `embryo_Time_t6`, `embryo_Time_t7`, `embryo_Time_t9`, `embryo_Time_tPB2`, `embryo_Timestamp_BlastExpandLast`, `embryo_Timestamp_BlastomereSize`, `embryo_Timestamp_EVEN2`, `embryo_Timestamp_EVEN4`, `embryo_Timestamp_EVEN8`, `embryo_Timestamp_FRAG2`, `embryo_Timestamp_FRAG2CAT`, `embryo_Timestamp_FRAG4`, `embryo_Timestamp_FRAG8`, `embryo_Timestamp_Fragmentation`, `embryo_Timestamp_ICM`, `embryo_Timestamp_Line`, `embryo_Timestamp_MN2Type`, `embryo_Timestamp_MorphologicalGrade`, `embryo_Timestamp_MultiNucleation`, `embryo_Timestamp_Nuclei2`, `embryo_Timestamp_PN`, `embryo_Timestamp_Pulsing`, `embryo_Timestamp_ReexpansionCount`, `embryo_Timestamp_Strings`, `embryo_Timestamp_TE`, `embryo_Timestamp_t2`, `embryo_Timestamp_t3`, `embryo_Timestamp_t4`, `embryo_Timestamp_t5`, `embryo_Timestamp_t6`, `embryo_Timestamp_t7`, `embryo_Timestamp_t8`, `embryo_Timestamp_t9`, `embryo_Timestamp_tB`, `embryo_Timestamp_tEB`, `embryo_Timestamp_tHB`, `embryo_Timestamp_tM`, `embryo_Timestamp_tPB2`, `embryo_Timestamp_tPNa`, `embryo_Timestamp_tPNf`, `embryo_Timestamp_tSB`, `embryo_Timestamp_tSC`, `embryo_Value_BlastExpandLast`, `embryo_Value_BlastomereSize`, `embryo_Value_EVEN2`, `embryo_Value_EVEN4`, `embryo_Value_EVEN8`, `embryo_Value_FRAG2`, `embryo_Value_FRAG4`, `embryo_Value_FRAG8`, `embryo_Value_Fragmentation`, `embryo_Value_Line`, `embryo_Value_MorphologicalGrade`, `embryo_Value_MultiNucleation`, `embryo_Value_Strings`, `embryo_Value_t2`, `embryo_Value_t3`, `embryo_Value_t4`, `embryo_Value_t5`, `embryo_Value_t6`, `embryo_Value_t7`, `embryo_Value_t8`, `embryo_Value_t9`, `embryo_Value_tB`, `embryo_Value_tEB`, `embryo_Value_tHB`, `embryo_Value_tM`, `embryo_Value_tPB2`, `embryo_Value_tPNa`, `embryo_Value_tPNf`, `embryo_Value_tSB`, `embryo_Value_tSC`, `fet_data_crio`, `fet_dia_cryo`, `fet_dia_et`, `fet_file_name`, `fet_idade_do_cong_de_embriao`, `fet_idade_mulher`, `fet_no_da_transfer_1a_2a_3a`, `fet_no_et`, `fet_preparo_para_transferencia`, `fet_sheet_name`, `fet_tipo_1`, `fet_tipo_biopsia`, `fet_tipo_da_doacao`, `fet_tipo_de_fet`, `fet_tipo_de_tratamento`, `fresh_altura`, `fresh_data_crio`, `fresh_data_de_nasc`, `fresh_dia_cryo`, `fresh_fator_1`, `fresh_file_name`, `fresh_idade_espermatozoide`, `fresh_incubadora`, `fresh_no_biopsiados`, `fresh_opu`, `fresh_origem`, `fresh_peso`, `fresh_qtd_analisados`, `fresh_qtd_blasto`, `fresh_qtd_blasto_tq_a_e_b`, `fresh_qtd_normais`, `fresh_sheet_name`, `fresh_tipo`, `fresh_tipo_1`, `fresh_tipo_biopsia`, `fresh_tipo_de_inseminacao`, `fresh_total_de_mii`, `idascore_IDAScore`, `idascore_IDATime`, `idascore_IDAVersion`, `join_step_1`, `micro_CicloDoadora`, `micro_recepcao_ovulos`, `number_of_fet_after_originally_frozen`, `patient_DateOfBirth`, `patient_FirstName`, `patient_LastName`, `patient_PatientID`, `patient_PatientIDx`, `patient_YearOfBirth`, `patient_name`, `patient_unit_huntington`, `prontuario`, `trat1_idade_esposa`, `trat1_idade_marido`, `trat1_prontuario_doadora`, `trat2_idade_esposa`, `trat2_idade_marido`, `trat2_prontuario_doadora`, `treatment_TreatmentName`, `unidade`, `year`
* Columns in Athena Only: `date_of_embryo_transfer`, `date_when_embryos_were_cryopreserved`, `embryo_first_name`, `embryo_last_name`, `embryo_patient_date_of_birth`, `embryo_patient_id`, `embryo_patient_id_x`, `embryo_patient_sk`, `embryo_patient_year_of_birth`, `embryo_prontuario`, `embryo_treatment_name`, `embryo_unit_huntington`, `number_of_newborns`
#### **Discrepancy Group Isolation**
* **By Year/Date**:
  * Year **1981**: 3 discrepant records
  * Year **2000**: 2 discrepant records
  * Year **2010**: 23 discrepant records
  * Year **2011**: 22 discrepant records
  * Year **2012**: 6 discrepant records
  * Year **2016**: 32 discrepant records
  * Year **2017**: 12 discrepant records
  * Year **2019**: 52 discrepant records
  * Year **2020**: 778 discrepant records
  * Year **2021**: 9638 discrepant records
  * Year **2022**: 15693 discrepant records
  * Year **2023**: 15335 discrepant records
  * Year **2024**: 16986 discrepant records
  * Year **2025**: 16244 discrepant records
  * Year **2026**: 5948 discrepant records
  * Year **N/A**: 132254 discrepant records
* **By Dimension Group**:
  * Group ** Adrianne Mary Leao Sette e Oliveira**: 10 discrepant records
  * Group **Alex  Sander Miguel**: 137 discrepant records
  * Group **Alex Sander Jose Miguel**: 8 discrepant records
  * Group **Alexander Kopelman**: 759 discrepant records
  * Group **Alice Campos de Pinho Tavares **: 5 discrepant records
  * Group **Aléssio Calil Mathias**: 4 discrepant records
  * Group **Amanda Matos **: 6 discrepant records
  * Group **Amanda Oliveira Cutalo Prates**: 422 discrepant records
  * Group **Ana Claudia Moura Trigo**: 599 discrepant records
  * Group **Ana Lucia Beltrame**: 431 discrepant records
  * Group **Ana Luiza Alvarenga Gomes**: 72 discrepant records
  * Group **Ana Luiza Mattos Tavares**: 1598 discrepant records
  * Group **Ana Luiza Nunes**: 1619 discrepant records
  * Group **Ana Paula Aquino**: 36747 discrepant records
  * Group **Ana Rita de Paiva Toledo**: 242 discrepant records
  * Group **Andre Giannini Rodrigues**: 11 discrepant records
  * Group **Andrea de Fatima Castro**: 421 discrepant records
  * Group **Anelisa Bueno Pereira**: 82 discrepant records
  * Group **Anna Luiza Moraes Souza**: 43 discrepant records
  * Group **Antonio Hochgreb De Freitas Junior**: 36 discrepant records
  * Group **Arnaldo S Cambiaghi**: 4767 discrepant records
  * Group **Arnaldo Schizzi Cambiaghi**: 6396 discrepant records
  * Group **Barbara Souza Melo**: 1358 discrepant records
  * Group **Beatriz Cabral Pires**: 18 discrepant records
  * Group **Beatriz Passaro Biscaro**: 794 discrepant records
  * Group **Beatriz Pavin de Toledo**: 104 discrepant records
  * Group **Benilson Eustaqio de Souza**: 36 discrepant records
  * Group **Bruna Barros Cavalcante**: 924 discrepant records
  * Group **Bruna Costa Queiroz**: 617 discrepant records
  * Group **Bruna Cristina Lobo Santos**: 616 discrepant records
  * Group **Bruna Lobo Santos**: 168 discrepant records
  * Group **Camila Campos**: 269 discrepant records
  * Group **Camila Schizzi Cambiaghi**: 353 discrepant records
  * Group **Carla Maria Franco Dias **: 157 discrepant records
  * Group **Carla Martins**: 736 discrepant records
  * Group **Carlos Augusto Vieira de Moraes Filho**: 37 discrepant records
  * Group **Carolina Andrade Guedes dos Santos**: 6 discrepant records
  * Group **Carolina Zendron Machado Rudge**: 12 discrepant records
  * Group **Carolina de Andrade Melo e Souza**: 390 discrepant records
  * Group **Cesar Augusto Ferrari Teixeira**: 48 discrepant records
  * Group **Claudia Gomes Padilla**: 3590 discrepant records
  * Group **Cybele Lopes da Silva Lascala**: 11 discrepant records
  * Group **Daniel Suslik Zylbersztejn**: 61 discrepant records
  * Group **Daniela Boechat Gomide**: 1 discrepant records
  * Group **Daniela de Lima e Montes Castanho**: 97 discrepant records
  * Group **Daniella Spilborghs Castellotti**: 8594 discrepant records
  * Group **Davi Vischi Paluello**: 3 discrepant records
  * Group **Dayana Couto**: 1169 discrepant records
  * Group **Drauzio Oppenheimer**: 7 discrepant records
  * Group **Eduardo Leme Alves da Motta**: 6743 discrepant records
  * Group **Eduardo Yoneyama Mourthe**: 9 discrepant records
  * Group **Erica Becker de Sousa Xavier**: 7290 discrepant records
  * Group **Fabio Aiello Padilla**: 11 discrepant records
  * Group **Fabio Padilla**: 6 discrepant records
  * Group **Fabiola Cesconeto**: 111 discrepant records
  * Group **Fabyanne Mazutti da Silva **: 262 discrepant records
  * Group **Fernanda Marques Luz da Ressurreição**: 9 discrepant records
  * Group **Fernanda Peres Mastrocola**: 327 discrepant records
  * Group **Fernanda de Paula Rodrigues**: 2601 discrepant records
  * Group **Fernando Barboza De Lima **: 177 discrepant records
  * Group **Flavia Torelli**: 1059 discrepant records
  * Group **Flavio Ramirez Rosário**: 25 discrepant records
  * Group **Frederico Jose Silva Correa**: 700 discrepant records
  * Group **Fábio Costa Peixoto**: 4860 discrepant records
  * Group **Gabriela Franco Mourao de Oliveira**: 14 discrepant records
  * Group **Gabriela de Oliveira Ribeiro Lima**: 62 discrepant records
  * Group **Gabriella Dória Monteiro Cardoso**: 13 discrepant records
  * Group **Gabriella de Oliveira Ferreira**: 996 discrepant records
  * Group **Geraldo Caldeira**: 10830 discrepant records
  * Group **Gersia Araujo Viana**: 2015 discrepant records
  * Group **Giuliana Gatto**: 309 discrepant records
  * Group **Guilherme Jacom Abdulmassih Wood**: 11 discrepant records
  * Group **Guilherme Leme de Souza**: 37 discrepant records
  * Group **Gustavo Comodo**: 254 discrepant records
  * Group **Gustavo Nardini Cecchino**: 2 discrepant records
  * Group **Gustavo Teles**: 944 discrepant records
  * Group **Hanna Park**: 1628 discrepant records
  * Group **Helio Haddad Filho**: 359 discrepant records
  * Group **Herica Cristina Mendonça**: 6799 discrepant records
  * Group **Joao Frederico Luciano de Mello**: 9 discrepant records
  * Group **Joao Oscar Almeida Falcao Junior**: 12 discrepant records
  * Group **Joaquim Roberto Costa Lopes**: 1326 discrepant records
  * Group **Jorge Hallak**: 25 discrepant records
  * Group **Jose Geraldo Alves Caldeira**: 1609 discrepant records
  * Group **Josenice de Araujo SIlva Gomes**: 147 discrepant records
  * Group **João Pedro Junqueira Caetano**: 7431 discrepant records
  * Group **Juliana Halley Hatty**: 89 discrepant records
  * Group **Karen de Lima Souza Ortiz**: 28 discrepant records
  * Group **Karina de Sa Adami**: 35 discrepant records
  * Group **Karla Giusti Zacharias Fantin**: 1 discrepant records
  * Group **Keila Veludo Santos**: 18 discrepant records
  * Group **Laura Maria Almeida Maia**: 3303 discrepant records
  * Group **Lauriane G Schmidt De Abreu**: 163 discrepant records
  * Group **Layza Borges**: 3366 discrepant records
  * Group **Leci Veiga Caetano Amorim**: 5424 discrepant records
  * Group **Leonardo Araripe Dantas**: 108 discrepant records
  * Group **Leonardo Matheus Ribeiro Pereira**: 606 discrepant records
  * Group **Leticia Couto Motta Piccolo**: 18 discrepant records
  * Group **Lillian Silvestre Califre**: 11 discrepant records
  * Group **Livia Munhoz**: 3529 discrepant records
  * Group **Luana Lopes Toledo**: 730 discrepant records
  * Group **Luciana Campomizzi Calazans**: 5052 discrepant records
  * Group **Luciana Ferreira Potiguara Amador Sousa**: 786 discrepant records
  * Group **Luiz Fernando Bellintani **: 2 discrepant records
  * Group **Luiz Fernando Pina de Carvalho**: 50 discrepant records
  * Group **Manoela Porto Silva Resende**: 11 discrepant records
  * Group **Marcelo Afonso Goncalves**: 66 discrepant records
  * Group **Marcelo Afonso Gonçalves**: 414 discrepant records
  * Group **Marcelo Lopes Cancado**: 1827 discrepant records
  * Group **Marcos Eiji Shiroma**: 2206 discrepant records
  * Group **Marcos Eji Shiroma**: 208 discrepant records
  * Group **Maria Augusta Engler Tamm de Lima - VM**: 320 discrepant records
  * Group **Maria Juliana Albuquerque**: 2082 discrepant records
  * Group **Mariana Santana de A L Yoshida**: 14 discrepant records
  * Group **Mariana Santana de Almeida Liporoni Yoshida**: 7 discrepant records
  * Group **Marina de Melo Mendes**: 89 discrepant records
  * Group **Marjorie Fasolin **: 928 discrepant records
  * Group **Matheus Teixeira Roque**: 1493 discrepant records
  * Group **Mauricio Barbour Chehin**: 399 discrepant records
  * Group **Mauricio Chehin**: 6807 discrepant records
  * Group **Melissa Cavagnoli**: 159 discrepant records
  * Group **Michele Quaranta Panzan **: 3206 discrepant records
  * Group **Médico Externo**: 84 discrepant records
  * Group **Médico TI Huntington**: 5 discrepant records
  * Group **Mônica de Oliveira Jorge**: 2 discrepant records
  * Group **N/A**: 3976 discrepant records
  * Group **Nina Rotsen Santos Ferreira**: 9 discrepant records
  * Group **Patricia Santos Marques**: 256 discrepant records
  * Group **Paula Beatriz (desativado)**: 12 discrepant records
  * Group **Paula Beatriz Tavares Fettback**: 6 discrepant records
  * Group **Paula Bortolai Martins Araujo**: 4387 discrepant records
  * Group **Paula Vieira Nunes Brito**: 538 discrepant records
  * Group **Paulo Homem de Mello Bianchi**: 15 discrepant records
  * Group **Pedro Ivan de Almeida Pretti**: 10 discrepant records
  * Group **Pedro Paulo Bastos Filho**: 96 discrepant records
  * Group **Priscila Morais Galvão Sousa**: 68 discrepant records
  * Group **Priscilla Lopes Caldeira**: 236 discrepant records
  * Group **Pró-FIV**: 1239 discrepant records
  * Group **RAIMUNDO  CESAR PINHEIRO**: 22 discrepant records
  * Group **Rafael Lacordia**: 1700 discrepant records
  * Group **Raimundo Cesar Pinheiro**: 10 discrepant records
  * Group **Raphaela Menin Franco Martins**: 1526 discrepant records
  * Group **Renata Fioravanti Schaal**: 191 discrepant records
  * Group **Renato Fraietta**: 145 discrepant records
  * Group **Ricardo Barini**: 10 discrepant records
  * Group **Ricardo Mello Marinho**: 2697 discrepant records
  * Group **Rodrigo da Rosa Filho**: 20 discrepant records
  * Group **Rogerio de Barros Ferreira Leao**: 6347 discrepant records
  * Group **Samara Laham**: 94 discrepant records
  * Group **Simone Portugal Silva Lima**: 487 discrepant records
  * Group **Sofia Andrade de Oliveira**: 3468 discrepant records
  * Group **Stephanie Majer Franceschini Cinquetti**: 5 discrepant records
  * Group **Tatiana Magalhães Aguiar **: 47 discrepant records
  * Group **Tatianna Quintas Furtado Ribeiro**: 53 discrepant records
  * Group **Thais Cambiaghi**: 43 discrepant records
  * Group **Thais Sanches Domingues**: 6940 discrepant records
  * Group **Thomas Gabriel Miklos**: 14 discrepant records
  * Group **Valentina Nascimento Cotrim**: 1000 discrepant records
  * Group **Victoria Furquim Werneck Marinho**: 31 discrepant records
  * Group **Wendy Delmondes Galvão**: 29 discrepant records
  * Group **Zuleica Antunes Guimarães Cardoso **: 42 discrepant records

#### **Anonymized Column Discrepancy Samples**
| Composite Key | Column Name | DuckDB Value | Athena Value |
| :--- | :--- | :--- | :--- |
| oocito_id=187946, emb_cong_id=null, embryo_embryo_id=null | `date_of_delivery` | `NaT` | `2023-09-23` |
| oocito_id=187947, emb_cong_id=null, embryo_embryo_id=null | `date_of_delivery` | `NaT` | `2023-09-23` |
| oocito_id=187948, emb_cong_id=null, embryo_embryo_id=null | `date_of_delivery` | `NaT` | `2023-09-23` |
| oocito_id=187949, emb_cong_id=null, embryo_embryo_id=null | `date_of_delivery` | `NaT` | `2023-09-23` |
| oocito_id=187950, emb_cong_id=null, embryo_embryo_id=null | `date_of_delivery` | `NaT` | `2023-09-23` |
| oocito_id=187951, emb_cong_id=null, embryo_embryo_id=null | `date_of_delivery` | `NaT` | `2023-09-23` |
| oocito_id=203627, emb_cong_id=null, embryo_embryo_id=d2024.06.24_s02472_i3254_p-2 | `date_of_delivery` | `NaT` | `2025-06-30` |
| oocito_id=207719, emb_cong_id=null, embryo_embryo_id=null | `date_of_delivery` | `NaT` | `2025-09-05` |
| oocito_id=207720, emb_cong_id=null, embryo_embryo_id=null | `date_of_delivery` | `NaT` | `2025-09-05` |
| oocito_id=207722, emb_cong_id=null, embryo_embryo_id=null | `date_of_delivery` | `NaT` | `2025-09-05` |
| oocito_id=10201, emb_cong_id=null, embryo_embryo_id=d2021.08.06_s00833_i4120_p-2 | `embryo_description` | `nan` | `NAO HOUVE EXTRUSAO DO 2CP` |
| oocito_id=62156, emb_cong_id=null, embryo_embryo_id=d2019.07.26_s00080_i3254_p-2 | `embryo_description` | `nan` | `NF` |
| oocito_id=62157, emb_cong_id=null, embryo_embryo_id=d2019.07.26_s00080_i3254_p-3 | `embryo_description` | `nan` | `DEG` |
| oocito_id=122944, emb_cong_id=null, embryo_embryo_id=d2023.01.20_s01733_i4120_p-1 | `embryo_description` | `nan` | `BAIXO RISCO - ANEUPLOIDE COMPLEXO` |
| oocito_id=129920, emb_cong_id=null, embryo_embryo_id=d2023.02.21_s01775_i4120_p-7 | `embryo_description` | `nan` | `ANEUPLOIDE COMPLEXO XX` |
| oocito_id=130028, emb_cong_id=null, embryo_embryo_id=d2023.02.22_s01780_i4120_p-3 | `embryo_description` | `nan` | `Alterado complexo, Feminino` |
| oocito_id=187968, emb_cong_id=null, embryo_embryo_id=d2024.03.11_s02317_i3254_p-5 | `embryo_embryo_date` | `2024-03-11 00:00:00` | `2024-03-11` |
| oocito_id=188434, emb_cong_id=null, embryo_embryo_id=d2024.03.14_s02043_i4120_p-1 | `embryo_embryo_date` | `2024-03-14 00:00:00` | `2024-03-14` |
| oocito_id=188439, emb_cong_id=null, embryo_embryo_id=d2024.03.14_s02320_i3254_p-4 | `embryo_embryo_date` | `2024-03-14 00:00:00` | `2024-03-14` |
| oocito_id=189503, emb_cong_id=null, embryo_embryo_id=d2024.03.22_s02326_i3254_p-6 | `embryo_embryo_date` | `2024-03-22 00:00:00` | `2024-03-22` |
| oocito_id=189504, emb_cong_id=null, embryo_embryo_id=d2024.03.22_s02326_i3254_p-7 | `embryo_embryo_date` | `2024-03-22 00:00:00` | `2024-03-22` |
| oocito_id=189505, emb_cong_id=null, embryo_embryo_id=d2024.03.22_s02326_i3254_p-8 | `embryo_embryo_date` | `2024-03-22 00:00:00` | `2024-03-22` |
| oocito_id=187964, emb_cong_id=null, embryo_embryo_id=d2024.03.11_s02317_i3254_p-1 | `embryo_embryo_date` | `2024-03-11 00:00:00` | `2024-03-11` |
| oocito_id=187965, emb_cong_id=null, embryo_embryo_id=d2024.03.11_s02317_i3254_p-2 | `embryo_embryo_date` | `2024-03-11 00:00:00` | `2024-03-11` |
| oocito_id=187966, emb_cong_id=null, embryo_embryo_id=d2024.03.11_s02317_i3254_p-3 | `embryo_embryo_date` | `2024-03-11 00:00:00` | `2024-03-11` |
| oocito_id=188440, emb_cong_id=null, embryo_embryo_id=d2024.03.14_s02320_i3254_p-5 | `embryo_embryo_date` | `2024-03-14 00:00:00` | `2024-03-14` |
| oocito_id=247608, emb_cong_id=null, embryo_embryo_id=d2025.04.03_s02868_i3254_p-16 | `embryo_embryo_fate` | `Freeze` | `FrozenEmbryoTransfer` |
| oocito_id=264828, emb_cong_id=null, embryo_embryo_id=d2025.07.17_s03040_i3254_p-6 | `embryo_embryo_fate` | `Unknown` | `Avoid` |
| oocito_id=264829, emb_cong_id=null, embryo_embryo_id=d2025.07.17_s03040_i3254_p-7 | `embryo_embryo_fate` | `Unknown` | `Avoid` |
| oocito_id=264194, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-1 | `embryo_embryo_fate` | `Unknown` | `Avoid` |
| oocito_id=264196, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-3 | `embryo_embryo_fate` | `Unknown` | `Avoid` |
| oocito_id=264197, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-4 | `embryo_embryo_fate` | `Unknown` | `Avoid` |
| oocito_id=264198, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-5 | `embryo_embryo_fate` | `Unknown` | `Avoid` |
| oocito_id=264202, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-9 | `embryo_embryo_fate` | `Unknown` | `Avoid` |
| oocito_id=264203, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-10 | `embryo_embryo_fate` | `Unknown` | `Avoid` |
| oocito_id=264204, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-11 | `embryo_embryo_fate` | `Unknown` | `Avoid` |
| oocito_id=255663, emb_cong_id=null, embryo_embryo_id=d2021.06.01_s00709_i4120_p-3 | `embryo_fertilization_method` | `nan` | `ICSI` |
| oocito_id=255661, emb_cong_id=null, embryo_embryo_id=d2021.06.01_s00709_i4120_p-1 | `embryo_fertilization_method` | `nan` | `ICSI` |
| oocito_id=255662, emb_cong_id=null, embryo_embryo_id=d2021.06.01_s00709_i4120_p-2 | `embryo_fertilization_method` | `nan` | `ICSI` |
| oocito_id=255664, emb_cong_id=null, embryo_embryo_id=d2021.06.01_s00709_i4120_p-4 | `embryo_fertilization_method` | `nan` | `ICSI` |
| oocito_id=255665, emb_cong_id=null, embryo_embryo_id=d2021.06.01_s00709_i4120_p-5 | `embryo_fertilization_method` | `nan` | `ICSI` |
| oocito_id=255666, emb_cong_id=null, embryo_embryo_id=d2021.06.01_s00709_i4120_p-6 | `embryo_fertilization_method` | `nan` | `ICSI` |
| oocito_id=255667, emb_cong_id=null, embryo_embryo_id=d2021.06.01_s00709_i4120_p-7 | `embryo_fertilization_method` | `nan` | `ICSI` |
| oocito_id=255668, emb_cong_id=null, embryo_embryo_id=d2021.06.01_s00709_i4120_p-8 | `embryo_fertilization_method` | `nan` | `ICSI` |
| oocito_id=255669, emb_cong_id=null, embryo_embryo_id=d2021.06.01_s00709_i4120_p-9 | `embryo_fertilization_method` | `nan` | `ICSI` |
| oocito_id=255670, emb_cong_id=null, embryo_embryo_id=d2021.06.01_s00709_i4120_p-10 | `embryo_fertilization_method` | `nan` | `ICSI` |
| oocito_id=200940, emb_cong_id=null, embryo_embryo_id=d2024.06.08_s02445_i3254_p-2 | `embryo_kid_date` | `2025-02-05 00:00:00` | `2025-08-18 00:00:00` |
| oocito_id=200941, emb_cong_id=null, embryo_embryo_id=d2024.06.08_s02445_i3254_p-3 | `embryo_kid_date` | `2025-02-05 00:00:00` | `2025-08-18 00:00:00` |
| oocito_id=200943, emb_cong_id=null, embryo_embryo_id=d2024.06.08_s02445_i3254_p-5 | `embryo_kid_date` | `2025-02-05 00:00:00` | `2025-08-18 00:00:00` |
| oocito_id=264181, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s03038_i3254_p-2 | `embryo_kid_date` | `NaT` | `2025-07-24 00:00:00` |
| oocito_id=264183, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s03038_i3254_p-4 | `embryo_kid_date` | `NaT` | `2025-07-24 00:00:00` |
| oocito_id=264184, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s03038_i3254_p-5 | `embryo_kid_date` | `NaT` | `2025-07-24 00:00:00` |
| oocito_id=264185, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s03038_i3254_p-6 | `embryo_kid_date` | `NaT` | `2025-07-24 00:00:00` |
| oocito_id=264828, emb_cong_id=null, embryo_embryo_id=d2025.07.17_s03040_i3254_p-6 | `embryo_kid_date` | `NaT` | `2025-07-29 00:00:00` |
| oocito_id=264829, emb_cong_id=null, embryo_embryo_id=d2025.07.17_s03040_i3254_p-7 | `embryo_kid_date` | `NaT` | `2025-07-29 00:00:00` |
| oocito_id=264794, emb_cong_id=null, embryo_embryo_id=d2025.07.17_s03039_i3254_p-3 | `embryo_kid_date` | `NaT` | `2025-07-22 00:00:00` |
| oocito_id=264181, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s03038_i3254_p-2 | `embryo_kid_score` | `nan` | `1.7` |
| oocito_id=264183, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s03038_i3254_p-4 | `embryo_kid_score` | `nan` | `1.7` |
| oocito_id=264184, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s03038_i3254_p-5 | `embryo_kid_score` | `nan` | `1.7` |
| oocito_id=264185, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s03038_i3254_p-6 | `embryo_kid_score` | `nan` | `1.7` |
| oocito_id=264828, emb_cong_id=null, embryo_embryo_id=d2025.07.17_s03040_i3254_p-6 | `embryo_kid_score` | `nan` | `NA` |
| oocito_id=264829, emb_cong_id=null, embryo_embryo_id=d2025.07.17_s03040_i3254_p-7 | `embryo_kid_score` | `nan` | `NA` |
| oocito_id=264794, emb_cong_id=null, embryo_embryo_id=d2025.07.17_s03039_i3254_p-3 | `embryo_kid_score` | `nan` | `NA` |
| oocito_id=281558, emb_cong_id=null, embryo_embryo_id=d2025.10.17_s02497_i4120_p-2 | `embryo_kid_score` | `nan` | `1.7` |
| oocito_id=281560, emb_cong_id=null, embryo_embryo_id=d2025.10.17_s02497_i4120_p-4 | `embryo_kid_score` | `nan` | `NA` |
| oocito_id=281561, emb_cong_id=null, embryo_embryo_id=d2025.10.17_s02497_i4120_p-5 | `embryo_kid_score` | `nan` | `1.7` |
| oocito_id=200940, emb_cong_id=null, embryo_embryo_id=d2024.06.08_s02445_i3254_p-2 | `embryo_kid_user` | `BLENDA` | `TAYNARA` |
| oocito_id=200941, emb_cong_id=null, embryo_embryo_id=d2024.06.08_s02445_i3254_p-3 | `embryo_kid_user` | `BLENDA` | `TAYNARA` |
| oocito_id=200943, emb_cong_id=null, embryo_embryo_id=d2024.06.08_s02445_i3254_p-5 | `embryo_kid_user` | `BLENDA` | `TAYNARA` |
| oocito_id=264181, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s03038_i3254_p-2 | `embryo_kid_user` | `nan` | `BLENDA` |
| oocito_id=264183, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s03038_i3254_p-4 | `embryo_kid_user` | `nan` | `BLENDA` |
| oocito_id=264184, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s03038_i3254_p-5 | `embryo_kid_user` | `nan` | `BLENDA` |
| oocito_id=264185, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s03038_i3254_p-6 | `embryo_kid_user` | `nan` | `BLENDA` |
| oocito_id=264828, emb_cong_id=null, embryo_embryo_id=d2025.07.17_s03040_i3254_p-6 | `embryo_kid_user` | `nan` | `TAYNARA` |
| oocito_id=264829, emb_cong_id=null, embryo_embryo_id=d2025.07.17_s03040_i3254_p-7 | `embryo_kid_user` | `nan` | `TAYNARA` |
| oocito_id=264794, emb_cong_id=null, embryo_embryo_id=d2025.07.17_s03039_i3254_p-3 | `embryo_kid_user` | `nan` | `BLENDA` |
| oocito_id=264181, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s03038_i3254_p-2 | `embryo_kid_version` | `nan` | `KIDScoreD5 v3.3` |
| oocito_id=264183, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s03038_i3254_p-4 | `embryo_kid_version` | `nan` | `KIDScoreD5 v3.3` |
| oocito_id=264184, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s03038_i3254_p-5 | `embryo_kid_version` | `nan` | `KIDScoreD5 v3.3` |
| oocito_id=264185, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s03038_i3254_p-6 | `embryo_kid_version` | `nan` | `KIDScoreD5 v3.3` |
| oocito_id=264828, emb_cong_id=null, embryo_embryo_id=d2025.07.17_s03040_i3254_p-6 | `embryo_kid_version` | `nan` | `KIDScoreD5 v3.3` |
| oocito_id=264829, emb_cong_id=null, embryo_embryo_id=d2025.07.17_s03040_i3254_p-7 | `embryo_kid_version` | `nan` | `KIDScoreD5 v3.3` |
| oocito_id=264794, emb_cong_id=null, embryo_embryo_id=d2025.07.17_s03039_i3254_p-3 | `embryo_kid_version` | `nan` | `KIDScoreD5 v3.3` |
| oocito_id=281558, emb_cong_id=null, embryo_embryo_id=d2025.10.17_s02497_i4120_p-2 | `embryo_kid_version` | `nan` | `KIDScoreD5 v3.1` |
| oocito_id=281560, emb_cong_id=null, embryo_embryo_id=d2025.10.17_s02497_i4120_p-4 | `embryo_kid_version` | `nan` | `KIDScoreD5 v3.1` |
| oocito_id=281561, emb_cong_id=null, embryo_embryo_id=d2025.10.17_s02497_i4120_p-5 | `embryo_kid_version` | `nan` | `KIDScoreD5 v3.1` |
| oocito_id=268057, emb_cong_id=null, embryo_embryo_id=d2025.08.06_s02438_i4120_p-1 | `embryo_position` | `2` | `10.0` |
| oocito_id=268060, emb_cong_id=null, embryo_embryo_id=d2025.08.06_s02438_i4120_p-4 | `embryo_position` | `2` | `10.0` |
| oocito_id=264828, emb_cong_id=null, embryo_embryo_id=d2025.07.17_s03040_i3254_p-6 | `embryo_position` | `4` | `11.0` |
| oocito_id=264829, emb_cong_id=null, embryo_embryo_id=d2025.07.17_s03040_i3254_p-7 | `embryo_position` | `4` | `11.0` |
| oocito_id=264194, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-1 | `embryo_position` | `9` | `6.0` |
| oocito_id=264196, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-3 | `embryo_position` | `9` | `6.0` |
| oocito_id=264197, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-4 | `embryo_position` | `9` | `6.0` |
| oocito_id=264198, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-5 | `embryo_position` | `9` | `6.0` |
| oocito_id=264202, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-9 | `embryo_position` | `9` | `6.0` |
| oocito_id=264203, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-10 | `embryo_position` | `9` | `6.0` |
| oocito_id=188434, emb_cong_id=null, embryo_embryo_id=d2024.03.14_s02043_i4120_p-1 | `embryo_time_fragmentation` | `22.8` | `62.5` |
| oocito_id=187838, emb_cong_id=null, embryo_embryo_id=d2024.03.09_s02041_i4120_p-8 | `embryo_time_fragmentation` | `24.6` | `61.5` |
| oocito_id=188195, emb_cong_id=null, embryo_embryo_id=d2024.03.13_s02042_i4120_p-5 | `embryo_time_fragmentation` | `42.7` | `109.7` |
| oocito_id=188604, emb_cong_id=null, embryo_embryo_id=d2024.03.15_s02044_i4120_p-2 | `embryo_time_fragmentation` | `28.1` | `77.4` |
| oocito_id=188606, emb_cong_id=null, embryo_embryo_id=d2024.03.15_s02044_i4120_p-4 | `embryo_time_fragmentation` | `26.1` | `60.9` |
| oocito_id=190039, emb_cong_id=null, embryo_embryo_id=d2024.03.25_s02053_i4120_p-1 | `embryo_time_fragmentation` | `27.9` | `71.9` |
| oocito_id=190024, emb_cong_id=null, embryo_embryo_id=d2024.03.25_s02052_i4120_p-3 | `embryo_time_fragmentation` | `30.2` | `64.0` |
| oocito_id=190025, emb_cong_id=null, embryo_embryo_id=d2024.03.25_s02052_i4120_p-4 | `embryo_time_fragmentation` | `24.4` | `85.5` |
| oocito_id=190310, emb_cong_id=null, embryo_embryo_id=d2024.03.27_s02056_i4120_p-7 | `embryo_time_fragmentation` | `23.4` | `45.5` |
| oocito_id=190311, emb_cong_id=null, embryo_embryo_id=d2024.03.27_s02056_i4120_p-8 | `embryo_time_fragmentation` | `25.3` | `80.8` |
| oocito_id=264196, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-3 | `embryo_time_t2` | `nan` | `27.4` |
| oocito_id=264197, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-4 | `embryo_time_t2` | `nan` | `28.7` |
| oocito_id=264202, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-9 | `embryo_time_t2` | `nan` | `36.5` |
| oocito_id=269976, emb_cong_id=null, embryo_embryo_id=d2025.08.18_s02446_i4120_p-1 | `embryo_time_t2` | `nan` | `24.9` |
| oocito_id=269980, emb_cong_id=null, embryo_embryo_id=d2025.08.18_s02445_i4120_p-5 | `embryo_time_t2` | `nan` | `24.3` |
| oocito_id=269978, emb_cong_id=null, embryo_embryo_id=d2025.08.18_s02446_i4120_p-3 | `embryo_time_t2` | `nan` | `27.7` |
| oocito_id=268053, emb_cong_id=null, embryo_embryo_id=d2025.08.06_s02437_i4120_p-9 | `embryo_time_t2` | `nan` | `29.6` |
| oocito_id=278120, emb_cong_id=null, embryo_embryo_id=d2025.09.29_s02477_i4120_p-1 | `embryo_time_t2` | `nan` | `49.2` |
| oocito_id=282074, emb_cong_id=null, embryo_embryo_id=d2025.10.20_s02499_i4120_p-2 | `embryo_time_t2` | `nan` | `35.5` |
| oocito_id=282077, emb_cong_id=null, embryo_embryo_id=d2025.10.20_s02499_i4120_p-5 | `embryo_time_t2` | `nan` | `34.7` |
| oocito_id=264194, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-1 | `embryo_time_t3` | `nan` | `60.0` |
| oocito_id=264196, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-3 | `embryo_time_t3` | `nan` | `38.8` |
| oocito_id=264197, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-4 | `embryo_time_t3` | `nan` | `37.0` |
| oocito_id=264202, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-9 | `embryo_time_t3` | `nan` | `37.9` |
| oocito_id=269976, emb_cong_id=null, embryo_embryo_id=d2025.08.18_s02446_i4120_p-1 | `embryo_time_t3` | `nan` | `35.4` |
| oocito_id=269980, emb_cong_id=null, embryo_embryo_id=d2025.08.18_s02445_i4120_p-5 | `embryo_time_t3` | `nan` | `27.3` |
| oocito_id=269977, emb_cong_id=null, embryo_embryo_id=d2025.08.18_s02446_i4120_p-2 | `embryo_time_t3` | `nan` | `32.2` |
| oocito_id=269978, emb_cong_id=null, embryo_embryo_id=d2025.08.18_s02446_i4120_p-3 | `embryo_time_t3` | `nan` | `27.9` |
| oocito_id=268053, emb_cong_id=null, embryo_embryo_id=d2025.08.06_s02437_i4120_p-9 | `embryo_time_t3` | `nan` | `43.1` |
| oocito_id=278120, emb_cong_id=null, embryo_embryo_id=d2025.09.29_s02477_i4120_p-1 | `embryo_time_t3` | `nan` | `50.6` |
| oocito_id=264194, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-1 | `embryo_time_t4` | `nan` | `62.2` |
| oocito_id=264196, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-3 | `embryo_time_t4` | `nan` | `40.4` |
| oocito_id=264197, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-4 | `embryo_time_t4` | `nan` | `38.3` |
| oocito_id=264202, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-9 | `embryo_time_t4` | `nan` | `39.1` |
| oocito_id=269976, emb_cong_id=null, embryo_embryo_id=d2025.08.18_s02446_i4120_p-1 | `embryo_time_t4` | `nan` | `35.7` |
| oocito_id=269980, emb_cong_id=null, embryo_embryo_id=d2025.08.18_s02445_i4120_p-5 | `embryo_time_t4` | `nan` | `31.0` |
| oocito_id=269977, emb_cong_id=null, embryo_embryo_id=d2025.08.18_s02446_i4120_p-2 | `embryo_time_t4` | `nan` | `32.4` |
| oocito_id=269978, emb_cong_id=null, embryo_embryo_id=d2025.08.18_s02446_i4120_p-3 | `embryo_time_t4` | `nan` | `38.7` |
| oocito_id=268053, emb_cong_id=null, embryo_embryo_id=d2025.08.06_s02437_i4120_p-9 | `embryo_time_t4` | `nan` | `46.4` |
| oocito_id=278120, emb_cong_id=null, embryo_embryo_id=d2025.09.29_s02477_i4120_p-1 | `embryo_time_t4` | `nan` | `79.9` |
| oocito_id=264194, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-1 | `embryo_time_t5` | `nan` | `65.6` |
| oocito_id=264196, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-3 | `embryo_time_t5` | `nan` | `51.5` |
| oocito_id=264197, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-4 | `embryo_time_t5` | `nan` | `39.4` |
| oocito_id=264202, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-9 | `embryo_time_t5` | `nan` | `49.8` |
| oocito_id=269976, emb_cong_id=null, embryo_embryo_id=d2025.08.18_s02446_i4120_p-1 | `embryo_time_t5` | `nan` | `44.7` |
| oocito_id=269980, emb_cong_id=null, embryo_embryo_id=d2025.08.18_s02445_i4120_p-5 | `embryo_time_t5` | `nan` | `38.0` |
| oocito_id=269977, emb_cong_id=null, embryo_embryo_id=d2025.08.18_s02446_i4120_p-2 | `embryo_time_t5` | `nan` | `40.8` |
| oocito_id=269978, emb_cong_id=null, embryo_embryo_id=d2025.08.18_s02446_i4120_p-3 | `embryo_time_t5` | `nan` | `45.4` |
| oocito_id=268053, emb_cong_id=null, embryo_embryo_id=d2025.08.06_s02437_i4120_p-9 | `embryo_time_t5` | `nan` | `73.2` |
| oocito_id=278120, emb_cong_id=null, embryo_embryo_id=d2025.09.29_s02477_i4120_p-1 | `embryo_time_t5` | `nan` | `81.0` |
| oocito_id=264194, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-1 | `embryo_time_t8` | `nan` | `70.8` |
| oocito_id=264196, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-3 | `embryo_time_t8` | `nan` | `63.2` |
| oocito_id=264197, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-4 | `embryo_time_t8` | `nan` | `53.6` |
| oocito_id=264202, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-9 | `embryo_time_t8` | `nan` | `68.5` |
| oocito_id=269976, emb_cong_id=null, embryo_embryo_id=d2025.08.18_s02446_i4120_p-1 | `embryo_time_t8` | `nan` | `48.9` |
| oocito_id=269980, emb_cong_id=null, embryo_embryo_id=d2025.08.18_s02445_i4120_p-5 | `embryo_time_t8` | `nan` | `52.3` |
| oocito_id=269977, emb_cong_id=null, embryo_embryo_id=d2025.08.18_s02446_i4120_p-2 | `embryo_time_t8` | `nan` | `53.0` |
| oocito_id=269978, emb_cong_id=null, embryo_embryo_id=d2025.08.18_s02446_i4120_p-3 | `embryo_time_t8` | `nan` | `52.7` |
| oocito_id=268053, emb_cong_id=null, embryo_embryo_id=d2025.08.06_s02437_i4120_p-9 | `embryo_time_t8` | `nan` | `110.9` |
| oocito_id=282273, emb_cong_id=null, embryo_embryo_id=d2025.10.21_s02500_i4120_p-2 | `embryo_time_t8` | `nan` | `53.5` |
| oocito_id=269976, emb_cong_id=null, embryo_embryo_id=d2025.08.18_s02446_i4120_p-1 | `embryo_time_tb` | `nan` | `101.1` |
| oocito_id=269977, emb_cong_id=null, embryo_embryo_id=d2025.08.18_s02446_i4120_p-2 | `embryo_time_tb` | `nan` | `99.2` |
| oocito_id=277667, emb_cong_id=null, embryo_embryo_id=d2025.09.26_s02475_i4120_p-3 | `embryo_time_tb` | `nan` | `118.5` |
| oocito_id=284232, emb_cong_id=null, embryo_embryo_id=d2025.11.01_s02510_i4120_p-11 | `embryo_time_tb` | `nan` | `116.5` |
| oocito_id=284234, emb_cong_id=null, embryo_embryo_id=d2025.11.01_s02510_i4120_p-13 | `embryo_time_tb` | `nan` | `97.1` |
| oocito_id=287030, emb_cong_id=null, embryo_embryo_id=d2025.11.19_s02523_i4120_p-1 | `embryo_time_tb` | `nan` | `104.8` |
| oocito_id=298566, emb_cong_id=null, embryo_embryo_id=d2026.02.05_s02568_i4120_p-9 | `embryo_time_tb` | `nan` | `141.9` |
| oocito_id=313426, emb_cong_id=null, embryo_embryo_id=d2026.05.13_s02626_i4120_p-3 | `embryo_time_tb` | `nan` | `99.5` |
| oocito_id=269976, emb_cong_id=null, embryo_embryo_id=d2025.08.18_s02446_i4120_p-1 | `embryo_time_teb` | `nan` | `106.0` |
| oocito_id=269977, emb_cong_id=null, embryo_embryo_id=d2025.08.18_s02446_i4120_p-2 | `embryo_time_teb` | `nan` | `106.4` |
| oocito_id=277667, emb_cong_id=null, embryo_embryo_id=d2025.09.26_s02475_i4120_p-3 | `embryo_time_teb` | `nan` | `127.0` |
| oocito_id=287030, emb_cong_id=null, embryo_embryo_id=d2025.11.19_s02523_i4120_p-1 | `embryo_time_teb` | `nan` | `114.9` |
| oocito_id=313426, emb_cong_id=null, embryo_embryo_id=d2026.05.13_s02626_i4120_p-3 | `embryo_time_teb` | `nan` | `105.6` |
| oocito_id=189240, emb_cong_id=null, embryo_embryo_id=d2024.03.20_s02325_i3254_p-2 | `embryo_time_thb` | `nan` | `111.3` |
| oocito_id=189549, emb_cong_id=null, embryo_embryo_id=d2024.03.22_s02327_i3254_p-9 | `embryo_time_thb` | `nan` | `111.2` |
| oocito_id=191926, emb_cong_id=null, embryo_embryo_id=d2024.04.06_s02345_i3254_p-5 | `embryo_time_thb` | `nan` | `99.8` |
| oocito_id=195862, emb_cong_id=null, embryo_embryo_id=d2024.05.06_s02384_i3254_p-1 | `embryo_time_thb` | `nan` | `112.8` |
| oocito_id=196745, emb_cong_id=null, embryo_embryo_id=d2024.05.10_s02394_i3254_p-3 | `embryo_time_thb` | `nan` | `98.7` |
| oocito_id=196746, emb_cong_id=null, embryo_embryo_id=d2024.05.10_s02394_i3254_p-4 | `embryo_time_thb` | `nan` | `123.2` |
| oocito_id=197369, emb_cong_id=null, embryo_embryo_id=d2024.05.15_s02402_i3254_p-3 | `embryo_time_thb` | `nan` | `117.8` |
| oocito_id=197370, emb_cong_id=null, embryo_embryo_id=d2024.05.15_s02402_i3254_p-4 | `embryo_time_thb` | `nan` | `115.8` |
| oocito_id=197961, emb_cong_id=null, embryo_embryo_id=d2024.05.18_s02408_i3254_p-2 | `embryo_time_thb` | `nan` | `109.7` |
| oocito_id=197963, emb_cong_id=null, embryo_embryo_id=d2024.05.18_s02408_i3254_p-4 | `embryo_time_thb` | `nan` | `94.6` |
| oocito_id=264196, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-3 | `embryo_time_tm` | `nan` | `90.5` |
| oocito_id=264197, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-4 | `embryo_time_tm` | `nan` | `91.1` |
| oocito_id=268332, emb_cong_id=null, embryo_embryo_id=d2025.08.08_s02441_i4120_p-3 | `embryo_time_tm` | `nan` | `103.9` |
| oocito_id=268338, emb_cong_id=null, embryo_embryo_id=d2025.08.08_s02441_i4120_p-9 | `embryo_time_tm` | `nan` | `116.3` |
| oocito_id=269976, emb_cong_id=null, embryo_embryo_id=d2025.08.18_s02446_i4120_p-1 | `embryo_time_tm` | `nan` | `83.9` |
| oocito_id=269980, emb_cong_id=null, embryo_embryo_id=d2025.08.18_s02445_i4120_p-5 | `embryo_time_tm` | `nan` | `86.0` |
| oocito_id=269977, emb_cong_id=null, embryo_embryo_id=d2025.08.18_s02446_i4120_p-2 | `embryo_time_tm` | `nan` | `82.9` |
| oocito_id=277667, emb_cong_id=null, embryo_embryo_id=d2025.09.26_s02475_i4120_p-3 | `embryo_time_tm` | `nan` | `95.3` |
| oocito_id=280545, emb_cong_id=null, embryo_embryo_id=d2025.10.11_s02491_i4120_p-1 | `embryo_time_tm` | `nan` | `100.2` |
| oocito_id=280547, emb_cong_id=null, embryo_embryo_id=d2025.10.11_s02491_i4120_p-3 | `embryo_time_tm` | `nan` | `89.2` |
| oocito_id=278120, emb_cong_id=null, embryo_embryo_id=d2025.09.29_s02477_i4120_p-1 | `embryo_time_tpna` | `nan` | `31.8` |
| oocito_id=264194, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-1 | `embryo_time_tpnf` | `nan` | `57.4` |
| oocito_id=264196, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-3 | `embryo_time_tpnf` | `nan` | `24.9` |
| oocito_id=264202, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-9 | `embryo_time_tpnf` | `nan` | `34.2` |
| oocito_id=269978, emb_cong_id=null, embryo_embryo_id=d2025.08.18_s02446_i4120_p-3 | `embryo_time_tpnf` | `nan` | `26.1` |
| oocito_id=278120, emb_cong_id=null, embryo_embryo_id=d2025.09.29_s02477_i4120_p-1 | `embryo_time_tpnf` | `nan` | `46.0` |
| oocito_id=282073, emb_cong_id=null, embryo_embryo_id=d2025.10.20_s02499_i4120_p-1 | `embryo_time_tpnf` | `nan` | `33.8` |
| oocito_id=282074, emb_cong_id=null, embryo_embryo_id=d2025.10.20_s02499_i4120_p-2 | `embryo_time_tpnf` | `nan` | `32.9` |
| oocito_id=282077, emb_cong_id=null, embryo_embryo_id=d2025.10.20_s02499_i4120_p-5 | `embryo_time_tpnf` | `nan` | `32.5` |
| oocito_id=282080, emb_cong_id=null, embryo_embryo_id=d2025.10.20_s02499_i4120_p-8 | `embryo_time_tpnf` | `nan` | `31.4` |
| oocito_id=282273, emb_cong_id=null, embryo_embryo_id=d2025.10.21_s02500_i4120_p-2 | `embryo_time_tpnf` | `nan` | `31.0` |
| oocito_id=264196, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-3 | `embryo_time_tsb` | `nan` | `103.7` |
| oocito_id=268332, emb_cong_id=null, embryo_embryo_id=d2025.08.08_s02441_i4120_p-3 | `embryo_time_tsb` | `nan` | `110.9` |
| oocito_id=269976, emb_cong_id=null, embryo_embryo_id=d2025.08.18_s02446_i4120_p-1 | `embryo_time_tsb` | `nan` | `88.1` |
| oocito_id=269977, emb_cong_id=null, embryo_embryo_id=d2025.08.18_s02446_i4120_p-2 | `embryo_time_tsb` | `nan` | `90.1` |
| oocito_id=277667, emb_cong_id=null, embryo_embryo_id=d2025.09.26_s02475_i4120_p-3 | `embryo_time_tsb` | `nan` | `98.4` |
| oocito_id=280545, emb_cong_id=null, embryo_embryo_id=d2025.10.11_s02491_i4120_p-1 | `embryo_time_tsb` | `nan` | `105.0` |
| oocito_id=284232, emb_cong_id=null, embryo_embryo_id=d2025.11.01_s02510_i4120_p-11 | `embryo_time_tsb` | `nan` | `110.4` |
| oocito_id=284234, emb_cong_id=null, embryo_embryo_id=d2025.11.01_s02510_i4120_p-13 | `embryo_time_tsb` | `nan` | `93.9` |
| oocito_id=284379, emb_cong_id=null, embryo_embryo_id=d2025.11.03_s02512_i4120_p-2 | `embryo_time_tsb` | `nan` | `116.9` |
| oocito_id=284384, emb_cong_id=null, embryo_embryo_id=d2025.11.03_s02512_i4120_p-7 | `embryo_time_tsb` | `nan` | `111.3` |
| oocito_id=264194, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-1 | `embryo_time_tsc` | `nan` | `115.6` |
| oocito_id=264196, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-3 | `embryo_time_tsc` | `nan` | `76.7` |
| oocito_id=264197, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-4 | `embryo_time_tsc` | `nan` | `66.0` |
| oocito_id=264202, emb_cong_id=null, embryo_embryo_id=d2025.07.14_s02420_i4120_p-9 | `embryo_time_tsc` | `nan` | `90.2` |
| oocito_id=268332, emb_cong_id=null, embryo_embryo_id=d2025.08.08_s02441_i4120_p-3 | `embryo_time_tsc` | `nan` | `90.0` |
| oocito_id=268335, emb_cong_id=null, embryo_embryo_id=d2025.08.08_s02441_i4120_p-6 | `embryo_time_tsc` | `nan` | `83.1` |
| oocito_id=268338, emb_cong_id=null, embryo_embryo_id=d2025.08.08_s02441_i4120_p-9 | `embryo_time_tsc` | `nan` | `83.3` |
| oocito_id=269976, emb_cong_id=null, embryo_embryo_id=d2025.08.18_s02446_i4120_p-1 | `embryo_time_tsc` | `nan` | `79.4` |
| oocito_id=269980, emb_cong_id=null, embryo_embryo_id=d2025.08.18_s02445_i4120_p-5 | `embryo_time_tsc` | `nan` | `79.0` |
| oocito_id=269977, emb_cong_id=null, embryo_embryo_id=d2025.08.18_s02446_i4120_p-2 | `embryo_time_tsc` | `nan` | `78.6` |
| oocito_id=269976, emb_cong_id=null, embryo_embryo_id=d2025.08.18_s02446_i4120_p-1 | `embryo_value_icm` | `nan` | `B` |
| oocito_id=269977, emb_cong_id=null, embryo_embryo_id=d2025.08.18_s02446_i4120_p-2 | `embryo_value_icm` | `nan` | `B` |
| oocito_id=277667, emb_cong_id=null, embryo_embryo_id=d2025.09.26_s02475_i4120_p-3 | `embryo_value_icm` | `nan` | `C` |
| oocito_id=287030, emb_cong_id=null, embryo_embryo_id=d2025.11.19_s02523_i4120_p-1 | `embryo_value_icm` | `nan` | `A` |
| oocito_id=298566, emb_cong_id=null, embryo_embryo_id=d2026.02.05_s02568_i4120_p-9 | `embryo_value_icm` | `nan` | `B` |
| oocito_id=313426, emb_cong_id=null, embryo_embryo_id=d2026.05.13_s02626_i4120_p-3 | `embryo_value_icm` | `nan` | `A` |
| oocito_id=190308, emb_cong_id=null, embryo_embryo_id=d2024.03.27_s02056_i4120_p-5 | `embryo_value_pn` | `0` | `4` |
| oocito_id=207193, emb_cong_id=null, embryo_embryo_id=d2024.07.17_s02150_i4120_p-11 | `embryo_value_pn` | `0` | `1` |
| oocito_id=209653, emb_cong_id=null, embryo_embryo_id=d2024.08.02_s02157_i4120_p-1 | `embryo_value_pn` | `2` | `3` |
| oocito_id=213230, emb_cong_id=null, embryo_embryo_id=d2024.08.22_s02186_i4120_p-12 | `embryo_value_pn` | `1` | `2` |
| oocito_id=253432, emb_cong_id=null, embryo_embryo_id=d2025.05.09_s02377_i4120_p-3 | `embryo_value_pn` | `0` | `2` |
| oocito_id=255664, emb_cong_id=null, embryo_embryo_id=d2021.06.01_s00709_i4120_p-4 | `embryo_value_pn` | `2` | `3` |
| oocito_id=257666, emb_cong_id=null, embryo_embryo_id=d2025.06.02_s02396_i4120_p-2 | `embryo_value_pn` | `2` | `3` |
| oocito_id=272035, emb_cong_id=null, embryo_embryo_id=d2025.08.28_s02456_i4120_p-4 | `embryo_value_pn` | `2` | `3` |
| oocito_id=278120, emb_cong_id=null, embryo_embryo_id=d2025.09.29_s02477_i4120_p-1 | `embryo_value_pn` | `0` | `2` |
| oocito_id=282080, emb_cong_id=null, embryo_embryo_id=d2025.10.20_s02499_i4120_p-8 | `embryo_value_pn` | `2` | `3` |
| oocito_id=269976, emb_cong_id=null, embryo_embryo_id=d2025.08.18_s02446_i4120_p-1 | `embryo_value_te` | `nan` | `B` |
| oocito_id=269977, emb_cong_id=null, embryo_embryo_id=d2025.08.18_s02446_i4120_p-2 | `embryo_value_te` | `nan` | `B` |
| oocito_id=277667, emb_cong_id=null, embryo_embryo_id=d2025.09.26_s02475_i4120_p-3 | `embryo_value_te` | `nan` | `C` |
| oocito_id=287030, emb_cong_id=null, embryo_embryo_id=d2025.11.19_s02523_i4120_p-1 | `embryo_value_te` | `nan` | `B` |
| oocito_id=298566, emb_cong_id=null, embryo_embryo_id=d2026.02.05_s02568_i4120_p-9 | `embryo_value_te` | `nan` | `C` |
| oocito_id=313426, emb_cong_id=null, embryo_embryo_id=d2026.05.13_s02626_i4120_p-3 | `embryo_value_te` | `nan` | `A` |
| oocito_id=188048, emb_cong_id=null, embryo_embryo_id=null | `fet_gravidez_bioquimica` | `nan` | `1` |
| oocito_id=188049, emb_cong_id=null, embryo_embryo_id=null | `fet_gravidez_bioquimica` | `nan` | `1` |
| oocito_id=188050, emb_cong_id=null, embryo_embryo_id=null | `fet_gravidez_bioquimica` | `nan` | `1` |
| oocito_id=188051, emb_cong_id=null, embryo_embryo_id=null | `fet_gravidez_bioquimica` | `nan` | `1` |
| oocito_id=188052, emb_cong_id=null, embryo_embryo_id=null | `fet_gravidez_bioquimica` | `nan` | `1` |
| oocito_id=188053, emb_cong_id=null, embryo_embryo_id=null | `fet_gravidez_bioquimica` | `nan` | `1` |
| oocito_id=188644, emb_cong_id=null, embryo_embryo_id=null | `fet_gravidez_bioquimica` | `nan` | `X` |
| oocito_id=188645, emb_cong_id=null, embryo_embryo_id=null | `fet_gravidez_bioquimica` | `nan` | `X` |
| oocito_id=188646, emb_cong_id=null, embryo_embryo_id=null | `fet_gravidez_bioquimica` | `nan` | `X` |
| oocito_id=188647, emb_cong_id=null, embryo_embryo_id=null | `fet_gravidez_bioquimica` | `nan` | `X` |
| oocito_id=187946, emb_cong_id=null, embryo_embryo_id=null | `fet_gravidez_clinica` | `1` | `nan` |
| oocito_id=187947, emb_cong_id=null, embryo_embryo_id=null | `fet_gravidez_clinica` | `1` | `nan` |
| oocito_id=187948, emb_cong_id=null, embryo_embryo_id=null | `fet_gravidez_clinica` | `1` | `nan` |
| oocito_id=187949, emb_cong_id=null, embryo_embryo_id=null | `fet_gravidez_clinica` | `1` | `nan` |
| oocito_id=187950, emb_cong_id=null, embryo_embryo_id=null | `fet_gravidez_clinica` | `1` | `nan` |
| oocito_id=187951, emb_cong_id=null, embryo_embryo_id=null | `fet_gravidez_clinica` | `1` | `nan` |
| oocito_id=188048, emb_cong_id=null, embryo_embryo_id=null | `fet_gravidez_clinica` | `nan` | `1` |
| oocito_id=188049, emb_cong_id=null, embryo_embryo_id=null | `fet_gravidez_clinica` | `nan` | `1` |
| oocito_id=188050, emb_cong_id=null, embryo_embryo_id=null | `fet_gravidez_clinica` | `nan` | `1` |
| oocito_id=188051, emb_cong_id=null, embryo_embryo_id=null | `fet_gravidez_clinica` | `nan` | `1` |
| oocito_id=186890, emb_cong_id=null, embryo_embryo_id=null | `fet_resultado` | `nan` | `NEGATIVO` |
| oocito_id=186891, emb_cong_id=null, embryo_embryo_id=null | `fet_resultado` | `nan` | `NEGATIVO` |
| oocito_id=187968, emb_cong_id=null, embryo_embryo_id=d2024.03.11_s02317_i3254_p-5 | `fet_resultado` | `nan` | `EMBRYO TRANSFER` |
| oocito_id=189503, emb_cong_id=null, embryo_embryo_id=d2024.03.22_s02326_i3254_p-6 | `fet_resultado` | `nan` | `EMBRYO TRANSFER` |
| oocito_id=189504, emb_cong_id=null, embryo_embryo_id=d2024.03.22_s02326_i3254_p-7 | `fet_resultado` | `nan` | `EMBRYO TRANSFER` |
| oocito_id=189505, emb_cong_id=null, embryo_embryo_id=d2024.03.22_s02326_i3254_p-8 | `fet_resultado` | `nan` | `EMBRYO TRANSFER` |
| oocito_id=187964, emb_cong_id=null, embryo_embryo_id=d2024.03.11_s02317_i3254_p-1 | `fet_resultado` | `nan` | `EMBRYO TRANSFER` |
| oocito_id=187965, emb_cong_id=null, embryo_embryo_id=d2024.03.11_s02317_i3254_p-2 | `fet_resultado` | `nan` | `EMBRYO TRANSFER` |
| oocito_id=187966, emb_cong_id=null, embryo_embryo_id=d2024.03.11_s02317_i3254_p-3 | `fet_resultado` | `nan` | `EMBRYO TRANSFER` |
| oocito_id=189500, emb_cong_id=null, embryo_embryo_id=d2024.03.22_s02326_i3254_p-3 | `fet_resultado` | `nan` | `EMBRYO TRANSFER` |
| oocito_id=186890, emb_cong_id=null, embryo_embryo_id=null | `fet_tipo_resultado` | `nan` | `TRANSFERÊNCIA DE CONGELADOS` |
| oocito_id=186891, emb_cong_id=null, embryo_embryo_id=null | `fet_tipo_resultado` | `nan` | `TRANSFERÊNCIA DE CONGELADOS` |
| oocito_id=187968, emb_cong_id=null, embryo_embryo_id=d2024.03.11_s02317_i3254_p-5 | `fet_tipo_resultado` | `nan` | `NÃO ENGRAVIDOU` |
| oocito_id=189503, emb_cong_id=null, embryo_embryo_id=d2024.03.22_s02326_i3254_p-6 | `fet_tipo_resultado` | `nan` | `POSITIVO` |
| oocito_id=189504, emb_cong_id=null, embryo_embryo_id=d2024.03.22_s02326_i3254_p-7 | `fet_tipo_resultado` | `nan` | `POSITIVO` |
| oocito_id=189505, emb_cong_id=null, embryo_embryo_id=d2024.03.22_s02326_i3254_p-8 | `fet_tipo_resultado` | `nan` | `POSITIVO` |
| oocito_id=187964, emb_cong_id=null, embryo_embryo_id=d2024.03.11_s02317_i3254_p-1 | `fet_tipo_resultado` | `nan` | `NÃO ENGRAVIDOU` |
| oocito_id=187965, emb_cong_id=null, embryo_embryo_id=d2024.03.11_s02317_i3254_p-2 | `fet_tipo_resultado` | `nan` | `NÃO ENGRAVIDOU` |
| oocito_id=187966, emb_cong_id=null, embryo_embryo_id=d2024.03.11_s02317_i3254_p-3 | `fet_tipo_resultado` | `nan` | `NÃO ENGRAVIDOU` |
| oocito_id=189500, emb_cong_id=null, embryo_embryo_id=d2024.03.22_s02326_i3254_p-3 | `fet_tipo_resultado` | `nan` | `POSITIVO` |
| oocito_id=187946, emb_cong_id=null, embryo_embryo_id=null | `gestational_age_at_delivery` | `nan` | `31.0` |
| oocito_id=187947, emb_cong_id=null, embryo_embryo_id=null | `gestational_age_at_delivery` | `nan` | `31.0` |
| oocito_id=187948, emb_cong_id=null, embryo_embryo_id=null | `gestational_age_at_delivery` | `nan` | `31.0` |
| oocito_id=187949, emb_cong_id=null, embryo_embryo_id=null | `gestational_age_at_delivery` | `nan` | `31.0` |
| oocito_id=187950, emb_cong_id=null, embryo_embryo_id=null | `gestational_age_at_delivery` | `nan` | `31.0` |
| oocito_id=187951, emb_cong_id=null, embryo_embryo_id=null | `gestational_age_at_delivery` | `nan` | `31.0` |
| oocito_id=203627, emb_cong_id=null, embryo_embryo_id=d2024.06.24_s02472_i3254_p-2 | `gestational_age_at_delivery` | `nan` | `40.0` |
| oocito_id=207719, emb_cong_id=null, embryo_embryo_id=null | `gestational_age_at_delivery` | `nan` | `40.0` |
| oocito_id=207720, emb_cong_id=null, embryo_embryo_id=null | `gestational_age_at_delivery` | `nan` | `40.0` |
| oocito_id=207722, emb_cong_id=null, embryo_embryo_id=null | `gestational_age_at_delivery` | `nan` | `40.0` |
| oocito_id=186620, emb_cong_id=null, embryo_embryo_id=null | `has_biopsy` | `False` | `True` |
| oocito_id=186598, emb_cong_id=null, embryo_embryo_id=null | `has_biopsy` | `False` | `True` |
| oocito_id=186600, emb_cong_id=null, embryo_embryo_id=null | `has_biopsy` | `False` | `True` |
| oocito_id=186601, emb_cong_id=null, embryo_embryo_id=null | `has_biopsy` | `False` | `True` |
| oocito_id=186602, emb_cong_id=null, embryo_embryo_id=null | `has_biopsy` | `False` | `True` |
| oocito_id=186603, emb_cong_id=null, embryo_embryo_id=null | `has_biopsy` | `False` | `True` |
| oocito_id=186604, emb_cong_id=null, embryo_embryo_id=null | `has_biopsy` | `False` | `True` |
| oocito_id=186605, emb_cong_id=null, embryo_embryo_id=null | `has_biopsy` | `False` | `True` |
| oocito_id=186606, emb_cong_id=null, embryo_embryo_id=null | `has_biopsy` | `False` | `True` |
| oocito_id=186607, emb_cong_id=null, embryo_embryo_id=null | `has_biopsy` | `False` | `True` |
| oocito_id=186890, emb_cong_id=null, embryo_embryo_id=null | `has_transfer_logging_gap` | `False` | `True` |
| oocito_id=186891, emb_cong_id=null, embryo_embryo_id=null | `has_transfer_logging_gap` | `False` | `True` |
| oocito_id=187396, emb_cong_id=null, embryo_embryo_id=null | `has_transfer_logging_gap` | `False` | `True` |
| oocito_id=187397, emb_cong_id=null, embryo_embryo_id=null | `has_transfer_logging_gap` | `False` | `True` |
| oocito_id=187398, emb_cong_id=null, embryo_embryo_id=null | `has_transfer_logging_gap` | `False` | `True` |
| oocito_id=187399, emb_cong_id=null, embryo_embryo_id=null | `has_transfer_logging_gap` | `False` | `True` |
| oocito_id=187400, emb_cong_id=null, embryo_embryo_id=null | `has_transfer_logging_gap` | `False` | `True` |
| oocito_id=187401, emb_cong_id=null, embryo_embryo_id=null | `has_transfer_logging_gap` | `False` | `True` |
| oocito_id=187402, emb_cong_id=null, embryo_embryo_id=null | `has_transfer_logging_gap` | `False` | `True` |
| oocito_id=187403, emb_cong_id=null, embryo_embryo_id=null | `has_transfer_logging_gap` | `False` | `True` |
| oocito_id=187968, emb_cong_id=null, embryo_embryo_id=d2024.03.11_s02317_i3254_p-5 | `has_valid_outcome` | `False` | `True` |
| oocito_id=189503, emb_cong_id=null, embryo_embryo_id=d2024.03.22_s02326_i3254_p-6 | `has_valid_outcome` | `False` | `True` |
| oocito_id=189504, emb_cong_id=null, embryo_embryo_id=d2024.03.22_s02326_i3254_p-7 | `has_valid_outcome` | `False` | `True` |
| oocito_id=189505, emb_cong_id=null, embryo_embryo_id=d2024.03.22_s02326_i3254_p-8 | `has_valid_outcome` | `False` | `True` |
| oocito_id=187964, emb_cong_id=null, embryo_embryo_id=d2024.03.11_s02317_i3254_p-1 | `has_valid_outcome` | `False` | `True` |
| oocito_id=187965, emb_cong_id=null, embryo_embryo_id=d2024.03.11_s02317_i3254_p-2 | `has_valid_outcome` | `False` | `True` |
| oocito_id=187966, emb_cong_id=null, embryo_embryo_id=d2024.03.11_s02317_i3254_p-3 | `has_valid_outcome` | `False` | `True` |
| oocito_id=189500, emb_cong_id=null, embryo_embryo_id=d2024.03.22_s02326_i3254_p-3 | `has_valid_outcome` | `False` | `True` |
| oocito_id=189501, emb_cong_id=null, embryo_embryo_id=d2024.03.22_s02326_i3254_p-4 | `has_valid_outcome` | `False` | `True` |
| oocito_id=187969, emb_cong_id=null, embryo_embryo_id=null | `has_valid_outcome` | `False` | `True` |
| oocito_id=186620, emb_cong_id=null, embryo_embryo_id=null | `incubadora_padronizada` | `Embryoscope` | `nan` |
| oocito_id=186598, emb_cong_id=null, embryo_embryo_id=null | `incubadora_padronizada` | `K-SYSTEM` | `nan` |
| oocito_id=186600, emb_cong_id=null, embryo_embryo_id=null | `incubadora_padronizada` | `K-SYSTEM` | `nan` |
| oocito_id=186601, emb_cong_id=null, embryo_embryo_id=null | `incubadora_padronizada` | `K-SYSTEM` | `nan` |
| oocito_id=186602, emb_cong_id=null, embryo_embryo_id=null | `incubadora_padronizada` | `K-SYSTEM` | `nan` |
| oocito_id=186603, emb_cong_id=null, embryo_embryo_id=null | `incubadora_padronizada` | `K-SYSTEM` | `nan` |
| oocito_id=186604, emb_cong_id=null, embryo_embryo_id=null | `incubadora_padronizada` | `K-SYSTEM` | `nan` |
| oocito_id=186605, emb_cong_id=null, embryo_embryo_id=null | `incubadora_padronizada` | `K-SYSTEM` | `nan` |
| oocito_id=186606, emb_cong_id=null, embryo_embryo_id=null | `incubadora_padronizada` | `K-SYSTEM` | `nan` |
| oocito_id=186607, emb_cong_id=null, embryo_embryo_id=null | `incubadora_padronizada` | `K-SYSTEM` | `nan` |
| oocito_id=233231, emb_cong_id=null, embryo_embryo_id=null | `is_transferred_combined` | `True` | `None` |
| oocito_id=233232, emb_cong_id=null, embryo_embryo_id=null | `is_transferred_combined` | `True` | `None` |
| oocito_id=233233, emb_cong_id=null, embryo_embryo_id=null | `is_transferred_combined` | `True` | `None` |
| oocito_id=233234, emb_cong_id=null, embryo_embryo_id=null | `is_transferred_combined` | `True` | `None` |
| oocito_id=233235, emb_cong_id=null, embryo_embryo_id=null | `is_transferred_combined` | `True` | `None` |
| oocito_id=233236, emb_cong_id=null, embryo_embryo_id=null | `is_transferred_combined` | `True` | `None` |
| oocito_id=247608, emb_cong_id=null, embryo_embryo_id=d2025.04.03_s02868_i3254_p-16 | `is_transferred_combined` | `<NA>` | `True` |
| oocito_id=264765, emb_cong_id=null, embryo_embryo_id=d2025.07.16_s03640_i3253_p-1 | `is_transferred_combined` | `<NA>` | `True` |
| oocito_id=287030, emb_cong_id=null, embryo_embryo_id=d2025.11.19_s02523_i4120_p-1 | `is_transferred_combined` | `<NA>` | `True` |
| oocito_id=318765, emb_cong_id=null, embryo_embryo_id=null | `is_transferred_combined` | `<NA>` | `True` |
| oocito_id=186620, emb_cong_id=null, embryo_embryo_id=null | `join_step` | `1` | `nan` |
| oocito_id=186598, emb_cong_id=null, embryo_embryo_id=null | `join_step` | `1` | `nan` |
| oocito_id=186600, emb_cong_id=null, embryo_embryo_id=null | `join_step` | `1` | `nan` |
| oocito_id=186601, emb_cong_id=null, embryo_embryo_id=null | `join_step` | `1` | `nan` |
| oocito_id=186602, emb_cong_id=null, embryo_embryo_id=null | `join_step` | `1` | `nan` |
| oocito_id=186603, emb_cong_id=null, embryo_embryo_id=null | `join_step` | `1` | `nan` |
| oocito_id=186604, emb_cong_id=null, embryo_embryo_id=null | `join_step` | `1` | `nan` |
| oocito_id=186605, emb_cong_id=null, embryo_embryo_id=null | `join_step` | `1` | `nan` |
| oocito_id=186606, emb_cong_id=null, embryo_embryo_id=null | `join_step` | `1` | `nan` |
| oocito_id=186607, emb_cong_id=null, embryo_embryo_id=null | `join_step` | `1` | `nan` |
| oocito_id=186620, emb_cong_id=null, embryo_embryo_id=null | `matched_planilha_prontuario` | `754929` | `nan` |
| oocito_id=186598, emb_cong_id=null, embryo_embryo_id=null | `matched_planilha_prontuario` | `526078` | `nan` |
| oocito_id=186600, emb_cong_id=null, embryo_embryo_id=null | `matched_planilha_prontuario` | `526078` | `nan` |
| oocito_id=186601, emb_cong_id=null, embryo_embryo_id=null | `matched_planilha_prontuario` | `526078` | `nan` |
| oocito_id=186602, emb_cong_id=null, embryo_embryo_id=null | `matched_planilha_prontuario` | `526078` | `nan` |
| oocito_id=186603, emb_cong_id=null, embryo_embryo_id=null | `matched_planilha_prontuario` | `526078` | `nan` |
| oocito_id=186604, emb_cong_id=null, embryo_embryo_id=null | `matched_planilha_prontuario` | `526078` | `nan` |
| oocito_id=186605, emb_cong_id=null, embryo_embryo_id=null | `matched_planilha_prontuario` | `526078` | `nan` |
| oocito_id=186606, emb_cong_id=null, embryo_embryo_id=null | `matched_planilha_prontuario` | `526078` | `nan` |
| oocito_id=186607, emb_cong_id=null, embryo_embryo_id=null | `matched_planilha_prontuario` | `526078` | `nan` |
| oocito_id=186890, emb_cong_id=null, embryo_embryo_id=null | `matched_planilha_transfer_date` | `NaT` | `2022-02-15` |
| oocito_id=186891, emb_cong_id=null, embryo_embryo_id=null | `matched_planilha_transfer_date` | `NaT` | `2022-02-15` |
| oocito_id=187968, emb_cong_id=null, embryo_embryo_id=d2024.03.11_s02317_i3254_p-5 | `matched_planilha_transfer_date` | `NaT` | `2024-10-27` |
| oocito_id=189503, emb_cong_id=null, embryo_embryo_id=d2024.03.22_s02326_i3254_p-6 | `matched_planilha_transfer_date` | `NaT` | `2025-12-17` |
| oocito_id=189504, emb_cong_id=null, embryo_embryo_id=d2024.03.22_s02326_i3254_p-7 | `matched_planilha_transfer_date` | `NaT` | `2025-12-17` |
| oocito_id=189505, emb_cong_id=null, embryo_embryo_id=d2024.03.22_s02326_i3254_p-8 | `matched_planilha_transfer_date` | `NaT` | `2025-12-17` |
| oocito_id=187964, emb_cong_id=null, embryo_embryo_id=d2024.03.11_s02317_i3254_p-1 | `matched_planilha_transfer_date` | `NaT` | `2024-10-27` |
| oocito_id=187965, emb_cong_id=null, embryo_embryo_id=d2024.03.11_s02317_i3254_p-2 | `matched_planilha_transfer_date` | `NaT` | `2024-10-27` |
| oocito_id=187966, emb_cong_id=null, embryo_embryo_id=d2024.03.11_s02317_i3254_p-3 | `matched_planilha_transfer_date` | `NaT` | `2024-10-27` |
| oocito_id=189500, emb_cong_id=null, embryo_embryo_id=d2024.03.22_s02326_i3254_p-3 | `matched_planilha_transfer_date` | `NaT` | `2025-12-17` |
| oocito_id=186620, emb_cong_id=null, embryo_embryo_id=null | `matched_src_row_id` | `24080` | `nan` |
| oocito_id=186598, emb_cong_id=null, embryo_embryo_id=null | `matched_src_row_id` | `24081` | `nan` |
| oocito_id=186600, emb_cong_id=null, embryo_embryo_id=null | `matched_src_row_id` | `24081` | `nan` |
| oocito_id=186601, emb_cong_id=null, embryo_embryo_id=null | `matched_src_row_id` | `24081` | `nan` |
| oocito_id=186602, emb_cong_id=null, embryo_embryo_id=null | `matched_src_row_id` | `24081` | `nan` |
| oocito_id=186603, emb_cong_id=null, embryo_embryo_id=null | `matched_src_row_id` | `24081` | `nan` |
| oocito_id=186604, emb_cong_id=null, embryo_embryo_id=null | `matched_src_row_id` | `24081` | `nan` |
| oocito_id=186605, emb_cong_id=null, embryo_embryo_id=null | `matched_src_row_id` | `24081` | `nan` |
| oocito_id=186606, emb_cong_id=null, embryo_embryo_id=null | `matched_src_row_id` | `24081` | `nan` |
| oocito_id=186607, emb_cong_id=null, embryo_embryo_id=null | `matched_src_row_id` | `24081` | `nan` |
| oocito_id=187946, emb_cong_id=null, embryo_embryo_id=null | `merged_numero_de_nascidos` | `<NA>` | `1.0` |
| oocito_id=187947, emb_cong_id=null, embryo_embryo_id=null | `merged_numero_de_nascidos` | `<NA>` | `1.0` |
| oocito_id=187948, emb_cong_id=null, embryo_embryo_id=null | `merged_numero_de_nascidos` | `<NA>` | `1.0` |
| oocito_id=187949, emb_cong_id=null, embryo_embryo_id=null | `merged_numero_de_nascidos` | `<NA>` | `1.0` |
| oocito_id=187950, emb_cong_id=null, embryo_embryo_id=null | `merged_numero_de_nascidos` | `<NA>` | `1.0` |
| oocito_id=187951, emb_cong_id=null, embryo_embryo_id=null | `merged_numero_de_nascidos` | `<NA>` | `1.0` |
| oocito_id=203627, emb_cong_id=null, embryo_embryo_id=d2024.06.24_s02472_i3254_p-2 | `merged_numero_de_nascidos` | `<NA>` | `1.0` |
| oocito_id=207719, emb_cong_id=null, embryo_embryo_id=null | `merged_numero_de_nascidos` | `<NA>` | `1.0` |
| oocito_id=207720, emb_cong_id=null, embryo_embryo_id=null | `merged_numero_de_nascidos` | `<NA>` | `1.0` |
| oocito_id=207722, emb_cong_id=null, embryo_embryo_id=null | `merged_numero_de_nascidos` | `<NA>` | `1.0` |
| oocito_id=186620, emb_cong_id=null, embryo_embryo_id=null | `micro_data_dl` | `2024-03-02 00:00:00` | `2024-03-02` |
| oocito_id=186598, emb_cong_id=null, embryo_embryo_id=null | `micro_data_dl` | `2024-03-02 00:00:00` | `2024-03-02` |
| oocito_id=186600, emb_cong_id=null, embryo_embryo_id=null | `micro_data_dl` | `2024-03-02 00:00:00` | `2024-03-02` |
| oocito_id=186601, emb_cong_id=null, embryo_embryo_id=null | `micro_data_dl` | `2024-03-02 00:00:00` | `2024-03-02` |
| oocito_id=186602, emb_cong_id=null, embryo_embryo_id=null | `micro_data_dl` | `2024-03-02 00:00:00` | `2024-03-02` |
| oocito_id=186603, emb_cong_id=null, embryo_embryo_id=null | `micro_data_dl` | `2024-03-02 00:00:00` | `2024-03-02` |
| oocito_id=186604, emb_cong_id=null, embryo_embryo_id=null | `micro_data_dl` | `2024-03-02 00:00:00` | `2024-03-02` |
| oocito_id=186605, emb_cong_id=null, embryo_embryo_id=null | `micro_data_dl` | `2024-03-02 00:00:00` | `2024-03-02` |
| oocito_id=186606, emb_cong_id=null, embryo_embryo_id=null | `micro_data_dl` | `2024-03-02 00:00:00` | `2024-03-02` |
| oocito_id=186607, emb_cong_id=null, embryo_embryo_id=null | `micro_data_dl` | `2024-03-02 00:00:00` | `2024-03-02` |
| oocito_id=186620, emb_cong_id=null, embryo_embryo_id=null | `micro_data_procedimento` | `2024-03-02 00:00:00` | `2024-03-02` |
| oocito_id=186598, emb_cong_id=null, embryo_embryo_id=null | `micro_data_procedimento` | `2024-03-02 00:00:00` | `2024-03-02` |
| oocito_id=186600, emb_cong_id=null, embryo_embryo_id=null | `micro_data_procedimento` | `2024-03-02 00:00:00` | `2024-03-02` |
| oocito_id=186601, emb_cong_id=null, embryo_embryo_id=null | `micro_data_procedimento` | `2024-03-02 00:00:00` | `2024-03-02` |
| oocito_id=186602, emb_cong_id=null, embryo_embryo_id=null | `micro_data_procedimento` | `2024-03-02 00:00:00` | `2024-03-02` |
| oocito_id=186603, emb_cong_id=null, embryo_embryo_id=null | `micro_data_procedimento` | `2024-03-02 00:00:00` | `2024-03-02` |
| oocito_id=186604, emb_cong_id=null, embryo_embryo_id=null | `micro_data_procedimento` | `2024-03-02 00:00:00` | `2024-03-02` |
| oocito_id=186605, emb_cong_id=null, embryo_embryo_id=null | `micro_data_procedimento` | `2024-03-02 00:00:00` | `2024-03-02` |
| oocito_id=186606, emb_cong_id=null, embryo_embryo_id=null | `micro_data_procedimento` | `2024-03-02 00:00:00` | `2024-03-02` |
| oocito_id=186607, emb_cong_id=null, embryo_embryo_id=null | `micro_data_procedimento` | `2024-03-02 00:00:00` | `2024-03-02` |
| oocito_id=200186, emb_cong_id=null, embryo_embryo_id=null | `micro_prontuario` | `842186` | `818980` |
| oocito_id=200187, emb_cong_id=null, embryo_embryo_id=null | `micro_prontuario` | `842186` | `818980` |
| oocito_id=200188, emb_cong_id=null, embryo_embryo_id=null | `micro_prontuario` | `842186` | `818980` |
| oocito_id=200189, emb_cong_id=null, embryo_embryo_id=null | `micro_prontuario` | `842186` | `818980` |
| oocito_id=233231, emb_cong_id=null, embryo_embryo_id=null | `micro_prontuario` | `842186` | `818980` |
| oocito_id=233232, emb_cong_id=null, embryo_embryo_id=null | `micro_prontuario` | `842186` | `818980` |
| oocito_id=233233, emb_cong_id=null, embryo_embryo_id=null | `micro_prontuario` | `842186` | `818980` |
| oocito_id=233234, emb_cong_id=null, embryo_embryo_id=null | `micro_prontuario` | `842186` | `818980` |
| oocito_id=233235, emb_cong_id=null, embryo_embryo_id=null | `micro_prontuario` | `842186` | `818980` |
| oocito_id=233236, emb_cong_id=null, embryo_embryo_id=null | `micro_prontuario` | `842186` | `818980` |
| oocito_id=187968, emb_cong_id=null, embryo_embryo_id=d2024.03.11_s02317_i3254_p-5 | `n_of_biopsied` | `nan` | `3.0` |
| oocito_id=187964, emb_cong_id=null, embryo_embryo_id=d2024.03.11_s02317_i3254_p-1 | `n_of_biopsied` | `nan` | `3.0` |
| oocito_id=187965, emb_cong_id=null, embryo_embryo_id=d2024.03.11_s02317_i3254_p-2 | `n_of_biopsied` | `nan` | `3.0` |
| oocito_id=187966, emb_cong_id=null, embryo_embryo_id=d2024.03.11_s02317_i3254_p-3 | `n_of_biopsied` | `nan` | `3.0` |
| oocito_id=187969, emb_cong_id=null, embryo_embryo_id=null | `n_of_biopsied` | `nan` | `3.0` |
| oocito_id=187946, emb_cong_id=null, embryo_embryo_id=null | `n_of_biopsied` | `0` | `nan` |
| oocito_id=187947, emb_cong_id=null, embryo_embryo_id=null | `n_of_biopsied` | `0` | `nan` |
| oocito_id=187948, emb_cong_id=null, embryo_embryo_id=null | `n_of_biopsied` | `0` | `nan` |
| oocito_id=187949, emb_cong_id=null, embryo_embryo_id=null | `n_of_biopsied` | `0` | `nan` |
| oocito_id=187950, emb_cong_id=null, embryo_embryo_id=null | `n_of_biopsied` | `0` | `nan` |
| oocito_id=187968, emb_cong_id=null, embryo_embryo_id=d2024.03.11_s02317_i3254_p-5 | `n_of_normal` | `nan` | `1.0` |
| oocito_id=187964, emb_cong_id=null, embryo_embryo_id=d2024.03.11_s02317_i3254_p-1 | `n_of_normal` | `nan` | `1.0` |
| oocito_id=187965, emb_cong_id=null, embryo_embryo_id=d2024.03.11_s02317_i3254_p-2 | `n_of_normal` | `nan` | `1.0` |
| oocito_id=187966, emb_cong_id=null, embryo_embryo_id=d2024.03.11_s02317_i3254_p-3 | `n_of_normal` | `nan` | `1.0` |
| oocito_id=187969, emb_cong_id=null, embryo_embryo_id=null | `n_of_normal` | `nan` | `1.0` |
| oocito_id=187946, emb_cong_id=null, embryo_embryo_id=null | `n_of_normal` | `0.0` | `nan` |
| oocito_id=187947, emb_cong_id=null, embryo_embryo_id=null | `n_of_normal` | `0.0` | `nan` |
| oocito_id=187948, emb_cong_id=null, embryo_embryo_id=null | `n_of_normal` | `0.0` | `nan` |
| oocito_id=187949, emb_cong_id=null, embryo_embryo_id=null | `n_of_normal` | `0.0` | `nan` |
| oocito_id=187950, emb_cong_id=null, embryo_embryo_id=null | `n_of_normal` | `0.0` | `nan` |
| oocito_id=187968, emb_cong_id=null, embryo_embryo_id=d2024.03.11_s02317_i3254_p-5 | `number_of_embryos_transferred` | `nan` | `1.0` |
| oocito_id=187964, emb_cong_id=null, embryo_embryo_id=d2024.03.11_s02317_i3254_p-1 | `number_of_embryos_transferred` | `nan` | `1.0` |
| oocito_id=187965, emb_cong_id=null, embryo_embryo_id=d2024.03.11_s02317_i3254_p-2 | `number_of_embryos_transferred` | `nan` | `1.0` |
| oocito_id=187966, emb_cong_id=null, embryo_embryo_id=d2024.03.11_s02317_i3254_p-3 | `number_of_embryos_transferred` | `nan` | `1.0` |
| oocito_id=187969, emb_cong_id=null, embryo_embryo_id=null | `number_of_embryos_transferred` | `nan` | `1.0` |
| oocito_id=187946, emb_cong_id=null, embryo_embryo_id=null | `number_of_embryos_transferred` | `1.0` | `2.0` |
| oocito_id=187947, emb_cong_id=null, embryo_embryo_id=null | `number_of_embryos_transferred` | `1.0` | `2.0` |
| oocito_id=187948, emb_cong_id=null, embryo_embryo_id=null | `number_of_embryos_transferred` | `1.0` | `2.0` |
| oocito_id=187949, emb_cong_id=null, embryo_embryo_id=null | `number_of_embryos_transferred` | `1.0` | `2.0` |
| oocito_id=187950, emb_cong_id=null, embryo_embryo_id=null | `number_of_embryos_transferred` | `1.0` | `2.0` |
| oocito_id=315567, emb_cong_id=null, embryo_embryo_id=null | `oocito_embryo_number` | `10` | `1` |
| oocito_id=315568, emb_cong_id=null, embryo_embryo_id=null | `oocito_embryo_number` | `11` | `2` |
| oocito_id=315569, emb_cong_id=null, embryo_embryo_id=null | `oocito_embryo_number` | `12` | `3` |
| oocito_id=315570, emb_cong_id=null, embryo_embryo_id=null | `oocito_embryo_number` | `13` | `4` |
| oocito_id=315571, emb_cong_id=null, embryo_embryo_id=null | `oocito_embryo_number` | `14` | `5` |
| oocito_id=315572, emb_cong_id=null, embryo_embryo_id=null | `oocito_embryo_number` | `15` | `6` |
| oocito_id=315574, emb_cong_id=null, embryo_embryo_id=null | `oocito_embryo_number` | `17` | `8` |
| oocito_id=315575, emb_cong_id=null, embryo_embryo_id=null | `oocito_embryo_number` | `18` | `9` |
| oocito_id=87818, emb_cong_id=null, embryo_embryo_id=null | `oocito_origem_oocito` | `Descongelado` | `Descongelado OR` |
| oocito_id=87819, emb_cong_id=null, embryo_embryo_id=null | `oocito_origem_oocito` | `Descongelado` | `Descongelado OR` |
| oocito_id=87820, emb_cong_id=null, embryo_embryo_id=null | `oocito_origem_oocito` | `Descongelado` | `Descongelado OR` |
| oocito_id=87821, emb_cong_id=null, embryo_embryo_id=null | `oocito_origem_oocito` | `Descongelado` | `Descongelado OR` |
| oocito_id=87822, emb_cong_id=null, embryo_embryo_id=null | `oocito_origem_oocito` | `Descongelado` | `Descongelado OR` |
| oocito_id=87823, emb_cong_id=null, embryo_embryo_id=null | `oocito_origem_oocito` | `Descongelado` | `Descongelado OR` |
| oocito_id=87824, emb_cong_id=null, embryo_embryo_id=null | `oocito_origem_oocito` | `Descongelado` | `Descongelado OR` |
| oocito_id=87825, emb_cong_id=null, embryo_embryo_id=null | `oocito_origem_oocito` | `Descongelado` | `Descongelado OR` |
| oocito_id=100153, emb_cong_id=null, embryo_embryo_id=d2022.08.26_s01502_i4120_p-5 | `oocito_origem_oocito` | `Fresco` | `Fresco OR` |
| oocito_id=100155, emb_cong_id=null, embryo_embryo_id=d2022.08.26_s01502_i4120_p-7 | `oocito_origem_oocito` | `Fresco` | `Fresco OR` |
| oocito_id=318772, emb_cong_id=null, embryo_embryo_id=null | `oocito_tcd` | `nan` | `Descartado` |
| oocito_id=318919, emb_cong_id=null, embryo_embryo_id=null | `oocito_tcd` | `nan` | `Descartado` |
| oocito_id=318968, emb_cong_id=null, embryo_embryo_id=null | `oocito_tcd` | `nan` | `Descartado` |
| oocito_id=318973, emb_cong_id=null, embryo_embryo_id=null | `oocito_tcd` | `nan` | `Descartado` |
| oocito_id=318987, emb_cong_id=null, embryo_embryo_id=null | `oocito_tcd` | `nan` | `Descartado` |
| oocito_id=318988, emb_cong_id=null, embryo_embryo_id=null | `oocito_tcd` | `nan` | `Descartado` |
| oocito_id=318990, emb_cong_id=null, embryo_embryo_id=null | `oocito_tcd` | `nan` | `Descartado` |
| oocito_id=318992, emb_cong_id=null, embryo_embryo_id=null | `oocito_tcd` | `nan` | `Descartado` |
| oocito_id=318996, emb_cong_id=null, embryo_embryo_id=null | `oocito_tcd` | `nan` | `Descartado` |
| oocito_id=318997, emb_cong_id=null, embryo_embryo_id=null | `oocito_tcd` | `nan` | `Descartado` |
| oocito_id=187968, emb_cong_id=null, embryo_embryo_id=d2024.03.11_s02317_i3254_p-5 | `outcome_type` | `nan` | `No pregnancy` |
| oocito_id=187964, emb_cong_id=null, embryo_embryo_id=d2024.03.11_s02317_i3254_p-1 | `outcome_type` | `nan` | `No pregnancy` |
| oocito_id=187965, emb_cong_id=null, embryo_embryo_id=d2024.03.11_s02317_i3254_p-2 | `outcome_type` | `nan` | `No pregnancy` |
| oocito_id=187966, emb_cong_id=null, embryo_embryo_id=d2024.03.11_s02317_i3254_p-3 | `outcome_type` | `nan` | `No pregnancy` |
| oocito_id=187969, emb_cong_id=null, embryo_embryo_id=null | `outcome_type` | `nan` | `No pregnancy` |
| oocito_id=187946, emb_cong_id=null, embryo_embryo_id=null | `outcome_type` | `Clinical pregnancy lost to follow-up` | ` Delivery (> 22 WA)` |
| oocito_id=187947, emb_cong_id=null, embryo_embryo_id=null | `outcome_type` | `Clinical pregnancy lost to follow-up` | ` Delivery (> 22 WA)` |
| oocito_id=187948, emb_cong_id=null, embryo_embryo_id=null | `outcome_type` | `Clinical pregnancy lost to follow-up` | ` Delivery (> 22 WA)` |
| oocito_id=187949, emb_cong_id=null, embryo_embryo_id=null | `outcome_type` | `Clinical pregnancy lost to follow-up` | ` Delivery (> 22 WA)` |
| oocito_id=187950, emb_cong_id=null, embryo_embryo_id=null | `outcome_type` | `Clinical pregnancy lost to follow-up` | ` Delivery (> 22 WA)` |
| oocito_id=187968, emb_cong_id=null, embryo_embryo_id=d2024.03.11_s02317_i3254_p-5 | `redlara_outcome` | `nan` | `EMBRYO TRANSFER` |
| oocito_id=187964, emb_cong_id=null, embryo_embryo_id=d2024.03.11_s02317_i3254_p-1 | `redlara_outcome` | `nan` | `EMBRYO TRANSFER` |
| oocito_id=187965, emb_cong_id=null, embryo_embryo_id=d2024.03.11_s02317_i3254_p-2 | `redlara_outcome` | `nan` | `EMBRYO TRANSFER` |
| oocito_id=187966, emb_cong_id=null, embryo_embryo_id=d2024.03.11_s02317_i3254_p-3 | `redlara_outcome` | `nan` | `EMBRYO TRANSFER` |
| oocito_id=187969, emb_cong_id=null, embryo_embryo_id=null | `redlara_outcome` | `nan` | `EMBRYO TRANSFER` |
| oocito_id=188644, emb_cong_id=null, embryo_embryo_id=null | `redlara_outcome` | `nan` | ` No embryo transfer` |
| oocito_id=188645, emb_cong_id=null, embryo_embryo_id=null | `redlara_outcome` | `nan` | ` No embryo transfer` |
| oocito_id=188646, emb_cong_id=null, embryo_embryo_id=null | `redlara_outcome` | `nan` | ` No embryo transfer` |
| oocito_id=188647, emb_cong_id=null, embryo_embryo_id=null | `redlara_outcome` | `nan` | ` No embryo transfer` |
| oocito_id=188648, emb_cong_id=null, embryo_embryo_id=null | `redlara_outcome` | `nan` | ` No embryo transfer` |
| oocito_id=192066, emb_cong_id=null, embryo_embryo_id=d2024.04.08_s02347_i3254_p-2 | `trat1_bmi` | `1933.59` | `19.14` |
| oocito_id=192068, emb_cong_id=null, embryo_embryo_id=d2024.04.08_s02347_i3254_p-4 | `trat1_bmi` | `1933.59` | `19.14` |
| oocito_id=192069, emb_cong_id=null, embryo_embryo_id=null | `trat1_bmi` | `1933.59` | `19.14` |
| oocito_id=192474, emb_cong_id=null, embryo_embryo_id=null | `trat1_bmi` | `2281.23` | `22.58` |
| oocito_id=192475, emb_cong_id=null, embryo_embryo_id=null | `trat1_bmi` | `2281.23` | `22.58` |
| oocito_id=192477, emb_cong_id=null, embryo_embryo_id=null | `trat1_bmi` | `2281.23` | `22.58` |
| oocito_id=192479, emb_cong_id=null, embryo_embryo_id=null | `trat1_bmi` | `2281.23` | `22.58` |
| oocito_id=192481, emb_cong_id=null, embryo_embryo_id=null | `trat1_bmi` | `2281.23` | `22.58` |
| oocito_id=192483, emb_cong_id=null, embryo_embryo_id=null | `trat1_bmi` | `2281.23` | `22.58` |
| oocito_id=192484, emb_cong_id=null, embryo_embryo_id=null | `trat1_bmi` | `2281.23` | `22.58` |
| oocito_id=186890, emb_cong_id=null, embryo_embryo_id=null | `trat1_data_inicio_inducao` | `2024-02-21 00:00:00` | `2024-02-21` |
| oocito_id=186891, emb_cong_id=null, embryo_embryo_id=null | `trat1_data_inicio_inducao` | `2024-02-21 00:00:00` | `2024-02-21` |
| oocito_id=187361, emb_cong_id=null, embryo_embryo_id=null | `trat1_data_inicio_inducao` | `2024-02-23 00:00:00` | `2024-02-23` |
| oocito_id=187362, emb_cong_id=null, embryo_embryo_id=null | `trat1_data_inicio_inducao` | `2024-02-23 00:00:00` | `2024-02-23` |
| oocito_id=187363, emb_cong_id=null, embryo_embryo_id=null | `trat1_data_inicio_inducao` | `2024-02-23 00:00:00` | `2024-02-23` |
| oocito_id=187367, emb_cong_id=null, embryo_embryo_id=null | `trat1_data_inicio_inducao` | `2024-02-23 00:00:00` | `2024-02-23` |
| oocito_id=187369, emb_cong_id=null, embryo_embryo_id=null | `trat1_data_inicio_inducao` | `2024-02-23 00:00:00` | `2024-02-23` |
| oocito_id=187370, emb_cong_id=null, embryo_embryo_id=null | `trat1_data_inicio_inducao` | `2024-02-23 00:00:00` | `2024-02-23` |
| oocito_id=187371, emb_cong_id=null, embryo_embryo_id=null | `trat1_data_inicio_inducao` | `2024-02-23 00:00:00` | `2024-02-23` |
| oocito_id=187372, emb_cong_id=null, embryo_embryo_id=null | `trat1_data_inicio_inducao` | `2024-02-23 00:00:00` | `2024-02-23` |
| oocito_id=187268, emb_cong_id=null, embryo_embryo_id=null | `trat1_data_transferencia` | `2024-03-11 00:00:00` | `2024-03-11` |
| oocito_id=187332, emb_cong_id=null, embryo_embryo_id=null | `trat1_data_transferencia` | `2024-03-11 00:00:00` | `2024-03-11` |
| oocito_id=187333, emb_cong_id=null, embryo_embryo_id=null | `trat1_data_transferencia` | `2024-03-11 00:00:00` | `2024-03-11` |
| oocito_id=187334, emb_cong_id=null, embryo_embryo_id=null | `trat1_data_transferencia` | `2024-03-11 00:00:00` | `2024-03-11` |
| oocito_id=187335, emb_cong_id=null, embryo_embryo_id=null | `trat1_data_transferencia` | `2024-03-11 00:00:00` | `2024-03-11` |
| oocito_id=187336, emb_cong_id=null, embryo_embryo_id=null | `trat1_data_transferencia` | `2024-03-11 00:00:00` | `2024-03-11` |
| oocito_id=189239, emb_cong_id=null, embryo_embryo_id=d2024.03.20_s02325_i3254_p-1 | `trat1_data_transferencia` | `2024-03-25 00:00:00` | `2024-03-25` |
| oocito_id=189240, emb_cong_id=null, embryo_embryo_id=d2024.03.20_s02325_i3254_p-2 | `trat1_data_transferencia` | `2024-03-25 00:00:00` | `2024-03-25` |
| oocito_id=189241, emb_cong_id=null, embryo_embryo_id=d2024.03.20_s02325_i3254_p-3 | `trat1_data_transferencia` | `2024-03-25 00:00:00` | `2024-03-25` |
| oocito_id=189242, emb_cong_id=null, embryo_embryo_id=d2024.03.20_s02325_i3254_p-4 | `trat1_data_transferencia` | `2024-03-25 00:00:00` | `2024-03-25` |
| oocito_id=200186, emb_cong_id=null, embryo_embryo_id=null | `trat1_fator_infertilidade1` | `Insuficiência ovariana` | `nan` |
| oocito_id=200187, emb_cong_id=null, embryo_embryo_id=null | `trat1_fator_infertilidade1` | `Insuficiência ovariana` | `nan` |
| oocito_id=200188, emb_cong_id=null, embryo_embryo_id=null | `trat1_fator_infertilidade1` | `Insuficiência ovariana` | `nan` |
| oocito_id=200189, emb_cong_id=null, embryo_embryo_id=null | `trat1_fator_infertilidade1` | `Insuficiência ovariana` | `nan` |
| oocito_id=233231, emb_cong_id=null, embryo_embryo_id=null | `trat1_fator_infertilidade1` | `Insuficiência ovariana` | `nan` |
| oocito_id=233232, emb_cong_id=null, embryo_embryo_id=null | `trat1_fator_infertilidade1` | `Insuficiência ovariana` | `nan` |
| oocito_id=233233, emb_cong_id=null, embryo_embryo_id=null | `trat1_fator_infertilidade1` | `Insuficiência ovariana` | `nan` |
| oocito_id=233234, emb_cong_id=null, embryo_embryo_id=null | `trat1_fator_infertilidade1` | `Insuficiência ovariana` | `nan` |
| oocito_id=233235, emb_cong_id=null, embryo_embryo_id=null | `trat1_fator_infertilidade1` | `Insuficiência ovariana` | `nan` |
| oocito_id=233236, emb_cong_id=null, embryo_embryo_id=null | `trat1_fator_infertilidade1` | `Insuficiência ovariana` | `nan` |
| oocito_id=200186, emb_cong_id=null, embryo_embryo_id=null | `trat1_id` | `27124` | `nan` |
| oocito_id=200187, emb_cong_id=null, embryo_embryo_id=null | `trat1_id` | `27124` | `nan` |
| oocito_id=200188, emb_cong_id=null, embryo_embryo_id=null | `trat1_id` | `27124` | `nan` |
| oocito_id=200189, emb_cong_id=null, embryo_embryo_id=null | `trat1_id` | `27124` | `nan` |
| oocito_id=233231, emb_cong_id=null, embryo_embryo_id=null | `trat1_id` | `31172` | `nan` |
| oocito_id=233232, emb_cong_id=null, embryo_embryo_id=null | `trat1_id` | `31172` | `nan` |
| oocito_id=233233, emb_cong_id=null, embryo_embryo_id=null | `trat1_id` | `31172` | `nan` |
| oocito_id=233234, emb_cong_id=null, embryo_embryo_id=null | `trat1_id` | `31172` | `nan` |
| oocito_id=233235, emb_cong_id=null, embryo_embryo_id=null | `trat1_id` | `31172` | `nan` |
| oocito_id=233236, emb_cong_id=null, embryo_embryo_id=null | `trat1_id` | `31172` | `nan` |
| oocito_id=200186, emb_cong_id=null, embryo_embryo_id=null | `trat1_motivo_nao_transferir` | `Criopreservação total` | `nan` |
| oocito_id=200187, emb_cong_id=null, embryo_embryo_id=null | `trat1_motivo_nao_transferir` | `Criopreservação total` | `nan` |
| oocito_id=200188, emb_cong_id=null, embryo_embryo_id=null | `trat1_motivo_nao_transferir` | `Criopreservação total` | `nan` |
| oocito_id=200189, emb_cong_id=null, embryo_embryo_id=null | `trat1_motivo_nao_transferir` | `Criopreservação total` | `nan` |
| oocito_id=250910, emb_cong_id=null, embryo_embryo_id=d2025.04.21_s02368_i4120_p-3 | `trat1_motivo_nao_transferir` | `nan` | `Criopreservação total` |
| oocito_id=289069, emb_cong_id=null, embryo_embryo_id=null | `trat1_motivo_nao_transferir` | `Criopreservação total` | `Ausência de embriões geneticamente normais` |
| oocito_id=306973, emb_cong_id=null, embryo_embryo_id=null | `trat1_motivo_nao_transferir` | `nan` | `Criopreservação total` |
| oocito_id=306974, emb_cong_id=null, embryo_embryo_id=null | `trat1_motivo_nao_transferir` | `nan` | `Criopreservação total` |
| oocito_id=306975, emb_cong_id=null, embryo_embryo_id=null | `trat1_motivo_nao_transferir` | `nan` | `Criopreservação total` |
| oocito_id=308967, emb_cong_id=null, embryo_embryo_id=null | `trat1_motivo_nao_transferir` | `nan` | `Ausência de embriões geneticamente normais` |
| oocito_id=233231, emb_cong_id=null, embryo_embryo_id=null | `trat1_origem_ovulo` | `Homólogo` | `nan` |
| oocito_id=233232, emb_cong_id=null, embryo_embryo_id=null | `trat1_origem_ovulo` | `Homólogo` | `nan` |
| oocito_id=233233, emb_cong_id=null, embryo_embryo_id=null | `trat1_origem_ovulo` | `Homólogo` | `nan` |
| oocito_id=233234, emb_cong_id=null, embryo_embryo_id=null | `trat1_origem_ovulo` | `Homólogo` | `nan` |
| oocito_id=233235, emb_cong_id=null, embryo_embryo_id=null | `trat1_origem_ovulo` | `Homólogo` | `nan` |
| oocito_id=233236, emb_cong_id=null, embryo_embryo_id=null | `trat1_origem_ovulo` | `Homólogo` | `nan` |
| oocito_id=250910, emb_cong_id=null, embryo_embryo_id=d2025.04.21_s02368_i4120_p-3 | `trat1_origem_ovulo` | `nan` | `Homólogo` |
| oocito_id=313960, emb_cong_id=null, embryo_embryo_id=null | `trat1_origem_ovulo` | `nan` | `Heterólogo` |
| oocito_id=314501, emb_cong_id=null, embryo_embryo_id=null | `trat1_origem_ovulo` | `nan` | `Homólogo` |
| oocito_id=314502, emb_cong_id=null, embryo_embryo_id=null | `trat1_origem_ovulo` | `nan` | `Homólogo` |
| oocito_id=200186, emb_cong_id=null, embryo_embryo_id=null | `trat1_previous_et` | `0` | `nan` |
| oocito_id=200187, emb_cong_id=null, embryo_embryo_id=null | `trat1_previous_et` | `0` | `nan` |
| oocito_id=200188, emb_cong_id=null, embryo_embryo_id=null | `trat1_previous_et` | `0` | `nan` |
| oocito_id=200189, emb_cong_id=null, embryo_embryo_id=null | `trat1_previous_et` | `0` | `nan` |
| oocito_id=227756, emb_cong_id=null, embryo_embryo_id=null | `trat1_previous_et` | `0` | `1.0` |
| oocito_id=227757, emb_cong_id=null, embryo_embryo_id=null | `trat1_previous_et` | `0` | `1.0` |
| oocito_id=227758, emb_cong_id=null, embryo_embryo_id=null | `trat1_previous_et` | `0` | `1.0` |
| oocito_id=227759, emb_cong_id=null, embryo_embryo_id=null | `trat1_previous_et` | `0` | `1.0` |
| oocito_id=227760, emb_cong_id=null, embryo_embryo_id=null | `trat1_previous_et` | `0` | `1.0` |
| oocito_id=227762, emb_cong_id=null, embryo_embryo_id=null | `trat1_previous_et` | `0` | `1.0` |
| oocito_id=200186, emb_cong_id=null, embryo_embryo_id=null | `trat1_previous_et_od` | `0` | `nan` |
| oocito_id=200187, emb_cong_id=null, embryo_embryo_id=null | `trat1_previous_et_od` | `0` | `nan` |
| oocito_id=200188, emb_cong_id=null, embryo_embryo_id=null | `trat1_previous_et_od` | `0` | `nan` |
| oocito_id=200189, emb_cong_id=null, embryo_embryo_id=null | `trat1_previous_et_od` | `0` | `nan` |
| oocito_id=233231, emb_cong_id=null, embryo_embryo_id=null | `trat1_previous_et_od` | `0` | `nan` |
| oocito_id=233232, emb_cong_id=null, embryo_embryo_id=null | `trat1_previous_et_od` | `0` | `nan` |
| oocito_id=233233, emb_cong_id=null, embryo_embryo_id=null | `trat1_previous_et_od` | `0` | `nan` |
| oocito_id=233234, emb_cong_id=null, embryo_embryo_id=null | `trat1_previous_et_od` | `0` | `nan` |
| oocito_id=233235, emb_cong_id=null, embryo_embryo_id=null | `trat1_previous_et_od` | `0` | `nan` |
| oocito_id=233236, emb_cong_id=null, embryo_embryo_id=null | `trat1_previous_et_od` | `0` | `nan` |
| oocito_id=200186, emb_cong_id=null, embryo_embryo_id=null | `trat1_resultado_tratamento` | `No transfer` | `nan` |
| oocito_id=200187, emb_cong_id=null, embryo_embryo_id=null | `trat1_resultado_tratamento` | `No transfer` | `nan` |
| oocito_id=200188, emb_cong_id=null, embryo_embryo_id=null | `trat1_resultado_tratamento` | `No transfer` | `nan` |
| oocito_id=200189, emb_cong_id=null, embryo_embryo_id=null | `trat1_resultado_tratamento` | `No transfer` | `nan` |
| oocito_id=233231, emb_cong_id=null, embryo_embryo_id=null | `trat1_resultado_tratamento` | `Negativo` | `nan` |
| oocito_id=233232, emb_cong_id=null, embryo_embryo_id=null | `trat1_resultado_tratamento` | `Negativo` | `nan` |
| oocito_id=233233, emb_cong_id=null, embryo_embryo_id=null | `trat1_resultado_tratamento` | `Negativo` | `nan` |
| oocito_id=233234, emb_cong_id=null, embryo_embryo_id=null | `trat1_resultado_tratamento` | `Negativo` | `nan` |
| oocito_id=233235, emb_cong_id=null, embryo_embryo_id=null | `trat1_resultado_tratamento` | `Negativo` | `nan` |
| oocito_id=233236, emb_cong_id=null, embryo_embryo_id=null | `trat1_resultado_tratamento` | `Negativo` | `nan` |
| oocito_id=200186, emb_cong_id=null, embryo_embryo_id=null | `trat1_tentativa` | `1` | `nan` |
| oocito_id=200187, emb_cong_id=null, embryo_embryo_id=null | `trat1_tentativa` | `1` | `nan` |
| oocito_id=200188, emb_cong_id=null, embryo_embryo_id=null | `trat1_tentativa` | `1` | `nan` |
| oocito_id=200189, emb_cong_id=null, embryo_embryo_id=null | `trat1_tentativa` | `1` | `nan` |
| oocito_id=233231, emb_cong_id=null, embryo_embryo_id=null | `trat1_tentativa` | `4` | `nan` |
| oocito_id=233232, emb_cong_id=null, embryo_embryo_id=null | `trat1_tentativa` | `4` | `nan` |
| oocito_id=233233, emb_cong_id=null, embryo_embryo_id=null | `trat1_tentativa` | `4` | `nan` |
| oocito_id=233234, emb_cong_id=null, embryo_embryo_id=null | `trat1_tentativa` | `4` | `nan` |
| oocito_id=233235, emb_cong_id=null, embryo_embryo_id=null | `trat1_tentativa` | `4` | `nan` |
| oocito_id=233236, emb_cong_id=null, embryo_embryo_id=null | `trat1_tentativa` | `4` | `nan` |
| oocito_id=200186, emb_cong_id=null, embryo_embryo_id=null | `trat1_tipo_procedimento` | `Ciclo a Fresco Vitrificação` | `nan` |
| oocito_id=200187, emb_cong_id=null, embryo_embryo_id=null | `trat1_tipo_procedimento` | `Ciclo a Fresco Vitrificação` | `nan` |
| oocito_id=200188, emb_cong_id=null, embryo_embryo_id=null | `trat1_tipo_procedimento` | `Ciclo a Fresco Vitrificação` | `nan` |
| oocito_id=200189, emb_cong_id=null, embryo_embryo_id=null | `trat1_tipo_procedimento` | `Ciclo a Fresco Vitrificação` | `nan` |
| oocito_id=233231, emb_cong_id=null, embryo_embryo_id=null | `trat1_tipo_procedimento` | `Ciclo a Fresco FIV` | `nan` |
| oocito_id=233232, emb_cong_id=null, embryo_embryo_id=null | `trat1_tipo_procedimento` | `Ciclo a Fresco FIV` | `nan` |
| oocito_id=233233, emb_cong_id=null, embryo_embryo_id=null | `trat1_tipo_procedimento` | `Ciclo a Fresco FIV` | `nan` |
| oocito_id=233234, emb_cong_id=null, embryo_embryo_id=null | `trat1_tipo_procedimento` | `Ciclo a Fresco FIV` | `nan` |
| oocito_id=233235, emb_cong_id=null, embryo_embryo_id=null | `trat1_tipo_procedimento` | `Ciclo a Fresco FIV` | `nan` |
| oocito_id=233236, emb_cong_id=null, embryo_embryo_id=null | `trat1_tipo_procedimento` | `Ciclo a Fresco FIV` | `nan` |
| oocito_id=187946, emb_cong_id=null, embryo_embryo_id=null | `type_of_delivery` | `nan` | ` Cesarean section` |
| oocito_id=187947, emb_cong_id=null, embryo_embryo_id=null | `type_of_delivery` | `nan` | ` Cesarean section` |
| oocito_id=187948, emb_cong_id=null, embryo_embryo_id=null | `type_of_delivery` | `nan` | ` Cesarean section` |
| oocito_id=187949, emb_cong_id=null, embryo_embryo_id=null | `type_of_delivery` | `nan` | ` Cesarean section` |
| oocito_id=187950, emb_cong_id=null, embryo_embryo_id=null | `type_of_delivery` | `nan` | ` Cesarean section` |
| oocito_id=187951, emb_cong_id=null, embryo_embryo_id=null | `type_of_delivery` | `nan` | ` Cesarean section` |
| oocito_id=203627, emb_cong_id=null, embryo_embryo_id=d2024.06.24_s02472_i3254_p-2 | `type_of_delivery` | `nan` | `Cesarean section` |
| oocito_id=207719, emb_cong_id=null, embryo_embryo_id=null | `type_of_delivery` | `nan` | `VAGINAL` |
| oocito_id=207720, emb_cong_id=null, embryo_embryo_id=null | `type_of_delivery` | `nan` | `VAGINAL` |
| oocito_id=207722, emb_cong_id=null, embryo_embryo_id=null | `type_of_delivery` | `nan` | `VAGINAL` |

### `redlara_planilha_combined`
#### **Schema Mismatches**
* Columns in DuckDB Only: `date_of_birth`, `fet_file_name`, `fet_idade_do_cong_de_embriao`, `fet_no_da_transfer_1a_2a_3a`, `fet_preparo_para_transferencia`, `fet_sheet_name`, `fet_tipo_1`, `fet_tipo_da_doacao`, `fet_tipo_de_fet`, `fet_tipo_de_tratamento`, `fresh_data_da_puncao`, `fresh_file_name`, `fresh_idade_espermatozoide`, `fresh_no_biopsiados`, `fresh_qtd_blasto_tq_a_e_b`, `fresh_sheet_name`, `fresh_tipo_1`, `fresh_tipo_de_inseminacao`, `join_step`, `patient_name`, `unidade`, `year`
* Columns in Athena Only: `date_of_embryo_transfer`, `fet_data_da_fet`, `fet_idade_cong_embriao`, `fet_no_da_transfer`, `fet_no_nascidos`, `fet_preparo_transferencia`, `fet_tipo_doacao`, `fet_tipo_fet`, `fet_tipo_tratamento`, `fresh_data_puncao`, `fresh_n_biopsiados`, `fresh_qtd_blasto_tq`, `fresh_tipo_inseminacao`, `fresh_tipo_tratamento`, `number_of_newborns`, `planilha_internal_join_step`
#### **Numeric Metric Alignment**
| Column Name | DuckDB Sum | Athena Sum | Difference | Alignment Rate |
| :--- | :---: | :---: | :---: | :---: |
| `merged_numero_de_nascidos` | 2.00 | 2.00 | +0.00 | 100.0000% |

#### **Discrepancy Group Isolation**
* **By Year/Date**:
  * Year **N/A**: 3482 discrepant records
* **By Dimension Group**:
  * Group **N/A**: 371 discrepant records
  * Group **embryoscope**: 2176 discrepant records
  * Group **k-system**: 798 discrepant records
  * Group **thermo**: 137 discrepant records

#### **Anonymized Column Discrepancy Samples**
| Composite Key | Column Name | DuckDB Value | Athena Value |
| :--- | :--- | :--- | :--- |
| prontuario=-1, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=null | `baby_1_weight` | `nan` | `\` |
| prontuario=-1, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=null | `baby_2_weight` | `nan` | `\` |
| prontuario=-1, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=null | `baby_3_weight` | `nan` | `\` |
| prontuario=-1, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=null | `complications_of_pregnancy_specify` | `nan` | `\` |
| prontuario=513761, transfer_date=null, redlara_chart_or_pin=513761.0, incubadora_padronizada=null | `date_of_delivery` | `2025-07-26 00:00:00` | `2025-07-26` |
| prontuario=828956, transfer_date=null, redlara_chart_or_pin=828956, incubadora_padronizada=null | `date_of_delivery` | `2025-08-12 00:00:00` | `2025-08-12` |
| prontuario=509327, transfer_date=null, redlara_chart_or_pin=509327, incubadora_padronizada=embryoscope | `date_when_embryos_were_cryopreserved` | `2022-02-16 00:00:00` | `2022-02-16` |
| prontuario=525338, transfer_date=null, redlara_chart_or_pin=525338, incubadora_padronizada=embryoscope | `date_when_embryos_were_cryopreserved` | `2022-06-04 00:00:00` | `2022-06-04` |
| prontuario=509513, transfer_date=null, redlara_chart_or_pin=509513, incubadora_padronizada=embryoscope | `date_when_embryos_were_cryopreserved` | `2022-09-28 00:00:00` | `2022-09-28` |
| prontuario=207430, transfer_date=null, redlara_chart_or_pin=207430, incubadora_padronizada=null | `date_when_embryos_were_cryopreserved` | `2022-01-13 00:00:00` | `2022-01-13` |
| prontuario=216584, transfer_date=null, redlara_chart_or_pin=216584, incubadora_padronizada=null | `date_when_embryos_were_cryopreserved` | `2022-01-13 00:00:00` | `2022-01-13` |
| prontuario=515090, transfer_date=null, redlara_chart_or_pin=515090, incubadora_padronizada=null | `date_when_embryos_were_cryopreserved` | `2021-05-12 00:00:00` | `2021-05-12` |
| prontuario=162510, transfer_date=null, redlara_chart_or_pin=162510, incubadora_padronizada=null | `date_when_embryos_were_cryopreserved` | `2021-03-03 00:00:00` | `2021-03-03` |
| prontuario=183374, transfer_date=null, redlara_chart_or_pin=85339, incubadora_padronizada=null | `date_when_embryos_were_cryopreserved` | `2021-01-18 00:00:00` | `2021-01-18` |
| prontuario=174588, transfer_date=null, redlara_chart_or_pin=174588, incubadora_padronizada=null | `date_when_embryos_were_cryopreserved` | `2022-01-20 00:00:00` | `2022-01-20` |
| prontuario=219334, transfer_date=null, redlara_chart_or_pin=50033855, incubadora_padronizada=null | `date_when_embryos_were_cryopreserved` | `2022-01-26 00:00:00` | `2022-01-26` |
| prontuario=183122, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=embryoscope | `fet_data_crio` | `2021-08-11 00:00:00` | `None` |
| prontuario=512644, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=k-system | `fet_data_crio` | `2021-11-15 00:00:00` | `None` |
| prontuario=509327, transfer_date=null, redlara_chart_or_pin=509327, incubadora_padronizada=embryoscope | `fet_data_crio` | `2022-02-16 00:00:00` | `2022-02-16` |
| prontuario=525338, transfer_date=null, redlara_chart_or_pin=525338, incubadora_padronizada=embryoscope | `fet_data_crio` | `2022-06-04 00:00:00` | `2022-06-04` |
| prontuario=509513, transfer_date=null, redlara_chart_or_pin=509513, incubadora_padronizada=embryoscope | `fet_data_crio` | `2022-09-28 00:00:00` | `2022-09-28` |
| prontuario=207430, transfer_date=null, redlara_chart_or_pin=207430, incubadora_padronizada=null | `fet_data_crio` | `2022-01-13 00:00:00` | `None` |
| prontuario=216584, transfer_date=null, redlara_chart_or_pin=216584, incubadora_padronizada=null | `fet_data_crio` | `2022-01-13 00:00:00` | `2022-01-13` |
| prontuario=515090, transfer_date=null, redlara_chart_or_pin=515090, incubadora_padronizada=null | `fet_data_crio` | `2021-05-12 00:00:00` | `None` |
| prontuario=162510, transfer_date=null, redlara_chart_or_pin=162510, incubadora_padronizada=null | `fet_data_crio` | `2021-03-03 00:00:00` | `None` |
| prontuario=183374, transfer_date=null, redlara_chart_or_pin=85339, incubadora_padronizada=null | `fet_data_crio` | `2021-01-18 00:00:00` | `None` |
| prontuario=183122, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=embryoscope | `fet_dia_cryo` | `5` | `nan` |
| prontuario=512644, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=k-system | `fet_dia_cryo` | `5/6` | `nan` |
| prontuario=223394, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=null | `fet_dia_cryo` | `\` | `nan` |
| prontuario=510927, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=null | `fet_dia_cryo` | `5` | `nan` |
| prontuario=507942, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=null | `fet_dia_cryo` | `5` | `nan` |
| prontuario=508176, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=null | `fet_dia_cryo` | `5` | `nan` |
| prontuario=159913, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=null | `fet_dia_cryo` | `6` | `nan` |
| prontuario=509327, transfer_date=null, redlara_chart_or_pin=509327, incubadora_padronizada=embryoscope | `fet_dia_cryo` | `5` | `nan` |
| prontuario=525338, transfer_date=null, redlara_chart_or_pin=525338, incubadora_padronizada=embryoscope | `fet_dia_cryo` | `5` | `nan` |
| prontuario=509513, transfer_date=null, redlara_chart_or_pin=509513, incubadora_padronizada=embryoscope | `fet_dia_cryo` | `5` | `nan` |
| prontuario=512644, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=k-system | `fet_dia_et` | `5` | `None` |
| prontuario=510927, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=null | `fet_dia_et` | `5` | `None` |
| prontuario=507942, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=null | `fet_dia_et` | `5` | `None` |
| prontuario=508176, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=null | `fet_dia_et` | `5` | `None` |
| prontuario=159913, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=null | `fet_dia_et` | `6` | `None` |
| prontuario=509327, transfer_date=null, redlara_chart_or_pin=509327, incubadora_padronizada=embryoscope | `fet_dia_et` | `5` | `None` |
| prontuario=509513, transfer_date=null, redlara_chart_or_pin=509513, incubadora_padronizada=embryoscope | `fet_dia_et` | `5` | `None` |
| prontuario=162510, transfer_date=null, redlara_chart_or_pin=162510, incubadora_padronizada=null | `fet_dia_et` | `5` | `None` |
| prontuario=514432, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=k-system | `fet_dia_et` | `5` | `None` |
| prontuario=184316, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=embryoscope | `fet_dia_et` | `6` | `None` |
| prontuario=815019, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=thermo | `fet_gravidez_bioquimica` | `nan` | `0.0` |
| prontuario=779694, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=null | `fet_gravidez_bioquimica` | `nan` | `0.0` |
| prontuario=782692, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=null | `fet_gravidez_bioquimica` | `nan` | `1.0` |
| prontuario=521756, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=null | `fet_gravidez_bioquimica` | `nan` | `1.0` |
| prontuario=781815, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=null | `fet_gravidez_bioquimica` | `nan` | `0.0` |
| prontuario=828956, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=null | `fet_gravidez_bioquimica` | `nan` | `1` |
| prontuario=892453, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=k-system | `fet_gravidez_bioquimica` | `nan` | `1` |
| prontuario=513761, transfer_date=null, redlara_chart_or_pin=513761.0, incubadora_padronizada=null | `fet_gravidez_bioquimica` | `nan` | `1` |
| prontuario=513761, transfer_date=null, redlara_chart_or_pin=513761.0, incubadora_padronizada=null | `fet_gravidez_clinica` | `nan` | `1` |
| prontuario=183122, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=embryoscope | `fet_idade_mulher` | `35` | `nan` |
| prontuario=512644, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=k-system | `fet_idade_mulher` | `34` | `nan` |
| prontuario=223394, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=null | `fet_idade_mulher` | `37` | `nan` |
| prontuario=508176, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=null | `fet_idade_mulher` | `34` | `nan` |
| prontuario=509327, transfer_date=null, redlara_chart_or_pin=509327, incubadora_padronizada=embryoscope | `fet_idade_mulher` | `32` | `34.0` |
| prontuario=207430, transfer_date=null, redlara_chart_or_pin=207430, incubadora_padronizada=null | `fet_idade_mulher` | `38` | `nan` |
| prontuario=515090, transfer_date=null, redlara_chart_or_pin=515090, incubadora_padronizada=null | `fet_idade_mulher` | `36` | `nan` |
| prontuario=162510, transfer_date=null, redlara_chart_or_pin=162510, incubadora_padronizada=null | `fet_idade_mulher` | `44` | `nan` |
| prontuario=183374, transfer_date=null, redlara_chart_or_pin=85339, incubadora_padronizada=null | `fet_idade_mulher` | `33` | `nan` |
| prontuario=174588, transfer_date=null, redlara_chart_or_pin=174588, incubadora_padronizada=null | `fet_idade_mulher` | `37` | `nan` |
| prontuario=512644, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=k-system | `fet_no_et` | `1` | `nan` |
| prontuario=508176, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=null | `fet_no_et` | `1` | `nan` |
| prontuario=509327, transfer_date=null, redlara_chart_or_pin=509327, incubadora_padronizada=embryoscope | `fet_no_et` | `1` | `nan` |
| prontuario=525338, transfer_date=null, redlara_chart_or_pin=525338, incubadora_padronizada=embryoscope | `fet_no_et` | `0` | `nan` |
| prontuario=509513, transfer_date=null, redlara_chart_or_pin=509513, incubadora_padronizada=embryoscope | `fet_no_et` | `2` | `0.0` |
| prontuario=162510, transfer_date=null, redlara_chart_or_pin=162510, incubadora_padronizada=null | `fet_no_et` | `1` | `nan` |
| prontuario=519413, transfer_date=null, redlara_chart_or_pin=519413, incubadora_padronizada=null | `fet_no_et` | `0` | `1.0` |
| prontuario=214270, transfer_date=null, redlara_chart_or_pin=214270, incubadora_padronizada=null | `fet_no_et` | `0` | `nan` |
| prontuario=515211, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=embryoscope | `fet_no_et` | `<NA>` | `2.0` |
| prontuario=517203, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=embryoscope | `fet_no_et` | `0` | `nan` |
| prontuario=183122, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=embryoscope | `fet_resultado` | `EMBRYO VITRI` | `nan` |
| prontuario=512644, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=k-system | `fet_resultado` | `POSITIVO` | `nan` |
| prontuario=223394, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=null | `fet_resultado` | `NO ET` | `nan` |
| prontuario=508176, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=null | `fet_resultado` | `NEGATIVO` | `nan` |
| prontuario=509327, transfer_date=null, redlara_chart_or_pin=509327, incubadora_padronizada=embryoscope | `fet_resultado` | `NEGATIVO` | `NO ET` |
| prontuario=525338, transfer_date=null, redlara_chart_or_pin=525338, incubadora_padronizada=embryoscope | `fet_resultado` | `EMBRYO VITRI` | `NO ET` |
| prontuario=509513, transfer_date=null, redlara_chart_or_pin=509513, incubadora_padronizada=embryoscope | `fet_resultado` | `POSITIVO` | `EMBRYO VITRI` |
| prontuario=207430, transfer_date=null, redlara_chart_or_pin=207430, incubadora_padronizada=null | `fet_resultado` | `EMBRYO VITRI` | `nan` |
| prontuario=515090, transfer_date=null, redlara_chart_or_pin=515090, incubadora_padronizada=null | `fet_resultado` | `EMBRYO VITRI` | `nan` |
| prontuario=162510, transfer_date=null, redlara_chart_or_pin=162510, incubadora_padronizada=null | `fet_resultado` | `NEGATIVO` | `nan` |
| prontuario=183122, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=embryoscope | `fet_tipo_biopsia` | `PGT-A` | `nan` |
| prontuario=512644, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=k-system | `fet_tipo_biopsia` | `\` | `nan` |
| prontuario=223394, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=null | `fet_tipo_biopsia` | `\` | `nan` |
| prontuario=508176, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=null | `fet_tipo_biopsia` | `\` | `nan` |
| prontuario=207430, transfer_date=null, redlara_chart_or_pin=207430, incubadora_padronizada=null | `fet_tipo_biopsia` | `PGT-A` | `nan` |
| prontuario=515090, transfer_date=null, redlara_chart_or_pin=515090, incubadora_padronizada=null | `fet_tipo_biopsia` | `PGT-A` | `nan` |
| prontuario=162510, transfer_date=null, redlara_chart_or_pin=162510, incubadora_padronizada=null | `fet_tipo_biopsia` | `\` | `nan` |
| prontuario=183374, transfer_date=null, redlara_chart_or_pin=85339, incubadora_padronizada=null | `fet_tipo_biopsia` | `\` | `nan` |
| prontuario=174588, transfer_date=null, redlara_chart_or_pin=174588, incubadora_padronizada=null | `fet_tipo_biopsia` | `\` | `nan` |
| prontuario=219334, transfer_date=null, redlara_chart_or_pin=50033855, incubadora_padronizada=null | `fet_tipo_biopsia` | `\` | `nan` |
| prontuario=183122, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=embryoscope | `fet_tipo_resultado` | `FIV INCOMPLETA` | `nan` |
| prontuario=512644, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=k-system | `fet_tipo_resultado` | `FIV COMPLETA` | `nan` |
| prontuario=223394, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=null | `fet_tipo_resultado` | `FIV INCOMPLETA` | `nan` |
| prontuario=508176, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=null | `fet_tipo_resultado` | `TRANSFERÊNCIA DE CONGELADOS` | `nan` |
| prontuario=509327, transfer_date=null, redlara_chart_or_pin=509327, incubadora_padronizada=embryoscope | `fet_tipo_resultado` | `nan` | `FIV INCOMPLETA` |
| prontuario=525338, transfer_date=null, redlara_chart_or_pin=525338, incubadora_padronizada=embryoscope | `fet_tipo_resultado` | `nan` | `FIV INCOMPLETA` |
| prontuario=509513, transfer_date=null, redlara_chart_or_pin=509513, incubadora_padronizada=embryoscope | `fet_tipo_resultado` | `nan` | `FIV INCOMPLETA` |
| prontuario=216584, transfer_date=null, redlara_chart_or_pin=216584, incubadora_padronizada=null | `fet_tipo_resultado` | `nan` | `FIV INCOMPLETA` |
| prontuario=141864, transfer_date=null, redlara_chart_or_pin=141864, incubadora_padronizada=null | `fet_tipo_resultado` | `nan` | `FIV INCOMPLETA` |
| prontuario=145623, transfer_date=null, redlara_chart_or_pin=145623, incubadora_padronizada=null | `fet_tipo_resultado` | `nan` | `FIV INCOMPLETA` |
| prontuario=512644, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=k-system | `fresh_altura` | `nan` | `1.62` |
| prontuario=223394, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=null | `fresh_altura` | `nan` | `1.75` |
| prontuario=508176, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=null | `fresh_altura` | `nan` | `\` |
| prontuario=509327, transfer_date=null, redlara_chart_or_pin=509327, incubadora_padronizada=embryoscope | `fresh_altura` | `nan` | `\` |
| prontuario=509075, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=k-system | `fresh_altura` | `nan` | `\` |
| prontuario=221142, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=embryoscope | `fresh_altura` | `nan` | `1.58` |
| prontuario=183251, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=embryoscope | `fresh_altura` | `nan` | `\` |
| prontuario=512696, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=embryoscope | `fresh_altura` | `nan` | `\` |
| prontuario=515211, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=embryoscope | `fresh_altura` | `nan` | `\` |
| prontuario=-1, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=embryoscope | `fresh_altura` | `nan` | `163.0` |
| prontuario=183122, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=embryoscope | `fresh_data_crio` | `2021-08-11 00:00:00` | `2021-08-11` |
| prontuario=512644, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=k-system | `fresh_data_crio` | `2021-11-15 00:00:00` | `2023-04-26` |
| prontuario=508176, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=null | `fresh_data_crio` | `NaT` | `2021-08-26` |
| prontuario=509327, transfer_date=null, redlara_chart_or_pin=509327, incubadora_padronizada=embryoscope | `fresh_data_crio` | `2022-02-16 00:00:00` | `2022-02-16` |
| prontuario=525338, transfer_date=null, redlara_chart_or_pin=525338, incubadora_padronizada=embryoscope | `fresh_data_crio` | `2022-06-04 00:00:00` | `2022-06-04` |
| prontuario=509513, transfer_date=null, redlara_chart_or_pin=509513, incubadora_padronizada=embryoscope | `fresh_data_crio` | `2022-09-28 00:00:00` | `2022-09-28` |
| prontuario=509075, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=k-system | `fresh_data_crio` | `2022-01-12 00:00:00` | `2021-12-14` |
| prontuario=221142, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=embryoscope | `fresh_data_crio` | `2022-01-12 00:00:00` | `2021-07-02` |
| prontuario=217870, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=embryoscope | `fresh_data_crio` | `2022-01-15 00:00:00` | `2022-01-15` |
| prontuario=507637, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=embryoscope | `fresh_data_crio` | `2022-10-22 00:00:00` | `2022-10-22` |
| prontuario=183122, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=embryoscope | `fresh_data_de_nasc` | `1986-04-22 00:00:00` | `1986-04-22` |
| prontuario=512644, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=k-system | `fresh_data_de_nasc` | `1987-04-18 00:00:00` | `1987-04-18` |
| prontuario=223394, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=null | `fresh_data_de_nasc` | `NaT` | `1984-06-20` |
| prontuario=508176, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=null | `fresh_data_de_nasc` | `NaT` | `1987-04-04` |
| prontuario=509327, transfer_date=null, redlara_chart_or_pin=509327, incubadora_padronizada=embryoscope | `fresh_data_de_nasc` | `1989-08-11 00:00:00` | `1989-08-11` |
| prontuario=525338, transfer_date=null, redlara_chart_or_pin=525338, incubadora_padronizada=embryoscope | `fresh_data_de_nasc` | `1985-04-15 00:00:00` | `1985-04-15` |
| prontuario=509513, transfer_date=null, redlara_chart_or_pin=509513, incubadora_padronizada=embryoscope | `fresh_data_de_nasc` | `1981-09-05 00:00:00` | `1981-09-05` |
| prontuario=509075, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=k-system | `fresh_data_de_nasc` | `1986-04-02 00:00:00` | `1986-04-02` |
| prontuario=221142, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=embryoscope | `fresh_data_de_nasc` | `1982-04-17 00:00:00` | `1982-04-17` |
| prontuario=217870, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=embryoscope | `fresh_data_de_nasc` | `1986-09-02 00:00:00` | `1986-09-02` |
| prontuario=512644, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=k-system | `fresh_dia_cryo` | `5/6` | `nan` |
| prontuario=509327, transfer_date=null, redlara_chart_or_pin=509327, incubadora_padronizada=embryoscope | `fresh_dia_cryo` | `5` | `nan` |
| prontuario=525338, transfer_date=null, redlara_chart_or_pin=525338, incubadora_padronizada=embryoscope | `fresh_dia_cryo` | `5` | `nan` |
| prontuario=509513, transfer_date=null, redlara_chart_or_pin=509513, incubadora_padronizada=embryoscope | `fresh_dia_cryo` | `5` | `nan` |
| prontuario=509075, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=k-system | `fresh_dia_cryo` | `7` | `nan` |
| prontuario=221142, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=embryoscope | `fresh_dia_cryo` | `5` | `nan` |
| prontuario=217870, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=embryoscope | `fresh_dia_cryo` | `5/6` | `nan` |
| prontuario=507637, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=embryoscope | `fresh_dia_cryo` | `6` | `nan` |
| prontuario=183251, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=embryoscope | `fresh_dia_cryo` | `5/6` | `nan` |
| prontuario=512696, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=embryoscope | `fresh_dia_cryo` | `5/6` | `nan` |
| prontuario=512644, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=k-system | `fresh_fator_1` | `NOT AVAIBLE` | `NOT AVAILABLE` |
| prontuario=223394, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=null | `fresh_fator_1` | `nan` | `OVARIAN INSUFFICIENCY` |
| prontuario=508176, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=null | `fresh_fator_1` | `nan` | `NOT AVAIBLE` |
| prontuario=509075, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=k-system | `fresh_fator_1` | `\` | `NOT AVAILABLE` |
| prontuario=221142, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=embryoscope | `fresh_fator_1` | `\` | `NOT AVAILABLE` |
| prontuario=-1, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=embryoscope | `fresh_fator_1` | `MALE FACTOR` | `OVARIAN INSUFFICIENCY` |
| prontuario=522878, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=embryoscope | `fresh_fator_1` | `\` | `NOT AVAILABLE` |
| prontuario=523274, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=embryoscope | `fresh_fator_1` | `\` | `NOT AVAILABLE` |
| prontuario=681256, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=embryoscope | `fresh_fator_1` | `OTHER (GENÉTICO, ABORTAMENTO, IDADE OVULAR COM BOA RESERVA)` | `NOT AVAILABLE` |
| prontuario=513327, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=embryoscope | `fresh_fator_1` | `OVARIAN INSUFFICIENCY` | `NOT AVAILABLE` |
| prontuario=512644, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=k-system | `fresh_incubadora` | `4.7` | `7.9` |
| prontuario=509075, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=k-system | `fresh_incubadora` | `8.7` | `6.5` |
| prontuario=-1, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=k-system | `fresh_incubadora` | `6.4` | `K-SYSTEM` |
| prontuario=786874, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=k-system | `fresh_incubadora` | `8.5` | `7.6` |
| prontuario=217019, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=k-system | `fresh_incubadora` | `7.9` | `7.1` |
| prontuario=518585, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=k-system | `fresh_incubadora` | `7.4` | `6.5` |
| prontuario=693481, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=k-system | `fresh_incubadora` | `8.8` | `8.6` |
| prontuario=806977, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=k-system | `fresh_incubadora` | `8.3` | `K-SYSTEM` |
| prontuario=788129, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=k-system | `fresh_incubadora` | `8.9` | `6.6` |
| prontuario=797400, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=embryoscope | `fresh_incubadora` | `ES` | `ES 1` |
| prontuario=512644, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=k-system | `fresh_opu` | `11` | `6.0` |
| prontuario=223394, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=null | `fresh_opu` | `<NA>` | `8.0` |
| prontuario=508176, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=null | `fresh_opu` | `<NA>` | `4.0` |
| prontuario=509075, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=k-system | `fresh_opu` | `0` | `26.0` |
| prontuario=221142, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=embryoscope | `fresh_opu` | `<NA>` | `11.0` |
| prontuario=183251, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=embryoscope | `fresh_opu` | `5` | `6.0` |
| prontuario=-1, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=embryoscope | `fresh_opu` | `17` | `2.0` |
| prontuario=-1, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=k-system | `fresh_opu` | `14` | `15.0` |
| prontuario=219785, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=embryoscope | `fresh_opu` | `4` | `0.0` |
| prontuario=176325, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=embryoscope | `fresh_opu` | `0` | `25.0` |
| prontuario=183122, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=embryoscope | `fresh_origem` | `nan` | `EJACULATED` |
| prontuario=512644, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=k-system | `fresh_origem` | `nan` | `EJACULATED` |
| prontuario=223394, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=null | `fresh_origem` | `nan` | `EJACULATED` |
| prontuario=508176, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=null | `fresh_origem` | `nan` | `EJACULATED` |
| prontuario=509327, transfer_date=null, redlara_chart_or_pin=509327, incubadora_padronizada=embryoscope | `fresh_origem` | `nan` | `\` |
| prontuario=525338, transfer_date=null, redlara_chart_or_pin=525338, incubadora_padronizada=embryoscope | `fresh_origem` | `nan` | `EJACULATED` |
| prontuario=509513, transfer_date=null, redlara_chart_or_pin=509513, incubadora_padronizada=embryoscope | `fresh_origem` | `nan` | `EJACULATED` |
| prontuario=509075, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=k-system | `fresh_origem` | `nan` | `DONATED` |
| prontuario=221142, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=embryoscope | `fresh_origem` | `nan` | `EJACULATED` |
| prontuario=217870, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=embryoscope | `fresh_origem` | `nan` | `EJACULATED` |
| prontuario=512644, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=k-system | `fresh_peso` | `<NA>` | `60` |
| prontuario=223394, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=null | `fresh_peso` | `<NA>` | `72` |
| prontuario=508176, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=null | `fresh_peso` | `<NA>` | `\` |
| prontuario=509327, transfer_date=null, redlara_chart_or_pin=509327, incubadora_padronizada=embryoscope | `fresh_peso` | `<NA>` | `\` |
| prontuario=509075, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=k-system | `fresh_peso` | `<NA>` | `\` |
| prontuario=221142, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=embryoscope | `fresh_peso` | `<NA>` | `57` |
| prontuario=183251, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=embryoscope | `fresh_peso` | `<NA>` | `\` |
| prontuario=512696, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=embryoscope | `fresh_peso` | `<NA>` | `\` |
| prontuario=515211, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=embryoscope | `fresh_peso` | `<NA>` | `\` |
| prontuario=-1, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=embryoscope | `fresh_peso` | `<NA>` | `72.0` |
| prontuario=512644, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=k-system | `fresh_qtd_analisados` | `<NA>` | `2.0` |
| prontuario=509075, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=k-system | `fresh_qtd_analisados` | `1` | `3.0` |
| prontuario=183251, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=embryoscope | `fresh_qtd_analisados` | `2` | `1.0` |
| prontuario=514156, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=embryoscope | `fresh_qtd_analisados` | `1` | `2.0` |
| prontuario=-1, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=k-system | `fresh_qtd_analisados` | `4` | `8.0` |
| prontuario=520827, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=embryoscope | `fresh_qtd_analisados` | `3` | `0.0` |
| prontuario=219785, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=embryoscope | `fresh_qtd_analisados` | `1` | `0.0` |
| prontuario=176325, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=embryoscope | `fresh_qtd_analisados` | `4` | `10.0` |
| prontuario=522950, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=embryoscope | `fresh_qtd_analisados` | `2` | `0.0` |
| prontuario=523274, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=embryoscope | `fresh_qtd_analisados` | `3` | `4.0` |
| prontuario=512644, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=k-system | `fresh_qtd_blasto` | `5` | `2.0` |
| prontuario=508176, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=null | `fresh_qtd_blasto` | `<NA>` | `1.0` |
| prontuario=509075, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=k-system | `fresh_qtd_blasto` | `2` | `3.0` |
| prontuario=221142, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=embryoscope | `fresh_qtd_blasto` | `1` | `4.0` |
| prontuario=183251, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=embryoscope | `fresh_qtd_blasto` | `2` | `1.0` |
| prontuario=514156, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=embryoscope | `fresh_qtd_blasto` | `1` | `4.0` |
| prontuario=-1, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=k-system | `fresh_qtd_blasto` | `4` | `8.0` |
| prontuario=520827, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=embryoscope | `fresh_qtd_blasto` | `3` | `0.0` |
| prontuario=512455, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=embryoscope | `fresh_qtd_blasto` | `1` | `2.0` |
| prontuario=219785, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=embryoscope | `fresh_qtd_blasto` | `1` | `5.0` |
| prontuario=512644, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=k-system | `fresh_qtd_normais` | `<NA>` | `0.0` |
| prontuario=509075, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=k-system | `fresh_qtd_normais` | `1` | `0.0` |
| prontuario=221142, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=embryoscope | `fresh_qtd_normais` | `0` | `nan` |
| prontuario=-1, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=embryoscope | `fresh_qtd_normais` | `0` | `nan` |
| prontuario=514156, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=embryoscope | `fresh_qtd_normais` | `0` | `2.0` |
| prontuario=-1, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=k-system | `fresh_qtd_normais` | `1` | `0.0` |
| prontuario=176325, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=embryoscope | `fresh_qtd_normais` | `0` | `3.0` |
| prontuario=523274, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=embryoscope | `fresh_qtd_normais` | `0` | `1.0` |
| prontuario=513327, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=embryoscope | `fresh_qtd_normais` | `1` | `0.0` |
| prontuario=202412, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=embryoscope | `fresh_qtd_normais` | `0` | `4.0` |
| prontuario=183122, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=embryoscope | `fresh_tipo` | `nan` | `FRESH` |
| prontuario=512644, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=k-system | `fresh_tipo` | `nan` | `FRESH` |
| prontuario=223394, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=null | `fresh_tipo` | `nan` | `FRESH` |
| prontuario=508176, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=null | `fresh_tipo` | `nan` | `CRYOPRESERVED` |
| prontuario=509327, transfer_date=null, redlara_chart_or_pin=509327, incubadora_padronizada=embryoscope | `fresh_tipo` | `nan` | `\` |
| prontuario=525338, transfer_date=null, redlara_chart_or_pin=525338, incubadora_padronizada=embryoscope | `fresh_tipo` | `nan` | `FRESH` |
| prontuario=509513, transfer_date=null, redlara_chart_or_pin=509513, incubadora_padronizada=embryoscope | `fresh_tipo` | `nan` | `FRESH` |
| prontuario=509075, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=k-system | `fresh_tipo` | `nan` | `CRYOPRESERVED` |
| prontuario=221142, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=embryoscope | `fresh_tipo` | `nan` | `FRESH` |
| prontuario=217870, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=embryoscope | `fresh_tipo` | `nan` | `FRESH` |
| prontuario=512644, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=k-system | `fresh_tipo_biopsia` | `\` | `PGT-A` |
| prontuario=223394, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=null | `fresh_tipo_biopsia` | `nan` | `\` |
| prontuario=508176, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=null | `fresh_tipo_biopsia` | `nan` | `\` |
| prontuario=219785, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=embryoscope | `fresh_tipo_biopsia` | `PGT-A` | `\` |
| prontuario=176325, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=embryoscope | `fresh_tipo_biopsia` | `PGT-M` | `PGT-A` |
| prontuario=522950, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=embryoscope | `fresh_tipo_biopsia` | `PGT-A` | `\` |
| prontuario=525042, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=embryoscope | `fresh_tipo_biopsia` | `\` | `PGT-A` |
| prontuario=681256, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=embryoscope | `fresh_tipo_biopsia` | `PGT-A` | `\` |
| prontuario=186016, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=null | `fresh_tipo_biopsia` | `nan` | `\` |
| prontuario=182391, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=null | `fresh_tipo_biopsia` | `nan` | `PGT-A` |
| prontuario=512644, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=k-system | `fresh_total_de_mii` | `7` | `5.0` |
| prontuario=223394, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=null | `fresh_total_de_mii` | `<NA>` | `2.0` |
| prontuario=508176, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=null | `fresh_total_de_mii` | `<NA>` | `3.0` |
| prontuario=509075, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=k-system | `fresh_total_de_mii` | `8` | `20.0` |
| prontuario=221142, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=embryoscope | `fresh_total_de_mii` | `6` | `10.0` |
| prontuario=-1, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=embryoscope | `fresh_total_de_mii` | `10` | `1.0` |
| prontuario=-1, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=k-system | `fresh_total_de_mii` | `12` | `14.0` |
| prontuario=520827, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=embryoscope | `fresh_total_de_mii` | `12` | `11.0` |
| prontuario=512455, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=embryoscope | `fresh_total_de_mii` | `10` | `8.0` |
| prontuario=219785, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=embryoscope | `fresh_total_de_mii` | `3` | `7.0` |
| prontuario=-1, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=null | `gestational_age_at_delivery` | `nan` | `\` |
| prontuario=182704, transfer_date=null, redlara_chart_or_pin=182704, incubadora_padronizada=null | `merged_numero_de_nascidos` | `0` | `nan` |
| prontuario=509327, transfer_date=null, redlara_chart_or_pin=509327, incubadora_padronizada=embryoscope | `n_of_biopsied` | `1` | `nan` |
| prontuario=525338, transfer_date=null, redlara_chart_or_pin=525338, incubadora_padronizada=embryoscope | `n_of_biopsied` | `1` | `nan` |
| prontuario=509513, transfer_date=null, redlara_chart_or_pin=509513, incubadora_padronizada=embryoscope | `n_of_biopsied` | `2` | `nan` |
| prontuario=207430, transfer_date=null, redlara_chart_or_pin=207430, incubadora_padronizada=null | `n_of_biopsied` | `1` | `nan` |
| prontuario=216584, transfer_date=null, redlara_chart_or_pin=216584, incubadora_padronizada=null | `n_of_biopsied` | `5` | `nan` |
| prontuario=515090, transfer_date=null, redlara_chart_or_pin=515090, incubadora_padronizada=null | `n_of_biopsied` | `1` | `nan` |
| prontuario=162510, transfer_date=null, redlara_chart_or_pin=162510, incubadora_padronizada=null | `n_of_biopsied` | `0` | `nan` |
| prontuario=183374, transfer_date=null, redlara_chart_or_pin=85339, incubadora_padronizada=null | `n_of_biopsied` | `5` | `nan` |
| prontuario=174588, transfer_date=null, redlara_chart_or_pin=174588, incubadora_padronizada=null | `n_of_biopsied` | `1` | `nan` |
| prontuario=219334, transfer_date=null, redlara_chart_or_pin=50033855, incubadora_padronizada=null | `n_of_biopsied` | `0` | `nan` |
| prontuario=160610, transfer_date=null, redlara_chart_or_pin=160610, incubadora_padronizada=null | `n_of_normal` | `nan` | `1.0` |
| prontuario=752648, transfer_date=null, redlara_chart_or_pin=752648, incubadora_padronizada=null | `n_of_normal` | `1.0` | `2.0` |
| prontuario=181862, transfer_date=null, redlara_chart_or_pin=181862, incubadora_padronizada=null | `n_of_normal` | `1` | `0.0` |
| prontuario=836954, transfer_date=null, redlara_chart_or_pin=836954, incubadora_padronizada=null | `n_of_normal` | `nan` | `0.0` |
| prontuario=182704, transfer_date=null, redlara_chart_or_pin=182704, incubadora_padronizada=null | `number_of_embryos_transferred` | `0` | `nan` |
| prontuario=509591, transfer_date=null, redlara_chart_or_pin=509591, incubadora_padronizada=null | `number_of_fet_after_originally_frozen` | `FET from PGT` | `nan` |
| prontuario=183514, transfer_date=null, redlara_chart_or_pin=183514, incubadora_padronizada=null | `number_of_fet_after_originally_frozen` | `FET from PGT` | `nan` |
| prontuario=139500, transfer_date=null, redlara_chart_or_pin=139500, incubadora_padronizada=null | `number_of_fet_after_originally_frozen` | `FET from PGT` | `nan` |
| prontuario=150015, transfer_date=null, redlara_chart_or_pin=150015, incubadora_padronizada=null | `number_of_fet_after_originally_frozen` | `FET from PGT` | `nan` |
| prontuario=508356, transfer_date=null, redlara_chart_or_pin=508356, incubadora_padronizada=null | `number_of_fet_after_originally_frozen` | `FET from PGT` | `nan` |
| prontuario=522782, transfer_date=null, redlara_chart_or_pin=522782, incubadora_padronizada=null | `number_of_fet_after_originally_frozen` | `FET from PGT` | `nan` |
| prontuario=513761, transfer_date=null, redlara_chart_or_pin=513761.0, incubadora_padronizada=null | `number_of_fet_after_originally_frozen` | `2nd transfer` | `nan` |
| prontuario=219497, transfer_date=null, redlara_chart_or_pin=50034023.0, incubadora_padronizada=null | `number_of_fet_after_originally_frozen` | `\` | `nan` |
| prontuario=180941, transfer_date=null, redlara_chart_or_pin=82907.0, incubadora_padronizada=null | `number_of_fet_after_originally_frozen` | `1st` | `nan` |
| prontuario=153510, transfer_date=null, redlara_chart_or_pin=153510.0, incubadora_padronizada=null | `number_of_fet_after_originally_frozen` | `1st` | `nan` |
| prontuario=836954, transfer_date=null, redlara_chart_or_pin=836954, incubadora_padronizada=null | `outcome_type` | `Due to abnormal or no embryo development` | `OTHER` |
| prontuario=-1, transfer_date=null, redlara_chart_or_pin=null, incubadora_padronizada=null | `type_of_delivery` | `nan` | `\` |

### `protheus_mesclada_vendas`
#### **Schema Mismatches**
* Columns in DuckDB Only: `CPF`, `Descr.TES`, `Descrição Gerencial`, `Descrição Mapping Actividad`, `Interno/Externo`, `Lead Time`, `Médico`, `Mês`, `NF Eletr.`, `Qnt Cons.`, `Valor Mercadoria`, `extraction_timestamp`
* Columns in Athena Only: `bronze_updated_at`, `company_id`, `descricao_gerencial`, `descricao_mapping_actividad`, `interno_externo`, `item_seq`, `lead_time`, `medico`, `mes`, `nfe_numero`, `prontuario_match_tier`, `qnt_cons`, `tes_descricao`, `valor_mercadoria`
#### **Numeric Metric Alignment**
| Column Name | DuckDB Sum | Athena Sum | Difference | Alignment Rate |
| :--- | :---: | :---: | :---: | :---: |
| `qntd` | 684867.25 | 686501.84 | -1634.59 | 99.7619% |
| `total` | 589241830.41 | 638391050.97 | -49149220.56 | 92.3011% |

#### **Discrepancy Group Isolation**
* **By Year/Date**:
  * Year **2022**: 77586 discrepant records
  * Year **2023**: 123550 discrepant records
  * Year **2024**: 147442 discrepant records
  * Year **2025**: 168410 discrepant records
  * Year **2026**: 84878 discrepant records
* **By Dimension Group**:
  * Group **010101**: 211652 discrepant records
  * Group **010104**: 85386 discrepant records
  * Group **010106**: 642 discrepant records
  * Group **010150**: 89151 discrepant records
  * Group **010155**: 59260 discrepant records
  * Group **020101**: 2094 discrepant records
  * Group **030101**: 70516 discrepant records
  * Group **060101**: 83165 discrepant records

#### **Anonymized Column Discrepancy Samples**
| Composite Key | Column Name | DuckDB Value | Athena Value |
| :--- | :--- | :--- | :--- |
| file_name=protheus api, line_number=51120 | `ano` | `2026` | `2025` |
| file_name=protheus api, line_number=51121 | `ano` | `2026` | `2025` |
| file_name=protheus api, line_number=51122 | `ano` | `2026` | `2025` |
| file_name=protheus api, line_number=51123 | `ano` | `2026` | `2025` |
| file_name=protheus api, line_number=51124 | `ano` | `2026` | `2025` |
| file_name=protheus api, line_number=51125 | `ano` | `2026` | `2025` |
| file_name=protheus api, line_number=51126 | `ano` | `2026` | `2025` |
| file_name=protheus api, line_number=51127 | `ano` | `2026` | `2025` |
| file_name=protheus api, line_number=51128 | `ano` | `2026` | `2025` |
| file_name=protheus api, line_number=51129 | `ano` | `2026` | `2025` |
| file_name=protheus api, line_number=1 | `cliente_codms` | `846358` | `851293.0` |
| file_name=protheus api, line_number=2 | `cliente_codms` | `824249` | `851292.0` |
| file_name=protheus api, line_number=3 | `cliente_codms` | `822559` | `863419.0` |
| file_name=protheus api, line_number=4 | `cliente_codms` | `814408` | `876090.0` |
| file_name=protheus api, line_number=5 | `cliente_codms` | `796004` | `903225.0` |
| file_name=protheus api, line_number=6 | `cliente_codms` | `789230` | `903225.0` |
| file_name=protheus api, line_number=7 | `cliente_codms` | `754197` | `869151.0` |
| file_name=protheus api, line_number=8 | `cliente_codms` | `524783` | `863167.0` |
| file_name=protheus api, line_number=9 | `cliente_codms` | `524141` | `862248.0` |
| file_name=protheus api, line_number=10 | `cliente_codms` | `509595` | `851894.0` |
| file_name=protheus api, line_number=1 | `cliente_totvs` | `882943` | `884751` |
| file_name=protheus api, line_number=2 | `cliente_totvs` | `874235` | `884750` |
| file_name=protheus api, line_number=3 | `cliente_totvs` | `873600` | `886245` |
| file_name=protheus api, line_number=4 | `cliente_totvs` | `870258` | `891349` |
| file_name=protheus api, line_number=5 | `cliente_totvs` | `862051` | `226798` |
| file_name=protheus api, line_number=6 | `cliente_totvs` | `861656` | `226798` |
| file_name=protheus api, line_number=7 | `cliente_totvs` | `805154` | `888515` |
| file_name=protheus api, line_number=8 | `cliente_totvs` | `524783` | `886130` |
| file_name=protheus api, line_number=9 | `cliente_totvs` | `524141` | `885739` |
| file_name=protheus api, line_number=10 | `cliente_totvs` | `509595` | `885477` |
| file_name=protheus api, line_number=1 | `cta_ctbl` | `[PHONE_REDACTED]` | `[PHONE_REDACTED]` |
| file_name=protheus api, line_number=2 | `cta_ctbl` | `[PHONE_REDACTED]` | `[PHONE_REDACTED]` |
| file_name=protheus api, line_number=3 | `cta_ctbl` | `[PHONE_REDACTED]` | `[PHONE_REDACTED]` |
| file_name=protheus api, line_number=4 | `cta_ctbl` | `[PHONE_REDACTED]` | `[PHONE_REDACTED]` |
| file_name=protheus api, line_number=5 | `cta_ctbl` | `[PHONE_REDACTED]` | `[PHONE_REDACTED]` |
| file_name=protheus api, line_number=6 | `cta_ctbl` | `[PHONE_REDACTED]` | `[PHONE_REDACTED]` |
| file_name=protheus api, line_number=38 | `cta_ctbl` | `[PHONE_REDACTED]` | `[PHONE_REDACTED]` |
| file_name=protheus api, line_number=39 | `cta_ctbl` | `[PHONE_REDACTED]` | `[PHONE_REDACTED]` |
| file_name=protheus api, line_number=41 | `cta_ctbl` | `[PHONE_REDACTED]` | `[PHONE_REDACTED]` |
| file_name=protheus api, line_number=42 | `cta_ctbl` | `[PHONE_REDACTED]` | `[PHONE_REDACTED]` |
| file_name=protheus api, line_number=1 | `data_emissao` | `2026-06-23 00:00:00` | `2026-07-13 00:00:00` |
| file_name=protheus api, line_number=2 | `data_emissao` | `2026-06-23 00:00:00` | `2026-07-13 00:00:00` |
| file_name=protheus api, line_number=3 | `data_emissao` | `2026-06-23 00:00:00` | `2026-07-13 00:00:00` |
| file_name=protheus api, line_number=4 | `data_emissao` | `2026-06-23 00:00:00` | `2026-07-13 00:00:00` |
| file_name=protheus api, line_number=5 | `data_emissao` | `2026-06-23 00:00:00` | `2026-07-13 00:00:00` |
| file_name=protheus api, line_number=6 | `data_emissao` | `2026-06-23 00:00:00` | `2026-07-13 00:00:00` |
| file_name=protheus api, line_number=7 | `data_emissao` | `2026-06-23 00:00:00` | `2026-07-13 00:00:00` |
| file_name=protheus api, line_number=8 | `data_emissao` | `2026-06-23 00:00:00` | `2026-07-13 00:00:00` |
| file_name=protheus api, line_number=9 | `data_emissao` | `2026-06-23 00:00:00` | `2026-07-13 00:00:00` |
| file_name=protheus api, line_number=10 | `data_emissao` | `2026-06-23 00:00:00` | `2026-07-13 00:00:00` |
| file_name=protheus api, line_number=2 | `desconto` | `0.0` | `5.0000` |
| file_name=protheus api, line_number=38 | `desconto` | `4.76` | `0.0000` |
| file_name=protheus api, line_number=42 | `desconto` | `1.96` | `0.0000` |
| file_name=protheus api, line_number=44 | `desconto` | `1.96` | `0.0000` |
| file_name=protheus api, line_number=46 | `desconto` | `1.96` | `0.0000` |
| file_name=protheus api, line_number=48 | `desconto` | `1.96` | `0.0000` |
| file_name=protheus api, line_number=51 | `desconto` | `4.76` | `0.0000` |
| file_name=protheus api, line_number=52 | `desconto` | `4.76` | `0.0000` |
| file_name=protheus api, line_number=99 | `desconto` | `0.36` | `0.0000` |
| file_name=protheus api, line_number=287 | `desconto` | `0.0` | `4.7600` |
| file_name=protheus api, line_number=1 | `grp` | `1` | `06` |
| file_name=protheus api, line_number=2 | `grp` | `1` | `06` |
| file_name=protheus api, line_number=3 | `grp` | `1` | `06` |
| file_name=protheus api, line_number=4 | `grp` | `1` | `06` |
| file_name=protheus api, line_number=5 | `grp` | `1` | `06` |
| file_name=protheus api, line_number=6 | `grp` | `1` | `06` |
| file_name=protheus api, line_number=7 | `grp` | `1` | `06` |
| file_name=protheus api, line_number=8 | `grp` | `1` | `06` |
| file_name=protheus api, line_number=9 | `grp` | `1` | `06` |
| file_name=protheus api, line_number=10 | `grp` | `1` | `06` |
| file_name=protheus api, line_number=1 | `loja` | `010101` | `060101` |
| file_name=protheus api, line_number=2 | `loja` | `010101` | `060101` |
| file_name=protheus api, line_number=3 | `loja` | `010101` | `060101` |
| file_name=protheus api, line_number=4 | `loja` | `010101` | `060101` |
| file_name=protheus api, line_number=5 | `loja` | `010101` | `060101` |
| file_name=protheus api, line_number=6 | `loja` | `010101` | `060101` |
| file_name=protheus api, line_number=7 | `loja` | `010101` | `060101` |
| file_name=protheus api, line_number=8 | `loja` | `010101` | `060101` |
| file_name=protheus api, line_number=9 | `loja` | `010101` | `060101` |
| file_name=protheus api, line_number=10 | `loja` | `010101` | `060101` |
| file_name=protheus api, line_number=1 | `nome` | `SOFIA VILELA DE MORAES E SILVA` | `RODOLFO MITSURU TANAKA` |
| file_name=protheus api, line_number=2 | `nome` | `JAKELLYNE COUREL` | `ROSANA SAYURI MAESHIRO TANAKA` |
| file_name=protheus api, line_number=3 | `nome` | `BRUNA BANDEIRA PINHEIRO MESTRES` | `LYGIA SPINOZA HERRERO` |
| file_name=protheus api, line_number=4 | `nome` | `ERIKA PINTO` | `THAMIRES BENTO NEGRO` |
| file_name=protheus api, line_number=5 | `nome` | `PAULA BOMBONATTI MAIA` | `LARISSA FERRARI DA COSTA LIMA` |
| file_name=protheus api, line_number=6 | `nome` | `CAROLINA DUTRA CARVALHO` | `LARISSA FERRARI DA COSTA LIMA` |
| file_name=protheus api, line_number=7 | `nome` | `THIAGO UEDA` | `ISA REGIA TAVARES DE MELO BARBOSA` |
| file_name=protheus api, line_number=8 | `nome` | `MARCO AURELIO LUZ GONCALVES` | `ANNA BEATRIZ FERREIRA MANRUBIA` |
| file_name=protheus api, line_number=9 | `nome` | `CAMILA SALGADO FERRAZ` | `MELANIE GAKIYA BRANDAO` |
| file_name=protheus api, line_number=10 | `nome` | `ALESSANDRA VISINTAINER HACKRADT` | `JULIANA CRISTINA AYRES COSTA` |
| file_name=protheus api, line_number=1 | `nome_paciente` | `SOFIA VILELA DE MORAES E SILVA` | `RODOLFO MITSURU TANAKA` |
| file_name=protheus api, line_number=2 | `nome_paciente` | `JAKELLYNE COUREL` | `ROSANA SAYURI MAESHIRO TANAKA` |
| file_name=protheus api, line_number=3 | `nome_paciente` | `BRUNA BANDEIRA PINHEIRO MESTRES` | `LYGIA SPINOZA HERRERO` |
| file_name=protheus api, line_number=4 | `nome_paciente` | `ERIKA PINTO` | `THAMIRES BENTO NEGRO` |
| file_name=protheus api, line_number=5 | `nome_paciente` | `PAULA BOMBONATTI MAIA` | `LARISSA FERRARI DA COSTA LIMA` |
| file_name=protheus api, line_number=6 | `nome_paciente` | `CAROLINA DUTRA CARVALHO` | `LARISSA FERRARI DA COSTA LIMA` |
| file_name=protheus api, line_number=7 | `nome_paciente` | `IRIS GALDINO UEDA` | `ISA REGIA TAVARES DE MELO BARBOSA` |
| file_name=protheus api, line_number=8 | `nome_paciente` | `MARCO AURELIO LUZ GONCALVES` | `ANNA BEATRIZ FERREIRA MANRUBIA` |
| file_name=protheus api, line_number=9 | `nome_paciente` | `CAMILA SALGADO FERRAZ` | `MELANIE GAKIYA BRANDAO` |
| file_name=protheus api, line_number=10 | `nome_paciente` | `ALESSANDRA VISINTAINER HACKRADT` | `JULIANA CRISTINA AYRES COSTA` |
| file_name=protheus api, line_number=1 | `numero_nota` | `251165` | `256844` |
| file_name=protheus api, line_number=2 | `numero_nota` | `251164` | `256843` |
| file_name=protheus api, line_number=3 | `numero_nota` | `251163` | `256842` |
| file_name=protheus api, line_number=4 | `numero_nota` | `251162` | `256841` |
| file_name=protheus api, line_number=5 | `numero_nota` | `251161` | `256840` |
| file_name=protheus api, line_number=6 | `numero_nota` | `251160` | `256840` |
| file_name=protheus api, line_number=7 | `numero_nota` | `251159` | `256839` |
| file_name=protheus api, line_number=8 | `numero_nota` | `251158` | `256838` |
| file_name=protheus api, line_number=9 | `numero_nota` | `251157` | `256837` |
| file_name=protheus api, line_number=10 | `numero_nota` | `251156` | `256836` |
| file_name=protheus api, line_number=1 | `paciente_codms` | `846358` | `00851293` |
| file_name=protheus api, line_number=2 | `paciente_codms` | `824249` | `00851292` |
| file_name=protheus api, line_number=3 | `paciente_codms` | `822559` | `00863419` |
| file_name=protheus api, line_number=4 | `paciente_codms` | `814408` | `00876090` |
| file_name=protheus api, line_number=5 | `paciente_codms` | `796004` | `00903225` |
| file_name=protheus api, line_number=6 | `paciente_codms` | `789230` | `00903225` |
| file_name=protheus api, line_number=7 | `paciente_codms` | `754196` | `00869151` |
| file_name=protheus api, line_number=8 | `paciente_codms` | `524783` | `00863167` |
| file_name=protheus api, line_number=9 | `paciente_codms` | `524141` | `00862248` |
| file_name=protheus api, line_number=10 | `paciente_codms` | `509595` | `00851894` |
| file_name=protheus api, line_number=1 | `produto` | `S3566.39` | `[PHONE_REDACTED]48` |
| file_name=protheus api, line_number=2 | `produto` | `S3566.39` | `[PHONE_REDACTED]48` |
| file_name=protheus api, line_number=3 | `produto` | `S3566.39` | `[PHONE_REDACTED]31` |
| file_name=protheus api, line_number=4 | `produto` | `S3566.39` | `[PHONE_REDACTED]19` |
| file_name=protheus api, line_number=5 | `produto` | `S3566.40` | `[PHONE_REDACTED]09` |
| file_name=protheus api, line_number=6 | `produto` | `S3566.39` | `[PHONE_REDACTED]16` |
| file_name=protheus api, line_number=8 | `produto` | `S3566.41` | `S3566.39` |
| file_name=protheus api, line_number=9 | `produto` | `S3566.40` | `S3566.39` |
| file_name=protheus api, line_number=13 | `produto` | `S3566.40` | `S3566.39` |
| file_name=protheus api, line_number=14 | `produto` | `S3566.39` | `S3566.40` |
| file_name=protheus api, line_number=1 | `produto_descricao` | `MENSALIDADE CRIOPRESERVACAO DE EMBRIAO - MEDICOS INTERNOS` | `PERGOVERIS PEN 900/450 UI -1,44ML` |
| file_name=protheus api, line_number=2 | `produto_descricao` | `MENSALIDADE CRIOPRESERVACAO DE EMBRIAO - MEDICOS INTERNOS` | `PERGOVERIS PEN 900/450 UI -1,44ML` |
| file_name=protheus api, line_number=3 | `produto_descricao` | `MENSALIDADE CRIOPRESERVACAO DE EMBRIAO - MEDICOS INTERNOS` | `OESTROGEL PUMP GEL 0,6MG/GC/80G (C.E.)` |
| file_name=protheus api, line_number=4 | `produto_descricao` | `MENSALIDADE CRIOPRESERVACAO DE EMBRIAO - MEDICOS INTERNOS` | `GONAPEPTYL DAILY 0,1 MGML 7 SERINGAS DE 1 ML (C.E.)` |
| file_name=protheus api, line_number=5 | `produto_descricao` | `MENSALIDADE CRIOPRESERVACAO DE OOCITOS - MEDICOS INTERNOS` | `MENOPUR MD 600UI (C.E.)` |
| file_name=protheus api, line_number=6 | `produto_descricao` | `MENSALIDADE CRIOPRESERVACAO DE EMBRIAO - MEDICOS INTERNOS` | `GONAL F PEN 300UI 22MCG 0,5ML (C.E.)` |
| file_name=protheus api, line_number=8 | `produto_descricao` | `MENSALIDADE CRIOPRESERVACAO DE SEMEN - MEDICOS INTERNOS` | `MENSALIDADE CRIOPRESERVACAO DE EMBRIAO - MEDICOS INTERNOS` |
| file_name=protheus api, line_number=9 | `produto_descricao` | `MENSALIDADE CRIOPRESERVACAO DE OOCITOS - MEDICOS INTERNOS` | `MENSALIDADE CRIOPRESERVACAO DE EMBRIAO - MEDICOS INTERNOS` |
| file_name=protheus api, line_number=13 | `produto_descricao` | `MENSALIDADE CRIOPRESERVACAO DE OOCITOS - MEDICOS INTERNOS` | `MENSALIDADE CRIOPRESERVACAO DE EMBRIAO - MEDICOS INTERNOS` |
| file_name=protheus api, line_number=14 | `produto_descricao` | `MENSALIDADE CRIOPRESERVACAO DE EMBRIAO - MEDICOS INTERNOS` | `MENSALIDADE CRIOPRESERVACAO DE OOCITOS - MEDICOS INTERNOS` |
| file_name=protheus api, line_number=1 | `prontuario` | `846358` | `851292` |
| file_name=protheus api, line_number=2 | `prontuario` | `824249` | `851292` |
| file_name=protheus api, line_number=3 | `prontuario` | `822559` | `863419` |
| file_name=protheus api, line_number=4 | `prontuario` | `814408` | `876090` |
| file_name=protheus api, line_number=5 | `prontuario` | `796004` | `903225` |
| file_name=protheus api, line_number=6 | `prontuario` | `789230` | `903225` |
| file_name=protheus api, line_number=7 | `prontuario` | `754196` | `869151` |
| file_name=protheus api, line_number=8 | `prontuario` | `524782` | `863167` |
| file_name=protheus api, line_number=9 | `prontuario` | `524141` | `862248` |
| file_name=protheus api, line_number=10 | `prontuario` | `509595` | `851894` |
| file_name=protheus api, line_number=42 | `qntd` | `3.0` | `1.0000` |
| file_name=protheus api, line_number=43 | `qntd` | `3.0` | `1.0000` |
| file_name=protheus api, line_number=46 | `qntd` | `3.0` | `1.0000` |
| file_name=protheus api, line_number=47 | `qntd` | `3.0` | `1.0000` |
| file_name=protheus api, line_number=64 | `qntd` | `2.0` | `1.0000` |
| file_name=protheus api, line_number=72 | `qntd` | `2.0` | `1.0000` |
| file_name=protheus api, line_number=83 | `qntd` | `2.0` | `1.0000` |
| file_name=protheus api, line_number=84 | `qntd` | `5.0` | `1.0000` |
| file_name=protheus api, line_number=85 | `qntd` | `5.0` | `1.0000` |
| file_name=protheus api, line_number=87 | `qntd` | `5.0` | `1.0000` |
| file_name=protheus api, line_number=590 | `serie` | `RPS` | `3` |
| file_name=protheus api, line_number=591 | `serie` | `RPS` | `3` |
| file_name=protheus api, line_number=592 | `serie` | `RPS` | `3` |
| file_name=protheus api, line_number=593 | `serie` | `RPS` | `3` |
| file_name=protheus api, line_number=594 | `serie` | `RPS` | `3` |
| file_name=protheus api, line_number=595 | `serie` | `RPS` | `3` |
| file_name=protheus api, line_number=596 | `serie` | `RPS` | `3` |
| file_name=protheus api, line_number=597 | `serie` | `RPS` | `3` |
| file_name=protheus api, line_number=598 | `serie` | `RPS` | `3` |
| file_name=protheus api, line_number=599 | `serie` | `RPS` | `3` |
| file_name=protheus api, line_number=1 | `tipo_nota` | `502` | `520` |
| file_name=protheus api, line_number=2 | `tipo_nota` | `502` | `510` |
| file_name=protheus api, line_number=3 | `tipo_nota` | `502` | `510` |
| file_name=protheus api, line_number=4 | `tipo_nota` | `502` | `510` |
| file_name=protheus api, line_number=5 | `tipo_nota` | `502` | `510` |
| file_name=protheus api, line_number=6 | `tipo_nota` | `502` | `510` |
| file_name=protheus api, line_number=55 | `tipo_nota` | `520` | `502` |
| file_name=protheus api, line_number=56 | `tipo_nota` | `520` | `502` |
| file_name=protheus api, line_number=64 | `tipo_nota` | `520` | `502` |
| file_name=protheus api, line_number=65 | `tipo_nota` | `520` | `502` |
| file_name=protheus api, line_number=1 | `total` | `131.0` | `3249.0000` |
| file_name=protheus api, line_number=2 | `total` | `131.0` | `3249.0000` |
| file_name=protheus api, line_number=3 | `total` | `131.0` | `92.0000` |
| file_name=protheus api, line_number=4 | `total` | `131.0` | `520.0000` |
| file_name=protheus api, line_number=5 | `total` | `131.0` | `1667.0000` |
| file_name=protheus api, line_number=6 | `total` | `131.0` | `1440.0000` |
| file_name=protheus api, line_number=22 | `total` | `131.0` | `137.0000` |
| file_name=protheus api, line_number=38 | `total` | `1899.0` | `131.0000` |
| file_name=protheus api, line_number=39 | `total` | `1218.0` | `131.0000` |
| file_name=protheus api, line_number=40 | `total` | `2594.0` | `131.0000` |
| file_name=protheus api, line_number=1 | `unidade` | `Ibirapuera` | `Pro Fiv` |
| file_name=protheus api, line_number=2 | `unidade` | `Ibirapuera` | `Pro Fiv` |
| file_name=protheus api, line_number=3 | `unidade` | `Ibirapuera` | `Pro Fiv` |
| file_name=protheus api, line_number=4 | `unidade` | `Ibirapuera` | `Pro Fiv` |
| file_name=protheus api, line_number=5 | `unidade` | `Ibirapuera` | `Pro Fiv` |
| file_name=protheus api, line_number=6 | `unidade` | `Ibirapuera` | `Pro Fiv` |
| file_name=protheus api, line_number=7 | `unidade` | `Ibirapuera` | `Pro Fiv` |
| file_name=protheus api, line_number=8 | `unidade` | `Ibirapuera` | `Pro Fiv` |
| file_name=protheus api, line_number=9 | `unidade` | `Ibirapuera` | `Pro Fiv` |
| file_name=protheus api, line_number=10 | `unidade` | `Ibirapuera` | `Pro Fiv` |
| file_name=protheus api, line_number=1 | `vendedor_codigo` | `000000` | `000001` |
| file_name=protheus api, line_number=2 | `vendedor_codigo` | `000000` | `000001` |
| file_name=protheus api, line_number=3 | `vendedor_codigo` | `000000` | `000064` |
| file_name=protheus api, line_number=4 | `vendedor_codigo` | `000000` | `000124` |
| file_name=protheus api, line_number=5 | `vendedor_codigo` | `000000` | `000001` |
| file_name=protheus api, line_number=6 | `vendedor_codigo` | `000000` | `000001` |
| file_name=protheus api, line_number=38 | `vendedor_codigo` | `000007` | `000000` |
| file_name=protheus api, line_number=39 | `vendedor_codigo` | `000007` | `000000` |
| file_name=protheus api, line_number=40 | `vendedor_codigo` | `000007` | `000000` |
| file_name=protheus api, line_number=41 | `vendedor_codigo` | `000012` | `000000` |


---

## 3. Actionable Recommendations
1. **Investigate Athena Sync Lag**: For tables with mismatched row counts (such as `protheus_mesclada_vendas` or `redlara_planilha_combined`), check if the S3 promotion pipelines or Athena crawlers are running out of sync or ignoring deletions.
2. **Resolve Column Mappings**: Ensure schema promotion scripts explicitly map DuckDB fields to standard Athena lowercase naming conventions.
