# REDLARA Silver Layer Audit & Validation Report
**Generated**: `2026-09-11 13:53:33`  
**Database**: `huntington_data_lake.duckdb`  

---

## 📊 Executive Summary: 6 Silver Procedure Tables

| Procedure Stream | Silver Target Table | Rows | Transferred Cycles | Clinical Pregnancies | Clinical Pregnancy Rate (%) | Biochemical Pregnancies | Deliveries | Prontuário Matches (%) |
| :--- | :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **FRESH** | [`silver.redlara_fresh`](file:///G:\My Drive\projetos_individuais\Huntington\database\huntington_data_lake.duckdb) | **7,801** | 279 | **125** | **44.80%** | 20 | 76 | **95.32%** (7,436/7,801) |
| **FET** | [`silver.redlara_fet`](file:///G:\My Drive\projetos_individuais\Huntington\database\huntington_data_lake.duckdb) | **7,733** | 7,139 | **3,783** | **52.99%** | 398 | 2,666 | **98.66%** (7,629/7,733) |
| **FOT** | [`silver.redlara_fot`](file:///G:\My Drive\projetos_individuais\Huntington\database\huntington_data_lake.duckdb) | **2,020** | 928 | **588** | **63.36%** | 77 | 378 | **95.20%** (1,923/2,020) |
| **RECEP** | [`silver.redlara_recep`](file:///G:\My Drive\projetos_individuais\Huntington\database\huntington_data_lake.duckdb) | **761** | 26 | **20** | **76.92%** | 2 | 12 | **98.42%** (749/761) |
| **FP** | [`silver.redlara_fp`](file:///G:\My Drive\projetos_individuais\Huntington\database\huntington_data_lake.duckdb) | **3,303** | 0 | **0** | **0.00%** | 0 | 0 | **98.88%** (3,266/3,303) |
| **IUI** | [`silver.redlara_iui`](file:///G:\My Drive\projetos_individuais\Huntington\database\huntington_data_lake.duckdb) | **138** | 134 | **35** | **26.12%** | 4 | 30 | **99.28%** (137/138) |

**Total Consolidated Rows across Silver**: **21,756**  
**Overall Clinical Pregnancy Rate across All Transferred Cycles**: **53.50%** (4,551 / 8,506)  

---

## 🏥 Breakdown by Clinic (Unidade) and Year

### Procedure: FRESH (`silver.redlara_fresh`)

| Unidade | Year | Total Rows | Transferred Cycles | Clinical Pregnancies | CP Rate (%) | Prontuário Match (%) |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| Ibirapuera | 2021 | 1,160 | 53 | 30 | 56.6% | 99.66% |
| Ibirapuera | 2022 | 842 | 45 | 24 | 53.33% | 98.69% |
| Ibirapuera | 2023 | 879 | 38 | 14 | 36.84% | 99.89% |
| Ibirapuera | 2024 | 826 | 18 | 5 | 27.78% | 99.39% |
| Santa Joana | 2021 | 696 | 33 | 9 | 27.27% | 53.74% |
| Santa Joana | 2022 | 457 | 22 | 10 | 45.45% | 99.56% |
| Santa Joana | 2023 | 383 | 17 | 5 | 29.41% | 99.48% |
| Santa Joana | 2024 | 338 | 16 | 9 | 56.25% | 99.11% |
| Vila Mariana | 2021 | 751 | 16 | 7 | 43.75% | 100.0% |
| Vila Mariana | 2022 | 531 | 8 | 5 | 62.5% | 99.06% |
| Vila Mariana | 2023 | 491 | 9 | 4 | 44.44% | 98.78% |
| Vila Mariana | 2024 | 447 | 4 | 3 | 75.0% | 99.11% |

### Procedure: FET (`silver.redlara_fet`)

| Unidade | Year | Total Rows | Transferred Cycles | Clinical Pregnancies | CP Rate (%) | Prontuário Match (%) |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| Ibirapuera | 2021 | 971 | 912 | 491 | 53.84% | 99.69% |
| Ibirapuera | 2022 | 776 | 699 | 386 | 55.22% | 98.58% |
| Ibirapuera | 2023 | 884 | 825 | 446 | 54.06% | 99.55% |
| Ibirapuera | 2024 | 847 | 766 | 422 | 55.09% | 99.06% |
| Santa Joana | 2021 | 465 | 405 | 180 | 44.44% | 91.4% |
| Santa Joana | 2022 | 589 | 557 | 285 | 51.17% | 98.98% |
| Santa Joana | 2023 | 400 | 395 | 198 | 50.13% | 98.75% |
| Santa Joana | 2024 | 410 | 381 | 163 | 42.78% | 99.02% |
| Vila Mariana | 2021 | 641 | 594 | 301 | 50.67% | 100.0% |
| Vila Mariana | 2022 | 645 | 598 | 330 | 55.18% | 98.29% |
| Vila Mariana | 2023 | 601 | 559 | 321 | 57.42% | 98.84% |
| Vila Mariana | 2024 | 504 | 448 | 260 | 58.04% | 99.01% |

### Procedure: FOT (`silver.redlara_fot`)

| Unidade | Year | Total Rows | Transferred Cycles | Clinical Pregnancies | CP Rate (%) | Prontuário Match (%) |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| Ibirapuera | 2021 | 162 | 94 | 57 | 60.64% | 100.0% |
| Ibirapuera | 2022 | 179 | 85 | 70 | 82.35% | 99.44% |
| Ibirapuera | 2023 | 191 | 86 | 53 | 61.63% | 100.0% |
| Ibirapuera | 2024 | 183 | 69 | 45 | 65.22% | 56.83% |
| Santa Joana | 2021 | 125 | 52 | 29 | 55.77% | 96.8% |
| Santa Joana | 2022 | 133 | 59 | 29 | 49.15% | 99.25% |
| Santa Joana | 2023 | 82 | 25 | 11 | 44.0% | 98.78% |
| Santa Joana | 2024 | 81 | 29 | 16 | 55.17% | 100.0% |
| Vila Mariana | 2021 | 205 | 102 | 52 | 50.98% | 100.0% |
| Vila Mariana | 2022 | 216 | 106 | 69 | 65.09% | 98.15% |
| Vila Mariana | 2023 | 256 | 121 | 95 | 78.51% | 99.22% |
| Vila Mariana | 2024 | 207 | 100 | 62 | 62.0% | 97.58% |

### Procedure: RECEP (`silver.redlara_recep`)

| Unidade | Year | Total Rows | Transferred Cycles | Clinical Pregnancies | CP Rate (%) | Prontuário Match (%) |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| Ibirapuera | 2021 | 6 | 1 | 1 | 100.0% | 100.0% |
| Ibirapuera | 2022 | 11 | 2 | 1 | 50.0% | 100.0% |
| Ibirapuera | 2023 | 6 | 1 | 1 | 100.0% | 100.0% |
| Ibirapuera | 2024 | 4 | 0 | 0 | -% | 100.0% |
| Santa Joana | 2021 | 56 | 2 | 2 | 100.0% | 96.43% |
| Santa Joana | 2022 | 96 | 1 | 0 | 0.0% | 100.0% |
| Santa Joana | 2023 | 25 | 0 | 0 | -% | 96.0% |
| Santa Joana | 2024 | 27 | 2 | 1 | 50.0% | 100.0% |
| Vila Mariana | 2021 | 176 | 5 | 5 | 100.0% | 100.0% |
| Vila Mariana | 2022 | 109 | 2 | 0 | 0.0% | 97.25% |
| Vila Mariana | 2023 | 139 | 6 | 5 | 83.33% | 99.28% |
| Vila Mariana | 2024 | 106 | 4 | 4 | 100.0% | 95.28% |

### Procedure: FP (`silver.redlara_fp`)

| Unidade | Year | Total Rows | Transferred Cycles | Clinical Pregnancies | CP Rate (%) | Prontuário Match (%) |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| Ibirapuera | 2021 | 380 | 0 | 0 | -% | 99.74% |
| Ibirapuera | 2022 | 330 | 0 | 0 | -% | 97.58% |
| Ibirapuera | 2023 | 298 | 0 | 0 | -% | 99.66% |
| Ibirapuera | 2024 | 284 | 0 | 0 | -% | 98.94% |
| Santa Joana | 2021 | 107 | 0 | 0 | -% | 100.0% |
| Santa Joana | 2022 | 172 | 0 | 0 | -% | 100.0% |
| Santa Joana | 2023 | 135 | 0 | 0 | -% | 100.0% |
| Santa Joana | 2024 | 225 | 0 | 0 | -% | 100.0% |
| Vila Mariana | 2021 | 324 | 0 | 0 | -% | 99.38% |
| Vila Mariana | 2022 | 200 | 0 | 0 | -% | 99.0% |
| Vila Mariana | 2023 | 181 | 0 | 0 | -% | 99.45% |
| Vila Mariana | 2024 | 667 | 0 | 0 | -% | 97.15% |

### Procedure: IUI (`silver.redlara_iui`)

| Unidade | Year | Total Rows | Transferred Cycles | Clinical Pregnancies | CP Rate (%) | Prontuário Match (%) |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| Ibirapuera | 2021 | 36 | 34 | 13 | 38.24% | 100.0% |
| Ibirapuera | 2022 | 21 | 21 | 6 | 28.57% | 100.0% |
| Ibirapuera | 2023 | 31 | 30 | 9 | 30.0% | 100.0% |
| Ibirapuera | 2024 | 30 | 29 | 5 | 17.24% | 96.67% |
| Santa Joana | 2021 | 11 | 11 | 1 | 9.09% | 100.0% |
| Santa Joana | 2022 | 9 | 9 | 1 | 11.11% | 100.0% |

---

## 🛡️ Data Quality & Medical Consistency Assertions

| Check Description | Target Constraint | Result | Status |
| :--- | :--- | :---: | :---: |
| PK Uniqueness (`row_id`) in `silver.redlara_fresh` | 0 duplicates | 0 duplicates | ✅ PASS |
| PK Uniqueness (`row_id`) in `silver.redlara_fet` | 0 duplicates | 0 duplicates | ✅ PASS |
| PK Uniqueness (`row_id`) in `silver.redlara_fot` | 0 duplicates | 0 duplicates | ✅ PASS |
| PK Uniqueness (`row_id`) in `silver.redlara_recep` | 0 duplicates | 0 duplicates | ✅ PASS |
| PK Uniqueness (`row_id`) in `silver.redlara_fp` | 0 duplicates | 0 duplicates | ✅ PASS |
| PK Uniqueness (`row_id`) in `silver.redlara_iui` | 0 duplicates | 0 duplicates | ✅ PASS |
| Sac Consistency (`sacs >= 1` $\implies$ `cp = '1'`) in `silver.redlara_fresh` | 0 violations | 0 violations | ✅ PASS |
| Sac Consistency (`sacs >= 1` $\implies$ `cp = '1'`) in `silver.redlara_fet` | 0 violations | 0 violations | ✅ PASS |
| Sac Consistency (`sacs >= 1` $\implies$ `cp = '1'`) in `silver.redlara_fot` | 0 violations | 0 violations | ✅ PASS |
| Sac Consistency (`sacs >= 1` $\implies$ `cp = '1'`) in `silver.redlara_recep` | 0 violations | 0 violations | ✅ PASS |
| Sac Consistency (`sacs >= 1` $\implies$ `cp = '1'`) in `silver.redlara_iui` | 0 violations | 0 violations | ✅ PASS |
| Newborn Consistency (`newborns >= 1` $\implies$ `cp = '1'`) in `silver.redlara_fresh` | 0 violations | 0 violations | ✅ PASS |
| Newborn Consistency (`newborns >= 1` $\implies$ `cp = '1'`) in `silver.redlara_fet` | 0 violations | 0 violations | ✅ PASS |
| Newborn Consistency (`newborns >= 1` $\implies$ `cp = '1'`) in `silver.redlara_fot` | 0 violations | 0 violations | ✅ PASS |
| Newborn Consistency (`newborns >= 1` $\implies$ `cp = '1'`) in `silver.redlara_recep` | 0 violations | 0 violations | ✅ PASS |
| Newborn Consistency (`newborns >= 1` $\implies$ `cp = '1'`) in `silver.redlara_iui` | 0 violations | 0 violations | ✅ PASS |