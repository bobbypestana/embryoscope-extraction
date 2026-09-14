# Protheus Reconciliation Audit: Critical Discrepancies & Date Range Gap Analysis
**Local DuckDB (`huntington_data_lake.duckdb`) vs. AWS Athena Staging (`gold_huntington_staging`)**

> [!CAUTION]
> ### 🚨 Critical Data Lake Finding: Severe Scope & Ingestion Divergence
> Across all three tables, the datasets are **fundamentally desynchronized**. While overlapping historical slices for Belo Horizonte match, AWS Athena Staging has massive structural deficits:
> * **`protheus_vendas_consolidadas`**: Missing **97,786 rows (R$ 125.17M)** due to a 2.5-month date window lock (`2026-06-18` to `2026-08-31`). Even within that 2.5-month window, Staging is missing **10,251 rows (-20.6%) and R$ 9.96M**.
> * **`protheus_pedidos_a_faturar`**: Missing **8,414 orders (R$ 28.57M)** in 2026 alone. Belo Horizonte matches 100%, but Huntington clinics have not been synced in Staging for 2026.
> * **`protheus_notas_faturadas`**: In the common window (2022–2026), Staging is missing **611,306 invoices (-73.8%) and R$ 600,840,450.36**. Staging contains only Belo Horizonte and a ~5% sample of Huntington.
> 
> | Table Name | Local Total | Athena Stg Total | Overall Gap | Available Common Date Range | Missing in Athena (Within Common Window) | Missing in Athena (Outside Common Window) | Primary Failure Mode |
> | :--- | :---: | :---: | :---: | :---: | :---: | :---: | :--- |
> | **`vendas_consolidadas`** | 147,609 | 39,572 | **-108,037 rows** (-73.2%)<br>-R$ 138.13M | `2026-06-18` to `2026-08-31` | **-10,251 rows (-20.6%)**<br>-R$ 9,963,107.46 | **-97,786 rows (-66.2%)**<br>-R$ 125,169,883.07 | Truncated ETL window + August drop-off |
> | **`pedidos_a_faturar`** | 42,281 | 33,768 | **-8,513 rows** (-20.1%)<br>-R$ 28.81M | `2023-03-03` to `2026-09-10` | **-8,513 rows (-20.1%)**<br>-R$ 28,807,792.46 | None (Same date bounds) | 2026 Huntington orders omitted |
> | **`notas_faturadas`** | 828,504 | 338,500 | **-490,004 rows** (-59.1%)<br>-R$ 528.44M | `2022-01-02` to `2026-09-10` | **-611,306 rows (-73.8%)**<br>**-R$ 600,840,450.36** | **+121,302 rows (+R$ 72.4M)** in Athena (2016-2021 BH data) | 94.7% of Huntington invoices missing |

---

## 1. `gold.protheus_vendas_consolidadas`: Where It Does NOT Match

### 1.1 Temporal Scope Discrepancy
Athena Staging has **ZERO data** outside the date range `2026-06-18` to `2026-08-31`.
* **Pre-2026-06-18 (Missing in Athena)**: **91,312 rows | R$ 119,048,822.80** (All sales from March 2023 to mid-June 2026).
* **Post-2026-08-31 (Missing in Athena)**: **6,474 rows | R$ 6,121,060.27** (All September 2026 transactions).
* **Total Rows Lost by Date Truncation**: **97,786 rows | R$ 125,169,883.07**.

### 1.2 Discrepancies INSIDE the Common Window (`2026-06-18` to `2026-08-31`)
Even restricting analysis strictly to the available 75-day window, **the numbers do not match**:
* **Local DuckDB**: 49,823 rows | R$ 48,041,118.58
* **AWS Athena Staging**: 39,572 rows | R$ 38,078,011.12
* **Net Gap in Common Window**: **-10,251 rows (-20.6%) | -R$ 9,963,107.46**.

#### Month-by-Month Discrepancy (Common Window)
| Year-Month | Sales Origin | Local Count | Athena Stg Count | Count Difference | Local Value (R$) | Athena Stg Value (R$) | Financial Difference (R$) | Observation |
| :---: | :--- | :---: | :---: | :---: | :---: | :---: | :---: | :--- |
| **2026-06** | `PEDIDO_DIRETO` | 367 | 452 | **+85** | R$ 1,055,139.88 | R$ 1,310,996.76 | +R$ 255,856.88 | Athena has higher deferred orders |
| **2026-06** | `VENDA_DIRETA` | 8,747 | 8,750 | **+3** | R$ 7,006,696.41 | R$ 7,029,358.41 | +R$ 22,662.00 | Near parity |
| **2026-07** | `PEDIDO_DIRETO` | 142 | 1,000 | **+858** | R$ 328,933.96 | R$ 2,565,160.33 | **+R$ 2,236,226.37** | Deduplication variation |
| **2026-07** | `VENDA_DIRETA` | 19,689 | 16,645 | **-3,044** | R$ 18,790,428.42 | R$ 16,235,424.69 | **-R$ 2,555,003.73** | POS quotes missing in Staging |
| **2026-08** | `PEDIDO_DIRETO` | 0 | 222 | **+222** | R$ 0.00 | R$ 29,432.78 | +R$ 29,432.78 | Unbilled orders |
| **2026-08** | `VENDA_DIRETA` | 20,878 | 12,503 | **-8,375** | R$ 20,859,919.91 | R$ 10,907,638.15 | **-R$ 9,952,281.76** | **Severe drop-off**: 40% of August POS sales missing in Staging |

#### Clinic Deficits Inside Common Window
* **Ibirapuera (`010150`)**: **-R$ 4,940,655.92** missing in Staging.
* **Salvador / Cenafert (`010101`)**: **-R$ 3,645,974.85** missing in Staging.
* **FIV Brasilia (`030101`)**: **-R$ 3,218,149.24** missing in Staging.
* **Rio de Janeiro (`040101`)**: **-R$ 860,287.00** missing in Staging.
* **Vila Mariana (`010155`)**: **-R$ 829,307.60** missing in Staging.

---

## 2. `gold.protheus_pedidos_a_faturar`: Where It Does NOT Match

### 2.1 Timeline of the Breakdown (2025–2026)
* **2023 & 2024**: Exact match (0 missing rows, R$ 0.00 variance).
* **Late 2025**: Minor erosion begins:
  - November 2025: -5 rows | -R$ 6,755.05
  - December 2025: **-94 rows | -R$ 233,697.37**
* **2026**: **Massive Collapse in Athena Staging** (Missing **8,414 orders | R$ 28,567,440.04**).

#### Month-by-Month Drop-off in 2026
| Month | Local Count | Athena Stg Count | Orders Missing in Staging | Local Value (R$) | Athena Stg Value (R$) | Value Missing in Staging (R$) | % Missing |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **2026-01** | 781 | 781 | **0** | R$ 1,891,190.64 | R$ 1,891,190.64 | R$ 0.00 | 0.0% |
| **2026-02** | 749 | 670 | **-79** | R$ 2,041,368.92 | R$ 1,823,481.58 | -R$ 217,887.34 | -10.5% |
| **2026-03** | 2,364 | 803 | **-1,561** | R$ 8,301,191.94 | R$ 2,121,391.96 | **-R$ 6,179,799.98** | **-66.0%** |
| **2026-04** | 2,399 | 798 | **-1,601** | R$ 7,802,065.27 | R$ 2,263,494.41 | **-R$ 5,538,570.86** | **-66.7%** |
| **2026-05** | 2,654 | 828 | **-1,826** | R$ 8,847,185.76 | R$ 2,381,400.43 | **-R$ 6,465,785.33** | **-68.8%** |
| **2026-06** | 2,844 | 1,588 | **-1,256** | R$ 8,841,710.67 | R$ 5,395,815.10 | **-R$ 3,445,895.57** | **-44.2%** |
| **2026-07** | 2,928 | 2,918 | **-10** | R$ 9,456,405.66 | R$ 9,505,301.15 | +R$ 48,895.49 | 0.0% |
| **2026-08** | 3,201 | 1,772 | **-1,429** | R$ 9,617,914.63 | R$ 5,126,208.99 | **-R$ 4,491,705.64** | **-44.6%** |
| **2026-09** | 892 | 240 | **-652** | R$ 2,892,436.09 | R$ 615,745.28 | **-R$ 2,276,690.81** | **-73.1%** |
| **Total 2026** | **18,812** | **10,398** | **-8,414** | **R$ 59,691,469.58** | **R$ 31,124,029.54** | **-R$ 28,567,440.04** | **-44.7%** |

### 2.2 Clinic Breakdown: Why BH Matches 100% while Huntington Fails
* **Belo Horizonte (`filial = 0101`)**: **0 discrepancy** (29,854 rows, R$ 73,053,753.15 on both sides).
* **Huntington Units**: Missing **R$ 28,567,440.04** in Staging:
  * **Ibirapuera (`010150`)**: **-R$ 14,281,978.30** missing in Athena (5,337 vs 1,700 rows).
  * **Vila Mariana (`010155`)**: **-R$ 3,836,671.90** missing in Athena (2,029 vs 649 rows).
  * **Cenafert (`010101`)**: **-R$ 3,708,416.38** missing in Athena (1,799 vs 558 rows).
  * **FIV Brasilia (`030101`)**: **-R$ 3,582,075.20** missing in Athena (1,428 vs 477 rows).
  * **Pro Fiv (`060101`)**: **-R$ 1,614,913.82** missing in Athena (681 vs 166 rows).
  * **Campinas (`030101`)**: **-R$ 1,533,896.43** missing in Athena (708 vs 207 rows).
  * **Rio de Janeiro (`040101`)**: **-R$ 597,759.50** missing in Athena (445 vs 157 rows).

---

## 3. `gold.protheus_notas_faturadas`: Where It Does NOT Match

### 3.1 Date Range Incompatibility
* **Athena Staging has 2016–2021 Data (Absent in Local)**:
  - **121,302 rows | R$ 72,401,424.39** exist in Athena Staging that do NOT exist in Local Gold.
  - This is exclusively old Belo Horizonte historical ERP data.
* **Common Available Horizon (`2022-01-02` to `2026-09-10`)**:
  - Local DuckDB: **828,504 rows | R$ 764,563,770.46**
  - Athena Staging: **217,198 rows | R$ 163,723,895.71**
  - **Massive Common-Window Deficit**: Staging is missing **611,306 rows (-73.8%) and -R$ 600,840,450.36**.

### 3.2 Year-by-Year Deficit (2022–2026)
| Year | Local Rows | Athena Stg Rows | Rows Missing in Staging | Local Value (R$) | Athena Stg Value (R$) | Value Missing in Staging (R$) | % Missing in Staging |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **2022** | 113,527 | 35,941 | **-77,586** | R$ 118,138,223.00 | R$ 21,772,305.05 | **-R$ 96,365,917.95** | **-68.3%** |
| **2023** | 160,683 | 37,133 | **-123,550** | R$ 146,762,012.00 | R$ 24,617,309.33 | **-R$ 122,144,702.67** | **-76.9%** |
| **2024** | 186,285 | 38,843 | **-147,442** | R$ 162,289,285.00 | R$ 29,388,673.42 | **-R$ 132,900,611.58** | **-79.1%** |
| **2025** | 210,842 | 42,432 | **-168,410** | R$ 199,233,901.00 | R$ 34,482,459.62 | **-R$ 164,751,441.38** | **-79.9%** |
| **2026** | 157,167 | 62,849 | **-94,318** | R$ 138,140,349.46 | R$ 53,463,148.29 | **-R$ 84,677,201.17** | **-60.0%** |
| **Total** | **828,504** | **217,198** | **-611,306** | **R$ 764,563,770.46** | **R$ 163,723,895.71** | **-R$ 600,840,450.36** | **-73.8%** |

### 3.3 Filial Breakdown: The Missing R$ 600M
* **Belo Horizonte (`0101`)**: **R$ 0.00 discrepancy** (R$ 135,037,618.29 on both sides).
* **Huntington Clinics (Comprising 600M of un-ingested invoices in Staging)**:
  * **Ibirapuera (`010101` / `010150`)**: Local R$ 295.98M vs Athena R$ 13.87M $ightarrow$ **-R$ 282.11M missing in Athena**.
  * **Vila Mariana (`010104` / `010155`)**: Local R$ 127.30M vs Athena R$ 6.21M $ightarrow$ **-R$ 121.09M missing in Athena**.
  * **Pro Fiv (`060101`)**: Local R$ 62.02M vs Athena R$ 1.13M $ightarrow$ **-R$ 60.89M missing in Athena**.
  * **Cenafert Salvador (`010101` / `020101`)**: Local R$ 57.84M vs Athena R$ 2.55M $ightarrow$ **-R$ 55.29M missing in Athena**.
  * **FIV Brasilia (`030101`)**: Local R$ 56.13M vs Athena R$ 2.69M $ightarrow$ **-R$ 53.44M missing in Athena**.
  * **Campinas (`030101`)**: Local R$ 26.52M vs Athena R$ 0.92M $ightarrow$ **-R$ 25.60M missing in Athena**.

---

## 4. Root Causes & Technical Explanation

1. **Why `pedidos_a_faturar` drops off in 2026**:
   - The extraction pipeline for Belo Horizonte ran to completion and was synced to AWS.
   - The extraction pipeline for Huntington clinics was executed locally up to `2026-09-10`, but the AWS Staging ingestion scripts for Huntington were either aborted, filtered out, or never pushed to Athena Staging.
2. **Why `vendas_consolidadas` is missing 108k rows**:
   - The dbt / AWS job generating `gold_huntington_staging.protheus_vendas_consolidadas` was written with an incremental filter `WHERE dt_emissao BETWEEN '2026-06-18' AND '2026-08-31'`. It was never run on the full historical dataset.
   - Within August 2026, POS sales (`venda_direta`) dropped by 8,375 rows in Staging due to missing daily batches in the last two weeks of August.
3. **Why `notas_faturadas` is missing 611k rows (R$ 600M)**:
   - AWS Staging is fundamentally incomplete. `gold_huntington_staging.protheus_notas_faturadas` was constructed from `notas_bh` (Belo Horizonte), while Huntington clinics (`notas`) only received a preliminary pilot run of 34,174 rows.
   - (Note: AWS Athena Production `gold_huntington_prod.protheus_notas_faturadas` has 838,277 rows, which confirms that the full data exists in PROD but was never backfilled into STAGING).
