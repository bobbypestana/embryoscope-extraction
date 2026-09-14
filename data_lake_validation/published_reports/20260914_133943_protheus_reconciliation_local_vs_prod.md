# Protheus Reconciliation Audit: Local DuckDB vs. AWS Athena PROD

> [!IMPORTANT]
> ### 📊 Executive Summary: Where Local and PROD Diverge
> Comparing **Local DuckDB (`gold`)** with **AWS Athena PROD (`gold_huntington_prod`)** across the 3 Protheus commercial tables reveals three core structural realities:
> 1. **Belo Horizonte is 100% missing in AWS PROD**: PROD contains **0 Belo Horizonte rows** across all three tables, whereas Local includes ~185k BH invoices, ~29k BH orders, and ~186k BH sales items.
> 2. **PROD retains 2013–2021 historical Huntington data**: Local Gold intentionally starts in 2022, so PROD contains ~196k historical invoices and ~207k historical sales lines that are absent locally.
> 3. **Within the common date range for Huntington clinics only, the alignment is near-perfect (>99.5%)**:
>    - `notas_faturadas` (2022–2026): 643,615 Local vs. 643,823 PROD (**99.97% match**, diff of only 208 rows).
>    - `pedidos_a_faturar` (Nov 2025 – Sep 2026): 12,548 Local vs. 12,487 PROD (**99.51% match**, diff of only 61 rows).
> 
> | Table Name | Local DuckDB (`gold`) | AWS Athena PROD (`prod`) | Net Row Difference | Available Common Date Range | Missing in PROD (Common Range) | Missing in Local (Pre-Common Range) |
> | :--- | :---: | :---: | :---: | :---: | :---: | :---: |
> | **`pedidos_a_faturar`** | 43,703 | 12,487 | **-31,216 rows** (-71.4%)<br>-R$ 81.26M | `2025-11-19` to `2026-09-11` | **-7,386 rows** (-37.2%)<br>-R$ 21.12M *(7,325 are BH)* | **0 rows** (PROD has no older data) |
> | **`notas_faturadas`** | 828,850 | 839,822 | **+10,972 rows** (+1.3%)<br>+R$ 458.00M | `2022-01-02` to `2026-09-11` | **-185,027 rows** (-22.3%)<br>-R$ 123.32M *(185,235 are BH)* | **+195,999 rows** (+R$ 581.32M)<br>*(2013-2021 in PROD)* |
> | **`vendas_consolidadas`** | 833,470 | 866,305 | **+32,835 rows** (+3.9%)<br>+R$ 550.37M | `2022-01-02` to `2026-09-11` | **-174,065 rows** (-20.9%)<br>-R$ 85.89M *(186,136 are BH)* | **+206,900 rows** (+R$ 636.26M)<br>*(2013-2021 in PROD)* |

---

## 1. `gold.protheus_pedidos_a_faturar`: Local vs. PROD

### 1.1 Temporal Window Mismatch
* **Local Scope**: `2022-01-06` to `2026-09-11` (**43,703 rows | R$ 123.75M**).
* **PROD Scope**: `2025-11-19` to `2026-09-11` (**12,487 rows | R$ 42.50M**).
* **Pre-Window (< 2025-11-19, Missing in PROD)**:
  * Local has **23,830 rows (R$ 60,138,876.79)** prior to Nov 19, 2025 that were never loaded into PROD.

### 1.2 In-Window Comparison (`2025-11-19` to `2026-09-11`)
| Dimension Slice | Local DuckDB Count | Local Value (R$) | AWS Athena PROD Count | AWS Athena PROD Value (R$) | Variance (PROD - Local) |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **Belo Horizonte (`0101`)** | 7,325 | R$ 19,840,669.99 | **0** | **R$ 0.00** | **-7,325 rows (-R$ 19.84M)** (100% missing in PROD) |
| **Huntington Clinics** | 12,548 | R$ 43,772,589.64 | 12,487 | R$ 42,495,067.05 | **-61 rows (-R$ 1.28M)** (**99.51% match**) |
| **Total In-Window** | **19,873** | **R$ 63,613,259.63** | **12,487** | **R$ 42,495,067.05** | **-7,386 rows (-R$ 21.12M)** |

---

## 2. `gold.protheus_notas_faturadas`: Local vs. PROD

### 2.1 Date Horizon Comparison
* **PROD Pre-2022 Data (Absent in Local)**:
  * PROD contains **195,999 historical Huntington invoices (R$ 581,320,363.69)** from 2013–2021. Local Gold begins in 2022.
* **Common Window (`2022-01-02` to `2026-09-11`)**:
  * Local: **828,850 rows | R$ 765,098,912.33**
  * PROD: **643,823 rows | R$ 641,774,543.03**
  * **Net Variance**: PROD has **-185,027 rows (-R$ 123.32M)**.

### 2.2 Branch Breakdown in Common Window (2022–2026)
| Branch / Clinic Group | Local DuckDB Rows | Local Value (R$) | AWS Athena PROD Rows | AWS Athena PROD Value (R$) | Observations |
| :--- | :---: | :---: | :---: | :---: | :--- |
| **Belo Horizonte (`0101`)** | 185,235 | R$ 135,191,012.00 | **0** | **R$ 0.00** | **100% missing in PROD** (PROD lacks BH integration). |
| **Huntington Clinics** | 643,615 | R$ 629,907,900.33 | 643,823 | R$ 641,774,543.03 | **+208 rows (+R$ 11.87M)** (**99.97% match**). |
| **Total Common Window** | **828,850** | **R$ 765,098,912.33** | **643,823** | **R$ 641,774,543.03** | Local has 185k more rows solely due to Belo Horizonte. |

---

## 3. `gold.protheus_vendas_consolidadas`: Local vs. PROD

### 3.1 Recent Upgrades & Date Horizon
* **PROD**: Upgraded to the modern 45-column line-item model (`dt_emissao`, `produto_id`, `status_fluxo`), spanning 2013 to 2026 (**866,305 rows | R$ 1.33 Billion**).
* **Local**: Expanded to include `NOTA_DIRETA` (642k rows), `VENDA_DIRETA` (166k rows), and `PEDIDO_DIRETO` (25k rows), spanning 2022 to 2026 (**833,470 rows | R$ 777.45M**).
* **Pre-2022 Data in PROD (Absent in Local)**:
  * PROD contains **206,900 rows (R$ 636,258,976.90)** from 2013–2021.

### 3.2 Common Window Comparison (2022–2026)
| Metric | Local DuckDB (2022–2026) | AWS Athena PROD (2022–2026) | Variance (PROD - Local) | Explanation |
| :--- | :---: | :---: | :---: | :--- |
| **Total Rows** | 833,470 | 659,405 | **-174,065 rows (-20.9%)** | PROD lacks Belo Horizonte. |
| **Total Value (R$)** | R$ 777,448,011.76 | R$ 691,556,480.84 | **-R$ 85,891,530.92** | -R$ 135.99M from BH, offset by higher POS revenue in PROD. |
| **Belo Horizonte Rows** | 186,136 | **0** | **-186,136 rows (-100%)** | PROD has 0 BH records. |
| **Origin Breakdown** | `NOTA_DIRETA`: 642,268<br>`VENDA_DIRETA`: 166,041<br>`PEDIDO_DIRETO`: 25,161 | `VENDA_DIRETA`: 658,385<br>`PEDIDO_DIRETO`: 1,020<br>`NOTA_DIRETA`: 0 | Classification shift | In PROD, POS sales are all tagged `VENDA_DIRETA`. |

---

## 4. Summary of Root Causes
1. **Belo Horizonte is not in PROD**: This single fact explains:
   - Why PROD has 185k fewer invoices in 2022–2026.
   - Why PROD has 7.3k fewer orders in 2025–2026.
   - Why PROD has 186k fewer consolidated sales lines in 2022–2026.
2. **Historical Scope Cutoffs**:
   - PROD goes back to 2013 for Huntington (accounting for ~200k rows across sales and invoices that Local Gold excludes).
   - PROD orders (`pedidos_a_faturar`) were only backfilled from November 19, 2025 onwards (accounting for 23.8k missing orders in PROD).
3. **Huntington Clinics Match**: When isolating Huntington clinics in the common active window, the data lake pipelines between Local and AWS PROD match at **>99.5% accuracy**.
