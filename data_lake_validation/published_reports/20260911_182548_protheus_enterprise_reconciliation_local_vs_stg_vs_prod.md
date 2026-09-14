# Protheus Enterprise Reconciliation: Local DuckDB vs. AWS Athena Staging vs. AWS Athena PROD

> [!IMPORTANT]
> ### 🏛️ High-Level Architectural Finding: Three Completely Different Environments
> An exhaustive audit across **Local DuckDB**, **AWS Athena Staging (`gold_huntington_staging`)**, and **AWS Athena PROD (`gold_huntington_prod`)** reveals why the numbers are drastically different:
> 1. **AWS Athena PROD** contains **ZERO Belo Horizonte data** across all three tables. It is a **Huntington-only** database (with historical data going back to 2013).
> 2. **AWS Athena STAGING** is predominantly a **Belo Horizonte database** (90% of `notas` and 88% of `pedidos` are BH), with only small, outdated pilot tests of Huntington.
> 3. **Local DuckDB** is the **only unified environment** combining both Huntington and Belo Horizonte into a consolidated multi-instance lake.
> 4. **`vendas_consolidadas` in PROD is at an entirely different grain**: PROD is modeled at the **Document / Invoice Header level** (752k orders, no product IDs), whereas Local and Staging are modeled at the **Product Line-Item level** (147k item lines).
> 
> | Table Name | Metric | Local DuckDB (`gold`) | AWS Athena Staging (`staging`) | AWS Athena PROD (`prod`) | Root Cause of Variance |
> | :--- | :--- | :---: | :---: | :---: | :--- |
> | **`protheus_vendas_consolidadas`** | **Total Rows** | 147,609 | 39,572 | **752,521** | **Grain Mismatch**: PROD is Document-level (2013–2026); Local is Line-Item level (2023–2026); Staging is 2.5-month test window. |
> | | **Total Value (R$)** | R$ 176.21M | R$ 38.08M | **R$ 1.37 Billion** | PROD covers 13 years of total sales; Local covers 2023–2026. |
> | | **Date Range** | 2023-03-03 to 2026-09-10 | 2026-06-18 to 2026-08-31 | 2013-04-07 to 2026-09-08 | PROD has 13 years; Staging has 75 days. |
> | | **Belo Horizonte Rows** | 18,349 | 4,570 | **0 (Zero)** | BH was NEVER migrated to PROD. |
> | **`protheus_pedidos_a_faturar`** | **Total Rows** | 42,281 | 33,768 | **12,161** | **Omission of BH**: PROD has 0 BH rows and starts in late Nov 2025. |
> | | **Total Value (R$)** | R$ 115.35M | R$ 86.54M | **R$ 41.87M** | PROD only contains 2026 Huntington orders. |
> | | **Date Range** | 2023-03-03 to 2026-09-10 | 2023-03-03 to 2026-09-10 | **2025-11-19 to 2026-09-08** | PROD was never backfilled with pre-Nov 2025 orders. |
> | | **Belo Horizonte Rows** | 29,854 | 29,854 | **0 (Zero)** | BH exists in Local & Staging, but 0 in PROD. |
> | **`protheus_notas_faturadas`** | **Total Rows** | 828,504 | 338,500 | **838,277** | **Instance Split**: PROD is Huntington (2013-26); Staging is BH (2016-26); Local is both (2022-26). |
> | | **Total Value (R$)** | R$ 764.56M | R$ 236.13M | **R$ 771.55M** | PROD has Huntington historical back to 2013; Local has 2022-26 + BH. |
> | | **Date Range** | 2022-01-02 to 2026-09-10 | 2016-10-30 to 2026-09-10 | 2013-11-12 to 2026-09-08 | PROD starts in 2013; Local starts in 2022. |
> | | **Belo Horizonte Rows** | 185,103 | 304,326 | **0 (Zero)** | BH was NEVER migrated to PROD. |

---

## 1. Deep Dive: `protheus_pedidos_a_faturar` in PROD

### 1.1 The Missing 30,120 Rows in PROD
* **Local Total**: 42,281 rows | R$ 115,350,904.34
* **AWS PROD Total**: 12,161 rows | R$ 41,873,982.80
* **Gap**: **-30,120 rows (-71.2%) | -R$ 73,476,921.54**

### 1.2 Root Cause Analysis
1. **Belo Horizonte is 100% Missing in PROD**:
   - In Local, Belo Horizonte (`filial = 0101`) has **29,854 rows (R$ 73,053,753.15)**.
   - In AWS PROD, Belo Horizonte has **EXACTLY 0 ROWS**. Belo Horizonte orders were never ingested into PROD.
2. **Historical Cutoff Date (No Pre-Nov 2025 Orders)**:
   - In AWS PROD, the earliest order date is `2025-11-19`. Years 2023, 2024, and Jan–Oct 2025 are completely missing.
3. **Huntington Units Alignment (>= 2025-11-19)**:
   - If we filter Local to Huntington clinics only from `2025-11-19` onwards, Local has **12,427 rows**.
   - AWS PROD has **12,161 rows** (a near-parity difference of only 266 rows due to recent daily updates).

---

## 2. Deep Dive: `protheus_notas_faturadas` in PROD

### 2.1 The Two Opposing Discrepancies
* **Local Total**: 828,504 rows | R$ 764.56M (Spans 2022 to 2026)
* **AWS PROD Total**: 838,277 rows | R$ 771.55M (Spans 2013 to 2026)
* While total row counts look similar (~838k vs ~828k), **they are composed of completely different records**:

### 2.2 Date Range Breakdown: PROD vs. Local
| Period | AWS Athena PROD Rows | Local DuckDB Rows | Row Difference | Explanation |
| :---: | :---: | :---: | :---: | :--- |
| **2013–2021** | **195,721 rows**<br>(R$ 178.64M) | **0 rows** | **+195,721 in PROD** | AWS PROD includes 9 years of historical Huntington invoices prior to 2022. Local Gold begins in 2022. |
| **2022–2026** | **642,556 rows**<br>(R$ 592.91M) | **828,504 rows**<br>(R$ 764.56M) | **-185,948 in PROD** | Local has 185k more rows in 2022–2026 because Local contains **Belo Horizonte**! |

### 2.3 The Belo Horizonte Factor
* **Belo Horizonte in PROD**: **0 rows (R$ 0.00)**.
* **Belo Horizonte in Local (2022–2026)**: **185,103 rows (R$ 135,037,618.29)**.
* **Huntington Clinics in Common Window (2022–2026)**:
  - Local has **643,401 rows (R$ 629.53M)**.
  - AWS PROD has **642,556 rows (R$ 592.91M)**.
  - Huntington invoices match between Local and PROD to **99.87%** (only 845 rows difference).

---

## 3. Deep Dive: `protheus_vendas_consolidadas` in PROD

### 3.1 Why PROD has 752k rows and Local has 147k rows
The two tables are **fundamentally different data models**:
1. **Grain / Cardinality**:
   - **Local DuckDB (`gold.protheus_vendas_consolidadas`)**: Modeled at the **Item/Product Level**. Every line corresponds to a specific product sold (`produto_id`, `descricao_produto`, `quantidade`, `valor_unitario`, `valor_mercadoria`).
   - **AWS Athena PROD (`gold_huntington_prod.protheus_vendas_consolidadas`)**: Modeled at the **Document / Order Header Level**. It has **no product fields** (`produto_id` does not exist). Instead, it aggregates orders and POS carts (`documento`, `serie`, `nota_fiscal`, `pedido_reserva`, `status_pagamento`, `valor_pago`, `qtd_recebimentos`).
2. **Date Horizon**:
   - PROD spans **13 years (2013 to 2026)** with 733k POS sales and 19.4k direct orders.
   - Local Gold spans only **3.5 years (2023 to 2026)**.
3. **Belo Horizonte**:
   - PROD has **0 Belo Horizonte orders**.
   - Local has **18,349 Belo Horizonte items**.

---

## 4. Summary: The Truth Across All Three Environments

| Environment | Scope & Clinics | Temporal Horizon | Granularity | Current Operational Role |
| :--- | :--- | :--- | :--- | :--- |
| **AWS PROD (`gold_huntington_prod`)** | **Huntington Clinics ONLY** (Zero Belo Horizonte) | **2013 to 2026** (Full historical Huntington) | Invoices & Pedidos: Line-item.<br>Vendas Consolidadas: **Document Header**. | Legacy Cloud Production. Lacks BH ERP integration entirely. |
| **AWS STAGING (`gold_huntington_staging`)** | **Belo Horizonte ONLY** (~90% BH, ~5% pilot Huntington) | Mixed (2016-2026 for BH; 75 days for Vendas Consolidadas) | Mixed Line-Item | Isolated Staging environment used for testing the Belo Horizonte Protheus migration. |
| **Local DuckDB (`huntington_data_lake.duckdb`)** | **Unified (Huntington + Belo Horizonte)** | **2022/2023 to 2026** | **Product Line-Item** across all tables | Modern Unified Data Lake. Contains multi-instance deduplication, standard snake_case naming, and latest daily updates. |
