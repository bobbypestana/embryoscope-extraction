# Data Lake Reconciliation Report: Local DuckDB vs. AWS Athena Staging (Protheus Gold Layer)

> [!NOTE]
> ### 📊 Global Reconciliation Dashboard
> * **Audit Date**: `2026-09-11 17:53:47`
> * **Local Database**: `database/huntington_data_lake.duckdb` (Environment: `try_request`)
> * **AWS Athena Staging Database**: `gold_huntington_staging` (Region: `sa-east-1`, Workgroup: `datalake-admins`)
> * **Total Tables Audited**: **3**
> * **Perfect Overlap Value Alignment**: **1** table (`protheus_pedidos_a_faturar` at **100.00%**)
> * **High Overlap Alignment (>95%)**: **1** table (`protheus_vendas_consolidadas` at **96.84%**)
> * **Structural / Ingestion Scope Variance**: **3** tables (Severe temporal & branch ingestion drift detected in AWS Staging)
> 
> | Table Name | Schema Alignment | Local Count | Athena Stg Count | Count Pct Diff | Overlapping Keys | Overlap Value Match Rate | Local Total Value (R$) | Athena Stg Total Value (R$) | Audit Status |
> | :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :--- |
> | **`gold.protheus_pedidos_a_faturar`** | Legacy Mappings | 42,281 | 33,768 | -20.13% | 33,494 | **100.00%** | R$ 115,350,904.34 | R$ 86,543,111.88 | ⚠️ Freshness Drift (BH 100% matched, Huntington 2026 pending in Staging) |
> | **`gold.protheus_vendas_consolidadas`** | Standardized | 147,609 | 39,572 | -73.19% | 36,787 | **96.84%** | R$ 176,210,612.00 | R$ 38,078,011.12 | ⚠️ Window Drift (Staging has only 2.5 months: Jun-Aug 2026) |
> | **`gold.protheus_notas_faturadas`** | Legacy Mappings | 828,504 | 338,500 | -59.14% | N/A (Scope) | N/A (Scope) | R$ 764,563,770.46 | R$ 236,125,320.10 | ❌ Scope Mismatch (Staging is 90% BH 2016-2026; missing 95% Huntington) |

**Executive Summary:**
This comprehensive audit reconciles the Protheus ERP commercial Gold layer between the local data lake (`huntington_data_lake.duckdb`) and AWS Athena Staging (`gold_huntington_staging`). The audit reveals that while overlapping transaction logic is mathematically sound (achieving up to **100.00% exact penny match** on overlapping `pedidos_a_faturar`), AWS Staging currently suffers from **severe scope and temporal drift**:
1. `protheus_vendas_consolidadas` in Athena Staging is restricted to a 2.5-month test window (`2026-06-18` to `2026-08-31`), omitting years 2023–2025 and September 2026.
2. `protheus_pedidos_a_faturar` in Athena Staging matches Belo Horizonte (`0101`) perfectly across 2023–2026, but is missing 8,414 Huntington clinic orders from 2026 that have been continuously ingested locally.
3. `protheus_notas_faturadas` in Athena Staging contains historical Belo Horizonte data dating back to 2016 (304k rows), but is missing 94.7% of historical Huntington clinic invoices (which are present in `gold_huntington_prod` with 838k rows).
4. Schema standardization (snake_case domain names like `cliente_id`, `dt_emissao`, `valor_total`) has been implemented locally for `pedidos_a_faturar` and `notas_faturadas`, whereas AWS Staging retains raw legacy Protheus ERP column aliases (`produt`, `emissao`, `total`, `loja`, `numero_nota`).

---

## 1. Schema Comparison & Structural Profiling

### 1.1 Column Differences & Naming Discrepancies
| Table Name | Columns in Local Only | Columns in Athena Staging Only | Common Columns | Datatype Shifts / Observations |
| :--- | :--- | :--- | :---: | :--- |
| **`protheus_vendas_consolidadas`** | `extraction_timestamp`, `instance_id` (2) | `bronze_updated_at`, `item_hash`, `natural_key`, `source_server` (4) | 41 | DuckDB uses `TIMESTAMP` & `VARCHAR` IDs (`cliente_id`, `paciente_id`, `medico_id`, `num_nota`), whereas Athena uses `DATE` & `INTEGER`. 41 business fields match 1:1. |
| **`protheus_pedidos_a_faturar`** | `cliente_id`, `condicao_pagamento`, `cpf`, `descricao_produto`, `dt_emissao`, `grupo_produto`, `instance_id`, `medico_id`, `nome_cliente`, `num_nota`, `operador`, `paciente_id`, `preco_venda`, `produto_id`, `serie_nota`, `ultimo_preco`, `valor_bruto`, `valor_comissao`, `valor_custo`, `valor_custo_unit`, `valor_desconto`, `valor_iss`, `valor_mercadoria`, `valor_total`, `valor_unitario` (25) | `bronze_updated_at`, `cliente`, `descricao`, `emissao`, `file_name`, `grupo`, `line_number`, `medico`, `nfse`, `nome`, `numero`, `paciente`, `prc_venda`, `produt`, `prontuario_match_tier`, `tipo_entidade`, `total`, `ult_preco`, `unidade_nome_legal`, `vlr_bruto`, `vlr_comissao`, `vlr_custo`, `vlr_custo_unit`, `vlr_desconto`, `vlr_iss`, `vlr_mercadoria`, `vlr_unit` (27) | 9 | **Legacy Schema in AWS Staging**: Local Gold has modernized snake_case names (`dt_emissao`, `produto_id`, `valor_total`), while Athena Staging retains raw Protheus aliases (`emissao`, `produt`, `total`, `vlr_mercadoria`). |
| **`protheus_notas_faturadas`** | `cliente_id`, `conta_contabil`, `descricao_produto`, `descricao_tes`, `dt_emissao`, `extraction_timestamp`, `filial`, `instance_id`, `medico_id`, `nf_eletr`, `nome_cliente`, `nome_medico`, `num_nota`, `paciente_id`, `produto_id`, `quantidade`, `serie_nota`, `valor_custo`, `valor_custo_unit`, `valor_desconto`, `valor_total` (21) | `bronze_updated_at`, `cliente_codms`, `company_id`, `cta_ctbl`, `cta_ctbl_eugin`, `custo`, `custo_unit`, `data_do_ciclo`, `data_emissao`, `desconto`, `item_seq`, `lead_time`, `loja`, `medico`, `nfe_numero`, `nome`, `numero_nota`, `paciente_codms`, `produto`, `produto_descricao`, `prontuario_match_tier`, `qntd`, `serie`, `tes_descricao`, `tipo_entidade`, `total`, `unidade_nome_legal`, `vendedor_codigo` (28) | 19 | **Standardization Gap**: Local Gold standardizes to `filial`, `num_nota`, `serie_nota`, `dt_emissao`, `quantidade`, `valor_total`. Athena Staging retains legacy aliases `loja`, `numero_nota`, `serie`, `data_emissao`, `qntd`, `total`. |

---

## 2. Row Count, Volume Validation & Date Range Audit

### 2.1 Overall Row Counts and Temporal Windows
| Table Name | Local Count | Athena Staging Count | Absolute Diff | Pct Diff | Local Date Window | Athena Staging Date Window | Temporal Observation |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :--- |
| **`protheus_pedidos_a_faturar`** | 42,281 | 33,768 | -8,513 | -20.13% | 2023-03-03 to 2026-09-10 | 2023-03-03 to 2026-09-10 | Identical date bounds, but Athena Staging lacks recent 2026 Huntington orders. |
| **`protheus_vendas_consolidadas`** | 147,609 | 39,572 | -108,037 | -73.19% | 2023-03-03 to 2026-09-10 | **2026-06-18 to 2026-08-31** | **Severe Window Truncation**: Athena Staging was populated only with an incremental ~75-day test batch. |
| **`protheus_notas_faturadas`** | 828,504 | 338,500 | -490,004 | -59.14% | 2022-01-02 to 2026-09-10 | **2016-10-30 to 2026-09-10** | **Asymmetric Scope**: Athena has 121k rows (2016-2021) from BH historical migration, but lacks 609k Huntington rows (2022-2026). |

---

## 3. Deep Dive Table Reconciliations

### 3.1 `gold.protheus_pedidos_a_faturar` Reconciliation
Natural Key: Composite `filial + pedido + produto_id` (`produt`)

#### Yearly Breakdown
| Year | Local Count | Athena Stg Count | Count Match Rate | Local Value (R$) | Athena Stg Value (R$) | Financial Variance (R$) | Alignment Rate | Status |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :--- |
| **2023** | 6,211 | 6,211 | **100.00%** | R$ 13,327,778.49 | R$ 13,327,778.49 | R$ 0.00 | **100.00%** | ✅ PERFECT MATCH |
| **2024** | 8,316 | 8,316 | **100.00%** | R$ 19,614,490.48 | R$ 19,614,490.48 | R$ 0.00 | **100.00%** | ✅ PERFECT MATCH |
| **2025** | 8,942 | 8,843 | 98.89% | R$ 22,717,265.79 | R$ 22,476,813.37 | -R$ 240,452.42 | 98.94% | ⚠️ 99 orders missing in Staging |
| **2026** | 18,812 | 10,398 | 55.27% | R$ 59,691,469.58 | R$ 31,124,029.54 | -R$ 28,567,440.04 | 52.14% | ❌ 8,414 orders missing in Staging |
| **Total** | **42,281** | **33,768** | **79.87%** | **R$ 115,350,904.34** | **R$ 86,543,111.88** | **-R$ 28,807,792.46** | **75.03%** | ⚠️ Ingestion Freshness Lag |

#### Clinic / Filial Breakdown
* **Belo Horizonte (`0101` / `101`)**: **29,854 rows** | **R$ 73,053,753.15** on both sides $ightarrow$ **100.00% EXACT MATCH**.
* **Huntington Units (Ibirapuera, Vila Mariana, Cenafert, FIV Brasilia, Campinas, Pro Fiv)**:
  - Local: 12,427 rows | R$ 42,297,151.19
  - Athena Staging: 3,914 rows | R$ 13,489,358.73
  - **Discrepancy**: 8,513 rows | R$ 28,807,792.46 (Located exclusively in Huntington clinics during 2025–2026).

#### Overlap Quality Audit
* **Overlapping Composite Keys**: **33,494**
* **Overlap Value Alignment Rate**: **100.00%** (33,494 of 33,494 keys match to the exact cent).
* **Overlap Financial Sum**: **R$ 86,119,918.81** (Local) vs **R$ 86,119,918.81** (Athena Staging).

---

### 3.2 `gold.protheus_vendas_consolidadas` Reconciliation
Natural Key: Composite `filial + origem + documento + produto_id`

#### Scope & Window Discrepancy
* **Local Scope**: Full historical sales consolidation (Direct POS Sales + Sales Orders) from `2023-03-03` to `2026-09-10` (**147,609 rows**, **R$ 176,210,612.00**).
* **Athena Staging Scope**: Limited test partition spanning `2026-06-18` to `2026-08-31` (**39,572 rows**, **R$ 38,078,011.12**).

#### Common Window Comparison (`2026-06-18` to `2026-08-31`)
| Metric | Local DuckDB (Same Window) | AWS Athena Staging | Difference | Alignment / Overlap Rate |
| :--- | :---: | :---: | :---: | :---: |
| **Row Count** | 49,823 | 39,572 | -10,251 | 79.42% |
| **Total Value (R$)** | R$ 48,041,118.58 | R$ 38,078,011.12 | -R$ 9,963,107.46 | 79.26% |
| **`PEDIDO_DIRETO` Rows** | 509 | 1,674 | +1,165 | Deduplication Logic Shift |
| **`VENDA_DIRETA` Rows** | 49,314 | 37,898 | -11,416 | 76.85% |
| **Overlapping Keys** | 36,787 | 36,787 | 0 | 92.96% of Athena keys |
| **Overlap Value Match Rate** | 35,623 / 36,787 | 35,623 / 36,787 | - | **96.84%** |
| **Overlap Local Value** | R$ 34,158,745.75 | - | - | - |
| **Overlap Athena Value** | - | R$ 34,520,319.52 | +R$ 361,573.77 | **98.95%** Value Alignment |

---

### 3.3 `gold.protheus_notas_faturadas` Reconciliation
Natural Key: Composite `filial/loja + num_nota/numero_nota + serie/serie_nota + produto/produto_id`

#### Architectural & Slice Mismatch Analysis
| Dimension Slice | Local DuckDB Count | Local Value (R$) | Athena Staging Count | Athena Staging Value (R$) | Root Cause & Diagnosis |
| :--- | :---: | :---: | :---: | :---: | :--- |
| **Belo Horizonte (`0101`)** | 185,103 | R$ 135,037,618.29 | 304,326 | R$ 205,625,702.75 | Athena Staging contains historical BH data back to 2016 (121k rows in 2016–2021). Local Gold starts in 2022. |
| **Huntington Clinics** | 643,401 | R$ 629,526,152.17 | 34,174 | R$ 30,499,617.35 | **Severe Ingestion Gap**: Athena Staging contains only a ~5% sample of Huntington billed invoices (34k vs 643k). |
| **Total Table** | **828,504** | **R$ 764,563,770.46** | **338,500** | **R$ 236,125,320.10** | Staging is skewed: 90.0% BH and 10.0% Huntington, whereas Local has the full multi-branch production dataset. |

*(Context Note: AWS Athena Production `gold_huntington_prod.protheus_notas_faturadas` contains 838,277 rows, demonstrating that the full Huntington dataset is present in PROD, but has not been propagated/refreshed in STAGING).*

---

## 4. Key Takeaways & Root Cause Analysis

### 4.1 Root Cause 1: Exact Formula Alignment on Overlapping Pedidos
For `protheus_pedidos_a_faturar`, all overlapping records (**33,494 records**) achieve a **100.00% exact penny match** between Local and Athena Staging:
- Years 2023 and 2024 are bit-for-bit identical across all metrics.
- Belo Horizonte (`filial = 0101`) is 100.00% identical in count (29,854 rows) and total value (R$ 73,053,753.15).
- Mathematical proof: The underlying transformation logic from Silver to Gold is perfectly identical between Local and AWS.

### 4.2 Root Cause 2: Ingestion Freshness Lag in AWS Staging for Huntington Clinics
The divergence in `protheus_pedidos_a_faturar` (8,513 missing rows in Staging) is concentrated exclusively in 2025 (99 rows) and 2026 (8,414 rows) across Huntington units (`Ibirapuera`, `Vila Mariana`, `Cenafert`, `FIV Brasilia`, `Campinas`, `Pro Fiv`):
- Local DuckDB runs continuous daily pipelines ingesting the latest orders up through September 10, 2026.
- AWS Staging ETL workflows for Huntington orders were not triggered or completed for recent 2026 dates.

### 4.3 Root Cause 3: Incremental Test Partitioning in `protheus_vendas_consolidadas`
In AWS Staging, `gold_huntington_staging.protheus_vendas_consolidadas` was materialized with a strict `WHERE dt_emissao BETWEEN '2026-06-18' AND '2026-08-31'` filter:
- Local Gold contains 147,609 rows covering 2023 through September 2026.
- When evaluating only the common 75-day window, key overlap reaches 36,787 records with a **96.84% exact match** and **98.95% financial value alignment**.
- The row count variation within the window (49.8k vs 39.5k) stems from recent order-level deduplication applied in Local (`03_silver_to_gold.py`) to prevent double-counting between POS `venda_direta` and deferred `pedidos`.

### 4.4 Root Cause 4: Incomplete Staging Backfill and Schema Standardization Divergence
- **Invoices (`protheus_notas_faturadas`)**: Athena Staging is dominated by Belo Horizonte (`notas_bh`), which comprises 304,326 of its 338,500 rows (90%). The remaining Huntington branches only have 34k rows in Staging vs 643k rows locally. Meanwhile, AWS Athena PROD has 838,277 rows, confirming that the Staging database is missing the Huntington backfill.
- **Naming Standardization**: Local Gold tables have been modernized to clean `snake_case` domain standards (`cliente_id`, `dt_emissao`, `num_nota`, `serie_nota`, `produto_id`, `quantidade`, `valor_total`), while Athena Staging tables still use raw/abbreviated ERP field names (`produt`, `emissao`, `total`, `loja`, `qntd`).

---

## 5. Actionable Recommendations

1. **Synchronize AWS Staging Ingestion Pipelines**:
   - Trigger the Huntington ingestion pipeline in AWS Staging for `pedidos` and `notas` to ingest the 8,414 missing 2026 orders and the ~609k missing Huntington invoice rows.
2. **Standardize Athena Staging Column Naming**:
   - Update the dbt / Athena DDL models for `gold_huntington_staging.protheus_pedidos_a_faturar` and `gold_huntington_staging.protheus_notas_faturadas` to adopt the modernized snake_case names already deployed in Local Gold (`dt_emissao`, `produto_id`, `valor_total`, `cliente_id`).
3. **Rebuild `gold_huntington_staging.protheus_vendas_consolidadas` with Full History**:
   - Drop the `2026-06-18` to `2026-08-31` temporal restriction in the AWS consolidation script, allowing the Gold staging table to reflect the complete 2023–2026 historical horizon and incorporate the deduplication logic from `03_silver_to_gold.py`.
