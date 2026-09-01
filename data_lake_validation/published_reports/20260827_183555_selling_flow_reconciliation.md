# Data Lake Selling Flow Reconciliation & Quality Validation Report

> [!NOTE]
> ### 📊 Global Selling Flow Quality Dashboard
> * **Generated At**: `2026-08-27 18:35:55`
> * **Data Lake Target**: `huntington_data_lake.duckdb`
> * **Common Date Window**: `2026-04-04` to `2026-08-26`
> * **Overall Flow Quality Status**: **✅ PASS**
> 
> | Flow Check | Scope | Analyzed Count | Matched Counterparts | Match Rate | Quality Threshold | Status |
> | :--- | :--- | :---: | :---: | :---: | :---: | :--- |
> | **Venda Direta $\rightarrow$ (Notas $\lor$ Pedidos)** | Unique Quotes | 70,775 | 65,568 | **92.64%** | 90.00% (Expected Unconverted Quotes) | ✅ PASS |
> | **Pedidos $\rightarrow$ Venda Direta** | Common Window | 4,749 | 4,717 | **99.33%** | 98.00% | ✅ PASS |
> | **Invoices (Notas) $\rightarrow$ (VD $\lor$ Pedidos)** | Common Window | 124,316 | 124,285 | **99.98%** | 99.00% | ✅ PASS |
> | **Financial Value Consistency** | Line-Item Overlap | 76,167 | 74,875 | **98.30%** | 95.00% | ✅ PASS |
> | **Global Financial Value Alignment** | Total Amount | R$ 62,925,222.81 | R$ 62,971,470.68 | **97.89%** | 95.00% | ✅ PASS |

---

## 1. Flow Breakdown & Lineage Summary

### 1.1 Venda Direta Conversion Breakdown
* **Direct POS Invoicing Path (`silver.notas`)**: 64,736 (91.47%)
* **Deferred Sales Order Path (`silver.pedidos`)**: 4,676 (6.61%)
* **Unconverted / Pending Quotes**: 5,207 (7.36%)

### 1.2 Unconverted Venda Direta Analysis (Status `L1_SITUA`)
| Situation Code | Description | Unconverted Quotes | Total Unconverted Value (R$) |
| :--- | :--- | :---: | :---: |
| **`FR`** | Faturar Reserva (Open quote / pending patient approval) | 4,347 | R$ 31,620,112.82 |
| **`OK`** | Isolated cancelled transmission | 1 | R$ 15,651.00 |
| **`P3`** | Pré-venda (Temporary hold) | 3 | R$ 5,299.00 |
| **`NULL (Draft POS Cart)`** | Draft / Interrupted session | 856 | R$ 4,554,392.37 |

---

## 2. Real System Examples of Discrepancies (For Protheus Verification)

### 2.1 Category 1: Unconverted Quotes in `venda_direta` (No Invoices or Orders)
*These represent patient treatment proposals created in the POS (`LOJA701` / `SL1010`) that remained open/unapproved.*

| Company | Filial | Orçamento (`L1_NUM`) | Data | Status | Cliente / Paciente | Produto | Valor (R$) | Como Validar no Protheus |
| :---: | :---: | :---: | :---: | :---: | :--- | :--- | :---: | :--- |
| `01` | `010150` | **`068898`** | 2026-04-15 | `FR` | TATIANA ARATA (895920) | RESERVA LOTE - FIV OD INTERNOS (S3551.70) | R$ 33,052.19 | Consultar em `LOJA701` (Venda Assistida): Orçamento em aberto status `FR` |
| `01` | `010150` | **`083908`** | 2026-07-03 | `FR` | SAMIRA COLLA (224769) | RESERVA LOTE - FIV OD INTERNOS (S3551.70) | R$ 31,000.00 | Consultar em `LOJA701` (Venda Assistida): Orçamento em aberto status `FR` |
| `01` | `010150` | **`074246`** | 2026-05-14 | `FR` | DOUGLAS GELEILEIATE BRESCHIGLIARI (872615) | RESERVA LOTE - FIV OD INTERNOS (S3551.70) | R$ 31,000.00 | Consultar em `LOJA701` (Venda Assistida): Orçamento em aberto status `FR` |
| `01` | `010150` | **`071618`** | 2026-04-30 | `FR` | CLAUDIA XAVIER HELVADJIAN (231728) | RESERVA LOTE - FIV OD INTERNOS (S3551.70) | R$ 31,000.00 | Consultar em `LOJA701` (Venda Assistida): Orçamento em aberto status `FR` |

### 2.2 Category 2: Direct Administrative Orders in `pedidos` (No `venda_direta` Quote)
*These represent corporate or partner orders entered directly via ERP module `MATA410` (SIGAFAT) without a POS quote.*

| Company | Filial | Pedido (`C5_NUM`) | Orçamento (`C5_ORCRES`) | Data | Cliente | Produto | Valor (R$) | Nota Faturada | Como Validar no Protheus |
| :---: | :---: | :---: | :---: | :---: | :--- | :--- | :---: | :---: | :--- |
| `01` | `010155` | **`000801`** | `NULL` (Vazio) | 2026-06-30 | FERTILIDADE E VIDA NUCLEO DE EXCE EM REPROD. HUM. (050087) | PROGRAMA DE OVORECEPCAO ETAPA 1 (S3553.25) | R$ 168,000.00 | `Pendente` | Consultar em `MATA410`: Pedido de venda direto sem orçamento prévio |
| `01` | `010155` | **`000815`** | `NULL` (Vazio) | 2026-07-03 | FERTILIDADE E VIDA NUCLEO DE EXCE EM REPROD. HUM. (050087) | PROGRAMA DE OVORECEPCAO ETAPA 1 (S3553.25) | R$ 168,000.00 | `000053566` | Consultar em `MATA410`: Pedido de venda direto sem orçamento prévio |
| `01` | `010155` | **`000562`** | `NULL` (Vazio) | 2026-05-29 | FERTILIDADE E VIDA NUCLEO DE EXCE EM REPROD. HUM. (050087) | PROGRAMA DE OVORECEPCAO ETAPA 1 (S3553.25) | R$ 75,247.20 | `000050197` | Consultar em `MATA410`: Pedido de venda direto sem orçamento prévio |
| `01` | `010101` | **`008960`** | `NULL` (Vazio) | 2026-07-13 | ORGANON FARMACEUTICA LTDA (034664) | PATROCINIO (S3576.06) | R$ 60,000.00 | `000251342` | Consultar em `MATA410`: Pedido de venda direto sem orçamento prévio |

### 2.3 Category 3: Invoices without Direct Counterpart (Boundary / Cross-Branch)
*These represent invoices billed in early April 2026 from quotes registered in late March 2026.*

| Company | Filial | Nota Fiscal (`F2_DOC`) | Série | Data | TES | Cliente | Produto | Valor (R$) | Diagnóstico & Validação |
| :---: | :---: | :---: | :---: | :---: | :---: | :--- | :--- | :---: | :--- |
| `01` | `010150` | **`000064329`** | `RPS` | 2026-04-06 | `502` | VINCENZO RANIERI (232115) | GONAL F PEN 900UI 66MCG 1,5ML (C.E.) ([PHONE_REDACTED]15) | R$ 14,396.00 | Consultar em `MATA461` / `SF2010`: Faturada em início de Abril/26 com orçamento em Março/26 |
| `01` | `010150` | **`000064808`** | `RPS` | 2026-04-09 | `502` | NELSON CIRILO RAMOS (227608) | ASPIRACAO - COLETA ADICIONAL INTERNOS (S3547.61) | R$ 13,892.23 | Consultar em `MATA461` / `SF2010`: Faturada em início de Abril/26 com orçamento em Março/26 |
| `01` | `010150` | **`000064808`** | `RPS` | 2026-04-09 | `502` | NELSON CIRILO RAMOS (227608) | INDUCAO - COLETA ADICIONAL INTERNOS (S3547.60) | R$ 6,846.36 | Consultar em `MATA461` / `SF2010`: Faturada em início de Abril/26 com orçamento em Março/26 |
| `01` | `010150` | **`000065064`** | `RPS` | 2026-04-13 | `502` | GABRIEL MAFRA (899688) | PERGOVERIS PEN 300/150 UI -0,48 ML ([PHONE_REDACTED]47) | R$ 4,472.00 | Consultar em `MATA461` / `SF2010`: Faturada em início de Abril/26 com orçamento em Março/26 |

---

## 3. Implemented Improvements & Pipeline Next Steps

1. **Deduplication Applied in `gold.protheus_vendas_consolidadas`**:
   - `03_silver_to_gold.py` now excludes orders already captured in `venda_direta`, eliminating R$ 75M+ in duplicate revenue counting.
   - Added column `status_fluxo` categorizing sales into `FATURADO_DIRETO`, `FATURADO_VIA_PEDIDO`, `PEDIDO_A_FATURAR`, `ORCAMENTO_ABERTO`, and `ORCAMENTO_AVULSO`.
2. **Execute Full Backfill for `bronze.venda_direta`**:
   - Run `ingest_venda_direta(force_backfill=True)` in `01_source_to_bronze.py` to extract historical direct sales prior to 2026-04-04.
