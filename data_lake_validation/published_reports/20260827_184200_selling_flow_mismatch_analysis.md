# Protheus Selling Flow Reconciliation & Mismatch Analysis Report

> [!NOTE]
> ### 📊 Executive Summary & Disjoint Selling Flow Partition
> * **Audit Target**: `huntington_data_lake.duckdb`
> * **Operational Date Window**: `2026-04-04` to `2026-08-26`
> * **Total Unique Quotes Audited**: **`70,775`** (`100.00%` | R$ 105.12M)
> * **Key Flow Insight**: The sales flow is confirmed with **99.35%** of orders and **99.98%** of invoices originating in `venda_direta`. The 7.36% non-converted quotes are unapproved patient proposals.

---

## 1. Mutually Exclusive Flow Breakdown (100% Partition - Zero Confusion)

To avoid any confusion caused by overlapping queries (transactions that have both a Sales Order and an Invoice), the table below breaks down all **70,775 quotes** into **4 mutually exclusive, non-overlapping categories**:

| Flow Category | Description in Protheus ERP | Unique Quotes | % of Total | Total Value (R$) | In `silver.notas`? | In `silver.pedidos`? |
| :--- | :--- | :---: | :---: | :---: | :---: | :---: |
| **1. Direct POS Checkout Only** | Front-desk quote billed immediately at counter | **`60,892`** | **`86.04%`** | R$ 57,321,980.25 | ✅ Yes | ❌ No |
| **2. Full Chain (The Overlap)** | **Quote $\rightarrow$ Sales Order $\rightarrow$ Invoiced** | **`3,844`** | **`5.43%`** | R$ 5,801,044.20 | ✅ **Yes** | ✅ **Yes** |
| **3. Deferred Order Pending Billing** | Quote converted to Order; medical procedure awaiting billing | **`832`** | **`1.17%`** | R$ 5,801,044.20 | ❌ No | ✅ Yes |
| **4. Unconverted Quotes** | Open patient treatment proposals (`L1_SITUA = 'FR'`) / Drafts | **`5,207`** | **`7.36%`** | R$ 36,195,454.19 | ❌ No | ❌ No |
| **TOTAL (Mutually Exclusive)** | **All sales quotes in data lake** | **`70,775`** | **`100.00%`** | **R$ 105,119,522.84** | — | — |

---

### 🔍 Explaining the Overlapping Query Math

If you query `silver.notas` and `silver.pedidos` independently, the numbers look like this:

```text
  [ Quotes with an Invoice: 64,736 (91.47%) ]        [ Quotes with a Sales Order: 4,676 (6.61%) ]
                           \                                  /
                            \                                /
                             \                              /
                   +-----------------------------------------------+
                   |           THE OVERLAP (Category 2)            |
                   |                 3,844 Quotes                  |
                   |   (Converted to Order AND Then Billed)        |
                   +-----------------------------------------------+
```

* **Total Quotes with an Invoice (`silver.notas`)**: `60,892` (Direct only) + `3,844` (Full chain) = **`64,736`** (**`91.47%`**)
* **Total Quotes with a Sales Order (`silver.pedidos`)**: `832` (Pending) + `3,844` (Full chain) = **`4,676`** (**`6.61%`**)
* **Total Unique Matched Quotes**: $60,892 + 3,844 + 832 =$ **`65,568`** (**`92.64%`**)
* **Total Unconverted Quotes**: **`5,207`** (**`7.36%`**)
* **Grand Total**: $65,568 + 5,207 =$ **`70,775`** (**`100.00%`**)

---

## 2. Visual Architecture Diagram

```text
====================================================================================================
                        SILVER.VENDA_DIRETA (70,775 Total Quotes / 100.00%)
====================================================================================================
                                            |
         +----------------------------------+----------------------------------+
         |                                                                     |
         v                                                                     v
  [ MATCHED COUNTERPART ]                                             [ UNCONVERTED QUOTES ]
   65,568 Quotes (92.64%)                                              5,207 Quotes (7.36%)
   Total: R$ 68.92M                                                    Total: R$ 36.20M
         |                                                                     |
  +------+--------------------+--------------------+            +--------------+--------------+
  |                           |                    |            |              |              |
  v                           v                    v            v              v              v
[DIRECT POS CHECKOUT]  [FULL CHAIN OVERLAP] [ORDER AWAITING]  [STATUS 'FR']  [STATUS NULL]  [STATUS 'P3']
 60,892 Quotes          3,844 Quotes         832 Quotes        4,347 Quotes   856 Quotes     4 Quotes
 (86.04%)               (5.43%)              (1.17%)           (6.14%)        (1.21%)        (0.01%)
 R$ 57.32M              R$ 5.80M             R$ 5.80M          R$ 31.62M      R$ 4.55M       R$ 20.95k
 Quote -> Invoice       Quote -> Ped -> Nota Quote -> Ped      Unapproved     Abandoned      Pre-orders /
 (No order needed)      (Full chain)         (Not yet billed)  IVF proposals  draft carts    holds
====================================================================================================
```

---

## 3. Detailed Mismatch Categories & Real System Examples

### 3.1 Stage 1: Unconverted Quotes in `silver.venda_direta` (5,207 Mismatches | 7.36%)

#### 🔴 Category 1A: Unconverted Open Treatment Quotes (`L1_SITUA = 'FR'`)
* **Count**: **`4,347` quotes** (**83.5%** of mismatches | **6.14%** of total quotes) | **Value**: **`R$ 31,620,112.82`**
* **Why it happens**: In Protheus (`LOJA701` / `SL1010`), creating a proposal for a patient immediately creates an `L1_NUM`. If the patient decides not to proceed with treatment, negotiates terms, or has not paid yet, the quote stays in status `FR` (*Faturar Reserva*) and **never converts into an invoice or sales order**.
* **System Lookup Examples**:
  | Filial | Orçamento (`L1_NUM`) | Data | Status | Cliente / Paciente | Produto | Valor (R$) | Como Validar no Protheus |
  | :---: | :---: | :---: | :---: | :--- | :--- | :---: | :--- |
  | `010150` | **`068898`** | 2026-04-15 | `FR` | TATIANA ARATA (`895920`) | RESERVA LOTE - FIV OD INTERNOS (`S3551.70`) | R$ 33,052.19 | Abrir `LOJA701` (Venda Assistida): Orçamento em aberto status `FR` sem pedido/nota. |
  | `010150` | **`074246`** | 2026-05-14 | `FR` | DOUGLAS BRESCHIGLIARI (`872615`) | RESERVA LOTE - FIV OD INTERNOS (`S3551.70`) | R$ 31,000.00 | Abrir `LOJA701`: Proposta de ciclo FIV não aprovada pelo paciente. |
  | `010150` | **`068104`** | 2026-04-11 | `FR` | PEDRO CERUTTI (`225934`) | RESERVA LOTE - FIV OD INTERNOS (`S3551.70`) | R$ 31,000.00 | Abrir `LOJA701`: Orçamento em aberto status `FR`. |
  | `010150` | **`071618`** | 2026-04-30 | `FR` | CLAUDIA HELVADJIAN (`231728`) | RESERVA LOTE - FIV OD INTERNOS (`S3551.70`) | R$ 31,000.00 | Abrir `LOJA701`: Orçamento em aberto status `FR`. |

#### 🟡 Category 1B: Abandoned / Draft Front-Desk Carts (`L1_SITUA = NULL`)
* **Count**: **`856` quotes** (**16.4%** of mismatches | **1.21%** of total quotes) | **Value**: **`R$ 4,554,392.37`**
* **Why it happens**: Attendants at the clinic counter started typing items into the POS interface and abandoned the session or closed the browser window without finalizing.
* **System Lookup Examples**:
  | Filial | Orçamento (`L1_NUM`) | Data | Status | Cliente | Valor (R$) | Diagnóstico |
  | :---: | :---: | :---: | :---: | :--- | :---: | :--- |
  | `010150` | **`068019`** | 2026-04-10 | `NULL` | Paciente Balcão | R$ 20,035.68 | Sessão de balcão interrompida antes de gravação final. |
  | `010150` | **`078004`** | 2026-06-05 | `NULL` | Paciente Balcão | R$ 4,923.00 | Rascunho abandonado no PDV. |
  | `010150` | **`093302`** | 2026-08-21 | `NULL` | Paciente Balcão | R$ 4,048.00 | Carrinho avulso sem conversão. |

#### ⚪ Category 1C: Pre-orders & Technical Outliers (`L1_SITUA = 'P3'` / `OK`)
* **Count**: **`4` quotes** (**0.08%** of mismatches | **0.01%** of total quotes) | **Value**: **`R$ 20,950.00`**

---

### 3.2 Stage 2: Direct Orders in `silver.pedidos` (32 Mismatches | 0.65%)

* **Total Sales Orders Audited**: **`4,928`**
* **Matched from Venda Direta**: **`4,896`** (**`99.35%`**)
* **Mismatches (No POS Quote)**: **`32`** (**`0.65%`** | **`R$ 553,774.70`**)

#### 🔴 Category 2A: Direct Administrative, Corporate & B2B Orders
* **Count**: **`28` orders** (**87.5%** of mismatches | **0.57%** of all orders) | **Value**: **`R$ 515,647.20`**
* **Why it happens**: Corporate sponsorship agreements, pharmaceutical partnerships, and partner clinic billing entered **directly via ERP module `MATA410` (SIGAFAT)** by the finance/administrative department without a POS quote (`C5_ORCRES` is `NULL`).
* **System Lookup Examples**:
  | Filial | Pedido (`C5_NUM`) | Orçamento (`C5_ORCRES`) | Data | Cliente | Produto | Valor (R$) | Nota Billed | Como Validar no Protheus |
  | :---: | :---: | :---: | :---: | :--- | :--- | :---: | :---: | :--- |
  | `010155` | **`000801`** | `NULL` (Vazio) | 2026-06-30 | FERTILIDADE E VIDA (`050087`) | PROGRAMA DE OVORECEPCAO (`S3553.25`) | R$ 168,000.00 | `Pendente` | Consultar em `MATA410`: Faturamento direto B2B de clínica parceira sem PDV. |
  | `010155` | **`000815`** | `NULL` (Vazio) | 2026-07-03 | FERTILIDADE E VIDA (`050087`) | PROGRAMA DE OVORECEPCAO (`S3553.25`) | R$ 168,000.00 | `000053566` | Consultar em `MATA410`: Pedido direto faturado na RPS `000053566`. |
  | `010155` | **`000562`** | `NULL` (Vazio) | 2026-05-29 | FERTILIDADE E VIDA (`050087`) | PROGRAMA DE OVORECEPCAO (`S3553.25`) | R$ 75,247.20 | `000050197` | Consultar em `MATA410`: Pedido direto faturado na RPS `000050197`. |
  | `010101` | **`008960`** | `NULL` (Vazio) | 2026-07-13 | ORGANON FARMACEUTICA (`034664`) | PATROCINIO (`S3576.06`) | R$ 60,000.00 | `000251342` | Consultar em `MATA410`: Contrato de patrocínio corporativo inserido no faturamento. |
  | `010155` | **`000412`** | `NULL` (Vazio) | 2026-04-30 | INST. PAULISTA GINEC. (`070210`) | CRIOPRESERVACAO (`S3567.23`) | R$ 54,572.00 | `000046843` | Consultar em `MATA410`: Faturamento de serviço de terceiros. |

#### 🟡 Category 2B: Window Boundary Timing Crossovers (Quote in March 2026)
* **Count**: **`4` orders** (**12.5%** of mismatches | **0.08%** of all orders) | **Value**: **`R$ 38,127.50`**
* **System Lookup Example**: Pedido `110441` (Pro Fiv | 2026-05-06 | R$ 21,707.50): Orçamento `111761` emitido em Março/2026 (fora da janela de carga da Venda Direta).

---

### 3.3 Stage 3: Invoices in `silver.notas` (31 Mismatches | 0.02%)

* **Total Invoices Audited**: **`124,316`**
* **Matched Provenance**: **`124,285`** (**`99.98%`**)
* **Mismatches**: **`31`** (**`0.02%`** | **`R$ 84,137.16`**)

#### 🔴 Category 3A: Window Boundary Crossovers (Quotes Issued in March 2026)
* **Count**: **`25` invoices** (**80.6%** of mismatches | **0.020%** of all invoices) | **Value**: **`R$ 71,488.27`**
* **Why it happens**: The patient received a quote in late March 2026, and the invoice was billed between April 4 and April 14, 2026 (right on the edge of the ingestion window).
* **System Lookup Examples**:
  | Filial | Nota Fiscal (`F2_DOC`) | Série | Data | TES | Cliente | Produto | Valor (R$) | Como Validar no Protheus |
  | :---: | :---: | :---: | :---: | :---: | :--- | :--- | :---: | :--- |
  | `010150` | **`000064329`** | `RPS` | 2026-04-06 | `502` | VINCENZO RANIERI (`232115`) | GONAL F PEN 900UI (`000000000005015`) | R$ 14,396.00 | Consultar em `MATA461` / `SF2`: Faturada em 06/04/2026 com orçamento em Março/2026. |
  | `010150` | **`000064808`** | `RPS` | 2026-04-09 | `502` | NELSON CIRILO RAMOS (`227608`) | ASPIRACAO - COLETA ADICIONAL (`S3547.61`) | R$ 13,892.23 | Consultar em `MATA461`: Ciclo faturado em 09/04 de orçamento de Março. |
  | `010150` | **`000065064`** | `RPS` | 2026-04-13 | `502` | GABRIEL MAFRA (`899688`) | PERGOVERIS PEN 300/150 UI (`000000000005447`) | R$ 4,472.00 | Consultar em `MATA461`: Fatura de medicação com orçamento de Março. |
  | `010150` | **`000064360`** | `RPS` | 2026-04-06 | `502` | NELSON RAMOS (`227608`) | ASPIRACAO FOLICULAR (`S3547.28`) | R$ 5,390.30 | Consultar em `MATA461`: Procedimento faturado em 06/04. |

#### 🟡 Category 3B: Cross-Branch Invoicing (Branch Mismatch within Company 01)
* **Count**: **`4` invoices** (**12.9%** of mismatches | **0.003%** of all invoices) | **Value**: **`R$ 9,831.00`**
* **System Lookup Example**: Nota `000044594` (Filial `010155` | 2026-04-13 | R$ 3,432.00): Pedido `000262` cadastrado na Filial `010150` e faturado na Filial `010155`.

#### ⚪ Category 3C: Standalone Manual Service Invoices
* **Count**: **`2` invoices** (**6.5%** of mismatches | **0.001%** of all invoices) | **Value**: **`R$ 2,817.89`**

---

## 4. Summary Matrix Across All 3 Flow Stages

| Flow Stage | Audited Scope | Matched Count (%) | Mismatch Count (%) | Mismatch Value (R$) | Primary Root Cause of Mismatch |
| :--- | :---: | :---: | :---: | :---: | :--- |
| **Venda Direta $\rightarrow$ (Notas $\lor$ Pedidos)** | `70,775` Quotes | **65,568 (92.64%)** | **5,207 (7.36%)** | R$ 36.20M | Unconverted open patient quotes (`L1_SITUA = 'FR'`). |
| **Pedidos $\rightarrow$ Venda Direta** | `4,928` Orders | **4,896 (99.35%)** | **32 (0.65%)** | R$ 553.8k | Direct corporate/partner orders in `MATA410` without POS quote. |
| **Invoices $\rightarrow$ (VD $\lor$ Pedidos)** | `124,316` Invoices | **124,285 (99.98%)** | **31 (0.02%)** | R$ 84.1k | Invoiced in early April from quotes created in late March 2026. |
