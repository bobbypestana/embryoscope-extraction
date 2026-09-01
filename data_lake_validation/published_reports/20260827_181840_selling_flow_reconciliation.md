# Data Lake Selling Flow Reconciliation & Quality Validation Report

> [!NOTE]
> ### 📊 Global Selling Flow Quality Dashboard
> * **Generated At**: `2026-08-27 18:18:40`
> * **Data Lake Target**: `huntington_data_lake.duckdb`
> * **Common Date Window**: `2026-04-04` to `2026-08-26`
> * **Overall Flow Quality Status**: **❌ FAIL**
> 
> | Flow Check | Scope | Analyzed Count | Matched Counterparts | Match Rate | Quality Threshold | Status |
> | :--- | :--- | :---: | :---: | :---: | :---: | :--- |
> | **Venda Direta $ightarrow$ (Notas $\lor$ Pedidos)** | Unique Quotes | 70,775 | 65,568 | **92.64%** | 90.00% (Expected Unconverted Quotes) | ✅ PASS |
> | **Pedidos $ightarrow$ Venda Direta** | Common Window | 4,749 | 4,717 | **99.33%** | 98.00% | ✅ PASS |
> | **Invoices (Notas) $ightarrow$ (VD $\lor$ Pedidos)** | Common Window | 124,316 | 124,285 | **99.98%** | 99.00% | ✅ PASS |
> | **Financial Value Consistency** | Line-Item Overlap | 76,167 | 74,875 | **98.30%** | 95.00% | ✅ PASS |
> | **Global Financial Value Alignment** | Total Amount | R$ 62,925,222.81 | R$ 62,971,470.68 | **97.89%** | 99.00% | ❌ FAIL |

---

## 1. Flow Breakdown & Summary

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

## 2. Invoices & Sales Orders Traceability

* **Invoices with Verified Provenance**: 124,285 of 124,316 (**99.98%**).
* **Orders with Verified Provenance**: 4,717 of 4,749 (**99.33%**).
* **Residual Discrepancies**:
  - 32 orders entered as direct administrative sales orders (`MATA410`) without prior POS quote.
  - 31 invoices billed across the March-April 2026 ingestion boundary.

---

## 3. Actionable Pipeline Recommendations

1. **Deduplicate `gold.protheus_vendas_consolidadas`**: Prevent the dual-counting of 4,676 orders when combining `venda_direta` and `pedidos`.
2. **Execute Full Backfill for `bronze.venda_direta`**: Backfill historical direct sales prior to 2026-04-04 to cover pre-2026 orders and invoices.
3. **Cross-Branch Linking**: Allow order-to-invoice joins at the `company_id` level to accommodate legitimate inter-filial medical procedures.
