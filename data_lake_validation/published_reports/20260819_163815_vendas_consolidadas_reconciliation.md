# Data Lake Reconciliation Report: `gold.protheus_vendas_consolidadas`

> [!NOTE]
> ### 📊 Consolidation Quality & Reconciliation Dashboard
> * **Generated At**: `2026-08-19 16:38:16`
> * **Total Consolidated Sales Records**: **91,100**
> * **Direct Sales (`silver.venda_direta`) Alignment**: **100.00%** (80,546 Gold vs 80,546 Silver)
> * **Sales Orders (`gold.protheus_pedidos_a_faturar`) Alignment**: **100.00%** (10,554 Gold vs 10,554 Pedidos)
> * **Direct Sales Value Difference**: **R$ -0.00** (R$ 97,949,449.09 Gold vs R$ 97,949,449.09 Silver)
> * **Sales Orders Value Difference**: **R$ -0.00** (R$ 36,004,599.40 Gold vs R$ 36,004,599.40 Source)
> * **Overall Prontuário Match Rate**: **97.24%** (88,583 matched / 91,100 total)

---

## 1. Row Count & Volume Reconciliation

| Dataset Origin | Gold Row Count | Input Source Row Count | Row Match Rate | Gold Value (R$) | Source Value (R$) | Value Diff (R$) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **`VENDA_DIRETA`** | 80,546 | 80,546 | 100.00% | R$ 97,949,449.09 | R$ 97,949,449.09 | R$ -0.00 |
| **`PEDIDO_A_FATURAR`** | 10,554 | 10,554 | 100.00% | R$ 36,004,599.40 | R$ 36,004,599.40 | R$ -0.00 |
| **Total Consolidated** | **91,100** | **91,100** | **100.00%** | **R$ 133,954,048.49** | **R$ 133,954,048.49** | **R$ -0.00** |

---

## 2. Clinisys Prontuário Matching Rate

| Origin Slice | Total Records | Matched Prontuário | Unmatched (-1) | Match Rate (%) |
| :--- | :--- | :--- | :--- | :--- |
| **`PEDIDO_A_FATURAR`** | 10,554 | 10,189 | 365 | **96.54%** |
| **`VENDA_DIRETA`** | 80,546 | 78,394 | 2,152 | **97.33%** |
| **Total Consolidated** | **91,100** | **88,583** | **2,517** | **97.24%** |

---

## 3. Entity & Dimensional Field Coverage

| Origin | Total Rows | Client Name % | CPF % | Patient Name % | Prescribing Doctor % | Product Description % |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **`VENDA_DIRETA`** | 80,546 | 99.98% | 99.02% | 100.00% | 100.00% | 99.98% |
| **`PEDIDO_A_FATURAR`** | 10,554 | 100.00% | 98.78% | 100.00% | 100.00% | 100.00% |

---

## 4. Breakdown by Clinic Unit

| Clinic Unit | Total Rows | Direct Sales Rows | Sales Orders Rows | Total Sales Value (R$) |
| :--- | :--- | :--- | :--- | :--- |
| **Ibirapuera** | 38,825 | 34,319 | 4,506 | R$ 64,665,521.34 |
| **Vila Mariana** | 20,416 | 18,572 | 1,844 | R$ 23,846,401.44 |
| **Salvador - Cenafert** | 10,327 | 8,767 | 1,560 | R$ 15,120,845.56 |
| **FIV Brasilia** | 9,285 | 8,047 | 1,238 | R$ 14,272,611.64 |
| **Pro Fiv** | 7,677 | 7,088 | 589 | R$ 6,812,766.58 |
| **Campinas** | 3,324 | 2,730 | 594 | R$ 6,301,522.31 |
| **Unknown Unit (07, 040101)** | 1,023 | 1,023 | 0 | R$ 2,372,348.90 |
| **Unknown Unit (7, 40101)** | 223 | 0 | 223 | R$ 562,030.72 |

---

## 5. Yearly Sales Distribution

| Year | Total Rows | Direct Sales Rows | Sales Orders Rows | Total Sales Value (R$) |
| :--- | :--- | :--- | :--- | :--- |
| **2025.0** | 99.0 | 0.0 | 99.0 | R$ 240,452.42 |
| **2026.0** | 91,001.0 | 80,546.0 | 10,455.0 | R$ 133,713,596.07 |
