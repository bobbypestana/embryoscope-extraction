# Data Lake Reconciliation Report: `gold.protheus_vendas_consolidadas`

> [!NOTE]
> ### 📊 Consolidation Quality & Reconciliation Dashboard
> * **Generated At**: `2026-08-19 16:49:43`
> * **Total Consolidated Sales Records**: **89,215**
> * **Direct Sales (`silver.venda_direta`) Alignment**: **100.00%** (80,546 Gold vs 80,546 Silver)
> * **Sales Orders (`gold.protheus_pedidos_a_faturar`) Alignment**: **100.00%** (8,669 Gold vs 8,669 Pedidos)
> * **Direct Sales Value Difference**: **R$ -0.00** (R$ 97,949,449.09 Gold vs R$ 97,949,449.09 Silver)
> * **Sales Orders Value Difference**: **R$ -0.00** (R$ 28,756,642.38 Gold vs R$ 28,756,642.38 Source)
> * **Overall Prontuário Match Rate**: **97.21%** (86,727 matched / 89,215 total)

---

## 1. Row Count & Volume Reconciliation

| Dataset Origin | Gold Row Count | Input Source Row Count | Row Match Rate | Gold Value (R$) | Source Value (R$) | Value Diff (R$) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **`VENDA_DIRETA`** | 80,546 | 80,546 | 100.00% | R$ 97,949,449.09 | R$ 97,949,449.09 | R$ -0.00 |
| **`PEDIDO_A_FATURAR`** | 8,669 | 8,669 | 100.00% | R$ 28,756,642.38 | R$ 28,756,642.38 | R$ -0.00 |
| **Total Consolidated** | **89,215** | **89,215** | **100.00%** | **R$ 126,706,091.47** | **R$ 126,706,091.47** | **R$ -0.00** |

---

## 2. Clinisys Prontuário Matching Rate

| Origin Slice | Total Records | Matched Prontuário | Unmatched (-1) | Match Rate (%) |
| :--- | :--- | :--- | :--- | :--- |
| **`PEDIDO_A_FATURAR`** | 8,669 | 8,333 | 336 | **96.12%** |
| **`VENDA_DIRETA`** | 80,546 | 78,394 | 2,152 | **97.33%** |
| **Total Consolidated** | **89,215** | **86,727** | **2,488** | **97.21%** |

---

## 3. Entity & Dimensional Field Coverage

| Origin | Total Rows | Client Name % | CPF % | Patient Name % | Prescribing Doctor % | Product Description % |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **`VENDA_DIRETA`** | 80,546 | 99.98% | 99.02% | 100.00% | 100.00% | 99.98% |
| **`PEDIDO_A_FATURAR`** | 8,669 | 100.00% | 98.71% | 100.00% | 100.00% | 100.00% |

---

## 4. Breakdown by Clinic Unit

| Clinic Unit | Total Rows | Direct Sales Rows | Sales Orders Rows | Total Sales Value (R$) |
| :--- | :--- | :--- | :--- | :--- |
| **Ibirapuera** | 38,020 | 34,319 | 3,701 | R$ 60,856,153.86 |
| **Vila Mariana** | 20,006 | 18,572 | 1,434 | R$ 22,568,134.36 |
| **Salvador - Cenafert** | 10,077 | 8,767 | 1,310 | R$ 14,403,319.86 |
| **FIV Brasilia** | 9,055 | 8,047 | 1,008 | R$ 13,395,344.02 |
| **Pro Fiv** | 7,554 | 7,088 | 466 | R$ 6,481,287.66 |
| **Campinas** | 3,257 | 2,730 | 527 | R$ 6,067,472.09 |
| **Rio de Janeiro** | 1,246 | 1,023 | 223 | R$ 2,934,379.62 |

---

## 5. Yearly Sales Distribution

| Year | Total Rows | Direct Sales Rows | Sales Orders Rows | Total Sales Value (R$) |
| :--- | :--- | :--- | :--- | :--- |
| **2026.0** | 89,215.0 | 80,546.0 | 8,669.0 | R$ 126,706,091.47 |
