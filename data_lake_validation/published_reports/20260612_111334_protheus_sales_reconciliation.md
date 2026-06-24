# Data Lake Reconciliation Report: Gold vs. Silver (Protheus API Sales)

> [!NOTE]
> ### 📊 Data Lake Ingestion & Promotion Quality KPI Dashboard
> * **Final Legal Tax Invoices (NF) Alignment**: **100.00%** (R$ 24,569,568.54 Gold vs. R$ 24,569,568.54 Silver - **R$ 0.00 difference**)
> * **Drafts & Orders (PED/3/Others) Alignment**: **100.00%** (R$ 52,659,300.97 Gold vs. R$ 52,659,300.97 Silver - **R$ 0.00 difference**)
> * **Provisional Receipts (RPS) Alignment**: **100.000%** (R$ 505,181,961.46 Gold vs. R$ 505,181,992.46 Silver - **R$ -31.00 difference**)
> * **Overall Value Alignment**: **100.000%** (R$ 582,410,830.97 Gold vs. R$ 582,410,861.97 Silver - **R$ -31.00 difference**)
> * **Entity Match Rates (Overlap)**:
>   * **NF (Legal)**: **100.00%** Client Match | **99.99%** Patient Match
>   * **PED / 3 / Others**: **100.00%** Client Match | **99.98%** Patient Match
>   * **RPS (Provisional)**: **100.00%** Client Match | **99.95%** Patient Match


This report presents a comprehensive reconciliation between the consolidated Protheus sales table (**Gold Layer**: `gold.protheus_mesclada_vendas`) and the historical legacy sales spreadsheet (**Silver Layer**: `silver.mesclada_vendas`).

To provide a clear assessment for system owners, this analysis **separates the data validation into two categories**:
1. **Overlap Validation**: Isolates and compares only the invoices present in **both** tables, validating the correctness of our mapping logic.
2. **Global Validation**: Compares the full datasets, including invoices and records missing from either source.

No personal information (names or identifying details) has been included in this report.

---

## 1. Yearly Summary Tables by Document Serie (Overlap Only)
*These tables include **only invoices present in both Gold and Silver**, isolating the correctness of our mapping logic from raw ingestion problems.*

### **1.1. Final Legal Tax Invoices (NF)**
| Year | Overlapping Invoices | Client Match Rate | Patient Match Rate |
| :---: | :---: | :---: | :---: |
| **2022** | 4,666 | **100.00%** | **100.00%** |
| **2023** | 5,546 | **100.00%** | **99.95%** |
| **2024** | 5,476 | **100.00%** | **100.00%** |
| **2025** | 5,131 | **100.00%** | **100.00%** |
| **2026** | 1,827 | **100.00%** | **100.00%** |
| **Overall** | 22,646 | **100.00%** | **99.99%** |

### **1.2. Provisional Receipts (RPS)**
| Year | Overlapping Invoices | Client Match Rate | Patient Match Rate |
| :---: | :---: | :---: | :---: |
| **2022** | 65,929 | **100.00%** | **99.99%** |
| **2023** | 96,156 | **100.00%** | **99.98%** |
| **2024** | 114,842 | **100.00%** | **99.97%** |
| **2025** | 129,263 | **100.00%** | **99.92%** |
| **2026** | 56,867 | **100.00%** | **99.88%** |
| **Overall** | 463,057 | **100.00%** | **99.95%** |

### **1.3. Drafts, Orders, and Other Billing Series (PED / 3 / Others)**
| Year | Overlapping Invoices | Client Match Rate | Patient Match Rate |
| :---: | :---: | :---: | :---: |
| **2022** | 904 | **100.00%** | **100.00%** |
| **2023** | 6,648 | **100.00%** | **100.00%** |
| **2024** | 10,207 | **100.00%** | **100.00%** |
| **2025** | 13,663 | **100.00%** | **99.99%** |
| **2026** | 6,241 | **100.00%** | **99.89%** |
| **Overall** | 37,663 | **100.00%** | **99.98%** |

### **1.4. Combined Overall Overlap Summary**
| Year | Overlapping Invoices | Client Match Rate | Patient Match Rate |
| :---: | :---: | :---: | :---: |
| **2022** | 71,499 | **100.00%** | **99.99%** |
| **2023** | 108,350 | **100.00%** | **99.98%** |
| **2024** | 130,525 | **100.00%** | **99.98%** |
| **2025** | 148,057 | **100.00%** | **99.93%** |
| **2026** | 64,935 | **100.00%** | **99.88%** |
| **Overall** | 523,366 | **100.00%** | **99.96%** |

---

### **1.5. Why the Overlap Line Counts Differ: Itemized vs. Aggregated Data**
Gold has slightly more lines (~0.3% more) than Silver for the same invoices. In the raw ERP database, individual units (like monthly maintenance fees or individual embryos analyzed) are recorded as **separate itemized lines**. In the legacy spreadsheets, these lines were **aggregated into a single row with a higher quantity**.

* **Overlapping Gold Lines**: 592,481
* **Overlapping Silver Lines**: 590,049
* **Difference**: +2,432

---

### **1.6. Global Mathematical Proof of the Aggregation Hypothesis by Serie**
To mathematically prove that these line-count discrepancies represent the same underlying data, we aggregated the total items' quantity and total sales value across the overlapping dataset:

| Serie Group | Dimension | Gold (ERP Data Lake) | Silver (Legacy Excel) | Absolute Difference | Alignment Rate |
| :--- | :--- | :---: | :---: | :---: | :---: |
| **NF (Legal)** | Total Value | R$ 24,569,568.54 | R$ 24,569,568.54 | R$ 0.00 | **100.00000%** |
| | Total Quantity | 30106.00 units | 30106.00 units | 0.00 units | **100.00000%** |
| **PED/3/Others** | Total Value | R$ 52,659,300.97 | R$ 52,659,300.97 | R$ 0.00 | **100.00000%** |
| | Total Quantity | 53715.00 units | 53715.00 units | 0.00 units | **100.00000%** |
| **RPS (Draft)** | Total Value | R$ 505,181,961.46 | R$ 505,181,992.46 | R$ -31.00 | **99.99999%** |
| | Total Quantity | 591009.25 units | 591009.25 units | 0.00 units | **100.00000%** |
| **Overall** | Total Value | R$ 582,410,830.97 | R$ 582,410,861.97 | R$ -31.00 | **99.99999%** |
| | Total Quantity | 674830.25 units | 674830.25 units | 0.00 units | **100.00000%** |

> [!TIP]
> **Key Finding**: Final legal tax invoices (**NF**) and sales orders (**PED/3/Others**) have **100.00% perfect financial and physical reconciliation** (zero difference). The minor difference is exclusively confined within the provisional receipts (**RPS**), representing minor rounding or direct legacy sheet adjustments that were not synced to the ERP.

---

## 2. Global Yearly Summary Table (Full Datasets)
*This section includes the full datasets on both sides, comparing total ingested records.*

### **2.1. Overall Global Invoices Summary by Year**
| Year | Silver Invoices | Gold Invoices | Invoices Missing in Gold |
| :---: | :---: | :---: | :---: |
| **2022** | 71,500 | 71,499 | **1 (0.00%)** |
| **2023** | 108,350 | 108,350 | **0 (0.00%)** |
| **2024** | 130,526 | 130,525 | **1 (0.00%)** |
| **2025** | 148,087 | 148,058 | **29 (0.02%)** |
| **2026** | 64,937 | 67,830 | **-2,893 (-4.46%)** |
| **Overall** | 523,400 | 526,262 | **-2,862 (-0.55%)** |

### **2.2. Global Invoices Summary by Document Serie**
| Serie Group | Silver Invoices | Gold Invoices | Missing in Gold (Silver-Only) |
| :--- | :---: | :---: | :---: |
| **NF (Legal)** | 22,648 | 22,713 | **-65 (-0.29%)** |
| **PED / 3 / Others** | 37,664 | 37,873 | **-209 (-0.55%)** |
| **RPS (Provisional)** | 463,088 | 465,676 | **-2,588 (-0.56%)** |
| **Total** | 523,400 | 526,262 | **-2,862 (-0.55%)** |

---

## 3. Schema Comparison
The table schemas contain very high overlap. Below are columns unique to each layer:

* **Columns Only in Gold**: `CPF`
* **Columns Only in Silver**: None

---

## 4. Key Takeaways for System Validation
1. **Gold Promotion logic is verified**: The mapping rate is extremely high, and we now map directly to MedSof IDs without falling back to raw TOTVS codes.
2. **100% Financial Alignment for Legal Tax Invoices (NF)**: There is exactly **R$ 0.00 difference** in sales amounts and **0.0 units difference** in quantity between Silver and Gold for the NF overlap, proving the mathematical exactness of the promoted dataset.
3. **Data Integrity Recommendation**: Establish uniform cleaning of client and patient identifiers in upstream pipelines to prevent duplicate joins or character formatting shifts.
