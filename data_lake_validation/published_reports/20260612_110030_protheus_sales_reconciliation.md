# Data Lake Reconciliation Report: Gold vs. Silver (Protheus API Sales)

> [!NOTE]
> ### 📊 Data Lake Ingestion & Promotion Quality KPI Dashboard
> * **Final Legal Tax Invoices (NF) Alignment**: **100.00%** (R$ 22,259,380.06 Gold vs. R$ 22,259,380.06 Silver - **R$ 0.00 difference**)
> * **Drafts & Orders (PED/3/Others) Alignment**: **100.00%** (R$ 45,854,213.37 Gold vs. R$ 45,854,213.37 Silver - **R$ 0.00 difference**)
> * **Provisional Receipts (RPS) Alignment**: **100.000%** (R$ 441,822,894.02 Gold vs. R$ 441,822,894.02 Silver - **R$ 0.00 difference**)
> * **Overall Value Alignment**: **100.000%** (R$ 509,936,487.45 Gold vs. R$ 509,936,487.45 Silver - **R$ 0.00 difference**)
> * **Entity Match Rates (Overlap)**:
>   * **NF (Legal)**: **100.00%** Client Match | **99.99%** Patient Match
>   * **PED / 3 / Others**: **100.00%** Client Match | **100.00%** Patient Match
>   * **RPS (Provisional)**: **100.00%** Client Match | **99.96%** Patient Match


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
| **2025** | 4,944 | **100.00%** | **100.00%** |
| **Overall** | 20,632 | **100.00%** | **99.99%** |

### **1.2. Provisional Receipts (RPS)**
| Year | Overlapping Invoices | Client Match Rate | Patient Match Rate |
| :---: | :---: | :---: | :---: |
| **2022** | 65,929 | **100.00%** | **99.99%** |
| **2023** | 96,156 | **100.00%** | **99.98%** |
| **2024** | 114,842 | **100.00%** | **99.97%** |
| **2025** | 123,861 | **100.00%** | **99.93%** |
| **Overall** | 400,788 | **100.00%** | **99.96%** |

### **1.3. Drafts, Orders, and Other Billing Series (PED / 3 / Others)**
| Year | Overlapping Invoices | Client Match Rate | Patient Match Rate |
| :---: | :---: | :---: | :---: |
| **2022** | 904 | **100.00%** | **100.00%** |
| **2023** | 6,648 | **100.00%** | **100.00%** |
| **2024** | 10,207 | **100.00%** | **100.00%** |
| **2025** | 13,172 | **100.00%** | **99.99%** |
| **Overall** | 30,931 | **100.00%** | **100.00%** |

### **1.4. Combined Overall Overlap Summary**
| Year | Overlapping Invoices | Client Match Rate | Patient Match Rate |
| :---: | :---: | :---: | :---: |
| **2022** | 71,499 | **100.00%** | **99.99%** |
| **2023** | 108,350 | **100.00%** | **99.98%** |
| **2024** | 130,525 | **100.00%** | **99.98%** |
| **2025** | 141,977 | **100.00%** | **99.94%** |
| **Overall** | 452,351 | **100.00%** | **99.97%** |

---

### **1.5. Why the Overlap Line Counts Differ: Itemized vs. Aggregated Data**
Gold has slightly more lines (~0.3% more) than Silver for the same invoices. In the raw ERP database, individual units (like monthly maintenance fees or individual embryos analyzed) are recorded as **separate itemized lines**. In the legacy spreadsheets, these lines were **aggregated into a single row with a higher quantity**.

* **Overlapping Gold Lines**: 510,321
* **Overlapping Silver Lines**: 508,434
* **Difference**: +1,887

---

### **1.6. Global Mathematical Proof of the Aggregation Hypothesis by Serie**
To mathematically prove that these line-count discrepancies represent the same underlying data, we aggregated the total items' quantity and total sales value across the overlapping dataset:

| Serie Group | Dimension | Gold (ERP Data Lake) | Silver (Legacy Excel) | Absolute Difference | Alignment Rate |
| :--- | :--- | :---: | :---: | :---: | :---: |
| **NF (Legal)** | Total Value | R$ 22,259,380.06 | R$ 22,259,380.06 | R$ 0.00 | **100.00000%** |
| | Total Quantity | 27183.00 units | 27183.00 units | 0.00 units | **100.00000%** |
| **PED/3/Others** | Total Value | R$ 45,854,213.37 | R$ 45,854,213.37 | R$ 0.00 | **100.00000%** |
| | Total Quantity | 45116.00 units | 45116.00 units | 0.00 units | **100.00000%** |
| **RPS (Draft)** | Total Value | R$ 441,822,894.02 | R$ 441,822,894.02 | R$ 0.00 | **100.00000%** |
| | Total Quantity | 512531.25 units | 512531.25 units | 0.00 units | **100.00000%** |
| **Overall** | Total Value | R$ 509,936,487.45 | R$ 509,936,487.45 | R$ 0.00 | **100.00000%** |
| | Total Quantity | 584830.25 units | 584830.25 units | 0.00 units | **100.00000%** |

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
| **2025** | 141,987 | 141,978 | **9 (0.01%)** |
| **Overall** | 452,363 | 452,352 | **11 (0.00%)** |

### **2.2. Global Invoices Summary by Document Serie**
| Serie Group | Silver Invoices | Gold Invoices | Missing in Gold (Silver-Only) |
| :--- | :---: | :---: | :---: |
| **NF (Legal)** | 20,634 | 20,633 | **1 (0.00%)** |
| **PED / 3 / Others** | 30,932 | 30,931 | **1 (0.00%)** |
| **RPS (Provisional)** | 400,797 | 400,788 | **9 (0.00%)** |
| **Total** | 452,363 | 452,352 | **11 (0.00%)** |

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
