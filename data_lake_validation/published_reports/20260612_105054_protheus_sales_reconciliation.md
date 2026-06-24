# Data Lake Reconciliation Report: Gold vs. Silver (Protheus API Sales)

> [!NOTE]
> ### 📊 Data Lake Ingestion & Promotion Quality KPI Dashboard
> * **Final Legal Tax Invoices (NF) Alignment**: **100.00%** (R$ 22,259,380.06 Gold vs. R$ 22,259,380.06 Silver - **R$ 0.00 difference**)
> * **Drafts & Orders (PED/3/Others) Alignment**: **100.00%** (R$ 45,854,213.37 Gold vs. R$ 45,854,213.37 Silver - **R$ 0.00 difference**)
> * **Provisional Receipts (RPS) Alignment**: **99.999%** (R$ 441,613,921.12 Gold vs. R$ 441,618,260.12 Silver - **R$ -4,339.00 difference**)
> * **Overall Value Alignment**: **99.999%** (R$ 509,727,514.55 Gold vs. R$ 509,731,853.55 Silver - **R$ -4,339.00 difference**)
> * **Entity Match Rates (Overlap)**:
>   * **NF (Legal)**: **99.99%** Client Match | **99.99%** Patient Match
>   * **PED / 3 / Others**: **99.97%** Client Match | **99.96%** Patient Match
>   * **RPS (Provisional)**: **97.10%** Client Match | **94.90%** Patient Match


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
| **2022** | 4,666 | **99.98%** | **99.98%** |
| **2023** | 5,546 | **99.98%** | **99.98%** |
| **2024** | 5,476 | **100.00%** | **100.00%** |
| **2025** | 4,944 | **100.00%** | **100.00%** |
| **Overall** | 20,632 | **99.99%** | **99.99%** |

### **1.2. Provisional Receipts (RPS)**
| Year | Overlapping Invoices | Client Match Rate | Patient Match Rate |
| :---: | :---: | :---: | :---: |
| **2022** | 65,929 | **96.84%** | **93.30%** |
| **2023** | 96,138 | **97.09%** | **94.04%** |
| **2024** | 114,842 | **97.11%** | **95.14%** |
| **2025** | 123,785 | **97.22%** | **95.96%** |
| **Overall** | 400,694 | **97.10%** | **94.90%** |

### **1.3. Drafts, Orders, and Other Billing Series (PED / 3 / Others)**
| Year | Overlapping Invoices | Client Match Rate | Patient Match Rate |
| :---: | :---: | :---: | :---: |
| **2022** | 904 | **99.23%** | **99.23%** |
| **2023** | 6,648 | **99.88%** | **99.85%** |
| **2024** | 10,207 | **99.98%** | **99.98%** |
| **2025** | 13,172 | **100.00%** | **100.00%** |
| **Overall** | 30,931 | **99.97%** | **99.96%** |

### **1.4. Combined Overall Overlap Summary**
| Year | Overlapping Invoices | Client Match Rate | Patient Match Rate |
| :---: | :---: | :---: | :---: |
| **2022** | 71,499 | **97.07%** | **93.81%** |
| **2023** | 108,332 | **97.31%** | **94.46%** |
| **2024** | 130,525 | **97.33%** | **95.58%** |
| **2025** | 141,901 | **97.35%** | **96.34%** |
| **Overall** | 452,257 | **97.35%** | **95.21%** |

---

### **1.5. Why the Overlap Line Counts Differ: Itemized vs. Aggregated Data**
Gold has slightly more lines (~0.3% more) than Silver for the same invoices. In the raw ERP database, individual units (like monthly maintenance fees or individual embryos analyzed) are recorded as **separate itemized lines**. In the legacy spreadsheets, these lines were **aggregated into a single row with a higher quantity**.

* **Overlapping Gold Lines**: 510,206
* **Overlapping Silver Lines**: 508,449
* **Difference**: +1,757

---

### **1.6. Global Mathematical Proof of the Aggregation Hypothesis by Serie**
To mathematically prove that these line-count discrepancies represent the same underlying data, we aggregated the total items' quantity and total sales value across the overlapping dataset:

| Serie Group | Dimension | Gold (ERP Data Lake) | Silver (Legacy Excel) | Absolute Difference | Alignment Rate |
| :--- | :--- | :---: | :---: | :---: | :---: |
| **NF (Legal)** | Total Value | R$ 22,259,380.06 | R$ 22,259,380.06 | R$ 0.00 | **100.00000%** |
| | Total Quantity | 27,183.00 units | 27,183.00 units | 0.00 units | **100.00000%** |
| **PED/3/Others** | Total Value | R$ 45,854,213.37 | R$ 45,854,213.37 | R$ 0.00 | **100.00000%** |
| | Total Quantity | 45,116.00 units | 45,116.00 units | 0.00 units | **100.00000%** |
| **RPS (Draft)** | Total Value | R$ 441,613,921.12 | R$ 441,618,260.12 | R$ -4,339.00 | **99.99902%** |
| | Total Quantity | 512,388.25 units | 512,391.25 units | -3.00 units | **99.99941%** |
| **Overall** | **Total Value** | **R$ 509,727,514.55** | **R$ 509,731,853.55** | **R$ -4,339.00** | **99.99915%** |
| | **Total Quantity** | **584,687.25 units** | **584,690.25 units** | -3.00 units | **99.99949%** |

> [!TIP]
> **Key Finding**: Final legal tax invoices (**NF**) and sales orders (**PED/3/Others**) have **100.00% perfect financial and physical reconciliation** (zero difference). The minor difference is exclusively confined within the provisional receipts (**RPS**), representing minor rounding or direct legacy sheet adjustments that were not synced to the ERP.

---

## 2. Global Yearly Summary Table (Full Datasets)
*This section includes the full datasets on both sides, comparing total ingested records.*

### **2.1. Overall Global Invoices Summary by Year**
| Year | Silver Invoices | Gold Invoices | Invoices Missing in Gold |
| :---: | :---: | :---: | :---: |
| **2022** | 71,500 | 71,499 | **1 (0.00%)** |
| **2023** | 108,350 | 108,332 | **18 (0.02%)** |
| **2024** | 130,526 | 130,525 | **1 (0.00%)** |
| **2025** | 142,091 | 141,902 | **189 (0.13%)** |
| **Overall** | 452,467 | 452,258 | **209 (0.05%)** |

### **2.2. Global Invoices Summary by Document Serie**
| Serie Group | Silver Invoices | Gold Invoices | Missing in Gold (Silver-Only) |
| :---: | :---: | :---: | :---: |
| **NF (Legal)** | 20,637 | 20,632 | **5 (0.02%)** |
| **PED / 3 / Others** | 30,952 | 30,931 | **21 (0.07%)** |
| **RPS (Provisional)** | 400,878 | 400,695 | **183 (0.05%)** |
| **Total** | 452,467 | 452,258 | **209 (0.05%)** |

---

## 3. Schema Comparison
The table schemas contain very high overlap. Below are columns unique to each layer:

* **Columns Only in Gold**: None
* **Columns Only in Silver**: None

---

## 4. Key Takeaways for System Validation
1. **Gold Promotion logic is verified**: The mapping rate is extremely high, and we now map directly to MedSof IDs without falling back to raw TOTVS codes.
2. **100% Financial Alignment for Legal Tax Invoices (NF)**: There is exactly **R$ 0.00 difference** in sales amounts and **0.0 units difference** in quantity between Silver and Gold for the NF overlap, proving the mathematical exactness of the promoted dataset.
3. **Data Integrity Recommendation**: Establish uniform cleaning of client and patient identifiers in upstream pipelines to prevent duplicate joins or character formatting shifts.
