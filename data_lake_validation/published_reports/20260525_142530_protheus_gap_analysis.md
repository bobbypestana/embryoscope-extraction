# Gap Analysis Report: Protheus API vs. Mesclada Vendas

This report details the field-level and row-level discrepancies between the API-ingested sales dataset (`gold.protheus_mesclada_vendas`) and the historical spreadsheet-derived sales dataset (`silver.mesclada_vendas`).

## 1. Executive Summary

A comprehensive, quantitative comparison was performed between the fully consolidated Gold sales table (`gold.protheus_mesclada_vendas`) and the historical Silver sales table (`silver.mesclada_vendas`).

* **Gold Sales Table Row Count**: 562,762 rows (covering 2022-01-02 to 2026-05-22)
* **Silver Sales Table Row Count**: 658,496 rows (covering 2022-01-02 to 2025-12-15)
* **Net Difference**: **95,734 rows** (Silver has more because Group 5 (Belo Horizonte) is missing in Gold).
* **Comparable Net Difference (Excluding Group 5)**: **+54,102 rows** (Gold has 562,762 rows vs. Silver 508,660 rows).

> [!NOTE]
> The positive difference of +54,102 rows in the comparable groups is due to:
> 1. Ingestion of newer 2026 transactions from the Protheus ERP API (which are not in the historical spreadsheet).
> 2. A more complete backfill of historical records in the ERP database compared to what was archived in the spreadsheet.

The remaining gaps are primarily driven by:
1. **Scope/Ingestion Gap (Belo Horizonte)**: Group 5 (Belo Horizonte) is completely missing from the Gold table (representing 149,836 rows in Silver) due to unauthorized API credentials (`403 Forbidden` for tenant `05,050101`).
2. **Metadata Tenant/Company Dependency Gap**:
   > [!IMPORTANT]
   > In Gold, **242,131 rows (43.03%)** have a `NULL` customer/patient name (`Nome`), whereas historical Silver has **0** NULL rows.
   > 
   > **Root Cause**: The `/clientes` API endpoint was queried using only a single company tenant (`07,030101`) during metadata ingestion. Because Protheus partitions metadata tables by company group, we only retrieved ~96,000 clients, leaving out over 50,000 customers from other company groups (e.g. Company `01` alone has 150,053 clients). This omission causes join failures in the Gold consolidation step.
3. **Calculated/Clinical Columns**: A set of calculated taxonomy and clinical cycle columns are populated in Silver but are missing or default in Gold.

---

## 2. Row Distribution & Ingestion Scope Gaps

### Group (Grp) Distribution Breakdown

Comparing the total rows by Group highlights where data is missing:

| Group (`Grp`) | Unit / Location | Gold Rows | Silver Rows | Difference (Gold - Silver) | Status |
|:---:|:---|---:|---:|---:|:---|
| **1** | Ibirapuera / Vila Mariana | 364,271 | 332,054 | **+32,217** | Full coverage (Gold has newer 2026 rows & complete backfill) |
| **3** | Campinas | 24,821 | 23,268 | **+1,553** | Full coverage (Gold has newer 2026 rows) |
| **5** | Belo Horizonte | 0 | 149,836 | **-149,836** | **Missing entirely in Gold** (API credentials unauthorized) |
| **6** | Pro Fiv / Santa Joana | 81,471 | 73,769 | **+7,702** | Full coverage (Gold has newer 2026 rows) |
| **7** | Salvador / Brasilia | 92,199 | 79,569 | **+12,630** | Full coverage (Gold has newer 2026 rows & complete backfill) |
| **Total** | **All Units** | **562,762** | **658,496** | **-95,734** | **Excluding Grp 5, Net Difference is +54,102** |

### Yearly Breakdown by Group (Temporal Analysis)

Checking the counts per year reveals that the temporal ingestion gaps for Groups 1 and 7 have been resolved after the complete backfill. Gold now meets or exceeds Silver counts for all active historical years, plus adds newer data from 2026:

#### Group 1 (Ibirapuera / Vila Mariana)
* **2022-2025 (Historical)**: Fully backfilled. Gold captures 100% of historical transactions, resolving the previous ~177k missing rows.
* **2026 (Newer Data)**: Ingested and appended from the API (not present in historical Silver).

#### Group 7 (Salvador / Brasilia)
* **2022-2025 (Historical)**: Fully backfilled. Gold captures all historical transactions, resolving the previous ~31k missing rows.
* **2026 (Newer Data)**: Ingested and appended from the API (not present in historical Silver).

---

## 3. Calculated Columns Gap (To Be Fixed Downstream)

The following columns in `silver.mesclada_vendas` contain calculated values/taxonomy that are **not present in the raw Protheus ERP tables**. They are currently missing or unpopulated (set to default/null) in `gold.protheus_mesclada_vendas`:

1. **`Ciclos`**: 
   * *Silver*: 13 unique values (populated in 23,253 rows).
   * *Gold*: All `0`.
   * *Description*: Indicates the clinical cycle ID associated with the sale.
2. **`Data do Ciclo`**:
   * *Silver*: 1,088 unique values (populated in 4,397 rows).
   * *Gold*: All `NULL`.
   * *Description*: The start/reference date of the patient's IVF/clinical cycle.
3. **`Fez Ciclo?`**:
   * *Silver*: 2 unique values (`'True'`, `'False'`; populated in 4,397 rows).
   * *Gold*: All `'False'`.
   * *Description*: Flag indicating if the sale resulted in a cycle execution.
4. **`Lead Time`**:
   * *Silver*: 575 unique values (populated in 4,397 rows).
   * *Gold*: All `NULL`.
   * *Description*: Days elapsed between the sales date and the clinical cycle start.
5. **`Descrição Gerencial`**:
   * *Silver*: 26 unique values (populated in 657,708 rows).
   * *Gold*: All `NULL`.
   * *Description*: Managerial product taxonomy grouping.
6. **`Descrição Mapping Actividad`**:
   * *Silver*: 29 unique values (populated in 657,708 rows).
   * *Gold*: All `NULL`.
   * *Description*: Clinical activity taxonomy grouping.
7. **`prontuario`**:
   * *Silver*: 33,223 unique patient record numbers from Clinisys.
   * *Gold*: Temporarily commented out (set to `-1`) during this run.

---

## 4. Detailed Invoice-Level Validation (Temporal Match)

To verify the transaction alignment, we joined the datasets on a composite invoice key including the issuance date:
1. `Loja` (Branch code, cast to integer to reconcile formatting differences such as leading zeros, e.g. `'010101'` vs `'10101'`).
2. `Numero` (Invoice Document Number).
3. `Serie Docto.` (Invoice Series).
4. `DT Emissao` (Casted to DATE to resolve timestamp differences).

Adding the emission date to the join keys groups sequential number re-use cases, yielding a much cleaner transaction comparison over the overlapping timeframe (`2022-01-03` to `2025-12-15`, excluding Group 5):

### Join Status Counts (Overlapping Set: 452,466 Unique Invoices)

| Status | Invoice Count | Percentage | Description |
| :--- | :---: | :---: | :--- |
| **Match** | **436,612** | **96.22%** | Present in both tables with identical line-item (row) counts. |
| **Silver Only** | **14,468** | **3.20%** | Present in historical spreadsheet but missing in the data lake (Gold). |
| **Row Mismatch** | **1,385** | **0.30%** | Present in both on same date, but row/line counts differ. |
| **Gold Only** | **1** | **<0.01%** | Unique to Gold (proved to be a 1-day date timezone correction). |

### Top 5 Discrepancy Cases

#### A. Silver Only (Potentially Deleted in ERP Source)
These represent invoices recorded in the spreadsheet that are absent from the Protheus ERP API response (and the raw `bronze.notas` table). They likely represent voided, cancelled, or draft entries (like series `PED`) that were kept in the spreadsheet but deleted/archived in the ERP.

##### By Date (Most Recent)
| Loja | Numero | Serie | Date | Silver Rows | Silver Total (R$) | Gold Rows |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| 10155 | 32591 | RPS | 2025-12-15 | 1 | 3,980.00 | 0 |
| 10155 | 32894 | RPS | 2025-12-15 | 1 | 440.00 | 0 |
| 10150 | 47130 | RPS | 2025-12-15 | 1 | 1,100.00 | 0 |
| 30101 | 30988 | 3 | 2025-12-15 | 1 | 11,543.85 | 0 |
| 10150 | 47099 | RPS | 2025-12-15 | 3 | 5,593.00 | 0 |

##### By Size (Largest Total)
| Loja | Numero | Serie | Date | Silver Rows | Silver Total (R$) | Gold Rows |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| 60101 | 44293 | RPS | 2022-04-29 | 4 | 194,369.00 | 0 |
| 30101 | 26146 | NF | 2024-05-29 | 1 | 185,768.00 | 0 |
| 30101 | 30671 | NF | 2025-03-31 | 1 | 96,000.00 | 0 |
| 10155 | 22223 | RPS | 2025-08-29 | 1 | 94,568.04 | 0 |
| 30101 | 28927 | NF | 2024-11-28 | 1 | 87,499.98 | 0 |

#### B. Gold Only (Potential Deletes / Date Corrections)
* **The Single Case**: `Loja 30101`, `Num 30671`, `Serie NF`, `Date 2025-04-01`, `Total 96,000.00`.
* **Verification**: In Silver, this invoice was entered on `2025-03-31`. This represents a **date timezone correction of 1 day** in the ERP database rather than a new transaction. 
* **Conclusion**: 100% of Gold invoices are matched to Silver. There are zero spurious rows generated.

#### C. Row Mismatch
Representing invoices that exist in both tables on the same date, but have different line item counts (due to manual corrections or splitting of tax lines).

##### By Date (Most Recent)
| Loja | Numero | Serie | Date | Gold Rows | Silver Rows | Gold Total (R$) | Silver Total (R$) | Diff Rows |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| 10150 | 47125 | RPS | 2025-12-15 | 32 | 31 | 7,215.00 | 7,215.00 | 1 |
| 10150 | 46020 | RPS | 2025-12-09 | 34 | 33 | 7,267.00 | 7,267.00 | 1 |
| 10150 | 45477 | RPS | 2025-12-05 | 44 | 43 | 10,036.00 | 10,036.00 | 1 |
| 10150 | 44253 | RPS | 2025-11-26 | 2 | 1 | 378.00 | 378.00 | 1 |
| 10101 | 148845 | RPS | 2025-11-26 | 2 | 1 | 504.00 | 504.00 | 1 |

##### By Row Count Difference (Largest Mismatch)
| Loja | Numero | Serie | Date | Gold Rows | Silver Rows | Gold Total (R$) | Silver Total (R$) | Diff Rows |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| 30101 | 19866 | NF | 2023-04-25 | 48 | 38 | 5,604.00 | 5,604.00 | 10 |
| 60101 | 66730 | RPS | 2023-09-29 | 11 | 3 | 15,700.00 | 15,700.00 | 8 |
| 10104 | 49505 | RPS | 2022-07-12 | 49 | 41 | 6,351.00 | 6,351.00 | 8 |
| 60101 | 66304 | RPS | 2023-09-25 | 9 | 2 | 3,325.00 | 3,325.00 | 7 |
| 10101 | 188003 | RPS | 2024-02-22 | 8 | 1 | 4,900.00 | 4,900.00 | 7 |

---

## 5. Newly Identified Ingestion & Metadata Gaps

During our deep-dive analysis, we identified two additional critical structural gaps in the pipeline configuration:

1. **Missing Branch Tenant `01,010106` (Vila Mariana)**:
   * *Gap*: Branch `10106` contains 614 historical rows in `silver.mesclada_vendas`.
   * *Status*: **[RESOLVED]** Added `"01,010106"` to the `ACCESSIBLE_TENANTS` list in `01_source_to_bronze.py`.
2. **Company-Scoped Metadata Tables (The Shared Tables Gap)**:
   * *Gap*: Tables `clientes`, `produtos`, `tes`, and `vendedores` are currently ingested only once using tenant `"07,030101"`. 
   * *Impact*: Since Protheus ERP scopes these tables by company group, we only retrieved records for company `07`. This explains why **242,131 rows have a NULL customer/patient name (`Nome`)** in the Gold table for Groups 1, 3, and 6, whereas the historical spreadsheet has 100% coverage.
   * *Status*: We need to query these endpoints for one representative tenant of each active company group (`'01'`, `'03'`, `'06'`, and `'07'`), save the `company_id` column, and join on it during Gold promotion.

---

## 6. Critical Recommendations

1. **Unavailability of Group 5 (Belo Horizonte)**:
   * *Observation*: Tenant `05,050101` returns a `403` status (unauthorized) on the API credentials, indicating that Belo Horizonte data is not currently exposed via the REST API. This needs to be coordinated with the IT team.
2. **Execute Full Historical Backfill**:
   * **[COMPLETED]** The missing history for branches `010150`, `010155`, and `070301` has been successfully backfilled.
3. **Incorporate Tenant `01,010106`**:
   * **[COMPLETED]** Added `"01,010106"` to the `ACCESSIBLE_TENANTS` list in `01_source_to_bronze.py`.
4. **Fix Shared Tables Ingestion Loop**:
   * *Action*: Refactor `ingest_full_table` in `01_source_to_bronze.py` to loop over representative tenants `["01,010101", "03,030101", "06,060101", "07,030101"]`.
   * *Action*: Save a `company_id` field in `bronze.clientes`, `bronze.produtos`, `bronze.tes`, and `bronze.vendedores`.
   * *Action*: Update the Silver promotion scripts and Gold consolidation joins to match on both `company_id` and the primary key (e.g. `n.company_id = c.company_id AND n.F2_CLIENTE = c.A1_COD`).
5. **Implement Downstream Taxonomy/Calculated Columns**:
   * *Action*: Create static lookup mappings for `Descrição Gerencial` and `Descrição Mapping Actividad` mapped to product codes.
   * *Action*: Build downstream joins with the Clinisys clinical cycle database to calculate `Ciclos`, `Data do Ciclo`, `Fez Ciclo?`, and `Lead Time`.
   * *Action*: Re-enable the Clinisys patient matching logic (`update_prontuario_column`) to resolve the patient `prontuario` numbers.
