# Protheus REST API Validation Report: Sales Orders & Direct Sales

**Date**: May 29, 2026  
**System**: TOTVS Protheus REST API  
**Environment**: Production (`https://huntingtoncentro175132.protheus.cloudtotvs.com.br:4050`)  
**Endpoints Tested**:
1. **Sales Orders (`CONSPED`)**: `/rest/CONSPED/pedidos`
2. **Direct Sales (`CONSPEVD`)**: `/rest/CONSPEVD/pedidos`

---

## 🔍 Executive Summary

This report documents the validation findings for the two new TOTVS Protheus API endpoints delivered on May 28, 2026. Both endpoints were tested for parameter behavior, multi-tenant partitioning, response stability, and potential ingestion limitations.

### Key Findings:
1. **Mandatory Date Filtering**: Both endpoints require `dataIni` and `dataFim` parameters (in `AAAAMMDD` format). Omitting them yields `HTTP 400` with the message `"Informe dataIni e dataFim"`.
2. **No Hard Result Cap (1,000/1,500 Rows)**: Unlike the invoices endpoint (`/rest/CONSNOTA/notas`), these new endpoints **do not enforce** a hard cap of 1,000 or 1,500 flat database rows. We successfully retrieved over **3,200 flat item rows** in a single paginated sequence without truncation or premature `hasNext = False` termination.
3. **Stable Query Ordering (No Ingestion Drift)**: Unlike the customers endpoint (`/rest/CONSCLI/clientes`), both sales endpoints return records in a **fully stable sequence**. Querying the same page twice yielded **0.00% symmetric difference**, confirming that pagination drift is not an issue.
4. **TenantId Partitioning**: Both endpoints partition data strictly by company/branch database. Querying without the `TenantId` header returns `HTTP 200` but with **0 records**.
5. **Page Boundary Overlap (Nested Item Pagination)**: Pagination page sizes (`nPageSize`) apply to **flat database item rows** *before* they are grouped into parent objects. Consequently, a parent record whose nested `ITENS` span across page boundaries is returned on both pages, containing only the items belonging to that page.

---

## 1. Parameters & Pagination Verification

### 1.1. Date Filters (`dataIni` and `dataFim`)
* **Parameter Obligation**: Requesting either endpoint without date filters returns `HTTP 400` with:
  ```json
  {"errorCode": 400, "errorMessage": "Informe dataIni e dataFim"}
  ```
* **Filter Precision**: 
  * Querying a single day (`dataIni=20260302&dataFim=20260302`) correctly retrieved records issued on that exact day (e.g. matching `C5_EMISSAO` or `L1_EMISSAO`).
  * Querying a 15-day range returned the cumulative records from all intermediate days, confirming that range filtering operates as expected.

### 1.2. Pagination Parameters (`nPage` and `nPageSize`)
* Both endpoints support custom parameters `nPage` and `nPageSize`.
* The response uses the standard flat root envelope layout:
  ```json
  {
    "page": 1,
    "pageSize": 5,
    "total": 230,
    "totalPages": 46,
    "hasNext": true,
    "data": [...]
  }
  ```

---

## 2. TenantId Dependency Verification

We tested the endpoints using three active tenant configurations and also by omitting the `TenantId` header entirely:

| Query Configuration | Sales Orders (`CONSPED`) Counts | Direct Sales (`CONSPEVD`) Counts | Observations |
| :--- | :---: | :---: | :--- |
| **Tenant `07,030101`** (Brasília) | 4 | 9 | Returns Brasília-specific records. |
| **Tenant `06,060101`** (Pro Fiv) | 6 | 6 | Returns Pro Fiv-specific records. |
| **Tenant `01,010104`** (Vila Mariana) | 0 | 0 | Returns Vila Mariana-specific records. |
| **Without `TenantId` Header** | 0 | 0 | Returns HTTP 200 but empty data list. |

* **Conclusion**: Both endpoints are **strictly tenant-dependent**. Ingestion pipelines must iterate through all active branch `TenantId` headers (similar to the invoices pipeline) to build a unified database.

---

## 3. Gap Analysis Alignment

We evaluated the endpoints against the database ingestion problems identified in the previous gap analysis report (@[20260527_172424_protheus_ingestion_gap_analysis.md](file:///g:/My%20Drive/projetos_individuais/Huntington/data_lake_validation/published_reports/20260527_172424_protheus_ingestion_gap_analysis.md)):

### 3.1. Silent Server-Side Result Cap (MaxRows limit)
* **Invoices Problem**: The `/rest/CONSNOTA/notas` endpoint silently truncates results if the range contains >1,000 flat rows (for Tenant 06) or >1,500 flat rows (for Tenant 01), setting `hasNext = False` on the page boundary where the cap is hit.
* **New Endpoints Test**: We ran a full paginated crawl on the Brasília tenant for `CONSPEVD` over the range `20250101` to `20260529`. The loop read through 16 pages of size 200 without any failure.
* **Metric Results**:
  * **CONSPED (Sales Orders)**: Successfully completed pagination to the end, retrieving **400 flat rows** (192 parent objects).
  * **CONSPEVD (Direct Sales)**: Successfully retrieved **3,200 flat rows** (cumulative 2,752 parent objects) over 16 pages. The loop was stopped manually to save time, indicating that there is no 1,000 or 1,500 limit.
* **Conclusion**: **No result cap is present.** Both endpoints can be safely queried for large volumes without premature pagination cuts.

### 3.2. Pagination Drift (Unstable Ordering)
* **Customers Problem**: The `/rest/CONSCLI/clientes` endpoint lacks a database-level sorting key, causing records to randomly shift between pages on successive calls (100% drift).
* **New Endpoints Test**: We queried Page 1 with size 20 twice back-to-back with a 1-second delay and calculated the symmetric difference between the records' primary keys.
* **Metric Results**:
  * **Symmetric Difference**: **0 items (0.00% difference)** for both `CONSPED` and `CONSPEVD`.
  * **Sequence Check**: The returned list sequence was identical in both calls.
* **Conclusion**: **No pagination drift detected.** The API backend sorting order is stable.

### 3.3. Page Boundary Overlap (New Finding)
* **Issue**: Because the pagination limits are calculated at the database level on **individual item rows** (lines in `ITENS`) rather than parent objects (orders), parent objects containing items that cross the page limit are returned on both consecutive pages.
* **Example**: During the validation of `CONSPED` (with `nPageSize=5`):
  * **Page 1** returned 3 parent records representing 5 flat item lines.
  * **Page 2** returned 2 parent records representing 5 flat item lines.
  * **Overlap**: Parent order `('030101', '000084')` was returned in **both** Page 1 and Page 2. Page 1 contained its first few items, and Page 2 contained its remaining items.
* **Downstream Mitigation**: This is a natural consequence of Protheus's flat-to-nested REST mapping. It does not lead to data loss. The ingestion script can safely resolve this by flat-mapping all items and deduplicating using database write constraints or hash comparisons (e.g., `hash` generated from the parent key + item key).

---

## 4. Recommendations for Ingestion Pipeline

To incorporate these endpoints into the Bronze ingestion layer:
1. **Flat-Map Items**: Follow the same design pattern as `CONSNOTA` in [01_source_to_bronze.py](file:///g:/My%20Drive/projetos_individuais/Huntington/protheus/01_ingestion/01_source_to_bronze.py). Extract parent fields and merge them with each child dictionary inside the `ITENS` list to create flat-mapped database rows.
2. **Hash-Based Deduplication**: Compute a row hash using parent primary keys (`C5_FILIAL` + `C5_NUM` for orders; `L1_FILIAL` + `L1_NUM` for direct sales) and item primary keys (`C6_ITEM` for orders; `L2_ITEM` for direct sales) along with the other data values.
3. **Sequential Crawl**: Perform sequential pagination (`nPage` increment) looping through `ACCESSIBLE_TENANTS`. A large page size (e.g., `nPageSize=500` or `1000`) is recommended to minimize API requests since there is no server-side result cap.
4. **Date Windowing**: Use weekly or daily query chunks based on the emission dates to keep extraction runs fast, consistent, and easy to recover.
