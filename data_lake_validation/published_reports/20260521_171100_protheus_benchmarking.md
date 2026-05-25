# Protheus API Performance & Pagination Benchmarking Report
**Date**: May 21, 2026  
**Topic**: Ingestion Sweet Spot Analysis (Pagination & Tenant Dependencies)

---

## 1. Executive Summary
This benchmark evaluates the Protheus ERP REST API endpoint performance, limits, and behavior across different tenant headers. The goal is to determine the optimal pagination page size (`nPageSize`) and identify which endpoints depend on the `TenantId` parameter.

### Key Finding:
* The REST API imposes a strict ceiling of **500 records/items** per page across all key endpoints. Any requests for larger batch sizes (e.g., `1000`, `2000`, or `5000`) are silently capped at `500`.
* The optimal `nPageSize` to minimize HTTP request overhead and maximize ingestion speed is **`500`**.

---

## 2. Endpoint Tenant Dependency Analysis
We queried the metadata and transactional endpoints across three active tenants to compare the data boundaries:
* `01,010101` (Ibirapuera - Company 01)
* `03,030101` (Campinas - Company 03)
* `07,030101` (FIV Brasilia - Company 07)

### Results:
| Endpoint | `01,010101` | `03,030101` | `07,030101` | Tenant Dependent? | Notes |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **`/rest/CONSNOTA/notas`** | **1,438** *(10d)* | **108** *(10d)* | **283** *(10d)* | **YES** | Returns branch-specific invoices. |
| **`/rest/CONSCLI/clientes`** | 150,053 | ~91,500 | ~90,500 | **YES** | Partitioned per company. Omitting `TenantId` header returns a default subset of only 96,422 clients, causing join/name gaps. |
| **`/rest/CONSPROD/produtos`** | 9,494 | ~9,494 | ~9,494 | **YES** | Partitioned per company. Must be queried per-tenant/company to capture all items. |
| **`/rest/CONSVEN/vendedores`** | 304 | ~304 | ~304 | **YES** | Partitioned per company. Must be queried per-tenant/company to capture all records. |
| **`/rest/CONSTES/tes`** | 35 | ~35 | ~35 | **YES** | Partitioned per company. Must be queried per-tenant/company to capture all codes. |

**Implication**: Both transactional invoices (`notas`) and metadata tables (`clientes`, `produtos`, `vendedores`, `tes`) are tenant-dependent because Protheus partitions tables per company database. Omitting the `TenantId` header limits the returned records to a default sub-company, leaving large gaps (such as missing patient names and products from other companies). To build a complete unified data lake, all tables must be ingested by looping through the active tenant/company configurations.

---

## 3. Page Size Optimization (Sweet Spot Benchmarking)
We measured the API response time and the actual number of records returned for different `nPageSize` values.

### A. Customers (`/rest/CONSCLI/clientes`)
* **nPageSize = 100**: 100 records in **2.45s**
* **nPageSize = 200**: 200 records in **2.22s**
* **nPageSize = 500**: 500 records in **1.79s** *(Optimal)*
* **nPageSize = 1000**: 500 records in **2.94s** *(Capped at 500)*
* **nPageSize = 2000**: 500 records in **3.01s** *(Capped at 500)*
* **nPageSize = 5000**: 500 records in **1.75s** *(Capped at 500)*

### B. Invoices (`/rest/CONSNOTA/notas`)
* Benchmarked using 6 months of historical data (expected > 1,000 invoices).
* **nPageSize = 1000**: Returns 417 invoices containing **exactly 500 flattened line-item rows** in **5.31s**.
* **Conclusion**: The pagination is capped on the server side at **500 flat items** per response.

### C. Vendors (`/rest/CONSVEN/vendedores`)
* Total dataset size is 304.
* **nPageSize = 500+**: Returns all 304 records in **0.60s** in a single round-trip.

---

## 4. Incremental Load Feasibility Analysis

We evaluated whether the API endpoints support incremental loading via either a timestamp filter (`datetime` parameter) or record ID filters.

### A. Timestamp Filtering (`datetime`)
We tested passing a future date (`2030-01-01 00:00:00.000`) expecting `0` records:
* **`clientes`**: Returned **148,464** records (out of 150,002).
* **`produtos`**: Returned **9,426** records (out of 9,494).
* **`vendedores`**: Returned **303** records (out of 304).
* **`tes`**: Returned **35** records (out of 35).
* **`notas`**: Rejected unless `dataIni` and `dataFim` are provided; when provided, it ignored `datetime` entirely.

**Conclusion**: The `datetime` query parameter is **not a reliable filter** on these endpoints. The ERP server ignores or bypasses the filter, returning almost the entire dataset.

### B. Record ID Filtering (`A1_COD`, `B1_COD`, `A3_COD`)
We tested passing specific existing IDs under various parameter keys (e.g., `cod`, `codigo`, `id`, `A1_COD`, `B1_COD`, `key`) to see if the endpoints filter by ID:
* **`clientes`**: Ignored all ID parameters; returned the default list starting with `028393` (total = 150,002).
* **`produtos`**: Ignored all ID parameters; returned the default list starting with `S3547` (total = 9,494).
* **`vendedores`**: Ignored all ID parameters; returned the default list starting with `000001` (total = 304).

**Conclusion**: The API **does not support ID-based filtering** on any metadata endpoint.

---

## 5. Ingestion Recommendation & Pipeline Design

Based on these findings, we recommend the following design for the Protheus Bronze ingestion layer:

1. **Optimal Page Size**: Set `nPageSize = 500` for all API calls to maximize batch throughput without triggering server-side caps.
2. **Transactional Invoices (`notas`)**:
   * **Tenant-Dependent**: Must run sequentially looping through all active tenant configurations to consolidate invoices.
   * **Incremental Logic**: Fully supported by passing `dataIni` and `dataFim` based on the maximum emission date (`F2_EMISSAO`) currently stored in the database.
3. **Metadata Tables (`clientes`, `produtos`, `vendedores`, `tes`)**:
   * **Tenant-Aware Ingestion**: Loop through the distinct company group representatives (e.g., `01,010101`, `03,030101`, `06,060101`, `07,010101`) to query and merge the tables.
   * **Load Type**: Full load (overwrite/append with row deduplication via hash). Because filtering is unsupported, we must load the full lists from each company group, compute MD5 row hashes, and deduplicate to maintain a complete metadata catalog.

