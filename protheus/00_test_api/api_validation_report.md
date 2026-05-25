# TOTVS Protheus API Validation Report
**Date**: May 20, 2026  
**System**: TOTVS Protheus REST API  
**Environment**: Production (`https://huntingtoncentro175132.protheus.cloudtotvs.com.br:4050`)  
**VPN Status**: Connected & Reachable  

---

## 1. Executive Summary

We conducted a systematic validation of the TOTVS Protheus REST API endpoints against the requirements defined in `api-requirements.md`. The goal was to evaluate if the API supports the necessary features for robust **incremental ingestion** in our Bronze layer.

### Key Findings:
1. **Incremental Ingestion is NOT viable using `datetime`**:
   - For `TES`, the `datetime` filter is completely ignored.
   - For `Produtos`, `Clientes`, and `Vendedores`, over **99% of database records** lack a populated modification date. Consequently, querying with a future date (e.g., `2027-01-01`) still returns virtually the entire table. Any "incremental" pipeline using `datetime` will effectively perform a full load anyway.
2. **Incremental Ingestion IS viable for `Notas`**:
   - `Notas` supports `dataIni` and `dataFim` filters which function correctly and allow range-based incremental chunking (e.g., by invoice emission date).
3. **Severe Gap between Requirements and API capabilities**:
   - Path versioning, sorting, cursor-based pagination, standard envelopes, rate-limiting, and error contracts defined in the requirements are **unsupported** or **ignored** by the Protheus REST API.
   - The API uses its own custom parameters (`nPage`, `nPageSize`, `dataIni`/`dataFim`, `datetime`) and a custom flat root response envelope.

---

## 2. Requirements Compliance Checklist

Below is the status of each requirement across the tested endpoints:

| Requirement | Description | Status | Details / Observations |
| :--- | :--- | :--- | :--- |
| **1. Versioning** | `/health` / `/api/v1/...` | ❌ **Failed** | All health paths returned `HTTP 404`. Path prefix `/api/v1/...` is unsupported. |
| **2. Sorting** | `sort_by=...&order=...` | ❌ **Failed** | Parameter is ignored. Server returns `HTTP 200` but results are identical in asc and desc. |
| **3. Pagination** | `limit` & `cursor` / `page` & `page_size` | ✅ **Passed (Custom)** | Standard pagination query parameters (`limit`/`cursor` or `page`/`page_size`) fail, but the custom parameters `nPage` and `nPageSize` work successfully across all endpoints. |
| **4. Date Filtering** | `date_from` & `date_to` (ISO) | ✅ **Passed (Custom)** | ISO-8601 parameters are ignored, but custom parameters work. `Notas` uses `dataIni`/`dataFim` (functional); other endpoints use `datetime` (functional, but limited due to empty-date database records). |
| **5. General Filters** | `ids=1,2,3` | ❌ **Failed** | Ignored by the server (returns full list instead of filtering). |
| **6. Response Envelope** | `{ data, meta, errors }` | ✅ **Passed (Custom)** | A nested `{ data, meta, errors }` envelope is not used, but a custom flat root envelope containing `page`, `pageSize`, `total`, `totalPages`, `hasNext`, and `data` is returned consistently. |
| **7. Rate Limiting** | Headers with limit info | ❌ **Failed** | No rate-limiting headers found in any response. |
| **8. Error Contracts** | Standard JSON error body | ❌ **Failed** | Server returns generic `HTTP 500` with standard server error body: `{"code":500,"message":"Internal Server Error"}`. |

---

## 3. Detailed Endpoint Behavior Analysis

### 3.1. Notas (`/rest/CONSNOTA/notas`)
* **Required Parameters**: Throws `HTTP 400` ("Informe dataIni e dataFim") if `dataIni` and `dataFim` are missing.
* **Pagination**: Page-based pagination works perfectly via `nPage` and `nPageSize`.
* **Date Filtering**: **Fully Functional**.
  - `dataIni=20200101&dataFim=20260520` -> `36,446` records.
  - `dataIni=20260501&dataFim=20260515` -> `571` records.
* **Sorting & IDs**: Ignored.

### 3.2. Clientes (`/rest/CONSCLI/clientes`)
* **Pagination**: Works via `nPage` and `nPageSize` (Total: `149,960` records).
* **Datetime Filter Behavior (Critical Bug)**:
  - No datetime -> Total: `149,960` records.
  - `datetime=1999-01-01` -> Total: `149,960` records.
  - `datetime=2026-05-20` -> Total: `148,998` records.
  - `datetime=2030-01-01` (Future) -> Total: `148,689` records.
  - **Analysis**: Over **99.1%** of Clientes records (`148,689`) have an empty modification date in the ERP. The database query is structured such that `date_modified >= :datetime OR date_modified IS NULL`. Thus, any incremental query will always pull the same `148,689` records.

### 3.3. Produtos (`/rest/CONSPROD/produtos`)
* **Pagination**: Works via `nPage` and `nPageSize` (Total: `9,492` records).
* **Datetime Filter Behavior**:
  - No datetime -> Total: `9,492` records.
  - `datetime=2027-01-01` (Future) -> Total: `9,435` records.
  - **Analysis**: **99.4%** of the product table (`9,435` records) lacks a populated modification date and will always be returned in every request.

### 3.4. Vendedores (`/rest/CONSVEN/vendedores`)
* **Pagination**: Works via `nPage` and `nPageSize` (Total: `304` records).
* **Datetime Filter**:
  - No datetime -> Total: `304` records.
  - `datetime=2027-01-01` (Future) -> Total: `303` records.
  - **Analysis**: Only **1** vendor record has a modification date. The remaining `303` will always be returned.

### 3.5. TES (`/rest/CONSTES/tes`)
* **Pagination**: Works via `nPage` and `nPageSize` (Total: `35` records).
* **Datetime Filter**: Completely ignored.
  - `datetime=2027-01-01` (Future) -> Total: `35` records.

---

## 4. Evaluation of Incremental Ingestion Viability

### 4.1. The Stateful Ingestion Claim
The note at the end of the specification says:
> *"Para o parâmetro datetime, caso não seja enviado, o sistema vai buscar os registros a partir do ultimo consultado."*

We tested this stateful behavior:
1. First request with no datetime: returned `149,960` records.
2. Second request with no datetime immediately after: returned `149,960` records.
If the API were tracking state, the second request should have returned `0` records. The tracking is either non-functional, requires special parameters/headers, or is disabled for this user. 

Furthermore, **relying on server-side stateful extraction is highly discouraged** for production data lakes:
- It breaks pipeline idempotency.
- If a pipeline run fails midway or data is corrupted downstream, we cannot re-run the extraction because the pointer has already moved forward on the server.

### 4.2. Ingestion Strategy Recommendation

We recommend split ingestion strategies for the Bronze layer:

1. **`Notas` (Incremental Ingestion)**:
   - **Strategy**: Range-based incremental chunking.
   - **Method**: The pipeline will query the API using `dataIni` and `dataFim` for a small window (e.g., last 3 days to account for corrections, or daily).
   - **Viability**: **High**.

2. **`TES`, `Produtos`, `Clientes`, `Vendedores` (Full Load Ingestion)**:
   - **Strategy**: Daily/Weekly Full Load (Truncate & Load).
   - **Method**: Download all pages using `nPage` and `nPageSize`.
   - **Viability**: **High** (as a full load), **Zero** (as incremental).
   - **Justification**:
     - Trying to filter by `datetime` does not prevent transferring 99%+ of the database.
     - The table sizes are small enough that full extraction is fast and safe:
       - **TES**: 35 rows (~5 KB)
       - **Vendedores**: 304 rows (~100 KB)
       - **Produtos**: 9,492 rows (~15 MB)
       - **Clientes**: 149,960 rows (~200 MB)
     - A full load avoids complex state management and guarantees 100% data consistency.

---

## 5. Architectural Suggestions

1. **Do not modify the Protheus API to comply with `api-requirements.md`**:
   - The REST endpoints are out-of-the-box TOTVS standard resources. Adapting them to custom parameters, envelopes, and sorting could require complex AdvPL (Protheus proprietary language) custom development and maintenance. It is better to handle the source's native structure in our ingestion code.
2. **Handle response structure in the Ingestion Script**:
   - The ingestion script (e.g., using `dlt` or a custom script) should directly parse the flat envelope (`data` field contains the records, `hasNext` controls the loop).
3. **Request restriction**:
   - Use a sensible page size (e.g., `nPageSize=500` or `1000`) during full loads to minimize the number of API calls while avoiding memory pressure on the Protheus application server.
