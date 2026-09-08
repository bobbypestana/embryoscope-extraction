# Data Lake Reconciliation Report: DuckDB vs AWS Athena (Embryoscope Silver Layer)

> [!NOTE]
> ### 📊 Global Reconciliation Dashboard
> * **Total Tables Audited**: **3**
> * **Key Overlap Alignment Rate**: **> 99.9%** across all tables
> * **Historical Match (2017 - July 2026)**: **100.00%** perfect key match
> * **Primary Root Cause of Row Variance**: Production Athena duplicates 'Itaim' records under 'Vila Mariana'
>
> | Table Name | Key Definition | Local Rows | Athena Rows | Distinct Keys (Athena) | Overlap Match | Local Only | Athena Only | Key Match Rate | Status |
> | :--- | :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :--- |
> | **patients** | `PatientIDx` | 14,360 | 17,725 | 14,359 | 14,357 | 3 | 2 | **99.98%** | ⚠️ Duplicate Rows in Prod |
> | **treatments** | `PatientIDx`, `TreatmentName` | 24,053 | 28,546 | 24,066 | 24,049 | 4 | 17 | **99.98%** | ⚠️ Duplicate Rows in Prod |
> | **embryo_data** | `EmbryoID` | 154,413 | 192,411 | 154,450 | 154,373 | 40 | 77 | **99.97%** | ⚠️ Duplicate Rows in Prod |

---

## 1. Executive Summary
This report reconciles the local DuckDB database (`huntington_data_lake.duckdb`, schema `silver_embryoscope`) with AWS Athena production (`silver_embryoscope_prod`) across the three core embryology tables: `patients`, `treatments`, and `embryo_data`.

While raw row counts show Athena with ~24% more records than DuckDB (+37,998 embryos), deep-dive analysis revealed that **this discrepancy is entirely driven by 100% duplicate records in Athena where all 'Itaim' records are replicated under 'Vila Mariana'**. Once de-duplicated by primary key, the datasets exhibit a **>99.97% alignment rate**, with remaining variances restricted to the current active ingestion window (September 2026).

---

## 2. Row Count & Key Reconciliation Summary

### 2.1 Table Metrics Breakdown
* **`patients`**: Local 14,360 vs Athena 17,725 (Difference: -3,365). Overlap matches 14,357 unique patients. Athena has only 2 unique patients not in local, and local has 3 not in Athena.
* **`treatments`**: Local 24,053 vs Athena 28,546 (Difference: -4,493). Overlap matches 24,049 unique treatment keys. Athena has 17 treatments not in local, and local has 4 not in Athena.
* **`embryo_data`**: Local 154,413 vs Athena 192,411 (Difference: -37,998). Overlap matches 154,373 unique embryos. Athena has 77 unique embryos not in local, and local has 40 not in Athena.

---

## 3. Location / Clinic Unit Breakdown

| Unit / Clinic | `patients` Local | `patients` Athena | `treatments` Local | `treatments` Athena | `embryo_data` Local | `embryo_data` Athena | Notes |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :--- |
| **Belo Horizonte** | 2,621 | 2,621 | 3,484 | 3,485 | 27,637 | 27,638 | 100.0% aligned |
| **Ibirapuera** | 6,289 | 6,289 | 13,300 | 13,302 | 67,738 | 67,741 | 100.0% aligned |
| **Itaim** | 3,376 | 3,376 | 4,493 | 4,497 | 38,101 | 38,137 | Aligned (36 delta from Sept 2026) |
| **Brasilia** | 1,815 | 1,817 | 2,473 | 2,478 | 18,522 | 18,553 | Aligned (31 delta from Sept 2026) |
| **Salvador** | 259 | 256 | 303 | 304 | 2,415 | 2,381 | Local +34 (Sept 5 extract pending) |
| **Vila Mariana** | **0** | **3,366** | **0** | **4,480** | **0** | **37,961** | **Exact duplication of Itaim in Athena** |

---

## 4. Root Cause Analysis

### 4.1 Root Cause #1: Production Duplication of Itaim as Vila Mariana
In AWS Athena production:
* Every single row for `Vila Mariana` has an identical counterpart under `Itaim`.
* Total distinct `embryo_id` in Athena is **154,450**, compared to 192,411 total rows. The 37,961 difference exactly equals the count of `Vila Mariana` embryos.
* DuckDB correctly contains only the canonical unit (`Itaim`), preventing 37,961 duplicate embryo records.

### 4.2 Root Cause #2: Temporal Extraction Window Drift (September 2026)
* **Historical Data (2017 – July 2026)**: Perfect 100.00% match across all clinics.
* **August 2026**: 2,184 matched embryos; only 1 Local only, 17 Athena only.
* **September 2026**:
  * **Athena-only (60 embryos)**: Nightly automated pipeline on Athena ingested newer records up to **September 6-7, 2026** (e.g., Brasilia batch `S02698`).
  * **Local-only (39 embryos)**: Salvador local extraction from **September 5, 2026** (`GM0GZZ2Y_...` treatments `2909/26`, `2910/26`) has not yet been processed into Athena production.

---

## 5. Actionable Recommendations
1. **Deduplicate Athena Silver Layer**: Remove the duplicate `Vila Mariana` partition or fix the upstream mapping rule in dbt/dlt that replicates `Itaim` records under `Vila Mariana`.
2. **Ingest Salvador Pending Batch**: Trigger the production ingestion for Salvador's September 5th extraction.
3. **Synchronize Local DuckDB**: Run the local incremental pull to capture the latest September 6-7 records from Brasilia and Itaim already present in Athena.
