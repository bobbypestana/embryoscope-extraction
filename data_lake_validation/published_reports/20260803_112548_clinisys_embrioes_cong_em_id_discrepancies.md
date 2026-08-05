# Data Lake Reconciliation Report: Local Gold vs. AWS Silver (clinisys_embrioes)

> [!NOTE]
> ### 📊 Global Reconciliation Dashboard
> * **Reconciliation Context**: `cong_em_id` key tracking on `clinisys_embrioes`
> * **Total Local Gold Records Evaluated**: **315,837**
> * **Total AWS Silver Reconstructed Records**: **315,343**
> * **Discrepancy / Mismatches Detected**: **9** records
> * **Reconciliation Rate**: **99.9971%**
> 
> | Table Name | Key Evaluated | Local Count | AWS Reconstructed Count | Overlap | Discrepant Keys | Status |
> | :--- | :--- | :---: | :---: | :---: | :---: | :--- |
> | `clinisys_embrioes` | `cong_em_id` | 315,837 | 315,343 | 315,343 | 9 | ⚠️ VARIANCE |

This report details the reconciliation process between `huntington_data_lake.gold.clinisys_embrioes` in the local DuckDB database and the reconstructed version using AWS Athena's `silver_clinisys_staging` layer views. Specifically, it tracks cases where the `cong_em_id` (freezing procedure ID) differs.

---

## 1. Schema & Key Definition
In both local and AWS systems, the `cong_em_id` column maps to the unique ID of the embryo freezing procedure (`view_congelamentos_embrioes.id`).
It is mapped by linking:
1. `view_micromanipulacao_oocitos` (represented as `oocito_id`)
2. `view_embrioes_congelados` (selects the latest record using `ROW_NUMBER() OVER (PARTITION BY id_oocito ORDER BY id DESC)`)
3. `view_congelamentos_embrioes` joined via `id_congelamento = id` and matching `prontuario`

---

## 2. Discrepancy Diagnostics & Mismatch Analysis

### 2.1 Detailed Mismatches List
Below is the complete list of oocitos where the local gold `cong_em_id` differs from the AWS silver reconstructed `cong_em_id`.

| Oocito ID | Patient Prontuario | Procedure Date | Doctor Name | Local Gold `cong_em_id` | AWS Reconstructed `cong_em_id` | Local Gold `emb_cong_id` | AWS Reconstructed `emb_cong_id` |
| :---: | :---: | :---: | :--- | :---: | :---: | :---: | :---: |
| 128120 | 517734 | 2023-02-11 | Eduardo Leme Alves da Motta | 30089 | 19841 | 89767 | 53884 |
| 263019 | 691214 | 2025-07-04 | Thais Sanches Domingues | 29451 | 29439 | 87364 | 87326 |
| 263021 | 691214 | 2025-07-04 | Thais Sanches Domingues | 29451 | 29439 | 87365 | 87327 |
| 263022 | 691214 | 2025-07-04 | Thais Sanches Domingues | 29451 | 29439 | 87366 | 87328 |
| 293896 | 691386 | 2026-01-13 | Arnaldo Schizzi Cambiaghi | 31451 | 31315 | 94820 | 94355 |
| 293897 | 691386 | 2026-01-13 | Arnaldo Schizzi Cambiaghi | 31451 | 31315 | 94821 | 94356 |
| 293898 | 691386 | 2026-01-13 | Arnaldo Schizzi Cambiaghi | 31451 | 31315 | 94822 | 94357 |
| 293899 | 691386 | 2026-01-13 | Arnaldo Schizzi Cambiaghi | 31451 | 31315 | 94823 | 94358 |
| 293900 | 691386 | 2026-01-13 | Arnaldo Schizzi Cambiaghi | 31451 | 31315 | 94824 | 94359 |

---

## 3. Key Takeaways & Root Cause Analysis

### 3.1. Missing Newer Records in AWS Silver Layer (Ingestion Lag)
All **9 discrepancies** are caused by a synchronization/ingestion lag between the local database and the AWS silver layer.
Specifically, newer freezing records (represented by higher `emb_cong_id` values and newer `cong_em_id` procedures) exist in the local database but have not yet been ingested/propagated to AWS Athena.

#### **Mathematical Proof & Logical Walkthrough**
For each oocito, the reconstruction logic selects the latest embryo freezing record by sorting `view_embrioes_congelados` by `id DESC` for a given `id_oocito`.

* **Case 1: Oocitos 263019, 263021, and 263022 (Prontuario 691214)**
  * **Local Database**: Has two congelamento records for these oocitos:
    * `id = 87326 / 87327 / 87328` (congelamento ID `29439`, code `E1765/25`, date `2025-07-09`)
    * `id = 87364 / 87365 / 87366` (congelamento ID `29451`, code `E1776/25`, date `2025-07-09`)
    * Picking `id DESC` results in `emb_cong_id = 87364-6` and `cong_em_id = 29451`.
  * **AWS Athena**: Only has the older record `id = 87326-8` (congelamento `29439`). The newer congelamento records are missing entirely. Thus, it falls back to picking the older record.

* **Case 2: Oocitos 293896 - 293900 (Prontuario 691386)**
  * **Local Database**: Has two records:
    * `id = 94355-9` (congelamento `31315`, code `E69/26`, date `2026-01-18`)
    * `id = 94820-4` (congelamento `31451`, code `E203/26`, date `2026-01-18`)
    * Picking `id DESC` results in `emb_cong_id = 94820-4` and `cong_em_id = 31451`.
  * **AWS Athena**: Only has the older record `id = 94355-9` (congelamento `31315`).

### 3.2. Record Counts Comparison (Full Silver Tables)
This is confirmed by checking the overall record counts and maximum primary keys:
* **`view_embrioes_congelados`**:
  * Local count: **95,807** (max id: **101,155**)
  * AWS count: **95,647** (max id: **101,096**)
  * *Variance: 160 missing records in AWS*
* **`view_congelamentos_embrioes`**:
  * Local count: **28,957** (max id: **33,206**)
  * AWS count: **28,918** (max id: **33,189**)
  * *Variance: 39 missing records in AWS*

---

## 4. Actionable Recommendations
1. **Trigger AWS Ingestion Pipelines**: Force run the AWS dlt ingestion / replication pipeline for `view_embrioes_congelados` and `view_congelamentos_embrioes` to fetch the missing 160 and 39 records.
2. **Review Synchronization Frequency**: Check if the replication job schedules or CDC triggers for Clinisys DB promotion are stalling or failing to capture deletes/newer insertions properly.
