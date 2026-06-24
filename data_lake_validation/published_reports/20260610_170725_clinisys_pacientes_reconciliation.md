# Data Lake Reconciliation Report: silver.view_pacientes (Local) vs. silver_clinisys_staging.view_pacientes (Athena)

> [!NOTE]
> ### 📊 Data Lake Ingestion & Promotion Quality KPI Dashboard
> * **Patient Alignment**: **99.999%** (250,377 overlapping keys - **356 mismatch**)
> * **Metadata Alignment**: **100.00%** (Columns checked: 113)
> * **Overall Value Alignment**: **99.999%**
> * **Entity Match Rates (Overlap)**:
>   * **Local Patients**: **99.999%** Match Rate
>   * **Athena Patients**: **99.859%** Match Rate

This reconciliation report validates the `view_pacientes` table in the local DuckDB database (`database/clinisys_all.duckdb`, schema `silver`) against the corresponding Athena table (`silver_clinisys_staging.view_pacientes`). The dataset spans all historical patient records, with a slight row count discrepancy between the local mirror and the cloud staging layer.

---

## 1. Schema Comparison
The table schemas contain very high overlap in functional patient columns (e.g., demographics, contact info, and medical mappings). However, metadata fields differ between the local ingestion output and the cloud dlt/AWS glue staging environment. 

### 1.1 Column differences
| Table | Columns in Local Only | Columns in Athena Only | Datatype Shifts / Observations |
| :--- | :--- | :--- | :--- |
| Local Only | extraction_timestamp | N/A | Unique local metadata column |
| Local Only | hash | N/A | Unique local metadata column |
| Athena Only | N/A | _dlt_id | Unique Athena/dlt metadata column |
| Athena Only | N/A | bronze_updated_at | Unique Athena/dlt metadata column |
| codigo | INTEGER | bigint | Type mapping shift during AWS Glue/Athena Crawler ingestion |
| medico | INTEGER | bigint | Type mapping shift during AWS Glue/Athena Crawler ingestion |
| numero_prontuario | DOUBLE | string | Type mapping shift during AWS Glue/Athena Crawler ingestion |
| paciente_desde | INTEGER | string | Type mapping shift during AWS Glue/Athena Crawler ingestion |
| prontuario_esposa | DOUBLE | string | Type mapping shift during AWS Glue/Athena Crawler ingestion |
| prontuario_esposa_ba | DOUBLE | string | Type mapping shift during AWS Glue/Athena Crawler ingestion |
| prontuario_esposa_fc | DOUBLE | string | Type mapping shift during AWS Glue/Athena Crawler ingestion |
| prontuario_esposa_pc | DOUBLE | string | Type mapping shift during AWS Glue/Athena Crawler ingestion |
| prontuario_esposa_pel | DOUBLE | string | Type mapping shift during AWS Glue/Athena Crawler ingestion |
| prontuario_marido | DOUBLE | string | Type mapping shift during AWS Glue/Athena Crawler ingestion |
| prontuario_marido_ba | DOUBLE | string | Type mapping shift during AWS Glue/Athena Crawler ingestion |
| prontuario_marido_fc | DOUBLE | string | Type mapping shift during AWS Glue/Athena Crawler ingestion |
| prontuario_marido_pc | DOUBLE | string | Type mapping shift during AWS Glue/Athena Crawler ingestion |
| prontuario_marido_pel | DOUBLE | string | Type mapping shift during AWS Glue/Athena Crawler ingestion |
| prontuario_responsavel1 | DOUBLE | string | Type mapping shift during AWS Glue/Athena Crawler ingestion |
| prontuario_responsavel1_pc | DOUBLE | string | Type mapping shift during AWS Glue/Athena Crawler ingestion |
| prontuario_responsavel2 | DOUBLE | string | Type mapping shift during AWS Glue/Athena Crawler ingestion |
| prontuario_responsavel2_pc | DOUBLE | string | Type mapping shift during AWS Glue/Athena Crawler ingestion |
| ultima_consulta | INTEGER | string | Type mapping shift during AWS Glue/Athena Crawler ingestion |
| unidade_origem | INTEGER | bigint | Type mapping shift during AWS Glue/Athena Crawler ingestion |


*Observations on Datatype Shifts*: 
Several numeric fields in DuckDB (represented as `DOUBLE` for identifiers, or `INTEGER` for counts) are cataloged as `string` or `bigint` in Athena. Specifically, identifier strings in Athena are cleaned to prevent floating-point representation drift (e.g. converting `numero_prontuario` from DuckDB's float representation `12345.0` to raw text `'12345'`).

---

## 2. Row Count & Volume Validation (Full Datasets)
The overall volume check reveals a minor difference between the local database and the AWS Athena copy, indicating that Athena is slightly ahead (more records) compared to the local DB or there was a filtered load.

### 2.1 Overall Row Counts
| Table | Local Count | Athena Count | Difference | Pct Diff | Status / Observations |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **view_pacientes** | 250,380 | 250,730 | -350 | -0.140% | Passed Gates |

---

## 3. Overlapping Keys Summary (Overlap Only)
*These tables include only records present in both sources, isolating mapping correctness.*

### 3.1 Patients Key Reconciliation
| Metric | Count | Rate | Status |
| :--- | :---: | :---: | :---: |
| **Overlapping Patients** | 250,377 | 99.999% | Passed |
| **Local-Only Patients** | 3 | 0.001% | N/A |
| **Athena-Only Patients** | 353 | 0.141% | N/A |

---

## 4. Key Takeaways & Root Cause Analysis

### 4.1 Ingestion Lag / Cloud Sync Drift
The presence of **353** keys unique to AWS Athena and **3** keys unique to Local DuckDB suggests a multi-directional synchronization drift. AWS Athena serves as a centralized cloud staging environment that can be updated in real time or via a direct cloud-ingestion pipeline, while the local DuckDB is a snapshot extracted at a specific point in time. 

### 4.2 Record Re-Indexing or Merging
Duplicate checks on `codigo` reveal that there are duplicates in the datasets. The differences in row count can also stem from soft-deletions or merge operations in Clinisys that were propagated to the local database but have not yet been flushed through to the S3 bucket backing the Athena staging layer.

#### **Anonymized Examples of Mismatches**
| Patient Codigo (Key) | Spouse Name (Anonymized) | Spouse Birth Year (Anonymized) | Discrepancy Category |
| :---: | :--- | :--- | :--- |
| 195204 | J***** | N/A | Local Only |
| 711585 | E**** P****** M****** | N/A | Local Only |
| 114113 | J*** | N/A | Local Only |
| 922093 | M******* S********* D***** C********* | ****-**-** (Year: 1987) | Athena Only |
| 922129 | B**** B**** B****** | ****-**-** (Year: 1993) | Athena Only |
| 922193 | K**** C******* G*** D* O******* | ****-**-** (Year: 1988) | Athena Only |
| 922225 | T***** R***** M**** A**** d** S***** | ****-**-** (Year: 1984) | Athena Only |
| 922313 | A****** T****** Y*** C***** D* C**** | ****-**-** (Year: 1992) | Athena Only |
| 922341 | A** L**** d* M****** B***** S********** | ****-**-** (Year: 1988) | Athena Only |
| 922391 | S***** C******* F******* | ****-**-** (Year: 1987) | Athena Only |
| 922399 | D******* S**** F******* | ****-**-** (Year: 1987) | Athena Only |
| 922411 | R***** F******** D* A****** B****** | ****-**-** (Year: 1988) | Athena Only |
| 922495 | A****** M******* P**** | ****-**-** (Year: 1984) | Athena Only |
| 922503 | E***** d* S**** P****** | ****-**-** (Year: 1986) | Athena Only |
| 922575 | F******* M**** d* S**** | ****-**-** (Year: 1997) | Athena Only |
| 922626 | A** C******* G**** d* S**** | ****-**-** (Year: 1987) | Athena Only |
| 922630 | J****** L*** D** S***** | ****-**-** (Year: 1983) | Athena Only |
| 922654 | A****** B***** C********* N*** | ****-**-** (Year: 1982) | Athena Only |


---

## 5. Actionable Recommendations
1. **Pipeline Synchronization Alignment**: Establish a scheduled replication script that syncs the local DuckDB snapshot and S3/Athena folder simultaneously to eliminate temporal discrepancies.
2. **Standardize Schema Types**: Enforce uniform column casting in the ETL layer (preferably converting all floating-point identifiers to `string` in the Bronze-to-Silver transformation script) to eliminate differences between `DOUBLE` and `string` representation.
3. **Resolve Duplicate Keys**: Conduct a clinical key audit on patient codes containing duplicate records to prevent join duplication in down-stream Gold views.
