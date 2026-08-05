# Data Lake Reconciliation Report: DuckDB vs AWS Athena (Embryoscope Silver Layer)

> [!NOTE]
> ### 📊 Global Reconciliation Dashboard
> * **Total Tables Audited**: **3**
> * **Perfect Value Alignment**: **0** tables
> * **Variance / Discrepancy Detected**: **3** tables
> * **Audit Execution Failures**: **0** tables
> 
> | Table Name | Columns Missing (Exclude Metadata) | Local Count | Athena Count | Overlap | Local Only | Athena Only | Local Match Rate | Athena Match Rate | Status |
> | :--- | :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :--- |
| patients | *None* | 13,880 | 13,219 | 13,219 | 661 | 0 | 95.24% | 100.00% | Variance Detected |
| treatments | *None* | 23,472 | 22,305 | 22,305 | 1,167 | 0 | 95.03% | 100.00% | Variance Detected |
| embryo_data | *None* | 149,123 | 141,433 | 141,315 | 7,808 | 118 | 94.76% | 99.92% | Variance Detected |


This report validates the reconciliation of embryoscope silver layer tables between local DuckDB and AWS Athena.

---

## 1. Schema Comparison

### 1.1 Schema Comparison: `patients`
* Columns only in local `silver_embryoscope.patients`: `_extraction_timestamp`, `_location`, `_row_hash`, `_run_id`, `dateofbirth`, `firstname`, `lastname`, `patientid`, `patientidx`, `unit_huntington`
* Columns only in Athena `silver_embryoscope_prod.patients`: `_dlt_id`, `bronze_updated_at`, `date_of_birth`, `first_name`, `last_name`, `patient_id`, `patient_id_x`, `patient_sk`, `source_server`

### 1.2 Schema Comparison: `treatments`
* Columns only in local `silver_embryoscope.treatments`: `_extraction_timestamp`, `_location`, `_row_hash`, `_run_id`, `patientidx`, `treatmentname`, `unit_huntington`
* Columns only in Athena `silver_embryoscope_prod.treatments`: `_dlt_id`, `bronze_updated_at`, `patient_id_x`, `prontuario`, `source_server`, `treatment_name`, `treatment_sk`

### 1.3 Schema Comparison: `embryo_data`
* Columns only in local `silver_embryoscope.embryo_data`: `_extraction_timestamp`, `_location`, `_row_hash`, `_run_id`, `embryodescriptionid`, `embryofate`, `embryoid`, `fertilizationmethod`, `fertilizationtime`, `instrumentnumber`, `kiddate`, `kidscore`, `kiduser`, `kidversion`, `name_blastexpandlast`, `name_blastomeresize`, `name_dynamicscore`, `name_mn2type`, `name_morphologicalgrade`, `name_multinucleation`, `name_reexpansioncount`, `patientidx`, `time_blastexpandlast`, `time_blastomeresize`, `time_dynamicscore`, `time_mn2type`, `time_morphologicalgrade`, `time_multinucleation`, `time_reexpansioncount`, `timestamp_blastexpandlast`, `timestamp_blastomeresize`, `timestamp_dynamicscore`, `timestamp_mn2type`, `timestamp_morphologicalgrade`, `timestamp_multinucleation`, `timestamp_reexpansioncount`, `treatmentname`, `value_blastexpandlast`, `value_blastomeresize`, `value_dynamicscore`, `value_mn2type`, `value_morphologicalgrade`, `value_multinucleation`, `value_reexpansioncount`, `wellnumber`
* Columns only in Athena `silver_embryoscope_prod.embryo_data`: `_dlt_id`, `bronze_updated_at`, `embryo_data_sk`, `embryo_description_id`, `embryo_fate`, `embryo_id`, `fertilization_method`, `fertilization_time`, `instrument_number`, `kid_date`, `kid_score`, `kid_user`, `kid_version`, `name_blast_expand_last`, `name_blastomere_size`, `name_mn2_type`, `name_morphological_grade`, `name_multi_nucleation`, `name_reexpansion_count`, `patient_id_x`, `prontuario`, `time_blast_expand_last`, `time_blastomere_size`, `time_mn2_type`, `time_morphological_grade`, `time_multi_nucleation`, `time_reexpansion_count`, `timestamp_blast_expand_last`, `timestamp_blastomere_size`, `timestamp_mn2_type`, `timestamp_morphological_grade`, `timestamp_multi_nucleation`, `timestamp_reexpansion_count`, `treatment_name`, `value_blast_expand_last`, `value_blastomere_size`, `value_mn2_type`, `value_morphological_grade`, `value_multi_nucleation`, `value_reexpansion_count`, `well_number`


---

## 2. Row Count & Volume Validation (Full Datasets)
| Table | Local Count | Athena Count | Difference | Pct Diff | Status / Observations |
| :--- | :---: | :---: | :---: | :---: | :--- |
| patients | 13,880 | 13,219 | +661 | +5.00% | Variance Detected |
| treatments | 23,472 | 22,305 | +1,167 | +5.23% | Variance Detected |
| embryo_data | 149,123 | 141,433 | +7,690 | +5.44% | Variance Detected |

---

## 3. Actionable Recommendations
1. **Pipeline Sync**: Check if the AWS Athena ingest pipelines have run completely up to the latest extraction timestamps.
2. **Standardize Keys**: Ensure downstream integrations use standard lowercased keys for clean mappings.
