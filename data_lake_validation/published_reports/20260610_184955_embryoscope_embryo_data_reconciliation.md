# Data Lake Reconciliation Report: silver_embryoscope.embryo_data vs. Athena silver_embryoscope_staging.embryo_data

> [!NOTE]
> ### 📊 Data Lake Ingestion & Promotion Quality KPI Dashboard
> * **Row Count Match Rate**: **86.265%** (121,545 Local vs 140,898 Target - **-19,353 difference**)
> * **Local Key Overlap Rate**: **94.557%**
> * **Target Key Overlap Rate**: **81.569%**
> * **Overall Value Alignment**: **N/A**


This report presents a generalized reconciliation audit between **silver_embryoscope.embryo_data** and **Athena silver_embryoscope_staging.embryo_data** using automated keys verification.

---

## 1. Schema Comparison
* Local columns count: **173**
* Target columns count: **171**

### 1.1 Columns Only in Local
`EmbryoDescriptionID`, `EmbryoFate`, `EmbryoID`, `FertilizationMethod`, `FertilizationTime`, `InstrumentNumber`, `KIDDate`, `KIDScore`, `KIDUser`, `KIDVersion`, `Name_BlastExpandLast`, `Name_BlastomereSize`, `Name_MN2Type`, `Name_MorphologicalGrade`, `Name_MultiNucleation`, `Name_ReexpansionCount`, `PatientIDx`, `Time_BlastExpandLast`, `Time_BlastomereSize`, `Time_MN2Type`, `Time_MorphologicalGrade`, `Time_MultiNucleation`, `Time_ReexpansionCount`, `Timestamp_BlastExpandLast`, `Timestamp_BlastomereSize`, `Timestamp_MN2Type`, `Timestamp_MorphologicalGrade`, `Timestamp_MultiNucleation`, `Timestamp_ReexpansionCount`, `TreatmentName`, `Value_BlastExpandLast`, `Value_BlastomereSize`, `Value_MN2Type`, `Value_MorphologicalGrade`, `Value_MultiNucleation`, `Value_ReexpansionCount`, `WellNumber`, `_extraction_timestamp`, `_location`, `_row_hash`, `_run_id`, `embryo_number`

### 1.2 Columns Only in Target
`_dlt_id`, `bronze_updated_at`, `embryo_data_sk`, `embryo_description_id`, `embryo_fate`, `embryo_id`, `fertilization_method`, `fertilization_time`, `instrument_number`, `kid_date`, `kid_score`, `kid_user`, `kid_version`, `name_blast_expand_last`, `name_blastomere_size`, `name_mn2_type`, `name_morphological_grade`, `name_multi_nucleation`, `name_reexpansion_count`, `patient_id_x`, `time_blast_expand_last`, `time_blastomere_size`, `time_mn2_type`, `time_morphological_grade`, `time_multi_nucleation`, `time_reexpansion_count`, `timestamp_blast_expand_last`, `timestamp_blastomere_size`, `timestamp_mn2_type`, `timestamp_morphological_grade`, `timestamp_multi_nucleation`, `timestamp_reexpansion_count`, `treatment_name`, `value_blast_expand_last`, `value_blastomere_size`, `value_mn2_type`, `value_morphological_grade`, `value_multi_nucleation`, `value_reexpansion_count`, `well_number`

---

## 2. Row Count & Volume Validation (Full Datasets)

| Table | Local Count | Target Count | Difference | Pct Diff | Status |
| :--- | :---: | :---: | :---: | :---: | :--- |
| **embryo_data** | 121,545 | 140,898 | -19,353 | -13.74% | Variance Detected |

---

## 3. Key Overlap Summary (Joined on EmbryoID)

* **Total Overlapping Keys**: **114,929**
* **Keys ONLY in Local**: **6,616**
* **Keys ONLY in Target**: **25,969**

---

## 4. Key Takeaways & Root Cause Analysis

### 4.1 Sample Records Only in Local (6,616 total)
| Embryoid |
| :---: |
| D2025.04.13_S04155_I3166_P-1 |
| D2025.04.13_S04155_I3166_P-2 |
| D2025.04.13_S04155_I3166_P-3 |
| D2025.04.13_S04155_I3166_P-4 |
| D2025.04.13_S04155_I3166_P-5 |
| D2025.04.13_S04155_I3166_P-6 |
| D2025.04.13_S04155_I3166_P-7 |
| D2025.04.13_S04155_I3166_P-8 |
| D2025.06.10_S04275_I3166_P-1 |
| D2025.06.10_S04275_I3166_P-2 |
| D2025.06.10_S04275_I3166_P-3 |
| D2025.06.10_S04275_I3166_P-4 |
| D2025.06.10_S04275_I3166_P-5 |
| D2025.06.10_S04275_I3166_P-6 |
| D2025.06.10_S04275_I3166_P-7 |

### 4.2 Sample Records Only in Target (25,969 total)
| Embryo_Id |
| :---: |
| D2026.05.13_S04226_I3253_P-3 |
| D2026.05.18_S01123_I4268_8-2 |
| D2026.05.28_S04253_I3253_P-1 |
| D2026.05.21_S00101_I5521_P-1 |
| D2026.05.21_S00101_I5521_P-2 |
| D2026.05.21_S00101_I5521_P-3 |
| D2026.05.21_S00101_I5521_P-4 |
| D2026.05.21_S00101_I5521_P-5 |
| D2026.05.21_S00101_I5521_P-6 |
| D2026.05.21_S00101_I5521_P-7 |
| D2026.05.21_S00101_I5521_P-8 |
| D2026.05.21_S00101_I5521_P-9 |
| D2026.05.21_S00101_I5521_P-10 |
| D2026.05.21_S00101_I5521_P-11 |
| D2026.05.21_S00101_I5521_P-12 |


---

## 5. Actionable Recommendations
1. **Deduplicate / Trace Gaps**: Inspect missing records in either local or remote target databases using the sample keys above.
2. **Handle Schema Drift**: If column mismatches exist, update downstream mapping logic or execute database alter statements.
