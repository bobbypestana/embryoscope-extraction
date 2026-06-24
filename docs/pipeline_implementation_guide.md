# Huntington Data Lake: Pipeline Implementation Guide



## 3. Step-by-Step Implementation Guide

### Flow 1: Planilha Plóidia Pipeline (`planilha_ploidia`)

1.  **Step 1: Merge Clinisys and Embryoscope**
    *   *Script*: `01_merge_clinisys_embryoscope.py` (inside `data_lake_scripts/`)
    *   *Inputs*: `gold.clinisys_embrioes` + `gold.embryoscope_embrioes`
    *   *Output*: `gold.embryoscope_clinisys_combined`
    *   *Key Join*: Match on fertilization dates, patient charts (`prontuario`), and embryo counts.

2.  **Step 2: Combine Fresh & FET Sheets**
    *   *Script*: `01_combine_fresh_fet.py` (inside `planilha_embriologia/02_create_tables/`)
    *   *Inputs*: `silver.planilha_embriologia_*` tables
    *   *Output*: `silver.planilha_embriologia_combined`

3.  **Step 3: Combine Redlara with Combined Planilha**
    *   *Script*: `02_combine_redlara_planilha.py` (inside `data_lake_scripts/`)
    *   *Inputs*: `silver.redlara_unified` + `silver.planilha_embriologia_combined`
    *   *Output*: `gold.redlara_planilha_combined`

4.  **Step 4: Combine Embryoscope/Clinisys with Redlara/Planilha**
    *   *Script*: `03_combine_embryoscope_planilha.py` (inside `data_lake_scripts/`)
    *   *Inputs*: `gold.embryoscope_clinisys_combined` + `gold.redlara_planilha_combined`
    *   *Output*: `gold.planilha_embryoscope_combined`
    *   *Key Logic*: Waterfall join strategy combining punção, transfer, and cryopreservation events.

5.  **Step 5: Create Initial Plóidia Table**
    *   *Script*: `01_create_data_ploidia_table.py` (inside `planilha_ploidia/`)
    *   *Inputs*: `gold.planilha_embryoscope_combined` + column mapping config `00_02_column_mapping.py` + Clinisys `silver.view_tratamentos`
    *   *Output*: `gold.data_ploidia` (Initial Mapping)

6.  **Step 6: Metric Enrichment & Cleaning**
    *   *Script*: `02_fill_missing_values.py` (inside `planilha_ploidia/`)
    *   *Inputs*: `gold.data_ploidia` (Initial Mapping)
    *   *Output*: `gold.data_ploidia` (Metrics Enriched)
    *   *Details*: Calculates clinical values such as patient age, BMI, and relevant clinical flags.

7.  **Step 7: Join Image Availability**
    *   *Script*: `03_join_image_availability.py` (inside `planilha_ploidia/`)
    *   *Inputs*: `gold.data_ploidia` (Metrics Enriched) + `silver.embryo_image_availability_latest` (created by check/silver steps in `embryoscope/02_images_availability_report/`)
    *   *Output*: Fully populated `gold.data_ploidia` (Final table) with image API logs.

---

### Flow 2: Financial Operations Pipeline (`finops`)

1.  **Step 1: Protheus ERP Ingestion**
    *   *Scripts* (run in sequence inside `protheus/01_ingestion/`):
        1.  `01_source_to_bronze.py`: Ingests ERP source data to `bronze.protheus_*`.
        2.  `02_bronze_to_silver.py`: Cleans, formats, and casts data types into `silver.protheus_*`.
        3.  `03_silver_to_gold.py`: Combines silver tables and attaches Clinisys view `silver.view_pacientes` to resolve patient chart IDs (`prontuario`), writing the final `gold.protheus_mesclada_vendas` table.

2.  **Step 2: Create All Patients Timeline**
    *   *Script*: `01_create_all_patient_timeline.py` (inside `finops/02_create_tables/`)
    *   *Inputs*: `gold.protheus_mesclada_vendas` + Clinisys views (`silver.view_tratamentos`, `silver.view_extrato_atendimentos_central`, `silver.view_congelamentos_embrioes`, etc.)
    *   *Output*: `gold.all_patients_timeline`

3.  **Step 3: Timeline Cleaning**
    *   *Script*: `02_create_clean_timeline.py` (inside `finops/02_create_tables/`)
    *   *Inputs*: `gold.all_patients_timeline`
    *   *Output*: `gold.recent_patients_timeline`
    *   *Details*: Filters recent records (events from 2023 onwards) and ranks them chronologically.

4.  **Step 4: Create FinOps Summary**
    *   *Script*: `03_01_create_finops_summary.py` (inside `finops/02_create_tables/`)
    *   *Inputs*: `gold.recent_patients_timeline` + `gold.protheus_mesclada_vendas`
    *   *Output*: `gold.finops_summary`

5.  **Step 5: Patient Info & Domain Timelines Slices**
    *   *Scripts*:
        *   `03_00_create_patient_info.py` (Reads `gold.recent_patients_timeline`, outputs `gold.patient_info`)
        *   `03_02_biopsy_pgta_timeline.py` (Reads `gold.recent_patients_timeline`, outputs biopsy timeline)
        *   `03_03_embryoscope_timeline.py` (Reads `gold.recent_patients_timeline`, outputs embryoscope event timeline)
        *   `03_04_a_embryo_freeze_timeline.py` (Reads `gold.recent_patients_timeline`, outputs embryo freezing log)
        *   `03_04_b_cryopreservation_events_timeline.py` (Reads `gold.recent_patients_timeline`, outputs cryopreservation log)
        *   `03_05_consultas_timeline.py` (Reads `gold.recent_patients_timeline`, outputs consultation event log)

---

### Flow 3: Dados para Pesquisa (Prescription Pipeline)

1.  **Step 1: Join Prescriptions**
    *   *Script*: `01_join_prescriptions.py` (inside `embryos_with_prescription/`)
    *   *Inputs*: `gold.planilha_embryoscope_combined` + Clinisys `silver.view_medicamentos_prescricoes`
    *   *Output*: `gold.embryos_with_prescription_long`

2.  **Step 2: Pivot to Wide Format**
    *   *Script*: `02_create_wide_table.py` (inside `embryos_with_prescription/`)
    *   *Inputs*: `gold.embryos_with_prescription_long`
    *   *Output*: `gold.embryos_with_prescription_wide (Dados para Pesquisa)`
    *   *Details*: Reshapes the prescription list per embryo into a wide format, optimized for academic research.

---

### Flow 4: Feedback Finops / Relatório de cobranças [Planned]

*   **Status**: Planned / Placeholder
*   **Objectives**: To close the feedback loop on financial billing operations and generate structured billing reports (`relatórios de cobranças`).
*   **Expected Dependencies**:
    *   `gold.finops_summary`
    *   `gold.protheus_mesclada_vendas`
    *   `gold.all_patients_timeline`
*   **Implementation Steps**:
    *   *(To be defined: SQL aggregates, formatting rules, Excel/CSV output pipelines, and Power BI visual integrations)*.

---

### Flow 5: RD Station Integration [Planned]

*   **Status**: Planned / Placeholder
*   **Objectives**: Sync treatment updates, milestones, and patient details from the Data Lake to RD Station CRM for marketing automation and patient journey tracking.
*   **Expected Dependencies**:
    *   `I_RDStation` (Core Ingestions)
*   **Implementation Steps**:
    *   *(To be defined: API connectors, contact property mappings, sync schedules)*.
