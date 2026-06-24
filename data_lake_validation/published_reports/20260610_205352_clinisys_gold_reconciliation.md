# Data Lake Reconciliation Report: gold.clinisys_embrioes vs. silver.view_micromanipulacao_oocitos

> [!NOTE]
> ### 📊 Data Lake Ingestion & Promotion Quality KPI Dashboard
> * **Row Count Match Rate**: **99.803%** (307,516 Local vs 308,123 Target - **-607 difference**)
> * **Local Key Overlap Rate**: **100.000%**
> * **Target Key Overlap Rate**: **99.411%**
> * **Overall Value Alignment**: **99.751%**


This report presents a generalized reconciliation audit between **gold.clinisys_embrioes** and **silver.view_micromanipulacao_oocitos** using automated keys verification.

---

## 1. Schema Comparison
* Local columns count: **56**
* Target columns count: **56**

### 1.1 Columns Only in Local
`cong_em_CodCongelamento`, `cong_em_Data`, `cong_em_id`, `descong_em_CodDescongelamento`, `descong_em_DataCongelamento`, `descong_em_DataDescongelamento`, `descong_em_DataTransferencia`, `descong_em_id`, `emb_cong_id`, `emb_cong_qualidade`, `emb_cong_transferidos`, `flag_is_external_medical`, `micro_CicloDoadora`, `micro_Data_DL`, `micro_data_procedimento`, `micro_numero_caso`, `micro_oocitos`, `micro_prontuario`, `micro_recepcao_ovulos`, `nome_medico`, `oocito_OrigemOocito`, `oocito_ResultadoPGD`, `oocito_ResultadoPGDDetalhes`, `oocito_TCD`, `oocito_embryo_number`, `oocito_id`, `oocito_id_micromanipulacao`, `tipo_medico`, `trat1_bmi`, `trat1_data_inicio_inducao`, `trat1_data_transferencia`, `trat1_fator_infertilidade1`, `trat1_id`, `trat1_idade_esposa`, `trat1_idade_marido`, `trat1_motivo_nao_transferir`, `trat1_origem_ovulo`, `trat1_previous_et`, `trat1_previous_et_od`, `trat1_prontuario_doadora`, `trat1_resultado_tratamento`, `trat1_tentativa`, `trat1_tipo_procedimento`, `trat2_bmi`, `trat2_data_inicio_inducao`, `trat2_data_transferencia`, `trat2_id`, `trat2_idade_esposa`, `trat2_idade_marido`, `trat2_motivo_nao_transferir`, `trat2_previous_et`, `trat2_previous_et_od`, `trat2_prontuario_doadora`, `trat2_resultado_tratamento`, `trat2_tentativa`, `trat2_tipo_procedimento`

### 1.2 Columns Only in Target
`AH`, `Blastomero_D2`, `Blastomero_D3`, `ComentariosAntes`, `ComentariosDepois`, `Compactando_D4`, `Compactando_D5`, `Compactando_D6`, `Compactando_D7`, `CorpusculoPolar`, `Embriologista`, `Fertilizacao`, `Frag_D2`, `Frag_D3`, `GD1`, `GD2`, `ICSI`, `IdentificacaoPGD`, `InseminacaoOocito`, `MassaInterna_D4`, `MassaInterna_D5`, `MassaInterna_D6`, `MassaInterna_D7`, `Maturidade`, `NCelulas_D2`, `NCelulas_D3`, `NCelulas_D4`, `NCelulas_D5`, `NCelulas_D6`, `NCelulas_D7`, `NClivou_D2`, `NClivou_D3`, `NClivou_D4`, `NClivou_D5`, `NClivou_D6`, `NClivou_D7`, `OocitoDoado`, `OrigemOocito`, `PGD`, `PI`, `RC`, `ResultadoPGD`, `ResultadoPGDDetalhes`, `TCD`, `Trofoblasto_D4`, `Trofoblasto_D5`, `Trofoblasto_D6`, `Trofoblasto_D7`, `diaseguinte`, `embryo_number`, `extraction_timestamp`, `hash`, `id`, `id_micromanipulacao`, `relatorio_ia`, `score_maia`

---

## 2. Row Count & Volume Validation (Full Datasets)

| Table | Local Count | Target Count | Difference | Pct Diff | Status |
| :--- | :---: | :---: | :---: | :---: | :--- |
| **clinisys_embrioes** | 307,516 | 308,123 | -607 | -0.20% | Variance Detected |

---

## 3. Key Overlap Summary (Joined on oocito_id)

* **Total Overlapping Keys**: **306,307**
* **Keys ONLY in Local**: **0**
* **Keys ONLY in Target**: **1,816**

### 3.1 Overlap Value Comparison
| Column Name | Local Sum | Target Sum | Difference | Alignment Rate |
| :--- | :---: | :---: | :---: | :---: |
| **oocito_embryo_number** | 2934116.00 | 2926829.00 | +7287.00 | 99.7510% |

---

## 4. Key Takeaways & Root Cause Analysis

### 4.2 Sample Records Only in Target (1,816 total)
| Id | Embryo_Number |
| :---: | :---: |
| 315688 | 1 |
| 315689 | 2 |
| 315690 | 3 |
| 315691 | 4 |
| 315692 | 5 |
| 315871 | 1 |
| 315872 | 2 |
| 315873 | 3 |
| 315874 | 4 |
| 315875 | 5 |
| 315876 | 6 |
| 315877 | 7 |
| 315885 | 1 |
| 315886 | 2 |
| 315887 | 3 |


---

## 5. Actionable Recommendations
1. **Deduplicate / Trace Gaps**: Inspect missing records in either local or remote target databases using the sample keys above.
2. **Handle Schema Drift**: If column mismatches exist, update downstream mapping logic or execute database alter statements.
