# Data Lake Reconciliation Report: silver.view_exames vs. Athena silver_clinisys_staging.view_exames

> [!NOTE]
> ### 📊 Data Lake Ingestion & Promotion Quality KPI Dashboard
> * **Row Count Match Rate**: **99.941%** (35,586 Local vs 35,607 Target - **-21 difference**)
> * **Local Key Overlap Rate**: **99.927%**
> * **Target Key Overlap Rate**: **99.868%**
> * **Overall Value Alignment**: **N/A**


This report presents a generalized reconciliation audit between **silver.view_exames** and **Athena silver_clinisys_staging.view_exames** using automated keys verification.

---

## 1. Schema Comparison
* Local columns count: **100**
* Target columns count: **100**

### 1.1 Columns Only in Local
`AmostraCrio`, `CaneCrio`, `CateterIUIHom`, `CateterPSIUIHet`, `CelulasCromatina`, `CelulasTUNELPositivas`, `CelulasTunelControle`, `CheckAvaliacaoMacroscopica`, `CheckAvaliacaoMicroscopica`, `CheckCrio`, `CheckExamePersonalizado`, `CheckHalosperm`, `CheckIUIHet`, `CheckIUIHom`, `CheckTunel`, `CodExame`, `CodigoCrio`, `ConcentracaoMicro`, `ConcentracaoPosIUIHet`, `ConcentracaoPreIUIHet`, `ConcentracaoProgressivos`, `ConcentracaoTotalMicro`, `ConclusaoHalosperm`, `ConclusaoTUNEL`, `CongelamentoSemenIUIHet`, `CongelamentoSemenIUIHom`, `CriomotilidadeCrio`, `DataCongelamentoIUIHet`, `DataCrio`, `DataExame`, `DiagnosticoClinico`, `EptzMoveisPSIUIHet`, `EstimativaVialCrio`, `ExamePersonalizado`, `GavetaCilindroCrio`, `HoraColeta`, `IdadePacienteCrio`, `IdadePacienteExame`, `IdadePacienteExame2`, `InseminacoesPossiveisCrio`, `LavagemPSIUIHet`, `LocalColeta`, `LoteIUIHom`, `LotePSIUIHet`, `MeioCulturaPSIUIHet`, `MetodoColeta`, `MorfologiaMicro`, `MorfologiaPreIUIHet`, `MotilidadeDescongelamentoCrio`, `MotilidadeInicialPreIUIHet`, `MotilidadeNaoProgressiva`, `MotilidadePSIUIHet`, `MotilidadePosIUIHet`, `MotilidadeProgressiva`, `MotilidadeProgressivaA`, `MotilidadeProgressivaB`, `MotilidadeTotal`, `NumeroAmostraIUIHet`, `NumeroDoadorIUIHet`, `PacienteCrio`, `ProcessamentoMinutos`, `ProvenienciaIUIHet`, `ResultadosSorologiasIUIHet`, `SelectGavetaCilindroCrio`, `SelectPalhetaVialsCrio`, `SobrevidaDescongelamentoCrio`, `SorologiasDoadorIUIHet`, `TanqueCrio`, `TecnicaCongelamentoCrio`, `TempoAbstinencia`, `TotalSptzMoveis`, `UsoMedicamento`, `ValidadeIUIHom`, `ValidadePSIUIHet`, `VialsCrio`, `VialsDescongeladasIUIHet`, `VialsDescongeladasIUIHom`, `VialsSleeperCrio`, `VitalidadeMicro`, `VolumeFinalCrio`, `VolumeFinalPSIUIHet`, `VolumeInicialCrio`, `VolumeMicroscopica`, `VolumePreIUIHet`, `VolumeVialsCrio`, `VolumeVialsSleeperCrio`, `extraction_timestamp`, `hash`, `pH`

### 1.2 Columns Only in Target
`_dlt_id`, `amostra_crio`, `bronze_updated_at`, `cane_crio`, `cateter_iui_hom`, `cateter_psiui_het`, `celulas_cromatina`, `celulas_tunel_controle`, `celulas_tunel_positivas`, `check_avaliacao_macroscopica`, `check_avaliacao_microscopica`, `check_crio`, `check_exame_personalizado`, `check_halosperm`, `check_iui_het`, `check_iui_hom`, `check_tunel`, `cod_exame`, `codigo_crio`, `concentracao_micro`, `concentracao_pos_iui_het`, `concentracao_pre_iui_het`, `concentracao_progressivos`, `concentracao_total_micro`, `conclusao_halosperm`, `conclusao_tunel`, `congelamento_semen_iui_het`, `congelamento_semen_iui_hom`, `criomotilidade_crio`, `data_congelamento_iui_het`, `data_crio`, `data_exame`, `diagnostico_clinico`, `eptz_moveis_psiui_het`, `estimativa_vial_crio`, `exame_personalizado`, `gaveta_cilindro_crio`, `hora_coleta`, `idade_paciente_crio`, `idade_paciente_exame`, `idade_paciente_exame2`, `inseminacoes_possiveis_crio`, `lavagem_psiui_het`, `local_coleta`, `lote_iui_hom`, `lote_psiui_het`, `meio_cultura_psiui_het`, `metodo_coleta`, `morfologia_micro`, `morfologia_pre_iui_het`, `motilidade_descongelamento_crio`, `motilidade_inicial_pre_iui_het`, `motilidade_nao_progressiva`, `motilidade_pos_iui_het`, `motilidade_progressiva`, `motilidade_progressiva_a`, `motilidade_progressiva_b`, `motilidade_psiui_het`, `motilidade_total`, `numero_amostra_iui_het`, `numero_doador_iui_het`, `p_h`, `paciente_crio`, `processamento_minutos`, `proveniencia_iui_het`, `resultados_sorologias_iui_het`, `select_gaveta_cilindro_crio`, `select_palheta_vials_crio`, `sobrevida_descongelamento_crio`, `sorologias_doador_iui_het`, `tanque_crio`, `tecnica_congelamento_crio`, `tempo_abstinencia`, `total_sptz_moveis`, `uso_medicamento`, `validade_iui_hom`, `validade_psiui_het`, `vials_crio`, `vials_descongeladas_iui_het`, `vials_descongeladas_iui_hom`, `vials_sleeper_crio`, `vitalidade_micro`, `volume_final_crio`, `volume_final_psiui_het`, `volume_inicial_crio`, `volume_microscopica`, `volume_pre_iui_het`, `volume_vials_crio`, `volume_vials_sleeper_crio`

---

## 2. Row Count & Volume Validation (Full Datasets)

| Table | Local Count | Target Count | Difference | Pct Diff | Status |
| :--- | :---: | :---: | :---: | :---: | :--- |
| **view_exames** | 35,586 | 35,607 | -21 | -0.06% | Variance Detected |

---

## 3. Key Overlap Summary (Joined on id)

* **Total Overlapping Keys**: **35,560**
* **Keys ONLY in Local**: **26**
* **Keys ONLY in Target**: **47**

---

## 4. Key Takeaways & Root Cause Analysis

### 4.1 Sample Records Only in Local (26 total)
| Id |
| :---: |
| 34375 |
| 35867 |
| 30215 |
| 30970 |
| 36076 |
| 22941 |
| 29908 |
| 32944 |
| 33898 |
| 31078 |
| 33662 |
| 36275 |
| 30052 |
| 30124 |
| 30118 |

### 4.2 Sample Records Only in Target (47 total)
| Id |
| :---: |
| 36788 |
| 36765 |
| 36766 |
| 36767 |
| 36768 |
| 36769 |
| 36770 |
| 36771 |
| 36772 |
| 36773 |
| 36774 |
| 36775 |
| 36776 |
| 36777 |
| 36778 |


---

## 5. Actionable Recommendations
1. **Deduplicate / Trace Gaps**: Inspect missing records in either local or remote target databases using the sample keys above.
2. **Handle Schema Drift**: If column mismatches exist, update downstream mapping logic or execute database alter statements.
