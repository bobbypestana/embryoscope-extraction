# Plano de Alinhamento: Pipeline Local (DuckDB) vs. Produção (Athena)

> **Data de Emissão:** 26/08/2026  
> **Status:** Local Atualizado e Sincronizado  
> **Objetivo:** Documentar as correções implementadas no ambiente local e as ações necessárias em produção para garantir 100% de paridade entre as bases.

---

## 1. O que foi implementado no ambiente local (DuckDB)

1. **Exclusão total de `CASOS 2022 BSB.xlsx`**:
   - Adicionada regra no script de bronze (`01_planilha_embriologia_to_bronze.py`) para ignorar explicitamente o arquivo `CASOS 2022 BSB.xlsx`.
   - Removida a configuração da tabela `planilha_2022_bsb_sheet1` na Silver (`02_planilha_embriologia_to_silver.py`).
   - Executado o comando `DROP TABLE IF EXISTS bronze.planilha_2022_bsb_sheet1` no DuckDB local.

2. **Eliminação da duplicidade de `CASOS 2023 IBIRA`**:
   - O leitor de Bronze foi configurado para priorizar a aba `Total 2023 Nova` (2.279 linhas) em detrimento da aba legada `Total 2023` (2.163 linhas).
   - Adicionada rotina de expurgo com `DROP TABLE IF EXISTS bronze.planilha_2023_ibira_total_2023`, eliminando a duplicação de ~1.030 linhas com os mesmos pacientes no Silver local.

3. **Mapeamento de Salvador pré-2024 (`Prontuário` $\rightarrow$ `pin`)**:
   - No script Silver (`02_planilha_embriologia_to_silver.py`), as tabelas de SSA 2022 e 2023 já estão mapeadas para extrair o `pin` a partir da coluna original `Prontuário`, preservando a identidade dos pacientes no build local.

4. **Ingestão contínua da aba `FOT` para 2024–2026**:
   - O pipeline local lê as abas `['FRESH', 'FET', 'FOT', 'RECEP']` para todos os anos modernos, preservando a série histórica de procedimentos FOT após 2024.

5. **Deduplicação de arquivos `IBI` vs `IBIRA` (2022 e 2023)**:
   - O leitor local descarta arquivos terminados em `IBI.xlsx` quando existe um arquivo `IBIRA.xlsx` correspondente na mesma pasta de ano.

6. **Reexecução completa do pipeline local**:
   - **Bronze:** 120.234 linhas carregadas across 31 arquivos.
   - **Silver Fresh:** 17.820 linhas.
   - **Silver FET:** 17.298 linhas.
   - **Tabela Combinada:** 27.081 linhas reconciliadas.

---

## 2. O que precisa ser feito em Produção (Athena / Pipeline Prod)

| # | Item / Componente | Ação Necessária em Prod | Impacto / Justificativa |
| :---: | :--- | :--- | :--- |
| **1** | **Exclusão de `CASOS 2022 BSB`** | Configurar o crawler/ingestor S3 para ignorar `CASOS 2022 BSB.xlsx` e expurgar a partição correspondente no Athena. | Garante que o arquivo excluído a pedido não contamine os indicadores globais da clínica. |
| **2** | **Ingestão da aba `FOT` (2024+)** | Adicionar a leitura da aba `FOT` nos arquivos de 2024 em diante no crawler/pipeline de ingestão. | **Corrige o "degrau artificial de 2024"** no Athena, recuperando ~830 ciclos/ano de FOT que sumiram ao virar aba dedicada. |
| **3** | **Deploy do `coalesce` de Salvador** | Promover de staging para produção o modelo com `coalesce(pin, prontuario)` para SSA pré-2024. | Recupera a identidade e resolução de prontuário de 1.244 ciclos de SSA 2022/2023 que estavam sem PIN. |
| **4** | **Regra de abas do `2023 IBIRA`** | Assegurar que o pipeline leia exclusivamente a aba `Total 2023 Nova` (descartando `Total 2023`). | Evita dupla contagem de ~1.030 ciclos em São Paulo. |
| **5** | **Deduplicação `IBI` vs `IBIRA`** | Garantir que o pipeline no S3/Athena ingira apenas arquivos `IBIRA` quando houver duplicata `IBI`. | Evita ciclos duplicados em 2022 e 2023. |
| **6** | **Alerta de Schema `CASOS 2026 RIO`** | Notificar a equipe de origem da unidade Rio para incluir a coluna de identificador (`PIN`/`PRONTUÁRIO`) na planilha. | Atualmente 100% das linhas do Rio 2026 são descartadas no Silver por falta de coluna de PIN. |

---

## 3. Matriz de Paridade Atual

```
+------------------------------------+---------------+---------------+
| Funcionalidade / Regra             | Local (DuckDB)| Prod (Athena) |
+------------------------------------+---------------+---------------+
| Exclusão CASOS 2022 BSB            |   Concluído   |   Pendente    |
| Deduplicação CASOS 2023 IBIRA      |   Concluído   |   Concluído   |
| Mapeamento Prontuário SSA pré-2024 |   Concluído   |  Em Staging   |
| Ingestão da aba FOT (2024+)        |   Concluído   |   Pendente    |
| Deduplicação IBI vs IBIRA          |   Concluído   |   Pendente    |
+------------------------------------+---------------+---------------+
```
