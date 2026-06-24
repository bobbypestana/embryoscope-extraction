# Remoção de Casos de Teste no Pipeline do EmbryoScope

No pipeline de dados do EmbryoScope (camada **Bronze** para **Silver**), os casos de teste e registros inválidos (como IDs não numéricos, nulos ou associados a bancos de dados de teste) são filtrados e removidos por meio de regras específicas de limpeza.

Abaixo está a explicação detalhada de onde essa remoção ocorre, a lógica aplicada e os respectivos exemplos de código.

---

## 1. Filtragem de Bancos de Dados de Teste (Consolidação)
Durante a etapa de consolidação de múltiplos bancos de dados do EmbryoScope, qualquer arquivo de banco de dados que possua a palavra `'test'` em seu caminho/nome é explicitamente ignorado.

* **Arquivo:** [`02_03_consolidate_embryoscope_dbs.py`](file:///g:/My%20Drive/projetos_individuais/Huntington/embryoscope/01_get_embryo_data/02_03_consolidate_embryoscope_dbs.py)
* **Lógica (Python):**
```python
# Busca todos os DBs por clínica e exclui bases de teste e centrais
for db_path in db_files:
    if 'test' in db_path or 'huntington_data_lake' in db_path:
        logger.debug(f"Skipping DB (test or central): {db_path}")
        continue
```

---

## 2. Descarte de Pacientes com IDs de Teste / Inválidos
Durante a transformação de Bronze para Silver, a tabela de pacientes passa por uma higienização do campo `PatientID` através do script auxiliar `patient_id_cleaner.py`. Qualquer ID que contenha caracteres não-numéricos (comum em IDs de teste como `"TESTE01"`, `"DEMO"`, etc.), que resulte em zero (`0`), ou que seja nulo é classificado como inválido e descartado.

* **Arquivo:** [`patient_id_cleaner.py`](file:///g:/My%20Drive/projetos_individuais/Huntington/embryoscope/01_get_embryo_data/patient_id_cleaner.py) (chamado a partir do [`02_01_bronze_to_silver.py`](file:///g:/My%20Drive/projetos_individuais/Huntington/embryoscope/01_get_embryo_data/02_01_bronze_to_silver.py))
* **Lógica (Python):**
```python
def convert_to_int(patient_id):
    if pd.isna(patient_id) or patient_id is None:
        return None
    
    patient_id_str = str(patient_id).strip()
    
    # Remove pontos de IDs formatados (ex: "520.124" -> 520124)
    if '.' in patient_id_str:
        try:
            cleaned_str = patient_id_str.replace('.', '')
            if cleaned_str.isdigit():
                converted_id = int(cleaned_str)
                if converted_id == 0:
                    return None
                return converted_id
        except (ValueError, AttributeError):
            pass
    
    # Valida se é puramente numérico
    if patient_id_str.isdigit():
        converted_id = int(patient_id_str)
        if converted_id == 0:
            return None
        return converted_id
    
    # Se contiver letras (ex: 'TEST123') ou outros caracteres inválidos, retorna None (descartado)
    return None

# Aplica a limpeza e mantém apenas registros válidos
df_clean['PatientID'] = df_clean['PatientID'].apply(convert_to_int)
df_valid = df_clean[df_clean['PatientID'].notna()]
```

---

## 3. Limpeza dos Dados Órfãos (Cascade Cleanup)
Pacientes inválidos/de teste que foram eliminados no passo anterior deixariam registros órfãos nas outras tabelas relacionadas (`treatments`, `embryo_data`, e `idascore`). Um script de limpeza de pós-processamento remove em cascata esses dados órfãos da camada Silver.

* **Arquivo:** [`02_02_cleanup_silver_layer.py`](file:///g:/My%20Drive/projetos_individuais/Huntington/embryoscope/01_get_embryo_data/02_02_cleanup_silver_layer.py)
* **Lógica (SQL / DuckDB):**

Primeiro, identifica-se quais `PatientIDx` existem na camada Bronze (`bronze.raw_patients`), mas não foram salvos na tabela higienizada da camada Silver (`silver.patients`):

```sql
-- Obtém os IDs na camada Bronze
SELECT DISTINCT 
    raw_json->>'$.PatientIDx' as patient_idx,
    raw_json->>'$.PatientID' as patient_id
FROM bronze.raw_patients;

-- Obtém os IDs que de fato estão na camada Silver
SELECT DISTINCT PatientIDx FROM silver.patients;
```

Com a diferença de conjuntos (`bronze_set - silver_set`), os IDs órfãos (que incluem os casos de teste) são deletados das demais tabelas Silver:

```sql
-- Remove tratamentos de pacientes descartados
DELETE FROM silver.treatments 
WHERE PatientIDx IN (?);

-- Remove dados de embriões de pacientes descartados
DELETE FROM silver.embryo_data 
WHERE PatientIDx IN (?);

-- Remove pontuações IDA de pacientes descartados
DELETE FROM silver.idascore 
WHERE PatientIDx IN (?);
```
